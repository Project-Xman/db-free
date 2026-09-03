// SOT: libsql-integration, turso-adapter, libsql-http-pipeline, sqlite-over-http

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, validate_columns, where_clause};
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, PageQuery, ResolvedConnection,
    ResultSet, SchemaCatalog, SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Serialize)]
struct PipelineRequest {
    requests: Vec<PipelineItem>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PipelineItem {
    Execute { stmt: StatementPayload },
    Close,
}

#[derive(Debug, Serialize)]
struct StatementPayload {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct PipelineResponse {
    results: Vec<PipelineResultItem>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PipelineResultItem {
    Ok { response: PipelineExecuteResponse },
    Error { error: PipelineError },
}

#[derive(Debug, Deserialize)]
struct PipelineError {
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PipelineExecuteResponse {
    Execute { result: LibsqlQueryResult },
    Close,
}

#[derive(Debug, Deserialize)]
struct LibsqlQueryResult {
    cols: Vec<LibsqlCol>,
    rows: Vec<Vec<LibsqlValue>>,
    affected_row_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LibsqlCol {
    name: String,
    decltype: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LibsqlValue {
    Null,
    Integer { value: serde_json::Value },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}

pub struct LibsqlIntegration {
    client: Client,
    pipeline_url: String,
    auth_token: Option<String>,
    database_name: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let mut host = s.host.as_deref().map(str::trim).unwrap_or("localhost").to_string();

    // Support libsql://, https://, or raw host
    if let Some(stripped) = host.strip_prefix("libsql://") {
        host = format!("https://{stripped}");
    } else if !host.starts_with("http://") && !host.starts_with("https://") {
        host = format!("https://{host}");
    }

    // Ensure pipeline endpoint
    let pipeline_url = if host.ends_with("/v2/pipeline") {
        host.clone()
    } else {
        format!("{}/v2/pipeline", host.trim_end_matches('/'))
    };

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| AppError::driver(e.to_string()))?;

    let db_name = s.database.clone().unwrap_or_else(|| "default".into());

    let integration = LibsqlIntegration {
        client,
        pipeline_url,
        auth_token: conn.secret.clone(),
        database_name: db_name,
    };

    integration.ping().await?;
    Ok(Arc::new(integration))
}

impl LibsqlIntegration {
    async fn run_sql(&self, sql: &str) -> AppResult<LibsqlQueryResult> {
        let payload = PipelineRequest {
            requests: vec![
                PipelineItem::Execute {
                    stmt: StatementPayload { sql: sql.to_string() },
                },
                PipelineItem::Close,
            ],
        };

        let mut req = self.client.post(&self.pipeline_url).json(&payload);
        if let Some(token) = &self.auth_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        let resp = req.send().await.map_err(|e| AppError::driver(format!("Turso/LibSQL request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::driver(format!("Turso/LibSQL error ({status}): {body}")));
        }

        let pipeline_resp: PipelineResponse = resp
            .json()
            .await
            .map_err(|e| AppError::driver(format!("Failed to parse Turso/LibSQL response: {e}")))?;

        for item in pipeline_resp.results {
            match item {
                PipelineResultItem::Ok {
                    response: PipelineExecuteResponse::Execute { result },
                } => return Ok(result),
                PipelineResultItem::Error { error } => {
                    return Err(AppError::driver(format!("Turso/LibSQL query error: {}", error.message)));
                }
                _ => {}
            }
        }

        Ok(LibsqlQueryResult {
            cols: Vec::new(),
            rows: Vec::new(),
            affected_row_count: Some(0),
        })
    }
}

fn convert_libsql_value(val: LibsqlValue) -> Value {
    match val {
        LibsqlValue::Null => Value::Null,
        LibsqlValue::Integer { value } => {
            if let Some(i) = value.as_i64() {
                Value::Int(i)
            } else if let Some(s) = value.as_str() {
                s.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Text(s.to_string()))
            } else {
                Value::Int(0)
            }
        }
        LibsqlValue::Float { value } => Value::Float(value),
        LibsqlValue::Text { value } => Value::Text(value),
        LibsqlValue::Blob { base64 } => Value::Bytes(base64),
    }
}

#[async_trait]
impl Integration for LibsqlIntegration {
    fn engine(&self) -> Engine {
        Engine::Libsql
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: true,
            namespaces: false,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: true,
        }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.run_sql("SELECT 1").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let res = self.run_sql("SELECT sqlite_version()").await?;
        if let Some(row) = res.rows.into_iter().next() {
            if let Some(val) = row.into_iter().next() {
                if let Value::Text(s) = convert_libsql_value(val) {
                    return Ok(Some(format!("libSQL (SQLite {s})")));
                }
            }
        }
        Ok(Some("libSQL".into()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database_name.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.database_name.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let sql = "SELECT name, type FROM sqlite_master \
                   WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' \
                   ORDER BY name";
        let res = self.run_sql(sql).await?;
        let mut tables = Vec::new();
        for row in res.rows {
            let mut iter = row.into_iter();
            let name = match iter.next() {
                Some(LibsqlValue::Text { value }) => value,
                _ => continue,
            };
            let kind = match iter.next() {
                Some(LibsqlValue::Text { value }) if value.eq_ignore_ascii_case("view") => TableKind::View,
                _ => TableKind::Table,
            };
            tables.push(TableInfo { schema: None, name, kind, row_estimate: None });
        }

        Ok(SchemaCatalog {
            schemas: vec![SchemaInfo {
                name: "main".into(),
                tables,
            }],
        })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let sql = format!("PRAGMA table_info({})", quote_ident(&table.name));
        let res = self.run_sql(&sql).await?;
        let mut cols = Vec::new();

        for row in res.rows {
            let mut iter = row.into_iter();
            let _cid = iter.next();
            let name = match iter.next() {
                Some(LibsqlValue::Text { value }) => value,
                _ => continue,
            };
            let type_name = match iter.next() {
                Some(LibsqlValue::Text { value }) => value,
                _ => String::from("TEXT"),
            };
            let notnull = match iter.next() {
                Some(LibsqlValue::Integer { value }) => value.as_i64().unwrap_or(0) != 0,
                _ => false,
            };
            let _dflt_value = iter.next();
            let pk = match iter.next() {
                Some(LibsqlValue::Integer { value }) => value.as_i64().unwrap_or(0) > 0,
                _ => false,
            };

            let ordinal = cols.len() as u32;
            cols.push(ColumnInfo {
                name,
                data_type: type_name.to_ascii_lowercase(),
                nullable: !notnull,
                primary_key: pk,
                ordinal,
            });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let target = quote_ident(&table.name);
        let where_str = where_clause(Engine::Libsql, filters);
        let sql = format!("SELECT COUNT(*) FROM {target}{where_str}");
        let res = self.run_sql(&sql).await?;

        if let Some(row) = res.rows.into_iter().next() {
            if let Some(val) = row.into_iter().next() {
                if let Value::Int(c) = convert_libsql_value(val) {
                    return Ok(c);
                }
            }
        }
        Ok(0)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;

        let target = quote_ident(&table.name);
        let where_str = where_clause(Engine::Libsql, &query.filters);
        let order_str = order_clause(Engine::Libsql, &query.sort);

        let sql = format!(
            "SELECT * FROM {target}{where_str}{order_str} LIMIT {} OFFSET {}",
            query.limit, query.offset
        );
        let res = self.run_sql(&sql).await?;

        let columns = res
            .cols
            .into_iter()
            .map(|c| ColumnMeta {
                name: c.name,
                type_name: c.decltype.unwrap_or_else(|| "text".into()).to_lowercase(),
            })
            .collect();

        let mut rows: Vec<Vec<Value>> = res
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(convert_libsql_value).collect())
            .collect();

        let max_rows = query.limit as usize;
        let truncated = rows.len() > max_rows;
        if truncated {
            rows.truncate(max_rows);
        }

        Ok(ResultSet { columns, rows, truncated })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let stmts = split_statements(sql);
        let mut results = Vec::new();

        for stmt in stmts {
            let res = self.run_sql(&stmt).await?;
            if res.cols.is_empty() {
                results.push(StatementResult::Affected {
                    rows_affected: res.affected_row_count.unwrap_or(0),
                });
            } else {
                let columns = res
                    .cols
                    .into_iter()
                    .map(|c| ColumnMeta {
                        name: c.name,
                        type_name: c.decltype.unwrap_or_else(|| "text".into()).to_lowercase(),
                    })
                    .collect();

                let truncated = res.rows.len() > max_rows;
                let rows = res
                    .rows
                    .into_iter()
                    .take(max_rows)
                    .map(|row| row.into_iter().map(convert_libsql_value).collect())
                    .collect();

                results.push(StatementResult::Rows {
                    result: ResultSet { columns, rows, truncated },
                });
            }
        }

        Ok(results)
    }

    async fn close(&self) {}

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        let cat = self.catalog().await?;
        let mut fks = Vec::new();

        for schema in cat.schemas {
            for table in schema.tables {
                let sql = format!("PRAGMA foreign_key_list({})", quote_ident(&table.name));
                let res = match self.run_sql(&sql).await {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                for row in res.rows {
                    let mut iter = row.into_iter();
                    let id = match iter.next() {
                        Some(LibsqlValue::Integer { value }) => value.to_string(),
                        _ => "0".into(),
                    };
                    let _seq = iter.next();
                    let to_table = match iter.next() {
                        Some(LibsqlValue::Text { value }) => value,
                        _ => continue,
                    };
                    let from_col = match iter.next() {
                        Some(LibsqlValue::Text { value }) => value,
                        _ => continue,
                    };
                    let to_col = match iter.next() {
                        Some(LibsqlValue::Text { value }) => value,
                        _ => continue,
                    };

                    fks.push(ForeignKey {
                        name: format!("fk_{}_{}_{id}", table.name, from_col),
                        from_schema: None,
                        from_table: table.name.clone(),
                        from_columns: vec![from_col],
                        to_schema: None,
                        to_table,
                        to_columns: vec![to_col],
                    });
                }
            }
        }

        Ok(fks)
    }

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let sql = format!(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{}'",
            table.name.replace('\'', "''")
        );
        let res = self.run_sql(&sql).await?;
        if let Some(row) = res.rows.into_iter().next() {
            if let Some(val) = row.into_iter().next() {
                if let Value::Text(ddl_sql) = convert_libsql_value(val) {
                    return Ok(Some(ddl_sql));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn libsql_value_conversion() {
        assert_eq!(convert_libsql_value(LibsqlValue::Null), Value::Null);
        assert_eq!(
            convert_libsql_value(LibsqlValue::Text { value: "hello".into() }),
            Value::Text("hello".into())
        );
        assert_eq!(
            convert_libsql_value(LibsqlValue::Integer { value: serde_json::json!(42) }),
            Value::Int(42)
        );
    }
}
