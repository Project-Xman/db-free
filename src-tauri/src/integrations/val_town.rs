// SOT: val-town-integration, val-town-adapter, val-town-api, sqlite-over-http

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
struct ValTownPayload {
    statement: String,
    params: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ValTownResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

pub struct ValTownIntegration {
    client: Client,
    endpoint: String,
    api_token: Option<String>,
    database_name: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let endpoint = s
        .host
        .as_deref()
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(|h| {
            if h.starts_with("http://") || h.starts_with("https://") {
                if h.ends_with("/v1/sqlite/execute") {
                    h.to_string()
                } else {
                    format!("{}/v1/sqlite/execute", h.trim_end_matches('/'))
                }
            } else {
                format!("https://{}/v1/sqlite/execute", h.trim_end_matches('/'))
            }
        })
        .unwrap_or_else(|| "https://api.val.town/v1/sqlite/execute".to_string());

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| AppError::driver(e.to_string()))?;

    let database_name = s.database.clone().unwrap_or_else(|| "val_town".into());

    let integration = ValTownIntegration {
        client,
        endpoint,
        api_token: conn.secret.clone(),
        database_name,
    };

    integration.ping().await?;
    Ok(Arc::new(integration))
}

impl ValTownIntegration {
    async fn run_sql(&self, sql: &str) -> AppResult<ValTownResponse> {
        let payload = ValTownPayload {
            statement: sql.to_string(),
            params: Vec::new(),
        };

        let mut req = self.client.post(&self.endpoint).json(&payload);
        if let Some(token) = &self.api_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        let resp = req.send().await.map_err(|e| AppError::driver(format!("Val Town request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::driver(format!("Val Town error ({status}): {body}")));
        }

        let val_resp: ValTownResponse = resp
            .json()
            .await
            .map_err(|e| AppError::driver(format!("Failed to parse Val Town response: {e}")))?;

        Ok(val_resp)
    }
}

fn json_to_value(val: &serde_json::Value) -> Value {
    match val {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(val.clone()),
    }
}

fn val_town_to_result_set(resp: ValTownResponse, max_rows: usize) -> ResultSet {
    let columns = resp
        .columns
        .into_iter()
        .map(|name| ColumnMeta {
            name,
            type_name: "text".into(),
        })
        .collect();

    let total = resp.rows.len();
    let truncated = total > max_rows;
    let rows = resp
        .rows
        .into_iter()
        .take(max_rows)
        .map(|r| r.iter().map(json_to_value).collect())
        .collect();

    ResultSet {
        columns,
        rows,
        truncated,
    }
}

#[async_trait]
impl Integration for ValTownIntegration {
    fn engine(&self) -> Engine {
        Engine::ValTown
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: true,
            namespaces: false,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: true,
            transactions: false,
            exact_estimate: true,
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
                if let Some(s) = val.as_str() {
                    return Ok(Some(format!("Val Town (SQLite {s})")));
                }
            }
        }
        Ok(Some("Val Town".into()))
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
            let name = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                Some(n) => n,
                _ => continue,
            };
            let kind = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                Some(k) if k.eq_ignore_ascii_case("view") => TableKind::View,
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
            let name = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                Some(n) => n,
                _ => continue,
            };
            let type_name = iter
                .next()
                .and_then(|v| v.as_str().map(ToString::to_string))
                .unwrap_or_else(|| "TEXT".into());
            let notnull = iter.next().and_then(|v| v.as_i64()).unwrap_or(0) != 0;
            let _dflt_value = iter.next().and_then(|v| v.as_str().map(ToString::to_string));
            let pk = iter.next().and_then(|v| v.as_i64()).unwrap_or(0) > 0;

            cols.push(ColumnInfo {
                name,
                data_type: type_name,
                nullable: !notnull,
                primary_key: pk,
                ordinal: cols.len() as u32,
            });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let target = quote_ident(&table.name);
        let where_str = where_clause(Engine::ValTown, filters);
        let sql = format!("SELECT COUNT(*) FROM {target}{where_str}");
        let res = self.run_sql(&sql).await?;

        if let Some(row) = res.rows.into_iter().next() {
            if let Some(val) = row.into_iter().next() {
                if let Some(c) = val.as_i64() {
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
        let where_str = where_clause(Engine::ValTown, &query.filters);
        let order_str = order_clause(Engine::ValTown, &query.sort);

        let sql = format!(
            "SELECT * FROM {target}{where_str}{order_str} LIMIT {} OFFSET {}",
            query.limit, query.offset
        );
        let res = self.run_sql(&sql).await?;
        Ok(val_town_to_result_set(res, query.limit as usize))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let stmts = split_statements(sql);
        let mut results = Vec::new();

        for stmt in stmts {
            let res = self.run_sql(&stmt).await?;
            if res.columns.is_empty() {
                results.push(StatementResult::Affected { rows_affected: 0 });
            } else {
                results.push(StatementResult::Rows { result: val_town_to_result_set(res, max_rows) });
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
                    let id = match iter.next().and_then(|v| v.as_i64()) {
                        Some(i) => i.to_string(),
                        _ => "0".into(),
                    };
                    let _seq = iter.next();
                    let to_table = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                        Some(t) => t,
                        _ => continue,
                    };
                    let from_col = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                        Some(f) => f,
                        _ => continue,
                    };
                    let to_col = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                        Some(t) => t,
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
                if let Some(s) = val.as_str() {
                    return Ok(Some(s.to_string()));
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
    fn val_town_value_conversion() {
        assert_eq!(json_to_value(&serde_json::Value::Null), Value::Null);
        assert_eq!(json_to_value(&serde_json::json!(42)), Value::Int(42));
        assert_eq!(
            json_to_value(&serde_json::json!("val_town")),
            Value::Text("val_town".into())
        );
    }
}
