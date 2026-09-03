// SOT: cloudflare-d1-integration, d1-adapter, cloudflare-rest-api, sqlite-over-http

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
struct D1QueryPayload {
    sql: String,
    params: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct D1ApiResponse {
    result: Vec<D1ResultBlock>,
    success: bool,
    errors: Vec<D1ApiError>,
}

#[derive(Debug, Deserialize)]
struct D1ApiError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct D1ResultBlock {
    results: Vec<serde_json::Map<String, serde_json::Value>>,
    #[allow(dead_code)]
    success: bool,
    meta: Option<D1ResultMeta>,
}

#[derive(Debug, Deserialize)]
struct D1ResultMeta {
    changes: Option<u64>,
}

pub struct CloudflareD1Integration {
    client: Client,
    query_url: String,
    api_token: Option<String>,
    database_id: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let account_id = s
        .host
        .as_deref()
        .or(s.username.as_deref())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| AppError::invalid_input("Cloudflare Account ID is required in the Host or Username field."))?;

    let database_id = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| AppError::invalid_input("Cloudflare Database ID is required in the Database field."))?;

    let query_url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    );

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| AppError::driver(e.to_string()))?;

    let integration = CloudflareD1Integration {
        client,
        query_url,
        api_token: conn.secret.clone(),
        database_id: database_id.to_string(),
    };

    integration.ping().await?;
    Ok(Arc::new(integration))
}

impl CloudflareD1Integration {
    async fn run_sql(&self, sql: &str) -> AppResult<D1ResultBlock> {
        let payload = D1QueryPayload {
            sql: sql.to_string(),
            params: Vec::new(),
        };

        let mut req = self.client.post(&self.query_url).json(&payload);
        if let Some(token) = &self.api_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        let resp = req.send().await.map_err(|e| AppError::driver(format!("Cloudflare D1 request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::driver(format!("Cloudflare D1 error ({status}): {body}")));
        }

        let api_resp: D1ApiResponse = resp
            .json()
            .await
            .map_err(|e| AppError::driver(format!("Failed to parse Cloudflare D1 response: {e}")))?;

        if !api_resp.success && !api_resp.errors.is_empty() {
            let errs = api_resp
                .errors
                .into_iter()
                .map(|e| e.message)
                .collect::<Vec<_>>()
                .join("; ");
            return Err(AppError::driver(format!("Cloudflare D1 error: {errs}")));
        }

        api_resp.result.into_iter().next().ok_or_else(|| {
            AppError::driver("Cloudflare D1 returned empty result set.")
        })
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

fn d1_block_to_result_set(block: D1ResultBlock, max_rows: usize) -> ResultSet {
    let mut columns = Vec::new();
    if let Some(first) = block.results.first() {
        for (col_name, sample_val) in first {
            let type_name = match sample_val {
                serde_json::Value::Null => "text",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Number(n) if n.is_i64() => "integer",
                serde_json::Value::Number(_) => "real",
                serde_json::Value::String(_) => "text",
                serde_json::Value::Array(_) | serde_json::Value::Object(_) => "json",
            };
            columns.push(ColumnMeta {
                name: col_name.clone(),
                type_name: type_name.into(),
            });
        }
    }

    let mut rows = Vec::new();
    let truncated = block.results.len() > max_rows;
    for row_map in block.results.into_iter().take(max_rows) {
        let mut row = Vec::new();
        for col in &columns {
            let cell = row_map.get(&col.name).map(json_to_value).unwrap_or(Value::Null);
            row.push(cell);
        }
        rows.push(row);
    }

    ResultSet { columns, rows, truncated }
}

#[async_trait]
impl Integration for CloudflareD1Integration {
    fn engine(&self) -> Engine {
        Engine::CloudflareD1
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
        if let Some(first) = res.results.first() {
            if let Some(val) = first.values().next() {
                if let Some(s) = val.as_str() {
                    return Ok(Some(format!("Cloudflare D1 (SQLite {s})")));
                }
            }
        }
        Ok(Some("Cloudflare D1".into()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database_id.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.database_id.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let sql = "SELECT name, type FROM sqlite_master \
                   WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' \
                   ORDER BY name";
        let res = self.run_sql(sql).await?;
        let mut tables = Vec::new();
        for row in res.results {
            let name = match row.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                _ => continue,
            };
            let kind = match row.get("type").and_then(|v| v.as_str()) {
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

        for row in res.results {
            let name = match row.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                _ => continue,
            };
            let type_name = row
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("TEXT")
                .to_string();
            let notnull = row.get("notnull").and_then(|v| v.as_i64()).unwrap_or(0) != 0;
            let _dflt_value = row.get("dflt_value").and_then(|v| v.as_str());
            let pk = row.get("pk").and_then(|v| v.as_i64()).unwrap_or(0) > 0;

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
        let where_str = where_clause(Engine::CloudflareD1, filters);
        let sql = format!("SELECT COUNT(*) AS count FROM {target}{where_str}");
        let res = self.run_sql(&sql).await?;

        if let Some(first) = res.results.first() {
            if let Some(c) = first.get("count").and_then(|v| v.as_i64()) {
                return Ok(c);
            }
            if let Some(c) = first.values().next().and_then(|v| v.as_i64()) {
                return Ok(c);
            }
        }
        Ok(0)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;

        let target = quote_ident(&table.name);
        let where_str = where_clause(Engine::CloudflareD1, &query.filters);
        let order_str = order_clause(Engine::CloudflareD1, &query.sort);

        let sql = format!(
            "SELECT * FROM {target}{where_str}{order_str} LIMIT {} OFFSET {}",
            query.limit, query.offset
        );
        let res = self.run_sql(&sql).await?;
        Ok(d1_block_to_result_set(res, query.limit as usize))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let stmts = split_statements(sql);
        let mut results = Vec::new();

        for stmt in stmts {
            let res = self.run_sql(&stmt).await?;
            if res.results.is_empty() {
                results.push(StatementResult::Affected {
                    rows_affected: res.meta.and_then(|m| m.changes).unwrap_or(0),
                });
            } else {
                results.push(StatementResult::Rows {
                    result: d1_block_to_result_set(res, max_rows),
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
                for row in res.results {
                    let id = row.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                    let to_table = match row.get("table").and_then(|v| v.as_str()) {
                        Some(t) => t.to_string(),
                        _ => continue,
                    };
                    let from_col = match row.get("from").and_then(|v| v.as_str()) {
                        Some(f) => f.to_string(),
                        _ => continue,
                    };
                    let to_col = match row.get("to").and_then(|v| v.as_str()) {
                        Some(t) => t.to_string(),
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
        if let Some(first) = res.results.first() {
            if let Some(sql_val) = first.get("sql").and_then(|v| v.as_str()) {
                return Ok(Some(sql_val.to_string()));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloudflare_d1_json_conversion() {
        assert_eq!(json_to_value(&serde_json::Value::Null), Value::Null);
        assert_eq!(json_to_value(&serde_json::json!(123)), Value::Int(123));
        assert_eq!(
            json_to_value(&serde_json::json!("cloudflare")),
            Value::Text("cloudflare".into())
        );
    }
}
