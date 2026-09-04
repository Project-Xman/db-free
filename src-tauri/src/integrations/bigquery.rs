// SOT: bigquery-integration, bigquery-rest-api, google-service-account-jwt, bigquery-row-decoding

use crate::error::{AppError, AppResult};
use crate::integrations::gcp_auth::GcpAuth;
use crate::integrations::http::{Auth, HttpClient};
use crate::integrations::sql::{order_clause, validate_columns, where_clause};
use crate::integrations::{qualified_name_for, quote_ident_for, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================================
// WHAT:  Google BigQuery adapter over the REST API v2. `database` = project id
//        (or the service account's), `username` = optional dataset filter.
// WHY:   `jobs.query` covers everything: it runs SQL synchronously up to a
//        timeout and hands back a jobId to poll otherwise.
// HOW:   Datasets are schemas; tables/views come from `tables.list`. Rows are
//        decoded from the `f[].v` shape by the schema's field types (nested
//        RECORD / REPEATED → JSON). Identifiers are backtick-quoted as
//        `project.dataset.table`. DML returns numDmlAffectedRows.
// WHERE: src-tauri/src/integrations/gcp_auth.rs, src-tauri/src/integrations/sql.rs
// ============================================================================

const SCOPE: &str = "https://www.googleapis.com/auth/bigquery";
const QUERY_TIMEOUT_MS: u64 = 30_000;
const POLL_BUDGET: Duration = Duration::from_secs(60);

pub struct BigQueryIntegration {
    engine: Engine,
    http: HttpClient,
    auth: GcpAuth,
    project: String,
    dataset: Option<String>,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let auth = GcpAuth::from_connection(conn, SCOPE)?;
    if auth.is_anonymous() {
        return Err(AppError::invalid_input("BigQuery needs a service-account JSON key or an access token."));
    }
    let project = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .or_else(|| auth.project_hint.clone())
        .ok_or_else(|| AppError::invalid_input("BigQuery needs a project id (database field) or a service-account key that names one."))?;
    let dataset = s.username.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let base = s.host.as_deref().map(str::trim).filter(|h| h.starts_with("http")).unwrap_or("https://bigquery.googleapis.com").to_string();
    let http = HttpClient::new(base, Auth::None, false)?;
    let integration = BigQueryIntegration { engine: s.engine, http, auth, project, dataset };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Row decoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub kind: String,
    pub repeated: bool,
    pub fields: Vec<Field>,
}

pub fn parse_fields(schema: &serde_json::Value) -> Vec<Field> {
    schema
        .get("fields")
        .and_then(|f| f.as_array())
        .into_iter()
        .flatten()
        .map(|f| Field {
            name: f.get("name").and_then(|n| n.as_str()).unwrap_or_default().to_string(),
            kind: f.get("type").and_then(|t| t.as_str()).unwrap_or("STRING").to_uppercase(),
            repeated: f.get("mode").and_then(|m| m.as_str()) == Some("REPEATED"),
            fields: parse_fields(f),
        })
        .collect()
}

fn timestamp_text(raw: &str) -> String {
    raw.parse::<f64>()
        .ok()
        .and_then(|secs| chrono::DateTime::from_timestamp_micros((secs * 1_000_000.0).round() as i64))
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        .unwrap_or_else(|| raw.to_string())
}

// WHAT:  One cell (`{"v": …}` unwrapped) as plain JSON, following the field type.
fn cell_json(field: &Field, v: &serde_json::Value) -> serde_json::Value {
    if field.repeated {
        let items = v.as_array().map(|a| a.iter().map(|i| i.get("v").unwrap_or(i)).map(|i| scalar_json(field, i)).collect()).unwrap_or_default();
        return serde_json::Value::Array(items);
    }
    scalar_json(field, v)
}

fn scalar_json(field: &Field, v: &serde_json::Value) -> serde_json::Value {
    if v.is_null() {
        return serde_json::Value::Null;
    }
    match field.kind.as_str() {
        "RECORD" | "STRUCT" => {
            let cells = v.get("f").and_then(|f| f.as_array()).cloned().unwrap_or_default();
            serde_json::Value::Object(field.fields.iter().zip(cells.iter()).map(|(sub, c)| (sub.name.clone(), cell_json(sub, c.get("v").unwrap_or(c)))).collect())
        }
        "INTEGER" | "INT64" => v.as_str().and_then(|s| s.parse::<i64>().ok()).map(|i| serde_json::Value::Number(i.into())).unwrap_or(v.clone()),
        "FLOAT" | "FLOAT64" => v.as_str().and_then(|s| s.parse::<f64>().ok()).and_then(serde_json::Number::from_f64).map(serde_json::Value::Number).unwrap_or(v.clone()),
        "BOOLEAN" | "BOOL" => serde_json::Value::Bool(v.as_str().map(|s| s == "true").or_else(|| v.as_bool()).unwrap_or(false)),
        "TIMESTAMP" => serde_json::Value::String(v.as_str().map(timestamp_text).unwrap_or_default()),
        "JSON" => v.as_str().and_then(|s| serde_json::from_str(s).ok()).unwrap_or(v.clone()),
        _ => v.clone(),
    }
}

pub fn cell_value(field: &Field, v: &serde_json::Value) -> Value {
    if field.repeated || matches!(field.kind.as_str(), "RECORD" | "STRUCT" | "JSON") {
        let j = cell_json(field, v);
        return if j.is_null() { Value::Null } else { Value::Json(j) };
    }
    let Some(s) = v.as_str() else {
        return match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            other => Value::Text(other.to_string()),
        };
    };
    match field.kind.as_str() {
        "INTEGER" | "INT64" => s.parse().map(Value::Int).unwrap_or_else(|_| Value::Decimal(s.to_string())),
        "FLOAT" | "FLOAT64" => s.parse().map(Value::Float).unwrap_or_else(|_| Value::Text(s.to_string())),
        "NUMERIC" | "BIGNUMERIC" | "DECIMAL" | "BIGDECIMAL" => Value::Decimal(s.to_string()),
        "BOOLEAN" | "BOOL" => Value::Bool(s == "true"),
        "BYTES" => Value::Bytes(s.to_string()),
        "TIMESTAMP" => Value::DateTime(timestamp_text(s)),
        "DATE" | "DATETIME" | "TIME" => Value::DateTime(s.to_string()),
        _ => Value::Text(s.to_string()),
    }
}

pub fn type_name(field: &Field) -> String {
    let base = match field.kind.as_str() {
        "INTEGER" => "INT64".to_string(),
        "FLOAT" => "FLOAT64".to_string(),
        "BOOLEAN" => "BOOL".to_string(),
        "RECORD" => "STRUCT".to_string(),
        other => other.to_string(),
    };
    if field.repeated { format!("ARRAY<{base}>") } else { base }
}

pub fn rows_to_result(schema: &serde_json::Value, rows: &[serde_json::Value], max_rows: usize) -> ResultSet {
    let fields = parse_fields(schema);
    let truncated = rows.len() > max_rows;
    let rows = rows
        .iter()
        .take(max_rows)
        .map(|r| {
            let cells = r.get("f").and_then(|f| f.as_array()).cloned().unwrap_or_default();
            fields.iter().enumerate().map(|(i, f)| cells.get(i).map(|c| cell_value(f, c.get("v").unwrap_or(c))).unwrap_or(Value::Null)).collect()
        })
        .collect();
    let columns = fields.iter().map(|f| ColumnMeta { name: f.name.clone(), type_name: type_name(f) }).collect();
    ResultSet { columns, rows, truncated }
}

// WHAT:  Flattens RECORD fields to dotted names for the column list; the grid
//        still receives the whole struct as JSON in the parent column.
fn schema_columns(fields: &[Field]) -> Vec<ColumnInfo> {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| ColumnInfo { name: f.name.clone(), data_type: type_name(f), nullable: true, primary_key: false, ordinal: i as u32 + 1 })
        .collect()
}

#[derive(Debug)]
struct QueryOutcome {
    schema: serde_json::Value,
    rows: Vec<serde_json::Value>,
    affected: Option<u64>,
    has_schema: bool,
}

impl BigQueryIntegration {
    fn api(&self, path: &str) -> String {
        format!("/bigquery/v2/projects/{}{path}", self.project)
    }

    async fn req(&self, method: Method, path: &str, body: Option<serde_json::Value>) -> AppResult<serde_json::Value> {
        let mut r = self.http.request(method, path);
        if let Auth::Bearer(t) = self.auth.bearer().await? {
            r = r.bearer_auth(t);
        }
        if let Some(b) = body {
            r = r.json(&b);
        }
        let resp = self.http.send(r).await?;
        resp.json().await.map_err(|e| AppError::driver(format!("Malformed BigQuery response: {e}")))
    }

    async fn query(&self, sql: &str, max_results: usize) -> AppResult<QueryOutcome> {
        let body = serde_json::json!({"query": sql, "useLegacySql": false, "maxResults": max_results.clamp(1, 10_000), "timeoutMs": QUERY_TIMEOUT_MS});
        let mut resp = self.req(Method::POST, &self.api("/queries"), Some(body)).await?;
        let started = Instant::now();
        while resp.get("jobComplete").and_then(|c| c.as_bool()) == Some(false) {
            if started.elapsed() > POLL_BUDGET {
                return Err(AppError::timeout("BigQuery job did not complete within 60 s."));
            }
            let job_id = resp.pointer("/jobReference/jobId").and_then(|j| j.as_str()).ok_or_else(|| AppError::driver("BigQuery returned no jobId for an incomplete job."))?.to_string();
            let location = resp.pointer("/jobReference/location").and_then(|l| l.as_str()).unwrap_or_default().to_string();
            tokio::time::sleep(Duration::from_millis(1000)).await;
            resp = self.req(Method::GET, &format!("{}?location={location}&maxResults={}&timeoutMs={QUERY_TIMEOUT_MS}", self.api(&format!("/queries/{job_id}")), max_results.clamp(1, 10_000)), None).await?;
        }
        if let Some(errs) = resp.get("errors").and_then(|e| e.as_array()).filter(|e| !e.is_empty()) {
            let msg = errs.iter().filter_map(|e| e.get("message").and_then(|m| m.as_str())).collect::<Vec<_>>().join("; ");
            return Err(AppError::driver(msg));
        }
        let mut rows: Vec<serde_json::Value> = resp.get("rows").and_then(|r| r.as_array()).cloned().unwrap_or_default();
        let mut token = resp.get("pageToken").and_then(|t| t.as_str()).map(str::to_string);
        while let Some(t) = token.take() {
            if rows.len() >= max_results {
                break;
            }
            let job_id = resp.pointer("/jobReference/jobId").and_then(|j| j.as_str()).unwrap_or_default().to_string();
            let location = resp.pointer("/jobReference/location").and_then(|l| l.as_str()).unwrap_or_default().to_string();
            let page = self.req(Method::GET, &format!("{}?location={location}&pageToken={t}&maxResults={}", self.api(&format!("/queries/{job_id}")), max_results.clamp(1, 10_000)), None).await?;
            rows.extend(page.get("rows").and_then(|r| r.as_array()).cloned().unwrap_or_default());
            token = page.get("pageToken").and_then(|t| t.as_str()).map(str::to_string);
        }
        Ok(QueryOutcome {
            has_schema: resp.get("schema").is_some(),
            schema: resp.get("schema").cloned().unwrap_or(serde_json::json!({})),
            rows,
            affected: resp.get("numDmlAffectedRows").and_then(|n| n.as_str()).and_then(|s| s.parse().ok()),
        })
    }

    fn table_ref(&self, table: &TableRef) -> String {
        let dataset = table.schema.clone().or_else(|| self.dataset.clone()).unwrap_or_default();
        format!("{}.{}", quote_ident_for(Engine::Bigquery, &self.project), qualified_name_for(Engine::Bigquery, &TableRef { schema: Some(dataset), name: table.name.clone() }))
    }

    async fn table_meta(&self, table: &TableRef) -> AppResult<serde_json::Value> {
        let dataset = table.schema.clone().or_else(|| self.dataset.clone()).ok_or_else(|| AppError::invalid_input("A dataset is required."))?;
        self.req(Method::GET, &self.api(&format!("/datasets/{dataset}/tables/{}", table.name)), None).await
    }
}

#[async_trait]
impl Integration for BigQueryIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { transactions: false, exact_estimate: false, ..Capabilities::SQL }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.req(Method::GET, &self.api("/datasets?maxResults=1"), None).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some("BigQuery".into()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.project.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.project.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let datasets: Vec<String> = match &self.dataset {
            Some(d) => vec![d.clone()],
            None => {
                let mut out = Vec::new();
                let mut token: Option<String> = None;
                loop {
                    let path = match &token {
                        Some(t) => self.api(&format!("/datasets?maxResults=200&pageToken={t}")),
                        None => self.api("/datasets?maxResults=200"),
                    };
                    let resp = self.req(Method::GET, &path, None).await?;
                    for d in resp.get("datasets").and_then(|d| d.as_array()).into_iter().flatten() {
                        if let Some(id) = d.pointer("/datasetReference/datasetId").and_then(|i| i.as_str()) {
                            out.push(id.to_string());
                        }
                    }
                    match resp.get("nextPageToken").and_then(|t| t.as_str()) {
                        Some(t) if out.len() < 2_000 => token = Some(t.to_string()),
                        _ => break,
                    }
                }
                out
            }
        };
        let mut schemas = Vec::new();
        for ds in datasets {
            let mut tables = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let path = match &token {
                    Some(t) => self.api(&format!("/datasets/{ds}/tables?maxResults=500&pageToken={t}")),
                    None => self.api(&format!("/datasets/{ds}/tables?maxResults=500")),
                };
                let resp = match self.req(Method::GET, &path, None).await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                for t in resp.get("tables").and_then(|t| t.as_array()).into_iter().flatten() {
                    let Some(name) = t.pointer("/tableReference/tableId").and_then(|i| i.as_str()) else { continue };
                    let kind = match t.get("type").and_then(|k| k.as_str()).unwrap_or("TABLE") {
                        "VIEW" | "MATERIALIZED_VIEW" => TableKind::View,
                        _ => TableKind::Table,
                    };
                    tables.push(TableInfo { schema: Some(ds.clone()), name: name.to_string(), kind, row_estimate: None });
                }
                match resp.get("nextPageToken").and_then(|t| t.as_str()) {
                    Some(t) if tables.len() < 5_000 => token = Some(t.to_string()),
                    _ => break,
                }
            }
            schemas.push(SchemaInfo { name: ds, tables });
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let meta = self.table_meta(table).await?;
        let fields = parse_fields(meta.get("schema").unwrap_or(&serde_json::json!({})));
        Ok(schema_columns(&fields))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let meta = self.table_meta(table).await?;
        Ok(meta.get("numRows").and_then(|n| n.as_str()).and_then(|s| s.parse().ok()))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT COUNT(*) AS n FROM {}{}", self.table_ref(table), where_clause(Engine::Bigquery, filters));
        let out = self.query(&sql, 1).await?;
        let rs = rows_to_result(&out.schema, &out.rows, 1);
        Ok(match rs.rows.first().and_then(|r| r.first()) {
            Some(Value::Int(i)) => *i,
            _ => 0,
        })
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            self.table_ref(table),
            where_clause(Engine::Bigquery, &query.filters),
            order_clause(Engine::Bigquery, &query.sort),
            query.limit,
            query.offset
        );
        let out = self.query(&sql, query.limit as usize).await?;
        Ok(rows_to_result(&out.schema, &out.rows, query.limit as usize))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut results = Vec::new();
        for stmt in crate::guard::destructive::split_statements(sql) {
            let out = self.query(&stmt, max_rows).await?;
            if let Some(n) = out.affected {
                results.push(StatementResult::Affected { rows_affected: n });
            } else if out.has_schema {
                results.push(StatementResult::Rows { result: rows_to_result(&out.schema, &out.rows, max_rows) });
            } else {
                results.push(StatementResult::Affected { rows_affected: 0 });
            }
        }
        Ok(results)
    }

    async fn close(&self) {}

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let dataset = table.schema.clone().or_else(|| self.dataset.clone()).ok_or_else(|| AppError::invalid_input("A dataset is required."))?;
        let sql = format!(
            "SELECT ddl FROM {}.{}.INFORMATION_SCHEMA.TABLES WHERE table_name = '{}'",
            quote_ident_for(Engine::Bigquery, &self.project),
            quote_ident_for(Engine::Bigquery, &dataset),
            table.name.replace('\'', "\\'")
        );
        let out = self.query(&sql, 1).await?;
        let rs = rows_to_result(&out.schema, &out.rows, 1);
        Ok(match rs.rows.first().and_then(|r| r.first()) {
            Some(Value::Text(t)) => Some(t.clone()),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> serde_json::Value {
        serde_json::json!({"fields": [
            {"name": "id", "type": "INTEGER"},
            {"name": "price", "type": "NUMERIC"},
            {"name": "ok", "type": "BOOLEAN"},
            {"name": "ts", "type": "TIMESTAMP"},
            {"name": "tags", "type": "STRING", "mode": "REPEATED"},
            {"name": "addr", "type": "RECORD", "fields": [{"name": "city", "type": "STRING"}, {"name": "zip", "type": "INTEGER"}]},
            {"name": "j", "type": "JSON"},
            {"name": "ratio", "type": "FLOAT"},
            {"name": "raw", "type": "BYTES"}
        ]})
    }

    #[test]
    fn decodes_rows_by_type() {
        let rows = vec![serde_json::json!({"f": [
            {"v": "7"}, {"v": "12.50"}, {"v": "true"}, {"v": "1.7040672E9"},
            {"v": [{"v": "a"}, {"v": "b"}]},
            {"v": {"f": [{"v": "Paris"}, {"v": "75001"}]}},
            {"v": "{\"k\": [1]}"}, {"v": "0.25"}, {"v": "AQID"}
        ]})];
        let rs = rows_to_result(&schema(), &rows, 10);
        assert_eq!(rs.columns.iter().map(|c| c.type_name.as_str()).collect::<Vec<_>>(), vec!["INT64", "NUMERIC", "BOOL", "TIMESTAMP", "ARRAY<STRING>", "STRUCT", "JSON", "FLOAT64", "BYTES"]);
        assert_eq!(rs.rows[0][0], Value::Int(7));
        assert_eq!(rs.rows[0][1], Value::Decimal("12.50".into()));
        assert_eq!(rs.rows[0][2], Value::Bool(true));
        assert_eq!(rs.rows[0][3], Value::DateTime("2024-01-01T00:00:00Z".into()));
        assert_eq!(rs.rows[0][4], Value::Json(serde_json::json!(["a", "b"])));
        assert_eq!(rs.rows[0][5], Value::Json(serde_json::json!({"city": "Paris", "zip": 75001})));
        assert_eq!(rs.rows[0][6], Value::Json(serde_json::json!({"k": [1]})));
        assert_eq!(rs.rows[0][7], Value::Float(0.25));
        assert_eq!(rs.rows[0][8], Value::Bytes("AQID".into()));
        let nulls = vec![serde_json::json!({"f": [{"v": null}, {"v": null}, {"v": null}, {"v": null}, {"v": []}, {"v": null}, {"v": null}, {"v": null}, {"v": null}]})];
        let rs = rows_to_result(&schema(), &nulls, 10);
        assert_eq!(rs.rows[0][0], Value::Null);
        assert_eq!(rs.rows[0][4], Value::Json(serde_json::json!([])));
        assert_eq!(rs.rows[0][5], Value::Null);
    }

    #[test]
    fn truncates() {
        let rows = vec![serde_json::json!({"f": [{"v": "1"}]}), serde_json::json!({"f": [{"v": "2"}]})];
        let rs = rows_to_result(&serde_json::json!({"fields": [{"name": "a", "type": "INT64"}]}), &rows, 1);
        assert!(rs.truncated);
        assert_eq!(rs.rows.len(), 1);
    }

    #[test]
    fn timestamps_render_rfc3339() {
        assert_eq!(timestamp_text("1.7040672E9"), "2024-01-01T00:00:00Z");
        assert_eq!(timestamp_text("1704067200.5"), "2024-01-01T00:00:00.500Z");
        assert_eq!(timestamp_text("garbage"), "garbage");
    }
}
