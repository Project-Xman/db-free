// SOT: clickhouse-integration, clickhouse-adapter, clickhouse-http, jsoncompact-decoding

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, where_clause};
use crate::integrations::{qualified_name_for, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SslMode, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

// WHAT:  ClickHouse adapter over its HTTP interface (port 8123 / 8443).
// WHY:   No native driver crate is needed: every statement is a POST whose
//        result comes back as JSONCompact, so one decoder covers SELECT, SHOW,
//        EXPLAIN and DDL alike. `reqwest` is imported only here and in the AI
//        service (scripts/guardrail.py enforces the boundary).
// HOW:   Credentials travel in X-ClickHouse-User / X-ClickHouse-Key headers,
//        the session database in the `database` query parameter, and catalog
//        lookups use server-side query parameters (`{db:String}`) so identifiers
//        never get spliced into SQL text.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI entry)

impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            AppError::timeout("ClickHouse did not answer in time.")
        } else if err.is_connect() {
            AppError::driver(format!("Could not reach ClickHouse: {err}"))
        } else {
            AppError::driver(err)
        }
    }
}

const DEFAULT_DATABASE: &str = "default";
const DEFAULT_USER: &str = "default";
const DEFAULT_PORT: u16 = 8123;

pub struct ClickhouseIntegration {
    client: Client,
    base_url: String,
    user: String,
    password: Option<String>,
    database: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let scheme = match s.ssl_mode {
        SslMode::Disable | SslMode::Prefer => "http",
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => "https",
    };
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("localhost");
    let port = s.port.unwrap_or(DEFAULT_PORT);
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let user = s
        .username
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .unwrap_or(DEFAULT_USER)
        .to_string();
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(300))
        .build()?;
    let integration = ClickhouseIntegration {
        client,
        base_url: format!("{scheme}://{host}:{port}/"),
        user,
        password: conn.secret.clone(),
        database,
    };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// JSONCompact decoding (pure functions, unit-tested below)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CompactMeta {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

#[derive(Debug, Deserialize)]
struct CompactBody {
    meta: Vec<CompactMeta>,
    #[serde(default)]
    data: Vec<Vec<serde_json::Value>>,
}

/// Peels `Nullable(...)` / `LowCardinality(...)` wrappers so the inner type drives decoding.
fn base_type(type_name: &str) -> &str {
    let mut current = type_name.trim();
    loop {
        let stripped = current
            .strip_prefix("Nullable(")
            .or_else(|| current.strip_prefix("LowCardinality("))
            .and_then(|inner| inner.strip_suffix(')'));
        match stripped {
            Some(inner) => current = inner.trim(),
            None => return current,
        }
    }
}

fn is_integer_type(base: &str) -> bool {
    base.starts_with("Int") || base.starts_with("UInt")
}

fn is_float_type(base: &str) -> bool {
    base.starts_with("Float") || base == "BFloat16"
}

fn is_temporal_type(base: &str) -> bool {
    base.starts_with("Date") || base.starts_with("Time")
}

fn decode_cell(cell: &serde_json::Value, type_name: &str) -> Value {
    let base = base_type(type_name);
    match cell {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if base.starts_with("Decimal") {
                // Exact numerics stay textual; never round them through f64.
                Value::Decimal(n.to_string())
            } else if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                if is_integer_type(base) {
                    // u64 above i64::MAX: keep every digit instead of rounding through f64.
                    Value::Decimal(n.to_string())
                } else {
                    Value::Float(f)
                }
            } else {
                Value::Decimal(n.to_string())
            }
        }
        serde_json::Value::String(text) => {
            if is_temporal_type(base) {
                Value::DateTime(text.clone())
            } else if base.starts_with("Decimal") {
                Value::Decimal(text.clone())
            } else if is_integer_type(base) {
                text.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Decimal(text.clone()))
            } else if is_float_type(base) {
                // Quoted denormals ("nan", "inf", "-inf") arrive as strings.
                text.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::Text(text.clone()))
            } else if base == "Bool" {
                Value::Bool(text == "true" || text == "1")
            } else {
                Value::Text(text.clone())
            }
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(cell.clone()),
    }
}

/// Decodes a JSONCompact body into a result set, capping rows at `max_rows`.
fn decode_compact(body: &str, max_rows: usize) -> AppResult<ResultSet> {
    let parsed: CompactBody = serde_json::from_str(body).map_err(|e| AppError::driver(format!("ClickHouse returned unreadable JSON: {e}")))?;
    let columns: Vec<ColumnMeta> = parsed
        .meta
        .iter()
        .map(|m| ColumnMeta { name: m.name.clone(), type_name: m.type_name.clone() })
        .collect();
    let truncated = parsed.data.len() > max_rows;
    let rows: Vec<Vec<Value>> = parsed
        .data
        .iter()
        .take(max_rows)
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(i, col)| row.get(i).map(|cell| decode_cell(cell, &col.type_name)).unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    Ok(ResultSet { columns, rows, truncated })
}

/// Decodes any response body: JSONCompact when possible, otherwise one text row
/// per line (a statement that carries its own `FORMAT` clause, for example).
fn decode_body(body: &str, max_rows: usize) -> ResultSet {
    match decode_compact(body, max_rows) {
        Ok(set) => set,
        Err(_) => {
            let lines: Vec<&str> = body.lines().collect();
            let truncated = lines.len() > max_rows;
            ResultSet {
                columns: vec![ColumnMeta { name: "output".to_string(), type_name: "String".to_string() }],
                rows: lines.into_iter().take(max_rows).map(|l| vec![Value::Text(l.to_string())]).collect(),
                truncated,
            }
        }
    }
}

/// `written_rows` from the X-ClickHouse-Summary header (a JSON object of stringified numbers).
fn parse_written_rows(summary: &str) -> Option<u64> {
    let parsed: serde_json::Value = serde_json::from_str(summary).ok()?;
    match parsed.get("written_rows")? {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn first_cell(set: &ResultSet) -> Option<&Value> {
    set.rows.first().and_then(|r| r.first())
}

fn cell_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Int(i)) => Some(*i),
        Some(Value::Decimal(d)) | Some(Value::Text(d)) => d.parse().ok(),
        Some(Value::Float(f)) => Some(*f as i64),
        _ => None,
    }
}

fn cell_text(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Text(t)) | Some(Value::Decimal(t)) | Some(Value::DateTime(t)) => Some(t.clone()),
        Some(Value::Int(i)) => Some(i.to_string()),
        Some(Value::Float(f)) => Some(f.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(Value::Json(j)) => Some(j.to_string()),
        _ => None,
    }
}

struct RawResponse {
    body: String,
    written_rows: Option<u64>,
}

impl ClickhouseIntegration {
    // WHAT:  One HTTP round trip. `params` become `param_<name>` server-side
    //        query parameters referenced as `{name:Type}` inside the SQL.
    async fn post(&self, sql: &str, params: &[(&str, &str)]) -> AppResult<RawResponse> {
        let mut query: Vec<(String, String)> = vec![
            ("database".to_string(), self.database.clone()),
            ("default_format".to_string(), "JSONCompact".to_string()),
            ("output_format_json_quote_64bit_integers".to_string(), "0".to_string()),
            ("output_format_json_quote_denormals".to_string(), "1".to_string()),
            // Decimals as strings keep their declared scale ("12.50", not 12.5).
            ("output_format_json_quote_decimals".to_string(), "1".to_string()),
        ];
        for (name, value) in params {
            query.push((format!("param_{name}"), (*value).to_string()));
        }
        let mut request = self
            .client
            .post(&self.base_url)
            .query(&query)
            .header("X-ClickHouse-User", &self.user)
            .header("Content-Type", "text/plain; charset=utf-8");
        if let Some(password) = &self.password {
            request = request.header("X-ClickHouse-Key", password);
        }
        let response = request.body(sql.to_string()).send().await?;
        let status = response.status();
        let written_rows = response
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_written_rows);
        let body = response.text().await?;
        if !status.is_success() {
            let message = body.trim();
            return Err(AppError::driver(if message.is_empty() { format!("ClickHouse returned HTTP {status}") } else { message.to_string() }));
        }
        Ok(RawResponse { body, written_rows })
    }

    async fn query(&self, sql: &str, params: &[(&str, &str)], max_rows: usize) -> AppResult<ResultSet> {
        let raw = self.post(sql, params).await?;
        decode_compact(&raw.body, max_rows)
    }

    async fn run_statement(&self, sql: &str, max_rows: usize) -> AppResult<StatementResult> {
        let raw = self.post(sql, &[]).await?;
        if raw.body.trim().is_empty() {
            return Ok(StatementResult::Affected { rows_affected: raw.written_rows.unwrap_or(0) });
        }
        Ok(StatementResult::Rows { result: decode_body(&raw.body, max_rows) })
    }

    fn qualified(&self, table: &TableRef) -> String {
        let with_schema = TableRef {
            schema: Some(table.schema.clone().unwrap_or_else(|| self.database.clone())),
            name: table.name.clone(),
        };
        qualified_name_for(Engine::Clickhouse, &with_schema)
    }

    fn schema_of<'a>(&'a self, table: &'a TableRef) -> &'a str {
        table.schema.as_deref().unwrap_or(self.database.as_str())
    }
}

#[async_trait]
impl Integration for ClickhouseIntegration {
    fn engine(&self) -> Engine {
        Engine::Clickhouse
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: true, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: true }
    }

    async fn ping(&self) -> AppResult<()> {
        self.post("SELECT 1", &[]).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let set = self.query("SELECT version()", &[], 1).await?;
        Ok(cell_text(first_cell(&set)).map(|v| format!("ClickHouse {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let set = self
            .query(
                "SELECT name FROM system.databases \
                 WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY name",
                &[],
                10_000,
            )
            .await?;
        Ok(set.rows.iter().filter_map(|r| cell_text(r.first())).collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let set = self
            .query(
                "SELECT name, engine, total_rows FROM system.tables WHERE database = {db:String} ORDER BY name",
                &[("db", self.database.as_str())],
                100_000,
            )
            .await?;
        let tables: Vec<TableInfo> = set
            .rows
            .iter()
            .filter_map(|row| {
                let name = cell_text(row.first())?;
                let engine = cell_text(row.get(1)).unwrap_or_default();
                let kind = if engine.contains("View") { TableKind::View } else { TableKind::Table };
                Some(TableInfo { schema: Some(self.database.clone()), name, kind, row_estimate: cell_i64(row.get(2)) })
            })
            .collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let set = self
            .query(
                "SELECT name, type, position, is_in_primary_key FROM system.columns \
                 WHERE database = {db:String} AND table = {t:String} ORDER BY position",
                &[("db", self.schema_of(table)), ("t", table.name.as_str())],
                10_000,
            )
            .await?;
        Ok(set
            .rows
            .iter()
            .filter_map(|row| {
                let name = cell_text(row.first())?;
                let data_type = cell_text(row.get(1)).unwrap_or_default();
                Some(ColumnInfo {
                    nullable: data_type.starts_with("Nullable("),
                    primary_key: cell_i64(row.get(3)).unwrap_or(0) != 0,
                    ordinal: u32::try_from(cell_i64(row.get(2)).unwrap_or(0)).unwrap_or_default(),
                    name,
                    data_type,
                })
            })
            .collect())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let set = self
            .query(
                "SELECT total_rows FROM system.tables WHERE database = {db:String} AND name = {t:String}",
                &[("db", self.schema_of(table)), ("t", table.name.as_str())],
                1,
            )
            .await?;
        Ok(cell_i64(first_cell(&set)))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count() FROM {}{}", self.qualified(table), where_clause(Engine::Clickhouse, filters));
        let set = self.query(&sql, &[], 1).await?;
        cell_i64(first_cell(&set)).ok_or_else(|| AppError::driver("count() returned no value"))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            self.qualified(table),
            where_clause(Engine::Clickhouse, &query.filters),
            order_clause(Engine::Clickhouse, &query.sort),
            query.limit,
            query.offset
        );
        self.query(&sql, &[], query.limit as usize).await
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut out = Vec::new();
        for statement in split_statements(sql) {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }
            out.push(self.run_statement(trimmed, max_rows).await?);
        }
        Ok(out)
    }

    async fn close(&self) {}

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let set = self.query(&format!("SHOW CREATE TABLE {}", self.qualified(table)), &[], 1).await?;
        Ok(cell_text(first_cell(&set)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, FilterOp, SortRule};

    #[test]
    fn base_type_unwraps_wrappers() {
        assert_eq!(base_type("Nullable(UInt64)"), "UInt64");
        assert_eq!(base_type("LowCardinality(Nullable(String))"), "String");
        assert_eq!(base_type("DateTime('UTC')"), "DateTime('UTC')");
        assert_eq!(base_type("Decimal(10, 2)"), "Decimal(10, 2)");
    }

    #[test]
    fn jsoncompact_decodes_every_kind() {
        let body = r#"{
            "meta": [
                {"name":"id","type":"UInt64"},
                {"name":"name","type":"String"},
                {"name":"maybe","type":"Nullable(String)"},
                {"name":"ts","type":"DateTime"},
                {"name":"n","type":"Decimal(10, 2)"},
                {"name":"ok","type":"Bool"},
                {"name":"tags","type":"Array(String)"},
                {"name":"f","type":"Float64"},
                {"name":"big","type":"UInt64"},
                {"name":"quoted","type":"Int64"},
                {"name":"dnum","type":"Decimal(10, 2)"}
            ],
            "data": [
                [1, "ann", null, "2026-09-04 10:00:00", "12.50", true, ["a","b"], 1.5, 18446744073709551615, "42", 12.5],
                [2, "bob", "x", "2026-09-04 11:00:00", "0.10", false, [], "nan", 3, "7", 3]
            ],
            "rows": 2
        }"#;
        let set = decode_compact(body, 10).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(set.columns.len(), 11);
        assert_eq!(set.columns.first().map(|c| c.type_name.as_str()), Some("UInt64"));
        assert!(!set.truncated);
        let first = set.rows.first().cloned().unwrap_or_default();
        assert_eq!(first.first(), Some(&Value::Int(1)));
        assert_eq!(first.get(1), Some(&Value::Text("ann".into())));
        assert_eq!(first.get(2), Some(&Value::Null));
        assert_eq!(first.get(3), Some(&Value::DateTime("2026-09-04 10:00:00".into())));
        assert_eq!(first.get(4), Some(&Value::Decimal("12.50".into())));
        assert_eq!(first.get(5), Some(&Value::Bool(true)));
        assert!(matches!(first.get(6), Some(Value::Json(serde_json::Value::Array(a))) if a.len() == 2));
        assert_eq!(first.get(7), Some(&Value::Float(1.5)));
        assert_eq!(first.get(8), Some(&Value::Decimal("18446744073709551615".into())));
        assert_eq!(first.get(9), Some(&Value::Int(42)));
        assert_eq!(first.get(10), Some(&Value::Decimal("12.5".into())));
        let second = set.rows.get(1).cloned().unwrap_or_default();
        assert!(matches!(second.get(7), Some(Value::Float(f)) if f.is_nan()));
        assert_eq!(second.get(8), Some(&Value::Int(3)));
    }

    #[test]
    fn row_cap_marks_truncation() {
        let body = r#"{"meta":[{"name":"x","type":"UInt8"}],"data":[[1],[2],[3]],"rows":3}"#;
        let set = decode_compact(body, 2).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(set.rows.len(), 2);
        assert!(set.truncated);
    }

    #[test]
    fn non_json_bodies_become_text_rows() {
        let set = decode_body("line one\nline two", 10);
        assert_eq!(set.columns.first().map(|c| c.name.as_str()), Some("output"));
        assert_eq!(set.rows.len(), 2);
        assert_eq!(set.rows.get(1).and_then(|r| r.first()), Some(&Value::Text("line two".into())));
    }

    #[test]
    fn summary_header_yields_written_rows() {
        assert_eq!(parse_written_rows(r#"{"read_rows":"0","read_bytes":"0","written_rows":"2","written_bytes":"64","total_rows_to_read":"0"}"#), Some(2));
        assert_eq!(parse_written_rows(r#"{"written_rows":5}"#), Some(5));
        assert_eq!(parse_written_rows("not json"), None);
        assert_eq!(parse_written_rows(r#"{"read_rows":"1"}"#), None);
    }

    // WHAT:  Live round trip. Skipped unless DB_FREE_CLICKHOUSE_URL is set, e.g.
    //        DB_FREE_CLICKHOUSE_URL=http://127.0.0.1:8124 DB_FREE_CLICKHOUSE_USER=default DB_FREE_CLICKHOUSE_PASSWORD=dbfree
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DB_FREE_CLICKHOUSE_URL") else {
            return;
        };
        let parsed = reqwest::Url::parse(&url).unwrap_or_else(|e| panic!("bad url: {e}"));
        let input = ConnectionInput {
            name: "live-ch".into(),
            engine: Engine::Clickhouse,
            environment: Environment::Local,
            read_only: false,
            host: parsed.host_str().map(str::to_string),
            port: parsed.port(),
            database: Some("default".into()),
            username: std::env::var("DB_FREE_CLICKHOUSE_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: if parsed.scheme() == "https" { SslMode::Require } else { SslMode::Disable },
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary::draft(&input, true),
            secret: std::env::var("DB_FREE_CLICKHOUSE_PASSWORD").ok(),
        };
        let ch = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        ch.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert!(ch.server_version().await.unwrap_or_default().is_some_and(|v| v.starts_with("ClickHouse ")));
        assert_eq!(ch.current_database(), Some("default".into()));
        assert!(ch.databases().await.unwrap_or_default().iter().any(|d| d == "default"));

        let _ = ch.execute("DROP TABLE IF EXISTS dbfree_t", 10).await;
        let out = ch
            .execute(
                "CREATE TABLE dbfree_t (id UInt32, name String, meta String, ts DateTime, n Decimal(10,2), ok Bool, tags Array(String)) \
                 ENGINE = MergeTree ORDER BY id; \
                 INSERT INTO dbfree_t VALUES (1, 'ann', '{\"a\":1}', '2026-09-04 10:00:00', 12.50, true, ['x','y']), \
                 (2, 'bob', '', '2026-09-04 11:00:00', 0.10, false, []); \
                 SELECT * FROM dbfree_t ORDER BY id;",
                100,
            )
            .await
            .unwrap_or_else(|e| panic!("execute: {e}"));
        assert_eq!(out.len(), 3, "{out:?}");
        assert!(matches!(out.first(), Some(StatementResult::Affected { .. })));
        assert!(matches!(out.get(1), Some(StatementResult::Affected { rows_affected: 2 })), "{out:?}");
        match out.get(2) {
            Some(StatementResult::Rows { result }) => {
                assert_eq!(result.rows.len(), 2);
                assert_eq!(result.columns.len(), 7);
                let first = result.rows.first().cloned().unwrap_or_default();
                assert_eq!(first.first(), Some(&Value::Int(1)));
                assert_eq!(first.get(1), Some(&Value::Text("ann".into())));
                assert_eq!(first.get(2), Some(&Value::Text("{\"a\":1}".into())));
                assert_eq!(first.get(3), Some(&Value::DateTime("2026-09-04 10:00:00".into())));
                // Servers differ on trailing zeros ("12.5" vs "12.50"); the kind and value are what matter.
                assert!(matches!(first.get(4), Some(Value::Decimal(d)) if d.parse::<f64>().ok() == Some(12.5)), "{first:?}");
                assert_eq!(first.get(5), Some(&Value::Bool(true)));
                assert!(matches!(first.get(6), Some(Value::Json(serde_json::Value::Array(a))) if a.len() == 2));
            }
            other => panic!("expected rows, got {other:?}"),
        }

        let catalog = ch.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let table = catalog
            .schemas
            .iter()
            .flat_map(|s| s.tables.iter())
            .find(|t| t.name == "dbfree_t")
            .cloned()
            .unwrap_or_else(|| panic!("dbfree_t missing from catalog"));
        assert_eq!(table.kind, TableKind::Table);
        let table_ref = TableRef { schema: table.schema.clone(), name: table.name.clone() };
        let cols = ch.columns(&table_ref).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(cols.len(), 7);
        assert!(cols.iter().any(|c| c.name == "id" && c.primary_key && c.data_type == "UInt32"));
        assert_eq!(ch.row_estimate(&table_ref).await.unwrap_or_default(), Some(2));
        assert_eq!(ch.count(&table_ref, &[]).await.unwrap_or_default(), 2);

        let query = PageQuery {
            sort: vec![SortRule { column: "id".into(), desc: true }],
            filters: vec![FilterRule { column: "name".into(), op: FilterOp::Contains, value: "n".into() }],
            offset: 0,
            limit: 10,
        };
        let page = ch.fetch_page(&table_ref, &query).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1, "only 'ann' contains n: {page:?}");
        assert_eq!(ch.count(&table_ref, &query.filters).await.unwrap_or_default(), 1);

        let ddl = ch.ddl(&table_ref).await.unwrap_or_else(|e| panic!("ddl: {e}"));
        assert!(ddl.is_some_and(|d| d.contains("CREATE TABLE") && d.contains("dbfree_t")));

        let truncated = ch.execute("SELECT * FROM dbfree_t", 1).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(truncated.first(), Some(StatementResult::Rows { result }) if result.truncated));

        ch.execute("DROP TABLE dbfree_t", 10).await.unwrap_or_else(|e| panic!("drop: {e}"));
        ch.close().await;
    }
}
