// SOT: druid-integration, druid-sql-api, druid-native-query, druid-information-schema

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::http::{json_result, objects_to_result_set, HttpClient};
use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{qualified_name_for, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Apache Druid adapter over the Router (port 8888): Druid SQL through
//        `POST /druid/v2/sql` and native JSON queries through `POST /druid/v2`.
// WHY:   Druid exposes a full INFORMATION_SCHEMA, so catalog / columns are
//        plain SQL; identifiers are double-quoted (ANSI), which the shared
//        clause builders already produce for `Engine::Druid`.
// HOW:   `execute` sends SQL as `{query, resultFormat: "object", header: false}`
//        and decodes the array of objects; a body starting with `{"queryType"`
//        goes to the native endpoint and is shown verbatim (or as rows when the
//        result is a list of objects / `{timestamp, result}` pairs). `__time`
//        is reported as the primary key so rows are addressable.
// WHERE: src-tauri/src/integrations/mod.rs (trait), integrations/sql.rs (clauses)
// ============================================================================

const DEFAULT_PORT: u16 = 8888;
const DEFAULT_SCHEMA: &str = "druid";

pub struct DruidIntegration {
    http: HttpClient,
    schema: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = HttpClient::auth_from_connection(conn);
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, auth)?;
    let schema = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).unwrap_or(DEFAULT_SCHEMA).to_string();
    let integration = DruidIntegration { http, schema };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Response shaping
// ---------------------------------------------------------------------------

fn sql_rows_to_result_set(rows: &[Json], max_rows: usize) -> ResultSet {
    if rows.is_empty() {
        return ResultSet { columns: vec![], rows: vec![], truncated: false };
    }
    objects_to_result_set(rows, None, max_rows)
}

// WHAT:  Native query results come in several shapes; each is turned into rows
//        when it is regular enough, otherwise shown as one JSON cell.
fn native_result(body: &Json, max_rows: usize) -> ResultSet {
    let Some(items) = body.as_array() else { return json_result(body.clone()) };
    if items.is_empty() {
        return ResultSet { columns: vec![ColumnMeta { name: "result".into(), type_name: "array".into() }], rows: vec![], truncated: false };
    }
    // timeseries / topN: [{timestamp, result: {…} | [{…}]}]
    if items.iter().all(|i| i.get("timestamp").is_some() && i.get("result").is_some()) {
        let mut flat = Vec::new();
        for item in items {
            let ts = item.get("timestamp").cloned().unwrap_or(Json::Null);
            match item.get("result") {
                Some(Json::Array(list)) => {
                    for r in list {
                        let mut obj = r.as_object().cloned().unwrap_or_default();
                        obj.insert("timestamp".into(), ts.clone());
                        flat.push(Json::Object(obj));
                    }
                }
                Some(Json::Object(o)) => {
                    let mut obj = o.clone();
                    obj.insert("timestamp".into(), ts.clone());
                    flat.push(Json::Object(obj));
                }
                _ => flat.push(item.clone()),
            }
        }
        return objects_to_result_set(&flat, Some("timestamp"), max_rows);
    }
    // groupBy: [{version, timestamp, event: {…}}]
    if items.iter().all(|i| i.get("event").is_some()) {
        let flat: Vec<Json> = items
            .iter()
            .map(|i| {
                let mut obj = i.get("event").and_then(Json::as_object).cloned().unwrap_or_default();
                if let Some(ts) = i.get("timestamp") {
                    obj.insert("timestamp".into(), ts.clone());
                }
                Json::Object(obj)
            })
            .collect();
        return objects_to_result_set(&flat, Some("timestamp"), max_rows);
    }
    // scan: [{segmentId, columns, events: [[…]] | [{…}]}]
    if items.iter().all(|i| i.get("events").is_some()) {
        let mut flat = Vec::new();
        for item in items {
            let cols: Vec<String> = item.get("columns").and_then(Json::as_array).into_iter().flatten().filter_map(|c| c.as_str().map(str::to_string)).collect();
            for ev in item.get("events").and_then(Json::as_array).into_iter().flatten() {
                match ev {
                    Json::Array(cells) => {
                        let obj: serde_json::Map<String, Json> = cols.iter().cloned().zip(cells.iter().cloned()).collect();
                        flat.push(Json::Object(obj));
                    }
                    other => flat.push(other.clone()),
                }
            }
        }
        return objects_to_result_set(&flat, None, max_rows);
    }
    if items.iter().all(Json::is_object) {
        return objects_to_result_set(items, None, max_rows);
    }
    json_result(body.clone())
}

fn is_native_query(text: &str) -> bool {
    let t = text.trim_start();
    t.starts_with('{') && t.contains("\"queryType\"")
}

fn cell_text(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::Text(t)) | Some(Value::Decimal(t)) | Some(Value::DateTime(t)) => Some(t.clone()),
        Some(Value::Int(i)) => Some(i.to_string()),
        Some(Value::Float(f)) => Some(f.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(Value::Json(j)) => Some(j.to_string()),
        _ => None,
    }
}

fn cell_i64(v: Option<&Value>) -> Option<i64> {
    match v {
        Some(Value::Int(i)) => Some(*i),
        Some(Value::Float(f)) => Some(*f as i64),
        Some(Value::Decimal(d)) | Some(Value::Text(d)) => d.parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl DruidIntegration {
    async fn sql(&self, query: &str, max_rows: usize) -> AppResult<ResultSet> {
        let body = json!({"query": query, "resultFormat": "object", "header": false, "context": {"sqlQueryId": uuid::Uuid::new_v4().to_string()}});
        let out: Json = self.http.post_json("/druid/v2/sql", &body).await?;
        let rows = out.as_array().cloned().unwrap_or_default();
        Ok(sql_rows_to_result_set(&rows, max_rows))
    }

    async fn native(&self, query: &Json, max_rows: usize) -> AppResult<ResultSet> {
        let out: Json = self.http.post_json("/druid/v2", query).await?;
        Ok(native_result(&out, max_rows))
    }

    fn qualified(&self, table: &TableRef) -> String {
        let with_schema = TableRef { schema: Some(table.schema.clone().unwrap_or_else(|| self.schema.clone())), name: table.name.clone() };
        qualified_name_for(Engine::Druid, &with_schema)
    }

    fn schema_of<'a>(&'a self, table: &'a TableRef) -> &'a str {
        table.schema.as_deref().unwrap_or(self.schema.as_str())
    }
}

#[async_trait]
impl Integration for DruidIntegration {
    fn engine(&self) -> Engine {
        Engine::Druid
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { transactions: false, exact_estimate: false, ..Capabilities::SQL }
    }

    async fn ping(&self) -> AppResult<()> {
        let _: Json = self.http.get_json("/status").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let status: Json = self.http.get_json("/status").await?;
        Ok(status.get("version").and_then(Json::as_str).map(|v| format!("Apache Druid {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.schema.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let set = self.sql("SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES ORDER BY 1", 100).await?;
        let mut names: Vec<String> = set.rows.iter().filter_map(|r| cell_text(r.first())).collect();
        if !names.contains(&self.schema) {
            names.insert(0, self.schema.clone());
        }
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let set = self.sql("SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES ORDER BY 1, 2", 100_000).await?;
        let idx = |name: &str| set.columns.iter().position(|c| c.name == name);
        let (si, ni, ti) = (idx("TABLE_SCHEMA"), idx("TABLE_NAME"), idx("TABLE_TYPE"));
        let mut schemas: Vec<SchemaInfo> = Vec::new();
        for row in &set.rows {
            let Some(schema) = si.and_then(|i| cell_text(row.get(i))) else { continue };
            let Some(name) = ni.and_then(|i| cell_text(row.get(i))) else { continue };
            let ttype = ti.and_then(|i| cell_text(row.get(i))).unwrap_or_default();
            let kind = if ttype.eq_ignore_ascii_case("SYSTEM_TABLE") || ttype.eq_ignore_ascii_case("VIEW") { TableKind::View } else { TableKind::Table };
            let entry = TableInfo { schema: Some(schema.clone()), name, kind, row_estimate: None };
            match schemas.iter_mut().find(|s| s.name == schema) {
                Some(s) => s.tables.push(entry),
                None => schemas.push(SchemaInfo { name: schema, tables: vec![entry] }),
            }
        }
        // The session schema first, then the rest.
        schemas.sort_by_key(|s| (s.name != self.schema, s.name.clone()));
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let sql = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
            quote_literal(self.schema_of(table)),
            quote_literal(&table.name)
        );
        let set = self.sql(&sql, 10_000).await?;
        let idx = |name: &str| set.columns.iter().position(|c| c.name == name);
        let (ci, di, ni, oi) = (idx("COLUMN_NAME"), idx("DATA_TYPE"), idx("IS_NULLABLE"), idx("ORDINAL_POSITION"));
        let cols: Vec<ColumnInfo> = set
            .rows
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                let name = ci.and_then(|x| cell_text(row.get(x)))?;
                let data_type = di.and_then(|x| cell_text(row.get(x))).unwrap_or_else(|| "VARCHAR".into());
                let nullable = ni.and_then(|x| cell_text(row.get(x))).map(|v| v.eq_ignore_ascii_case("YES")).unwrap_or(true);
                let ordinal = oi.and_then(|x| cell_i64(row.get(x))).and_then(|o| u32::try_from(o).ok()).unwrap_or(i as u32 + 1);
                Some(ColumnInfo { primary_key: name == "__time", name, data_type, nullable, ordinal })
            })
            .collect();
        if cols.is_empty() {
            return Err(AppError::not_found(format!("Table \"{}\" has no columns in INFORMATION_SCHEMA (is it still loading?).", table.name)));
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        if self.schema_of(table) != "druid" {
            return Ok(None);
        }
        // sys.segments carries per-datasource row counts without scanning.
        let sql = format!("SELECT SUM(\"num_rows\") AS n FROM sys.segments WHERE \"datasource\" = {} AND is_active = 1", quote_literal(&table.name));
        match self.sql(&sql, 1).await {
            Ok(set) => Ok(set.rows.first().and_then(|r| cell_i64(r.first()))),
            Err(_) => Ok(None),
        }
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT COUNT(*) AS n FROM {}{}", self.qualified(table), where_clause(Engine::Druid, filters));
        let set = self.sql(&sql, 1).await?;
        Ok(set.rows.first().and_then(|r| cell_i64(r.first())).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            self.qualified(table),
            where_clause(Engine::Druid, &query.filters),
            order_clause(Engine::Druid, &query.sort),
            query.limit,
            query.offset
        );
        self.sql(&sql, query.limit as usize).await
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        if is_native_query(sql) {
            let q: Json = serde_json::from_str(sql.trim()).map_err(|e| AppError::invalid_input(format!("Native query is not valid JSON: {e}")))?;
            return Ok(vec![StatementResult::Rows { result: self.native(&q, max_rows).await? }]);
        }
        let mut results = Vec::new();
        for stmt in split_statements(sql) {
            let trimmed = stmt.trim().trim_end_matches(';').trim();
            if trimmed.is_empty() {
                continue;
            }
            let upper = trimmed.to_ascii_uppercase();
            let is_write = ["INSERT", "REPLACE", "DELETE", "DROP", "ALTER", "CREATE", "UPDATE"].iter().any(|kw| upper.starts_with(kw));
            let set = self.sql(trimmed, max_rows).await?;
            if is_write && set.columns.iter().all(|c| c.name == "TASK") {
                // Ingestion statements return a task id row (MSQ); report it as rows so the id is visible.
                results.push(StatementResult::Rows { result: set });
            } else if is_write && set.rows.is_empty() {
                results.push(StatementResult::Affected { rows_affected: 0 });
            } else {
                results.push(StatementResult::Rows { result: set });
            }
        }
        Ok(results)
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, FilterOp, SortRule, SslMode};

    #[test]
    fn page_sql_uses_double_quotes() {
        let table = TableRef { schema: Some("druid".into()), name: "wiki\"pedia".into() };
        assert_eq!(qualified_name_for(Engine::Druid, &table), "\"druid\".\"wiki\"\"pedia\"");
        let w = where_clause(Engine::Druid, &[FilterRule { column: "channel".into(), op: FilterOp::Contains, value: "en".into() }]);
        assert_eq!(w, " WHERE CAST(\"channel\" AS STRING) LIKE '%en%'");
        let o = order_clause(Engine::Druid, &[SortRule { column: "__time".into(), desc: true }]);
        assert_eq!(o, " ORDER BY \"__time\" DESC");
    }

    #[test]
    fn native_shapes_become_rows() {
        let ts = json!([{"timestamp": "2024-01-01T00:00:00.000Z", "result": {"count": 3}}]);
        let set = native_result(&ts, 10);
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["timestamp", "count"]);
        assert_eq!(set.rows[0][1], Value::Int(3));
        let topn = json!([{"timestamp": "t", "result": [{"dim": "a", "n": 1}, {"dim": "b", "n": 2}]}]);
        assert_eq!(native_result(&topn, 10).rows.len(), 2);
        let group = json!([{"version": "v1", "timestamp": "t", "event": {"dim": "a", "n": 1}}]);
        let set = native_result(&group, 10);
        assert_eq!(set.columns[0].name, "timestamp");
        assert_eq!(set.rows[0][1], Value::Text("a".into()));
        let scan = json!([{"segmentId": "s", "columns": ["a", "b"], "events": [[1, "x"], [2, "y"]]}]);
        let set = native_result(&scan, 10);
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["a", "b"]);
        assert_eq!(set.rows[1][1], Value::Text("y".into()));
        assert_eq!(native_result(&json!([]), 10).rows.len(), 0);
        assert_eq!(native_result(&json!({"error": "x"}), 10).columns[0].name, "result");
        assert!(is_native_query("{\"queryType\": \"timeseries\"}"));
        assert!(!is_native_query("SELECT 1"));
    }

    #[test]
    fn sql_object_rows() {
        let rows = vec![json!({"__time": "2024-01-01T00:00:00.000Z", "n": 1}), json!({"__time": "2024-01-02T00:00:00.000Z", "n": 2})];
        let set = sql_rows_to_result_set(&rows, 1);
        assert_eq!(set.columns.len(), 2);
        assert!(set.truncated);
        assert!(sql_rows_to_result_set(&[], 1).columns.is_empty());
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_DRUID_URL is set.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_DRUID_URL") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Druid,
            environment: Environment::Local,
            read_only: true,
            host: Some(url),
            port: None,
            database: None,
            username: std::env::var("DBFREE_TEST_DRUID_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let secret = std::env::var("DBFREE_TEST_DRUID_PASSWORD").ok();
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, secret.is_some()), secret };
        let d = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        assert!(d.server_version().await.unwrap_or_default().unwrap_or_default().starts_with("Apache Druid"));
        let cat = d.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas.iter().any(|s| s.name == "sys"), "{cat:?}");
        let table = TableRef { schema: Some("sys".into()), name: "servers".into() };
        let cols = d.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "server"), "{cols:?}");
        let page = d.fetch_page(&table, &PageQuery { sort: vec![SortRule { column: "server".into(), desc: false }], filters: vec![], offset: 0, limit: 10 }).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert!(!page.rows.is_empty());
        assert!(d.count(&table, &[]).await.unwrap_or_default() >= 1);
        let out = d.execute("SELECT COUNT(*) AS n FROM sys.segments", 10).await.unwrap_or_else(|e| panic!("sql: {e}"));
        assert!(matches!(&out[0], StatementResult::Rows { .. }));
        let out = d.execute("{\"queryType\":\"segmentMetadata\",\"dataSource\":\"nonexistent\"}", 10).await.unwrap_or_else(|e| panic!("native: {e}"));
        assert!(matches!(&out[0], StatementResult::Rows { .. }));
    }
}
