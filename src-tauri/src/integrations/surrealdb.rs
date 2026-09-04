// SOT: surrealdb-integration, surrealql, surreal-http-api, surreal-sql-endpoint

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, json_to_value, json_type_name, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SortRule, SslMode, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Method;
use serde_json::Value as Json;
use std::sync::Arc;

// ============================================================================
// WHAT:  SurrealDB adapter over the HTTP API (port 8000, `POST /sql`).
// WHY:   Tables are schemaless unless DEFINEd; the grid needs a header, so
//        columns = `INFO FOR TABLE` fields (schemafull) ∪ keys sampled from
//        `SELECT * FROM t LIMIT 50`, with `id` pinned and marked primary key.
// HOW:   The `database` field is "ns/db" (default test/test). Every request
//        sends both the v2 (`surreal-ns` / `surreal-db`) and v1 (`NS` / `DB`)
//        headers plus Basic auth (root user + secret). Paging is SurrealQL
//        (`WHERE … ORDER BY … LIMIT n START m`) with values inlined as
//        escaped literals (the HTTP `/sql` endpoint has no bind parameters).
//        `execute` is SurrealQL passthrough: one StatementResult per returned
//        statement; mutating statements are refused when read-only. Record ids
//        (`table:id`) arrive as strings and are shown as Text.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs (trait)
// ============================================================================

const DEFAULT_PORT: u16 = 8000;
const DEFAULT_NS: &str = "test";
const DEFAULT_DB: &str = "test";
const SAMPLE_SIZE: usize = 50;
const MAX_PAGE_ROWS: u32 = 5_000;
const WRITE_KEYWORDS: [&str; 9] = ["CREATE", "UPDATE", "DELETE", "RELATE", "INSERT", "UPSERT", "DEFINE", "REMOVE", "LET"];

pub struct SurrealIntegration {
    engine: Engine,
    http: HttpClient,
    namespace: String,
    database: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let (namespace, database) = split_ns_db(s.database.as_deref());
    let base = base_url(conn, Some(DEFAULT_PORT), false);
    let insecure = s.ssl_mode == SslMode::Require;
    let http = HttpClient::new(base, HttpClient::auth_from_connection(conn), insecure)?;
    let integration = SurrealIntegration { engine: s.engine, http, namespace, database, read_only: s.read_only };
    integration.ping().await?;
    // SurrealDB 2.x rejects every statement until the namespace and database
    // exist. Creating them is idempotent and needs no privileges beyond the
    // ones a user who can write already has, so a fresh server is usable
    // immediately instead of failing with "The namespace 'x' does not exist".
    if !s.read_only {
        let bootstrap = format!(
            "DEFINE NAMESPACE IF NOT EXISTS {}; USE NS {}; DEFINE DATABASE IF NOT EXISTS {};",
            ident(&integration.namespace),
            ident(&integration.namespace),
            ident(&integration.database)
        );
        let _ = integration.sql(&bootstrap).await;
    }
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// WHAT:  "ns/db" → (ns, db); a bare name is the database inside the default namespace.
fn split_ns_db(raw: Option<&str>) -> (String, String) {
    let t = raw.map(str::trim).unwrap_or("");
    if t.is_empty() {
        return (DEFAULT_NS.into(), DEFAULT_DB.into());
    }
    match t.split_once('/') {
        Some((ns, db)) => (
            if ns.trim().is_empty() { DEFAULT_NS.into() } else { ns.trim().into() },
            if db.trim().is_empty() { DEFAULT_DB.into() } else { db.trim().into() },
        ),
        None => (DEFAULT_NS.into(), t.into()),
    }
}

// WHAT:  Backtick-quotes an identifier (SurrealQL accepts `⟨ ⟩` and backticks).
fn ident(raw: &str) -> String {
    format!("`{}`", raw.replace('`', "\\`"))
}

fn quote_str(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('\'');
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

// WHAT:  A typed literal for comparisons: numbers / booleans as-is, `table:id`
//        as a record reference when filtering `id`, everything else a string.
fn literal(column: &str, raw: &str) -> String {
    let t = raw.trim();
    if column == "id" {
        if let Some((table, key)) = t.split_once(':') {
            let ok = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if ok(table) && ok(key) {
                return format!("{table}:{key}");
            }
        }
        return quote_str(t);
    }
    if t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        return t.to_ascii_lowercase();
    }
    if t.eq_ignore_ascii_case("null") {
        return "NULL".into();
    }
    if t.eq_ignore_ascii_case("none") {
        return "NONE".into();
    }
    if t.parse::<i64>().is_ok() || t.parse::<f64>().is_ok() {
        return t.to_string();
    }
    quote_str(t)
}

fn where_clause(filters: &[FilterRule]) -> String {
    if filters.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| {
            let col = ident(&f.column);
            let v = f.value.trim();
            match f.op {
                FilterOp::Eq => format!("{col} = {}", literal(&f.column, v)),
                FilterOp::Ne => format!("{col} != {}", literal(&f.column, v)),
                FilterOp::Gt => format!("{col} > {}", literal(&f.column, v)),
                FilterOp::Gte => format!("{col} >= {}", literal(&f.column, v)),
                FilterOp::Lt => format!("{col} < {}", literal(&f.column, v)),
                FilterOp::Lte => format!("{col} <= {}", literal(&f.column, v)),
                FilterOp::Contains => format!("string::contains(<string> {col}, {})", quote_str(v)),
                FilterOp::StartsWith => format!("string::starts_with(<string> {col}, {})", quote_str(v)),
                FilterOp::EndsWith => format!("string::ends_with(<string> {col}, {})", quote_str(v)),
                FilterOp::In => {
                    let items: Vec<String> = v.split(',').map(str::trim).filter(|x| !x.is_empty()).map(|x| literal(&f.column, x)).collect();
                    if items.is_empty() {
                        "false".into()
                    } else {
                        format!("{col} INSIDE [{}]", items.join(", "))
                    }
                }
                FilterOp::IsNull => format!("({col} = NONE OR {col} = NULL)"),
                FilterOp::IsNotNull => format!("({col} != NONE AND {col} != NULL)"),
            }
        })
        .collect();
    format!(" WHERE {}", parts.join(" AND "))
}

fn order_clause(sort: &[SortRule]) -> String {
    if sort.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = sort.iter().map(|s| format!("{} {}", ident(&s.column), if s.desc { "DESC" } else { "ASC" })).collect();
    format!(" ORDER BY {}", parts.join(", "))
}

// WHAT:  Splits a SurrealQL script on `;` outside of quotes / brackets.
fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut depth = 0i32;
    for ch in text.chars() {
        match quote {
            Some(q) => {
                cur.push(ch);
                if ch == q {
                    quote = None;
                }
            }
            None => match ch {
                '\'' | '"' | '`' => {
                    quote = Some(ch);
                    cur.push(ch);
                }
                '{' | '[' | '(' => {
                    depth += 1;
                    cur.push(ch);
                }
                '}' | ']' | ')' => {
                    depth -= 1;
                    cur.push(ch);
                }
                ';' if depth <= 0 => {
                    if !cur.trim().is_empty() {
                        out.push(cur.trim().to_string());
                    }
                    cur.clear();
                }
                _ => cur.push(ch),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

fn first_word(stmt: &str) -> String {
    stmt.trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

fn is_write_statement(stmt: &str) -> bool {
    let w = first_word(stmt);
    WRITE_KEYWORDS.contains(&w.as_str())
}

// WHAT:  One `/sql` response entry → StatementResult.
fn statement_to_result(status: &str, result: &Json, stmt: &str, max_rows: usize) -> AppResult<StatementResult> {
    if status != "OK" {
        let msg = result.as_str().map(str::to_string).unwrap_or_else(|| result.to_string());
        return Err(AppError::driver(format!("SurrealQL error: {msg}")));
    }
    let word = first_word(stmt);
    match result {
        Json::Array(items) => {
            if matches!(word.as_str(), "CREATE" | "UPDATE" | "DELETE" | "RELATE" | "INSERT" | "UPSERT") {
                return Ok(StatementResult::Affected { rows_affected: items.len() as u64 });
            }
            if items.is_empty() {
                return Ok(StatementResult::Rows { result: ResultSet { columns: vec![], rows: vec![], truncated: false } });
            }
            if items.iter().all(Json::is_object) {
                let id = items.iter().any(|d| d.get("id").is_some()).then_some("id");
                let rs = crate::integrations::http::objects_to_result_set(items, id, max_rows);
                return Ok(StatementResult::Rows { result: rs });
            }
            let truncated = items.len() > max_rows;
            let type_name = items.iter().find(|v| !v.is_null()).map(json_type_name).unwrap_or("json").to_string();
            Ok(StatementResult::Rows {
                result: ResultSet {
                    columns: vec![ColumnMeta { name: "value".into(), type_name }],
                    rows: items.iter().take(max_rows).map(|v| vec![json_to_value(v)]).collect(),
                    truncated,
                },
            })
        }
        Json::Null if matches!(word.as_str(), "DEFINE" | "REMOVE" | "LET" | "USE" | "BEGIN" | "COMMIT" | "CANCEL") => {
            Ok(StatementResult::Affected { rows_affected: 0 })
        }
        other => Ok(StatementResult::Rows { result: json_result(other.clone()) }),
    }
}

// WHAT:  Union of keys across sampled records + declared fields; `id` pinned.
fn union_columns(declared: &[String], docs: &[Json]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = vec!["id".into()];
    let mut types: Vec<Option<&'static str>> = vec![Some("record")];
    let mut push = |name: &str, value: Option<&Json>| {
        let idx = match names.iter().position(|n| n == name) {
            Some(i) => i,
            None => {
                names.push(name.to_string());
                types.push(None);
                names.len() - 1
            }
        };
        if let Some(v) = value {
            if types[idx].is_none() && !v.is_null() {
                types[idx] = Some(json_type_name(v));
            }
        }
    };
    for d in declared {
        // Nested field definitions (`a.b`, `a[*]`) are not top-level keys.
        if !d.contains('.') && !d.contains('[') {
            push(d, None);
        }
    }
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for (k, v) in obj {
                push(k, Some(v));
            }
        }
    }
    names
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, ty))| ColumnInfo {
            primary_key: name == "id",
            nullable: name != "id",
            data_type: ty.unwrap_or("null").to_string(),
            name,
            ordinal: u32::try_from(i + 1).unwrap_or(u32::MAX),
        })
        .collect()
}

fn docs_to_result_set(columns: &[ColumnInfo], docs: &[Json], truncated: bool) -> ResultSet {
    let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut types: Vec<String> = columns.iter().map(|c| c.data_type.clone()).collect();
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for (k, v) in obj {
                if !names.iter().any(|n| n == k) {
                    names.push(k.clone());
                    types.push(json_type_name(v).to_string());
                }
            }
        }
    }
    let rows = docs
        .iter()
        .map(|doc| {
            let obj = doc.as_object();
            names.iter().map(|n| obj.and_then(|o| o.get(n)).map(json_to_value).unwrap_or(Value::Null)).collect()
        })
        .collect();
    ResultSet { columns: names.into_iter().zip(types).map(|(name, type_name)| ColumnMeta { name, type_name }).collect(), rows, truncated }
}

// WHAT:  `INFO FOR …` returns maps of name → definition; extract the names.
fn info_keys(info: &Json, section: &str) -> Vec<String> {
    let mut keys: Vec<String> = info
        .get(section)
        .and_then(Json::as_object)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    keys.sort();
    keys
}

impl SurrealIntegration {
    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(reqwest::header::ACCEPT, HeaderValue::from_static("application/json"));
        h.insert(reqwest::header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        for (name, value) in [("surreal-ns", &self.namespace), ("surreal-db", &self.database), ("NS", &self.namespace), ("DB", &self.database)] {
            if let (Ok(n), Ok(v)) = (reqwest::header::HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(value)) {
                h.insert(n, v);
            }
        }
        h
    }

    // WHAT:  POST /sql → [{status, result, time}, …]
    async fn sql(&self, script: &str) -> AppResult<Vec<Json>> {
        let req = self.http.request(Method::POST, "/sql").headers(self.headers()).body(script.to_string());
        let resp = self.http.send(req).await?;
        let body: Json = resp.json().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))?;
        match body {
            Json::Array(items) => Ok(items),
            Json::Object(o) if o.get("code").is_some() || o.get("description").is_some() => {
                let msg = o.get("information").or_else(|| o.get("description")).and_then(Json::as_str).unwrap_or("request failed");
                Err(AppError::driver(format!("SurrealDB: {msg}")))
            }
            other => Ok(vec![other]),
        }
    }

    // WHAT:  Runs one statement and returns its `result` (or an error for a non-OK status).
    async fn one(&self, statement: &str) -> AppResult<Json> {
        let items = self.sql(statement).await?;
        let first = items.into_iter().next().ok_or_else(|| AppError::driver("SurrealDB returned no statement result."))?;
        let status = first.get("status").and_then(Json::as_str).unwrap_or("ERR");
        let result = first.get("result").cloned().unwrap_or(Json::Null);
        if status != "OK" {
            let msg = result.as_str().map(str::to_string).unwrap_or_else(|| result.to_string());
            return Err(AppError::driver(format!("SurrealQL error: {msg}")));
        }
        Ok(result)
    }

    async fn table_names(&self) -> AppResult<Vec<String>> {
        let info = self.one("INFO FOR DB").await?;
        Ok(info_keys(&info, "tables"))
    }
}

#[async_trait]
impl Integration for SurrealIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { exact_estimate: true, ..Capabilities::DOCUMENT }
    }

    async fn ping(&self) -> AppResult<()> {
        let req = self.http.request(Method::GET, "/health");
        self.http.send(req).await?;
        // A real query validates credentials + ns/db headers.
        let _ = self.one("RETURN 1").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let text = self.http.get_text("/version").await?;
        let v = text.trim();
        if v.is_empty() {
            return Ok(None);
        }
        Ok(Some(if v.to_ascii_lowercase().starts_with("surreal") { v.to_string() } else { format!("SurrealDB {v}") }))
    }

    fn current_database(&self) -> Option<String> {
        Some(format!("{}/{}", self.namespace, self.database))
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let mut out = Vec::new();
        let namespaces = match self.one("INFO FOR ROOT").await {
            Ok(info) => info_keys(&info, "namespaces"),
            Err(_) => vec![self.namespace.clone()],
        };
        for ns in namespaces {
            let req = |script: &str| {
                let mut h = self.headers();
                if let Ok(v) = HeaderValue::from_str(&ns) {
                    h.insert("surreal-ns", v.clone());
                    h.insert("NS", v);
                }
                h.remove("surreal-db");
                h.remove("DB");
                self.http.request(Method::POST, "/sql").headers(h).body(script.to_string())
            };
            let dbs: Vec<String> = match self.http.send(req("INFO FOR NS")).await {
                Ok(resp) => resp
                    .json::<Json>()
                    .await
                    .ok()
                    .and_then(|v| v.as_array().and_then(|a| a.first()).and_then(|f| f.get("result")).cloned())
                    .map(|info| info_keys(&info, "databases"))
                    .unwrap_or_default(),
                Err(_) => Vec::new(),
            };
            if dbs.is_empty() && ns == self.namespace {
                out.push(format!("{ns}/{}", self.database));
            }
            out.extend(dbs.into_iter().map(|db| format!("{ns}/{db}")));
        }
        if out.is_empty() {
            out.push(format!("{}/{}", self.namespace, self.database));
        }
        Ok(out)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let names = self.table_names().await?;
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let row_estimate = self
                .one(&format!("SELECT count() FROM {} GROUP ALL", ident(&name)))
                .await
                .ok()
                .and_then(|r| r.as_array().and_then(|a| a.first()).and_then(|f| f.get("count")).and_then(Json::as_i64))
                .or(Some(0));
            tables.push(TableInfo { schema: Some(self.database.clone()), name, kind: TableKind::Table, row_estimate });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let declared = match self.one(&format!("INFO FOR TABLE {}", ident(&table.name))).await {
            Ok(info) => info_keys(&info, "fields"),
            Err(_) => Vec::new(),
        };
        let sample = self.one(&format!("SELECT * FROM {} LIMIT {SAMPLE_SIZE}", ident(&table.name))).await?;
        let docs = sample.as_array().cloned().unwrap_or_default();
        Ok(union_columns(&declared, &docs))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count() FROM {}{} GROUP ALL", ident(&table.name), where_clause(filters));
        let r = self.one(&sql).await?;
        Ok(r.as_array().and_then(|a| a.first()).and_then(|f| f.get("count")).and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let limit = query.limit.min(MAX_PAGE_ROWS);
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {limit} START {}",
            ident(&table.name),
            where_clause(&query.filters),
            order_clause(&query.sort),
            query.offset
        );
        let r = self.one(&sql).await?;
        let docs = r.as_array().cloned().unwrap_or_default();
        Ok(docs_to_result_set(&cols, &docs, false))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let script = sql.trim();
        if script.is_empty() {
            return Err(AppError::invalid_input("Empty SurrealQL script."));
        }
        let statements = split_statements(script);
        if self.read_only {
            if let Some(w) = statements.iter().find(|s| is_write_statement(s)) {
                return Err(AppError::read_only(format!(
                    "This connection is read-only; `{}` statements are blocked.",
                    first_word(w)
                )));
            }
        }
        let items = self.sql(script).await?;
        let mut out = Vec::with_capacity(items.len());
        for (i, item) in items.iter().enumerate() {
            let status = item.get("status").and_then(Json::as_str).unwrap_or("ERR");
            let result = item.get("result").cloned().unwrap_or(Json::Null);
            let stmt = statements.get(i).map(String::as_str).unwrap_or("");
            out.push(statement_to_result(status, &result, stmt, max_rows.max(1))?);
        }
        Ok(out)
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    #[test]
    fn ns_db_splitting() {
        assert_eq!(split_ns_db(None), ("test".into(), "test".into()));
        assert_eq!(split_ns_db(Some("prod/app")), ("prod".into(), "app".into()));
        assert_eq!(split_ns_db(Some("app")), ("test".into(), "app".into()));
        assert_eq!(split_ns_db(Some("/app")), ("test".into(), "app".into()));
    }

    #[test]
    fn where_and_order_render() {
        let w = where_clause(&[
            rule("age", FilterOp::Gte, "5"),
            rule("name", FilterOp::Contains, "o'b"),
            rule("tier", FilterOp::In, "gold, 2"),
            rule("id", FilterOp::Eq, "person:tobie"),
            rule("note", FilterOp::IsNull, ""),
        ]);
        assert_eq!(
            w,
            " WHERE `age` >= 5 AND string::contains(<string> `name`, 'o\\'b') AND `tier` INSIDE ['gold', 2] AND `id` = person:tobie AND (`note` = NONE OR `note` = NULL)"
        );
        assert_eq!(order_clause(&[SortRule { column: "a".into(), desc: true }]), " ORDER BY `a` DESC");
        assert_eq!(literal("id", "weird id"), "'weird id'");
    }

    #[test]
    fn statements_split_and_writes_detected() {
        let parts = split_statements("SELECT * FROM a; CREATE b SET s = 'x;y'; DEFINE TABLE c");
        assert_eq!(parts.len(), 3);
        assert!(is_write_statement("  create b"));
        assert!(is_write_statement("DEFINE TABLE x"));
        assert!(!is_write_statement("SELECT * FROM a"));
        assert!(!is_write_statement("INFO FOR DB"));
    }

    #[test]
    fn response_entries_map_to_results() {
        let ok = statement_to_result("OK", &json!([{"id": "a:1", "n": 1}]), "SELECT * FROM a", 10).unwrap();
        match ok {
            StatementResult::Rows { result } => {
                assert_eq!(result.columns[0].name, "id");
                assert_eq!(result.rows[0][0], Value::Text("a:1".into()));
            }
            _ => panic!("rows expected"),
        }
        let aff = statement_to_result("OK", &json!([{"id": "a:1"}, {"id": "a:2"}]), "CREATE a", 10).unwrap();
        assert!(matches!(aff, StatementResult::Affected { rows_affected: 2 }));
        let def = statement_to_result("OK", &Json::Null, "DEFINE TABLE a", 10).unwrap();
        assert!(matches!(def, StatementResult::Affected { rows_affected: 0 }));
        assert!(statement_to_result("ERR", &json!("Parse error"), "SELEC", 10).is_err());
        let scalar = statement_to_result("OK", &json!(3), "RETURN 3", 10).unwrap();
        assert!(matches!(scalar, StatementResult::Rows { .. }));
    }

    #[test]
    fn columns_union_declared_and_sampled() {
        let cols = union_columns(&["name".into(), "tags[*]".into()], &[json!({"id": "t:1", "age": 3}), json!({"id": "t:2", "extra": true})]);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "name", "age", "extra"]);
        assert!(cols[0].primary_key);
        let info = json!({"tables": {"b": "DEFINE TABLE b", "a": "DEFINE TABLE a"}});
        assert_eq!(info_keys(&info, "tables"), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        use crate::model::{ConnectionSummary, Environment};
        let Ok(url) = std::env::var("DBFREE_TEST_SURREALDB_URL") else { return };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Surrealdb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: std::env::var("DBFREE_TEST_SURREALDB_DB").ok(),
                username: std::env::var("DBFREE_TEST_SURREALDB_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_SURREALDB_PASSWORD").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.to_lowercase().contains("surreal"), "{version}");
        db.execute("CREATE dbfree_smoke:one SET n = 1; CREATE dbfree_smoke:two SET n = 2", 10).await.unwrap_or_else(|e| panic!("create: {e}"));
        let cat = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == "dbfree_smoke"));
        let t = TableRef { schema: None, name: "dbfree_smoke".into() };
        let cols = db.columns(&t).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "n"));
        let page = db
            .fetch_page(&t, &PageQuery { sort: vec![SortRule { column: "n".into(), desc: true }], filters: vec![rule("n", FilterOp::Gte, "1")], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(db.count(&t, &[rule("n", FilterOp::Eq, "2")]).await.unwrap_or_default(), 1);
        db.execute("REMOVE TABLE dbfree_smoke", 10).await.unwrap_or_else(|e| panic!("remove: {e}"));
    }
}
