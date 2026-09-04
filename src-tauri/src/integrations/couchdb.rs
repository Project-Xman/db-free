// SOT: couchdb-integration, couch-http-api, mango-query, couch-design-views

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, json_to_value, json_type_name, local, objects_to_result_set, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SortRule, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use std::sync::Arc;

// ============================================================================
// WHAT:  Apache CouchDB adapter over its HTTP API (port 5984, Basic auth).
//        Schema = database, table `documents` = every document, plus one View
//        per design-document view (`design/view`). Pages go through Mango
//        (`_find`), falling back to `_all_docs` + client-side paging when the
//        requested sort has no usable index.
// WHY:   CouchDB is fully REST; no crate needed beyond the shared HttpClient.
// HOW:   `execute` accepts a Mango JSON body, a `{"method","path","body"}`
//        passthrough, or the shorthands `GET <id>` / `ALL [n]`.
// WHERE: src-tauri/src/integrations/http.rs, src-tauri/src/integrations/mod.rs
// ============================================================================

const SAMPLE: usize = 50;
const SCAN_CAP: usize = 10_000;
const DOCUMENTS: &str = "documents";
const ID: &str = "_id";

pub struct CouchIntegration {
    engine: Engine,
    http: HttpClient,
    database: Option<String>,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = HttpClient::auth_from_connection(conn);
    let http = HttpClient::from_connection(conn, Some(5984), false, auth)?;
    let database = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let integration = CouchIntegration { engine: conn.summary.engine, http, database, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

pub fn encode_path_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn lenient_json(raw: &str) -> serde_json::Value {
    let t = raw.trim();
    if t.eq_ignore_ascii_case("true") {
        return serde_json::Value::Bool(true);
    }
    if t.eq_ignore_ascii_case("false") {
        return serde_json::Value::Bool(false);
    }
    if t.eq_ignore_ascii_case("null") {
        return serde_json::Value::Null;
    }
    if let Ok(i) = t.parse::<i64>() {
        return serde_json::Value::Number(i.into());
    }
    if let Ok(f) = t.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return serde_json::Value::Number(n);
        }
    }
    serde_json::Value::String(t.to_string())
}

fn escape_regex(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 4);
    for ch in raw.chars() {
        if "\\^$.|?*+()[]{}/".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

// WHAT:  One filter rule → one Mango selector fragment.
pub fn mango_predicate(rule: &FilterRule) -> serde_json::Value {
    let v = rule.value.trim();
    let body = match rule.op {
        FilterOp::Eq => serde_json::json!({"$eq": lenient_json(v)}),
        FilterOp::Ne => serde_json::json!({"$ne": lenient_json(v)}),
        FilterOp::Gt => serde_json::json!({"$gt": lenient_json(v)}),
        FilterOp::Gte => serde_json::json!({"$gte": lenient_json(v)}),
        FilterOp::Lt => serde_json::json!({"$lt": lenient_json(v)}),
        FilterOp::Lte => serde_json::json!({"$lte": lenient_json(v)}),
        FilterOp::Contains => serde_json::json!({"$regex": format!("(?i){}", escape_regex(v))}),
        FilterOp::StartsWith => serde_json::json!({"$regex": format!("(?i)^{}", escape_regex(v))}),
        FilterOp::EndsWith => serde_json::json!({"$regex": format!("(?i){}$", escape_regex(v))}),
        FilterOp::In => {
            let items: Vec<serde_json::Value> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(lenient_json).collect();
            serde_json::json!({"$in": items})
        }
        FilterOp::IsNull => serde_json::json!({"$exists": false}),
        FilterOp::IsNotNull => serde_json::json!({"$exists": true}),
    };
    serde_json::json!({ rule.column.clone(): body })
}

pub fn mango_selector(filters: &[FilterRule]) -> serde_json::Value {
    match filters {
        [] => serde_json::json!({ "_id": {"$gt": null} }),
        [one] => mango_predicate(one),
        many => serde_json::json!({"$and": many.iter().map(mango_predicate).collect::<Vec<_>>()}),
    }
}

pub fn mango_sort(sort: &[SortRule]) -> serde_json::Value {
    serde_json::Value::Array(
        sort.iter().map(|s| serde_json::json!({ s.column.clone(): if s.desc { "desc" } else { "asc" } })).collect(),
    )
}

pub fn mango_body(query: &PageQuery) -> serde_json::Value {
    let mut body = serde_json::json!({
        "selector": mango_selector(&query.filters),
        "skip": query.offset,
        "limit": query.limit,
    });
    if !query.sort.is_empty() {
        body["sort"] = mango_sort(&query.sort);
    }
    body
}

fn union_columns(docs: &[serde_json::Value]) -> Vec<ColumnInfo> {
    let rs = objects_to_result_set(docs, Some(ID), 0);
    rs.columns
        .into_iter()
        .enumerate()
        .map(|(i, c)| ColumnInfo { primary_key: c.name == ID, name: c.name, data_type: c.type_name, nullable: true, ordinal: i as u32 + 1 })
        .collect()
}

fn docs_to_rows(columns: &[ColumnInfo], docs: &[serde_json::Value]) -> ResultSet {
    let rows = docs
        .iter()
        .map(|d| columns.iter().map(|c| d.get(&c.name).map(json_to_value).unwrap_or(Value::Null)).collect())
        .collect();
    ResultSet { columns: columns.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect(), rows, truncated: false }
}

// WHAT:  `design/view` table names → (design doc, view name).
fn view_parts(name: &str) -> Option<(&str, &str)> {
    let (d, v) = name.split_once('/')?;
    if d.is_empty() || v.is_empty() || name == DOCUMENTS {
        None
    } else {
        Some((d, v))
    }
}

#[derive(Debug)]
enum Command {
    Mango(serde_json::Value),
    Passthrough { method: String, path: String, body: Option<serde_json::Value> },
    Get(String),
    All(usize),
}

fn parse_command(raw: &str) -> AppResult<Command> {
    let text = raw.trim();
    if text.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if text.starts_with('{') {
        let v: serde_json::Value = serde_json::from_str(text).map_err(|e| AppError::invalid_input(format!("Command is not valid JSON: {e}")))?;
        if let Some(path) = v.get("path").and_then(|p| p.as_str()) {
            let method = v.get("method").and_then(|m| m.as_str()).unwrap_or("GET").to_uppercase();
            return Ok(Command::Passthrough { method, path: path.to_string(), body: v.get("body").cloned() });
        }
        if v.get("selector").is_some() {
            return Ok(Command::Mango(v));
        }
        return Ok(Command::Mango(serde_json::json!({"selector": v, "limit": 100})));
    }
    let mut words = text.split_whitespace();
    let head = words.next().unwrap_or_default().to_uppercase();
    match head.as_str() {
        "GET" => {
            let id = words.next().ok_or_else(|| AppError::invalid_input("GET needs a document id."))?;
            Ok(Command::Get(id.to_string()))
        }
        "ALL" => Ok(Command::All(words.next().and_then(|n| n.parse().ok()).unwrap_or(100))),
        _ => Err(AppError::invalid_input(
            "Enter a Mango query ({\"selector\": {...}}), a {\"method\",\"path\",\"body\"} request, `GET <id>` or `ALL [n]`.",
        )),
    }
}

impl CouchIntegration {
    fn db(&self) -> AppResult<&str> {
        self.database.as_deref().ok_or_else(|| AppError::invalid_input("Select a CouchDB database first."))
    }

    fn db_for(&self, table: &TableRef) -> AppResult<String> {
        match table.schema.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(s) => Ok(s.to_string()),
            None => self.db().map(str::to_string),
        }
    }

    async fn all_docs(&self, db: &str, skip: u64, limit: usize) -> AppResult<Vec<serde_json::Value>> {
        let path = format!("/{}/_all_docs?include_docs=true&skip={skip}&limit={limit}", encode_path_segment(db));
        let resp: serde_json::Value = self.http.get_json(&path).await?;
        Ok(resp
            .get("rows")
            .and_then(|r| r.as_array())
            .map(|rows| rows.iter().filter_map(|r| r.get("doc").cloned()).filter(|d| !d.is_null()).collect())
            .unwrap_or_default())
    }

    async fn find(&self, db: &str, body: &serde_json::Value) -> AppResult<Vec<serde_json::Value>> {
        let resp: serde_json::Value = self.http.post_json(&format!("/{}/_find", encode_path_segment(db)), body).await?;
        Ok(resp.get("docs").and_then(|d| d.as_array()).cloned().unwrap_or_default())
    }

    async fn view_rows(&self, db: &str, design: &str, view: &str, skip: u64, limit: usize) -> AppResult<Vec<serde_json::Value>> {
        let path = format!(
            "/{}/_design/{}/_view/{}?include_docs=true&skip={skip}&limit={limit}",
            encode_path_segment(db),
            encode_path_segment(design),
            encode_path_segment(view)
        );
        let resp: serde_json::Value = self.http.get_json(&path).await?;
        Ok(resp.get("rows").and_then(|r| r.as_array()).cloned().unwrap_or_default())
    }

    fn is_view(&self, table: &TableRef) -> bool {
        view_parts(&table.name).is_some()
    }

    async fn sample(&self, table: &TableRef) -> AppResult<Vec<serde_json::Value>> {
        let db = self.db_for(table)?;
        match view_parts(&table.name) {
            Some((d, v)) => self.view_rows(&db, d, v, 0, SAMPLE).await,
            None => self.all_docs(&db, 0, SAMPLE).await,
        }
    }

    async fn request_json(&self, method: Method, path: &str, body: Option<serde_json::Value>) -> AppResult<serde_json::Value> {
        let mut req = self.http.request(method, path);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = self.http.send(req).await?;
        let text = resp.text().await.map_err(|e| AppError::driver(e.to_string()))?;
        Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)))
    }
}

#[async_trait]
impl Integration for CouchIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { views: true, ..Capabilities::DOCUMENT }
    }

    async fn ping(&self) -> AppResult<()> {
        let _: serde_json::Value = self.http.get_json("/").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v: serde_json::Value = self.http.get_json("/").await?;
        Ok(v.get("version").and_then(|s| s.as_str()).map(|s| format!("CouchDB {s}")))
    }

    fn current_database(&self) -> Option<String> {
        self.database.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let all: Vec<String> = self.http.get_json("/_all_dbs").await?;
        let keep = |n: &String| !n.starts_with('_') || self.database.as_deref() == Some(n.as_str());
        Ok(all.into_iter().filter(keep).collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let dbs = match &self.database {
            Some(d) => vec![d.clone()],
            None => self.databases().await?,
        };
        let mut schemas = Vec::new();
        for db in dbs {
            let info: serde_json::Value = self.http.get_json(&format!("/{}", encode_path_segment(&db))).await?;
            let doc_count = info.get("doc_count").and_then(|c| c.as_i64());
            let mut tables = vec![TableInfo { schema: Some(db.clone()), name: DOCUMENTS.into(), kind: TableKind::Table, row_estimate: doc_count }];
            let designs: serde_json::Value = self
                .http
                .get_json(&format!("/{}/_design_docs?include_docs=true", encode_path_segment(&db)))
                .await
                .unwrap_or(serde_json::Value::Null);
            for row in designs.get("rows").and_then(|r| r.as_array()).into_iter().flatten() {
                let Some(doc) = row.get("doc") else { continue };
                let id = doc.get(ID).and_then(|i| i.as_str()).unwrap_or_default();
                let design = id.trim_start_matches("_design/");
                for view in doc.get("views").and_then(|v| v.as_object()).into_iter().flat_map(|o| o.keys()) {
                    tables.push(TableInfo { schema: Some(db.clone()), name: format!("{design}/{view}"), kind: TableKind::View, row_estimate: None });
                }
            }
            schemas.push(SchemaInfo { name: db, tables });
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        if self.is_view(table) {
            let rows = self.sample(table).await?;
            let mut cols: Vec<ColumnInfo> = ["id", "key", "value"]
                .iter()
                .enumerate()
                .map(|(i, n)| ColumnInfo {
                    name: (*n).to_string(),
                    data_type: rows.iter().find_map(|r| r.get(*n).filter(|v| !v.is_null()).map(json_type_name)).unwrap_or("json").to_string(),
                    nullable: true,
                    primary_key: *n == "id",
                    ordinal: i as u32 + 1,
                })
                .collect();
            cols.push(ColumnInfo { name: "doc".into(), data_type: "object".into(), nullable: true, primary_key: false, ordinal: 4 });
            return Ok(cols);
        }
        let docs = self.sample(table).await?;
        let mut cols = union_columns(&docs);
        if cols.is_empty() {
            cols.push(ColumnInfo { name: ID.into(), data_type: "string".into(), nullable: false, primary_key: true, ordinal: 1 });
            cols.push(ColumnInfo { name: "_rev".into(), data_type: "string".into(), nullable: true, primary_key: false, ordinal: 2 });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        if self.is_view(table) {
            return Ok(None);
        }
        let db = self.db_for(table)?;
        let info: serde_json::Value = self.http.get_json(&format!("/{}", encode_path_segment(&db))).await?;
        Ok(info.get("doc_count").and_then(|c| c.as_i64()))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let db = self.db_for(table)?;
        if let Some((d, v)) = view_parts(&table.name) {
            let path = format!("/{}/_design/{}/_view/{}?limit=0", encode_path_segment(&db), encode_path_segment(d), encode_path_segment(v));
            let resp: serde_json::Value = self.http.get_json(&path).await?;
            return Ok(resp.get("total_rows").and_then(|t| t.as_i64()).unwrap_or(0));
        }
        if filters.is_empty() {
            return Ok(self.row_estimate(table).await?.unwrap_or(0));
        }
        let mut total = 0i64;
        let mut skip = 0usize;
        loop {
            let body = serde_json::json!({"selector": mango_selector(filters), "fields": [ID], "skip": skip, "limit": 1000});
            let n = self.find(&db, &body).await?.len();
            total += n as i64;
            skip += n;
            if n < 1000 || skip >= SCAN_CAP {
                break;
            }
        }
        Ok(total)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let db = self.db_for(table)?;
        let columns = self.columns(table).await?;
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        if let Some((d, v)) = view_parts(&table.name) {
            let needs_local = !query.sort.is_empty() || !query.filters.is_empty();
            let (skip, limit) = if needs_local { (0, SCAN_CAP.min(query.offset as usize + query.limit as usize)) } else { (query.offset, query.limit as usize) };
            let rows = self.view_rows(&db, d, v, skip, limit).await?;
            let mut rs = docs_to_rows(&columns, &rows);
            if needs_local {
                rs.rows = local::page(&names, rs.rows, query);
            }
            return Ok(rs);
        }
        let docs = match self.find(&db, &mango_body(query)).await {
            Ok(docs) => docs,
            Err(e) if !query.sort.is_empty() && e.message().contains("no_usable_index") => {
                let cap = SCAN_CAP.min(query.offset as usize + query.limit as usize);
                let all = self.all_docs(&db, 0, cap).await?;
                let rs = docs_to_rows(&columns, &all);
                return Ok(ResultSet { rows: local::page(&names, rs.rows, query), ..rs });
            }
            Err(e) => return Err(e),
        };
        Ok(docs_to_rows(&columns, &docs))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut out = Vec::new();
        for stmt in split_blank_lines(sql) {
            let cmd = parse_command(&stmt)?;
            let result = match cmd {
                Command::Mango(mut body) => {
                    let db = self.db()?;
                    if body.get("limit").is_none() {
                        body["limit"] = serde_json::json!(max_rows);
                    }
                    let docs = self.find(db, &body).await?;
                    StatementResult::Rows { result: objects_to_result_set(&docs, Some(ID), max_rows) }
                }
                Command::Get(id) => {
                    let db = self.db()?;
                    let doc: serde_json::Value = self.http.get_json(&format!("/{}/{}", encode_path_segment(db), encode_path_segment(&id))).await?;
                    StatementResult::Rows { result: objects_to_result_set(std::slice::from_ref(&doc), Some(ID), max_rows) }
                }
                Command::All(n) => {
                    let db = self.db()?;
                    let docs = self.all_docs(db, 0, n.min(max_rows)).await?;
                    StatementResult::Rows { result: objects_to_result_set(&docs, Some(ID), max_rows) }
                }
                Command::Passthrough { method, path, body } => {
                    let m = Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Unknown HTTP method {method}.")))?;
                    let write = matches!(m, Method::PUT | Method::POST | Method::DELETE) && !path.contains("/_find") && !path.contains("/_all_docs");
                    if write && self.read_only {
                        return Err(AppError::read_only("This connection is read-only; PUT/POST/DELETE requests are blocked."));
                    }
                    let resp = self.request_json(m, &path, body).await?;
                    if write {
                        StatementResult::Affected { rows_affected: if resp.get("ok").and_then(|o| o.as_bool()) == Some(true) { 1 } else { 0 } }
                    } else {
                        let mut rs = json_result(resp);
                        rs.truncated = rs.rows.len() > max_rows;
                        rs.rows.truncate(max_rows);
                        StatementResult::Rows { result: rs }
                    }
                }
            };
            out.push(result);
        }
        Ok(out)
    }

    async fn close(&self) {}
}

pub fn split_blank_lines(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !cur.trim().is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            cur.clear();
        } else {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    #[test]
    fn selector_maps_operators() {
        let rules = vec![
            FilterRule { column: "age".into(), op: FilterOp::Gte, value: "18".into() },
            FilterRule { column: "name".into(), op: FilterOp::StartsWith, value: "a.b".into() },
            FilterRule { column: "x".into(), op: FilterOp::IsNull, value: String::new() },
            FilterRule { column: "t".into(), op: FilterOp::In, value: "a, 2".into() },
        ];
        let sel = mango_selector(&rules);
        assert_eq!(sel["$and"][0], serde_json::json!({"age": {"$gte": 18}}));
        assert_eq!(sel["$and"][1], serde_json::json!({"name": {"$regex": "(?i)^a\\.b"}}));
        assert_eq!(sel["$and"][2], serde_json::json!({"x": {"$exists": false}}));
        assert_eq!(sel["$and"][3], serde_json::json!({"t": {"$in": ["a", 2]}}));
        assert_eq!(mango_selector(&[]), serde_json::json!({"_id": {"$gt": null}}));
    }

    #[test]
    fn body_includes_sort_and_window() {
        let q = PageQuery { sort: vec![SortRule { column: "n".into(), desc: true }], filters: vec![], offset: 20, limit: 10 };
        let b = mango_body(&q);
        assert_eq!(b["skip"], 20);
        assert_eq!(b["limit"], 10);
        assert_eq!(b["sort"], serde_json::json!([{"n": "desc"}]));
    }

    #[test]
    fn commands_parse() {
        assert!(matches!(parse_command("GET abc"), Ok(Command::Get(id)) if id == "abc"));
        assert!(matches!(parse_command("all 5"), Ok(Command::All(5))));
        assert!(matches!(parse_command(r#"{"selector": {"a": 1}}"#), Ok(Command::Mango(_))));
        assert!(matches!(parse_command(r#"{"a": 1}"#), Ok(Command::Mango(v)) if v["selector"]["a"] == 1));
        assert!(matches!(parse_command(r#"{"method":"delete","path":"/db/x"}"#), Ok(Command::Passthrough { method, .. }) if method == "DELETE"));
        assert!(parse_command("SELECT 1").is_err());
    }

    #[test]
    fn view_names_and_encoding() {
        assert_eq!(view_parts("app/by_name"), Some(("app", "by_name")));
        assert_eq!(view_parts("documents"), None);
        assert_eq!(encode_path_segment("a b/c"), "a%20b%2Fc");
    }

    fn live_conn(url: String) -> ResolvedConnection {
        ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Couchdb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: Some("dbfree_test".into()),
                username: std::env::var("DBFREE_TEST_COUCHDB_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_COUCHDB_PASSWORD").ok(),
        }
    }

    // Runs only when DBFREE_TEST_COUCHDB_URL is set (with _USER and _PASSWORD):
    // `docker run --rm -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=pw couchdb:3`.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_COUCHDB_URL") else {
            return;
        };
        let resolved = live_conn(url);
        // The database must exist before `connect` can attach to it.
        let boot = HttpClient::from_connection(&resolved, Some(5984), false, HttpClient::auth_from_connection(&resolved))
            .unwrap_or_else(|e| panic!("client: {e}"));
        let _ = boot.send(boot.request(Method::PUT, "/dbfree_test")).await;
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.to_lowercase().contains("couchdb"), "{version}");
        db.execute(r#"{"method":"PUT","path":"/dbfree_test/doc1","body":{"city":"Berlin","n":1}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("put doc1: {e}"));
        db.execute(r#"{"method":"PUT","path":"/dbfree_test/doc2","body":{"city":"Paris","n":2}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("put doc2: {e}"));

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(catalog.schemas.iter().any(|s| s.tables.iter().any(|t| t.name == "documents")));
        let table = TableRef { schema: Some("dbfree_test".into()), name: "documents".into() };
        let cols = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "_id" && c.primary_key), "{cols:?}");
        assert!(cols.iter().any(|c| c.name == "city"), "{cols:?}");
        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert!(page.rows.len() >= 2, "{page:?}");
        assert!(db.count(&table, &[]).await.unwrap_or_default() >= 2);
        let found = db
            .execute(r#"{"selector":{"city":"Paris"}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("find: {e}"));
        match found.first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows.len(), 1, "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        let _ = db.execute(r#"{"method":"DELETE","path":"/dbfree_test"}"#, 10).await;
        db.close().await;
    }

}
