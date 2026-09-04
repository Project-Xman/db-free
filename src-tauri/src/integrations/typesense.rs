// SOT: typesense-integration, typesense-rest-api, typesense-filter-by, typesense-console

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, local, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SortRule, StatementResult, TableInfo, TableKind, TableRef,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Typesense adapter (REST, port 8108, `X-TYPESENSE-API-KEY`).
//        A collection is a table; its declared `fields` are the columns
//        (`id` pinned as the primary key); a document is a row.
// WHY:   Typesense has a real schema per collection and a `filter_by` /
//        `sort_by` search API, so most grid operations run server-side.
//        Contains / StartsWith / EndsWith have no filter form and run locally
//        on a capped window.
// HOW:   Pages come from `GET /collections/{c}/documents/search?q=*` with
//        `page` / `per_page`; `found` is the count. `execute` accepts JSON
//        `{"collection": "c", "search": {…}}`, `SEARCH <collection> <text>`,
//        `COLLECTIONS`, and `{"method","path","body"}` / `GET /path`
//        passthrough. Imports and deletes are refused when read-only.
// WHERE: src-tauri/src/integrations/mod.rs (trait), integrations/http.rs (client)
// ============================================================================

const DEFAULT_PORT: u16 = 8108;
const SCHEMA: &str = "collections";
const LOCAL_CAP: u32 = 5_000;
const PAGE_MAX: u32 = 250;

pub struct TypesenseIntegration {
    http: HttpClient,
    read_only: bool,
    default_collection: Option<String>,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let key = conn.secret.as_deref().map(str::trim).filter(|s| !s.is_empty()).unwrap_or_default();
    let auth = if key.is_empty() { Auth::None } else { HttpClient::header("X-TYPESENSE-API-KEY", key) };
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, auth)?;
    let default_collection = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let integration = TypesenseIntegration { http, read_only: conn.summary.read_only, default_collection };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Field {
    name: String,
    type_name: String,
}

fn fields_of(collection: &Json) -> Vec<Field> {
    collection
        .get("fields")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|f| {
            let name = f.get("name").and_then(Json::as_str)?;
            if name.contains('*') {
                return None;
            }
            Some(Field { name: name.to_string(), type_name: f.get("type").and_then(Json::as_str).unwrap_or("auto").to_string() })
        })
        .collect()
}

fn columns_of(collection: &Json) -> Vec<ColumnInfo> {
    let mut cols = vec![ColumnInfo { name: "id".into(), data_type: "string".into(), nullable: false, primary_key: true, ordinal: 1 }];
    for f in fields_of(collection) {
        if f.name == "id" {
            continue;
        }
        let ordinal = u32::try_from(cols.len() + 1).unwrap_or(u32::MAX);
        cols.push(ColumnInfo { name: f.name, data_type: f.type_name, nullable: true, primary_key: false, ordinal });
    }
    cols
}

/// First string field, which `query_by` needs even for `q=*`.
fn query_by(fields: &[Field]) -> String {
    fields.iter().find(|f| f.type_name == "string" || f.type_name == "string[]").map(|f| f.name.clone()).unwrap_or_else(|| "id".to_string())
}

// ---------------------------------------------------------------------------
// filter_by / sort_by
// ---------------------------------------------------------------------------

fn is_numeric(fields: &[Field], name: &str) -> bool {
    fields.iter().any(|f| f.name == name && matches!(f.type_name.as_str(), "int32" | "int64" | "float" | "int32[]" | "int64[]" | "float[]" | "bool"))
}

fn filter_value(fields: &[Field], name: &str, raw: &str) -> String {
    let t = raw.trim();
    if is_numeric(fields, name) || t.parse::<f64>().is_ok() || t == "true" || t == "false" {
        t.to_string()
    } else {
        format!("`{}`", t.replace('`', ""))
    }
}

fn filter_expr(fields: &[Field], rule: &FilterRule) -> Option<String> {
    let f = rule.column.as_str();
    let v = || filter_value(fields, f, &rule.value);
    Some(match rule.op {
        FilterOp::Eq => format!("{f}:={}", v()),
        FilterOp::Ne => format!("{f}:!={}", v()),
        FilterOp::Gt => format!("{f}:>{}", v()),
        FilterOp::Gte => format!("{f}:>={}", v()),
        FilterOp::Lt => format!("{f}:<{}", v()),
        FilterOp::Lte => format!("{f}:<={}", v()),
        FilterOp::In => {
            let items: Vec<String> = rule.value.split(',').map(str::trim).filter(|s| !s.is_empty()).map(|s| filter_value(fields, f, s)).collect();
            format!("{f}:=[{}]", items.join(","))
        }
        FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith | FilterOp::IsNull | FilterOp::IsNotNull => return None,
    })
}

fn split_filters(fields: &[Field], filters: &[FilterRule]) -> (Option<String>, Vec<FilterRule>) {
    let mut server = Vec::new();
    let mut local_rules = Vec::new();
    for rule in filters {
        match filter_expr(fields, rule) {
            Some(e) => server.push(e),
            None => local_rules.push(rule.clone()),
        }
    }
    (if server.is_empty() { None } else { Some(server.join(" && ")) }, local_rules)
}

fn sort_by(sort: &[SortRule]) -> Option<String> {
    if sort.is_empty() {
        return None;
    }
    Some(sort.iter().take(3).map(|s| format!("{}:{}", s.column, if s.desc { "desc" } else { "asc" })).collect::<Vec<_>>().join(","))
}

fn encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn search_path(collection: &str, params: &[(&str, String)]) -> String {
    let qs: Vec<String> = params.iter().map(|(k, v)| format!("{k}={}", encode(v))).collect();
    format!("/collections/{}/documents/search?{}", encode(collection), qs.join("&"))
}

fn hit_documents(body: &Json) -> Vec<Json> {
    body.get("hits").and_then(Json::as_array).into_iter().flatten().filter_map(|h| h.get("document").cloned()).collect()
}

fn search_result(body: &Json, max_rows: usize) -> ResultSet {
    match body.get("hits").and_then(Json::as_array) {
        Some(_) => objects_to_result_set(&hit_documents(body), Some("id"), max_rows),
        None => json_result(body.clone()),
    }
}

// ---------------------------------------------------------------------------
// Console commands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Command {
    Search { collection: String, params: Json },
    Import { collection: String, documents: Vec<Json>, action: String },
    Rest { method: String, path: String, body: Option<Json> },
    Collections,
}

fn parse_command(text: &str, default_collection: Option<&str>) -> AppResult<Command> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if trimmed.starts_with('{') {
        let mut body: Json = serde_json::from_str(trimmed).map_err(|e| AppError::invalid_input(format!("Command is not valid JSON: {e}")))?;
        let obj = body.as_object_mut().ok_or_else(|| AppError::invalid_input("Command must be a JSON object."))?;
        if let Some(method) = obj.get("method").and_then(Json::as_str).map(str::to_ascii_uppercase) {
            let path = obj.get("path").and_then(Json::as_str).ok_or_else(|| AppError::invalid_input("Passthrough needs \"path\"."))?.to_string();
            let body = obj.get("body").cloned().filter(|b| !b.is_null());
            return Ok(Command::Rest { method, path, body });
        }
        let collection = obj
            .remove("collection")
            .and_then(|v| v.as_str().map(str::to_string))
            .or_else(|| default_collection.map(str::to_string))
            .ok_or_else(|| AppError::invalid_input("Add a \"collection\" key (or set the connection's collection)."))?;
        if let Some(docs) = obj.remove("documents").or_else(|| obj.remove("import")) {
            let documents = docs.as_array().cloned().ok_or_else(|| AppError::invalid_input("\"documents\" must be an array of objects."))?;
            let action = obj.remove("action").and_then(|v| v.as_str().map(str::to_string)).unwrap_or_else(|| "upsert".into());
            return Ok(Command::Import { collection, documents, action });
        }
        let params = obj.remove("search").unwrap_or_else(|| Json::Object(std::mem::take(obj)));
        return Ok(Command::Search { collection, params });
    }
    let mut parts = trimmed.splitn(3, char::is_whitespace);
    let verb = parts.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "COLLECTIONS" => Ok(Command::Collections),
        "SEARCH" => {
            let collection = parts.next().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| AppError::invalid_input("Usage: SEARCH <collection> <query text>"))?;
            let q = parts.next().map(str::trim).unwrap_or("*");
            Ok(Command::Search { collection: collection.to_string(), params: json!({"q": q}) })
        }
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" => {
            let path = parts.next().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| AppError::invalid_input("Expected a path, e.g. `GET /collections`."))?;
            let body = match parts.next().map(str::trim).filter(|s| !s.is_empty()) {
                Some(raw) => Some(serde_json::from_str::<Json>(raw).map_err(|e| AppError::invalid_input(format!("Body is not valid JSON: {e}")))?),
                None => None,
            };
            let path = if path.starts_with('/') { path.to_string() } else { format!("/{path}") };
            Ok(Command::Rest { method: verb, path, body })
        }
        _ => Err(AppError::invalid_input("Enter JSON ({\"collection\": \"books\", \"search\": {\"q\": \"…\", \"query_by\": \"title\"}}), `SEARCH <collection> <text>`, `COLLECTIONS`, or `GET /path`.")),
    }
}

fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.trim().is_empty() {
                out.push(std::mem::take(&mut current));
            }
        } else {
            current.push_str(line);
            current.push('\n');
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

fn is_read_request(method: &str, path: &str) -> bool {
    method == "GET" || (method == "POST" && (path.contains("/documents/search") || path.contains("/multi_search")))
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl TypesenseIntegration {
    async fn collection(&self, name: &str) -> AppResult<Json> {
        self.http.get_json(&format!("/collections/{}", encode(name))).await
    }

    async fn search(&self, collection: &str, params: &[(&str, String)]) -> AppResult<Json> {
        self.http.get_json(&search_path(collection, params)).await
    }

    fn refuse_if_read_only(&self, what: &str) -> AppResult<()> {
        if self.read_only {
            return Err(AppError::invalid_input(format!("{what} is refused: this connection is read-only.")));
        }
        Ok(())
    }

    async fn run_command(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        match cmd {
            Command::Collections => {
                let out: Json = self.http.get_json("/collections").await?;
                Ok(StatementResult::Rows { result: json_result(out) })
            }
            Command::Search { collection, params } => {
                let coll = self.collection(&collection).await?;
                let fields = fields_of(&coll);
                let mut query: Vec<(&str, String)> = Vec::new();
                let obj = params.as_object().cloned().unwrap_or_default();
                let mut has_query_by = false;
                let mut has_per_page = false;
                let mut owned: Vec<(String, String)> = Vec::new();
                for (k, v) in obj {
                    let text = match v {
                        Json::String(s) => s,
                        other => other.to_string(),
                    };
                    has_query_by |= k == "query_by";
                    has_per_page |= k == "per_page";
                    owned.push((k, text));
                }
                for (k, v) in &owned {
                    query.push((k.as_str(), v.clone()));
                }
                if !query.iter().any(|(k, _)| *k == "q") {
                    query.push(("q", "*".into()));
                }
                if !has_query_by {
                    query.push(("query_by", query_by(&fields)));
                }
                if !has_per_page {
                    query.push(("per_page", (max_rows as u32).min(PAGE_MAX).to_string()));
                }
                let out = self.search(&collection, &query).await?;
                Ok(StatementResult::Rows { result: search_result(&out, max_rows) })
            }
            Command::Import { collection, documents, action } => {
                self.refuse_if_read_only("Importing documents")?;
                let lines: Vec<String> = documents.iter().map(Json::to_string).collect();
                let path = format!("/collections/{}/documents/import?action={}", encode(&collection), encode(&action));
                let text = self.http.post_raw(&path, "text/plain", lines.join("\n"), None).await?;
                let ok = text.lines().filter(|l| serde_json::from_str::<Json>(l).ok().and_then(|v| v.get("success").and_then(Json::as_bool)).unwrap_or(false)).count() as u64;
                if ok < documents.len() as u64 {
                    let first_err = text.lines().find(|l| l.contains("\"success\":false")).unwrap_or_default().to_string();
                    return Err(AppError::driver(format!("{} of {} documents failed: {first_err}", documents.len() as u64 - ok, documents.len())));
                }
                Ok(StatementResult::Affected { rows_affected: ok })
            }
            Command::Rest { method, path, body } => {
                if !is_read_request(&method, &path) {
                    self.refuse_if_read_only(&format!("{method} {path}"))?;
                }
                let m = Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Unsupported verb {method}.")))?;
                let mut req = self.http.request(m, &path);
                if let Some(b) = body {
                    req = req.json(&b);
                }
                let resp = self.http.send(req).await?;
                let text = resp.text().await.map_err(|e| AppError::driver(e.to_string()))?;
                let parsed: Json = serde_json::from_str(&text).unwrap_or(Json::String(text));
                if parsed.get("hits").map(Json::is_array).unwrap_or(false) {
                    return Ok(StatementResult::Rows { result: search_result(&parsed, max_rows) });
                }
                if method == "DELETE" {
                    if let Some(n) = parsed.get("num_deleted").and_then(Json::as_u64) {
                        return Ok(StatementResult::Affected { rows_affected: n });
                    }
                }
                Ok(StatementResult::Rows { result: json_result(parsed) })
            }
        }
    }
}

#[async_trait]
impl Integration for TypesenseIntegration {
    fn engine(&self) -> Engine {
        Engine::Typesense
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: false, fixed_columns: true, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true }
    }

    async fn ping(&self) -> AppResult<()> {
        let health: Json = self.http.get_json("/health").await?;
        if health.get("ok").and_then(Json::as_bool) == Some(false) {
            return Err(AppError::not_connected("Typesense reports it is not healthy."));
        }
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let debug: Json = self.http.get_json("/debug").await?;
        Ok(debug.get("version").and_then(Json::as_str).map(|v| format!("Typesense {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(SCHEMA.into())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![SCHEMA.into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let out: Vec<Json> = self.http.get_json("/collections").await?;
        let tables = out
            .iter()
            .filter_map(|c| {
                let name = c.get("name").and_then(Json::as_str)?;
                Some(TableInfo { schema: Some(SCHEMA.into()), name: name.to_string(), kind: TableKind::Table, row_estimate: c.get("num_documents").and_then(Json::as_i64) })
            })
            .collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: SCHEMA.into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let coll = self.collection(&table.name).await?;
        Ok(columns_of(&coll))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let coll = self.collection(&table.name).await?;
        Ok(coll.get("num_documents").and_then(Json::as_i64))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let coll = self.collection(&table.name).await?;
        let fields = fields_of(&coll);
        let (server, local_rules) = split_filters(&fields, filters);
        if server.is_none() && local_rules.is_empty() {
            return Ok(coll.get("num_documents").and_then(Json::as_i64).unwrap_or(0));
        }
        let mut params: Vec<(&str, String)> = vec![("q", "*".into()), ("query_by", query_by(&fields))];
        if let Some(f) = &server {
            params.push(("filter_by", f.clone()));
        }
        if local_rules.is_empty() {
            params.push(("per_page", "0".into()));
            let out = self.search(&table.name, &params).await?;
            return Ok(out.get("found").and_then(Json::as_i64).unwrap_or(0));
        }
        let docs = self.window(&table.name, &params, LOCAL_CAP).await?;
        let set = objects_to_result_set(&docs, Some("id"), docs.len());
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        Ok(local::apply_filters(&names, set.rows, &local_rules).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let coll = self.collection(&table.name).await?;
        let fields = fields_of(&coll);
        let (server, local_rules) = split_filters(&fields, &query.filters);
        let mut params: Vec<(&str, String)> = vec![("q", "*".into()), ("query_by", query_by(&fields))];
        if let Some(f) = &server {
            params.push(("filter_by", f.clone()));
        }
        // `id` cannot be sorted on; everything else the server handles.
        let server_sort: Vec<SortRule> = query.sort.iter().filter(|s| s.column != "id").cloned().collect();
        let local_sort: Vec<SortRule> = query.sort.iter().filter(|s| s.column == "id").cloned().collect();
        if let Some(s) = sort_by(&server_sort) {
            params.push(("sort_by", s));
        }
        if local_rules.is_empty() && local_sort.is_empty() && query.limit <= PAGE_MAX && query.offset % u64::from(query.limit.max(1)) == 0 {
            let page = query.offset / u64::from(query.limit.max(1)) + 1;
            params.push(("per_page", query.limit.to_string()));
            params.push(("page", page.to_string()));
            let out = self.search(&table.name, &params).await?;
            let docs = hit_documents(&out);
            return Ok(objects_to_result_set(&docs, Some("id"), query.limit as usize));
        }
        let docs = self.window(&table.name, &params, LOCAL_CAP).await?;
        let mut set = objects_to_result_set(&docs, Some("id"), docs.len());
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery { sort: local_sort, filters: local_rules, offset: query.offset, limit: query.limit };
        set.rows = local::page(&names, set.rows, &local_query);
        set.truncated = false;
        Ok(set)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut results = Vec::new();
        for stmt in split_statements(sql) {
            let cmd = parse_command(&stmt, self.default_collection.as_deref())?;
            results.push(self.run_command(cmd, max_rows).await?);
        }
        Ok(results)
    }

    async fn close(&self) {}
}

impl TypesenseIntegration {
    /// Loads up to `cap` documents by walking pages of PAGE_MAX.
    async fn window(&self, collection: &str, params: &[(&str, String)], cap: u32) -> AppResult<Vec<Json>> {
        let mut docs = Vec::new();
        let mut page = 1u32;
        loop {
            let mut p: Vec<(&str, String)> = params.to_vec();
            p.push(("per_page", PAGE_MAX.to_string()));
            p.push(("page", page.to_string()));
            let out = self.search(collection, &p).await?;
            let batch = hit_documents(&out);
            let n = batch.len();
            docs.extend(batch);
            if n < PAGE_MAX as usize || docs.len() as u32 >= cap {
                break;
            }
            page += 1;
        }
        Ok(docs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode, Value};

    fn fields() -> Vec<Field> {
        vec![
            Field { name: "id".into(), type_name: "string".into() },
            Field { name: "title".into(), type_name: "string".into() },
            Field { name: "year".into(), type_name: "int32".into() },
        ]
    }

    #[test]
    fn filter_by_syntax() {
        let f = |c: &str, op, v: &str| FilterRule { column: c.into(), op, value: v.into() };
        let fs = fields();
        assert_eq!(filter_expr(&fs, &f("title", FilterOp::Eq, "Dune")).as_deref(), Some("title:=`Dune`"));
        assert_eq!(filter_expr(&fs, &f("year", FilterOp::Gt, "1990")).as_deref(), Some("year:>1990"));
        assert_eq!(filter_expr(&fs, &f("year", FilterOp::Ne, "1")).as_deref(), Some("year:!=1"));
        assert_eq!(filter_expr(&fs, &f("year", FilterOp::In, "1,2, 3")).as_deref(), Some("year:=[1,2,3]"));
        assert_eq!(filter_expr(&fs, &f("title", FilterOp::In, "a,b")).as_deref(), Some("title:=[`a`,`b`]"));
        assert!(filter_expr(&fs, &f("title", FilterOp::Contains, "x")).is_none());
        let (server, local_rules) = split_filters(&fs, &[f("year", FilterOp::Gte, "1"), f("title", FilterOp::StartsWith, "D"), f("year", FilterOp::Lt, "9")]);
        assert_eq!(server.as_deref(), Some("year:>=1 && year:<9"));
        assert_eq!(local_rules.len(), 1);
        assert_eq!(sort_by(&[SortRule { column: "year".into(), desc: true }]).as_deref(), Some("year:desc"));
        assert!(sort_by(&[]).is_none());
        assert_eq!(query_by(&fs), "id");
        assert_eq!(query_by(&fs[1..]), "title");
    }

    #[test]
    fn search_url_is_encoded() {
        let path = search_path("books", &[("q", "*".into()), ("filter_by", "year:>1990 && title:=`a b`".into())]);
        assert!(path.starts_with("/collections/books/documents/search?q=%2A&filter_by=year%3A%3E1990"));
        assert!(!path.contains(' '));
    }

    #[test]
    fn columns_from_schema() {
        let coll = json!({"name": "books", "num_documents": 3, "fields": [
            {"name": "title", "type": "string"}, {"name": "year", "type": "int32"}, {"name": ".*", "type": "auto"}
        ]});
        let cols = columns_of(&coll);
        assert_eq!(cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id", "title", "year"]);
        assert!(cols[0].primary_key);
        assert_eq!(cols[2].data_type, "int32");
    }

    #[test]
    fn hits_become_rows() {
        let body = json!({"found": 2, "hits": [{"document": {"id": "1", "title": "A"}}, {"document": {"id": "2", "title": "B", "year": 2000}}]});
        let set = search_result(&body, 10);
        assert_eq!(set.columns[0].name, "id");
        assert_eq!(set.rows.len(), 2);
        assert_eq!(set.rows[1][2], Value::Int(2000));
        assert_eq!(search_result(&json!({"num_documents": 1}), 10).columns[0].name, "result");
    }

    #[test]
    fn command_parsing() {
        assert_eq!(parse_command("collections", None).ok(), Some(Command::Collections));
        assert_eq!(parse_command("SEARCH books dune", None).ok(), Some(Command::Search { collection: "books".into(), params: json!({"q": "dune"}) }));
        assert_eq!(parse_command("SEARCH books", None).ok(), Some(Command::Search { collection: "books".into(), params: json!({"q": "*"}) }));
        assert_eq!(
            parse_command("{\"collection\":\"b\",\"search\":{\"q\":\"x\",\"query_by\":\"title\"}}", None).ok(),
            Some(Command::Search { collection: "b".into(), params: json!({"q": "x", "query_by": "title"}) })
        );
        assert_eq!(parse_command("{\"q\":\"x\"}", Some("d")).ok(), Some(Command::Search { collection: "d".into(), params: json!({"q": "x"}) }));
        assert!(parse_command("{\"q\":\"x\"}", None).is_err());
        assert_eq!(
            parse_command("{\"collection\":\"b\",\"documents\":[{\"id\":\"1\"}]}", None).ok(),
            Some(Command::Import { collection: "b".into(), documents: vec![json!({"id": "1"})], action: "upsert".into() })
        );
        assert_eq!(parse_command("DELETE /collections/b", None).ok(), Some(Command::Rest { method: "DELETE".into(), path: "/collections/b".into(), body: None }));
        assert_eq!(
            parse_command("{\"method\":\"post\",\"path\":\"/collections\",\"body\":{\"name\":\"x\"}}", None).ok(),
            Some(Command::Rest { method: "POST".into(), path: "/collections".into(), body: Some(json!({"name": "x"})) })
        );
        assert!(is_read_request("GET", "/collections"));
        assert!(is_read_request("POST", "/collections/b/documents/search"));
        assert!(!is_read_request("POST", "/collections/b/documents/import"));
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_TYPESENSE_URL is set.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_TYPESENSE_URL") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Typesense,
            environment: Environment::Local,
            read_only: false,
            host: Some(url),
            port: None,
            database: None,
            username: None,
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let secret = std::env::var("DBFREE_TEST_TYPESENSE_KEY").ok();
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, secret.is_some()), secret };
        let t = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        assert!(t.server_version().await.unwrap_or_default().unwrap_or_default().starts_with("Typesense"));
        t.execute("DELETE /collections/dbfree_t", 10).await.ok();
        t.execute("POST /collections {\"name\":\"dbfree_t\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"n\",\"type\":\"int32\"}]}", 10).await.unwrap_or_else(|e| panic!("create: {e}"));
        let out = t
            .execute("{\"collection\":\"dbfree_t\",\"documents\":[{\"id\":\"1\",\"name\":\"alpha\",\"n\":1},{\"id\":\"2\",\"name\":\"beta\",\"n\":2},{\"id\":\"3\",\"name\":\"gamma\",\"n\":3}]}", 10)
            .await
            .unwrap_or_else(|e| panic!("import: {e}"));
        assert!(matches!(out[0], StatementResult::Affected { rows_affected: 3 }), "{out:?}");
        let table = TableRef { schema: Some(SCHEMA.into()), name: "dbfree_t".into() };
        let cols = t.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(cols.len(), 3, "{cols:?}");
        let q = PageQuery {
            sort: vec![SortRule { column: "n".into(), desc: true }],
            filters: vec![FilterRule { column: "n".into(), op: FilterOp::Gte, value: "2".into() }],
            offset: 0,
            limit: 10,
        };
        let page = t.fetch_page(&table, &q).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2, "{page:?}");
        assert_eq!(page.rows[0][0], Value::Text("3".into()));
        assert_eq!(t.count(&table, &q.filters).await.unwrap_or_default(), 2);
        let q2 = PageQuery { sort: vec![], filters: vec![FilterRule { column: "name".into(), op: FilterOp::Contains, value: "amm".into() }], offset: 0, limit: 10 };
        assert_eq!(t.fetch_page(&table, &q2).await.unwrap_or_else(|e| panic!("page2: {e}")).rows.len(), 1);
        let out = t.execute("SEARCH dbfree_t beta", 10).await.unwrap_or_else(|e| panic!("search: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 1),
            other => panic!("unexpected {other:?}"),
        }
        t.execute("DELETE /collections/dbfree_t", 10).await.ok();
    }
}
