// SOT: typesense-integration, typesense-rest-api, typesense-filter-by, typesense-console, object-explorer, server-stats, search-playground, typesense-admin-actions

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, json_to_value, json_type_name, local, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FacetCounts, FacetValue, FilterOp, FilterRule, ObjectAction,
    ObjectDetail, ObjectKind, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SearchRequest, SearchResult, ServerStats, SortRule, Stat, StatGroup, StatementResult, TableInfo,
    TableKind, TableRef, Value,
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

// ---------------------------------------------------------------------------
// Object explorer / server stats / search playground
//
// WHAT:  `objects()` lists collections, aliases, per-collection synonyms and
//        curation rules, API keys and the node; `object_detail()` adds the
//        JSON definition, a property sheet and `DELETE /path` actions that run
//        back through this adapter's console; `server_stats()` folds
//        `/stats.json`, `/metrics.json` and `/health`; `search()` is the
//        playground over `GET /collections/{c}/documents/search`.
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const SCORE_FIELD: &str = "_text_match";
const HIGHLIGHT_FIELD: &str = "_highlight";

fn text_of(v: &Json) -> String {
    match v {
        Json::Null => String::new(),
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn str_at<'a>(v: &'a Json, key: &str) -> &'a str {
    v.get(key).and_then(Json::as_str).unwrap_or("")
}

fn str_list(v: Option<&Json>) -> Vec<String> {
    v.and_then(Json::as_array).map(|a| a.iter().map(text_of).filter(|s| !s.is_empty()).collect()).unwrap_or_default()
}

fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

fn human_bytes(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", value as u64)
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn bytes_stat(label: &str, bytes: f64) -> Stat {
    Stat { label: label.to_string(), value: human_bytes(bytes), unit: None, hint: None, numeric: Some(bytes) }
}

// WHAT:  `/metrics.json` and `/stats.json` report every figure as a string.
fn num_at(v: &Json, key: &str) -> Option<f64> {
    let node = v.get(key)?;
    node.as_f64().or_else(|| node.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
}

fn finish(mut list: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    list.sort_by(|a, b| a.reference.name.cmp(&b.reference.name).then_with(|| a.reference.parent.cmp(&b.reference.parent)));
    list.truncate(OBJECT_CAP);
    list
}

fn summary(kind: ObjectKind, name: &str, parent: Option<&str>, detail: String, badge: Option<String>) -> ObjectSummary {
    let mut s = ObjectSummary::new(kind, name, parent.map(str::to_string));
    if !detail.is_empty() {
        s = s.with_detail(detail);
    }
    if let Some(b) = badge.filter(|b| !b.is_empty()) {
        s = s.with_badge(b);
    }
    s
}

fn rows_table(columns: &[(&str, &str)], rows: Vec<Vec<Value>>) -> ResultSet {
    ResultSet {
        columns: columns.iter().map(|(name, ty)| ColumnMeta { name: (*name).to_string(), type_name: (*ty).to_string() }).collect(),
        rows,
        truncated: false,
    }
}

// WHAT:  Every string-ish field, which the playground needs for `query_by`.
fn query_by_all(fields: &[Field]) -> String {
    let names: Vec<&str> = fields.iter().filter(|f| f.type_name.starts_with("string") || f.type_name == "auto").map(|f| f.name.as_str()).collect();
    if names.is_empty() {
        query_by(fields)
    } else {
        names.join(",")
    }
}

fn collection_summaries(list: &[Json]) -> Vec<ObjectSummary> {
    let out = list
        .iter()
        .filter_map(|c| {
            let name = c.get("name").and_then(Json::as_str)?;
            let fields = fields_of(c).len();
            let mut parts = Vec::new();
            if let Some(n) = c.get("num_documents").and_then(Json::as_f64) {
                parts.push(format!("{} docs", crate::model::objects::format_number(n)));
            }
            parts.push(format!("{fields} fields"));
            let badge = c.get("default_sorting_field").and_then(Json::as_str).filter(|s| !s.is_empty()).map(|s| format!("sort {s}"));
            Some(summary(ObjectKind::Collection, name, None, parts.join(" · "), badge))
        })
        .collect();
    finish(out)
}

fn alias_summaries(body: &Json, parent: Option<&str>) -> Vec<ObjectSummary> {
    let out = body
        .get("aliases")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|a| {
            let name = a.get("name").and_then(Json::as_str)?;
            let target = str_at(a, "collection_name");
            if parent.is_some_and(|p| p != target) {
                return None;
            }
            Some(summary(ObjectKind::Alias, name, Some(target), format!("→ {target}"), None))
        })
        .collect();
    finish(out)
}

fn synonym_summaries(collection: &str, body: &Json) -> Vec<ObjectSummary> {
    body.get("synonyms")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|s| {
            let id = s.get("id").and_then(Json::as_str)?;
            let words = str_list(s.get("synonyms"));
            let root = str_at(s, "root");
            let detail = if root.is_empty() { words.join(", ") } else { format!("{root} → {}", words.join(", ")) };
            let badge = if root.is_empty() { "multi-way" } else { "one-way" };
            Some(summary(ObjectKind::Synonym, id, Some(collection), detail, Some(badge.to_string())))
        })
        .collect()
}

fn rule_summaries(collection: &str, body: &Json) -> Vec<ObjectSummary> {
    body.get("overrides")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|o| {
            let id = o.get("id").and_then(Json::as_str)?;
            let query = o.pointer("/rule/query").map(text_of).unwrap_or_default();
            let mut parts = Vec::new();
            if !query.is_empty() {
                parts.push(format!("q: {query}"));
            }
            let pins = o.get("includes").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let hides = o.get("excludes").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            if pins > 0 {
                parts.push(format!("{pins} pinned"));
            }
            if hides > 0 {
                parts.push(format!("{hides} hidden"));
            }
            let filter = str_at(o, "filter_by");
            if !filter.is_empty() {
                parts.push(filter.to_string());
            }
            let badge = o.pointer("/rule/match").map(text_of).filter(|m| !m.is_empty());
            Some(summary(ObjectKind::Rule, id, Some(collection), parts.join(" · "), badge))
        })
        .collect()
}

fn api_key_summaries(body: &Json) -> Vec<ObjectSummary> {
    let out = body
        .get("keys")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|k| {
            let id = k.get("id").map(text_of).filter(|i| !i.is_empty())?;
            let actions = str_list(k.get("actions"));
            let collections = str_list(k.get("collections"));
            let mut parts = Vec::new();
            let desc = str_at(k, "description");
            if !desc.is_empty() {
                parts.push(desc.to_string());
            }
            parts.push(actions.join(", "));
            if !collections.is_empty() {
                parts.push(format!("on {}", collections.join(", ")));
            }
            let prefix = str_at(k, "value_prefix");
            if !prefix.is_empty() {
                parts.push(format!("{prefix}…"));
            }
            let badge = actions.iter().any(|a| a == "*").then(|| "all actions".to_string());
            Some(summary(ObjectKind::ApiKey, &id, None, parts.join(" · "), badge))
        })
        .collect();
    finish(out)
}

// WHAT:  Typesense exposes one process per connection; `/debug` reports the raft
//        state (1 = leader, 4 = follower) and the build version.
fn node_state(debug: &Json) -> &'static str {
    match debug.get("state").and_then(Json::as_i64) {
        Some(1) => "leader",
        Some(4) => "follower",
        Some(_) => "voting",
        None => "single",
    }
}

fn node_summary(name: &str, debug: &Json, health: &Json, metrics: &Json) -> Vec<ObjectSummary> {
    let mut parts = Vec::new();
    let version = str_at(debug, "version");
    if !version.is_empty() {
        parts.push(format!("v{version}"));
    }
    parts.push(if health.get("ok").and_then(Json::as_bool) == Some(false) { "unhealthy".into() } else { "healthy".to_string() });
    if let Some(cpu) = num_at(metrics, "system_cpu_active_percentage") {
        parts.push(format!("cpu {cpu}%"));
    }
    if let Some(mem) = num_at(metrics, "system_memory_used_bytes") {
        parts.push(format!("mem {}", human_bytes(mem)));
    }
    vec![summary(ObjectKind::Node, name, None, parts.join(" · "), Some(node_state(debug).to_string()))]
}

// ---- search playground ------------------------------------------------------

// WHAT:  Playground request → the search endpoint's query string. `query_by`
//        covers every string field unless the filter already names one; paging
//        prefers `page`/`per_page` and falls back to `offset`/`limit` when the
//        offset is not a whole number of pages.
fn playground_params(req: &SearchRequest, fields: &[Field]) -> Vec<(String, String)> {
    let q = req.query.trim();
    let mut params: Vec<(String, String)> = vec![
        ("q".into(), if q.is_empty() { "*".into() } else { q.to_string() }),
        ("query_by".into(), query_by_all(fields)),
    ];
    if let Some(f) = req.filter.as_deref().map(str::trim).filter(|f| !f.is_empty()) {
        params.push(("filter_by".into(), f.to_string()));
    }
    let facets: Vec<&str> = req.facets.iter().map(|f| f.trim()).filter(|f| !f.is_empty()).collect();
    if !facets.is_empty() {
        params.push(("facet_by".into(), facets.join(",")));
        params.push(("max_facet_values".into(), "20".into()));
    }
    let sort: Vec<String> = req
        .sort
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| if s.contains(':') { s.to_string() } else { format!("{s}:asc") })
        .collect();
    if !sort.is_empty() {
        params.push(("sort_by".into(), sort.join(",")));
    }
    if req.highlight {
        params.push(("highlight_full_fields".into(), query_by_all(fields)));
    }
    let limit = req.limit.clamp(1, PAGE_MAX);
    if u64::from(req.offset) % u64::from(limit) == 0 {
        params.push(("per_page".into(), limit.to_string()));
        params.push(("page".into(), (u64::from(req.offset) / u64::from(limit) + 1).to_string()));
    } else {
        params.push(("limit".into(), limit.to_string()));
        params.push(("offset".into(), req.offset.to_string()));
    }
    params
}

// WHAT:  Search response → hits grid (`id`, `_text_match`, document fields,
//        `_highlight`), `facet_counts` → FacetCounts, `found`, `search_time_ms`.
fn playground_result(body: &Json, highlight: bool) -> SearchResult {
    let hits: Vec<&Json> = body.get("hits").and_then(Json::as_array).map(|h| h.iter().collect()).unwrap_or_default();
    let docs: Vec<Json> = hits.iter().map(|h| h.get("document").cloned().unwrap_or(Json::Null)).collect();
    let mut names: Vec<String> = vec!["id".to_string(), SCORE_FIELD.to_string()];
    for obj in docs.iter().filter_map(Json::as_object) {
        for k in obj.keys() {
            if !names.iter().any(|n| n == k) {
                names.push(k.clone());
            }
        }
    }
    if highlight {
        names.push(HIGHLIGHT_FIELD.to_string());
    }
    let rows: Vec<Vec<Value>> = hits
        .iter()
        .zip(&docs)
        .map(|(hit, doc)| {
            let obj = doc.as_object();
            names
                .iter()
                .map(|n| match n.as_str() {
                    SCORE_FIELD => hit.get("text_match").map(json_to_value).unwrap_or(Value::Null),
                    HIGHLIGHT_FIELD => hit
                        .get("highlight")
                        .or_else(|| hit.get("highlights"))
                        .filter(|h| !h.is_null() && h.as_object().map(|o| !o.is_empty()).unwrap_or(true))
                        .map(|h| Value::Json(h.clone()))
                        .unwrap_or(Value::Null),
                    other => obj.and_then(|o| o.get(other)).map(json_to_value).unwrap_or(Value::Null),
                })
                .collect()
        })
        .collect();
    let columns = names
        .iter()
        .map(|n| {
            let type_name = match n.as_str() {
                SCORE_FIELD => "integer",
                HIGHLIGHT_FIELD => "object",
                other => docs.iter().find_map(|d| d.get(other).filter(|v| !v.is_null()).map(json_type_name)).unwrap_or("json"),
            };
            ColumnMeta { name: n.clone(), type_name: type_name.to_string() }
        })
        .collect();
    let facets = body
        .get("facet_counts")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|f| {
            let field = f.get("field_name").and_then(Json::as_str)?;
            let values = f
                .get("counts")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
                .map(|c| FacetValue { value: text_of(c.get("value").unwrap_or(&Json::Null)), count: c.get("count").and_then(Json::as_u64).unwrap_or(0) })
                .collect();
            Some(FacetCounts { field: field.to_string(), values })
        })
        .collect();
    SearchResult {
        hits: ResultSet { columns, rows, truncated: false },
        total: body.get("found").and_then(Json::as_u64),
        facets,
        took_ms: body.get("search_time_ms").and_then(Json::as_u64),
    }
}

// ---- server stats -----------------------------------------------------------

fn stats_groups(stats: &Json, metrics: &Json, health: &Json, debug: &Json, collections: &[Json]) -> Vec<StatGroup> {
    let mut server = Vec::new();
    let version = str_at(debug, "version");
    if !version.is_empty() {
        server.push(Stat::text("Version", version));
    }
    server.push(Stat::text("Health", if health.get("ok").and_then(Json::as_bool) == Some(false) { "unhealthy" } else { "ok" }));
    server.push(Stat::text("State", node_state(debug)));
    let docs: f64 = collections.iter().filter_map(|c| c.get("num_documents").and_then(Json::as_f64)).sum();
    let storage = vec![
        Stat::number("Collections", collections.len() as f64, None),
        Stat::number("Documents", docs, None),
    ];
    let mut throughput = Vec::new();
    for (label, key) in [
        ("Requests/s", "total_requests_per_second"),
        ("Searches/s", "search_requests_per_second"),
        ("Writes/s", "write_requests_per_second"),
        ("Imports/s", "import_requests_per_second"),
    ] {
        if let Some(v) = num_at(stats, key) {
            throughput.push(Stat::number(label, v, Some("/s")));
        }
    }
    for (label, key) in [("Search latency", "search_latency_ms"), ("Write latency", "write_latency_ms"), ("Overall latency", "overall_latency_ms")] {
        if let Some(v) = stats.get("latency_ms").and_then(|l| num_at(l, key)).or_else(|| num_at(stats, key)) {
            throughput.push(Stat::number(label, v, Some("ms")));
        }
    }
    let mut system = Vec::new();
    if let Some(v) = num_at(metrics, "system_cpu_active_percentage") {
        system.push(Stat::number("CPU", v, Some("%")));
    }
    for (label, key) in [("Memory used", "system_memory_used_bytes"), ("Memory total", "system_memory_total_bytes"), ("Disk used", "system_disk_used_bytes"), ("Disk total", "system_disk_total_bytes"), ("Process memory", "typesense_memory_active_bytes")] {
        if let Some(v) = num_at(metrics, key) {
            system.push(bytes_stat(label, v));
        }
    }
    let used = num_at(metrics, "system_memory_used_bytes");
    let total = num_at(metrics, "system_memory_total_bytes");
    if let (Some(u), Some(t)) = (used, total) {
        if t > 0.0 {
            system.push(Stat::number("Memory", (u / t * 100.0).round(), Some("%")));
        }
    }
    let mut groups = vec![StatGroup { title: "Server".into(), stats: server }, StatGroup { title: "Storage".into(), stats: storage }];
    for (title, stats) in [("Throughput", throughput), ("System", system)] {
        if !stats.is_empty() {
            groups.push(StatGroup { title: title.into(), stats });
        }
    }
    groups
}

impl TypesenseIntegration {
    async fn collection_list(&self) -> AppResult<Vec<Json>> {
        self.http.get_json::<Vec<Json>>("/collections").await
    }

    async fn collection_names(&self, parent: Option<&str>) -> AppResult<Vec<String>> {
        match parent {
            Some(p) => Ok(vec![p.to_string()]),
            None => {
                let mut names: Vec<String> = self.collection_list().await?.iter().filter_map(|c| c.get("name").and_then(Json::as_str).map(str::to_string)).collect();
                names.sort();
                Ok(names)
            }
        }
    }

    async fn list_collections(&self) -> AppResult<Vec<ObjectSummary>> {
        Ok(collection_summaries(&self.collection_list().await?))
    }

    async fn list_aliases(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let body: Json = self.http.get_json("/aliases").await?;
        Ok(alias_summaries(&body, parent))
    }

    async fn list_synonyms(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.collection_names(parent).await? {
            let body: Json = self.http.get_json(&format!("/collections/{}/synonyms", encode(&name))).await?;
            list.extend(synonym_summaries(&name, &body));
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn list_rules(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.collection_names(parent).await? {
            let body: Json = self.http.get_json(&format!("/collections/{}/overrides", encode(&name))).await?;
            list.extend(rule_summaries(&name, &body));
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn list_keys(&self) -> AppResult<Vec<ObjectSummary>> {
        match self.http.get_json::<Json>("/keys").await {
            Ok(body) => Ok(api_key_summaries(&body)),
            Err(AppError::NotConnected { .. }) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    fn node_name(&self) -> String {
        self.http.base().trim_start_matches("https://").trim_start_matches("http://").to_string()
    }

    async fn list_nodes(&self) -> AppResult<Vec<ObjectSummary>> {
        let debug: Json = self.http.get_json("/debug").await.unwrap_or(Json::Null);
        let health: Json = self.http.get_json("/health").await.unwrap_or(Json::Null);
        let metrics: Json = self.http.get_json("/metrics.json").await.unwrap_or(Json::Null);
        Ok(node_summary(&self.node_name(), &debug, &health, &metrics))
    }

    async fn collection_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let p = encode(name);
        let coll = self.collection(name).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&coll), CodeLanguage::Json);
        for (label, key) in [("Documents", "num_documents"), ("Default sorting field", "default_sorting_field"), ("Memory shards", "num_memory_shards"), ("Created", "created_at"), ("Nested fields", "enable_nested_fields")] {
            let v = coll.get(key).map(text_of).unwrap_or_default();
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        detail = detail.property("Fields", fields_of(&coll).len().to_string());
        detail.columns = columns_of(&coll);
        let mut children = Vec::new();
        if let Ok(body) = self.http.get_json::<Json>("/aliases").await {
            children.extend(alias_summaries(&body, Some(name)));
        }
        if let Ok(body) = self.http.get_json::<Json>(&format!("/collections/{p}/synonyms")).await {
            children.extend(synonym_summaries(name, &body));
        }
        if let Ok(body) = self.http.get_json::<Json>(&format!("/collections/{p}/overrides")).await {
            children.extend(rule_summaries(name, &body));
        }
        detail.children = finish(children);
        Ok(detail.action(ObjectAction::destructive("delete", "Delete collection", format!("DELETE /collections/{p}"))))
    }

    async fn alias_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let body: Json = self.http.get_json(&format!("/aliases/{}", encode(name))).await?;
        let target = str_at(&body, "collection_name").to_string();
        let detail = ObjectDetail::empty(reference).definition(pretty(&body), CodeLanguage::Json).property("Collection", &target);
        Ok(detail.action(ObjectAction::destructive("delete", "Delete alias", format!("DELETE /aliases/{}", encode(name)))))
    }

    async fn synonym_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let collection = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A synonym needs its collection as parent."))?;
        let path = format!("/collections/{}/synonyms/{}", encode(collection), encode(&reference.name));
        let body: Json = self.http.get_json(&path).await?;
        let words = str_list(body.get("synonyms"));
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&body), CodeLanguage::Json).property("Collection", collection);
        let root = str_at(&body, "root");
        if !root.is_empty() {
            detail = detail.property("Root", root);
        }
        detail = detail.property("Type", if root.is_empty() { "multi-way" } else { "one-way" });
        detail.rows = Some(rows_table(&[("synonym", "string")], words.into_iter().map(|w| vec![Value::Text(w)]).collect()));
        Ok(detail.action(ObjectAction::destructive("delete", "Delete synonym", format!("DELETE {path}"))))
    }

    async fn rule_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let collection = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A curation rule needs its collection as parent."))?;
        let path = format!("/collections/{}/overrides/{}", encode(collection), encode(&reference.name));
        let body: Json = self.http.get_json(&path).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&body), CodeLanguage::Json).property("Collection", collection);
        if let Some(rule) = body.get("rule") {
            for (label, key) in [("Query", "query"), ("Match", "match"), ("Filter", "filter_by"), ("Tags", "tags")] {
                let v = rule.get(key).map(text_of).unwrap_or_default();
                if !v.is_empty() {
                    detail = detail.property(label, v);
                }
            }
        }
        for (label, key) in [("Filter by", "filter_by"), ("Sort by", "sort_by"), ("Replace query", "replace_query")] {
            let v = str_at(&body, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        let mut rows: Vec<Vec<Value>> = body
            .get("includes")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .map(|i| vec![Value::Text("pin".into()), Value::Text(text_of(i.get("id").unwrap_or(&Json::Null))), Value::Text(text_of(i.get("position").unwrap_or(&Json::Null)))])
            .collect();
        rows.extend(
            body.get("excludes")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
                .map(|e| vec![Value::Text("hide".into()), Value::Text(text_of(e.get("id").unwrap_or(&Json::Null))), Value::Null]),
        );
        detail.rows = Some(rows_table(&[("action", "string"), ("document_id", "string"), ("position", "string")], rows));
        Ok(detail.action(ObjectAction::destructive("delete", "Delete rule", format!("DELETE {path}"))))
    }

    async fn key_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let body: Json = self.http.get_json(&format!("/keys/{}", encode(&reference.name))).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&body), CodeLanguage::Json);
        for (label, key) in [("Description", "description"), ("Prefix", "value_prefix")] {
            let v = str_at(&body, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        detail = detail.property("Actions", str_list(body.get("actions")).join(", ")).property("Collections", str_list(body.get("collections")).join(", "));
        if let Some(exp) = body.get("expires_at").and_then(Json::as_i64) {
            detail = detail.property("Expires at", if exp > 4_000_000_000 { "never".to_string() } else { exp.to_string() });
        }
        Ok(detail.action(ObjectAction::destructive("delete", "Delete API key", format!("DELETE /keys/{}", encode(&reference.name)))))
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let debug: Json = self.http.get_json("/debug").await.unwrap_or(Json::Null);
        let health: Json = self.http.get_json("/health").await.unwrap_or(Json::Null);
        let metrics: Json = self.http.get_json("/metrics.json").await.unwrap_or(Json::Null);
        let stats: Json = self.http.get_json("/stats.json").await.unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference)
            .definition(pretty(&json!({"debug": debug, "health": health, "metrics": metrics})), CodeLanguage::Json)
            .property("Address", self.node_name())
            .property("State", node_state(&debug))
            .property("Healthy", (health.get("ok").and_then(Json::as_bool) != Some(false)).to_string());
        let version = str_at(&debug, "version");
        if !version.is_empty() {
            detail = detail.property("Version", version);
        }
        if let (Some(u), Some(t)) = (num_at(&metrics, "system_memory_used_bytes"), num_at(&metrics, "system_memory_total_bytes")) {
            detail = detail.property("Memory", format!("{} / {}", human_bytes(u), human_bytes(t)));
        }
        if let (Some(u), Some(t)) = (num_at(&metrics, "system_disk_used_bytes"), num_at(&metrics, "system_disk_total_bytes")) {
            detail = detail.property("Disk", format!("{} / {}", human_bytes(u), human_bytes(t)));
        }
        if let Some(c) = num_at(&metrics, "system_cpu_active_percentage") {
            detail = detail.property("CPU", format!("{c}%"));
        }
        let mut rows: Vec<Vec<Value>> = stats
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(_, v)| !v.is_object())
            .map(|(k, v)| vec![Value::Text(k.clone()), Value::Text(text_of(v))])
            .collect();
        rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        detail.rows = Some(rows_table(&[("metric", "string"), ("value", "string")], rows));
        Ok(detail)
    }

    async fn playground(&self, req: &SearchRequest) -> AppResult<SearchResult> {
        let coll = self.collection(&req.index).await?;
        let fields = fields_of(&coll);
        let owned = playground_params(req, &fields);
        let params: Vec<(&str, String)> = owned.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let out = self.search(&req.index, &params).await?;
        Ok(playground_result(&out, req.highlight))
    }

    async fn stats(&self) -> AppResult<ServerStats> {
        let stats: Json = self.http.get_json("/stats.json").await.unwrap_or(Json::Null);
        let metrics: Json = self.http.get_json("/metrics.json").await.unwrap_or(Json::Null);
        let health: Json = self.http.get_json("/health").await.unwrap_or(Json::Null);
        let debug: Json = self.http.get_json("/debug").await.unwrap_or(Json::Null);
        let collections = self.collection_list().await.unwrap_or_default();
        Ok(ServerStats::now(stats_groups(&stats, &metrics, &health, &debug, &collections)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, sql: false, namespaces: false, fixed_columns: true, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true },
        object_kinds: vec![K::Collection, K::Alias, K::Synonym, K::Rule, K::ApiKey, K::Node],
        tools: vec![T::Stats, T::SearchPlayground],
    }
}

#[async_trait]
impl Integration for TypesenseIntegration {
    fn engine(&self) -> Engine {
        Engine::Typesense
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
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

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Collection => self.list_collections().await,
            ObjectKind::Alias => self.list_aliases(parent).await,
            ObjectKind::Synonym => self.list_synonyms(parent).await,
            ObjectKind::Rule => self.list_rules(parent).await,
            ObjectKind::ApiKey => self.list_keys().await,
            ObjectKind::Node => self.list_nodes().await,
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Collection => self.collection_detail(reference).await,
            ObjectKind::Alias => self.alias_detail(reference).await,
            ObjectKind::Synonym => self.synonym_detail(reference).await,
            ObjectKind::Rule => self.rule_detail(reference).await,
            ObjectKind::ApiKey => self.key_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        self.stats().await
    }

    async fn search(&self, req: &SearchRequest) -> AppResult<SearchResult> {
        self.playground(req).await
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
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode};

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

    #[test]
    fn explorer_lists_collections_aliases_synonyms_rules_keys() {
        let collections = vec![
            json!({"name": "books", "num_documents": 1200, "default_sorting_field": "year", "fields": [{"name": "title", "type": "string"}, {"name": "year", "type": "int32"}]}),
            json!({"name": "authors", "num_documents": 3, "fields": [{"name": "name", "type": "string"}]}),
        ];
        let list = collection_summaries(&collections);
        assert_eq!(list[0].reference.name, "authors");
        assert_eq!(list[0].detail.as_deref(), Some("3 docs · 1 fields"));
        assert!(list[0].badge.is_none());
        assert_eq!(list[1].detail.as_deref(), Some("1,200 docs · 2 fields"));
        assert_eq!(list[1].badge.as_deref(), Some("sort year"));

        let aliases = json!({"aliases": [{"name": "current", "collection_name": "books"}, {"name": "people", "collection_name": "authors"}]});
        let all = alias_summaries(&aliases, None);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].reference.parent.as_deref(), Some("books"));
        assert_eq!(all[0].detail.as_deref(), Some("→ books"));
        assert_eq!(alias_summaries(&aliases, Some("authors")).len(), 1);

        let syn = synonym_summaries("books", &json!({"synonyms": [{"id": "s1", "synonyms": ["blazer", "coat"]}, {"id": "s2", "root": "shoe", "synonyms": ["sneaker"]}]}));
        assert_eq!(syn[0].badge.as_deref(), Some("multi-way"));
        assert_eq!(syn[0].detail.as_deref(), Some("blazer, coat"));
        assert_eq!(syn[1].badge.as_deref(), Some("one-way"));
        assert_eq!(syn[1].detail.as_deref(), Some("shoe → sneaker"));
        assert_eq!(syn[1].reference.parent.as_deref(), Some("books"));

        let rules = rule_summaries("books", &json!({"overrides": [{"id": "r1", "rule": {"query": "dune", "match": "exact"}, "includes": [{"id": "1", "position": 1}], "excludes": [{"id": "9"}], "filter_by": "year:>1960"}]}));
        assert_eq!(rules[0].badge.as_deref(), Some("exact"));
        assert_eq!(rules[0].detail.as_deref(), Some("q: dune · 1 pinned · 1 hidden · year:>1960"));

        let keys = api_key_summaries(&json!({"keys": [
            {"id": 1, "description": "Admin", "actions": ["*"], "collections": ["*"], "value_prefix": "abcd"},
            {"id": 2, "description": "", "actions": ["documents:search"], "collections": ["books"], "value_prefix": "xy"}
        ]}));
        assert_eq!(keys[0].reference.name, "1");
        assert_eq!(keys[0].badge.as_deref(), Some("all actions"));
        assert_eq!(keys[0].detail.as_deref(), Some("Admin · * · on * · abcd…"));
        assert_eq!(keys[1].detail.as_deref(), Some("documents:search · on books · xy…"));
        assert!(keys[1].badge.is_none());
    }

    #[test]
    fn node_summary_reports_state_and_health() {
        let debug = json!({"version": "0.25.2", "state": 1});
        let metrics = json!({"system_cpu_active_percentage": "12.5", "system_memory_used_bytes": "2048"});
        let list = node_summary("localhost:8108", &debug, &json!({"ok": true}), &metrics);
        assert_eq!(list[0].reference.name, "localhost:8108");
        assert_eq!(list[0].badge.as_deref(), Some("leader"));
        assert_eq!(list[0].detail.as_deref(), Some("v0.25.2 · healthy · cpu 12.5% · mem 2.0 KB"));
        assert_eq!(node_state(&json!({"state": 4})), "follower");
        assert_eq!(node_state(&json!({})), "single");
        let unhealthy = node_summary("h", &json!({}), &json!({"ok": false}), &Json::Null);
        assert_eq!(unhealthy[0].detail.as_deref(), Some("unhealthy"));
    }

    #[test]
    fn playground_params_cover_query_by_and_paging() {
        let fs = vec![
            Field { name: "id".into(), type_name: "string".into() },
            Field { name: "title".into(), type_name: "string".into() },
            Field { name: "tags".into(), type_name: "string[]".into() },
            Field { name: "year".into(), type_name: "int32".into() },
        ];
        assert_eq!(query_by_all(&fs), "id,title,tags");
        assert_eq!(query_by_all(&fs[3..]), "id");
        let req = SearchRequest {
            index: "books".into(),
            query: "dune".into(),
            filter: Some("year:>1960".into()),
            facets: vec!["tags".into(), "".into()],
            sort: vec!["year:desc".into(), "title".into()],
            highlight: true,
            limit: 20,
            offset: 40,
        };
        let params = playground_params(&req, &fs);
        let get = |k: &str| params.iter().find(|(key, _)| key == k).map(|(_, v)| v.clone());
        assert_eq!(get("q").as_deref(), Some("dune"));
        assert_eq!(get("query_by").as_deref(), Some("id,title,tags"));
        assert_eq!(get("filter_by").as_deref(), Some("year:>1960"));
        assert_eq!(get("facet_by").as_deref(), Some("tags"));
        assert_eq!(get("sort_by").as_deref(), Some("year:desc,title:asc"));
        assert_eq!(get("highlight_full_fields").as_deref(), Some("id,title,tags"));
        assert_eq!(get("per_page").as_deref(), Some("20"));
        assert_eq!(get("page").as_deref(), Some("3"));
        assert!(get("offset").is_none());

        let odd = playground_params(&SearchRequest { query: "".into(), filter: None, facets: vec![], sort: vec![], highlight: false, offset: 5, limit: 20, ..req.clone() }, &fs);
        let get = |k: &str| odd.iter().find(|(key, _)| key == k).map(|(_, v)| v.clone());
        assert_eq!(get("q").as_deref(), Some("*"));
        assert_eq!(get("offset").as_deref(), Some("5"));
        assert_eq!(get("limit").as_deref(), Some("20"));
        assert!(get("page").is_none() && get("facet_by").is_none() && get("highlight_full_fields").is_none());
        let path = search_path("books", &odd.iter().map(|(k, v)| (k.as_str(), v.clone())).collect::<Vec<_>>());
        assert!(path.starts_with("/collections/books/documents/search?q=%2A&query_by="));
    }

    #[test]
    fn playground_maps_hits_facets_and_highlights() {
        let body = json!({
            "found": 12,
            "search_time_ms": 4,
            "hits": [
                {"document": {"id": "1", "title": "Dune", "year": 1965}, "text_match": 130, "highlight": {"title": {"snippet": "<mark>Dune</mark>"}}},
                {"document": {"id": "2", "title": "Neuromancer"}, "text_match": 90, "highlight": {}}
            ],
            "facet_counts": [{"field_name": "tags", "counts": [{"value": "scifi", "count": 7}, {"value": 3, "count": 1}]}]
        });
        let out = playground_result(&body, true);
        let names: Vec<&str> = out.hits.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "_text_match", "title", "year", "_highlight"]);
        assert_eq!(out.hits.rows[0][1], Value::Int(130));
        assert_eq!(out.hits.rows[0][4], Value::Json(json!({"title": {"snippet": "<mark>Dune</mark>"}})));
        assert_eq!(out.hits.rows[1][3], Value::Null);
        assert_eq!(out.hits.rows[1][4], Value::Null);
        assert_eq!(out.total, Some(12));
        assert_eq!(out.took_ms, Some(4));
        assert_eq!(out.facets[0].field, "tags");
        assert_eq!(out.facets[0].values[1], FacetValue { value: "3".into(), count: 1 });
        let plain = playground_result(&body, false);
        assert!(!plain.hits.columns.iter().any(|c| c.name == "_highlight"));
        let empty = playground_result(&json!({"hits": []}), false);
        assert_eq!(empty.hits.columns.len(), 2);
    }

    #[test]
    fn stats_groups_fold_server_figures() {
        let stats = json!({"latency_ms": {"search_latency_ms": 3.5, "write_latency_ms": 1.0}, "total_requests_per_second": 12.0, "search_requests_per_second": "10.0"});
        let metrics = json!({"system_cpu_active_percentage": "25", "system_memory_used_bytes": "1024", "system_memory_total_bytes": "2048", "system_disk_used_bytes": "10", "system_disk_total_bytes": "100"});
        let collections = vec![json!({"name": "a", "num_documents": 10}), json!({"name": "b", "num_documents": 5})];
        let groups = stats_groups(&stats, &metrics, &json!({"ok": true}), &json!({"version": "0.25.2", "state": 1}), &collections);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("0.25.2".into()));
        assert_eq!(find("Server", "State").map(|s| s.value), Some("leader".into()));
        assert_eq!(find("Storage", "Collections").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Documents").and_then(|s| s.numeric), Some(15.0));
        assert_eq!(find("Throughput", "Requests/s").and_then(|s| s.numeric), Some(12.0));
        assert_eq!(find("Throughput", "Searches/s").and_then(|s| s.numeric), Some(10.0));
        assert_eq!(find("Throughput", "Search latency").and_then(|s| s.numeric), Some(3.5));
        assert_eq!(find("System", "CPU").and_then(|s| s.numeric), Some(25.0));
        assert_eq!(find("System", "Memory used").map(|s| s.value), Some("1.0 KB".into()));
        assert_eq!(find("System", "Memory").and_then(|s| s.numeric), Some(50.0));
    }

    #[test]
    fn explorer_actions_parse_as_console_commands() {
        for stmt in ["DELETE /collections/books", "DELETE /aliases/current", "DELETE /collections/books/synonyms/s1", "DELETE /collections/books/overrides/r1", "DELETE /keys/2"] {
            match parse_command(stmt, None) {
                Ok(Command::Rest { method, path, body }) => {
                    assert_eq!(method, "DELETE");
                    assert!(path.starts_with('/') && !path.is_empty());
                    assert!(body.is_none());
                }
                other => panic!("unexpected {other:?} for {stmt}"),
            }
            assert!(!is_read_request("DELETE", stmt));
        }
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
