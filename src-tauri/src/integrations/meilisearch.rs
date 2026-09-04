// SOT: meilisearch-integration, meilisearch-rest-api, meilisearch-filter-syntax, meilisearch-console, object-explorer, server-stats, search-playground, meilisearch-admin-actions

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, json_to_value, json_type_name, local, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FacetCounts, FacetValue, FilterOp, FilterRule, ObjectAction,
    ObjectDetail, ObjectKind, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SearchRequest, SearchResult, ServerStats, Stat, StatGroup, StatementResult, TableInfo, TableKind,
    TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Meilisearch adapter (REST, port 7700, `Authorization: Bearer <key>`).
//        An index is a table, a document a row; columns are the union of keys
//        in a 50-document sample with the index's primaryKey marked.
// WHY:   Meilisearch has no schema and no general query language; the
//        documents endpoint pages with offset/limit and accepts its own filter
//        expressions, so the grid maps almost 1:1. Text matching (Contains…)
//        and sorting are not available on that endpoint and run client-side.
// HOW:   `execute` takes JSON `{"index": "uid", "q": "…", …search params}` →
//        POST /indexes/{uid}/search; `{"index", "documents": [...]}` →
//        add-or-replace; `{"method","path","body"}` → raw passthrough; and
//        the shorthands `SEARCH <index> <text>` and `INDEXES`.
// WHERE: src-tauri/src/integrations/mod.rs (trait), integrations/http.rs (client)
// ============================================================================

const DEFAULT_PORT: u16 = 7700;
const SAMPLE_SIZE: usize = 50;
const LOCAL_CAP: u64 = 5_000;
const SCHEMA: &str = "indexes";

pub struct MeilisearchIntegration {
    http: HttpClient,
    read_only: bool,
    default_index: Option<String>,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = match conn.secret.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(key) => Auth::Bearer(key.to_string()),
        None => Auth::None,
    };
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, auth)?;
    let default_index = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let integration = MeilisearchIntegration { http, read_only: conn.summary.read_only, default_index };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Filter translation
// ---------------------------------------------------------------------------

fn filter_literal(raw: &str) -> String {
    let t = raw.trim();
    if t.parse::<f64>().is_ok() || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        return t.to_string();
    }
    format!("'{}'", t.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn quote_attr(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-') {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\\\""))
    }
}

/// Server-side expression for one rule, or None when it must run locally.
fn filter_expr(rule: &FilterRule) -> Option<String> {
    let attr = quote_attr(&rule.column);
    let v = rule.value.trim();
    Some(match rule.op {
        FilterOp::Eq => format!("{attr} = {}", filter_literal(v)),
        FilterOp::Ne => format!("{attr} != {}", filter_literal(v)),
        FilterOp::Gt => format!("{attr} > {}", filter_literal(v)),
        FilterOp::Gte => format!("{attr} >= {}", filter_literal(v)),
        FilterOp::Lt => format!("{attr} < {}", filter_literal(v)),
        FilterOp::Lte => format!("{attr} <= {}", filter_literal(v)),
        FilterOp::In => {
            let items: Vec<String> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(filter_literal).collect();
            format!("{attr} IN [{}]", items.join(", "))
        }
        FilterOp::IsNull => format!("{attr} IS NULL"),
        FilterOp::IsNotNull => format!("{attr} EXISTS AND {attr} IS NOT NULL"),
        FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith => return None,
    })
}

// WHAT:  Splits the rules into (server filter string, rules to apply locally).
fn split_filters(filters: &[FilterRule]) -> (Option<String>, Vec<FilterRule>) {
    let mut server = Vec::new();
    let mut local_rules = Vec::new();
    for rule in filters {
        match filter_expr(rule) {
            Some(expr) => server.push(expr),
            None => local_rules.push(rule.clone()),
        }
    }
    let server = if server.is_empty() { None } else { Some(server.join(" AND ")) };
    (server, local_rules)
}

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

fn columns_from_docs(docs: &[Json], primary_key: Option<&str>) -> Vec<ColumnInfo> {
    let set = objects_to_result_set(docs, primary_key, docs.len());
    set.columns
        .into_iter()
        .enumerate()
        .map(|(i, c)| ColumnInfo {
            primary_key: primary_key == Some(c.name.as_str()),
            name: c.name,
            data_type: c.type_name,
            nullable: true,
            ordinal: u32::try_from(i + 1).unwrap_or(u32::MAX),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Console commands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Command {
    Search { index: String, params: Json },
    AddDocuments { index: String, documents: Vec<Json>, primary_key: Option<String> },
    Delete { index: String, ids: Vec<Json> },
    Rest { method: String, path: String, body: Option<Json> },
    Indexes,
}

fn parse_command(text: &str, default_index: Option<&str>) -> AppResult<Command> {
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
        let index = obj
            .remove("index")
            .and_then(|v| v.as_str().map(str::to_string))
            .or_else(|| default_index.map(str::to_string))
            .ok_or_else(|| AppError::invalid_input("Add an \"index\" key (or set the connection's index)."))?;
        if let Some(docs) = obj.remove("documents") {
            let documents = docs.as_array().cloned().ok_or_else(|| AppError::invalid_input("\"documents\" must be an array of objects."))?;
            let primary_key = obj.remove("primaryKey").and_then(|v| v.as_str().map(str::to_string));
            return Ok(Command::AddDocuments { index, documents, primary_key });
        }
        if let Some(ids) = obj.remove("delete") {
            let ids = ids.as_array().cloned().ok_or_else(|| AppError::invalid_input("\"delete\" must be an array of document ids."))?;
            return Ok(Command::Delete { index, ids });
        }
        return Ok(Command::Search { index, params: Json::Object(std::mem::take(obj)) });
    }
    let mut parts = trimmed.splitn(3, char::is_whitespace);
    let verb = parts.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "INDEXES" | "INDICES" => Ok(Command::Indexes),
        "SEARCH" => {
            let index = parts.next().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| AppError::invalid_input("Usage: SEARCH <index> <query text>"))?;
            let q = parts.next().map(str::trim).unwrap_or_default();
            Ok(Command::Search { index: index.to_string(), params: json!({"q": q}) })
        }
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" => {
            let path = parts.next().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| AppError::invalid_input("Expected a path, e.g. `GET /stats`."))?;
            let body = match parts.next().map(str::trim).filter(|s| !s.is_empty()) {
                Some(raw) => Some(serde_json::from_str::<Json>(raw).map_err(|e| AppError::invalid_input(format!("Body is not valid JSON: {e}")))?),
                None => None,
            };
            let path = if path.starts_with('/') { path.to_string() } else { format!("/{path}") };
            Ok(Command::Rest { method: verb, path, body })
        }
        _ => Err(AppError::invalid_input("Enter JSON ({\"index\": \"movies\", \"q\": \"…\"}), `SEARCH <index> <text>`, `INDEXES`, or `GET /path`.")),
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

fn search_result(body: &Json, max_rows: usize, primary_key: Option<&str>) -> ResultSet {
    match body.get("hits").and_then(Json::as_array) {
        Some(hits) if !hits.is_empty() => objects_to_result_set(hits, primary_key, max_rows),
        Some(_) => ResultSet { columns: vec![ColumnMeta { name: "hits".into(), type_name: "array".into() }], rows: vec![], truncated: false },
        None => json_result(body.clone()),
    }
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl MeilisearchIntegration {
    async fn primary_key(&self, index: &str) -> AppResult<Option<String>> {
        let info: Json = self.http.get_json(&format!("/indexes/{index}")).await?;
        Ok(info.get("primaryKey").and_then(Json::as_str).map(str::to_string))
    }

    async fn stats_count(&self, index: &str) -> AppResult<i64> {
        let stats: Json = self.http.get_json(&format!("/indexes/{index}/stats")).await?;
        Ok(stats.get("numberOfDocuments").and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_documents(&self, index: &str, offset: u64, limit: u64, filter: Option<&str>) -> AppResult<(Vec<Json>, i64)> {
        let mut body = json!({"offset": offset, "limit": limit});
        if let Some(f) = filter {
            body["filter"] = Json::String(f.to_string());
        }
        let out: Json = self.http.post_json(&format!("/indexes/{index}/documents/fetch"), &body).await?;
        let docs = out.get("results").and_then(Json::as_array).cloned().unwrap_or_default();
        let total = out.get("total").and_then(Json::as_i64).unwrap_or(docs.len() as i64);
        Ok((docs, total))
    }

    fn refuse_if_read_only(&self, what: &str) -> AppResult<()> {
        if self.read_only {
            return Err(AppError::invalid_input(format!("{what} is refused: this connection is read-only.")));
        }
        Ok(())
    }

    async fn run_command(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        match cmd {
            Command::Indexes => {
                let out: Json = self.http.get_json("/indexes?limit=1000").await?;
                let list = out.get("results").cloned().unwrap_or(Json::Array(vec![]));
                Ok(StatementResult::Rows { result: json_result(list) })
            }
            Command::Search { index, mut params } => {
                if params.get("limit").is_none() {
                    params["limit"] = Json::from(max_rows.min(1000));
                }
                let pk = self.primary_key(&index).await.unwrap_or(None);
                let out: Json = self.http.post_json(&format!("/indexes/{index}/search"), &params).await?;
                Ok(StatementResult::Rows { result: search_result(&out, max_rows, pk.as_deref()) })
            }
            Command::AddDocuments { index, documents, primary_key } => {
                self.refuse_if_read_only("Adding documents")?;
                let n = documents.len() as u64;
                let path = match primary_key {
                    Some(pk) => format!("/indexes/{index}/documents?primaryKey={pk}"),
                    None => format!("/indexes/{index}/documents"),
                };
                let _: Json = self.http.post_json(&path, &Json::Array(documents)).await?;
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Delete { index, ids } => {
                self.refuse_if_read_only("Deleting documents")?;
                let n = ids.len() as u64;
                let _: Json = self.http.post_json(&format!("/indexes/{index}/documents/delete-batch"), &Json::Array(ids)).await?;
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Rest { method, path, body } => {
                if method != "GET" && !(method == "POST" && path.contains("/search")) && !(method == "POST" && path.ends_with("/documents/fetch")) {
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
                if let Some(hits) = parsed.get("hits").filter(|h| h.is_array()) {
                    return Ok(StatementResult::Rows { result: objects_to_result_set(hits.as_array().map(Vec::as_slice).unwrap_or_default(), None, max_rows) });
                }
                if let Some(results) = parsed.get("results").and_then(Json::as_array).filter(|r| !r.is_empty() && r.iter().all(Json::is_object)) {
                    return Ok(StatementResult::Rows { result: objects_to_result_set(results, None, max_rows) });
                }
                Ok(StatementResult::Rows { result: json_result(parsed) })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Object explorer / server stats / search playground
//
// WHAT:  `objects()` lists indexes (`/indexes` + `/stats`), synonyms and
//        settings per index, the task queue and API keys; `object_detail()`
//        adds JSON definitions, property sheets and console actions in the
//        adapter's own `VERB /path {json}` language; `server_stats()` folds
//        `/stats`, `/version`, `/health` and the task queue; `search()` is the
//        playground (`q` + filter + facets + sort + `_formatted` highlights).
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const SCORE_FIELD: &str = "_rankingScore";
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

fn compact(v: &Json, max: usize) -> String {
    let s = text_of(v);
    if s.chars().count() > max {
        format!("{}…", s.chars().take(max).collect::<String>())
    } else {
        s
    }
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

// WHAT:  Meilisearch reports task durations as ISO-8601 (`PT1.5S`, `PT2M3S`).
fn iso_duration_secs(text: &str) -> Option<f64> {
    let body = text.trim().strip_prefix("PT")?;
    let mut total = 0.0;
    let mut num = String::new();
    for ch in body.chars() {
        match ch {
            'H' | 'M' | 'S' => {
                let n: f64 = num.parse().ok()?;
                total += match ch {
                    'H' => n * 3600.0,
                    'M' => n * 60.0,
                    _ => n,
                };
                num.clear();
            }
            other => num.push(other),
        }
    }
    Some(total)
}

fn duration_text(secs: f64) -> String {
    if secs < 60.0 {
        format!("{secs:.2} s")
    } else if secs < 3600.0 {
        format!("{:.1} min", secs / 60.0)
    } else {
        format!("{:.1} h", secs / 3600.0)
    }
}

// WHAT:  `searchCutoffMs` → `search-cutoff-ms` (the settings sub-routes).
fn camel_to_kebab(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for ch in s.chars() {
        if ch.is_ascii_uppercase() {
            out.push('-');
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
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

// WHAT:  `/indexes` results + the per-index block of `/stats`.
fn index_summaries(indexes: &Json, stats: &Json) -> Vec<ObjectSummary> {
    let list = indexes
        .get("results")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|i| {
            let uid = i.get("uid").and_then(Json::as_str)?;
            let st = stats.get("indexes").and_then(|x| x.get(uid)).cloned().unwrap_or(Json::Null);
            let mut parts = Vec::new();
            if let Some(n) = st.get("numberOfDocuments").and_then(Json::as_f64) {
                parts.push(format!("{} docs", crate::model::objects::format_number(n)));
            }
            if let Some(pk) = i.get("primaryKey").and_then(Json::as_str) {
                parts.push(format!("pk {pk}"));
            }
            let badge = (st.get("isIndexing").and_then(Json::as_bool) == Some(true)).then(|| "indexing".to_string());
            Some(summary(ObjectKind::Index, uid, None, parts.join(" · "), badge))
        })
        .collect();
    finish(list)
}

fn synonym_summaries(index: &str, body: &Json) -> Vec<ObjectSummary> {
    body.as_object()
        .into_iter()
        .flatten()
        .map(|(word, list)| summary(ObjectKind::Synonym, word, Some(index), str_list(Some(list)).join(", "), None))
        .collect()
}

// WHAT:  Newest task first (uids grow), chipped with the status.
fn task_summaries(body: &Json) -> Vec<ObjectSummary> {
    let mut tasks: Vec<(u64, ObjectSummary)> = body
        .get("results")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|t| {
            let uid = t.get("uid").and_then(Json::as_u64)?;
            let mut parts = vec![str_at(t, "type").to_string()];
            let index = str_at(t, "indexUid");
            if !index.is_empty() {
                parts.push(index.to_string());
            }
            if let Some(secs) = t.get("duration").and_then(Json::as_str).and_then(iso_duration_secs) {
                parts.push(duration_text(secs));
            }
            let err = t.pointer("/error/message").and_then(Json::as_str).unwrap_or("");
            if !err.is_empty() {
                parts.push(err.chars().take(80).collect());
            }
            let parent = if index.is_empty() { None } else { Some(index) };
            Some((uid, summary(ObjectKind::Task, &uid.to_string(), parent, parts.join(" · "), Some(str_at(t, "status").to_string()))))
        })
        .collect();
    tasks.sort_by_key(|t| std::cmp::Reverse(t.0));
    tasks.into_iter().map(|(_, s)| s).take(OBJECT_CAP).collect()
}

fn key_name(k: &Json) -> String {
    k.get("name").and_then(Json::as_str).filter(|n| !n.trim().is_empty()).map(str::to_string).unwrap_or_else(|| str_at(k, "uid").to_string())
}

fn api_key_summaries(body: &Json) -> Vec<ObjectSummary> {
    let list = body
        .get("results")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .map(|k| {
            let actions = str_list(k.get("actions"));
            let indexes = str_list(k.get("indexes"));
            let shown: Vec<&str> = actions.iter().take(3).map(String::as_str).collect();
            let mut parts = vec![format!("{}{}", shown.join(", "), if actions.len() > 3 { "…" } else { "" })];
            if !indexes.is_empty() {
                parts.push(format!("on {}", indexes.join(", ")));
            }
            let expires = k.get("expiresAt").and_then(Json::as_str).unwrap_or("never");
            parts.push(format!("expires {expires}"));
            let badge = actions.iter().any(|a| a == "*").then(|| "all actions".to_string());
            summary(ObjectKind::ApiKey, &key_name(k), None, parts.join(" · "), badge)
        })
        .collect();
    finish(list)
}

fn setting_summaries(index: &str, settings: &Json) -> Vec<ObjectSummary> {
    settings
        .as_object()
        .into_iter()
        .flatten()
        .map(|(key, value)| summary(ObjectKind::Setting, key, Some(index), compact(value, 80), Some(json_type_name(value).to_string())))
        .collect()
}

// WHAT:  Masks a secret so the sheet shows which key it is, never the key.
fn mask(secret: &str) -> String {
    let shown: String = secret.chars().take(4).collect();
    format!("{shown}…")
}

// ---- search playground ------------------------------------------------------

// WHAT:  Playground request → `/indexes/{uid}/search` body. The filter is
//        passed verbatim (Meilisearch's own expression syntax, or a JSON
//        array of expressions); sort entries are `field[:asc|desc]`;
//        highlighting asks for `_formatted` on every attribute.
fn playground_body(req: &SearchRequest) -> Json {
    let mut body = json!({"q": req.query, "limit": req.limit, "offset": req.offset, "showRankingScore": true});
    if let Some(f) = req.filter.as_deref().map(str::trim).filter(|f| !f.is_empty()) {
        body["filter"] = match serde_json::from_str::<Json>(f) {
            Ok(v) if v.is_array() => v,
            _ => Json::String(f.to_string()),
        };
    }
    let facets: Vec<&str> = req.facets.iter().map(|f| f.trim()).filter(|f| !f.is_empty()).collect();
    if !facets.is_empty() {
        body["facets"] = json!(facets);
    }
    let sort: Vec<String> = req
        .sort
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| if s.contains(':') { s.to_string() } else { format!("{s}:asc") })
        .collect();
    if !sort.is_empty() {
        body["sort"] = json!(sort);
    }
    if req.highlight {
        body["attributesToHighlight"] = json!(["*"]);
        body["highlightPreTag"] = json!("<em>");
        body["highlightPostTag"] = json!("</em>");
    }
    body
}

// WHAT:  Search response → hits grid (primary key first, `_rankingScore`,
//        document fields, `_highlight` from `_formatted`), facet counts from
//        `facetDistribution`, estimated total and processing time.
fn playground_result(body: &Json, primary_key: Option<&str>, highlight: bool) -> SearchResult {
    let hits: Vec<&Json> = body.get("hits").and_then(Json::as_array).map(|h| h.iter().collect()).unwrap_or_default();
    let mut names: Vec<String> = Vec::new();
    if let Some(pk) = primary_key {
        names.push(pk.to_string());
    }
    names.push(SCORE_FIELD.to_string());
    for obj in hits.iter().filter_map(|h| h.as_object()) {
        for k in obj.keys() {
            if k != "_formatted" && k != SCORE_FIELD && !names.iter().any(|n| n == k) {
                names.push(k.clone());
            }
        }
    }
    if highlight {
        names.push(HIGHLIGHT_FIELD.to_string());
    }
    let rows: Vec<Vec<Value>> = hits
        .iter()
        .map(|hit| {
            names
                .iter()
                .map(|n| match n.as_str() {
                    HIGHLIGHT_FIELD => hit.get("_formatted").filter(|f| !f.is_null()).map(|f| Value::Json(f.clone())).unwrap_or(Value::Null),
                    other => hit.get(other).map(json_to_value).unwrap_or(Value::Null),
                })
                .collect()
        })
        .collect();
    let columns = names
        .iter()
        .map(|n| {
            let type_name = match n.as_str() {
                SCORE_FIELD => "number",
                HIGHLIGHT_FIELD => "object",
                other => hits.iter().find_map(|h| h.get(other).filter(|v| !v.is_null()).map(json_type_name)).unwrap_or("json"),
            };
            ColumnMeta { name: n.clone(), type_name: type_name.to_string() }
        })
        .collect();
    let facets = body
        .get("facetDistribution")
        .and_then(Json::as_object)
        .into_iter()
        .flatten()
        .map(|(field, counts)| {
            let mut values: Vec<FacetValue> = counts
                .as_object()
                .into_iter()
                .flatten()
                .map(|(value, count)| FacetValue { value: value.clone(), count: count.as_u64().unwrap_or(0) })
                .collect();
            values.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
            FacetCounts { field: field.clone(), values }
        })
        .collect();
    SearchResult {
        hits: ResultSet { columns, rows, truncated: false },
        total: body.get("estimatedTotalHits").or_else(|| body.get("totalHits")).and_then(Json::as_u64),
        facets,
        took_ms: body.get("processingTimeMs").and_then(Json::as_u64),
    }
}

// ---- server stats -----------------------------------------------------------

fn stats_groups(stats: &Json, version: &Json, health: &Json, queue: &[(&str, Option<f64>)]) -> Vec<StatGroup> {
    let mut server = Vec::new();
    let v = str_at(version, "pkgVersion");
    if !v.is_empty() {
        server.push(Stat::text("Version", v));
    }
    let commit = str_at(version, "commitDate");
    if !commit.is_empty() {
        server.push(Stat::text("Built", commit));
    }
    let status = str_at(health, "status");
    if !status.is_empty() {
        server.push(Stat::text("Health", status));
    }
    let last = str_at(stats, "lastUpdate");
    if !last.is_empty() {
        server.push(Stat::text("Last update", last));
    }
    let mut storage = Vec::new();
    if let Some(b) = stats.get("databaseSize").and_then(Json::as_f64) {
        storage.push(bytes_stat("Database size", b));
    }
    if let Some(b) = stats.get("usedDatabaseSize").and_then(Json::as_f64) {
        storage.push(bytes_stat("Used size", b));
    }
    let indexes = stats.get("indexes").and_then(Json::as_object);
    let count = indexes.map(|i| i.len()).unwrap_or(0);
    let docs: f64 = indexes.into_iter().flat_map(|i| i.values()).filter_map(|i| i.get("numberOfDocuments").and_then(Json::as_f64)).sum();
    let indexing = indexes.into_iter().flat_map(|i| i.values()).filter(|i| i.get("isIndexing").and_then(Json::as_bool) == Some(true)).count();
    storage.push(Stat::number("Indexes", count as f64, None));
    storage.push(Stat::number("Documents", docs, None));
    storage.push(Stat::number("Indexing now", indexing as f64, None));
    let queue_stats: Vec<Stat> = queue.iter().filter_map(|(label, n)| n.map(|n| Stat::number(label, n, None))).collect();
    let mut groups = vec![StatGroup { title: "Server".into(), stats: server }, StatGroup { title: "Storage".into(), stats: storage }];
    if !queue_stats.is_empty() {
        groups.push(StatGroup { title: "Task queue".into(), stats: queue_stats });
    }
    groups
}

impl MeilisearchIntegration {
    async fn index_uids(&self) -> AppResult<Vec<String>> {
        let out: Json = self.http.get_json("/indexes?limit=1000").await?;
        let mut uids: Vec<String> = out.get("results").and_then(Json::as_array).into_iter().flatten().filter_map(|i| i.get("uid").and_then(Json::as_str).map(str::to_string)).collect();
        uids.sort();
        Ok(uids)
    }

    async fn scoped_uids(&self, parent: Option<&str>) -> AppResult<Vec<String>> {
        match parent {
            Some(p) => Ok(vec![p.to_string()]),
            None => self.index_uids().await,
        }
    }

    async fn list_indexes(&self) -> AppResult<Vec<ObjectSummary>> {
        let indexes: Json = self.http.get_json("/indexes?limit=1000").await?;
        let stats: Json = self.http.get_json("/stats").await.unwrap_or(Json::Null);
        Ok(index_summaries(&indexes, &stats))
    }

    async fn list_synonyms(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for uid in self.scoped_uids(parent).await? {
            let body: Json = self.http.get_json(&format!("/indexes/{uid}/settings/synonyms")).await?;
            list.extend(synonym_summaries(&uid, &body));
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn list_tasks(&self) -> AppResult<Vec<ObjectSummary>> {
        let body: Json = self.http.get_json("/tasks?limit=500").await?;
        Ok(task_summaries(&body))
    }

    async fn list_keys(&self) -> AppResult<Vec<ObjectSummary>> {
        match self.http.get_json::<Json>("/keys?limit=100").await {
            Ok(body) => Ok(api_key_summaries(&body)),
            Err(AppError::NotConnected { .. }) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    async fn list_settings(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for uid in self.scoped_uids(parent).await? {
            let body: Json = self.http.get_json(&format!("/indexes/{uid}/settings")).await?;
            list.extend(setting_summaries(&uid, &body));
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn index_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let uid = reference.name.as_str();
        let info: Json = self.http.get_json(&format!("/indexes/{uid}")).await?;
        let stats: Json = self.http.get_json(&format!("/indexes/{uid}/stats")).await.unwrap_or(Json::Null);
        let settings: Json = self.http.get_json(&format!("/indexes/{uid}/settings")).await.unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&settings), CodeLanguage::Json);
        for (label, key) in [("Primary key", "primaryKey"), ("Created", "createdAt"), ("Updated", "updatedAt")] {
            let v = str_at(&info, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        if let Some(n) = stats.get("numberOfDocuments").and_then(Json::as_f64) {
            detail = detail.property("Documents", crate::model::objects::format_number(n));
        }
        if let Some(b) = stats.get("isIndexing").and_then(Json::as_bool) {
            detail = detail.property("Indexing", b.to_string());
        }
        for key in ["searchableAttributes", "filterableAttributes", "sortableAttributes"] {
            if let Some(v) = settings.get(key) {
                detail = detail.property(key, compact(v, 120));
            }
        }
        detail.columns = self.columns(&TableRef { schema: Some(SCHEMA.into()), name: uid.to_string() }).await.unwrap_or_default();
        let mut fields: Vec<(String, f64)> = stats.get("fieldDistribution").and_then(Json::as_object).into_iter().flatten().map(|(k, v)| (k.clone(), v.as_f64().unwrap_or(0.0))).collect();
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        detail.rows = Some(rows_table(&[("attribute", "string"), ("documents", "integer")], fields.into_iter().map(|(k, n)| vec![Value::Text(k), Value::Int(n as i64)]).collect()));
        let mut children = synonym_summaries(uid, &settings.get("synonyms").cloned().unwrap_or(Json::Null));
        children.extend(setting_summaries(uid, &settings));
        detail.children = finish(children);
        Ok(detail
            .action(ObjectAction::destructive("clear", "Delete all documents", format!("DELETE /indexes/{uid}/documents")))
            .action(ObjectAction::destructive("reset-settings", "Reset settings", format!("DELETE /indexes/{uid}/settings")))
            .action(ObjectAction::destructive("delete", "Delete index", format!("DELETE /indexes/{uid}"))))
    }

    async fn synonym_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let index = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A synonym needs its index as parent."))?;
        let body: Json = self.http.get_json(&format!("/indexes/{index}/settings/synonyms")).await?;
        let entry = body.get(&reference.name).cloned().ok_or_else(|| AppError::not_found(format!("Synonym {} not found in {index}.", reference.name)))?;
        let list = str_list(Some(&entry));
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&json!({&reference.name: entry})), CodeLanguage::Json).property("Index", index).property("Synonyms", list.len().to_string());
        detail.rows = Some(rows_table(&[("synonym", "string")], list.into_iter().map(|s| vec![Value::Text(s)]).collect()));
        let mut remaining = body.clone();
        if let Some(obj) = remaining.as_object_mut() {
            obj.remove(&reference.name);
        }
        Ok(detail.action(ObjectAction::destructive("delete", "Delete synonym", format!("PUT /indexes/{index}/settings/synonyms {remaining}"))))
    }

    async fn task_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let uid = reference.name.trim();
        let task: Json = self.http.get_json(&format!("/tasks/{uid}")).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&task), CodeLanguage::Json);
        for (label, key) in [("Status", "status"), ("Type", "type"), ("Index", "indexUid"), ("Enqueued", "enqueuedAt"), ("Started", "startedAt"), ("Finished", "finishedAt")] {
            let v = str_at(&task, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        if let Some(secs) = task.get("duration").and_then(Json::as_str).and_then(iso_duration_secs) {
            detail = detail.property("Duration", duration_text(secs));
        }
        if let Some(err) = task.pointer("/error/message").and_then(Json::as_str) {
            detail = detail.property("Error", err);
        }
        let status = str_at(&task, "status");
        if matches!(status, "enqueued" | "processing") {
            detail = detail.action(ObjectAction::destructive("cancel", "Cancel task", format!("POST /tasks/cancel?uids={uid}")));
        } else {
            detail = detail.action(ObjectAction::destructive("delete", "Delete task", format!("DELETE /tasks?uids={uid}")));
        }
        Ok(detail)
    }

    async fn key_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let body: Json = self.http.get_json("/keys?limit=100").await?;
        let mut key = body
            .get("results")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .find(|k| key_name(k) == reference.name || str_at(k, "uid") == reference.name)
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("API key {} not found.", reference.name)))?;
        if let Some(secret) = key.get("key").and_then(Json::as_str).map(mask) {
            key["key"] = Json::String(secret);
        }
        let uid = str_at(&key, "uid").to_string();
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&key), CodeLanguage::Json).property("UID", &uid);
        for (label, k) in [("Description", "description"), ("Expires", "expiresAt"), ("Created", "createdAt"), ("Updated", "updatedAt")] {
            let v = str_at(&key, k);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        detail = detail.property("Actions", str_list(key.get("actions")).join(", ")).property("Indexes", str_list(key.get("indexes")).join(", "));
        Ok(detail.action(ObjectAction::destructive("delete", "Delete API key", format!("DELETE /keys/{uid}"))))
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let index = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A setting needs its index as parent."))?;
        let settings: Json = self.http.get_json(&format!("/indexes/{index}/settings")).await?;
        let value = settings.get(&reference.name).cloned().ok_or_else(|| AppError::not_found(format!("Setting {} not found.", reference.name)))?;
        let route = camel_to_kebab(&reference.name);
        let detail = ObjectDetail::empty(reference)
            .definition(pretty(&value), CodeLanguage::Json)
            .property("Index", index)
            .property("Type", json_type_name(&value))
            .property("Route", format!("/indexes/{index}/settings/{route}"));
        Ok(detail.action(ObjectAction::destructive("reset", "Reset to default", format!("DELETE /indexes/{index}/settings/{route}"))))
    }

    async fn playground(&self, req: &SearchRequest) -> AppResult<SearchResult> {
        let pk = self.primary_key(&req.index).await.unwrap_or(None);
        let out: Json = self.http.post_json(&format!("/indexes/{}/search", req.index), &playground_body(req)).await?;
        Ok(playground_result(&out, pk.as_deref(), req.highlight))
    }

    async fn queue_total(&self, status: &str) -> Option<f64> {
        let body: Json = self.http.get_json(&format!("/tasks?statuses={status}&limit=1")).await.ok()?;
        body.get("total").and_then(Json::as_f64)
    }

    async fn stats(&self) -> AppResult<ServerStats> {
        let stats: Json = self.http.get_json("/stats").await?;
        let version: Json = self.http.get_json("/version").await.unwrap_or(Json::Null);
        let health: Json = self.http.get_json("/health").await.unwrap_or(Json::Null);
        let queue = [
            ("Enqueued", self.queue_total("enqueued").await),
            ("Processing", self.queue_total("processing").await),
            ("Failed", self.queue_total("failed").await),
        ];
        Ok(ServerStats::now(stats_groups(&stats, &version, &health, &queue)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { sql: false, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true },
        object_kinds: vec![K::Index, K::Synonym, K::Task, K::ApiKey, K::Setting],
        tools: vec![T::Stats, T::SearchPlayground],
    }
}

#[async_trait]
impl Integration for MeilisearchIntegration {
    fn engine(&self) -> Engine {
        Engine::Meilisearch
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _: Json = self.http.get_json("/version").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v: Json = self.http.get_json("/version").await?;
        Ok(v.get("pkgVersion").and_then(Json::as_str).map(|s| format!("Meilisearch {s}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(SCHEMA.into())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![SCHEMA.into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let out: Json = self.http.get_json("/indexes?limit=1000").await?;
        let mut tables = Vec::new();
        for idx in out.get("results").and_then(Json::as_array).into_iter().flatten() {
            let Some(uid) = idx.get("uid").and_then(Json::as_str) else { continue };
            let row_estimate = self.stats_count(uid).await.ok();
            tables.push(TableInfo { schema: Some(SCHEMA.into()), name: uid.to_string(), kind: TableKind::Table, row_estimate });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: SCHEMA.into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let pk = self.primary_key(&table.name).await?;
        let (docs, _) = self.fetch_documents(&table.name, 0, SAMPLE_SIZE as u64, None).await?;
        let mut cols = columns_from_docs(&docs, pk.as_deref());
        if cols.is_empty() {
            cols.push(ColumnInfo { name: pk.unwrap_or_else(|| "id".into()), data_type: "string".into(), nullable: false, primary_key: true, ordinal: 1 });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.stats_count(&table.name).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (server, local_rules) = split_filters(filters);
        if local_rules.is_empty() {
            if server.is_none() {
                return self.stats_count(&table.name).await;
            }
            let (_, total) = self.fetch_documents(&table.name, 0, 1, server.as_deref()).await?;
            return Ok(total);
        }
        let (docs, _) = self.fetch_documents(&table.name, 0, LOCAL_CAP, server.as_deref()).await?;
        let set = objects_to_result_set(&docs, None, docs.len());
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        Ok(local::apply_filters(&names, set.rows, &local_rules).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let pk = self.primary_key(&table.name).await?;
        let (server, local_rules) = split_filters(&query.filters);
        let needs_local = !local_rules.is_empty() || !query.sort.is_empty();
        if !needs_local {
            let (docs, _) = self.fetch_documents(&table.name, query.offset, u64::from(query.limit), server.as_deref()).await?;
            return Ok(objects_to_result_set(&docs, pk.as_deref(), query.limit as usize));
        }
        // Sorting needs the whole (capped) set; text filters shrink it afterwards.
        let (docs, _) = self.fetch_documents(&table.name, 0, LOCAL_CAP, server.as_deref()).await?;
        let mut set = objects_to_result_set(&docs, pk.as_deref(), docs.len());
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery { sort: query.sort.clone(), filters: local_rules, offset: query.offset, limit: query.limit };
        set.rows = local::page(&names, set.rows, &local_query);
        set.truncated = false;
        Ok(set)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut results = Vec::new();
        for stmt in split_statements(sql) {
            let cmd = parse_command(&stmt, self.default_index.as_deref())?;
            results.push(self.run_command(cmd, max_rows).await?);
        }
        Ok(results)
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Index => self.list_indexes().await,
            ObjectKind::Synonym => self.list_synonyms(parent).await,
            ObjectKind::Task => self.list_tasks().await,
            ObjectKind::ApiKey => self.list_keys().await,
            ObjectKind::Setting => self.list_settings(parent).await,
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Index => self.index_detail(reference).await,
            ObjectKind::Synonym => self.synonym_detail(reference).await,
            ObjectKind::Task => self.task_detail(reference).await,
            ObjectKind::ApiKey => self.key_detail(reference).await,
            ObjectKind::Setting => self.setting_detail(reference).await,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode, SortRule};

    #[test]
    fn filter_expressions() {
        let f = |op, v: &str| FilterRule { column: "genre".into(), op, value: v.into() };
        assert_eq!(filter_expr(&f(FilterOp::Eq, "sci-fi")).as_deref(), Some("genre = 'sci-fi'"));
        assert_eq!(filter_expr(&f(FilterOp::Eq, "it's")).as_deref(), Some("genre = 'it\\'s'"));
        assert_eq!(filter_expr(&f(FilterOp::Gt, "3")).as_deref(), Some("genre > 3"));
        assert_eq!(filter_expr(&f(FilterOp::In, "a, 2,b")).as_deref(), Some("genre IN ['a', 2, 'b']"));
        assert_eq!(filter_expr(&f(FilterOp::IsNull, "")).as_deref(), Some("genre IS NULL"));
        assert!(filter_expr(&f(FilterOp::Contains, "x")).is_none());
        let (server, local_rules) = split_filters(&[f(FilterOp::Eq, "a"), f(FilterOp::Contains, "b"), f(FilterOp::Lte, "5")]);
        assert_eq!(server.as_deref(), Some("genre = 'a' AND genre <= 5"));
        assert_eq!(local_rules.len(), 1);
        assert_eq!(quote_attr("weird name"), "\"weird name\"");
    }

    #[test]
    fn columns_mark_primary_key() {
        let docs = vec![json!({"id": 1, "title": "A"}), json!({"id": 2, "year": 1999})];
        let cols = columns_from_docs(&docs, Some("id"));
        assert_eq!(cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id", "title", "year"]);
        assert!(cols[0].primary_key);
        assert!(!cols[1].primary_key);
        assert_eq!(cols[2].data_type, "integer");
    }

    #[test]
    fn command_parsing() {
        assert_eq!(parse_command("INDEXES", None).ok(), Some(Command::Indexes));
        assert_eq!(parse_command("search movies star wars", None).ok(), Some(Command::Search { index: "movies".into(), params: json!({"q": "star wars"}) }));
        assert_eq!(
            parse_command("{\"index\":\"movies\",\"q\":\"x\",\"limit\":5}", None).ok(),
            Some(Command::Search { index: "movies".into(), params: json!({"q": "x", "limit": 5}) })
        );
        assert_eq!(parse_command("{\"q\":\"x\"}", Some("dflt")).ok(), Some(Command::Search { index: "dflt".into(), params: json!({"q": "x"}) }));
        assert!(parse_command("{\"q\":\"x\"}", None).is_err());
        match parse_command("{\"index\":\"m\",\"documents\":[{\"id\":1}],\"primaryKey\":\"id\"}", None) {
            Ok(Command::AddDocuments { index, documents, primary_key }) => {
                assert_eq!(index, "m");
                assert_eq!(documents.len(), 1);
                assert_eq!(primary_key.as_deref(), Some("id"));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(parse_command("{\"index\":\"m\",\"delete\":[1,2]}", None).ok(), Some(Command::Delete { index: "m".into(), ids: vec![json!(1), json!(2)] }));
        assert_eq!(
            parse_command("{\"method\":\"get\",\"path\":\"/stats\"}", None).ok(),
            Some(Command::Rest { method: "GET".into(), path: "/stats".into(), body: None })
        );
        assert_eq!(parse_command("GET stats", None).ok(), Some(Command::Rest { method: "GET".into(), path: "/stats".into(), body: None }));
        assert_eq!(
            parse_command("POST /indexes/m/search {\"q\":\"a\"}", None).ok(),
            Some(Command::Rest { method: "POST".into(), path: "/indexes/m/search".into(), body: Some(json!({"q": "a"})) })
        );
        assert!(parse_command("SELECT 1", None).is_err());
        assert_eq!(split_statements("INDEXES\n\nSEARCH m x\n").len(), 2);
    }

    #[test]
    fn search_hits_become_rows() {
        let body = json!({"hits": [{"id": 1, "t": "a"}], "estimatedTotalHits": 1});
        let set = search_result(&body, 10, Some("id"));
        assert_eq!(set.columns[0].name, "id");
        assert_eq!(set.rows.len(), 1);
        assert_eq!(search_result(&json!({"hits": []}), 10, None).rows.len(), 0);
        assert_eq!(search_result(&json!({"taskUid": 1}), 10, None).columns[0].name, "result");
    }

    #[test]
    fn explorer_lists_indexes_synonyms_tasks_keys_settings() {
        let indexes = json!({"results": [{"uid": "movies", "primaryKey": "id"}, {"uid": "books", "primaryKey": null}]});
        let stats = json!({"indexes": {"movies": {"numberOfDocuments": 1500, "isIndexing": true}, "books": {"numberOfDocuments": 2}}});
        let list = index_summaries(&indexes, &stats);
        assert_eq!(list[0].reference.name, "books");
        assert_eq!(list[0].detail.as_deref(), Some("2 docs"));
        assert!(list[0].badge.is_none());
        assert_eq!(list[1].detail.as_deref(), Some("1,500 docs · pk id"));
        assert_eq!(list[1].badge.as_deref(), Some("indexing"));

        let syn = synonym_summaries("movies", &json!({"wolverine": ["xmen", "logan"]}));
        assert_eq!(syn[0].reference.parent.as_deref(), Some("movies"));
        assert_eq!(syn[0].detail.as_deref(), Some("xmen, logan"));

        let tasks = task_summaries(&json!({"results": [
            {"uid": 3, "indexUid": "movies", "status": "succeeded", "type": "documentAdditionOrUpdate", "duration": "PT1.5S"},
            {"uid": 9, "indexUid": null, "status": "failed", "type": "dumpCreation", "duration": null, "error": {"message": "disk full"}}
        ]}));
        assert_eq!(tasks[0].reference.name, "9");
        assert_eq!(tasks[0].badge.as_deref(), Some("failed"));
        assert_eq!(tasks[0].detail.as_deref(), Some("dumpCreation · disk full"));
        assert!(tasks[0].reference.parent.is_none());
        assert_eq!(tasks[1].detail.as_deref(), Some("documentAdditionOrUpdate · movies · 1.50 s"));
        assert_eq!(tasks[1].reference.parent.as_deref(), Some("movies"));
        assert_eq!(iso_duration_secs("PT2M3.5S"), Some(123.5));
        assert_eq!(iso_duration_secs("PT1H"), Some(3600.0));
        assert_eq!(iso_duration_secs("nope"), None);

        let keys = api_key_summaries(&json!({"results": [
            {"uid": "u1", "name": "Admin", "actions": ["*"], "indexes": ["*"], "expiresAt": null},
            {"uid": "u2", "name": "", "actions": ["search", "documents.get", "indexes.get", "stats.get"], "indexes": ["movies"], "expiresAt": "2027-01-01T00:00:00Z"}
        ]}));
        assert_eq!(keys[0].reference.name, "Admin");
        assert_eq!(keys[0].badge.as_deref(), Some("all actions"));
        assert_eq!(keys[0].detail.as_deref(), Some("* · on * · expires never"));
        assert_eq!(keys[1].reference.name, "u2");
        assert_eq!(keys[1].detail.as_deref(), Some("search, documents.get, indexes.get… · on movies · expires 2027-01-01T00:00:00Z"));
        assert_eq!(mask("abcdef123"), "abcd…");

        let settings = setting_summaries("movies", &json!({"searchableAttributes": ["*"], "searchCutoffMs": null}));
        assert_eq!(settings.len(), 2);
        assert!(settings.iter().any(|s| s.reference.name == "searchableAttributes" && s.detail.as_deref() == Some("[\"*\"]") && s.badge.as_deref() == Some("array")));
        assert_eq!(camel_to_kebab("searchCutoffMs"), "search-cutoff-ms");
        assert_eq!(camel_to_kebab("faceting"), "faceting");
        assert_eq!(compact(&json!("abcdefgh"), 4), "abcd…");
    }

    #[test]
    fn playground_body_and_result() {
        let req = SearchRequest {
            index: "movies".into(),
            query: "wolverine".into(),
            filter: Some("genre = 'action' AND year > 2000".into()),
            facets: vec!["genre".into(), " ".into()],
            sort: vec!["year:desc".into(), "title".into()],
            highlight: true,
            limit: 20,
            offset: 40,
        };
        let body = playground_body(&req);
        assert_eq!(body["q"], "wolverine");
        assert_eq!(body["filter"], "genre = 'action' AND year > 2000");
        assert_eq!(body["facets"], json!(["genre"]));
        assert_eq!(body["sort"], json!(["year:desc", "title:asc"]));
        assert_eq!(body["attributesToHighlight"], json!(["*"]));
        assert_eq!(body["limit"], 20);
        assert_eq!(body["offset"], 40);
        let arr = playground_body(&SearchRequest { filter: Some("[\"a = 1\", [\"b = 2\", \"c = 3\"]]".into()), highlight: false, ..req.clone() });
        assert!(arr["filter"].is_array());
        assert!(arr.get("attributesToHighlight").is_none());

        let out = json!({
            "hits": [
                {"id": 1, "title": "Logan", "year": 2017, "_rankingScore": 0.9, "_formatted": {"title": "<em>Logan</em>"}},
                {"id": 2, "title": "X-Men", "_rankingScore": 0.4}
            ],
            "estimatedTotalHits": 2,
            "processingTimeMs": 3,
            "facetDistribution": {"genre": {"action": 2, "drama": 5}}
        });
        let result = playground_result(&out, Some("id"), true);
        let names: Vec<&str> = result.hits.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names[..2], ["id", "_rankingScore"]);
        assert_eq!(names.last().copied(), Some("_highlight"));
        assert!(names.contains(&"year") && !names.contains(&"_formatted"));
        assert_eq!(result.hits.rows[0][1], Value::Float(0.9));
        let h = names.len() - 1;
        assert_eq!(result.hits.rows[0][h], Value::Json(json!({"title": "<em>Logan</em>"})));
        assert_eq!(result.hits.rows[1][h], Value::Null);
        assert_eq!(result.total, Some(2));
        assert_eq!(result.took_ms, Some(3));
        assert_eq!(result.facets[0].field, "genre");
        assert_eq!(result.facets[0].values[0], FacetValue { value: "drama".into(), count: 5 });
        let none = playground_result(&json!({"hits": []}), None, false);
        assert_eq!(none.hits.columns.len(), 1);
    }

    #[test]
    fn stats_groups_fold_server_figures() {
        let stats = json!({"databaseSize": 2048, "usedDatabaseSize": 1024, "lastUpdate": "2026-01-01T00:00:00Z", "indexes": {"a": {"numberOfDocuments": 10, "isIndexing": true}, "b": {"numberOfDocuments": 5, "isIndexing": false}}});
        let version = json!({"pkgVersion": "1.12.0", "commitDate": "2025-12-01"});
        let health = json!({"status": "available"});
        let groups = stats_groups(&stats, &version, &health, &[("Enqueued", Some(2.0)), ("Processing", None)]);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("1.12.0".into()));
        assert_eq!(find("Server", "Health").map(|s| s.value), Some("available".into()));
        assert_eq!(find("Storage", "Database size").map(|s| s.value), Some("2.0 KB".into()));
        assert_eq!(find("Storage", "Documents").and_then(|s| s.numeric), Some(15.0));
        assert_eq!(find("Storage", "Indexing now").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Task queue", "Enqueued").and_then(|s| s.numeric), Some(2.0));
        assert!(find("Task queue", "Processing").is_none());
    }

    #[test]
    fn explorer_actions_parse_as_console_commands() {
        let stmt = "PUT /indexes/movies/settings/synonyms {\"logan\":[\"wolverine\"]}";
        assert_eq!(
            parse_command(stmt, None).ok(),
            Some(Command::Rest { method: "PUT".into(), path: "/indexes/movies/settings/synonyms".into(), body: Some(json!({"logan": ["wolverine"]})) })
        );
        assert!(matches!(parse_command("DELETE /indexes/movies/settings/search-cutoff-ms", None), Ok(Command::Rest { method, .. }) if method == "DELETE"));
        assert!(matches!(parse_command("POST /tasks/cancel?uids=9", None), Ok(Command::Rest { path, .. }) if path == "/tasks/cancel?uids=9"));
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_MEILISEARCH_URL is set.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_MEILISEARCH_URL") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Meilisearch,
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
        let secret = std::env::var("DBFREE_TEST_MEILISEARCH_KEY").ok();
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, secret.is_some()), secret };
        let m = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        assert!(m.server_version().await.unwrap_or_default().unwrap_or_default().starts_with("Meilisearch"));
        m.execute("{\"index\":\"dbfree_t\",\"documents\":[{\"id\":1,\"name\":\"alpha\",\"n\":1},{\"id\":2,\"name\":\"beta\",\"n\":2},{\"id\":3,\"name\":\"gamma\",\"n\":3}],\"primaryKey\":\"id\"}", 10)
            .await
            .unwrap_or_else(|e| panic!("add: {e}"));
        // Make n filterable and wait for tasks.
        m.execute("PATCH /indexes/dbfree_t/settings {\"filterableAttributes\":[\"n\",\"name\"]}", 10).await.unwrap_or_else(|e| panic!("settings: {e}"));
        for _ in 0..50 {
            let tasks: Json = m_http(&m).get_json("/tasks?statuses=enqueued,processing").await.unwrap_or(Json::Null);
            if tasks.get("results").and_then(Json::as_array).map(Vec::is_empty).unwrap_or(true) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let table = TableRef { schema: Some(SCHEMA.into()), name: "dbfree_t".into() };
        let cat = m.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == "dbfree_t"));
        let cols = m.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols[0].primary_key && cols[0].name == "id", "{cols:?}");
        let q = PageQuery {
            sort: vec![SortRule { column: "n".into(), desc: true }],
            filters: vec![FilterRule { column: "n".into(), op: FilterOp::Gte, value: "2".into() }, FilterRule { column: "name".into(), op: FilterOp::Contains, value: "a".into() }],
            offset: 0,
            limit: 10,
        };
        let page = m.fetch_page(&table, &q).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2, "{page:?}");
        assert_eq!(page.rows[0][0], Value::Int(3));
        assert_eq!(m.count(&table, &q.filters).await.unwrap_or_default(), 2);
        let out = m.execute("SEARCH dbfree_t beta", 10).await.unwrap_or_else(|e| panic!("search: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 1),
            other => panic!("unexpected {other:?}"),
        }
        m.execute("DELETE /indexes/dbfree_t", 10).await.ok();
    }

    #[cfg(test)]
    fn m_http(m: &Arc<dyn Integration>) -> HttpClient {
        let url = std::env::var("DBFREE_TEST_MEILISEARCH_URL").unwrap_or_default();
        let key = std::env::var("DBFREE_TEST_MEILISEARCH_KEY").ok();
        let _ = m;
        HttpClient::new(url, key.map(Auth::Bearer).unwrap_or(Auth::None), false).unwrap_or_else(|e| panic!("{e}"))
    }
}
