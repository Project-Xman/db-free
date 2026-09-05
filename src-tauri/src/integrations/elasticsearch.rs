// SOT: elasticsearch-integration, opensearch-integration, query-dsl, es-sql, es-mapping-flatten, es-rest-console, object-explorer, server-stats, search-playground, es-admin-actions

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, json_to_value, json_type_name, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SortRule, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use crate::model::{
    CodeLanguage, FacetCounts, FacetValue, ObjectAction, ObjectDetail, ObjectKind, ObjectRef, ObjectSummary, SearchRequest,
    SearchResult, ServerStats, Stat, StatGroup,
};
use async_trait::async_trait;
use base64::Engine as _;
use reqwest::Method;
use serde_json::{json, Map, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Elasticsearch / OpenSearch adapter over the REST API (port 9200).
//        An index is a table, an alias is a view, a hit is a row whose
//        `_source` is flattened to dotted paths so nested objects become columns.
// WHY:   Both engines speak the same core API (search, count, mapping, cat);
//        only the SQL endpoint differs (`/_sql` vs `/_plugins/_sql`), so one
//        adapter serves `Engine::Elasticsearch` and `Engine::Opensearch`.
// HOW:   The grid's filters translate to a `bool` query (term / range /
//        wildcard / terms / exists) with exact matching routed to `.keyword`
//        sub-fields when the mapping has them. `execute` accepts Query DSL
//        JSON (with an `index` hint), Kibana dev-tools style `VERB /path` +
//        body, or SQL (`SELECT …`) forwarded to the engine's SQL plugin.
// WHERE: src-tauri/src/integrations/mod.rs (trait), integrations/http.rs (client)
// ============================================================================

const DEFAULT_PORT: u16 = 9200;
const ID_FIELD: &str = "_id";
const INDEX_SCHEMA: &str = "indices";
const MAX_WINDOW: u64 = 10_000;

pub struct ElasticsearchIntegration {
    http: HttpClient,
    engine: Engine,
    read_only: bool,
    default_index: Option<String>,
}

// WHAT:  Picks the Authorization scheme from what the user typed.
//        user + secret → Basic; secret alone → `ApiKey` when it looks like an
//        Elasticsearch API key (`id:key` or its base64 form), else Bearer.
fn pick_auth(conn: &ResolvedConnection) -> Auth {
    let user = conn.summary.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    let secret = conn.secret.as_deref().map(str::trim).filter(|s| !s.is_empty());
    match (user, secret) {
        (Some(u), Some(p)) => Auth::Basic { user: u.to_string(), password: p.to_string() },
        (Some(u), None) => Auth::Basic { user: u.to_string(), password: String::new() },
        (None, Some(s)) => api_key_header(s).unwrap_or_else(|| Auth::Bearer(s.to_string())),
        (None, None) => Auth::None,
    }
}

fn api_key_header(secret: &str) -> Option<Auth> {
    let b64 = base64::engine::general_purpose::STANDARD;
    if secret.contains(':') && !secret.contains(' ') {
        return Some(Auth::Header { name: "Authorization".into(), value: format!("ApiKey {}", b64.encode(secret)) });
    }
    let decoded = b64.decode(secret).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    if text.contains(':') {
        return Some(Auth::Header { name: "Authorization".into(), value: format!("ApiKey {secret}") });
    }
    None
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, pick_auth(conn))?;
    let default_index = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let integration = ElasticsearchIntegration { http, engine: conn.summary.engine, read_only: conn.summary.read_only, default_index };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Mapping → columns
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct FieldInfo {
    type_name: String,
    /// The mapping declares a `.keyword` (or any `keyword`) sub-field.
    keyword_subfield: Option<String>,
}

// WHAT:  Flattens a `properties` tree to dotted paths in mapping order.
fn flatten_properties(props: &Json, prefix: &str, out: &mut Vec<(String, FieldInfo)>) {
    let Some(obj) = props.as_object() else { return };
    for (name, spec) in obj {
        let path = if prefix.is_empty() { name.clone() } else { format!("{prefix}.{name}") };
        let type_name = spec.get("type").and_then(Json::as_str).unwrap_or("object").to_string();
        if let Some(children) = spec.get("properties") {
            if type_name == "object" || type_name == "nested" {
                if type_name == "nested" {
                    out.push((path.clone(), FieldInfo { type_name: "nested".into(), keyword_subfield: None }));
                }
                flatten_properties(children, &path, out);
                continue;
            }
        }
        let keyword_subfield = spec.get("fields").and_then(Json::as_object).and_then(|fields| {
            fields
                .get("keyword")
                .filter(|f| f.get("type").and_then(Json::as_str) == Some("keyword"))
                .map(|_| "keyword".to_string())
                .or_else(|| {
                    fields
                        .iter()
                        .find(|(_, f)| f.get("type").and_then(Json::as_str) == Some("keyword"))
                        .map(|(n, _)| n.clone())
                })
        });
        out.push((path, FieldInfo { type_name, keyword_subfield }));
    }
}

// WHAT:  Union of every index's mapping in a `GET /{index}/_mapping` response
//        (an alias may cover several indices).
fn fields_from_mapping(body: &Json) -> Vec<(String, FieldInfo)> {
    let mut out: Vec<(String, FieldInfo)> = Vec::new();
    if let Some(indices) = body.as_object() {
        for (_, index) in indices {
            let mut local = Vec::new();
            if let Some(props) = index.pointer("/mappings/properties") {
                flatten_properties(props, "", &mut local);
            }
            for (name, info) in local {
                if !out.iter().any(|(n, _)| *n == name) {
                    out.push((name, info));
                }
            }
        }
    }
    out
}

fn columns_from_fields(fields: &[(String, FieldInfo)]) -> Vec<ColumnInfo> {
    let mut cols = vec![ColumnInfo { name: ID_FIELD.into(), data_type: "keyword".into(), nullable: false, primary_key: true, ordinal: 1 }];
    for (i, (name, info)) in fields.iter().enumerate() {
        cols.push(ColumnInfo {
            name: name.clone(),
            data_type: info.type_name.clone(),
            nullable: true,
            primary_key: false,
            ordinal: u32::try_from(i + 2).unwrap_or(u32::MAX),
        });
    }
    cols
}

// ---------------------------------------------------------------------------
// _source → row
// ---------------------------------------------------------------------------

// WHAT:  Flattens nested objects to dotted keys; arrays stay JSON cells.
fn flatten_source(value: &Json, prefix: &str, out: &mut Map<String, Json>) {
    match value {
        Json::Object(obj) => {
            for (k, v) in obj {
                let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                match v {
                    Json::Object(_) => flatten_source(v, &path, out),
                    other => {
                        out.insert(path, other.clone());
                    }
                }
            }
        }
        other if !prefix.is_empty() => {
            out.insert(prefix.to_string(), other.clone());
        }
        _ => {}
    }
}

fn hit_to_object(hit: &Json) -> Json {
    let mut obj = Map::new();
    if let Some(id) = hit.get(ID_FIELD) {
        obj.insert(ID_FIELD.into(), id.clone());
    }
    if let Some(src) = hit.get("_source") {
        flatten_source(src, "", &mut obj);
    }
    if let Some(fields) = hit.get("fields").and_then(Json::as_object) {
        for (k, v) in fields {
            obj.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
    Json::Object(obj)
}

fn hits_of(body: &Json) -> Vec<Json> {
    body.pointer("/hits/hits").and_then(Json::as_array).map(|hits| hits.iter().map(hit_to_object).collect()).unwrap_or_default()
}

fn total_of(body: &Json) -> Option<i64> {
    match body.pointer("/hits/total") {
        Some(Json::Number(n)) => n.as_i64(),
        Some(obj) => obj.get("value").and_then(Json::as_i64),
        None => None,
    }
}

// WHAT:  Search response → grid: hits when there are any, aggregations when
//        it was an aggregation-only query, else the raw body.
fn search_result(body: &Json, max_rows: usize) -> ResultSet {
    let hits = hits_of(body);
    if !hits.is_empty() {
        return objects_to_result_set(&hits, Some(ID_FIELD), max_rows);
    }
    if let Some(aggs) = body.get("aggregations") {
        return json_result(aggs.clone());
    }
    json_result(body.clone())
}

// ---------------------------------------------------------------------------
// Filter / sort → Query DSL
// ---------------------------------------------------------------------------

fn lenient_json(raw: &str) -> Json {
    let t = raw.trim();
    if t.eq_ignore_ascii_case("true") {
        return Json::Bool(true);
    }
    if t.eq_ignore_ascii_case("false") {
        return Json::Bool(false);
    }
    if let Ok(i) = t.parse::<i64>() {
        return Json::from(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Json::Number(n);
        }
    }
    Json::String(t.to_string())
}

fn escape_wildcard(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('*', "\\*").replace('?', "\\?")
}

/// Field name to use for exact / wildcard / sort operations.
fn exact_field(field: &str, fields: &[(String, FieldInfo)]) -> String {
    match fields.iter().find(|(n, _)| n == field) {
        Some((_, info)) if info.type_name == "text" => match &info.keyword_subfield {
            Some(sub) => format!("{field}.{sub}"),
            None => field.to_string(),
        },
        _ => field.to_string(),
    }
}

fn is_text_without_keyword(field: &str, fields: &[(String, FieldInfo)]) -> bool {
    fields.iter().any(|(n, info)| n == field && info.type_name == "text" && info.keyword_subfield.is_none())
}

fn filter_clause(rule: &FilterRule, fields: &[(String, FieldInfo)]) -> (Option<Json>, Option<Json>) {
    let field = rule.column.as_str();
    let value = rule.value.trim();
    let exact = exact_field(field, fields);
    let text_only = is_text_without_keyword(field, fields);
    let list = || -> Vec<Json> { value.split(',').map(str::trim).filter(|v| !v.is_empty()).map(lenient_json).collect() };
    if field == ID_FIELD {
        return match rule.op {
            FilterOp::Eq => (Some(json!({"ids": {"values": [value]}})), None),
            FilterOp::Ne => (None, Some(json!({"ids": {"values": [value]}}))),
            FilterOp::In => (Some(json!({"ids": {"values": list()}})), None),
            FilterOp::IsNull => (None, Some(json!({"match_all": {}}))),
            FilterOp::IsNotNull => (Some(json!({"match_all": {}})), None),
            _ => (Some(json!({"wildcard": {"_id": {"value": format!("*{}*", escape_wildcard(value))}}})), None),
        };
    }
    let eq = if text_only { json!({"match_phrase": {field: value}}) } else { json!({"term": {exact.clone(): lenient_json(value)}}) };
    let wildcard = |pattern: String| json!({"wildcard": {exact.clone(): {"value": pattern, "case_insensitive": true}}});
    match rule.op {
        FilterOp::Eq => (Some(eq), None),
        FilterOp::Ne => (None, Some(eq)),
        FilterOp::Gt => (Some(json!({"range": {field: {"gt": lenient_json(value)}}})), None),
        FilterOp::Gte => (Some(json!({"range": {field: {"gte": lenient_json(value)}}})), None),
        FilterOp::Lt => (Some(json!({"range": {field: {"lt": lenient_json(value)}}})), None),
        FilterOp::Lte => (Some(json!({"range": {field: {"lte": lenient_json(value)}}})), None),
        FilterOp::Contains => {
            if text_only {
                (Some(json!({"match_phrase": {field: value}})), None)
            } else {
                (Some(wildcard(format!("*{}*", escape_wildcard(value)))), None)
            }
        }
        FilterOp::StartsWith => (Some(json!({"prefix": {exact.clone(): {"value": value, "case_insensitive": true}}})), None),
        FilterOp::EndsWith => (Some(wildcard(format!("*{}", escape_wildcard(value)))), None),
        FilterOp::In => (Some(json!({"terms": {exact.clone(): list()}})), None),
        FilterOp::IsNull => (None, Some(json!({"exists": {"field": field}}))),
        FilterOp::IsNotNull => (Some(json!({"exists": {"field": field}})), None),
    }
}

fn build_query(filters: &[FilterRule], fields: &[(String, FieldInfo)]) -> Json {
    if filters.is_empty() {
        return json!({"match_all": {}});
    }
    let mut must = Vec::new();
    let mut must_not = Vec::new();
    for rule in filters {
        let (yes, no) = filter_clause(rule, fields);
        must.extend(yes);
        must_not.extend(no);
    }
    let mut bool_q = Map::new();
    if !must.is_empty() {
        bool_q.insert("filter".into(), Json::Array(must));
    }
    if !must_not.is_empty() {
        bool_q.insert("must_not".into(), Json::Array(must_not));
    }
    json!({"bool": bool_q})
}

fn build_sort(sort: &[SortRule], fields: &[(String, FieldInfo)]) -> Vec<Json> {
    sort.iter()
        .map(|s| {
            let field = if s.column == ID_FIELD { "_doc".to_string() } else { exact_field(&s.column, fields) };
            json!({field: {"order": if s.desc { "desc" } else { "asc" }, "unmapped_type": "keyword"}})
        })
        .collect()
}

fn search_body(query: &PageQuery, fields: &[(String, FieldInfo)]) -> Json {
    let mut body = json!({
        "from": query.offset,
        "size": query.limit,
        "query": build_query(&query.filters, fields),
        "track_total_hits": true,
    });
    let sort = build_sort(&query.sort, fields);
    if !sort.is_empty() {
        body["sort"] = Json::Array(sort);
    }
    body
}

// ---------------------------------------------------------------------------
// Console input parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Command {
    /// Query DSL body against an index.
    Search { index: String, body: Json },
    /// Raw REST call.
    Rest { method: String, path: String, body: Option<String> },
    /// SQL for the engine's SQL endpoint.
    Sql(String),
}

const REST_VERBS: [&str; 6] = ["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"];

fn parse_command(text: &str, default_index: Option<&str>) -> AppResult<Command> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if trimmed.starts_with('{') {
        let mut body: Json = serde_json::from_str(trimmed).map_err(|e| AppError::invalid_input(format!("Body is not valid JSON: {e}")))?;
        let index = body
            .as_object_mut()
            .and_then(|o| o.remove("index").or_else(|| o.remove("from_index")))
            .and_then(|v| v.as_str().map(str::to_string))
            .or_else(|| default_index.map(str::to_string))
            .ok_or_else(|| AppError::invalid_input("Add an \"index\" key to the JSON body (or set the connection's index)."))?;
        // Raw Query DSL (a bare `{"term": …}`) is wrapped into a search body.
        let is_search_body = body.as_object().map(|o| o.keys().any(|k| matches!(k.as_str(), "query" | "size" | "from" | "aggs" | "aggregations" | "sort" | "_source" | "fields" | "track_total_hits" | "knn"))).unwrap_or(false);
        let body = if is_search_body { body } else { json!({"query": body}) };
        return Ok(Command::Search { index, body });
    }
    let first_line = trimmed.lines().next().unwrap_or_default().trim();
    let mut parts = first_line.splitn(2, char::is_whitespace);
    let verb = parts.next().unwrap_or_default().to_ascii_uppercase();
    if REST_VERBS.contains(&verb.as_str()) {
        let path = parts.next().map(str::trim).filter(|p| !p.is_empty()).ok_or_else(|| AppError::invalid_input("Expected a path after the verb, e.g. `GET /_cat/indices`."))?;
        let rest: String = trimmed.lines().skip(1).collect::<Vec<_>>().join("\n");
        let body = if rest.trim().is_empty() { None } else { Some(rest.trim().to_string()) };
        let path = if path.starts_with('/') || path.starts_with("http") { path.to_string() } else { format!("/{path}") };
        return Ok(Command::Rest { method: verb, path, body });
    }
    let upper = trimmed.to_ascii_uppercase();
    if ["SELECT", "DESCRIBE", "DESC ", "SHOW", "WITH", "EXPLAIN"].iter().any(|kw| upper.starts_with(kw)) {
        return Ok(Command::Sql(trimmed.trim_end_matches(';').to_string()));
    }
    Err(AppError::invalid_input("Enter Query DSL JSON ({\"index\": \"…\", \"query\": {…}}), a REST call (`GET /_cat/indices`), or SQL (`SELECT …`)."))
}

fn rest_is_read(method: &str, path: &str) -> bool {
    match method {
        "GET" | "HEAD" => true,
        "POST" => {
            let p = path.split('?').next().unwrap_or_default();
            ["_search", "_count", "_sql", "_msearch", "_explain", "_validate", "_analyze", "_field_caps", "_mget", "_pit", "_async_search", "_eql", "_knn_search", "_plugins/_sql", "_plugins/_ppl", "_terms_enum"]
                .iter()
                .any(|s| p.contains(s))
        }
        _ => false,
    }
}

// WHAT:  `/_cat/...` without a format → JSON so the grid can show it.
fn cat_with_json(path: &str) -> String {
    if path.contains("/_cat") && !path.contains("format=") {
        if path.contains('?') {
            format!("{path}&format=json")
        } else {
            format!("{path}?format=json")
        }
    } else {
        path.to_string()
    }
}

fn text_result(text: &str, max_rows: usize) -> ResultSet {
    let lines: Vec<&str> = text.lines().collect();
    ResultSet {
        columns: vec![ColumnMeta { name: "output".into(), type_name: "text".into() }],
        rows: lines.iter().take(max_rows).map(|l| vec![Value::Text((*l).to_string())]).collect(),
        truncated: lines.len() > max_rows,
    }
}

// WHAT:  Both SQL response shapes: ES `{columns:[{name,type}], rows}` and
//        OpenSearch `{schema:[{name,type}], datarows}`.
fn sql_result(body: &Json, max_rows: usize) -> ResultSet {
    let cols = body.get("columns").or_else(|| body.get("schema")).and_then(Json::as_array);
    let rows = body.get("rows").or_else(|| body.get("datarows")).and_then(Json::as_array);
    match (cols, rows) {
        (Some(cols), Some(rows)) => {
            let columns: Vec<ColumnMeta> = cols
                .iter()
                .map(|c| ColumnMeta {
                    name: c.get("name").and_then(Json::as_str).unwrap_or("?").to_string(),
                    type_name: c.get("type").and_then(Json::as_str).unwrap_or("json").to_string(),
                })
                .collect();
            let data: Vec<Vec<Value>> = rows.iter().take(max_rows).map(|r| r.as_array().map(|cells| cells.iter().map(json_to_value).collect()).unwrap_or_default()).collect();
            ResultSet { columns, rows: data, truncated: rows.len() > max_rows }
        }
        _ => json_result(body.clone()),
    }
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl ElasticsearchIntegration {
    fn is_opensearch(&self) -> bool {
        self.engine == Engine::Opensearch
    }

    async fn fields(&self, index: &str) -> AppResult<Vec<(String, FieldInfo)>> {
        let body: Json = self.http.get_json(&format!("/{}/_mapping", encode_path(index))).await?;
        Ok(fields_from_mapping(&body))
    }

    async fn raw(&self, method: &str, path: &str, body: Option<String>) -> AppResult<String> {
        let method = Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Unsupported HTTP verb {method}.")))?;
        let mut req = self.http.request(method, path);
        if let Some(b) = body {
            req = req.header("Content-Type", "application/json").body(b);
        }
        let resp = self.http.send(req).await?;
        resp.text().await.map_err(|e| AppError::driver(e.to_string()))
    }

    // WHAT:  Runs SQL through whichever SQL endpoint the server provides.
    // WHY:   The two disagree about `format`. Elasticsearch needs
    //        `?format=json` to return `{columns, rows}`. OpenSearch treats
    //        `format=json` as "give me raw DSL hits" and only its default
    //        (jdbc) shape carries `{schema, datarows}` — asking for json there
    //        silently returned one opaque row instead of the result table.
    async fn run_sql(&self, sql: &str, max_rows: usize) -> AppResult<ResultSet> {
        let path = if self.is_opensearch() { "/_plugins/_sql" } else { "/_sql?format=json" };
        let body = if self.is_opensearch() { json!({"query": sql}) } else { json!({"query": sql, "fetch_size": max_rows.min(10_000)}) };
        let out: Json = self.http.post_json(path, &body).await?;
        Ok(sql_result(&out, max_rows))
    }

    async fn run_command(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        match cmd {
            Command::Search { index, mut body } => {
                if body.get("size").is_none() {
                    body["size"] = Json::from(max_rows.min(MAX_WINDOW as usize));
                }
                let out: Json = self.http.post_json(&format!("/{}/_search", encode_path(&index)), &body).await?;
                Ok(StatementResult::Rows { result: search_result(&out, max_rows) })
            }
            Command::Rest { method, path, body } => {
                if self.read_only && !rest_is_read(&method, &path) {
                    return Err(AppError::invalid_input(format!("{method} {path} is refused: this connection is read-only.")));
                }
                let path = cat_with_json(&path);
                let text = self.raw(&method, &path, body).await?;
                let parsed: Option<Json> = serde_json::from_str(&text).ok();
                match parsed {
                    Some(v) if v.pointer("/hits/hits").is_some() => Ok(StatementResult::Rows { result: search_result(&v, max_rows) }),
                    Some(v) => {
                        let is_write = !matches!(method.as_str(), "GET" | "HEAD") && !rest_is_read(&method, &path);
                        if is_write {
                            let affected = v.get("total").or_else(|| v.get("deleted")).or_else(|| v.get("updated")).and_then(Json::as_u64);
                            if let Some(n) = affected {
                                return Ok(StatementResult::Affected { rows_affected: n });
                            }
                        }
                        Ok(StatementResult::Rows { result: json_result(v) })
                    }
                    None => Ok(StatementResult::Rows { result: text_result(&text, max_rows) }),
                }
            }
            Command::Sql(sql) => Ok(StatementResult::Rows { result: self.run_sql(&sql, max_rows).await? }),
        }
    }
}

fn encode_path(segment: &str) -> String {
    segment.replace('/', "%2F").replace(' ', "%20")
}

// ---------------------------------------------------------------------------
// Object explorer / server stats / search playground
//
// WHAT:  `objects()` answers every kind in `profile()` from the `_cat` and
//        management APIs; `object_detail()` adds the JSON definition, a
//        property sheet and console actions (`POST /idx/_refresh`, `DELETE
//        /idx`…) that run back through `execute`; `server_stats()` folds
//        `_cluster/health`, `_cluster/stats` and `_nodes/stats`; `search()` is
//        the playground (`query_string` + filter + terms facets + highlight).
// WHY:   Both engines share these endpoints; only the security and policy
//        plugins differ (`_security/*` vs `_plugins/_security/api/*`,
//        `_ilm/policy` vs `_plugins/_ism/policies`), so the paths are picked
//        by `is_opensearch()` and 403/404 answers degrade gracefully.
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const FACET_SIZE: u32 = 20;
const SCORE_FIELD: &str = "_score";
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

// WHAT:  `_cat` numbers arrive as strings, stats as numbers; accept both.
fn num_at(v: &Json, key: &str) -> Option<f64> {
    let node = if key.starts_with('/') { v.pointer(key) } else { v.get(key) }?;
    node.as_f64().or_else(|| node.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
}

fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

fn str_list(v: Option<&Json>) -> Vec<String> {
    v.and_then(Json::as_array).map(|a| a.iter().map(text_of).filter(|s| !s.is_empty()).collect()).unwrap_or_default()
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

fn duration_text(ms: f64) -> String {
    let secs = ms / 1000.0;
    if secs < 60.0 {
        format!("{secs:.1} s")
    } else if secs < 3600.0 {
        format!("{:.0} min", secs / 60.0)
    } else if secs < 86_400.0 {
        format!("{:.1} h", secs / 3600.0)
    } else {
        format!("{:.1} d", secs / 86_400.0)
    }
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

// WHAT:  `_cat/indices` rows → summaries. Dot-prefixed (system) indices are
//        kept but chipped `system`; everything else carries its health.
fn index_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    let list = rows
        .iter()
        .filter_map(|r| {
            let name = r.get("index").and_then(Json::as_str)?;
            let mut parts = Vec::new();
            if let Some(docs) = num_at(r, "docs.count") {
                parts.push(format!("{} docs", crate::model::objects::format_number(docs)));
            }
            let size = str_at(r, "store.size");
            if !size.is_empty() {
                parts.push(size.to_string());
            }
            let status = str_at(r, "status");
            if status == "close" {
                parts.push("closed".into());
            }
            let health = str_at(r, "health");
            let badge = if name.starts_with('.') { "system".to_string() } else { health.to_string() };
            Some(summary(ObjectKind::Index, name, None, parts.join(" · "), Some(badge)))
        })
        .collect();
    finish(list)
}

fn alias_summaries(rows: &[Json], parent: Option<&str>) -> Vec<ObjectSummary> {
    let list = rows
        .iter()
        .filter_map(|r| {
            let alias = r.get("alias").and_then(Json::as_str)?;
            let index = str_at(r, "index");
            if parent.is_some_and(|p| p != index) {
                return None;
            }
            let mut parts = vec![format!("→ {index}")];
            if str_at(r, "is_write_index") == "true" {
                parts.push("write index".into());
            }
            if str_at(r, "filter") == "*" {
                parts.push("filtered".into());
            }
            Some(summary(ObjectKind::Alias, alias, Some(index), parts.join(" · "), None))
        })
        .collect();
    finish(list)
}

// WHAT:  Composable (`_index_template`), legacy (`_template`) and component
//        (`_component_template`) templates in one list, chipped by kind.
fn template_summaries(composable: &Json, legacy: &Json, component: &Json) -> Vec<ObjectSummary> {
    let mut list = Vec::new();
    for t in composable.get("index_templates").and_then(Json::as_array).into_iter().flatten() {
        let Some(name) = t.get("name").and_then(Json::as_str) else { continue };
        let spec = t.get("index_template").cloned().unwrap_or(Json::Null);
        let mut parts = vec![str_list(spec.get("index_patterns")).join(", ")];
        if let Some(p) = spec.get("priority").and_then(Json::as_i64) {
            parts.push(format!("priority {p}"));
        }
        list.push(summary(ObjectKind::Template, name, None, parts.join(" · "), Some("composable".into())));
    }
    if let Some(obj) = legacy.as_object() {
        for (name, spec) in obj {
            let mut parts = vec![str_list(spec.get("index_patterns")).join(", ")];
            if let Some(o) = spec.get("order").and_then(Json::as_i64) {
                parts.push(format!("order {o}"));
            }
            list.push(summary(ObjectKind::Template, name, None, parts.join(" · "), Some("legacy".into())));
        }
    }
    for t in component.get("component_templates").and_then(Json::as_array).into_iter().flatten() {
        let Some(name) = t.get("name").and_then(Json::as_str) else { continue };
        let spec = t.get("component_template").cloned().unwrap_or(Json::Null);
        let keys: Vec<String> = spec.get("template").and_then(Json::as_object).map(|o| o.keys().cloned().collect()).unwrap_or_default();
        list.push(summary(ObjectKind::Template, name, None, keys.join(", "), Some("component".into())));
    }
    finish(list)
}

fn pipeline_summaries(body: &Json) -> Vec<ObjectSummary> {
    let Some(obj) = body.as_object() else { return Vec::new() };
    let list = obj
        .iter()
        .map(|(id, spec)| {
            let n = spec.get("processors").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let mut parts = vec![format!("{n} processors")];
            let desc = str_at(spec, "description");
            if !desc.is_empty() {
                parts.push(desc.to_string());
            }
            let badge = spec.pointer("/_meta/managed").and_then(Json::as_bool).filter(|m| *m).map(|_| "managed".to_string());
            summary(ObjectKind::Pipeline, id, None, parts.join(" · "), badge)
        })
        .collect();
    finish(list)
}

// WHAT:  ILM (`{name: {policy: {phases}}}`) and ISM (`{policies: [{_id, policy}]}`).
fn policy_summaries(ilm: &Json, ism: &Json) -> Vec<ObjectSummary> {
    let mut list = Vec::new();
    if let Some(obj) = ilm.as_object() {
        for (name, spec) in obj {
            let phases: Vec<String> = spec.pointer("/policy/phases").and_then(Json::as_object).map(|p| p.keys().cloned().collect()).unwrap_or_default();
            let used = spec.pointer("/in_use_by/indices").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let mut parts = vec![format!("phases: {}", phases.join(", "))];
            if used > 0 {
                parts.push(format!("{used} indices"));
            }
            list.push(summary(ObjectKind::Policy, name, None, parts.join(" · "), Some("ilm".into())));
        }
    }
    for p in ism.get("policies").and_then(Json::as_array).into_iter().flatten() {
        let Some(id) = p.get("_id").and_then(Json::as_str) else { continue };
        let spec = p.get("policy").cloned().unwrap_or(Json::Null);
        let states = spec.get("states").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
        let mut parts = vec![format!("{states} states")];
        let desc = str_at(&spec, "description");
        if !desc.is_empty() {
            parts.push(desc.to_string());
        }
        list.push(summary(ObjectKind::Policy, id, None, parts.join(" · "), Some("ism".into())));
    }
    finish(list)
}

// WHAT:  `_cat/nodes` role letters → one chip; the elected master (`*`) wins.
fn node_badge(roles: &str, master: &str) -> String {
    if master.trim() == "*" {
        return "master".into();
    }
    let r = roles.trim();
    if r == "-" || r.is_empty() {
        return "coordinating".into();
    }
    if ['d', 'h', 'w', 'c', 's', 'f'].iter().any(|c| r.contains(*c)) {
        return "data".into();
    }
    if r.contains('m') {
        return "master-eligible".into();
    }
    if r.contains('i') {
        return "ingest".into();
    }
    if r.contains('l') {
        return "ml".into();
    }
    r.to_string()
}

fn node_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    let list = rows
        .iter()
        .filter_map(|r| {
            let name = r.get("name").and_then(Json::as_str)?;
            let mut parts = Vec::new();
            for (key, label) in [("ip", ""), ("heap.percent", "heap "), ("cpu", "cpu "), ("load_1m", "load ")] {
                let v = str_at(r, key);
                if !v.is_empty() {
                    let suffix = if key == "heap.percent" || key == "cpu" { "%" } else { "" };
                    parts.push(format!("{label}{v}{suffix}"));
                }
            }
            let version = str_at(r, "version");
            if !version.is_empty() {
                parts.push(format!("v{version}"));
            }
            let roles = str_at(r, "node.role");
            if !roles.is_empty() {
                parts.push(format!("roles {roles}"));
            }
            Some(summary(ObjectKind::Node, name, None, parts.join(" · "), Some(node_badge(roles, str_at(r, "master")))))
        })
        .collect();
    finish(list)
}

fn shard_name(shard: &str, prirep: &str) -> String {
    format!("{shard}{}", if prirep == "p" { "p" } else { "r" })
}

// WHAT:  `"0p"` → (0, primary = true).
fn parse_shard_name(name: &str) -> Option<(u64, bool)> {
    let (num, kind) = name.trim().split_at(name.trim().len().checked_sub(1)?);
    let primary = match kind {
        "p" => true,
        "r" => false,
        _ => return None,
    };
    num.parse::<u64>().ok().map(|n| (n, primary))
}

fn shard_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    let list = rows
        .iter()
        .filter_map(|r| {
            let index = r.get("index").and_then(Json::as_str)?;
            let shard = r.get("shard").map(text_of)?;
            let name = shard_name(&shard, str_at(r, "prirep"));
            let mut parts = Vec::new();
            if let Some(docs) = num_at(r, "docs") {
                parts.push(format!("{} docs", crate::model::objects::format_number(docs)));
            }
            for key in ["store", "node", "unassigned.reason"] {
                let v = str_at(r, key);
                if !v.is_empty() {
                    parts.push(v.to_string());
                }
            }
            let state = str_at(r, "state").to_ascii_lowercase();
            Some(summary(ObjectKind::Shard, &name, Some(index), parts.join(" · "), Some(state)))
        })
        .collect();
    finish(list)
}

fn task_summaries(body: &Json) -> Vec<ObjectSummary> {
    let mut list = Vec::new();
    for node in body.get("nodes").and_then(Json::as_object).into_iter().flat_map(|n| n.values()) {
        for (id, t) in node.get("tasks").and_then(Json::as_object).into_iter().flatten() {
            let mut parts = vec![str_at(t, "action").to_string()];
            if let Some(nanos) = t.get("running_time_in_nanos").and_then(Json::as_f64) {
                parts.push(duration_text(nanos / 1_000_000.0));
            }
            let desc = str_at(t, "description");
            if !desc.is_empty() {
                parts.push(desc.chars().take(80).collect());
            }
            let badge = if t.get("cancellable").and_then(Json::as_bool) == Some(true) { "cancellable" } else { str_at(t, "type") };
            list.push(summary(ObjectKind::Task, id, None, parts.join(" · "), Some(badge.to_string())));
        }
    }
    finish(list)
}

fn snapshot_summaries(repo: &str, body: &Json) -> Vec<ObjectSummary> {
    body.get("snapshots")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|s| {
            let name = s.get("snapshot").and_then(Json::as_str)?;
            let indices = s.get("indices").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let mut parts = vec![format!("{indices} indices")];
            let start = str_at(s, "start_time");
            if !start.is_empty() {
                parts.push(start.to_string());
            }
            if let Some(ms) = s.get("duration_in_millis").and_then(Json::as_f64) {
                parts.push(duration_text(ms));
            }
            let state = str_at(s, "state").to_ascii_lowercase();
            Some(summary(ObjectKind::Snapshot, name, Some(repo), parts.join(" · "), Some(state)))
        })
        .collect()
}

// WHAT:  Elasticsearch (`roles`, `full_name`, `enabled`, `metadata._reserved`)
//        and OpenSearch internal users (`backend_roles`, `reserved`, `hidden`).
fn user_summaries(body: &Json) -> Vec<ObjectSummary> {
    let Some(obj) = body.as_object() else { return Vec::new() };
    let list = obj
        .iter()
        .map(|(name, spec)| {
            let roles = str_list(spec.get("roles").or_else(|| spec.get("backend_roles")));
            let mut detail = roles.join(", ");
            if let Some(full) = spec.get("full_name").and_then(Json::as_str).filter(|s| !s.is_empty()) {
                detail = if detail.is_empty() { full.to_string() } else { format!("{full} · {detail}") };
            }
            let flag = |key: &str| spec.pointer(key).and_then(Json::as_bool) == Some(true);
            let badge = if spec.get("enabled").and_then(Json::as_bool) == Some(false) {
                Some("disabled")
            } else if flag("/reserved") || flag("/metadata/_reserved") {
                Some("reserved")
            } else if flag("/hidden") {
                Some("hidden")
            } else {
                None
            };
            summary(ObjectKind::User, name, None, detail, badge.map(str::to_string))
        })
        .collect();
    finish(list)
}

// WHAT:  Elasticsearch (`cluster`, `indices[].names`) and OpenSearch
//        (`cluster_permissions`, `index_permissions[].index_patterns`).
fn role_summaries(body: &Json) -> Vec<ObjectSummary> {
    let Some(obj) = body.as_object() else { return Vec::new() };
    let list = obj
        .iter()
        .map(|(name, spec)| {
            let cluster = str_list(spec.get("cluster").or_else(|| spec.get("cluster_permissions")));
            let indices = spec.get("indices").or_else(|| spec.get("index_permissions")).and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let mut parts = Vec::new();
            if !cluster.is_empty() {
                let shown: Vec<&str> = cluster.iter().take(4).map(String::as_str).collect();
                parts.push(format!("cluster: {}{}", shown.join(", "), if cluster.len() > 4 { "…" } else { "" }));
            }
            parts.push(format!("{indices} index grants"));
            let flag = |key: &str| spec.pointer(key).and_then(Json::as_bool) == Some(true);
            let badge = if flag("/reserved") || flag("/metadata/_reserved") { Some("reserved".to_string()) } else if flag("/hidden") { Some("hidden".to_string()) } else { None };
            summary(ObjectKind::Role, name, None, parts.join(" · "), badge)
        })
        .collect();
    finish(list)
}

fn index_actions(name: &str, closed: bool) -> Vec<ObjectAction> {
    let p = encode_path(name);
    let mut acts = vec![
        ObjectAction::new("refresh", "Refresh", format!("POST /{p}/_refresh")),
        ObjectAction::new("flush", "Flush", format!("POST /{p}/_flush")),
        ObjectAction::new("forcemerge", "Force merge (1 segment)", format!("POST /{p}/_forcemerge?max_num_segments=1")),
        ObjectAction::new("clear-cache", "Clear cache", format!("POST /{p}/_cache/clear")),
    ];
    if closed {
        acts.push(ObjectAction::new("open", "Open", format!("POST /{p}/_open")));
    } else {
        acts.push(ObjectAction::destructive("close", "Close", format!("POST /{p}/_close")));
    }
    acts.push(ObjectAction::destructive("delete", "Delete index", format!("DELETE /{p}")));
    acts
}

fn rows_table(columns: &[(&str, &str)], rows: Vec<Vec<Value>>) -> ResultSet {
    ResultSet {
        columns: columns.iter().map(|(name, ty)| ColumnMeta { name: (*name).to_string(), type_name: (*ty).to_string() }).collect(),
        rows,
        truncated: false,
    }
}

fn cell(v: Option<&Json>) -> Value {
    match v {
        None | Some(Json::Null) => Value::Null,
        Some(Json::String(s)) => Value::Text(s.clone()),
        Some(Json::Array(a)) if a.iter().all(Json::is_string) => Value::Text(a.iter().filter_map(Json::as_str).collect::<Vec<_>>().join(", ")),
        Some(other) => json_to_value(other),
    }
}

// ---- search playground ------------------------------------------------------

// WHAT:  Playground request → Query DSL. Free text goes through
//        `query_string` (`*` / empty = match_all); the filter is either raw
//        Query DSL JSON (`{…}`) or another query_string; facets become terms
//        aggregations on the keyword form of the field; sort entries are
//        `field[:asc|desc]`; highlight covers every field.
fn playground_body(req: &SearchRequest, fields: &[(String, FieldInfo)]) -> AppResult<Json> {
    if u64::from(req.offset) + u64::from(req.limit) > MAX_WINDOW {
        return Err(AppError::invalid_input(format!("Elasticsearch only pages through the first {MAX_WINDOW} hits (index.max_result_window).")));
    }
    let q = req.query.trim();
    let main = if q.is_empty() || q == "*" { json!({"match_all": {}}) } else { json!({"query_string": {"query": q}}) };
    let mut bool_q = Map::new();
    bool_q.insert("must".into(), json!([main]));
    if let Some(f) = req.filter.as_deref().map(str::trim).filter(|f| !f.is_empty()) {
        let clause = if f.starts_with('{') {
            let parsed: Json = serde_json::from_str(f).map_err(|e| AppError::invalid_input(format!("Filter is not valid Query DSL JSON: {e}")))?;
            match parsed.get("query") {
                Some(inner) if parsed.as_object().map(|o| o.len() == 1).unwrap_or(false) => inner.clone(),
                _ => parsed,
            }
        } else {
            json!({"query_string": {"query": f}})
        };
        bool_q.insert("filter".into(), json!([clause]));
    }
    let mut body = json!({
        "from": req.offset,
        "size": req.limit,
        "query": {"bool": bool_q},
        "track_total_hits": true,
    });
    let mut aggs = Map::new();
    for facet in req.facets.iter().map(|f| f.trim()).filter(|f| !f.is_empty()) {
        aggs.insert(facet.to_string(), json!({"terms": {"field": exact_field(facet, fields), "size": FACET_SIZE}}));
    }
    if !aggs.is_empty() {
        body["aggs"] = Json::Object(aggs);
    }
    let sort: Vec<Json> = req
        .sort
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            let (field, order) = match s.rsplit_once(':') {
                Some((f, o)) if matches!(o.trim().to_ascii_lowercase().as_str(), "asc" | "desc") => (f.trim(), o.trim().to_ascii_lowercase()),
                _ => (s, "asc".to_string()),
            };
            if field == SCORE_FIELD || field == "_doc" {
                json!({field: {"order": order}})
            } else {
                json!({exact_field(field, fields): {"order": order, "unmapped_type": "keyword"}})
            }
        })
        .collect();
    if !sort.is_empty() {
        body["sort"] = Json::Array(sort);
    }
    if req.highlight {
        body["highlight"] = json!({"fields": {"*": {}}, "pre_tags": ["<em>"], "post_tags": ["</em>"]});
    }
    Ok(body)
}

// WHAT:  Search response → hits grid (`_id`, `_score`, source fields,
//        `_highlight`), facet counts from the terms aggregations, total, took.
fn playground_result(body: &Json, facets: &[String], highlight: bool) -> SearchResult {
    let raw: Vec<&Json> = body.pointer("/hits/hits").and_then(Json::as_array).map(|h| h.iter().collect()).unwrap_or_default();
    let flat: Vec<Json> = raw.iter().map(|h| hit_to_object(h)).collect();
    let mut names: Vec<String> = vec![ID_FIELD.to_string(), SCORE_FIELD.to_string()];
    for obj in flat.iter().filter_map(Json::as_object) {
        for k in obj.keys() {
            if !names.iter().any(|n| n == k) {
                names.push(k.clone());
            }
        }
    }
    if highlight {
        names.push(HIGHLIGHT_FIELD.to_string());
    }
    let rows: Vec<Vec<Value>> = raw
        .iter()
        .zip(&flat)
        .map(|(hit, obj)| {
            let map = obj.as_object();
            names
                .iter()
                .map(|n| match n.as_str() {
                    SCORE_FIELD => hit.get("_score").map(json_to_value).unwrap_or(Value::Null),
                    HIGHLIGHT_FIELD => hit.get("highlight").filter(|h| !h.is_null()).map(|h| Value::Json(h.clone())).unwrap_or(Value::Null),
                    other => map.and_then(|m| m.get(other)).map(json_to_value).unwrap_or(Value::Null),
                })
                .collect()
        })
        .collect();
    let columns = names
        .iter()
        .map(|n| {
            let type_name = match n.as_str() {
                ID_FIELD => "keyword",
                SCORE_FIELD => "float",
                HIGHLIGHT_FIELD => "object",
                other => flat.iter().find_map(|d| d.get(other).filter(|v| !v.is_null()).map(json_type_name)).unwrap_or("json"),
            };
            ColumnMeta { name: n.clone(), type_name: type_name.to_string() }
        })
        .collect();
    let facets = facets
        .iter()
        .map(|f| f.trim())
        .filter(|f| !f.is_empty())
        .filter_map(|f| {
            let buckets = body.get("aggregations")?.get(f)?.get("buckets")?.as_array()?;
            let values = buckets
                .iter()
                .map(|b| FacetValue {
                    value: b.get("key_as_string").and_then(Json::as_str).map(str::to_string).unwrap_or_else(|| text_of(b.get("key").unwrap_or(&Json::Null))),
                    count: b.get("doc_count").and_then(Json::as_u64).unwrap_or(0),
                })
                .collect();
            Some(FacetCounts { field: f.to_string(), values })
        })
        .collect();
    SearchResult {
        hits: ResultSet { columns, rows, truncated: false },
        total: total_of(body).and_then(|t| u64::try_from(t).ok()),
        facets,
        took_ms: body.get("took").and_then(Json::as_u64),
    }
}

// ---- server stats -----------------------------------------------------------

// WHAT:  `_cluster/health` + `_cluster/stats` + `_nodes/stats` → stat groups.
//        Per-node counters are summed (search / index / http totals), cpu is
//        averaged, uptime is the oldest node.
fn stats_groups(health: &Json, cluster: &Json, nodes: &Json) -> Vec<StatGroup> {
    let mut server = vec![Stat::text("Cluster", str_at(health, "cluster_name"))];
    if let Some(status) = health.get("status").and_then(Json::as_str) {
        server.push(Stat::text("Status", status));
    }
    let versions = str_list(cluster.pointer("/nodes/versions"));
    if !versions.is_empty() {
        server.push(Stat::text("Version", versions.join(", ")));
    }
    if let Some(up) = num_at(cluster, "/nodes/jvm/max_uptime_in_millis") {
        server.push(Stat::text("Uptime", duration_text(up)));
    }
    let mut cluster_group = Vec::new();
    for (label, key) in [
        ("Nodes", "number_of_nodes"),
        ("Data nodes", "number_of_data_nodes"),
        ("Primary shards", "active_primary_shards"),
        ("Active shards", "active_shards"),
        ("Relocating", "relocating_shards"),
        ("Initializing", "initializing_shards"),
        ("Unassigned", "unassigned_shards"),
        ("Pending tasks", "number_of_pending_tasks"),
    ] {
        if let Some(v) = num_at(health, key) {
            cluster_group.push(Stat::number(label, v, None));
        }
    }
    if let Some(v) = num_at(health, "active_shards_percent_as_number") {
        cluster_group.push(Stat::number("Active shards", v, Some("%")));
    }
    let mut storage = Vec::new();
    for (label, key) in [("Indices", "/indices/count"), ("Documents", "/indices/docs/count"), ("Deleted", "/indices/docs/deleted"), ("Segments", "/indices/segments/count")] {
        if let Some(v) = num_at(cluster, key) {
            storage.push(Stat::number(label, v, None));
        }
    }
    for (label, key) in [("Store size", "/indices/store/size_in_bytes"), ("Disk total", "/nodes/fs/total_in_bytes"), ("Disk free", "/nodes/fs/free_in_bytes")] {
        if let Some(v) = num_at(cluster, key) {
            storage.push(bytes_stat(label, v));
        }
    }
    let mut memory = Vec::new();
    let heap_used = num_at(cluster, "/nodes/jvm/mem/heap_used_in_bytes");
    let heap_max = num_at(cluster, "/nodes/jvm/mem/heap_max_in_bytes");
    if let Some(u) = heap_used {
        memory.push(bytes_stat("Heap used", u));
    }
    if let Some(m) = heap_max {
        memory.push(bytes_stat("Heap max", m));
    }
    if let (Some(u), Some(m)) = (heap_used, heap_max) {
        if m > 0.0 {
            memory.push(Stat::number("Heap", (u / m * 100.0).round(), Some("%")));
        }
    }
    if let Some(v) = num_at(cluster, "/nodes/os/mem/used_percent") {
        memory.push(Stat::number("OS memory", v, Some("%")));
    }
    for (label, key) in [("Field data", "/indices/fielddata/memory_size_in_bytes"), ("Query cache", "/indices/query_cache/memory_size_in_bytes"), ("Segments memory", "/indices/segments/memory_in_bytes")] {
        if let Some(v) = num_at(cluster, key) {
            memory.push(bytes_stat(label, v));
        }
    }
    let hits = num_at(cluster, "/indices/query_cache/hit_count");
    let misses = num_at(cluster, "/indices/query_cache/miss_count");
    if let (Some(h), Some(m)) = (hits, misses) {
        if h + m > 0.0 {
            memory.push(Stat::number("Query cache hit", (h / (h + m) * 100.0).round(), Some("%")));
        }
    }
    let mut sums: Vec<(&str, &str, f64)> = vec![
        ("Search queries", "/indices/search/query_total", 0.0),
        ("Search time", "/indices/search/query_time_in_millis", 0.0),
        ("Fetches", "/indices/search/fetch_total", 0.0),
        ("Index ops", "/indices/indexing/index_total", 0.0),
        ("Index time", "/indices/indexing/index_time_in_millis", 0.0),
        ("Get ops", "/indices/get/total", 0.0),
        ("HTTP open", "/http/current_open", 0.0),
        ("HTTP opened", "/http/total_opened", 0.0),
        ("Search rejected", "/thread_pool/search/rejected", 0.0),
        ("Write rejected", "/thread_pool/write/rejected", 0.0),
        ("Open files", "/process/open_file_descriptors", 0.0),
    ];
    let mut cpu_sum = 0.0;
    let mut cpu_n = 0.0;
    for node in nodes.get("nodes").and_then(Json::as_object).into_iter().flat_map(|n| n.values()) {
        for (_, key, total) in sums.iter_mut() {
            if let Some(v) = num_at(node, key) {
                *total += v;
            }
        }
        if let Some(c) = num_at(node, "/os/cpu/percent") {
            cpu_sum += c;
            cpu_n += 1.0;
        }
    }
    let mut throughput = Vec::new();
    for (label, key, total) in &sums {
        if key.ends_with("_in_millis") {
            throughput.push(Stat::text(label, duration_text(*total)));
        } else if *label == "Open files" {
            continue;
        } else {
            throughput.push(Stat::number(label, *total, None));
        }
    }
    let mut os = Vec::new();
    if cpu_n > 0.0 {
        os.push(Stat::number("CPU", (cpu_sum / cpu_n).round(), Some("%")));
    } else if let Some(c) = num_at(cluster, "/nodes/process/cpu/percent") {
        os.push(Stat::number("CPU", c, Some("%")));
    }
    if let Some((_, _, files)) = sums.iter().find(|(l, _, _)| *l == "Open files") {
        if *files > 0.0 {
            os.push(Stat::number("Open files", *files, None));
        }
    }
    let mut groups = vec![StatGroup { title: "Server".into(), stats: server }, StatGroup { title: "Cluster".into(), stats: cluster_group }];
    for (title, stats) in [("Storage", storage), ("Memory", memory), ("Throughput", throughput), ("OS", os)] {
        if !stats.is_empty() {
            groups.push(StatGroup { title: title.into(), stats });
        }
    }
    groups
}

impl ElasticsearchIntegration {
    fn security_path(&self, what: ObjectKind, name: Option<&str>) -> String {
        let base = match (self.is_opensearch(), what) {
            (true, ObjectKind::User) => "/_plugins/_security/api/internalusers",
            (true, _) => "/_plugins/_security/api/roles",
            (false, ObjectKind::User) => "/_security/user",
            (false, _) => "/_security/role",
        };
        match name {
            Some(n) => format!("{base}/{}", encode_path(n)),
            None => base.to_string(),
        }
    }

    // WHAT:  403 → a hint about the missing privilege; anything else (plugin
    //        absent, security disabled) → an empty list.
    fn security_list(what: &str, result: AppResult<Json>, map: fn(&Json) -> Vec<ObjectSummary>) -> AppResult<Vec<ObjectSummary>> {
        match result {
            Ok(body) => Ok(map(&body)),
            Err(AppError::NotConnected { .. }) => Err(AppError::invalid_input(format!("Listing {what} needs the manage_security privilege (the server refused the request)."))),
            Err(_) => Ok(Vec::new()),
        }
    }

    async fn cat(&self, path: &str) -> AppResult<Vec<Json>> {
        self.http.get_json(path).await
    }

    async fn list_indices(&self) -> AppResult<Vec<ObjectSummary>> {
        let rows = self.cat("/_cat/indices?format=json&h=index,health,status,docs.count,store.size&s=index").await?;
        Ok(index_summaries(&rows))
    }

    async fn list_aliases(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let rows = self.cat("/_cat/aliases?format=json&h=alias,index,filter,is_write_index&s=alias").await?;
        Ok(alias_summaries(&rows, parent))
    }

    async fn list_templates(&self) -> AppResult<Vec<ObjectSummary>> {
        let composable: Json = self.http.get_json("/_index_template").await.unwrap_or(Json::Null);
        let legacy: Json = self.http.get_json("/_template").await.unwrap_or(Json::Null);
        let component: Json = self.http.get_json("/_component_template").await.unwrap_or(Json::Null);
        Ok(template_summaries(&composable, &legacy, &component))
    }

    async fn list_pipelines(&self) -> AppResult<Vec<ObjectSummary>> {
        let body: Json = match self.http.get_json("/_ingest/pipeline").await {
            Ok(b) => b,
            Err(AppError::NotFound { .. }) => Json::Null,
            Err(e) => return Err(e),
        };
        Ok(pipeline_summaries(&body))
    }

    async fn list_policies(&self) -> AppResult<Vec<ObjectSummary>> {
        let (ilm, ism) = if self.is_opensearch() {
            (Json::Null, self.http.get_json::<Json>("/_plugins/_ism/policies").await.unwrap_or(Json::Null))
        } else {
            (self.http.get_json::<Json>("/_ilm/policy").await.unwrap_or(Json::Null), Json::Null)
        };
        Ok(policy_summaries(&ilm, &ism))
    }

    async fn list_nodes(&self) -> AppResult<Vec<ObjectSummary>> {
        let rows = self.cat("/_cat/nodes?format=json&h=name,ip,node.role,heap.percent,cpu,load_1m,master,version&s=name").await?;
        Ok(node_summaries(&rows))
    }

    async fn list_shards(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let scope = parent.map(|p| format!("/{}", encode_path(p))).unwrap_or_default();
        let rows = self.cat(&format!("/_cat/shards{scope}?format=json&h=index,shard,prirep,state,docs,store,node,unassigned.reason&s=index,shard")).await?;
        Ok(shard_summaries(&rows))
    }

    async fn list_tasks(&self) -> AppResult<Vec<ObjectSummary>> {
        let body: Json = self.http.get_json("/_tasks?detailed=true").await?;
        Ok(task_summaries(&body))
    }

    async fn repositories(&self) -> Vec<String> {
        let body: Json = self.http.get_json("/_snapshot/_all").await.unwrap_or(Json::Null);
        let mut names: Vec<String> = match &body {
            Json::Object(obj) => obj.keys().cloned().collect(),
            Json::Array(items) => items.iter().filter_map(|r| r.get("name").and_then(Json::as_str).map(str::to_string)).collect(),
            _ => Vec::new(),
        };
        names.sort();
        names
    }

    async fn list_snapshots(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let repos = match parent {
            Some(r) => vec![r.to_string()],
            None => self.repositories().await,
        };
        let mut list = Vec::new();
        for repo in repos {
            let body: Json = self.http.get_json(&format!("/_snapshot/{}/_all", encode_path(&repo))).await.unwrap_or(Json::Null);
            list.extend(snapshot_summaries(&repo, &body));
        }
        Ok(finish(list))
    }

    async fn list_users(&self) -> AppResult<Vec<ObjectSummary>> {
        let result = self.http.get_json::<Json>(&self.security_path(ObjectKind::User, None)).await;
        Self::security_list("users", result, user_summaries)
    }

    async fn list_roles(&self) -> AppResult<Vec<ObjectSummary>> {
        let result = self.http.get_json::<Json>(&self.security_path(ObjectKind::Role, None)).await;
        Self::security_list("roles", result, role_summaries)
    }

    // ---- details ----

    async fn index_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let path = encode_path(name);
        let info: Json = self.http.get_json(&format!("/{path}")).await?;
        let spec = info.get(name).cloned().or_else(|| info.as_object().and_then(|o| o.values().next().cloned())).unwrap_or(Json::Null);
        let cat = self.cat(&format!("/_cat/indices/{path}?format=json&h=health,status,docs.count,docs.deleted,store.size,pri,rep,creation.date.string,uuid")).await.unwrap_or_default();
        let row = cat.first().cloned().unwrap_or(Json::Null);
        let fields = fields_from_mapping(&info);
        let definition = json!({"aliases": spec.get("aliases"), "mappings": spec.get("mappings"), "settings": spec.get("settings")});
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&definition), CodeLanguage::Json);
        for (label, key) in [
            ("Health", "health"),
            ("Status", "status"),
            ("Documents", "docs.count"),
            ("Deleted", "docs.deleted"),
            ("Store size", "store.size"),
            ("Primary shards", "pri"),
            ("Replicas", "rep"),
            ("Created", "creation.date.string"),
            ("UUID", "uuid"),
        ] {
            let v = str_at(&row, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        if let Some(v) = spec.pointer("/settings/index/version/created").and_then(Json::as_str) {
            detail = detail.property("Version created", v);
        }
        detail.columns = columns_from_fields(&fields);
        let mut children = Vec::new();
        for (alias, cfg) in spec.get("aliases").and_then(Json::as_object).into_iter().flatten() {
            let write = cfg.get("is_write_index").and_then(Json::as_bool).unwrap_or(false);
            children.push(summary(ObjectKind::Alias, alias, Some(name), if write { "write index".into() } else { String::new() }, None));
        }
        children.extend(self.list_shards(Some(name)).await.unwrap_or_default());
        detail.children = children;
        detail.actions = index_actions(name, str_at(&row, "status") == "close");
        Ok(detail)
    }

    async fn alias_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let body: Json = self.http.get_json(&format!("/_alias/{}", encode_path(name))).await?;
        let mut rows = Vec::new();
        let mut indices = Vec::new();
        for (index, spec) in body.as_object().into_iter().flatten() {
            let cfg = spec.pointer(&format!("/aliases/{name}")).cloned().unwrap_or(Json::Null);
            indices.push(index.clone());
            rows.push(vec![
                Value::Text(index.clone()),
                Value::Bool(cfg.get("is_write_index").and_then(Json::as_bool).unwrap_or(false)),
                cfg.get("filter").map(|f| Value::Json(f.clone())).unwrap_or(Value::Null),
                cell(cfg.get("index_routing").or_else(|| cfg.get("routing"))),
            ]);
        }
        indices.sort();
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&body), CodeLanguage::Json).property("Indices", indices.join(", "));
        detail.rows = Some(rows_table(&[("index", "keyword"), ("is_write_index", "boolean"), ("filter", "object"), ("routing", "keyword")], rows));
        if !indices.is_empty() {
            let statement = format!("POST /_aliases\n{}", json!({"actions": [{"remove": {"indices": indices, "alias": name}}]}));
            detail = detail.action(ObjectAction::destructive("remove", "Remove alias", statement));
        }
        Ok(detail)
    }

    async fn template_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let p = encode_path(name);
        let composable: Json = self.http.get_json(&format!("/_index_template/{p}")).await.unwrap_or(Json::Null);
        if let Some(t) = composable.get("index_templates").and_then(Json::as_array).and_then(|a| a.first()) {
            let spec = t.get("index_template").cloned().unwrap_or(Json::Null);
            let mut detail = ObjectDetail::empty(reference)
                .definition(pretty(&spec), CodeLanguage::Json)
                .property("Kind", "composable")
                .property("Index patterns", str_list(spec.get("index_patterns")).join(", "));
            if let Some(v) = spec.get("priority") {
                detail = detail.property("Priority", text_of(v));
            }
            let composed = str_list(spec.get("composed_of"));
            if !composed.is_empty() {
                detail = detail.property("Composed of", composed.join(", "));
            }
            if let Some(v) = spec.get("version") {
                detail = detail.property("Version", text_of(v));
            }
            return Ok(detail.action(ObjectAction::destructive("delete", "Delete template", format!("DELETE /_index_template/{p}"))));
        }
        let legacy: Json = self.http.get_json(&format!("/_template/{p}")).await.unwrap_or(Json::Null);
        if let Some(spec) = legacy.get(name) {
            let mut detail = ObjectDetail::empty(reference)
                .definition(pretty(spec), CodeLanguage::Json)
                .property("Kind", "legacy")
                .property("Index patterns", str_list(spec.get("index_patterns")).join(", "));
            if let Some(v) = spec.get("order") {
                detail = detail.property("Order", text_of(v));
            }
            return Ok(detail.action(ObjectAction::destructive("delete", "Delete template", format!("DELETE /_template/{p}"))));
        }
        let component: Json = self.http.get_json(&format!("/_component_template/{p}")).await?;
        let spec = component.get("component_templates").and_then(Json::as_array).and_then(|a| a.first()).and_then(|t| t.get("component_template")).cloned().unwrap_or(Json::Null);
        let detail = ObjectDetail::empty(reference).definition(pretty(&spec), CodeLanguage::Json).property("Kind", "component");
        Ok(detail.action(ObjectAction::destructive("delete", "Delete template", format!("DELETE /_component_template/{p}"))))
    }

    async fn pipeline_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let id = reference.name.as_str();
        let body: Json = self.http.get_json(&format!("/_ingest/pipeline/{}", encode_path(id))).await?;
        let spec = body.get(id).cloned().unwrap_or(Json::Null);
        let processors = spec.get("processors").and_then(Json::as_array).cloned().unwrap_or_default();
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&spec), CodeLanguage::Json).property("Processors", processors.len().to_string());
        let desc = str_at(&spec, "description");
        if !desc.is_empty() {
            detail = detail.property("Description", desc);
        }
        if let Some(v) = spec.get("version") {
            detail = detail.property("Version", text_of(v));
        }
        let rows = processors
            .iter()
            .filter_map(|p| p.as_object().and_then(|o| o.iter().next()))
            .map(|(kind, cfg)| vec![Value::Text(kind.clone()), Value::Text(str_at(cfg, "description").to_string()), Value::Json(cfg.clone())])
            .collect();
        detail.rows = Some(rows_table(&[("processor", "keyword"), ("description", "text"), ("config", "object")], rows));
        Ok(detail.action(ObjectAction::destructive("delete", "Delete pipeline", format!("DELETE /_ingest/pipeline/{}", encode_path(id)))))
    }

    async fn policy_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let p = encode_path(name);
        if self.is_opensearch() {
            let body: Json = self.http.get_json(&format!("/_plugins/_ism/policies/{p}")).await?;
            let spec = body.get("policy").cloned().unwrap_or(Json::Null);
            let mut detail = ObjectDetail::empty(reference).definition(pretty(&spec), CodeLanguage::Json).property("Kind", "ism");
            let desc = str_at(&spec, "description");
            if !desc.is_empty() {
                detail = detail.property("Description", desc);
            }
            detail = detail.property("Default state", str_at(&spec, "default_state"));
            let rows = spec
                .get("states")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
                .map(|s| {
                    let actions: Vec<String> = s.get("actions").and_then(Json::as_array).into_iter().flatten().filter_map(|a| a.as_object().and_then(|o| o.keys().find(|k| *k != "retry" && *k != "timeout")).cloned()).collect();
                    let transitions = s.get("transitions").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
                    vec![Value::Text(str_at(s, "name").to_string()), Value::Text(actions.join(", ")), Value::Int(transitions as i64)]
                })
                .collect();
            detail.rows = Some(rows_table(&[("state", "keyword"), ("actions", "text"), ("transitions", "integer")], rows));
            return Ok(detail.action(ObjectAction::destructive("delete", "Delete policy", format!("DELETE /_plugins/_ism/policies/{p}"))));
        }
        let body: Json = self.http.get_json(&format!("/_ilm/policy/{p}")).await?;
        let spec = body.get(name).cloned().unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&spec), CodeLanguage::Json).property("Kind", "ilm");
        if let Some(v) = spec.get("version") {
            detail = detail.property("Version", text_of(v));
        }
        if let Some(v) = spec.get("modified_date").and_then(Json::as_str) {
            detail = detail.property("Modified", v);
        }
        let used = spec.pointer("/in_use_by/indices").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
        detail = detail.property("Used by indices", used.to_string());
        let rows = spec
            .pointer("/policy/phases")
            .and_then(Json::as_object)
            .into_iter()
            .flatten()
            .map(|(phase, cfg)| {
                let actions: Vec<String> = cfg.get("actions").and_then(Json::as_object).map(|o| o.keys().cloned().collect()).unwrap_or_default();
                vec![Value::Text(phase.clone()), Value::Text(str_at(cfg, "min_age").to_string()), Value::Text(actions.join(", "))]
            })
            .collect();
        detail.rows = Some(rows_table(&[("phase", "keyword"), ("min_age", "keyword"), ("actions", "text")], rows));
        Ok(detail.action(ObjectAction::destructive("delete", "Delete policy", format!("DELETE /_ilm/policy/{p}"))))
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = encode_path(&reference.name);
        let info: Json = self.http.get_json(&format!("/_nodes/{name}")).await?;
        let stats: Json = self.http.get_json(&format!("/_nodes/{name}/stats/jvm,os,fs,process,indices")).await.unwrap_or(Json::Null);
        let mut node = info.get("nodes").and_then(Json::as_object).and_then(|o| o.values().next().cloned()).ok_or_else(|| AppError::not_found(format!("Node {} not found.", reference.name)))?;
        let st = stats.get("nodes").and_then(Json::as_object).and_then(|o| o.values().next().cloned()).unwrap_or(Json::Null);
        if let Some(obj) = node.as_object_mut() {
            obj.remove("modules");
            obj.remove("plugins");
        }
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&node), CodeLanguage::Json);
        for (label, key) in [("Address", "transport_address"), ("IP", "ip"), ("Version", "version")] {
            let v = str_at(&node, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        let roles = str_list(node.get("roles"));
        if !roles.is_empty() {
            detail = detail.property("Roles", roles.join(", "));
        }
        let heap_used = num_at(&st, "/jvm/mem/heap_used_in_bytes");
        let heap_max = num_at(&st, "/jvm/mem/heap_max_in_bytes");
        if let (Some(u), Some(m)) = (heap_used, heap_max) {
            detail = detail.property("Heap", format!("{} / {}", human_bytes(u), human_bytes(m)));
        }
        if let Some(c) = num_at(&st, "/os/cpu/percent") {
            detail = detail.property("CPU", format!("{c}%"));
        }
        if let Some(l) = st.pointer("/os/cpu/load_average/1m") {
            detail = detail.property("Load 1m", text_of(l));
        }
        if let (Some(f), Some(t)) = (num_at(&st, "/fs/total/free_in_bytes"), num_at(&st, "/fs/total/total_in_bytes")) {
            detail = detail.property("Disk free", format!("{} / {}", human_bytes(f), human_bytes(t)));
        }
        if let Some(d) = num_at(&st, "/indices/docs/count") {
            detail = detail.property("Documents", crate::model::objects::format_number(d));
        }
        if let Some(fd) = num_at(&st, "/process/open_file_descriptors") {
            detail = detail.property("Open files", crate::model::objects::format_number(fd));
        }
        if let Some(up) = num_at(&st, "/jvm/uptime_in_millis") {
            detail = detail.property("Uptime", duration_text(up));
        }
        Ok(detail)
    }

    async fn shard_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let index = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A shard needs its index as parent."))?;
        let (number, primary) = parse_shard_name(&reference.name).ok_or_else(|| AppError::invalid_input(format!("Unrecognised shard name {:?} (expected e.g. 0p / 1r).", reference.name)))?;
        let rows = self.cat(&format!("/_cat/shards/{}?format=json&h=index,shard,prirep,state,docs,store,ip,node,unassigned.reason,unassigned.at,unassigned.for,segments.count,recoverysource.type", encode_path(index))).await?;
        let row = rows
            .iter()
            .find(|r| num_at(r, "shard") == Some(number as f64) && (str_at(r, "prirep") == "p") == primary)
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Shard {} of {index} not found.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&row), CodeLanguage::Json).property("Type", if primary { "primary" } else { "replica" });
        for (label, key) in [("State", "state"), ("Documents", "docs"), ("Store", "store"), ("Node", "node"), ("IP", "ip"), ("Segments", "segments.count"), ("Recovery source", "recoverysource.type"), ("Unassigned reason", "unassigned.reason"), ("Unassigned at", "unassigned.at"), ("Unassigned for", "unassigned.for")] {
            let v = str_at(&row, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        let explain = format!("GET /_cluster/allocation/explain\n{}", json!({"index": index, "shard": number, "primary": primary}));
        Ok(detail.action(ObjectAction::new("explain", "Explain allocation", explain)))
    }

    async fn task_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let id = reference.name.as_str();
        let body: Json = self.http.get_json(&format!("/_tasks/{}", encode_path(id))).await?;
        let task = body.get("task").cloned().unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&body), CodeLanguage::Json);
        for (label, key) in [("Action", "action"), ("Description", "description"), ("Type", "type"), ("Node", "node"), ("Parent task", "parent_task_id")] {
            let v = str_at(&task, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        if let Some(nanos) = task.get("running_time_in_nanos").and_then(Json::as_f64) {
            detail = detail.property("Running for", duration_text(nanos / 1_000_000.0));
        }
        let cancellable = task.get("cancellable").and_then(Json::as_bool).unwrap_or(false);
        detail = detail.property("Cancellable", cancellable.to_string()).property("Completed", body.get("completed").and_then(Json::as_bool).unwrap_or(false).to_string());
        if cancellable {
            detail = detail.action(ObjectAction::destructive("cancel", "Cancel task", format!("POST /_tasks/{}/_cancel", encode_path(id))));
        }
        Ok(detail)
    }

    async fn snapshot_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let repo = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A snapshot needs its repository as parent."))?;
        let path = format!("/_snapshot/{}/{}", encode_path(repo), encode_path(&reference.name));
        let body: Json = self.http.get_json(&path).await?;
        let snap = body.get("snapshots").and_then(Json::as_array).and_then(|a| a.first()).cloned().unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&snap), CodeLanguage::Json).property("Repository", repo);
        for (label, key) in [("State", "state"), ("Started", "start_time"), ("Finished", "end_time"), ("Version", "version"), ("UUID", "uuid")] {
            let v = str_at(&snap, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        if let Some(ms) = snap.get("duration_in_millis").and_then(Json::as_f64) {
            detail = detail.property("Duration", duration_text(ms));
        }
        if let Some(shards) = snap.get("shards") {
            detail = detail.property("Shards", format!("{} ok / {} failed / {} total", text_of(&shards["successful"]), text_of(&shards["failed"]), text_of(&shards["total"])));
        }
        let indices = str_list(snap.get("indices"));
        detail = detail.property("Indices", indices.len().to_string());
        detail.rows = Some(rows_table(&[("index", "keyword")], indices.into_iter().map(|i| vec![Value::Text(i)]).collect()));
        Ok(detail
            .action(ObjectAction::destructive("restore", "Restore snapshot", format!("POST {path}/_restore")))
            .action(ObjectAction::destructive("delete", "Delete snapshot", format!("DELETE {path}"))))
    }

    async fn user_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let path = self.security_path(ObjectKind::User, Some(name));
        let body: Json = self.http.get_json(&path).await?;
        let spec = body.get(name).cloned().unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&spec), CodeLanguage::Json);
        for (label, key) in [("Full name", "full_name"), ("Email", "email"), ("Description", "description")] {
            let v = str_at(&spec, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        if let Some(e) = spec.get("enabled").and_then(Json::as_bool) {
            detail = detail.property("Enabled", e.to_string());
        }
        for (label, key) in [("Reserved", "reserved"), ("Hidden", "hidden")] {
            if let Some(v) = spec.get(key).and_then(Json::as_bool) {
                detail = detail.property(label, v.to_string());
            }
        }
        let roles = str_list(spec.get("roles").or_else(|| spec.get("backend_roles")));
        let mapped = str_list(spec.get("opendistro_security_roles"));
        detail = detail.property("Roles", roles.join(", "));
        let mut rows: Vec<Vec<Value>> = roles.iter().map(|r| vec![Value::Text(r.clone()), Value::Text(if self.is_opensearch() { "backend role" } else { "role" }.into())]).collect();
        rows.extend(mapped.iter().map(|r| vec![Value::Text(r.clone()), Value::Text("security role".into())]));
        detail.rows = Some(rows_table(&[("role", "keyword"), ("kind", "keyword")], rows));
        if !self.is_opensearch() {
            if spec.get("enabled").and_then(Json::as_bool) == Some(false) {
                detail = detail.action(ObjectAction::new("enable", "Enable user", format!("POST {path}/_enable")));
            } else {
                detail = detail.action(ObjectAction::destructive("disable", "Disable user", format!("POST {path}/_disable")));
            }
        }
        Ok(detail.action(ObjectAction::destructive("delete", "Delete user", format!("DELETE {path}"))))
    }

    async fn role_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let path = self.security_path(ObjectKind::Role, Some(name));
        let body: Json = self.http.get_json(&path).await?;
        let spec = body.get(name).cloned().unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&spec), CodeLanguage::Json);
        let cluster = str_list(spec.get("cluster").or_else(|| spec.get("cluster_permissions")));
        detail = detail.property("Cluster privileges", if cluster.is_empty() { "—".to_string() } else { cluster.join(", ") });
        let run_as = str_list(spec.get("run_as"));
        if !run_as.is_empty() {
            detail = detail.property("Run as", run_as.join(", "));
        }
        for (label, key) in [("Reserved", "reserved"), ("Hidden", "hidden")] {
            if let Some(v) = spec.get(key).and_then(Json::as_bool) {
                detail = detail.property(label, v.to_string());
            }
        }
        let rows = spec
            .get("indices")
            .or_else(|| spec.get("index_permissions"))
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .map(|g| {
                let patterns = str_list(g.get("names").or_else(|| g.get("index_patterns")));
                let privileges = str_list(g.get("privileges").or_else(|| g.get("allowed_actions")));
                vec![Value::Text(patterns.join(", ")), Value::Text(privileges.join(", ")), cell(g.get("query").or_else(|| g.get("dls")))]
            })
            .collect();
        detail.rows = Some(rows_table(&[("indices", "text"), ("privileges", "text"), ("query", "object")], rows));
        Ok(detail.action(ObjectAction::destructive("delete", "Delete role", format!("DELETE {path}"))))
    }

    async fn playground(&self, req: &SearchRequest) -> AppResult<SearchResult> {
        let fields = self.fields(&req.index).await.unwrap_or_default();
        let body = playground_body(req, &fields)?;
        let out: Json = self.http.post_json(&format!("/{}/_search", encode_path(&req.index)), &body).await?;
        Ok(playground_result(&out, &req.facets, req.highlight))
    }

    async fn stats(&self) -> AppResult<ServerStats> {
        let health: Json = self.http.get_json("/_cluster/health").await?;
        let cluster: Json = self.http.get_json("/_cluster/stats").await.unwrap_or(Json::Null);
        let nodes: Json = self.http.get_json("/_nodes/stats/indices,os,jvm,process,http,thread_pool").await.unwrap_or(Json::Null);
        Ok(ServerStats::now(stats_groups(&health, &cluster, &nodes)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, sql: false, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: true, transactions: false, exact_estimate: true },
        object_kinds: vec![K::Index, K::Alias, K::Template, K::Pipeline, K::Policy, K::Node, K::Shard, K::Task, K::Snapshot, K::User, K::Role],
        tools: vec![T::Stats, T::SearchPlayground],
    }
}

#[async_trait]
impl Integration for ElasticsearchIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _: Json = self.http.get_json("/").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let body: Json = self.http.get_json("/").await?;
        let number = body.pointer("/version/number").and_then(Json::as_str).unwrap_or("?");
        let distribution = body
            .pointer("/version/distribution")
            .and_then(Json::as_str)
            .map(|d| if d == "opensearch" { "OpenSearch".to_string() } else { d.to_string() })
            .unwrap_or_else(|| "Elasticsearch".to_string());
        Ok(Some(format!("{distribution} {number}")))
    }

    fn current_database(&self) -> Option<String> {
        Some("default".into())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec!["default".into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let indices: Vec<Json> = self.http.get_json("/_cat/indices?format=json&h=index,docs.count&s=index").await?;
        let mut tables: Vec<TableInfo> = indices
            .iter()
            .filter_map(|i| {
                let name = i.get("index").and_then(Json::as_str)?;
                if name.starts_with('.') {
                    return None;
                }
                let count = i.get("docs.count").and_then(|c| c.as_str().and_then(|s| s.parse::<i64>().ok()).or_else(|| c.as_i64()));
                Some(TableInfo { schema: Some(INDEX_SCHEMA.into()), name: name.to_string(), kind: TableKind::Table, row_estimate: count })
            })
            .collect();
        let aliases: Json = self.http.get_json("/_aliases").await.unwrap_or(Json::Null);
        let mut alias_names: Vec<String> = Vec::new();
        if let Some(obj) = aliases.as_object() {
            for (_, spec) in obj {
                if let Some(al) = spec.get("aliases").and_then(Json::as_object) {
                    for name in al.keys() {
                        if !name.starts_with('.') && !alias_names.contains(name) {
                            alias_names.push(name.clone());
                        }
                    }
                }
            }
        }
        alias_names.sort();
        for name in alias_names {
            tables.push(TableInfo { schema: Some(INDEX_SCHEMA.into()), name, kind: TableKind::View, row_estimate: None });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: INDEX_SCHEMA.into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let fields = self.fields(&table.name).await?;
        let mut cols = columns_from_fields(&fields);
        if cols.len() == 1 {
            // No explicit mapping yet: sample documents for keys.
            let body: Json = self.http.post_json(&format!("/{}/_search", encode_path(&table.name)), &json!({"size": 50, "query": {"match_all": {}}})).await?;
            let sample = objects_to_result_set(&hits_of(&body), Some(ID_FIELD), 50);
            for meta in sample.columns.into_iter().skip(1) {
                let ordinal = u32::try_from(cols.len() + 1).unwrap_or(u32::MAX);
                cols.push(ColumnInfo { name: meta.name, data_type: meta.type_name, nullable: true, primary_key: false, ordinal });
            }
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let fields = if filters.is_empty() { Vec::new() } else { self.fields(&table.name).await? };
        let body = json!({"query": build_query(filters, &fields)});
        let out: Json = self.http.post_json(&format!("/{}/_count", encode_path(&table.name)), &body).await?;
        Ok(out.get("count").and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        if query.offset + u64::from(query.limit) > MAX_WINDOW {
            return Err(AppError::invalid_input(format!("Elasticsearch only pages through the first {MAX_WINDOW} hits (index.max_result_window). Narrow the result with a filter.")));
        }
        let fields = self.fields(&table.name).await?;
        let body = search_body(query, &fields);
        let out: Json = self.http.post_json(&format!("/{}/_search", encode_path(&table.name)), &body).await?;
        let hits = hits_of(&out);
        let mut set = objects_to_result_set(&hits, Some(ID_FIELD), query.limit as usize);
        // Keep mapped columns visible even when the page has no value for them.
        for (name, info) in &fields {
            if !set.columns.iter().any(|c| &c.name == name) {
                set.columns.push(ColumnMeta { name: name.clone(), type_name: info.type_name.clone() });
                for row in &mut set.rows {
                    row.push(Value::Null);
                }
            }
        }
        let _ = total_of(&out);
        Ok(set)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut results = Vec::new();
        for chunk in split_console(sql) {
            let cmd = parse_command(&chunk, self.default_index.as_deref())?;
            results.push(self.run_command(cmd, max_rows).await?);
        }
        Ok(results)
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Index => self.list_indices().await,
            ObjectKind::Alias => self.list_aliases(parent).await,
            ObjectKind::Template => self.list_templates().await,
            ObjectKind::Pipeline => self.list_pipelines().await,
            ObjectKind::Policy => self.list_policies().await,
            ObjectKind::Node => self.list_nodes().await,
            ObjectKind::Shard => self.list_shards(parent).await,
            ObjectKind::Task => self.list_tasks().await,
            ObjectKind::Snapshot => self.list_snapshots(parent).await,
            ObjectKind::User => self.list_users().await,
            ObjectKind::Role => self.list_roles().await,
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Index => self.index_detail(reference).await,
            ObjectKind::Alias => self.alias_detail(reference).await,
            ObjectKind::Template => self.template_detail(reference).await,
            ObjectKind::Pipeline => self.pipeline_detail(reference).await,
            ObjectKind::Policy => self.policy_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
            ObjectKind::Shard => self.shard_detail(reference).await,
            ObjectKind::Task => self.task_detail(reference).await,
            ObjectKind::Snapshot => self.snapshot_detail(reference).await,
            ObjectKind::User => self.user_detail(reference).await,
            ObjectKind::Role => self.role_detail(reference).await,
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

// WHAT:  Splits console text into statements: a REST verb line starts a new
//        statement; a blank line separates JSON / SQL statements.
fn split_console(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        let first = line.split_whitespace().next().unwrap_or_default().to_ascii_uppercase();
        let starts_rest = REST_VERBS.contains(&first.as_str()) && line.split_whitespace().nth(1).map(|p| p.starts_with('/') || p.starts_with("http") || p.starts_with('_')).unwrap_or(false);
        if starts_rest || (line.trim().is_empty() && !current.trim().is_empty() && !current.trim_start().starts_with('{')) {
            if !current.trim().is_empty() {
                out.push(std::mem::take(&mut current));
            }
            current.clear();
        }
        if line.trim().is_empty() && current.trim_start().starts_with('{') {
            // Blank lines inside JSON blocks separate statements only when the JSON so far is complete.
            if serde_json::from_str::<Json>(current.trim()).is_ok() {
                out.push(std::mem::take(&mut current));
            }
            continue;
        }
        if !line.trim().is_empty() || !current.is_empty() {
            current.push_str(line);
            current.push('\n');
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode};

    fn conn(engine: Engine, user: Option<&str>, secret: Option<&str>) -> ResolvedConnection {
        let input = ConnectionInput {
            name: "t".into(),
            engine,
            environment: Environment::Local,
            read_only: false,
            host: Some("localhost".into()),
            port: None,
            database: None,
            username: user.map(str::to_string),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        ResolvedConnection { summary: ConnectionSummary::draft(&input, secret.is_some()), secret: secret.map(str::to_string) }
    }

    #[test]
    fn auth_picks_scheme() {
        assert!(matches!(pick_auth(&conn(Engine::Elasticsearch, Some("elastic"), Some("pw"))), Auth::Basic { .. }));
        assert!(matches!(pick_auth(&conn(Engine::Elasticsearch, None, Some("plain-token"))), Auth::Bearer(_)));
        match pick_auth(&conn(Engine::Elasticsearch, None, Some("id123:secret456"))) {
            Auth::Header { name, value } => {
                assert_eq!(name, "Authorization");
                assert!(value.starts_with("ApiKey "));
                assert!(!value.contains(':'));
            }
            other => panic!("unexpected {other:?}"),
        }
        let encoded = base64::engine::general_purpose::STANDARD.encode("id123:secret456");
        match pick_auth(&conn(Engine::Elasticsearch, None, Some(&encoded))) {
            Auth::Header { value, .. } => assert_eq!(value, format!("ApiKey {encoded}")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn mapping_flattens_to_dotted_paths() {
        let body = json!({
            "products": {"mappings": {"properties": {
                "name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "price": {"type": "float"},
                "vendor": {"properties": {"id": {"type": "keyword"}, "address": {"properties": {"city": {"type": "text"}}}}},
                "tags": {"type": "nested", "properties": {"k": {"type": "keyword"}}}
            }}}
        });
        let fields = fields_from_mapping(&body);
        let names: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["name", "price", "vendor.id", "vendor.address.city", "tags", "tags.k"]);
        assert_eq!(fields[0].1.keyword_subfield.as_deref(), Some("keyword"));
        assert_eq!(exact_field("name", &fields), "name.keyword");
        assert_eq!(exact_field("vendor.address.city", &fields), "vendor.address.city");
        assert!(is_text_without_keyword("vendor.address.city", &fields));
        let cols = columns_from_fields(&fields);
        assert_eq!(cols[0].name, "_id");
        assert!(cols[0].primary_key);
        assert_eq!(cols.len(), 7);
    }

    #[test]
    fn filters_become_bool_query() {
        let fields = vec![
            ("name".to_string(), FieldInfo { type_name: "text".into(), keyword_subfield: Some("keyword".into()) }),
            ("price".to_string(), FieldInfo { type_name: "float".into(), keyword_subfield: None }),
        ];
        let q = PageQuery {
            sort: vec![SortRule { column: "name".into(), desc: true }],
            filters: vec![
                FilterRule { column: "name".into(), op: FilterOp::Eq, value: "Widget".into() },
                FilterRule { column: "price".into(), op: FilterOp::Gt, value: "3.5".into() },
                FilterRule { column: "price".into(), op: FilterOp::Ne, value: "9".into() },
                FilterRule { column: "name".into(), op: FilterOp::Contains, value: "wid*".into() },
                FilterRule { column: "price".into(), op: FilterOp::In, value: "1, 2,3".into() },
                FilterRule { column: "price".into(), op: FilterOp::IsNull, value: String::new() },
            ],
            offset: 20,
            limit: 10,
        };
        let body = search_body(&q, &fields);
        assert_eq!(body["from"], 20);
        assert_eq!(body["size"], 10);
        assert_eq!(body["sort"][0]["name.keyword"]["order"], "desc");
        let filter = body["query"]["bool"]["filter"].as_array().map(Vec::len).unwrap_or(0);
        let must_not = body["query"]["bool"]["must_not"].as_array().map(Vec::len).unwrap_or(0);
        assert_eq!(filter, 4);
        assert_eq!(must_not, 2);
        assert_eq!(body["query"]["bool"]["filter"][0]["term"]["name.keyword"], "Widget");
        assert_eq!(body["query"]["bool"]["filter"][1]["range"]["price"]["gt"], 3.5);
        assert_eq!(body["query"]["bool"]["filter"][2]["wildcard"]["name.keyword"]["value"], "*wid\\**");
        assert_eq!(body["query"]["bool"]["filter"][3]["terms"]["price"], json!([1, 2, 3]));
        assert_eq!(body["query"]["bool"]["must_not"][1]["exists"]["field"], "price");
        assert_eq!(build_query(&[], &fields), json!({"match_all": {}}));
    }

    #[test]
    fn id_filters_use_ids_query() {
        let (yes, no) = filter_clause(&FilterRule { column: "_id".into(), op: FilterOp::Eq, value: "abc".into() }, &[]);
        assert_eq!(yes, Some(json!({"ids": {"values": ["abc"]}})));
        assert!(no.is_none());
    }

    #[test]
    fn hits_flatten_source() {
        let body = json!({"hits": {"total": {"value": 2}, "hits": [
            {"_id": "1", "_source": {"a": 1, "n": {"x": "y", "arr": [1, 2]}}},
            {"_id": "2", "_source": {"a": 2}}
        ]}});
        let set = search_result(&body, 10);
        let names: Vec<&str> = set.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_id", "a", "n.x", "n.arr"]);
        assert_eq!(set.rows[0][2], Value::Text("y".into()));
        assert_eq!(set.rows[0][3], Value::Json(json!([1, 2])));
        assert_eq!(set.rows[1][2], Value::Null);
        assert_eq!(total_of(&body), Some(2));
        let aggs = json!({"hits": {"hits": []}, "aggregations": {"by": {"buckets": []}}});
        assert_eq!(search_result(&aggs, 10).columns[0].name, "result");
    }

    #[test]
    fn console_parsing() {
        assert_eq!(parse_command("GET /_cat/indices", None).ok(), Some(Command::Rest { method: "GET".into(), path: "/_cat/indices".into(), body: None }));
        assert_eq!(
            parse_command("post idx/_search\n{\"query\": {\"match_all\": {}}}", None).ok(),
            Some(Command::Rest { method: "POST".into(), path: "/idx/_search".into(), body: Some("{\"query\": {\"match_all\": {}}}".into()) })
        );
        assert_eq!(parse_command("SELECT * FROM idx LIMIT 5;", None).ok(), Some(Command::Sql("SELECT * FROM idx LIMIT 5".into())));
        match parse_command("{\"index\": \"idx\", \"query\": {\"match_all\": {}}, \"size\": 3}", None) {
            Ok(Command::Search { index, body }) => {
                assert_eq!(index, "idx");
                assert_eq!(body["size"], 3);
                assert!(body.get("index").is_none());
            }
            other => panic!("unexpected {other:?}"),
        }
        match parse_command("{\"term\": {\"a\": 1}}", Some("dflt")) {
            Ok(Command::Search { index, body }) => {
                assert_eq!(index, "dflt");
                assert_eq!(body["query"]["term"]["a"], 1);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(parse_command("{\"term\": {\"a\": 1}}", None).is_err());
        assert!(parse_command("DROP idx", None).is_err());
    }

    #[test]
    fn console_splits_statements() {
        let text = "GET /_cat/indices\n\nPOST /idx/_search\n{\n  \"query\": {\"match_all\": {}}\n}\nGET /\n\n{\"index\":\"a\"}\n\n{\"index\":\"b\"}";
        let parts = split_console(text);
        assert_eq!(parts.len(), 5, "{parts:?}");
        assert!(parts[1].contains("match_all"));
        assert_eq!(parts[2].trim(), "GET /");
    }

    #[test]
    fn read_only_guard_and_cat_format() {
        assert!(rest_is_read("GET", "/_cat/indices"));
        assert!(rest_is_read("POST", "/idx/_search?size=1"));
        assert!(rest_is_read("POST", "/_plugins/_sql"));
        assert!(!rest_is_read("POST", "/idx/_doc"));
        assert!(!rest_is_read("DELETE", "/idx"));
        assert_eq!(cat_with_json("/_cat/indices"), "/_cat/indices?format=json");
        assert_eq!(cat_with_json("/_cat/indices?v"), "/_cat/indices?v&format=json");
        assert_eq!(cat_with_json("/_cat/indices?format=json"), "/_cat/indices?format=json");
    }

    #[test]
    fn sql_result_handles_both_shapes() {
        let es = json!({"columns": [{"name": "a", "type": "long"}], "rows": [[1], [2]]});
        let set = sql_result(&es, 1);
        assert_eq!(set.columns[0].name, "a");
        assert_eq!(set.rows.len(), 1);
        assert!(set.truncated);
        let os = json!({"schema": [{"name": "b", "type": "text"}], "datarows": [["x"]], "total": 1, "size": 1});
        let set = sql_result(&os, 10);
        assert_eq!(set.columns[0].name, "b");
        assert_eq!(set.rows[0][0], Value::Text("x".into()));
    }

    #[test]
    fn explorer_lists_from_cat_and_management_apis() {
        let indices = vec![
            json!({"index": "products", "health": "green", "status": "open", "docs.count": "1200", "store.size": "3.4mb"}),
            json!({"index": ".kibana", "health": "yellow", "status": "open", "docs.count": "5", "store.size": "10kb"}),
            json!({"index": "archive", "health": "red", "status": "close", "docs.count": "7", "store.size": "1kb"}),
        ];
        let list = index_summaries(&indices);
        let names: Vec<&str> = list.iter().map(|s| s.reference.name.as_str()).collect();
        assert_eq!(names, vec![".kibana", "archive", "products"]);
        assert_eq!(list[0].badge.as_deref(), Some("system"));
        assert_eq!(list[1].badge.as_deref(), Some("red"));
        assert_eq!(list[1].detail.as_deref(), Some("7 docs · 1kb · closed"));
        assert_eq!(list[2].detail.as_deref(), Some("1,200 docs · 3.4mb"));

        let aliases = vec![json!({"alias": "prod", "index": "products", "filter": "-", "is_write_index": "true"}), json!({"alias": "all", "index": "archive", "filter": "*", "is_write_index": "-"})];
        let list = alias_summaries(&aliases, None);
        assert_eq!(list.len(), 2);
        assert_eq!(list[1].reference.parent.as_deref(), Some("products"));
        assert_eq!(list[1].detail.as_deref(), Some("→ products · write index"));
        assert_eq!(alias_summaries(&aliases, Some("archive")).len(), 1);

        let composable = json!({"index_templates": [{"name": "logs", "index_template": {"index_patterns": ["logs-*"], "priority": 200}}]});
        let legacy = json!({"old": {"index_patterns": ["old-*"], "order": 1}});
        let component = json!({"component_templates": [{"name": "base", "component_template": {"template": {"settings": {}}}}]});
        let list = template_summaries(&composable, &legacy, &component);
        let badges: Vec<&str> = list.iter().filter_map(|s| s.badge.as_deref()).collect();
        assert_eq!(badges, vec!["component", "composable", "legacy"]);
        assert_eq!(list[1].detail.as_deref(), Some("logs-* · priority 200"));

        let pipelines = pipeline_summaries(&json!({"geo": {"description": "adds geo", "processors": [{"geoip": {}}, {"set": {}}]}}));
        assert_eq!(pipelines[0].detail.as_deref(), Some("2 processors · adds geo"));

        let ilm = json!({"hot-warm": {"policy": {"phases": {"hot": {}, "warm": {}}}, "in_use_by": {"indices": ["a", "b"]}}});
        let ism = json!({"policies": [{"_id": "rollover", "policy": {"description": "roll", "states": [{"name": "hot"}]}}]});
        let list = policy_summaries(&ilm, &ism);
        assert_eq!(list[0].badge.as_deref(), Some("ilm"));
        assert_eq!(list[0].detail.as_deref(), Some("phases: hot, warm · 2 indices"));
        assert_eq!(list[1].badge.as_deref(), Some("ism"));
    }

    #[test]
    fn nodes_shards_tasks_and_snapshots() {
        assert_eq!(node_badge("dilmrt", "*"), "master");
        assert_eq!(node_badge("dilmrt", "-"), "data");
        assert_eq!(node_badge("mr", "-"), "master-eligible");
        assert_eq!(node_badge("-", "-"), "coordinating");
        assert_eq!(node_badge("i", "-"), "ingest");
        let nodes = node_summaries(&[json!({"name": "es1", "ip": "10.0.0.1", "node.role": "dim", "heap.percent": "42", "cpu": "3", "load_1m": "0.10", "master": "*", "version": "8.12.0"})]);
        assert_eq!(nodes[0].detail.as_deref(), Some("10.0.0.1 · heap 42% · cpu 3% · load 0.10 · v8.12.0 · roles dim"));
        assert_eq!(nodes[0].badge.as_deref(), Some("master"));

        let shards = shard_summaries(&[
            json!({"index": "products", "shard": "0", "prirep": "p", "state": "STARTED", "docs": "1200", "store": "3mb", "node": "es1"}),
            json!({"index": "products", "shard": "0", "prirep": "r", "state": "UNASSIGNED", "unassigned.reason": "INDEX_CREATED"}),
        ]);
        assert_eq!(shards[0].reference.name, "0p");
        assert_eq!(shards[0].reference.parent.as_deref(), Some("products"));
        assert_eq!(shards[0].badge.as_deref(), Some("started"));
        assert_eq!(shards[0].detail.as_deref(), Some("1,200 docs · 3mb · es1"));
        assert_eq!(shards[1].detail.as_deref(), Some("INDEX_CREATED"));
        assert_eq!(parse_shard_name("12r"), Some((12, false)));
        assert_eq!(parse_shard_name("0p"), Some((0, true)));
        assert_eq!(parse_shard_name("x"), None);
        assert_eq!(shard_name("3", "p"), "3p");

        let tasks = task_summaries(&json!({"nodes": {"n1": {"tasks": {"n1:7": {"action": "indices:data/write/bulk", "running_time_in_nanos": 2_500_000_000u64, "cancellable": true, "type": "transport"}}}}}));
        assert_eq!(tasks[0].reference.name, "n1:7");
        assert_eq!(tasks[0].badge.as_deref(), Some("cancellable"));
        assert_eq!(tasks[0].detail.as_deref(), Some("indices:data/write/bulk · 2.5 s"));

        let snaps = snapshot_summaries("backup", &json!({"snapshots": [{"snapshot": "daily-1", "state": "SUCCESS", "indices": ["a", "b"], "start_time": "2026-01-01T00:00:00Z", "duration_in_millis": 65_000}]}));
        assert_eq!(snaps[0].reference.parent.as_deref(), Some("backup"));
        assert_eq!(snaps[0].badge.as_deref(), Some("success"));
        assert_eq!(snaps[0].detail.as_deref(), Some("2 indices · 2026-01-01T00:00:00Z · 1 min"));
    }

    #[test]
    fn users_and_roles_both_flavours() {
        let es_users = user_summaries(&json!({"elastic": {"roles": ["superuser"], "full_name": "Admin", "enabled": true, "metadata": {"_reserved": true}}, "bob": {"roles": ["viewer"], "enabled": false}}));
        assert_eq!(es_users[0].reference.name, "bob");
        assert_eq!(es_users[0].badge.as_deref(), Some("disabled"));
        assert_eq!(es_users[1].badge.as_deref(), Some("reserved"));
        assert_eq!(es_users[1].detail.as_deref(), Some("Admin · superuser"));
        let os_users = user_summaries(&json!({"admin": {"backend_roles": ["admin"], "reserved": true, "hidden": false}}));
        assert_eq!(os_users[0].detail.as_deref(), Some("admin"));
        assert_eq!(os_users[0].badge.as_deref(), Some("reserved"));

        let es_roles = role_summaries(&json!({"viewer": {"cluster": ["monitor"], "indices": [{"names": ["*"], "privileges": ["read"]}], "metadata": {"_reserved": true}}}));
        assert_eq!(es_roles[0].detail.as_deref(), Some("cluster: monitor · 1 index grants"));
        assert_eq!(es_roles[0].badge.as_deref(), Some("reserved"));
        let os_roles = role_summaries(&json!({"reader": {"cluster_permissions": [], "index_permissions": [{"index_patterns": ["logs-*"], "allowed_actions": ["read"]}, {"index_patterns": ["x"], "allowed_actions": ["read"]}]}}));
        assert_eq!(os_roles[0].detail.as_deref(), Some("2 index grants"));
        assert!(os_roles[0].badge.is_none());
    }

    #[test]
    fn playground_builds_query_dsl() {
        let fields = vec![("title".to_string(), FieldInfo { type_name: "text".into(), keyword_subfield: Some("keyword".into()) }), ("price".to_string(), FieldInfo { type_name: "float".into(), keyword_subfield: None })];
        let req = SearchRequest {
            index: "products".into(),
            query: "red shoes".into(),
            filter: Some("{\"range\": {\"price\": {\"lt\": 50}}}".into()),
            facets: vec!["title".into(), "".into()],
            sort: vec!["price:desc".into(), "_score".into()],
            highlight: true,
            limit: 25,
            offset: 50,
        };
        let body = playground_body(&req, &fields).unwrap_or_default();
        assert_eq!(body["from"], 50);
        assert_eq!(body["size"], 25);
        assert_eq!(body["query"]["bool"]["must"][0]["query_string"]["query"], "red shoes");
        assert_eq!(body["query"]["bool"]["filter"][0]["range"]["price"]["lt"], 50);
        assert_eq!(body["aggs"]["title"]["terms"]["field"], "title.keyword");
        assert!(body["aggs"].as_object().map(|a| a.len() == 1).unwrap_or(false));
        assert_eq!(body["sort"][0]["price"]["order"], "desc");
        assert_eq!(body["sort"][1]["_score"]["order"], "asc");
        assert_eq!(body["highlight"]["fields"]["*"], json!({}));
        assert!(body["track_total_hits"].as_bool().unwrap_or(false));

        let plain = SearchRequest { index: "p".into(), query: "*".into(), filter: Some("status:active".into()), facets: vec![], sort: vec![], highlight: false, limit: 10, offset: 0 };
        let body = playground_body(&plain, &fields).unwrap_or_default();
        assert_eq!(body["query"]["bool"]["must"][0], json!({"match_all": {}}));
        assert_eq!(body["query"]["bool"]["filter"][0]["query_string"]["query"], "status:active");
        assert!(body.get("aggs").is_none() && body.get("sort").is_none() && body.get("highlight").is_none());

        let wrapped = SearchRequest { filter: Some("{\"query\": {\"term\": {\"a\": 1}}}".into()), ..plain.clone() };
        let body = playground_body(&wrapped, &fields).unwrap_or_default();
        assert_eq!(body["query"]["bool"]["filter"][0], json!({"term": {"a": 1}}));
        assert!(playground_body(&SearchRequest { filter: Some("{bad".into()), ..plain.clone() }, &fields).is_err());
        assert!(playground_body(&SearchRequest { offset: 10_000, ..plain }, &fields).is_err());
    }

    #[test]
    fn playground_maps_hits_facets_and_highlights() {
        let body = json!({
            "took": 7,
            "hits": {"total": {"value": 2}, "hits": [
                {"_id": "1", "_score": 1.5, "_source": {"title": "Red shoes", "price": 20}, "highlight": {"title": ["<em>Red</em> shoes"]}},
                {"_id": "2", "_score": 0.5, "_source": {"title": "Blue", "meta": {"k": "v"}}}
            ]},
            "aggregations": {"title": {"buckets": [{"key": "Red shoes", "doc_count": 1}, {"key": 3, "key_as_string": "three", "doc_count": 4}]}}
        });
        let out = playground_result(&body, &["title".to_string(), "missing".to_string()], true);
        let names: Vec<&str> = out.hits.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names[..2], ["_id", "_score"]);
        assert_eq!(names.last().copied(), Some("_highlight"));
        assert!(names.contains(&"meta.k"));
        assert_eq!(out.hits.rows[0][1], Value::Float(1.5));
        let h = names.iter().position(|n| *n == "_highlight").unwrap_or_default();
        assert_eq!(out.hits.rows[0][h], Value::Json(json!({"title": ["<em>Red</em> shoes"]})));
        assert_eq!(out.hits.rows[1][h], Value::Null);
        assert_eq!(out.total, Some(2));
        assert_eq!(out.took_ms, Some(7));
        assert_eq!(out.facets.len(), 1);
        assert_eq!(out.facets[0].values[1], FacetValue { value: "three".into(), count: 4 });
        let none = playground_result(&json!({"hits": {"hits": []}}), &[], false);
        assert_eq!(none.hits.columns.len(), 2);
        assert!(none.hits.rows.is_empty());
    }

    #[test]
    fn stats_fold_cluster_and_node_figures() {
        let health = json!({"cluster_name": "dev", "status": "yellow", "number_of_nodes": 2, "number_of_data_nodes": 2, "active_primary_shards": 5, "active_shards": 8, "relocating_shards": 0, "initializing_shards": 0, "unassigned_shards": 2, "number_of_pending_tasks": 0, "active_shards_percent_as_number": 80.0});
        let cluster = json!({
            "nodes": {"versions": ["8.12.0"], "jvm": {"max_uptime_in_millis": 7_200_000, "mem": {"heap_used_in_bytes": 512, "heap_max_in_bytes": 1024}}, "fs": {"total_in_bytes": 100, "free_in_bytes": 40}, "os": {"mem": {"used_percent": 55}}},
            "indices": {"count": 3, "docs": {"count": 1000, "deleted": 5}, "store": {"size_in_bytes": 2048}, "segments": {"count": 12}, "query_cache": {"hit_count": 3, "miss_count": 1, "memory_size_in_bytes": 10}}
        });
        let nodes = json!({"nodes": {
            "a": {"indices": {"search": {"query_total": 10, "query_time_in_millis": 500, "fetch_total": 4}, "indexing": {"index_total": 20, "index_time_in_millis": 1000}, "get": {"total": 1}}, "http": {"current_open": 2, "total_opened": 9}, "os": {"cpu": {"percent": 10}}, "process": {"open_file_descriptors": 100}, "thread_pool": {"search": {"rejected": 0}, "write": {"rejected": 1}}},
            "b": {"indices": {"search": {"query_total": 5, "query_time_in_millis": 500, "fetch_total": 1}, "indexing": {"index_total": 5, "index_time_in_millis": 0}, "get": {"total": 0}}, "http": {"current_open": 1, "total_opened": 1}, "os": {"cpu": {"percent": 30}}, "process": {"open_file_descriptors": 50}, "thread_pool": {"search": {"rejected": 0}, "write": {"rejected": 0}}}
        }});
        let groups = stats_groups(&health, &cluster, &nodes);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Status").map(|s| s.value), Some("yellow".into()));
        assert_eq!(find("Server", "Uptime").map(|s| s.value), Some("2.0 h".into()));
        assert_eq!(find("Cluster", "Unassigned").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Store size").map(|s| s.value), Some("2.0 KB".into()));
        assert_eq!(find("Memory", "Heap").and_then(|s| s.numeric), Some(50.0));
        assert_eq!(find("Memory", "Query cache hit").and_then(|s| s.numeric), Some(75.0));
        assert_eq!(find("Throughput", "Search queries").and_then(|s| s.numeric), Some(15.0));
        assert_eq!(find("Throughput", "Search time").map(|s| s.value), Some("1.0 s".into()));
        assert_eq!(find("Throughput", "Write rejected").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("OS", "CPU").and_then(|s| s.numeric), Some(20.0));
        assert_eq!(find("OS", "Open files").and_then(|s| s.numeric), Some(150.0));
        assert_eq!(human_bytes(1536.0), "1.5 KB");
        assert_eq!(human_bytes(12.0), "12 B");
        assert_eq!(duration_text(90_000.0), "2 min");
    }

    #[test]
    fn index_actions_follow_state() {
        let open = index_actions("my index", false);
        assert!(open.iter().any(|a| a.id == "refresh" && a.statement == "POST /my%20index/_refresh" && !a.destructive));
        assert!(open.iter().any(|a| a.id == "close" && a.destructive));
        assert!(open.iter().any(|a| a.id == "delete" && a.statement == "DELETE /my%20index" && a.destructive));
        let closed = index_actions("idx", true);
        assert!(closed.iter().any(|a| a.id == "open" && !a.destructive));
        assert!(!closed.iter().any(|a| a.id == "close"));
        assert_eq!(parse_command(&open[0].statement, None).ok(), Some(Command::Rest { method: "POST".into(), path: "/my%20index/_refresh".into(), body: None }));
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_ELASTICSEARCH_URL is set
    //        (e.g. http://localhost:9200 with security disabled).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_ELASTICSEARCH_URL") else {
            return;
        };
        let engine = if std::env::var("DBFREE_TEST_ELASTICSEARCH_OPENSEARCH").is_ok() { Engine::Opensearch } else { Engine::Elasticsearch };
        let mut c = conn(engine, std::env::var("DBFREE_TEST_ELASTICSEARCH_USER").ok().as_deref(), std::env::var("DBFREE_TEST_ELASTICSEARCH_SECRET").ok().as_deref());
        c.summary.host = Some(url);
        c.summary.ssl_mode = SslMode::Require;
        let es = connect(&c).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = es.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(!version.is_empty());
        es.execute("DELETE /dbfree_t", 10).await.ok();
        es.execute("PUT /dbfree_t\n{\"mappings\":{\"properties\":{\"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},\"n\":{\"type\":\"integer\"},\"meta\":{\"properties\":{\"k\":{\"type\":\"keyword\"}}}}}}", 10)
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        for (i, name) in ["alpha", "beta", "gamma"].iter().enumerate() {
            es.execute(&format!("PUT /dbfree_t/_doc/{i}?refresh=true\n{{\"name\":\"{name}\",\"n\":{i},\"meta\":{{\"k\":\"v{i}\"}}}}"), 10).await.unwrap_or_else(|e| panic!("index: {e}"));
        }
        let cat = es.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == "dbfree_t"), "{cat:?}");
        let table = TableRef { schema: Some(INDEX_SCHEMA.into()), name: "dbfree_t".into() };
        let cols = es.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"meta.k"), "{names:?}");
        let q = PageQuery {
            sort: vec![SortRule { column: "name".into(), desc: true }],
            filters: vec![FilterRule { column: "n".into(), op: FilterOp::Gte, value: "1".into() }],
            offset: 0,
            limit: 10,
        };
        let page = es.fetch_page(&table, &q).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2, "{page:?}");
        let idx = page.columns.iter().position(|c| c.name == "name").unwrap_or_default();
        assert_eq!(page.rows[0][idx], Value::Text("gamma".into()));
        assert_eq!(es.count(&table, &q.filters).await.unwrap_or_default(), 2);
        let out = es.execute("{\"index\":\"dbfree_t\",\"query\":{\"term\":{\"name.keyword\":\"alpha\"}}}", 10).await.unwrap_or_else(|e| panic!("dsl: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 1),
            other => panic!("unexpected {other:?}"),
        }
        let out = es.execute("SELECT name, n FROM dbfree_t ORDER BY n", 10).await.unwrap_or_else(|e| panic!("sql: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 3),
            other => panic!("unexpected {other:?}"),
        }
        let out = es.execute("GET /_cat/indices", 10).await.unwrap_or_else(|e| panic!("cat: {e}"));
        assert!(matches!(&out[0], StatementResult::Rows { .. }));
        es.execute("DELETE /dbfree_t", 10).await.ok();
    }
}
