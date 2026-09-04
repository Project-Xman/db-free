// SOT: elasticsearch-integration, opensearch-integration, query-dsl, es-sql, es-mapping-flatten, es-rest-console

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, json_to_value, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SortRule, StatementResult, TableInfo, TableKind, TableRef, Value,
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

    async fn run_sql(&self, sql: &str, max_rows: usize) -> AppResult<ResultSet> {
        let path = if self.is_opensearch() { "/_plugins/_sql?format=json" } else { "/_sql?format=json" };
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

#[async_trait]
impl Integration for ElasticsearchIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: true, transactions: false, exact_estimate: true }
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
