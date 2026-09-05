// SOT: weaviate-integration, weaviate-rest-api, graphql, vector-classes, weaviate-aggregate, weaviate-command-console, object-explorer, server-stats, vector-search-playground, weaviate-admin-actions

use crate::error::{AppError, AppResult};
use crate::integrations::http::{self, json_result, json_to_value, json_type_name, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat,
    StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value, VectorSearchRequest,
};
use async_trait::async_trait;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Weaviate adapter over the REST + GraphQL APIs. A "table" is a class
//        from `/v1/schema`; columns are the class properties plus `_id` (the
//        object uuid, primary key) and `_additional` (creation time, vector…).
// WHY:   The object listing endpoint (`GET /v1/objects`) supports offset
//        paging and sorting but no filtering, and GraphQL `Get` supports
//        `where` but not offset paging beyond `QUERY_MAXIMUM_RESULTS`. We use
//        the REST listing for browsing (server-side sort where the sort column
//        is a property, offset/limit) and fall back to a bounded window +
//        `http::local` when filters are present. Counts use GraphQL
//        `Aggregate { Class { meta { count } } }` with a translated `where`.
// HOW:   Auth is `Bearer <secret>` (Weaviate API key). `execute` accepts a
//        GraphQL document (`{ Get { … } }`), a JSON `{"query": "…"}` body,
//        or a raw `{"path","method","body"}` passthrough; mutations are refused
//        on read-only connections.
// WHERE: src-tauri/src/integrations/http.rs, integrations/mod.rs
// ============================================================================

const DEFAULT_PORT: u16 = 8080;
const ID_COLUMN: &str = "_id";
const ADDITIONAL_COLUMN: &str = "_additional";
const WINDOW_CAP: u64 = 5_000;

pub struct WeaviateIntegration {
    engine: Engine,
    http: HttpClient,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = match conn.secret.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(key) => Auth::Bearer(key.to_string()),
        None => Auth::None,
    };
    let is_url = conn.summary.host.as_deref().map(|h| h.starts_with("https://")).unwrap_or(false);
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), is_url, auth)?;
    let integration = WeaviateIntegration { engine: conn.summary.engine, http, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn gql_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

// WHAT:  One filter rule → GraphQL `where` operand text, or None when the rule
//        needs client-side evaluation (`_additional`, IsNotNull, In on text…).
fn where_operand(rule: &FilterRule) -> Option<String> {
    if !valid_name(&rule.column) || rule.column == ADDITIONAL_COLUMN {
        return None;
    }
    let path = if rule.column == ID_COLUMN { "id".to_string() } else { rule.column.clone() };
    let v = rule.value.trim();
    let is_int = v.parse::<i64>().is_ok();
    let is_num = v.parse::<f64>().is_ok();
    let is_bool = v == "true" || v == "false";
    let typed = |op: &str| {
        let value = if is_int {
            format!("valueInt: {v}")
        } else if is_num {
            format!("valueNumber: {v}")
        } else if is_bool {
            format!("valueBoolean: {v}")
        } else {
            format!("valueText: {}", gql_string(v))
        };
        Some(format!("{{ path: [{}], operator: {op}, {value} }}", gql_string(&path)))
    };
    match rule.op {
        FilterOp::Eq => typed("Equal"),
        FilterOp::Ne => typed("NotEqual"),
        FilterOp::Gt if is_num => typed("GreaterThan"),
        FilterOp::Gte if is_num => typed("GreaterThanEqual"),
        FilterOp::Lt if is_num => typed("LessThan"),
        FilterOp::Lte if is_num => typed("LessThanEqual"),
        FilterOp::Contains if !is_num && !is_bool => {
            Some(format!("{{ path: [{}], operator: Like, valueText: {} }}", gql_string(&path), gql_string(&format!("*{v}*"))))
        }
        FilterOp::StartsWith if !is_num && !is_bool => {
            Some(format!("{{ path: [{}], operator: Like, valueText: {} }}", gql_string(&path), gql_string(&format!("{v}*"))))
        }
        FilterOp::EndsWith if !is_num && !is_bool => {
            Some(format!("{{ path: [{}], operator: Like, valueText: {} }}", gql_string(&path), gql_string(&format!("*{v}"))))
        }
        FilterOp::IsNull => Some(format!("{{ path: [{}], operator: IsNull, valueBoolean: true }}", gql_string(&path))),
        FilterOp::IsNotNull => Some(format!("{{ path: [{}], operator: IsNull, valueBoolean: false }}", gql_string(&path))),
        FilterOp::In => {
            let ops: Vec<String> = v
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .filter_map(|item| where_operand(&FilterRule { column: rule.column.clone(), op: FilterOp::Eq, value: item.to_string() }))
                .collect();
            if ops.is_empty() {
                None
            } else {
                Some(format!("{{ operator: Or, operands: [{}] }}", ops.join(", ")))
            }
        }
        _ => None,
    }
}

// WHAT:  (GraphQL `where: {…}` clause, rules left for client-side filtering).
fn split_filters(filters: &[FilterRule]) -> (Option<String>, Vec<FilterRule>) {
    let mut server = Vec::new();
    let mut local = Vec::new();
    for rule in filters {
        match where_operand(rule) {
            Some(op) => server.push(op),
            None => local.push(rule.clone()),
        }
    }
    let clause = match server.len() {
        0 => None,
        1 => server.pop(),
        _ => Some(format!("{{ operator: And, operands: [{}] }}", server.join(", "))),
    };
    (clause, local)
}

fn columns_from_class(class: &Json) -> Vec<ColumnInfo> {
    let mut cols = vec![ColumnInfo { name: ID_COLUMN.into(), data_type: "uuid".into(), nullable: false, primary_key: true, ordinal: 0 }];
    for prop in class.get("properties").and_then(Json::as_array).into_iter().flatten() {
        let name = prop.get("name").and_then(Json::as_str).unwrap_or("property").to_string();
        let data_type = prop
            .get("dataType")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_str).collect::<Vec<_>>().join("|"))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "text".into());
        cols.push(ColumnInfo { name, data_type, nullable: true, primary_key: false, ordinal: cols.len() as u32 });
    }
    cols.push(ColumnInfo {
        name: ADDITIONAL_COLUMN.into(),
        data_type: "json".into(),
        nullable: true,
        primary_key: false,
        ordinal: cols.len() as u32,
    });
    cols
}

// WHAT:  REST object → flat row object: `_id`, properties, `_additional`.
fn flatten_object(obj: &Json) -> Json {
    let mut out = serde_json::Map::new();
    out.insert(ID_COLUMN.into(), obj.get("id").cloned().unwrap_or(Json::Null));
    if let Some(props) = obj.get("properties").and_then(Json::as_object) {
        for (k, v) in props {
            out.insert(k.clone(), v.clone());
        }
    }
    let mut additional = serde_json::Map::new();
    for key in ["creationTimeUnix", "lastUpdateTimeUnix", "vector", "vectors", "tenant", "additional"] {
        if let Some(v) = obj.get(key).filter(|v| !v.is_null()) {
            additional.insert(key.to_string(), v.clone());
        }
    }
    out.insert(ADDITIONAL_COLUMN.into(), if additional.is_empty() { Json::Null } else { Json::Object(additional) });
    Json::Object(out)
}

fn rows_aligned(docs: &[Json], columns: &[ColumnInfo]) -> ResultSet {
    let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut types: Vec<String> = columns.iter().map(|c| c.data_type.clone()).collect();
    for obj in docs.iter().filter_map(Json::as_object) {
        for (k, v) in obj {
            if !names.iter().any(|n| n == k) {
                names.push(k.clone());
                types.push(json_type_name(v).to_string());
            }
        }
    }
    let rows = docs
        .iter()
        .map(|d| {
            let obj = d.as_object();
            names.iter().map(|n| obj.and_then(|o| o.get(n)).map(json_to_value).unwrap_or(Value::Null)).collect()
        })
        .collect();
    ResultSet {
        columns: names.into_iter().zip(types).map(|(name, type_name)| ColumnMeta { name, type_name }).collect(),
        rows,
        truncated: false,
    }
}

#[derive(Debug, PartialEq)]
enum Command {
    GraphQl(String),
    Raw { method: String, path: String, body: Option<Json> },
}

impl Command {
    fn is_mutation(&self) -> bool {
        match self {
            Command::GraphQl(q) => q.trim_start().to_ascii_lowercase().starts_with("mutation"),
            Command::Raw { method, .. } => !matches!(method.as_str(), "GET" | "HEAD"),
        }
    }
}

fn parse_command(text: &str) -> AppResult<Command> {
    let text = text.trim();
    if text.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if text.starts_with('{') {
        if let Ok(value) = serde_json::from_str::<Json>(text) {
            if let Some(obj) = value.as_object() {
                if let Some(q) = obj.get("query").and_then(Json::as_str) {
                    return Ok(Command::GraphQl(q.to_string()));
                }
                if let Some(path) = obj.get("path").and_then(Json::as_str) {
                    let method = obj.get("method").and_then(Json::as_str).unwrap_or("GET").to_ascii_uppercase();
                    return Ok(Command::Raw { method, path: path.to_string(), body: obj.get("body").cloned() });
                }
                return Err(AppError::invalid_input("JSON must contain \"query\" (GraphQL) or \"path\" (raw REST request)."));
            }
        }
        // Not JSON: a bare GraphQL document `{ Get { … } }`.
        return Ok(Command::GraphQl(text.to_string()));
    }
    let lower = text.to_ascii_lowercase();
    if lower.starts_with("get") || lower.starts_with("aggregate") || lower.starts_with("explore") {
        return Ok(Command::GraphQl(format!("{{ {text} }}")));
    }
    if lower.starts_with("query") || lower.starts_with("mutation") {
        return Ok(Command::GraphQl(text.to_string()));
    }
    let mut words = text.split_whitespace();
    let verb = words.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "CLASSES" | "SCHEMA" => Ok(Command::Raw { method: "GET".into(), path: "/v1/schema".into(), body: None }),
        "OBJECTS" => {
            let class = words.next().ok_or_else(|| AppError::invalid_input("Usage: OBJECTS <class> [n]"))?;
            let n = words.next().map(|n| n.parse::<u64>().ok()).unwrap_or(Some(25)).unwrap_or(25);
            Ok(Command::Raw { method: "GET".into(), path: format!("/v1/objects?class={class}&limit={n}"), body: None })
        }
        _ => Err(AppError::invalid_input(
            "Unknown command. Use a GraphQL document ({ Get { Class { prop } } }), SCHEMA, OBJECTS <class> [n], or JSON {\"path\": ..}.",
        )),
    }
}

// WHAT:  GraphQL `{"data": {"Get": {"Class": [..]}}}` → rows of the first list found.
fn graphql_rows(data: &Json) -> Option<Vec<Json>> {
    match data {
        Json::Array(items) => Some(items.clone()),
        Json::Object(map) => map.values().find_map(graphql_rows),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

impl WeaviateIntegration {
    async fn graphql(&self, query: &str) -> AppResult<Json> {
        let resp: Json = self.http.post_json("/v1/graphql", &json!({"query": query})).await?;
        if let Some(errors) = resp.get("errors").and_then(Json::as_array).filter(|e| !e.is_empty()) {
            let msg = errors.iter().filter_map(|e| e.get("message").and_then(Json::as_str)).collect::<Vec<_>>().join("; ");
            return Err(AppError::driver(format!("GraphQL: {msg}")));
        }
        Ok(resp.get("data").cloned().unwrap_or(Json::Null))
    }

    async fn class_schema(&self, class: &str) -> AppResult<Json> {
        if !valid_name(class) {
            return Err(AppError::invalid_input(format!("Invalid class name: {class:?}")));
        }
        self.http.get_json(&format!("/v1/schema/{class}")).await
    }

    async fn list_objects(&self, class: &str, limit: u64, offset: u64, sort: &[(String, bool)]) -> AppResult<Vec<Json>> {
        if !valid_name(class) {
            return Err(AppError::invalid_input(format!("Invalid class name: {class:?}")));
        }
        let mut path = format!("/v1/objects?class={class}&limit={limit}&offset={offset}");
        if !sort.is_empty() {
            let cols: Vec<&str> = sort.iter().map(|(c, _)| c.as_str()).collect();
            let orders: Vec<&str> = sort.iter().map(|(_, d)| if *d { "desc" } else { "asc" }).collect();
            path.push_str(&format!("&sort={}&order={}", cols.join(","), orders.join(",")));
        }
        let resp: Json = self.http.get_json(&path).await?;
        Ok(resp.get("objects").and_then(Json::as_array).cloned().unwrap_or_default())
    }

    async fn aggregate_count(&self, class: &str, where_clause: Option<&str>) -> AppResult<i64> {
        if !valid_name(class) {
            return Err(AppError::invalid_input(format!("Invalid class name: {class:?}")));
        }
        let args = where_clause.map(|w| format!("(where: {w})")).unwrap_or_default();
        let query = format!("{{ Aggregate {{ {class}{args} {{ meta {{ count }} }} }} }}");
        let data = self.graphql(&query).await?;
        Ok(data
            .pointer(&format!("/Aggregate/{class}/0/meta/count"))
            .and_then(Json::as_i64)
            .unwrap_or(0))
    }

    // WHAT:  GraphQL `Get` with `where`, returning flattened rows (used when filters exist).
    async fn get_filtered(&self, class: &str, props: &[String], where_clause: &str, limit: u64) -> AppResult<Vec<Json>> {
        let fields = props.iter().filter(|p| valid_name(p)).cloned().collect::<Vec<_>>().join(" ");
        let query = format!(
            "{{ Get {{ {class}(where: {where_clause}, limit: {limit}) {{ {fields} _additional {{ id creationTimeUnix lastUpdateTimeUnix }} }} }} }}"
        );
        let data = self.graphql(&query).await?;
        let items = data.pointer(&format!("/Get/{class}")).and_then(Json::as_array).cloned().unwrap_or_default();
        Ok(items
            .iter()
            .map(|item| {
                let mut out = serde_json::Map::new();
                let additional = item.get("_additional").cloned().unwrap_or(Json::Null);
                out.insert(ID_COLUMN.into(), additional.get("id").cloned().unwrap_or(Json::Null));
                if let Some(obj) = item.as_object() {
                    for (k, v) in obj.iter().filter(|(k, _)| k.as_str() != "_additional") {
                        out.insert(k.clone(), v.clone());
                    }
                }
                out.insert(ADDITIONAL_COLUMN.into(), additional);
                Json::Object(out)
            })
            .collect())
    }

    async fn run(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        if self.read_only && cmd.is_mutation() {
            return Err(AppError::invalid_input("This connection is read-only; write operations are refused."));
        }
        match cmd {
            Command::GraphQl(query) => {
                let data = self.graphql(&query).await?;
                match graphql_rows(&data) {
                    Some(items) if items.iter().all(Json::is_object) && !items.is_empty() => {
                        let truncated = items.len() > max_rows;
                        let mut rs = http::objects_to_result_set(&items[..items.len().min(max_rows)], None, max_rows);
                        rs.truncated = truncated;
                        Ok(StatementResult::Rows { result: rs })
                    }
                    _ => Ok(StatementResult::Rows { result: json_result(data) }),
                }
            }
            Command::Raw { method, path, body } => {
                let m = reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Bad method {method}")))?;
                let mut req = self.http.request(m, &path);
                if let Some(b) = body {
                    req = req.json(&b);
                }
                let resp = self.http.send(req).await?;
                let text = resp.text().await.map_err(|e| AppError::driver(e.to_string()))?;
                let value: Json = serde_json::from_str(&text).unwrap_or(Json::String(text));
                if let Some(objects) = value.get("objects").and_then(Json::as_array) {
                    let flat: Vec<Json> = objects.iter().map(flatten_object).collect();
                    return Ok(StatementResult::Rows { result: http::objects_to_result_set(&flat, Some(ID_COLUMN), max_rows) });
                }
                if let Some(classes) = value.get("classes").and_then(Json::as_array) {
                    let docs: Vec<Json> = classes
                        .iter()
                        .map(|c| {
                            json!({
                                "class": c.get("class").cloned().unwrap_or(Json::Null),
                                "properties": c.get("properties").and_then(Json::as_array).map(|ps| ps.iter().filter_map(|p| p.get("name").and_then(Json::as_str)).collect::<Vec<_>>().join(", ")).unwrap_or_default(),
                                "vectorizer": c.get("vectorizer").cloned().unwrap_or(Json::Null),
                            })
                        })
                        .collect();
                    return Ok(StatementResult::Rows { result: http::objects_to_result_set(&docs, Some("class"), max_rows) });
                }
                Ok(StatementResult::Rows { result: json_result(value) })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Object explorer / server stats / vector search
//
// WHAT:  `objects()` lists schema classes, their shards, cluster nodes and the
//        backups of the three built-in backends; `object_detail()` adds the
//        class JSON, a property sheet and actions written as this adapter's own
//        `{"path", "method", "body"}` envelopes; `server_stats()` folds
//        `/v1/meta` and `/v1/nodes`; `vector_search()` runs a GraphQL `Get`
//        with `nearVector`.
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const BACKENDS: [&str; 3] = ["filesystem", "s3", "gcs"];
const DISTANCE_COLUMN: &str = "distance";
const CERTAINTY_COLUMN: &str = "certainty";

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

fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
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

fn raw_action(id: &str, label: &str, method: &str, path: &str, body: Option<Json>, destructive: bool) -> ObjectAction {
    let mut envelope = json!({"path": path, "method": method});
    if let Some(b) = body {
        envelope["body"] = b;
    }
    let statement = envelope.to_string();
    if destructive {
        ObjectAction::destructive(id, label, statement)
    } else {
        ObjectAction::new(id, label, statement)
    }
}

// WHAT:  `/v1/nodes?output=verbose` → objects per class, summed over shards.
fn class_counts(nodes: &Json) -> Vec<(String, f64)> {
    let mut totals: Vec<(String, f64)> = Vec::new();
    for shard in nodes.get("nodes").and_then(Json::as_array).into_iter().flatten().flat_map(|n| n.get("shards").and_then(Json::as_array).into_iter().flatten()) {
        let class = str_at(shard, "class").to_string();
        if class.is_empty() {
            continue;
        }
        let count = shard.get("objectCount").and_then(Json::as_f64).unwrap_or(0.0);
        match totals.iter_mut().find(|(c, _)| *c == class) {
            Some((_, total)) => *total += count,
            None => totals.push((class, count)),
        }
    }
    totals
}

fn class_summaries(schema: &Json, counts: &[(String, f64)]) -> Vec<ObjectSummary> {
    let list = schema
        .get("classes")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|c| {
            let name = c.get("class").and_then(Json::as_str)?;
            let props = c.get("properties").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let mut parts = Vec::new();
            if let Some((_, count)) = counts.iter().find(|(cls, _)| cls == name) {
                parts.push(format!("{} objects", crate::model::objects::format_number(*count)));
            }
            parts.push(format!("{props} properties"));
            let index = str_at(c, "vectorIndexType");
            if !index.is_empty() {
                parts.push(index.to_string());
            }
            let badge = c.get("vectorizer").map(text_of).filter(|v| !v.is_empty());
            Some(summary(ObjectKind::Collection, name, None, parts.join(" · "), badge))
        })
        .collect();
    finish(list)
}

fn shard_summaries(class: &str, body: &Json) -> Vec<ObjectSummary> {
    body.as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| {
            let name = s.get("name").and_then(Json::as_str)?;
            let mut parts = Vec::new();
            if let Some(n) = s.get("objectCount").and_then(Json::as_f64) {
                parts.push(format!("{} objects", crate::model::objects::format_number(n)));
            }
            if let Some(v) = s.get("vectorQueueSize").and_then(Json::as_f64).filter(|v| *v > 0.0) {
                parts.push(format!("queue {}", crate::model::objects::format_number(v)));
            }
            let status = str_at(s, "status").to_ascii_lowercase();
            Some(summary(ObjectKind::Shard, name, Some(class), parts.join(" · "), Some(status)))
        })
        .collect()
}

fn node_summaries(nodes: &Json) -> Vec<ObjectSummary> {
    let list = nodes
        .get("nodes")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|n| {
            let name = n.get("name").and_then(Json::as_str)?;
            let shards = n.get("shards").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let mut parts = Vec::new();
            if let Some(objects) = n.pointer("/stats/objectCount").and_then(Json::as_f64) {
                parts.push(format!("{} objects", crate::model::objects::format_number(objects)));
            }
            parts.push(format!("{shards} shards"));
            let version = str_at(n, "version");
            if !version.is_empty() {
                parts.push(format!("v{version}"));
            }
            Some(summary(ObjectKind::Node, name, None, parts.join(" · "), Some(str_at(n, "status").to_ascii_lowercase())))
        })
        .collect();
    finish(list)
}

fn backup_summaries(backend: &str, body: &Json) -> Vec<ObjectSummary> {
    body.as_array()
        .into_iter()
        .flatten()
        .filter_map(|b| {
            let id = b.get("id").and_then(Json::as_str)?;
            let classes = b.get("classes").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let mut parts = vec![format!("{backend} · {classes} classes")];
            let path = str_at(b, "path");
            if !path.is_empty() {
                parts.push(path.to_string());
            }
            Some(summary(ObjectKind::Backup, id, Some(backend), parts.join(" · "), Some(str_at(b, "status").to_ascii_lowercase())))
        })
        .collect()
}

// ---- vector search ----------------------------------------------------------

// WHAT:  JSON → a GraphQL literal: object keys and `operator` values are bare
//        identifiers, everything else keeps JSON's own quoting. Lets the user
//        paste a `where` filter as JSON and have Weaviate accept it.
fn gql_literal(value: &Json) -> String {
    match value {
        Json::Object(map) => {
            let body: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    let rendered = if k == "operator" {
                        v.as_str().map(str::to_string).unwrap_or_else(|| gql_literal(v))
                    } else {
                        gql_literal(v)
                    };
                    format!("{k}: {rendered}")
                })
                .collect();
            format!("{{{}}}", body.join(", "))
        }
        Json::Array(items) => format!("[{}]", items.iter().map(gql_literal).collect::<Vec<_>>().join(", ")),
        Json::String(s) => gql_string(s),
        other => other.to_string(),
    }
}

// WHAT:  Playground request → `{ Get { Class(nearVector: …) { props _additional {…} } } }`.
fn near_vector_query(class: &str, properties: &[String], req: &VectorSearchRequest) -> AppResult<String> {
    if !valid_name(class) {
        return Err(AppError::invalid_input(format!("Invalid class name: {class:?}")));
    }
    if req.vector.is_empty() {
        return Err(AppError::invalid_input("A query vector is required."));
    }
    let vector = req.vector.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ");
    let mut args = vec![format!("nearVector: {{vector: [{vector}]}}"), format!("limit: {}", req.top_k.max(1))];
    if let Some(filter) = req.filter.as_ref().filter(|f| f.is_object()) {
        args.push(format!("where: {}", gql_literal(filter)));
    }
    let fields: Vec<&str> = properties.iter().filter(|p| valid_name(p)).map(String::as_str).collect();
    let mut additional = vec!["id", "distance", "certainty"];
    if req.include_vectors {
        additional.push("vector");
    }
    Ok(format!(
        "{{ Get {{ {class}({args}) {{ {fields} _additional {{ {additional} }} }} }} }}",
        args = args.join(", "),
        fields = fields.join(" "),
        additional = additional.join(" ")
    ))
}

// WHAT:  GraphQL hits → grid: `_id`, `distance`, `certainty`, properties and
//        the vector when it was asked for.
fn search_hits(data: &Json, class: &str, include_vectors: bool) -> ResultSet {
    let hits = data.pointer(&format!("/Get/{class}")).and_then(Json::as_array).cloned().unwrap_or_default();
    let mut names: Vec<String> = vec![ID_COLUMN.to_string(), DISTANCE_COLUMN.to_string(), CERTAINTY_COLUMN.to_string()];
    let mut types: Vec<Option<&'static str>> = vec![Some("uuid"), Some("number"), Some("number")];
    for hit in &hits {
        for (k, v) in hit.as_object().into_iter().flatten() {
            if k == "_additional" {
                continue;
            }
            match names.iter().position(|n| n == k) {
                Some(i) => {
                    if types[i].is_none() && !v.is_null() {
                        types[i] = Some(json_type_name(v));
                    }
                }
                None => {
                    names.push(k.clone());
                    types.push((!v.is_null()).then(|| json_type_name(v)));
                }
            }
        }
    }
    if include_vectors {
        names.push("vector".to_string());
        types.push(Some("json"));
    }
    let rows = hits
        .iter()
        .map(|hit| {
            let additional = hit.get("_additional");
            names
                .iter()
                .map(|n| match n.as_str() {
                    ID_COLUMN => additional.and_then(|a| a.get("id")).map(json_to_value).unwrap_or(Value::Null),
                    DISTANCE_COLUMN => additional.and_then(|a| a.get("distance")).map(json_to_value).unwrap_or(Value::Null),
                    CERTAINTY_COLUMN => additional.and_then(|a| a.get("certainty")).map(json_to_value).unwrap_or(Value::Null),
                    "vector" => additional.and_then(|a| a.get("vector")).filter(|v| !v.is_null()).map(|v| Value::Json(v.clone())).unwrap_or(Value::Null),
                    other => hit.get(other).map(json_to_value).unwrap_or(Value::Null),
                })
                .collect()
        })
        .collect();
    ResultSet {
        columns: names.into_iter().zip(types).map(|(name, ty)| ColumnMeta { name, type_name: ty.unwrap_or("json").to_string() }).collect(),
        rows,
        truncated: false,
    }
}

// ---- server stats -----------------------------------------------------------

fn stats_groups(meta: &Json, nodes: &Json, schema: &Json) -> Vec<StatGroup> {
    let mut server = Vec::new();
    let version = str_at(meta, "version");
    if !version.is_empty() {
        server.push(Stat::text("Version", version));
    }
    let hostname = str_at(meta, "hostname");
    if !hostname.is_empty() {
        server.push(Stat::text("Hostname", hostname));
    }
    let modules: Vec<String> = meta.get("modules").and_then(Json::as_object).map(|m| m.keys().cloned().collect()).unwrap_or_default();
    server.push(Stat::text("Modules", if modules.is_empty() { "none".to_string() } else { modules.join(", ") }));
    let node_list = nodes.get("nodes").and_then(Json::as_array).cloned().unwrap_or_default();
    let classes = schema.get("classes").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
    let objects: f64 = node_list.iter().filter_map(|n| n.pointer("/stats/objectCount").and_then(Json::as_f64)).sum();
    let shards: f64 = node_list.iter().map(|n| n.get("shards").and_then(Json::as_array).map(Vec::len).unwrap_or(0) as f64).sum();
    let storage = vec![
        Stat::number("Classes", classes as f64, None),
        Stat::number("Objects", objects, None),
        Stat::number("Shards", shards, None),
    ];
    let healthy = node_list.iter().filter(|n| str_at(n, "status").eq_ignore_ascii_case("HEALTHY")).count();
    let cluster = vec![Stat::number("Nodes", node_list.len() as f64, None), Stat::number("Healthy nodes", healthy as f64, None)];
    vec![
        StatGroup { title: "Server".into(), stats: server },
        StatGroup { title: "Storage".into(), stats: storage },
        StatGroup { title: "Cluster".into(), stats: cluster },
    ]
}

impl WeaviateIntegration {
    async fn nodes_verbose(&self) -> Json {
        match self.http.get_json::<Json>("/v1/nodes?output=verbose").await {
            Ok(v) => v,
            Err(_) => self.http.get_json::<Json>("/v1/nodes").await.unwrap_or(Json::Null),
        }
    }

    async fn schema(&self) -> AppResult<Json> {
        self.http.get_json("/v1/schema").await
    }

    async fn class_names(&self, parent: Option<&str>) -> AppResult<Vec<String>> {
        if let Some(p) = parent {
            return Ok(vec![p.to_string()]);
        }
        let schema = self.schema().await?;
        let mut names: Vec<String> = schema.get("classes").and_then(Json::as_array).into_iter().flatten().filter_map(|c| c.get("class").and_then(Json::as_str).map(str::to_string)).collect();
        names.sort();
        Ok(names)
    }

    async fn list_classes(&self) -> AppResult<Vec<ObjectSummary>> {
        let schema = self.schema().await?;
        let counts = class_counts(&self.nodes_verbose().await);
        Ok(class_summaries(&schema, &counts))
    }

    async fn list_shards(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for class in self.class_names(parent).await? {
            if !valid_name(&class) {
                continue;
            }
            if let Ok(body) = self.http.get_json::<Json>(&format!("/v1/schema/{class}/shards")).await {
                list.extend(shard_summaries(&class, &body));
            }
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn list_nodes(&self) -> AppResult<Vec<ObjectSummary>> {
        Ok(node_summaries(&self.nodes_verbose().await))
    }

    // WHAT:  Backups live per backend; a backend that is not configured answers
    //        4xx, which is not an error for the explorer — just no rows.
    async fn list_backups(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let backends: Vec<&str> = match parent {
            Some(p) => vec![p],
            None => BACKENDS.to_vec(),
        };
        let mut list = Vec::new();
        for backend in backends {
            if let Ok(body) = self.http.get_json::<Json>(&format!("/v1/backups/{backend}")).await {
                list.extend(backup_summaries(backend, &body));
            }
        }
        Ok(finish(list))
    }

    async fn class_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let class = self.class_schema(name).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&class), CodeLanguage::Json);
        for (label, key) in [("Vectorizer", "vectorizer"), ("Vector index", "vectorIndexType"), ("Description", "description"), ("Replication factor", "/replicationConfig/factor")] {
            let v = if key.starts_with('/') { class.pointer(key).map(text_of) } else { class.get(key).map(text_of) };
            if let Some(v) = v.filter(|v| !v.is_empty()) {
                detail = detail.property(label, v);
            }
        }
        if let Ok(count) = self.aggregate_count(name, None).await {
            detail = detail.property("Objects", crate::model::objects::format_number(count as f64));
        }
        detail.columns = columns_from_class(&class);
        let rows = class
            .get("properties")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .map(|p| {
                let types = p.get("dataType").and_then(Json::as_array).map(|a| a.iter().filter_map(Json::as_str).collect::<Vec<_>>().join("|")).unwrap_or_default();
                vec![
                    Value::Text(str_at(p, "name").to_string()),
                    Value::Text(types),
                    Value::Text(str_at(p, "tokenization").to_string()),
                    Value::Bool(p.get("indexFilterable").and_then(Json::as_bool).unwrap_or(false)),
                ]
            })
            .collect();
        detail.rows = Some(rows_table(&[("property", "string"), ("data_type", "string"), ("tokenization", "string"), ("filterable", "boolean")], rows));
        detail.children = self.list_shards(Some(name)).await.unwrap_or_default();
        Ok(detail.action(raw_action("delete", "Delete class", "DELETE", &format!("/v1/schema/{name}"), None, true)))
    }

    async fn shard_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let class = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A shard needs its class as parent."))?;
        if !valid_name(class) {
            return Err(AppError::invalid_input(format!("Invalid class name: {class:?}")));
        }
        let body: Json = self.http.get_json(&format!("/v1/schema/{class}/shards")).await?;
        let shard = body
            .as_array()
            .into_iter()
            .flatten()
            .find(|s| s.get("name").and_then(Json::as_str) == Some(reference.name.as_str()))
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Shard {} not found in {class}.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&shard), CodeLanguage::Json).property("Class", class).property("Status", str_at(&shard, "status"));
        for (label, key) in [("Objects", "objectCount"), ("Vector queue", "vectorQueueSize")] {
            if let Some(v) = shard.get(key) {
                detail = detail.property(label, text_of(v));
            }
        }
        let path = format!("/v1/schema/{class}/shards/{}", reference.name);
        Ok(detail
            .action(raw_action("ready", "Set READY", "PUT", &path, Some(json!({"status": "READY"})), true))
            .action(raw_action("readonly", "Set READONLY", "PUT", &path, Some(json!({"status": "READONLY"})), true)))
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let nodes = self.nodes_verbose().await;
        let node = nodes
            .get("nodes")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .find(|n| n.get("name").and_then(Json::as_str) == Some(reference.name.as_str()))
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Node {} not found.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&node), CodeLanguage::Json).property("Status", str_at(&node, "status"));
        for (label, key) in [("Version", "version"), ("Git hash", "gitHash")] {
            let v = str_at(&node, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        for (label, key) in [("Objects", "/stats/objectCount"), ("Shards", "/stats/shardCount")] {
            if let Some(v) = node.pointer(key) {
                detail = detail.property(label, text_of(v));
            }
        }
        let rows = node
            .get("shards")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .map(|s| {
                vec![
                    Value::Text(str_at(s, "name").to_string()),
                    Value::Text(str_at(s, "class").to_string()),
                    Value::Int(s.get("objectCount").and_then(Json::as_i64).unwrap_or(0)),
                    Value::Text(str_at(s, "vectorIndexingStatus").to_string()),
                ]
            })
            .collect();
        detail.rows = Some(rows_table(&[("shard", "string"), ("class", "string"), ("objects", "integer"), ("vector_indexing", "string")], rows));
        Ok(detail)
    }

    async fn backup_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let backend = reference.parent.as_deref().unwrap_or("filesystem");
        let id = reference.name.as_str();
        let body: Json = self.http.get_json(&format!("/v1/backups/{backend}/{id}")).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&body), CodeLanguage::Json).property("Backend", backend);
        for (label, key) in [("Status", "status"), ("Path", "path"), ("Error", "error")] {
            let v = str_at(&body, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        let classes = body.get("classes").and_then(Json::as_array).cloned().unwrap_or_default();
        detail = detail.property("Classes", classes.len().to_string());
        detail.rows = Some(rows_table(&[("class", "string")], classes.iter().map(|c| vec![Value::Text(text_of(c))]).collect()));
        Ok(detail.action(raw_action("restore", "Restore backup", "POST", &format!("/v1/backups/{backend}/{id}/restore"), Some(json!({})), true)))
    }

    async fn similarity(&self, req: &VectorSearchRequest) -> AppResult<ResultSet> {
        let class = self.class_schema(&req.collection).await?;
        let properties: Vec<String> = class
            .get("properties")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .filter_map(|p| p.get("name").and_then(Json::as_str).map(str::to_string))
            .collect();
        let query = near_vector_query(&req.collection, &properties, req)?;
        let data = self.graphql(&query).await?;
        Ok(search_hits(&data, &req.collection, req.include_vectors))
    }

    async fn stats(&self) -> AppResult<ServerStats> {
        let meta: Json = self.http.get_json("/v1/meta").await?;
        let nodes = self.nodes_verbose().await;
        let schema = self.schema().await.unwrap_or(Json::Null);
        Ok(ServerStats::now(stats_groups(&meta, &nodes, &schema)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true,
            sql: false,
            namespaces: false,
            fixed_columns: false,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        },
        object_kinds: vec![K::Collection, K::Shard, K::Node, K::Backup],
        tools: vec![T::Stats, T::VectorSearch],
    }
}

#[async_trait]
impl Integration for WeaviateIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.http.get_text("/v1/.well-known/ready").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let meta: Json = self.http.get_json("/v1/meta").await?;
        Ok(meta.get("version").and_then(Json::as_str).map(|v| format!("Weaviate {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some("default".into())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec!["default".into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let schema: Json = self.http.get_json("/v1/schema").await?;
        let mut tables = Vec::new();
        for class in schema.get("classes").and_then(Json::as_array).into_iter().flatten() {
            let Some(name) = class.get("class").and_then(Json::as_str) else {
                continue;
            };
            let row_estimate = self.aggregate_count(name, None).await.ok();
            tables.push(TableInfo { schema: Some("classes".into()), name: name.to_string(), kind: TableKind::Table, row_estimate });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: "classes".into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let class = self.class_schema(&table.name).await?;
        Ok(columns_from_class(&class))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.aggregate_count(&table.name, None).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (clause, local) = split_filters(filters);
        if local.is_empty() {
            return self.aggregate_count(&table.name, clause.as_deref()).await;
        }
        let columns = self.columns(table).await?;
        let rs = self.window(table, &columns, clause.as_deref(), WINDOW_CAP).await?;
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        Ok(http::local::apply_filters(&names, rs.rows, &local).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let columns = self.columns(table).await?;
        let (clause, local) = split_filters(&query.filters);
        let sortable = query
            .sort
            .iter()
            .all(|s| s.column != ID_COLUMN && s.column != ADDITIONAL_COLUMN && valid_name(&s.column));
        if query.filters.is_empty() && sortable {
            let sort: Vec<(String, bool)> = query.sort.iter().map(|s| (s.column.clone(), s.desc)).collect();
            let objects = self.list_objects(&table.name, u64::from(query.limit).max(1), query.offset, &sort).await?;
            let flat: Vec<Json> = objects.iter().map(flatten_object).collect();
            return Ok(rows_aligned(&flat, &columns));
        }
        let window = (query.offset + u64::from(query.limit)).clamp(1, WINDOW_CAP);
        let rs = self.window(table, &columns, clause.as_deref(), window).await?;
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery { sort: query.sort.clone(), filters: local, offset: query.offset, limit: query.limit };
        let rows = http::local::page(&names, rs.rows, &local_query);
        Ok(ResultSet { columns: rs.columns, rows, truncated: false })
    }

    async fn execute(&self, text: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let cmd = parse_command(text)?;
        Ok(vec![self.run(cmd, max_rows).await?])
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Collection => self.list_classes().await,
            ObjectKind::Shard => self.list_shards(parent).await,
            ObjectKind::Node => self.list_nodes().await,
            ObjectKind::Backup => self.list_backups(parent).await,
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Collection => self.class_detail(reference).await,
            ObjectKind::Shard => self.shard_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
            ObjectKind::Backup => self.backup_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        self.stats().await
    }

    async fn vector_search(&self, req: &VectorSearchRequest) -> AppResult<ResultSet> {
        self.similarity(req).await
    }

    async fn close(&self) {}
}

impl WeaviateIntegration {
    // WHAT:  A bounded, unsorted window of rows: GraphQL `Get` when a `where`
    //        clause exists, otherwise the REST listing.
    async fn window(&self, table: &TableRef, columns: &[ColumnInfo], clause: Option<&str>, limit: u64) -> AppResult<ResultSet> {
        let docs = match clause {
            Some(w) => {
                let props: Vec<String> = columns
                    .iter()
                    .map(|c| c.name.clone())
                    .filter(|n| n != ID_COLUMN && n != ADDITIONAL_COLUMN)
                    .collect();
                self.get_filtered(&table.name, &props, w, limit).await?
            }
            None => self.list_objects(&table.name, limit, 0, &[]).await?.iter().map(flatten_object).collect(),
        };
        Ok(rows_aligned(&docs, columns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    #[test]
    fn where_clause_translation() {
        let rules = vec![
            FilterRule { column: "title".into(), op: FilterOp::Eq, value: "Dune".into() },
            FilterRule { column: "year".into(), op: FilterOp::Gte, value: "1965".into() },
            FilterRule { column: "title".into(), op: FilterOp::Contains, value: "un".into() },
            FilterRule { column: "_additional".into(), op: FilterOp::Eq, value: "x".into() },
        ];
        let (clause, local) = split_filters(&rules);
        let clause = clause.unwrap_or_default();
        assert!(clause.starts_with("{ operator: And, operands: ["));
        assert!(clause.contains(r#"{ path: ["title"], operator: Equal, valueText: "Dune" }"#));
        assert!(clause.contains(r#"{ path: ["year"], operator: GreaterThanEqual, valueInt: 1965 }"#));
        assert!(clause.contains(r#"operator: Like, valueText: "*un*""#));
        assert_eq!(local.len(), 1);
        let single = split_filters(&rules[..1]).0.unwrap_or_default();
        assert_eq!(single, r#"{ path: ["title"], operator: Equal, valueText: "Dune" }"#);
        let (id, _) = split_filters(&[FilterRule { column: "_id".into(), op: FilterOp::Eq, value: "abc".into() }]);
        assert_eq!(id.unwrap_or_default(), r#"{ path: ["id"], operator: Equal, valueText: "abc" }"#);
        let (any, _) = split_filters(&[FilterRule { column: "n".into(), op: FilterOp::In, value: "1,2".into() }]);
        assert!(any.unwrap_or_default().starts_with("{ operator: Or"));
    }

    #[test]
    fn class_to_columns_and_rows() {
        let class = json!({"class": "Book", "properties": [{"name": "title", "dataType": ["text"]}, {"name": "year", "dataType": ["int"]}]});
        let cols = columns_from_class(&class);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_id", "title", "year", "_additional"]);
        assert!(cols[0].primary_key);
        let objects = [json!({"id": "u1", "properties": {"title": "Dune", "extra": 1}, "creationTimeUnix": 5})];
        let flat: Vec<Json> = objects.iter().map(flatten_object).collect();
        let rs = rows_aligned(&flat, &cols);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_id", "title", "year", "_additional", "extra"]);
        assert_eq!(rs.rows[0][0], Value::Text("u1".into()));
        assert_eq!(rs.rows[0][2], Value::Null);
        assert_eq!(rs.rows[0][3], Value::Json(json!({"creationTimeUnix": 5})));
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse_command("{ Get { Book { title } } }").ok(), Some(Command::GraphQl("{ Get { Book { title } } }".into())));
        assert_eq!(parse_command("Get { Book { title } }").ok(), Some(Command::GraphQl("{ Get { Book { title } } }".into())));
        assert_eq!(parse_command(r#"{"query": "{ Get { Book { title } } }"}"#).ok(), Some(Command::GraphQl("{ Get { Book { title } } }".into())));
        assert_eq!(parse_command("schema").ok(), Some(Command::Raw { method: "GET".into(), path: "/v1/schema".into(), body: None }));
        assert_eq!(parse_command("objects Book 5").ok(), Some(Command::Raw { method: "GET".into(), path: "/v1/objects?class=Book&limit=5".into(), body: None }));
        let raw = parse_command(r#"{"path":"/v1/objects","method":"post","body":{"class":"Book"}}"#).ok();
        assert_eq!(raw, Some(Command::Raw { method: "POST".into(), path: "/v1/objects".into(), body: Some(json!({"class": "Book"})) }));
        assert!(raw.map(|c| c.is_mutation()).unwrap_or(false));
        assert!(Command::GraphQl("mutation { x }".into()).is_mutation());
        assert!(parse_command("DROP TABLE x").is_err());
    }

    #[test]
    fn graphql_rows_finds_first_list() {
        let data = json!({"Get": {"Book": [{"title": "Dune"}]}});
        assert_eq!(graphql_rows(&data).map(|r| r.len()), Some(1));
    }

    #[test]
    fn explorer_lists_classes_shards_nodes_backups() {
        let schema = json!({"classes": [
            {"class": "Book", "vectorizer": "text2vec-openai", "vectorIndexType": "hnsw", "properties": [{"name": "title", "dataType": ["text"]}, {"name": "year", "dataType": ["int"]}]},
            {"class": "Author", "vectorizer": "none", "properties": []}
        ]});
        let nodes = json!({"nodes": [
            {"name": "node1", "status": "HEALTHY", "version": "1.24.1", "stats": {"objectCount": 12, "shardCount": 2}, "shards": [{"name": "s1", "class": "Book", "objectCount": 10}, {"name": "s2", "class": "Author", "objectCount": 2}]}
        ]});
        let counts = class_counts(&nodes);
        assert_eq!(counts, vec![("Book".to_string(), 10.0), ("Author".to_string(), 2.0)]);
        let list = class_summaries(&schema, &counts);
        assert_eq!(list[0].reference.name, "Author");
        assert_eq!(list[0].badge.as_deref(), Some("none"));
        assert_eq!(list[0].detail.as_deref(), Some("2 objects · 0 properties"));
        assert_eq!(list[1].badge.as_deref(), Some("text2vec-openai"));
        assert_eq!(list[1].detail.as_deref(), Some("10 objects · 2 properties · hnsw"));

        let shards = shard_summaries("Book", &json!([{"name": "s1", "status": "READY", "objectCount": 10, "vectorQueueSize": 0}, {"name": "s2", "status": "READONLY", "objectCount": 2, "vectorQueueSize": 5}]));
        assert_eq!(shards[0].reference.parent.as_deref(), Some("Book"));
        assert_eq!(shards[0].badge.as_deref(), Some("ready"));
        assert_eq!(shards[0].detail.as_deref(), Some("10 objects"));
        assert_eq!(shards[1].detail.as_deref(), Some("2 objects · queue 5"));

        let n = node_summaries(&nodes);
        assert_eq!(n[0].reference.name, "node1");
        assert_eq!(n[0].badge.as_deref(), Some("healthy"));
        assert_eq!(n[0].detail.as_deref(), Some("12 objects · 2 shards · v1.24.1"));

        let backups = backup_summaries("s3", &json!([{"id": "nightly", "status": "SUCCESS", "classes": ["Book"], "path": "s3://bucket/nightly"}]));
        assert_eq!(backups[0].reference.parent.as_deref(), Some("s3"));
        assert_eq!(backups[0].badge.as_deref(), Some("success"));
        assert_eq!(backups[0].detail.as_deref(), Some("s3 · 1 classes · s3://bucket/nightly"));
    }

    #[test]
    fn near_vector_query_and_filter_translation() {
        let req = VectorSearchRequest {
            collection: "Book".into(),
            vector: vec![0.1, 0.25],
            vector_name: None,
            top_k: 3,
            filter: Some(json!({"path": ["year"], "operator": "GreaterThan", "valueInt": 1960})),
            include_vectors: false,
        };
        let q = near_vector_query("Book", &["title".to_string(), "bad name".to_string()], &req).unwrap_or_default();
        assert!(q.starts_with("{ Get { Book(nearVector: {vector: [0.1, 0.25]}, limit: 3, where: "));
        assert!(q.contains(r#"{path: ["year"], operator: GreaterThan, valueInt: 1960}"#));
        assert!(q.contains("{ title _additional { id distance certainty } }"));
        assert!(!q.contains("bad name"));
        let with_vec = near_vector_query("Book", &[], &VectorSearchRequest { include_vectors: true, filter: None, top_k: 0, ..req.clone() }).unwrap_or_default();
        assert!(with_vec.contains("limit: 1"));
        assert!(with_vec.contains("_additional { id distance certainty vector }"));
        assert!(!with_vec.contains("where:"));
        assert!(near_vector_query("Bad Name", &[], &req).is_err());
        assert!(near_vector_query("Book", &[], &VectorSearchRequest { vector: vec![], ..req }).is_err());
        assert_eq!(gql_literal(&json!({"operator": "And", "operands": [{"path": ["a"], "valueText": "x\"y"}]})), r#"{operator: And, operands: [{path: ["a"], valueText: "x\"y"}]}"#);
        assert_eq!(gql_literal(&json!(true)), "true");
    }

    #[test]
    fn search_hits_flatten_additional() {
        let data = json!({"Get": {"Book": [
            {"title": "Dune", "year": 1965, "_additional": {"id": "u1", "distance": 0.12, "certainty": 0.94, "vector": [0.1]}},
            {"title": "X", "_additional": {"id": "u2", "distance": 0.5, "certainty": null}}
        ]}});
        let rs = search_hits(&data, "Book", false);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_id", "distance", "certainty", "title", "year"]);
        assert_eq!(rs.rows[0][0], Value::Text("u1".into()));
        assert_eq!(rs.rows[0][1], Value::Float(0.12));
        assert_eq!(rs.rows[0][3], Value::Text("Dune".into()));
        assert_eq!(rs.rows[1][2], Value::Null);
        assert_eq!(rs.rows[1][4], Value::Null);
        let with_vec = search_hits(&data, "Book", true);
        assert_eq!(with_vec.columns.last().map(|c| c.name.as_str()), Some("vector"));
        assert_eq!(with_vec.rows[0][5], Value::Json(json!([0.1])));
        assert!(search_hits(&json!({"Get": {}}), "Book", false).rows.is_empty());
    }

    #[test]
    fn stats_groups_fold_meta_and_nodes() {
        let meta = json!({"version": "1.24.1", "hostname": "http://[::]:8080", "modules": {"text2vec-openai": {}}});
        let nodes = json!({"nodes": [
            {"name": "n1", "status": "HEALTHY", "stats": {"objectCount": 10}, "shards": [{"name": "s"}]},
            {"name": "n2", "status": "UNHEALTHY", "stats": {"objectCount": 5}, "shards": []}
        ]});
        let schema = json!({"classes": [{"class": "Book"}, {"class": "Author"}]});
        let groups = stats_groups(&meta, &nodes, &schema);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("1.24.1".into()));
        assert_eq!(find("Server", "Modules").map(|s| s.value), Some("text2vec-openai".into()));
        assert_eq!(find("Storage", "Classes").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Objects").and_then(|s| s.numeric), Some(15.0));
        assert_eq!(find("Storage", "Shards").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Cluster", "Nodes").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Cluster", "Healthy nodes").and_then(|s| s.numeric), Some(1.0));
    }

    #[test]
    fn explorer_actions_parse_as_console_commands() {
        let drop = raw_action("delete", "Delete class", "DELETE", "/v1/schema/Book", None, true);
        assert!(drop.destructive);
        assert_eq!(parse_command(&drop.statement).ok(), Some(Command::Raw { method: "DELETE".into(), path: "/v1/schema/Book".into(), body: None }));
        let ready = raw_action("ready", "Set READY", "PUT", "/v1/schema/Book/shards/s1", Some(json!({"status": "READY"})), true);
        match parse_command(&ready.statement) {
            Ok(cmd @ Command::Raw { .. }) => assert!(cmd.is_mutation()),
            other => panic!("unexpected {other:?}"),
        }
        let restore = raw_action("restore", "Restore backup", "POST", "/v1/backups/s3/nightly/restore", Some(json!({})), true);
        assert!(parse_command(&restore.statement).map(|c| c.is_mutation()).unwrap_or(false));
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_WEAVIATE_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Weaviate,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: None,
                username: None,
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: false,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_WEAVIATE_KEY").ok(),
        };
        let w = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = w.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("Weaviate"), "{version}");
        let _ = w.execute(r#"{"path":"/v1/schema/DbfreeTest","method":"DELETE"}"#, 10).await;
        w.execute(
            r#"{"path":"/v1/schema","method":"POST","body":{"class":"DbfreeTest","vectorizer":"none","properties":[{"name":"title","dataType":["text"]},{"name":"year","dataType":["int"]}]}}"#,
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("create class: {e}"));
        for (t, y) in [("Dune", 1965), ("Neuromancer", 1984)] {
            w.execute(&format!(r#"{{"path":"/v1/objects","method":"POST","body":{{"class":"DbfreeTest","properties":{{"title":"{t}","year":{y}}},"vector":[0.1,0.2]}}}}"#), 10)
                .await
                .unwrap_or_else(|e| panic!("insert: {e}"));
        }
        let table = TableRef { schema: Some("classes".into()), name: "DbfreeTest".into() };
        let cols = w.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "title"));
        assert_eq!(w.count(&table, &[]).await.unwrap_or_default(), 2);
        let filters = vec![FilterRule { column: "year".into(), op: FilterOp::Gt, value: "1970".into() }];
        assert_eq!(w.count(&table, &filters).await.unwrap_or_default(), 1);
        let page = w
            .fetch_page(&table, &PageQuery { sort: vec![], filters, offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1);
        let page = w
            .fetch_page(&table, &PageQuery { sort: vec![crate::model::SortRule { column: "year".into(), desc: true }], filters: vec![], offset: 0, limit: 1 })
            .await
            .unwrap_or_else(|e| panic!("sorted page: {e}"));
        assert_eq!(page.rows[0][2], Value::Int(1984));
        let res = w.execute("{ Get { DbfreeTest { title } } }", 10).await.unwrap_or_else(|e| panic!("gql: {e}"));
        assert!(matches!(&res[0], StatementResult::Rows { result } if result.rows.len() == 2));
        let _ = w.execute(r#"{"path":"/v1/schema/DbfreeTest","method":"DELETE"}"#, 10).await;
    }
}
