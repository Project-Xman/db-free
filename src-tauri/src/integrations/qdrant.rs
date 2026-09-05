// SOT: qdrant-integration, qdrant-rest-api, vector-collections, qdrant-scroll, qdrant-filter-dsl, qdrant-command-console, object-explorer, server-stats, vector-search-playground, qdrant-admin-actions

use crate::error::{AppError, AppResult};
use crate::integrations::http::{self, json_result, json_to_value, json_type_name, objects_to_result_set, Auth, HttpClient};
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
// WHAT:  Qdrant adapter over its REST API. A "table" is a collection, a "row"
//        is a point flattened to `id` + top-level payload keys + `_vector`.
// WHY:   Qdrant has no SQL; the grid needs stable columns, so `columns()`
//        samples 50 points via `scroll` and unions the payload keys.
// HOW:   Filters that Qdrant understands (match / range / is_null / any) are
//        pushed down as a `must` filter; the rest (Contains, Ne, …) fall back
//        to client-side filtering. Sort is always client-side over a bounded
//        scroll window (`offset + limit`, capped at 2 000). `execute` accepts
//        JSON envelopes `{"collection": …, "search"|"scroll"|"upsert"|"delete": …}`,
//        a raw `{"method","path","body"}` passthrough, plus the shorthands
//        `COLLECTIONS`, `INFO <c>` and `SCROLL <c> [n]`.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs
// ============================================================================

const DEFAULT_PORT: u16 = 6333;
const SAMPLE_SIZE: u64 = 50;
const SCROLL_CAP: u64 = 2_000;
const ID_COLUMN: &str = "id";
const VECTOR_COLUMN: &str = "_vector";

pub struct QdrantIntegration {
    engine: Engine,
    http: HttpClient,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = match conn.secret.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(key) => Auth::Header { name: "api-key".into(), value: key.to_string() },
        None => Auth::None,
    };
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, auth)?;
    let integration = QdrantIntegration { engine: conn.summary.engine, http, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested)
// ---------------------------------------------------------------------------

fn collection_path(name: &str) -> AppResult<String> {
    if name.is_empty() || name.contains('/') || name.contains('?') {
        return Err(AppError::invalid_input(format!("Invalid collection name: {name:?}")));
    }
    Ok(format!("/collections/{name}"))
}

fn parse_scalar(text: &str) -> Json {
    let t = text.trim();
    if let Ok(i) = t.parse::<i64>() {
        return Json::from(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return serde_json::Number::from_f64(f).map(Json::Number).unwrap_or(Json::String(t.to_string()));
    }
    match t {
        "true" => Json::Bool(true),
        "false" => Json::Bool(false),
        _ => Json::String(t.to_string()),
    }
}

fn number(text: &str) -> Option<Json> {
    let t = text.trim();
    t.parse::<i64>().ok().map(Json::from).or_else(|| t.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(Json::Number))
}

// WHAT:  One filter rule → one Qdrant `must` condition, or None when the rule
//        must be evaluated client-side (id column, text ops, Ne, non-numeric range).
fn qdrant_condition(rule: &FilterRule) -> Option<Json> {
    if rule.column == ID_COLUMN || rule.column == VECTOR_COLUMN {
        return None;
    }
    let key = &rule.column;
    match rule.op {
        FilterOp::Eq => Some(json!({"key": key, "match": {"value": parse_scalar(&rule.value)}})),
        FilterOp::Gt => number(&rule.value).map(|n| json!({"key": key, "range": {"gt": n}})),
        FilterOp::Gte => number(&rule.value).map(|n| json!({"key": key, "range": {"gte": n}})),
        FilterOp::Lt => number(&rule.value).map(|n| json!({"key": key, "range": {"lt": n}})),
        FilterOp::Lte => number(&rule.value).map(|n| json!({"key": key, "range": {"lte": n}})),
        FilterOp::IsNull => Some(json!({"is_null": {"key": key}})),
        FilterOp::In => {
            let values: Vec<Json> = rule.value.split(',').map(parse_scalar).collect();
            Some(json!({"key": key, "match": {"any": values}}))
        }
        FilterOp::Ne | FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith | FilterOp::IsNotNull => None,
    }
}

// WHAT:  Splits the rules into (server-side filter document, rules left for local::page).
fn split_filters(filters: &[FilterRule]) -> (Option<Json>, Vec<FilterRule>) {
    let mut must = Vec::new();
    let mut local = Vec::new();
    for rule in filters {
        match qdrant_condition(rule) {
            Some(cond) => must.push(cond),
            None => local.push(rule.clone()),
        }
    }
    let filter = if must.is_empty() { None } else { Some(json!({"must": must})) };
    (filter, local)
}

// WHAT:  Point → flat object: `id`, payload keys, `_vector` (if present).
fn flatten_point(point: &Json) -> Json {
    let mut obj = serde_json::Map::new();
    obj.insert(ID_COLUMN.into(), point.get("id").cloned().unwrap_or(Json::Null));
    if let Some(payload) = point.get("payload").and_then(Json::as_object) {
        for (k, v) in payload {
            obj.insert(k.clone(), v.clone());
        }
    }
    if let Some(vector) = point.get("vector").filter(|v| !v.is_null()) {
        obj.insert(VECTOR_COLUMN.into(), vector.clone());
    }
    Json::Object(obj)
}

fn columns_from_points(points: &[Json]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = vec![ID_COLUMN.to_string()];
    let mut types: Vec<Option<&'static str>> = vec![Some("integer")];
    for point in points {
        if let Some(payload) = point.get("payload").and_then(Json::as_object) {
            for (k, v) in payload {
                let idx = match names.iter().position(|n| n == k) {
                    Some(i) => i,
                    None => {
                        names.push(k.clone());
                        types.push(None);
                        names.len() - 1
                    }
                };
                if types[idx].is_none() && !v.is_null() {
                    types[idx] = Some(json_type_name(v));
                }
            }
        }
    }
    if let Some(first) = points.first() {
        if let Some(id) = first.get("id") {
            types[0] = Some(json_type_name(id));
        }
    }
    let mut cols: Vec<ColumnInfo> = names
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, ty))| ColumnInfo {
            primary_key: i == 0,
            name,
            data_type: ty.unwrap_or("json").to_string(),
            nullable: i != 0,
            ordinal: i as u32,
        })
        .collect();
    cols.push(ColumnInfo {
        name: VECTOR_COLUMN.into(),
        data_type: "json".into(),
        nullable: true,
        primary_key: false,
        ordinal: cols.len() as u32,
    });
    cols
}

// WHAT:  Rows aligned to the known column set (extra payload keys appended).
fn points_to_result_set(points: &[Json], columns: &[ColumnInfo]) -> ResultSet {
    let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut types: Vec<String> = columns.iter().map(|c| c.data_type.clone()).collect();
    let flat: Vec<Json> = points.iter().map(flatten_point).collect();
    for obj in flat.iter().filter_map(Json::as_object) {
        for (k, v) in obj {
            if !names.iter().any(|n| n == k) {
                names.push(k.clone());
                types.push(json_type_name(v).to_string());
            }
        }
    }
    let rows = flat
        .iter()
        .map(|doc| {
            let obj = doc.as_object();
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
    Collections,
    Info(String),
    Scroll { collection: String, limit: u64 },
    Search { collection: String, body: Json },
    ScrollJson { collection: String, body: Json },
    Upsert { collection: String, body: Json },
    Delete { collection: String, body: Json },
    Count { collection: String, body: Json },
    Raw { method: String, path: String, body: Option<Json> },
}

impl Command {
    fn is_mutation(&self) -> bool {
        match self {
            Command::Upsert { .. } | Command::Delete { .. } => true,
            Command::Raw { method, .. } => !matches!(method.as_str(), "GET" | "HEAD"),
            _ => false,
        }
    }
}

fn parse_command(text: &str) -> AppResult<Command> {
    let text = text.trim();
    if text.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if text.starts_with('{') {
        let value: Json = serde_json::from_str(text).map_err(|e| AppError::invalid_input(format!("Invalid JSON: {e}")))?;
        return parse_json_command(value);
    }
    let mut words = text.split_whitespace();
    let verb = words.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "COLLECTIONS" => Ok(Command::Collections),
        "INFO" => {
            let c = words.next().ok_or_else(|| AppError::invalid_input("Usage: INFO <collection>"))?;
            Ok(Command::Info(c.to_string()))
        }
        "SCROLL" => {
            let c = words.next().ok_or_else(|| AppError::invalid_input("Usage: SCROLL <collection> [n]"))?;
            let limit = match words.next() {
                Some(n) => n.parse::<u64>().map_err(|_| AppError::invalid_input("SCROLL limit must be a number"))?,
                None => 100,
            };
            Ok(Command::Scroll { collection: c.to_string(), limit })
        }
        _ => Err(AppError::invalid_input(
            "Unknown command. Use COLLECTIONS, INFO <c>, SCROLL <c> [n], or a JSON body like {\"collection\": \"c\", \"search\": {...}}.",
        )),
    }
}

fn parse_json_command(value: Json) -> AppResult<Command> {
    let obj = value.as_object().ok_or_else(|| AppError::invalid_input("Expected a JSON object."))?;
    if let Some(path) = obj.get("path").and_then(Json::as_str) {
        let method = obj.get("method").and_then(Json::as_str).unwrap_or("GET").to_ascii_uppercase();
        return Ok(Command::Raw { method, path: path.to_string(), body: obj.get("body").cloned() });
    }
    let collection = obj
        .get("collection")
        .and_then(Json::as_str)
        .ok_or_else(|| AppError::invalid_input("Missing \"collection\" (or \"path\" for a raw request)."))?
        .to_string();
    let body = |key: &str| obj.get(key).cloned();
    if let Some(body) = body("search") {
        return Ok(Command::Search { collection, body });
    }
    if let Some(body) = body("scroll") {
        return Ok(Command::ScrollJson { collection, body });
    }
    if let Some(body) = body("upsert") {
        return Ok(Command::Upsert { collection, body });
    }
    if let Some(body) = body("delete") {
        return Ok(Command::Delete { collection, body });
    }
    if let Some(body) = body("count") {
        return Ok(Command::Count { collection, body });
    }
    Ok(Command::Info(collection))
}

fn unwrap_result(value: Json) -> Json {
    match value {
        Json::Object(mut obj) if obj.contains_key("result") => obj.remove("result").unwrap_or(Json::Null),
        other => other,
    }
}

fn result_points(value: &Json) -> Vec<Json> {
    value
        .get("points")
        .or(Some(value))
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

impl QdrantIntegration {
    async fn scroll(&self, collection: &str, limit: u64, filter: Option<Json>, with_vector: bool) -> AppResult<Vec<Json>> {
        let mut body = json!({"limit": limit.min(SCROLL_CAP), "with_payload": true, "with_vector": with_vector});
        if let Some(f) = filter {
            body["filter"] = f;
        }
        let path = format!("{}/points/scroll", collection_path(collection)?);
        let resp: Json = self.http.post_json(&path, &body).await?;
        Ok(result_points(&unwrap_result(resp)))
    }

    async fn collection_info(&self, collection: &str) -> AppResult<Json> {
        let resp: Json = self.http.get_json(&collection_path(collection)?).await?;
        Ok(unwrap_result(resp))
    }

    async fn collection_names(&self) -> AppResult<Vec<String>> {
        let resp: Json = self.http.get_json("/collections").await?;
        let result = unwrap_result(resp);
        let names = result
            .get("collections")
            .and_then(Json::as_array)
            .map(|items| items.iter().filter_map(|c| c.get("name").and_then(Json::as_str).map(str::to_string)).collect())
            .unwrap_or_default();
        Ok(names)
    }

    async fn run(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        if self.read_only && cmd.is_mutation() {
            return Err(AppError::invalid_input("This connection is read-only; write operations are refused."));
        }
        let rows = |value: Json| StatementResult::Rows { result: json_result(value) };
        match cmd {
            Command::Collections => {
                let names = self.collection_names().await?;
                let docs: Vec<Json> = names.into_iter().map(|n| json!({"name": n})).collect();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, Some("name"), max_rows) })
            }
            Command::Info(c) => Ok(rows(self.collection_info(&c).await?)),
            Command::Scroll { collection, limit } => {
                let points = self.scroll(&collection, limit.min(max_rows as u64), None, false).await?;
                let flat: Vec<Json> = points.iter().map(flatten_point).collect();
                Ok(StatementResult::Rows { result: objects_to_result_set(&flat, Some(ID_COLUMN), max_rows) })
            }
            Command::ScrollJson { collection, mut body } => {
                if body.get("limit").is_none() {
                    body["limit"] = json!(max_rows.min(SCROLL_CAP as usize));
                }
                if body.get("with_payload").is_none() {
                    body["with_payload"] = json!(true);
                }
                let path = format!("{}/points/scroll", collection_path(&collection)?);
                let resp: Json = self.http.post_json(&path, &body).await?;
                let points = result_points(&unwrap_result(resp));
                let flat: Vec<Json> = points.iter().map(flatten_point).collect();
                Ok(StatementResult::Rows { result: objects_to_result_set(&flat, Some(ID_COLUMN), max_rows) })
            }
            Command::Search { collection, mut body } => {
                if body.get("limit").is_none() {
                    body["limit"] = json!(max_rows.min(SCROLL_CAP as usize));
                }
                if body.get("with_payload").is_none() {
                    body["with_payload"] = json!(true);
                }
                let path = format!("{}/points/search", collection_path(&collection)?);
                let resp: Json = self.http.post_json(&path, &body).await?;
                let hits = unwrap_result(resp);
                let flat: Vec<Json> = hits
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .map(|hit| {
                                let mut obj = flatten_point(hit);
                                if let (Some(map), Some(score)) = (obj.as_object_mut(), hit.get("score")) {
                                    map.insert("_score".into(), score.clone());
                                }
                                obj
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(StatementResult::Rows { result: objects_to_result_set(&flat, Some(ID_COLUMN), max_rows) })
            }
            Command::Count { collection, body } => {
                let path = format!("{}/points/count", collection_path(&collection)?);
                let resp: Json = self.http.post_json(&path, &body).await?;
                Ok(rows(unwrap_result(resp)))
            }
            Command::Upsert { collection, body } => {
                let n = body.get("points").and_then(Json::as_array).map(Vec::len).unwrap_or(0) as u64;
                let path = format!("{}/points?wait=true", collection_path(&collection)?);
                let _: Json = self.http.put_json(&path, &body).await?;
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Delete { collection, body } => {
                let n = body.get("points").and_then(Json::as_array).map(Vec::len).unwrap_or(0) as u64;
                let path = format!("{}/points/delete?wait=true", collection_path(&collection)?);
                let _: Json = self.http.post_json(&path, &body).await?;
                Ok(StatementResult::Affected { rows_affected: n })
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
                Ok(rows(value))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Object explorer / server stats / vector search
//
// WHAT:  `objects()` lists collections, aliases, per-collection snapshots and
//        payload indexes, and cluster peers; `object_detail()` adds the JSON
//        config, a property sheet and actions expressed as this adapter's own
//        `{"method": …, "path": …}` envelopes; `server_stats()` folds `/`,
//        `/telemetry`, `/cluster` and the collection aggregates;
//        `vector_search()` is the similarity playground.
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const SCORE_COLUMN: &str = "score";

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

// WHAT:  `config.params.vectors` is either one unnamed spec `{size, distance}`
//        or a map of named ones; both become (name, size, distance) triples.
fn vector_specs(info: &Json) -> Vec<(String, u64, String)> {
    let Some(vectors) = info.pointer("/config/params/vectors") else { return Vec::new() };
    if let Some(size) = vectors.get("size").and_then(Json::as_u64) {
        return vec![(String::new(), size, str_at(vectors, "distance").to_string())];
    }
    let mut specs: Vec<(String, u64, String)> = vectors
        .as_object()
        .into_iter()
        .flatten()
        .map(|(name, spec)| (name.clone(), spec.get("size").and_then(Json::as_u64).unwrap_or(0), str_at(spec, "distance").to_string()))
        .collect();
    specs.sort_by(|a, b| a.0.cmp(&b.0));
    specs
}

fn vector_text(specs: &[(String, u64, String)]) -> String {
    specs
        .iter()
        .map(|(name, size, distance)| if name.is_empty() { format!("{size}d {distance}") } else { format!("{name}: {size}d {distance}") })
        .collect::<Vec<_>>()
        .join(", ")
}

fn collection_summary(name: &str, info: &Json) -> ObjectSummary {
    let mut parts = Vec::new();
    if let Some(points) = info.get("points_count").and_then(Json::as_f64) {
        parts.push(format!("{} points", crate::model::objects::format_number(points)));
    }
    let specs = vector_text(&vector_specs(info));
    if !specs.is_empty() {
        parts.push(specs);
    }
    if let Some(segments) = info.get("segments_count").and_then(Json::as_f64) {
        parts.push(format!("{} segments", crate::model::objects::format_number(segments)));
    }
    summary(ObjectKind::Collection, name, None, parts.join(" · "), Some(str_at(info, "status").to_string()))
}

fn alias_summaries(body: &Json, parent: Option<&str>) -> Vec<ObjectSummary> {
    let list = body
        .get("aliases")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|a| {
            let alias = a.get("alias_name").and_then(Json::as_str)?;
            let target = str_at(a, "collection_name");
            if parent.is_some_and(|p| p != target) {
                return None;
            }
            Some(summary(ObjectKind::Alias, alias, Some(target), format!("→ {target}"), None))
        })
        .collect();
    finish(list)
}

fn snapshot_summaries(collection: &str, body: &Json) -> Vec<ObjectSummary> {
    body.as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| {
            let name = s.get("name").and_then(Json::as_str)?;
            let mut parts = Vec::new();
            if let Some(size) = s.get("size").and_then(Json::as_f64) {
                parts.push(human_bytes(size));
            }
            let created = str_at(s, "creation_time");
            if !created.is_empty() {
                parts.push(created.to_string());
            }
            Some(summary(ObjectKind::Snapshot, name, Some(collection), parts.join(" · "), None))
        })
        .collect()
}

// WHAT:  `payload_schema` entries are the payload indexes of a collection.
fn index_summaries(collection: &str, info: &Json) -> Vec<ObjectSummary> {
    info.get("payload_schema")
        .and_then(Json::as_object)
        .into_iter()
        .flatten()
        .map(|(field, spec)| {
            let kind = spec.get("data_type").map(text_of).filter(|k| !k.is_empty()).unwrap_or_else(|| text_of(spec));
            let detail = spec.get("points").and_then(Json::as_f64).map(|p| format!("{} points", crate::model::objects::format_number(p))).unwrap_or_default();
            summary(ObjectKind::Index, field, Some(collection), detail, Some(kind))
        })
        .collect()
}

// WHAT:  `/cluster` when clustering is on, else one synthetic local node.
fn node_summaries(cluster: &Json, local: &str) -> Vec<ObjectSummary> {
    let status = str_at(cluster, "status");
    let peers = cluster.get("peers").and_then(Json::as_object);
    if status != "enabled" || peers.map(|p| p.is_empty()).unwrap_or(true) {
        return vec![summary(ObjectKind::Node, local, None, "clustering disabled".into(), Some("single".into()))];
    }
    let self_id = cluster.get("peer_id").map(text_of).unwrap_or_default();
    let leader = cluster.pointer("/raft_info/leader").map(text_of).unwrap_or_default();
    let list = peers
        .into_iter()
        .flatten()
        .map(|(id, peer)| {
            let uri = str_at(peer, "uri");
            let mut parts = Vec::new();
            if !uri.is_empty() {
                parts.push(uri.to_string());
            }
            if *id == self_id {
                parts.push("this node".into());
            }
            let badge = if *id == leader { "leader" } else { "peer" };
            summary(ObjectKind::Node, id, None, parts.join(" · "), Some(badge.to_string()))
        })
        .collect();
    finish(list)
}

fn raw_action(id: &str, label: &str, method: &str, path: &str, destructive: bool) -> ObjectAction {
    let statement = json!({"method": method, "path": path}).to_string();
    if destructive {
        ObjectAction::destructive(id, label, statement)
    } else {
        ObjectAction::new(id, label, statement)
    }
}

// ---- vector search ----------------------------------------------------------

// WHAT:  Playground request → `/collections/{c}/points/search` body. A named
//        vector becomes `{"name": …, "vector": […]}`; the filter is Qdrant's
//        own filter document, passed through untouched.
fn search_body(req: &VectorSearchRequest) -> Json {
    let vector = match req.vector_name.as_deref().map(str::trim).filter(|n| !n.is_empty()) {
        Some(name) => json!({"name": name, "vector": req.vector}),
        None => json!(req.vector),
    };
    let mut body = json!({
        "vector": vector,
        "limit": req.top_k.max(1),
        "with_payload": true,
        "with_vector": req.include_vectors,
    });
    if let Some(filter) = req.filter.clone().filter(|f| !f.is_null()) {
        body["filter"] = filter;
    }
    body
}

// WHAT:  Scored hits → grid: `id`, `score`, payload keys, `_vector` when asked.
fn search_hits(result: &Json, include_vectors: bool) -> ResultSet {
    let hits = result.as_array().cloned().unwrap_or_default();
    let mut names: Vec<String> = vec![ID_COLUMN.to_string(), SCORE_COLUMN.to_string()];
    let mut types: Vec<Option<&'static str>> = vec![None, Some("number")];
    for hit in &hits {
        if types[0].is_none() {
            if let Some(id) = hit.get("id").filter(|v| !v.is_null()) {
                types[0] = Some(json_type_name(id));
            }
        }
        for (k, v) in hit.get("payload").and_then(Json::as_object).into_iter().flatten() {
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
        names.push(VECTOR_COLUMN.to_string());
        types.push(Some("json"));
    }
    let rows = hits
        .iter()
        .map(|hit| {
            names
                .iter()
                .map(|n| match n.as_str() {
                    ID_COLUMN => hit.get("id").map(json_to_value).unwrap_or(Value::Null),
                    SCORE_COLUMN => hit.get("score").map(json_to_value).unwrap_or(Value::Null),
                    VECTOR_COLUMN => hit.get("vector").filter(|v| !v.is_null()).map(|v| Value::Json(v.clone())).unwrap_or(Value::Null),
                    other => hit.get("payload").and_then(|p| p.get(other)).map(json_to_value).unwrap_or(Value::Null),
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

// WHAT:  Root (version), telemetry (requests, peers) and the per-collection
//        info the explorer already loaded → stat groups.
fn stats_groups(root: &Json, telemetry: &Json, cluster: &Json, collections: &[(String, Json)]) -> Vec<StatGroup> {
    let mut server = Vec::new();
    let version = str_at(root, "version");
    if !version.is_empty() {
        server.push(Stat::text("Version", version));
    }
    let title = str_at(root, "title");
    if !title.is_empty() {
        server.push(Stat::text("Service", title));
    }
    if let Some(id) = telemetry.pointer("/id").map(text_of).filter(|s| !s.is_empty()) {
        server.push(Stat::text("Instance", id));
    }
    let status = str_at(cluster, "status");
    if !status.is_empty() {
        server.push(Stat::text("Clustering", status));
    }
    let mut storage = vec![Stat::number("Collections", collections.len() as f64, None)];
    for (label, key) in [("Points", "points_count"), ("Vectors", "vectors_count"), ("Indexed vectors", "indexed_vectors_count"), ("Segments", "segments_count")] {
        let total: f64 = collections.iter().filter_map(|(_, info)| info.get(key).and_then(Json::as_f64)).sum();
        if total > 0.0 {
            storage.push(Stat::number(label, total, None));
        }
    }
    let green = collections.iter().filter(|(_, info)| str_at(info, "status") == "green").count();
    storage.push(Stat::number("Green collections", green as f64, None));
    let mut clusterg = Vec::new();
    let peers = cluster.get("peers").and_then(Json::as_object).map(|p| p.len()).unwrap_or(0);
    if peers > 0 {
        clusterg.push(Stat::number("Peers", peers as f64, None));
    }
    for (label, key) in [("Raft term", "/raft_info/term"), ("Raft commit", "/raft_info/commit"), ("Pending operations", "/raft_info/pending_operations")] {
        if let Some(v) = cluster.pointer(key).and_then(Json::as_f64) {
            clusterg.push(Stat::number(label, v, None));
        }
    }
    if let Some(role) = cluster.pointer("/raft_info/role").map(text_of).filter(|r| !r.is_empty()) {
        clusterg.push(Stat::text("Raft role", role));
    }
    let mut throughput = Vec::new();
    for (label, key) in [("REST responses", "/requests/rest/responses"), ("gRPC responses", "/requests/grpc/responses")] {
        if let Some(node) = telemetry.pointer(key).and_then(Json::as_object) {
            let total: f64 = node
                .values()
                .flat_map(|by_status| by_status.as_object().into_iter().flatten().map(|(_, v)| v.get("count").and_then(Json::as_f64).unwrap_or(0.0)).collect::<Vec<f64>>())
                .sum();
            if total > 0.0 {
                throughput.push(Stat::number(label, total, None));
            }
        }
    }
    if let Some(v) = telemetry.pointer("/collections/number_of_collections").and_then(Json::as_f64) {
        throughput.push(Stat::number("Telemetry collections", v, None));
    }
    let mut groups = vec![StatGroup { title: "Server".into(), stats: server }, StatGroup { title: "Storage".into(), stats: storage }];
    for (title, stats) in [("Cluster", clusterg), ("Throughput", throughput)] {
        if !stats.is_empty() {
            groups.push(StatGroup { title: title.into(), stats });
        }
    }
    groups
}

impl QdrantIntegration {
    async fn cluster_info(&self) -> Json {
        self.http.get_json::<Json>("/cluster").await.map(unwrap_result).unwrap_or(Json::Null)
    }

    fn local_name(&self) -> String {
        self.http.base().trim_start_matches("https://").trim_start_matches("http://").to_string()
    }

    async fn scoped_collections(&self, parent: Option<&str>) -> AppResult<Vec<String>> {
        match parent {
            Some(p) => Ok(vec![p.to_string()]),
            None => {
                let mut names = self.collection_names().await?;
                names.sort();
                Ok(names)
            }
        }
    }

    async fn list_collections(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.scoped_collections(None).await? {
            let info = self.collection_info(&name).await.unwrap_or(Json::Null);
            list.push(collection_summary(&name, &info));
        }
        Ok(finish(list))
    }

    async fn list_aliases(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let path = match parent {
            Some(p) => format!("{}/aliases", collection_path(p)?),
            None => "/aliases".to_string(),
        };
        let body: Json = self.http.get_json(&path).await?;
        Ok(alias_summaries(&unwrap_result(body), parent))
    }

    async fn list_snapshots(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.scoped_collections(parent).await? {
            let path = format!("{}/snapshots", collection_path(&name)?);
            if let Ok(body) = self.http.get_json::<Json>(&path).await {
                list.extend(snapshot_summaries(&name, &unwrap_result(body)));
            }
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn list_indexes(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.scoped_collections(parent).await? {
            let info = self.collection_info(&name).await.unwrap_or(Json::Null);
            list.extend(index_summaries(&name, &info));
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn list_nodes(&self) -> AppResult<Vec<ObjectSummary>> {
        Ok(node_summaries(&self.cluster_info().await, &self.local_name()))
    }

    async fn collection_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let info = self.collection_info(name).await?;
        let specs = vector_specs(&info);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&info), CodeLanguage::Json);
        for (label, key) in [("Status", "status"), ("Optimizer", "optimizer_status")] {
            let v = info.get(key).map(text_of).unwrap_or_default();
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        for (label, key) in [("Points", "points_count"), ("Vectors", "vectors_count"), ("Indexed vectors", "indexed_vectors_count"), ("Segments", "segments_count")] {
            if let Some(v) = info.get(key).and_then(Json::as_f64) {
                detail = detail.property(label, crate::model::objects::format_number(v));
            }
        }
        if !specs.is_empty() {
            detail = detail.property("Vectors", vector_text(&specs));
        }
        for (label, key) in [("Shards", "/config/params/shard_number"), ("Replication factor", "/config/params/replication_factor"), ("Write consistency factor", "/config/params/write_consistency_factor")] {
            if let Some(v) = info.pointer(key) {
                detail = detail.property(label, text_of(v));
            }
        }
        detail.columns = self.columns(&TableRef { schema: Some("collections".into()), name: name.to_string() }).await.unwrap_or_default();
        detail.rows = Some(rows_table(
            &[("vector", "string"), ("size", "integer"), ("distance", "string")],
            specs
                .iter()
                .map(|(n, size, distance)| vec![Value::Text(if n.is_empty() { "default".into() } else { n.clone() }), Value::Int(*size as i64), Value::Text(distance.clone())])
                .collect(),
        ));
        let mut children = index_summaries(name, &info);
        if let Ok(aliases) = self.list_aliases(Some(name)).await {
            children.extend(aliases);
        }
        if let Ok(snaps) = self.list_snapshots(Some(name)).await {
            children.extend(snaps);
        }
        detail.children = finish(children);
        let p = format!("/collections/{name}");
        Ok(detail
            .action(raw_action("snapshot", "Create snapshot", "POST", &format!("{p}/snapshots"), false))
            .action(ObjectAction::destructive("clear", "Delete all points", json!({"collection": name, "delete": {"filter": {}}}).to_string()))
            .action(raw_action("delete", "Delete collection", "DELETE", &p, true)))
    }

    async fn alias_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let body: Json = self.http.get_json("/aliases").await?;
        let aliases = unwrap_result(body);
        let entry = aliases
            .get("aliases")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .find(|a| a.get("alias_name").and_then(Json::as_str) == Some(name))
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Alias {name} not found.")))?;
        let target = str_at(&entry, "collection_name").to_string();
        let detail = ObjectDetail::empty(reference).definition(pretty(&entry), CodeLanguage::Json).property("Collection", &target);
        let statement = json!({"method": "POST", "path": "/collections/aliases", "body": {"actions": [{"delete_alias": {"alias_name": name}}]}}).to_string();
        Ok(detail.action(ObjectAction::destructive("delete", "Delete alias", statement)))
    }

    async fn snapshot_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let collection = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A snapshot needs its collection as parent."))?;
        let path = format!("{}/snapshots", collection_path(collection)?);
        let body: Json = self.http.get_json(&path).await?;
        let entry = unwrap_result(body)
            .as_array()
            .into_iter()
            .flatten()
            .find(|s| s.get("name").and_then(Json::as_str) == Some(reference.name.as_str()))
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Snapshot {} not found.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&entry), CodeLanguage::Json).property("Collection", collection);
        if let Some(size) = entry.get("size").and_then(Json::as_f64) {
            detail = detail.property("Size", human_bytes(size));
        }
        for (label, key) in [("Created", "creation_time"), ("Checksum", "checksum")] {
            let v = str_at(&entry, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        Ok(detail.action(raw_action("delete", "Delete snapshot", "DELETE", &format!("{path}/{}", reference.name), true)))
    }

    async fn index_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let collection = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A payload index needs its collection as parent."))?;
        let info = self.collection_info(collection).await?;
        let spec = info
            .pointer(&format!("/payload_schema/{}", reference.name))
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Payload index {} not found in {collection}.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference)
            .definition(pretty(&spec), CodeLanguage::Json)
            .property("Collection", collection)
            .property("Data type", spec.get("data_type").map(text_of).unwrap_or_else(|| text_of(&spec)));
        if let Some(points) = spec.get("points").and_then(Json::as_f64) {
            detail = detail.property("Indexed points", crate::model::objects::format_number(points));
        }
        let statement = json!({"method": "DELETE", "path": format!("/collections/{collection}/index/{}", reference.name)}).to_string();
        Ok(detail.action(ObjectAction::destructive("delete", "Delete payload index", statement)))
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let cluster = self.cluster_info().await;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&cluster), CodeLanguage::Json).property("Clustering", str_at(&cluster, "status"));
        if let Some(peer) = cluster.pointer(&format!("/peers/{}", reference.name)) {
            detail = detail.property("URI", str_at(peer, "uri"));
        }
        if let Some(id) = cluster.get("peer_id") {
            detail = detail.property("This peer", text_of(id));
        }
        for (label, key) in [("Raft term", "/raft_info/term"), ("Raft commit", "/raft_info/commit"), ("Pending operations", "/raft_info/pending_operations"), ("Role", "/raft_info/role"), ("Leader", "/raft_info/leader")] {
            if let Some(v) = cluster.pointer(key) {
                detail = detail.property(label, text_of(v));
            }
        }
        let rows = cluster
            .get("peers")
            .and_then(Json::as_object)
            .into_iter()
            .flatten()
            .map(|(id, peer)| vec![Value::Text(id.clone()), Value::Text(str_at(peer, "uri").to_string())])
            .collect();
        detail.rows = Some(rows_table(&[("peer", "string"), ("uri", "string")], rows));
        Ok(detail)
    }

    async fn similarity(&self, req: &VectorSearchRequest) -> AppResult<ResultSet> {
        if req.vector.is_empty() {
            return Err(AppError::invalid_input("A query vector is required."));
        }
        let path = format!("{}/points/search", collection_path(&req.collection)?);
        let resp: Json = self.http.post_json(&path, &search_body(req)).await?;
        Ok(search_hits(&unwrap_result(resp), req.include_vectors))
    }

    async fn stats(&self) -> AppResult<ServerStats> {
        let root: Json = self.http.get_json("/").await.unwrap_or(Json::Null);
        let telemetry: Json = self.http.get_json::<Json>("/telemetry?details_level=1").await.map(unwrap_result).unwrap_or(Json::Null);
        let cluster = self.cluster_info().await;
        let mut collections = Vec::new();
        for name in self.collection_names().await.unwrap_or_default() {
            let info = self.collection_info(&name).await.unwrap_or(Json::Null);
            collections.push((name, info));
        }
        Ok(ServerStats::now(stats_groups(&root, &telemetry, &cluster, &collections)))
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
        object_kinds: vec![K::Collection, K::Alias, K::Snapshot, K::Index, K::Node],
        tools: vec![T::Stats, T::VectorSearch],
    }
}

#[async_trait]
impl Integration for QdrantIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        if self.http.get_text("/healthz").await.is_ok() {
            return Ok(());
        }
        let _: Json = self.http.get_json("/").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let root: Json = self.http.get_json("/").await?;
        Ok(root.get("version").and_then(Json::as_str).map(|v| format!("Qdrant {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some("default".into())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec!["default".into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut tables = Vec::new();
        for name in self.collection_names().await? {
            let row_estimate = self.collection_info(&name).await.ok().and_then(|i| i.get("points_count").and_then(Json::as_i64));
            tables.push(TableInfo { schema: Some("collections".into()), name, kind: TableKind::Table, row_estimate });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: "collections".into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let points = self.scroll(&table.name, SAMPLE_SIZE, None, false).await?;
        Ok(columns_from_points(&points))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let info = self.collection_info(&table.name).await?;
        Ok(info.get("points_count").and_then(Json::as_i64))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (filter, local) = split_filters(filters);
        if !local.is_empty() {
            // Client-side rules: count over a bounded scroll window.
            let points = self.scroll(&table.name, SCROLL_CAP, filter, false).await?;
            let columns = columns_from_points(&points);
            let rs = points_to_result_set(&points, &columns);
            let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
            return Ok(http::local::apply_filters(&names, rs.rows, &local).len() as i64);
        }
        let mut body = json!({"exact": true});
        if let Some(f) = filter {
            body["filter"] = f;
        }
        let path = format!("{}/points/count", collection_path(&table.name)?);
        let resp: Json = self.http.post_json(&path, &body).await?;
        Ok(unwrap_result(resp).get("count").and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let (filter, local) = split_filters(&query.filters);
        let window = (query.offset + u64::from(query.limit)).clamp(1, SCROLL_CAP);
        let points = self.scroll(&table.name, window, filter, false).await?;
        let columns = columns_from_points(&points);
        let rs = points_to_result_set(&points, &columns);
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
            ObjectKind::Collection => self.list_collections().await,
            ObjectKind::Alias => self.list_aliases(parent).await,
            ObjectKind::Snapshot => self.list_snapshots(parent).await,
            ObjectKind::Index => self.list_indexes(parent).await,
            ObjectKind::Node => self.list_nodes().await,
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Collection => self.collection_detail(reference).await,
            ObjectKind::Alias => self.alias_detail(reference).await,
            ObjectKind::Snapshot => self.snapshot_detail(reference).await,
            ObjectKind::Index => self.index_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    #[test]
    fn eq_and_range_filters_push_down() {
        let rules = vec![
            FilterRule { column: "city".into(), op: FilterOp::Eq, value: "Berlin".into() },
            FilterRule { column: "age".into(), op: FilterOp::Gte, value: "18".into() },
            FilterRule { column: "tags".into(), op: FilterOp::In, value: "a, 2".into() },
            FilterRule { column: "name".into(), op: FilterOp::Contains, value: "x".into() },
            FilterRule { column: "id".into(), op: FilterOp::Eq, value: "1".into() },
        ];
        let (filter, local) = split_filters(&rules);
        let filter = filter.unwrap_or_default();
        let must = filter["must"].as_array().map(Vec::len).unwrap_or(0);
        assert_eq!(must, 3);
        assert_eq!(filter["must"][0], json!({"key": "city", "match": {"value": "Berlin"}}));
        assert_eq!(filter["must"][1], json!({"key": "age", "range": {"gte": 18}}));
        assert_eq!(filter["must"][2], json!({"key": "tags", "match": {"any": ["a", 2]}}));
        assert_eq!(local.len(), 2);
    }

    #[test]
    fn columns_union_payload_keys_and_vector() {
        let points = vec![
            json!({"id": 1, "payload": {"city": "Berlin"}}),
            json!({"id": "uuid", "payload": {"age": 3, "city": null}}),
        ];
        let cols = columns_from_points(&points);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "city", "age", "_vector"]);
        assert!(cols[0].primary_key);
        assert_eq!(cols[2].data_type, "integer");
        let rs = points_to_result_set(&points, &cols);
        assert_eq!(rs.rows[0][1], Value::Text("Berlin".into()));
        assert_eq!(rs.rows[1][0], Value::Text("uuid".into()));
    }

    #[test]
    fn parses_shorthands_and_json() {
        assert_eq!(parse_command("collections").ok(), Some(Command::Collections));
        assert_eq!(parse_command("SCROLL docs 5").ok(), Some(Command::Scroll { collection: "docs".into(), limit: 5 }));
        assert_eq!(parse_command("info docs").ok(), Some(Command::Info("docs".into())));
        let cmd = parse_command(r#"{"collection":"docs","search":{"vector":[0.1],"limit":3}}"#).ok();
        assert_eq!(cmd, Some(Command::Search { collection: "docs".into(), body: json!({"vector":[0.1],"limit":3}) }));
        let raw = parse_command(r#"{"method":"post","path":"/collections/x/points/search","body":{}}"#).ok();
        assert_eq!(raw, Some(Command::Raw { method: "POST".into(), path: "/collections/x/points/search".into(), body: Some(json!({})) }));
        assert!(parse_command("DROP everything").is_err());
        assert!(parse_command("{\"collection\":\"c\",\"upsert\":{}}").map(|c| c.is_mutation()).unwrap_or(false));
        assert!(collection_path("a/b").is_err());
    }

    #[test]
    fn unwraps_result_envelope() {
        let v = unwrap_result(json!({"result": {"points": [{"id": 1}]}, "status": "ok"}));
        assert_eq!(result_points(&v).len(), 1);
    }

    #[test]
    fn explorer_summaries_from_collection_info() {
        let info = json!({
            "status": "green",
            "points_count": 1500,
            "vectors_count": 3000,
            "segments_count": 4,
            "config": {"params": {"vectors": {"size": 384, "distance": "Cosine"}, "shard_number": 1}},
            "payload_schema": {"city": {"data_type": "keyword", "points": 1500}, "year": {"data_type": "integer"}}
        });
        let s = collection_summary("docs", &info);
        assert_eq!(s.badge.as_deref(), Some("green"));
        assert_eq!(s.detail.as_deref(), Some("1,500 points · 384d Cosine · 4 segments"));
        assert_eq!(vector_specs(&info), vec![(String::new(), 384, "Cosine".to_string())]);

        let named = json!({"config": {"params": {"vectors": {"image": {"size": 512, "distance": "Dot"}, "text": {"size": 768, "distance": "Cosine"}}}}});
        assert_eq!(vector_text(&vector_specs(&named)), "image: 512d Dot, text: 768d Cosine");
        assert!(vector_specs(&json!({})).is_empty());

        let idx = index_summaries("docs", &info);
        assert_eq!(idx.len(), 2);
        assert!(idx.iter().any(|i| i.reference.name == "city" && i.badge.as_deref() == Some("keyword") && i.detail.as_deref() == Some("1,500 points")));
        assert!(idx.iter().any(|i| i.reference.parent.as_deref() == Some("docs") && i.reference.name == "year"));

        let aliases = json!({"aliases": [{"alias_name": "current", "collection_name": "docs"}, {"alias_name": "other", "collection_name": "misc"}]});
        assert_eq!(alias_summaries(&aliases, None).len(), 2);
        let scoped = alias_summaries(&aliases, Some("docs"));
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].reference.parent.as_deref(), Some("docs"));

        let snaps = snapshot_summaries("docs", &json!([{"name": "docs-2026.snapshot", "size": 2048, "creation_time": "2026-01-01T00:00:00"}]));
        assert_eq!(snaps[0].reference.parent.as_deref(), Some("docs"));
        assert_eq!(snaps[0].detail.as_deref(), Some("2.0 KB · 2026-01-01T00:00:00"));
    }

    #[test]
    fn nodes_reflect_cluster_state() {
        let single = node_summaries(&json!({"status": "disabled"}), "localhost:6333");
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].reference.name, "localhost:6333");
        assert_eq!(single[0].badge.as_deref(), Some("single"));
        let clustered = node_summaries(
            &json!({"status": "enabled", "peer_id": 1, "peers": {"1": {"uri": "http://a:6335"}, "2": {"uri": "http://b:6335"}}, "raft_info": {"leader": 2, "role": "Follower"}}),
            "local",
        );
        assert_eq!(clustered.len(), 2);
        assert_eq!(clustered[0].reference.name, "1");
        assert_eq!(clustered[0].detail.as_deref(), Some("http://a:6335 · this node"));
        assert_eq!(clustered[0].badge.as_deref(), Some("peer"));
        assert_eq!(clustered[1].badge.as_deref(), Some("leader"));
    }

    #[test]
    fn vector_search_body_and_hits() {
        let req = VectorSearchRequest {
            collection: "docs".into(),
            vector: vec![0.1, 0.9],
            vector_name: None,
            top_k: 5,
            filter: Some(json!({"must": [{"key": "city", "match": {"value": "Berlin"}}]})),
            include_vectors: false,
        };
        let body = search_body(&req);
        assert_eq!(body["vector"], json!([0.1, 0.9]));
        assert_eq!(body["limit"], 5);
        assert_eq!(body["with_payload"], json!(true));
        assert_eq!(body["with_vector"], json!(false));
        assert_eq!(body["filter"]["must"][0]["key"], "city");
        let named = search_body(&VectorSearchRequest { vector_name: Some("image".into()), filter: None, include_vectors: true, top_k: 0, ..req.clone() });
        assert_eq!(named["vector"], json!({"name": "image", "vector": [0.1, 0.9]}));
        assert_eq!(named["limit"], 1);
        assert_eq!(named["with_vector"], json!(true));
        assert!(named.get("filter").is_none());

        let result = json!([
            {"id": 1, "score": 0.98, "payload": {"city": "Berlin", "n": 3}, "vector": [0.1, 0.9]},
            {"id": 2, "score": 0.42, "payload": {"city": "Paris"}}
        ]);
        let rs = search_hits(&result, false);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "score", "city", "n"]);
        assert_eq!(rs.columns[0].type_name, "integer");
        assert_eq!(rs.rows[0][1], Value::Float(0.98));
        assert_eq!(rs.rows[0][2], Value::Text("Berlin".into()));
        assert_eq!(rs.rows[1][3], Value::Null);
        let with_vec = search_hits(&result, true);
        assert_eq!(with_vec.columns.last().map(|c| c.name.as_str()), Some("_vector"));
        assert_eq!(with_vec.rows[0][4], Value::Json(json!([0.1, 0.9])));
        assert_eq!(with_vec.rows[1][4], Value::Null);
        assert!(search_hits(&Json::Null, false).rows.is_empty());
    }

    #[test]
    fn stats_groups_aggregate_collections() {
        let collections = vec![
            ("a".to_string(), json!({"status": "green", "points_count": 10, "vectors_count": 10, "segments_count": 2})),
            ("b".to_string(), json!({"status": "yellow", "points_count": 5, "vectors_count": 5, "segments_count": 1})),
        ];
        let cluster = json!({"status": "enabled", "peers": {"1": {}, "2": {}}, "raft_info": {"term": 3, "commit": 42, "pending_operations": 0, "role": "Leader"}});
        let telemetry = json!({"id": "abc", "requests": {"rest": {"responses": {"POST /collections": {"200": {"count": 12}}}}}});
        let groups = stats_groups(&json!({"title": "qdrant - vector search engine", "version": "1.9.0"}), &telemetry, &cluster, &collections);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("1.9.0".into()));
        assert_eq!(find("Server", "Clustering").map(|s| s.value), Some("enabled".into()));
        assert_eq!(find("Storage", "Collections").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Points").and_then(|s| s.numeric), Some(15.0));
        assert_eq!(find("Storage", "Green collections").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Cluster", "Peers").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Cluster", "Raft commit").and_then(|s| s.numeric), Some(42.0));
        assert_eq!(find("Throughput", "REST responses").and_then(|s| s.numeric), Some(12.0));
        assert_eq!(human_bytes(1024.0), "1.0 KB");
    }

    #[test]
    fn explorer_actions_parse_as_console_commands() {
        let drop = raw_action("delete", "Delete collection", "DELETE", "/collections/docs", true);
        assert!(drop.destructive);
        assert_eq!(
            parse_command(&drop.statement).ok(),
            Some(Command::Raw { method: "DELETE".into(), path: "/collections/docs".into(), body: None })
        );
        let alias = json!({"method": "POST", "path": "/collections/aliases", "body": {"actions": [{"delete_alias": {"alias_name": "x"}}]}}).to_string();
        assert!(parse_command(&alias).map(|c| c.is_mutation()).unwrap_or(false));
        let clear = json!({"collection": "docs", "delete": {"filter": {}}}).to_string();
        assert!(matches!(parse_command(&clear), Ok(Command::Delete { .. })));
        let snap = raw_action("snapshot", "Create snapshot", "POST", "/collections/docs/snapshots", false);
        assert!(!snap.destructive);
        assert!(parse_command(&snap.statement).is_ok());
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_QDRANT_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Qdrant,
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
            secret: std::env::var("DBFREE_TEST_QDRANT_KEY").ok(),
        };
        let q = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = q.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("Qdrant"), "{version}");
        // Create a collection, upsert, browse, count, search, delete.
        let _ = q
            .execute(r#"{"method":"DELETE","path":"/collections/dbfree_test"}"#, 10)
            .await;
        q.execute(r#"{"method":"PUT","path":"/collections/dbfree_test","body":{"vectors":{"size":2,"distance":"Cosine"}}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        q.execute(
            r#"{"collection":"dbfree_test","upsert":{"points":[{"id":1,"vector":[0.1,0.9],"payload":{"city":"Berlin","n":1}},{"id":2,"vector":[0.9,0.1],"payload":{"city":"Paris","n":2}}]}}"#,
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("upsert: {e}"));
        let table = TableRef { schema: Some("collections".into()), name: "dbfree_test".into() };
        let cols = q.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "city"));
        let filters = vec![FilterRule { column: "city".into(), op: FilterOp::Eq, value: "Paris".into() }];
        assert_eq!(q.count(&table, &filters).await.unwrap_or_default(), 1);
        let page = q
            .fetch_page(&table, &PageQuery { sort: vec![], filters, offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1);
        let res = q
            .execute(r#"{"collection":"dbfree_test","search":{"vector":[0.1,0.9],"limit":1}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("search: {e}"));
        assert!(matches!(&res[0], StatementResult::Rows { result } if result.rows.len() == 1));
        let _ = q.execute(r#"{"method":"DELETE","path":"/collections/dbfree_test"}"#, 10).await;
    }
}
