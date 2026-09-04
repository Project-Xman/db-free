// SOT: chroma-integration, chroma-rest-api, vector-collections, chroma-api-v2, chroma-api-v1-fallback, chroma-where-filter, chroma-command-console, object-explorer, vector-search-playground, chroma-admin-actions

use crate::error::{AppError, AppResult};
use crate::integrations::http::{self, json_result, json_to_value, json_type_name, objects_to_result_set, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, StatementResult,
    TableInfo, TableKind, TableRef, Value, VectorSearchRequest,
};
use async_trait::async_trait;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// ============================================================================
// WHAT:  Chroma adapter over the HTTP API. v2 (`/api/v2/tenants/{t}/databases/{d}/…`)
//        is preferred; when the v2 heartbeat 404s the adapter falls back to v1
//        (`/api/v1/…` with `?tenant=&database=` query params). A "table" is a
//        collection; a "row" is `id` + `document` + top-level metadata keys.
// WHY:   Chroma addresses collections by uuid, not name, so the adapter
//        resolves and caches `name → id`. Columns come from sampling 50
//        records (`/get`) and unioning metadata keys; there is no schema.
// HOW:   Eq / Ne / Gt / Gte / Lt / Lte / In on metadata keys push down as a
//        `where` document (`$eq`, `$gt`, `$in`…); `Contains` on `document`
//        becomes `where_document: {$contains}`; anything else runs client-side
//        over a bounded window. Counts use `/count` when everything pushes
//        down. `execute` accepts JSON `{"collection": …, "query"|"get"|"add"|
//        "upsert"|"update"|"delete": {…}}`, raw `{"path","method","body"}`, plus
//        `COLLECTIONS`, `GET <collection> [n]` and `COUNT <collection>`.
// WHERE: src-tauri/src/integrations/http.rs, integrations/mod.rs
// ============================================================================

const DEFAULT_PORT: u16 = 8000;
const DEFAULT_TENANT: &str = "default_tenant";
const DEFAULT_DATABASE: &str = "default_database";
const SAMPLE_SIZE: u64 = 50;
const WINDOW_CAP: u64 = 5_000;
const ID_COLUMN: &str = "id";
const DOCUMENT_COLUMN: &str = "document";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApiVersion {
    V2,
    V1,
}

pub struct ChromaIntegration {
    engine: Engine,
    http: HttpClient,
    tenant: String,
    database: String,
    api: ApiVersion,
    ids: Mutex<HashMap<String, String>>,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let auth = match conn.secret.as_deref().map(str::trim).filter(|k| !k.is_empty()) {
        Some(key) => http::Auth::Bearer(key.to_string()),
        None => http::Auth::None,
    };
    let is_url = s.host.as_deref().map(|h| h.starts_with("https://")).unwrap_or(false);
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), is_url, auth)?;
    let tenant = s.username.as_deref().map(str::trim).filter(|t| !t.is_empty()).unwrap_or(DEFAULT_TENANT).to_string();
    let database = s.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).unwrap_or(DEFAULT_DATABASE).to_string();
    let api = probe_version(&http).await?;
    Ok(Arc::new(ChromaIntegration { engine: s.engine, http, tenant, database, api, ids: Mutex::new(HashMap::new()), read_only: s.read_only }))
}

// WHAT:  v2 heartbeat first; a 404 means an older server → v1. Auth/network
//        errors propagate so the user sees the real cause.
async fn probe_version(http: &HttpClient) -> AppResult<ApiVersion> {
    match http.get_text("/api/v2/heartbeat").await {
        Ok(_) => Ok(ApiVersion::V2),
        Err(AppError::NotFound { .. }) => {
            http.get_text("/api/v1/heartbeat").await?;
            Ok(ApiVersion::V1)
        }
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

fn scalar(text: &str) -> Json {
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

// WHAT:  (where, where_document, local rules) from the grid filters.
fn split_filters(filters: &[FilterRule]) -> (Option<Json>, Option<Json>, Vec<FilterRule>) {
    let mut where_ops = Vec::new();
    let mut doc_ops = Vec::new();
    let mut local = Vec::new();
    for f in filters {
        let v = f.value.trim();
        let numeric = v.parse::<f64>().is_ok();
        if f.column == DOCUMENT_COLUMN {
            match f.op {
                FilterOp::Contains => doc_ops.push(json!({"$contains": v})),
                _ => local.push(f.clone()),
            }
            continue;
        }
        if f.column == ID_COLUMN {
            local.push(f.clone());
            continue;
        }
        let cond = match f.op {
            FilterOp::Eq => Some(json!({&f.column: {"$eq": scalar(v)}})),
            FilterOp::Ne => Some(json!({&f.column: {"$ne": scalar(v)}})),
            FilterOp::Gt if numeric => Some(json!({&f.column: {"$gt": scalar(v)}})),
            FilterOp::Gte if numeric => Some(json!({&f.column: {"$gte": scalar(v)}})),
            FilterOp::Lt if numeric => Some(json!({&f.column: {"$lt": scalar(v)}})),
            FilterOp::Lte if numeric => Some(json!({&f.column: {"$lte": scalar(v)}})),
            FilterOp::In => Some(json!({&f.column: {"$in": v.split(',').map(scalar).collect::<Vec<_>>()}})),
            _ => None,
        };
        match cond {
            Some(c) => where_ops.push(c),
            None => local.push(f.clone()),
        }
    }
    let combine = |mut ops: Vec<Json>| match ops.len() {
        0 => None,
        1 => ops.pop(),
        _ => Some(json!({"$and": ops})),
    };
    (combine(where_ops), combine(doc_ops), local)
}

// WHAT:  Chroma's columnar `/get` response → row objects.
fn records_to_objects(resp: &Json) -> Vec<Json> {
    let ids = resp.get("ids").and_then(Json::as_array).cloned().unwrap_or_default();
    let docs = resp.get("documents").and_then(Json::as_array).cloned().unwrap_or_default();
    let metas = resp.get("metadatas").and_then(Json::as_array).cloned().unwrap_or_default();
    let embeddings = resp.get("embeddings").and_then(Json::as_array).cloned().unwrap_or_default();
    let distances = resp.get("distances").and_then(Json::as_array).cloned().unwrap_or_default();
    ids.iter()
        .enumerate()
        .map(|(i, id)| {
            let mut obj = serde_json::Map::new();
            obj.insert(ID_COLUMN.into(), id.clone());
            obj.insert(DOCUMENT_COLUMN.into(), docs.get(i).cloned().unwrap_or(Json::Null));
            if let Some(meta) = metas.get(i).and_then(Json::as_object) {
                for (k, v) in meta {
                    obj.insert(k.clone(), v.clone());
                }
            }
            if let Some(e) = embeddings.get(i).filter(|e| !e.is_null()) {
                obj.insert("_embedding".into(), e.clone());
            }
            if let Some(d) = distances.get(i).filter(|d| !d.is_null()) {
                obj.insert("_distance".into(), d.clone());
            }
            Json::Object(obj)
        })
        .collect()
}

// WHAT:  Query responses are nested one level deeper (one list per query embedding); flatten.
fn query_to_objects(resp: &Json) -> Vec<Json> {
    let take = |key: &str, q: usize| resp.get(key).and_then(Json::as_array).and_then(|a| a.get(q)).cloned().unwrap_or(Json::Null);
    let n = resp.get("ids").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
    let mut out = Vec::new();
    for q in 0..n {
        let flat = json!({
            "ids": take("ids", q),
            "documents": take("documents", q),
            "metadatas": take("metadatas", q),
            "embeddings": take("embeddings", q),
            "distances": take("distances", q),
        });
        for mut obj in records_to_objects(&flat) {
            if n > 1 {
                if let Some(m) = obj.as_object_mut() {
                    m.insert("_query".into(), json!(q));
                }
            }
            out.push(obj);
        }
    }
    out
}

fn columns_from_objects(objects: &[Json]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = vec![ID_COLUMN.into(), DOCUMENT_COLUMN.into()];
    let mut types: Vec<Option<&'static str>> = vec![Some("string"), Some("string")];
    for obj in objects.iter().filter_map(Json::as_object) {
        for (k, v) in obj {
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
    names
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, ty))| ColumnInfo { primary_key: i == 0, nullable: i != 0, name, data_type: ty.unwrap_or("json").into(), ordinal: i as u32 })
        .collect()
}

fn rows_aligned(objects: &[Json], columns: &[ColumnInfo]) -> ResultSet {
    let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut types: Vec<String> = columns.iter().map(|c| c.data_type.clone()).collect();
    for obj in objects.iter().filter_map(Json::as_object) {
        for (k, v) in obj {
            if !names.iter().any(|n| n == k) {
                names.push(k.clone());
                types.push(json_type_name(v).into());
            }
        }
    }
    let rows = objects
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
    Collections,
    Count(String),
    Get { collection: String, body: Json },
    Query { collection: String, body: Json },
    Write { collection: String, op: String, body: Json },
    Raw { method: String, path: String, body: Option<Json> },
}

impl Command {
    fn is_mutation(&self) -> bool {
        match self {
            Command::Write { .. } => true,
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
        let obj = value.as_object().ok_or_else(|| AppError::invalid_input("Expected a JSON object."))?;
        if let Some(path) = obj.get("path").and_then(Json::as_str) {
            let method = obj.get("method").and_then(Json::as_str).unwrap_or("GET").to_ascii_uppercase();
            return Ok(Command::Raw { method, path: path.to_string(), body: obj.get("body").cloned() });
        }
        let collection = obj
            .get("collection")
            .and_then(Json::as_str)
            .map(str::to_string)
            .ok_or_else(|| AppError::invalid_input("Missing \"collection\" (or \"path\" for a raw request)."))?;
        if let Some(body) = obj.get("query").cloned() {
            return Ok(Command::Query { collection, body });
        }
        if let Some(body) = obj.get("get").cloned() {
            return Ok(Command::Get { collection, body });
        }
        for op in ["add", "upsert", "update", "delete"] {
            if let Some(body) = obj.get(op).cloned() {
                return Ok(Command::Write { collection, op: op.to_string(), body });
            }
        }
        if obj.get("count").is_some() {
            return Ok(Command::Count(collection));
        }
        return Err(AppError::invalid_input("JSON needs one of: query, get, add, upsert, update, delete, count."));
    }
    let mut words = text.split_whitespace();
    let verb = words.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "COLLECTIONS" => Ok(Command::Collections),
        "COUNT" => {
            let c = words.next().ok_or_else(|| AppError::invalid_input("Usage: COUNT <collection>"))?;
            Ok(Command::Count(c.to_string()))
        }
        "GET" => {
            let c = words.next().ok_or_else(|| AppError::invalid_input("Usage: GET <collection> [n]"))?;
            let n = match words.next() {
                Some(n) => n.parse::<u64>().map_err(|_| AppError::invalid_input("GET limit must be a number"))?,
                None => 100,
            };
            Ok(Command::Get { collection: c.to_string(), body: json!({"limit": n}) })
        }
        _ => Err(AppError::invalid_input(
            "Unknown command. Use COLLECTIONS, COUNT <c>, GET <c> [n], or JSON like {\"collection\": \"c\", \"query\": {...}}.",
        )),
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

impl ChromaIntegration {
    fn collections_path(&self) -> String {
        match self.api {
            ApiVersion::V2 => format!("/api/v2/tenants/{}/databases/{}/collections", self.tenant, self.database),
            ApiVersion::V1 => format!("/api/v1/collections?tenant={}&database={}", self.tenant, self.database),
        }
    }

    fn collection_path(&self, id: &str, suffix: &str) -> String {
        match self.api {
            ApiVersion::V2 => format!("/api/v2/tenants/{}/databases/{}/collections/{id}{suffix}", self.tenant, self.database),
            ApiVersion::V1 => format!("/api/v1/collections/{id}{suffix}"),
        }
    }

    async fn list_collections(&self) -> AppResult<Vec<Json>> {
        let path = match self.api {
            ApiVersion::V2 => format!("{}?limit=1000", self.collections_path()),
            ApiVersion::V1 => self.collections_path(),
        };
        let resp: Json = self.http.get_json(&path).await?;
        let list = resp.as_array().cloned().or_else(|| resp.get("collections").and_then(Json::as_array).cloned()).unwrap_or_default();
        let mut ids = self.ids.lock().await;
        for c in &list {
            if let (Some(name), Some(id)) = (c.get("name").and_then(Json::as_str), c.get("id").and_then(Json::as_str)) {
                ids.insert(name.to_string(), id.to_string());
            }
        }
        Ok(list)
    }

    async fn collection_id(&self, name: &str) -> AppResult<String> {
        if let Some(id) = self.ids.lock().await.get(name) {
            return Ok(id.clone());
        }
        self.list_collections().await?;
        self.ids
            .lock()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Collection {name} not found.")))
    }

    async fn get_records(&self, name: &str, mut body: Json) -> AppResult<Vec<Json>> {
        let id = self.collection_id(name).await?;
        if body.get("include").is_none() {
            body["include"] = json!(["metadatas", "documents"]);
        }
        let resp: Json = self.http.post_json(&self.collection_path(&id, "/get"), &body).await?;
        Ok(records_to_objects(&resp))
    }

    async fn count_all(&self, name: &str) -> AppResult<i64> {
        let id = self.collection_id(name).await?;
        let resp: Json = self.http.get_json(&self.collection_path(&id, "/count")).await?;
        Ok(resp.as_i64().unwrap_or(0))
    }

    async fn run(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        if self.read_only && cmd.is_mutation() {
            return Err(AppError::invalid_input("This connection is read-only; write operations are refused."));
        }
        match cmd {
            Command::Collections => {
                let list = self.list_collections().await?;
                let docs: Vec<Json> = list
                    .iter()
                    .map(|c| {
                        json!({
                            "name": c.get("name").cloned().unwrap_or(Json::Null),
                            "id": c.get("id").cloned().unwrap_or(Json::Null),
                            "metadata": c.get("metadata").cloned().unwrap_or(Json::Null),
                            "dimension": c.get("dimension").cloned().unwrap_or(Json::Null),
                        })
                    })
                    .collect();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, Some("name"), max_rows) })
            }
            Command::Count(c) => Ok(StatementResult::Rows { result: json_result(json!({"count": self.count_all(&c).await?})) }),
            Command::Get { collection, mut body } => {
                if body.get("limit").is_none() {
                    body["limit"] = json!(max_rows.min(WINDOW_CAP as usize));
                }
                let objects = self.get_records(&collection, body).await?;
                Ok(StatementResult::Rows { result: objects_to_result_set(&objects, Some(ID_COLUMN), max_rows) })
            }
            Command::Query { collection, mut body } => {
                let id = self.collection_id(&collection).await?;
                if body.get("n_results").is_none() {
                    body["n_results"] = json!(max_rows.min(1000));
                }
                if body.get("include").is_none() {
                    body["include"] = json!(["metadatas", "documents", "distances"]);
                }
                let resp: Json = self.http.post_json(&self.collection_path(&id, "/query"), &body).await?;
                let objects = query_to_objects(&resp);
                Ok(StatementResult::Rows { result: objects_to_result_set(&objects, Some(ID_COLUMN), max_rows) })
            }
            Command::Write { collection, op, body } => {
                let id = self.collection_id(&collection).await?;
                let n = body.get("ids").and_then(Json::as_array).map(Vec::len).unwrap_or(0) as u64;
                let resp = self.http.send(self.http.request(reqwest::Method::POST, &self.collection_path(&id, &format!("/{op}"))).json(&body)).await?;
                let _ = resp.text().await;
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
                Ok(StatementResult::Rows { result: json_result(value) })
            }
        }
    }

    async fn window(&self, name: &str, where_: Option<Json>, where_doc: Option<Json>, limit: u64) -> AppResult<Vec<Json>> {
        let mut body = json!({"limit": limit.clamp(1, WINDOW_CAP)});
        if let Some(w) = where_ {
            body["where"] = w;
        }
        if let Some(d) = where_doc {
            body["where_document"] = d;
        }
        self.get_records(name, body).await
    }
}

// ---------------------------------------------------------------------------
// Object explorer / vector search
//
// WHAT:  `objects()` lists the tenant's databases and the collections of the
//        current one; `object_detail()` adds the collection JSON, a property
//        sheet (uuid, count, metadata, dimension) and a delete action written
//        as this adapter's own `{"path", "method"}` envelope; `vector_search()`
//        posts `/query` with `query_embeddings`.
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const DISTANCE_COLUMN: &str = "_distance";

fn text_of(v: &Json) -> String {
    match v {
        Json::Null => String::new(),
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
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

// WHAT:  One collection row (name, uuid, metadata, dimension) → a summary; the
//        count comes from `/count` and is passed in because it costs a call.
fn collection_summary(c: &Json, count: Option<f64>) -> Option<ObjectSummary> {
    let name = c.get("name").and_then(Json::as_str)?;
    let mut parts = Vec::new();
    if let Some(n) = count {
        parts.push(format!("{} records", crate::model::objects::format_number(n)));
    }
    if let Some(d) = c.get("dimension").and_then(Json::as_f64) {
        parts.push(format!("{d}d"));
    }
    let keys: Vec<String> = c.get("metadata").and_then(Json::as_object).map(|m| m.keys().cloned().collect()).unwrap_or_default();
    if !keys.is_empty() {
        parts.push(keys.join(", "));
    }
    let badge = c
        .get("metadata")
        .and_then(|m| m.get("hnsw:space"))
        .map(text_of)
        .filter(|s| !s.is_empty())
        .or_else(|| c.get("configuration_json").and_then(|cfg| cfg.pointer("/hnsw/space")).map(text_of).filter(|s| !s.is_empty()));
    Some(summary(ObjectKind::Collection, name, None, parts.join(" · "), badge))
}

fn database_summaries(names: &[String], current: &str, tenant: &str) -> Vec<ObjectSummary> {
    let list = names
        .iter()
        .map(|name| {
            let is_current = name == current;
            summary(
                ObjectKind::Database,
                name,
                None,
                format!("tenant {tenant}"),
                is_current.then(|| "current".to_string()),
            )
        })
        .collect();
    finish(list)
}

// ---- vector search ----------------------------------------------------------

// WHAT:  Playground request → `/collections/{id}/query`. The filter is Chroma's
//        own `where` document; `include` always carries metadata + documents +
//        distances, and embeddings when the caller asked for vectors.
fn query_body(req: &VectorSearchRequest) -> Json {
    let mut include = vec!["metadatas", "documents", "distances"];
    if req.include_vectors {
        include.push("embeddings");
    }
    let mut body = json!({
        "query_embeddings": [req.vector],
        "n_results": req.top_k.max(1),
        "include": include,
    });
    if let Some(filter) = req.filter.clone().filter(|f| f.is_object() && f.as_object().map(|o| !o.is_empty()).unwrap_or(false)) {
        body["where"] = filter;
    }
    body
}

// WHAT:  The columnar query response → grid: `id`, `_distance`, `document`,
//        metadata keys, `_embedding` when embeddings were requested.
fn query_hits(resp: &Json, include_vectors: bool) -> ResultSet {
    let objects = query_to_objects(resp);
    let mut names: Vec<String> = vec![ID_COLUMN.to_string(), DISTANCE_COLUMN.to_string(), DOCUMENT_COLUMN.to_string()];
    let mut types: Vec<Option<&'static str>> = vec![Some("string"), Some("number"), Some("string")];
    for obj in objects.iter().filter_map(Json::as_object) {
        for (k, v) in obj {
            if k == "_embedding" {
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
        names.push("_embedding".to_string());
        types.push(Some("json"));
    }
    let rows = objects
        .iter()
        .map(|obj| {
            let map = obj.as_object();
            names.iter().map(|n| map.and_then(|m| m.get(n)).map(json_to_value).unwrap_or(Value::Null)).collect()
        })
        .collect();
    ResultSet {
        columns: names.into_iter().zip(types).map(|(name, ty)| ColumnMeta { name, type_name: ty.unwrap_or("json").to_string() }).collect(),
        rows,
        truncated: false,
    }
}

impl ChromaIntegration {
    async fn database_names(&self) -> Vec<String> {
        if self.api == ApiVersion::V2 {
            let path = format!("/api/v2/tenants/{}/databases?limit=1000", self.tenant);
            if let Ok(resp) = self.http.get_json::<Json>(&path).await {
                let names: Vec<String> = resp
                    .as_array()
                    .map(|a| a.iter().filter_map(|d| d.get("name").and_then(Json::as_str).map(str::to_string)).collect())
                    .unwrap_or_default();
                if !names.is_empty() {
                    return names;
                }
            }
        }
        vec![self.database.clone()]
    }

    async fn list_databases(&self) -> AppResult<Vec<ObjectSummary>> {
        Ok(database_summaries(&self.database_names().await, &self.database, &self.tenant))
    }

    async fn list_collections_summaries(&self) -> AppResult<Vec<ObjectSummary>> {
        let list = self.list_collections().await?;
        let mut out = Vec::new();
        for c in &list {
            let count = match c.get("name").and_then(Json::as_str) {
                Some(name) => self.count_all(name).await.ok().map(|n| n as f64),
                None => None,
            };
            if let Some(s) = collection_summary(c, count) {
                out.push(s);
            }
        }
        Ok(finish(out))
    }

    async fn database_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let mut detail = ObjectDetail::empty(reference)
            .property("Tenant", &self.tenant)
            .property("Current", (name == self.database).to_string())
            .property("API", if self.api == ApiVersion::V2 { "v2" } else { "v1" });
        if self.api == ApiVersion::V2 {
            if let Ok(body) = self.http.get_json::<Json>(&format!("/api/v2/tenants/{}/databases/{name}", self.tenant)).await {
                detail = detail.definition(pretty(&body), CodeLanguage::Json);
            }
        }
        if name == self.database {
            let collections = self.list_collections().await.unwrap_or_default();
            detail = detail.property("Collections", collections.len().to_string());
            detail.rows = Some(rows_table(
                &[("collection", "string"), ("id", "string")],
                collections
                    .iter()
                    .map(|c| vec![Value::Text(text_of(c.get("name").unwrap_or(&Json::Null))), Value::Text(text_of(c.get("id").unwrap_or(&Json::Null)))])
                    .collect(),
            ));
            detail.children = self.list_collections_summaries().await.unwrap_or_default();
        }
        Ok(detail)
    }

    async fn collection_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let id = self.collection_id(name).await?;
        let list = self.list_collections().await.unwrap_or_default();
        let spec = list.iter().find(|c| c.get("name").and_then(Json::as_str) == Some(name)).cloned().unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&spec), CodeLanguage::Json).property("Id", &id).property("Database", &self.database);
        if let Ok(count) = self.count_all(name).await {
            detail = detail.property("Records", crate::model::objects::format_number(count as f64));
        }
        if let Some(d) = spec.get("dimension").filter(|d| !d.is_null()) {
            detail = detail.property("Dimension", text_of(d));
        }
        let sample = self.get_records(name, json!({"limit": 1, "include": ["embeddings"]})).await.unwrap_or_default();
        if let Some(dim) = sample.first().and_then(|r| r.get("_embedding")).and_then(Json::as_array).map(Vec::len) {
            detail = detail.property("Embedding length", dim.to_string());
        }
        let metadata: Vec<Vec<Value>> = spec
            .get("metadata")
            .and_then(Json::as_object)
            .into_iter()
            .flatten()
            .map(|(k, v)| vec![Value::Text(k.clone()), Value::Text(text_of(v))])
            .collect();
        detail.rows = Some(rows_table(&[("metadata", "string"), ("value", "string")], metadata));
        detail.columns = self.columns(&TableRef { schema: Some(self.database.clone()), name: name.to_string() }).await.unwrap_or_default();
        let path = self.collection_path(&id, "");
        Ok(detail.action(ObjectAction::destructive("delete", "Delete collection", json!({"method": "DELETE", "path": path}).to_string())))
    }

    async fn similarity(&self, req: &VectorSearchRequest) -> AppResult<ResultSet> {
        if req.vector.is_empty() {
            return Err(AppError::invalid_input("A query vector is required."));
        }
        let id = self.collection_id(&req.collection).await?;
        let resp: Json = self.http.post_json(&self.collection_path(&id, "/query"), &query_body(req)).await?;
        Ok(query_hits(&resp, req.include_vectors))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities {
            sql: false,
            namespaces: true,
            fixed_columns: false,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        },
        object_kinds: vec![K::Database, K::Collection],
        tools: vec![T::VectorSearch],
    }
}

#[async_trait]
impl Integration for ChromaIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let path = match self.api {
            ApiVersion::V2 => "/api/v2/heartbeat",
            ApiVersion::V1 => "/api/v1/heartbeat",
        };
        let _ = self.http.get_text(path).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let path = match self.api {
            ApiVersion::V2 => "/api/v2/version",
            ApiVersion::V1 => "/api/v1/version",
        };
        let text = self.http.get_text(path).await?;
        let v = serde_json::from_str::<Json>(&text).ok().and_then(|j| j.as_str().map(str::to_string)).unwrap_or(text);
        Ok(Some(format!("Chroma {}", v.trim().trim_matches('"'))))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        if self.api == ApiVersion::V2 {
            let path = format!("/api/v2/tenants/{}/databases?limit=1000", self.tenant);
            if let Ok(resp) = self.http.get_json::<Json>(&path).await {
                let names: Vec<String> = resp.as_array().map(|a| a.iter().filter_map(|d| d.get("name").and_then(Json::as_str).map(str::to_string)).collect()).unwrap_or_default();
                if !names.is_empty() {
                    return Ok(names);
                }
            }
        }
        Ok(vec![self.database.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let list = self.list_collections().await?;
        let mut tables = Vec::new();
        for c in list {
            let Some(name) = c.get("name").and_then(Json::as_str) else {
                continue;
            };
            let row_estimate = self.count_all(name).await.ok();
            tables.push(TableInfo { schema: Some(self.database.clone()), name: name.to_string(), kind: TableKind::Table, row_estimate });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let sample = self.get_records(&table.name, json!({"limit": SAMPLE_SIZE})).await?;
        Ok(columns_from_objects(&sample))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count_all(&table.name).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        if filters.is_empty() {
            return self.count_all(&table.name).await;
        }
        let (where_, where_doc, local) = split_filters(filters);
        let objects = self.window(&table.name, where_, where_doc, WINDOW_CAP).await?;
        let rs = rows_aligned(&objects, &columns_from_objects(&objects));
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        Ok(http::local::apply_filters(&names, rs.rows, &local).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let (where_, where_doc, local) = split_filters(&query.filters);
        let columns = self.columns(table).await?;
        if local.is_empty() && query.sort.is_empty() {
            let mut body = json!({"limit": u64::from(query.limit).max(1), "offset": query.offset});
            if let Some(w) = where_ {
                body["where"] = w;
            }
            if let Some(d) = where_doc {
                body["where_document"] = d;
            }
            let objects = self.get_records(&table.name, body).await?;
            return Ok(rows_aligned(&objects, &columns));
        }
        let window = if local.is_empty() { (query.offset + u64::from(query.limit)).min(WINDOW_CAP) } else { WINDOW_CAP };
        let objects = self.window(&table.name, where_, where_doc, window).await?;
        let rs = rows_aligned(&objects, &columns);
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery { sort: query.sort.clone(), filters: local, offset: query.offset, limit: query.limit };
        let rows = http::local::page(&names, rs.rows, &local_query);
        Ok(ResultSet { columns: rs.columns, rows, truncated: false })
    }

    async fn execute(&self, text: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let cmd = parse_command(text)?;
        Ok(vec![self.run(cmd, max_rows).await?])
    }

    async fn objects(&self, kind: ObjectKind, _parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Database => self.list_databases().await,
            ObjectKind::Collection => self.list_collections_summaries().await,
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Database => self.database_detail(reference).await,
            ObjectKind::Collection => self.collection_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
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
    fn where_translation() {
        let rules = vec![
            FilterRule { column: "genre".into(), op: FilterOp::Eq, value: "scifi".into() },
            FilterRule { column: "year".into(), op: FilterOp::Gt, value: "1970".into() },
            FilterRule { column: "document".into(), op: FilterOp::Contains, value: "sand".into() },
            FilterRule { column: "genre".into(), op: FilterOp::StartsWith, value: "s".into() },
            FilterRule { column: "id".into(), op: FilterOp::Eq, value: "a".into() },
        ];
        let (w, d, local) = split_filters(&rules);
        assert_eq!(w, Some(json!({"$and": [{"genre": {"$eq": "scifi"}}, {"year": {"$gt": 1970}}]})));
        assert_eq!(d, Some(json!({"$contains": "sand"})));
        assert_eq!(local.len(), 2);
        let (single, _, _) = split_filters(&rules[..1]);
        assert_eq!(single, Some(json!({"genre": {"$eq": "scifi"}})));
        let (any, _, _) = split_filters(&[FilterRule { column: "n".into(), op: FilterOp::In, value: "1, b".into() }]);
        assert_eq!(any, Some(json!({"n": {"$in": [1, "b"]}})));
    }

    #[test]
    fn columnar_response_to_rows() {
        let resp = json!({
            "ids": ["a", "b"],
            "documents": ["doc a", null],
            "metadatas": [{"genre": "x", "year": 1}, {"genre": "y"}],
        });
        let objects = records_to_objects(&resp);
        let cols = columns_from_objects(&objects);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "document", "genre", "year"]);
        assert!(cols[0].primary_key);
        assert_eq!(cols[3].data_type, "integer");
        let rs = rows_aligned(&objects, &cols);
        assert_eq!(rs.rows[1][1], Value::Null);
        assert_eq!(rs.rows[1][2], Value::Text("y".into()));
        let q = query_to_objects(&json!({"ids": [["a"]], "documents": [["d"]], "metadatas": [[null]], "distances": [[0.5]]}));
        assert_eq!(q.len(), 1);
        assert_eq!(q[0]["_distance"], json!(0.5));
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse_command("collections").ok(), Some(Command::Collections));
        assert_eq!(parse_command("GET docs 5").ok(), Some(Command::Get { collection: "docs".into(), body: json!({"limit": 5}) }));
        assert_eq!(parse_command("count docs").ok(), Some(Command::Count("docs".into())));
        let q = parse_command(r#"{"collection":"docs","query":{"query_embeddings":[[0.1]],"n_results":2}}"#).ok();
        assert_eq!(q, Some(Command::Query { collection: "docs".into(), body: json!({"query_embeddings": [[0.1]], "n_results": 2}) }));
        let w = parse_command(r#"{"collection":"docs","upsert":{"ids":["a"],"embeddings":[[0.1]]}}"#).ok();
        assert!(matches!(&w, Some(Command::Write { op, .. }) if op == "upsert"));
        assert!(w.map(|c| c.is_mutation()).unwrap_or(false));
        assert_eq!(
            parse_command(r#"{"path":"/api/v2/version"}"#).ok(),
            Some(Command::Raw { method: "GET".into(), path: "/api/v2/version".into(), body: None })
        );
        assert!(parse_command("SELECT 1").is_err());
    }

    #[test]
    fn paths_per_api_version() {
        let mk = |api| ChromaIntegration {
            engine: Engine::Chroma,
            http: HttpClient::new("http://localhost:8000", http::Auth::None, false).unwrap_or_else(|e| panic!("{e}")),
            tenant: "t".into(),
            database: "d".into(),
            api,
            ids: Mutex::new(HashMap::new()),
            read_only: false,
        };
        assert_eq!(mk(ApiVersion::V2).collection_path("x", "/get"), "/api/v2/tenants/t/databases/d/collections/x/get");
        assert_eq!(mk(ApiVersion::V1).collection_path("x", "/get"), "/api/v1/collections/x/get");
        assert_eq!(mk(ApiVersion::V1).collections_path(), "/api/v1/collections?tenant=t&database=d");
    }

    #[test]
    fn explorer_summaries_for_databases_and_collections() {
        let dbs = database_summaries(&["default_database".to_string(), "other".to_string()], "default_database", "default_tenant");
        assert_eq!(dbs[0].reference.name, "default_database");
        assert_eq!(dbs[0].badge.as_deref(), Some("current"));
        assert_eq!(dbs[0].detail.as_deref(), Some("tenant default_tenant"));
        assert!(dbs[1].badge.is_none());

        let c = json!({"name": "docs", "id": "uuid-1", "dimension": 384, "metadata": {"hnsw:space": "cosine", "owner": "team"}});
        let s = collection_summary(&c, Some(1200.0)).unwrap_or_else(|| panic!("summary"));
        assert_eq!(s.reference.name, "docs");
        assert_eq!(s.badge.as_deref(), Some("cosine"));
        assert_eq!(s.detail.as_deref(), Some("1,200 records · 384d · hnsw:space, owner"));
        let bare = collection_summary(&json!({"name": "plain"}), None).unwrap_or_else(|| panic!("summary"));
        assert!(bare.detail.is_none() && bare.badge.is_none());
        let configured = collection_summary(&json!({"name": "x", "configuration_json": {"hnsw": {"space": "l2"}}}), None).unwrap_or_else(|| panic!("summary"));
        assert_eq!(configured.badge.as_deref(), Some("l2"));
        assert!(collection_summary(&json!({"id": "no-name"}), None).is_none());
    }

    #[test]
    fn vector_search_body_and_hits() {
        let req = VectorSearchRequest {
            collection: "docs".into(),
            vector: vec![0.1, 0.9],
            vector_name: None,
            top_k: 3,
            filter: Some(json!({"year": {"$gt": 1970}})),
            include_vectors: false,
        };
        let body = query_body(&req);
        assert_eq!(body["query_embeddings"], json!([[0.1, 0.9]]));
        assert_eq!(body["n_results"], 3);
        assert_eq!(body["include"], json!(["metadatas", "documents", "distances"]));
        assert_eq!(body["where"], json!({"year": {"$gt": 1970}}));
        let with_vec = query_body(&VectorSearchRequest { include_vectors: true, filter: Some(json!({})), top_k: 0, ..req.clone() });
        assert_eq!(with_vec["include"], json!(["metadatas", "documents", "distances", "embeddings"]));
        assert_eq!(with_vec["n_results"], 1);
        assert!(with_vec.get("where").is_none());

        let resp = json!({
            "ids": [["a", "b"]],
            "documents": [["sand worms", null]],
            "metadatas": [[{"year": 1965}, {"year": 1984, "genre": "cyber"}]],
            "distances": [[0.02, 0.5]],
            "embeddings": [[[0.1, 0.9], [0.9, 0.1]]]
        });
        let rs = query_hits(&resp, false);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "_distance", "document", "year", "genre"]);
        assert_eq!(rs.rows[0][0], Value::Text("a".into()));
        assert_eq!(rs.rows[0][1], Value::Float(0.02));
        assert_eq!(rs.rows[0][2], Value::Text("sand worms".into()));
        assert_eq!(rs.rows[0][4], Value::Null);
        assert_eq!(rs.rows[1][4], Value::Text("cyber".into()));
        let with_vec = query_hits(&resp, true);
        assert_eq!(with_vec.columns.last().map(|c| c.name.as_str()), Some("_embedding"));
        assert_eq!(with_vec.rows[0][5], Value::Json(json!([0.1, 0.9])));
        assert!(query_hits(&json!({"ids": []}), false).rows.is_empty());
    }

    #[test]
    fn explorer_actions_parse_as_console_commands() {
        let stmt = json!({"method": "DELETE", "path": "/api/v2/tenants/t/databases/d/collections/uuid-1"}).to_string();
        match parse_command(&stmt) {
            Ok(cmd @ Command::Raw { .. }) => {
                assert!(cmd.is_mutation());
                assert_eq!(cmd, Command::Raw { method: "DELETE".into(), path: "/api/v2/tenants/t/databases/d/collections/uuid-1".into(), body: None });
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_CHROMA_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Chroma,
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
            secret: std::env::var("DBFREE_TEST_CHROMA_KEY").ok(),
        };
        let c = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = c.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("Chroma"), "{version}");
        let base = "/api/v2/tenants/default_tenant/databases/default_database/collections";
        let _ = c.execute(&format!(r#"{{"path":"{base}/dbfree_test","method":"DELETE"}}"#), 10).await;
        c.execute(&format!(r#"{{"path":"{base}","method":"POST","body":{{"name":"dbfree_test"}}}}"#), 10)
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        c.execute(
            r#"{"collection":"dbfree_test","upsert":{"ids":["a","b"],"embeddings":[[0.1,0.9],[0.9,0.1]],"documents":["sand worms","cyber space"],"metadatas":[{"year":1965},{"year":1984}]}}"#,
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("upsert: {e}"));
        let table = TableRef { schema: None, name: "dbfree_test".into() };
        let cols = c.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|col| col.name == "year"));
        assert_eq!(c.count(&table, &[]).await.unwrap_or_default(), 2);
        let filters = vec![FilterRule { column: "year".into(), op: FilterOp::Gt, value: "1970".into() }];
        assert_eq!(c.count(&table, &filters).await.unwrap_or_default(), 1);
        let page = c
            .fetch_page(&table, &PageQuery { sort: vec![], filters, offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1);
        let doc = vec![FilterRule { column: "document".into(), op: FilterOp::Contains, value: "worms".into() }];
        assert_eq!(c.count(&table, &doc).await.unwrap_or_default(), 1);
        let res = c
            .execute(r#"{"collection":"dbfree_test","query":{"query_embeddings":[[0.1,0.9]],"n_results":1}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("query: {e}"));
        assert!(matches!(&res[0], StatementResult::Rows { result } if result.rows.len() == 1));
        let _ = c.execute(&format!(r#"{{"path":"{base}/dbfree_test","method":"DELETE"}}"#), 10).await;
    }
}
