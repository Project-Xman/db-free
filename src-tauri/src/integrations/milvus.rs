// SOT: milvus-integration, milvus-rest-api-v2, vector-collections, milvus-filter-expr, milvus-command-console, zilliz-cloud, object-explorer, server-stats, vector-search-playground, milvus-admin-actions

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
// WHAT:  Milvus adapter over the RESTful API v2 (`/v2/vectordb/...`). Works
//        unchanged against Zilliz Cloud (host is the cluster URL, secret is
//        the API key). A "table" is a collection; columns come from the
//        collection schema (`collections/describe`).
// WHY:   Milvus has a typed schema and a boolean filter expression language,
//        so filters are translated to `field == "x" && n >= 3` expressions and
//        pushed down; `count(*)` is exact. Sort has no server-side equivalent
//        for scalar queries, so it is applied client-side over the page.
// HOW:   Auth is `Bearer user:password` (or `Bearer <token>` when only a
//        secret is set). The `database` field selects the Milvus database
//        (`dbName`), default `default`. `execute` takes JSON envelopes
//        `{"collection": …, "search"|"query"|"insert"|"upsert"|"delete": {…}}`,
//        a raw `{"path": "/v2/vectordb/…", "body": {…}}` passthrough, plus
//        `COLLECTIONS`, `DESCRIBE <c>` and `QUERY <c> [filter]` shorthands.
// WHERE: src-tauri/src/integrations/http.rs, integrations/mod.rs
// ============================================================================

const DEFAULT_PORT: u16 = 19530;
const DEFAULT_DATABASE: &str = "default";
const PAGE_CAP: u64 = 16_384;
const API: &str = "/v2/vectordb";

pub struct MilvusIntegration {
    engine: Engine,
    http: HttpClient,
    database: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    let secret = conn.secret.as_deref().map(str::trim).filter(|p| !p.is_empty());
    let auth = match (user, secret) {
        (Some(u), Some(p)) => Auth::Bearer(format!("{u}:{p}")),
        (None, Some(p)) => Auth::Bearer(p.to_string()),
        (Some(u), None) => Auth::Bearer(format!("{u}:")),
        (None, None) => Auth::None,
    };
    let is_url = s.host.as_deref().map(|h| h.starts_with("https://")).unwrap_or(false);
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), is_url, auth)?;
    let database = s.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).unwrap_or(DEFAULT_DATABASE).to_string();
    let integration = MilvusIntegration { engine: s.engine, http, database, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

fn quote_string(s: &str) -> String {
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

fn literal(text: &str) -> String {
    let t = text.trim();
    if t.parse::<i64>().is_ok() || t.parse::<f64>().is_ok() || t == "true" || t == "false" {
        t.to_string()
    } else {
        quote_string(t)
    }
}

fn valid_ident(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        && !name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true)
}

// WHAT:  Filter rules → Milvus boolean expression. Text ops use `like`;
//        `In` becomes `field in [...]`; null checks map to `is null`.
fn filter_expr(filters: &[FilterRule]) -> AppResult<String> {
    let mut parts = Vec::with_capacity(filters.len());
    for f in filters {
        if !valid_ident(&f.column) {
            return Err(AppError::invalid_input(format!("Unsupported field name for filtering: {}", f.column)));
        }
        let col = &f.column;
        let expr = match f.op {
            FilterOp::Eq => format!("{col} == {}", literal(&f.value)),
            FilterOp::Ne => format!("{col} != {}", literal(&f.value)),
            FilterOp::Gt => format!("{col} > {}", literal(&f.value)),
            FilterOp::Gte => format!("{col} >= {}", literal(&f.value)),
            FilterOp::Lt => format!("{col} < {}", literal(&f.value)),
            FilterOp::Lte => format!("{col} <= {}", literal(&f.value)),
            FilterOp::Contains => format!("{col} like {}", quote_string(&format!("%{}%", f.value.trim()))),
            FilterOp::StartsWith => format!("{col} like {}", quote_string(&format!("{}%", f.value.trim()))),
            FilterOp::EndsWith => format!("{col} like {}", quote_string(&format!("%{}", f.value.trim()))),
            FilterOp::In => {
                let items: Vec<String> = f.value.split(',').map(literal).collect();
                format!("{col} in [{}]", items.join(", "))
            }
            FilterOp::IsNull => format!("{col} is null"),
            FilterOp::IsNotNull => format!("{col} is not null"),
        };
        parts.push(format!("({expr})"));
    }
    Ok(parts.join(" && "))
}

fn columns_from_describe(desc: &Json) -> Vec<ColumnInfo> {
    let fields = desc.get("fields").and_then(Json::as_array).cloned().unwrap_or_default();
    let mut cols: Vec<ColumnInfo> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let name = f.get("name").and_then(Json::as_str).unwrap_or("field").to_string();
            let mut data_type = f.get("type").and_then(Json::as_str).unwrap_or("unknown").to_string();
            if let Some(dim) = f.get("params").and_then(Json::as_array).and_then(|ps| {
                ps.iter().find(|p| p.get("key").and_then(Json::as_str) == Some("dim")).and_then(|p| p.get("value"))
            }) {
                let dim = dim.as_str().map(str::to_string).unwrap_or_else(|| dim.to_string());
                data_type = format!("{data_type}({dim})");
            }
            let primary_key = f.get("primaryKey").and_then(Json::as_bool).unwrap_or(false);
            let nullable = f.get("nullable").and_then(Json::as_bool).unwrap_or(!primary_key);
            ColumnInfo { name, data_type, nullable, primary_key, ordinal: i as u32 }
        })
        .collect();
    if desc.get("enableDynamicField").and_then(Json::as_bool).unwrap_or(false) {
        cols.push(ColumnInfo {
            name: "$meta".into(),
            data_type: "JSON".into(),
            nullable: true,
            primary_key: false,
            ordinal: cols.len() as u32,
        });
    }
    cols
}

fn unwrap_data(resp: Json) -> AppResult<Json> {
    let code = resp.get("code").and_then(Json::as_i64).unwrap_or(0);
    if code != 0 {
        let msg = resp.get("message").and_then(Json::as_str).unwrap_or("Milvus error").to_string();
        return Err(if code == 1800 || msg.to_lowercase().contains("auth") {
            AppError::not_connected(format!("Milvus ({code}): {msg}"))
        } else {
            AppError::driver(format!("Milvus ({code}): {msg}"))
        });
    }
    Ok(resp.get("data").cloned().unwrap_or(Json::Null))
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
    Collections,
    Describe(String),
    Query { collection: String, body: Json },
    Search { collection: String, body: Json },
    Insert { collection: String, body: Json },
    Upsert { collection: String, body: Json },
    Delete { collection: String, body: Json },
    Raw { path: String, body: Json },
}

impl Command {
    fn is_mutation(&self) -> bool {
        match self {
            Command::Insert { .. } | Command::Upsert { .. } | Command::Delete { .. } => true,
            Command::Raw { path, .. } => {
                let p = path.to_ascii_lowercase();
                ["/insert", "/upsert", "/delete", "/create", "/drop", "/alter", "/load", "/release", "/rename", "/flush", "/compact", "/grant", "/revoke"]
                    .iter()
                    .any(|m| p.ends_with(m))
            }
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
            return Ok(Command::Raw { path: path.to_string(), body: obj.get("body").cloned().unwrap_or_else(|| json!({})) });
        }
        let collection = obj
            .get("collection")
            .and_then(Json::as_str)
            .ok_or_else(|| AppError::invalid_input("Missing \"collection\" (or \"path\" for a raw request)."))?
            .to_string();
        let body = |k: &str| obj.get(k).cloned();
        if let Some(body) = body("search") {
            return Ok(Command::Search { collection, body });
        }
        if let Some(body) = body("query") {
            return Ok(Command::Query { collection, body });
        }
        if let Some(body) = body("insert") {
            return Ok(Command::Insert { collection, body });
        }
        if let Some(body) = body("upsert") {
            return Ok(Command::Upsert { collection, body });
        }
        if let Some(body) = body("delete") {
            return Ok(Command::Delete { collection, body });
        }
        return Ok(Command::Describe(collection));
    }
    let mut words = text.splitn(3, char::is_whitespace).map(str::trim);
    let verb = words.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "COLLECTIONS" => Ok(Command::Collections),
        "DESCRIBE" | "INFO" => {
            let c = words.next().filter(|c| !c.is_empty()).ok_or_else(|| AppError::invalid_input("Usage: DESCRIBE <collection>"))?;
            Ok(Command::Describe(c.to_string()))
        }
        "QUERY" => {
            let c = words.next().filter(|c| !c.is_empty()).ok_or_else(|| AppError::invalid_input("Usage: QUERY <collection> [filter]"))?;
            let filter = words.next().unwrap_or("").to_string();
            Ok(Command::Query { collection: c.to_string(), body: json!({"filter": filter, "outputFields": ["*"]}) })
        }
        _ => Err(AppError::invalid_input(
            "Unknown command. Use COLLECTIONS, DESCRIBE <c>, QUERY <c> [filter], or JSON like {\"collection\": \"c\", \"search\": {...}}.",
        )),
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

impl MilvusIntegration {
    async fn call(&self, path: &str, mut body: Json) -> AppResult<Json> {
        if let Some(obj) = body.as_object_mut() {
            obj.entry("dbName").or_insert_with(|| Json::String(self.database.clone()));
        }
        let full = if path.starts_with("/v2/") { path.to_string() } else { format!("{API}/{}", path.trim_start_matches('/')) };
        let resp: Json = self.http.post_json(&full, &body).await?;
        unwrap_data(resp)
    }

    async fn describe(&self, collection: &str) -> AppResult<Json> {
        self.call("collections/describe", json!({"collectionName": collection})).await
    }

    async fn query(&self, collection: &str, filter: &str, limit: u64, offset: u64, output: Json) -> AppResult<Vec<Json>> {
        let body = json!({
            "collectionName": collection,
            "filter": filter,
            "limit": limit,
            "offset": offset,
            "outputFields": output,
        });
        let data = self.call("entities/query", body).await?;
        Ok(data.as_array().cloned().unwrap_or_default())
    }

    async fn run(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        if self.read_only && cmd.is_mutation() {
            return Err(AppError::invalid_input("This connection is read-only; write operations are refused."));
        }
        let cap = |body: &mut Json| {
            if body.get("limit").is_none() {
                body["limit"] = json!(max_rows.min(PAGE_CAP as usize));
            }
        };
        match cmd {
            Command::Collections => {
                let data = self.call("collections/list", json!({})).await?;
                let docs: Vec<Json> = data.as_array().cloned().unwrap_or_default().into_iter().map(|n| json!({"name": n})).collect();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, Some("name"), max_rows) })
            }
            Command::Describe(c) => Ok(StatementResult::Rows { result: json_result(self.describe(&c).await?) }),
            Command::Query { collection, mut body } => {
                cap(&mut body);
                body["collectionName"] = json!(collection);
                if body.get("outputFields").is_none() {
                    body["outputFields"] = json!(["*"]);
                }
                let data = self.call("entities/query", body).await?;
                let docs = data.as_array().cloned().unwrap_or_default();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, None, max_rows) })
            }
            Command::Search { collection, mut body } => {
                cap(&mut body);
                body["collectionName"] = json!(collection);
                if body.get("outputFields").is_none() {
                    body["outputFields"] = json!(["*"]);
                }
                let data = self.call("entities/search", body).await?;
                let docs = data.as_array().cloned().unwrap_or_default();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, None, max_rows) })
            }
            Command::Insert { collection, mut body } => {
                body["collectionName"] = json!(collection);
                let data = self.call("entities/insert", body).await?;
                let n = data.get("insertCount").and_then(Json::as_u64).unwrap_or(0);
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Upsert { collection, mut body } => {
                body["collectionName"] = json!(collection);
                let data = self.call("entities/upsert", body).await?;
                let n = data.get("upsertCount").and_then(Json::as_u64).unwrap_or(0);
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Delete { collection, mut body } => {
                body["collectionName"] = json!(collection);
                let data = self.call("entities/delete", body).await?;
                let n = data.get("deleteCount").and_then(Json::as_u64).unwrap_or(0);
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Raw { path, body } => {
                let data = self.call(&path, body).await?;
                Ok(StatementResult::Rows { result: json_result(data) })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Object explorer / server stats / vector search
//
// WHAT:  `objects()` answers every kind in `profile()` from the v2 management
//        endpoints (`databases/list`, `collections/list` + `describe`,
//        `partitions/list`, `indexes/list`, `aliases/list`, `users/list`,
//        `roles/list`); `object_detail()` adds the JSON description, a property
//        sheet and actions written as this adapter's own `{"path", "body"}`
//        envelopes; `vector_search()` drives `entities/search`.
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const ID_COLUMN: &str = "id";
const DISTANCE_COLUMN: &str = "distance";

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

fn name_list(data: &Json) -> Vec<String> {
    data.as_array()
        .map(|a| a.iter().map(text_of).filter(|s| !s.is_empty()).collect())
        .unwrap_or_default()
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

// WHAT:  `params: [{key: "dim", value: "128"}]` → the dimension of a field.
fn field_dim(field: &Json) -> Option<u64> {
    let raw = field.get("params").and_then(Json::as_array).and_then(|ps| ps.iter().find(|p| p.get("key").and_then(Json::as_str) == Some("dim")).and_then(|p| p.get("value")))?;
    raw.as_u64().or_else(|| raw.as_str().and_then(|s| s.parse::<u64>().ok()))
}

fn is_vector_field(field: &Json) -> bool {
    str_at(field, "type").to_ascii_lowercase().contains("vector")
}

// WHAT:  (field name, dimension) for every vector field of a collection.
fn vector_fields(desc: &Json) -> Vec<(String, Option<u64>)> {
    desc.get("fields")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter(|f| is_vector_field(f))
        .map(|f| (str_at(f, "name").to_string(), field_dim(f)))
        .collect()
}

fn primary_field(desc: &Json) -> Option<String> {
    desc.get("fields")
        .and_then(Json::as_array)?
        .iter()
        .find(|f| f.get("primaryKey").and_then(Json::as_bool) == Some(true))
        .map(|f| str_at(f, "name").to_string())
}

fn metric_of(desc: &Json) -> String {
    desc.get("indexes")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .find_map(|i| i.get("metricType").and_then(Json::as_str).map(str::to_string))
        .unwrap_or_default()
}

fn collection_summary(name: &str, desc: &Json, rows: Option<f64>) -> ObjectSummary {
    let mut parts = Vec::new();
    if let Some(n) = rows {
        parts.push(format!("{} rows", crate::model::objects::format_number(n)));
    }
    let dims: Vec<String> = vector_fields(desc)
        .into_iter()
        .map(|(field, dim)| match dim {
            Some(d) => format!("{field} {d}d"),
            None => field,
        })
        .collect();
    if !dims.is_empty() {
        parts.push(dims.join(", "));
    }
    let metric = metric_of(desc);
    if !metric.is_empty() {
        parts.push(metric);
    }
    let load = str_at(desc, "load");
    let badge = match load {
        "" => None,
        other => Some(other.trim_start_matches("LoadState").to_ascii_lowercase()),
    };
    summary(ObjectKind::Collection, name, None, parts.join(" · "), badge)
}

fn index_summary(collection: &str, name: &str, desc: &Json) -> ObjectSummary {
    let mut parts = Vec::new();
    let field = str_at(desc, "fieldName");
    if !field.is_empty() {
        parts.push(format!("on {field}"));
    }
    let metric = str_at(desc, "metricType");
    if !metric.is_empty() {
        parts.push(metric.to_string());
    }
    let state = str_at(desc, "indexState");
    if !state.is_empty() {
        parts.push(state.to_string());
    }
    let badge = desc.get("indexType").map(text_of).filter(|t| !t.is_empty());
    summary(ObjectKind::Index, name, Some(collection), parts.join(" · "), badge)
}

// ---- vector search ----------------------------------------------------------

// WHAT:  Playground request → `entities/search`. `annsField` is the named
//        vector or the collection's first vector field; the filter is Milvus's
//        boolean expression, taken from a JSON string (or a `{"filter": "…"}`
//        envelope) since the language has no JSON form.
fn search_body(req: &VectorSearchRequest, anns_field: &str) -> Json {
    let mut body = json!({
        "collectionName": req.collection,
        "data": [req.vector],
        "limit": req.top_k.max(1),
        "outputFields": ["*"],
    });
    if !anns_field.is_empty() {
        body["annsField"] = Json::String(anns_field.to_string());
    }
    if let Some(expr) = filter_string(req.filter.as_ref()) {
        body["filter"] = Json::String(expr);
    }
    body
}

fn filter_string(filter: Option<&Json>) -> Option<String> {
    match filter? {
        Json::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Json::Object(o) => o.get("filter").and_then(Json::as_str).map(str::to_string).filter(|s| !s.trim().is_empty()),
        _ => None,
    }
}

// WHAT:  Search hits → grid: `id`, `distance`, then the returned fields.
fn search_hits(data: &Json, include_vectors: bool, vector_names: &[String]) -> ResultSet {
    let hits = data.as_array().cloned().unwrap_or_default();
    let mut names: Vec<String> = vec![ID_COLUMN.to_string(), DISTANCE_COLUMN.to_string()];
    let mut types: Vec<Option<&'static str>> = vec![None, Some("number")];
    for hit in &hits {
        for (k, v) in hit.as_object().into_iter().flatten() {
            if k == ID_COLUMN || k == DISTANCE_COLUMN {
                if k == ID_COLUMN && types[0].is_none() && !v.is_null() {
                    types[0] = Some(json_type_name(v));
                }
                continue;
            }
            if !include_vectors && vector_names.iter().any(|n| n == k) {
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
    let rows = hits
        .iter()
        .map(|hit| names.iter().map(|n| hit.get(n).map(json_to_value).unwrap_or(Value::Null)).collect())
        .collect();
    ResultSet {
        columns: names.into_iter().zip(types).map(|(name, ty)| ColumnMeta { name, type_name: ty.unwrap_or("json").to_string() }).collect(),
        rows,
        truncated: false,
    }
}

// ---- server stats -----------------------------------------------------------

fn stats_groups(database: &str, version: &str, databases: &[String], collections: &[(String, Json, Option<f64>)]) -> Vec<StatGroup> {
    let server = vec![Stat::text("Version", version), Stat::text("Database", database), Stat::number("Databases", databases.len() as f64, None)];
    let rows: f64 = collections.iter().filter_map(|(_, _, r)| *r).sum();
    let loaded = collections.iter().filter(|(_, d, _)| str_at(d, "load").contains("Loaded")).count();
    let vectors: usize = collections.iter().map(|(_, d, _)| vector_fields(d).len()).sum();
    let partitions: f64 = collections.iter().filter_map(|(_, d, _)| d.get("partitionsNum").and_then(Json::as_f64)).sum();
    let indexes: usize = collections.iter().map(|(_, d, _)| d.get("indexes").and_then(Json::as_array).map(Vec::len).unwrap_or(0)).sum();
    let mut storage = vec![
        Stat::number("Collections", collections.len() as f64, None),
        Stat::number("Rows", rows, None),
        Stat::number("Loaded collections", loaded as f64, None),
        Stat::number("Vector fields", vectors as f64, None),
        Stat::number("Indexes", indexes as f64, None),
    ];
    if partitions > 0.0 {
        storage.push(Stat::number("Partitions", partitions, None));
    }
    vec![StatGroup { title: "Server".into(), stats: server }, StatGroup { title: "Storage".into(), stats: storage }]
}

impl MilvusIntegration {
    async fn list_names(&self, path: &str, body: Json) -> Vec<String> {
        let mut names = self.call(path, body).await.map(|d| name_list(&d)).unwrap_or_default();
        names.sort();
        names
    }

    async fn row_count(&self, collection: &str) -> Option<f64> {
        self.call("collections/get_stats", json!({"collectionName": collection})).await.ok().and_then(|d| d.get("rowCount").and_then(|r| r.as_f64().or_else(|| r.as_str().and_then(|s| s.parse().ok()))))
    }

    async fn scoped_collections(&self, parent: Option<&str>) -> Vec<String> {
        match parent {
            Some(p) => vec![p.to_string()],
            None => self.list_names("collections/list", json!({})).await,
        }
    }

    async fn list_databases(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut names = self.list_names("databases/list", json!({})).await;
        if names.is_empty() {
            names.push(self.database.clone());
        }
        Ok(finish(
            names
                .into_iter()
                .map(|n| {
                    let current = n == self.database;
                    summary(ObjectKind::Database, &n, None, if current { "current".into() } else { String::new() }, current.then(|| "current".to_string()))
                })
                .collect(),
        ))
    }

    async fn list_collections(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.scoped_collections(None).await {
            let desc = self.describe(&name).await.unwrap_or(Json::Null);
            let rows = self.row_count(&name).await;
            list.push(collection_summary(&name, &desc, rows));
        }
        Ok(finish(list))
    }

    async fn list_partitions(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for collection in self.scoped_collections(parent).await {
            for name in self.list_names("partitions/list", json!({"collectionName": collection})).await {
                let rows = self
                    .call("partitions/get_stats", json!({"collectionName": collection, "partitionName": name}))
                    .await
                    .ok()
                    .and_then(|d| d.get("rowCount").and_then(|r| r.as_f64().or_else(|| r.as_str().and_then(|s| s.parse().ok()))));
                let detail = rows.map(|r| format!("{} rows", crate::model::objects::format_number(r))).unwrap_or_default();
                list.push(summary(ObjectKind::Partition, &name, Some(&collection), detail, None));
            }
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn list_indexes(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for collection in self.scoped_collections(parent).await {
            for name in self.list_names("indexes/list", json!({"collectionName": collection})).await {
                let desc = self.index_describe(&collection, &name).await.unwrap_or(Json::Null);
                list.push(index_summary(&collection, &name, &desc));
            }
            if list.len() >= OBJECT_CAP {
                break;
            }
        }
        Ok(finish(list))
    }

    async fn list_aliases(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.list_names("aliases/list", json!({})).await {
            let desc = self.call("aliases/describe", json!({"aliasName": name})).await.unwrap_or(Json::Null);
            let target = str_at(&desc, "collectionName").to_string();
            let parent = if target.is_empty() { None } else { Some(target.as_str()) };
            let detail = if target.is_empty() { String::new() } else { format!("→ {target}") };
            list.push(summary(ObjectKind::Alias, &name, parent, detail, None));
        }
        Ok(finish(list))
    }

    async fn list_users(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.list_names("users/list", json!({})).await {
            let roles = self.call("users/describe", json!({"userName": name})).await.map(|d| name_list(&d)).unwrap_or_default();
            list.push(summary(ObjectKind::User, &name, None, roles.join(", "), (name == "root").then(|| "built-in".to_string())));
        }
        Ok(finish(list))
    }

    async fn list_roles(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut list = Vec::new();
        for name in self.list_names("roles/list", json!({})).await {
            let grants = self.call("roles/describe", json!({"roleName": name})).await.ok().and_then(|d| d.as_array().map(Vec::len)).unwrap_or(0);
            let badge = matches!(name.as_str(), "admin" | "public").then(|| "built-in".to_string());
            list.push(summary(ObjectKind::Role, &name, None, format!("{grants} privileges"), badge));
        }
        Ok(finish(list))
    }

    async fn index_describe(&self, collection: &str, index: &str) -> AppResult<Json> {
        let data = self.call("indexes/describe", json!({"collectionName": collection, "indexName": index})).await?;
        Ok(match &data {
            Json::Array(items) => items.first().cloned().unwrap_or(Json::Null),
            other => other.clone(),
        })
    }

    fn drop_action(id: &str, label: &str, path: &str, body: Json) -> ObjectAction {
        ObjectAction::destructive(id, label, json!({"path": path, "body": body}).to_string())
    }

    async fn database_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let desc = self.call("databases/describe", json!({"dbName": name})).await.unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&desc), CodeLanguage::Json).property("Current", (name == self.database).to_string());
        if let Some(id) = desc.get("dbID") {
            detail = detail.property("Database id", text_of(id));
        }
        let rows = desc
            .get("properties")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .map(|p| vec![Value::Text(str_at(p, "key").to_string()), Value::Text(text_of(p.get("value").unwrap_or(&Json::Null)))])
            .collect();
        detail.rows = Some(rows_table(&[("property", "string"), ("value", "string")], rows));
        Ok(detail.action(Self::drop_action("drop", "Drop database", "databases/drop", json!({"dbName": name}))))
    }

    async fn collection_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let desc = self.describe(name).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&desc), CodeLanguage::Json);
        if let Some(rows) = self.row_count(name).await {
            detail = detail.property("Rows", crate::model::objects::format_number(rows));
        }
        for (label, key) in [("Description", "description"), ("Load state", "load"), ("Consistency", "consistencyLevel")] {
            let v = desc.get(key).map(text_of).unwrap_or_default();
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        for (label, key) in [("Shards", "shardsNum"), ("Partitions", "partitionsNum"), ("Auto id", "autoID"), ("Dynamic field", "enableDynamicField")] {
            if let Some(v) = desc.get(key) {
                detail = detail.property(label, text_of(v));
            }
        }
        if let Some(pk) = primary_field(&desc) {
            detail = detail.property("Primary key", pk);
        }
        let metric = metric_of(&desc);
        if !metric.is_empty() {
            detail = detail.property("Metric", metric);
        }
        let vectors = vector_fields(&desc);
        if !vectors.is_empty() {
            detail = detail.property("Vector fields", vectors.iter().map(|(f, d)| match d { Some(d) => format!("{f} ({d}d)"), None => f.clone() }).collect::<Vec<_>>().join(", "));
        }
        detail.columns = columns_from_describe(&desc);
        let mut children = self.list_partitions(Some(name)).await.unwrap_or_default();
        children.extend(self.list_indexes(Some(name)).await.unwrap_or_default());
        detail.children = children;
        let body = json!({"collectionName": name});
        Ok(detail
            .action(ObjectAction::new("load", "Load collection", json!({"path": "collections/load", "body": body}).to_string()))
            .action(ObjectAction::destructive("release", "Release collection", json!({"path": "collections/release", "body": body}).to_string()))
            .action(Self::drop_action("drop", "Drop collection", "collections/drop", body)))
    }

    async fn partition_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let collection = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("A partition needs its collection as parent."))?;
        let name = reference.name.as_str();
        let body = json!({"collectionName": collection, "partitionName": name});
        let stats = self.call("partitions/get_stats", body.clone()).await.unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&stats), CodeLanguage::Json).property("Collection", collection);
        if let Some(rows) = stats.get("rowCount") {
            detail = detail.property("Rows", text_of(rows));
        }
        let mut detail = detail
            .action(ObjectAction::new("load", "Load partition", json!({"path": "partitions/load", "body": {"collectionName": collection, "partitionNames": [name]}}).to_string()))
            .action(ObjectAction::destructive("release", "Release partition", json!({"path": "partitions/release", "body": {"collectionName": collection, "partitionNames": [name]}}).to_string()));
        if name != "_default" {
            detail = detail.action(Self::drop_action("drop", "Drop partition", "partitions/drop", body));
        }
        Ok(detail)
    }

    async fn index_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let collection = reference.parent.as_deref().ok_or_else(|| AppError::invalid_input("An index needs its collection as parent."))?;
        let name = reference.name.as_str();
        let desc = self.index_describe(collection, name).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&desc), CodeLanguage::Json).property("Collection", collection);
        for (label, key) in [("Field", "fieldName"), ("Index type", "indexType"), ("Metric", "metricType"), ("State", "indexState"), ("Failed reason", "failReason")] {
            let v = str_at(&desc, key);
            if !v.is_empty() {
                detail = detail.property(label, v);
            }
        }
        if let Some(rows) = desc.get("indexedRows") {
            detail = detail.property("Indexed rows", text_of(rows));
        }
        Ok(detail.action(Self::drop_action("drop", "Drop index", "indexes/drop", json!({"collectionName": collection, "indexName": name}))))
    }

    async fn alias_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let desc = self.call("aliases/describe", json!({"aliasName": name})).await?;
        let detail = ObjectDetail::empty(reference)
            .definition(pretty(&desc), CodeLanguage::Json)
            .property("Collection", str_at(&desc, "collectionName"))
            .property("Database", str_at(&desc, "dbName"));
        Ok(detail.action(Self::drop_action("drop", "Drop alias", "aliases/drop", json!({"aliasName": name}))))
    }

    async fn user_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let data = self.call("users/describe", json!({"userName": name})).await?;
        let roles = name_list(&data);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&data), CodeLanguage::Json).property("Roles", roles.join(", "));
        detail.rows = Some(rows_table(&[("role", "string")], roles.into_iter().map(|r| vec![Value::Text(r)]).collect()));
        Ok(detail.action(Self::drop_action("drop", "Drop user", "users/drop", json!({"userName": name}))))
    }

    async fn role_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let data = self.call("roles/describe", json!({"roleName": name})).await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&data), CodeLanguage::Json);
        let grants = data.as_array().cloned().unwrap_or_default();
        detail = detail.property("Privileges", grants.len().to_string());
        let rows = grants
            .iter()
            .map(|g| {
                vec![
                    Value::Text(str_at(g, "objectType").to_string()),
                    Value::Text(str_at(g, "objectName").to_string()),
                    Value::Text(str_at(g, "privilege").to_string()),
                    Value::Text(str_at(g, "grantor").to_string()),
                ]
            })
            .collect();
        detail.rows = Some(rows_table(&[("object_type", "string"), ("object", "string"), ("privilege", "string"), ("grantor", "string")], rows));
        Ok(detail.action(Self::drop_action("drop", "Drop role", "roles/drop", json!({"roleName": name}))))
    }

    async fn similarity(&self, req: &VectorSearchRequest) -> AppResult<ResultSet> {
        if req.vector.is_empty() {
            return Err(AppError::invalid_input("A query vector is required."));
        }
        let desc = self.describe(&req.collection).await.unwrap_or(Json::Null);
        let vectors = vector_fields(&desc);
        let anns = req
            .vector_name
            .as_deref()
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .map(str::to_string)
            .or_else(|| vectors.first().map(|(n, _)| n.clone()))
            .unwrap_or_default();
        let data = self.call("entities/search", search_body(req, &anns)).await?;
        let names: Vec<String> = vectors.into_iter().map(|(n, _)| n).collect();
        Ok(search_hits(&data, req.include_vectors, &names))
    }

    async fn stats(&self) -> AppResult<ServerStats> {
        let databases = self.list_names("databases/list", json!({})).await;
        let mut collections = Vec::new();
        for name in self.list_names("collections/list", json!({})).await {
            let desc = self.describe(&name).await.unwrap_or(Json::Null);
            let rows = self.row_count(&name).await;
            collections.push((name, desc, rows));
        }
        let version = self.server_version().await.unwrap_or(None).unwrap_or_else(|| "Milvus (REST v2)".to_string());
        Ok(ServerStats::now(stats_groups(&self.database, &version, &databases, &collections)))
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
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        },
        object_kinds: vec![K::Database, K::Collection, K::Partition, K::Index, K::Alias, K::User, K::Role],
        tools: vec![T::Stats, T::VectorSearch],
    }
}

#[async_trait]
impl Integration for MilvusIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.call("collections/list", json!({})).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        // v2 REST has no version endpoint; the legacy v1 `/v1/vector/collections` and
        // the metrics endpoint are not guaranteed either. Report the API level.
        Ok(Some(format!("Milvus (REST v2, db {})", self.database)))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        match self.call("databases/list", json!({})).await {
            Ok(data) => {
                let mut names: Vec<String> = data.as_array().map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();
                if names.is_empty() {
                    names.push(self.database.clone());
                }
                Ok(names)
            }
            Err(_) => Ok(vec![self.database.clone()]),
        }
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let data = self.call("collections/list", json!({})).await?;
        let mut names: Vec<String> = data.as_array().map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();
        names.sort();
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let row_estimate = self.row_estimate(&TableRef { schema: None, name: name.clone() }).await.unwrap_or(None);
            tables.push(TableInfo { schema: Some(self.database.clone()), name, kind: TableKind::Table, row_estimate });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let desc = self.describe(&table.name).await?;
        let cols = columns_from_describe(&desc);
        if cols.is_empty() {
            return Err(AppError::driver(format!("Collection {} has no fields.", table.name)));
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let filter = filter_expr(filters)?;
        let body = json!({"collectionName": table.name, "filter": filter, "outputFields": ["count(*)"]});
        let data = self.call("entities/query", body).await?;
        let n = data
            .as_array()
            .and_then(|a| a.first())
            .and_then(|row| row.get("count(*)"))
            .and_then(Json::as_i64)
            .unwrap_or(0);
        Ok(n)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let filter = filter_expr(&query.filters)?;
        let columns = self.columns(table).await?;
        let limit = u64::from(query.limit).clamp(1, PAGE_CAP);
        let docs = self.query(&table.name, &filter, limit, query.offset, json!(["*"])).await?;
        let rs = rows_aligned(&docs, &columns);
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        let mut rows = rs.rows;
        http::local::apply_sort(&names, &mut rows, &query.sort);
        Ok(ResultSet { columns: rs.columns, rows, truncated: false })
    }

    async fn execute(&self, text: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let cmd = parse_command(text)?;
        Ok(vec![self.run(cmd, max_rows).await?])
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Database => self.list_databases().await,
            ObjectKind::Collection => self.list_collections().await,
            ObjectKind::Partition => self.list_partitions(parent).await,
            ObjectKind::Index => self.list_indexes(parent).await,
            ObjectKind::Alias => self.list_aliases().await,
            ObjectKind::User => self.list_users().await,
            ObjectKind::Role => self.list_roles().await,
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Database => self.database_detail(reference).await,
            ObjectKind::Collection => self.collection_detail(reference).await,
            ObjectKind::Partition => self.partition_detail(reference).await,
            ObjectKind::Index => self.index_detail(reference).await,
            ObjectKind::Alias => self.alias_detail(reference).await,
            ObjectKind::User => self.user_detail(reference).await,
            ObjectKind::Role => self.role_detail(reference).await,
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
    fn builds_filter_expressions() {
        let rules = vec![
            FilterRule { column: "city".into(), op: FilterOp::Eq, value: "Ber\"lin".into() },
            FilterRule { column: "age".into(), op: FilterOp::Gte, value: "18".into() },
            FilterRule { column: "name".into(), op: FilterOp::Contains, value: "an".into() },
            FilterRule { column: "tag".into(), op: FilterOp::In, value: "a, 2".into() },
            FilterRule { column: "x".into(), op: FilterOp::IsNull, value: String::new() },
        ];
        let expr = filter_expr(&rules).unwrap_or_default();
        assert_eq!(expr, r#"(city == "Ber\"lin") && (age >= 18) && (name like "%an%") && (tag in ["a", 2]) && (x is null)"#);
        assert!(filter_expr(&[FilterRule { column: "bad name".into(), op: FilterOp::Eq, value: "1".into() }]).is_err());
        assert!(filter_expr(&[]).unwrap_or_default().is_empty());
    }

    #[test]
    fn describe_to_columns() {
        let desc = json!({
            "fields": [
                {"name": "id", "type": "Int64", "primaryKey": true},
                {"name": "vec", "type": "FloatVector", "params": [{"key": "dim", "value": "128"}]},
                {"name": "title", "type": "VarChar", "nullable": true}
            ],
            "enableDynamicField": true
        });
        let cols = columns_from_describe(&desc);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "vec", "title", "$meta"]);
        assert!(cols[0].primary_key && !cols[0].nullable);
        assert_eq!(cols[1].data_type, "FloatVector(128)");
    }

    #[test]
    fn unwraps_data_and_errors() {
        assert_eq!(unwrap_data(json!({"code": 0, "data": [1]})).ok(), Some(json!([1])));
        assert!(unwrap_data(json!({"code": 1100, "message": "collection not found"})).is_err());
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse_command("collections").ok(), Some(Command::Collections));
        assert_eq!(parse_command("describe books").ok(), Some(Command::Describe("books".into())));
        assert_eq!(
            parse_command("QUERY books id > 3").ok(),
            Some(Command::Query { collection: "books".into(), body: json!({"filter": "id > 3", "outputFields": ["*"]}) })
        );
        let search = parse_command(r#"{"collection":"books","search":{"data":[[0.1]],"annsField":"vec","limit":2}}"#).ok();
        assert!(matches!(search, Some(Command::Search { .. })));
        let raw = parse_command(r#"{"path":"/v2/vectordb/collections/list"}"#).ok();
        assert_eq!(raw, Some(Command::Raw { path: "/v2/vectordb/collections/list".into(), body: json!({}) }));
        assert!(Command::Raw { path: "/v2/vectordb/entities/delete".into(), body: json!({}) }.is_mutation());
        assert!(!Command::Raw { path: "/v2/vectordb/collections/list".into(), body: json!({}) }.is_mutation());
        assert!(parse_command("SELECT 1").is_err());
    }

    #[test]
    fn aligns_rows_to_schema() {
        let cols = columns_from_describe(&json!({"fields": [{"name": "id", "type": "Int64", "primaryKey": true}, {"name": "t", "type": "VarChar"}]}));
        let rs = rows_aligned(&[json!({"id": 1, "t": "a", "extra": true})], &cols);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "t", "extra"]);
        assert_eq!(rs.rows[0][2], Value::Bool(true));
    }

    #[test]
    fn describe_drives_collection_and_index_summaries() {
        let desc = json!({
            "collectionName": "books",
            "load": "LoadStateLoaded",
            "partitionsNum": 2,
            "fields": [
                {"name": "id", "type": "Int64", "primaryKey": true},
                {"name": "vec", "type": "FloatVector", "params": [{"key": "dim", "value": "128"}]},
                {"name": "title", "type": "VarChar"}
            ],
            "indexes": [{"fieldName": "vec", "indexName": "vec_idx", "metricType": "COSINE"}]
        });
        let s = collection_summary("books", &desc, Some(1500.0));
        assert_eq!(s.badge.as_deref(), Some("loaded"));
        assert_eq!(s.detail.as_deref(), Some("1,500 rows · vec 128d · COSINE"));
        assert_eq!(vector_fields(&desc), vec![("vec".to_string(), Some(128))]);
        assert_eq!(primary_field(&desc).as_deref(), Some("id"));
        assert_eq!(metric_of(&desc), "COSINE");
        assert_eq!(field_dim(&json!({"params": [{"key": "dim", "value": 64}]})), Some(64));
        assert!(field_dim(&json!({"params": []})).is_none());
        assert!(is_vector_field(&json!({"type": "BinaryVector"})));
        assert!(!is_vector_field(&json!({"type": "VarChar"})));

        let idx = index_summary("books", "vec_idx", &json!({"fieldName": "vec", "indexType": "HNSW", "metricType": "L2", "indexState": "Finished"}));
        assert_eq!(idx.reference.parent.as_deref(), Some("books"));
        assert_eq!(idx.badge.as_deref(), Some("HNSW"));
        assert_eq!(idx.detail.as_deref(), Some("on vec · L2 · Finished"));
        assert_eq!(name_list(&json!(["a", "b"])), vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn vector_search_body_and_hits() {
        let req = VectorSearchRequest {
            collection: "books".into(),
            vector: vec![0.1, 0.9],
            vector_name: None,
            top_k: 3,
            filter: Some(json!("year > 1990")),
            include_vectors: false,
        };
        let body = search_body(&req, "vec");
        assert_eq!(body["collectionName"], "books");
        assert_eq!(body["data"], json!([[0.1, 0.9]]));
        assert_eq!(body["limit"], 3);
        assert_eq!(body["annsField"], "vec");
        assert_eq!(body["filter"], "year > 1990");
        assert_eq!(body["outputFields"], json!(["*"]));
        let named = search_body(&VectorSearchRequest { vector_name: Some("image".into()), filter: Some(json!({"filter": "n == 1"})), top_k: 0, ..req.clone() }, "image");
        assert_eq!(named["annsField"], "image");
        assert_eq!(named["filter"], "n == 1");
        assert_eq!(named["limit"], 1);
        let none = search_body(&VectorSearchRequest { filter: Some(json!({"must": []})), ..req.clone() }, "");
        assert!(none.get("filter").is_none() && none.get("annsField").is_none());
        assert_eq!(filter_string(Some(&json!("  a == 1 "))).as_deref(), Some("a == 1"));
        assert!(filter_string(None).is_none());

        let data = json!([
            {"id": 1, "distance": 0.02, "title": "Dune", "vec": [0.1, 0.9]},
            {"id": 2, "distance": 0.5, "title": "X", "year": 1984}
        ]);
        let rs = search_hits(&data, false, &["vec".to_string()]);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "distance", "title", "year"]);
        assert_eq!(rs.columns[0].type_name, "integer");
        assert_eq!(rs.rows[0][1], Value::Float(0.02));
        assert_eq!(rs.rows[0][3], Value::Null);
        assert_eq!(rs.rows[1][3], Value::Int(1984));
        let with_vec = search_hits(&data, true, &["vec".to_string()]);
        assert!(with_vec.columns.iter().any(|c| c.name == "vec"));
        assert!(search_hits(&Json::Null, false, &[]).rows.is_empty());
    }

    #[test]
    fn stats_groups_aggregate_collections() {
        let collections = vec![
            ("a".to_string(), json!({"load": "LoadStateLoaded", "partitionsNum": 2, "fields": [{"name": "v", "type": "FloatVector", "params": [{"key": "dim", "value": "8"}]}], "indexes": [{"indexName": "i"}]}), Some(10.0)),
            ("b".to_string(), json!({"load": "LoadStateNotLoad", "partitionsNum": 1, "fields": [], "indexes": []}), Some(5.0)),
        ];
        let groups = stats_groups("default", "Milvus 2.4", &["default".to_string(), "other".to_string()], &collections);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("Milvus 2.4".into()));
        assert_eq!(find("Server", "Databases").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Collections").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Rows").and_then(|s| s.numeric), Some(15.0));
        assert_eq!(find("Storage", "Loaded collections").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Storage", "Vector fields").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Storage", "Indexes").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Storage", "Partitions").and_then(|s| s.numeric), Some(3.0));
    }

    #[test]
    fn explorer_actions_parse_as_console_commands() {
        let drop = MilvusIntegration::drop_action("drop", "Drop collection", "collections/drop", json!({"collectionName": "books"}));
        assert!(drop.destructive);
        match parse_command(&drop.statement) {
            Ok(cmd @ Command::Raw { .. }) => {
                assert!(cmd.is_mutation());
                assert_eq!(cmd, Command::Raw { path: "collections/drop".into(), body: json!({"collectionName": "books"}) });
            }
            other => panic!("unexpected {other:?}"),
        }
        let load = json!({"path": "collections/load", "body": {"collectionName": "books"}}).to_string();
        assert!(parse_command(&load).map(|c| c.is_mutation()).unwrap_or(false));
        for path in ["partitions/drop", "indexes/drop", "aliases/drop", "users/drop", "roles/drop", "databases/drop"] {
            let stmt = MilvusIntegration::drop_action("drop", "Drop", path, json!({})).statement;
            assert!(parse_command(&stmt).map(|c| c.is_mutation()).unwrap_or(false), "{path}");
        }
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_MILVUS_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Milvus,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: None,
                username: std::env::var("DBFREE_TEST_MILVUS_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: false,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_MILVUS_SECRET").ok(),
        };
        let m = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let _ = m.execute(r#"{"path":"collections/drop","body":{"collectionName":"dbfree_test"}}"#, 10).await;
        m.execute(r#"{"path":"collections/create","body":{"collectionName":"dbfree_test","dimension":2}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        m.execute(r#"{"collection":"dbfree_test","upsert":{"data":[{"id":1,"vector":[0.1,0.9],"city":"Berlin"},{"id":2,"vector":[0.9,0.1],"city":"Paris"}]}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("upsert: {e}"));
        let table = TableRef { schema: None, name: "dbfree_test".into() };
        let cols = m.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.primary_key));
        let filters = vec![FilterRule { column: "city".into(), op: FilterOp::Eq, value: "Paris".into() }];
        let page = m
            .fetch_page(&table, &PageQuery { sort: vec![], filters, offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1);
        let _ = m.execute(r#"{"path":"collections/drop","body":{"collectionName":"dbfree_test"}}"#, 10).await;
    }
}
