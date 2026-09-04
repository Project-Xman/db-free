// SOT: firestore-integration, firestore-rest-api, google-service-account-jwt, firestore-value-decoding, structured-query

use crate::error::{AppError, AppResult};
use crate::integrations::gcp_auth::GcpAuth;
use crate::integrations::http::{json_result, json_to_value, json_type_name, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SortRule, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use std::sync::Arc;

// ============================================================================
// WHAT:  Cloud Firestore adapter over the REST API (v1). `database` = project
//        id (or the service account's), `username` = database id (default
//        `(default)`), `host` = emulator (no auth) when set.
// WHY:   Firestore's gRPC SDK is heavy; the REST surface covers everything the
//        grid needs: listCollectionIds, runQuery, runAggregationQuery, documents.
// HOW:   Schema `collections` lists root collections; a table name with `/`
//        (e.g. `users/u1/orders`) addresses a nested collection. Columns are the
//        union of fields over 50 sampled documents plus `_name` (document id).
//        `execute` takes a structuredQuery JSON, the `{"collection","where",
//        "orderBy","limit"}` sugar, `COLLECTIONS`, `GET <path>` and write ops
//        (`{"set":{path, fields}}` / `{"delete": path}`) unless read-only.
// WHERE: src-tauri/src/integrations/gcp_auth.rs, src-tauri/src/integrations/http.rs
// ============================================================================

const SCOPE: &str = "https://www.googleapis.com/auth/datastore";
const SAMPLE: usize = 50;
const NAME: &str = "_name";
const COLLECTIONS: &str = "collections";

pub struct FirestoreIntegration {
    engine: Engine,
    http: HttpClient,
    auth: Option<GcpAuth>,
    project: String,
    database: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let emulator = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).map(str::to_string);
    let auth = GcpAuth::from_connection(conn, SCOPE)?;
    let project = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .or_else(|| auth.project_hint.clone())
        .ok_or_else(|| AppError::invalid_input("Firestore needs a project id (database field) or a service-account key that names one."))?;
    let database = s.username.as_deref().map(str::trim).filter(|d| !d.is_empty()).unwrap_or("(default)").to_string();
    let (http, auth) = match emulator {
        Some(h) => {
            let base = if h.starts_with("http") { h } else { format!("http://{h}") };
            (HttpClient::new(base, Auth::None, false)?, None)
        }
        None => (HttpClient::new("https://firestore.googleapis.com", Auth::None, false)?, Some(auth)),
    };
    let integration = FirestoreIntegration { engine: s.engine, http, auth, project, database, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Firestore Value ⇄ JSON / model::Value
// ---------------------------------------------------------------------------

pub fn fs_to_json(v: &serde_json::Value) -> serde_json::Value {
    let Some(obj) = v.as_object() else { return v.clone() };
    let Some((kind, inner)) = obj.iter().next() else { return serde_json::Value::Null };
    match kind.as_str() {
        "nullValue" => serde_json::Value::Null,
        "booleanValue" | "doubleValue" | "stringValue" | "timestampValue" | "referenceValue" | "bytesValue" => inner.clone(),
        "integerValue" => inner.as_str().and_then(|s| s.parse::<i64>().ok()).map(|i| serde_json::Value::Number(i.into())).unwrap_or(inner.clone()),
        "mapValue" => serde_json::Value::Object(
            inner.get("fields").and_then(|f| f.as_object()).map(|f| f.iter().map(|(k, v)| (k.clone(), fs_to_json(v))).collect()).unwrap_or_default(),
        ),
        "arrayValue" => serde_json::Value::Array(inner.get("values").and_then(|a| a.as_array()).map(|a| a.iter().map(fs_to_json).collect()).unwrap_or_default()),
        "geoPointValue" => inner.clone(),
        _ => v.clone(),
    }
}

pub fn fs_to_value(v: &serde_json::Value) -> Value {
    let Some(obj) = v.as_object() else { return Value::Null };
    let Some((kind, inner)) = obj.iter().next() else { return Value::Null };
    match kind.as_str() {
        "nullValue" => Value::Null,
        "booleanValue" => Value::Bool(inner.as_bool().unwrap_or(false)),
        "integerValue" => inner.as_str().and_then(|s| s.parse().ok()).or_else(|| inner.as_i64()).map(Value::Int).unwrap_or_else(|| Value::Text(inner.to_string())),
        "doubleValue" => Value::Float(inner.as_f64().unwrap_or(0.0)),
        "stringValue" | "referenceValue" => Value::Text(inner.as_str().unwrap_or_default().to_string()),
        "timestampValue" => Value::DateTime(inner.as_str().unwrap_or_default().to_string()),
        "bytesValue" => Value::Bytes(inner.as_str().unwrap_or_default().to_string()),
        "mapValue" | "arrayValue" | "geoPointValue" => Value::Json(fs_to_json(v)),
        _ => Value::Json(v.clone()),
    }
}

pub fn fs_type_name(v: &serde_json::Value) -> &'static str {
    match v.as_object().and_then(|o| o.keys().next()).map(String::as_str) {
        Some("nullValue") => "null",
        Some("booleanValue") => "boolean",
        Some("integerValue") => "integer",
        Some("doubleValue") => "double",
        Some("stringValue") => "string",
        Some("timestampValue") => "timestamp",
        Some("referenceValue") => "reference",
        Some("bytesValue") => "bytes",
        Some("mapValue") => "map",
        Some("arrayValue") => "array",
        Some("geoPointValue") => "geopoint",
        _ => "json",
    }
}

// WHAT:  Plain JSON → Firestore Value (for write ops and filter values).
pub fn json_to_fs(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Null => serde_json::json!({"nullValue": null}),
        serde_json::Value::Bool(b) => serde_json::json!({"booleanValue": b}),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::json!({"integerValue": i.to_string()})
            } else {
                serde_json::json!({"doubleValue": n.as_f64().unwrap_or(0.0)})
            }
        }
        serde_json::Value::String(s) => serde_json::json!({"stringValue": s}),
        serde_json::Value::Array(a) => serde_json::json!({"arrayValue": {"values": a.iter().map(json_to_fs).collect::<Vec<_>>()}}),
        serde_json::Value::Object(o) => serde_json::json!({"mapValue": {"fields": o.iter().map(|(k, v)| (k.clone(), json_to_fs(v))).collect::<serde_json::Map<_, _>>()}}),
    }
}

fn lenient_fs(raw: &str) -> serde_json::Value {
    let t = raw.trim();
    if t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        return serde_json::json!({"booleanValue": t.eq_ignore_ascii_case("true")});
    }
    if let Ok(i) = t.parse::<i64>() {
        return serde_json::json!({"integerValue": i.to_string()});
    }
    if let Ok(f) = t.parse::<f64>() {
        return serde_json::json!({"doubleValue": f});
    }
    serde_json::json!({"stringValue": t})
}

fn doc_id(name: &str) -> String {
    name.rsplit('/').next().unwrap_or(name).to_string()
}

// WHAT:  Firestore document → flat JSON object with `_name` = document id.
pub fn document_to_object(doc: &serde_json::Value) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    out.insert(NAME.into(), serde_json::Value::String(doc.get("name").and_then(|n| n.as_str()).map(doc_id).unwrap_or_default()));
    for (k, v) in doc.get("fields").and_then(|f| f.as_object()).into_iter().flatten() {
        out.insert(k.clone(), fs_to_json(v));
    }
    serde_json::Value::Object(out)
}

fn field_filter(rule: &FilterRule) -> Option<serde_json::Value> {
    let field = serde_json::json!({"fieldPath": rule.column});
    let v = rule.value.trim();
    let op = match rule.op {
        FilterOp::Eq => "EQUAL",
        FilterOp::Ne => "NOT_EQUAL",
        FilterOp::Gt => "GREATER_THAN",
        FilterOp::Gte => "GREATER_THAN_OR_EQUAL",
        FilterOp::Lt => "LESS_THAN",
        FilterOp::Lte => "LESS_THAN_OR_EQUAL",
        FilterOp::In => {
            let items: Vec<serde_json::Value> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(lenient_fs).collect();
            return Some(serde_json::json!({"fieldFilter": {"field": field, "op": "IN", "value": {"arrayValue": {"values": items}}}}));
        }
        FilterOp::IsNull => return Some(serde_json::json!({"unaryFilter": {"field": field, "op": "IS_NULL"}})),
        FilterOp::IsNotNull => return Some(serde_json::json!({"unaryFilter": {"field": field, "op": "IS_NOT_NULL"}})),
        // Prefix match via range on strings; Contains / EndsWith are applied client-side.
        FilterOp::StartsWith => {
            return Some(serde_json::json!({"compositeFilter": {"op": "AND", "filters": [
                {"fieldFilter": {"field": field, "op": "GREATER_THAN_OR_EQUAL", "value": {"stringValue": v}}},
                {"fieldFilter": {"field": field, "op": "LESS_THAN", "value": {"stringValue": format!("{v}\u{f8ff}")}}}
            ]}}));
        }
        FilterOp::Contains | FilterOp::EndsWith => return None,
    };
    Some(serde_json::json!({"fieldFilter": {"field": field, "op": op, "value": lenient_fs(v)}}))
}

// WHAT:  Grid filters → structuredQuery.where (None if nothing is server-side).
pub fn where_filter(filters: &[FilterRule]) -> Option<serde_json::Value> {
    let parts: Vec<serde_json::Value> = filters.iter().filter_map(field_filter).collect();
    match parts.len() {
        0 => None,
        1 => parts.into_iter().next(),
        _ => Some(serde_json::json!({"compositeFilter": {"op": "AND", "filters": parts}})),
    }
}

fn local_filters(filters: &[FilterRule]) -> Vec<FilterRule> {
    filters.iter().filter(|f| matches!(f.op, FilterOp::Contains | FilterOp::EndsWith)).cloned().collect()
}

pub fn order_by(sort: &[SortRule]) -> serde_json::Value {
    serde_json::Value::Array(
        sort.iter()
            .map(|s| serde_json::json!({"field": {"fieldPath": if s.column == NAME { "__name__".to_string() } else { s.column.clone() }}, "direction": if s.desc { "DESCENDING" } else { "ASCENDING" }}))
            .collect(),
    )
}

// WHAT:  Splits `a/b/c` into (parent document path, collection id).
fn split_collection(table: &str) -> (Option<String>, String) {
    match table.rsplit_once('/') {
        Some((parent, coll)) => (Some(parent.to_string()), coll.to_string()),
        None => (None, table.to_string()),
    }
}

pub fn structured_query(collection: &str, query: &PageQuery) -> serde_json::Value {
    let mut sq = serde_json::json!({"from": [{"collectionId": collection}], "limit": query.limit, "offset": query.offset});
    if let Some(w) = where_filter(&query.filters) {
        sq["where"] = w;
    }
    if !query.sort.is_empty() {
        sq["orderBy"] = order_by(&query.sort);
    }
    sq
}

#[derive(Debug)]
enum Command {
    Collections,
    Get(String),
    Query { parent: Option<String>, query: serde_json::Value },
    Set { path: String, fields: serde_json::Value },
    Delete(String),
}

fn parse_command(raw: &str) -> AppResult<Command> {
    let text = raw.trim();
    if text.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if text.starts_with('{') {
        let v: serde_json::Value = serde_json::from_str(text).map_err(|e| AppError::invalid_input(format!("Command is not valid JSON: {e}")))?;
        if let Some(set) = v.get("set") {
            let path = set.get("path").and_then(|p| p.as_str()).ok_or_else(|| AppError::invalid_input("{\"set\": {\"path\": \"col/id\", \"fields\": {...}}}"))?;
            return Ok(Command::Set { path: path.to_string(), fields: set.get("fields").cloned().unwrap_or(serde_json::json!({})) });
        }
        if let Some(del) = v.get("delete").and_then(|d| d.as_str()) {
            return Ok(Command::Delete(del.to_string()));
        }
        if let Some(sq) = v.get("structuredQuery") {
            return Ok(Command::Query { parent: v.get("parent").and_then(|p| p.as_str()).map(str::to_string), query: sq.clone() });
        }
        if v.get("from").is_some() {
            return Ok(Command::Query { parent: None, query: v });
        }
        if let Some(coll) = v.get("collection").and_then(|c| c.as_str()) {
            let (parent, coll) = split_collection(coll);
            let mut sq = serde_json::json!({"from": [{"collectionId": coll}]});
            let rules: Vec<FilterRule> = v
                .get("where")
                .and_then(|w| w.as_array())
                .into_iter()
                .flatten()
                .filter_map(|t| {
                    let a = t.as_array()?;
                    let op = match a.get(1)?.as_str()? {
                        "==" | "=" => FilterOp::Eq,
                        "!=" => FilterOp::Ne,
                        ">" => FilterOp::Gt,
                        ">=" => FilterOp::Gte,
                        "<" => FilterOp::Lt,
                        "<=" => FilterOp::Lte,
                        "in" => FilterOp::In,
                        _ => return None,
                    };
                    let val = a.get(2)?;
                    let value = match val {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Array(items) => items.iter().map(|i| i.as_str().map(str::to_string).unwrap_or_else(|| i.to_string())).collect::<Vec<_>>().join(","),
                        other => other.to_string(),
                    };
                    Some(FilterRule { column: a.first()?.as_str()?.to_string(), op, value })
                })
                .collect();
            if let Some(w) = where_filter(&rules) {
                sq["where"] = w;
            }
            if let Some(ob) = v.get("orderBy") {
                let sort: Vec<SortRule> = match ob {
                    serde_json::Value::String(s) => vec![SortRule { column: s.trim_start_matches('-').to_string(), desc: s.starts_with('-') }],
                    serde_json::Value::Array(a) => a.iter().filter_map(|s| s.as_str()).map(|s| SortRule { column: s.trim_start_matches('-').to_string(), desc: s.starts_with('-') }).collect(),
                    _ => vec![],
                };
                sq["orderBy"] = order_by(&sort);
            }
            if let Some(l) = v.get("limit") {
                sq["limit"] = l.clone();
            }
            return Ok(Command::Query { parent, query: sq });
        }
        return Err(AppError::invalid_input("Expected {\"structuredQuery\": …}, {\"collection\": …, \"where\": [[field, op, value]]}, {\"set\": …} or {\"delete\": …}."));
    }
    let mut words = text.split_whitespace();
    let head = words.next().unwrap_or_default().to_uppercase();
    match head.as_str() {
        "COLLECTIONS" => Ok(Command::Collections),
        "GET" => Ok(Command::Get(words.next().ok_or_else(|| AppError::invalid_input("GET needs a document path (collection/id)."))?.to_string())),
        _ => Err(AppError::invalid_input("Enter a structuredQuery JSON, {\"collection\": …} sugar, `COLLECTIONS` or `GET <collection>/<id>`.")),
    }
}

impl FirestoreIntegration {
    fn docs_path(&self) -> String {
        format!("/v1/projects/{}/databases/{}/documents", self.project, self.database)
    }

    async fn req(&self, method: Method, path: &str, body: Option<serde_json::Value>) -> AppResult<serde_json::Value> {
        let mut r = self.http.request(method, path);
        if let Some(a) = &self.auth {
            if let Auth::Bearer(t) = a.bearer().await? {
                r = r.bearer_auth(t);
            }
        }
        if let Some(b) = body {
            r = r.json(&b);
        }
        let resp = self.http.send(r).await?;
        let text = resp.text().await.map_err(|e| AppError::driver(e.to_string()))?;
        if text.trim().is_empty() {
            return Ok(serde_json::json!({}));
        }
        serde_json::from_str(&text).map_err(|e| AppError::driver(format!("Malformed Firestore response: {e}")))
    }

    async fn list_collection_ids(&self, parent: Option<&str>) -> AppResult<Vec<String>> {
        let path = match parent {
            Some(p) => format!("{}/{p}:listCollectionIds", self.docs_path()),
            None => format!("{}:listCollectionIds", self.docs_path()),
        };
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut body = serde_json::json!({"pageSize": 300});
            if let Some(t) = &token {
                body["pageToken"] = serde_json::Value::String(t.clone());
            }
            let resp = self.req(Method::POST, &path, Some(body)).await?;
            for id in resp.get("collectionIds").and_then(|c| c.as_array()).into_iter().flatten() {
                if let Some(s) = id.as_str() {
                    out.push(s.to_string());
                }
            }
            match resp.get("nextPageToken").and_then(|t| t.as_str()).filter(|t| !t.is_empty()) {
                Some(t) if out.len() < 5_000 => token = Some(t.to_string()),
                _ => break,
            }
        }
        Ok(out)
    }

    async fn run_query(&self, parent: Option<&str>, sq: &serde_json::Value) -> AppResult<Vec<serde_json::Value>> {
        let path = match parent {
            Some(p) => format!("{}/{p}:runQuery", self.docs_path()),
            None => format!("{}:runQuery", self.docs_path()),
        };
        let resp = self.req(Method::POST, &path, Some(serde_json::json!({"structuredQuery": sq}))).await?;
        Ok(resp.as_array().into_iter().flatten().filter_map(|r| r.get("document").cloned()).map(|d| document_to_object(&d)).collect())
    }

    async fn sample(&self, table: &str) -> AppResult<Vec<serde_json::Value>> {
        let (parent, coll) = split_collection(table);
        self.run_query(parent.as_deref(), &serde_json::json!({"from": [{"collectionId": coll}], "limit": SAMPLE})).await
    }

    fn write_check(&self) -> AppResult<()> {
        if self.read_only {
            return Err(AppError::read_only("This connection is read-only; set/delete are blocked."));
        }
        Ok(())
    }
}

fn union_columns(docs: &[serde_json::Value]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = vec![NAME.into()];
    let mut types: Vec<String> = vec!["string".into()];
    for d in docs {
        for (k, v) in d.as_object().into_iter().flatten() {
            if !names.contains(k) {
                names.push(k.clone());
                types.push(json_type_name(v).to_string());
            }
        }
    }
    names
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, data_type))| ColumnInfo { primary_key: name == NAME, nullable: name != NAME, name, data_type, ordinal: i as u32 + 1 })
        .collect()
}

#[async_trait]
impl Integration for FirestoreIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::DOCUMENT
    }

    async fn ping(&self) -> AppResult<()> {
        self.list_collection_ids(None).await.map(|_| ())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some(format!("Firestore ({})", self.database)))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.project.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.project.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let ids = self.list_collection_ids(None).await?;
        let tables = ids.into_iter().map(|name| TableInfo { schema: Some(COLLECTIONS.into()), name, kind: TableKind::Table, row_estimate: None }).collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: COLLECTIONS.into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(union_columns(&self.sample(&table.name).await?))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (parent, coll) = split_collection(&table.name);
        let locals = local_filters(filters);
        if !locals.is_empty() {
            let q = PageQuery { sort: vec![], filters: filters.to_vec(), offset: 0, limit: 5_000 };
            return Ok(self.fetch_page(table, &q).await?.rows.len() as i64);
        }
        let mut sq = serde_json::json!({"from": [{"collectionId": coll}]});
        if let Some(w) = where_filter(filters) {
            sq["where"] = w;
        }
        let path = match parent {
            Some(p) => format!("{}/{p}:runAggregationQuery", self.docs_path()),
            None => format!("{}:runAggregationQuery", self.docs_path()),
        };
        let body = serde_json::json!({"structuredAggregationQuery": {"aggregations": [{"alias": "n", "count": {}}], "structuredQuery": sq}});
        let resp = self.req(Method::POST, &path, Some(body)).await?;
        let n = resp
            .as_array()
            .and_then(|a| a.first())
            .and_then(|r| r.pointer("/result/aggregateFields/n"))
            .map(fs_to_value)
            .unwrap_or(Value::Null);
        Ok(match n {
            Value::Int(i) => i,
            Value::Text(t) => t.parse().unwrap_or(0),
            _ => 0,
        })
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let (parent, coll) = split_collection(&table.name);
        let locals = local_filters(&query.filters);
        let server_query = if locals.is_empty() { query.clone() } else { PageQuery { sort: query.sort.clone(), filters: query.filters.clone(), offset: 0, limit: (query.offset as u32 + query.limit).min(5_000) } };
        let docs = self.run_query(parent.as_deref(), &structured_query(&coll, &server_query)).await?;
        let columns = union_columns(&docs);
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let mut rows: Vec<Vec<Value>> = docs.iter().map(|d| names.iter().map(|n| d.get(n).map(json_to_value).unwrap_or(Value::Null)).collect()).collect();
        if !locals.is_empty() {
            rows = crate::integrations::http::local::page(&names, rows, &PageQuery { sort: vec![], filters: locals, offset: query.offset, limit: query.limit });
        }
        Ok(ResultSet { columns: columns.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect(), rows, truncated: false })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut out = Vec::new();
        for stmt in split_blank_lines(sql) {
            let result = match parse_command(&stmt)? {
                Command::Collections => {
                    let ids = self.list_collection_ids(None).await?;
                    StatementResult::Rows { result: json_result(serde_json::Value::Array(ids.into_iter().map(|c| serde_json::json!({"collection": c})).collect())) }
                }
                Command::Get(path) => {
                    let doc = self.req(Method::GET, &format!("{}/{}", self.docs_path(), path.trim_matches('/')), None).await?;
                    StatementResult::Rows { result: json_result(document_to_object(&doc)) }
                }
                Command::Query { parent, mut query } => {
                    if query.get("limit").is_none() {
                        query["limit"] = serde_json::json!(max_rows);
                    }
                    let docs = self.run_query(parent.as_deref(), &query).await?;
                    let mut rs = crate::integrations::http::objects_to_result_set(&docs, Some(NAME), max_rows);
                    rs.truncated = docs.len() > max_rows;
                    StatementResult::Rows { result: rs }
                }
                Command::Set { path, fields } => {
                    self.write_check()?;
                    let body = serde_json::json!({"fields": fields.as_object().map(|o| o.iter().map(|(k, v)| (k.clone(), json_to_fs(v))).collect::<serde_json::Map<_, _>>()).unwrap_or_default()});
                    self.req(Method::PATCH, &format!("{}/{}", self.docs_path(), path.trim_matches('/')), Some(body)).await?;
                    StatementResult::Affected { rows_affected: 1 }
                }
                Command::Delete(path) => {
                    self.write_check()?;
                    self.req(Method::DELETE, &format!("{}/{}", self.docs_path(), path.trim_matches('/')), None).await?;
                    StatementResult::Affected { rows_affected: 1 }
                }
            };
            out.push(result);
        }
        Ok(out)
    }

    async fn close(&self) {}
}

fn split_blank_lines(text: &str) -> Vec<String> {
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

    #[test]
    fn decodes_firestore_values() {
        assert_eq!(fs_to_value(&serde_json::json!({"stringValue": "a"})), Value::Text("a".into()));
        assert_eq!(fs_to_value(&serde_json::json!({"integerValue": "12"})), Value::Int(12));
        assert_eq!(fs_to_value(&serde_json::json!({"doubleValue": 1.5})), Value::Float(1.5));
        assert_eq!(fs_to_value(&serde_json::json!({"booleanValue": true})), Value::Bool(true));
        assert_eq!(fs_to_value(&serde_json::json!({"nullValue": null})), Value::Null);
        assert_eq!(fs_to_value(&serde_json::json!({"timestampValue": "2024-01-01T00:00:00Z"})), Value::DateTime("2024-01-01T00:00:00Z".into()));
        assert_eq!(fs_to_value(&serde_json::json!({"referenceValue": "projects/p/databases/(default)/documents/a/b"})), Value::Text("projects/p/databases/(default)/documents/a/b".into()));
        assert_eq!(fs_to_value(&serde_json::json!({"bytesValue": "AQ=="})), Value::Bytes("AQ==".into()));
        assert_eq!(fs_to_value(&serde_json::json!({"geoPointValue": {"latitude": 1.0, "longitude": 2.0}})), Value::Json(serde_json::json!({"latitude": 1.0, "longitude": 2.0})));
        assert_eq!(
            fs_to_value(&serde_json::json!({"mapValue": {"fields": {"n": {"integerValue": "1"}, "l": {"arrayValue": {"values": [{"stringValue": "x"}]}}}}})),
            Value::Json(serde_json::json!({"n": 1, "l": ["x"]}))
        );
        assert_eq!(fs_type_name(&serde_json::json!({"arrayValue": {}})), "array");
    }

    #[test]
    fn document_flattens_with_name() {
        let doc = serde_json::json!({"name": "projects/p/databases/(default)/documents/users/u1", "fields": {"age": {"integerValue": "3"}}});
        let o = document_to_object(&doc);
        assert_eq!(o["_name"], "u1");
        assert_eq!(o["age"], 3);
        let cols = union_columns(&[o]);
        assert_eq!(cols[0].name, "_name");
        assert!(cols[0].primary_key);
        assert_eq!(cols[1].data_type, "integer");
    }

    #[test]
    fn json_round_trips_to_firestore_value() {
        let v = serde_json::json!({"a": 1, "b": [true, "x"], "c": null, "d": 1.5});
        assert_eq!(fs_to_json(&json_to_fs(&v)), v);
    }

    #[test]
    fn builds_structured_query() {
        let q = PageQuery {
            sort: vec![SortRule { column: "age".into(), desc: true }],
            filters: vec![
                FilterRule { column: "age".into(), op: FilterOp::Gte, value: "18".into() },
                FilterRule { column: "tag".into(), op: FilterOp::In, value: "a,b".into() },
                FilterRule { column: "x".into(), op: FilterOp::IsNull, value: String::new() },
                FilterRule { column: "name".into(), op: FilterOp::Contains, value: "z".into() },
            ],
            offset: 5,
            limit: 10,
        };
        let sq = structured_query("users", &q);
        assert_eq!(sq["from"][0]["collectionId"], "users");
        assert_eq!(sq["limit"], 10);
        assert_eq!(sq["offset"], 5);
        let f = &sq["where"]["compositeFilter"]["filters"];
        assert_eq!(f.as_array().map(|a| a.len()), Some(3));
        assert_eq!(f[0]["fieldFilter"]["op"], "GREATER_THAN_OR_EQUAL");
        assert_eq!(f[0]["fieldFilter"]["value"]["integerValue"], "18");
        assert_eq!(f[1]["fieldFilter"]["op"], "IN");
        assert_eq!(f[2]["unaryFilter"]["op"], "IS_NULL");
        assert_eq!(sq["orderBy"][0]["direction"], "DESCENDING");
        assert_eq!(local_filters(&q.filters).len(), 1);
    }

    #[test]
    fn parses_commands() {
        assert!(matches!(parse_command("COLLECTIONS"), Ok(Command::Collections)));
        assert!(matches!(parse_command("GET users/u1"), Ok(Command::Get(p)) if p == "users/u1"));
        match parse_command(r#"{"collection": "users", "where": [["age", ">=", 18]], "orderBy": "-age", "limit": 3}"#) {
            Ok(Command::Query { parent, query }) => {
                assert!(parent.is_none());
                assert_eq!(query["from"][0]["collectionId"], "users");
                assert_eq!(query["where"]["fieldFilter"]["op"], "GREATER_THAN_OR_EQUAL");
                assert_eq!(query["orderBy"][0]["direction"], "DESCENDING");
                assert_eq!(query["limit"], 3);
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(parse_command(r#"{"collection": "users/u1/orders"}"#), Ok(Command::Query { parent: Some(p), .. }) if p == "users/u1"));
        assert!(matches!(parse_command(r#"{"set": {"path": "users/u1", "fields": {"a": 1}}}"#), Ok(Command::Set { .. })));
        assert!(matches!(parse_command(r#"{"delete": "users/u1"}"#), Ok(Command::Delete(_))));
        assert!(parse_command("SELECT 1").is_err());
    }
}
