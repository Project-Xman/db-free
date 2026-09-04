// SOT: qdrant-integration, qdrant-rest-api, vector-collections, qdrant-scroll, qdrant-filter-dsl, qdrant-command-console

use crate::error::{AppError, AppResult};
use crate::integrations::http::{self, json_result, json_to_value, json_type_name, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
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

#[async_trait]
impl Integration for QdrantIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: false,
            namespaces: false,
            fixed_columns: false,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        }
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
