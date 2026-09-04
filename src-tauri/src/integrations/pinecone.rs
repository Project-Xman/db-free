// SOT: pinecone-integration, pinecone-rest-api, vector-indexes, pinecone-control-plane, pinecone-data-plane, pinecone-command-console

use crate::error::{AppError, AppResult};
use crate::integrations::http::{self, json_result, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SslMode, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

// ============================================================================
// WHAT:  Pinecone adapter. Control plane (`https://api.pinecone.io`) lists
//        indexes; each index has its own data-plane host used for
//        `describe_index_stats`, `vectors/list`, `vectors/fetch`, `query`,
//        `upsert` and `delete`. A "table" is an index with fixed columns
//        `id`, `values`, `metadata`, `sparse_values` (+ `namespace`).
// WHY:   Pinecone has no scan API that returns vectors: browsing is
//        `vectors/list` (ids, paginated by token, serverless only) followed by
//        `vectors/fetch` for the page's ids. Filters on `namespace` select the
//        namespace; everything else is applied client-side on the page.
// HOW:   `host` (connection form) may be a single data-plane host, in which
//        case the index is named by `database` and the control plane is not
//        needed. Data-plane hosts are cached per index. `execute` accepts JSON
//        envelopes `{"index": …, "query"|"upsert"|"delete"|"fetch"|"list"|"stats": …}`
//        and a raw `{"path","method","body","host"?}` passthrough, plus the
//        `INDEXES`, `STATS <index>` and `LIST <index> [n]` shorthands.
// WHERE: src-tauri/src/integrations/http.rs, integrations/mod.rs
// ============================================================================

const CONTROL_PLANE: &str = "https://api.pinecone.io";
const API_VERSION: &str = "2025-01";
const ID_COLUMN: &str = "id";
const NAMESPACE_COLUMN: &str = "namespace";
const LIST_CAP: u64 = 100;
const WINDOW_CAP: u64 = 1_000;

pub struct PineconeIntegration {
    engine: Engine,
    control: HttpClient,
    /// Data-plane client when the connection points straight at an index host.
    fixed: Option<(String, HttpClient)>,
    api_key: String,
    insecure: bool,
    hosts: Mutex<HashMap<String, HttpClient>>,
    read_only: bool,
}

fn auth_headers(api_key: &str) -> Auth {
    Auth::Header { name: "Api-Key".into(), value: api_key.to_string() }
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let api_key = conn
        .secret
        .as_deref()
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .ok_or_else(|| AppError::not_connected("Pinecone needs an API key (secret)."))?
        .to_string();
    let insecure = s.ssl_mode == SslMode::Require;
    let control = HttpClient::new(CONTROL_PLANE, auth_headers(&api_key), insecure)?;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty() && !h.contains("api.pinecone.io"));
    let fixed = match host {
        Some(h) => {
            let index = s
                .database
                .as_deref()
                .map(str::trim)
                .filter(|d| !d.is_empty())
                .map(str::to_string)
                .or_else(|| h.trim_start_matches("https://").split('.').next().and_then(|first| first.rsplit_once('-').map(|(name, _)| name.to_string())))
                .ok_or_else(|| AppError::invalid_input("Set the index name in the database field when connecting to a data-plane host."))?;
            Some((index, HttpClient::new(normalize_host(h), auth_headers(&api_key), insecure)?))
        }
        None => None,
    };
    let integration = PineconeIntegration {
        engine: s.engine,
        control,
        fixed,
        api_key,
        insecure,
        hosts: Mutex::new(HashMap::new()),
        read_only: s.read_only,
    };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

fn normalize_host(host: &str) -> String {
    let h = host.trim().trim_end_matches('/');
    if h.starts_with("http://") || h.starts_with("https://") {
        h.to_string()
    } else {
        format!("https://{h}")
    }
}

fn fixed_columns() -> Vec<ColumnInfo> {
    let spec = [(ID_COLUMN, "string", true), ("values", "json", false), ("metadata", "json", false), ("sparse_values", "json", false), (NAMESPACE_COLUMN, "string", false)];
    spec.iter()
        .enumerate()
        .map(|(i, (name, ty, pk))| ColumnInfo { name: (*name).into(), data_type: (*ty).into(), nullable: !pk, primary_key: *pk, ordinal: i as u32 })
        .collect()
}

// WHAT:  Pulls a `namespace = x` (or `IN`) rule out of the filter list; the rest run locally.
fn split_namespace(filters: &[FilterRule]) -> (String, Vec<FilterRule>) {
    let mut ns = String::new();
    let mut rest = Vec::new();
    for f in filters {
        if f.column == NAMESPACE_COLUMN && matches!(f.op, FilterOp::Eq | FilterOp::In) && ns.is_empty() {
            ns = f.value.split(',').next().unwrap_or("").trim().to_string();
        } else {
            rest.push(f.clone());
        }
    }
    (ns, rest)
}

fn vectors_to_rows(vectors: &[Json], namespace: &str, columns: &[ColumnInfo]) -> ResultSet {
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let rows = vectors
        .iter()
        .map(|v| {
            names
                .iter()
                .map(|n| match n.as_str() {
                    NAMESPACE_COLUMN => Value::Text(namespace.to_string()),
                    ID_COLUMN => v.get("id").map(http::json_to_value).unwrap_or(Value::Null),
                    "values" => v.get("values").filter(|x| !x.is_null()).map(|x| Value::Json(x.clone())).unwrap_or(Value::Null),
                    "metadata" => v.get("metadata").filter(|x| !x.is_null()).map(|x| Value::Json(x.clone())).unwrap_or(Value::Null),
                    "sparse_values" => v.get("sparseValues").or(v.get("sparse_values")).filter(|x| !x.is_null()).map(|x| Value::Json(x.clone())).unwrap_or(Value::Null),
                    other => v.get(other).map(http::json_to_value).unwrap_or(Value::Null),
                })
                .collect()
        })
        .collect();
    ResultSet {
        columns: columns.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect(),
        rows,
        truncated: false,
    }
}

fn fetched_vectors(resp: &Json) -> Vec<Json> {
    resp.get("vectors")
        .and_then(Json::as_object)
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default()
}

#[derive(Debug, PartialEq)]
enum Command {
    Indexes,
    Stats(String),
    List { index: String, body: Json },
    Fetch { index: String, body: Json },
    Query { index: String, body: Json },
    Upsert { index: String, body: Json },
    Delete { index: String, body: Json },
    Raw { method: String, path: String, body: Option<Json>, host: Option<String> },
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
        let obj = value.as_object().ok_or_else(|| AppError::invalid_input("Expected a JSON object."))?;
        if let Some(path) = obj.get("path").and_then(Json::as_str) {
            let method = obj.get("method").and_then(Json::as_str).unwrap_or("GET").to_ascii_uppercase();
            let host = obj.get("host").and_then(Json::as_str).map(str::to_string);
            return Ok(Command::Raw { method, path: path.to_string(), body: obj.get("body").cloned(), host });
        }
        let index = obj
            .get("index")
            .and_then(Json::as_str)
            .map(str::to_string)
            .ok_or_else(|| AppError::invalid_input("Missing \"index\" (or \"path\" for a raw request)."))?;
        let body = |k: &str| obj.get(k).cloned();
        if let Some(body) = body("query") {
            return Ok(Command::Query { index, body });
        }
        if let Some(body) = body("upsert") {
            return Ok(Command::Upsert { index, body });
        }
        if let Some(body) = body("delete") {
            return Ok(Command::Delete { index, body });
        }
        if let Some(body) = body("fetch") {
            return Ok(Command::Fetch { index, body });
        }
        if let Some(body) = body("list") {
            return Ok(Command::List { index, body });
        }
        return Ok(Command::Stats(index));
    }
    let mut words = text.split_whitespace();
    let verb = words.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "INDEXES" => Ok(Command::Indexes),
        "STATS" | "DESCRIBE" => {
            let i = words.next().ok_or_else(|| AppError::invalid_input("Usage: STATS <index>"))?;
            Ok(Command::Stats(i.to_string()))
        }
        "LIST" => {
            let i = words.next().ok_or_else(|| AppError::invalid_input("Usage: LIST <index> [n] [namespace]"))?;
            let n = words.next().and_then(|n| n.parse::<u64>().ok()).unwrap_or(LIST_CAP).min(LIST_CAP);
            let mut body = json!({"limit": n});
            if let Some(ns) = words.next() {
                body["namespace"] = json!(ns);
            }
            Ok(Command::List { index: i.to_string(), body })
        }
        _ => Err(AppError::invalid_input(
            "Unknown command. Use INDEXES, STATS <index>, LIST <index> [n], or JSON like {\"index\": \"i\", \"query\": {...}}.",
        )),
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

impl PineconeIntegration {
    fn versioned(&self, client: &HttpClient, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        client.request(method, path).header("X-Pinecone-API-Version", API_VERSION)
    }

    async fn call(&self, client: &HttpClient, method: reqwest::Method, path: &str, body: Option<&Json>) -> AppResult<Json> {
        let mut req = self.versioned(client, method, path);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = client.send(req).await?;
        let text = resp.text().await.map_err(|e| AppError::driver(e.to_string()))?;
        if text.trim().is_empty() {
            return Ok(json!({}));
        }
        Ok(serde_json::from_str(&text).unwrap_or(Json::String(text)))
    }

    async fn describe_index(&self, name: &str) -> AppResult<Json> {
        if name.is_empty() || name.contains('/') {
            return Err(AppError::invalid_input(format!("Invalid index name: {name:?}")));
        }
        self.call(&self.control, reqwest::Method::GET, &format!("/indexes/{name}"), None).await
    }

    async fn data_client(&self, index: &str) -> AppResult<HttpClient> {
        if let Some((fixed_name, client)) = &self.fixed {
            if fixed_name == index {
                return Ok(client.clone());
            }
        }
        if let Some(c) = self.hosts.lock().await.get(index) {
            return Ok(c.clone());
        }
        let desc = self.describe_index(index).await?;
        let host = desc
            .get("host")
            .and_then(Json::as_str)
            .ok_or_else(|| AppError::driver(format!("Index {index} has no host yet (still initialising?)")))?;
        let client = HttpClient::new(normalize_host(host), auth_headers(&self.api_key), self.insecure)?;
        self.hosts.lock().await.insert(index.to_string(), client.clone());
        Ok(client)
    }

    async fn data_call(&self, index: &str, method: reqwest::Method, path: &str, body: Option<&Json>) -> AppResult<Json> {
        let client = self.data_client(index).await?;
        self.call(&client, method, path, body).await
    }

    async fn stats(&self, index: &str) -> AppResult<Json> {
        self.data_call(index, reqwest::Method::POST, "/describe_index_stats", Some(&json!({}))).await
    }

    async fn index_names(&self) -> AppResult<Vec<String>> {
        if let Some((name, _)) = &self.fixed {
            return Ok(vec![name.clone()]);
        }
        let resp = self.call(&self.control, reqwest::Method::GET, "/indexes", None).await?;
        let mut names: Vec<String> = resp
            .get("indexes")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(|i| i.get("name").and_then(Json::as_str).map(str::to_string)).collect())
            .unwrap_or_default();
        names.sort();
        Ok(names)
    }

    // WHAT:  Lists up to `want` ids (following pagination tokens), then fetches them.
    async fn window(&self, index: &str, namespace: &str, want: u64) -> AppResult<Vec<Json>> {
        let client = self.data_client(index).await?;
        let mut ids: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        while (ids.len() as u64) < want {
            let batch = (want - ids.len() as u64).min(LIST_CAP);
            let mut path = format!("/vectors/list?limit={batch}");
            if !namespace.is_empty() {
                path.push_str(&format!("&namespace={namespace}"));
            }
            if let Some(t) = &token {
                path.push_str(&format!("&paginationToken={t}"));
            }
            let resp = self.call(&client, reqwest::Method::GET, &path, None).await?;
            let page: Vec<String> = resp
                .get("vectors")
                .and_then(Json::as_array)
                .map(|a| a.iter().filter_map(|v| v.get("id").and_then(Json::as_str).map(str::to_string)).collect())
                .unwrap_or_default();
            if page.is_empty() {
                break;
            }
            ids.extend(page);
            token = resp.pointer("/pagination/next").and_then(Json::as_str).map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(100) {
            let query: Vec<String> = chunk.iter().map(|id| format!("ids={}", urlencode(id))).collect();
            let mut path = format!("/vectors/fetch?{}", query.join("&"));
            if !namespace.is_empty() {
                path.push_str(&format!("&namespace={namespace}"));
            }
            let resp = self.call(&client, reqwest::Method::GET, &path, None).await?;
            let mut fetched = fetched_vectors(&resp);
            // Keep list order so paging is stable.
            fetched.sort_by_key(|v| chunk.iter().position(|id| Some(id.as_str()) == v.get("id").and_then(Json::as_str)).unwrap_or(usize::MAX));
            out.extend(fetched);
        }
        Ok(out)
    }

    async fn run(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        if self.read_only && cmd.is_mutation() {
            return Err(AppError::invalid_input("This connection is read-only; write operations are refused."));
        }
        let rows = |v: Json| StatementResult::Rows { result: json_result(v) };
        match cmd {
            Command::Indexes => {
                if self.fixed.is_some() {
                    let names = self.index_names().await?;
                    let docs: Vec<Json> = names.into_iter().map(|n| json!({"name": n})).collect();
                    return Ok(StatementResult::Rows { result: objects_to_result_set(&docs, Some("name"), max_rows) });
                }
                let resp = self.call(&self.control, reqwest::Method::GET, "/indexes", None).await?;
                let docs: Vec<Json> = resp
                    .get("indexes")
                    .and_then(Json::as_array)
                    .map(|a| {
                        a.iter()
                            .map(|i| {
                                json!({
                                    "name": i.get("name").cloned().unwrap_or(Json::Null),
                                    "dimension": i.get("dimension").cloned().unwrap_or(Json::Null),
                                    "metric": i.get("metric").cloned().unwrap_or(Json::Null),
                                    "host": i.get("host").cloned().unwrap_or(Json::Null),
                                    "status": i.pointer("/status/state").cloned().unwrap_or(Json::Null),
                                    "spec": i.get("spec").cloned().unwrap_or(Json::Null),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, Some("name"), max_rows) })
            }
            Command::Stats(index) => Ok(rows(self.stats(&index).await?)),
            Command::List { index, body } => {
                let ns = body.get("namespace").and_then(Json::as_str).unwrap_or("").to_string();
                let n = body.get("limit").and_then(Json::as_u64).unwrap_or(LIST_CAP).min(max_rows as u64).min(WINDOW_CAP);
                let vectors = self.window(&index, &ns, n).await?;
                Ok(StatementResult::Rows { result: vectors_to_rows(&vectors, &ns, &fixed_columns()) })
            }
            Command::Fetch { index, body } => {
                let ns = body.get("namespace").and_then(Json::as_str).unwrap_or("").to_string();
                let ids: Vec<String> = body.get("ids").and_then(Json::as_array).map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();
                let query: Vec<String> = ids.iter().map(|id| format!("ids={}", urlencode(id))).collect();
                let mut path = format!("/vectors/fetch?{}", query.join("&"));
                if !ns.is_empty() {
                    path.push_str(&format!("&namespace={ns}"));
                }
                let resp = self.data_call(&index, reqwest::Method::GET, &path, None).await?;
                Ok(StatementResult::Rows { result: vectors_to_rows(&fetched_vectors(&resp), &ns, &fixed_columns()) })
            }
            Command::Query { index, mut body } => {
                if body.get("topK").is_none() {
                    body["topK"] = json!(max_rows.min(10_000));
                }
                if body.get("includeMetadata").is_none() {
                    body["includeMetadata"] = json!(true);
                }
                let resp = self.data_call(&index, reqwest::Method::POST, "/query", Some(&body)).await?;
                let ns = resp.get("namespace").and_then(Json::as_str).unwrap_or("").to_string();
                let matches = resp.get("matches").and_then(Json::as_array).cloned().unwrap_or_default();
                let mut columns = fixed_columns();
                columns.insert(1, ColumnInfo { name: "score".into(), data_type: "number".into(), nullable: true, primary_key: false, ordinal: 1 });
                Ok(StatementResult::Rows { result: vectors_to_rows(&matches, &ns, &columns) })
            }
            Command::Upsert { index, body } => {
                let resp = self.data_call(&index, reqwest::Method::POST, "/vectors/upsert", Some(&body)).await?;
                Ok(StatementResult::Affected { rows_affected: resp.get("upsertedCount").and_then(Json::as_u64).unwrap_or(0) })
            }
            Command::Delete { index, body } => {
                let n = body.get("ids").and_then(Json::as_array).map(Vec::len).unwrap_or(0) as u64;
                let _ = self.data_call(&index, reqwest::Method::POST, "/vectors/delete", Some(&body)).await?;
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Raw { method, path, body, host } => {
                let m = reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Bad method {method}")))?;
                let client = match host {
                    Some(h) => HttpClient::new(normalize_host(&h), auth_headers(&self.api_key), self.insecure)?,
                    None => match &self.fixed {
                        Some((_, c)) if !path.starts_with("/indexes") => c.clone(),
                        _ => self.control.clone(),
                    },
                };
                Ok(rows(self.call(&client, m, &path, body.as_ref()).await?))
            }
        }
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[async_trait]
impl Integration for PineconeIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: false,
            namespaces: false,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        }
    }

    async fn ping(&self) -> AppResult<()> {
        match &self.fixed {
            Some((name, _)) => {
                let _ = self.stats(name).await?;
            }
            None => {
                let _ = self.call(&self.control, reqwest::Method::GET, "/indexes", None).await?;
            }
        }
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some(format!("Pinecone API {API_VERSION}")))
    }

    fn current_database(&self) -> Option<String> {
        self.fixed.as_ref().map(|(n, _)| n.clone()).or_else(|| Some("indexes".into()))
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.current_database().unwrap_or_default()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut tables = Vec::new();
        for name in self.index_names().await? {
            let row_estimate = self.stats(&name).await.ok().and_then(|s| s.get("totalVectorCount").and_then(Json::as_i64));
            tables.push(TableInfo { schema: Some("indexes".into()), name, kind: TableKind::Table, row_estimate });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: "indexes".into(), tables }] })
    }

    async fn columns(&self, _table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(fixed_columns())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let stats = self.stats(&table.name).await?;
        Ok(stats.get("totalVectorCount").and_then(Json::as_i64))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (ns, rest) = split_namespace(filters);
        let stats = self.stats(&table.name).await?;
        if rest.is_empty() {
            if ns.is_empty() {
                return Ok(stats.get("totalVectorCount").and_then(Json::as_i64).unwrap_or(0));
            }
            return Ok(stats.pointer(&format!("/namespaces/{ns}/vectorCount")).and_then(Json::as_i64).unwrap_or(0));
        }
        let vectors = self.window(&table.name, &ns, WINDOW_CAP).await?;
        let rs = vectors_to_rows(&vectors, &ns, &fixed_columns());
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        Ok(http::local::apply_filters(&names, rs.rows, &rest).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let (ns, rest) = split_namespace(&query.filters);
        let need_all = !rest.is_empty() || !query.sort.is_empty();
        let want = if need_all { WINDOW_CAP } else { (query.offset + u64::from(query.limit)).clamp(1, WINDOW_CAP) };
        let vectors = self.window(&table.name, &ns, want).await?;
        let rs = vectors_to_rows(&vectors, &ns, &fixed_columns());
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery { sort: query.sort.clone(), filters: rest, offset: query.offset, limit: query.limit };
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
    use crate::model::{ConnectionSummary, Environment};

    #[test]
    fn host_and_url_helpers() {
        assert_eq!(normalize_host("idx-abc.svc.us-east1.pinecone.io/"), "https://idx-abc.svc.us-east1.pinecone.io");
        assert_eq!(normalize_host("http://localhost:5080"), "http://localhost:5080");
        assert_eq!(urlencode("a b/c"), "a%20b%2Fc");
    }

    #[test]
    fn namespace_filter_is_split_out() {
        let rules = vec![
            FilterRule { column: "namespace".into(), op: FilterOp::Eq, value: "ns1".into() },
            FilterRule { column: "id".into(), op: FilterOp::StartsWith, value: "doc".into() },
        ];
        let (ns, rest) = split_namespace(&rules);
        assert_eq!(ns, "ns1");
        assert_eq!(rest.len(), 1);
    }

    #[test]
    fn vectors_to_grid() {
        let vectors = vec![json!({"id": "v1", "values": [0.1, 0.2], "metadata": {"genre": "x"}, "sparseValues": {"indices": [1], "values": [0.5]}})];
        let rs = vectors_to_rows(&vectors, "ns", &fixed_columns());
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "values", "metadata", "sparse_values", "namespace"]);
        assert_eq!(rs.rows[0][0], Value::Text("v1".into()));
        assert_eq!(rs.rows[0][1], Value::Json(json!([0.1, 0.2])));
        assert_eq!(rs.rows[0][3], Value::Json(json!({"indices": [1], "values": [0.5]})));
        assert_eq!(rs.rows[0][4], Value::Text("ns".into()));
        let fetched = fetched_vectors(&json!({"vectors": {"v1": {"id": "v1"}}}));
        assert_eq!(fetched.len(), 1);
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse_command("indexes").ok(), Some(Command::Indexes));
        assert_eq!(parse_command("STATS docs").ok(), Some(Command::Stats("docs".into())));
        assert_eq!(parse_command("list docs 5 ns").ok(), Some(Command::List { index: "docs".into(), body: json!({"limit": 5, "namespace": "ns"}) }));
        let q = parse_command(r#"{"index":"docs","query":{"vector":[0.1],"topK":3}}"#).ok();
        assert_eq!(q, Some(Command::Query { index: "docs".into(), body: json!({"vector": [0.1], "topK": 3}) }));
        let raw = parse_command(r#"{"path":"/query","method":"POST","body":{},"host":"h.pinecone.io"}"#).ok();
        assert_eq!(raw, Some(Command::Raw { method: "POST".into(), path: "/query".into(), body: Some(json!({})), host: Some("h.pinecone.io".into()) }));
        assert!(parse_command(r#"{"index":"docs","delete":{"deleteAll":true}}"#).map(|c| c.is_mutation()).unwrap_or(false));
        assert!(parse_command("SELECT 1").is_err());
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(key) = std::env::var("DBFREE_TEST_PINECONE_KEY") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Pinecone,
                environment: Environment::Local,
                read_only: false,
                host: std::env::var("DBFREE_TEST_PINECONE_HOST").ok(),
                port: None,
                database: std::env::var("DBFREE_TEST_PINECONE_INDEX").ok(),
                username: None,
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: Some(key),
        };
        let p = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let catalog = p.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let Some(index) = catalog.schemas.first().and_then(|s| s.tables.first()).map(|t| t.name.clone()) else {
            return;
        };
        let table = TableRef { schema: Some("indexes".into()), name: index };
        let _ = p.fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 5 }).await.unwrap_or_else(|e| panic!("page: {e}"));
    }
}
