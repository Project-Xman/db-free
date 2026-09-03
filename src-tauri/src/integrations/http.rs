// SOT: http-adapter-client, rest-integration-helpers, json-to-value, http-auth

use crate::error::{AppError, AppResult};
use crate::model::{ColumnMeta, ResolvedConnection, ResultSet, SslMode, Value};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Method, Response, StatusCode};
use serde::de::DeserializeOwned;
use std::time::Duration;

// ============================================================================
// SHARED HTTP CLIENT FOR REST-STYLE ENGINES
//
// WHAT:  One thin wrapper every REST adapter (Qdrant, Elasticsearch, CouchDB,
//        InfluxDB, SPARQL endpoints, …) builds on: base URL normalisation,
//        auth header strategy, timeouts, status → AppError mapping, and the
//        JSON → model::Value helpers the grid needs.
// WHY:   Twenty adapters re-implementing "POST json, check status, parse" is
//        twenty places to get error handling wrong. `reqwest` stays confined
//        to this module plus the adapters listed in scripts/guardrail.py.
// HOW:   `HttpClient::from_connection` reads host/port/ssl_mode from the
//        summary; adapters pick the auth scheme their engine expects.
// WHERE: src-tauri/src/integrations/*.rs (consumers)
// ============================================================================

#[derive(Debug, Clone)]
pub enum Auth {
    None,
    /// `Authorization: Bearer <token>`
    Bearer(String),
    /// HTTP basic with the connection's username + secret.
    Basic { user: String, password: String },
    /// Arbitrary header, e.g. `api-key: …` (Qdrant), `X-TYPESENSE-API-KEY: …`.
    Header { name: String, value: String },
}

#[derive(Clone)]
pub struct HttpClient {
    client: Client,
    base: String,
    auth: Auth,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient").field("base", &self.base).finish()
    }
}

// WHAT:  Turns host/port/ssl into `https://host:port` (or passes a full URL through).
pub fn base_url(conn: &ResolvedConnection, default_port: Option<u16>, default_tls: bool) -> String {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("localhost");
    if host.starts_with("http://") || host.starts_with("https://") {
        return host.trim_end_matches('/').to_string();
    }
    let tls = match s.ssl_mode {
        SslMode::Disable => false,
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => true,
        SslMode::Prefer => default_tls,
    };
    let scheme = if tls { "https" } else { "http" };
    match s.port.or(default_port) {
        Some(port) if !host.contains(':') => format!("{scheme}://{host}:{port}"),
        _ => format!("{scheme}://{host}"),
    }
}

impl HttpClient {
    pub fn new(base: impl Into<String>, auth: Auth, accept_invalid_certs: bool) -> AppResult<HttpClient> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(90))
            .danger_accept_invalid_certs(accept_invalid_certs)
            .user_agent("db-free")
            .build()
            .map_err(|e| AppError::driver(e.to_string()))?;
        Ok(HttpClient { client, base: base.into().trim_end_matches('/').to_string(), auth })
    }

    /// Base URL + auth derived from the connection. `ssl_mode = Require` accepts
    /// self-signed certificates (encrypted but unverified), VerifyCa/VerifyFull verify.
    pub fn from_connection(conn: &ResolvedConnection, default_port: Option<u16>, default_tls: bool, auth: Auth) -> AppResult<HttpClient> {
        let base = base_url(conn, default_port, default_tls);
        let insecure = conn.summary.ssl_mode == SslMode::Require;
        HttpClient::new(base, auth, insecure)
    }

    /// Bearer when only a secret is set, Basic when username + secret, else none.
    pub fn auth_from_connection(conn: &ResolvedConnection) -> Auth {
        let user = conn.summary.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
        let secret = conn.secret.as_deref().filter(|p| !p.is_empty());
        match (user, secret) {
            (Some(u), Some(p)) => Auth::Basic { user: u.to_string(), password: p.to_string() },
            (Some(u), None) => Auth::Basic { user: u.to_string(), password: String::new() },
            (None, Some(p)) => Auth::Bearer(p.to_string()),
            (None, None) => Auth::None,
        }
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else {
            format!("{}/{}", self.base, path.trim_start_matches('/'))
        }
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            Auth::None => req,
            Auth::Bearer(t) => req.bearer_auth(t),
            Auth::Basic { user, password } => req.basic_auth(user, Some(password)),
            Auth::Header { name, value } => req.header(name.as_str(), value.as_str()),
        }
    }

    pub fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.apply_auth(self.client.request(method, self.url(path)))
    }

    pub async fn send(&self, req: reqwest::RequestBuilder) -> AppResult<Response> {
        let resp = req.send().await.map_err(map_reqwest)?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        Err(status_error(status, &body))
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> AppResult<T> {
        let resp = self.send(self.request(Method::GET, path)).await?;
        resp.json::<T>().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))
    }

    pub async fn post_json<T: DeserializeOwned>(&self, path: &str, body: &serde_json::Value) -> AppResult<T> {
        let resp = self.send(self.request(Method::POST, path).json(body)).await?;
        resp.json::<T>().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))
    }

    pub async fn put_json<T: DeserializeOwned>(&self, path: &str, body: &serde_json::Value) -> AppResult<T> {
        let resp = self.send(self.request(Method::PUT, path).json(body)).await?;
        resp.json::<T>().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))
    }

    pub async fn delete_json<T: DeserializeOwned>(&self, path: &str) -> AppResult<T> {
        let resp = self.send(self.request(Method::DELETE, path)).await?;
        resp.json::<T>().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))
    }

    pub async fn get_text(&self, path: &str) -> AppResult<String> {
        let resp = self.send(self.request(Method::GET, path)).await?;
        resp.text().await.map_err(map_reqwest)
    }

    /// POST with a raw body + content type (SPARQL, InfluxQL, XQuery endpoints).
    pub async fn post_raw(&self, path: &str, content_type: &str, body: String, accept: Option<&str>) -> AppResult<String> {
        let mut headers = HeaderMap::new();
        if let Ok(v) = HeaderValue::from_str(content_type) {
            headers.insert(reqwest::header::CONTENT_TYPE, v);
        }
        if let Some(a) = accept {
            if let Ok(v) = HeaderValue::from_str(a) {
                headers.insert(reqwest::header::ACCEPT, v);
            }
        }
        let resp = self.send(self.request(Method::POST, path).headers(headers).body(body)).await?;
        resp.text().await.map_err(map_reqwest)
    }

    pub fn header(name: &str, value: &str) -> Auth {
        let _ = HeaderName::from_bytes(name.as_bytes());
        Auth::Header { name: name.to_string(), value: value.to_string() }
    }
}

fn map_reqwest(err: reqwest::Error) -> AppError {
    if err.is_timeout() {
        AppError::timeout("The server did not respond in time.")
    } else if err.is_connect() {
        AppError::not_connected(format!("Could not reach the server: {err}"))
    } else {
        AppError::driver(err.to_string())
    }
}

// WHAT:  HTTP status → AppError, surfacing the server's own message when it is JSON.
pub fn status_error(status: StatusCode, body: &str) -> AppError {
    let detail = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            ["error", "message", "reason", "detail", "status", "msg"]
                .iter()
                .find_map(|k| v.get(k).map(|x| match x {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                }))
        })
        .unwrap_or_else(|| body.chars().take(500).collect());
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => AppError::not_connected(format!("Authentication failed ({status}): {detail}")),
        StatusCode::NOT_FOUND => AppError::not_found(format!("Not found ({status}): {detail}")),
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => AppError::timeout(format!("{status}: {detail}")),
        _ => AppError::driver(format!("{status}: {detail}")),
    }
}

// ---------------------------------------------------------------------------
// JSON → model::Value
// ---------------------------------------------------------------------------

pub fn json_to_value(val: &serde_json::Value) -> Value {
    match val {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(val.clone()),
    }
}

pub fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// WHAT:  A list of JSON objects → a grid. Columns are the union of keys in
//        first-seen order; `id_first` (e.g. "_id", "id") is pinned to column 0.
pub fn objects_to_result_set(docs: &[serde_json::Value], id_first: Option<&str>, max_rows: usize) -> ResultSet {
    let mut names: Vec<String> = Vec::new();
    if let Some(id) = id_first {
        names.push(id.to_string());
    }
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for k in obj.keys() {
                if !names.iter().any(|n| n == k) {
                    names.push(k.clone());
                }
            }
        }
    }
    let truncated = docs.len() > max_rows;
    let rows: Vec<Vec<Value>> = docs
        .iter()
        .take(max_rows)
        .map(|doc| match doc.as_object() {
            Some(obj) => names.iter().map(|n| obj.get(n).map(json_to_value).unwrap_or(Value::Null)).collect(),
            None => {
                let mut row = vec![Value::Null; names.len()];
                if let Some(cell) = row.first_mut() {
                    *cell = json_to_value(doc);
                }
                row
            }
        })
        .collect();
    let columns = names
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let type_name = docs
                .iter()
                .find_map(|d| d.as_object().and_then(|o| o.get(&name)).filter(|v| !v.is_null()).map(json_type_name))
                .unwrap_or("json")
                .to_string();
            let _ = i;
            ColumnMeta { name, type_name }
        })
        .collect();
    ResultSet { columns, rows, truncated }
}

// WHAT:  A raw JSON payload (anything) → a one-cell grid so `execute` can always
//        show the engine's response verbatim.
pub fn json_result(value: serde_json::Value) -> ResultSet {
    match &value {
        serde_json::Value::Array(items) if items.iter().all(|i| i.is_object()) && !items.is_empty() => objects_to_result_set(items, None, usize::MAX),
        _ => ResultSet {
            columns: vec![ColumnMeta { name: "result".into(), type_name: json_type_name(&value).into() }],
            rows: vec![vec![json_to_value(&value)]],
            truncated: false,
        },
    }
}

// WHAT:  Client-side filter + sort + slice for engines with no server-side paging.
pub mod local {
    use crate::model::{FilterOp, FilterRule, PageQuery, SortRule, Value};
    use std::cmp::Ordering;

    fn cell_text(v: &Value) -> String {
        match v {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => s.clone(),
            Value::Json(j) => j.to_string(),
        }
    }

    fn compare(a: &Value, b: &Value) -> Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
            (Value::Int(x), Value::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
            (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            _ => cell_text(a).cmp(&cell_text(b)),
        }
    }

    fn matches(rule: &FilterRule, v: &Value) -> bool {
        let text = cell_text(v);
        let needle = rule.value.trim();
        let num = |s: &str| s.parse::<f64>().ok();
        let cmp = || match (num(&text), num(needle)) {
            (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            _ => text.cmp(&needle.to_string()),
        };
        match rule.op {
            FilterOp::Eq => text == needle,
            FilterOp::Ne => text != needle,
            FilterOp::Gt => cmp() == Ordering::Greater,
            FilterOp::Gte => cmp() != Ordering::Less,
            FilterOp::Lt => cmp() == Ordering::Less,
            FilterOp::Lte => cmp() != Ordering::Greater,
            FilterOp::Contains => text.to_lowercase().contains(&needle.to_lowercase()),
            FilterOp::StartsWith => text.to_lowercase().starts_with(&needle.to_lowercase()),
            FilterOp::EndsWith => text.to_lowercase().ends_with(&needle.to_lowercase()),
            FilterOp::In => needle.split(',').map(str::trim).any(|x| x == text),
            FilterOp::IsNull => matches!(v, Value::Null),
            FilterOp::IsNotNull => !matches!(v, Value::Null),
        }
    }

    pub fn apply_filters(columns: &[String], rows: Vec<Vec<Value>>, filters: &[FilterRule]) -> Vec<Vec<Value>> {
        if filters.is_empty() {
            return rows;
        }
        rows.into_iter()
            .filter(|row| {
                filters.iter().all(|f| {
                    columns
                        .iter()
                        .position(|c| c == &f.column)
                        .and_then(|i| row.get(i))
                        .map(|v| matches(f, v))
                        .unwrap_or(false)
                })
            })
            .collect()
    }

    pub fn apply_sort(columns: &[String], rows: &mut [Vec<Value>], sort: &[SortRule]) {
        if sort.is_empty() {
            return;
        }
        let idx: Vec<(usize, bool)> = sort
            .iter()
            .filter_map(|s| columns.iter().position(|c| c == &s.column).map(|i| (i, s.desc)))
            .collect();
        rows.sort_by(|a, b| {
            for (i, desc) in &idx {
                let ord = compare(a.get(*i).unwrap_or(&Value::Null), b.get(*i).unwrap_or(&Value::Null));
                if ord != Ordering::Equal {
                    return if *desc { ord.reverse() } else { ord };
                }
            }
            Ordering::Equal
        });
    }

    /// Filter → sort → offset/limit, in one call.
    pub fn page(columns: &[String], rows: Vec<Vec<Value>>, query: &PageQuery) -> Vec<Vec<Value>> {
        let mut rows = apply_filters(columns, rows, &query.filters);
        apply_sort(columns, &mut rows, &query.sort);
        rows.into_iter().skip(query.offset as usize).take(query.limit as usize).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Engine, Environment};

    fn conn(host: &str, port: Option<u16>, ssl: SslMode) -> ResolvedConnection {
        ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Qdrant,
                environment: Environment::Local,
                read_only: false,
                host: Some(host.into()),
                port,
                database: None,
                username: None,
                file_path: None,
                ssl_mode: ssl,
                has_secret: false,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: None,
        }
    }

    #[test]
    fn base_url_respects_scheme_and_port() {
        assert_eq!(base_url(&conn("localhost", Some(6333), SslMode::Prefer), Some(6333), false), "http://localhost:6333");
        assert_eq!(base_url(&conn("db.example.com", None, SslMode::Require), Some(443), true), "https://db.example.com:443");
        assert_eq!(base_url(&conn("https://x.cloud/", None, SslMode::Disable), Some(1), false), "https://x.cloud");
        assert_eq!(base_url(&conn("host:9000", None, SslMode::Disable), Some(1), false), "http://host:9000");
    }

    #[test]
    fn objects_union_columns_and_pin_id() {
        let docs = vec![
            serde_json::json!({"id": 1, "name": "a"}),
            serde_json::json!({"id": 2, "extra": true}),
        ];
        let rs = objects_to_result_set(&docs, Some("id"), 10);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "name", "extra"]);
        assert_eq!(rs.rows[1][1], Value::Null);
        assert_eq!(rs.rows[1][2], Value::Bool(true));
        assert!(!rs.truncated);
        assert!(objects_to_result_set(&docs, None, 1).truncated);
    }

    #[test]
    fn local_page_filters_sorts_and_slices() {
        use crate::model::{FilterOp, FilterRule, PageQuery, SortRule};
        let cols = vec!["k".to_string(), "n".to_string()];
        let rows = vec![
            vec![Value::Text("a".into()), Value::Int(3)],
            vec![Value::Text("b".into()), Value::Int(1)],
            vec![Value::Text("c".into()), Value::Int(2)],
        ];
        let q = PageQuery {
            sort: vec![SortRule { column: "n".into(), desc: true }],
            filters: vec![FilterRule { column: "n".into(), op: FilterOp::Gte, value: "2".into() }],
            offset: 0,
            limit: 10,
        };
        let out = local::page(&cols, rows, &q);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0][0], Value::Text("a".into()));
        assert_eq!(out[1][0], Value::Text("c".into()));
    }

    #[test]
    fn status_error_extracts_message() {
        let err = status_error(StatusCode::UNAUTHORIZED, r#"{"error":"bad key"}"#);
        assert!(matches!(err, AppError::NotConnected { .. }));
        assert!(err.message().contains("bad key"));
        let err = status_error(StatusCode::INTERNAL_SERVER_ERROR, "boom");
        assert!(matches!(err, AppError::Driver { .. }));
    }
}
