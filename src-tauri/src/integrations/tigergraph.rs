// SOT: tigergraph-integration, gsql, tigergraph-rest, restpp, tigergraph-token

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, json_to_value, json_type_name, local, objects_to_result_set, Auth, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SslMode, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  TigerGraph adapter over REST++ (port 9000) and the GSQL server
//        (port 14240). The `database` field is the graph name (required).
// WHY:   Vertex and edge types have declared attributes, so the grid gets
//        fixed columns: `v_id` (pk) + attributes for vertices, `from_id` /
//        `to_id` (pks) + `e_type` + attributes for edges. Types are grouped in
//        two schemas: `vertices` and `edges`.
// HOW:   Auth: username + secret → Basic for the GSQL server and a REST++
//        token via POST /restpp/requesttoken {secret} (falls back to a GET
//        with `?secret=`); a secret without a user is used as a Bearer token
//        directly. Vertex pages use GET /restpp/graph/{g}/vertices/{type}
//        with TigerGraph's `filter=attr=val,attr2>3` syntax for simple
//        comparisons (remaining filters, sort and offset are client-side).
//        Edge pages list a handful of source vertices then their edges
//        (capped). Counts use POST /restpp/builtins/{g} stat_vertex_number /
//        stat_edge_number. `execute` runs installed queries (JSON
//        `{"query","params"}`), GSQL `INTERPRET QUERY` / `SELECT` text through
//        POST /gsqlserver/interpreted_query, or a raw `{"method","path","body"}`
//        passthrough. Mutating requests are refused when read-only.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs (trait)
// ============================================================================

const DEFAULT_PORT: u16 = 9000;
const GSQL_PORT: u16 = 14240;
const MAX_PAGE_ROWS: usize = 5_000;
const EDGE_SOURCE_SAMPLE: usize = 200;
const VERTEX_SCHEMA: &str = "vertices";
const EDGE_SCHEMA: &str = "edges";

pub struct TigerGraphIntegration {
    engine: Engine,
    restpp: HttpClient,
    gsql: HttpClient,
    graph: String,
    read_only: bool,
}

// WHAT:  Derives the GSQL-server base from the REST++ base: same host, port 14240
//        (unless the user gave a full URL with an explicit path, then reuse it).
fn gsql_base(restpp_base: &str) -> String {
    let (scheme, rest) = restpp_base.split_once("://").unwrap_or(("http", restpp_base));
    let host_port = rest.split('/').next().unwrap_or(rest);
    let host = host_port.rsplit_once(':').map(|(h, p)| if p.chars().all(|c| c.is_ascii_digit()) { h } else { host_port }).unwrap_or(host_port);
    if host_port.ends_with(&format!(":{DEFAULT_PORT}")) || !host_port.contains(':') {
        format!("{scheme}://{host}:{GSQL_PORT}")
    } else {
        // Non-default port (e.g. TigerGraph Cloud on 443 with path-based routing): reuse.
        format!("{scheme}://{host_port}")
    }
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let graph = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|g| !g.is_empty())
        .ok_or_else(|| AppError::invalid_input("TigerGraph needs a graph name in the database field."))?
        .to_string();
    let base = base_url(conn, Some(DEFAULT_PORT), false);
    let insecure = s.ssl_mode == SslMode::Require;
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    let secret = conn.secret.as_deref().map(str::trim).filter(|p| !p.is_empty());
    let gsql_auth = match (user, secret) {
        (Some(u), Some(p)) => Auth::Basic { user: u.to_string(), password: p.to_string() },
        (Some(u), None) => Auth::Basic { user: u.to_string(), password: String::new() },
        (None, Some(p)) => Auth::Bearer(p.to_string()),
        (None, None) => Auth::None,
    };
    let gsql = HttpClient::new(gsql_base(&base), gsql_auth, insecure)?;
    let restpp_auth = match (user, secret) {
        (Some(_), Some(p)) => {
            let anon = HttpClient::new(&base, Auth::None, insecure)?;
            match request_token(&anon, p, &graph).await {
                Ok(token) => Auth::Bearer(token),
                // Auth disabled on the server, or the password is not a secret: try without a token.
                Err(_) => Auth::Bearer(p.to_string()),
            }
        }
        (None, Some(p)) => Auth::Bearer(p.to_string()),
        _ => Auth::None,
    };
    let restpp = HttpClient::new(base, restpp_auth, insecure)?;
    let integration = TigerGraphIntegration { engine: s.engine, restpp, gsql, graph, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// WHAT:  POST /restpp/requesttoken {secret, graph, lifetime} (v3.5+), else GET ?secret=.
async fn request_token(anon: &HttpClient, secret: &str, graph: &str) -> AppResult<String> {
    let body = json!({ "secret": secret, "graph": graph, "lifetime": 86_400 });
    let resp: Json = match anon.post_json("/restpp/requesttoken", &body).await {
        Ok(v) => v,
        Err(_) => anon.get_json(&format!("/restpp/requesttoken?secret={}&lifetime=86400", pct(secret))).await?,
    };
    token_from_response(&resp).ok_or_else(|| AppError::not_connected("TigerGraph did not return a token for the given secret."))
}

fn token_from_response(v: &Json) -> Option<String> {
    if v.get("error").and_then(Json::as_bool).unwrap_or(false) {
        return None;
    }
    v.get("token")
        .or_else(|| v.get("results").and_then(|r| r.get("token")))
        .and_then(Json::as_str)
        .map(str::to_string)
}

fn pct(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

// WHAT:  Unwraps a REST++ envelope `{error, message, results}`.
fn unwrap_results(v: Json) -> AppResult<Json> {
    if v.get("error").and_then(Json::as_bool).unwrap_or(false) {
        let msg = v.get("message").and_then(Json::as_str).unwrap_or("request failed");
        return Err(AppError::driver(format!("TigerGraph: {msg}")));
    }
    Ok(v.get("results").cloned().unwrap_or(v))
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TypeInfo {
    name: String,
    attributes: Vec<(String, String)>,
    is_edge: bool,
}

fn attr_type_name(attr: &Json) -> String {
    let at = attr.get("AttributeType").unwrap_or(&Json::Null);
    let name = at.get("Name").and_then(Json::as_str).unwrap_or("STRING");
    match name {
        "LIST" | "SET" => {
            let inner = at.get("ValueTypeName").and_then(Json::as_str).unwrap_or("STRING");
            format!("{name}<{inner}>").to_ascii_lowercase()
        }
        "MAP" => "map".into(),
        other => other.to_ascii_lowercase(),
    }
}

fn parse_schema(schema: &Json) -> Vec<TypeInfo> {
    let mut out = Vec::new();
    for (key, is_edge) in [("VertexTypes", false), ("EdgeTypes", true)] {
        for t in schema.get(key).and_then(Json::as_array).into_iter().flatten() {
            let Some(name) = t.get("Name").and_then(Json::as_str) else { continue };
            let attributes = t
                .get("Attributes")
                .and_then(Json::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|attr| attr.get("AttributeName").and_then(Json::as_str).map(|n| (n.to_string(), attr_type_name(attr))))
                        .collect()
                })
                .unwrap_or_default();
            out.push(TypeInfo { name: name.to_string(), attributes, is_edge });
        }
    }
    out
}

fn type_columns(t: &TypeInfo) -> Vec<ColumnInfo> {
    let mut cols: Vec<ColumnInfo> = if t.is_edge {
        vec![
            ("from_id", "string", true),
            ("to_id", "string", true),
            ("from_type", "string", false),
            ("to_type", "string", false),
            ("e_type", "string", false),
        ]
    } else {
        vec![("v_id", "string", true), ("v_type", "string", false)]
    }
    .into_iter()
    .map(|(n, ty, pk)| ColumnInfo { name: n.into(), data_type: ty.into(), nullable: false, primary_key: pk, ordinal: 0 })
    .collect();
    for (name, ty) in &t.attributes {
        cols.push(ColumnInfo { name: name.clone(), data_type: ty.clone(), nullable: true, primary_key: false, ordinal: 0 });
    }
    for (i, c) in cols.iter_mut().enumerate() {
        c.ordinal = i as u32 + 1;
    }
    cols
}

// WHAT:  A REST++ vertex / edge object → one grid row aligned to `columns`.
fn element_row(columns: &[ColumnInfo], el: &Json) -> Vec<Value> {
    let attrs = el.get("attributes").and_then(Json::as_object);
    columns
        .iter()
        .map(|c| match c.name.as_str() {
            "v_id" | "v_type" | "from_id" | "to_id" | "from_type" | "to_type" | "e_type" if el.get(&c.name).is_some() => {
                el.get(&c.name).map(json_to_value).unwrap_or(Value::Null)
            }
            _ => attrs.and_then(|a| a.get(&c.name)).map(json_to_value).unwrap_or(Value::Null),
        })
        .collect()
}

// WHAT:  Filters TigerGraph can evaluate server-side (`attr=val,attr2>3`, no strings with commas).
fn server_filter(filters: &[FilterRule], attributes: &[(String, String)]) -> Option<String> {
    let parts: Vec<String> = filters
        .iter()
        .filter(|f| attributes.iter().any(|(a, _)| a == &f.column))
        .filter_map(|f| {
            let v = f.value.trim();
            if v.is_empty() || v.contains(',') || v.contains('&') || v.contains('=') || v.contains('"') {
                return None;
            }
            let quoted = if v.parse::<f64>().is_ok() || v == "true" || v == "false" { v.to_string() } else { format!("\"{v}\"") };
            let op = match f.op {
                FilterOp::Eq => "=",
                FilterOp::Ne => "!=",
                FilterOp::Gt => ">",
                FilterOp::Gte => ">=",
                FilterOp::Lt => "<",
                FilterOp::Lte => "<=",
                _ => return None,
            };
            Some(format!("{}{op}{quoted}", f.column))
        })
        .collect();
    (!parts.is_empty()).then(|| parts.join(","))
}

// ---------------------------------------------------------------------------
// execute parsing
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum Command {
    Installed { name: String, params: Json },
    Interpreted(String),
    Passthrough { method: String, path: String, body: Option<Json> },
}

fn parse_command(text: &str) -> AppResult<Command> {
    let t = text.trim();
    if t.is_empty() {
        return Err(AppError::invalid_input("Empty query."));
    }
    if t.starts_with('{') {
        let v: Json = serde_json::from_str(t).map_err(|e| AppError::invalid_input(format!("Invalid JSON body: {e}")))?;
        if let Some(name) = v.get("query").and_then(Json::as_str) {
            return Ok(Command::Installed { name: name.to_string(), params: v.get("params").cloned().unwrap_or(json!({})) });
        }
        if let (Some(method), Some(path)) = (v.get("method").and_then(Json::as_str), v.get("path").and_then(Json::as_str)) {
            return Ok(Command::Passthrough { method: method.to_ascii_uppercase(), path: path.to_string(), body: v.get("body").cloned() });
        }
        return Err(AppError::invalid_input("JSON body needs {\"query\",\"params\"} or {\"method\",\"path\",\"body\"}."));
    }
    let upper = t.to_ascii_uppercase();
    if upper.starts_with("INTERPRET QUERY") || upper.starts_with("SELECT") || upper.starts_with("CREATE QUERY") || upper.starts_with("INTERPRET") {
        return Ok(Command::Interpreted(t.to_string()));
    }
    if let Some(rest) = t.strip_prefix("RUN QUERY ").or_else(|| t.strip_prefix("run query ")) {
        let (name, args) = rest.trim().trim_end_matches(')').split_once('(').unwrap_or((rest.trim(), ""));
        let params: Json = if args.trim().is_empty() { json!({}) } else { serde_json::from_str(&format!("{{{args}}}")).unwrap_or(json!({})) };
        return Ok(Command::Installed { name: name.trim().to_string(), params });
    }
    Err(AppError::invalid_input(
        "Use `INTERPRET QUERY () FOR GRAPH g { … }`, `RUN QUERY name(param: value)`, JSON {\"query\",\"params\"} or {\"method\",\"path\",\"body\"}.",
    ))
}

// WHAT:  Installed-query params → `?k=v&k2=v2` (arrays repeat the key).
fn params_query(params: &Json) -> String {
    let Some(obj) = params.as_object() else { return String::new() };
    let mut parts = Vec::new();
    for (k, v) in obj {
        match v {
            Json::Array(items) => {
                for it in items {
                    parts.push(format!("{}={}", pct(k), pct(&scalar_text(it))));
                }
            }
            other => parts.push(format!("{}={}", pct(k), pct(&scalar_text(other)))),
        }
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("?{}", parts.join("&"))
    }
}

fn scalar_text(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// WHAT:  Query results (`[{"alias": …}, …]`) → one StatementResult per result item.
fn query_results_to_statements(results: &Json, max_rows: usize) -> Vec<StatementResult> {
    let Some(items) = results.as_array() else {
        return vec![StatementResult::Rows { result: json_result(results.clone()) }];
    };
    let mut out = Vec::new();
    for item in items {
        let Some(obj) = item.as_object() else {
            out.push(StatementResult::Rows { result: json_result(item.clone()) });
            continue;
        };
        for (alias, val) in obj {
            match val {
                Json::Array(rows) if !rows.is_empty() && rows.iter().all(Json::is_object) => {
                    let flat: Vec<Json> = rows.iter().map(flatten_element).collect();
                    let id = flat.iter().any(|d| d.get("v_id").is_some()).then_some("v_id");
                    out.push(StatementResult::Rows { result: objects_to_result_set(&flat, id, max_rows) });
                }
                Json::Array(rows) => {
                    let truncated = rows.len() > max_rows;
                    let type_name = rows.iter().find(|v| !v.is_null()).map(json_type_name).unwrap_or("json").to_string();
                    out.push(StatementResult::Rows {
                        result: ResultSet {
                            columns: vec![ColumnMeta { name: alias.clone(), type_name }],
                            rows: rows.iter().take(max_rows).map(|v| vec![json_to_value(v)]).collect(),
                            truncated,
                        },
                    });
                }
                other => out.push(StatementResult::Rows {
                    result: ResultSet {
                        columns: vec![ColumnMeta { name: alias.clone(), type_name: json_type_name(other).into() }],
                        rows: vec![vec![json_to_value(other)]],
                        truncated: false,
                    },
                }),
            }
        }
    }
    if out.is_empty() {
        out.push(StatementResult::Rows { result: json_result(results.clone()) });
    }
    out
}

// WHAT:  `{v_id, v_type, attributes:{…}}` → flat object so the grid shows attributes as columns.
fn flatten_element(el: &Json) -> Json {
    let Some(obj) = el.as_object() else { return el.clone() };
    let mut out = serde_json::Map::new();
    for (k, v) in obj {
        if k == "attributes" {
            if let Some(attrs) = v.as_object() {
                for (ak, av) in attrs {
                    out.insert(ak.clone(), av.clone());
                }
                continue;
            }
        }
        out.insert(k.clone(), v.clone());
    }
    Json::Object(out)
}

fn is_mutating_path(method: &str, path: &str) -> bool {
    let p = path.to_ascii_lowercase();
    match method {
        "DELETE" => true,
        "POST" | "PUT" => p.contains("/graph/") || p.contains("/ddl") || p.contains("/gsql") || p.contains("/rebuildnow") || p.contains("/requesttoken"),
        _ => false,
    }
}

impl TigerGraphIntegration {
    async fn schema(&self) -> AppResult<Vec<TypeInfo>> {
        let v: Json = self.gsql.get_json(&format!("/gsqlserver/gsql/schema?graph={}", pct(&self.graph))).await?;
        let results = unwrap_results(v)?;
        let types = parse_schema(&results);
        if types.is_empty() {
            return Err(AppError::not_found(format!("Graph `{}` has no vertex or edge types (or does not exist).", self.graph)));
        }
        Ok(types)
    }

    async fn type_info(&self, table: &TableRef) -> AppResult<TypeInfo> {
        let want_edge = table.schema.as_deref() == Some(EDGE_SCHEMA);
        self.schema()
            .await?
            .into_iter()
            .find(|t| t.name == table.name && (table.schema.is_none() || t.is_edge == want_edge))
            .ok_or_else(|| AppError::not_found(format!("Unknown type `{}`.", table.name)))
    }

    async fn stat(&self, function: &str, type_name: &str) -> AppResult<i64> {
        let body = json!({ "function": function, "type": type_name });
        let v: Json = self.restpp.post_json(&format!("/restpp/builtins/{}", pct(&self.graph)), &body).await?;
        let results = unwrap_results(v)?;
        Ok(results.as_array().and_then(|a| a.first()).and_then(|r| r.get("count")).and_then(Json::as_i64).unwrap_or(0))
    }

    async fn vertices(&self, type_name: &str, limit: usize, filter: Option<&str>) -> AppResult<Vec<Json>> {
        let mut path = format!("/restpp/graph/{}/vertices/{}?limit={limit}", pct(&self.graph), pct(type_name));
        if let Some(f) = filter {
            path.push_str(&format!("&filter={}", pct(f)));
        }
        let v: Json = self.restpp.get_json(&path).await?;
        Ok(unwrap_results(v)?.as_array().cloned().unwrap_or_default())
    }

    // WHAT:  Edges of one type: sample source vertices of every vertex type, then
    //        list their outgoing edges of `edge_type` until `limit` is reached.
    async fn edges(&self, edge_type: &str, limit: usize, types: &[TypeInfo]) -> AppResult<(Vec<Json>, bool)> {
        let mut out = Vec::new();
        for vt in types.iter().filter(|t| !t.is_edge) {
            let sources = self.vertices(&vt.name, EDGE_SOURCE_SAMPLE, None).await.unwrap_or_default();
            for src in sources {
                let Some(id) = src.get("v_id").and_then(Json::as_str) else { continue };
                let path = format!(
                    "/restpp/graph/{}/edges/{}/{}/{}?limit={}",
                    pct(&self.graph),
                    pct(&vt.name),
                    pct(id),
                    pct(edge_type),
                    limit.saturating_sub(out.len()).max(1)
                );
                let v: Json = match self.restpp.get_json(&path).await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Ok(res) = unwrap_results(v) {
                    out.extend(res.as_array().cloned().unwrap_or_default());
                }
                if out.len() >= limit {
                    out.truncate(limit);
                    return Ok((out, true));
                }
            }
        }
        Ok((out, false))
    }

    async fn elements(&self, table: &TableRef, query: &PageQuery, t: &TypeInfo) -> AppResult<(Vec<Json>, bool)> {
        let want = (query.offset as usize).saturating_add(query.limit as usize).clamp(1, MAX_PAGE_ROWS);
        let cap = if query.filters.is_empty() && query.sort.is_empty() { want } else { MAX_PAGE_ROWS };
        if t.is_edge {
            let types = self.schema().await?;
            self.edges(&table.name, cap, &types).await
        } else {
            let filter = server_filter(&query.filters, &t.attributes);
            let rows = self.vertices(&table.name, cap, filter.as_deref()).await?;
            let truncated = rows.len() >= cap;
            Ok((rows, truncated))
        }
    }
}

#[async_trait]
impl Integration for TigerGraphIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { namespaces: true, fixed_columns: true, exact_estimate: true, ..Capabilities::DOCUMENT }
    }

    async fn ping(&self) -> AppResult<()> {
        let req = self.restpp.request(Method::GET, "/restpp/echo");
        if self.restpp.send(req).await.is_ok() {
            return Ok(());
        }
        let req = self.restpp.request(Method::GET, "/api/ping");
        self.restpp.send(req).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let text = match self.restpp.get_text("/restpp/version").await {
            Ok(t) => t,
            Err(_) => return Ok(Some("TigerGraph".into())),
        };
        let version = serde_json::from_str::<Json>(&text)
            .ok()
            .and_then(|v| v.get("message").and_then(Json::as_str).map(str::to_string))
            .unwrap_or(text);
        let line = version.lines().find(|l| l.contains("TigerGraph version") || l.contains("release")).unwrap_or("").trim();
        let short = line.split_whitespace().find(|w| w.chars().next().is_some_and(|c| c.is_ascii_digit())).unwrap_or("");
        Ok(Some(if short.is_empty() { "TigerGraph".into() } else { format!("TigerGraph {short}") }))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.graph.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let v: Json = match self.gsql.get_json("/gsqlserver/gsql/schema").await {
            Ok(v) => v,
            Err(_) => return Ok(vec![self.graph.clone()]),
        };
        let mut names: Vec<String> = unwrap_results(v)
            .ok()
            .and_then(|r| r.get("GraphNames").or_else(|| r.get("graphs")).and_then(Json::as_array).cloned())
            .map(|a| a.iter().filter_map(Json::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        if !names.iter().any(|n| n == &self.graph) {
            names.push(self.graph.clone());
        }
        names.sort();
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let types = self.schema().await?;
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        for t in &types {
            let (schema, function) = if t.is_edge { (EDGE_SCHEMA, "stat_edge_number") } else { (VERTEX_SCHEMA, "stat_vertex_number") };
            let row_estimate = self.stat(function, &t.name).await.ok();
            let info = TableInfo { schema: Some(schema.into()), name: t.name.clone(), kind: TableKind::Table, row_estimate };
            if t.is_edge {
                edges.push(info);
            } else {
                vertices.push(info);
            }
        }
        Ok(SchemaCatalog {
            schemas: vec![SchemaInfo { name: VERTEX_SCHEMA.into(), tables: vertices }, SchemaInfo { name: EDGE_SCHEMA.into(), tables: edges }],
        })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(type_columns(&self.type_info(table).await?))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let t = self.type_info(table).await?;
        let function = if t.is_edge { "stat_edge_number" } else { "stat_vertex_number" };
        self.stat(function, &t.name).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        if filters.is_empty() {
            return self.row_estimate(table).await.map(|c| c.unwrap_or(0));
        }
        let t = self.type_info(table).await?;
        let cols = type_columns(&t);
        let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
        let q = PageQuery { sort: vec![], filters: filters.to_vec(), offset: 0, limit: MAX_PAGE_ROWS as u32 };
        let (els, _) = self.elements(table, &q, &t).await?;
        let rows: Vec<Vec<Value>> = els.iter().map(|e| element_row(&cols, e)).collect();
        Ok(local::apply_filters(&names, rows, filters).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let t = self.type_info(table).await?;
        let cols = type_columns(&t);
        validate_columns(&cols, &query.sort, &query.filters)?;
        let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
        let (els, truncated) = self.elements(table, query, &t).await?;
        let rows: Vec<Vec<Value>> = els.iter().map(|e| element_row(&cols, e)).collect();
        let rows = local::page(&names, rows, query);
        let columns = cols.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect();
        Ok(ResultSet { columns, rows, truncated })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let max_rows = max_rows.max(1);
        match parse_command(sql)? {
            Command::Installed { name, params } => {
                let path = format!("/restpp/query/{}/{}{}", pct(&self.graph), pct(&name), params_query(&params));
                let v: Json = self.restpp.get_json(&path).await?;
                Ok(query_results_to_statements(&unwrap_results(v)?, max_rows))
            }
            Command::Interpreted(text) => {
                let upper = text.trim_start().to_ascii_uppercase();
                if self.read_only && (upper.contains("INSERT INTO") || upper.contains("DELETE ") || upper.contains("UPDATE ")) {
                    return Err(AppError::read_only("This connection is read-only; mutating GSQL is blocked."));
                }
                let path = format!("/gsqlserver/interpreted_query?graph={}", pct(&self.graph));
                let body = self.gsql.post_raw(&path, "text/plain", text, Some("application/json")).await?;
                let v: Json = serde_json::from_str(&body).map_err(|e| AppError::driver(format!("Malformed GSQL response: {e}")))?;
                Ok(query_results_to_statements(&unwrap_results(v)?, max_rows))
            }
            Command::Passthrough { method, path, body } => {
                if self.read_only && is_mutating_path(&method, &path) {
                    return Err(AppError::read_only("This connection is read-only; mutating REST++ requests are blocked."));
                }
                let m = Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Unknown HTTP method `{method}`.")))?;
                let client = if path.starts_with("/gsqlserver") { &self.gsql } else { &self.restpp };
                let mut req = client.request(m, &path);
                if let Some(b) = body {
                    req = req.json(&b);
                }
                let resp = client.send(req).await?;
                let text = resp.text().await.unwrap_or_default();
                let v: Json = serde_json::from_str(&text).unwrap_or(Json::String(text));
                match unwrap_results(v) {
                    Ok(r) => Ok(query_results_to_statements(&r, max_rows)),
                    Err(e) => Err(e),
                }
            }
        }
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gsql_base_derivation() {
        assert_eq!(gsql_base("http://localhost:9000"), "http://localhost:14240");
        assert_eq!(gsql_base("https://tg.example.com"), "https://tg.example.com:14240");
        assert_eq!(gsql_base("https://x.i.tgcloud.io:443"), "https://x.i.tgcloud.io:443");
    }

    #[test]
    fn schema_parses_into_types_and_columns() {
        let schema = json!({
            "VertexTypes": [{"Name": "Person", "Attributes": [
                {"AttributeName": "name", "AttributeType": {"Name": "STRING"}},
                {"AttributeName": "tags", "AttributeType": {"Name": "LIST", "ValueTypeName": "STRING"}}
            ]}],
            "EdgeTypes": [{"Name": "KNOWS", "Attributes": [{"AttributeName": "since", "AttributeType": {"Name": "INT"}}]}]
        });
        let types = parse_schema(&schema);
        assert_eq!(types.len(), 2);
        let v = type_columns(&types[0]);
        let names: Vec<&str> = v.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["v_id", "v_type", "name", "tags"]);
        assert!(v[0].primary_key);
        assert_eq!(v[3].data_type, "list<string>");
        let e = type_columns(&types[1]);
        assert_eq!(e[0].name, "from_id");
        assert!(e[0].primary_key && e[1].primary_key);
        let row = element_row(&v, &json!({"v_id": "p1", "v_type": "Person", "attributes": {"name": "Ann", "tags": ["a"]}}));
        assert_eq!(row[0], Value::Text("p1".into()));
        assert_eq!(row[2], Value::Text("Ann".into()));
        assert!(matches!(row[3], Value::Json(_)));
    }

    #[test]
    fn server_filter_only_simple_comparisons() {
        let attrs = vec![("age".to_string(), "int".to_string()), ("name".to_string(), "string".to_string())];
        let f = server_filter(
            &[
                FilterRule { column: "age".into(), op: FilterOp::Gt, value: "3".into() },
                FilterRule { column: "name".into(), op: FilterOp::Eq, value: "Ann".into() },
                FilterRule { column: "name".into(), op: FilterOp::Contains, value: "x".into() },
                FilterRule { column: "v_id".into(), op: FilterOp::Eq, value: "p1".into() },
            ],
            &attrs,
        );
        assert_eq!(f.as_deref(), Some("age>3,name=\"Ann\""));
        assert_eq!(server_filter(&[], &attrs), None);
    }

    #[test]
    fn commands_parse() {
        assert_eq!(
            parse_command(r#"{"query":"top_users","params":{"k":3}}"#).unwrap(),
            Command::Installed { name: "top_users".into(), params: json!({"k": 3}) }
        );
        assert_eq!(parse_command("RUN QUERY top_users(\"k\": 3)").unwrap(), Command::Installed { name: "top_users".into(), params: json!({"k": 3}) });
        assert!(matches!(parse_command("INTERPRET QUERY () FOR GRAPH g { PRINT 1; }").unwrap(), Command::Interpreted(_)));
        assert_eq!(
            parse_command(r#"{"method":"get","path":"/restpp/endpoints"}"#).unwrap(),
            Command::Passthrough { method: "GET".into(), path: "/restpp/endpoints".into(), body: None }
        );
        assert!(parse_command("nonsense").is_err());
        let q = params_query(&json!({"k": 3, "ids": ["a", "b"]}));
        assert!(q.starts_with('?') && q.contains("ids=a&ids=b") && q.contains("k=3"), "{q}");
        assert!(is_mutating_path("POST", "/restpp/graph/g/vertices"));
        assert!(!is_mutating_path("GET", "/restpp/graph/g/vertices"));
        assert!(is_mutating_path("DELETE", "/restpp/graph/g/vertices/Person/1"));
    }

    #[test]
    fn query_results_become_statements() {
        let results = json!([{ "res": [{"v_id": "1", "v_type": "P", "attributes": {"name": "a"}}] }, { "n": 5 }]);
        let out = query_results_to_statements(&results, 10);
        assert_eq!(out.len(), 2);
        match &out[0] {
            StatementResult::Rows { result } => {
                let names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
                assert_eq!(names, vec!["v_id", "v_type", "name"]);
            }
            _ => panic!("rows"),
        }
        match &out[1] {
            StatementResult::Rows { result } => assert_eq!(result.rows[0][0], Value::Int(5)),
            _ => panic!("rows"),
        }
        assert!(unwrap_results(json!({"error": true, "message": "bad"})).is_err());
        assert_eq!(token_from_response(&json!({"error": false, "token": "abc"})).as_deref(), Some("abc"));
        assert_eq!(token_from_response(&json!({"error": false, "results": {"token": "xyz"}})).as_deref(), Some("xyz"));
    }
}
