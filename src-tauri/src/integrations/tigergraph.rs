// SOT: tigergraph-integration, gsql, tigergraph-rest, restpp, tigergraph-token, tigergraph-object-explorer, tigergraph-server-stats, gsql-show-output

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, json_to_value, json_type_name, local, objects_to_result_set, Auth, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectProperty, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    ServerStats, SslMode, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;
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
    /// The schema entry verbatim, so the object explorer can show the config
    /// and rebuild the CREATE statement without a second request.
    raw: Json,
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
            out.push(TypeInfo { name: name.to_string(), attributes, is_edge, raw: t.clone() });
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

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Graphs, vertex types (Label), edge types (RelationshipType), installed
//        queries (Procedure), users and roles, plus the stats tab, from the GSQL
//        server's catalog endpoints and REST++ builtins.
// WHY:   TigerGraph exposes the same facts three ways (JSON endpoints, the
//        `SHOW …` text output of the GSQL shell, and REST++ builtins) and which
//        ones answer depends on the version and the user's privileges, so every
//        listing has a JSON path and a text fallback, and 401/403 becomes a
//        readable hint instead of a dead sidebar.
// HOW:   Actions go through `execute`, so they are written in the languages it
//        accepts: `RUN QUERY name(...)` for installed queries and
//        `{"method","path","body"}` REST++ passthroughs for everything else
//        (`is_mutating_path` already refuses the destructive ones read-only).
// ---------------------------------------------------------------------------

const LIST_CAP: usize = 2_000;

fn jstr(row: &Json, key: &str) -> Option<String> {
    row.get(key).filter(|v| !v.is_null()).map(|v| match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    })
}

fn preview(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        format!("{}…", flat.chars().take(max).collect::<String>())
    }
}

fn finish(mut out: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    out.truncate(LIST_CAP);
    out
}

fn config_properties(raw: &Json) -> Vec<ObjectProperty> {
    let Some(cfg) = raw.get("Config").and_then(Json::as_object) else { return Vec::new() };
    let mut keys: Vec<&String> = cfg.keys().collect();
    keys.sort();
    keys.into_iter()
        .filter_map(|k| {
            let v = cfg.get(k)?;
            let text = match v {
                Json::String(s) => s.clone(),
                other => other.to_string(),
            };
            (!text.is_empty() && text != "null").then(|| ObjectProperty { name: k.clone(), value: preview(&text, 200) })
        })
        .collect()
}

// WHAT:  `{"From": "Person", "To": "Post"}` pairs, or the single
//        FromVertexTypeName / ToVertexTypeName pair on older schemas.
fn edge_pairs(raw: &Json) -> Vec<(String, String)> {
    let pairs: Vec<(String, String)> = raw
        .get("EdgePairs")
        .and_then(Json::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|p| Some((p.get("From")?.as_str()?.to_string(), p.get("To")?.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();
    if !pairs.is_empty() {
        return pairs;
    }
    match (jstr(raw, "FromVertexTypeName"), jstr(raw, "ToVertexTypeName")) {
        (Some(f), Some(t)) => vec![(f, t)],
        _ => Vec::new(),
    }
}

// WHAT:  The CREATE statement for a vertex / edge type, rebuilt from the schema
//        JSON (TigerGraph's REST catalog has no DDL text of its own).
fn type_ddl(t: &TypeInfo) -> String {
    let attrs: Vec<String> = t.attributes.iter().map(|(n, ty)| format!("{n} {}", ty.to_uppercase())).collect();
    let with = |raw: &Json, keys: &[&str]| -> String {
        let cfg = raw.get("Config");
        let parts: Vec<String> = keys
            .iter()
            .filter_map(|k| cfg.and_then(|c| jstr(c, k)).map(|v| format!("{k}=\"{v}\"")))
            .collect();
        if parts.is_empty() {
            String::new()
        } else {
            format!(" WITH {}", parts.join(", "))
        }
    };
    if t.is_edge {
        let directed = t.raw.get("IsDirected").and_then(Json::as_bool).unwrap_or(false);
        let pairs: Vec<String> = edge_pairs(&t.raw).into_iter().map(|(f, to)| format!("FROM {f}, TO {to}")).collect();
        let body: Vec<String> = pairs.into_iter().chain(attrs).collect();
        format!(
            "CREATE {} EDGE {} ({}){}",
            if directed { "DIRECTED" } else { "UNDIRECTED" },
            t.name,
            body.join(", "),
            with(&t.raw, &["REVERSE_EDGE"])
        )
    } else {
        let pk = t.raw.get("PrimaryId").map(|p| {
            let name = jstr(p, "AttributeName").unwrap_or_else(|| "id".into());
            format!("PRIMARY_ID {name} {}", attr_type_name(p).to_uppercase())
        });
        let body: Vec<String> = pk.into_iter().chain(attrs).collect();
        format!("CREATE VERTEX {} ({}){}", t.name, body.join(", "), with(&t.raw, &["STATS", "PRIMARY_ID_AS_ATTRIBUTE"]))
    }
}

// ---- graphs ---------------------------------------------------------------------

fn graph_summaries(names: &[String], current: &str) -> Vec<ObjectSummary> {
    finish(
        names
            .iter()
            .map(|n| {
                let mut s = ObjectSummary::new(ObjectKind::Graph, n.clone(), None);
                if n == current {
                    s = s.with_badge("current");
                }
                s
            })
            .collect(),
    )
}

fn graph_detail(reference: &ObjectRef, types: &[TypeInfo], schema: &Json) -> ObjectDetail {
    let vertices = types.iter().filter(|t| !t.is_edge).count();
    let edges = types.len() - vertices;
    let mut d = ObjectDetail::empty(reference)
        .definition(serde_json::to_string_pretty(schema).unwrap_or_default(), CodeLanguage::Json)
        .property("vertex types", vertices.to_string())
        .property("edge types", edges.to_string());
    if let Some(v) = jstr(schema, "VertexCount") {
        d = d.property("vertices", v);
    }
    d.children = types
        .iter()
        .map(|t| {
            let kind = if t.is_edge { ObjectKind::RelationshipType } else { ObjectKind::Label };
            ObjectSummary::new(kind, t.name.clone(), Some(reference.name.clone()))
                .with_detail(format!("{} attribute(s)", t.attributes.len()))
                .with_badge(if t.is_edge { "edge" } else { "vertex" })
        })
        .collect();
    d.action(ObjectAction::new("schema", "Show schema", json!({ "method": "GET", "path": format!("/gsqlserver/gsql/schema?graph={}", pct(&reference.name)) }).to_string()))
}

// ---- vertex / edge types ----------------------------------------------------------

// WHAT:  `stat_vertex_number` / `stat_edge_number` with type `*` → name → count.
fn counts_from_builtin(results: &Json) -> BTreeMap<String, i64> {
    results
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|r| {
                    let name = jstr(r, "v_type").or_else(|| jstr(r, "e_type"))?;
                    Some((name, r.get("count").and_then(Json::as_i64).unwrap_or(0)))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn type_summaries(kind: ObjectKind, types: &[TypeInfo], graph: &str, counts: &BTreeMap<String, i64>) -> Vec<ObjectSummary> {
    let want_edge = kind == ObjectKind::RelationshipType;
    finish(
        types
            .iter()
            .filter(|t| t.is_edge == want_edge)
            .map(|t| {
                let mut parts = Vec::new();
                if let Some(c) = counts.get(&t.name) {
                    parts.push(format!("{} {}", format_number(*c as f64), if want_edge { "edges" } else { "vertices" }));
                }
                parts.push(format!("{} attribute(s)", t.attributes.len()));
                if want_edge {
                    let pairs: Vec<String> = edge_pairs(&t.raw).into_iter().map(|(f, to)| format!("{f}→{to}")).collect();
                    if !pairs.is_empty() {
                        parts.push(pairs.join(", "));
                    }
                }
                let badge = if want_edge {
                    Some(if t.raw.get("IsDirected").and_then(Json::as_bool).unwrap_or(false) { "directed".to_string() } else { "undirected".to_string() })
                } else {
                    t.raw.get("PrimaryId").and_then(|p| jstr(p, "AttributeName"))
                };
                ObjectSummary { reference: ObjectRef { kind, name: t.name.clone(), parent: Some(graph.to_string()) }, detail: Some(parts.join(" · ")), badge }
            })
            .collect(),
    )
}

fn type_detail(reference: &ObjectRef, t: &TypeInfo, graph: &str, count: Option<i64>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(type_ddl(t), CodeLanguage::Sql);
    if let Some(c) = count {
        d = d.property(if t.is_edge { "edges" } else { "vertices" }, format_number(c as f64));
    }
    if t.is_edge {
        let pairs: Vec<String> = edge_pairs(&t.raw).into_iter().map(|(f, to)| format!("{f} → {to}")).collect();
        if !pairs.is_empty() {
            d = d.property("pairs", pairs.join(", "));
        }
        d = d.property("directed", t.raw.get("IsDirected").and_then(Json::as_bool).unwrap_or(false).to_string());
    } else if let Some(pk) = t.raw.get("PrimaryId").and_then(|p| jstr(p, "AttributeName")) {
        d = d.property("primary id", pk);
    }
    for p in config_properties(&t.raw) {
        d = d.property(&p.name, p.value);
    }
    d.columns = type_columns(t);
    let function = if t.is_edge { "stat_edge_number" } else { "stat_vertex_number" };
    let count_action = json!({ "method": "POST", "path": format!("/restpp/builtins/{}", pct(graph)), "body": { "function": function, "type": t.name } });
    d = d.action(ObjectAction::new("count", "Count", count_action.to_string()));
    if t.is_edge {
        return d;
    }
    let path = format!("/restpp/graph/{}/vertices/{}", pct(graph), pct(&t.name));
    d.action(ObjectAction::new("sample", "Sample 20", json!({ "method": "GET", "path": format!("{path}?limit=20") }).to_string()))
        .action(ObjectAction::destructive("delete-all", "Delete all vertices", json!({ "method": "DELETE", "path": path }).to_string()))
}

// ---- installed queries ---------------------------------------------------------------

// WHAT:  `/restpp/endpoints/{graph}` is keyed `"GET /query/{graph}/{name}"` →
//        `{parameters: {…}}`; keep the ones belonging to this graph.
fn installed_queries(endpoints: &Json, graph: &str) -> Vec<(String, Json)> {
    let Some(obj) = endpoints.as_object() else { return Vec::new() };
    let needle = format!("/query/{graph}/");
    let mut out: Vec<(String, Json)> = obj
        .iter()
        .filter_map(|(key, spec)| {
            let path = key.split_whitespace().next_back()?;
            let name = path.split(&needle).nth(1)?;
            let name = name.split('/').next().filter(|n| !n.is_empty())?;
            Some((name.to_string(), spec.clone()))
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.dedup_by(|a, b| a.0 == b.0);
    out
}

fn query_parameters(spec: &Json) -> Vec<(String, String)> {
    let Some(params) = spec.get("parameters").and_then(Json::as_object) else { return Vec::new() };
    let mut out: Vec<(String, String)> = params
        .iter()
        .filter(|(k, _)| k.as_str() != "query")
        .map(|(k, v)| {
            let ty = jstr(v, "type").or_else(|| v.as_str().map(str::to_string)).unwrap_or_else(|| "STRING".into());
            (k.clone(), ty)
        })
        .collect();
    out.sort();
    out
}

fn query_summaries(queries: &[(String, Json)], graph: &str) -> Vec<ObjectSummary> {
    finish(
        queries
            .iter()
            .map(|(name, spec)| {
                let params = query_parameters(spec);
                let signature = format!("{name}({})", params.iter().map(|(n, t)| format!("{n}: {t}")).collect::<Vec<_>>().join(", "));
                ObjectSummary::new(ObjectKind::Procedure, name.clone(), Some(graph.to_string())).with_detail(signature).with_badge("installed")
            })
            .collect(),
    )
}

fn query_detail(reference: &ObjectRef, spec: &Json, graph: &str, text: Option<String>) -> ObjectDetail {
    let params = query_parameters(spec);
    let mut d = ObjectDetail::empty(reference);
    d = match text {
        Some(gsql) => d.definition(gsql, CodeLanguage::Sql),
        None => d.definition(
            format!("{}({})", reference.name, params.iter().map(|(n, t)| format!("{n}: {t}")).collect::<Vec<_>>().join(", ")),
            CodeLanguage::Text,
        ),
    };
    d = d.property("graph", graph).property("parameters", params.len().to_string());
    if !params.is_empty() {
        d.rows = Some(ResultSet {
            columns: vec![ColumnMeta { name: "parameter".into(), type_name: "string".into() }, ColumnMeta { name: "type".into(), type_name: "string".into() }],
            rows: params.iter().map(|(n, t)| vec![Value::Text(n.clone()), Value::Text(t.clone())]).collect(),
            truncated: false,
        });
    }
    let args = params.iter().map(|(n, _)| format!("\"{n}\": ")).collect::<Vec<_>>().join(", ");
    d.action(ObjectAction::new("run", "Run query", format!("RUN QUERY {}({args})", reference.name)))
        .action(ObjectAction::new("endpoint", "Show endpoint", json!({ "method": "GET", "path": format!("/restpp/endpoints/{}?dynamic=true", pct(graph)) }).to_string()))
}

// ---- users / roles -----------------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct GsqlUser {
    name: String,
    /// `(role, graph)`; an empty graph is a global role.
    roles: Vec<(String, String)>,
    superuser: bool,
}

fn role_pairs(value: &Json) -> Vec<(String, String)> {
    match value {
        Json::Array(items) => items
            .iter()
            .flat_map(|r| match r {
                Json::String(s) => vec![(s.clone(), String::new())],
                other => {
                    let graph = jstr(other, "graph").or_else(|| jstr(other, "graphName")).unwrap_or_default();
                    match other.get("roles").or_else(|| other.get("name")).or_else(|| other.get("role")) {
                        Some(Json::Array(names)) => names.iter().filter_map(Json::as_str).map(|n| (n.to_string(), graph.clone())).collect(),
                        Some(Json::String(n)) => vec![(n.clone(), graph)],
                        _ => Vec::new(),
                    }
                }
            })
            .collect(),
        Json::String(s) => s.split(',').map(str::trim).filter(|s| !s.is_empty()).map(|s| (s.to_string(), String::new())).collect(),
        Json::Object(map) => map
            .iter()
            .flat_map(|(graph, names)| match names {
                Json::Array(items) => items.iter().filter_map(Json::as_str).map(|n| (n.to_string(), graph.clone())).collect::<Vec<_>>(),
                Json::String(n) => vec![(n.clone(), graph.clone())],
                _ => Vec::new(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

// WHAT:  `GET /gsqlserver/gsql/users` → users, whichever of the shapes this
//        version returns (`results` as a list, or `{users: […]}`).
fn parse_users_json(results: &Json) -> Vec<GsqlUser> {
    let items: Vec<Json> = match results {
        Json::Array(a) => a.clone(),
        Json::Object(_) => results.get("users").and_then(Json::as_array).cloned().unwrap_or_default(),
        _ => Vec::new(),
    };
    items
        .iter()
        .filter_map(|u| {
            let name = jstr(u, "name").or_else(|| jstr(u, "Name")).or_else(|| jstr(u, "username")).filter(|n| !n.is_empty())?;
            let roles = u
                .get("roles")
                .or_else(|| u.get("Roles"))
                .or_else(|| u.get("globalRoles"))
                .map(role_pairs)
                .unwrap_or_default();
            let extra = u.get("graphRoles").map(role_pairs).unwrap_or_default();
            let superuser = u.get("isSuperUser").and_then(Json::as_bool).unwrap_or(false) || roles.iter().chain(&extra).any(|(r, _)| r == "superuser");
            Some(GsqlUser { name, roles: roles.into_iter().chain(extra).collect(), superuser })
        })
        .collect()
}

// WHAT:  The GSQL shell's `SHOW USER` text:
//          - Name: tigergraph
//            - GlobalRoles: superuser
//            - GraphName: social
//              - LocalRoles: designer
fn parse_users_text(text: &str) -> Vec<GsqlUser> {
    let mut out: Vec<GsqlUser> = Vec::new();
    let mut graph = String::new();
    for line in text.lines() {
        let t = line.trim().trim_start_matches('-').trim();
        let Some((key, value)) = t.split_once(':') else { continue };
        let key = key.trim().to_ascii_lowercase().replace(' ', "");
        let value = value.trim();
        match key.as_str() {
            "name" | "username" if !value.is_empty() => {
                graph.clear();
                out.push(GsqlUser { name: value.to_string(), ..GsqlUser::default() });
            }
            "graphname" | "graph" => graph = value.to_string(),
            k if k.contains("role") => {
                if let Some(user) = out.last_mut() {
                    let scope = if k.starts_with("global") { String::new() } else { graph.clone() };
                    for role in value.split(',').map(str::trim).filter(|r| !r.is_empty()) {
                        user.roles.push((role.to_string(), scope.clone()));
                        user.superuser = user.superuser || role == "superuser";
                    }
                }
            }
            _ => {}
        }
    }
    out
}

fn role_text(roles: &[(String, String)]) -> String {
    roles
        .iter()
        .map(|(r, g)| if g.is_empty() { r.clone() } else { format!("{r}@{g}") })
        .collect::<Vec<_>>()
        .join(", ")
}

fn user_summaries(users: &[GsqlUser]) -> Vec<ObjectSummary> {
    finish(
        users
            .iter()
            .map(|u| {
                let mut s = ObjectSummary::new(ObjectKind::User, u.name.clone(), None);
                if !u.roles.is_empty() {
                    s = s.with_detail(role_text(&u.roles));
                }
                if u.superuser {
                    s = s.with_badge("superuser");
                }
                s
            })
            .collect(),
    )
}

fn user_detail(reference: &ObjectRef, user: &GsqlUser) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).property("superuser", user.superuser.to_string()).property("roles", user.roles.len().to_string());
    d.rows = Some(ResultSet {
        columns: vec![ColumnMeta { name: "role".into(), type_name: "string".into() }, ColumnMeta { name: "graph".into(), type_name: "string".into() }],
        rows: user
            .roles
            .iter()
            .map(|(r, g)| vec![Value::Text(r.clone()), Value::Text(if g.is_empty() { "(global)".into() } else { g.clone() })])
            .collect(),
        truncated: false,
    });
    d
}

// WHAT:  Roles as the server reports them, else the distinct roles the users
//        actually hold (badge says which, so the list is never a guess in disguise).
fn role_summaries(declared: &[Json], users: &[GsqlUser]) -> Vec<ObjectSummary> {
    let listed: Vec<ObjectSummary> = declared
        .iter()
        .filter_map(|r| {
            let name = jstr(r, "name").or_else(|| jstr(r, "Name")).or_else(|| r.as_str().map(str::to_string)).filter(|n| !n.is_empty())?;
            let privileges = r.get("privileges").and_then(Json::as_array).map(Vec::len);
            let mut s = ObjectSummary::new(ObjectKind::Role, name, None);
            if let Some(p) = privileges {
                s = s.with_detail(format!("{p} privilege(s)"));
            }
            s.badge = Some(jstr(r, "graph").filter(|g| !g.is_empty()).unwrap_or_else(|| "defined".to_string()));
            Some(s)
        })
        .collect();
    if !listed.is_empty() {
        return finish(listed);
    }
    let mut holders: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for u in users {
        for (role, graph) in &u.roles {
            let entry = holders.entry(role.clone()).or_default();
            let who = if graph.is_empty() { u.name.clone() } else { format!("{}@{graph}", u.name) };
            entry.push(who);
        }
    }
    finish(
        holders
            .into_iter()
            .map(|(role, who)| {
                ObjectSummary::new(ObjectKind::Role, role, None)
                    .with_detail(format!("held by {}", who.join(", ")))
                    .with_badge("in use")
            })
            .collect(),
    )
}

fn role_detail(reference: &ObjectRef, declared: Option<&Json>, users: &[GsqlUser]) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(r) = declared {
        d = d.definition(serde_json::to_string_pretty(r).unwrap_or_default(), CodeLanguage::Json);
    }
    let members: Vec<Vec<Value>> = users
        .iter()
        .flat_map(|u| {
            u.roles
                .iter()
                .filter(|(role, _)| role == &reference.name)
                .map(|(_, graph)| vec![Value::Text(u.name.clone()), Value::Text(if graph.is_empty() { "(global)".into() } else { graph.clone() })])
                .collect::<Vec<_>>()
        })
        .collect();
    d = d.property("members", members.len().to_string());
    d.rows = Some(ResultSet {
        columns: vec![ColumnMeta { name: "user".into(), type_name: "string".into() }, ColumnMeta { name: "graph".into(), type_name: "string".into() }],
        rows: members,
        truncated: false,
    });
    d
}

// ---- stats ---------------------------------------------------------------------------------

// WHAT:  `/restpp/statistics?seconds=n` returns per-endpoint request records;
//        sum the numeric fields so the tab shows real totals for the window.
fn request_stats(results: &Json, seconds: u32) -> Vec<Stat> {
    let Some(items) = results.as_array() else { return Vec::new() };
    let mut totals: BTreeMap<String, f64> = BTreeMap::new();
    for item in items {
        let Some(obj) = item.as_object() else { continue };
        for (k, v) in obj {
            if let Some(n) = v.as_f64() {
                *totals.entry(k.clone()).or_insert(0.0) += n;
            }
        }
    }
    totals
        .into_iter()
        .take(12)
        .map(|(k, v)| {
            let label = k.replace('_', " ");
            Stat::number(&label, (v * 100.0).round() / 100.0, None).with_hint(format!("last {seconds}s"))
        })
        .collect()
}

fn stat_groups(version: Option<&str>, graph: &str, types: &[TypeInfo], vertices: &BTreeMap<String, i64>, edges: &BTreeMap<String, i64>, requests: Vec<Stat>) -> Vec<StatGroup> {
    let mut server = vec![Stat::text("Version", version.unwrap_or("TigerGraph")), Stat::text("Graph", graph)];
    let vertex_types = types.iter().filter(|t| !t.is_edge).count();
    server.push(Stat::number("Vertex types", vertex_types as f64, None));
    server.push(Stat::number("Edge types", (types.len() - vertex_types) as f64, None));
    let mut groups = vec![StatGroup { title: "Server".into(), stats: server }];
    let total = |m: &BTreeMap<String, i64>| m.values().sum::<i64>() as f64;
    let mut graph_stats = vec![Stat::number("Vertices", total(vertices), None), Stat::number("Edges", total(edges), None)];
    if let Some((name, count)) = vertices.iter().max_by_key(|(_, c)| **c) {
        graph_stats.push(Stat::number("Largest vertex type", *count as f64, None).with_hint(name.clone()));
    }
    if let Some((name, count)) = edges.iter().max_by_key(|(_, c)| **c) {
        graph_stats.push(Stat::number("Largest edge type", *count as f64, None).with_hint(name.clone()));
    }
    groups.push(StatGroup { title: "Graph".into(), stats: graph_stats });
    if !requests.is_empty() {
        groups.push(StatGroup { title: "Throughput".into(), stats: requests });
    }
    groups
}

impl TigerGraphIntegration {
    // WHAT:  A GSQL-server catalog endpoint; 401/403 becomes a readable hint
    //        because these need a GSQL login, not just a REST++ token.
    async fn gsql_json(&self, path: &str) -> AppResult<Json> {
        match self.gsql.get_json::<Json>(path).await {
            Ok(v) => unwrap_results(v),
            Err(AppError::NotConnected { message }) => Err(AppError::not_connected(format!(
                "{message} — this list comes from the GSQL server (port {GSQL_PORT}); connect with a GSQL username and password to see it."
            ))),
            Err(e) => Err(e),
        }
    }

    // WHAT:  `SHOW USER` / `SHOW ROLE` through the GSQL file endpoint, which
    //        takes shell commands as plain text and answers with their output.
    async fn gsql_show(&self, command: &str) -> Option<String> {
        let body = format!("USE GRAPH {}\n{command}\n", self.graph);
        self.gsql.post_raw("/gsqlserver/gsql/file", "text/plain", body, Some("text/plain")).await.ok()
    }

    async fn type_counts(&self, function: &str) -> BTreeMap<String, i64> {
        let body = json!({ "function": function, "type": "*" });
        match self.restpp.post_json::<Json>(&format!("/restpp/builtins/{}", pct(&self.graph)), &body).await {
            Ok(v) => unwrap_results(v).map(|r| counts_from_builtin(&r)).unwrap_or_default(),
            Err(_) => BTreeMap::new(),
        }
    }

    async fn endpoints(&self) -> AppResult<Vec<(String, Json)>> {
        let path = format!("/restpp/endpoints/{}?dynamic=true", pct(&self.graph));
        let v: Json = match self.restpp.get_json(&path).await {
            Ok(v) => v,
            Err(_) => self.restpp.get_json("/restpp/endpoints?dynamic=true").await?,
        };
        let body = if v.get("results").is_some() { unwrap_results(v)? } else { v };
        Ok(installed_queries(&body, &self.graph))
    }

    async fn users(&self) -> AppResult<Vec<GsqlUser>> {
        match self.gsql_json("/gsqlserver/gsql/users").await {
            Ok(results) => {
                let users = parse_users_json(&results);
                if !users.is_empty() {
                    return Ok(users);
                }
                Ok(self.gsql_show("SHOW USER").await.map(|t| parse_users_text(&t)).unwrap_or_default())
            }
            Err(e) => match self.gsql_show("SHOW USER").await {
                Some(text) => Ok(parse_users_text(&text)),
                None => Err(e),
            },
        }
    }

    async fn roles(&self) -> Vec<Json> {
        match self.gsql_json(&format!("/gsqlserver/gsql/roles?graph={}", pct(&self.graph))).await {
            Ok(Json::Array(a)) => a,
            Ok(other) => other.get("roles").and_then(Json::as_array).cloned().unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }

    async fn query_text(&self, name: &str) -> Option<String> {
        let text = self.gsql_show(&format!("SHOW QUERY {name}")).await?;
        let trimmed = text.trim();
        (trimmed.contains("CREATE QUERY") || trimmed.contains("CREATE DISTRIBUTED QUERY")).then(|| trimmed.to_string())
    }

    async fn explorer_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let graph = parent.map(str::trim).filter(|g| !g.is_empty()).unwrap_or(&self.graph);
        match kind {
            ObjectKind::Graph => Ok(graph_summaries(&self.databases().await?, &self.graph)),
            ObjectKind::Label | ObjectKind::RelationshipType => {
                let types = self.schema().await?;
                let counts = self.type_counts(if kind == ObjectKind::RelationshipType { "stat_edge_number" } else { "stat_vertex_number" }).await;
                Ok(type_summaries(kind, &types, graph, &counts))
            }
            ObjectKind::Procedure => Ok(query_summaries(&self.endpoints().await?, graph)),
            ObjectKind::User => Ok(user_summaries(&self.users().await?)),
            ObjectKind::Role => {
                let users = self.users().await.unwrap_or_default();
                Ok(role_summaries(&self.roles().await, &users))
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn explorer_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let graph = reference.parent.as_deref().map(str::trim).filter(|g| !g.is_empty()).unwrap_or(&self.graph);
        let missing = || AppError::not_found(format!("`{name}` is not in graph `{graph}`."));
        match reference.kind {
            ObjectKind::Graph => {
                let v: Json = self.gsql.get_json(&format!("/gsqlserver/gsql/schema?graph={}", pct(name))).await?;
                let schema = unwrap_results(v)?;
                Ok(graph_detail(reference, &parse_schema(&schema), &schema))
            }
            ObjectKind::Label | ObjectKind::RelationshipType => {
                let is_edge = reference.kind == ObjectKind::RelationshipType;
                let types = self.schema().await?;
                let t = types.iter().find(|t| t.name == name && t.is_edge == is_edge).ok_or_else(missing)?;
                let count = self.stat(if is_edge { "stat_edge_number" } else { "stat_vertex_number" }, name).await.ok();
                Ok(type_detail(reference, t, graph, count))
            }
            ObjectKind::Procedure => {
                let queries = self.endpoints().await?;
                let spec = queries.iter().find(|(n, _)| n == name).map(|(_, s)| s.clone()).ok_or_else(missing)?;
                Ok(query_detail(reference, &spec, graph, self.query_text(name).await))
            }
            ObjectKind::User => {
                let users = self.users().await?;
                let user = users.iter().find(|u| u.name == name).ok_or_else(|| AppError::not_found(format!("No user named `{name}`.")))?;
                Ok(user_detail(reference, user))
            }
            ObjectKind::Role => {
                let users = self.users().await.unwrap_or_default();
                let roles = self.roles().await;
                let declared = roles.iter().find(|r| jstr(r, "name").as_deref() == Some(name) || r.as_str() == Some(name));
                Ok(role_detail(reference, declared, &users))
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn explorer_stats(&self) -> AppResult<ServerStats> {
        let version = self.server_version().await.unwrap_or(None);
        let types = self.schema().await.unwrap_or_default();
        let vertices = self.type_counts("stat_vertex_number").await;
        let edges = self.type_counts("stat_edge_number").await;
        let seconds = 60;
        let requests = match self.restpp.get_json::<Json>(&format!("/restpp/statistics?seconds={seconds}")).await {
            Ok(v) => unwrap_results(v).map(|r| request_stats(&r, seconds)).unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        Ok(ServerStats::now(stat_groups(version.as_deref(), &self.graph, &types, &vertices, &edges, requests)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { namespaces: true, fixed_columns: true, exact_estimate: true, ..Capabilities::DOCUMENT },
        object_kinds: vec![K::Graph, K::Label, K::RelationshipType, K::Procedure, K::User, K::Role],
        tools: vec![T::Stats, T::GraphView],
    }
}

#[async_trait]
impl Integration for TigerGraphIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
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

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        self.explorer_objects(kind, parent).await
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        self.explorer_detail(reference).await
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        self.explorer_stats().await
    }
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

    fn sample_schema() -> Json {
        json!({
            "VertexTypes": [{
                "Name": "Person",
                "PrimaryId": {"AttributeName": "id", "AttributeType": {"Name": "STRING"}},
                "Attributes": [{"AttributeName": "name", "AttributeType": {"Name": "STRING"}}, {"AttributeName": "age", "AttributeType": {"Name": "INT"}}],
                "Config": {"STATS": "OUTDEGREE_BY_EDGETYPE", "PRIMARY_ID_AS_ATTRIBUTE": "true"}
            }],
            "EdgeTypes": [{
                "Name": "KNOWS",
                "IsDirected": true,
                "EdgePairs": [{"From": "Person", "To": "Person"}],
                "Attributes": [{"AttributeName": "since", "AttributeType": {"Name": "INT"}}],
                "Config": {"REVERSE_EDGE": "KNOWS_REVERSE"}
            }]
        })
    }

    #[test]
    fn graph_and_type_objects_map() {
        let types = parse_schema(&sample_schema());
        let g = graph_summaries(&["social".into(), "work".into()], "social");
        assert_eq!(g[0].badge.as_deref(), Some("current"));
        assert!(g[1].badge.is_none());
        let d = graph_detail(&g[0].reference, &types, &sample_schema());
        assert!(d.properties.iter().any(|p| p.name == "vertex types" && p.value == "1"));
        assert_eq!(d.children.len(), 2);
        assert_eq!(d.children[1].reference.kind, ObjectKind::RelationshipType);
        assert_eq!(d.children[1].reference.parent.as_deref(), Some("social"));
        assert!(d.actions[0].statement.contains("/gsqlserver/gsql/schema?graph=social"));

        let mut counts = BTreeMap::new();
        counts.insert("Person".to_string(), 1500);
        let v = type_summaries(ObjectKind::Label, &types, "social", &counts);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].detail.as_deref(), Some("1,500 vertices · 2 attribute(s)"));
        assert_eq!(v[0].badge.as_deref(), Some("id"));
        let e = type_summaries(ObjectKind::RelationshipType, &types, "social", &BTreeMap::new());
        assert_eq!(e[0].detail.as_deref(), Some("1 attribute(s) · Person→Person"));
        assert_eq!(e[0].badge.as_deref(), Some("directed"));

        assert_eq!(
            type_ddl(&types[0]),
            "CREATE VERTEX Person (PRIMARY_ID id STRING, name STRING, age INT) WITH STATS=\"OUTDEGREE_BY_EDGETYPE\", PRIMARY_ID_AS_ATTRIBUTE=\"true\""
        );
        assert_eq!(type_ddl(&types[1]), "CREATE DIRECTED EDGE KNOWS (FROM Person, TO Person, since INT) WITH REVERSE_EDGE=\"KNOWS_REVERSE\"");

        let d = type_detail(&v[0].reference, &types[0], "social", Some(1500));
        assert_eq!(d.language, CodeLanguage::Sql);
        assert_eq!(d.columns.len(), 4);
        assert!(d.properties.iter().any(|p| p.name == "primary id" && p.value == "id"));
        assert_eq!(d.actions.len(), 3);
        let del = &d.actions[2];
        assert!(del.destructive);
        assert!(is_mutating_path("DELETE", "/restpp/graph/social/vertices/Person"));
        assert!(matches!(parse_command(&del.statement).unwrap(), Command::Passthrough { .. }));
        assert!(matches!(parse_command(&d.actions[0].statement).unwrap(), Command::Passthrough { .. }));
        let ed = type_detail(&e[0].reference, &types[1], "social", None);
        assert_eq!(ed.actions.len(), 1);
        assert!(ed.properties.iter().any(|p| p.name == "pairs" && p.value == "Person → Person"));

        let counts = counts_from_builtin(&json!([{"v_type": "Person", "count": 3}, {"e_type": "KNOWS", "count": 4}]));
        assert_eq!(counts.get("Person"), Some(&3));
        assert_eq!(counts.get("KNOWS"), Some(&4));
    }

    #[test]
    fn installed_queries_map() {
        let endpoints = json!({
            "GET /query/social/top_users": {"parameters": {"k": {"type": "INT64"}, "query": {"type": "STRING"}}},
            "POST /query/social/add_user": {"parameters": {}},
            "GET /query/other/elsewhere": {"parameters": {}},
            "GET /graph/social": {"parameters": {}}
        });
        let queries = installed_queries(&endpoints, "social");
        assert_eq!(queries.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), vec!["add_user", "top_users"]);
        let s = query_summaries(&queries, "social");
        assert_eq!(s[1].detail.as_deref(), Some("top_users(k: INT64)"));
        assert_eq!(s[1].badge.as_deref(), Some("installed"));
        assert_eq!(s[1].reference.parent.as_deref(), Some("social"));
        let d = query_detail(&s[1].reference, &queries[1].1, "social", None);
        assert_eq!(d.rows.as_ref().map(|r| r.rows.len()), Some(1));
        assert_eq!(d.actions[0].statement, "RUN QUERY top_users(\"k\": )");
        assert!(matches!(parse_command("RUN QUERY top_users(\"k\": 3)").unwrap(), Command::Installed { .. }));
        let with_text = query_detail(&s[1].reference, &queries[1].1, "social", Some("CREATE QUERY top_users(INT k) FOR GRAPH social { PRINT k; }".into()));
        assert_eq!(with_text.language, CodeLanguage::Sql);
    }

    #[test]
    fn users_and_roles_parse_json_and_text() {
        let json_users = parse_users_json(&json!([
            {"name": "tigergraph", "isSuperUser": true, "roles": ["superuser"]},
            {"name": "ann", "roles": [{"graph": "social", "roles": ["designer", "queryreader"]}]}
        ]));
        assert_eq!(json_users.len(), 2);
        assert!(json_users[0].superuser);
        assert_eq!(json_users[1].roles, vec![("designer".to_string(), "social".to_string()), ("queryreader".to_string(), "social".to_string())]);
        assert_eq!(parse_users_json(&json!({"users": [{"Name": "bob", "Roles": "querywriter"}]}))[0].roles, vec![("querywriter".to_string(), String::new())]);

        let text = "--- Users ---\n- Name: tigergraph\n  - GlobalRoles: superuser\n- Name: ann\n  - GraphName: social\n    - LocalRoles: designer, queryreader\n";
        let users = parse_users_text(text);
        assert_eq!(users.len(), 2);
        assert!(users[0].superuser);
        assert_eq!(users[1].name, "ann");
        assert_eq!(users[1].roles, vec![("designer".to_string(), "social".to_string()), ("queryreader".to_string(), "social".to_string())]);

        let s = user_summaries(&users);
        assert_eq!(s[0].reference.name, "ann");
        assert_eq!(s[0].detail.as_deref(), Some("designer@social, queryreader@social"));
        assert_eq!(s[1].badge.as_deref(), Some("superuser"));
        let d = user_detail(&s[0].reference, &users[1]);
        assert_eq!(d.rows.as_ref().map(|r| r.rows.len()), Some(2));

        let declared = role_summaries(&[json!({"name": "designer", "privileges": ["READ_QUERY", "WRITE_QUERY"], "graph": "social"})], &users);
        assert_eq!(declared[0].detail.as_deref(), Some("2 privilege(s)"));
        assert_eq!(declared[0].badge.as_deref(), Some("social"));
        let derived = role_summaries(&[], &users);
        assert_eq!(derived.iter().map(|r| r.reference.name.as_str()).collect::<Vec<_>>(), vec!["designer", "queryreader", "superuser"]);
        assert_eq!(derived[0].badge.as_deref(), Some("in use"));
        assert_eq!(derived[0].detail.as_deref(), Some("held by ann@social"));
        let rd = role_detail(&derived[0].reference, None, &users);
        assert_eq!(rd.rows.as_ref().map(|r| r.rows.len()), Some(1));
        assert!(rd.properties.iter().any(|p| p.name == "members" && p.value == "1"));
    }

    #[test]
    fn stats_groups_build() {
        let types = parse_schema(&sample_schema());
        let vertices: BTreeMap<String, i64> = [("Person".to_string(), 1500)].into_iter().collect();
        let edges: BTreeMap<String, i64> = [("KNOWS".to_string(), 42)].into_iter().collect();
        let requests = request_stats(&json!([{"CompletedRequests": 10, "ErrorRequests": 1}, {"CompletedRequests": 5, "ErrorRequests": 0}]), 60);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].label, "CompletedRequests");
        assert_eq!(requests[0].numeric, Some(15.0));
        assert_eq!(requests[0].hint.as_deref(), Some("last 60s"));
        let groups = stat_groups(Some("TigerGraph 3.9"), "social", &types, &vertices, &edges, requests);
        assert_eq!(groups.iter().map(|g| g.title.as_str()).collect::<Vec<_>>(), vec!["Server", "Graph", "Throughput"]);
        assert_eq!(groups[0].stats[0].value, "TigerGraph 3.9");
        assert_eq!(groups[1].stats[0].numeric, Some(1500.0));
        assert_eq!(groups[1].stats[2].hint.as_deref(), Some("Person"));
        assert_eq!(stat_groups(None, "g", &[], &BTreeMap::new(), &BTreeMap::new(), vec![]).len(), 2);
    }
}
