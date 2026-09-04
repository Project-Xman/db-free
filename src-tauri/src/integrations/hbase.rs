// SOT: hbase-integration, hbase-rest-api, stargate, hbase-scanner, hbase-object-explorer, hbase-cluster-status

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, local, objects_to_result_set, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, SslMode, Stat,
    StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Method;
use serde_json::{json, Map, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  HBase adapter over the REST gateway (Stargate, port 8080).
// WHY:   HBase rows are `rowkey → {family:qualifier → bytes}` with no fixed
//        schema. The grid gets one fixed column per column family (a JSON map
//        of qualifier → value) plus `row` (pk), so any table opens.
// HOW:   Namespaces (GET /namespaces) become schemas, GET /namespaces/{ns}/tables
//        their tables. A page opens a scanner (POST /{table}/scanner/ with an
//        XML <Scanner …/>), GETs the returned Location until it is exhausted or
//        the cap is reached, then DELETEs it. Cells arrive base64-encoded
//        (`{Row:[{key, Cell:[{column, timestamp, $}]}]}`) and are decoded to
//        UTF-8 text when valid, else kept as base64. Filters other than a
//        StartsWith on `row` (→ PrefixFilter) are applied client-side after a
//        bounded scan. `execute` takes JSON `{"table", "get"|"scan"|"put"}` or
//        hbase-shell-style `get 'table','row'`, `scan 'table'`, `list`. Puts are
//        refused when read-only.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs (trait)
// ============================================================================

const DEFAULT_PORT: u16 = 8080;
const MAX_SCAN_ROWS: usize = 5_000;
const MAX_COUNT_ROWS: usize = 100_000;
const SCAN_BATCH: usize = 500;
const ROW_COLUMN: &str = "row";

pub struct HbaseIntegration {
    engine: Engine,
    http: HttpClient,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let base = base_url(conn, Some(DEFAULT_PORT), false);
    let insecure = s.ssl_mode == SslMode::Require;
    let http = HttpClient::new(base, HttpClient::auth_from_connection(conn), insecure)?;
    let integration = HbaseIntegration { engine: s.engine, http, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn b64(raw: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(raw)
}

// WHAT:  base64 → UTF-8 text when printable, else the base64 string unchanged.
fn decode_cell(raw: &str) -> Value {
    match base64::engine::general_purpose::STANDARD.decode(raw) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(s) if !s.chars().any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t')) => Value::Text(s),
            _ => Value::Bytes(raw.to_string()),
        },
        Err(_) => Value::Text(raw.to_string()),
    }
}

fn decode_text(raw: &str) -> String {
    match decode_cell(raw) {
        Value::Text(s) => s,
        Value::Bytes(b) => b,
        other => format!("{other:?}"),
    }
}

fn xml_escape(raw: &str) -> String {
    raw.replace('&', "&amp;").replace('"', "&quot;").replace('<', "&lt;").replace('>', "&gt;")
}

fn pct(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b':') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

// WHAT:  `ns:table` for non-default namespaces, bare name for `default`.
fn table_path(table: &TableRef) -> String {
    match table.schema.as_deref() {
        Some(ns) if !ns.is_empty() && ns != "default" => format!("{ns}:{}", table.name),
        _ => table.name.clone(),
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct ScanSpec {
    start_row: Option<String>,
    end_row: Option<String>,
    prefix: Option<String>,
    limit: usize,
    batch: usize,
    key_only: bool,
    filter: Option<String>,
}

// WHAT:  Builds the Stargate scanner XML.
fn scanner_xml(spec: &ScanSpec) -> String {
    let mut attrs = format!("batch=\"{}\"", spec.batch.max(1));
    if let Some(s) = &spec.start_row {
        attrs.push_str(&format!(" startRow=\"{}\"", b64(s.as_bytes())));
    }
    if let Some(e) = &spec.end_row {
        attrs.push_str(&format!(" endRow=\"{}\"", b64(e.as_bytes())));
    }
    let filter = match (&spec.filter, &spec.prefix, spec.key_only) {
        (Some(f), _, _) => Some(f.clone()),
        (None, Some(p), true) => Some(format!(
            "{{\"type\":\"FilterList\",\"op\":\"MUST_PASS_ALL\",\"filters\":[{{\"type\":\"PrefixFilter\",\"value\":\"{}\"}},{{\"type\":\"KeyOnlyFilter\"}}]}}",
            b64(p.as_bytes())
        )),
        (None, Some(p), false) => Some(format!("{{\"type\":\"PrefixFilter\",\"value\":\"{}\"}}", b64(p.as_bytes()))),
        (None, None, true) => Some("{\"type\":\"KeyOnlyFilter\"}".to_string()),
        (None, None, false) => None,
    };
    match filter {
        Some(f) => format!("<Scanner {attrs}><filter>{}</filter></Scanner>", xml_escape(&f)),
        None => format!("<Scanner {attrs}/>"),
    }
}

// WHAT:  A scanner page (`{Row:[…]}`) → (rowkey, family → {qualifier → text}).
fn decode_rows(body: &Json) -> Vec<(String, Map<String, Json>)> {
    let mut out = Vec::new();
    for row in body.get("Row").and_then(Json::as_array).into_iter().flatten() {
        let key = row.get("key").and_then(Json::as_str).map(decode_text).unwrap_or_default();
        let mut families: Map<String, Json> = Map::new();
        for cell in row.get("Cell").and_then(Json::as_array).into_iter().flatten() {
            let column = cell.get("column").and_then(Json::as_str).map(decode_text).unwrap_or_default();
            let (family, qualifier) = column.split_once(':').unwrap_or((column.as_str(), ""));
            let value = cell.get("$").and_then(Json::as_str).map(decode_cell).unwrap_or(Value::Null);
            let jv = match value {
                Value::Text(s) => Json::String(s),
                Value::Bytes(b) => json!({ "base64": b }),
                _ => Json::Null,
            };
            let entry = families.entry(family.to_string()).or_insert_with(|| Json::Object(Map::new()));
            if let Some(obj) = entry.as_object_mut() {
                obj.insert(qualifier.to_string(), jv);
            }
        }
        out.push((key, families));
    }
    out
}

fn fixed_columns(families: &[String]) -> Vec<ColumnInfo> {
    let mut cols = vec![ColumnInfo { name: ROW_COLUMN.into(), data_type: "string".into(), nullable: false, primary_key: true, ordinal: 1 }];
    for (i, f) in families.iter().enumerate() {
        cols.push(ColumnInfo { name: f.clone(), data_type: "object".into(), nullable: true, primary_key: false, ordinal: i as u32 + 2 });
    }
    cols
}

fn rows_to_grid(columns: &[ColumnInfo], rows: Vec<(String, Map<String, Json>)>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|(key, fams)| {
            columns
                .iter()
                .map(|c| {
                    if c.name == ROW_COLUMN {
                        Value::Text(key.clone())
                    } else {
                        fams.get(&c.name).cloned().map(Value::Json).unwrap_or(Value::Null)
                    }
                })
                .collect()
        })
        .collect()
}

// WHAT:  Lifts a `row StartsWith x` (or `row = x`) filter into a server-side prefix scan.
fn prefix_from_filters(filters: &[FilterRule]) -> Option<String> {
    filters.iter().find_map(|f| {
        if f.column != ROW_COLUMN {
            return None;
        }
        match f.op {
            FilterOp::StartsWith | FilterOp::Eq => Some(f.value.trim().to_string()).filter(|s| !s.is_empty()),
            _ => None,
        }
    })
}

// ---------------------------------------------------------------------------
// execute parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Command {
    List,
    Get { table: String, row: String },
    Scan { table: String, spec: ScanSpec },
    Put { table: String, row: String, cells: Vec<(String, String)> },
    /// Raw REST call, for schema work and anything the shorthand does not cover.
    Passthrough { method: String, path: String, body: Option<Json> },
}

fn shell_args(rest: &str) -> Vec<String> {
    // Splits `'a', 'b'` / `"a", "b"` / bare tokens on commas.
    rest.split(',')
        .map(|s| s.trim().trim_matches(|c| c == '\'' || c == '"').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_command(text: &str, max_rows: usize) -> AppResult<Command> {
    let t = text.trim();
    if t.is_empty() {
        return Err(AppError::invalid_input("Empty command."));
    }
    if t.starts_with('{') {
        let v: Json = serde_json::from_str(t).map_err(|e| AppError::invalid_input(format!("Invalid JSON command: {e}")))?;
        if v.get("list").is_some() {
            return Ok(Command::List);
        }
        // `{"method","path","body"}` reaches any REST endpoint — creating a
        // table is `PUT /t/schema`, which no shorthand can express.
        if let Some(path) = v.get("path").and_then(Json::as_str) {
            let method = v.get("method").and_then(Json::as_str).unwrap_or("GET").to_ascii_uppercase();
            return Ok(Command::Passthrough { method, path: path.to_string(), body: v.get("body").cloned() });
        }
        let table = v.get("table").and_then(Json::as_str).ok_or_else(|| AppError::invalid_input("JSON command needs a \"table\"."))?.to_string();
        if let Some(row) = v.get("get").and_then(Json::as_str) {
            return Ok(Command::Get { table, row: row.to_string() });
        }
        if let Some(scan) = v.get("scan") {
            let spec = ScanSpec {
                start_row: scan.get("startRow").and_then(Json::as_str).map(str::to_string),
                end_row: scan.get("endRow").and_then(Json::as_str).map(str::to_string),
                prefix: scan.get("prefix").and_then(Json::as_str).map(str::to_string),
                limit: scan.get("limit").and_then(Json::as_u64).map(|n| n as usize).unwrap_or(max_rows).clamp(1, MAX_SCAN_ROWS),
                batch: SCAN_BATCH,
                key_only: false,
                filter: scan.get("filter").map(|f| match f {
                    Json::String(s) => s.clone(),
                    other => other.to_string(),
                }),
            };
            return Ok(Command::Scan { table, spec });
        }
        if let Some(put) = v.get("put").and_then(Json::as_object) {
            let row = put.get("row").and_then(Json::as_str).ok_or_else(|| AppError::invalid_input("\"put\" needs a \"row\"."))?.to_string();
            let cells: Vec<(String, String)> = put
                .iter()
                .filter(|(k, _)| k.as_str() != "row")
                .map(|(k, v)| (k.clone(), v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string())))
                .collect();
            if cells.is_empty() || cells.iter().any(|(k, _)| !k.contains(':')) {
                return Err(AppError::invalid_input("\"put\" needs at least one \"family:qualifier\": value pair."));
            }
            return Ok(Command::Put { table, row, cells });
        }
        return Err(AppError::invalid_input(
            "JSON command needs one of \"get\", \"scan\", \"put\", \"list\", or a raw {\"method\", \"path\", \"body\"}.",
        ));
    }
    let (verb, rest) = t.split_once(char::is_whitespace).unwrap_or((t, ""));
    let args = shell_args(rest);
    match verb.to_ascii_lowercase().as_str() {
        "list" => Ok(Command::List),
        "get" => match args.as_slice() {
            [table, row, ..] => Ok(Command::Get { table: table.clone(), row: row.clone() }),
            _ => Err(AppError::invalid_input("Usage: get 'table', 'rowkey'")),
        },
        "scan" => match args.as_slice() {
            [table, ..] => Ok(Command::Scan {
                table: table.clone(),
                spec: ScanSpec { limit: max_rows.clamp(1, MAX_SCAN_ROWS), batch: SCAN_BATCH, ..ScanSpec::default() },
            }),
            _ => Err(AppError::invalid_input("Usage: scan 'table'")),
        },
        "put" => match args.as_slice() {
            [table, row, column, value, ..] if column.contains(':') => {
                Ok(Command::Put { table: table.clone(), row: row.clone(), cells: vec![(column.clone(), value.clone())] })
            }
            _ => Err(AppError::invalid_input("Usage: put 'table', 'rowkey', 'family:qualifier', 'value'")),
        },
        other => Err(AppError::invalid_input(format!(
            "Unknown command `{other}`. Use list / get / scan / put or a JSON body {{\"table\", \"get\"|\"scan\"|\"put\"}}."
        ))),
    }
}

impl HbaseIntegration {
    fn json_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(reqwest::header::ACCEPT, HeaderValue::from_static("application/json"));
        h
    }

    // WHAT:  Column families of a table, from its REST schema document.
    // WHY:   HBase REST defaults to a Ruby-ish text dump; only an explicit
    //        `Accept: application/json` yields JSON, so the header goes on
    //        every read here (see `json_headers`).
    async fn families(&self, table: &str) -> AppResult<Vec<String>> {
        let req = self.http.request(Method::GET, &format!("/{}/schema", pct(table))).headers(Self::json_headers());
        let resp = self.http.send(req).await?;
        let schema: Json = resp.json().await.map_err(|e| AppError::driver(format!("Malformed schema response: {e}")))?;
        let mut fams: Vec<String> = schema
            .get("ColumnSchema")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(|c| c.get("name").and_then(Json::as_str)).map(str::to_string).collect())
            .unwrap_or_default();
        fams.sort();
        Ok(fams)
    }

    // WHAT:  Open → drain (up to `spec.limit`) → delete a scanner.
    async fn scan(&self, table: &str, spec: &ScanSpec) -> AppResult<(Vec<(String, Map<String, Json>)>, bool)> {
        let xml = scanner_xml(spec);
        let req = self
            .http
            .request(Method::POST, &format!("/{}/scanner/", pct(table)))
            .header(reqwest::header::CONTENT_TYPE, "text/xml")
            .header(reqwest::header::ACCEPT, "application/json")
            .body(xml);
        let resp = self.http.send(req).await?;
        let location = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| AppError::driver("HBase did not return a scanner Location."))?;
        let mut rows = Vec::new();
        let mut truncated = false;
        loop {
            let req = self.http.request(Method::GET, &location).headers(Self::json_headers());
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = self.http.send(self.http.request(Method::DELETE, &location)).await;
                    return Err(AppError::driver(e.to_string()));
                }
            };
            let status = resp.status();
            if status == reqwest::StatusCode::NO_CONTENT || status == reqwest::StatusCode::NOT_FOUND {
                break;
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                let _ = self.http.send(self.http.request(Method::DELETE, &location)).await;
                return Err(crate::integrations::http::status_error(status, &body));
            }
            let body: Json = match resp.json().await {
                Ok(b) => b,
                Err(_) => break,
            };
            let page = decode_rows(&body);
            if page.is_empty() {
                break;
            }
            rows.extend(page);
            if rows.len() >= spec.limit {
                truncated = rows.len() > spec.limit;
                rows.truncate(spec.limit);
                break;
            }
        }
        let _ = self.http.send(self.http.request(Method::DELETE, &location)).await;
        Ok((rows, truncated))
    }

    async fn all_tables(&self) -> AppResult<Vec<String>> {
        let req = self.http.request(Method::GET, "/").headers(Self::json_headers());
        let resp = self.http.send(req).await?;
        let v: Json = resp.json().await.map_err(|e| AppError::driver(format!("Malformed table list: {e}")))?;
        Ok(v.get("table")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(|t| t.get("name").and_then(Json::as_str)).map(str::to_string).collect())
            .unwrap_or_default())
    }

    async fn page_rows(&self, table: &TableRef, query: &PageQuery, columns: &[ColumnInfo]) -> AppResult<(Vec<Vec<Value>>, bool)> {
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let prefix = prefix_from_filters(&query.filters);
        let want = (query.offset as usize).saturating_add(query.limit as usize).clamp(1, MAX_SCAN_ROWS);
        // Scan further than the page when client-side filters will drop rows.
        let cap = if query.filters.is_empty() { want } else { MAX_SCAN_ROWS };
        let spec = ScanSpec { prefix, limit: cap, batch: SCAN_BATCH, ..ScanSpec::default() };
        let (raw, truncated) = self.scan(&table_path(table), &spec).await?;
        let rows = rows_to_grid(columns, raw);
        Ok((local::page(&names, rows, query), truncated))
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Namespaces (GET /namespaces), tables (GET /namespaces/{ns}/tables or
//        GET /) with their schema (GET /{table}/schema) and regions
//        (GET /{table}/regions), and region servers from GET /status/cluster.
// WHY:   The generic explorer / admin UI; the REST gateway exposes no DDL, so
//        a table's definition is its schema document as JSON.
// HOW:   Pure decoders below turn the gateway's JSON into summaries / result
//        sets and are unit-tested offline; the async methods only fetch.
//        There are no actions: schema changes go through the raw
//        `{"method","path","body"}` passthrough in `execute`.
// ---------------------------------------------------------------------------

const SYSTEM_NAMESPACE: &str = "hbase";
const MAX_OBJECTS: usize = 2_000;

/// `ns:table` → (ns, table); bare names live in `default`.
fn split_table_name(full: &str) -> (String, String) {
    match full.split_once(':') {
        Some((ns, name)) => (ns.to_string(), name.to_string()),
        None => ("default".to_string(), full.to_string()),
    }
}

fn json_i64(v: Option<&Json>) -> i64 {
    match v {
        Some(Json::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)).unwrap_or(0),
        Some(Json::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

fn json_f64(v: Option<&Json>) -> f64 {
    match v {
        Some(Json::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Json::String(s)) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// The gateway capitalises some keys (`LiveNodes`) and not others (`liveNodes`) depending on the version.
fn key<'a>(obj: &'a Json, names: &[&str]) -> Option<&'a Json> {
    names.iter().find_map(|n| obj.get(n))
}

#[derive(Debug, Clone, PartialEq)]
struct LiveNode {
    name: String,
    requests: i64,
    regions: usize,
    heap_mb: i64,
    max_heap_mb: i64,
}

fn live_nodes(status: &Json) -> Vec<LiveNode> {
    key(status, &["LiveNodes", "liveNodes"])
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .map(|n| LiveNode {
            name: n.get("name").and_then(Json::as_str).unwrap_or_default().to_string(),
            requests: json_i64(n.get("requests")),
            regions: key(n, &["Region", "region"]).and_then(Json::as_array).map(Vec::len).unwrap_or(0),
            heap_mb: json_i64(n.get("heapSizeMB")),
            max_heap_mb: json_i64(n.get("maxHeapSizeMB")),
        })
        .collect()
}

fn dead_nodes(status: &Json) -> Vec<String> {
    key(status, &["DeadNodes", "deadNodes"])
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|n| n.as_str().map(str::to_string).or_else(|| n.get("name").and_then(Json::as_str).map(str::to_string)))
        .collect()
}

fn node_summaries(status: &Json) -> Vec<ObjectSummary> {
    let mut out: Vec<ObjectSummary> = live_nodes(status)
        .into_iter()
        .map(|n| {
            ObjectSummary::new(ObjectKind::Node, n.name, None)
                .with_detail(format!("{} requests · {} regions · heap {}/{} MB", n.requests, n.regions, n.heap_mb, n.max_heap_mb))
                .with_badge("live")
        })
        .collect();
    out.extend(dead_nodes(status).into_iter().map(|n| ObjectSummary::new(ObjectKind::Node, n, None).with_badge("dead")));
    out
}

/// Column families of a schema document with their attributes (VERSIONS, TTL, …).
fn schema_families(schema: &Json) -> Vec<(String, Map<String, Json>)> {
    let mut fams: Vec<(String, Map<String, Json>)> = schema
        .get("ColumnSchema")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|c| {
            let name = c.get("name").and_then(Json::as_str)?.to_string();
            let attrs: Map<String, Json> = c.as_object().map(|o| o.iter().filter(|(k, _)| k.as_str() != "name").map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default();
            Some((name, attrs))
        })
        .collect();
    fams.sort_by(|a, b| a.0.cmp(&b.0));
    fams
}

fn family_caption(attrs: &Map<String, Json>) -> String {
    ["VERSIONS", "TTL", "COMPRESSION", "BLOOMFILTER", "IN_MEMORY", "DATA_BLOCK_ENCODING"]
        .iter()
        .filter_map(|k| attrs.get(*k).map(|v| format!("{k}={}", v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()))))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `{"name": t, "Region": [{id, name, startKey, endKey, location}]}` → one row per region.
fn regions_result(info: &Json) -> ResultSet {
    let rows: Vec<Json> = key(info, &["Region", "region"])
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .map(|r| {
            let mut obj = Map::new();
            for k in ["name", "id", "startKey", "endKey", "location"] {
                if let Some(v) = r.get(k) {
                    obj.insert(k.to_string(), v.clone());
                }
            }
            Json::Object(obj)
        })
        .collect();
    objects_to_result_set(&rows, Some("name"), MAX_OBJECTS)
}

/// Regions hosted by one live node (`Region` entries of a `LiveNodes` element).
fn node_regions_result(node: &Json) -> ResultSet {
    let rows: Vec<Json> = key(node, &["Region", "region"])
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .map(|r| {
            let mut obj = r.as_object().cloned().unwrap_or_default();
            if let Some(Json::String(raw)) = obj.get("name").cloned() {
                obj.insert("name".into(), Json::String(decode_text(&raw)));
            }
            Json::Object(obj)
        })
        .collect();
    objects_to_result_set(&rows, Some("name"), MAX_OBJECTS)
}

fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

impl HbaseIntegration {
    async fn get_json(&self, path: &str) -> AppResult<Json> {
        let req = self.http.request(Method::GET, path).headers(Self::json_headers());
        let resp = self.http.send(req).await?;
        resp.json().await.map_err(|e| AppError::driver(format!("Malformed response from {path}: {e}")))
    }

    async fn namespaces(&self) -> AppResult<Vec<String>> {
        let v = self.get_json("/namespaces").await?;
        let mut names: Vec<String> = key(&v, &["Namespace", "namespace"])
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        names.sort();
        Ok(names)
    }

    async fn tables_in(&self, namespace: &str) -> AppResult<Vec<String>> {
        let v = self.get_json(&format!("/namespaces/{}/tables", pct(namespace))).await?;
        let mut names: Vec<String> = v
            .get("table")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(|t| t.get("name").and_then(Json::as_str)).map(|n| split_table_name(n).1).collect())
            .unwrap_or_default();
        names.sort();
        Ok(names)
    }

    async fn list_namespace_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut out = Vec::new();
        for ns in self.namespaces().await? {
            if ns == SYSTEM_NAMESPACE {
                continue;
            }
            let count = self.tables_in(&ns).await.map(|t| t.len()).unwrap_or(0);
            out.push(ObjectSummary::new(ObjectKind::Namespace, ns, None).with_detail(format!("{count} tables")));
        }
        Ok(out)
    }

    async fn list_table_objects(&self, namespace: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match namespace {
            Some(ns) => Ok(self.tables_in(ns).await?.into_iter().map(|t| ObjectSummary::new(ObjectKind::Table, t, Some(ns.to_string()))).collect()),
            None => {
                let mut out: Vec<ObjectSummary> = self
                    .all_tables()
                    .await?
                    .into_iter()
                    .map(|full| split_table_name(&full))
                    .filter(|(ns, _)| ns != SYSTEM_NAMESPACE)
                    .map(|(ns, name)| ObjectSummary::new(ObjectKind::Table, name, Some(ns)))
                    .collect();
                out.sort_by(|a, b| a.reference.parent.cmp(&b.reference.parent).then(a.reference.name.cmp(&b.reference.name)));
                Ok(out)
            }
        }
    }

    async fn namespace_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ns = &reference.name;
        let props = self.get_json(&format!("/namespaces/{}", pct(ns))).await.unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&props), CodeLanguage::Json);
        if let Some(obj) = props.get("properties").and_then(Json::as_object) {
            for (k, v) in obj {
                detail = detail.property(k, v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()));
            }
        }
        detail.children = self.list_table_objects(Some(ns)).await.unwrap_or_default();
        Ok(detail)
    }

    async fn table_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let table = TableRef { schema: reference.parent.clone(), name: reference.name.clone() };
        let path = table_path(&table);
        let schema = self.get_json(&format!("/{}/schema", pct(&path))).await?;
        let families = schema_families(&schema);
        let names: Vec<String> = families.iter().map(|(n, _)| n.clone()).collect();
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&schema), CodeLanguage::Json).property("name", path.clone());
        if let Some(obj) = schema.as_object() {
            for (k, v) in obj.iter().filter(|(k, _)| k.as_str() != "name" && k.as_str() != "ColumnSchema") {
                detail = detail.property(k, v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()));
            }
        }
        for (name, attrs) in &families {
            detail = detail.property(&format!("family {name}"), family_caption(attrs));
        }
        detail.columns = fixed_columns(&names);
        if let Ok(regions) = self.get_json(&format!("/{}/regions", pct(&path))).await {
            let set = regions_result(&regions);
            detail = detail.property("regions", set.rows.len().to_string());
            detail.rows = Some(set);
        }
        Ok(detail)
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let status = self.get_json("/status/cluster").await?;
        let live = key(&status, &["LiveNodes", "liveNodes"]).and_then(Json::as_array).into_iter().flatten().find(|n| n.get("name").and_then(Json::as_str) == Some(reference.name.as_str()));
        let Some(node) = live else {
            if dead_nodes(&status).iter().any(|n| n == &reference.name) {
                return Ok(ObjectDetail::empty(reference).property("state", "dead"));
            }
            return Err(AppError::not_found(format!("Region server {} is not in the cluster status.", reference.name)));
        };
        let mut detail = ObjectDetail::empty(reference).property("state", "live");
        for (k, v) in node.as_object().into_iter().flatten().filter(|(k, _)| !matches!(k.as_str(), "Region" | "region" | "name")) {
            detail = detail.property(k, v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()));
        }
        let regions = node_regions_result(node);
        detail = detail.property("regions", regions.rows.len().to_string());
        detail.rows = Some(regions);
        Ok(detail)
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { namespaces: true, fixed_columns: true, row_estimate: false, ..Capabilities::KEY_VALUE },
        object_kinds: vec![K::Namespace, K::Table, K::Node],
        tools: vec![T::Stats],
    }
}

#[async_trait]
impl Integration for HbaseIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let req = self.http.request(Method::GET, "/version/cluster").headers(Self::json_headers());
        self.http.send(req).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let req = self.http.request(Method::GET, "/version/cluster").headers(Self::json_headers());
        let resp = self.http.send(req).await?;
        let text = resp.text().await.unwrap_or_default();
        let v = serde_json::from_str::<Json>(&text).ok().and_then(|j| j.as_str().map(str::to_string)).unwrap_or(text);
        let v = v.trim().trim_matches('"').to_string();
        Ok(Some(if v.is_empty() { "HBase".into() } else { format!("HBase {v}") }))
    }

    fn current_database(&self) -> Option<String> {
        None
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec!["hbase".into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let req = self.http.request(Method::GET, "/namespaces").headers(Self::json_headers());
        let namespaces: Vec<String> = match self.http.send(req).await {
            Ok(resp) => resp
                .json::<Json>()
                .await
                .ok()
                .and_then(|v| v.get("Namespace").and_then(Json::as_array).cloned())
                .map(|a| a.iter().filter_map(Json::as_str).map(str::to_string).collect())
                .unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let mut schemas = Vec::new();
        if namespaces.is_empty() {
            let tables = self
                .all_tables()
                .await?
                .into_iter()
                .map(|full| {
                    let (ns, name) = full.split_once(':').map(|(a, b)| (a.to_string(), b.to_string())).unwrap_or(("default".into(), full.clone()));
                    TableInfo { schema: Some(ns), name, kind: TableKind::Table, row_estimate: None }
                })
                .collect();
            schemas.push(SchemaInfo { name: "default".into(), tables });
            return Ok(SchemaCatalog { schemas });
        }
        for ns in namespaces {
            if ns == "hbase" {
                continue;
            }
            let req = self.http.request(Method::GET, &format!("/namespaces/{}/tables", pct(&ns))).headers(Self::json_headers());
            let mut tables: Vec<TableInfo> = match self.http.send(req).await {
                Ok(resp) => resp
                    .json::<Json>()
                    .await
                    .ok()
                    .and_then(|v| v.get("table").and_then(Json::as_array).cloned())
                    .map(|a| {
                        a.iter()
                            .filter_map(|t| t.get("name").and_then(Json::as_str))
                            .map(|n| n.split_once(':').map(|(_, b)| b).unwrap_or(n).to_string())
                            .map(|name| TableInfo { schema: Some(ns.clone()), name, kind: TableKind::Table, row_estimate: None })
                            .collect()
                    })
                    .unwrap_or_default(),
                Err(_) => Vec::new(),
            };
            tables.sort_by(|a, b| a.name.cmp(&b.name));
            schemas.push(SchemaInfo { name: ns, tables });
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let fams = self.families(&table_path(table)).await?;
        Ok(fixed_columns(&fams))
    }

    async fn row_estimate(&self, _table: &TableRef) -> AppResult<Option<i64>> {
        Ok(None)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let key_only = filters.iter().all(|f| f.column == ROW_COLUMN);
        let spec = ScanSpec { prefix: prefix_from_filters(filters), limit: MAX_COUNT_ROWS, batch: SCAN_BATCH * 4, key_only, ..ScanSpec::default() };
        let (raw, _) = self.scan(&table_path(table), &spec).await?;
        if filters.is_empty() {
            return Ok(raw.len() as i64);
        }
        let fams = self.families(&table_path(table)).await.unwrap_or_default();
        let cols = fixed_columns(&fams);
        let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
        let rows = rows_to_grid(&cols, raw);
        Ok(local::apply_filters(&names, rows, filters).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let (rows, truncated) = self.page_rows(table, query, &cols).await?;
        let columns = cols.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect();
        Ok(ResultSet { columns, rows, truncated })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let cmd = parse_command(sql, max_rows.max(1))?;
        match cmd {
            Command::List => {
                let tables = self.all_tables().await?;
                let rows = tables.into_iter().map(|t| vec![Value::Text(t)]).collect();
                Ok(vec![StatementResult::Rows {
                    result: ResultSet { columns: vec![ColumnMeta { name: "table".into(), type_name: "string".into() }], rows, truncated: false },
                }])
            }
            Command::Get { table, row } => {
                let req = self.http.request(Method::GET, &format!("/{}/{}", pct(&table), pct(&row))).headers(Self::json_headers());
                let resp = match self.http.send(req).await {
                    Ok(r) => r,
                    Err(AppError::NotFound { .. }) => {
                        return Ok(vec![StatementResult::Rows { result: ResultSet { columns: vec![], rows: vec![], truncated: false } }]);
                    }
                    Err(e) => return Err(e),
                };
                let body: Json = resp.json().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))?;
                let decoded = decode_rows(&body);
                let mut cells = Vec::new();
                for (key, fams) in decoded {
                    for (fam, quals) in fams {
                        for (q, v) in quals.as_object().into_iter().flatten() {
                            cells.push(json!({ "row": key, "column": format!("{fam}:{q}"), "value": v }));
                        }
                    }
                }
                Ok(vec![StatementResult::Rows { result: json_result(Json::Array(cells)) }])
            }
            Command::Scan { table, spec } => {
                let (raw, truncated) = self.scan(&table, &spec).await?;
                let fams: Vec<String> = {
                    let mut f: Vec<String> = raw.iter().flat_map(|(_, m)| m.keys().cloned()).collect();
                    f.sort();
                    f.dedup();
                    f
                };
                let cols = fixed_columns(&fams);
                let rows = rows_to_grid(&cols, raw);
                let columns = cols.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect();
                Ok(vec![StatementResult::Rows { result: ResultSet { columns, rows, truncated } }])
            }
            Command::Put { table, row, cells } => {
                if self.read_only {
                    return Err(AppError::read_only("This connection is read-only; `put` is blocked."));
                }
                let cell_json: Vec<Json> = cells
                    .iter()
                    .map(|(col, val)| json!({ "column": b64(col.as_bytes()), "$": b64(val.as_bytes()) }))
                    .collect();
                let body = json!({ "Row": [{ "key": b64(row.as_bytes()), "Cell": cell_json }] });
                let req = self
                    .http
                    .request(Method::PUT, &format!("/{}/{}", pct(&table), pct(&row)))
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .header(reqwest::header::ACCEPT, "application/json")
                    .json(&body);
                self.http.send(req).await?;
                Ok(vec![StatementResult::Affected { rows_affected: 1 }])
            }
            Command::Passthrough { method, path, body } => {
                if self.read_only && method != "GET" {
                    return Err(AppError::read_only(format!("This connection is read-only; {method} is blocked.")));
                }
                let verb = Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Unsupported HTTP method `{method}`.")))?;
                let mut req = self.http.request(verb, &path).headers(Self::json_headers());
                if let Some(b) = body {
                    req = req.header(reqwest::header::CONTENT_TYPE, "application/json").json(&b);
                }
                let resp = self.http.send(req).await?;
                let text = resp.text().await.unwrap_or_default();
                if text.trim().is_empty() {
                    return Ok(vec![StatementResult::Affected { rows_affected: 0 }]);
                }
                match serde_json::from_str::<Json>(&text) {
                    Ok(v) => Ok(vec![StatementResult::Rows { result: crate::integrations::http::json_result(v) }]),
                    Err(_) => Ok(vec![StatementResult::Rows {
                        result: ResultSet {
                            columns: vec![ColumnMeta { name: "response".into(), type_name: "string".into() }],
                            rows: vec![vec![Value::Text(text)]],
                            truncated: false,
                        },
                    }]),
                }
            }
        }
    }

    async fn close(&self) {}

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut out = match kind {
            ObjectKind::Namespace => self.list_namespace_objects().await?,
            ObjectKind::Table => self.list_table_objects(parent).await?,
            ObjectKind::Node => node_summaries(&self.get_json("/status/cluster").await?),
            _ => Vec::new(),
        };
        out.truncate(MAX_OBJECTS);
        Ok(out)
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Namespace => self.namespace_detail(reference).await,
            ObjectKind::Table => self.table_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    // WHAT:  /status/cluster totals (regions, requests, load, live / dead
    //        servers, heap across live servers) plus /version/cluster.
    async fn server_stats(&self) -> AppResult<ServerStats> {
        let status = self.get_json("/status/cluster").await?;
        let live = live_nodes(&status);
        let dead = dead_nodes(&status);
        let version = self.server_version().await.unwrap_or_default().unwrap_or_else(|| "HBase".into());
        let heap: i64 = live.iter().map(|n| n.heap_mb).sum();
        let max_heap: i64 = live.iter().map(|n| n.max_heap_mb).sum();
        let groups = vec![
            StatGroup { title: "Server".into(), stats: vec![Stat::text("Version", version), Stat::text("Gateway", self.http.base().to_string())] },
            StatGroup {
                title: "Cluster".into(),
                stats: vec![
                    Stat::number("Live region servers", live.len() as f64, None),
                    Stat::number("Dead region servers", dead.len() as f64, None),
                    Stat::number("Regions", json_f64(status.get("regions")), None),
                    Stat::number("Average load", json_f64(status.get("averageLoad")), Some("regions/server")),
                ],
            },
            StatGroup {
                title: "Throughput".into(),
                stats: vec![Stat::number("Requests", json_f64(status.get("requests")), None).with_hint("cluster-wide request counter from /status/cluster")],
            },
            StatGroup {
                title: "Memory".into(),
                stats: vec![Stat::number("Heap used", heap as f64, Some("MB")), Stat::number("Heap max", max_heap as f64, Some("MB"))],
            },
        ];
        Ok(ServerStats::now(groups))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    #[test]
    fn cluster_status_decodes_nodes() {
        let status = json!({
            "regions": 3, "requests": 42, "averageLoad": 1.5,
            "LiveNodes": [{
                "name": "rs1:16020", "startCode": 1, "requests": 40, "heapSizeMB": 120, "maxHeapSizeMB": 1024,
                "Region": [{"name": b64(b"t,,1.abc."), "stores": 1, "storefiles": 2, "readRequestsCount": 7}, {"name": b64(b"u,,2.def."), "stores": 1}]
            }],
            "DeadNodes": ["rs2,16020,99"]
        });
        let live = live_nodes(&status);
        assert_eq!(live, vec![LiveNode { name: "rs1:16020".into(), requests: 40, regions: 2, heap_mb: 120, max_heap_mb: 1024 }]);
        assert_eq!(dead_nodes(&status), vec!["rs2,16020,99"]);
        let summaries = node_summaries(&status);
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].badge.as_deref(), Some("live"));
        assert_eq!(summaries[0].detail.as_deref(), Some("40 requests · 2 regions · heap 120/1024 MB"));
        assert_eq!(summaries[1].badge.as_deref(), Some("dead"));
        let regions = node_regions_result(&status["LiveNodes"][0]);
        assert_eq!(regions.rows.len(), 2);
        assert_eq!(regions.rows[0][0], Value::Text("t,,1.abc.".into()));
        assert_eq!(json_f64(status.get("averageLoad")), 1.5);
        assert_eq!(json_i64(Some(&json!("12"))), 12);
        // Lower-case keys from older gateways are accepted too.
        assert_eq!(live_nodes(&json!({"liveNodes": [{"name": "x"}]})).len(), 1);
    }

    #[test]
    fn schema_and_regions_decode() {
        let schema = json!({"name": "users", "IS_META": "false", "ColumnSchema": [
            {"name": "meta", "VERSIONS": "3", "TTL": "FOREVER"},
            {"name": "cf", "VERSIONS": "1", "TTL": "86400", "COMPRESSION": "SNAPPY", "BLOOMFILTER": "ROW"}
        ]});
        let fams = schema_families(&schema);
        assert_eq!(fams.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), vec!["cf", "meta"]);
        assert_eq!(family_caption(&fams[0].1), "VERSIONS=1, TTL=86400, COMPRESSION=SNAPPY, BLOOMFILTER=ROW");
        let info = json!({"name": "users", "Region": [
            {"id": 1, "name": "users,,1.a.", "startKey": "", "endKey": "m", "location": "rs1:16020"},
            {"id": 2, "name": "users,m,2.b.", "startKey": "m", "endKey": "", "location": "rs2:16020"}
        ]});
        let set = regions_result(&info);
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["name", "id", "startKey", "endKey", "location"]);
        assert_eq!(set.rows.len(), 2);
        assert_eq!(set.rows[1][4], Value::Text("rs2:16020".into()));
        assert_eq!(split_table_name("ns:t"), ("ns".to_string(), "t".to_string()));
        assert_eq!(split_table_name("t"), ("default".to_string(), "t".to_string()));
    }

    #[test]
    fn scanner_xml_encodes_prefix_and_batch() {
        let spec = ScanSpec { prefix: Some("user".into()), limit: 10, batch: 50, ..ScanSpec::default() };
        let xml = scanner_xml(&spec);
        assert!(xml.starts_with("<Scanner batch=\"50\">"));
        assert!(xml.contains("PrefixFilter"));
        assert!(xml.contains(&b64(b"user")));
        assert!(xml.contains("&quot;type&quot;"));
        let plain = scanner_xml(&ScanSpec { batch: 3, start_row: Some("a".into()), ..ScanSpec::default() });
        assert_eq!(plain, format!("<Scanner batch=\"3\" startRow=\"{}\"/>", b64(b"a")));
        let keys = scanner_xml(&ScanSpec { batch: 1, key_only: true, ..ScanSpec::default() });
        assert!(keys.contains("KeyOnlyFilter"));
    }

    #[test]
    fn rows_decode_base64_cells() {
        let body = json!({
            "Row": [{
                "key": b64(b"row1"),
                "Cell": [
                    { "column": b64(b"cf:name"), "timestamp": 1, "$": b64(b"Ann") },
                    { "column": b64(b"cf:age"), "timestamp": 1, "$": b64(b"30") },
                    { "column": b64(b"other:x"), "timestamp": 1, "$": b64(&[0xff, 0x00]) }
                ]
            }]
        });
        let rows = decode_rows(&body);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "row1");
        assert_eq!(rows[0].1["cf"]["name"], json!("Ann"));
        assert_eq!(rows[0].1["other"]["x"]["base64"], json!(b64(&[0xff, 0x00])));
        let cols = fixed_columns(&["cf".into(), "other".into()]);
        let grid = rows_to_grid(&cols, rows);
        assert_eq!(grid[0][0], Value::Text("row1".into()));
        assert!(matches!(grid[0][1], Value::Json(_)));
    }

    #[test]
    fn commands_parse_json_and_shell_style() {
        assert_eq!(parse_command("list", 10).unwrap(), Command::List);
        assert_eq!(parse_command("get 'users', 'row1'", 10).unwrap(), Command::Get { table: "users".into(), row: "row1".into() });
        match parse_command("scan \"users\"", 7).unwrap() {
            Command::Scan { table, spec } => {
                assert_eq!(table, "users");
                assert_eq!(spec.limit, 7);
            }
            _ => panic!("scan"),
        }
        match parse_command(r#"{"table":"t","scan":{"startRow":"a","limit":3}}"#, 10).unwrap() {
            Command::Scan { spec, .. } => {
                assert_eq!(spec.start_row.as_deref(), Some("a"));
                assert_eq!(spec.limit, 3);
            }
            _ => panic!("scan"),
        }
        match parse_command(r#"{"table":"t","put":{"row":"r","cf:q":"v"}}"#, 10).unwrap() {
            Command::Put { cells, .. } => assert_eq!(cells, vec![("cf:q".to_string(), "v".to_string())]),
            _ => panic!("put"),
        }
        assert!(parse_command(r#"{"table":"t","put":{"row":"r","bad":"v"}}"#, 10).is_err());
        assert!(parse_command("frobnicate", 10).is_err());
        assert!(parse_command("get 'only'", 10).is_err());
    }

    #[test]
    fn prefix_and_table_path() {
        let f = vec![FilterRule { column: "row".into(), op: FilterOp::StartsWith, value: "ab".into() }];
        assert_eq!(prefix_from_filters(&f), Some("ab".into()));
        let g = vec![FilterRule { column: "cf".into(), op: FilterOp::Contains, value: "x".into() }];
        assert_eq!(prefix_from_filters(&g), None);
        assert_eq!(table_path(&TableRef { schema: Some("ns".into()), name: "t".into() }), "ns:t");
        assert_eq!(table_path(&TableRef { schema: Some("default".into()), name: "t".into() }), "t");
        assert_eq!(pct("ns:t x"), "ns:t%20x");
    }

    // Runs only when DBFREE_TEST_HBASE_URL is set:
    // `docker run --rm -d -p 8080:8080 harisekhon/hbase:latest`.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_HBASE_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Hbase,
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
            secret: None,
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(!version.is_empty(), "no version");
        // Create the table through the REST schema endpoint, then write two rows.
        let _ = db.execute(r#"{"method":"DELETE","path":"/dbfree_test/schema"}"#, 10).await;
        db.execute(
            r#"{"method":"PUT","path":"/dbfree_test/schema","body":{"name":"dbfree_test","ColumnSchema":[{"name":"cf"}]}}"#,
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
        db.execute(r#"{"table":"dbfree_test","put":{"row":"r1","cf:city":"Berlin"}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("put r1: {e}"));
        db.execute(r#"{"table":"dbfree_test","put":{"row":"r2","cf:city":"Paris"}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("put r2: {e}"));

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(
            catalog.schemas.iter().any(|s| s.tables.iter().any(|t| t.name.ends_with("dbfree_test"))),
            "{:?}",
            catalog.schemas.iter().flat_map(|s| s.tables.iter().map(|t| t.name.clone())).collect::<Vec<_>>()
        );
        let table = TableRef { schema: None, name: "dbfree_test".into() };
        let cols = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.first().map(|c| c.name == "row" && c.primary_key).unwrap_or(false), "{cols:?}");
        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2, "{page:?}");
        assert_eq!(db.count(&table, &[]).await.unwrap_or_default(), 2);
        let got = db
            .execute(r#"{"table":"dbfree_test","get":"r1"}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("get: {e}"));
        match got.first() {
            Some(StatementResult::Rows { result }) => assert!(!result.rows.is_empty(), "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        let _ = db.execute(r#"{"method":"DELETE","path":"/dbfree_test/schema"}"#, 10).await;
        db.close().await;
    }

}
