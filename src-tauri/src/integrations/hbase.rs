// SOT: hbase-integration, hbase-rest-api, stargate, hbase-scanner

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, local, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SslMode, StatementResult, TableInfo, TableKind, TableRef, Value,
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
        return Err(AppError::invalid_input("JSON command needs one of \"get\", \"scan\", \"put\" or \"list\"."));
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

    async fn families(&self, table: &str) -> AppResult<Vec<String>> {
        let schema: Json = self.http.get_json(&format!("/{}/schema", pct(table))).await?;
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
        let v: Json = self.http.get_json("/").await?;
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

#[async_trait]
impl Integration for HbaseIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { namespaces: true, fixed_columns: true, row_estimate: false, ..Capabilities::KEY_VALUE }
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
        }
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
