// SOT: influxdb-integration, influxql, flux, influx-v2-api, influx-annotated-csv, influx-v1-fallback

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, json_to_value, local, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SortRule, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::Value as Json;
use std::sync::Arc;

// ============================================================================
// WHAT:  InfluxDB adapter. v2 first (port 8086, `Authorization: Token …`,
//        username = org, database = bucket) with a v1 fallback (InfluxQL over
//        `/query?db=…`, Basic auth) when the server reports 1.x.
//        A bucket is a schema, a measurement a table; rows are the pivoted
//        `_time` × field columns plus tags.
// WHY:   Time series have no primary key; `_time` is pinned as the row
//        address. Flux is the only language that can pivot and page on v2,
//        so the grid is driven by generated Flux over a 30-day window, while
//        `execute` accepts both Flux (`from(…) |> …`) and InfluxQL (`SELECT …`).
// HOW:   Flux responses are annotated CSV (`#datatype`, `#group`, `#default`
//        header rows, blank line between tables); the parser below turns the
//        first table into a ResultSet using the datatype row for typing.
// WHERE: src-tauri/src/integrations/mod.rs (trait), integrations/http.rs (client)
// ============================================================================

const DEFAULT_PORT: u16 = 8086;
const DEFAULT_RANGE: &str = "-30d";
const LOCAL_CAP: u32 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Api {
    V2,
    V1,
}

pub struct InfluxIntegration {
    http: HttpClient,
    api: Api,
    org: Option<String>,
    bucket: Option<String>,
    read_only: bool,
    /// Kept for InfluxQL over the v1 compatibility endpoint on a v2 server (Basic user:token).
    v1_compat: Option<HttpClient>,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty()).map(str::to_string);
    let secret = conn.secret.as_deref().map(str::trim).filter(|p| !p.is_empty()).map(str::to_string);
    let bucket = s.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);

    let token_auth = match &secret {
        Some(t) => Auth::Header { name: "Authorization".into(), value: format!("Token {t}") },
        None => Auth::None,
    };
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, token_auth)?;
    let api = detect_api(&http).await?;
    let basic = match (&user, &secret) {
        (Some(u), Some(p)) => Some(Auth::Basic { user: u.clone(), password: p.clone() }),
        (None, Some(p)) => Some(Auth::Basic { user: String::new(), password: p.clone() }),
        (Some(u), None) => Some(Auth::Basic { user: u.clone(), password: String::new() }),
        (None, None) => None,
    };
    let integration = match api {
        Api::V2 => {
            let v1_compat = basic.map(|a| HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, a)).transpose()?;
            InfluxIntegration { http, api, org: user, bucket, read_only: s.read_only, v1_compat }
        }
        Api::V1 => {
            let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, basic.unwrap_or(Auth::None))?;
            InfluxIntegration { http, api, org: None, bucket, read_only: s.read_only, v1_compat: None }
        }
    };
    Ok(Arc::new(integration))
}

// WHAT:  `/health` answers on both; a 1.x version (or a missing /api/v2/ping) means v1.
async fn detect_api(http: &HttpClient) -> AppResult<Api> {
    let health: Json = http.get_json("/health").await?;
    let version = health.get("version").and_then(Json::as_str).unwrap_or_default();
    if version.starts_with("1.") {
        return Ok(Api::V1);
    }
    if version.starts_with('2') || version.starts_with('3') {
        return Ok(Api::V2);
    }
    let ping = http.send(http.request(Method::GET, "/api/v2/ping")).await;
    Ok(match ping {
        Ok(_) => Api::V2,
        Err(AppError::NotFound { .. }) => Api::V1,
        Err(e) => return Err(e),
    })
}

// ---------------------------------------------------------------------------
// Annotated CSV → ResultSet
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct CsvTable {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<Value>>,
}

fn csv_split(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

fn typed_cell(raw: &str, datatype: &str) -> Value {
    if raw.is_empty() {
        return Value::Null;
    }
    match datatype {
        "long" | "unsignedLong" => raw.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Decimal(raw.to_string())),
        "double" => raw.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::Text(raw.to_string())),
        "boolean" => Value::Bool(raw == "true"),
        d if d.starts_with("dateTime") => Value::DateTime(raw.to_string()),
        _ => Value::Text(raw.to_string()),
    }
}

// WHAT:  Parses every table of an annotated CSV response. Tables are
//        separated by blank lines; each starts with annotation rows then a
//        header row. Un-annotated CSV (plain header) is accepted too.
pub(crate) fn parse_annotated_csv(text: &str) -> Vec<CsvTable> {
    let mut tables = Vec::new();
    let mut datatypes: Vec<String> = Vec::new();
    let mut header: Option<Vec<String>> = None;
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut hidden_first = false;
    let flush = |header: &mut Option<Vec<String>>, rows: &mut Vec<Vec<Value>>, datatypes: &mut Vec<String>, hidden: bool, tables: &mut Vec<CsvTable>| {
        if let Some(h) = header.take() {
            let skip = usize::from(hidden);
            let columns = h
                .iter()
                .enumerate()
                .skip(skip)
                .map(|(i, name)| ColumnMeta { name: name.clone(), type_name: datatypes.get(i).cloned().unwrap_or_else(|| "string".into()) })
                .collect();
            let rows = std::mem::take(rows).into_iter().map(|r| r.into_iter().skip(skip).collect()).collect();
            tables.push(CsvTable { columns, rows });
        }
        datatypes.clear();
    };
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() {
            flush(&mut header, &mut rows, &mut datatypes, hidden_first, &mut tables);
            continue;
        }
        let cells = csv_split(line);
        if let Some(first) = cells.first() {
            if first.starts_with('#') {
                if first == "#datatype" {
                    datatypes = cells.clone();
                }
                continue;
            }
        }
        match &header {
            None => {
                hidden_first = cells.first().map(|c| c.is_empty()).unwrap_or(false) && cells.len() > 1;
                header = Some(cells);
            }
            Some(h) => {
                let row: Vec<Value> = h.iter().enumerate().map(|(i, _)| typed_cell(cells.get(i).map(String::as_str).unwrap_or(""), datatypes.get(i).map(String::as_str).unwrap_or("string"))).collect();
                rows.push(row);
            }
        }
    }
    flush(&mut header, &mut rows, &mut datatypes, hidden_first, &mut tables);
    tables
}

/// Drops the Flux bookkeeping columns (`result`, `table`) when the caller does not want them.
fn strip_meta(mut t: CsvTable) -> CsvTable {
    let drop: Vec<usize> = t.columns.iter().enumerate().filter(|(_, c)| c.name == "result" || c.name == "table").map(|(i, _)| i).collect();
    if drop.is_empty() {
        return t;
    }
    t.columns = t.columns.into_iter().enumerate().filter(|(i, _)| !drop.contains(i)).map(|(_, c)| c).collect();
    t.rows = t.rows.into_iter().map(|r| r.into_iter().enumerate().filter(|(i, _)| !drop.contains(i)).map(|(_, v)| v).collect()).collect();
    t
}

fn csv_to_result_set(text: &str, max_rows: usize) -> ResultSet {
    let mut tables = parse_annotated_csv(text);
    if tables.is_empty() {
        return ResultSet { columns: vec![ColumnMeta { name: "result".into(), type_name: "string".into() }], rows: vec![], truncated: false };
    }
    // Merge tables that share a header (one Flux table per series is the norm).
    let first = strip_meta(tables.remove(0));
    let columns = first.columns;
    let mut rows = first.rows;
    for t in tables {
        let t = strip_meta(t);
        if t.columns == columns {
            rows.extend(t.rows);
        }
    }
    let truncated = rows.len() > max_rows;
    rows.truncate(max_rows);
    ResultSet { columns, rows, truncated }
}

// ---------------------------------------------------------------------------
// Flux generation
// ---------------------------------------------------------------------------

fn flux_string(raw: &str) -> String {
    format!("\"{}\"", raw.replace('\\', "\\\\").replace('"', "\\\""))
}

fn flux_literal(raw: &str) -> String {
    let t = raw.trim();
    if t.parse::<i64>().is_ok() || t.parse::<f64>().is_ok() || t == "true" || t == "false" {
        return t.to_string();
    }
    flux_string(t)
}

fn flux_field(column: &str) -> String {
    if column.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        format!("r.{column}")
    } else {
        format!("r[{}]", flux_string(column))
    }
}

/// Server-side Flux predicate for one rule, or None when it must run locally.
fn flux_predicate(rule: &FilterRule) -> Option<String> {
    let f = flux_field(&rule.column);
    let v = rule.value.trim();
    Some(match rule.op {
        FilterOp::Eq => format!("{f} == {}", flux_literal(v)),
        FilterOp::Ne => format!("{f} != {}", flux_literal(v)),
        FilterOp::Gt => format!("{f} > {}", flux_literal(v)),
        FilterOp::Gte => format!("{f} >= {}", flux_literal(v)),
        FilterOp::Lt => format!("{f} < {}", flux_literal(v)),
        FilterOp::Lte => format!("{f} <= {}", flux_literal(v)),
        FilterOp::Contains => format!("strings.containsStr(v: string(v: {f}), substr: {})", flux_string(v)),
        FilterOp::StartsWith => format!("strings.hasPrefix(v: string(v: {f}), prefix: {})", flux_string(v)),
        FilterOp::EndsWith => format!("strings.hasSuffix(v: string(v: {f}), suffix: {})", flux_string(v)),
        FilterOp::In => {
            let items: Vec<String> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(|s| format!("{f} == {}", flux_literal(s))).collect();
            format!("({})", items.join(" or "))
        }
        FilterOp::IsNull => format!("not exists {f}"),
        FilterOp::IsNotNull => format!("exists {f}"),
    })
}

fn needs_strings_import(filters: &[FilterRule]) -> bool {
    filters.iter().any(|f| matches!(f.op, FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith))
}

// WHAT:  The pivoted table query for one measurement. Comparisons on `_time`
//        need time literals, so those filters are applied inside `range`.
fn page_flux(bucket: &str, measurement: &str, query: &PageQuery, count_only: bool) -> String {
    let mut out = String::new();
    if needs_strings_import(&query.filters) {
        out.push_str("import \"strings\"\n");
    }
    let mut start = DEFAULT_RANGE.to_string();
    let mut stop: Option<String> = None;
    let mut preds = Vec::new();
    for rule in &query.filters {
        if rule.column == "_time" {
            let v = rule.value.trim();
            match rule.op {
                FilterOp::Gt | FilterOp::Gte => start = time_literal(v),
                FilterOp::Lt | FilterOp::Lte => stop = Some(time_literal(v)),
                _ => {}
            }
            continue;
        }
        if let Some(p) = flux_predicate(rule) {
            preds.push(p);
        }
    }
    let range = match stop {
        Some(s) => format!("range(start: {start}, stop: {s})"),
        None => format!("range(start: {start})"),
    };
    out.push_str(&format!("from(bucket: {})\n  |> {range}\n  |> filter(fn: (r) => r._measurement == {})\n", flux_string(bucket), flux_string(measurement)));
    out.push_str("  |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n");
    out.push_str("  |> drop(columns: [\"_start\", \"_stop\", \"_measurement\"])\n");
    if !preds.is_empty() {
        out.push_str(&format!("  |> filter(fn: (r) => {})\n", preds.join(" and ")));
    }
    out.push_str("  |> group()\n");
    if count_only {
        out.push_str("  |> count(column: \"_time\")\n");
        return out;
    }
    let sort: Vec<String> = query.sort.iter().map(|s| flux_string(&s.column)).collect();
    let desc = query.sort.first().map(|s| s.desc).unwrap_or(true);
    let cols = if sort.is_empty() { "[\"_time\"]".to_string() } else { format!("[{}]", sort.join(", ")) };
    out.push_str(&format!("  |> sort(columns: {cols}, desc: {desc})\n"));
    out.push_str(&format!("  |> limit(n: {}, offset: {})\n", query.limit, query.offset));
    out
}

fn time_literal(raw: &str) -> String {
    let t = raw.trim();
    if t.starts_with('-') || t.parse::<i64>().is_ok() || t.contains('T') {
        t.to_string()
    } else {
        format!("time(v: {})", flux_string(t))
    }
}

// ---------------------------------------------------------------------------
// Console parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Command {
    Flux(String),
    InfluxQl(String),
}

fn classify(text: &str) -> AppResult<Command> {
    let t = text.trim();
    if t.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    let upper = t.to_ascii_uppercase();
    if t.starts_with("from(") || t.starts_with("import ") || t.contains("|>") || t.starts_with("buckets(") {
        return Ok(Command::Flux(t.to_string()));
    }
    if ["SELECT", "SHOW", "CREATE", "DROP", "DELETE", "GRANT", "REVOKE", "ALTER", "EXPLAIN", "KILL"].iter().any(|kw| upper.starts_with(kw)) {
        return Ok(Command::InfluxQl(t.trim_end_matches(';').to_string()));
    }
    Err(AppError::invalid_input("Enter Flux (`from(bucket: \"b\") |> range(start: -1h)`) or InfluxQL (`SELECT * FROM m LIMIT 10`)."))
}

fn influxql_is_write(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    ["CREATE", "DROP", "DELETE", "GRANT", "REVOKE", "ALTER", "KILL"].iter().any(|kw| upper.starts_with(kw))
}

// WHAT:  InfluxQL JSON (`results[].series[]{name,columns,values}`) → grid.
//        Every series becomes rows with a leading `name` column when several exist.
fn influxql_result(body: &Json, max_rows: usize) -> AppResult<ResultSet> {
    let results = body.get("results").and_then(Json::as_array).cloned().unwrap_or_default();
    let mut columns: Vec<ColumnMeta> = Vec::new();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for r in &results {
        if let Some(err) = r.get("error").and_then(Json::as_str) {
            return Err(AppError::driver(err.to_string()));
        }
        let series = r.get("series").and_then(Json::as_array).cloned().unwrap_or_default();
        let many = series.len() > 1;
        for s in &series {
            let name = s.get("name").and_then(Json::as_str).unwrap_or_default().to_string();
            let cols: Vec<String> = s.get("columns").and_then(Json::as_array).into_iter().flatten().filter_map(|c| c.as_str().map(str::to_string)).collect();
            let tags = s.get("tags").and_then(Json::as_object).cloned().unwrap_or_default();
            if columns.is_empty() {
                if many {
                    columns.push(ColumnMeta { name: "name".into(), type_name: "string".into() });
                }
                for (k, _) in &tags {
                    columns.push(ColumnMeta { name: k.clone(), type_name: "string".into() });
                }
                for c in &cols {
                    columns.push(ColumnMeta { name: c.clone(), type_name: if c == "time" { "dateTime".into() } else { "json".into() } });
                }
            }
            for v in s.get("values").and_then(Json::as_array).into_iter().flatten() {
                let mut row: Vec<Value> = Vec::new();
                if many {
                    row.push(Value::Text(name.clone()));
                }
                for (_, tv) in &tags {
                    row.push(json_to_value(tv));
                }
                for (i, cell) in v.as_array().into_iter().flatten().enumerate() {
                    row.push(if cols.get(i).map(String::as_str) == Some("time") { Value::DateTime(cell.as_str().unwrap_or_default().to_string()) } else { json_to_value(cell) });
                }
                rows.push(row);
            }
        }
    }
    if columns.is_empty() {
        return Ok(json_result(body.clone()));
    }
    let truncated = rows.len() > max_rows;
    rows.truncate(max_rows);
    Ok(ResultSet { columns, rows, truncated })
}

fn quote_ident(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\\\""))
}

fn influxql_literal(raw: &str) -> String {
    let t = raw.trim();
    if t.parse::<f64>().is_ok() || t == "true" || t == "false" {
        t.to_string()
    } else {
        format!("'{}'", t.replace('\'', "\\'"))
    }
}

fn influxql_predicate(rule: &FilterRule) -> Option<String> {
    let c = quote_ident(&rule.column);
    let v = rule.value.trim();
    Some(match rule.op {
        FilterOp::Eq => format!("{c} = {}", influxql_literal(v)),
        FilterOp::Ne => format!("{c} != {}", influxql_literal(v)),
        FilterOp::Gt => format!("{c} > {}", influxql_literal(v)),
        FilterOp::Gte => format!("{c} >= {}", influxql_literal(v)),
        FilterOp::Lt => format!("{c} < {}", influxql_literal(v)),
        FilterOp::Lte => format!("{c} <= {}", influxql_literal(v)),
        FilterOp::Contains => format!("{c} =~ /{}/", regex_escape(v)),
        FilterOp::StartsWith => format!("{c} =~ /^{}/", regex_escape(v)),
        FilterOp::EndsWith => format!("{c} =~ /{}$/", regex_escape(v)),
        FilterOp::In => {
            let items: Vec<String> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(|s| format!("{c} = {}", influxql_literal(s))).collect();
            format!("({})", items.join(" OR "))
        }
        FilterOp::IsNull | FilterOp::IsNotNull => return None,
    })
}

fn regex_escape(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if "\\^$.|?*+()[]{}/".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl InfluxIntegration {
    fn org_query(&self) -> String {
        match &self.org {
            Some(o) => format!("?org={}", encode(o)),
            None => String::new(),
        }
    }

    async fn flux(&self, script: &str) -> AppResult<String> {
        if self.api != Api::V2 {
            return Err(AppError::invalid_input("Flux needs InfluxDB 2.x; this server is 1.x. Use InfluxQL instead."));
        }
        let path = format!("/api/v2/query{}", self.org_query());
        self.http.post_raw(&path, "application/vnd.flux", script.to_string(), Some("application/csv")).await
    }

    async fn influxql(&self, sql: &str, db: Option<&str>) -> AppResult<Json> {
        let client = match self.api {
            Api::V1 => &self.http,
            Api::V2 => self.v1_compat.as_ref().unwrap_or(&self.http),
        };
        let mut path = format!("/query?q={}", encode(sql));
        if let Some(db) = db.or(self.bucket.as_deref()) {
            path.push_str(&format!("&db={}", encode(db)));
        }
        let method = if influxql_is_write(sql) { Method::POST } else { Method::GET };
        let resp = client.send(client.request(method, &path)).await?;
        resp.json::<Json>().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))
    }

    fn bucket_of<'a>(&'a self, table: &'a TableRef) -> AppResult<&'a str> {
        table.schema.as_deref().or(self.bucket.as_deref()).ok_or_else(|| AppError::invalid_input("Set the bucket (database field) on the connection."))
    }

    async fn buckets(&self) -> AppResult<Vec<String>> {
        match self.api {
            Api::V2 => {
                let path = match &self.org {
                    Some(o) => format!("/api/v2/buckets?limit=100&org={}", encode(o)),
                    None => "/api/v2/buckets?limit=100".to_string(),
                };
                let out: Json = self.http.get_json(&path).await?;
                let mut names: Vec<String> = out
                    .get("buckets")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|b| b.get("name").and_then(Json::as_str))
                    .filter(|n| !n.starts_with('_'))
                    .map(str::to_string)
                    .collect();
                names.sort();
                Ok(names)
            }
            Api::V1 => {
                let out = self.influxql("SHOW DATABASES", None).await?;
                let set = influxql_result(&out, 1000)?;
                Ok(set.rows.iter().filter_map(|r| match r.first() { Some(Value::Text(t)) => Some(t.clone()), _ => None }).filter(|n| n != "_internal").collect())
            }
        }
    }

    async fn measurements(&self, bucket: &str) -> AppResult<Vec<String>> {
        match self.api {
            Api::V2 => {
                let script = format!("import \"influxdata/influxdb/schema\"\nschema.measurements(bucket: {})", flux_string(bucket));
                let csv = self.flux(&script).await?;
                let set = csv_to_result_set(&csv, 10_000);
                let idx = set.columns.iter().position(|c| c.name == "_value").unwrap_or(0);
                Ok(set.rows.iter().filter_map(|r| match r.get(idx) { Some(Value::Text(t)) => Some(t.clone()), _ => None }).collect())
            }
            Api::V1 => {
                let out = self.influxql("SHOW MEASUREMENTS", Some(bucket)).await?;
                let set = influxql_result(&out, 10_000)?;
                Ok(set.rows.iter().filter_map(|r| match r.first() { Some(Value::Text(t)) => Some(t.clone()), _ => None }).collect())
            }
        }
    }

    async fn keys(&self, bucket: &str, measurement: &str, kind: &str) -> AppResult<Vec<String>> {
        let script = format!(
            "import \"influxdata/influxdb/schema\"\nschema.measurement{kind}(bucket: {}, measurement: {}, start: {DEFAULT_RANGE})",
            flux_string(bucket),
            flux_string(measurement)
        );
        let csv = self.flux(&script).await?;
        let set = csv_to_result_set(&csv, 10_000);
        let idx = set.columns.iter().position(|c| c.name == "_value").unwrap_or(0);
        Ok(set.rows.iter().filter_map(|r| match r.get(idx) { Some(Value::Text(t)) => Some(t.clone()), _ => None }).collect())
    }

    async fn run_command(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        match cmd {
            Command::Flux(script) => {
                if self.read_only && script.contains("to(") {
                    return Err(AppError::invalid_input("`to()` writes are refused: this connection is read-only."));
                }
                let csv = self.flux(&script).await?;
                Ok(StatementResult::Rows { result: csv_to_result_set(&csv, max_rows) })
            }
            Command::InfluxQl(sql) => {
                if self.read_only && influxql_is_write(&sql) {
                    return Err(AppError::invalid_input("This statement is refused: the connection is read-only."));
                }
                let out = self.influxql(&sql, None).await?;
                if influxql_is_write(&sql) {
                    if let Some(err) = out.pointer("/results/0/error").and_then(Json::as_str) {
                        return Err(AppError::driver(err.to_string()));
                    }
                    return Ok(StatementResult::Affected { rows_affected: 0 });
                }
                Ok(StatementResult::Rows { result: influxql_result(&out, max_rows)? })
            }
        }
    }
}

fn encode(raw: &str) -> String {
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

#[async_trait]
impl Integration for InfluxIntegration {
    fn engine(&self) -> Engine {
        Engine::Influxdb
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: true, fixed_columns: false, paging: true, row_estimate: false, views: false, transactions: false, exact_estimate: false }
    }

    async fn ping(&self) -> AppResult<()> {
        let _: Json = self.http.get_json("/health").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let health: Json = self.http.get_json("/health").await?;
        Ok(health.get("version").and_then(Json::as_str).map(|v| format!("InfluxDB {v}")))
    }

    fn current_database(&self) -> Option<String> {
        self.bucket.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let mut names = self.buckets().await?;
        if let Some(b) = &self.bucket {
            if !names.contains(b) {
                names.insert(0, b.clone());
            }
        }
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let buckets = match &self.bucket {
            Some(b) => vec![b.clone()],
            None => self.buckets().await?,
        };
        let mut schemas = Vec::new();
        for bucket in buckets {
            let tables = self
                .measurements(&bucket)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|name| TableInfo { schema: Some(bucket.clone()), name, kind: TableKind::Table, row_estimate: None })
                .collect();
            schemas.push(SchemaInfo { name: bucket, tables });
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let bucket = self.bucket_of(table)?;
        let mut cols = vec![ColumnInfo { name: "_time".into(), data_type: "dateTime".into(), nullable: false, primary_key: true, ordinal: 1 }];
        let (tags, fields) = match self.api {
            Api::V2 => (self.keys(bucket, &table.name, "TagKeys").await?, self.keys(bucket, &table.name, "FieldKeys").await?),
            Api::V1 => {
                let t = influxql_result(&self.influxql(&format!("SHOW TAG KEYS FROM {}", quote_ident(&table.name)), Some(bucket)).await?, 1000)?;
                let f = influxql_result(&self.influxql(&format!("SHOW FIELD KEYS FROM {}", quote_ident(&table.name)), Some(bucket)).await?, 1000)?;
                let names = |s: &ResultSet| -> Vec<String> { s.rows.iter().filter_map(|r| match r.first() { Some(Value::Text(t)) => Some(t.clone()), _ => None }).collect() };
                (names(&t), names(&f))
            }
        };
        for name in tags.into_iter().filter(|t| !t.starts_with('_')) {
            let ordinal = u32::try_from(cols.len() + 1).unwrap_or(u32::MAX);
            cols.push(ColumnInfo { name, data_type: "tag".into(), nullable: true, primary_key: false, ordinal });
        }
        for name in fields {
            let ordinal = u32::try_from(cols.len() + 1).unwrap_or(u32::MAX);
            cols.push(ColumnInfo { name, data_type: "field".into(), nullable: true, primary_key: false, ordinal });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, _table: &TableRef) -> AppResult<Option<i64>> {
        Ok(None)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let bucket = self.bucket_of(table)?;
        match self.api {
            Api::V2 => {
                let q = PageQuery { sort: vec![], filters: filters.to_vec(), offset: 0, limit: 1 };
                let csv = self.flux(&page_flux(bucket, &table.name, &q, true)).await?;
                let set = csv_to_result_set(&csv, 10);
                let idx = set.columns.iter().position(|c| c.name == "_time").unwrap_or(0);
                Ok(match set.rows.first().and_then(|r| r.get(idx)) {
                    Some(Value::Int(n)) => *n,
                    _ => 0,
                })
            }
            Api::V1 => {
                let set = self.fetch_page(table, &PageQuery { sort: vec![], filters: filters.to_vec(), offset: 0, limit: LOCAL_CAP }).await?;
                Ok(set.rows.len() as i64)
            }
        }
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let bucket = self.bucket_of(table)?;
        match self.api {
            Api::V2 => {
                let csv = self.flux(&page_flux(bucket, &table.name, query, false)).await?;
                Ok(csv_to_result_set(&csv, query.limit as usize))
            }
            Api::V1 => {
                let preds: Vec<String> = query.filters.iter().filter_map(influxql_predicate).collect();
                let local_rules: Vec<FilterRule> = query.filters.iter().filter(|f| influxql_predicate(f).is_none()).cloned().collect();
                let where_sql = if preds.is_empty() { format!(" WHERE time > now() - {}", DEFAULT_RANGE.trim_start_matches('-')) } else { format!(" WHERE time > now() - {} AND {}", DEFAULT_RANGE.trim_start_matches('-'), preds.join(" AND ")) };
                let order = match query.sort.first() {
                    Some(SortRule { column, desc }) if column == "time" || column == "_time" => format!(" ORDER BY time {}", if *desc { "DESC" } else { "ASC" }),
                    _ => " ORDER BY time DESC".to_string(),
                };
                let sql = format!("SELECT * FROM {}{where_sql}{order} LIMIT {}", quote_ident(&table.name), LOCAL_CAP);
                let out = self.influxql(&sql, Some(bucket)).await?;
                let mut set = influxql_result(&out, LOCAL_CAP as usize)?;
                for c in &mut set.columns {
                    if c.name == "time" {
                        c.name = "_time".into();
                    }
                }
                let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
                let sort: Vec<SortRule> = query.sort.iter().filter(|s| s.column != "_time").cloned().collect();
                set.rows = local::page(&names, set.rows, &PageQuery { sort, filters: local_rules, offset: query.offset, limit: query.limit });
                set.truncated = false;
                Ok(set)
            }
        }
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let cmd = classify(sql)?;
        Ok(vec![self.run_command(cmd, max_rows).await?])
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode};

    const CSV: &str = "#group,false,false,true,true,false,false,true\n#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string\n#default,_result,,,,,,\n,result,table,_start,_stop,_time,_value,host\n,,0,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,2024-01-01T01:00:00Z,1.5,\"a,b\"\n,,0,2024-01-01T00:00:00Z,2024-01-02T00:00:00Z,2024-01-01T02:00:00Z,,h2\n\n#group,false,false\n#datatype,string,long\n#default,_result,\n,result,_value\n,,42\n";

    #[test]
    fn annotated_csv_parses_tables_and_types() {
        let tables = parse_annotated_csv(CSV);
        assert_eq!(tables.len(), 2);
        let first = &tables[0];
        let names: Vec<&str> = first.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["result", "table", "_start", "_stop", "_time", "_value", "host"]);
        assert_eq!(first.columns[5].type_name, "double");
        assert_eq!(first.rows.len(), 2);
        assert_eq!(first.rows[0][5], Value::Float(1.5));
        assert_eq!(first.rows[0][6], Value::Text("a,b".into()));
        assert_eq!(first.rows[0][4], Value::DateTime("2024-01-01T01:00:00Z".into()));
        assert_eq!(first.rows[1][5], Value::Null);
        assert_eq!(tables[1].rows[0][1], Value::Int(42));
    }

    #[test]
    fn csv_result_set_strips_meta_and_merges_tables() {
        let csv = "#datatype,string,long,dateTime:RFC3339,double\n#group,false,false,false,false\n#default,_result,,,\n,result,table,_time,v\n,,0,2024-01-01T00:00:00Z,1\n\n#datatype,string,long,dateTime:RFC3339,double\n#group,false,false,false,false\n#default,_result,,,\n,result,table,_time,v\n,,1,2024-01-01T00:01:00Z,2\n";
        let set = csv_to_result_set(csv, 10);
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["_time", "v"]);
        assert_eq!(set.rows.len(), 2);
        assert!(!set.truncated);
        assert!(csv_to_result_set(csv, 1).truncated);
        let plain = "a,b\n1,x\n";
        let set = csv_to_result_set(plain, 10);
        assert_eq!(set.columns[0].name, "a");
        assert_eq!(set.rows[0][0], Value::Text("1".into()));
        assert_eq!(csv_to_result_set("", 10).rows.len(), 0);
    }

    #[test]
    fn csv_split_handles_quotes() {
        assert_eq!(csv_split("a,\"b,c\",\"d\"\"e\",,f"), vec!["a", "b,c", "d\"e", "", "f"]);
    }

    #[test]
    fn page_flux_shape() {
        let q = PageQuery {
            sort: vec![SortRule { column: "host".into(), desc: false }],
            filters: vec![
                FilterRule { column: "host".into(), op: FilterOp::Eq, value: "h1".into() },
                FilterRule { column: "usage".into(), op: FilterOp::Gt, value: "0.5".into() },
                FilterRule { column: "host".into(), op: FilterOp::Contains, value: "h".into() },
                FilterRule { column: "_time".into(), op: FilterOp::Gte, value: "-7d".into() },
                FilterRule { column: "region".into(), op: FilterOp::In, value: "eu, us".into() },
            ],
            offset: 20,
            limit: 10,
        };
        let flux = page_flux("b", "cpu", &q, false);
        assert!(flux.starts_with("import \"strings\"\nfrom(bucket: \"b\")\n  |> range(start: -7d)\n"));
        assert!(flux.contains("r._measurement == \"cpu\""));
        assert!(flux.contains("pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")"));
        assert!(flux.contains("r.host == \"h1\" and r.usage > 0.5 and strings.containsStr(v: string(v: r.host), substr: \"h\") and (r.region == \"eu\" or r.region == \"us\")"));
        assert!(flux.contains("sort(columns: [\"host\"], desc: false)"));
        assert!(flux.ends_with("limit(n: 10, offset: 20)\n"));
        let count = page_flux("b", "cpu", &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 1 }, true);
        assert!(count.contains("range(start: -30d)"));
        assert!(count.ends_with("count(column: \"_time\")\n"));
        assert!(!count.contains("limit("));
        assert_eq!(flux_field("weird-name"), "r[\"weird-name\"]");
    }

    #[test]
    fn classify_console_input() {
        assert_eq!(classify("from(bucket: \"b\") |> range(start: -1h)").ok(), Some(Command::Flux("from(bucket: \"b\") |> range(start: -1h)".into())));
        assert_eq!(classify("import \"strings\"\nbuckets()").ok(), Some(Command::Flux("import \"strings\"\nbuckets()".into())));
        assert_eq!(classify("select * from cpu limit 5;").ok(), Some(Command::InfluxQl("select * from cpu limit 5".into())));
        assert_eq!(classify("SHOW MEASUREMENTS").ok(), Some(Command::InfluxQl("SHOW MEASUREMENTS".into())));
        assert!(classify("hello").is_err());
        assert!(influxql_is_write("DROP MEASUREMENT x"));
        assert!(!influxql_is_write("SELECT 1"));
    }

    #[test]
    fn influxql_json_to_rows() {
        let body = serde_json::json!({"results": [{"statement_id": 0, "series": [
            {"name": "cpu", "columns": ["time", "usage"], "values": [["2024-01-01T00:00:00Z", 0.5], ["2024-01-01T00:01:00Z", 0.7]]}
        ]}]});
        let set = influxql_result(&body, 10).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["time", "usage"]);
        assert_eq!(set.rows[0][0], Value::DateTime("2024-01-01T00:00:00Z".into()));
        assert_eq!(set.rows[1][1], Value::Float(0.7));
        let many = serde_json::json!({"results": [{"series": [
            {"name": "a", "tags": {"host": "h1"}, "columns": ["time", "v"], "values": [["t", 1]]},
            {"name": "b", "tags": {"host": "h2"}, "columns": ["time", "v"], "values": [["t", 2]]}
        ]}]});
        let set = influxql_result(&many, 10).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["name", "host", "time", "v"]);
        assert_eq!(set.rows[1][0], Value::Text("b".into()));
        let err = serde_json::json!({"results": [{"error": "bad"}]});
        assert!(influxql_result(&err, 10).is_err());
        let empty = serde_json::json!({"results": [{"statement_id": 0}]});
        assert_eq!(influxql_result(&empty, 10).unwrap_or_else(|e| panic!("{e}")).columns[0].name, "result");
    }

    #[test]
    fn influxql_predicates() {
        let p = influxql_predicate(&FilterRule { column: "host".into(), op: FilterOp::Contains, value: "a.b".into() });
        assert_eq!(p.as_deref(), Some("\"host\" =~ /a\\.b/"));
        let p = influxql_predicate(&FilterRule { column: "v".into(), op: FilterOp::Gt, value: "2".into() });
        assert_eq!(p.as_deref(), Some("\"v\" > 2"));
        assert!(influxql_predicate(&FilterRule { column: "v".into(), op: FilterOp::IsNull, value: String::new() }).is_none());
    }

    // WHAT:  Live round trip against InfluxDB 2.x. Skipped unless
    //        DBFREE_TEST_INFLUXDB_URL is set (with _ORG, _BUCKET, _TOKEN).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_INFLUXDB_URL") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Influxdb,
            environment: Environment::Local,
            read_only: false,
            host: Some(url),
            port: None,
            database: std::env::var("DBFREE_TEST_INFLUXDB_BUCKET").ok(),
            username: std::env::var("DBFREE_TEST_INFLUXDB_ORG").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let secret = std::env::var("DBFREE_TEST_INFLUXDB_TOKEN").ok();
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, secret.is_some()), secret };
        let i = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        assert!(i.server_version().await.unwrap_or_default().unwrap_or_default().starts_with("InfluxDB"));
        let bucket = input.database.clone().unwrap_or_default();
        // Write a few points through the v2 write API.
        let http = HttpClient::new(input.host.clone().unwrap_or_default(), Auth::Header { name: "Authorization".into(), value: format!("Token {}", resolved.secret.clone().unwrap_or_default()) }, false).unwrap_or_else(|e| panic!("{e}"));
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let lines = format!("dbfree_cpu,host=h1 usage=0.5 {}\ndbfree_cpu,host=h2 usage=0.9 {}\ndbfree_cpu,host=h1 usage=0.7 {}", now - 3_000_000_000, now - 2_000_000_000, now - 1_000_000_000);
        http.post_raw(&format!("/api/v2/write?org={}&bucket={}&precision=ns", encode(&input.username.clone().unwrap_or_default()), encode(&bucket)), "text/plain", lines, None).await.unwrap_or_else(|e| panic!("write: {e}"));
        let cat = i.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas.iter().any(|s| s.tables.iter().any(|t| t.name == "dbfree_cpu")), "{cat:?}");
        let table = TableRef { schema: Some(bucket.clone()), name: "dbfree_cpu".into() };
        let cols = i.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"host") && names.contains(&"usage"), "{names:?}");
        let q = PageQuery {
            sort: vec![SortRule { column: "_time".into(), desc: true }],
            filters: vec![FilterRule { column: "host".into(), op: FilterOp::Eq, value: "h1".into() }],
            offset: 0,
            limit: 10,
        };
        let page = i.fetch_page(&table, &q).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2, "{page:?}");
        assert_eq!(i.count(&table, &q.filters).await.unwrap_or_default(), 2);
        let out = i.execute(&format!("from(bucket: \"{bucket}\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"dbfree_cpu\")"), 100).await.unwrap_or_else(|e| panic!("flux: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 3, "{result:?}"),
            other => panic!("unexpected {other:?}"),
        }
        let out = i.execute("SELECT * FROM dbfree_cpu", 100).await.unwrap_or_else(|e| panic!("influxql: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 3, "{result:?}"),
            other => panic!("unexpected {other:?}"),
        }
    }
}
