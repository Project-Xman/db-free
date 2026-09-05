// SOT: influxdb-integration, influxql, flux, influx-v2-api, influx-annotated-csv, influx-v1-fallback, influx-object-explorer, influx-server-stats, influx-range-query

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, json_to_value, local, Auth, HttpClient};
use crate::integrations::prometheus::{human_duration, jnum, jtext, mib, parse_exposition, pretty, truncate};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectProperty, ObjectRef, ObjectSummary, PageQuery, RangeQueryRequest, RangeResult, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, Series, ServerStats, SortRule, Stat, StatGroup, StatementResult, TableInfo, TableKind,
    TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value as Json};
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

// WHAT:  `/health` answers on both; a 1.x version means v1, 2.x/3.x mean v2.
// HOW:   OSS 2.x reports "v2.9.1" (leading `v`) while older builds report
//        "2.7.1", so the prefix is stripped before the major digit is read.
//        `/api/v2/ping` is the fallback and answers 401 (not 404) when it
//        exists but the request is unauthenticated — that still proves v2.
async fn detect_api(http: &HttpClient) -> AppResult<Api> {
    let health: Json = http.get_json("/health").await?;
    let version = health.get("version").and_then(Json::as_str).unwrap_or_default();
    let major = version.trim_start_matches(['v', 'V']);
    if major.starts_with("1.") || major == "1" {
        return Ok(Api::V1);
    }
    if major.starts_with('2') || major.starts_with('3') {
        return Ok(Api::V2);
    }
    Ok(match http.send(http.request(Method::GET, "/api/v2/ping")).await {
        Ok(_) => Api::V2,
        Err(AppError::NotFound { .. }) => Api::V1,
        // 401/403 means the endpoint exists and only the credentials were missing.
        Err(AppError::NotConnected { .. }) => Api::V2,
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
        // `count(column: "_time")` is rejected ("unsupported aggregate column
        // type time") and any field column may be absent after a filter, so the
        // row count is accumulated instead — it works for every schema.
        out.push_str("  |> reduce(identity: {n: 0}, fn: (r, accumulator) => ({n: accumulator.n + 1}))\n");
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

    // WHAT:  Runs a Flux script and returns annotated CSV.
    // WHY:   Posting `application/vnd.flux` returns CSV *without* the
    //        `#datatype` header, so every column would decode as text. The JSON
    //        dialect form is the only way to ask for annotations, which is what
    //        makes numbers, booleans and timestamps come back typed.
    async fn flux(&self, script: &str) -> AppResult<String> {
        if self.api != Api::V2 {
            return Err(AppError::invalid_input("Flux needs InfluxDB 2.x; this server is 1.x. Use InfluxQL instead."));
        }
        let path = format!("/api/v2/query{}", self.org_query());
        let body = json!({
            "query": script,
            "dialect": { "annotations": ["datatype", "group", "default"], "header": true },
        });
        self.http.post_raw(&path, "application/json", body.to_string(), Some("application/csv")).await
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

// ---------------------------------------------------------------------------
// Object explorer / stats / range query
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
// Listing measurements costs one Flux query per bucket, so an unscoped ask
// walks only the first few buckets rather than every one on the server.
const BUCKET_WALK: usize = 25;
const CHILD_CAP: usize = 200;

// WHAT:  Bookkeeping columns of a Flux table: never part of a series' label set.
const FLUX_META: [&str; 6] = ["result", "table", "_start", "_stop", "_time", "_value"];

fn value_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => s.clone(),
        Value::Json(j) => j.to_string(),
    }
}

fn value_number(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        Value::Decimal(s) | Value::Text(s) => s.parse().ok(),
        _ => None,
    }
}

// WHAT:  A `_time` cell → epoch seconds. RFC3339 is what both APIs emit by
//        default; a bare number is nanoseconds (v1 with `epoch=ns`), scaled by
//        magnitude so ms / µs / s payloads land on the same axis.
fn time_seconds(v: &Value) -> Option<f64> {
    if let Some(n) = match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    } {
        return Some(scale_epoch(n));
    }
    let text = value_text(v);
    if text.is_empty() {
        return None;
    }
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(&text) {
        return Some(t.timestamp() as f64 + f64::from(t.timestamp_subsec_millis()) / 1000.0);
    }
    text.parse::<f64>().ok().map(scale_epoch)
}

fn scale_epoch(n: f64) -> f64 {
    let a = n.abs();
    if a >= 1e17 {
        n / 1e9
    } else if a >= 1e14 {
        n / 1e6
    } else if a >= 1e11 {
        n / 1e3
    } else {
        n
    }
}

// WHAT:  `cpu.usage_idle{host="h1"}` from a series' label set.
fn series_name(labels: &[ObjectProperty]) -> String {
    let get = |n: &str| labels.iter().find(|p| p.name == n).map(|p| p.value.clone()).filter(|v| !v.is_empty());
    let base = match (get("_measurement"), get("_field")) {
        (Some(m), Some(f)) => format!("{m}.{f}"),
        (Some(m), None) => m,
        (None, Some(f)) => f,
        (None, None) => "value".to_string(),
    };
    let tags: Vec<String> = labels
        .iter()
        .filter(|p| p.name != "_measurement" && p.name != "_field" && !p.value.is_empty())
        .map(|p| format!("{}=\"{}\"", p.name, p.value))
        .collect();
    if tags.is_empty() {
        base
    } else {
        format!("{base}{{{}}}", tags.join(","))
    }
}

// WHAT:  Annotated CSV → one Series per Flux table (i.e. per tag set).
// HOW:   Rows carry their group in the `table` column; the label set is every
//        non-bookkeeping column, so two tables that share tags still stay apart.
pub(crate) fn csv_to_series(text: &str) -> Vec<Series> {
    let mut out: Vec<Series> = Vec::new();
    for (section, table) in parse_annotated_csv(text).into_iter().enumerate() {
        let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
        let pos = |n: &str| names.iter().position(|x| *x == n);
        let (Some(i_time), Some(i_value)) = (pos("_time"), pos("_value")) else { continue };
        let i_table = pos("table");
        let mut keys: Vec<String> = Vec::new();
        for row in &table.rows {
            let labels: Vec<ObjectProperty> = names
                .iter()
                .enumerate()
                .filter(|(_, n)| !FLUX_META.contains(*n))
                .filter_map(|(i, n)| row.get(i).map(|v| ObjectProperty { name: (*n).to_string(), value: value_text(v) }))
                .collect();
            let group = i_table.and_then(|i| row.get(i)).map(value_text).unwrap_or_default();
            let key = format!("{section}\u{1}{group}\u{1}{}", labels.iter().map(|p| format!("{}={}", p.name, p.value)).collect::<Vec<_>>().join("\u{2}"));
            let idx = match keys.iter().position(|k| *k == key) {
                Some(i) => i,
                None => {
                    keys.push(key);
                    out.push(Series { name: series_name(&labels), labels, points: Vec::new() });
                    out.len() - 1
                }
            };
            if let (Some(ts), Some(v)) = (row.get(i_time).and_then(time_seconds), row.get(i_value).and_then(value_number)) {
                if v.is_finite() {
                    out[idx].points.push([ts, v]);
                }
            }
        }
    }
    for s in &mut out {
        s.points.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap_or(std::cmp::Ordering::Equal));
    }
    out.retain(|s| !s.points.is_empty());
    out
}

// WHAT:  InfluxQL JSON → Series, one per (series, numeric column) pair.
pub(crate) fn influxql_series(body: &Json) -> AppResult<Vec<Series>> {
    let mut out = Vec::new();
    for result in body.get("results").and_then(Json::as_array).into_iter().flatten() {
        if let Some(err) = result.get("error").and_then(Json::as_str) {
            return Err(AppError::driver(err.to_string()));
        }
        for s in result.get("series").and_then(Json::as_array).into_iter().flatten() {
            let measurement = jtext(s, "name");
            let cols: Vec<String> = s.get("columns").and_then(Json::as_array).into_iter().flatten().filter_map(|c| c.as_str().map(str::to_string)).collect();
            let tags: Vec<ObjectProperty> = s
                .get("tags")
                .and_then(Json::as_object)
                .into_iter()
                .flatten()
                .map(|(k, v)| ObjectProperty { name: k.clone(), value: crate::integrations::prometheus::text_value(v) })
                .collect();
            let Some(i_time) = cols.iter().position(|c| c == "time") else { continue };
            for (i, col) in cols.iter().enumerate() {
                if i == i_time {
                    continue;
                }
                let mut labels = tags.clone();
                labels.insert(0, ObjectProperty { name: "_measurement".into(), value: measurement.clone() });
                labels.insert(1, ObjectProperty { name: "_field".into(), value: col.clone() });
                let mut points: Vec<[f64; 2]> = Vec::new();
                for row in s.get("values").and_then(Json::as_array).into_iter().flatten() {
                    let cells = row.as_array().cloned().unwrap_or_default();
                    let ts = cells.get(i_time).map(json_to_value).and_then(|v| time_seconds(&v));
                    let v = cells.get(i).map(json_to_value).and_then(|v| value_number(&v));
                    if let (Some(ts), Some(v)) = (ts, v) {
                        if v.is_finite() {
                            points.push([ts, v]);
                        }
                    }
                }
                if points.is_empty() {
                    continue;
                }
                points.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap_or(std::cmp::Ordering::Equal));
                out.push(Series { name: series_name(&labels), labels, points });
            }
        }
    }
    Ok(out)
}

fn rfc3339(seconds: f64) -> String {
    chrono::DateTime::from_timestamp(seconds as i64, 0).unwrap_or_default().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

// WHAT:  Prepends the `v` record every dashboard-style Flux script expects, so
//        `v.timeRangeStart` / `v.timeRangeStop` / `v.windowPeriod` resolve to
//        the window the explorer asked for.
pub(crate) fn flux_with_window(script: &str, start: f64, end: f64, step: f64) -> String {
    let step = step.max(1.0).round() as i64;
    format!(
        "v = {{timeRangeStart: time(v: \"{}\"), timeRangeStop: time(v: \"{}\"), windowPeriod: {step}s}}\n{script}",
        rfc3339(start),
        rfc3339(end)
    )
}

// WHAT:  Adds the explorer's window to an InfluxQL statement that has no time
//        bound of its own; a query that already filters on `time` is left alone.
pub(crate) fn influxql_with_window(sql: &str, start: f64, end: f64) -> String {
    let trimmed = sql.trim().trim_end_matches(';');
    let lower = trimmed.to_ascii_lowercase();
    let bound = format!("time >= '{}' AND time <= '{}'", rfc3339(start), rfc3339(end));
    if lower.contains("time >") || lower.contains("time <") || lower.contains("time between") {
        return trimmed.to_string();
    }
    // The bound goes before GROUP BY / ORDER BY / LIMIT, which must stay last.
    let tail = ["group by", "order by", "limit", "slimit", "offset"].iter().filter_map(|kw| lower.find(kw)).min();
    let (head, rest) = match tail {
        Some(i) => (trimmed[..i].trim_end(), trimmed[i..].to_string()),
        None => (trimmed, String::new()),
    };
    let head_lower = head.to_ascii_lowercase();
    let joined = if head_lower.contains(" where ") || head_lower.ends_with(" where") {
        format!("{head} AND {bound}")
    } else {
        format!("{head} WHERE {bound}")
    };
    if rest.is_empty() {
        joined
    } else {
        format!("{joined} {rest}")
    }
}

fn looks_like_flux(query: &str) -> bool {
    let t = query.trim();
    t.contains("|>") || t.contains("from(") || t.starts_with("import ")
}

fn seconds_label(secs: f64) -> String {
    if secs <= 0.0 {
        "infinite".to_string()
    } else {
        human_duration(secs)
    }
}

// WHAT:  A bucket's retention, from the first non-zero rule (0 = keep forever).
fn retention_of(bucket: &Json) -> String {
    let secs = bucket
        .get("retentionRules")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|r| jnum(r, "everySeconds"))
        .find(|s| *s > 0.0);
    seconds_label(secs.unwrap_or(0.0))
}

fn task_schedule(task: &Json) -> String {
    let every = jtext(task, "every");
    let cron = jtext(task, "cron");
    match (every.is_empty(), cron.is_empty()) {
        (false, _) => format!("every {every}"),
        (_, false) => format!("cron {cron}"),
        _ => String::new(),
    }
}

fn props(detail: ObjectDetail, source: &Json, keys: &[&str]) -> ObjectDetail {
    let mut detail = detail;
    for key in keys {
        let v = jtext(source, key);
        if !v.is_empty() {
            detail = detail.property(key, v);
        }
    }
    detail
}

fn key_rows(fields: &[String], tags: &[String]) -> ResultSet {
    let mut rows: Vec<Vec<Value>> = tags.iter().map(|t| vec![Value::Text("tag".into()), Value::Text(t.clone())]).collect();
    rows.extend(fields.iter().map(|f| vec![Value::Text("field".into()), Value::Text(f.clone())]));
    ResultSet {
        columns: vec![ColumnMeta { name: "kind".into(), type_name: "string".into() }, ColumnMeta { name: "name".into(), type_name: "string".into() }],
        rows,
        truncated: false,
    }
}

impl InfluxIntegration {
    // WHAT:  A v2 collection endpoint → its array. v1 servers 404 every one of
    //        them, which is reported as an empty list rather than an error.
    async fn v2_list(&self, path: &str, key: &str) -> Vec<Json> {
        if self.api != Api::V2 {
            return Vec::new();
        }
        let body: Json = self.http.get_json(path).await.unwrap_or(Json::Null);
        body.get(key).and_then(Json::as_array).cloned().unwrap_or_default()
    }

    async fn orgs(&self) -> Vec<Json> {
        self.v2_list("/api/v2/orgs", "orgs").await
    }

    async fn bucket_objects(&self) -> Vec<Json> {
        let path = match &self.org {
            Some(o) => format!("/api/v2/buckets?limit=100&org={}", encode(o)),
            None => "/api/v2/buckets?limit=100".to_string(),
        };
        self.v2_list(&path, "buckets").await
    }

    async fn tasks(&self) -> Vec<Json> {
        self.v2_list("/api/v2/tasks?limit=100", "tasks").await
    }

    async fn users(&self) -> Vec<Json> {
        self.v2_list("/api/v2/users", "users").await
    }

    /// `/api/v2/config` when the token may read it, else facts from /health + /ready.
    async fn config(&self) -> Option<Json> {
        if self.api != Api::V2 {
            return None;
        }
        match self.http.get_json::<Json>("/api/v2/config").await {
            Ok(v) if v.is_object() => Some(v),
            _ => None,
        }
    }

    async fn health_facts(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        if let Ok(h) = self.http.get_json::<Json>("/health").await {
            for key in ["status", "version", "commit", "name", "message"] {
                let v = jtext(&h, key);
                if !v.is_empty() {
                    out.push((key.to_string(), v));
                }
            }
        }
        if let Ok(r) = self.http.get_json::<Json>("/ready").await {
            for key in ["status", "started", "up"] {
                let v = jtext(&r, key);
                if !v.is_empty() {
                    out.push((format!("ready.{key}"), v));
                }
            }
        }
        out
    }

    async fn setting_objects(&self) -> Vec<ObjectSummary> {
        if let Some(cfg) = self.config().await {
            return cfg
                .as_object()
                .into_iter()
                .flatten()
                .map(|(k, v)| ObjectSummary::new(ObjectKind::Setting, k.as_str(), None).with_detail(truncate(&crate::integrations::prometheus::text_value(v), 120)).with_badge("config"))
                .collect();
        }
        self.health_facts()
            .await
            .into_iter()
            .map(|(k, v)| ObjectSummary::new(ObjectKind::Setting, k, None).with_detail(truncate(&v, 120)).with_badge("health"))
            .collect()
    }

    /// Buckets the explorer walks when no parent was given.
    async fn scan_buckets(&self) -> Vec<String> {
        match &self.bucket {
            Some(b) => vec![b.clone()],
            None => {
                let mut names = self.buckets().await.unwrap_or_default();
                names.truncate(BUCKET_WALK);
                names
            }
        }
    }

    async fn measurement_objects(&self, parent: Option<&str>) -> Vec<ObjectSummary> {
        let buckets = match parent {
            Some(b) => vec![b.to_string()],
            None => self.scan_buckets().await,
        };
        let mut out = Vec::new();
        for bucket in buckets {
            for name in self.measurements(&bucket).await.unwrap_or_default() {
                out.push(ObjectSummary::new(ObjectKind::Measurement, name, Some(bucket.clone())));
            }
        }
        out
    }

    async fn list_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut out = match kind {
            ObjectKind::Database => {
                let orgs = self.orgs().await;
                if orgs.is_empty() {
                    // v1 (and a token without org read rights): databases are the containers.
                    self.buckets().await.unwrap_or_default().into_iter().map(|n| ObjectSummary::new(ObjectKind::Database, n, None).with_badge("database")).collect()
                } else {
                    orgs.iter()
                        .map(|o| {
                            let mut s = ObjectSummary::new(ObjectKind::Database, jtext(o, "name"), None).with_badge("org");
                            let description = jtext(o, "description");
                            let detail = if description.is_empty() { jtext(o, "id") } else { description };
                            if !detail.is_empty() {
                                s = s.with_detail(truncate(&detail, 120));
                            }
                            s
                        })
                        .collect()
                }
            }
            ObjectKind::Bucket => self
                .bucket_objects()
                .await
                .iter()
                .map(|b| {
                    let kind_badge = jtext(b, "type");
                    let mut s = ObjectSummary::new(ObjectKind::Bucket, jtext(b, "name"), None).with_detail(format!("retention {}", retention_of(b)));
                    if !kind_badge.is_empty() {
                        s = s.with_badge(kind_badge);
                    }
                    s
                })
                .collect(),
            ObjectKind::Measurement => self.measurement_objects(parent).await,
            ObjectKind::Task => self
                .tasks()
                .await
                .iter()
                .map(|t| {
                    let mut s = ObjectSummary::new(ObjectKind::Task, jtext(t, "name"), None);
                    let schedule = task_schedule(t);
                    if !schedule.is_empty() {
                        s = s.with_detail(schedule);
                    }
                    let status = jtext(t, "status");
                    if !status.is_empty() {
                        s = s.with_badge(status);
                    }
                    s
                })
                .collect(),
            ObjectKind::User => self
                .users()
                .await
                .iter()
                .map(|u| {
                    let mut s = ObjectSummary::new(ObjectKind::User, jtext(u, "name"), None);
                    let status = jtext(u, "status");
                    if !status.is_empty() {
                        s = s.with_badge(status);
                    }
                    let id = jtext(u, "id");
                    if !id.is_empty() {
                        s = s.with_detail(id);
                    }
                    s
                })
                .collect(),
            ObjectKind::Setting => self.setting_objects().await,
            _ => Vec::new(),
        };
        out.sort_by(|a, b| a.reference.parent.cmp(&b.reference.parent).then_with(|| a.reference.name.cmp(&b.reference.name)));
        out.truncate(OBJECT_CAP);
        Ok(out)
    }

    async fn find_named(&self, items: Vec<Json>, name: &str, what: &str) -> AppResult<Json> {
        items.into_iter().find(|i| jtext(i, "name") == name).ok_or_else(|| AppError::not_found(format!("{what} `{name}` not found.")))
    }

    async fn measurement_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let bucket = reference.parent.clone().or_else(|| self.bucket.clone()).ok_or_else(|| AppError::invalid_input("A measurement needs its bucket."))?;
        let fields = self.keys(&bucket, &reference.name, "FieldKeys").await.unwrap_or_default();
        let tags: Vec<String> = self.keys(&bucket, &reference.name, "TagKeys").await.unwrap_or_default().into_iter().filter(|t| !t.starts_with('_')).collect();
        let script = format!(
            "from(bucket: {})\n  |> range(start: {DEFAULT_RANGE})\n  |> filter(fn: (r) => r._measurement == {})",
            flux_string(&bucket),
            flux_string(&reference.name)
        );
        let mut detail = ObjectDetail::empty(reference)
            .definition(script, CodeLanguage::Text)
            .property("bucket", bucket.clone())
            .property("fields", fields.len().to_string())
            .property("tags", tags.len().to_string());
        detail.rows = Some(key_rows(&fields, &tags));
        detail.columns = tags
            .iter()
            .map(|t| (t.clone(), "tag"))
            .chain(fields.iter().map(|f| (f.clone(), "field")))
            .enumerate()
            .map(|(i, (name, ty))| ColumnInfo { name, data_type: ty.into(), nullable: true, primary_key: false, ordinal: i as u32 + 1 })
            .collect();
        // `DROP MEASUREMENT` is InfluxQL, which `execute` routes at the session
        // bucket — so it is only offered when that is the bucket being viewed.
        if self.bucket.as_deref() == Some(bucket.as_str()) {
            detail = detail.action(ObjectAction::destructive("drop", "Drop measurement", format!("DROP MEASUREMENT {}", quote_ident(&reference.name))));
        }
        Ok(detail)
    }

    async fn bucket_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let bucket = self.find_named(self.bucket_objects().await, &reference.name, "Bucket").await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&bucket), CodeLanguage::Json).property("retention", retention_of(&bucket));
        detail = props(detail, &bucket, &["id", "orgID", "type", "description", "createdAt", "updatedAt", "schemaType"]);
        let mut children: Vec<ObjectSummary> = self
            .measurements(&reference.name)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|m| ObjectSummary::new(ObjectKind::Measurement, m, Some(reference.name.clone())))
            .collect();
        children.truncate(CHILD_CAP);
        detail.children = children;
        Ok(detail)
    }

    async fn org_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let orgs = self.orgs().await;
        if orgs.is_empty() {
            // v1: the "database" is its own container; list its measurements.
            let mut detail = ObjectDetail::empty(reference).definition(format!("SHOW MEASUREMENTS ON {}", quote_ident(&reference.name)), CodeLanguage::Text);
            detail.children = self
                .measurements(&reference.name)
                .await
                .unwrap_or_default()
                .into_iter()
                .take(CHILD_CAP)
                .map(|m| ObjectSummary::new(ObjectKind::Measurement, m, Some(reference.name.clone())))
                .collect();
            return Ok(detail);
        }
        let org = self.find_named(orgs, &reference.name, "Organization").await?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(&org), CodeLanguage::Json);
        detail = props(detail, &org, &["id", "description", "createdAt", "updatedAt"]);
        let org_id = jtext(&org, "id");
        detail.children = self
            .bucket_objects()
            .await
            .iter()
            .filter(|b| org_id.is_empty() || jtext(b, "orgID") == org_id)
            .map(|b| ObjectSummary::new(ObjectKind::Bucket, jtext(b, "name"), None).with_detail(format!("retention {}", retention_of(b))))
            .collect();
        Ok(detail)
    }

    async fn task_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let task = self.find_named(self.tasks().await, &reference.name, "Task").await?;
        let flux = jtext(&task, "flux");
        let mut detail = ObjectDetail::empty(reference).definition(flux, CodeLanguage::Text);
        detail = props(detail, &task, &["id", "status", "every", "cron", "offset", "orgID", "org", "createdAt", "updatedAt", "latestCompleted", "lastRunStatus", "lastRunError"]);
        Ok(detail)
    }

    async fn user_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let user = self.find_named(self.users().await, &reference.name, "User").await?;
        Ok(props(ObjectDetail::empty(reference).definition(pretty(&user), CodeLanguage::Json), &user, &["id", "status"]))
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        if let Some(cfg) = self.config().await {
            if let Some(v) = cfg.get(&reference.name) {
                let text = crate::integrations::prometheus::text_value(v);
                let structured = v.is_object() || v.is_array();
                let language = if structured { CodeLanguage::Json } else { CodeLanguage::Text };
                let body = if structured { pretty(v) } else { text.clone() };
                return Ok(ObjectDetail::empty(reference).definition(body, language).property("value", truncate(&text, 500)));
            }
        }
        let value = self
            .health_facts()
            .await
            .into_iter()
            .find(|(k, _)| *k == reference.name)
            .map(|(_, v)| v)
            .ok_or_else(|| AppError::not_found(format!("Setting `{}` not found.", reference.name)))?;
        Ok(ObjectDetail::empty(reference).definition(value.clone(), CodeLanguage::Text).property("value", value))
    }

    async fn detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Database => self.org_detail(reference).await,
            ObjectKind::Bucket => self.bucket_detail(reference).await,
            ObjectKind::Measurement => self.measurement_detail(reference).await,
            ObjectKind::Task => self.task_detail(reference).await,
            ObjectKind::User => self.user_detail(reference).await,
            ObjectKind::Setting => self.setting_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    // WHAT:  Flux scripts run through /api/v2/query with the dashboard `v`
    //        record injected; anything else is InfluxQL with the window appended.
    async fn range(&self, req: &RangeQueryRequest) -> AppResult<RangeResult> {
        let query = req.query.trim();
        if query.is_empty() {
            return Err(AppError::invalid_input("Enter a Flux or InfluxQL expression."));
        }
        if looks_like_flux(query) {
            let csv = self.flux(&flux_with_window(query, req.start, req.end, req.step_seconds)).await?;
            return Ok(RangeResult { series: csv_to_series(&csv), warnings: Vec::new() });
        }
        let sql = influxql_with_window(query, req.start, req.end);
        let body = self.influxql(&sql, None).await?;
        Ok(RangeResult { series: influxql_series(&body)?, warnings: Vec::new() })
    }

    async fn stats(&self) -> AppResult<ServerStats> {
        let health: Json = self.http.get_json("/health").await.unwrap_or(Json::Null);
        let metrics = self.http.get_text("/metrics").await.map(|t| parse_exposition(&t)).unwrap_or_default();
        if health.is_null() && metrics.is_empty() {
            return Err(AppError::driver("Neither /health nor /metrics answered."));
        }
        let now = chrono::Utc::now();

        let mut server = vec![Stat::text("API", if self.api == Api::V2 { "v2" } else { "v1" })];
        for (label, key) in [("Version", "version"), ("Status", "status"), ("Commit", "commit")] {
            let v = jtext(&health, key);
            if !v.is_empty() {
                server.push(Stat::text(label, truncate(&v, 40)));
            }
        }
        if let Some(started) = metrics.first("process_start_time_seconds") {
            server.push(Stat::text("Uptime", human_duration(now.timestamp() as f64 - started)));
        }
        if let Some(org) = &self.org {
            server.push(Stat::text("Org", org.clone()));
        }
        if let Some(b) = &self.bucket {
            server.push(Stat::text("Bucket", b.clone()));
        }

        let buckets = self.bucket_objects().await;
        let mut storage = Vec::new();
        if !buckets.is_empty() {
            storage.push(Stat::number("Buckets", buckets.len() as f64, None));
        }
        if let Some(bucket) = &self.bucket {
            if let Ok(m) = self.measurements(bucket).await {
                storage.push(Stat::number("Measurements", m.len() as f64, None).with_hint(format!("in {bucket}")));
            }
        }
        for (label, metric) in [("Bolt reads", "boltdb_reads_total"), ("Bolt writes", "boltdb_writes_total")] {
            if let Some(v) = metrics.sum(metric) {
                storage.push(Stat::number(label, v, None));
            }
        }
        if let Some(v) = metrics.sum("influxdb_info_uptime_seconds") {
            storage.push(Stat::text("Reported uptime", human_duration(v)));
        }

        let mut queries = Vec::new();
        for (label, metric) in [
            ("Query requests", "influxdb_http_query_request_count"),
            ("Query request bytes", "influxdb_http_query_request_bytes"),
            ("Query response bytes", "influxdb_http_query_response_bytes"),
            ("Write requests", "influxdb_http_write_request_count"),
            ("API requests", "http_api_requests_total"),
        ] {
            if let Some(v) = metrics.sum(metric) {
                queries.push(Stat::number(label, v, None));
            }
        }

        let mut tasks = Vec::new();
        for (label, metric) in [
            ("Scheduler total", "task_scheduler_total_execute_promises"),
            ("Scheduler failures", "task_scheduler_total_execute_failure"),
            ("Currently running", "task_scheduler_current_execution"),
            ("Workers busy", "task_scheduler_workers_busy"),
        ] {
            if let Some(v) = metrics.sum(metric) {
                tasks.push(Stat::number(label, v, None));
            }
        }
        let task_list = self.tasks().await;
        if !task_list.is_empty() {
            let active = task_list.iter().filter(|t| jtext(t, "status") == "active").count();
            tasks.push(Stat::number("Tasks", task_list.len() as f64, None));
            tasks.push(Stat::number("Tasks active", active as f64, None));
        }

        let mut memory = Vec::new();
        for (label, metric) in [
            ("RSS", "process_resident_memory_bytes"),
            ("Heap alloc", "go_memstats_alloc_bytes"),
            ("Heap in use", "go_memstats_heap_inuse_bytes"),
            ("Sys", "go_memstats_sys_bytes"),
        ] {
            if let Some(v) = metrics.first(metric) {
                memory.push(Stat::number(label, mib(v), Some("MB")));
            }
        }
        if let Some(v) = metrics.first("go_goroutines") {
            memory.push(Stat::number("Goroutines", v, None));
        }

        let groups = [("Server", server), ("Storage", storage), ("Queries", queries), ("Tasks", tasks), ("Memory", memory)]
            .into_iter()
            .filter(|(_, stats)| !stats.is_empty())
            .map(|(title, stats)| StatGroup { title: title.to_string(), stats })
            .collect();
        Ok(ServerStats::now(groups))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, sql: false, namespaces: true, fixed_columns: false, paging: true, row_estimate: false, views: false, transactions: false, exact_estimate: false },
        object_kinds: vec![K::Database, K::Bucket, K::Measurement, K::Task, K::User, K::Setting],
        tools: vec![T::Stats, T::MetricsExplorer],
    }
}

#[async_trait]
impl Integration for InfluxIntegration {
    fn engine(&self) -> Engine {
        Engine::Influxdb
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
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
                let idx = set.columns.iter().position(|c| c.name == "n").unwrap_or(0);
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

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        self.list_objects(kind, parent).await
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        self.detail(reference).await
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        self.stats().await
    }

    async fn query_range(&self, req: &RangeQueryRequest) -> AppResult<RangeResult> {
        self.range(req).await
    }
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
        assert!(count.ends_with("reduce(identity: {n: 0}, fn: (r, accumulator) => ({n: accumulator.n + 1}))\n"));
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

    #[test]
    fn annotated_csv_becomes_series_per_tag_set() {
        let csv = "#datatype,string,long,dateTime:RFC3339,double,string,string,string\n#group,false,false,false,false,true,true,true\n#default,_result,,,,,,\n,result,table,_time,_value,_measurement,_field,host\n,,0,2024-01-01T00:01:00Z,1.5,cpu,usage,h1\n,,0,2024-01-01T00:00:00Z,1,cpu,usage,h1\n,,1,2024-01-01T00:00:00Z,2,cpu,usage,h2\n";
        let series = csv_to_series(csv);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].name, "cpu.usage{host=\"h1\"}");
        // Points come back ascending even though the CSV listed them out of order.
        assert_eq!(series[0].points, vec![[1704067200.0, 1.0], [1704067260.0, 1.5]]);
        assert_eq!(series[1].name, "cpu.usage{host=\"h2\"}");
        assert_eq!(series[1].points.len(), 1);
        assert!(series[0].labels.iter().any(|l| l.name == "host" && l.value == "h1"));
        // A table with no _time / _value column contributes nothing.
        assert!(csv_to_series("a,b\n1,2\n").is_empty());
    }

    #[test]
    fn influxql_json_becomes_series_per_column() {
        let body = serde_json::json!({"results": [{"series": [
            {"name": "cpu", "tags": {"host": "h1"}, "columns": ["time", "usage", "load"], "values": [
                ["2024-01-01T00:00:00Z", 0.5, 3],
                ["2024-01-01T00:01:00Z", 0.7, null]
            ]}
        ]}]});
        let series = influxql_series(&body).unwrap_or_default();
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].name, "cpu.usage{host=\"h1\"}");
        assert_eq!(series[0].points, vec![[1704067200.0, 0.5], [1704067260.0, 0.7]]);
        assert_eq!(series[1].name, "cpu.load{host=\"h1\"}");
        assert_eq!(series[1].points, vec![[1704067200.0, 3.0]]);
        assert!(influxql_series(&serde_json::json!({"results": [{"error": "boom"}]})).is_err());
    }

    #[test]
    fn range_queries_carry_the_window() {
        let flux = flux_with_window("from(bucket: \"b\") |> range(start: v.timeRangeStart)", 1704067200.0, 1704070800.0, 60.0);
        assert!(flux.starts_with("v = {timeRangeStart: time(v: \"2024-01-01T00:00:00Z\"), timeRangeStop: time(v: \"2024-01-01T01:00:00Z\"), windowPeriod: 60s}\n"));
        assert!(flux.ends_with("|> range(start: v.timeRangeStart)"));
        assert!(looks_like_flux("from(bucket: \"b\")"));
        assert!(looks_like_flux("x |> mean()"));
        assert!(!looks_like_flux("SELECT * FROM cpu"));

        let sql = influxql_with_window("SELECT mean(usage) FROM cpu", 1704067200.0, 1704070800.0);
        assert_eq!(sql, "SELECT mean(usage) FROM cpu WHERE time >= '2024-01-01T00:00:00Z' AND time <= '2024-01-01T01:00:00Z'");
        let grouped = influxql_with_window("SELECT mean(usage) FROM cpu WHERE host = 'h1' GROUP BY time(1m)", 1704067200.0, 1704070800.0);
        assert_eq!(grouped, "SELECT mean(usage) FROM cpu WHERE host = 'h1' AND time >= '2024-01-01T00:00:00Z' AND time <= '2024-01-01T01:00:00Z' GROUP BY time(1m)");
        // A query that already bounds time is left untouched.
        assert_eq!(influxql_with_window("SELECT * FROM cpu WHERE time > now() - 1h;", 0.0, 1.0), "SELECT * FROM cpu WHERE time > now() - 1h");
    }

    #[test]
    fn stat_helpers_read_influx_shapes() {
        assert_eq!(retention_of(&serde_json::json!({"retentionRules": [{"everySeconds": 604800}]})), "7d 0h 0m");
        assert_eq!(retention_of(&serde_json::json!({"retentionRules": [{"everySeconds": 0}]})), "infinite");
        assert_eq!(retention_of(&serde_json::json!({})), "infinite");
        assert_eq!(task_schedule(&serde_json::json!({"every": "1h"})), "every 1h");
        assert_eq!(task_schedule(&serde_json::json!({"cron": "0 * * * *"})), "cron 0 * * * *");
        assert_eq!(task_schedule(&serde_json::json!({})), "");
        assert_eq!(time_seconds(&Value::DateTime("2024-01-01T00:00:00Z".into())), Some(1704067200.0));
        assert_eq!(time_seconds(&Value::Int(1_704_067_200_000_000_000)), Some(1704067200.0));
        assert_eq!(time_seconds(&Value::Int(1_704_067_200)), Some(1704067200.0));
        assert_eq!(time_seconds(&Value::Null), None);
        assert_eq!(value_number(&Value::Text("2.5".into())), Some(2.5));
        let rows = key_rows(&["usage".to_string()], &["host".to_string()]);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][0], Value::Text("tag".into()));
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
        // A unique measurement per run so a re-used bucket cannot skew the counts.
        let measurement = format!("dbfree_cpu_{now}");
        let lines = format!(
            "{measurement},host=h1 usage=0.5 {}\n{measurement},host=h2 usage=0.9 {}\n{measurement},host=h1 usage=0.7 {}",
            now - 3_000_000_000,
            now - 2_000_000_000,
            now - 1_000_000_000
        );
        http.post_raw(&format!("/api/v2/write?org={}&bucket={}&precision=ns", encode(&input.username.clone().unwrap_or_default()), encode(&bucket)), "text/plain", lines, None).await.unwrap_or_else(|e| panic!("write: {e}"));
        let cat = i.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas.iter().any(|s| s.tables.iter().any(|t| t.name == measurement)), "{cat:?}");
        let table = TableRef { schema: Some(bucket.clone()), name: measurement.clone() };
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
        let out = i.execute(&format!("from(bucket: \"{bucket}\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"{measurement}\")"), 100).await.unwrap_or_else(|e| panic!("flux: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 3, "{result:?}"),
            other => panic!("unexpected {other:?}"),
        }
        let out = i.execute(&format!("SELECT * FROM {measurement}"), 100).await.unwrap_or_else(|e| panic!("influxql: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 3, "{result:?}"),
            other => panic!("unexpected {other:?}"),
        }
    }
}
