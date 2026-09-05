// SOT: prometheus-integration, victoriametrics-integration, promql, prometheus-http-api, prometheus-instant-vector, prometheus-exposition-parser, prometheus-object-explorer, prometheus-server-stats, prometheus-range-query

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, local, objects_to_result_set, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectDetail, ObjectKind, ObjectProperty, ObjectRef,
    ObjectSummary, PageQuery, RangeQueryRequest, RangeResult, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    Series, ServerStats, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::{json, Map, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Prometheus / VictoriaMetrics adapter over the HTTP API (9090 / 8428).
//        Every metric name is a table under the `metrics` schema; a row is one
//        series of the instant vector (`labels… + timestamp + value`).
// WHY:   Both engines share `/api/v1/*`; VictoriaMetrics only differs in the
//        buildinfo shape and in accepting `limit=` on label lookups, so one
//        adapter serves `Engine::Prometheus` and `Engine::Victoriametrics`.
// HOW:   Filters on label columns become matchers inside the selector
//        (`metric{label="v"}`) for Eq / Ne / Contains-family (regex); anything
//        on `value` / `timestamp` and any sort runs locally. `execute` takes
//        PromQL text (instant), JSON `{"query","start","end","step"}` (range),
//        and the shorthands `LABELS`, `SERIES <match>`, `RANGE <q> <start> <end> <step>`.
//        Always read-only: the API has no write surface we expose.
// WHERE: src-tauri/src/integrations/mod.rs (trait), integrations/http.rs (client)
// ============================================================================

const SCHEMA: &str = "metrics";
const MAX_METRICS: usize = 5_000;
const SERIES_SAMPLE: usize = 100;
const LOCAL_CAP: usize = 10_000;

pub struct PrometheusIntegration {
    http: HttpClient,
    engine: Engine,
}

fn default_port(engine: Engine) -> u16 {
    if engine == Engine::Victoriametrics { 8428 } else { 9090 }
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let engine = conn.summary.engine;
    let auth = HttpClient::auth_from_connection(conn);
    let http = HttpClient::from_connection(conn, Some(default_port(engine)), false, auth)?;
    let integration = PrometheusIntegration { http, engine };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Selector / response shaping
// ---------------------------------------------------------------------------

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

fn label_string(raw: &str) -> String {
    format!("\"{}\"", raw.replace('\\', "\\\\").replace('"', "\\\""))
}

fn regex_escape(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if "\\^$.|?*+()[]{}".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

const BUILTIN: [&str; 3] = ["__name__", "timestamp", "value"];

/// Label matcher for a rule, or None when the rule must be applied locally.
fn matcher(rule: &FilterRule) -> Option<String> {
    if BUILTIN.contains(&rule.column.as_str()) {
        return None;
    }
    let l = &rule.column;
    let v = rule.value.trim();
    Some(match rule.op {
        FilterOp::Eq => format!("{l}={}", label_string(v)),
        FilterOp::Ne => format!("{l}!={}", label_string(v)),
        FilterOp::Contains => format!("{l}=~{}", label_string(&format!(".*{}.*", regex_escape(v)))),
        FilterOp::StartsWith => format!("{l}=~{}", label_string(&format!("{}.*", regex_escape(v)))),
        FilterOp::EndsWith => format!("{l}=~{}", label_string(&format!(".*{}", regex_escape(v)))),
        FilterOp::In => {
            let alts: Vec<String> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(regex_escape).collect();
            format!("{l}=~{}", label_string(&alts.join("|")))
        }
        FilterOp::IsNull => format!("{l}=\"\""),
        FilterOp::IsNotNull => format!("{l}!=\"\""),
        FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => return None,
    })
}

fn selector(metric: &str, filters: &[FilterRule]) -> (String, Vec<FilterRule>) {
    let mut matchers = Vec::new();
    let mut local_rules = Vec::new();
    for rule in filters {
        match matcher(rule) {
            Some(m) => matchers.push(m),
            None => local_rules.push(rule.clone()),
        }
    }
    let sel = if matchers.is_empty() { metric.to_string() } else { format!("{metric}{{{}}}", matchers.join(",")) };
    (sel, local_rules)
}

// WHAT:  A Prometheus sample value ("1", "0.7", "NaN", "+Inf") → JSON.
// WHY:   Integral samples (counters, `up`) must stay integers so the grid shows
//        `1`, not `1.0`; non-finite values keep their Prometheus spelling.
fn number_json(raw: &str) -> Json {
    if let Ok(i) = raw.parse::<i64>() {
        return Json::from(i);
    }
    match raw.parse::<f64>() {
        Ok(f) if f.is_finite() => serde_json::Number::from_f64(f).map(Json::Number).unwrap_or_else(|| Json::String(raw.to_string())),
        _ => Json::String(raw.to_string()),
    }
}

// WHAT:  One `vector` result item → flat object (labels + timestamp + value).
fn vector_item(item: &Json) -> Json {
    let mut obj = Map::new();
    if let Some(metric) = item.get("metric").and_then(Json::as_object) {
        if let Some(name) = metric.get("__name__") {
            obj.insert("__name__".into(), name.clone());
        }
        for (k, v) in metric {
            if k != "__name__" {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    if let Some(pair) = item.get("value").and_then(Json::as_array) {
        obj.insert("timestamp".into(), pair.first().cloned().unwrap_or(Json::Null));
        obj.insert("value".into(), pair.get(1).and_then(Json::as_str).map(number_json).unwrap_or(Json::Null));
    }
    Json::Object(obj)
}

// WHAT:  One `matrix` item → one object per sample.
fn matrix_items(item: &Json) -> Vec<Json> {
    let base = vector_item(item);
    item.get("values")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .map(|pair| {
            let mut obj = base.as_object().cloned().unwrap_or_default();
            let pair = pair.as_array().cloned().unwrap_or_default();
            obj.insert("timestamp".into(), pair.first().cloned().unwrap_or(Json::Null));
            obj.insert("value".into(), pair.get(1).and_then(Json::as_str).map(number_json).unwrap_or(Json::Null));
            Json::Object(obj)
        })
        .collect()
}

fn data_of(body: &Json) -> AppResult<&Json> {
    if body.get("status").and_then(Json::as_str) == Some("error") {
        let msg = body.get("error").and_then(Json::as_str).unwrap_or("query failed");
        return Err(AppError::driver(msg.to_string()));
    }
    body.get("data").ok_or_else(|| AppError::driver("Response has no data field."))
}

fn query_rows(body: &Json) -> AppResult<Vec<Json>> {
    let data = data_of(body)?;
    let result_type = data.get("resultType").and_then(Json::as_str).unwrap_or_default();
    let result = data.get("result");
    Ok(match result_type {
        "vector" => result.and_then(Json::as_array).into_iter().flatten().map(vector_item).collect(),
        "matrix" => result.and_then(Json::as_array).into_iter().flatten().flat_map(matrix_items).collect(),
        "scalar" | "string" => {
            let pair = result.and_then(Json::as_array).cloned().unwrap_or_default();
            vec![json!({"timestamp": pair.first().cloned().unwrap_or(Json::Null), "value": pair.get(1).and_then(Json::as_str).map(number_json).unwrap_or(Json::Null)})]
        }
        _ => result.and_then(Json::as_array).cloned().unwrap_or_default(),
    })
}

fn rows_to_result_set(rows: &[Json], max_rows: usize) -> ResultSet {
    let mut set = objects_to_result_set(rows, Some("__name__"), max_rows);
    // Drop the pinned `__name__` column when no row carries it (aggregations).
    if rows.iter().all(|r| r.get("__name__").is_none()) && !set.columns.is_empty() {
        set.columns.remove(0);
        for r in &mut set.rows {
            if !r.is_empty() {
                r.remove(0);
            }
        }
    }
    set
}

// ---------------------------------------------------------------------------
// Console
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Command {
    Instant(String),
    Range { query: String, start: String, end: String, step: String },
    Labels,
    Series(String),
}

fn parse_command(text: &str) -> AppResult<Command> {
    let t = text.trim();
    if t.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if t.starts_with('{') && t.ends_with('}') {
        if let Ok(v) = serde_json::from_str::<Json>(t) {
            if let Some(q) = v.get("query").and_then(Json::as_str) {
                let s = |k: &str| v.get(k).map(|x| match x { Json::String(s) => s.clone(), other => other.to_string() });
                return Ok(match (s("start"), s("end")) {
                    (Some(start), Some(end)) => Command::Range { query: q.to_string(), start, end, step: s("step").unwrap_or_else(|| "60s".into()) },
                    _ => Command::Instant(q.to_string()),
                });
            }
        }
    }
    let mut words = t.split_whitespace();
    let verb = words.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "LABELS" => Ok(Command::Labels),
        "SERIES" => {
            let m = t[6..].trim();
            if m.is_empty() {
                return Err(AppError::invalid_input("Usage: SERIES <selector>, e.g. SERIES up{job=\"api\"}"));
            }
            Ok(Command::Series(m.to_string()))
        }
        "RANGE" => {
            // RANGE <query…> <start> <end> <step>: the last three whitespace-separated words are times.
            let parts: Vec<&str> = t.split_whitespace().skip(1).collect();
            if parts.len() < 4 {
                return Err(AppError::invalid_input("Usage: RANGE <query> <start> <end> <step>, e.g. RANGE up -1h now 60s"));
            }
            let n = parts.len();
            let query = parts[..n - 3].join(" ");
            Ok(Command::Range { query, start: parts[n - 3].to_string(), end: parts[n - 2].to_string(), step: parts[n - 1].to_string() })
        }
        _ => Ok(Command::Instant(t.to_string())),
    }
}

// WHAT:  `now`, `-1h`-style relative offsets → unix seconds; RFC3339 / numbers pass through.
fn resolve_time(raw: &str) -> String {
    let t = raw.trim();
    let now = chrono::Utc::now().timestamp();
    if t.eq_ignore_ascii_case("now") {
        return now.to_string();
    }
    if let Some(rel) = t.strip_prefix('-') {
        if let Some(secs) = duration_secs(rel) {
            return (now - secs).to_string();
        }
    }
    t.to_string()
}

fn duration_secs(raw: &str) -> Option<i64> {
    let (num, unit) = raw.split_at(raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len()));
    let n: i64 = num.parse().ok()?;
    Some(match unit {
        "s" | "" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        "w" => n * 604_800,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl PrometheusIntegration {
    async fn instant(&self, query: &str) -> AppResult<Vec<Json>> {
        let body: Json = self.http.get_json(&format!("/api/v1/query?query={}", encode(query))).await?;
        query_rows(&body)
    }

    async fn run_command(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        let rows = match cmd {
            Command::Instant(q) => self.instant(&q).await?,
            Command::Range { query, start, end, step } => {
                let path = format!("/api/v1/query_range?query={}&start={}&end={}&step={}", encode(&query), encode(&resolve_time(&start)), encode(&resolve_time(&end)), encode(&step));
                let body: Json = self.http.get_json(&path).await?;
                query_rows(&body)?
            }
            Command::Labels => {
                let body: Json = self.http.get_json("/api/v1/labels").await?;
                let list = data_of(&body)?.clone();
                return Ok(StatementResult::Rows { result: json_result(list) });
            }
            Command::Series(m) => {
                let body: Json = self.http.get_json(&format!("/api/v1/series?match[]={}&limit={max_rows}", encode(&m))).await?;
                let list = data_of(&body)?.as_array().cloned().unwrap_or_default();
                return Ok(StatementResult::Rows { result: rows_to_result_set(&list, max_rows) });
            }
        };
        Ok(StatementResult::Rows { result: rows_to_result_set(&rows, max_rows) })
    }
}

// ---------------------------------------------------------------------------
// Prometheus text exposition (`/metrics`)
// ---------------------------------------------------------------------------

// WHAT:  One sample of the text exposition format: `name{k="v",…} value [ts]`.
// WHY:   Every Go-based engine (Prometheus, VictoriaMetrics, InfluxDB 2,
//        immudb) publishes its own figures this way, so the parser lives here
//        once and the InfluxDB / immudb adapters borrow it for their stats.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Sample {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Samples(pub Vec<Sample>);

pub(crate) fn parse_exposition(text: &str) -> Samples {
    let mut out = Vec::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, labels, rest) = match line.find('{') {
            Some(open) => {
                let Some(close) = closing_brace(&line[open..]) else { continue };
                let close = open + close;
                (&line[..open], parse_labels(&line[open + 1..close]), &line[close + 1..])
            }
            None => {
                let (n, r) = line.split_once(char::is_whitespace).unwrap_or((line, ""));
                (n, Vec::new(), r)
            }
        };
        let Some(value) = rest.split_whitespace().next().and_then(|v| v.parse::<f64>().ok()) else { continue };
        out.push(Sample { name: name.trim().to_string(), labels, value });
    }
    Samples(out)
}

// WHAT:  Byte index of the `}` closing the label block that `s` starts with,
//        skipping braces inside quoted label values.
fn closing_brace(s: &str) -> Option<usize> {
    let mut in_str = false;
    let mut esc = false;
    for (i, c) in s.char_indices() {
        if in_str {
            if esc {
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '}' => return Some(i),
            _ => {}
        }
    }
    None
}

fn parse_labels(raw: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut chars = raw.chars().peekable();
    loop {
        while matches!(chars.peek(), Some(c) if c.is_whitespace() || *c == ',') {
            chars.next();
        }
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' {
                break;
            }
            key.push(c);
            chars.next();
        }
        if key.is_empty() || chars.next() != Some('=') || chars.next() != Some('"') {
            break;
        }
        let mut value = String::new();
        let mut esc = false;
        for c in chars.by_ref() {
            if esc {
                value.push(if c == 'n' { '\n' } else { c });
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                break;
            } else {
                value.push(c);
            }
        }
        out.push((key.trim().to_string(), value));
    }
    out
}

impl Samples {
    /// Sum over every label set of `name`; None when the metric is absent.
    pub fn sum(&self, name: &str) -> Option<f64> {
        let mut seen = false;
        let mut total = 0.0;
        for s in self.0.iter().filter(|s| s.name == name) {
            seen = true;
            total += s.value;
        }
        seen.then_some(total)
    }

    pub fn first(&self, name: &str) -> Option<f64> {
        self.0.iter().find(|s| s.name == name).map(|s| s.value)
    }

    // Used by the parser tests only; kept next to the accessors they check.
    #[cfg(test)]
    pub fn count(&self, name: &str) -> usize {
        self.0.iter().filter(|s| s.name == name).count()
    }

    /// Value of `label` on the first sample of `name` (`influxdb_info{version=…}`).
    #[cfg(test)]
    pub fn label(&self, name: &str, label: &str) -> Option<String> {
        self.0.iter().find(|s| s.name == name)?.labels.iter().find(|(k, _)| k == label).map(|(_, v)| v.clone())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// ---- small formatting helpers shared with the InfluxDB / immudb adapters ----

pub(crate) fn human_duration(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3_600, (s % 3_600) / 60);
    if d > 0 {
        format!("{d}d {h}h {m}m")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {}s", s % 60)
    } else {
        format!("{s}s")
    }
}

pub(crate) fn human_bytes(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut v = bytes.max(0.0);
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} B", v as u64)
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

/// Bytes → mebibytes with one decimal, for `Stat::number(…, Some("MB"))`.
pub(crate) fn mib(bytes: f64) -> f64 {
    (bytes / 1_048_576.0 * 10.0).round() / 10.0
}

pub(crate) fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        format!("{}…", text.chars().take(max).collect::<String>())
    }
}

/// `obj[key]` as display text ("" when absent / null; JSON for non-strings).
pub(crate) fn jtext(v: &Json, key: &str) -> String {
    match v.get(key) {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

pub(crate) fn jnum(v: &Json, key: &str) -> Option<f64> {
    match v.get(key)? {
        Json::Number(n) => n.as_f64(),
        Json::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub(crate) fn text_value(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        Json::Null => String::new(),
        other => other.to_string(),
    }
}

pub(crate) fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Object explorer / stats / range query
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const DETAIL_SERIES: usize = 200;

// WHAT:  `metric{a="1",b="2"}` compact spelling of a label set. `name_key` is
//        the label that names it (`__name__` for series, `alertname` for alerts).
fn compact_name(labels: &Map<String, Json>, name_key: &str) -> String {
    let name = labels.get(name_key).and_then(Json::as_str).unwrap_or_default();
    let pairs: Vec<String> = labels
        .iter()
        .filter(|(k, _)| *k != name_key)
        .map(|(k, v)| format!("{k}=\"{}\"", text_value(v)))
        .collect();
    if pairs.is_empty() {
        name.to_string()
    } else {
        format!("{name}{{{}}}", pairs.join(","))
    }
}

fn label_props(labels: &Map<String, Json>) -> Vec<ObjectProperty> {
    labels.iter().map(|(k, v)| ObjectProperty { name: k.clone(), value: text_value(v) }).collect()
}

// WHAT:  `[ts, "value"]` → point; non-finite samples (NaN, ±Inf) are skipped
//        because JSON cannot carry them to the chart.
fn point(pair: &Json) -> Option<[f64; 2]> {
    let a = pair.as_array()?;
    let ts = a.first()?.as_f64()?;
    let v = a.get(1)?.as_str()?.parse::<f64>().ok().filter(|v| v.is_finite())?;
    Some([ts, v])
}

fn series_of(item: &Json, pairs: &[Json]) -> Series {
    let metric = item.get("metric").and_then(Json::as_object).cloned().unwrap_or_default();
    let mut points: Vec<[f64; 2]> = pairs.iter().filter_map(point).collect();
    points.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap_or(std::cmp::Ordering::Equal));
    Series { name: compact_name(&metric, "__name__"), labels: label_props(&metric), points }
}

// WHAT:  `/api/v1/query_range` body → one Series per result item.
pub(crate) fn range_result(body: &Json) -> AppResult<RangeResult> {
    let data = data_of(body)?;
    let warnings: Vec<String> = body.get("warnings").and_then(Json::as_array).into_iter().flatten().filter_map(|w| w.as_str().map(str::to_string)).collect();
    let items = data.get("result").and_then(Json::as_array).cloned().unwrap_or_default();
    let series = match data.get("resultType").and_then(Json::as_str).unwrap_or_default() {
        "matrix" => items.iter().map(|item| series_of(item, item.get("values").and_then(Json::as_array).map(Vec::as_slice).unwrap_or(&[]))).collect(),
        "vector" => items.iter().map(|item| series_of(item, std::slice::from_ref(item.get("value").unwrap_or(&Json::Null)))).collect(),
        "scalar" | "string" => {
            let pair = data.get("result").cloned().unwrap_or(Json::Null);
            vec![Series { name: "scalar".into(), labels: vec![], points: point(&pair).into_iter().collect() }]
        }
        _ => Vec::new(),
    };
    Ok(RangeResult { series, warnings })
}

// WHAT:  VictoriaMetrics prints `-flag=value` lines on `/flags`.
fn parse_flag_lines(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|l| {
            let l = l.trim().trim_start_matches('-');
            let (k, v) = l.split_once('=')?;
            let k = k.trim();
            (!k.is_empty() && !k.contains(' ')).then(|| (k.to_string(), v.trim().trim_matches('"').to_string()))
        })
        .collect()
}

fn parse_rfc3339(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw).ok().map(|t| t.with_timezone(&chrono::Utc))
}

fn format_ms(ms: f64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64).map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)).unwrap_or_default()
}

fn triples_result(names: [&str; 3], rows: Vec<(String, String, String)>) -> ResultSet {
    ResultSet {
        columns: names.iter().map(|n| ColumnMeta { name: (*n).to_string(), type_name: "string".into() }).collect(),
        rows: rows.into_iter().map(|(a, b, c)| vec![Value::Text(a), Value::Text(b), Value::Text(c)]).collect(),
        truncated: false,
    }
}

fn push_text(stats: &mut Vec<Stat>, label: &str, value: String) {
    if !value.is_empty() {
        stats.push(Stat::text(label, value));
    }
}

fn push_num(stats: &mut Vec<Stat>, label: &str, value: Option<f64>, unit: Option<&str>) {
    if let Some(v) = value {
        stats.push(Stat::number(label, v, unit));
    }
}

fn push_mib(stats: &mut Vec<Stat>, label: &str, bytes: Option<f64>) {
    push_num(stats, label, bytes.map(mib), Some("MB"));
}

impl PrometheusIntegration {
    async fn data(&self, path: &str) -> AppResult<Json> {
        let body: Json = self.http.get_json(path).await?;
        Ok(data_of(&body)?.clone())
    }

    /// Like `data`, but endpoints one engine lacks (404 on VictoriaMetrics) yield Null.
    async fn data_opt(&self, path: &str) -> Json {
        self.data(path).await.unwrap_or(Json::Null)
    }

    async fn metric_names(&self) -> AppResult<Vec<String>> {
        let path = if self.engine == Engine::Victoriametrics { format!("/api/v1/label/__name__/values?limit={MAX_METRICS}") } else { "/api/v1/label/__name__/values".to_string() };
        let data = self.data(&path).await?;
        let mut names: Vec<String> = data.as_array().into_iter().flatten().filter_map(|v| v.as_str().map(str::to_string)).collect();
        names.sort();
        names.truncate(MAX_METRICS);
        Ok(names)
    }

    async fn metric_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let names = self.metric_names().await?;
        let meta = self.data_opt("/api/v1/metadata").await;
        Ok(names
            .into_iter()
            .take(OBJECT_CAP)
            .map(|name| {
                let mut summary = ObjectSummary::new(ObjectKind::Metric, name.as_str(), None);
                if let Some(m) = meta.get(&name).and_then(Json::as_array).and_then(|a| a.first()) {
                    let kind = jtext(m, "type");
                    if !kind.is_empty() && kind != "unknown" {
                        summary = summary.with_badge(kind);
                    }
                    let help = jtext(m, "help");
                    if !help.is_empty() {
                        summary = summary.with_detail(truncate(&help, 120));
                    }
                }
                summary
            })
            .collect())
    }

    async fn active_targets(&self) -> Vec<Json> {
        self.data_opt("/api/v1/targets?state=active").await.get("activeTargets").and_then(Json::as_array).cloned().unwrap_or_default()
    }

    fn target_summary(t: &Json) -> ObjectSummary {
        let labels = t.get("labels").cloned().unwrap_or(Json::Null);
        let instance = jtext(&labels, "instance");
        let name = if instance.is_empty() { jtext(t, "scrapeUrl") } else { instance };
        let pool = jtext(t, "scrapePool");
        let pool = if pool.is_empty() { jtext(&labels, "job") } else { pool };
        let err = jtext(t, "lastError");
        let detail = if err.is_empty() { jtext(t, "scrapeUrl") } else { truncate(&err, 120) };
        let mut summary = ObjectSummary::new(ObjectKind::Target, name, Some(pool).filter(|p| !p.is_empty())).with_detail(detail);
        let health = jtext(t, "health");
        if !health.is_empty() {
            summary = summary.with_badge(health);
        }
        summary
    }

    async fn rule_groups(&self) -> Vec<Json> {
        self.data_opt("/api/v1/rules").await.get("groups").and_then(Json::as_array).cloned().unwrap_or_default()
    }

    fn rule_type(kind: ObjectKind) -> &'static str {
        if kind == ObjectKind::AlertRule {
            "alerting"
        } else {
            "recording"
        }
    }

    fn rule_summary(kind: ObjectKind, group: &str, rule: &Json) -> ObjectSummary {
        let mut summary = ObjectSummary::new(kind, jtext(rule, "name"), Some(group.to_string())).with_detail(truncate(&jtext(rule, "query"), 100));
        let badge = if kind == ObjectKind::AlertRule { jtext(rule, "state") } else { jtext(rule, "health") };
        if !badge.is_empty() {
            summary = summary.with_badge(badge);
        }
        summary
    }

    async fn rule_objects(&self, kind: ObjectKind, parent: Option<&str>) -> Vec<ObjectSummary> {
        let mut out = Vec::new();
        for group in self.rule_groups().await {
            let group_name = jtext(&group, "name");
            if parent.is_some_and(|p| p != group_name) {
                continue;
            }
            for rule in group.get("rules").and_then(Json::as_array).into_iter().flatten() {
                if jtext(rule, "type") == Self::rule_type(kind) {
                    out.push(Self::rule_summary(kind, &group_name, rule));
                }
            }
        }
        out
    }

    async fn alerts(&self) -> Vec<Json> {
        self.data_opt("/api/v1/alerts").await.get("alerts").and_then(Json::as_array).cloned().unwrap_or_default()
    }

    fn alert_summary(a: &Json) -> ObjectSummary {
        let labels = a.get("labels").and_then(Json::as_object).cloned().unwrap_or_default();
        let annotations = a.get("annotations").cloned().unwrap_or(Json::Null);
        let summary_text = jtext(&annotations, "summary");
        let detail = if summary_text.is_empty() { jtext(a, "activeAt") } else { summary_text };
        let mut summary = ObjectSummary::new(ObjectKind::Alert, compact_name(&labels, "alertname"), None).with_detail(truncate(&detail, 120));
        let state = jtext(a, "state");
        if !state.is_empty() {
            summary = summary.with_badge(state);
        }
        summary
    }

    async fn flags(&self) -> Vec<(String, String)> {
        let data = self.data_opt("/api/v1/status/flags").await;
        if let Some(map) = data.as_object() {
            return map.iter().map(|(k, v)| (k.clone(), text_value(v))).collect();
        }
        parse_flag_lines(&self.http.get_text("/flags").await.unwrap_or_default())
    }

    async fn config_yaml(&self) -> Option<String> {
        self.data_opt("/api/v1/status/config").await.get("yaml").and_then(Json::as_str).map(str::to_string)
    }

    async fn setting_objects(&self) -> Vec<ObjectSummary> {
        let mut out: Vec<ObjectSummary> = self
            .flags()
            .await
            .into_iter()
            .map(|(k, v)| ObjectSummary::new(ObjectKind::Setting, k, None).with_detail(truncate(&v, 120)).with_badge("flag"))
            .collect();
        if self.config_yaml().await.is_some() {
            out.push(ObjectSummary::new(ObjectKind::Setting, "config", None).with_detail("Loaded configuration (YAML)").with_badge("yaml"));
        }
        out
    }

    async fn list_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut out = match kind {
            ObjectKind::Metric => self.metric_objects().await?,
            ObjectKind::Target => self.active_targets().await.iter().map(Self::target_summary).collect(),
            ObjectKind::RecordingRule | ObjectKind::AlertRule => self.rule_objects(kind, parent).await,
            ObjectKind::Alert => self.alerts().await.iter().map(Self::alert_summary).collect(),
            ObjectKind::Setting => self.setting_objects().await,
            _ => Vec::new(),
        };
        out.sort_by(|a, b| a.reference.parent.cmp(&b.reference.parent).then_with(|| a.reference.name.cmp(&b.reference.name)));
        out.truncate(OBJECT_CAP);
        Ok(out)
    }

    async fn metric_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = &reference.name;
        let meta = self.data_opt(&format!("/api/v1/metadata?metric={}", encode(name))).await;
        let mut detail = ObjectDetail::empty(reference).definition(name.clone(), CodeLanguage::Text);
        if let Some(m) = meta.get(name).and_then(Json::as_array).and_then(|a| a.first()) {
            for key in ["type", "help", "unit"] {
                let v = jtext(m, key);
                if !v.is_empty() {
                    detail = detail.property(key, v);
                }
            }
        }
        let series = self.data(&format!("/api/v1/series?match[]={}&limit={DETAIL_SERIES}", encode(name))).await?;
        let mut label_sets: Vec<Json> = series.as_array().cloned().unwrap_or_default();
        for s in &mut label_sets {
            if let Some(o) = s.as_object_mut() {
                o.remove("__name__");
            }
        }
        let count = label_sets.len();
        detail = detail.property("series", if count >= DETAIL_SERIES { format!("{DETAIL_SERIES}+ (first {DETAIL_SERIES} shown)") } else { count.to_string() });
        detail.rows = Some(objects_to_result_set(&label_sets, None, DETAIL_SERIES));
        Ok(detail)
    }

    async fn target_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let targets = self.active_targets().await;
        let found = targets
            .iter()
            .find(|t| {
                let s = Self::target_summary(t);
                s.reference.name == reference.name && (reference.parent.is_none() || s.reference.parent == reference.parent)
            })
            .ok_or_else(|| AppError::not_found(format!("Target `{}` is not active.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(found), CodeLanguage::Json);
        for key in ["health", "scrapePool", "scrapeUrl", "globalUrl", "lastScrape", "lastScrapeDuration", "scrapeInterval", "scrapeTimeout", "lastError"] {
            let v = jtext(found, key);
            if !v.is_empty() {
                detail = detail.property(key, v);
            }
        }
        let mut rows = Vec::new();
        for (source, key) in [("label", "labels"), ("discovered", "discoveredLabels")] {
            for (k, v) in found.get(key).and_then(Json::as_object).into_iter().flatten() {
                rows.push((source.to_string(), k.clone(), text_value(v)));
            }
        }
        detail.rows = Some(triples_result(["source", "name", "value"], rows));
        Ok(detail)
    }

    async fn rule_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let kind = reference.kind;
        let (group, rule) = self
            .rule_groups()
            .await
            .into_iter()
            .find_map(|g| {
                let group_name = jtext(&g, "name");
                if reference.parent.as_deref().is_some_and(|p| p != group_name) {
                    return None;
                }
                let rule = g.get("rules").and_then(Json::as_array)?.iter().find(|r| jtext(r, "name") == reference.name && jtext(r, "type") == Self::rule_type(kind))?.clone();
                Some((g, rule))
            })
            .ok_or_else(|| AppError::not_found(format!("Rule `{}` not found.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference).definition(jtext(&rule, "query"), CodeLanguage::Text).property("group", jtext(&group, "name"));
        let file = jtext(&group, "file");
        if !file.is_empty() {
            detail = detail.property("file", file);
        }
        for key in ["type", "health", "state", "duration", "keepFiringFor", "evaluationTime", "lastEvaluation", "lastError"] {
            let v = jtext(&rule, key);
            if !v.is_empty() {
                detail = detail.property(key, v);
            }
        }
        let mut rows = Vec::new();
        for (source, key) in [("label", "labels"), ("annotation", "annotations")] {
            for (k, v) in rule.get(key).and_then(Json::as_object).into_iter().flatten() {
                rows.push((source.to_string(), k.clone(), text_value(v)));
            }
        }
        detail.rows = Some(triples_result(["kind", "name", "value"], rows));
        if kind == ObjectKind::AlertRule {
            detail.children = rule.get("alerts").and_then(Json::as_array).into_iter().flatten().map(Self::alert_summary).collect();
        }
        Ok(detail)
    }

    async fn alert_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let alerts = self.alerts().await;
        let found = alerts
            .iter()
            .find(|a| Self::alert_summary(a).reference.name == reference.name)
            .ok_or_else(|| AppError::not_found(format!("Alert `{}` is no longer active.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty(found), CodeLanguage::Json);
        for key in ["state", "activeAt", "value", "keepFiringSince"] {
            let v = jtext(found, key);
            if !v.is_empty() {
                detail = detail.property(key, v);
            }
        }
        let mut rows = Vec::new();
        for (source, key) in [("label", "labels"), ("annotation", "annotations")] {
            for (k, v) in found.get(key).and_then(Json::as_object).into_iter().flatten() {
                rows.push((source.to_string(), k.clone(), text_value(v)));
            }
        }
        detail.rows = Some(triples_result(["kind", "name", "value"], rows));
        Ok(detail)
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        if reference.name == "config" {
            let yaml = self.config_yaml().await.ok_or_else(|| AppError::not_found("The server does not expose its loaded configuration."))?;
            return Ok(ObjectDetail::empty(reference).definition(yaml, CodeLanguage::Text).property("source", "/api/v1/status/config"));
        }
        let value = self
            .flags()
            .await
            .into_iter()
            .find(|(k, _)| *k == reference.name)
            .map(|(_, v)| v)
            .ok_or_else(|| AppError::not_found(format!("Flag `{}` not found.", reference.name)))?;
        Ok(ObjectDetail::empty(reference).definition(format!("--{}={value}", reference.name), CodeLanguage::Text).property("value", value))
    }

    async fn detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Metric => self.metric_detail(reference).await,
            ObjectKind::Target => self.target_detail(reference).await,
            ObjectKind::RecordingRule | ObjectKind::AlertRule => self.rule_detail(reference).await,
            ObjectKind::Alert => self.alert_detail(reference).await,
            ObjectKind::Setting => self.setting_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn range(&self, req: &RangeQueryRequest) -> AppResult<RangeResult> {
        let step = req.step_seconds.max(1.0);
        let path = format!("/api/v1/query_range?query={}&start={}&end={}&step={}", encode(&req.query), req.start, req.end, step);
        let body: Json = self.http.get_json(&path).await?;
        range_result(&body)
    }

    // WHAT:  Server overview from the status endpoints plus the engine's own
    //        `/metrics`. Every source is optional: VictoriaMetrics lacks
    //        runtimeinfo / targets, Prometheus lacks the `vm_*` figures.
    async fn stats(&self) -> AppResult<ServerStats> {
        let build = self.data_opt("/api/v1/status/buildinfo").await;
        let runtime = self.data_opt("/api/v1/status/runtimeinfo").await;
        let tsdb = self.data_opt("/api/v1/status/tsdb").await;
        let targets = self.active_targets().await;
        let alerts = self.alerts().await;
        let metrics = self.http.get_text("/metrics").await.map(|t| parse_exposition(&t)).unwrap_or_default();
        if build.is_null() && metrics.is_empty() {
            return Err(AppError::driver("Neither /api/v1/status/buildinfo nor /metrics answered."));
        }
        let now = chrono::Utc::now();

        let mut server = Vec::new();
        push_text(&mut server, "Version", jtext(&build, "version"));
        push_text(&mut server, "Revision", truncate(&jtext(&build, "revision"), 12));
        push_text(&mut server, "Go", jtext(&build, "goVersion"));
        let uptime = parse_rfc3339(&jtext(&runtime, "startTime"))
            .map(|t| (now - t).num_seconds() as f64)
            .or_else(|| metrics.first("vm_app_uptime_seconds"))
            .or_else(|| metrics.first("process_start_time_seconds").map(|s| now.timestamp() as f64 - s));
        if let Some(u) = uptime {
            server.push(Stat::text("Uptime", human_duration(u)));
        }
        push_text(&mut server, "Retention", jtext(&runtime, "storageRetention"));
        push_num(&mut server, "Goroutines", jnum(&runtime, "goroutineCount").or_else(|| metrics.first("go_goroutines")), None);
        push_num(&mut server, "GOMAXPROCS", jnum(&runtime, "GOMAXPROCS"), None);
        match runtime.get("reloadConfigSuccess") {
            Some(Json::Bool(true)) => server.push(Stat::text("Config reload", "ok")),
            Some(Json::Bool(false)) => server.push(Stat::text("Config reload", "failed")),
            _ => {}
        }
        push_text(&mut server, "Last config", jtext(&runtime, "lastConfigTime"));

        let head = tsdb.get("headStats").cloned().unwrap_or(Json::Null);
        let mut storage = Vec::new();
        push_num(&mut storage, "Series", jnum(&head, "numSeries").or_else(|| jnum(&tsdb, "totalSeries")), None);
        push_num(&mut storage, "Label pairs", jnum(&head, "numLabelPairs").or_else(|| jnum(&tsdb, "totalLabelValuePairs")), None);
        push_num(&mut storage, "Head chunks", jnum(&head, "chunkCount"), None);
        if let Some(t) = jnum(&head, "minTime").filter(|t| *t > 0.0) {
            storage.push(Stat::text("Oldest sample", format_ms(t)));
        }
        if let Some(t) = jnum(&head, "maxTime").filter(|t| *t > 0.0) {
            storage.push(Stat::text("Newest sample", format_ms(t)));
        }
        push_num(&mut storage, "Samples appended", metrics.first("prometheus_tsdb_head_samples_appended_total"), None);
        push_mib(&mut storage, "Blocks on disk", metrics.first("prometheus_tsdb_storage_blocks_bytes"));
        push_num(&mut storage, "WAL segment", metrics.first("prometheus_tsdb_wal_segment_current"), None);
        push_num(&mut storage, "Rows", metrics.sum("vm_rows"), None);
        push_mib(&mut storage, "Data size", metrics.sum("vm_data_size_bytes"));
        push_mib(&mut storage, "Free disk", metrics.first("vm_free_disk_space_bytes"));

        let mut memory = Vec::new();
        push_mib(&mut memory, "RSS", metrics.first("process_resident_memory_bytes"));
        push_mib(&mut memory, "Heap alloc", metrics.first("go_memstats_alloc_bytes"));
        push_mib(&mut memory, "Heap in use", metrics.first("go_memstats_heap_inuse_bytes"));
        push_mib(&mut memory, "Sys", metrics.first("go_memstats_sys_bytes"));

        let mut cache = Vec::new();
        push_mib(&mut cache, "Cache size", metrics.sum("vm_cache_size_bytes"));
        push_num(&mut cache, "Cache entries", metrics.sum("vm_cache_entries"), None);

        let mut scrape = Vec::new();
        if !targets.is_empty() {
            let up = targets.iter().filter(|t| jtext(t, "health") == "up").count();
            let pools: std::collections::BTreeSet<String> = targets.iter().map(|t| jtext(t, "scrapePool")).collect();
            scrape.push(Stat::number("Targets up", up as f64, None));
            scrape.push(Stat::number("Targets down", (targets.len() - up) as f64, None));
            scrape.push(Stat::number("Scrape pools", pools.len() as f64, None));
        }
        push_num(&mut scrape, "Out-of-order samples", metrics.first("prometheus_target_scrapes_sample_out_of_order_total"), None);

        let mut queries = Vec::new();
        push_num(&mut queries, "Active queries", metrics.first("prometheus_engine_queries"), None);
        push_num(&mut queries, "Max concurrent", metrics.first("prometheus_engine_queries_concurrent_max"), None);
        push_num(&mut queries, "HTTP requests", metrics.sum("prometheus_http_requests_total").or_else(|| metrics.sum("vm_http_requests_total")), None);
        push_num(&mut queries, "Active merges", metrics.sum("vm_active_merges"), None);
        if !alerts.is_empty() {
            let firing = alerts.iter().filter(|a| jtext(a, "state") == "firing").count();
            queries.push(Stat::number("Alerts firing", firing as f64, None));
            queries.push(Stat::number("Alerts pending", (alerts.len() - firing) as f64, None));
        }

        let groups = [("Server", server), ("Storage", storage), ("Memory", memory), ("Cache", cache), ("Scraping", scrape), ("Queries", queries)]
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
        capabilities: Capabilities { describes_fields: true, sql: false, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true },
        object_kinds: vec![K::Metric, K::Target, K::RecordingRule, K::AlertRule, K::Alert, K::Setting],
        tools: vec![T::Stats, T::MetricsExplorer],
    }
}

#[async_trait]
impl Integration for PrometheusIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        // `/-/healthy` is plain text on both engines; VictoriaMetrics answers `/health` too.
        match self.http.get_text("/-/healthy").await {
            Ok(_) => Ok(()),
            Err(AppError::NotFound { .. }) => self.http.get_text("/health").await.map(|_| ()),
            Err(e) => Err(e),
        }
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let body: Json = self.http.get_json("/api/v1/status/buildinfo").await?;
        let data = body.get("data").unwrap_or(&body);
        let version = data.get("version").and_then(Json::as_str).unwrap_or("?");
        let name = if version.contains("victoria") || self.engine == Engine::Victoriametrics { "VictoriaMetrics" } else { "Prometheus" };
        Ok(Some(format!("{name} {version}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(SCHEMA.into())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![SCHEMA.into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let names = self.metric_names().await?;
        let tables = names.into_iter().map(|name| TableInfo { schema: Some(SCHEMA.into()), name, kind: TableKind::Table, row_estimate: None }).collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: SCHEMA.into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let body: Json = self.http.get_json(&format!("/api/v1/series?match[]={}&limit={SERIES_SAMPLE}", encode(&table.name))).await?;
        let mut labels: Vec<String> = Vec::new();
        for series in data_of(&body)?.as_array().into_iter().flatten() {
            for k in series.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()).unwrap_or_default() {
                if k != "__name__" && !labels.contains(&k) {
                    labels.push(k);
                }
            }
        }
        let mut cols = vec![
            ColumnInfo { name: "__name__".into(), data_type: "string".into(), nullable: false, primary_key: false, ordinal: 1 },
            ColumnInfo { name: "timestamp".into(), data_type: "number".into(), nullable: false, primary_key: true, ordinal: 2 },
            ColumnInfo { name: "value".into(), data_type: "float".into(), nullable: true, primary_key: false, ordinal: 3 },
        ];
        for l in labels {
            let ordinal = u32::try_from(cols.len() + 1).unwrap_or(u32::MAX);
            cols.push(ColumnInfo { name: l, data_type: "label".into(), nullable: true, primary_key: false, ordinal });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (sel, local_rules) = selector(&table.name, filters);
        if local_rules.is_empty() {
            let body: Json = self.http.get_json(&format!("/api/v1/series?match[]={}", encode(&sel))).await?;
            return Ok(data_of(&body)?.as_array().map(Vec::len).unwrap_or(0) as i64);
        }
        let rows = self.instant(&sel).await?;
        let set = rows_to_result_set(&rows, LOCAL_CAP);
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        Ok(local::apply_filters(&names, set.rows, &local_rules).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let (sel, local_rules) = selector(&table.name, &query.filters);
        let rows = self.instant(&sel).await?;
        let mut set = rows_to_result_set(&rows, LOCAL_CAP);
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery { sort: query.sort.clone(), filters: local_rules, offset: query.offset, limit: query.limit };
        set.rows = local::page(&names, set.rows, &local_query);
        set.truncated = false;
        Ok(set)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut results = Vec::new();
        for stmt in split_statements(sql) {
            results.push(self.run_command(parse_command(&stmt)?, max_rows).await?);
        }
        Ok(results)
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

fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.trim().is_empty() {
                out.push(std::mem::take(&mut current));
            }
        } else {
            current.push_str(line);
            current.push('\n');
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SortRule, SslMode, Value};

    #[test]
    fn selector_builds_matchers() {
        let f = |c: &str, op, v: &str| FilterRule { column: c.into(), op, value: v.into() };
        let (sel, local_rules) = selector("up", &[f("job", FilterOp::Eq, "api"), f("instance", FilterOp::Contains, "10.0"), f("value", FilterOp::Gt, "0"), f("env", FilterOp::In, "a,b")]);
        // PromQL label values are Go-style string literals: the regex `\.` has to be
        // written `\\.` inside the quotes to survive string unescaping.
        assert_eq!(sel, "up{job=\"api\",instance=~\".*10\\\\.0.*\",env=~\"a|b\"}");
        assert_eq!(local_rules.len(), 1);
        assert_eq!(local_rules[0].column, "value");
        assert_eq!(selector("up", &[]).0, "up");
        assert_eq!(matcher(&f("job", FilterOp::Ne, "x\"y")).as_deref(), Some("job!=\"x\\\"y\""));
        assert_eq!(matcher(&f("job", FilterOp::IsNull, "")).as_deref(), Some("job=\"\""));
    }

    #[test]
    fn vector_and_matrix_flatten() {
        let body = json!({"status": "success", "data": {"resultType": "vector", "result": [
            {"metric": {"__name__": "up", "job": "api"}, "value": [1700000000.1, "1"]},
            {"metric": {"__name__": "up", "job": "db", "extra": "x"}, "value": [1700000000.1, "NaN"]}
        ]}});
        let rows = query_rows(&body).unwrap_or_else(|e| panic!("{e}"));
        let set = rows_to_result_set(&rows, 10);
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["__name__", "job", "timestamp", "value", "extra"]);
        assert_eq!(set.rows[0][3], Value::Int(1));
        assert_eq!(set.rows[1][3], Value::Text("NaN".into()));
        let matrix = json!({"status": "success", "data": {"resultType": "matrix", "result": [
            {"metric": {"job": "api"}, "values": [[1, "0.5"], [2, "0.7"]]}
        ]}});
        let rows = query_rows(&matrix).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(rows.len(), 2);
        let set = rows_to_result_set(&rows, 10);
        assert_eq!(set.columns[0].name, "job");
        assert_eq!(set.rows[1][2], Value::Float(0.7));
        let scalar = json!({"status": "success", "data": {"resultType": "scalar", "result": [1, "3"]}});
        assert_eq!(query_rows(&scalar).unwrap_or_default().len(), 1);
        let err = json!({"status": "error", "errorType": "bad_data", "error": "parse error"});
        assert!(query_rows(&err).is_err());
    }

    #[test]
    fn console_parsing() {
        assert_eq!(parse_command("up").ok(), Some(Command::Instant("up".into())));
        assert_eq!(parse_command("sum(rate(http_requests_total[5m])) by (job)").ok(), Some(Command::Instant("sum(rate(http_requests_total[5m])) by (job)".into())));
        assert_eq!(parse_command("labels").ok(), Some(Command::Labels));
        assert_eq!(parse_command("SERIES up{job=\"a\"}").ok(), Some(Command::Series("up{job=\"a\"}".into())));
        assert_eq!(
            parse_command("RANGE rate(up[1m]) -1h now 30s").ok(),
            Some(Command::Range { query: "rate(up[1m])".into(), start: "-1h".into(), end: "now".into(), step: "30s".into() })
        );
        assert_eq!(
            parse_command("{\"query\": \"up\", \"start\": \"-1h\", \"end\": \"now\", \"step\": 15}").ok(),
            Some(Command::Range { query: "up".into(), start: "-1h".into(), end: "now".into(), step: "15".into() })
        );
        assert_eq!(parse_command("{\"query\": \"up\"}").ok(), Some(Command::Instant("up".into())));
        assert!(parse_command("RANGE up -1h").is_err());
        assert_eq!(duration_secs("90m"), Some(5400));
        assert_eq!(resolve_time("2024-01-01T00:00:00Z"), "2024-01-01T00:00:00Z");
        assert!(resolve_time("now").parse::<i64>().is_ok());
        assert!(resolve_time("-1h").parse::<i64>().map(|t| t < chrono::Utc::now().timestamp()).unwrap_or(false));
    }

    #[test]
    fn exposition_text_parses_labels_and_values() {
        let text = "# HELP up Scrape health.\n# TYPE up gauge\nup{job=\"api\",instance=\"a:1\"} 1\nup{job=\"db\",instance=\"b}2\",note=\"q\\\"x\"} 0 1700000000000\ngo_goroutines 42\nbad line here\nvm_rows{type=\"indexdb\"} 10\nvm_rows{type=\"storage/big\"} 5.5\n";
        let s = parse_exposition(text);
        assert_eq!(s.0.len(), 5);
        assert_eq!(s.sum("up"), Some(1.0));
        assert_eq!(s.count("up"), 2);
        assert_eq!(s.first("go_goroutines"), Some(42.0));
        assert_eq!(s.sum("vm_rows"), Some(15.5));
        assert_eq!(s.label("up", "instance").as_deref(), Some("a:1"));
        assert_eq!(s.0[1].labels[1], ("instance".to_string(), "b}2".to_string()));
        assert_eq!(s.0[1].labels[2], ("note".to_string(), "q\"x".to_string()));
        assert_eq!(s.sum("missing"), None);
        assert!(parse_exposition("").is_empty());
    }

    #[test]
    fn range_body_becomes_series() {
        let body = json!({"status": "success", "warnings": ["slow"], "data": {"resultType": "matrix", "result": [
            {"metric": {"__name__": "up", "job": "api"}, "values": [[1700000060, "1"], [1700000000, "0.5"], [1700000120, "NaN"]]},
            {"metric": {"job": "db"}, "values": []}
        ]}});
        let r = range_result(&body).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(r.warnings, vec!["slow"]);
        assert_eq!(r.series.len(), 2);
        assert_eq!(r.series[0].name, "up{job=\"api\"}");
        assert_eq!(r.series[0].labels.len(), 2);
        assert_eq!(r.series[0].points, vec![[1700000000.0, 0.5], [1700000060.0, 1.0]]);
        assert_eq!(r.series[1].name, "{job=\"db\"}");
        assert!(r.series[1].points.is_empty());
        let scalar = json!({"status": "success", "data": {"resultType": "scalar", "result": [1700000000, "3"]}});
        let r = range_result(&scalar).unwrap_or_default();
        assert_eq!(r.series[0].points, vec![[1700000000.0, 3.0]]);
        assert!(range_result(&json!({"status": "error", "error": "bad"})).is_err());
    }

    #[test]
    fn helpers_format_and_parse() {
        let mut labels = Map::new();
        labels.insert("alertname".into(), json!("HighLoad"));
        labels.insert("severity".into(), json!("page"));
        assert_eq!(compact_name(&labels, "alertname"), "HighLoad{severity=\"page\"}");
        assert_eq!(compact_name(&Map::new(), "__name__"), "");
        assert_eq!(parse_flag_lines("-httpListenAddr=\":8428\"\n-retentionPeriod=1\nnot a flag\n--web.enable-lifecycle=true"), vec![
            ("httpListenAddr".to_string(), ":8428".to_string()),
            ("retentionPeriod".to_string(), "1".to_string()),
            ("web.enable-lifecycle".to_string(), "true".to_string()),
        ]);
        assert_eq!(human_duration(90_061.0), "1d 1h 1m");
        assert_eq!(human_duration(125.0), "2m 5s");
        assert_eq!(human_duration(7.0), "7s");
        assert_eq!(human_bytes(512.0), "512 B");
        assert_eq!(human_bytes(1_572_864.0), "1.5 MB");
        assert_eq!(mib(1_048_576.0 * 2.25), 2.3);
        assert_eq!(truncate("abcdef", 3), "abc…");
        assert_eq!(jtext(&json!({"a": 1, "b": "x", "c": null}), "a"), "1");
        assert_eq!(jtext(&json!({"a": 1, "b": "x", "c": null}), "c"), "");
        assert_eq!(jnum(&json!({"n": "12.5"}), "n"), Some(12.5));
        assert_eq!(format_ms(0.0), "1970-01-01T00:00:00Z");
        let flags: Vec<ObjectSummary> = vec![ObjectSummary::new(ObjectKind::Setting, "x", None)];
        assert_eq!(flags[0].reference.kind, ObjectKind::Setting);
    }

    #[test]
    fn local_paging_sorts_by_value() {
        let rows = vec![json!({"__name__": "up", "job": "a", "timestamp": 1, "value": 3}), json!({"__name__": "up", "job": "b", "timestamp": 1, "value": 1})];
        let set = rows_to_result_set(&rows, 10);
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        let q = PageQuery { sort: vec![SortRule { column: "value".into(), desc: false }], filters: vec![], offset: 0, limit: 1 };
        let out = local::page(&names, set.rows, &q);
        assert_eq!(out[0][1], Value::Text("b".into()));
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_PROMETHEUS_URL is set.
    //        DBFREE_TEST_PROMETHEUS_VM=1 selects the VictoriaMetrics engine.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_PROMETHEUS_URL") else {
            return;
        };
        let engine = if std::env::var("DBFREE_TEST_PROMETHEUS_VM").is_ok() { Engine::Victoriametrics } else { Engine::Prometheus };
        let input = ConnectionInput {
            name: "live".into(),
            engine,
            environment: Environment::Local,
            read_only: true,
            host: Some(url),
            port: None,
            database: None,
            username: None,
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, false), secret: None };
        let p = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = p.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(!version.is_empty());
        let cat = p.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let metric = std::env::var("DBFREE_TEST_PROMETHEUS_METRIC").unwrap_or_else(|_| "up".into());
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == metric), "{:?}", cat.schemas[0].tables.iter().map(|t| &t.name).take(20).collect::<Vec<_>>());
        let table = TableRef { schema: Some(SCHEMA.into()), name: metric.clone() };
        let cols = p.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "value"));
        let page = p.fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 }).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert!(!page.rows.is_empty(), "{page:?}");
        assert!(p.count(&table, &[]).await.unwrap_or_default() >= 1);
        let out = p.execute(&format!("RANGE {metric} -5m now 60s"), 100).await.unwrap_or_else(|e| panic!("range: {e}"));
        assert!(matches!(&out[0], StatementResult::Rows { result } if !result.rows.is_empty()));
        let out = p.execute("LABELS", 100).await.unwrap_or_else(|e| panic!("labels: {e}"));
        assert!(matches!(&out[0], StatementResult::Rows { .. }));
    }
}
