// SOT: prometheus-integration, victoriametrics-integration, promql, prometheus-http-api, prometheus-instant-vector

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, local, objects_to_result_set, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, StatementResult, TableInfo, TableKind, TableRef,
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

#[async_trait]
impl Integration for PrometheusIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true }
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
        let path = if self.engine == Engine::Victoriametrics { format!("/api/v1/label/__name__/values?limit={MAX_METRICS}") } else { "/api/v1/label/__name__/values".to_string() };
        let body: Json = self.http.get_json(&path).await?;
        let mut names: Vec<String> = data_of(&body)?.as_array().into_iter().flatten().filter_map(|v| v.as_str().map(str::to_string)).collect();
        names.sort();
        names.truncate(MAX_METRICS);
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
