// SOT: bigquery-integration, bigquery-rest-api, google-service-account-jwt, bigquery-row-decoding, bigquery-object-explorer, bigquery-jobs-api

use crate::error::{AppError, AppResult};
use crate::integrations::gcp_auth::GcpAuth;
use crate::integrations::http::{Auth, HttpClient};
use crate::integrations::sql::{order_clause, validate_columns, where_clause};
use crate::integrations::{qualified_name_for, quote_ident_for, Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat, StatGroup,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================================
// WHAT:  Google BigQuery adapter over the REST API v2. `database` = project id
//        (or the service account's), `username` = optional dataset filter.
// WHY:   `jobs.query` covers everything: it runs SQL synchronously up to a
//        timeout and hands back a jobId to poll otherwise.
// HOW:   Datasets are schemas; tables/views come from `tables.list`. Rows are
//        decoded from the `f[].v` shape by the schema's field types (nested
//        RECORD / REPEATED → JSON). Identifiers are backtick-quoted as
//        `project.dataset.table`. DML returns numDmlAffectedRows.
// WHERE: src-tauri/src/integrations/gcp_auth.rs, src-tauri/src/integrations/sql.rs
// ============================================================================

const SCOPE: &str = "https://www.googleapis.com/auth/bigquery";
const QUERY_TIMEOUT_MS: u64 = 30_000;
const POLL_BUDGET: Duration = Duration::from_secs(60);

pub struct BigQueryIntegration {
    engine: Engine,
    http: HttpClient,
    auth: GcpAuth,
    project: String,
    dataset: Option<String>,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let auth = GcpAuth::from_connection(conn, SCOPE)?;
    if auth.is_anonymous() {
        return Err(AppError::invalid_input("BigQuery needs a service-account JSON key or an access token."));
    }
    let project = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .or_else(|| auth.project_hint.clone())
        .ok_or_else(|| AppError::invalid_input("BigQuery needs a project id (database field) or a service-account key that names one."))?;
    let dataset = s.username.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let base = s.host.as_deref().map(str::trim).filter(|h| h.starts_with("http")).unwrap_or("https://bigquery.googleapis.com").to_string();
    let http = HttpClient::new(base, Auth::None, false)?;
    let integration = BigQueryIntegration { engine: s.engine, http, auth, project, dataset };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Row decoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub kind: String,
    pub repeated: bool,
    pub fields: Vec<Field>,
}

pub fn parse_fields(schema: &serde_json::Value) -> Vec<Field> {
    schema
        .get("fields")
        .and_then(|f| f.as_array())
        .into_iter()
        .flatten()
        .map(|f| Field {
            name: f.get("name").and_then(|n| n.as_str()).unwrap_or_default().to_string(),
            kind: f.get("type").and_then(|t| t.as_str()).unwrap_or("STRING").to_uppercase(),
            repeated: f.get("mode").and_then(|m| m.as_str()) == Some("REPEATED"),
            fields: parse_fields(f),
        })
        .collect()
}

fn timestamp_text(raw: &str) -> String {
    raw.parse::<f64>()
        .ok()
        .and_then(|secs| chrono::DateTime::from_timestamp_micros((secs * 1_000_000.0).round() as i64))
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        .unwrap_or_else(|| raw.to_string())
}

// WHAT:  One cell (`{"v": …}` unwrapped) as plain JSON, following the field type.
fn cell_json(field: &Field, v: &serde_json::Value) -> serde_json::Value {
    if field.repeated {
        let items = v.as_array().map(|a| a.iter().map(|i| i.get("v").unwrap_or(i)).map(|i| scalar_json(field, i)).collect()).unwrap_or_default();
        return serde_json::Value::Array(items);
    }
    scalar_json(field, v)
}

fn scalar_json(field: &Field, v: &serde_json::Value) -> serde_json::Value {
    if v.is_null() {
        return serde_json::Value::Null;
    }
    match field.kind.as_str() {
        "RECORD" | "STRUCT" => {
            let cells = v.get("f").and_then(|f| f.as_array()).cloned().unwrap_or_default();
            serde_json::Value::Object(field.fields.iter().zip(cells.iter()).map(|(sub, c)| (sub.name.clone(), cell_json(sub, c.get("v").unwrap_or(c)))).collect())
        }
        "INTEGER" | "INT64" => v.as_str().and_then(|s| s.parse::<i64>().ok()).map(|i| serde_json::Value::Number(i.into())).unwrap_or(v.clone()),
        "FLOAT" | "FLOAT64" => v.as_str().and_then(|s| s.parse::<f64>().ok()).and_then(serde_json::Number::from_f64).map(serde_json::Value::Number).unwrap_or(v.clone()),
        "BOOLEAN" | "BOOL" => serde_json::Value::Bool(v.as_str().map(|s| s == "true").or_else(|| v.as_bool()).unwrap_or(false)),
        "TIMESTAMP" => serde_json::Value::String(v.as_str().map(timestamp_text).unwrap_or_default()),
        "JSON" => v.as_str().and_then(|s| serde_json::from_str(s).ok()).unwrap_or(v.clone()),
        _ => v.clone(),
    }
}

pub fn cell_value(field: &Field, v: &serde_json::Value) -> Value {
    if field.repeated || matches!(field.kind.as_str(), "RECORD" | "STRUCT" | "JSON") {
        let j = cell_json(field, v);
        return if j.is_null() { Value::Null } else { Value::Json(j) };
    }
    let Some(s) = v.as_str() else {
        return match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            other => Value::Text(other.to_string()),
        };
    };
    match field.kind.as_str() {
        "INTEGER" | "INT64" => s.parse().map(Value::Int).unwrap_or_else(|_| Value::Decimal(s.to_string())),
        "FLOAT" | "FLOAT64" => s.parse().map(Value::Float).unwrap_or_else(|_| Value::Text(s.to_string())),
        "NUMERIC" | "BIGNUMERIC" | "DECIMAL" | "BIGDECIMAL" => Value::Decimal(s.to_string()),
        "BOOLEAN" | "BOOL" => Value::Bool(s == "true"),
        "BYTES" => Value::Bytes(s.to_string()),
        "TIMESTAMP" => Value::DateTime(timestamp_text(s)),
        "DATE" | "DATETIME" | "TIME" => Value::DateTime(s.to_string()),
        _ => Value::Text(s.to_string()),
    }
}

pub fn type_name(field: &Field) -> String {
    let base = match field.kind.as_str() {
        "INTEGER" => "INT64".to_string(),
        "FLOAT" => "FLOAT64".to_string(),
        "BOOLEAN" => "BOOL".to_string(),
        "RECORD" => "STRUCT".to_string(),
        other => other.to_string(),
    };
    if field.repeated { format!("ARRAY<{base}>") } else { base }
}

pub fn rows_to_result(schema: &serde_json::Value, rows: &[serde_json::Value], max_rows: usize) -> ResultSet {
    let fields = parse_fields(schema);
    let truncated = rows.len() > max_rows;
    let rows = rows
        .iter()
        .take(max_rows)
        .map(|r| {
            let cells = r.get("f").and_then(|f| f.as_array()).cloned().unwrap_or_default();
            fields.iter().enumerate().map(|(i, f)| cells.get(i).map(|c| cell_value(f, c.get("v").unwrap_or(c))).unwrap_or(Value::Null)).collect()
        })
        .collect();
    let columns = fields.iter().map(|f| ColumnMeta { name: f.name.clone(), type_name: type_name(f) }).collect();
    ResultSet { columns, rows, truncated }
}

// WHAT:  Flattens RECORD fields to dotted names for the column list; the grid
//        still receives the whole struct as JSON in the parent column.
fn schema_columns(fields: &[Field]) -> Vec<ColumnInfo> {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| ColumnInfo { name: f.name.clone(), data_type: type_name(f), nullable: true, primary_key: false, ordinal: i as u32 + 1 })
        .collect()
}

#[derive(Debug)]
struct QueryOutcome {
    schema: serde_json::Value,
    rows: Vec<serde_json::Value>,
    affected: Option<u64>,
    has_schema: bool,
}

impl BigQueryIntegration {
    fn api(&self, path: &str) -> String {
        format!("/bigquery/v2/projects/{}{path}", self.project)
    }

    async fn req(&self, method: Method, path: &str, body: Option<serde_json::Value>) -> AppResult<serde_json::Value> {
        let mut r = self.http.request(method, path);
        if let Auth::Bearer(t) = self.auth.bearer().await? {
            r = r.bearer_auth(t);
        }
        if let Some(b) = body {
            r = r.json(&b);
        }
        let resp = self.http.send(r).await?;
        resp.json().await.map_err(|e| AppError::driver(format!("Malformed BigQuery response: {e}")))
    }

    async fn query(&self, sql: &str, max_results: usize) -> AppResult<QueryOutcome> {
        let body = serde_json::json!({"query": sql, "useLegacySql": false, "maxResults": max_results.clamp(1, 10_000), "timeoutMs": QUERY_TIMEOUT_MS});
        let mut resp = self.req(Method::POST, &self.api("/queries"), Some(body)).await?;
        let started = Instant::now();
        while resp.get("jobComplete").and_then(|c| c.as_bool()) == Some(false) {
            if started.elapsed() > POLL_BUDGET {
                return Err(AppError::timeout("BigQuery job did not complete within 60 s."));
            }
            let job_id = resp.pointer("/jobReference/jobId").and_then(|j| j.as_str()).ok_or_else(|| AppError::driver("BigQuery returned no jobId for an incomplete job."))?.to_string();
            let location = resp.pointer("/jobReference/location").and_then(|l| l.as_str()).unwrap_or_default().to_string();
            tokio::time::sleep(Duration::from_millis(1000)).await;
            resp = self.req(Method::GET, &format!("{}?location={location}&maxResults={}&timeoutMs={QUERY_TIMEOUT_MS}", self.api(&format!("/queries/{job_id}")), max_results.clamp(1, 10_000)), None).await?;
        }
        if let Some(errs) = resp.get("errors").and_then(|e| e.as_array()).filter(|e| !e.is_empty()) {
            let msg = errs.iter().filter_map(|e| e.get("message").and_then(|m| m.as_str())).collect::<Vec<_>>().join("; ");
            return Err(AppError::driver(msg));
        }
        let mut rows: Vec<serde_json::Value> = resp.get("rows").and_then(|r| r.as_array()).cloned().unwrap_or_default();
        let mut token = resp.get("pageToken").and_then(|t| t.as_str()).map(str::to_string);
        while let Some(t) = token.take() {
            if rows.len() >= max_results {
                break;
            }
            let job_id = resp.pointer("/jobReference/jobId").and_then(|j| j.as_str()).unwrap_or_default().to_string();
            let location = resp.pointer("/jobReference/location").and_then(|l| l.as_str()).unwrap_or_default().to_string();
            let page = self.req(Method::GET, &format!("{}?location={location}&pageToken={t}&maxResults={}", self.api(&format!("/queries/{job_id}")), max_results.clamp(1, 10_000)), None).await?;
            rows.extend(page.get("rows").and_then(|r| r.as_array()).cloned().unwrap_or_default());
            token = page.get("pageToken").and_then(|t| t.as_str()).map(str::to_string);
        }
        Ok(QueryOutcome {
            has_schema: resp.get("schema").is_some(),
            schema: resp.get("schema").cloned().unwrap_or(serde_json::json!({})),
            rows,
            affected: resp.get("numDmlAffectedRows").and_then(|n| n.as_str()).and_then(|s| s.parse().ok()),
        })
    }

    fn table_ref(&self, table: &TableRef) -> String {
        let dataset = table.schema.clone().or_else(|| self.dataset.clone()).unwrap_or_default();
        format!("{}.{}", quote_ident_for(Engine::Bigquery, &self.project), qualified_name_for(Engine::Bigquery, &TableRef { schema: Some(dataset), name: table.name.clone() }))
    }

    async fn table_meta(&self, table: &TableRef) -> AppResult<serde_json::Value> {
        let dataset = table.schema.clone().or_else(|| self.dataset.clone()).ok_or_else(|| AppError::invalid_input("A dataset is required."))?;
        self.req(Method::GET, &self.api(&format!("/datasets/{dataset}/tables/{}", table.name)), None).await
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Datasets, tables / views / materialized views, routines (functions
//        and procedures) and recent jobs, from the REST API's list endpoints.
// WHY:   The generic explorer / admin UI. `datasets.list` / `tables.list` /
//        `routines.list` are cheap and paginated; the expensive per-object
//        `get` (row counts, definition bodies) is only issued for the detail.
// HOW:   The pure decoders below turn each endpoint's JSON into summaries and
//        are unit-tested offline; the async methods only fetch and page.
//        Actions are SQL DDL, so they run back through `execute` and the
//        guard's read-only lock and destructive confirmation apply.
// ---------------------------------------------------------------------------

const MAX_OBJECTS: usize = 2_000;
const MAX_JOBS: usize = 100;

fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", value as u64, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn json_text(v: Option<&serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

/// BigQuery returns 64-bit counters as strings.
fn json_num(v: Option<&serde_json::Value>) -> Option<f64> {
    match v {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

fn pointer_text(v: &serde_json::Value, pointer: &str) -> String {
    json_text(v.pointer(pointer))
}

/// `tables.list` entry type → explorer kind.
fn table_object_kind(kind: &str) -> ObjectKind {
    match kind {
        "VIEW" => ObjectKind::View,
        "MATERIALIZED_VIEW" => ObjectKind::MaterializedView,
        _ => ObjectKind::Table,
    }
}

fn dataset_summary(entry: &serde_json::Value) -> Option<ObjectSummary> {
    let id = entry.pointer("/datasetReference/datasetId")?.as_str()?.to_string();
    let mut summary = ObjectSummary::new(ObjectKind::Dataset, id, None);
    let location = json_text(entry.get("location"));
    if !location.is_empty() {
        summary = summary.with_badge(location);
    }
    let description = json_text(entry.get("friendlyName"));
    if !description.is_empty() {
        summary = summary.with_detail(description);
    }
    Some(summary)
}

fn table_summary(entry: &serde_json::Value, kind: ObjectKind) -> Option<ObjectSummary> {
    let name = entry.pointer("/tableReference/tableId")?.as_str()?.to_string();
    let dataset = entry.pointer("/tableReference/datasetId").and_then(|d| d.as_str()).map(str::to_string);
    let mut summary = ObjectSummary::new(kind, name, dataset).with_badge(json_text(entry.get("type")).to_ascii_lowercase());
    let description = json_text(entry.get("friendlyName"));
    let partitioning = entry.get("timePartitioning").map(|p| format!("partitioned by {}", json_text(p.get("type")).to_ascii_lowercase()));
    match (description.is_empty(), partitioning) {
        (false, _) => summary = summary.with_detail(description),
        (true, Some(p)) => summary = summary.with_detail(p),
        (true, None) => {}
    }
    Some(summary)
}

fn routine_summary(entry: &serde_json::Value) -> Option<(ObjectKind, ObjectSummary)> {
    let name = entry.pointer("/routineReference/routineId")?.as_str()?.to_string();
    let dataset = entry.pointer("/routineReference/datasetId").and_then(|d| d.as_str()).map(str::to_string);
    let routine_type = json_text(entry.get("routineType"));
    let kind = if routine_type == "PROCEDURE" { ObjectKind::Procedure } else { ObjectKind::Function };
    let mut summary = ObjectSummary::new(kind, name, dataset).with_badge(routine_type.to_ascii_lowercase().replace('_', " "));
    let language = json_text(entry.get("language"));
    if !language.is_empty() {
        summary = summary.with_detail(language);
    }
    Some((kind, summary))
}

/// `jobs.list` entry → a Job summary; the state and statement type are badges.
fn job_summary(entry: &serde_json::Value) -> Option<ObjectSummary> {
    let id = entry.pointer("/jobReference/jobId")?.as_str()?.to_string();
    let state = pointer_text(entry, "/status/state");
    let statement = pointer_text(entry, "/statistics/query/statementType");
    let bytes = json_num(entry.pointer("/statistics/query/totalBytesProcessed"));
    let mut parts = Vec::new();
    if !statement.is_empty() {
        parts.push(statement);
    }
    if let Some(b) = bytes {
        parts.push(format_bytes(b));
    }
    let (start, end) = (json_num(entry.pointer("/statistics/startTime")), json_num(entry.pointer("/statistics/endTime")));
    if let (Some(s), Some(e)) = (start, end) {
        if e >= s {
            parts.push(format!("{:.1}s", (e - s) / 1000.0));
        }
    }
    if entry.pointer("/status/errorResult").is_some() {
        parts.push(pointer_text(entry, "/status/errorResult/message"));
    }
    let mut summary = ObjectSummary::new(ObjectKind::Job, id, None).with_badge(state.to_ascii_lowercase());
    let caption = parts.into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>().join(" · ");
    if !caption.is_empty() {
        summary = summary.with_detail(caption);
    }
    Some(summary)
}

/// Scalar fields of a JSON object as properties, byte counters humanised.
fn json_properties(mut detail: ObjectDetail, obj: &serde_json::Value, skip: &[&str]) -> ObjectDetail {
    for (k, v) in obj.as_object().into_iter().flatten() {
        if skip.contains(&k.as_str()) || v.is_null() || v.is_object() || v.is_array() {
            continue;
        }
        detail = detail.property(k, json_text(Some(v)));
        if k.ends_with("Bytes") {
            if let Some(bytes) = json_num(Some(v)) {
                detail = detail.property(&format!("{k} (human)"), format_bytes(bytes));
            }
        }
    }
    detail
}

impl BigQueryIntegration {
    async fn list_pages(&self, path: &str, collection: &str, cap: usize) -> AppResult<Vec<serde_json::Value>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let sep = if path.contains('?') { "&" } else { "?" };
            let url = match &token {
                Some(t) => format!("{path}{sep}pageToken={t}"),
                None => path.to_string(),
            };
            let resp = self.req(Method::GET, &url, None).await?;
            for item in resp.get(collection).and_then(|d| d.as_array()).into_iter().flatten() {
                out.push(item.clone());
            }
            match resp.get("nextPageToken").and_then(|t| t.as_str()) {
                Some(t) if out.len() < cap => token = Some(t.to_string()),
                _ => break,
            }
        }
        out.truncate(cap);
        Ok(out)
    }

    async fn dataset_ids(&self) -> AppResult<Vec<String>> {
        if let Some(d) = &self.dataset {
            return Ok(vec![d.clone()]);
        }
        let entries = self.list_pages(&self.api("/datasets?maxResults=200"), "datasets", MAX_OBJECTS).await?;
        Ok(entries.iter().filter_map(|d| d.pointer("/datasetReference/datasetId").and_then(|i| i.as_str()).map(str::to_string)).collect())
    }

    async fn list_dataset_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let entries = self.list_pages(&self.api("/datasets?maxResults=200"), "datasets", MAX_OBJECTS).await?;
        Ok(entries.iter().filter_map(dataset_summary).collect())
    }

    async fn list_table_objects(&self, kind: ObjectKind, dataset: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let datasets = match dataset {
            Some(d) => vec![d.to_string()],
            None => self.dataset_ids().await?,
        };
        let mut out = Vec::new();
        for ds in datasets {
            let entries = self.list_pages(&self.api(&format!("/datasets/{ds}/tables?maxResults=500")), "tables", MAX_OBJECTS).await.unwrap_or_default();
            for entry in &entries {
                if table_object_kind(&json_text(entry.get("type"))) == kind {
                    if let Some(summary) = table_summary(entry, kind) {
                        out.push(summary);
                    }
                }
            }
            if out.len() >= MAX_OBJECTS {
                break;
            }
        }
        Ok(out)
    }

    async fn list_routine_objects(&self, kind: ObjectKind, dataset: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let datasets = match dataset {
            Some(d) => vec![d.to_string()],
            None => self.dataset_ids().await?,
        };
        let mut out = Vec::new();
        for ds in datasets {
            let entries = self.list_pages(&self.api(&format!("/datasets/{ds}/routines?maxResults=500")), "routines", MAX_OBJECTS).await.unwrap_or_default();
            for entry in &entries {
                if let Some((routine_kind, summary)) = routine_summary(entry) {
                    if routine_kind == kind {
                        out.push(summary);
                    }
                }
            }
            if out.len() >= MAX_OBJECTS {
                break;
            }
        }
        Ok(out)
    }

    async fn job_entries(&self) -> AppResult<Vec<serde_json::Value>> {
        self.list_pages(&self.api(&format!("/jobs?projection=full&allUsers=true&maxResults={MAX_JOBS}")), "jobs", MAX_JOBS).await
    }

    async fn list_job_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        Ok(self.job_entries().await?.iter().filter_map(job_summary).collect())
    }

    async fn dataset_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ds = &reference.name;
        let info = self.req(Method::GET, &self.api(&format!("/datasets/{ds}")), None).await?;
        let mut detail = json_properties(ObjectDetail::empty(reference), &info, &["etag", "selfLink"])
            .definition(serde_json::to_string_pretty(&info).unwrap_or_default(), CodeLanguage::Json);
        let tables = self.list_pages(&self.api(&format!("/datasets/{ds}/tables?maxResults=500")), "tables", MAX_OBJECTS).await.unwrap_or_default();
        detail.children = tables
            .iter()
            .filter_map(|entry| {
                let kind = table_object_kind(&json_text(entry.get("type")));
                table_summary(entry, kind)
            })
            .collect();
        let name = format!("{}.{}", quote_ident_for(Engine::Bigquery, &self.project), quote_ident_for(Engine::Bigquery, ds));
        Ok(detail.action(ObjectAction::destructive("drop", "Drop schema (dataset)", format!("DROP SCHEMA {name}"))))
    }

    async fn table_object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let table = TableRef { schema: reference.parent.clone(), name: reference.name.clone() };
        let meta = self.table_meta(&table).await?;
        let mut detail = json_properties(ObjectDetail::empty(reference), &meta, &["etag", "selfLink", "id"]);
        if let Some(bytes) = json_num(meta.pointer("/numBytes")) {
            detail = detail.property("size", format_bytes(bytes));
        }
        if let Some(view) = meta.pointer("/view/query").and_then(|q| q.as_str()) {
            detail = detail.definition(view.to_string(), CodeLanguage::Sql);
        } else if let Some(mview) = meta.pointer("/materializedView/query").and_then(|q| q.as_str()) {
            detail = detail.definition(mview.to_string(), CodeLanguage::Sql);
        } else if let Ok(Some(ddl)) = self.ddl(&table).await {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.columns = schema_columns(&parse_fields(meta.get("schema").unwrap_or(&serde_json::json!({}))));
        let name = self.table_ref(&table);
        match reference.kind {
            ObjectKind::View => detail = detail.action(ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {name}"))),
            ObjectKind::MaterializedView => {
                detail = detail
                    .action(ObjectAction::new("refresh", "Refresh materialized view", format!("CALL BQ.REFRESH_MATERIALIZED_VIEW('{}')", self.dotted_name(&table))))
                    .action(ObjectAction::destructive("drop", "Drop materialized view", format!("DROP MATERIALIZED VIEW {name}")));
            }
            _ => {
                detail = detail
                    .action(ObjectAction::destructive("truncate", "Truncate table", format!("TRUNCATE TABLE {name}")))
                    .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {name}")));
            }
        }
        Ok(detail)
    }

    async fn routine_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let dataset = reference.parent.clone().or_else(|| self.dataset.clone()).ok_or_else(|| AppError::invalid_input("A dataset is required."))?;
        let info = self.req(Method::GET, &self.api(&format!("/datasets/{dataset}/routines/{}", reference.name)), None).await?;
        let mut detail = json_properties(ObjectDetail::empty(reference), &info, &["etag", "selfLink", "definitionBody"]);
        let body = json_text(info.get("definitionBody"));
        if !body.is_empty() {
            let language = json_text(info.get("language"));
            let code = if language == "SQL" || language.is_empty() { CodeLanguage::Sql } else { CodeLanguage::Text };
            detail = detail.definition(body, code);
        }
        detail.columns = info
            .get("arguments")
            .and_then(|a| a.as_array())
            .into_iter()
            .flatten()
            .enumerate()
            .map(|(i, arg)| ColumnInfo {
                name: json_text(arg.get("name")),
                data_type: json_text(arg.pointer("/dataType/typeKind")),
                nullable: true,
                primary_key: false,
                ordinal: i as u32 + 1,
            })
            .collect();
        let keyword = if reference.kind == ObjectKind::Procedure { "PROCEDURE" } else { "FUNCTION" };
        let name = format!(
            "{}.{}.{}",
            quote_ident_for(Engine::Bigquery, &self.project),
            quote_ident_for(Engine::Bigquery, &dataset),
            quote_ident_for(Engine::Bigquery, &reference.name)
        );
        let label = format!("Drop {}", keyword.to_ascii_lowercase());
        Ok(detail.action(ObjectAction::destructive("drop", &label, format!("DROP {keyword} {name}"))))
    }

    async fn job_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let info = self.req(Method::GET, &self.api(&format!("/jobs/{}", reference.name)), None).await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(query) = info.pointer("/configuration/query/query").and_then(|q| q.as_str()) {
            detail = detail.definition(query.to_string(), CodeLanguage::Sql);
        } else {
            detail = detail.definition(serde_json::to_string_pretty(&info).unwrap_or_default(), CodeLanguage::Json);
        }
        detail = detail.property("state", pointer_text(&info, "/status/state")).property("user", json_text(info.get("user_email")));
        if let Some(stats) = info.get("statistics") {
            detail = json_properties(detail, stats, &[]);
            if let Some(query_stats) = stats.get("query") {
                detail = json_properties(detail, query_stats, &[]);
            }
        }
        if let Some(error) = info.pointer("/status/errorResult") {
            detail = detail.property("error", json_text(error.get("message")));
        }
        let state = pointer_text(&info, "/status/state");
        if state != "DONE" {
            let location = pointer_text(&info, "/jobReference/location");
            let path = format!("/jobs/{}/cancel?location={location}", reference.name);
            detail = detail.action(ObjectAction::destructive("cancel", "Cancel job", format!("-- Cancel via the REST API: POST {}", self.api(&path))));
        }
        Ok(detail)
    }

    /// `project.dataset.table` without quoting, for string arguments like BQ.REFRESH_MATERIALIZED_VIEW.
    fn dotted_name(&self, table: &TableRef) -> String {
        let dataset = table.schema.clone().or_else(|| self.dataset.clone()).unwrap_or_default();
        format!("{}.{dataset}.{}", self.project, table.name)
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, transactions: false, exact_estimate: false, ..Capabilities::SQL },
        object_kinds: vec![K::Dataset, K::Table, K::View, K::MaterializedView, K::Function, K::Procedure, K::Job],
        tools: vec![T::Stats],
    }
}

#[async_trait]
impl Integration for BigQueryIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.req(Method::GET, &self.api("/datasets?maxResults=1"), None).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some("BigQuery".into()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.project.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.project.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let datasets: Vec<String> = match &self.dataset {
            Some(d) => vec![d.clone()],
            None => {
                let mut out = Vec::new();
                let mut token: Option<String> = None;
                loop {
                    let path = match &token {
                        Some(t) => self.api(&format!("/datasets?maxResults=200&pageToken={t}")),
                        None => self.api("/datasets?maxResults=200"),
                    };
                    let resp = self.req(Method::GET, &path, None).await?;
                    for d in resp.get("datasets").and_then(|d| d.as_array()).into_iter().flatten() {
                        if let Some(id) = d.pointer("/datasetReference/datasetId").and_then(|i| i.as_str()) {
                            out.push(id.to_string());
                        }
                    }
                    match resp.get("nextPageToken").and_then(|t| t.as_str()) {
                        Some(t) if out.len() < 2_000 => token = Some(t.to_string()),
                        _ => break,
                    }
                }
                out
            }
        };
        let mut schemas = Vec::new();
        for ds in datasets {
            let mut tables = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let path = match &token {
                    Some(t) => self.api(&format!("/datasets/{ds}/tables?maxResults=500&pageToken={t}")),
                    None => self.api(&format!("/datasets/{ds}/tables?maxResults=500")),
                };
                let resp = match self.req(Method::GET, &path, None).await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                for t in resp.get("tables").and_then(|t| t.as_array()).into_iter().flatten() {
                    let Some(name) = t.pointer("/tableReference/tableId").and_then(|i| i.as_str()) else { continue };
                    let kind = match t.get("type").and_then(|k| k.as_str()).unwrap_or("TABLE") {
                        "VIEW" | "MATERIALIZED_VIEW" => TableKind::View,
                        _ => TableKind::Table,
                    };
                    tables.push(TableInfo { schema: Some(ds.clone()), name: name.to_string(), kind, row_estimate: None });
                }
                match resp.get("nextPageToken").and_then(|t| t.as_str()) {
                    Some(t) if tables.len() < 5_000 => token = Some(t.to_string()),
                    _ => break,
                }
            }
            schemas.push(SchemaInfo { name: ds, tables });
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let meta = self.table_meta(table).await?;
        let fields = parse_fields(meta.get("schema").unwrap_or(&serde_json::json!({})));
        Ok(schema_columns(&fields))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let meta = self.table_meta(table).await?;
        Ok(meta.get("numRows").and_then(|n| n.as_str()).and_then(|s| s.parse().ok()))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT COUNT(*) AS n FROM {}{}", self.table_ref(table), where_clause(Engine::Bigquery, filters));
        let out = self.query(&sql, 1).await?;
        let rs = rows_to_result(&out.schema, &out.rows, 1);
        Ok(match rs.rows.first().and_then(|r| r.first()) {
            Some(Value::Int(i)) => *i,
            _ => 0,
        })
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            self.table_ref(table),
            where_clause(Engine::Bigquery, &query.filters),
            order_clause(Engine::Bigquery, &query.sort),
            query.limit,
            query.offset
        );
        let out = self.query(&sql, query.limit as usize).await?;
        Ok(rows_to_result(&out.schema, &out.rows, query.limit as usize))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut results = Vec::new();
        for stmt in crate::guard::destructive::split_statements(sql) {
            let out = self.query(&stmt, max_rows).await?;
            if let Some(n) = out.affected {
                results.push(StatementResult::Affected { rows_affected: n });
            } else if out.has_schema {
                results.push(StatementResult::Rows { result: rows_to_result(&out.schema, &out.rows, max_rows) });
            } else {
                results.push(StatementResult::Affected { rows_affected: 0 });
            }
        }
        Ok(results)
    }

    async fn close(&self) {}

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let dataset = table.schema.clone().or_else(|| self.dataset.clone()).ok_or_else(|| AppError::invalid_input("A dataset is required."))?;
        let sql = format!(
            "SELECT ddl FROM {}.{}.INFORMATION_SCHEMA.TABLES WHERE table_name = '{}'",
            quote_ident_for(Engine::Bigquery, &self.project),
            quote_ident_for(Engine::Bigquery, &dataset),
            table.name.replace('\'', "\\'")
        );
        let out = self.query(&sql, 1).await?;
        let rs = rows_to_result(&out.schema, &out.rows, 1);
        Ok(match rs.rows.first().and_then(|r| r.first()) {
            Some(Value::Text(t)) => Some(t.clone()),
            _ => None,
        })
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut out = match kind {
            ObjectKind::Dataset => self.list_dataset_objects().await?,
            ObjectKind::Table | ObjectKind::View | ObjectKind::MaterializedView => self.list_table_objects(kind, parent).await?,
            ObjectKind::Function | ObjectKind::Procedure => self.list_routine_objects(kind, parent).await?,
            ObjectKind::Job => self.list_job_objects().await?,
            _ => Vec::new(),
        };
        if kind != ObjectKind::Job {
            out.sort_by(|a, b| a.reference.parent.cmp(&b.reference.parent).then(a.reference.name.cmp(&b.reference.name)));
        }
        out.truncate(MAX_OBJECTS);
        Ok(out)
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Dataset => self.dataset_detail(reference).await,
            ObjectKind::Table | ObjectKind::View | ObjectKind::MaterializedView => self.table_object_detail(reference).await,
            ObjectKind::Function | ObjectKind::Procedure => self.routine_detail(reference).await,
            ObjectKind::Job => self.job_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    // WHAT:  Project totals: datasets and tables from the list endpoints, job
    //        states and bytes processed from the last page of `jobs.list`.
    async fn server_stats(&self) -> AppResult<ServerStats> {
        let datasets = self.dataset_ids().await.unwrap_or_default();
        let mut tables = 0usize;
        let mut views = 0usize;
        for ds in datasets.iter().take(50) {
            let entries = self.list_pages(&self.api(&format!("/datasets/{ds}/tables?maxResults=500")), "tables", MAX_OBJECTS).await.unwrap_or_default();
            for entry in &entries {
                match table_object_kind(&json_text(entry.get("type"))) {
                    ObjectKind::Table => tables += 1,
                    _ => views += 1,
                }
            }
        }
        let jobs = self.job_entries().await.unwrap_or_default();
        let mut by_state: BTreeMap<String, f64> = BTreeMap::new();
        let mut bytes = 0f64;
        let mut failed = 0f64;
        for job in &jobs {
            *by_state.entry(pointer_text(job, "/status/state").to_ascii_lowercase()).or_insert(0.0) += 1.0;
            bytes += json_num(job.pointer("/statistics/query/totalBytesProcessed")).unwrap_or(0.0);
            if job.pointer("/status/errorResult").is_some() {
                failed += 1.0;
            }
        }
        let mut job_stats = vec![Stat::number("Jobs (last page)", jobs.len() as f64, None).with_hint(format!("newest {MAX_JOBS} jobs"))];
        job_stats.extend(by_state.iter().filter(|(s, _)| !s.is_empty()).map(|(state, count)| Stat::number(state, *count, None)));
        job_stats.push(Stat::number("Failed", failed, None));
        let groups = vec![
            StatGroup {
                title: "Server".into(),
                stats: vec![
                    Stat::text("Service", "BigQuery"),
                    Stat::text("Project", self.project.clone()),
                    Stat::text("Dataset filter", self.dataset.clone().unwrap_or_else(|| "all".into())),
                ],
            },
            StatGroup {
                title: "Storage".into(),
                stats: vec![
                    Stat::number("Datasets", datasets.len() as f64, None),
                    Stat::number("Tables", tables as f64, None),
                    Stat::number("Views", views as f64, None),
                ],
            },
            StatGroup { title: "Queries".into(), stats: job_stats },
            StatGroup {
                title: "Throughput".into(),
                stats: vec![Stat::number("Bytes processed", (bytes / 1_048_576.0 * 10.0).round() / 10.0, Some("MB"))
                    .with_hint(format!("{} across the last {} jobs", format_bytes(bytes), jobs.len()))],
            },
        ];
        Ok(ServerStats::now(groups))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> serde_json::Value {
        serde_json::json!({"fields": [
            {"name": "id", "type": "INTEGER"},
            {"name": "price", "type": "NUMERIC"},
            {"name": "ok", "type": "BOOLEAN"},
            {"name": "ts", "type": "TIMESTAMP"},
            {"name": "tags", "type": "STRING", "mode": "REPEATED"},
            {"name": "addr", "type": "RECORD", "fields": [{"name": "city", "type": "STRING"}, {"name": "zip", "type": "INTEGER"}]},
            {"name": "j", "type": "JSON"},
            {"name": "ratio", "type": "FLOAT"},
            {"name": "raw", "type": "BYTES"}
        ]})
    }

    #[test]
    fn decodes_rows_by_type() {
        let rows = vec![serde_json::json!({"f": [
            {"v": "7"}, {"v": "12.50"}, {"v": "true"}, {"v": "1.7040672E9"},
            {"v": [{"v": "a"}, {"v": "b"}]},
            {"v": {"f": [{"v": "Paris"}, {"v": "75001"}]}},
            {"v": "{\"k\": [1]}"}, {"v": "0.25"}, {"v": "AQID"}
        ]})];
        let rs = rows_to_result(&schema(), &rows, 10);
        assert_eq!(rs.columns.iter().map(|c| c.type_name.as_str()).collect::<Vec<_>>(), vec!["INT64", "NUMERIC", "BOOL", "TIMESTAMP", "ARRAY<STRING>", "STRUCT", "JSON", "FLOAT64", "BYTES"]);
        assert_eq!(rs.rows[0][0], Value::Int(7));
        assert_eq!(rs.rows[0][1], Value::Decimal("12.50".into()));
        assert_eq!(rs.rows[0][2], Value::Bool(true));
        assert_eq!(rs.rows[0][3], Value::DateTime("2024-01-01T00:00:00Z".into()));
        assert_eq!(rs.rows[0][4], Value::Json(serde_json::json!(["a", "b"])));
        assert_eq!(rs.rows[0][5], Value::Json(serde_json::json!({"city": "Paris", "zip": 75001})));
        assert_eq!(rs.rows[0][6], Value::Json(serde_json::json!({"k": [1]})));
        assert_eq!(rs.rows[0][7], Value::Float(0.25));
        assert_eq!(rs.rows[0][8], Value::Bytes("AQID".into()));
        let nulls = vec![serde_json::json!({"f": [{"v": null}, {"v": null}, {"v": null}, {"v": null}, {"v": []}, {"v": null}, {"v": null}, {"v": null}, {"v": null}]})];
        let rs = rows_to_result(&schema(), &nulls, 10);
        assert_eq!(rs.rows[0][0], Value::Null);
        assert_eq!(rs.rows[0][4], Value::Json(serde_json::json!([])));
        assert_eq!(rs.rows[0][5], Value::Null);
    }

    #[test]
    fn truncates() {
        let rows = vec![serde_json::json!({"f": [{"v": "1"}]}), serde_json::json!({"f": [{"v": "2"}]})];
        let rs = rows_to_result(&serde_json::json!({"fields": [{"name": "a", "type": "INT64"}]}), &rows, 1);
        assert!(rs.truncated);
        assert_eq!(rs.rows.len(), 1);
    }

    #[test]
    fn list_entries_become_summaries() {
        let ds = serde_json::json!({"datasetReference": {"datasetId": "analytics", "projectId": "p"}, "location": "EU", "friendlyName": "Analytics"});
        let s = dataset_summary(&ds).unwrap_or_else(|| panic!("dataset"));
        assert_eq!(s.reference.name, "analytics");
        assert_eq!(s.badge.as_deref(), Some("EU"));
        assert_eq!(s.detail.as_deref(), Some("Analytics"));
        assert!(dataset_summary(&serde_json::json!({})).is_none());

        assert_eq!(table_object_kind("TABLE"), ObjectKind::Table);
        assert_eq!(table_object_kind("VIEW"), ObjectKind::View);
        assert_eq!(table_object_kind("MATERIALIZED_VIEW"), ObjectKind::MaterializedView);
        let t = serde_json::json!({"tableReference": {"datasetId": "analytics", "tableId": "events"}, "type": "TABLE", "timePartitioning": {"type": "DAY"}});
        let s = table_summary(&t, ObjectKind::Table).unwrap_or_else(|| panic!("table"));
        assert_eq!(s.reference.parent.as_deref(), Some("analytics"));
        assert_eq!(s.badge.as_deref(), Some("table"));
        assert_eq!(s.detail.as_deref(), Some("partitioned by day"));

        let r = serde_json::json!({"routineReference": {"datasetId": "analytics", "routineId": "udf"}, "routineType": "SCALAR_FUNCTION", "language": "SQL"});
        let (kind, s) = routine_summary(&r).unwrap_or_else(|| panic!("routine"));
        assert_eq!(kind, ObjectKind::Function);
        assert_eq!(s.badge.as_deref(), Some("scalar function"));
        let p = serde_json::json!({"routineReference": {"datasetId": "a", "routineId": "proc"}, "routineType": "PROCEDURE"});
        assert_eq!(routine_summary(&p).map(|(k, _)| k), Some(ObjectKind::Procedure));

        let job = serde_json::json!({
            "jobReference": {"jobId": "job_1"},
            "status": {"state": "DONE"},
            "statistics": {"startTime": "1000", "endTime": "3500", "query": {"statementType": "SELECT", "totalBytesProcessed": "1572864"}}
        });
        let s = job_summary(&job).unwrap_or_else(|| panic!("job"));
        assert_eq!(s.reference.name, "job_1");
        assert_eq!(s.badge.as_deref(), Some("done"));
        assert_eq!(s.detail.as_deref(), Some("SELECT · 1.5 MB · 2.5s"));
        let failed = serde_json::json!({"jobReference": {"jobId": "j2"}, "status": {"state": "DONE", "errorResult": {"message": "boom"}}});
        assert!(job_summary(&failed).and_then(|s| s.detail).is_some_and(|d| d.contains("boom")));
    }

    #[test]
    fn json_helpers_read_string_encoded_numbers() {
        assert_eq!(json_num(Some(&serde_json::json!("1572864"))), Some(1_572_864.0));
        assert_eq!(json_num(Some(&serde_json::json!(42))), Some(42.0));
        assert_eq!(json_num(Some(&serde_json::json!("x"))), None);
        assert_eq!(json_num(None), None);
        assert_eq!(json_text(Some(&serde_json::json!("a"))), "a");
        assert_eq!(json_text(Some(&serde_json::Value::Null)), "");
        assert_eq!(json_text(Some(&serde_json::json!(3))), "3");
        assert_eq!(pointer_text(&serde_json::json!({"a": {"b": "c"}}), "/a/b"), "c");
        assert_eq!(format_bytes(0.0), "0 B");
        assert_eq!(format_bytes(1_073_741_824.0), "1.0 GB");
        let detail = json_properties(
            ObjectDetail::empty(&ObjectRef { kind: ObjectKind::Table, name: "t".into(), parent: None }),
            &serde_json::json!({"numBytes": "2048", "etag": "x", "nested": {"a": 1}}),
            &["etag"],
        );
        let names: Vec<&str> = detail.properties.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["numBytes", "numBytes (human)"]);
        assert_eq!(detail.properties[1].value, "2.0 KB");
    }

    #[test]
    fn timestamps_render_rfc3339() {
        assert_eq!(timestamp_text("1.7040672E9"), "2024-01-01T00:00:00Z");
        assert_eq!(timestamp_text("1704067200.5"), "2024-01-01T00:00:00.500Z");
        assert_eq!(timestamp_text("garbage"), "garbage");
    }
}
