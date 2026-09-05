// SOT: druid-integration, druid-sql-api, druid-native-query, druid-information-schema, druid-object-explorer, druid-coordinator-api, druid-rest-passthrough

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::http::{json_result, objects_to_result_set, HttpClient};
use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{qualified_name_for, Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat, StatGroup,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;
use std::sync::Arc;

// ============================================================================
// WHAT:  Apache Druid adapter over the Router (port 8888): Druid SQL through
//        `POST /druid/v2/sql` and native JSON queries through `POST /druid/v2`.
// WHY:   Druid exposes a full INFORMATION_SCHEMA, so catalog / columns are
//        plain SQL; identifiers are double-quoted (ANSI), which the shared
//        clause builders already produce for `Engine::Druid`.
// HOW:   `execute` sends SQL as `{query, resultFormat: "object", header: false}`
//        and decodes the array of objects; a body starting with `{"queryType"`
//        goes to the native endpoint and is shown verbatim (or as rows when the
//        result is a list of objects / `{timestamp, result}` pairs). `__time`
//        is reported as the primary key so rows are addressable. A body of the
//        form `{"method", "path", "body"}` is forwarded to that REST endpoint
//        (coordinator / overlord APIs, which the router proxies); anything but
//        GET is refused on a read-only connection because the SQL guard cannot
//        parse it. Object actions (shut a task down, mark a datasource unused)
//        are expressed that way.
// WHERE: src-tauri/src/integrations/mod.rs (trait), integrations/sql.rs (clauses)
// ============================================================================

const DEFAULT_PORT: u16 = 8888;
const DEFAULT_SCHEMA: &str = "druid";
const MAX_SEGMENTS: usize = 500;
const MAX_OBJECTS: usize = 2_000;

pub struct DruidIntegration {
    http: HttpClient,
    schema: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = HttpClient::auth_from_connection(conn);
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, auth)?;
    let schema = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).unwrap_or(DEFAULT_SCHEMA).to_string();
    let integration = DruidIntegration { http, schema, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Response shaping
// ---------------------------------------------------------------------------

fn sql_rows_to_result_set(rows: &[Json], max_rows: usize) -> ResultSet {
    if rows.is_empty() {
        return ResultSet { columns: vec![], rows: vec![], truncated: false };
    }
    objects_to_result_set(rows, None, max_rows)
}

// WHAT:  Native query results come in several shapes; each is turned into rows
//        when it is regular enough, otherwise shown as one JSON cell.
fn native_result(body: &Json, max_rows: usize) -> ResultSet {
    let Some(items) = body.as_array() else { return json_result(body.clone()) };
    if items.is_empty() {
        return ResultSet { columns: vec![ColumnMeta { name: "result".into(), type_name: "array".into() }], rows: vec![], truncated: false };
    }
    // timeseries / topN: [{timestamp, result: {…} | [{…}]}]
    if items.iter().all(|i| i.get("timestamp").is_some() && i.get("result").is_some()) {
        let mut flat = Vec::new();
        for item in items {
            let ts = item.get("timestamp").cloned().unwrap_or(Json::Null);
            match item.get("result") {
                Some(Json::Array(list)) => {
                    for r in list {
                        let mut obj = r.as_object().cloned().unwrap_or_default();
                        obj.insert("timestamp".into(), ts.clone());
                        flat.push(Json::Object(obj));
                    }
                }
                Some(Json::Object(o)) => {
                    let mut obj = o.clone();
                    obj.insert("timestamp".into(), ts.clone());
                    flat.push(Json::Object(obj));
                }
                _ => flat.push(item.clone()),
            }
        }
        return objects_to_result_set(&flat, Some("timestamp"), max_rows);
    }
    // groupBy: [{version, timestamp, event: {…}}]
    if items.iter().all(|i| i.get("event").is_some()) {
        let flat: Vec<Json> = items
            .iter()
            .map(|i| {
                let mut obj = i.get("event").and_then(Json::as_object).cloned().unwrap_or_default();
                if let Some(ts) = i.get("timestamp") {
                    obj.insert("timestamp".into(), ts.clone());
                }
                Json::Object(obj)
            })
            .collect();
        return objects_to_result_set(&flat, Some("timestamp"), max_rows);
    }
    // scan: [{segmentId, columns, events: [[…]] | [{…}]}]
    if items.iter().all(|i| i.get("events").is_some()) {
        let mut flat = Vec::new();
        for item in items {
            let cols: Vec<String> = item.get("columns").and_then(Json::as_array).into_iter().flatten().filter_map(|c| c.as_str().map(str::to_string)).collect();
            for ev in item.get("events").and_then(Json::as_array).into_iter().flatten() {
                match ev {
                    Json::Array(cells) => {
                        let obj: serde_json::Map<String, Json> = cols.iter().cloned().zip(cells.iter().cloned()).collect();
                        flat.push(Json::Object(obj));
                    }
                    other => flat.push(other.clone()),
                }
            }
        }
        return objects_to_result_set(&flat, None, max_rows);
    }
    if items.iter().all(Json::is_object) {
        return objects_to_result_set(items, None, max_rows);
    }
    json_result(body.clone())
}

fn is_native_query(text: &str) -> bool {
    let t = text.trim_start();
    t.starts_with('{') && t.contains("\"queryType\"")
}

fn cell_text(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::Text(t)) | Some(Value::Decimal(t)) | Some(Value::DateTime(t)) => Some(t.clone()),
        Some(Value::Int(i)) => Some(i.to_string()),
        Some(Value::Float(f)) => Some(f.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(Value::Json(j)) => Some(j.to_string()),
        _ => None,
    }
}

fn cell_i64(v: Option<&Value>) -> Option<i64> {
    match v {
        Some(Value::Int(i)) => Some(*i),
        Some(Value::Float(f)) => Some(*f as i64),
        Some(Value::Decimal(d)) | Some(Value::Text(d)) => d.parse().ok(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl DruidIntegration {
    async fn sql(&self, query: &str, max_rows: usize) -> AppResult<ResultSet> {
        let body = json!({"query": query, "resultFormat": "object", "header": false, "context": {"sqlQueryId": uuid::Uuid::new_v4().to_string()}});
        let out: Json = self.http.post_json("/druid/v2/sql", &body).await?;
        let rows = out.as_array().cloned().unwrap_or_default();
        Ok(sql_rows_to_result_set(&rows, max_rows))
    }

    async fn native(&self, query: &Json, max_rows: usize) -> AppResult<ResultSet> {
        let out: Json = self.http.post_json("/druid/v2", query).await?;
        Ok(native_result(&out, max_rows))
    }

    fn qualified(&self, table: &TableRef) -> String {
        let with_schema = TableRef { schema: Some(table.schema.clone().unwrap_or_else(|| self.schema.clone())), name: table.name.clone() };
        qualified_name_for(Engine::Druid, &with_schema)
    }

    fn schema_of<'a>(&'a self, table: &'a TableRef) -> &'a str {
        table.schema.as_deref().unwrap_or(self.schema.as_str())
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Datasources (coordinator metadata), their segments as partitions,
//        indexing tasks from the overlord and the servers of the cluster.
// WHY:   None of this is in INFORMATION_SCHEMA: Druid keeps cluster state in
//        the coordinator / overlord REST APIs that the router proxies.
// HOW:   The pure decoders below turn each endpoint's JSON into summaries and
//        are unit-tested offline; the async methods only fetch. Actions are
//        `{"method","path"}` REST commands that run back through `execute`,
//        which refuses every non-GET on a read-only connection.
// ---------------------------------------------------------------------------

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

fn json_f64(v: Option<&Json>) -> f64 {
    match v {
        Some(Json::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Json::String(s)) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn json_text(v: Option<&Json>) -> String {
    match v {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn field(obj: &Json, name: &str) -> String {
    json_text(obj.get(name))
}

/// `{"properties": {"segments": {"count", "size", "minTime", "maxTime"}}}` → caption.
fn datasource_caption(info: &Json) -> String {
    let segments = info.pointer("/properties/segments").unwrap_or(&Json::Null);
    let count = json_f64(segments.get("count"));
    let size = json_f64(segments.get("size"));
    let mut caption = format!("{} segments · {}", count as u64, format_bytes(size));
    let (min, max) = (json_text(segments.get("minTime")), json_text(segments.get("maxTime")));
    if !min.is_empty() && !max.is_empty() {
        caption.push_str(&format!(" · {min} → {max}"));
    }
    caption
}

/// Segments carry an `id` / `identifier`, or are named after datasource + interval + version.
fn segment_id(seg: &Json) -> String {
    for key in ["id", "identifier"] {
        let value = field(seg, key);
        if !value.is_empty() {
            return value;
        }
    }
    let (ds, interval, version) = (field(seg, "dataSource"), field(seg, "interval"), field(seg, "version"));
    let partition = seg.pointer("/shardSpec/partitionNum").map(|p| json_f64(Some(p)) as u64).unwrap_or(0);
    format!("{ds}_{interval}_{version}_{partition}")
}

fn segment_summary(datasource: &str, seg: &Json) -> ObjectSummary {
    let size = json_f64(seg.get("size"));
    let rows = json_f64(seg.get("num_rows"));
    let mut caption = format!("{} · {}", field(seg, "interval"), format_bytes(size));
    if rows > 0.0 {
        caption.push_str(&format!(" · {} rows", crate::model::objects::format_number(rows)));
    }
    let mut summary = ObjectSummary::new(ObjectKind::Partition, segment_id(seg), Some(datasource.to_string())).with_detail(caption);
    let version = field(seg, "version");
    if !version.is_empty() {
        summary = summary.with_badge(version);
    }
    summary
}

/// Overlord task states differ per version (`statusCode` / `status` / `state`).
fn task_state(task: &Json) -> String {
    for key in ["statusCode", "status", "state"] {
        let value = field(task, key);
        if !value.is_empty() {
            return value.to_ascii_lowercase();
        }
    }
    String::new()
}

fn task_summary(task: &Json) -> ObjectSummary {
    let duration = json_f64(task.get("duration"));
    let mut parts = vec![field(task, "type")];
    let datasource = field(task, "dataSource");
    if !datasource.is_empty() {
        parts.push(datasource);
    }
    if duration > 0.0 {
        parts.push(format!("{:.1}s", duration / 1000.0));
    }
    let created = field(task, "createdTime");
    if !created.is_empty() {
        parts.push(created);
    }
    ObjectSummary::new(ObjectKind::Task, field(task, "id"), None)
        .with_detail(parts.into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>().join(" · "))
        .with_badge(task_state(task))
}

fn server_summary(server: &Json) -> ObjectSummary {
    let (curr, max) = (json_f64(server.get("currSize")), json_f64(server.get("maxSize")));
    let mut caption = format!("{} / {}", format_bytes(curr), format_bytes(max));
    if max > 0.0 {
        caption.push_str(&format!(" ({:.0}%)", curr / max * 100.0));
    }
    let tier = field(server, "tier");
    if !tier.is_empty() {
        caption.push_str(&format!(" · tier {tier}"));
    }
    ObjectSummary::new(ObjectKind::Node, field(server, "host"), None).with_detail(caption).with_badge(field(server, "type"))
}

/// Every scalar field of a JSON object as properties, nested ones as compact JSON.
fn json_properties(mut detail: ObjectDetail, obj: &Json, skip: &[&str]) -> ObjectDetail {
    for (k, v) in obj.as_object().into_iter().flatten() {
        if skip.contains(&k.as_str()) || v.is_null() {
            continue;
        }
        detail = detail.property(k, json_text(Some(v)));
    }
    detail
}

/// Druid's SQL schemas, which are never datasource names.
fn is_schema_name(name: &str) -> bool {
    ["druid", "sys", "INFORMATION_SCHEMA", "lookup", "view", "ext"].iter().any(|s| s.eq_ignore_ascii_case(name))
}

fn rest_action(id: &str, label: &str, method: &str, path: &str, destructive: bool) -> ObjectAction {
    let statement = json!({"method": method, "path": path}).to_string();
    if destructive {
        ObjectAction::destructive(id, label, statement)
    } else {
        ObjectAction::new(id, label, statement)
    }
}

/// `{"method","path","body"}` → a REST call the adapter forwards to the router.
fn parse_rest_command(text: &str) -> Option<(String, String, Option<Json>)> {
    let trimmed = text.trim();
    if !trimmed.starts_with('{') {
        return None;
    }
    let parsed: Json = serde_json::from_str(trimmed).ok()?;
    let path = parsed.get("path")?.as_str()?.to_string();
    let method = parsed.get("method").and_then(Json::as_str).unwrap_or("GET").to_ascii_uppercase();
    Some((method, path, parsed.get("body").cloned()))
}

impl DruidIntegration {
    async fn get(&self, path: &str) -> AppResult<Json> {
        self.http.get_json(path).await
    }

    async fn rest(&self, method: &str, path: &str, body: Option<Json>, max_rows: usize) -> AppResult<ResultSet> {
        if self.read_only && method != "GET" {
            return Err(AppError::read_only(format!("This connection is read-only; {method} {path} is blocked.")));
        }
        let verb = Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Unsupported HTTP method `{method}`.")))?;
        let mut req = self.http.request(verb, path);
        if let Some(b) = &body {
            req = req.json(b);
        }
        let resp = self.http.send(req).await?;
        let text = resp.text().await.unwrap_or_default();
        if text.trim().is_empty() {
            return Ok(ResultSet {
                columns: vec![ColumnMeta { name: "result".into(), type_name: "string".into() }],
                rows: vec![vec![Value::Text("ok".into())]],
                truncated: false,
            });
        }
        match serde_json::from_str::<Json>(&text) {
            Ok(Json::Array(items)) if items.iter().all(Json::is_object) && !items.is_empty() => Ok(objects_to_result_set(&items, None, max_rows)),
            Ok(v) => Ok(json_result(v)),
            Err(_) => Ok(ResultSet {
                columns: vec![ColumnMeta { name: "response".into(), type_name: "string".into() }],
                rows: vec![vec![Value::Text(text)]],
                truncated: false,
            }),
        }
    }

    async fn datasource_names(&self) -> AppResult<Vec<String>> {
        let list = self.get("/druid/coordinator/v1/metadata/datasources").await?;
        let mut names: Vec<String> = list.as_array().into_iter().flatten().filter_map(|d| d.as_str().map(str::to_string)).collect();
        names.sort();
        Ok(names)
    }

    async fn list_dataset_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut out = Vec::new();
        for name in self.datasource_names().await? {
            let caption = match self.get(&format!("/druid/coordinator/v1/datasources/{name}")).await {
                Ok(info) => datasource_caption(&info),
                Err(_) => String::new(),
            };
            let mut summary = ObjectSummary::new(ObjectKind::Dataset, name, None);
            if !caption.is_empty() {
                summary = summary.with_detail(caption);
            }
            out.push(summary);
        }
        Ok(out)
    }

    async fn segments_of(&self, datasource: &str) -> AppResult<Vec<Json>> {
        let full = self.get(&format!("/druid/coordinator/v1/datasources/{datasource}/segments?full")).await?;
        Ok(full.as_array().cloned().unwrap_or_default())
    }

    // WHAT:  `parent` is a datasource when the list comes from a dataset's
    //        children, but `Partition` is a scoped kind, so the sidebar sends
    //        the current schema ("druid", "sys", "INFORMATION_SCHEMA") instead.
    //        A schema name means "every datasource".
    async fn list_partition_objects(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let datasources = match parent.filter(|p| !is_schema_name(p)) {
            Some(ds) => vec![ds.to_string()],
            None => self.datasource_names().await?,
        };
        let mut out = Vec::new();
        for ds in datasources {
            for seg in self.segments_of(&ds).await.unwrap_or_default() {
                out.push(segment_summary(&ds, &seg));
                if out.len() >= MAX_SEGMENTS {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    async fn list_task_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let tasks = self.get("/druid/indexer/v1/tasks").await?;
        Ok(tasks.as_array().into_iter().flatten().map(task_summary).collect())
    }

    async fn list_node_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let servers = self.get("/druid/coordinator/v1/servers?simple").await?;
        Ok(servers.as_array().into_iter().flatten().map(server_summary).collect())
    }

    async fn dataset_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = &reference.name;
        let info = self.get(&format!("/druid/coordinator/v1/datasources/{name}")).await?;
        let mut detail = ObjectDetail::empty(reference).definition(serde_json::to_string_pretty(&info).unwrap_or_default(), CodeLanguage::Json);
        if let Some(segments) = info.pointer("/properties/segments") {
            detail = json_properties(detail, segments, &[]);
            detail = detail.property("size (human)", format_bytes(json_f64(segments.get("size"))));
        }
        for (tier, stats) in info.pointer("/tiers").and_then(Json::as_object).into_iter().flatten() {
            detail = detail.property(&format!("tier {tier}"), format!("{} segments · {}", json_f64(stats.get("segmentCount")) as u64, format_bytes(json_f64(stats.get("size")))));
        }
        detail.columns = self.columns(&TableRef { schema: Some(self.schema.clone()), name: name.clone() }).await.unwrap_or_default();
        detail.children = self.list_partition_objects(Some(name)).await.unwrap_or_default();
        Ok(detail.action(rest_action("unused", "Mark all segments unused", "DELETE", &format!("/druid/coordinator/v1/datasources/{name}"), true)))
    }

    async fn partition_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let datasource = reference.parent.clone().ok_or_else(|| AppError::invalid_input("A segment reference needs its datasource as parent."))?;
        let id = &reference.name;
        let seg = self
            .segments_of(&datasource)
            .await?
            .into_iter()
            .find(|s| segment_id(s) == *id)
            .ok_or_else(|| AppError::not_found(format!("Segment {id} is not in {datasource}.")))?;
        let mut detail = ObjectDetail::empty(reference)
            .definition(serde_json::to_string_pretty(&seg).unwrap_or_default(), CodeLanguage::Json)
            .property("datasource", datasource.clone());
        detail = json_properties(detail, &seg, &["dimensions", "metrics"]);
        detail = detail.property("size (human)", format_bytes(json_f64(seg.get("size"))));
        Ok(detail.action(rest_action(
            "unused",
            "Mark segment unused",
            "DELETE",
            &format!("/druid/coordinator/v1/datasources/{datasource}/segments/{id}"),
            true,
        )))
    }

    async fn task_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let id = &reference.name;
        let status = self.get(&format!("/druid/indexer/v1/task/{id}/status")).await?;
        let payload = self.get(&format!("/druid/indexer/v1/task/{id}")).await.unwrap_or(Json::Null);
        let mut detail = ObjectDetail::empty(reference);
        if !payload.is_null() {
            detail = detail.definition(serde_json::to_string_pretty(&payload).unwrap_or_default(), CodeLanguage::Json);
        }
        if let Some(s) = status.get("status") {
            detail = json_properties(detail, s, &["location"]);
        }
        Ok(detail.action(rest_action("shutdown", "Shut task down", "POST", &format!("/druid/indexer/v1/task/{id}/shutdown"), true)))
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let servers = self.get("/druid/coordinator/v1/servers?simple").await?;
        let server = servers
            .as_array()
            .into_iter()
            .flatten()
            .find(|s| field(s, "host") == reference.name)
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Server {} is not in the cluster.", reference.name)))?;
        let mut detail = json_properties(ObjectDetail::empty(reference), &server, &[]);
        detail = detail
            .property("currSize (human)", format_bytes(json_f64(server.get("currSize"))))
            .property("maxSize (human)", format_bytes(json_f64(server.get("maxSize"))));
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
        capabilities: Capabilities { describes_fields: true, transactions: false, exact_estimate: false, ..Capabilities::SQL },
        object_kinds: vec![K::Dataset, K::Partition, K::Task, K::Node],
        tools: vec![T::Stats],
    }
}

#[async_trait]
impl Integration for DruidIntegration {
    fn engine(&self) -> Engine {
        Engine::Druid
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _: Json = self.http.get_json("/status").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let status: Json = self.http.get_json("/status").await?;
        Ok(status.get("version").and_then(Json::as_str).map(|v| format!("Apache Druid {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.schema.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let set = self.sql("SELECT DISTINCT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES ORDER BY 1", 100).await?;
        let mut names: Vec<String> = set.rows.iter().filter_map(|r| cell_text(r.first())).collect();
        if !names.contains(&self.schema) {
            names.insert(0, self.schema.clone());
        }
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let set = self.sql("SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES ORDER BY 1, 2", 100_000).await?;
        let idx = |name: &str| set.columns.iter().position(|c| c.name == name);
        let (si, ni, ti) = (idx("TABLE_SCHEMA"), idx("TABLE_NAME"), idx("TABLE_TYPE"));
        let mut schemas: Vec<SchemaInfo> = Vec::new();
        for row in &set.rows {
            let Some(schema) = si.and_then(|i| cell_text(row.get(i))) else { continue };
            let Some(name) = ni.and_then(|i| cell_text(row.get(i))) else { continue };
            let ttype = ti.and_then(|i| cell_text(row.get(i))).unwrap_or_default();
            let kind = if ttype.eq_ignore_ascii_case("SYSTEM_TABLE") || ttype.eq_ignore_ascii_case("VIEW") { TableKind::View } else { TableKind::Table };
            let entry = TableInfo { schema: Some(schema.clone()), name, kind, row_estimate: None };
            match schemas.iter_mut().find(|s| s.name == schema) {
                Some(s) => s.tables.push(entry),
                None => schemas.push(SchemaInfo { name: schema, tables: vec![entry] }),
            }
        }
        // The session schema first, then the rest.
        schemas.sort_by_key(|s| (s.name != self.schema, s.name.clone()));
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let sql = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
            quote_literal(self.schema_of(table)),
            quote_literal(&table.name)
        );
        let set = self.sql(&sql, 10_000).await?;
        let idx = |name: &str| set.columns.iter().position(|c| c.name == name);
        let (ci, di, ni, oi) = (idx("COLUMN_NAME"), idx("DATA_TYPE"), idx("IS_NULLABLE"), idx("ORDINAL_POSITION"));
        let cols: Vec<ColumnInfo> = set
            .rows
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                let name = ci.and_then(|x| cell_text(row.get(x)))?;
                let data_type = di.and_then(|x| cell_text(row.get(x))).unwrap_or_else(|| "VARCHAR".into());
                let nullable = ni.and_then(|x| cell_text(row.get(x))).map(|v| v.eq_ignore_ascii_case("YES")).unwrap_or(true);
                let ordinal = oi.and_then(|x| cell_i64(row.get(x))).and_then(|o| u32::try_from(o).ok()).unwrap_or(i as u32 + 1);
                Some(ColumnInfo { primary_key: name == "__time", name, data_type, nullable, ordinal })
            })
            .collect();
        if cols.is_empty() {
            return Err(AppError::not_found(format!("Table \"{}\" has no columns in INFORMATION_SCHEMA (is it still loading?).", table.name)));
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        if self.schema_of(table) != "druid" {
            return Ok(None);
        }
        // sys.segments carries per-datasource row counts without scanning.
        let sql = format!("SELECT SUM(\"num_rows\") AS n FROM sys.segments WHERE \"datasource\" = {} AND is_active = 1", quote_literal(&table.name));
        match self.sql(&sql, 1).await {
            Ok(set) => Ok(set.rows.first().and_then(|r| cell_i64(r.first()))),
            Err(_) => Ok(None),
        }
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT COUNT(*) AS n FROM {}{}", self.qualified(table), where_clause(Engine::Druid, filters));
        let set = self.sql(&sql, 1).await?;
        Ok(set.rows.first().and_then(|r| cell_i64(r.first())).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            self.qualified(table),
            where_clause(Engine::Druid, &query.filters),
            order_clause(Engine::Druid, &query.sort),
            query.limit,
            query.offset
        );
        self.sql(&sql, query.limit as usize).await
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        if is_native_query(sql) {
            let q: Json = serde_json::from_str(sql.trim()).map_err(|e| AppError::invalid_input(format!("Native query is not valid JSON: {e}")))?;
            return Ok(vec![StatementResult::Rows { result: self.native(&q, max_rows).await? }]);
        }
        // Coordinator / overlord REST call (object actions are written this way).
        if let Some((method, path, body)) = parse_rest_command(sql) {
            return Ok(vec![StatementResult::Rows { result: self.rest(&method, &path, body, max_rows).await? }]);
        }
        let mut results = Vec::new();
        for stmt in split_statements(sql) {
            let trimmed = stmt.trim().trim_end_matches(';').trim();
            if trimmed.is_empty() {
                continue;
            }
            let upper = trimmed.to_ascii_uppercase();
            let is_write = ["INSERT", "REPLACE", "DELETE", "DROP", "ALTER", "CREATE", "UPDATE"].iter().any(|kw| upper.starts_with(kw));
            let set = self.sql(trimmed, max_rows).await?;
            if is_write && set.columns.iter().all(|c| c.name == "TASK") {
                // Ingestion statements return a task id row (MSQ); report it as rows so the id is visible.
                results.push(StatementResult::Rows { result: set });
            } else if is_write && set.rows.is_empty() {
                results.push(StatementResult::Affected { rows_affected: 0 });
            } else {
                results.push(StatementResult::Rows { result: set });
            }
        }
        Ok(results)
    }

    async fn close(&self) {}

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut out = match kind {
            ObjectKind::Dataset => self.list_dataset_objects().await?,
            ObjectKind::Partition => self.list_partition_objects(parent).await?,
            ObjectKind::Task => self.list_task_objects().await?,
            ObjectKind::Node => self.list_node_objects().await?,
            _ => Vec::new(),
        };
        if kind != ObjectKind::Task {
            out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        }
        out.truncate(MAX_OBJECTS);
        Ok(out)
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Dataset => self.dataset_detail(reference).await,
            ObjectKind::Partition => self.partition_detail(reference).await,
            ObjectKind::Task => self.task_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    // WHAT:  Version from /status, cluster totals from the coordinator, task
    //        states from the overlord and the load queue from /loadstatus.
    async fn server_stats(&self) -> AppResult<ServerStats> {
        let version = self.server_version().await.unwrap_or_default().unwrap_or_else(|| "Apache Druid".into());
        let datasources = self.datasource_names().await.unwrap_or_default();
        let servers = self.get("/druid/coordinator/v1/servers?simple").await.unwrap_or(Json::Null);
        let server_list: Vec<Json> = servers.as_array().cloned().unwrap_or_default();
        let (used, capacity) = server_list.iter().fold((0.0, 0.0), |(u, c), s| (u + json_f64(s.get("currSize")), c + json_f64(s.get("maxSize"))));
        let tasks = self.get("/druid/indexer/v1/tasks").await.unwrap_or(Json::Null);
        let mut by_state: BTreeMap<String, f64> = BTreeMap::new();
        for task in tasks.as_array().into_iter().flatten() {
            *by_state.entry(task_state(task)).or_insert(0.0) += 1.0;
        }
        let segments = self
            .sql("SELECT COUNT(*) AS n, SUM(\"size\") AS bytes FROM sys.segments WHERE is_active = 1", 1)
            .await
            .ok()
            .and_then(|set| set.rows.first().map(|r| (cell_i64(r.first()).unwrap_or(0), cell_i64(r.get(1)).unwrap_or(0))))
            .unwrap_or((0, 0));
        let load = self.get("/druid/coordinator/v1/loadstatus").await.unwrap_or(Json::Null);
        let load_values: Vec<f64> = load.as_object().into_iter().flatten().map(|(_, v)| json_f64(Some(v))).collect();
        let mut groups = vec![
            StatGroup {
                title: "Server".into(),
                stats: vec![Stat::text("Version", version), Stat::text("Router", self.http.base().to_string()), Stat::text("Schema", self.schema.clone())],
            },
            StatGroup {
                title: "Cluster".into(),
                stats: vec![
                    Stat::number("Datasources", datasources.len() as f64, None),
                    Stat::number("Servers", server_list.len() as f64, None),
                    Stat::number("Used capacity", (used / 1_048_576.0 * 10.0).round() / 10.0, Some("MB")).with_hint(format_bytes(used)),
                    Stat::number("Total capacity", (capacity / 1_048_576.0 * 10.0).round() / 10.0, Some("MB")).with_hint(format_bytes(capacity)),
                ],
            },
            StatGroup {
                title: "Storage".into(),
                stats: vec![
                    Stat::number("Active segments", segments.0 as f64, None).with_hint("sys.segments where is_active = 1"),
                    Stat::number("Segment size", (segments.1 as f64 / 1_048_576.0 * 10.0).round() / 10.0, Some("MB")).with_hint(format_bytes(segments.1 as f64)),
                ],
            },
        ];
        let mut task_stats = vec![Stat::number("Tasks", by_state.values().sum::<f64>(), None)];
        task_stats.extend(by_state.iter().filter(|(state, _)| !state.is_empty()).map(|(state, count)| Stat::number(state, *count, None)));
        groups.push(StatGroup { title: "Tasks".into(), stats: task_stats });
        if !load_values.is_empty() {
            let fully_loaded = load_values.iter().filter(|v| **v >= 100.0).count();
            groups.push(StatGroup {
                title: "Replication".into(),
                stats: vec![
                    Stat::number("Fully loaded datasources", fully_loaded as f64, None).with_hint("/druid/coordinator/v1/loadstatus"),
                    Stat::number("Average load", load_values.iter().sum::<f64>() / load_values.len() as f64, Some("%")),
                ],
            });
        }
        Ok(ServerStats::now(groups))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, FilterOp, SortRule, SslMode};

    #[test]
    fn datasource_segment_task_and_server_summaries() {
        let info = json!({"name": "wiki", "properties": {"segments": {"count": 12, "size": 3_221_225_472u64, "minTime": "2024-01-01T00:00:00.000Z", "maxTime": "2024-02-01T00:00:00.000Z"}}});
        assert_eq!(datasource_caption(&info), "12 segments · 3.0 GB · 2024-01-01T00:00:00.000Z → 2024-02-01T00:00:00.000Z");
        assert_eq!(datasource_caption(&json!({"properties": {"segments": {"count": 0, "size": 0}}})), "0 segments · 0 B");

        let seg = json!({"dataSource": "wiki", "interval": "2024-01-01/2024-01-02", "version": "v1", "size": 1_572_864, "shardSpec": {"partitionNum": 2}});
        assert_eq!(segment_id(&seg), "wiki_2024-01-01/2024-01-02_v1_2");
        assert_eq!(segment_id(&json!({"id": "explicit"})), "explicit");
        assert_eq!(segment_id(&json!({"identifier": "older"})), "older");
        let s = segment_summary("wiki", &seg);
        assert_eq!(s.reference.parent.as_deref(), Some("wiki"));
        assert_eq!(s.detail.as_deref(), Some("2024-01-01/2024-01-02 · 1.5 MB"));
        assert_eq!(s.badge.as_deref(), Some("v1"));

        let task = json!({"id": "index_wiki_1", "type": "index_parallel", "dataSource": "wiki", "statusCode": "RUNNING", "duration": 1500, "createdTime": "2024-01-01T00:00:00.000Z"});
        let t = task_summary(&task);
        assert_eq!(t.badge.as_deref(), Some("running"));
        assert_eq!(t.detail.as_deref(), Some("index_parallel · wiki · 1.5s · 2024-01-01T00:00:00.000Z"));
        assert_eq!(task_state(&json!({"status": "SUCCESS"})), "success");
        assert_eq!(task_state(&json!({})), "");

        let server = json!({"host": "historical:8083", "type": "historical", "tier": "_default_tier", "currSize": 536_870_912, "maxSize": 1_073_741_824u64});
        let n = server_summary(&server);
        assert_eq!(n.reference.name, "historical:8083");
        assert_eq!(n.badge.as_deref(), Some("historical"));
        assert_eq!(n.detail.as_deref(), Some("512.0 MB / 1.0 GB (50%) · tier _default_tier"));
    }

    #[test]
    fn rest_commands_parse_and_actions_are_rest() {
        assert_eq!(
            parse_rest_command(r#"{"method":"POST","path":"/druid/indexer/v1/task/x/shutdown"}"#),
            Some(("POST".to_string(), "/druid/indexer/v1/task/x/shutdown".to_string(), None))
        );
        let (method, path, body) = parse_rest_command(r#"{"path":"/druid/v2/x","body":{"a":1}}"#).unwrap_or_else(|| panic!("parse"));
        assert_eq!((method.as_str(), path.as_str()), ("GET", "/druid/v2/x"));
        assert_eq!(body, Some(json!({"a": 1})));
        assert_eq!(parse_rest_command("SELECT 1"), None);
        // A native query has no "path" and must stay a native query.
        assert_eq!(parse_rest_command(r#"{"queryType":"timeseries"}"#), None);
        let action = rest_action("shutdown", "Shut task down", "POST", "/druid/indexer/v1/task/x/shutdown", true);
        assert!(action.destructive);
        assert_eq!(parse_rest_command(&action.statement).map(|(m, p, _)| (m, p)), Some(("POST".to_string(), "/druid/indexer/v1/task/x/shutdown".to_string())));
        assert_eq!(format_bytes(0.0), "0 B");
        assert_eq!(format_bytes(2048.0), "2.0 KB");
        // Partition is a scoped kind, so the sidebar sends a schema, not a datasource.
        assert!(is_schema_name("druid") && is_schema_name("sys") && is_schema_name("information_schema"));
        assert!(!is_schema_name("wikipedia"));
    }

    #[test]
    fn page_sql_uses_double_quotes() {
        let table = TableRef { schema: Some("druid".into()), name: "wiki\"pedia".into() };
        assert_eq!(qualified_name_for(Engine::Druid, &table), "\"druid\".\"wiki\"\"pedia\"");
        let w = where_clause(Engine::Druid, &[FilterRule { column: "channel".into(), op: FilterOp::Contains, value: "en".into() }]);
        assert_eq!(w, " WHERE CAST(\"channel\" AS STRING) LIKE '%en%'");
        let o = order_clause(Engine::Druid, &[SortRule { column: "__time".into(), desc: true }]);
        assert_eq!(o, " ORDER BY \"__time\" DESC");
    }

    #[test]
    fn native_shapes_become_rows() {
        let ts = json!([{"timestamp": "2024-01-01T00:00:00.000Z", "result": {"count": 3}}]);
        let set = native_result(&ts, 10);
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["timestamp", "count"]);
        assert_eq!(set.rows[0][1], Value::Int(3));
        let topn = json!([{"timestamp": "t", "result": [{"dim": "a", "n": 1}, {"dim": "b", "n": 2}]}]);
        assert_eq!(native_result(&topn, 10).rows.len(), 2);
        let group = json!([{"version": "v1", "timestamp": "t", "event": {"dim": "a", "n": 1}}]);
        let set = native_result(&group, 10);
        assert_eq!(set.columns[0].name, "timestamp");
        assert_eq!(set.rows[0][1], Value::Text("a".into()));
        let scan = json!([{"segmentId": "s", "columns": ["a", "b"], "events": [[1, "x"], [2, "y"]]}]);
        let set = native_result(&scan, 10);
        assert_eq!(set.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["a", "b"]);
        assert_eq!(set.rows[1][1], Value::Text("y".into()));
        assert_eq!(native_result(&json!([]), 10).rows.len(), 0);
        assert_eq!(native_result(&json!({"error": "x"}), 10).columns[0].name, "result");
        assert!(is_native_query("{\"queryType\": \"timeseries\"}"));
        assert!(!is_native_query("SELECT 1"));
    }

    #[test]
    fn sql_object_rows() {
        let rows = vec![json!({"__time": "2024-01-01T00:00:00.000Z", "n": 1}), json!({"__time": "2024-01-02T00:00:00.000Z", "n": 2})];
        let set = sql_rows_to_result_set(&rows, 1);
        assert_eq!(set.columns.len(), 2);
        assert!(set.truncated);
        assert!(sql_rows_to_result_set(&[], 1).columns.is_empty());
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_DRUID_URL is set.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_DRUID_URL") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Druid,
            environment: Environment::Local,
            read_only: true,
            host: Some(url),
            port: None,
            database: None,
            username: std::env::var("DBFREE_TEST_DRUID_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let secret = std::env::var("DBFREE_TEST_DRUID_PASSWORD").ok();
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, secret.is_some()), secret };
        let d = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        assert!(d.server_version().await.unwrap_or_default().unwrap_or_default().starts_with("Apache Druid"));
        let cat = d.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas.iter().any(|s| s.name == "sys"), "{cat:?}");
        let table = TableRef { schema: Some("sys".into()), name: "servers".into() };
        let cols = d.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "server"), "{cols:?}");
        let page = d.fetch_page(&table, &PageQuery { sort: vec![SortRule { column: "server".into(), desc: false }], filters: vec![], offset: 0, limit: 10 }).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert!(!page.rows.is_empty());
        assert!(d.count(&table, &[]).await.unwrap_or_default() >= 1);
        let out = d.execute("SELECT COUNT(*) AS n FROM sys.segments", 10).await.unwrap_or_else(|e| panic!("sql: {e}"));
        assert!(matches!(&out[0], StatementResult::Rows { .. }));
        let out = d.execute("{\"queryType\":\"segmentMetadata\",\"dataSource\":\"nonexistent\"}", 10).await.unwrap_or_else(|e| panic!("native: {e}"));
        assert!(matches!(&out[0], StatementResult::Rows { .. }));
    }
}
