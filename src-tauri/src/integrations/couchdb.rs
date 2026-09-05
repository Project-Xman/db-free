// SOT: couchdb-integration, couch-http-api, mango-query, couch-design-views, couch-object-explorer, couch-server-stats

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, json_to_value, json_type_name, local, objects_to_result_set, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, SortRule, Stat,
    StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use std::sync::Arc;

// ============================================================================
// WHAT:  Apache CouchDB adapter over its HTTP API (port 5984, Basic auth).
//        Schema = database, table `documents` = every document, plus one View
//        per design-document view (`design/view`). Pages go through Mango
//        (`_find`), falling back to `_all_docs` + client-side paging when the
//        requested sort has no usable index.
// WHY:   CouchDB is fully REST; no crate needed beyond the shared HttpClient.
// HOW:   `execute` accepts a Mango JSON body, a `{"method","path","body"}`
//        passthrough, or the shorthands `GET <id>` / `ALL [n]`.
// WHERE: src-tauri/src/integrations/http.rs, src-tauri/src/integrations/mod.rs
// ============================================================================

const SAMPLE: usize = 50;
const SCAN_CAP: usize = 10_000;
const DOCUMENTS: &str = "documents";
const ID: &str = "_id";

pub struct CouchIntegration {
    engine: Engine,
    http: HttpClient,
    database: Option<String>,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = HttpClient::auth_from_connection(conn);
    let http = HttpClient::from_connection(conn, Some(5984), false, auth)?;
    let database = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let integration = CouchIntegration { engine: conn.summary.engine, http, database, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

pub fn encode_path_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn lenient_json(raw: &str) -> serde_json::Value {
    let t = raw.trim();
    if t.eq_ignore_ascii_case("true") {
        return serde_json::Value::Bool(true);
    }
    if t.eq_ignore_ascii_case("false") {
        return serde_json::Value::Bool(false);
    }
    if t.eq_ignore_ascii_case("null") {
        return serde_json::Value::Null;
    }
    if let Ok(i) = t.parse::<i64>() {
        return serde_json::Value::Number(i.into());
    }
    if let Ok(f) = t.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return serde_json::Value::Number(n);
        }
    }
    serde_json::Value::String(t.to_string())
}

fn escape_regex(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 4);
    for ch in raw.chars() {
        if "\\^$.|?*+()[]{}/".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

// WHAT:  One filter rule → one Mango selector fragment.
pub fn mango_predicate(rule: &FilterRule) -> serde_json::Value {
    let v = rule.value.trim();
    let body = match rule.op {
        FilterOp::Eq => serde_json::json!({"$eq": lenient_json(v)}),
        FilterOp::Ne => serde_json::json!({"$ne": lenient_json(v)}),
        FilterOp::Gt => serde_json::json!({"$gt": lenient_json(v)}),
        FilterOp::Gte => serde_json::json!({"$gte": lenient_json(v)}),
        FilterOp::Lt => serde_json::json!({"$lt": lenient_json(v)}),
        FilterOp::Lte => serde_json::json!({"$lte": lenient_json(v)}),
        FilterOp::Contains => serde_json::json!({"$regex": format!("(?i){}", escape_regex(v))}),
        FilterOp::StartsWith => serde_json::json!({"$regex": format!("(?i)^{}", escape_regex(v))}),
        FilterOp::EndsWith => serde_json::json!({"$regex": format!("(?i){}$", escape_regex(v))}),
        FilterOp::In => {
            let items: Vec<serde_json::Value> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(lenient_json).collect();
            serde_json::json!({"$in": items})
        }
        FilterOp::IsNull => serde_json::json!({"$exists": false}),
        FilterOp::IsNotNull => serde_json::json!({"$exists": true}),
    };
    serde_json::json!({ rule.column.clone(): body })
}

pub fn mango_selector(filters: &[FilterRule]) -> serde_json::Value {
    match filters {
        [] => serde_json::json!({ "_id": {"$gt": null} }),
        [one] => mango_predicate(one),
        many => serde_json::json!({"$and": many.iter().map(mango_predicate).collect::<Vec<_>>()}),
    }
}

pub fn mango_sort(sort: &[SortRule]) -> serde_json::Value {
    serde_json::Value::Array(
        sort.iter().map(|s| serde_json::json!({ s.column.clone(): if s.desc { "desc" } else { "asc" } })).collect(),
    )
}

pub fn mango_body(query: &PageQuery) -> serde_json::Value {
    let mut body = serde_json::json!({
        "selector": mango_selector(&query.filters),
        "skip": query.offset,
        "limit": query.limit,
    });
    if !query.sort.is_empty() {
        body["sort"] = mango_sort(&query.sort);
    }
    body
}

fn union_columns(docs: &[serde_json::Value]) -> Vec<ColumnInfo> {
    let rs = objects_to_result_set(docs, Some(ID), 0);
    rs.columns
        .into_iter()
        .enumerate()
        .map(|(i, c)| ColumnInfo { primary_key: c.name == ID, name: c.name, data_type: c.type_name, nullable: true, ordinal: i as u32 + 1 })
        .collect()
}

fn docs_to_rows(columns: &[ColumnInfo], docs: &[serde_json::Value]) -> ResultSet {
    let rows = docs
        .iter()
        .map(|d| columns.iter().map(|c| d.get(&c.name).map(json_to_value).unwrap_or(Value::Null)).collect())
        .collect();
    ResultSet { columns: columns.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect(), rows, truncated: false }
}

// WHAT:  `design/view` table names → (design doc, view name).
fn view_parts(name: &str) -> Option<(&str, &str)> {
    let (d, v) = name.split_once('/')?;
    if d.is_empty() || v.is_empty() || name == DOCUMENTS {
        None
    } else {
        Some((d, v))
    }
}

#[derive(Debug)]
enum Command {
    Mango(serde_json::Value),
    Passthrough { method: String, path: String, body: Option<serde_json::Value> },
    Get(String),
    All(usize),
}

fn parse_command(raw: &str) -> AppResult<Command> {
    let text = raw.trim();
    if text.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if text.starts_with('{') {
        let v: serde_json::Value = serde_json::from_str(text).map_err(|e| AppError::invalid_input(format!("Command is not valid JSON: {e}")))?;
        if let Some(path) = v.get("path").and_then(|p| p.as_str()) {
            let method = v.get("method").and_then(|m| m.as_str()).unwrap_or("GET").to_uppercase();
            return Ok(Command::Passthrough { method, path: path.to_string(), body: v.get("body").cloned() });
        }
        if v.get("selector").is_some() {
            return Ok(Command::Mango(v));
        }
        return Ok(Command::Mango(serde_json::json!({"selector": v, "limit": 100})));
    }
    let mut words = text.split_whitespace();
    let head = words.next().unwrap_or_default().to_uppercase();
    match head.as_str() {
        "GET" => {
            let id = words.next().ok_or_else(|| AppError::invalid_input("GET needs a document id."))?;
            Ok(Command::Get(id.to_string()))
        }
        "ALL" => Ok(Command::All(words.next().and_then(|n| n.parse().ok()).unwrap_or(100))),
        _ => Err(AppError::invalid_input(
            "Enter a Mango query ({\"selector\": {...}}), a {\"method\",\"path\",\"body\"} request, `GET <id>` or `ALL [n]`.",
        )),
    }
}

impl CouchIntegration {
    fn db(&self) -> AppResult<&str> {
        self.database.as_deref().ok_or_else(|| AppError::invalid_input("Select a CouchDB database first."))
    }

    fn db_for(&self, table: &TableRef) -> AppResult<String> {
        match table.schema.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(s) => Ok(s.to_string()),
            None => self.db().map(str::to_string),
        }
    }

    async fn all_docs(&self, db: &str, skip: u64, limit: usize) -> AppResult<Vec<serde_json::Value>> {
        let path = format!("/{}/_all_docs?include_docs=true&skip={skip}&limit={limit}", encode_path_segment(db));
        let resp: serde_json::Value = self.http.get_json(&path).await?;
        Ok(resp
            .get("rows")
            .and_then(|r| r.as_array())
            .map(|rows| rows.iter().filter_map(|r| r.get("doc").cloned()).filter(|d| !d.is_null()).collect())
            .unwrap_or_default())
    }

    async fn find(&self, db: &str, body: &serde_json::Value) -> AppResult<Vec<serde_json::Value>> {
        let resp: serde_json::Value = self.http.post_json(&format!("/{}/_find", encode_path_segment(db)), body).await?;
        Ok(resp.get("docs").and_then(|d| d.as_array()).cloned().unwrap_or_default())
    }

    async fn view_rows(&self, db: &str, design: &str, view: &str, skip: u64, limit: usize) -> AppResult<Vec<serde_json::Value>> {
        let path = format!(
            "/{}/_design/{}/_view/{}?include_docs=true&skip={skip}&limit={limit}",
            encode_path_segment(db),
            encode_path_segment(design),
            encode_path_segment(view)
        );
        let resp: serde_json::Value = self.http.get_json(&path).await?;
        Ok(resp.get("rows").and_then(|r| r.as_array()).cloned().unwrap_or_default())
    }

    fn is_view(&self, table: &TableRef) -> bool {
        view_parts(&table.name).is_some()
    }

    async fn sample(&self, table: &TableRef) -> AppResult<Vec<serde_json::Value>> {
        let db = self.db_for(table)?;
        match view_parts(&table.name) {
            Some((d, v)) => self.view_rows(&db, d, v, 0, SAMPLE).await,
            None => self.all_docs(&db, 0, SAMPLE).await,
        }
    }

    async fn request_json(&self, method: Method, path: &str, body: Option<serde_json::Value>) -> AppResult<serde_json::Value> {
        let mut req = self.http.request(method, path);
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = self.http.send(req).await?;
        let text = resp.text().await.map_err(|e| AppError::driver(e.to_string()))?;
        Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)))
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
// ---------------------------------------------------------------------------
//
// WHAT:  Databases (`_dbs_info`), design documents and their views, Mango
//        indexes (`_index`), replications (`_scheduler/docs`), active tasks,
//        users (`_users` + config admins), cluster nodes (`_membership`) and
//        the node config (`_node/_local/_config`), plus `_stats` / `_system`
//        for the Stats tab. Every payload is mapped by a pure function so the
//        shapes are unit-tested offline.
// WHY:   All of it is plain REST on the same client `execute` uses, so the
//        actions are `{"method","path"}` passthrough requests — the read-only
//        lock in `execute` applies to them unchanged.
// HOW:   A view's parent is `db/_design/name` (owner), a design document's is
//        its database, a replication's is the database that holds its doc,
//        a task's is its Erlang pid (the only stable id `_active_tasks` has).
//        Admin-only endpoints (`_config`, `_stats`, `_users`) answer empty on
//        401 / 403 / 404 instead of failing the whole listing.

type Json = serde_json::Value;

const OBJECT_CAP: usize = 2_000;
const DBS_INFO_CHUNK: usize = 100;
const DESIGN_PREFIX: &str = "_design/";
const USER_PREFIX: &str = "org.couchdb.user:";
const VIEW_SAMPLE: usize = 20;
const MIB: f64 = 1_048_576.0;

fn jstr<'a>(v: &'a Json, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Json::as_str)
}

fn jbool(v: &Json, key: &str) -> bool {
    v.get(key).and_then(Json::as_bool).unwrap_or(false)
}

fn number_of(v: &Json) -> Option<f64> {
    match v {
        Json::Number(n) => n.as_f64(),
        Json::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn jnum(v: &Json, key: &str) -> Option<f64> {
    v.get(key).and_then(number_of)
}

fn pnum(v: &Json, pointer: &str) -> Option<f64> {
    v.pointer(pointer).and_then(number_of)
}

fn items<'a>(v: &'a Json, key: &str) -> impl Iterator<Item = &'a Json> {
    v.get(key).and_then(Json::as_array).into_iter().flatten()
}

fn object_len(v: &Json, key: &str) -> usize {
    v.get(key).and_then(Json::as_object).map(serde_json::Map::len).unwrap_or(0)
}

fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

fn compact(v: &Json) -> String {
    let text = match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    };
    if text.chars().count() > 120 {
        format!("{}…", text.chars().take(119).collect::<String>())
    } else {
        text
    }
}

fn bytes_text(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", value as u64)
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn duration_text(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3_600, (s % 3_600) / 60);
    if d > 0 {
        format!("{d}d {h}h {m}m")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else {
        format!("{m}m {}s", s % 60)
    }
}

fn epoch_text(secs: f64) -> String {
    chrono::DateTime::from_timestamp(secs as i64, 0).map(|t| t.to_rfc3339()).unwrap_or_else(|| format!("{secs}"))
}

fn sorted(mut list: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    list.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    list.truncate(OBJECT_CAP);
    list
}

fn is_system_db(name: &str) -> bool {
    name.starts_with('_')
}

// WHAT:  Admin-only endpoints answer 401 / 403 (NotConnected) or 404 (NotFound)
//        for ordinary users and older servers; those become "nothing to list".
fn tolerated<T: Default>(result: AppResult<T>) -> AppResult<T> {
    match result {
        Err(AppError::NotConnected { .. }) | Err(AppError::NotFound { .. }) => Ok(T::default()),
        other => other,
    }
}

// WHAT:  A replication endpoint (`"http://user:pw@host/db"` or `{"url": …}`)
//        as display text with credentials removed.
fn endpoint_text(v: &Json) -> String {
    let raw = match v {
        Json::String(s) => s.clone(),
        Json::Object(o) => o.get("url").and_then(Json::as_str).unwrap_or_default().to_string(),
        other => other.to_string(),
    };
    match (raw.find("://"), raw.rfind('@')) {
        (Some(scheme), Some(at)) if at > scheme => format!("{}{}", &raw[..scheme + 3], &raw[at + 1..]),
        _ => raw,
    }
}

fn passthrough(method: &str, path: &str) -> String {
    serde_json::json!({"method": method, "path": path}).to_string()
}

fn db_path(db: &str) -> String {
    format!("/{}", encode_path_segment(db))
}

fn design_path(db: &str, design: &str) -> String {
    format!("/{}/_design/{}", encode_path_segment(db), encode_path_segment(design))
}

// WHAT:  `db/_design/x` (a view's parent) → (db, design name).
fn split_design_parent(parent: &str) -> Option<(&str, &str)> {
    let (db, design) = parent.split_once("/_design/")?;
    (!db.is_empty() && !design.is_empty()).then_some((db, design))
}

fn rows_from(objects: &[Json], id_first: Option<&str>) -> Option<ResultSet> {
    (!objects.is_empty()).then(|| objects_to_result_set(objects, id_first, OBJECT_CAP))
}

// ---- listings ---------------------------------------------------------------

// WHAT:  (name, `GET /{db}` info) pairs → database rows.
fn database_summaries(infos: &[(String, Json)]) -> Vec<ObjectSummary> {
    let list = infos
        .iter()
        .map(|(name, info)| {
            let mut parts: Vec<String> = Vec::new();
            if let Some(n) = jnum(info, "doc_count") {
                parts.push(format!("{} docs", crate::model::objects::format_number(n)));
            }
            if let Some(size) = pnum(info, "/sizes/active").or_else(|| pnum(info, "/sizes/file")) {
                parts.push(bytes_text(size));
            }
            let mut s = ObjectSummary::new(ObjectKind::Database, name.clone(), None);
            if !parts.is_empty() {
                s = s.with_detail(parts.join(" · "));
            }
            if is_system_db(name) {
                s = s.with_badge("system");
            } else if info.pointer("/props/partitioned").and_then(Json::as_bool) == Some(true) {
                s = s.with_badge("partitioned");
            }
            s
        })
        .collect();
    sorted(list)
}

fn design_doc_detail_text(doc: &Json) -> String {
    let mut parts: Vec<String> = Vec::new();
    for (key, label) in [("views", "views"), ("updates", "updates"), ("filters", "filters"), ("shows", "shows"), ("lists", "lists")] {
        let n = object_len(doc, key);
        if n > 0 {
            parts.push(format!("{n} {label}"));
        }
    }
    if doc.get("validate_doc_update").is_some() {
        parts.push("validate".into());
    }
    parts.join(" · ")
}

// WHAT:  `_design_docs?include_docs=true` rows → design documents of `db`.
fn design_doc_summaries(rows: &[Json], db: &str) -> Vec<ObjectSummary> {
    let list = rows
        .iter()
        .filter_map(|row| {
            let doc = row.get("doc").filter(|d| d.is_object()).unwrap_or(row);
            let id = jstr(doc, ID)?;
            let mut s = ObjectSummary::new(ObjectKind::Document, id, Some(db.to_string())).with_badge(jstr(doc, "language").unwrap_or("javascript"));
            let detail = design_doc_detail_text(doc);
            if !detail.is_empty() {
                s = s.with_detail(detail);
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

fn reduce_text(view: &Json) -> Option<String> {
    let reduce = jstr(view, "reduce")?;
    Some(if reduce.starts_with('_') { reduce.to_string() } else { "custom".to_string() })
}

// WHAT:  Views of one design document; parent = `db/_design/name`.
fn view_summaries(design: &Json, db: &str) -> Vec<ObjectSummary> {
    let Some(id) = jstr(design, ID) else { return Vec::new() };
    let parent = format!("{db}/{id}");
    let list = design
        .get("views")
        .and_then(Json::as_object)
        .into_iter()
        .flatten()
        .map(|(name, view)| {
            let mut s = ObjectSummary::new(ObjectKind::View, name, Some(parent.clone()));
            s = match reduce_text(view) {
                Some(r) => s.with_badge("reduce").with_detail(format!("map + reduce {r}")),
                None => s.with_badge("map").with_detail("map"),
            };
            s
        })
        .collect();
    sorted(list)
}

fn index_fields_text(index: &Json) -> String {
    index
        .pointer("/def/fields")
        .and_then(Json::as_array)
        .map(|fields| {
            fields
                .iter()
                .map(|f| match f {
                    Json::Object(o) => o.iter().map(|(k, v)| format!("{k} {}", compact(v))).collect::<Vec<_>>().join(", "),
                    other => compact(other),
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

// WHAT:  `GET /{db}/_index` → Mango indexes (the built-in `_all_docs` included).
fn index_summaries(reply: &Json, db: &str) -> Vec<ObjectSummary> {
    let list = items(reply, "indexes")
        .filter_map(|index| {
            let name = jstr(index, "name")?;
            let kind = jstr(index, "type").unwrap_or("json");
            let mut s = ObjectSummary::new(ObjectKind::Index, name, Some(db.to_string())).with_badge(kind);
            let fields = index_fields_text(index);
            if !fields.is_empty() {
                s = s.with_detail(fields);
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

// WHAT:  `_scheduler/docs` entries → replications with their scheduler state.
fn replica_summaries(reply: &Json) -> Vec<ObjectSummary> {
    let list = items(reply, "docs")
        .filter_map(|doc| {
            let id = jstr(doc, "doc_id")?;
            let db = jstr(doc, "database").unwrap_or("_replicator");
            let mut s = ObjectSummary::new(ObjectKind::Replica, id, Some(db.to_string()));
            if let (Some(src), Some(dst)) = (doc.get("source"), doc.get("target")) {
                s = s.with_detail(format!("{} → {}", endpoint_text(src), endpoint_text(dst)));
            }
            if let Some(state) = jstr(doc, "state") {
                s = s.with_badge(state);
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

// WHAT:  Fallback for servers without `_scheduler`: the `_replicator` docs.
fn replicator_doc_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    let list = rows
        .iter()
        .filter_map(|row| {
            let doc = row.get("doc").filter(|d| d.is_object())?;
            let id = jstr(doc, ID)?;
            if id.starts_with(DESIGN_PREFIX) {
                return None;
            }
            let mut s = ObjectSummary::new(ObjectKind::Replica, id, Some("_replicator".into()));
            if let (Some(src), Some(dst)) = (doc.get("source"), doc.get("target")) {
                s = s.with_detail(format!("{} → {}", endpoint_text(src), endpoint_text(dst)));
            }
            if let Some(state) = jstr(doc, "_replication_state") {
                s = s.with_badge(state);
            } else if jbool(doc, "continuous") {
                s = s.with_badge("continuous");
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

fn task_subject(task: &Json) -> Option<&str> {
    ["database", "design_document", "doc_id", "replication_id", "target"].iter().find_map(|k| jstr(task, k).filter(|v| !v.is_empty()))
}

fn task_progress_text(task: &Json) -> Option<String> {
    let progress = jnum(task, "progress")?;
    let mut text = format!("{progress}%");
    if let (Some(done), Some(total)) = (jnum(task, "changes_done"), jnum(task, "total_changes")) {
        text.push_str(&format!(" ({done}/{total})"));
    }
    Some(text)
}

// WHAT:  The display name of a task: its type plus what it is working on.
//        Shared with the detail lookup so a name always maps back to its task.
fn task_name(task: &Json) -> Option<String> {
    let kind = jstr(task, "type")?;
    Some(match task_subject(task) {
        Some(subject) => format!("{kind} {subject}"),
        None => kind.to_string(),
    })
}

// WHAT:  `_active_tasks` → tasks; parent = pid so the detail can find the task again.
fn task_summaries(tasks: &[Json]) -> Vec<ObjectSummary> {
    let list = tasks
        .iter()
        .filter_map(|task| {
            let kind = jstr(task, "type")?;
            let mut s = ObjectSummary::new(ObjectKind::Task, task_name(task)?, jstr(task, "pid").map(str::to_string)).with_badge(kind);
            if let Some(p) = task_progress_text(task) {
                s = s.with_detail(p);
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

// WHAT:  `_users` docs (`org.couchdb.user:*`) plus the server admins from config.
fn user_summaries(rows: &[Json], admins: &Json) -> Vec<ObjectSummary> {
    let mut list: Vec<ObjectSummary> = admins
        .as_object()
        .into_iter()
        .flatten()
        .map(|(name, _)| ObjectSummary::new(ObjectKind::User, name, Some("_config".into())).with_badge("admin").with_detail("server admin"))
        .collect();
    list.extend(rows.iter().filter_map(|row| {
        let doc = row.get("doc").filter(|d| d.is_object()).unwrap_or(row);
        let id = jstr(doc, ID)?;
        let name = jstr(doc, "name").unwrap_or_else(|| id.trim_start_matches(USER_PREFIX));
        let roles: Vec<&str> = items(doc, "roles").filter_map(Json::as_str).collect();
        let mut s = ObjectSummary::new(ObjectKind::User, name, Some("_users".into())).with_badge("user");
        if !roles.is_empty() {
            s = s.with_detail(roles.join(", "));
        }
        Some(s)
    }));
    sorted(list)
}

// WHAT:  `_membership` → nodes, flagged whether they joined the cluster.
fn node_summaries(membership: &Json) -> Vec<ObjectSummary> {
    let cluster: Vec<&str> = items(membership, "cluster_nodes").filter_map(Json::as_str).collect();
    let mut names: Vec<&str> = items(membership, "all_nodes").filter_map(Json::as_str).collect();
    for n in &cluster {
        if !names.contains(n) {
            names.push(n);
        }
    }
    let list = names
        .into_iter()
        .map(|n| ObjectSummary::new(ObjectKind::Node, n, None).with_badge(if cluster.contains(&n) { "cluster" } else { "unjoined" }))
        .collect();
    sorted(list)
}

fn setting_value_text(section: &str, value: &Json) -> String {
    if section == "admins" {
        "••••••".to_string()
    } else {
        compact(value)
    }
}

// WHAT:  `_node/_local/_config` → one `section/key` per entry, admin hashes masked.
fn setting_summaries(config: &Json) -> Vec<ObjectSummary> {
    let list = config
        .as_object()
        .into_iter()
        .flatten()
        .flat_map(|(section, keys)| {
            keys.as_object().into_iter().flatten().map(move |(key, value)| {
                ObjectSummary::new(ObjectKind::Setting, format!("{section}/{key}"), None).with_badge(section.clone()).with_detail(setting_value_text(section, value))
            })
        })
        .collect();
    sorted(list)
}

// ---- details ----------------------------------------------------------------

fn database_detail(reference: &ObjectRef, info: &Json, design_docs: Vec<ObjectSummary>, indexes: Vec<ObjectSummary>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(info), CodeLanguage::Json);
    for (label, key) in [("Documents", "doc_count"), ("Deleted documents", "doc_del_count")] {
        if let Some(n) = jnum(info, key) {
            d = d.property(label, crate::model::objects::format_number(n));
        }
    }
    for (label, pointer) in [("File size", "/sizes/file"), ("Active size", "/sizes/active"), ("External size", "/sizes/external")] {
        if let Some(n) = pnum(info, pointer) {
            d = d.property(label, bytes_text(n));
        }
    }
    if let Some(seq) = info.get("update_seq") {
        d = d.property("Update sequence", compact(seq));
    }
    if let Some(cluster) = info.get("cluster").and_then(Json::as_object) {
        let text: Vec<String> = ["q", "n", "r", "w"].iter().filter_map(|k| cluster.get(*k).map(|v| format!("{k}={v}"))).collect();
        d = d.property("Cluster", text.join(" "));
    }
    if info.pointer("/props/partitioned").and_then(Json::as_bool) == Some(true) {
        d = d.property("Partitioned", "yes");
    }
    if jbool(info, "compact_running") {
        d = d.property("Compaction", "running");
    }
    d.children = design_docs.into_iter().chain(indexes).collect();
    let path = db_path(&reference.name);
    d.action(ObjectAction::new("compact", "Compact", passthrough("POST", &format!("{path}/_compact"))))
        .action(ObjectAction::new("view_cleanup", "Clean up old view indexes", passthrough("POST", &format!("{path}/_view_cleanup"))))
        .action(ObjectAction::destructive("delete", "Delete database", passthrough("DELETE", &path)))
}

fn design_doc_detail(reference: &ObjectRef, doc: &Json, db: &str) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(doc), CodeLanguage::Json).property("Language", jstr(doc, "language").unwrap_or("javascript"));
    if let Some(rev) = jstr(doc, "_rev") {
        d = d.property("Revision", rev);
    }
    for (label, key) in [("Views", "views"), ("Update handlers", "updates"), ("Filters", "filters"), ("Shows", "shows"), ("Lists", "lists")] {
        let n = object_len(doc, key);
        if n > 0 {
            d = d.property(label, n.to_string());
        }
    }
    if doc.get("validate_doc_update").is_some() {
        d = d.property("Validate doc update", "yes");
    }
    d.children = view_summaries(doc, db);
    let design = reference.name.trim_start_matches(DESIGN_PREFIX);
    let path = design_path(db, design);
    d = d.action(ObjectAction::new("compact", "Compact view indexes", passthrough("POST", &format!("{}/_compact/{}", db_path(db), encode_path_segment(design)))));
    if let Some(rev) = jstr(doc, "_rev") {
        d = d.action(ObjectAction::destructive("delete", "Delete design document", passthrough("DELETE", &format!("{path}?rev={}", encode_path_segment(rev)))));
    }
    d
}

fn view_detail(reference: &ObjectRef, design: &Json, sample: &[Json]) -> ObjectDetail {
    let view = design.get("views").and_then(|v| v.get(&reference.name)).cloned().unwrap_or(Json::Null);
    let mut source = String::new();
    if let Some(map) = jstr(&view, "map") {
        source.push_str("// map\n");
        source.push_str(map);
    }
    if let Some(reduce) = jstr(&view, "reduce") {
        source.push_str("\n\n// reduce\n");
        source.push_str(reduce);
    }
    let mut d = ObjectDetail::empty(reference).definition(source, CodeLanguage::Text);
    if let Some(id) = jstr(design, ID) {
        d = d.property("Design document", id);
    }
    d = d.property("Language", jstr(design, "language").unwrap_or("javascript"));
    d = d.property("Reduce", reduce_text(&view).unwrap_or_else(|| "none".into()));
    d.rows = rows_from(sample, Some("id"));
    d
}

fn index_detail(reference: &ObjectRef, index: &Json, db: &str) -> ObjectDetail {
    let kind = jstr(index, "type").unwrap_or("json");
    let mut d = ObjectDetail::empty(reference).definition(pretty(index), CodeLanguage::Json).property("Type", kind);
    if let Some(ddoc) = jstr(index, "ddoc") {
        d = d.property("Design document", ddoc);
    }
    let fields = index_fields_text(index);
    if !fields.is_empty() {
        d = d.property("Fields", fields);
    }
    if jbool(index, "partitioned") {
        d = d.property("Partitioned", "yes");
    }
    if let Some(ddoc) = jstr(index, "ddoc") {
        let path = format!("{}/_index/{}/{}/{}", db_path(db), encode_path_segment(ddoc.trim_start_matches(DESIGN_PREFIX)), encode_path_segment(kind), encode_path_segment(&reference.name));
        d = d.action(ObjectAction::destructive("delete", "Delete index", passthrough("DELETE", &path)));
    }
    d
}

fn replica_detail(reference: &ObjectRef, scheduler: Option<&Json>, doc: Option<&Json>, db: &str) -> ObjectDetail {
    let definition = serde_json::json!({"scheduler": scheduler.cloned().unwrap_or(Json::Null), "document": doc.cloned().unwrap_or(Json::Null)});
    let mut d = ObjectDetail::empty(reference).definition(pretty(&definition), CodeLanguage::Json);
    let source = scheduler.or(doc);
    if let Some(s) = source {
        if let Some(state) = jstr(s, "state").or_else(|| jstr(s, "_replication_state")) {
            d = d.property("State", state);
        }
        if let Some(src) = s.get("source") {
            d = d.property("Source", endpoint_text(src));
        }
        if let Some(dst) = s.get("target") {
            d = d.property("Target", endpoint_text(dst));
        }
    }
    if let Some(doc) = doc {
        d = d.property("Continuous", if jbool(doc, "continuous") { "yes" } else { "no" });
    }
    if let Some(s) = scheduler {
        if let Some(n) = jnum(s, "error_count") {
            d = d.property("Errors", crate::model::objects::format_number(n));
        }
        for (label, key) in [("Started", "start_time"), ("Last updated", "last_updated"), ("Node", "node")] {
            if let Some(v) = jstr(s, key) {
                d = d.property(label, v);
            }
        }
        if let Some(info) = s.get("info").filter(|i| i.is_object()) {
            d = d.property("Progress", compact(info));
        }
    }
    if let Some(rev) = doc.and_then(|x| jstr(x, "_rev")) {
        let path = format!("{}/{}?rev={}", db_path(db), encode_path_segment(&reference.name), encode_path_segment(rev));
        d = d.action(ObjectAction::destructive("delete", "Delete replication", passthrough("DELETE", &path)));
    }
    d
}

fn task_detail(reference: &ObjectRef, task: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(task), CodeLanguage::Json);
    for (label, key) in [("Type", "type"), ("Database", "database"), ("Design document", "design_document"), ("Node", "node"), ("Pid", "pid"), ("Replication", "replication_id")] {
        if let Some(v) = jstr(task, key).filter(|v| !v.is_empty()) {
            d = d.property(label, v);
        }
    }
    if let Some(p) = task_progress_text(task) {
        d = d.property("Progress", p);
    }
    for (label, key) in [("Started", "started_on"), ("Updated", "updated_on")] {
        if let Some(t) = jnum(task, key) {
            d = d.property(label, epoch_text(t));
        }
    }
    d
}

// WHAT:  A `_users` document with its password material removed.
fn redacted_user(doc: &Json) -> Json {
    let mut out = doc.clone();
    if let Some(o) = out.as_object_mut() {
        for key in ["derived_key", "salt", "password_sha", "password_scheme", "iterations", "pbkdf2_prf"] {
            o.remove(key);
        }
    }
    out
}

fn user_detail(reference: &ObjectRef, doc: &Json) -> ObjectDetail {
    let clean = redacted_user(doc);
    let mut d = ObjectDetail::empty(reference).definition(pretty(&clean), CodeLanguage::Json);
    let roles: Vec<&str> = items(doc, "roles").filter_map(Json::as_str).collect();
    d = d.property("Roles", if roles.is_empty() { "none".to_string() } else { roles.join(", ") });
    if let Some(t) = jstr(doc, "type") {
        d = d.property("Type", t);
    }
    if let (Some(id), Some(rev)) = (jstr(doc, ID), jstr(doc, "_rev")) {
        let path = format!("/_users/{}?rev={}", encode_path_segment(id), encode_path_segment(rev));
        d = d.action(ObjectAction::destructive("delete", "Delete user", passthrough("DELETE", &path)));
    }
    d
}

fn node_detail(reference: &ObjectRef, system: &Json, versions: &Json) -> ObjectDetail {
    let definition = serde_json::json!({"system": system, "versions": versions});
    let mut d = ObjectDetail::empty(reference).definition(pretty(&definition), CodeLanguage::Json);
    if let Some(up) = jnum(system, "uptime") {
        d = d.property("Uptime", duration_text(up));
    }
    if let Some(total) = pnum(system, "/memory/total") {
        d = d.property("Memory", bytes_text(total));
    }
    for (label, key) in [("Processes", "process_count"), ("Run queue", "run_queue"), ("Context switches", "context_switches")] {
        if let Some(n) = jnum(system, key) {
            d = d.property(label, crate::model::objects::format_number(n));
        }
    }
    if let Some(erlang) = versions.pointer("/erlang/version").and_then(Json::as_str) {
        d = d.property("Erlang", erlang);
    }
    d
}

fn setting_detail(reference: &ObjectRef, value: &Json) -> ObjectDetail {
    let (section, key) = reference.name.split_once('/').unwrap_or((reference.name.as_str(), ""));
    let text = setting_value_text(section, value);
    ObjectDetail::empty(reference).definition(text.clone(), CodeLanguage::Text).property("Section", section).property("Key", key).property("Value", text)
}

// ---- server stats -------------------------------------------------------------

fn number_stats(source: &Json, specs: &[(&str, &str, Option<&str>)]) -> Vec<Stat> {
    specs
        .iter()
        .filter_map(|(label, pointer, unit)| {
            let value = pnum(source, pointer)?;
            let value = if *unit == Some("MB") { value / MIB } else { value };
            Some(Stat::number(label, (value * 100.0).round() / 100.0, *unit))
        })
        .collect()
}

fn push_group(groups: &mut Vec<StatGroup>, title: &str, stats: Vec<Stat>) {
    if !stats.is_empty() {
        groups.push(StatGroup { title: title.to_string(), stats });
    }
}

// WHAT:  `/` + `_node/_local/_stats` + `_node/_local/_system` → Stats groups.
fn server_stat_groups(root: &Json, stats: &Json, system: &Json) -> Vec<StatGroup> {
    let mut groups = Vec::new();
    let mut server = Vec::new();
    if let Some(v) = jstr(root, "version") {
        server.push(Stat::text("Version", v));
    }
    if let Some(vendor) = root.pointer("/vendor/name").and_then(Json::as_str) {
        server.push(Stat::text("Vendor", vendor));
    }
    if let Some(up) = jnum(system, "uptime") {
        server.push(Stat::text("Uptime", duration_text(up)));
    }
    let features: Vec<&str> = items(root, "features").filter_map(Json::as_str).collect();
    if !features.is_empty() {
        server.push(Stat::text("Features", features.join(", ")));
    }
    server.extend(number_stats(system, &[("Processes", "/process_count", None), ("Run queue", "/run_queue", None)]));
    push_group(&mut groups, "Server", server);
    push_group(
        &mut groups,
        "Requests",
        number_stats(
            stats,
            &[
                ("Requests", "/couchdb/httpd/requests/value", None),
                ("Bulk requests", "/couchdb/httpd/bulk_requests/value", None),
                ("GET", "/couchdb/httpd_request_methods/GET/value", None),
                ("POST", "/couchdb/httpd_request_methods/POST/value", None),
                ("PUT", "/couchdb/httpd_request_methods/PUT/value", None),
                ("DELETE", "/couchdb/httpd_request_methods/DELETE/value", None),
                ("2xx", "/couchdb/httpd_status_codes/200/value", None),
                ("4xx", "/couchdb/httpd_status_codes/404/value", None),
                ("5xx", "/couchdb/httpd_status_codes/500/value", None),
                ("Mean request time", "/couchdb/request_time/value/arithmetic_mean", Some("ms")),
            ],
        ),
    );
    push_group(
        &mut groups,
        "Storage",
        number_stats(
            stats,
            &[
                ("Database reads", "/couchdb/database_reads/value", None),
                ("Database writes", "/couchdb/database_writes/value", None),
                ("Document inserts", "/couchdb/document_inserts/value", None),
                ("Document writes", "/couchdb/document_writes/value", None),
                ("Open databases", "/couchdb/open_databases/value", None),
                ("Open OS files", "/couchdb/open_os_files/value", None),
            ],
        ),
    );
    push_group(
        &mut groups,
        "Cache",
        number_stats(stats, &[("Auth cache hits", "/couchdb/auth_cache_hits/value", None), ("Auth cache misses", "/couchdb/auth_cache_misses/value", None)]),
    );
    push_group(
        &mut groups,
        "Memory",
        number_stats(
            system,
            &[
                ("Total", "/memory/total", Some("MB")),
                ("Processes", "/memory/processes", Some("MB")),
                ("Binary", "/memory/binary", Some("MB")),
                ("ETS", "/memory/ets", Some("MB")),
                ("Code", "/memory/code", Some("MB")),
                ("Atoms", "/memory/atom", Some("MB")),
            ],
        ),
    );
    push_group(&mut groups, "IO", number_stats(system, &[("Input", "/io_input", Some("MB")), ("Output", "/io_output", Some("MB")), ("Reductions", "/reductions", None)]));
    groups
}

impl CouchIntegration {
    async fn get_tolerant(&self, path: &str) -> AppResult<Json> {
        tolerated(self.http.get_json::<Json>(path).await)
    }

    async fn user_databases(&self, parent: Option<&str>) -> AppResult<Vec<String>> {
        if let Some(p) = parent.map(str::trim).filter(|p| !p.is_empty()) {
            return Ok(vec![p.to_string()]);
        }
        match &self.database {
            Some(d) => Ok(vec![d.clone()]),
            None => {
                let all: Vec<String> = self.http.get_json("/_all_dbs").await?;
                Ok(all.into_iter().filter(|n| !is_system_db(n)).collect())
            }
        }
    }

    // WHAT:  `POST /_dbs_info` in chunks (CouchDB ≥ 2.2), falling back to one
    //        `GET /{db}` per database on older servers.
    async fn database_infos(&self, names: &[String]) -> AppResult<Vec<(String, Json)>> {
        let mut out = Vec::with_capacity(names.len());
        for chunk in names.chunks(DBS_INFO_CHUNK) {
            let body = serde_json::json!({"keys": chunk});
            match self.http.post_json::<Json>("/_dbs_info", &body).await {
                Ok(reply) => {
                    for entry in reply.as_array().into_iter().flatten() {
                        if let Some(key) = jstr(entry, "key") {
                            out.push((key.to_string(), entry.get("info").cloned().unwrap_or(Json::Null)));
                        }
                    }
                }
                Err(_) => {
                    for name in chunk {
                        out.push((name.clone(), self.get_tolerant(&db_path(name)).await?));
                    }
                }
            }
        }
        Ok(out)
    }

    async fn design_docs(&self, db: &str) -> AppResult<Vec<Json>> {
        let reply = self.get_tolerant(&format!("{}/_design_docs?include_docs=true", db_path(db))).await?;
        Ok(items(&reply, "rows").cloned().collect())
    }

    async fn design_doc(&self, db: &str, id: &str) -> AppResult<Json> {
        self.http.get_json(&design_path(db, id.trim_start_matches(DESIGN_PREFIX))).await
    }

    async fn indexes(&self, db: &str) -> AppResult<Vec<ObjectSummary>> {
        Ok(index_summaries(&self.get_tolerant(&format!("{}/_index", db_path(db))).await?, db))
    }

    async fn replications(&self) -> AppResult<Vec<ObjectSummary>> {
        let scheduler = self.get_tolerant("/_scheduler/docs").await?;
        if scheduler.get("docs").is_some() {
            return Ok(replica_summaries(&scheduler));
        }
        let rows = self.get_tolerant("/_replicator/_all_docs?include_docs=true").await?;
        Ok(replicator_doc_summaries(&items(&rows, "rows").cloned().collect::<Vec<_>>()))
    }

    async fn active_tasks(&self) -> AppResult<Vec<Json>> {
        Ok(self.get_tolerant("/_active_tasks").await?.as_array().cloned().unwrap_or_default())
    }

    async fn users(&self) -> AppResult<Vec<ObjectSummary>> {
        let path = format!("/_users/_all_docs?include_docs=true&startkey=%22{USER_PREFIX}%22&endkey=%22{USER_PREFIX}%EF%BF%B0%22");
        let rows = self.get_tolerant(&path).await?;
        let admins = self.get_tolerant("/_node/_local/_config/admins").await?;
        Ok(user_summaries(&items(&rows, "rows").cloned().collect::<Vec<_>>(), &admins))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, views: true, ..Capabilities::DOCUMENT },
        object_kinds: vec![K::Database, K::Document, K::View, K::Index, K::Replica, K::Task, K::User, K::Node, K::Setting],
        tools: vec![T::Stats],
    }
}

#[async_trait]
impl Integration for CouchIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _: serde_json::Value = self.http.get_json("/").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v: serde_json::Value = self.http.get_json("/").await?;
        Ok(v.get("version").and_then(|s| s.as_str()).map(|s| format!("CouchDB {s}")))
    }

    fn current_database(&self) -> Option<String> {
        self.database.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let all: Vec<String> = self.http.get_json("/_all_dbs").await?;
        let keep = |n: &String| !n.starts_with('_') || self.database.as_deref() == Some(n.as_str());
        Ok(all.into_iter().filter(keep).collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let dbs = match &self.database {
            Some(d) => vec![d.clone()],
            None => self.databases().await?,
        };
        let mut schemas = Vec::new();
        for db in dbs {
            let info: serde_json::Value = self.http.get_json(&format!("/{}", encode_path_segment(&db))).await?;
            let doc_count = info.get("doc_count").and_then(|c| c.as_i64());
            let mut tables = vec![TableInfo { schema: Some(db.clone()), name: DOCUMENTS.into(), kind: TableKind::Table, row_estimate: doc_count }];
            let designs: serde_json::Value = self
                .http
                .get_json(&format!("/{}/_design_docs?include_docs=true", encode_path_segment(&db)))
                .await
                .unwrap_or(serde_json::Value::Null);
            for row in designs.get("rows").and_then(|r| r.as_array()).into_iter().flatten() {
                let Some(doc) = row.get("doc") else { continue };
                let id = doc.get(ID).and_then(|i| i.as_str()).unwrap_or_default();
                let design = id.trim_start_matches("_design/");
                for view in doc.get("views").and_then(|v| v.as_object()).into_iter().flat_map(|o| o.keys()) {
                    tables.push(TableInfo { schema: Some(db.clone()), name: format!("{design}/{view}"), kind: TableKind::View, row_estimate: None });
                }
            }
            schemas.push(SchemaInfo { name: db, tables });
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        if self.is_view(table) {
            let rows = self.sample(table).await?;
            let mut cols: Vec<ColumnInfo> = ["id", "key", "value"]
                .iter()
                .enumerate()
                .map(|(i, n)| ColumnInfo {
                    name: (*n).to_string(),
                    data_type: rows.iter().find_map(|r| r.get(*n).filter(|v| !v.is_null()).map(json_type_name)).unwrap_or("json").to_string(),
                    nullable: true,
                    primary_key: *n == "id",
                    ordinal: i as u32 + 1,
                })
                .collect();
            cols.push(ColumnInfo { name: "doc".into(), data_type: "object".into(), nullable: true, primary_key: false, ordinal: 4 });
            return Ok(cols);
        }
        let docs = self.sample(table).await?;
        let mut cols = union_columns(&docs);
        if cols.is_empty() {
            cols.push(ColumnInfo { name: ID.into(), data_type: "string".into(), nullable: false, primary_key: true, ordinal: 1 });
            cols.push(ColumnInfo { name: "_rev".into(), data_type: "string".into(), nullable: true, primary_key: false, ordinal: 2 });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        if self.is_view(table) {
            return Ok(None);
        }
        let db = self.db_for(table)?;
        let info: serde_json::Value = self.http.get_json(&format!("/{}", encode_path_segment(&db))).await?;
        Ok(info.get("doc_count").and_then(|c| c.as_i64()))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let db = self.db_for(table)?;
        if let Some((d, v)) = view_parts(&table.name) {
            let path = format!("/{}/_design/{}/_view/{}?limit=0", encode_path_segment(&db), encode_path_segment(d), encode_path_segment(v));
            let resp: serde_json::Value = self.http.get_json(&path).await?;
            return Ok(resp.get("total_rows").and_then(|t| t.as_i64()).unwrap_or(0));
        }
        if filters.is_empty() {
            return Ok(self.row_estimate(table).await?.unwrap_or(0));
        }
        let mut total = 0i64;
        let mut skip = 0usize;
        loop {
            let body = serde_json::json!({"selector": mango_selector(filters), "fields": [ID], "skip": skip, "limit": 1000});
            let n = self.find(&db, &body).await?.len();
            total += n as i64;
            skip += n;
            if n < 1000 || skip >= SCAN_CAP {
                break;
            }
        }
        Ok(total)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let db = self.db_for(table)?;
        let columns = self.columns(table).await?;
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        if let Some((d, v)) = view_parts(&table.name) {
            let needs_local = !query.sort.is_empty() || !query.filters.is_empty();
            let (skip, limit) = if needs_local { (0, SCAN_CAP.min(query.offset as usize + query.limit as usize)) } else { (query.offset, query.limit as usize) };
            let rows = self.view_rows(&db, d, v, skip, limit).await?;
            let mut rs = docs_to_rows(&columns, &rows);
            if needs_local {
                rs.rows = local::page(&names, rs.rows, query);
            }
            return Ok(rs);
        }
        let docs = match self.find(&db, &mango_body(query)).await {
            Ok(docs) => docs,
            Err(e) if !query.sort.is_empty() && e.message().contains("no_usable_index") => {
                let cap = SCAN_CAP.min(query.offset as usize + query.limit as usize);
                let all = self.all_docs(&db, 0, cap).await?;
                let rs = docs_to_rows(&columns, &all);
                return Ok(ResultSet { rows: local::page(&names, rs.rows, query), ..rs });
            }
            Err(e) => return Err(e),
        };
        Ok(docs_to_rows(&columns, &docs))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut out = Vec::new();
        for stmt in split_blank_lines(sql) {
            let cmd = parse_command(&stmt)?;
            let result = match cmd {
                Command::Mango(mut body) => {
                    let db = self.db()?;
                    if body.get("limit").is_none() {
                        body["limit"] = serde_json::json!(max_rows);
                    }
                    let docs = self.find(db, &body).await?;
                    StatementResult::Rows { result: objects_to_result_set(&docs, Some(ID), max_rows) }
                }
                Command::Get(id) => {
                    let db = self.db()?;
                    let doc: serde_json::Value = self.http.get_json(&format!("/{}/{}", encode_path_segment(db), encode_path_segment(&id))).await?;
                    StatementResult::Rows { result: objects_to_result_set(std::slice::from_ref(&doc), Some(ID), max_rows) }
                }
                Command::All(n) => {
                    let db = self.db()?;
                    let docs = self.all_docs(db, 0, n.min(max_rows)).await?;
                    StatementResult::Rows { result: objects_to_result_set(&docs, Some(ID), max_rows) }
                }
                Command::Passthrough { method, path, body } => {
                    let m = Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Unknown HTTP method {method}.")))?;
                    let write = matches!(m, Method::PUT | Method::POST | Method::DELETE) && !path.contains("/_find") && !path.contains("/_all_docs");
                    if write && self.read_only {
                        return Err(AppError::read_only("This connection is read-only; PUT/POST/DELETE requests are blocked."));
                    }
                    let resp = self.request_json(m, &path, body).await?;
                    if write {
                        StatementResult::Affected { rows_affected: if resp.get("ok").and_then(|o| o.as_bool()) == Some(true) { 1 } else { 0 } }
                    } else {
                        let mut rs = json_result(resp);
                        rs.truncated = rs.rows.len() > max_rows;
                        rs.rows.truncate(max_rows);
                        StatementResult::Rows { result: rs }
                    }
                }
            };
            out.push(result);
        }
        Ok(out)
    }

    async fn close(&self) {}

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Database => {
                let all: Vec<String> = self.http.get_json("/_all_dbs").await?;
                let names: Vec<String> = all.into_iter().take(OBJECT_CAP).collect();
                Ok(database_summaries(&self.database_infos(&names).await?))
            }
            ObjectKind::Document => {
                let mut all = Vec::new();
                for db in self.user_databases(parent).await? {
                    all.extend(design_doc_summaries(&self.design_docs(&db).await?, &db));
                    if all.len() >= OBJECT_CAP {
                        break;
                    }
                }
                Ok(sorted(all))
            }
            ObjectKind::View => {
                if let Some((db, design)) = parent.and_then(split_design_parent) {
                    return Ok(view_summaries(&self.design_doc(db, design).await?, db));
                }
                let mut all = Vec::new();
                for db in self.user_databases(parent).await? {
                    for row in self.design_docs(&db).await? {
                        let doc = row.get("doc").filter(|d| d.is_object()).unwrap_or(&row);
                        all.extend(view_summaries(doc, &db));
                    }
                    if all.len() >= OBJECT_CAP {
                        break;
                    }
                }
                Ok(sorted(all))
            }
            ObjectKind::Index => {
                let mut all = Vec::new();
                for db in self.user_databases(parent).await? {
                    all.extend(self.indexes(&db).await?);
                    if all.len() >= OBJECT_CAP {
                        break;
                    }
                }
                Ok(sorted(all))
            }
            ObjectKind::Replica => self.replications().await,
            ObjectKind::Task => Ok(task_summaries(&self.active_tasks().await?)),
            ObjectKind::User => self.users().await,
            ObjectKind::Node => Ok(node_summaries(&self.get_tolerant("/_membership").await?)),
            ObjectKind::Setting => Ok(setting_summaries(&self.get_tolerant("/_node/_local/_config").await?)),
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let parent = reference.parent.as_deref().map(str::trim).filter(|p| !p.is_empty());
        match reference.kind {
            ObjectKind::Database => {
                let info: Json = self.http.get_json(&db_path(name)).await?;
                let design_docs = design_doc_summaries(&self.design_docs(name).await?, name);
                let indexes = self.indexes(name).await?;
                Ok(database_detail(reference, &info, design_docs, indexes))
            }
            ObjectKind::Document => {
                let db = parent.map(str::to_string).or_else(|| self.database.clone()).ok_or_else(|| AppError::invalid_input("A design document reference needs its database as parent."))?;
                let doc = self.design_doc(&db, name).await?;
                Ok(design_doc_detail(reference, &doc, &db))
            }
            ObjectKind::View => {
                let (db, design) = parent.and_then(split_design_parent).ok_or_else(|| AppError::invalid_input("A view reference needs `db/_design/name` as parent."))?;
                let doc = self.design_doc(db, design).await?;
                let sample_path = format!("{}/_view/{}?limit={VIEW_SAMPLE}", design_path(db, design), encode_path_segment(name));
                let sample = self.get_tolerant(&sample_path).await?;
                Ok(view_detail(reference, &doc, &items(&sample, "rows").cloned().collect::<Vec<_>>()))
            }
            ObjectKind::Index => {
                let db = parent.map(str::to_string).or_else(|| self.database.clone()).ok_or_else(|| AppError::invalid_input("An index reference needs its database as parent."))?;
                let reply: Json = self.http.get_json(&format!("{}/_index", db_path(&db))).await?;
                let index = items(&reply, "indexes").find(|i| jstr(i, "name") == Some(name)).ok_or_else(|| AppError::not_found(format!("Index {name} not found in {db}.")))?;
                Ok(index_detail(reference, index, &db))
            }
            ObjectKind::Replica => {
                let db = parent.unwrap_or("_replicator");
                let scheduler = self.get_tolerant(&format!("/_scheduler/docs/{}/{}", encode_path_segment(db), encode_path_segment(name))).await?;
                let doc = self.get_tolerant(&format!("{}/{}", db_path(db), encode_path_segment(name))).await?;
                fn present(v: &Json) -> bool {
                    v.as_object().is_some_and(|o| !o.is_empty())
                }
                Ok(replica_detail(reference, Some(&scheduler).filter(|v| present(v)), Some(&doc).filter(|v| present(v)), db))
            }
            ObjectKind::Task => {
                // Match on the pid first (stable), then on the display name.
                let tasks = self.active_tasks().await?;
                let task = tasks
                    .iter()
                    .find(|t| parent.is_some() && jstr(t, "pid") == parent)
                    .or_else(|| tasks.iter().find(|t| task_name(t).as_deref() == Some(name)))
                    .ok_or_else(|| AppError::not_found("This task has finished."))?;
                Ok(task_detail(reference, task))
            }
            ObjectKind::User => {
                if parent == Some("_config") {
                    let admins = self.get_tolerant("/_node/_local/_config/admins").await?;
                    let value = admins.get(name).cloned().unwrap_or(Json::Null);
                    return Ok(ObjectDetail::empty(reference).definition(setting_value_text("admins", &value), CodeLanguage::Text).property("Role", "server admin"));
                }
                let doc: Json = self.http.get_json(&format!("/_users/{}", encode_path_segment(&format!("{USER_PREFIX}{name}")))).await?;
                Ok(user_detail(reference, &doc))
            }
            ObjectKind::Node => {
                let node = encode_path_segment(name);
                let system = self.get_tolerant(&format!("/_node/{node}/_system")).await?;
                let versions = self.get_tolerant(&format!("/_node/{node}/_versions")).await?;
                Ok(node_detail(reference, &system, &versions))
            }
            ObjectKind::Setting => {
                let (section, key) = name.split_once('/').ok_or_else(|| AppError::invalid_input("A setting is named `section/key`."))?;
                let value = self.get_tolerant(&format!("/_node/_local/_config/{}/{}", encode_path_segment(section), encode_path_segment(key))).await?;
                Ok(setting_detail(reference, &value))
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let root: Json = self.http.get_json("/").await?;
        let stats = self.get_tolerant("/_node/_local/_stats").await?;
        let system = self.get_tolerant("/_node/_local/_system").await?;
        Ok(ServerStats::now(server_stat_groups(&root, &stats, &system)))
    }
}

pub fn split_blank_lines(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !cur.trim().is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            cur.clear();
        } else {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    #[test]
    fn selector_maps_operators() {
        let rules = vec![
            FilterRule { column: "age".into(), op: FilterOp::Gte, value: "18".into() },
            FilterRule { column: "name".into(), op: FilterOp::StartsWith, value: "a.b".into() },
            FilterRule { column: "x".into(), op: FilterOp::IsNull, value: String::new() },
            FilterRule { column: "t".into(), op: FilterOp::In, value: "a, 2".into() },
        ];
        let sel = mango_selector(&rules);
        assert_eq!(sel["$and"][0], serde_json::json!({"age": {"$gte": 18}}));
        assert_eq!(sel["$and"][1], serde_json::json!({"name": {"$regex": "(?i)^a\\.b"}}));
        assert_eq!(sel["$and"][2], serde_json::json!({"x": {"$exists": false}}));
        assert_eq!(sel["$and"][3], serde_json::json!({"t": {"$in": ["a", 2]}}));
        assert_eq!(mango_selector(&[]), serde_json::json!({"_id": {"$gt": null}}));
    }

    #[test]
    fn body_includes_sort_and_window() {
        let q = PageQuery { sort: vec![SortRule { column: "n".into(), desc: true }], filters: vec![], offset: 20, limit: 10 };
        let b = mango_body(&q);
        assert_eq!(b["skip"], 20);
        assert_eq!(b["limit"], 10);
        assert_eq!(b["sort"], serde_json::json!([{"n": "desc"}]));
    }

    #[test]
    fn commands_parse() {
        assert!(matches!(parse_command("GET abc"), Ok(Command::Get(id)) if id == "abc"));
        assert!(matches!(parse_command("all 5"), Ok(Command::All(5))));
        assert!(matches!(parse_command(r#"{"selector": {"a": 1}}"#), Ok(Command::Mango(_))));
        assert!(matches!(parse_command(r#"{"a": 1}"#), Ok(Command::Mango(v)) if v["selector"]["a"] == 1));
        assert!(matches!(parse_command(r#"{"method":"delete","path":"/db/x"}"#), Ok(Command::Passthrough { method, .. }) if method == "DELETE"));
        assert!(parse_command("SELECT 1").is_err());
    }

    #[test]
    fn view_names_and_encoding() {
        assert_eq!(view_parts("app/by_name"), Some(("app", "by_name")));
        assert_eq!(view_parts("documents"), None);
        assert_eq!(encode_path_segment("a b/c"), "a%20b%2Fc");
    }

    #[test]
    fn explorer_helpers() {
        assert_eq!(split_design_parent("shop/_design/app"), Some(("shop", "app")));
        assert_eq!(split_design_parent("shop"), None);
        assert_eq!(endpoint_text(&serde_json::json!("http://admin:pw@host:5984/db")), "http://host:5984/db");
        assert_eq!(endpoint_text(&serde_json::json!({"url": "https://h/db"})), "https://h/db");
        assert_eq!(passthrough("DELETE", "/shop"), r#"{"method":"DELETE","path":"/shop"}"#);
        assert_eq!(bytes_text(2_621_440.0), "2.5 MB");
        assert_eq!(duration_text(3_661.0), "1h 1m");
        assert!(matches!(tolerated::<Vec<u8>>(Err(AppError::not_connected("403"))), Ok(v) if v.is_empty()));
        assert!(tolerated::<Vec<u8>>(Err(AppError::driver("500"))).is_err());
        assert!(epoch_text(0.0).starts_with("1970-01-01"));
    }

    #[test]
    fn databases_and_design_docs_map() {
        let infos = vec![
            ("shop".to_string(), serde_json::json!({"doc_count": 1200, "sizes": {"file": 4096, "active": 2048}, "props": {"partitioned": true}})),
            ("_users".to_string(), serde_json::json!({"doc_count": 2, "sizes": {"file": 100}})),
        ];
        let dbs = database_summaries(&infos);
        assert_eq!(dbs[0].reference.name, "_users");
        assert_eq!(dbs[0].badge.as_deref(), Some("system"));
        assert_eq!(dbs[0].detail.as_deref(), Some("2 docs · 100 B"));
        assert_eq!(dbs[1].badge.as_deref(), Some("partitioned"));
        assert_eq!(dbs[1].detail.as_deref(), Some("1,200 docs · 2.0 KB"));

        let r = ObjectRef { kind: ObjectKind::Database, name: "shop".into(), parent: None };
        let d = database_detail(&r, &infos[0].1, vec![ObjectSummary::new(ObjectKind::Document, "_design/app", Some("shop".into()))], vec![]);
        assert!(d.properties.iter().any(|p| p.name == "Documents" && p.value == "1,200"));
        assert!(d.properties.iter().any(|p| p.name == "Partitioned"));
        assert_eq!(d.children.len(), 1);
        let ids: Vec<&str> = d.actions.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(ids, vec!["compact", "view_cleanup", "delete"]);
        assert!(d.actions[2].destructive && !d.actions[0].destructive);
        assert_eq!(d.actions[2].statement, r#"{"method":"DELETE","path":"/shop"}"#);

        let rows = vec![serde_json::json!({"id": "_design/app", "doc": {
            "_id": "_design/app", "_rev": "3-abc", "language": "javascript",
            "views": {"by_city": {"map": "function(doc){emit(doc.city)}", "reduce": "_count"}, "all": {"map": "function(doc){emit(doc._id)}"}},
            "filters": {"mine": "function(){}"}, "validate_doc_update": "function(){}"
        }})];
        let docs = design_doc_summaries(&rows, "shop");
        assert_eq!(docs[0].reference.name, "_design/app");
        assert_eq!(docs[0].detail.as_deref(), Some("2 views · 1 filters · validate"));
        assert_eq!(docs[0].badge.as_deref(), Some("javascript"));
        let views = view_summaries(&rows[0]["doc"], "shop");
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].reference.name, "all");
        assert_eq!(views[0].badge.as_deref(), Some("map"));
        assert_eq!(views[1].detail.as_deref(), Some("map + reduce _count"));
        assert_eq!(views[1].reference.parent.as_deref(), Some("shop/_design/app"));

        let dr = ObjectRef { kind: ObjectKind::Document, name: "_design/app".into(), parent: Some("shop".into()) };
        let dd = design_doc_detail(&dr, &rows[0]["doc"], "shop");
        assert_eq!(dd.children.len(), 2);
        assert!(dd.properties.iter().any(|p| p.name == "Views" && p.value == "2"));
        assert_eq!(dd.actions[1].statement, r#"{"method":"DELETE","path":"/shop/_design/app?rev=3-abc"}"#);
        assert_eq!(dd.actions[0].statement, r#"{"method":"POST","path":"/shop/_compact/app"}"#);

        let vr = ObjectRef { kind: ObjectKind::View, name: "by_city".into(), parent: Some("shop/_design/app".into()) };
        let vd = view_detail(&vr, &rows[0]["doc"], &[serde_json::json!({"id": "d1", "key": "Oslo", "value": 1})]);
        assert_eq!(vd.language, CodeLanguage::Text);
        assert_eq!(vd.definition.as_deref(), Some("// map\nfunction(doc){emit(doc.city)}\n\n// reduce\n_count"));
        assert_eq!(vd.rows.as_ref().map(|r| r.columns[0].name.as_str()), Some("id"));
    }

    #[test]
    fn indexes_replications_tasks_map() {
        let reply = serde_json::json!({"indexes": [
            {"ddoc": null, "name": "_all_docs", "type": "special", "def": {"fields": [{"_id": "asc"}]}},
            {"ddoc": "_design/idx", "name": "by_age", "type": "json", "def": {"fields": [{"age": "asc"}, {"name": "desc"}]}}
        ]});
        let idx = index_summaries(&reply, "shop");
        assert_eq!(idx[0].reference.name, "_all_docs");
        assert_eq!(idx[0].badge.as_deref(), Some("special"));
        assert_eq!(idx[1].detail.as_deref(), Some("age asc, name desc"));
        let ir = ObjectRef { kind: ObjectKind::Index, name: "by_age".into(), parent: Some("shop".into()) };
        let id = index_detail(&ir, &reply["indexes"][1], "shop");
        assert_eq!(id.actions[0].statement, r#"{"method":"DELETE","path":"/shop/_index/idx/json/by_age"}"#);
        assert!(index_detail(&ir, &reply["indexes"][0], "shop").actions.is_empty(), "the built-in index cannot be deleted");

        let sched = serde_json::json!({"docs": [{"database": "_replicator", "doc_id": "rep1", "source": "http://u:p@a/db", "target": {"url": "http://b/db"}, "state": "running", "error_count": 0, "start_time": "2024-01-01T00:00:00Z", "info": {"changes_pending": 3}}]});
        let reps = replica_summaries(&sched);
        assert_eq!(reps[0].detail.as_deref(), Some("http://a/db → http://b/db"));
        assert_eq!(reps[0].badge.as_deref(), Some("running"));
        let rr = ObjectRef { kind: ObjectKind::Replica, name: "rep1".into(), parent: Some("_replicator".into()) };
        let doc = serde_json::json!({"_id": "rep1", "_rev": "1-x", "source": "http://a/db", "target": "http://b/db", "continuous": true});
        let rd = replica_detail(&rr, Some(&sched["docs"][0]), Some(&doc), "_replicator");
        assert!(rd.properties.iter().any(|p| p.name == "Continuous" && p.value == "yes"));
        assert!(rd.properties.iter().any(|p| p.name == "State" && p.value == "running"));
        assert_eq!(rd.actions[0].statement, r#"{"method":"DELETE","path":"/_replicator/rep1?rev=1-x"}"#);
        let legacy = replicator_doc_summaries(&[serde_json::json!({"id": "rep2", "doc": {"_id": "rep2", "source": "a", "target": "b", "_replication_state": "completed"}}), serde_json::json!({"id": "_design/_replicator", "doc": {"_id": "_design/_replicator"}})]);
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].badge.as_deref(), Some("completed"));

        let tasks = vec![serde_json::json!({"type": "database_compaction", "database": "shards/00000000-1fffffff/shop.1700000000", "pid": "<0.123.0>", "progress": 42, "changes_done": 420, "total_changes": 1000, "started_on": 1700000000})];
        let t = task_summaries(&tasks);
        assert_eq!(t[0].reference.name, "database_compaction shards/00000000-1fffffff/shop.1700000000");
        assert_eq!(t[0].reference.parent.as_deref(), Some("<0.123.0>"));
        assert_eq!(t[0].detail.as_deref(), Some("42% (420/1000)"));
        let tr = ObjectRef { kind: ObjectKind::Task, name: t[0].reference.name.clone(), parent: t[0].reference.parent.clone() };
        let td = task_detail(&tr, &tasks[0]);
        assert!(td.properties.iter().any(|p| p.name == "Started" && p.value.starts_with("2023-11-14")));
    }

    #[test]
    fn users_nodes_settings_map() {
        let rows = vec![serde_json::json!({"id": "org.couchdb.user:bob", "doc": {"_id": "org.couchdb.user:bob", "_rev": "1-a", "name": "bob", "roles": ["reader"], "type": "user", "derived_key": "secret", "salt": "s"}})];
        let admins = serde_json::json!({"admin": "-pbkdf2-abc"});
        let users = user_summaries(&rows, &admins);
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].reference.name, "admin");
        assert_eq!(users[0].badge.as_deref(), Some("admin"));
        assert_eq!(users[1].detail.as_deref(), Some("reader"));
        let ur = ObjectRef { kind: ObjectKind::User, name: "bob".into(), parent: Some("_users".into()) };
        let ud = user_detail(&ur, &rows[0]["doc"]);
        assert!(!ud.definition.as_deref().unwrap_or_default().contains("secret"), "password material is redacted");
        assert_eq!(ud.actions[0].statement, r#"{"method":"DELETE","path":"/_users/org.couchdb.user%3Abob?rev=1-a"}"#);

        let nodes = node_summaries(&serde_json::json!({"all_nodes": ["couchdb@a", "couchdb@b"], "cluster_nodes": ["couchdb@a"]}));
        assert_eq!(nodes[0].badge.as_deref(), Some("cluster"));
        assert_eq!(nodes[1].badge.as_deref(), Some("unjoined"));
        let nr = ObjectRef { kind: ObjectKind::Node, name: "couchdb@a".into(), parent: None };
        let nd = node_detail(&nr, &serde_json::json!({"uptime": 7200, "memory": {"total": 52428800}, "process_count": 300}), &serde_json::json!({"erlang": {"version": "24"}}));
        assert!(nd.properties.iter().any(|p| p.name == "Memory" && p.value == "50.0 MB"));
        assert!(nd.properties.iter().any(|p| p.name == "Erlang" && p.value == "24"));

        let settings = setting_summaries(&serde_json::json!({"httpd": {"port": "5984", "bind_address": "0.0.0.0"}, "admins": {"admin": "-pbkdf2-x"}}));
        let names: Vec<&str> = settings.iter().map(|s| s.reference.name.as_str()).collect();
        assert_eq!(names, vec!["admins/admin", "httpd/bind_address", "httpd/port"]);
        assert_eq!(settings[0].detail.as_deref(), Some("••••••"));
        assert_eq!(settings[2].badge.as_deref(), Some("httpd"));
        let sr = ObjectRef { kind: ObjectKind::Setting, name: "httpd/port".into(), parent: None };
        let sd = setting_detail(&sr, &serde_json::json!("5984"));
        assert!(sd.properties.iter().any(|p| p.name == "Section" && p.value == "httpd"));
        assert_eq!(sd.definition.as_deref(), Some("5984"));
    }

    #[test]
    fn server_stats_group_figures() {
        let root = serde_json::json!({"couchdb": "Welcome", "version": "3.3.2", "vendor": {"name": "The Apache Software Foundation"}, "features": ["partitioned", "scheduler"]});
        let stats = serde_json::json!({"couchdb": {
            "httpd": {"requests": {"value": 120}, "bulk_requests": {"value": 3}},
            "httpd_request_methods": {"GET": {"value": 100}, "POST": {"value": 20}},
            "httpd_status_codes": {"200": {"value": 110}},
            "request_time": {"value": {"arithmetic_mean": 4.5}},
            "database_reads": {"value": 50}, "database_writes": {"value": 7},
            "open_databases": {"value": 4}, "open_os_files": {"value": 9},
            "auth_cache_hits": {"value": 1}
        }});
        let system = serde_json::json!({"uptime": 90, "memory": {"total": 104857600, "processes": 52428800}, "process_count": 250, "run_queue": 0, "io_input": 1048576});
        let groups = server_stat_groups(&root, &stats, &system);
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Requests", "Storage", "Cache", "Memory", "IO"]);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("3.3.2".into()));
        assert_eq!(find("Server", "Uptime").map(|s| s.value), Some("1m 30s".into()));
        assert_eq!(find("Requests", "Requests").and_then(|s| s.numeric), Some(120.0));
        assert_eq!(find("Requests", "Mean request time").map(|s| s.unit), Some(Some("ms".into())));
        assert_eq!(find("Memory", "Total").and_then(|s| s.numeric), Some(100.0));
        assert_eq!(find("Storage", "Open databases").and_then(|s| s.numeric), Some(4.0));
        assert_eq!(server_stat_groups(&root, &serde_json::json!({}), &serde_json::json!({})).len(), 1, "non-admins still get the Server group");
    }

    fn live_conn(url: String) -> ResolvedConnection {
        ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Couchdb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: Some("dbfree_test".into()),
                username: std::env::var("DBFREE_TEST_COUCHDB_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_COUCHDB_PASSWORD").ok(),
        }
    }

    // Runs only when DBFREE_TEST_COUCHDB_URL is set (with _USER and _PASSWORD):
    // `docker run --rm -d -p 5984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=pw couchdb:3`.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_COUCHDB_URL") else {
            return;
        };
        let resolved = live_conn(url);
        // The database must exist before `connect` can attach to it.
        let boot = HttpClient::from_connection(&resolved, Some(5984), false, HttpClient::auth_from_connection(&resolved))
            .unwrap_or_else(|e| panic!("client: {e}"));
        let _ = boot.send(boot.request(Method::PUT, "/dbfree_test")).await;
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.to_lowercase().contains("couchdb"), "{version}");
        db.execute(r#"{"method":"PUT","path":"/dbfree_test/doc1","body":{"city":"Berlin","n":1}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("put doc1: {e}"));
        db.execute(r#"{"method":"PUT","path":"/dbfree_test/doc2","body":{"city":"Paris","n":2}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("put doc2: {e}"));

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(catalog.schemas.iter().any(|s| s.tables.iter().any(|t| t.name == "documents")));
        let table = TableRef { schema: Some("dbfree_test".into()), name: "documents".into() };
        let cols = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "_id" && c.primary_key), "{cols:?}");
        assert!(cols.iter().any(|c| c.name == "city"), "{cols:?}");
        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert!(page.rows.len() >= 2, "{page:?}");
        assert!(db.count(&table, &[]).await.unwrap_or_default() >= 2);
        let found = db
            .execute(r#"{"selector":{"city":"Paris"}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("find: {e}"));
        match found.first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows.len(), 1, "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        let _ = db.execute(r#"{"method":"DELETE","path":"/dbfree_test"}"#, 10).await;
        db.close().await;
    }

}
