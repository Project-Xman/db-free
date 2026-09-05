// SOT: arangodb-integration, aql, arango-http-api, arango-cursor, arango-graphs, arango-object-explorer, arango-server-stats, arango-cluster-health

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_to_value, json_type_name, objects_to_result_set, Auth, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectProperty,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, SortRule, SslMode,
    Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Map, Value as Json};
use std::collections::BTreeMap;
use std::sync::Arc;

// ============================================================================
// WHAT:  ArangoDB adapter over the HTTP API (port 8529).
// WHY:   Collections are schemaless documents; the grid needs a stable header,
//        so columns are the union of keys across a 50-document AQL sample with
//        `_key` pinned first and marked primary key (`_id`, `_rev`, and for edge
//        collections `_from` / `_to` follow).
// HOW:   Auth: username + secret → POST /_open/auth for a JWT (falls back to
//        Basic when the JWT endpoint is unavailable); secret only → Bearer.
//        Every request is scoped to `/_db/{database}` (`_system` by default).
//        Paging / filtering / counting are AQL through POST /_api/cursor with
//        bind variables (`d[@c] == @v`, `LIKE(TO_STRING(d[@c]), @v, true)`,
//        `d[@c] IN @v`, `d[@c] == null`), following cursors with PUT
//        /_api/cursor/{id} up to the row cap. Named graphs (GET /_api/gharial)
//        appear as a `graphs` schema whose tables are read-only views.
//        `execute` takes AQL text or JSON `{"query","bindVars"}`; mutating AQL
//        keywords are refused when the connection is read-only.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs (trait)
// ============================================================================

const DEFAULT_PORT: u16 = 8529;
const DEFAULT_DATABASE: &str = "_system";
const GRAPH_SCHEMA: &str = "graphs";
const SAMPLE_SIZE: u32 = 50;
const MAX_PAGE_ROWS: u32 = 5_000;
const CURSOR_BATCH: usize = 1_000;
const SYSTEM_FIELDS: [&str; 5] = ["_key", "_id", "_rev", "_from", "_to"];
const WRITE_KEYWORDS: [&str; 5] = ["INSERT", "UPDATE", "REMOVE", "REPLACE", "UPSERT"];

pub struct ArangoIntegration {
    engine: Engine,
    http: HttpClient,
    database: String,
    read_only: bool,
}

#[derive(Debug, serde::Deserialize)]
struct CursorResponse {
    #[serde(default)]
    result: Vec<Json>,
    #[serde(default, rename = "hasMore")]
    has_more: bool,
    id: Option<String>,
    #[serde(default)]
    extra: Json,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let base = base_url(conn, Some(DEFAULT_PORT), false);
    let insecure = s.ssl_mode == SslMode::Require;
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    let secret = conn.secret.as_deref().filter(|p| !p.is_empty());
    let auth = match (user, secret) {
        (Some(u), Some(p)) => {
            let anon = HttpClient::new(&base, Auth::None, insecure)?;
            match anon.post_json::<Json>("/_open/auth", &json!({ "username": u, "password": p })).await {
                Ok(v) => match v.get("jwt").and_then(Json::as_str) {
                    Some(jwt) => Auth::Bearer(jwt.to_string()),
                    None => Auth::Basic { user: u.to_string(), password: p.to_string() },
                },
                Err(AppError::NotConnected { .. }) => Auth::Basic { user: u.to_string(), password: p.to_string() },
                Err(e) => return Err(e),
            }
        }
        _ => HttpClient::auth_from_connection(conn),
    };
    let http = HttpClient::new(base, auth, insecure)?;
    let integration = ArangoIntegration { engine: s.engine, http, database, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// WHAT:  Parses a filter value the way a person types it; system fields stay strings.
fn lenient_value(column: &str, raw: &str) -> Json {
    let t = raw.trim();
    if SYSTEM_FIELDS.contains(&column) {
        return Json::String(t.to_string());
    }
    if t.eq_ignore_ascii_case("true") {
        return Json::Bool(true);
    }
    if t.eq_ignore_ascii_case("false") {
        return Json::Bool(false);
    }
    if t.eq_ignore_ascii_case("null") {
        return Json::Null;
    }
    if let Ok(i) = t.parse::<i64>() {
        return json!(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return json!(f);
    }
    Json::String(t.to_string())
}

// WHAT:  Filters → AQL FILTER clauses with bind variables (`c{i}` = attribute, `v{i}` = value).
fn filter_clause(filters: &[FilterRule], binds: &mut Map<String, Json>) -> String {
    let mut parts = Vec::new();
    for (i, f) in filters.iter().enumerate() {
        let c = format!("c{i}");
        let v = format!("v{i}");
        binds.insert(c.clone(), Json::String(f.column.clone()));
        let value = f.value.trim();
        let expr = match f.op {
            FilterOp::Eq => {
                binds.insert(v.clone(), lenient_value(&f.column, value));
                format!("d[@{c}] == @{v}")
            }
            FilterOp::Ne => {
                binds.insert(v.clone(), lenient_value(&f.column, value));
                format!("d[@{c}] != @{v}")
            }
            FilterOp::Gt => {
                binds.insert(v.clone(), lenient_value(&f.column, value));
                format!("d[@{c}] > @{v}")
            }
            FilterOp::Gte => {
                binds.insert(v.clone(), lenient_value(&f.column, value));
                format!("d[@{c}] >= @{v}")
            }
            FilterOp::Lt => {
                binds.insert(v.clone(), lenient_value(&f.column, value));
                format!("d[@{c}] < @{v}")
            }
            FilterOp::Lte => {
                binds.insert(v.clone(), lenient_value(&f.column, value));
                format!("d[@{c}] <= @{v}")
            }
            FilterOp::Contains => {
                binds.insert(v.clone(), Json::String(format!("%{value}%")));
                format!("LIKE(TO_STRING(d[@{c}]), @{v}, true)")
            }
            FilterOp::StartsWith => {
                binds.insert(v.clone(), Json::String(format!("{value}%")));
                format!("LIKE(TO_STRING(d[@{c}]), @{v}, true)")
            }
            FilterOp::EndsWith => {
                binds.insert(v.clone(), Json::String(format!("%{value}")));
                format!("LIKE(TO_STRING(d[@{c}]), @{v}, true)")
            }
            FilterOp::In => {
                let items: Vec<Json> = value
                    .split(',')
                    .map(str::trim)
                    .filter(|x| !x.is_empty())
                    .map(|x| lenient_value(&f.column, x))
                    .collect();
                binds.insert(v.clone(), Json::Array(items));
                format!("d[@{c}] IN @{v}")
            }
            FilterOp::IsNull => format!("d[@{c}] == null"),
            FilterOp::IsNotNull => format!("d[@{c}] != null"),
        };
        parts.push(expr);
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" FILTER {}", parts.join(" AND "))
    }
}

fn sort_clause(sort: &[SortRule], binds: &mut Map<String, Json>) -> String {
    if sort.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = sort
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let key = format!("s{i}");
            binds.insert(key.clone(), Json::String(s.column.clone()));
            format!("d[@{key}] {}", if s.desc { "DESC" } else { "ASC" })
        })
        .collect();
    format!(" SORT {}", parts.join(", "))
}

// WHAT:  Union of keys across sampled documents, system fields first.
fn union_columns(docs: &[Json], is_edge: bool) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = Vec::new();
    let mut types: Vec<Option<&'static str>> = Vec::new();
    let mut push = |name: &str, value: Option<&Json>| {
        let idx = match names.iter().position(|n| n == name) {
            Some(i) => i,
            None => {
                names.push(name.to_string());
                types.push(None);
                names.len() - 1
            }
        };
        if let Some(v) = value {
            if types[idx].is_none() && !v.is_null() {
                types[idx] = Some(json_type_name(v));
            }
        }
    };
    let pinned: &[&str] = if is_edge { &SYSTEM_FIELDS } else { &SYSTEM_FIELDS[..3] };
    for f in pinned {
        push(f, None);
    }
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for (k, v) in obj {
                push(k, Some(v));
            }
        }
    }
    names
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, ty))| ColumnInfo {
            primary_key: name == "_key",
            data_type: ty.unwrap_or(if name == "_key" || name == "_id" || name == "_rev" { "string" } else { "null" }).to_string(),
            nullable: name != "_key",
            name,
            ordinal: u32::try_from(i + 1).unwrap_or(u32::MAX),
        })
        .collect()
}

// WHAT:  Aligns documents to the known columns, appending any keys the sample missed.
fn docs_to_result_set(columns: &[ColumnInfo], docs: &[Json], truncated: bool) -> ResultSet {
    let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut types: Vec<String> = columns.iter().map(|c| c.data_type.clone()).collect();
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for (k, v) in obj {
                if !names.iter().any(|n| n == k) {
                    names.push(k.clone());
                    types.push(json_type_name(v).to_string());
                }
            }
        }
    }
    let rows = docs
        .iter()
        .map(|doc| match doc.as_object() {
            Some(obj) => names.iter().map(|n| obj.get(n).map(json_to_value).unwrap_or(Value::Null)).collect(),
            None => {
                let mut row = vec![Value::Null; names.len()];
                if let Some(c) = row.first_mut() {
                    *c = json_to_value(doc);
                }
                row
            }
        })
        .collect();
    let columns = names.into_iter().zip(types).map(|(name, type_name)| ColumnMeta { name, type_name }).collect();
    ResultSet { columns, rows, truncated }
}

// WHAT:  A cursor result → grid: objects become a table, scalars a `value` column.
fn cursor_rows_to_result_set(items: &[Json], max_rows: usize) -> ResultSet {
    if !items.is_empty() && items.iter().all(Json::is_object) {
        let id = items.iter().any(|d| d.get("_key").is_some()).then_some("_key");
        return objects_to_result_set(items, id, max_rows);
    }
    let truncated = items.len() > max_rows;
    let type_name = items.iter().find(|v| !v.is_null()).map(json_type_name).unwrap_or("json").to_string();
    ResultSet {
        columns: vec![ColumnMeta { name: "value".into(), type_name }],
        rows: items.iter().take(max_rows).map(|v| vec![json_to_value(v)]).collect(),
        truncated,
    }
}

// WHAT:  True when the AQL text contains a data-modification keyword (whole word, any case).
fn is_write_aql(query: &str) -> bool {
    query
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .filter(|w| !w.is_empty())
        .any(|w| WRITE_KEYWORDS.iter().any(|k| w.eq_ignore_ascii_case(k)))
}

// WHAT:  `execute` input: raw AQL, or JSON `{"query": "...", "bindVars": {...}}`.
fn parse_execute_input(text: &str) -> AppResult<(String, Map<String, Json>)> {
    let t = text.trim();
    if t.starts_with('{') {
        let v: Json = serde_json::from_str(t).map_err(|e| AppError::invalid_input(format!("Invalid JSON query body: {e}")))?;
        let query = v
            .get("query")
            .and_then(Json::as_str)
            .ok_or_else(|| AppError::invalid_input("JSON body needs a \"query\" string."))?
            .to_string();
        let binds = v.get("bindVars").and_then(Json::as_object).cloned().unwrap_or_default();
        return Ok((query, binds));
    }
    if t.is_empty() {
        return Err(AppError::invalid_input("Empty AQL query."));
    }
    Ok((t.to_string(), Map::new()))
}

fn is_system_class(name: &str) -> bool {
    name.starts_with('_')
}

impl ArangoIntegration {
    fn db_path(&self, path: &str) -> String {
        format!("/_db/{}/{}", pct(&self.database), path.trim_start_matches('/'))
    }

    // WHAT:  Runs AQL and drains the cursor up to `max_rows`; returns (rows, more, extra).
    async fn aql(&self, query: &str, binds: Map<String, Json>, max_rows: usize) -> AppResult<(Vec<Json>, bool, Json)> {
        let batch = max_rows.clamp(1, CURSOR_BATCH);
        let body = json!({ "query": query, "bindVars": binds, "batchSize": batch });
        let mut resp: CursorResponse = self.http.post_json(&self.db_path("_api/cursor"), &body).await?;
        let extra = std::mem::take(&mut resp.extra);
        let mut rows = Vec::new();
        loop {
            rows.append(&mut resp.result);
            if !resp.has_more {
                return Ok((rows, false, extra));
            }
            let Some(id) = resp.id.clone() else {
                return Ok((rows, false, extra));
            };
            if rows.len() >= max_rows {
                let path = self.db_path(&format!("_api/cursor/{}", pct(&id)));
                let _ = self.http.send(self.http.request(Method::DELETE, &path)).await;
                rows.truncate(max_rows);
                return Ok((rows, true, extra));
            }
            let path = self.db_path(&format!("_api/cursor/{}", pct(&id)));
            let next = self.http.send(self.http.request(Method::PUT, &path)).await?;
            resp = next.json::<CursorResponse>().await.map_err(|e| AppError::driver(format!("Malformed cursor response: {e}")))?;
        }
    }

    async fn collection_type(&self, name: &str) -> AppResult<bool> {
        let v: Json = self.http.get_json(&self.db_path(&format!("_api/collection/{}", pct(name)))).await?;
        Ok(v.get("type").and_then(Json::as_i64) == Some(3))
    }

    async fn graphs(&self) -> AppResult<Vec<Json>> {
        let v: Json = self.http.get_json(&self.db_path("_api/gharial")).await?;
        Ok(v.get("graphs").and_then(Json::as_array).cloned().unwrap_or_default())
    }

    fn graph_columns() -> Vec<ColumnInfo> {
        ["name", "edgeDefinitions", "orphanCollections"]
            .iter()
            .enumerate()
            .map(|(i, n)| ColumnInfo {
                name: (*n).to_string(),
                data_type: if i == 0 { "string" } else { "array" }.into(),
                nullable: i != 0,
                primary_key: i == 0,
                ordinal: i as u32 + 1,
            })
            .collect()
    }

    fn graph_row(g: &Json) -> Vec<Value> {
        let name = g.get("_key").or_else(|| g.get("name")).map(json_to_value).unwrap_or(Value::Null);
        let edges = g.get("edgeDefinitions").map(json_to_value).unwrap_or(Value::Null);
        let orphans = g.get("orphanCollections").map(json_to_value).unwrap_or(Value::Null);
        vec![name, edges, orphans]
    }

    fn is_graph_table(table: &TableRef) -> bool {
        table.schema.as_deref() == Some(GRAPH_SCHEMA)
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  The HTTP API's catalog endpoints (`_api/collection`, `_api/index`,
//        `_api/gharial`, `_api/view`, `_api/aqlfunction`, `_api/foxx`,
//        `_api/user`, `_api/query/current`, `_admin/cluster/health`,
//        `_admin/statistics`) mapped by pure functions into summaries, details
//        and stat groups so offline tests can feed literal JSON.
// WHY:   Actions run through `execute`, which only speaks AQL. AQL can sample,
//        count and remove documents but cannot drop collections, indexes,
//        views or kill queries, so only the AQL-expressible actions exist.
// ---------------------------------------------------------------------------

const LIST_CAP: usize = 2_000;
const COUNTED_COLLECTIONS: usize = 300;

fn jget<'a>(row: &'a Json, key: &str) -> Option<&'a Json> {
    row.get(key).filter(|v| !v.is_null())
}

fn scalar_text(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        Json::Array(items) => items.iter().map(scalar_text).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

fn jstr(row: &Json, key: &str) -> Option<String> {
    jget(row, key).map(scalar_text).filter(|s| !s.is_empty())
}

fn jnum(row: &Json, key: &str) -> Option<f64> {
    jget(row, key).and_then(Json::as_f64)
}

fn jflag(row: &Json, key: &str) -> bool {
    jget(row, key).and_then(Json::as_bool).unwrap_or(false)
}

fn jnames(row: &Json, key: &str) -> Vec<String> {
    jget(row, key).and_then(Json::as_array).map(|a| a.iter().map(scalar_text).collect()).unwrap_or_default()
}

fn preview(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        format!("{}…", flat.chars().take(max).collect::<String>())
    }
}

fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

// WHAT:  AQL identifier quoting (backticks; a backtick cannot occur in a name).
fn aql_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', ""))
}

fn finish(mut out: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    out.truncate(LIST_CAP);
    out
}

fn props_of(row: &Json, skip: &[&str]) -> Vec<ObjectProperty> {
    let Some(obj) = row.as_object() else { return Vec::new() };
    let mut keys: Vec<&String> = obj.keys().filter(|k| !skip.contains(&k.as_str())).collect();
    keys.sort();
    keys.into_iter()
        .filter_map(|k| {
            let v = obj.get(k)?;
            if v.is_null() {
                return None;
            }
            let text = match v {
                Json::Object(_) => v.to_string(),
                other => scalar_text(other),
            };
            (!text.is_empty()).then(|| ObjectProperty { name: k.clone(), value: preview(&text, 400) })
        })
        .collect()
}

fn bytes_text(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{v:.0} {}", UNITS[i])
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

fn stat_bytes(label: &str, bytes: f64) -> Stat {
    Stat { label: label.to_string(), value: bytes_text(bytes), unit: None, hint: None, numeric: Some(bytes) }
}

fn duration_text(secs: f64) -> String {
    if secs < 60.0 {
        format!("{secs:.0} s")
    } else if secs < 3600.0 {
        format!("{}m {}s", (secs / 60.0).floor(), (secs % 60.0).floor())
    } else if secs < 86_400.0 {
        format!("{}h {}m", (secs / 3600.0).floor(), ((secs % 3600.0) / 60.0).floor())
    } else {
        format!("{}d {}h", (secs / 86_400.0).floor(), ((secs % 86_400.0) / 3600.0).floor())
    }
}

fn collection_status(code: i64) -> &'static str {
    match code {
        1 => "new",
        2 => "unloaded",
        3 => "loaded",
        4 => "unloading",
        5 => "deleted",
        6 => "loading",
        _ => "unknown",
    }
}

fn collection_type(code: i64) -> &'static str {
    if code == 3 { "edge" } else { "document" }
}

// ---- collections -----------------------------------------------------------------

fn collection_summaries(items: &[Json], kind: ObjectKind, db: &str, counts: &BTreeMap<String, i64>) -> Vec<ObjectSummary> {
    let want_edge = kind == ObjectKind::EdgeCollection;
    finish(
        items
            .iter()
            .filter_map(|c| {
                let name = jstr(c, "name")?;
                let is_edge = jget(c, "type").and_then(Json::as_i64) == Some(3);
                if is_edge != want_edge || jflag(c, "isSystem") || name.starts_with('_') {
                    return None;
                }
                let mut s = ObjectSummary::new(kind, name.clone(), Some(db.to_string()));
                if let Some(n) = counts.get(&name) {
                    s = s.with_detail(format!("{} docs", format_number(*n as f64)));
                }
                s.badge = jget(c, "status").and_then(Json::as_i64).map(|st| collection_status(st).to_string());
                Some(s)
            })
            .collect(),
    )
}

fn collection_detail(reference: &ObjectRef, properties: &Json, figures: Option<&Json>, columns: Vec<ColumnInfo>, indexes: Vec<ObjectSummary>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(properties), CodeLanguage::Json);
    if let Some(f) = figures {
        if let Some(n) = jnum(f, "count") {
            d = d.property("documents", format_number(n));
        }
        if let Some(fig) = f.get("figures") {
            if let Some(b) = jnum(fig, "documentsSize") {
                d = d.property("documents size", bytes_text(b));
            }
            if let Some(b) = fig.get("indexes").and_then(|i| jnum(i, "size")) {
                d = d.property("indexes size", bytes_text(b));
            }
            if let Some(b) = jnum(fig, "cacheSize").filter(|b| *b > 0.0) {
                d = d.property("cache size", bytes_text(b));
            }
        }
    }
    if let Some(t) = jget(properties, "type").and_then(Json::as_i64) {
        d = d.property("type", collection_type(t));
    }
    if let Some(st) = jget(properties, "status").and_then(Json::as_i64) {
        d = d.property("status", collection_status(st));
    }
    for key in ["waitForSync", "cacheEnabled", "numberOfShards", "shardKeys", "replicationFactor", "writeConcern", "keyOptions", "schema", "computedValues", "globallyUniqueId"] {
        if let Some(v) = jget(properties, key) {
            let text = match v {
                Json::Object(_) => v.to_string(),
                other => scalar_text(other),
            };
            d = d.property(key, preview(&text, 400));
        }
    }
    d.columns = columns;
    d.children = indexes;
    let ident = aql_ident(&reference.name);
    d.action(ObjectAction::new("sample", "Sample 20", format!("FOR d IN {ident} LIMIT 20 RETURN d")))
        .action(ObjectAction::new("count", "Count", format!("RETURN LENGTH({ident})")))
        .action(ObjectAction::destructive("remove-all", "Remove all documents", format!("FOR d IN {ident} REMOVE d IN {ident}")))
}

// ---- indexes -----------------------------------------------------------------------

fn index_name(idx: &Json) -> Option<String> {
    jstr(idx, "name").or_else(|| jstr(idx, "id").map(|id| id.rsplit('/').next().unwrap_or(&id).to_string()))
}

fn index_summaries(indexes: &[Json], collection: &str) -> Vec<ObjectSummary> {
    finish(
        indexes
            .iter()
            .filter_map(|idx| {
                let name = index_name(idx)?;
                let mut parts = vec![jnames(idx, "fields").join(", ")];
                if jflag(idx, "unique") {
                    parts.push("unique".into());
                }
                if jflag(idx, "sparse") {
                    parts.push("sparse".into());
                }
                if let Some(ttl) = jnum(idx, "expireAfter") {
                    parts.push(format!("expires after {ttl:.0}s"));
                }
                let detail = parts.into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>().join(" · ");
                let mut s = ObjectSummary::new(ObjectKind::Index, name, Some(collection.to_string()));
                if !detail.is_empty() {
                    s = s.with_detail(detail);
                }
                s.badge = jstr(idx, "type");
                Some(s)
            })
            .collect(),
    )
}

fn index_detail(reference: &ObjectRef, idx: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(idx), CodeLanguage::Json);
    d.properties = props_of(idx, &["id", "name", "fields"]);
    let fields = jnames(idx, "fields");
    if !fields.is_empty() {
        d.rows = Some(ResultSet {
            columns: vec![ColumnMeta { name: "field".into(), type_name: "string".into() }],
            rows: fields.into_iter().map(|f| vec![Value::Text(f)]).collect(),
            truncated: false,
        });
    }
    d
}

// ---- graphs ------------------------------------------------------------------------

fn graph_name(g: &Json) -> Option<String> {
    jstr(g, "_key").or_else(|| jstr(g, "name"))
}

fn graph_badge(g: &Json) -> &'static str {
    if jflag(g, "isSatellite") {
        "satellite"
    } else if jflag(g, "isSmart") {
        "smart"
    } else {
        "general"
    }
}

fn graph_summaries(graphs: &[Json]) -> Vec<ObjectSummary> {
    finish(
        graphs
            .iter()
            .filter_map(|g| {
                let name = graph_name(g)?;
                let edges = g.get("edgeDefinitions").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
                let orphans = jnames(g, "orphanCollections").len();
                let mut detail = format!("{edges} edge definition(s)");
                if orphans > 0 {
                    detail.push_str(&format!(" · {orphans} orphan(s)"));
                }
                Some(ObjectSummary::new(ObjectKind::Graph, name, None).with_detail(detail).with_badge(graph_badge(g)))
            })
            .collect(),
    )
}

fn graph_detail(reference: &ObjectRef, g: &Json, db: &str) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(g), CodeLanguage::Json);
    d = d.property("kind", graph_badge(g));
    for key in ["numberOfShards", "replicationFactor", "writeConcern", "smartGraphAttribute", "isDisjoint"] {
        if let Some(v) = jstr(g, key) {
            d = d.property(key, v);
        }
    }
    let defs = g.get("edgeDefinitions").and_then(Json::as_array).cloned().unwrap_or_default();
    let mut edge_collections = Vec::new();
    let mut vertex_collections: Vec<String> = jnames(g, "orphanCollections");
    let rows: Vec<Vec<Value>> = defs
        .iter()
        .map(|def| {
            let collection = jstr(def, "collection").unwrap_or_default();
            let from = jnames(def, "from");
            let to = jnames(def, "to");
            edge_collections.push(collection.clone());
            vertex_collections.extend(from.iter().cloned());
            vertex_collections.extend(to.iter().cloned());
            vec![Value::Text(collection), Value::Text(from.join(", ")), Value::Text(to.join(", "))]
        })
        .collect();
    d.rows = Some(ResultSet {
        columns: ["collection", "from", "to"].iter().map(|n| ColumnMeta { name: (*n).into(), type_name: "string".into() }).collect(),
        rows,
        truncated: false,
    });
    vertex_collections.sort();
    vertex_collections.dedup();
    edge_collections.sort();
    edge_collections.dedup();
    d.children = edge_collections
        .into_iter()
        .map(|c| ObjectSummary::new(ObjectKind::EdgeCollection, c, Some(db.to_string())).with_badge("edge"))
        .chain(vertex_collections.into_iter().map(|c| ObjectSummary::new(ObjectKind::Collection, c, Some(db.to_string())).with_badge("vertex")))
        .collect();
    d
}

// ---- views / functions / services ----------------------------------------------------

fn view_summaries(views: &[Json]) -> Vec<ObjectSummary> {
    finish(
        views
            .iter()
            .filter_map(|v| {
                let name = jstr(v, "name")?;
                let mut s = ObjectSummary::new(ObjectKind::View, name, None);
                s.badge = jstr(v, "type");
                Some(s)
            })
            .collect(),
    )
}

fn view_detail(reference: &ObjectRef, props: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(props), CodeLanguage::Json);
    if let Some(t) = jstr(props, "type") {
        d = d.property("type", t);
    }
    if let Some(links) = props.get("links").and_then(Json::as_object) {
        // The map preserves the server's insertion order; sort so the sheet reads
        // the same way on every refresh.
        let mut names: Vec<&str> = links.keys().map(String::as_str).collect();
        names.sort_unstable();
        d = d.property("links", names.join(", "));
    }
    if let Some(indexes) = props.get("indexes").and_then(Json::as_array) {
        let names: Vec<String> = indexes.iter().map(|i| format!("{}/{}", jstr(i, "collection").unwrap_or_default(), jstr(i, "index").unwrap_or_default())).collect();
        d = d.property("indexes", names.join(", "));
    }
    for key in ["primarySort", "storedValues", "consolidationIntervalMsec", "commitIntervalMsec", "cleanupIntervalStep"] {
        if let Some(v) = jget(props, key) {
            let text = match v {
                Json::Array(_) | Json::Object(_) => v.to_string(),
                other => scalar_text(other),
            };
            d = d.property(key, preview(&text, 400));
        }
    }
    d.action(ObjectAction::new("sample", "Sample 20", format!("FOR d IN {} LIMIT 20 RETURN d", aql_ident(&reference.name))))
}

fn function_summaries(functions: &[Json], db: &str) -> Vec<ObjectSummary> {
    finish(
        functions
            .iter()
            .filter_map(|f| {
                let name = jstr(f, "name")?;
                let mut s = ObjectSummary::new(ObjectKind::Function, name, Some(db.to_string()));
                if let Some(code) = jstr(f, "code") {
                    s = s.with_detail(preview(&code, 100));
                }
                if jflag(f, "isDeterministic") {
                    s = s.with_badge("deterministic");
                }
                Some(s)
            })
            .collect(),
    )
}

fn function_detail(reference: &ObjectRef, f: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(code) = jstr(f, "code") {
        d = d.definition(code, CodeLanguage::Text);
    }
    d.property("deterministic", jflag(f, "isDeterministic").to_string())
}

fn service_summaries(services: &[Json]) -> Vec<ObjectSummary> {
    finish(
        services
            .iter()
            .filter_map(|s| {
                let mount = jstr(s, "mount")?;
                let detail = [jstr(s, "name"), jstr(s, "version")].into_iter().flatten().collect::<Vec<_>>().join(" ");
                let mut out = ObjectSummary::new(ObjectKind::Service, mount, None);
                if !detail.is_empty() {
                    out = out.with_detail(detail);
                }
                if jflag(s, "development") {
                    out = out.with_badge("development");
                } else if jflag(s, "legacy") {
                    out = out.with_badge("legacy");
                }
                Some(out)
            })
            .collect(),
    )
}

fn service_detail(reference: &ObjectRef, s: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(s), CodeLanguage::Json);
    d.properties = props_of(s, &["manifest", "options", "mount"]);
    if let Some(m) = s.get("manifest") {
        for key in ["name", "version", "description", "author", "license", "main"] {
            if let Some(v) = jstr(m, key) {
                d = d.property(key, v);
            }
        }
    }
    d
}

// ---- users / queries / nodes ------------------------------------------------------------

fn user_summaries(users: &[Json]) -> Vec<ObjectSummary> {
    finish(
        users
            .iter()
            .filter_map(|u| {
                let name = jstr(u, "user")?;
                let active = jget(u, "active").and_then(Json::as_bool).unwrap_or(true);
                Some(ObjectSummary::new(ObjectKind::User, name, None).with_badge(if active { "active" } else { "inactive" }))
            })
            .collect(),
    )
}

fn user_detail(reference: &ObjectRef, user: Option<&Json>, databases: Option<&Json>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(u) = user {
        d = d.property("active", jget(u, "active").and_then(Json::as_bool).unwrap_or(true).to_string());
        if let Some(extra) = u.get("extra").filter(|e| e.as_object().is_some_and(|o| !o.is_empty())) {
            d = d.property("extra", preview(&extra.to_string(), 400));
        }
    }
    if let Some(map) = databases.and_then(|v| v.get("result")).and_then(Json::as_object) {
        let mut rows: Vec<(String, String)> = map.iter().map(|(db, access)| (db.clone(), scalar_text(access))).collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let rows: Vec<Vec<Value>> = rows.into_iter().map(|(db, access)| vec![Value::Text(db), Value::Text(access)]).collect();
        d.rows = Some(ResultSet {
            columns: vec![ColumnMeta { name: "database".into(), type_name: "string".into() }, ColumnMeta { name: "access".into(), type_name: "string".into() }],
            rows,
            truncated: false,
        });
    }
    d
}

fn query_id(q: &Json) -> Option<String> {
    jget(q, "id").map(scalar_text)
}

fn query_summaries(kind: ObjectKind, queries: &[Json]) -> Vec<ObjectSummary> {
    finish(
        queries
            .iter()
            .filter_map(|q| {
                let id = query_id(q)?;
                let mut parts = Vec::new();
                if let Some(u) = jstr(q, "user") {
                    parts.push(u);
                }
                if let Some(t) = jnum(q, "runTime") {
                    parts.push(format!("{t:.2} s"));
                }
                if let Some(text) = jstr(q, "query") {
                    parts.push(preview(&text, 60));
                }
                let mut s = ObjectSummary::new(kind, id, None);
                if !parts.is_empty() {
                    s = s.with_detail(parts.join(" · "));
                }
                s.badge = jstr(q, "state");
                Some(s)
            })
            .collect(),
    )
}

fn query_detail(reference: &ObjectRef, q: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(text) = jstr(q, "query") {
        d = d.definition(text, CodeLanguage::Text);
    }
    d.properties = props_of(q, &["id", "query"]);
    if let Some(mem) = jnum(q, "peakMemoryUsage") {
        d = d.property("peak memory", bytes_text(mem));
    }
    d
}

// WHAT:  `_admin/cluster/health` → one node per Health entry (coordinators,
//        DB servers, agents); `single_node` covers a single-server deployment.
fn cluster_nodes(health: &Json) -> Vec<ObjectSummary> {
    let Some(map) = health.get("Health").and_then(Json::as_object) else { return Vec::new() };
    finish(
        map.iter()
            .map(|(id, entry)| {
                let name = jstr(entry, "ShortName").unwrap_or_else(|| id.clone());
                let mut parts = Vec::new();
                if let Some(s) = jstr(entry, "Status") {
                    parts.push(s);
                }
                if let Some(e) = jstr(entry, "Endpoint") {
                    parts.push(e);
                }
                if let Some(v) = jstr(entry, "Version") {
                    parts.push(format!("v{v}"));
                }
                if jflag(entry, "Leading") {
                    parts.push("leading".into());
                }
                let mut s = ObjectSummary::new(ObjectKind::Node, name, None);
                if !parts.is_empty() {
                    s = s.with_detail(parts.join(" · "));
                }
                s.badge = jstr(entry, "Role").map(|r| r.to_lowercase());
                s
            })
            .collect(),
    )
}

fn single_node(role: Option<&str>, version: &Json) -> ObjectSummary {
    let mut parts = Vec::new();
    if let Some(v) = jstr(version, "version") {
        parts.push(format!("v{v}"));
    }
    if let Some(l) = jstr(version, "license") {
        parts.push(l);
    }
    let mut s = ObjectSummary::new(ObjectKind::Node, "single", None).with_badge(role.map(|r| r.to_lowercase()).unwrap_or_else(|| "single".into()));
    if !parts.is_empty() {
        s = s.with_detail(parts.join(" · "));
    }
    s
}

fn node_detail(reference: &ObjectRef, entry: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(entry), CodeLanguage::Json);
    d.properties = props_of(entry, &[]);
    d
}

// ---- stats ------------------------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CatalogCounts {
    databases: usize,
    collections: usize,
    edge_collections: usize,
    graphs: usize,
    views: usize,
    running_queries: usize,
}

// WHAT:  `client.bytesSent` and friends are distributions `{sum, count, counts}`.
fn stat_sum(section: &Json, key: &str) -> Option<f64> {
    match section.get(key)? {
        Json::Object(o) => o.get("sum").and_then(Json::as_f64),
        other => other.as_f64(),
    }
}

fn stat_groups(version: &Json, engine: Option<&Json>, role: Option<&str>, mode: Option<&str>, statistics: Option<&Json>, catalog: &CatalogCounts) -> Vec<StatGroup> {
    let mut server = Vec::new();
    if let Some(v) = jstr(version, "version") {
        server.push(Stat::text("Version", format!("ArangoDB {v}")));
    }
    if let Some(l) = jstr(version, "license") {
        server.push(Stat::text("License", l));
    }
    if let Some(e) = engine.and_then(|e| jstr(e, "name")) {
        server.push(Stat::text("Storage engine", e));
    }
    if let Some(r) = role {
        server.push(Stat::text("Role", r.to_lowercase()));
    }
    if let Some(m) = mode {
        server.push(Stat::text("Mode", m));
    }
    let srv = statistics.and_then(|s| s.get("server"));
    if let Some(up) = srv.and_then(|s| jnum(s, "uptime")) {
        server.push(Stat::text("Uptime", duration_text(up)));
    }
    if let Some(mem) = srv.and_then(|s| jnum(s, "physicalMemory")) {
        server.push(stat_bytes("Physical memory", mem));
    }
    let mut groups = vec![StatGroup { title: "Server".into(), stats: server }];

    if let Some(stats) = statistics {
        let mut connections = Vec::new();
        if let Some(c) = stats.get("client").and_then(|c| jnum(c, "httpConnections")) {
            connections.push(Stat::number("HTTP connections", c, None));
        }
        if let Some(t) = stats.get("system").and_then(|s| jnum(s, "numberOfThreads")) {
            connections.push(Stat::number("Threads", t, None));
        }
        if !connections.is_empty() {
            groups.push(StatGroup { title: "Connections".into(), stats: connections });
        }
        let mut memory = Vec::new();
        if let Some(sys) = stats.get("system") {
            if let Some(r) = jnum(sys, "residentSize") {
                memory.push(stat_bytes("Resident", r));
            }
            if let Some(v) = jnum(sys, "virtualSize") {
                memory.push(stat_bytes("Virtual", v));
            }
            if let Some(p) = jnum(sys, "residentSizePercent") {
                memory.push(Stat::number("Resident of physical", (p * 1000.0).round() / 10.0, Some("%")));
            }
        }
        if !memory.is_empty() {
            groups.push(StatGroup { title: "Memory".into(), stats: memory });
        }
        let mut throughput = Vec::new();
        if let Some(http) = stats.get("http") {
            for (key, label) in [
                ("requestsTotal", "Requests"),
                ("requestsAsync", "Async requests"),
                ("requestsGet", "GET"),
                ("requestsPost", "POST"),
                ("requestsPut", "PUT"),
                ("requestsPatch", "PATCH"),
                ("requestsDelete", "DELETE"),
            ] {
                if let Some(n) = jnum(http, key) {
                    throughput.push(Stat::number(label, n, None));
                }
            }
        }
        if let Some(client) = stats.get("client") {
            if let Some(b) = stat_sum(client, "bytesSent") {
                throughput.push(stat_bytes("Bytes sent", b));
            }
            if let Some(b) = stat_sum(client, "bytesReceived") {
                throughput.push(stat_bytes("Bytes received", b));
            }
        }
        if !throughput.is_empty() {
            groups.push(StatGroup { title: "Throughput".into(), stats: throughput });
        }
        let mut tx = Vec::new();
        if let Some(t) = srv.and_then(|s| s.get("transactions")) {
            for (key, label) in [("started", "Started"), ("committed", "Committed"), ("aborted", "Aborted"), ("intermediateCommits", "Intermediate commits")] {
                if let Some(n) = jnum(t, key) {
                    tx.push(Stat::number(label, n, None));
                }
            }
        }
        if !tx.is_empty() {
            groups.push(StatGroup { title: "Transactions".into(), stats: tx });
        }
    }
    groups.push(StatGroup {
        title: "Catalog".into(),
        stats: vec![
            Stat::number("Databases", catalog.databases as f64, None),
            Stat::number("Collections", catalog.collections as f64, None),
            Stat::number("Edge collections", catalog.edge_collections as f64, None),
            Stat::number("Graphs", catalog.graphs as f64, None),
            Stat::number("Views", catalog.views as f64, None),
            Stat::number("Running queries", catalog.running_queries as f64, None),
        ],
    });
    groups
}

fn result_array(v: Json) -> Vec<Json> {
    match v {
        Json::Array(a) => a,
        Json::Object(mut o) => match o.remove("result").or_else(|| o.remove("graphs")) {
            Some(Json::Array(a)) => a,
            Some(Json::Object(inner)) => vec![Json::Object(inner)],
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

impl ArangoIntegration {
    fn path_in(&self, db: &str, path: &str) -> String {
        format!("/_db/{}/{}", pct(db), path.trim_start_matches('/'))
    }

    async fn list_in(&self, db: &str, path: &str) -> AppResult<Vec<Json>> {
        let v: Json = self.http.get_json(&self.path_in(db, path)).await?;
        Ok(result_array(v))
    }

    async fn collections_in(&self, db: &str) -> AppResult<Vec<Json>> {
        self.list_in(db, "_api/collection?excludeSystem=true").await
    }

    async fn collection_counts(&self, db: &str, items: &[Json], kind: ObjectKind) -> BTreeMap<String, i64> {
        let want_edge = kind == ObjectKind::EdgeCollection;
        let mut out = BTreeMap::new();
        let names = items
            .iter()
            .filter(|c| (jget(c, "type").and_then(Json::as_i64) == Some(3)) == want_edge)
            .filter_map(|c| jstr(c, "name"))
            .take(COUNTED_COLLECTIONS);
        for name in names {
            if let Ok(v) = self.http.get_json::<Json>(&self.path_in(db, &format!("_api/collection/{}/count", pct(&name)))).await {
                if let Some(n) = v.get("count").and_then(Json::as_i64) {
                    out.insert(name, n);
                }
            }
        }
        out
    }

    async fn indexes_of(&self, db: &str, collection: &str) -> AppResult<Vec<Json>> {
        let v: Json = self.http.get_json(&self.path_in(db, &format!("_api/index?collection={}", pct(collection)))).await?;
        Ok(v.get("indexes").and_then(Json::as_array).cloned().unwrap_or_default())
    }

    async fn is_collection(&self, name: &str) -> bool {
        self.http.get_json::<Json>(&self.db_path(&format!("_api/collection/{}", pct(name)))).await.is_ok()
    }

    async fn running_queries(&self) -> AppResult<Vec<Json>> {
        match self.list_in(&self.database, "_api/query/current?all=true").await {
            Ok(v) => Ok(v),
            Err(_) => self.list_in(&self.database, "_api/query/current").await,
        }
    }

    async fn cluster_health(&self) -> Option<Json> {
        self.http.get_json::<Json>("/_admin/cluster/health").await.ok().filter(|h| h.get("Health").is_some())
    }

    async fn server_role(&self) -> Option<String> {
        self.http.get_json::<Json>("/_admin/server/role").await.ok().and_then(|v| jstr(&v, "role"))
    }

    async fn explorer_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let session_db = self.database.as_str();
        let db = parent.map(str::trim).filter(|p| !p.is_empty()).unwrap_or(session_db);
        match kind {
            ObjectKind::Database => {
                let names = self.databases().await?;
                Ok(finish(
                    names
                        .into_iter()
                        .map(|n| {
                            let mut s = ObjectSummary::new(ObjectKind::Database, n.clone(), None);
                            if n == session_db {
                                s = s.with_badge("current");
                            } else if n == DEFAULT_DATABASE {
                                s = s.with_badge("system");
                            }
                            s
                        })
                        .collect(),
                ))
            }
            ObjectKind::Collection | ObjectKind::EdgeCollection => {
                let items = self.collections_in(db).await?;
                let counts = self.collection_counts(db, &items, kind).await;
                Ok(collection_summaries(&items, kind, db, &counts))
            }
            ObjectKind::Graph => Ok(graph_summaries(&self.graphs().await?)),
            ObjectKind::View => Ok(view_summaries(&self.list_in(session_db, "_api/view").await?)),
            ObjectKind::Index => {
                // The parent is either one collection (from a collection's detail)
                // or a database (from the sidebar).
                let (db, collections): (&str, Vec<String>) = match parent.filter(|p| *p != session_db) {
                    Some(p) if self.is_collection(p).await => (session_db, vec![p.to_string()]),
                    _ => (db, self.collections_in(db).await?.iter().filter_map(|c| jstr(c, "name")).take(COUNTED_COLLECTIONS).collect()),
                };
                let mut out = Vec::new();
                for c in collections {
                    if let Ok(idx) = self.indexes_of(db, &c).await {
                        out.extend(index_summaries(&idx, &c));
                    }
                }
                Ok(finish(out))
            }
            ObjectKind::Function => Ok(function_summaries(&self.list_in(db, "_api/aqlfunction").await?, db)),
            ObjectKind::Service => Ok(service_summaries(&self.list_in(session_db, "_api/foxx").await?)),
            ObjectKind::User => Ok(user_summaries(&self.list_in(session_db, "_api/user").await?)),
            ObjectKind::Session => Ok(query_summaries(kind, &self.running_queries().await?)),
            ObjectKind::SlowQuery => Ok(query_summaries(kind, &self.list_in(session_db, "_api/query/slow").await?)),
            ObjectKind::Node => match self.cluster_health().await {
                Some(health) => Ok(cluster_nodes(&health)),
                None => {
                    let version: Json = self.http.get_json(&self.db_path("_api/version?details=true")).await?;
                    Ok(vec![single_node(self.server_role().await.as_deref(), &version)])
                }
            },
            _ => Ok(Vec::new()),
        }
    }

    async fn explorer_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let session_db = self.database.as_str();
        let missing = || AppError::not_found(format!("No {:?} named `{name}`.", reference.kind));
        match reference.kind {
            ObjectKind::Database => {
                let current: Json = self.http.get_json(&self.path_in(name, "_api/database/current")).await?;
                let info = current.get("result").cloned().unwrap_or(current);
                let mut d = ObjectDetail::empty(reference).definition(pretty(&info), CodeLanguage::Json);
                d.properties = props_of(&info, &["name"]);
                let items = self.collections_in(name).await.unwrap_or_default();
                let empty = BTreeMap::new();
                let mut children = collection_summaries(&items, ObjectKind::Collection, name, &empty);
                children.extend(collection_summaries(&items, ObjectKind::EdgeCollection, name, &empty));
                d = d.property("collections", children.len().to_string());
                if let Ok(graphs) = self.list_in(name, "_api/gharial").await {
                    d = d.property("graphs", graphs.len().to_string());
                }
                d.children = children;
                Ok(d)
            }
            ObjectKind::Collection | ObjectKind::EdgeCollection => {
                let db = reference.parent.as_deref().filter(|p| !p.is_empty()).unwrap_or(session_db);
                let properties: Json = self.http.get_json(&self.path_in(db, &format!("_api/collection/{}/properties", pct(name)))).await?;
                let figures = self.http.get_json::<Json>(&self.path_in(db, &format!("_api/collection/{}/figures", pct(name)))).await.ok();
                let columns = if db == session_db { self.columns(&TableRef { schema: Some(db.to_string()), name: name.to_string() }).await.unwrap_or_default() } else { Vec::new() };
                let indexes = self.indexes_of(db, name).await.map(|idx| index_summaries(&idx, name)).unwrap_or_default();
                Ok(collection_detail(reference, &properties, figures.as_ref(), columns, indexes))
            }
            ObjectKind::Graph => {
                let graphs = self.graphs().await?;
                let g = graphs.iter().find(|g| graph_name(g).as_deref() == Some(name)).ok_or_else(missing)?;
                Ok(graph_detail(reference, g, session_db))
            }
            ObjectKind::View => {
                let props: Json = self.http.get_json(&self.db_path(&format!("_api/view/{}/properties", pct(name)))).await?;
                Ok(view_detail(reference, &props))
            }
            ObjectKind::Index => {
                let collection = reference.parent.as_deref().filter(|p| !p.is_empty()).ok_or_else(|| AppError::invalid_input("An index needs its collection as parent."))?;
                let indexes = self.indexes_of(session_db, collection).await?;
                let idx = indexes.iter().find(|i| index_name(i).as_deref() == Some(name)).ok_or_else(missing)?;
                Ok(index_detail(reference, idx))
            }
            ObjectKind::Function => {
                let db = reference.parent.as_deref().filter(|p| !p.is_empty()).unwrap_or(session_db);
                let functions = self.list_in(db, "_api/aqlfunction").await?;
                let f = functions.iter().find(|f| jstr(f, "name").as_deref() == Some(name)).ok_or_else(missing)?;
                Ok(function_detail(reference, f))
            }
            ObjectKind::Service => {
                let s: Json = self.http.get_json(&self.db_path(&format!("_api/foxx/service?mount={}", pct(name)))).await?;
                Ok(service_detail(reference, &s))
            }
            ObjectKind::User => {
                let user = self.http.get_json::<Json>(&format!("/_api/user/{}", pct(name))).await.ok();
                let databases = self.http.get_json::<Json>(&format!("/_api/user/{}/database", pct(name))).await.ok();
                Ok(user_detail(reference, user.as_ref(), databases.as_ref()))
            }
            ObjectKind::Session | ObjectKind::SlowQuery => {
                let queries = if reference.kind == ObjectKind::Session { self.running_queries().await? } else { self.list_in(session_db, "_api/query/slow").await? };
                let q = queries.iter().find(|q| query_id(q).as_deref() == Some(name)).ok_or_else(missing)?;
                Ok(query_detail(reference, q))
            }
            ObjectKind::Node => {
                if let Some(health) = self.cluster_health().await {
                    let entry = health
                        .get("Health")
                        .and_then(Json::as_object)
                        .and_then(|m| m.iter().find(|(id, e)| jstr(e, "ShortName").as_deref() == Some(name) || id.as_str() == name).map(|(_, e)| e))
                        .ok_or_else(missing)?;
                    return Ok(node_detail(reference, entry));
                }
                let version: Json = self.http.get_json(&self.db_path("_api/version?details=true")).await?;
                let mut d = node_detail(reference, &version);
                if let Some(role) = self.server_role().await {
                    d = d.property("role", role.to_lowercase());
                }
                Ok(d)
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn explorer_stats(&self) -> AppResult<ServerStats> {
        let version: Json = self.http.get_json(&self.db_path("_api/version?details=true")).await?;
        let engine = self.http.get_json::<Json>(&self.db_path("_api/engine")).await.ok();
        let role = self.server_role().await;
        let mode = self.http.get_json::<Json>("/_admin/server/mode").await.ok().and_then(|v| jstr(&v, "mode"));
        let statistics = self.http.get_json::<Json>(&self.db_path("_admin/statistics")).await.ok();
        let collections = self.collections_in(&self.database).await.unwrap_or_default();
        let edge = collections.iter().filter(|c| jget(c, "type").and_then(Json::as_i64) == Some(3)).count();
        let catalog = CatalogCounts {
            databases: self.databases().await.map(|d| d.len()).unwrap_or(1),
            collections: collections.len() - edge,
            edge_collections: edge,
            graphs: self.graphs().await.map(|g| g.len()).unwrap_or(0),
            views: self.list_in(&self.database, "_api/view").await.map(|v| v.len()).unwrap_or(0),
            running_queries: self.running_queries().await.map(|q| q.len()).unwrap_or(0),
        };
        Ok(ServerStats::now(stat_groups(&version, engine.as_ref(), role.as_deref(), mode.as_deref(), statistics.as_ref(), &catalog)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, views: true, exact_estimate: true, ..Capabilities::DOCUMENT },
        object_kinds: vec![
            K::Database,
            K::Collection,
            K::EdgeCollection,
            K::Graph,
            K::View,
            K::Index,
            K::Function,
            K::Service,
            K::User,
            K::Session,
            K::SlowQuery,
            K::Node,
        ],
        tools: vec![T::Stats, T::GraphView],
    }
}

#[async_trait]
impl Integration for ArangoIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _: Json = self.http.get_json(&self.db_path("_api/version")).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v: Json = self.http.get_json(&self.db_path("_api/version")).await?;
        Ok(v.get("version").and_then(Json::as_str).map(|s| format!("ArangoDB {s}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let v: Json = match self.http.get_json("/_api/database/user").await {
            Ok(v) => v,
            Err(_) => return Ok(vec![self.database.clone()]),
        };
        let mut names: Vec<String> = v
            .get("result")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        if names.is_empty() {
            names.push(self.database.clone());
        }
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let v: Json = self.http.get_json(&self.db_path("_api/collection?excludeSystem=true")).await?;
        let mut tables = Vec::new();
        for c in v.get("result").and_then(Json::as_array).into_iter().flatten() {
            let Some(name) = c.get("name").and_then(Json::as_str) else { continue };
            if is_system_class(name) || c.get("isSystem").and_then(Json::as_bool).unwrap_or(false) {
                continue;
            }
            let row_estimate = self
                .http
                .get_json::<Json>(&self.db_path(&format!("_api/collection/{}/count", pct(name))))
                .await
                .ok()
                .and_then(|r| r.get("count").and_then(Json::as_i64));
            tables.push(TableInfo { schema: Some(self.database.clone()), name: name.to_string(), kind: TableKind::Table, row_estimate });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        let mut schemas = vec![SchemaInfo { name: self.database.clone(), tables }];
        if let Ok(graphs) = self.graphs().await {
            if !graphs.is_empty() {
                let gtables = graphs
                    .iter()
                    .filter_map(|g| g.get("_key").or_else(|| g.get("name")).and_then(Json::as_str))
                    .map(|n| TableInfo { schema: Some(GRAPH_SCHEMA.into()), name: n.to_string(), kind: TableKind::View, row_estimate: None })
                    .collect();
                schemas.push(SchemaInfo { name: GRAPH_SCHEMA.into(), tables: gtables });
            }
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        if Self::is_graph_table(table) {
            return Ok(Self::graph_columns());
        }
        let is_edge = self.collection_type(&table.name).await?;
        let mut binds = Map::new();
        binds.insert("@c".into(), Json::String(table.name.clone()));
        let (docs, _, _) = self.aql(&format!("FOR d IN @@c LIMIT {SAMPLE_SIZE} RETURN d"), binds, SAMPLE_SIZE as usize).await?;
        Ok(union_columns(&docs, is_edge))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        if Self::is_graph_table(table) {
            return Ok(None);
        }
        let v: Json = self.http.get_json(&self.db_path(&format!("_api/collection/{}/count", pct(&table.name)))).await?;
        Ok(v.get("count").and_then(Json::as_i64))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        if Self::is_graph_table(table) {
            let graphs = self.graphs().await?;
            let rows: Vec<Vec<Value>> = graphs.iter().map(Self::graph_row).collect();
            let cols: Vec<String> = Self::graph_columns().into_iter().map(|c| c.name).collect();
            return Ok(crate::integrations::http::local::apply_filters(&cols, rows, filters).len() as i64);
        }
        let mut binds = Map::new();
        binds.insert("@c".into(), Json::String(table.name.clone()));
        let filter = filter_clause(filters, &mut binds);
        let query = format!("FOR d IN @@c{filter} COLLECT WITH COUNT INTO n RETURN n");
        let (rows, _, _) = self.aql(&query, binds, 1).await?;
        Ok(rows.first().and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        if Self::is_graph_table(table) {
            let graphs = self.graphs().await?;
            let rows: Vec<Vec<Value>> = graphs.iter().map(Self::graph_row).collect();
            let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
            let rows = crate::integrations::http::local::page(&names, rows, query);
            let columns = cols.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect();
            return Ok(ResultSet { columns, rows, truncated: false });
        }
        let limit = query.limit.min(MAX_PAGE_ROWS);
        let mut binds = Map::new();
        binds.insert("@c".into(), Json::String(table.name.clone()));
        let filter = filter_clause(&query.filters, &mut binds);
        let sort = sort_clause(&query.sort, &mut binds);
        binds.insert("offset".into(), json!(query.offset));
        binds.insert("limit".into(), json!(limit));
        let aql = format!("FOR d IN @@c{filter}{sort} LIMIT @offset, @limit RETURN d");
        let (docs, more, _) = self.aql(&aql, binds, limit as usize).await?;
        Ok(docs_to_result_set(&cols, &docs, more))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let (query, binds) = parse_execute_input(sql)?;
        if self.read_only && is_write_aql(&query) {
            return Err(AppError::read_only("This connection is read-only; data-modification AQL (INSERT/UPDATE/REMOVE/REPLACE/UPSERT) is blocked."));
        }
        let (rows, more, extra) = self.aql(&query, binds, max_rows.max(1)).await?;
        let writes = extra
            .get("stats")
            .and_then(|s| s.get("writesExecuted"))
            .and_then(Json::as_u64)
            .unwrap_or(0);
        if rows.is_empty() && writes > 0 {
            return Ok(vec![StatementResult::Affected { rows_affected: writes }]);
        }
        let mut result = cursor_rows_to_result_set(&rows, max_rows.max(1));
        result.truncated = result.truncated || more;
        Ok(vec![StatementResult::Rows { result }])
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

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    #[test]
    fn filters_become_bound_aql() {
        let mut binds = Map::new();
        let clause = filter_clause(
            &[
                rule("age", FilterOp::Gte, "5"),
                rule("name", FilterOp::Contains, "bo"),
                rule("tier", FilterOp::In, "gold, 2"),
                rule("_key", FilterOp::Eq, "42"),
                rule("note", FilterOp::IsNull, ""),
            ],
            &mut binds,
        );
        assert_eq!(
            clause,
            " FILTER d[@c0] >= @v0 AND LIKE(TO_STRING(d[@c1]), @v1, true) AND d[@c2] IN @v2 AND d[@c3] == @v3 AND d[@c4] == null"
        );
        assert_eq!(binds["v0"], json!(5));
        assert_eq!(binds["v1"], json!("%bo%"));
        assert_eq!(binds["v2"], json!(["gold", 2]));
        assert_eq!(binds["v3"], json!("42"));
        assert_eq!(binds["c4"], json!("note"));
        let mut b2 = Map::new();
        assert_eq!(sort_clause(&[SortRule { column: "x".into(), desc: true }], &mut b2), " SORT d[@s0] DESC");
        assert_eq!(b2["s0"], json!("x"));
    }

    #[test]
    fn write_detection_is_word_based() {
        assert!(is_write_aql("FOR d IN c INSERT d INTO other"));
        assert!(is_write_aql("upsert { a: 1 } insert {} update {} in c"));
        assert!(!is_write_aql("FOR d IN inserted RETURN d.updates"));
        assert!(!is_write_aql("RETURN LENGTH(c)"));
    }

    #[test]
    fn execute_input_accepts_json_and_text() {
        let (q, b) = parse_execute_input(r#"{"query":"RETURN @x","bindVars":{"x":1}}"#).unwrap();
        assert_eq!(q, "RETURN @x");
        assert_eq!(b["x"], json!(1));
        let (q, b) = parse_execute_input("  RETURN 1 ").unwrap();
        assert_eq!(q, "RETURN 1");
        assert!(b.is_empty());
        assert!(parse_execute_input("").is_err());
        assert!(parse_execute_input("{\"nope\":1}").is_err());
    }

    #[test]
    fn columns_union_pins_system_fields() {
        let docs = vec![json!({"_key": "1", "_id": "c/1", "_rev": "x", "a": 1}), json!({"_key": "2", "b": "s"})];
        let cols = union_columns(&docs, true);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_key", "_id", "_rev", "_from", "_to", "a", "b"]);
        assert!(cols[0].primary_key);
        assert_eq!(cols[5].data_type, "integer");
        let rs = docs_to_result_set(&cols, &[json!({"_key": "3", "zzz": true})], false);
        assert_eq!(rs.columns.len(), 8);
        assert_eq!(rs.rows[0][7], Value::Bool(true));
    }

    #[test]
    fn cursor_rows_map_scalars_and_objects() {
        let rs = cursor_rows_to_result_set(&[json!(1), json!(2)], 10);
        assert_eq!(rs.columns[0].name, "value");
        assert_eq!(rs.rows.len(), 2);
        let rs = cursor_rows_to_result_set(&[json!({"_key": "a", "x": 1})], 10);
        assert_eq!(rs.columns[0].name, "_key");
        assert_eq!(pct("a b/c"), "a%20b%2Fc");
    }

    #[test]
    fn collections_and_indexes_map() {
        let items = vec![
            json!({"name": "users", "type": 2, "status": 3, "isSystem": false}),
            json!({"name": "knows", "type": 3, "status": 3, "isSystem": false}),
            json!({"name": "_graphs", "type": 2, "status": 3, "isSystem": true}),
        ];
        let mut counts = BTreeMap::new();
        counts.insert("users".to_string(), 1234);
        let docs = collection_summaries(&items, ObjectKind::Collection, "app", &counts);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].reference.name, "users");
        assert_eq!(docs[0].reference.parent.as_deref(), Some("app"));
        assert_eq!(docs[0].detail.as_deref(), Some("1,234 docs"));
        assert_eq!(docs[0].badge.as_deref(), Some("loaded"));
        let edges = collection_summaries(&items, ObjectKind::EdgeCollection, "app", &counts);
        assert_eq!(edges[0].reference.name, "knows");
        assert!(edges[0].detail.is_none());

        let idx = vec![
            json!({"id": "users/0", "type": "primary", "fields": ["_key"], "unique": true, "sparse": false}),
            json!({"id": "users/12", "name": "by_email", "type": "persistent", "fields": ["email", "tenant"], "unique": true, "sparse": true}),
            json!({"id": "users/13", "name": "ttl", "type": "ttl", "fields": ["createdAt"], "expireAfter": 3600}),
        ];
        let s = index_summaries(&idx, "users");
        assert_eq!(s.iter().map(|i| i.reference.name.as_str()).collect::<Vec<_>>(), vec!["0", "by_email", "ttl"]);
        assert_eq!(s[1].detail.as_deref(), Some("email, tenant · unique · sparse"));
        assert_eq!(s[1].badge.as_deref(), Some("persistent"));
        assert_eq!(s[1].reference.parent.as_deref(), Some("users"));
        assert_eq!(s[2].detail.as_deref(), Some("createdAt · expires after 3600s"));
        let d = index_detail(&s[1].reference, &idx[1]);
        assert_eq!(d.language, CodeLanguage::Json);
        assert_eq!(d.rows.map(|r| r.rows.len()), Some(2));
        assert!(d.properties.iter().any(|p| p.name == "unique" && p.value == "true"));

        let r = ObjectRef { kind: ObjectKind::Collection, name: "users".into(), parent: Some("app".into()) };
        let props = json!({"type": 2, "status": 3, "waitForSync": false, "keyOptions": {"type": "traditional", "allowUserKeys": true}});
        let figures = json!({"count": 1234, "figures": {"documentsSize": 2048.0, "indexes": {"count": 2, "size": 1024.0}}});
        let d = collection_detail(&r, &props, Some(&figures), vec![], vec![]);
        assert_eq!(d.properties[0].value, "1,234");
        assert!(d.properties.iter().any(|p| p.name == "documents size" && p.value == "2.0 KB"));
        assert!(d.properties.iter().any(|p| p.name == "type" && p.value == "document"));
        assert!(d.properties.iter().any(|p| p.name == "keyOptions"));
        assert_eq!(d.actions.len(), 3);
        assert_eq!(d.actions[2].statement, "FOR d IN `users` REMOVE d IN `users`");
        assert!(d.actions[2].destructive && !d.actions[0].destructive);
        assert!(is_write_aql(&d.actions[2].statement));
    }

    #[test]
    fn graphs_views_functions_services_map() {
        let g = json!({"_key": "social", "isSmart": true, "numberOfShards": 3, "edgeDefinitions": [{"collection": "knows", "from": ["people"], "to": ["people", "orgs"]}], "orphanCollections": ["tags"]});
        let s = graph_summaries(std::slice::from_ref(&g));
        assert_eq!(s[0].badge.as_deref(), Some("smart"));
        assert_eq!(s[0].detail.as_deref(), Some("1 edge definition(s) · 1 orphan(s)"));
        let d = graph_detail(&s[0].reference, &g, "app");
        assert_eq!(d.rows.as_ref().map(|r| r.rows[0][2].clone()), Some(Value::Text("people, orgs".into())));
        let kids: Vec<(ObjectKind, &str)> = d.children.iter().map(|c| (c.reference.kind, c.reference.name.as_str())).collect();
        assert_eq!(kids, vec![(ObjectKind::EdgeCollection, "knows"), (ObjectKind::Collection, "orgs"), (ObjectKind::Collection, "people"), (ObjectKind::Collection, "tags")]);
        assert!(d.properties.iter().any(|p| p.name == "numberOfShards" && p.value == "3"));

        let v = view_summaries(&[json!({"name": "search", "type": "arangosearch"})]);
        assert_eq!(v[0].badge.as_deref(), Some("arangosearch"));
        let d = view_detail(&v[0].reference, &json!({"type": "arangosearch", "links": {"users": {}, "orgs": {}}, "primarySort": [{"field": "a", "asc": true}]}));
        assert!(d.properties.iter().any(|p| p.name == "links" && p.value == "orgs, users"));
        assert_eq!(d.actions[0].statement, "FOR d IN `search` LIMIT 20 RETURN d");

        let f = function_summaries(&[json!({"name": "myfuncs::add", "code": "function (a, b) {\n  return a + b;\n}", "isDeterministic": true})], "app");
        assert_eq!(f[0].badge.as_deref(), Some("deterministic"));
        assert_eq!(f[0].detail.as_deref(), Some("function (a, b) { return a + b; }"));
        assert_eq!(function_detail(&f[0].reference, &json!({"code": "x"})).definition.as_deref(), Some("x"));

        let sv = service_summaries(&[json!({"mount": "/api", "name": "demo", "version": "1.2.0", "development": true}), json!({"mount": "/_admin/aardvark", "name": "aardvark", "version": "3", "legacy": true})]);
        assert_eq!(sv[0].reference.name, "/_admin/aardvark");
        assert_eq!(sv[0].badge.as_deref(), Some("legacy"));
        assert_eq!(sv[1].detail.as_deref(), Some("demo 1.2.0"));
        let d = service_detail(&sv[1].reference, &json!({"mount": "/api", "development": true, "manifest": {"name": "demo", "author": "me"}}));
        assert!(d.properties.iter().any(|p| p.name == "author" && p.value == "me"));
    }

    #[test]
    fn users_queries_nodes_map() {
        let u = user_summaries(&[json!({"user": "root", "active": true}), json!({"user": "bob", "active": false, "extra": {"team": "x"}})]);
        assert_eq!(u[0].reference.name, "bob");
        assert_eq!(u[0].badge.as_deref(), Some("inactive"));
        let d = user_detail(&u[0].reference, Some(&json!({"user": "bob", "active": false, "extra": {"team": "x"}})), Some(&json!({"result": {"app": "rw", "_system": "ro"}})));
        assert!(d.properties.iter().any(|p| p.name == "extra"));
        let rows = d.rows.map(|r| r.rows).unwrap_or_default();
        assert_eq!(rows[0], vec![Value::Text("_system".into()), Value::Text("ro".into())]);

        let q = query_summaries(ObjectKind::Session, &[json!({"id": "42", "query": "FOR d IN users\n RETURN d", "user": "root", "runTime": 1.234, "state": "executing", "peakMemoryUsage": 4096})]);
        assert_eq!(q[0].reference.name, "42");
        assert_eq!(q[0].badge.as_deref(), Some("executing"));
        assert_eq!(q[0].detail.as_deref(), Some("root · 1.23 s · FOR d IN users RETURN d"));
        let d = query_detail(&q[0].reference, &json!({"id": 42, "query": "RETURN 1", "peakMemoryUsage": 4096}));
        assert_eq!(d.definition.as_deref(), Some("RETURN 1"));
        assert!(d.properties.iter().any(|p| p.name == "peak memory" && p.value == "4.0 KB"));
        assert!(d.actions.is_empty());

        let health = json!({"Health": {
            "CRDN-1": {"ShortName": "Coordinator0001", "Role": "Coordinator", "Status": "GOOD", "Endpoint": "tcp://c1:8529", "Version": "3.11.0"},
            "PRMR-1": {"ShortName": "DBServer0001", "Role": "DBServer", "Status": "GOOD", "Endpoint": "tcp://d1:8530", "Version": "3.11.0", "Leading": true}
        }});
        let n = cluster_nodes(&health);
        assert_eq!(n[0].reference.name, "Coordinator0001");
        assert_eq!(n[0].badge.as_deref(), Some("coordinator"));
        assert_eq!(n[1].detail.as_deref(), Some("GOOD · tcp://d1:8530 · v3.11.0 · leading"));
        let single = single_node(Some("SINGLE"), &json!({"version": "3.11.0", "license": "community"}));
        assert_eq!(single.badge.as_deref(), Some("single"));
        assert_eq!(single.detail.as_deref(), Some("v3.11.0 · community"));
    }

    #[test]
    fn stats_groups_from_statistics() {
        let version = json!({"version": "3.11.4", "license": "community"});
        let statistics = json!({
            "system": {"residentSize": 268435456.0, "virtualSize": 1073741824.0, "residentSizePercent": 0.0156, "numberOfThreads": 42},
            "client": {"httpConnections": 3, "bytesSent": {"sum": 2048.0, "count": 3}, "bytesReceived": 512},
            "http": {"requestsTotal": 100, "requestsGet": 60, "requestsPost": 40},
            "server": {"uptime": 3661.5, "physicalMemory": 17179869184.0, "transactions": {"started": 10, "committed": 9, "aborted": 1}}
        });
        let catalog = CatalogCounts { databases: 2, collections: 5, edge_collections: 1, graphs: 1, views: 0, running_queries: 0 };
        let groups = stat_groups(&version, Some(&json!({"name": "rocksdb"})), Some("SINGLE"), Some("default"), Some(&statistics), &catalog);
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Connections", "Memory", "Throughput", "Transactions", "Catalog"]);
        let server = &groups[0].stats;
        assert_eq!(server[0].value, "ArangoDB 3.11.4");
        assert!(server.iter().any(|s| s.label == "Uptime" && s.value == "1h 1m"));
        assert!(server.iter().any(|s| s.label == "Physical memory" && s.value == "16.0 GB"));
        assert_eq!(groups[1].stats[0].numeric, Some(3.0));
        assert!(groups[2].stats.iter().any(|s| s.label == "Resident of physical" && s.value == "1.6"));
        assert!(groups[3].stats.iter().any(|s| s.label == "Bytes sent" && s.numeric == Some(2048.0)));
        assert!(groups[3].stats.iter().any(|s| s.label == "Bytes received" && s.numeric == Some(512.0)));
        assert_eq!(groups[4].stats.len(), 3);
        assert_eq!(groups[5].stats[1].value, "5");
        let minimal = stat_groups(&version, None, None, None, None, &CatalogCounts::default());
        assert_eq!(minimal.len(), 2);
        assert_eq!(result_array(json!({"result": [1, 2]})).len(), 2);
        assert_eq!(result_array(json!({"graphs": [{"_key": "g"}]})).len(), 1);
        assert_eq!(result_array(json!([1])).len(), 1);
        assert_eq!(duration_text(90000.0), "1d 1h");
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        use crate::model::{ConnectionSummary, Environment};
        let Ok(url) = std::env::var("DBFREE_TEST_ARANGODB_URL") else { return };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Arangodb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: std::env::var("DBFREE_TEST_ARANGODB_DB").ok(),
                username: std::env::var("DBFREE_TEST_ARANGODB_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_ARANGODB_PASSWORD").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("ArangoDB"), "{version}");
        let out = db.execute("RETURN 1 + 1", 10).await.unwrap_or_else(|e| panic!("execute: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows[0][0], Value::Int(2)),
            _ => panic!("expected rows"),
        }
        let cat = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(!cat.schemas.is_empty());
    }
}
