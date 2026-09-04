// SOT: arangodb-integration, aql, arango-http-api, arango-cursor, arango-graphs

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_to_value, json_type_name, objects_to_result_set, Auth, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SortRule, SslMode, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Map, Value as Json};
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

#[async_trait]
impl Integration for ArangoIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { views: true, exact_estimate: true, ..Capabilities::DOCUMENT }
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
