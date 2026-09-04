// SOT: surrealdb-integration, surrealql, surreal-http-api, surreal-sql-endpoint, surreal-object-explorer, surreal-server-stats, surreal-info-statements

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, json_to_value, json_type_name, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, SortRule, SslMode,
    Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Method;
use serde_json::Value as Json;
use std::sync::Arc;

// ============================================================================
// WHAT:  SurrealDB adapter over the HTTP API (port 8000, `POST /sql`).
// WHY:   Tables are schemaless unless DEFINEd; the grid needs a header, so
//        columns = `INFO FOR TABLE` fields (schemafull) ∪ keys sampled from
//        `SELECT * FROM t LIMIT 50`, with `id` pinned and marked primary key.
// HOW:   The `database` field is "ns/db" (default test/test). Every request
//        sends both the v2 (`surreal-ns` / `surreal-db`) and v1 (`NS` / `DB`)
//        headers plus Basic auth (root user + secret). Paging is SurrealQL
//        (`WHERE … ORDER BY … LIMIT n START m`) with values inlined as
//        escaped literals (the HTTP `/sql` endpoint has no bind parameters).
//        `execute` is SurrealQL passthrough: one StatementResult per returned
//        statement; mutating statements are refused when read-only. Record ids
//        (`table:id`) arrive as strings and are shown as Text.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs (trait)
// ============================================================================

const DEFAULT_PORT: u16 = 8000;
const DEFAULT_NS: &str = "test";
const DEFAULT_DB: &str = "test";
const SAMPLE_SIZE: usize = 50;
const MAX_PAGE_ROWS: u32 = 5_000;
const WRITE_KEYWORDS: [&str; 9] = ["CREATE", "UPDATE", "DELETE", "RELATE", "INSERT", "UPSERT", "DEFINE", "REMOVE", "LET"];

pub struct SurrealIntegration {
    engine: Engine,
    http: HttpClient,
    namespace: String,
    database: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let (namespace, database) = split_ns_db(s.database.as_deref());
    let base = base_url(conn, Some(DEFAULT_PORT), false);
    let insecure = s.ssl_mode == SslMode::Require;
    let http = HttpClient::new(base, HttpClient::auth_from_connection(conn), insecure)?;
    let integration = SurrealIntegration { engine: s.engine, http, namespace, database, read_only: s.read_only };
    integration.ping().await?;
    // SurrealDB 2.x rejects every statement until the namespace and database
    // exist. Creating them is idempotent and needs no privileges beyond the
    // ones a user who can write already has, so a fresh server is usable
    // immediately instead of failing with "The namespace 'x' does not exist".
    if !s.read_only {
        let bootstrap = format!(
            "DEFINE NAMESPACE IF NOT EXISTS {}; USE NS {}; DEFINE DATABASE IF NOT EXISTS {};",
            ident(&integration.namespace),
            ident(&integration.namespace),
            ident(&integration.database)
        );
        let _ = integration.sql(&bootstrap).await;
    }
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// WHAT:  "ns/db" → (ns, db); a bare name is the database inside the default namespace.
fn split_ns_db(raw: Option<&str>) -> (String, String) {
    let t = raw.map(str::trim).unwrap_or("");
    if t.is_empty() {
        return (DEFAULT_NS.into(), DEFAULT_DB.into());
    }
    match t.split_once('/') {
        Some((ns, db)) => (
            if ns.trim().is_empty() { DEFAULT_NS.into() } else { ns.trim().into() },
            if db.trim().is_empty() { DEFAULT_DB.into() } else { db.trim().into() },
        ),
        None => (DEFAULT_NS.into(), t.into()),
    }
}

// WHAT:  Backtick-quotes an identifier (SurrealQL accepts `⟨ ⟩` and backticks).
fn ident(raw: &str) -> String {
    format!("`{}`", raw.replace('`', "\\`"))
}

fn quote_str(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('\'');
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

// WHAT:  A typed literal for comparisons: numbers / booleans as-is, `table:id`
//        as a record reference when filtering `id`, everything else a string.
fn literal(column: &str, raw: &str) -> String {
    let t = raw.trim();
    if column == "id" {
        if let Some((table, key)) = t.split_once(':') {
            let ok = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if ok(table) && ok(key) {
                return format!("{table}:{key}");
            }
        }
        return quote_str(t);
    }
    if t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        return t.to_ascii_lowercase();
    }
    if t.eq_ignore_ascii_case("null") {
        return "NULL".into();
    }
    if t.eq_ignore_ascii_case("none") {
        return "NONE".into();
    }
    if t.parse::<i64>().is_ok() || t.parse::<f64>().is_ok() {
        return t.to_string();
    }
    quote_str(t)
}

fn where_clause(filters: &[FilterRule]) -> String {
    if filters.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| {
            let col = ident(&f.column);
            let v = f.value.trim();
            match f.op {
                FilterOp::Eq => format!("{col} = {}", literal(&f.column, v)),
                FilterOp::Ne => format!("{col} != {}", literal(&f.column, v)),
                FilterOp::Gt => format!("{col} > {}", literal(&f.column, v)),
                FilterOp::Gte => format!("{col} >= {}", literal(&f.column, v)),
                FilterOp::Lt => format!("{col} < {}", literal(&f.column, v)),
                FilterOp::Lte => format!("{col} <= {}", literal(&f.column, v)),
                FilterOp::Contains => format!("string::contains(<string> {col}, {})", quote_str(v)),
                FilterOp::StartsWith => format!("string::starts_with(<string> {col}, {})", quote_str(v)),
                FilterOp::EndsWith => format!("string::ends_with(<string> {col}, {})", quote_str(v)),
                FilterOp::In => {
                    let items: Vec<String> = v.split(',').map(str::trim).filter(|x| !x.is_empty()).map(|x| literal(&f.column, x)).collect();
                    if items.is_empty() {
                        "false".into()
                    } else {
                        format!("{col} INSIDE [{}]", items.join(", "))
                    }
                }
                FilterOp::IsNull => format!("({col} = NONE OR {col} = NULL)"),
                FilterOp::IsNotNull => format!("({col} != NONE AND {col} != NULL)"),
            }
        })
        .collect();
    format!(" WHERE {}", parts.join(" AND "))
}

fn order_clause(sort: &[SortRule]) -> String {
    if sort.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = sort.iter().map(|s| format!("{} {}", ident(&s.column), if s.desc { "DESC" } else { "ASC" })).collect();
    format!(" ORDER BY {}", parts.join(", "))
}

// WHAT:  Splits a SurrealQL script on `;` outside of quotes / brackets.
fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut depth = 0i32;
    for ch in text.chars() {
        match quote {
            Some(q) => {
                cur.push(ch);
                if ch == q {
                    quote = None;
                }
            }
            None => match ch {
                '\'' | '"' | '`' => {
                    quote = Some(ch);
                    cur.push(ch);
                }
                '{' | '[' | '(' => {
                    depth += 1;
                    cur.push(ch);
                }
                '}' | ']' | ')' => {
                    depth -= 1;
                    cur.push(ch);
                }
                ';' if depth <= 0 => {
                    if !cur.trim().is_empty() {
                        out.push(cur.trim().to_string());
                    }
                    cur.clear();
                }
                _ => cur.push(ch),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

fn first_word(stmt: &str) -> String {
    stmt.trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

fn is_write_statement(stmt: &str) -> bool {
    let w = first_word(stmt);
    WRITE_KEYWORDS.contains(&w.as_str())
}

// WHAT:  One `/sql` response entry → StatementResult.
fn statement_to_result(status: &str, result: &Json, stmt: &str, max_rows: usize) -> AppResult<StatementResult> {
    if status != "OK" {
        let msg = result.as_str().map(str::to_string).unwrap_or_else(|| result.to_string());
        return Err(AppError::driver(format!("SurrealQL error: {msg}")));
    }
    let word = first_word(stmt);
    match result {
        Json::Array(items) => {
            if matches!(word.as_str(), "CREATE" | "UPDATE" | "DELETE" | "RELATE" | "INSERT" | "UPSERT") {
                return Ok(StatementResult::Affected { rows_affected: items.len() as u64 });
            }
            if items.is_empty() {
                return Ok(StatementResult::Rows { result: ResultSet { columns: vec![], rows: vec![], truncated: false } });
            }
            if items.iter().all(Json::is_object) {
                let id = items.iter().any(|d| d.get("id").is_some()).then_some("id");
                let rs = crate::integrations::http::objects_to_result_set(items, id, max_rows);
                return Ok(StatementResult::Rows { result: rs });
            }
            let truncated = items.len() > max_rows;
            let type_name = items.iter().find(|v| !v.is_null()).map(json_type_name).unwrap_or("json").to_string();
            Ok(StatementResult::Rows {
                result: ResultSet {
                    columns: vec![ColumnMeta { name: "value".into(), type_name }],
                    rows: items.iter().take(max_rows).map(|v| vec![json_to_value(v)]).collect(),
                    truncated,
                },
            })
        }
        Json::Null if matches!(word.as_str(), "DEFINE" | "REMOVE" | "LET" | "USE" | "BEGIN" | "COMMIT" | "CANCEL") => {
            Ok(StatementResult::Affected { rows_affected: 0 })
        }
        other => Ok(StatementResult::Rows { result: json_result(other.clone()) }),
    }
}

// WHAT:  Union of keys across sampled records + declared fields; `id` pinned.
fn union_columns(declared: &[String], docs: &[Json]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = vec!["id".into()];
    let mut types: Vec<Option<&'static str>> = vec![Some("record")];
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
    for d in declared {
        // Nested field definitions (`a.b`, `a[*]`) are not top-level keys.
        if !d.contains('.') && !d.contains('[') {
            push(d, None);
        }
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
            primary_key: name == "id",
            nullable: name != "id",
            data_type: ty.unwrap_or("null").to_string(),
            name,
            ordinal: u32::try_from(i + 1).unwrap_or(u32::MAX),
        })
        .collect()
}

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
        .map(|doc| {
            let obj = doc.as_object();
            names.iter().map(|n| obj.and_then(|o| o.get(n)).map(json_to_value).unwrap_or(Value::Null)).collect()
        })
        .collect();
    ResultSet { columns: names.into_iter().zip(types).map(|(name, type_name)| ColumnMeta { name, type_name }).collect(), rows, truncated }
}

// WHAT:  `INFO FOR …` returns maps of name → definition; extract the names.
fn info_keys(info: &Json, section: &str) -> Vec<String> {
    let mut keys: Vec<String> = info
        .get(section)
        .and_then(Json::as_object)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    keys.sort();
    keys
}

impl SurrealIntegration {
    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(reqwest::header::ACCEPT, HeaderValue::from_static("application/json"));
        h.insert(reqwest::header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        for (name, value) in [("surreal-ns", &self.namespace), ("surreal-db", &self.database), ("NS", &self.namespace), ("DB", &self.database)] {
            if let (Ok(n), Ok(v)) = (reqwest::header::HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(value)) {
                h.insert(n, v);
            }
        }
        h
    }

    // WHAT:  POST /sql → [{status, result, time}, …]
    async fn sql(&self, script: &str) -> AppResult<Vec<Json>> {
        let req = self.http.request(Method::POST, "/sql").headers(self.headers()).body(script.to_string());
        let resp = self.http.send(req).await?;
        let body: Json = resp.json().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))?;
        match body {
            Json::Array(items) => Ok(items),
            Json::Object(o) if o.get("code").is_some() || o.get("description").is_some() => {
                let msg = o.get("information").or_else(|| o.get("description")).and_then(Json::as_str).unwrap_or("request failed");
                Err(AppError::driver(format!("SurrealDB: {msg}")))
            }
            other => Ok(vec![other]),
        }
    }

    // WHAT:  Runs one statement and returns its `result` (or an error for a non-OK status).
    async fn one(&self, statement: &str) -> AppResult<Json> {
        let items = self.sql(statement).await?;
        let first = items.into_iter().next().ok_or_else(|| AppError::driver("SurrealDB returned no statement result."))?;
        let status = first.get("status").and_then(Json::as_str).unwrap_or("ERR");
        let result = first.get("result").cloned().unwrap_or(Json::Null);
        if status != "OK" {
            let msg = result.as_str().map(str::to_string).unwrap_or_else(|| result.to_string());
            return Err(AppError::driver(format!("SurrealQL error: {msg}")));
        }
        Ok(result)
    }

    async fn table_names(&self) -> AppResult<Vec<String>> {
        let info = self.one("INFO FOR DB").await?;
        Ok(info_keys(&info, "tables"))
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Everything the explorer shows comes from the `INFO FOR …` family:
//        ROOT (namespaces, users, system), NS (databases, users), DB (tables,
//        functions, params, accesses, users) and TABLE (fields, indexes,
//        events). Each section is a map of name → the DEFINE statement that
//        created it, so the definition pane is the server's own DDL.
// WHY:   One statement per kind, no schema guessing, and the definitions are
//        exactly what a user would type to recreate the object.
// HOW:   Actions are SurrealQL REMOVE statements, which `is_write_statement`
//        already blocks on a read-only connection.
// ---------------------------------------------------------------------------

const LIST_CAP: usize = 2_000;

// WHAT:  An `INFO FOR …` section as (name, DEFINE statement) pairs, sorted.
fn info_entries(info: &Json, section: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = info
        .get(section)
        .and_then(Json::as_object)
        .map(|m| {
            m.iter()
                .map(|(k, v)| {
                    let text = match v {
                        Json::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), text)
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.truncate(LIST_CAP);
    out
}

// WHAT:  The word(s) following `keyword` in a DEFINE statement, up to the next
//        clause; used to read ROLES / TYPE / COMMENT out of the server's DDL.
fn clause_after(definition: &str, keyword: &str) -> Option<String> {
    let upper = definition.to_ascii_uppercase();
    let at = upper.find(&format!(" {keyword} "))? + keyword.len() + 2;
    // THEN closes an event's WHEN condition; without it the whole event body
    // would be reported as the condition.
    const STOPS: [&str; 9] = [" DURATION ", " COMMENT ", " PERMISSIONS ", " SESSION ", " SIGNIN ", " SIGNUP ", " ON ", " WITH ", " THEN "];
    let rest = &definition[at..];
    let upper_rest = &upper[at..];
    let end = STOPS.iter().filter_map(|s| upper_rest.find(s)).min().unwrap_or(rest.len());
    let value = rest[..end].trim().trim_matches('"').trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn has_word(definition: &str, word: &str) -> bool {
    definition.to_ascii_uppercase().split(|c: char| !c.is_ascii_alphanumeric() && c != '_').any(|w| w == word)
}

fn preview(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        format!("{}…", flat.chars().take(max).collect::<String>())
    }
}

fn table_badge(definition: &str) -> Option<String> {
    if has_word(definition, "RELATION") {
        Some("relation".into())
    } else if has_word(definition, "SCHEMAFULL") {
        Some("schemafull".into())
    } else if has_word(definition, "SCHEMALESS") {
        Some("schemaless".into())
    } else {
        None
    }
}

fn index_badge(definition: &str) -> Option<String> {
    for (word, badge) in [("UNIQUE", "unique"), ("SEARCH", "search"), ("MTREE", "mtree"), ("HNSW", "hnsw")] {
        if has_word(definition, word) {
            return Some(badge.into());
        }
    }
    Some("index".into())
}

fn definition_summary(kind: ObjectKind, name: &str, definition: &str, parent: Option<String>, badge: Option<String>) -> ObjectSummary {
    let detail = match kind {
        ObjectKind::Index => clause_after(definition, "FIELDS").or_else(|| clause_after(definition, "COLUMNS")),
        ObjectKind::Event => clause_after(definition, "WHEN"),
        ObjectKind::User => clause_after(definition, "ROLES"),
        _ => None,
    }
    .unwrap_or_else(|| preview(definition, 120));
    ObjectSummary { reference: ObjectRef { kind, name: name.to_string(), parent }, detail: Some(detail), badge }
}

fn summaries(kind: ObjectKind, entries: &[(String, String)], parent: Option<&str>, badge: impl Fn(&str) -> Option<String>) -> Vec<ObjectSummary> {
    entries
        .iter()
        .map(|(name, def)| definition_summary(kind, name, def, parent.map(str::to_string), badge(def)))
        .collect()
}

// WHAT:  The REMOVE statement that undoes a DEFINE, per kind.
fn remove_statement(kind: ObjectKind, name: &str, parent: Option<&str>) -> Option<String> {
    let id = ident(name);
    match kind {
        ObjectKind::Namespace => Some(format!("REMOVE NAMESPACE {id}")),
        ObjectKind::Database => Some(format!("REMOVE DATABASE {id}")),
        ObjectKind::Table => Some(format!("REMOVE TABLE {id}")),
        ObjectKind::Index => parent.map(|t| format!("REMOVE INDEX {id} ON TABLE {}", ident(t))),
        ObjectKind::Event => parent.map(|t| format!("REMOVE EVENT {id} ON TABLE {}", ident(t))),
        ObjectKind::Function => Some(format!("REMOVE FUNCTION fn::{name}")),
        ObjectKind::Setting => Some(format!("REMOVE PARAM ${name}")),
        ObjectKind::Role => Some(format!("REMOVE ACCESS {id} ON DATABASE")),
        ObjectKind::User => {
            let level = match parent {
                Some("root") => "ROOT",
                Some("namespace") => "NAMESPACE",
                _ => "DATABASE",
            };
            Some(format!("REMOVE USER {id} ON {level}"))
        }
        _ => None,
    }
}

fn definition_detail(reference: &ObjectRef, definition: &str) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(definition, CodeLanguage::Sql);
    for (label, keyword) in [("type", "TYPE"), ("fields", "FIELDS"), ("when", "WHEN"), ("then", "THEN"), ("roles", "ROLES"), ("value", "VALUE"), ("comment", "COMMENT"), ("permissions", "PERMISSIONS")] {
        if let Some(v) = clause_after(definition, keyword) {
            d = d.property(label, preview(&v, 300));
        }
    }
    if let Some(statement) = remove_statement(reference.kind, &reference.name, reference.parent.as_deref()) {
        let label = format!("Remove {}", format!("{:?}", reference.kind).to_lowercase());
        d = d.action(ObjectAction::destructive("remove", &label, statement));
    }
    d
}

fn table_detail(reference: &ObjectRef, definition: &str, info: &Json, columns: Vec<ColumnInfo>, count: Option<i64>) -> ObjectDetail {
    let mut d = definition_detail(reference, definition);
    if let Some(c) = count {
        d = d.property("records", crate::model::objects::format_number(c as f64));
    }
    let fields = info_entries(info, "fields");
    d = d.property("fields", fields.len().to_string());
    d.columns = columns;
    d.rows = Some(ResultSet {
        columns: vec![ColumnMeta { name: "field".into(), type_name: "string".into() }, ColumnMeta { name: "definition".into(), type_name: "string".into() }],
        rows: fields.iter().map(|(n, def)| vec![Value::Text(n.clone()), Value::Text(def.clone())]).collect(),
        truncated: false,
    });
    let name = reference.name.as_str();
    d.children = summaries(ObjectKind::Index, &info_entries(info, "indexes"), Some(name), index_badge)
        .into_iter()
        .chain(summaries(ObjectKind::Event, &info_entries(info, "events"), Some(name), |_| Some("event".into())))
        .collect();
    let id = ident(name);
    d.action(ObjectAction::new("sample", "Sample 20", format!("SELECT * FROM {id} LIMIT 20")))
        .action(ObjectAction::new("count", "Count", format!("SELECT count() FROM {id} GROUP ALL")))
        .action(ObjectAction::destructive("delete-all", "Delete all records", format!("DELETE {id}")))
}

// WHAT:  `INFO FOR ROOT`'s `system` section (2.x) → the Server stat group.
fn system_stats(root: &Json) -> Vec<Stat> {
    let Some(system) = root.get("system") else { return Vec::new() };
    let num = |k: &str| system.get(k).and_then(Json::as_f64);
    let mut out = Vec::new();
    for (key, label, unit) in [
        ("available_parallelism", "Parallelism", None),
        ("physical_cores", "Physical cores", None),
        ("threads", "Threads", None),
        ("cpu_usage", "CPU usage", Some("%")),
        ("load_average", "Load average", None),
    ] {
        if let Some(v) = num(key) {
            out.push(Stat::number(label, (v * 100.0).round() / 100.0, unit));
        }
    }
    for (key, label) in [("memory_usage", "Memory usage"), ("memory_allocated", "Memory allocated")] {
        if let Some(v) = num(key) {
            out.push(Stat { label: label.to_string(), value: bytes_text(v), unit: None, hint: None, numeric: Some(v) });
        }
    }
    out
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

fn stat_groups(version: Option<&str>, namespace: &str, database: &str, root: Option<&Json>, ns: Option<&Json>, db: Option<&Json>, records: Option<i64>) -> Vec<StatGroup> {
    let mut server = vec![Stat::text("Version", version.unwrap_or("SurrealDB"))];
    server.push(Stat::text("Namespace", namespace));
    server.push(Stat::text("Database", database));
    if let Some(r) = root {
        server.push(Stat::number("Namespaces", info_entries(r, "namespaces").len() as f64, None));
        if let Some(nodes) = r.get("nodes").and_then(Json::as_object) {
            server.push(Stat::number("Cluster nodes", nodes.len() as f64, None));
        }
        server.extend(system_stats(r));
    }
    let mut groups = vec![StatGroup { title: "Server".into(), stats: server }];
    let mut catalog = Vec::new();
    if let Some(n) = ns {
        catalog.push(Stat::number("Databases", info_entries(n, "databases").len() as f64, None));
    }
    if let Some(d) = db {
        catalog.push(Stat::number("Tables", info_entries(d, "tables").len() as f64, None));
        catalog.push(Stat::number("Functions", info_entries(d, "functions").len() as f64, None));
        catalog.push(Stat::number("Parameters", info_entries(d, "params").len() as f64, None));
        catalog.push(Stat::number("Analyzers", info_entries(d, "analyzers").len() as f64, None));
        catalog.push(Stat::number("Accesses", info_entries(d, "accesses").len() as f64, None));
    }
    if let Some(r) = records {
        catalog.push(Stat::number("Records", r as f64, None).with_hint("all tables"));
    }
    if !catalog.is_empty() {
        groups.push(StatGroup { title: "Catalog".into(), stats: catalog });
    }
    let users = |info: Option<&Json>| info.map(|i| info_entries(i, "users").len()).unwrap_or(0);
    groups.push(StatGroup {
        title: "Security".into(),
        stats: vec![
            Stat::number("Root users", users(root) as f64, None),
            Stat::number("Namespace users", users(ns) as f64, None),
            Stat::number("Database users", users(db) as f64, None),
        ],
    });
    groups
}

impl SurrealIntegration {
    // WHAT:  One statement with the namespace / database headers overridden, so
    //        another namespace's databases can be listed without reconnecting.
    async fn one_scoped(&self, statement: &str, ns: Option<&str>, db: Option<&str>) -> AppResult<Json> {
        let mut headers = self.headers();
        for (v2, v1, value) in [("surreal-ns", "NS", ns), ("surreal-db", "DB", db)] {
            match value {
                Some(v) => {
                    if let Ok(hv) = HeaderValue::from_str(v) {
                        headers.insert(v2, hv.clone());
                        headers.insert(v1, hv);
                    }
                }
                None => {
                    headers.remove(v2);
                    headers.remove(v1);
                }
            }
        }
        let req = self.http.request(Method::POST, "/sql").headers(headers).body(statement.to_string());
        let resp = self.http.send(req).await?;
        let body: Json = resp.json().await.map_err(|e| AppError::driver(format!("Malformed response: {e}")))?;
        let first = body.as_array().and_then(|a| a.first()).cloned().ok_or_else(|| AppError::driver("SurrealDB returned no statement result."))?;
        if first.get("status").and_then(Json::as_str) != Some("OK") {
            let result = first.get("result").cloned().unwrap_or(Json::Null);
            let msg = result.as_str().map(str::to_string).unwrap_or_else(|| result.to_string());
            return Err(AppError::driver(format!("SurrealQL error: {msg}")));
        }
        Ok(first.get("result").cloned().unwrap_or(Json::Null))
    }

    async fn info_root(&self) -> AppResult<Json> {
        self.one_scoped("INFO FOR ROOT", None, None).await
    }

    async fn info_ns(&self, ns: &str) -> AppResult<Json> {
        self.one_scoped("INFO FOR NS", Some(ns), None).await
    }

    async fn info_db(&self) -> AppResult<Json> {
        self.one("INFO FOR DB").await
    }

    async fn info_table(&self, table: &str) -> AppResult<Json> {
        self.one(&format!("INFO FOR TABLE {}", ident(table))).await
    }

    // WHAT:  Users live at three levels; the badge says which one an entry came from.
    async fn all_users(&self) -> Vec<ObjectSummary> {
        let mut out = Vec::new();
        for (level, info) in [
            ("root", self.info_root().await.ok()),
            ("namespace", self.info_ns(&self.namespace).await.ok()),
            ("database", self.info_db().await.ok()),
        ] {
            if let Some(info) = info {
                out.extend(summaries(ObjectKind::User, &info_entries(&info, "users"), Some(level), |_| Some(level.to_string())));
            }
        }
        out.truncate(LIST_CAP);
        out
    }

    async fn table_members(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let section = if kind == ObjectKind::Index { "indexes" } else { "events" };
        let badge = |def: &str| if kind == ObjectKind::Index { index_badge(def) } else { Some("event".to_string()) };
        let tables: Vec<String> = match parent.map(str::trim).filter(|p| !p.is_empty() && *p != self.database) {
            Some(t) => vec![t.to_string()],
            None => self.table_names().await?,
        };
        let mut out = Vec::new();
        for table in tables {
            if let Ok(info) = self.info_table(&table).await {
                out.extend(summaries(kind, &info_entries(&info, section), Some(&table), badge));
            }
            if out.len() >= LIST_CAP {
                break;
            }
        }
        out.truncate(LIST_CAP);
        Ok(out)
    }

    async fn total_records(&self) -> Option<i64> {
        let tables = self.table_names().await.ok()?;
        let mut total = 0;
        for t in tables.iter().take(200) {
            let sql = format!("SELECT count() FROM {} GROUP ALL", ident(t));
            if let Ok(r) = self.one(&sql).await {
                total += r.as_array().and_then(|a| a.first()).and_then(|f| f.get("count")).and_then(Json::as_i64).unwrap_or(0);
            }
        }
        Some(total)
    }

    async fn explorer_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Namespace => {
                let root = self.info_root().await?;
                Ok(summaries(ObjectKind::Namespace, &info_entries(&root, "namespaces"), None, |_| Some("namespace".into())))
            }
            ObjectKind::Database => {
                let ns = parent.map(str::trim).filter(|n| !n.is_empty()).unwrap_or(&self.namespace);
                let info = self.info_ns(ns).await?;
                Ok(summaries(ObjectKind::Database, &info_entries(&info, "databases"), Some(ns), |_| Some("database".into())))
            }
            ObjectKind::Table => {
                let info = self.info_db().await?;
                Ok(summaries(ObjectKind::Table, &info_entries(&info, "tables"), Some(&self.database), table_badge))
            }
            ObjectKind::Index | ObjectKind::Event => self.table_members(kind, parent).await,
            ObjectKind::Function => {
                let info = self.info_db().await?;
                Ok(summaries(ObjectKind::Function, &info_entries(&info, "functions"), Some(&self.database), |_| Some("function".into())))
            }
            ObjectKind::Setting => {
                let info = self.info_db().await?;
                Ok(summaries(ObjectKind::Setting, &info_entries(&info, "params"), Some(&self.database), |_| Some("param".into())))
            }
            ObjectKind::User => Ok(self.all_users().await),
            ObjectKind::Role => {
                // 2.x calls them accesses; 1.x had scopes and tokens.
                let info = self.info_db().await?;
                let mut out = summaries(ObjectKind::Role, &info_entries(&info, "accesses"), Some(&self.database), |def| {
                    clause_after(def, "TYPE").map(|t| t.split_whitespace().next().unwrap_or("access").to_lowercase())
                });
                if out.is_empty() {
                    out = summaries(ObjectKind::Role, &info_entries(&info, "scopes"), Some(&self.database), |_| Some("scope".into()));
                    out.extend(summaries(ObjectKind::Role, &info_entries(&info, "tokens"), Some(&self.database), |_| Some("token".into())));
                }
                Ok(out)
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn explorer_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let missing = || AppError::not_found(format!("`{name}` no longer exists."));
        let lookup = |entries: Vec<(String, String)>| entries.into_iter().find(|(n, _)| n == name).map(|(_, d)| d);
        match reference.kind {
            ObjectKind::Namespace => {
                let root = self.info_root().await?;
                let def = lookup(info_entries(&root, "namespaces")).ok_or_else(missing)?;
                let info = self.info_ns(name).await.ok();
                let mut d = definition_detail(reference, &def);
                if let Some(info) = &info {
                    let dbs = info_entries(info, "databases");
                    d = d.property("databases", dbs.len().to_string());
                    d.children = summaries(ObjectKind::Database, &dbs, Some(name), |_| Some("database".into()));
                    d.children.extend(summaries(ObjectKind::User, &info_entries(info, "users"), Some("namespace"), |_| Some("namespace".into())));
                }
                Ok(d)
            }
            ObjectKind::Database => {
                let ns = reference.parent.as_deref().filter(|p| !p.is_empty()).unwrap_or(&self.namespace);
                let info = self.info_ns(ns).await?;
                let def = lookup(info_entries(&info, "databases")).ok_or_else(missing)?;
                let mut d = definition_detail(reference, &def);
                if name == self.database {
                    let db = self.info_db().await?;
                    let tables = info_entries(&db, "tables");
                    d = d.property("tables", tables.len().to_string());
                    d.children = summaries(ObjectKind::Table, &tables, Some(name), table_badge);
                }
                Ok(d)
            }
            ObjectKind::Table => {
                let db = self.info_db().await?;
                let def = lookup(info_entries(&db, "tables")).ok_or_else(missing)?;
                let info = self.info_table(name).await?;
                let columns = self.columns(&TableRef { schema: Some(self.database.clone()), name: name.to_string() }).await.unwrap_or_default();
                let count = self.count(&TableRef { schema: None, name: name.to_string() }, &[]).await.ok();
                Ok(table_detail(reference, &def, &info, columns, count))
            }
            ObjectKind::Index | ObjectKind::Event => {
                let table = reference.parent.as_deref().filter(|p| !p.is_empty()).ok_or_else(|| AppError::invalid_input("This object needs its table as parent."))?;
                let info = self.info_table(table).await?;
                let section = if reference.kind == ObjectKind::Index { "indexes" } else { "events" };
                let def = lookup(info_entries(&info, section)).ok_or_else(missing)?;
                Ok(definition_detail(reference, &def))
            }
            ObjectKind::Function | ObjectKind::Setting | ObjectKind::Role => {
                let db = self.info_db().await?;
                let section = match reference.kind {
                    ObjectKind::Function => "functions",
                    ObjectKind::Setting => "params",
                    _ => "accesses",
                };
                let def = lookup(info_entries(&db, section))
                    .or_else(|| lookup(info_entries(&db, "scopes")))
                    .or_else(|| lookup(info_entries(&db, "tokens")))
                    .ok_or_else(missing)?;
                Ok(definition_detail(reference, &def))
            }
            ObjectKind::User => {
                let level = reference.parent.as_deref().unwrap_or("database");
                let info = match level {
                    "root" => self.info_root().await?,
                    "namespace" => self.info_ns(&self.namespace).await?,
                    _ => self.info_db().await?,
                };
                let def = lookup(info_entries(&info, "users")).ok_or_else(missing)?;
                Ok(definition_detail(reference, &def).property("level", level))
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn explorer_stats(&self) -> AppResult<ServerStats> {
        let version = self.server_version().await.unwrap_or(None);
        let root = self.info_root().await.ok();
        let ns = self.info_ns(&self.namespace).await.ok();
        let db = self.info_db().await.ok();
        let records = self.total_records().await;
        Ok(ServerStats::now(stat_groups(version.as_deref(), &self.namespace, &self.database, root.as_ref(), ns.as_ref(), db.as_ref(), records)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { exact_estimate: true, ..Capabilities::DOCUMENT },
        object_kinds: vec![K::Namespace, K::Database, K::Table, K::Index, K::Event, K::Function, K::Setting, K::User, K::Role],
        // RELATE edges carry in / out, which the graph view can draw.
        tools: vec![T::Stats, T::GraphView],
    }
}

#[async_trait]
impl Integration for SurrealIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let req = self.http.request(Method::GET, "/health");
        self.http.send(req).await?;
        // A real query validates credentials + ns/db headers.
        let _ = self.one("RETURN 1").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let text = self.http.get_text("/version").await?;
        let v = text.trim();
        if v.is_empty() {
            return Ok(None);
        }
        Ok(Some(if v.to_ascii_lowercase().starts_with("surreal") { v.to_string() } else { format!("SurrealDB {v}") }))
    }

    fn current_database(&self) -> Option<String> {
        Some(format!("{}/{}", self.namespace, self.database))
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let mut out = Vec::new();
        let namespaces = match self.one("INFO FOR ROOT").await {
            Ok(info) => info_keys(&info, "namespaces"),
            Err(_) => vec![self.namespace.clone()],
        };
        for ns in namespaces {
            let req = |script: &str| {
                let mut h = self.headers();
                if let Ok(v) = HeaderValue::from_str(&ns) {
                    h.insert("surreal-ns", v.clone());
                    h.insert("NS", v);
                }
                h.remove("surreal-db");
                h.remove("DB");
                self.http.request(Method::POST, "/sql").headers(h).body(script.to_string())
            };
            let dbs: Vec<String> = match self.http.send(req("INFO FOR NS")).await {
                Ok(resp) => resp
                    .json::<Json>()
                    .await
                    .ok()
                    .and_then(|v| v.as_array().and_then(|a| a.first()).and_then(|f| f.get("result")).cloned())
                    .map(|info| info_keys(&info, "databases"))
                    .unwrap_or_default(),
                Err(_) => Vec::new(),
            };
            if dbs.is_empty() && ns == self.namespace {
                out.push(format!("{ns}/{}", self.database));
            }
            out.extend(dbs.into_iter().map(|db| format!("{ns}/{db}")));
        }
        if out.is_empty() {
            out.push(format!("{}/{}", self.namespace, self.database));
        }
        Ok(out)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let names = self.table_names().await?;
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let row_estimate = self
                .one(&format!("SELECT count() FROM {} GROUP ALL", ident(&name)))
                .await
                .ok()
                .and_then(|r| r.as_array().and_then(|a| a.first()).and_then(|f| f.get("count")).and_then(Json::as_i64))
                .or(Some(0));
            tables.push(TableInfo { schema: Some(self.database.clone()), name, kind: TableKind::Table, row_estimate });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let declared = match self.one(&format!("INFO FOR TABLE {}", ident(&table.name))).await {
            Ok(info) => info_keys(&info, "fields"),
            Err(_) => Vec::new(),
        };
        let sample = self.one(&format!("SELECT * FROM {} LIMIT {SAMPLE_SIZE}", ident(&table.name))).await?;
        let docs = sample.as_array().cloned().unwrap_or_default();
        Ok(union_columns(&declared, &docs))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count() FROM {}{} GROUP ALL", ident(&table.name), where_clause(filters));
        let r = self.one(&sql).await?;
        Ok(r.as_array().and_then(|a| a.first()).and_then(|f| f.get("count")).and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let limit = query.limit.min(MAX_PAGE_ROWS);
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {limit} START {}",
            ident(&table.name),
            where_clause(&query.filters),
            order_clause(&query.sort),
            query.offset
        );
        let r = self.one(&sql).await?;
        let docs = r.as_array().cloned().unwrap_or_default();
        Ok(docs_to_result_set(&cols, &docs, false))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let script = sql.trim();
        if script.is_empty() {
            return Err(AppError::invalid_input("Empty SurrealQL script."));
        }
        let statements = split_statements(script);
        if self.read_only {
            if let Some(w) = statements.iter().find(|s| is_write_statement(s)) {
                return Err(AppError::read_only(format!(
                    "This connection is read-only; `{}` statements are blocked.",
                    first_word(w)
                )));
            }
        }
        let items = self.sql(script).await?;
        let mut out = Vec::with_capacity(items.len());
        for (i, item) in items.iter().enumerate() {
            let status = item.get("status").and_then(Json::as_str).unwrap_or("ERR");
            let result = item.get("result").cloned().unwrap_or(Json::Null);
            let stmt = statements.get(i).map(String::as_str).unwrap_or("");
            out.push(statement_to_result(status, &result, stmt, max_rows.max(1))?);
        }
        Ok(out)
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
    use serde_json::json;

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    #[test]
    fn ns_db_splitting() {
        assert_eq!(split_ns_db(None), ("test".into(), "test".into()));
        assert_eq!(split_ns_db(Some("prod/app")), ("prod".into(), "app".into()));
        assert_eq!(split_ns_db(Some("app")), ("test".into(), "app".into()));
        assert_eq!(split_ns_db(Some("/app")), ("test".into(), "app".into()));
    }

    #[test]
    fn where_and_order_render() {
        let w = where_clause(&[
            rule("age", FilterOp::Gte, "5"),
            rule("name", FilterOp::Contains, "o'b"),
            rule("tier", FilterOp::In, "gold, 2"),
            rule("id", FilterOp::Eq, "person:tobie"),
            rule("note", FilterOp::IsNull, ""),
        ]);
        assert_eq!(
            w,
            " WHERE `age` >= 5 AND string::contains(<string> `name`, 'o\\'b') AND `tier` INSIDE ['gold', 2] AND `id` = person:tobie AND (`note` = NONE OR `note` = NULL)"
        );
        assert_eq!(order_clause(&[SortRule { column: "a".into(), desc: true }]), " ORDER BY `a` DESC");
        assert_eq!(literal("id", "weird id"), "'weird id'");
    }

    #[test]
    fn statements_split_and_writes_detected() {
        let parts = split_statements("SELECT * FROM a; CREATE b SET s = 'x;y'; DEFINE TABLE c");
        assert_eq!(parts.len(), 3);
        assert!(is_write_statement("  create b"));
        assert!(is_write_statement("DEFINE TABLE x"));
        assert!(!is_write_statement("SELECT * FROM a"));
        assert!(!is_write_statement("INFO FOR DB"));
    }

    #[test]
    fn response_entries_map_to_results() {
        let ok = statement_to_result("OK", &json!([{"id": "a:1", "n": 1}]), "SELECT * FROM a", 10).unwrap();
        match ok {
            StatementResult::Rows { result } => {
                assert_eq!(result.columns[0].name, "id");
                assert_eq!(result.rows[0][0], Value::Text("a:1".into()));
            }
            _ => panic!("rows expected"),
        }
        let aff = statement_to_result("OK", &json!([{"id": "a:1"}, {"id": "a:2"}]), "CREATE a", 10).unwrap();
        assert!(matches!(aff, StatementResult::Affected { rows_affected: 2 }));
        let def = statement_to_result("OK", &Json::Null, "DEFINE TABLE a", 10).unwrap();
        assert!(matches!(def, StatementResult::Affected { rows_affected: 0 }));
        assert!(statement_to_result("ERR", &json!("Parse error"), "SELEC", 10).is_err());
        let scalar = statement_to_result("OK", &json!(3), "RETURN 3", 10).unwrap();
        assert!(matches!(scalar, StatementResult::Rows { .. }));
    }

    #[test]
    fn columns_union_declared_and_sampled() {
        let cols = union_columns(&["name".into(), "tags[*]".into()], &[json!({"id": "t:1", "age": 3}), json!({"id": "t:2", "extra": true})]);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "name", "age", "extra"]);
        assert!(cols[0].primary_key);
        let info = json!({"tables": {"b": "DEFINE TABLE b", "a": "DEFINE TABLE a"}});
        assert_eq!(info_keys(&info, "tables"), vec!["a", "b"]);
    }

    fn db_info() -> Json {
        json!({
            "accesses": {"account": "DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user) DURATION FOR SESSION 1h"},
            "analyzers": {"ascii": "DEFINE ANALYZER ascii TOKENIZERS class"},
            "functions": {"greet": "DEFINE FUNCTION fn::greet($name: string) { RETURN 'hi ' + $name; }"},
            "params": {"limit": "DEFINE PARAM $limit VALUE 50"},
            "tables": {
                "person": "DEFINE TABLE person TYPE NORMAL SCHEMAFULL PERMISSIONS NONE",
                "wrote": "DEFINE TABLE wrote TYPE RELATION IN person OUT post SCHEMALESS"
            },
            "users": {"app": "DEFINE USER app ON DATABASE PASSHASH '...' ROLES EDITOR DURATION FOR SESSION NONE"}
        })
    }

    fn table_info() -> Json {
        json!({
            "events": {"on_update": "DEFINE EVENT on_update ON person WHEN $event = 'UPDATE' THEN (UPDATE audit SET at = time::now())"},
            "fields": {"email": "DEFINE FIELD email ON person TYPE string", "name": "DEFINE FIELD name ON person TYPE string"},
            "indexes": {"email_idx": "DEFINE INDEX email_idx ON person FIELDS email UNIQUE", "name_idx": "DEFINE INDEX name_idx ON person FIELDS name"},
            "lives": {}
        })
    }

    #[test]
    fn info_sections_become_summaries() {
        let db = db_info();
        let entries = info_entries(&db, "tables");
        assert_eq!(entries.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), vec!["person", "wrote"]);
        let tables = summaries(ObjectKind::Table, &entries, Some("app"), table_badge);
        assert_eq!(tables[0].badge.as_deref(), Some("schemafull"));
        assert_eq!(tables[1].badge.as_deref(), Some("relation"));
        assert_eq!(tables[0].reference.parent.as_deref(), Some("app"));
        assert!(tables[0].detail.as_deref().is_some_and(|d| d.starts_with("DEFINE TABLE person")));

        let idx = summaries(ObjectKind::Index, &info_entries(&table_info(), "indexes"), Some("person"), index_badge);
        assert_eq!(idx[0].reference.name, "email_idx");
        assert_eq!(idx[0].badge.as_deref(), Some("unique"));
        assert_eq!(idx[0].detail.as_deref(), Some("email UNIQUE"));
        assert_eq!(idx[1].badge.as_deref(), Some("index"));
        let ev = summaries(ObjectKind::Event, &info_entries(&table_info(), "events"), Some("person"), |_| Some("event".into()));
        assert_eq!(ev[0].detail.as_deref(), Some("$event = 'UPDATE'"));
        let users = summaries(ObjectKind::User, &info_entries(&db, "users"), Some("database"), |_| Some("database".into()));
        assert_eq!(users[0].detail.as_deref(), Some("EDITOR"));
    }

    #[test]
    fn remove_statements_are_write_blocked() {
        assert_eq!(remove_statement(ObjectKind::Index, "email_idx", Some("person")).as_deref(), Some("REMOVE INDEX `email_idx` ON TABLE `person`"));
        assert_eq!(remove_statement(ObjectKind::Event, "on_update", Some("person")).as_deref(), Some("REMOVE EVENT `on_update` ON TABLE `person`"));
        assert_eq!(remove_statement(ObjectKind::Function, "greet", None).as_deref(), Some("REMOVE FUNCTION fn::greet"));
        assert_eq!(remove_statement(ObjectKind::Setting, "limit", None).as_deref(), Some("REMOVE PARAM $limit"));
        assert_eq!(remove_statement(ObjectKind::User, "root", Some("root")).as_deref(), Some("REMOVE USER `root` ON ROOT"));
        assert_eq!(remove_statement(ObjectKind::Role, "account", None).as_deref(), Some("REMOVE ACCESS `account` ON DATABASE"));
        assert_eq!(remove_statement(ObjectKind::Namespace, "test", None).as_deref(), Some("REMOVE NAMESPACE `test`"));
        assert!(remove_statement(ObjectKind::Index, "x", None).is_none());
        for kind in [ObjectKind::Index, ObjectKind::Table, ObjectKind::Function, ObjectKind::Role] {
            let stmt = remove_statement(kind, "x", Some("t")).unwrap_or_default();
            assert!(is_write_statement(&stmt), "{stmt}");
        }
    }

    #[test]
    fn details_carry_definition_and_actions() {
        let r = ObjectRef { kind: ObjectKind::Index, name: "email_idx".into(), parent: Some("person".into()) };
        let d = definition_detail(&r, "DEFINE INDEX email_idx ON person FIELDS email UNIQUE");
        assert_eq!(d.language, CodeLanguage::Sql);
        assert!(d.properties.iter().any(|p| p.name == "fields" && p.value == "email UNIQUE"));
        assert!(d.actions[0].destructive && d.actions[0].statement.contains("REMOVE INDEX"));

        let tr = ObjectRef { kind: ObjectKind::Table, name: "person".into(), parent: Some("app".into()) };
        let td = table_detail(&tr, "DEFINE TABLE person TYPE NORMAL SCHEMAFULL", &table_info(), vec![], Some(1234));
        assert!(td.properties.iter().any(|p| p.name == "records" && p.value == "1,234"));
        assert!(td.properties.iter().any(|p| p.name == "fields" && p.value == "2"));
        assert_eq!(td.rows.as_ref().map(|r| r.rows.len()), Some(2));
        let kids: Vec<(ObjectKind, &str)> = td.children.iter().map(|c| (c.reference.kind, c.reference.name.as_str())).collect();
        assert_eq!(kids, vec![(ObjectKind::Index, "email_idx"), (ObjectKind::Index, "name_idx"), (ObjectKind::Event, "on_update")]);
        assert_eq!(td.actions.len(), 4);
        assert_eq!(td.actions[3].statement, "DELETE `person`");
        assert!(td.actions[3].destructive && !td.actions[1].destructive);
        assert!(!is_write_statement(&td.actions[1].statement));

        assert_eq!(clause_after("DEFINE USER a ON ROOT PASSHASH 'x' ROLES OWNER DURATION FOR SESSION NONE", "ROLES").as_deref(), Some("OWNER"));
        assert_eq!(clause_after("DEFINE PARAM $limit VALUE 50", "VALUE").as_deref(), Some("50"));
        assert!(clause_after("DEFINE TABLE person", "ROLES").is_none());
        assert!(has_word("DEFINE TABLE t TYPE RELATION", "RELATION"));
        assert!(!has_word("DEFINE TABLE relationships", "RELATION"));
    }

    #[test]
    fn stats_groups_count_the_catalog() {
        let root = json!({
            "namespaces": {"test": "DEFINE NAMESPACE test", "other": "DEFINE NAMESPACE other"},
            "nodes": {"n1": "NODE n1"},
            "users": {"root": "DEFINE USER root ON ROOT ROLES OWNER"},
            "system": {"available_parallelism": 8, "cpu_usage": 12.5, "memory_usage": 268435456.0, "physical_cores": 4, "threads": 16}
        });
        let ns = json!({"databases": {"app": "DEFINE DATABASE app"}, "users": {}});
        let groups = stat_groups(Some("SurrealDB 2.1.0"), "test", "app", Some(&root), Some(&ns), Some(&db_info()), Some(99));
        assert_eq!(groups.iter().map(|g| g.title.as_str()).collect::<Vec<_>>(), vec!["Server", "Catalog", "Security"]);
        let server = &groups[0].stats;
        assert_eq!(server[0].value, "SurrealDB 2.1.0");
        assert!(server.iter().any(|s| s.label == "Namespaces" && s.numeric == Some(2.0)));
        assert!(server.iter().any(|s| s.label == "Cluster nodes" && s.numeric == Some(1.0)));
        assert!(server.iter().any(|s| s.label == "Memory usage" && s.value == "256.0 MB"));
        assert!(server.iter().any(|s| s.label == "CPU usage" && s.unit.as_deref() == Some("%")));
        let catalog = &groups[1].stats;
        assert_eq!(catalog[0].numeric, Some(1.0));
        assert!(catalog.iter().any(|s| s.label == "Tables" && s.numeric == Some(2.0)));
        assert!(catalog.iter().any(|s| s.label == "Records" && s.hint.as_deref() == Some("all tables")));
        assert_eq!(groups[2].stats[0].numeric, Some(1.0));
        assert_eq!(groups[2].stats[2].numeric, Some(1.0));
        assert_eq!(stat_groups(None, "test", "app", None, None, None, None).len(), 2);
        assert_eq!(bytes_text(1536.0), "1.5 KB");
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        use crate::model::{ConnectionSummary, Environment};
        let Ok(url) = std::env::var("DBFREE_TEST_SURREALDB_URL") else { return };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Surrealdb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: std::env::var("DBFREE_TEST_SURREALDB_DB").ok(),
                username: std::env::var("DBFREE_TEST_SURREALDB_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_SURREALDB_PASSWORD").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.to_lowercase().contains("surreal"), "{version}");
        db.execute("CREATE dbfree_smoke:one SET n = 1; CREATE dbfree_smoke:two SET n = 2", 10).await.unwrap_or_else(|e| panic!("create: {e}"));
        let cat = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == "dbfree_smoke"));
        let t = TableRef { schema: None, name: "dbfree_smoke".into() };
        let cols = db.columns(&t).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "n"));
        let page = db
            .fetch_page(&t, &PageQuery { sort: vec![SortRule { column: "n".into(), desc: true }], filters: vec![rule("n", FilterOp::Gte, "1")], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(db.count(&t, &[rule("n", FilterOp::Eq, "2")]).await.unwrap_or_default(), 1);
        db.execute("REMOVE TABLE dbfree_smoke", 10).await.unwrap_or_else(|e| panic!("remove: {e}"));
    }
}
