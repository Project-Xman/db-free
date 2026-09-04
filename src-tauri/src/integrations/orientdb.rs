// SOT: orientdb-integration, orientdb-rest-api, orient-sql, orient-gremlin, orientdb-object-explorer, orientdb-server-stats, orient-class-ddl

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, json_to_value, json_type_name, objects_to_result_set, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectProperty, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    ServerStats, SortRule, SslMode, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  OrientDB adapter over the REST API (port 2480).
// WHY:   OrientDB classes carry declared properties but records may hold extra
//        fields, so columns = `@rid` (pk) + `@class` + declared properties ∪
//        keys sampled from the first 50 records.
// HOW:   Basic auth (root/secret); `database` defaults to `demodb`. Catalog:
//        GET /database/{db} → classes (system classes and `_`-prefixed ones
//        skipped) with `records` as the row estimate. Paging / counting use
//        OrientDB SQL through POST /command/{db}/sql with backtick-quoted
//        identifiers and escaped string literals (`SKIP n LIMIT m`). `execute`
//        runs SQL (or Gremlin when the text starts with `g.`) via the command
//        endpoint; result arrays become grids, `{count: n}` becomes Affected.
//        INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/TRUNCATE are refused read-only.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs (trait)
// ============================================================================

const DEFAULT_PORT: u16 = 2480;
const DEFAULT_DATABASE: &str = "demodb";
const SAMPLE_SIZE: u32 = 50;
const MAX_PAGE_ROWS: u32 = 5_000;
const SYSTEM_CLASSES: [&str; 9] = ["OIdentity", "ORole", "OUser", "OFunction", "OSchedule", "OSequence", "OTriggered", "ORestricted", "OSecurityPolicy"];
// REBUILD and REVOKE are here because the object explorer offers them as
// actions; they change the database, so a read-only session must refuse them.
const WRITE_KEYWORDS: [&str; 11] = ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE", "MOVE", "GRANT", "REBUILD", "REVOKE"];

pub struct OrientIntegration {
    engine: Engine,
    http: HttpClient,
    database: String,
    read_only: bool,
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
    let http = HttpClient::new(base, HttpClient::auth_from_connection(conn), insecure)?;
    let integration = OrientIntegration { engine: s.engine, http, database, read_only: s.read_only };
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

// WHAT:  Backtick-quoted identifier. `@rid` / `@class` are metadata fields and stay bare.
fn ident(raw: &str) -> String {
    if raw.starts_with('@') && raw[1..].chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return raw.to_string();
    }
    format!("`{}`", raw.replace('`', ""))
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
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn is_rid(raw: &str) -> bool {
    let Some(rest) = raw.strip_prefix('#') else { return false };
    match rest.split_once(':') {
        Some((a, b)) => !a.is_empty() && !b.is_empty() && a.chars().all(|c| c.is_ascii_digit() || c == '-') && b.chars().all(|c| c.is_ascii_digit()),
        None => false,
    }
}

fn literal(column: &str, raw: &str) -> String {
    let t = raw.trim();
    if column == "@rid" && is_rid(t) {
        return t.to_string();
    }
    if t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        return t.to_ascii_lowercase();
    }
    if t.eq_ignore_ascii_case("null") {
        return "null".into();
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
                FilterOp::Ne => format!("{col} <> {}", literal(&f.column, v)),
                FilterOp::Gt => format!("{col} > {}", literal(&f.column, v)),
                FilterOp::Gte => format!("{col} >= {}", literal(&f.column, v)),
                FilterOp::Lt => format!("{col} < {}", literal(&f.column, v)),
                FilterOp::Lte => format!("{col} <= {}", literal(&f.column, v)),
                FilterOp::Contains => format!("{col}.toString().toLowerCase() LIKE {}", quote_str(&format!("%{}%", v.to_lowercase()))),
                FilterOp::StartsWith => format!("{col}.toString().toLowerCase() LIKE {}", quote_str(&format!("{}%", v.to_lowercase()))),
                FilterOp::EndsWith => format!("{col}.toString().toLowerCase() LIKE {}", quote_str(&format!("%{}", v.to_lowercase()))),
                FilterOp::In => {
                    let items: Vec<String> = v.split(',').map(str::trim).filter(|x| !x.is_empty()).map(|x| literal(&f.column, x)).collect();
                    if items.is_empty() {
                        "false".into()
                    } else {
                        format!("{col} IN [{}]", items.join(", "))
                    }
                }
                FilterOp::IsNull => format!("{col} IS NULL"),
                FilterOp::IsNotNull => format!("{col} IS NOT NULL"),
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

fn first_word(text: &str) -> String {
    text.trim_start().split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("").to_ascii_uppercase()
}

fn is_write_sql(text: &str) -> bool {
    WRITE_KEYWORDS.contains(&first_word(text).as_str())
}

fn is_gremlin(text: &str) -> bool {
    text.trim_start().starts_with("g.")
}

fn is_system_class(name: &str) -> bool {
    name.starts_with('_') || SYSTEM_CLASSES.contains(&name)
}

// WHAT:  Strips OrientDB's per-record metadata (`@type`, `@version`, `@fieldTypes`)
//        so the grid shows only `@rid`, `@class` and the user fields.
fn clean_record(rec: &Json) -> Json {
    match rec.as_object() {
        Some(o) => Json::Object(o.iter().filter(|(k, _)| !matches!(k.as_str(), "@type" | "@version" | "@fieldTypes")).map(|(k, v)| (k.clone(), v.clone())).collect()),
        None => rec.clone(),
    }
}

// WHAT:  Declared properties ∪ sampled keys; `@rid` first (pk), `@class` second.
fn union_columns(declared: &[(String, String)], docs: &[Json]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = vec!["@rid".into(), "@class".into()];
    let mut types: Vec<Option<String>> = vec![Some("rid".into()), Some("string".into())];
    let mut push = |name: &str, ty: Option<String>| {
        let idx = match names.iter().position(|n| n == name) {
            Some(i) => i,
            None => {
                names.push(name.to_string());
                types.push(None);
                names.len() - 1
            }
        };
        if types[idx].is_none() && ty.is_some() {
            types[idx] = ty;
        }
    };
    for (name, ty) in declared {
        push(name, Some(ty.clone()));
    }
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for (k, v) in obj {
                if matches!(k.as_str(), "@type" | "@version" | "@fieldTypes") {
                    continue;
                }
                push(k, (!v.is_null()).then(|| json_type_name(v).to_string()));
            }
        }
    }
    names
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, ty))| ColumnInfo {
            primary_key: name == "@rid",
            nullable: name != "@rid",
            data_type: ty.unwrap_or_else(|| "null".into()),
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
                if matches!(k.as_str(), "@type" | "@version" | "@fieldTypes") {
                    continue;
                }
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

// WHAT:  A command response → StatementResult. `{result:[{count:n}]}` = Affected,
//        object arrays = grid (@rid pinned), scalars = one `value` column.
fn command_to_result(body: &Json, max_rows: usize) -> StatementResult {
    let items: Vec<Json> = body.get("result").and_then(Json::as_array).cloned().unwrap_or_else(|| match body {
        Json::Array(a) => a.clone(),
        other => vec![other.clone()],
    });
    if items.len() == 1 {
        if let Some(obj) = items[0].as_object() {
            if obj.len() <= 3 && obj.get("count").is_some() && obj.keys().all(|k| k == "count" || k.starts_with('@')) {
                let n = obj.get("count").and_then(Json::as_u64).unwrap_or(0);
                return StatementResult::Affected { rows_affected: n };
            }
        }
    }
    if items.is_empty() {
        return StatementResult::Rows { result: ResultSet { columns: vec![], rows: vec![], truncated: false } };
    }
    if items.iter().all(Json::is_object) {
        let cleaned: Vec<Json> = items.iter().map(clean_record).collect();
        let id = cleaned.iter().any(|d| d.get("@rid").is_some()).then_some("@rid");
        return StatementResult::Rows { result: objects_to_result_set(&cleaned, id, max_rows) };
    }
    let truncated = items.len() > max_rows;
    let type_name = items.iter().find(|v| !v.is_null()).map(json_type_name).unwrap_or("json").to_string();
    StatementResult::Rows {
        result: ResultSet {
            columns: vec![ColumnMeta { name: "value".into(), type_name }],
            rows: items.iter().take(max_rows).map(|v| vec![json_to_value(v)]).collect(),
            truncated,
        },
    }
}

impl OrientIntegration {
    async fn command(&self, language: &str, text: &str) -> AppResult<Json> {
        let path = format!("/command/{}/{language}", pct(&self.database));
        self.http.post_json(&path, &json!({ "command": text })).await
    }

    async fn sql_rows(&self, sql: &str) -> AppResult<Vec<Json>> {
        let body = self.command("sql", sql).await?;
        Ok(body.get("result").and_then(Json::as_array).cloned().unwrap_or_default())
    }

    async fn database_info(&self) -> AppResult<Json> {
        self.http.get_json(&format!("/database/{}", pct(&self.database))).await
    }

    fn class_entries(info: &Json) -> Vec<Json> {
        info.get("classes").and_then(Json::as_array).cloned().unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Classes, indexes, functions, users, roles and server connections, from
//        GET /database/{db} (the whole schema in one response), GET /server
//        (connections + properties) and OrientDB SQL over the metadata classes
//        (`metadata:indexmanager`, `OFunction`, `OUser`, `ORole`).
// WHY:   One catalog request answers most of the sidebar; the SQL fallbacks
//        cover what the REST document leaves out (index definitions, code).
// HOW:   Actions are OrientDB SQL, which `is_write_sql` already blocks on a
//        read-only connection (REBUILD and REVOKE were added to that list for
//        the actions offered here).
// ---------------------------------------------------------------------------

const LIST_CAP: usize = 2_000;

fn scalar_text(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        Json::Array(items) => items.iter().map(scalar_text).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

fn jstr(row: &Json, key: &str) -> Option<String> {
    row.get(key).filter(|v| !v.is_null()).map(scalar_text).filter(|s| !s.is_empty())
}

fn jint(row: &Json, key: &str) -> Option<i64> {
    row.get(key).and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)).or_else(|| v.as_str().and_then(|s| s.parse().ok())))
}

fn jflag(row: &Json, key: &str) -> bool {
    row.get(key).and_then(Json::as_bool).unwrap_or(false)
}

fn jnames(row: &Json, key: &str) -> Vec<String> {
    match row.get(key) {
        Some(Json::Array(items)) => items
            .iter()
            .map(|i| match i {
                Json::String(s) => s.clone(),
                other => jstr(other, "name").unwrap_or_else(|| other.to_string()),
            })
            .collect(),
        Some(Json::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

fn preview(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        flat
    } else {
        format!("{}…", flat.chars().take(max).collect::<String>())
    }
}

fn finish(mut out: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    out.truncate(LIST_CAP);
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

fn props_of(row: &Json, skip: &[&str]) -> Vec<ObjectProperty> {
    let Some(obj) = row.as_object() else { return Vec::new() };
    let mut keys: Vec<&String> = obj.keys().filter(|k| !skip.contains(&k.as_str()) && !k.starts_with('@')).collect();
    keys.sort();
    keys.into_iter()
        .filter_map(|k| {
            let v = obj.get(k)?;
            if v.is_null() {
                return None;
            }
            let text = match v {
                Json::Object(_) | Json::Array(_) => v.to_string(),
                other => scalar_text(other),
            };
            (!text.is_empty()).then(|| ObjectProperty { name: k.clone(), value: preview(&text, 400) })
        })
        .collect()
}

// ---- classes -------------------------------------------------------------------

fn class_properties(c: &Json) -> Vec<Json> {
    c.get("properties").and_then(Json::as_array).cloned().unwrap_or_default()
}

fn class_badge(c: &Json) -> Option<String> {
    jstr(c, "superClass")
        .or_else(|| jnames(c, "superClasses").first().cloned())
        .filter(|s| !s.is_empty())
        .or_else(|| jflag(c, "abstract").then(|| "abstract".to_string()))
}

fn class_summaries(classes: &[Json], include_system: bool) -> Vec<ObjectSummary> {
    finish(
        classes
            .iter()
            .filter_map(|c| {
                let name = jstr(c, "name")?;
                if !include_system && is_system_class(&name) {
                    return None;
                }
                let mut parts = Vec::new();
                if let Some(r) = jint(c, "records") {
                    parts.push(format!("{} records", format_number(r as f64)));
                }
                parts.push(format!("{} properties", class_properties(c).len()));
                let mut s = ObjectSummary::new(ObjectKind::Class, name, None).with_detail(parts.join(" · "));
                s.badge = class_badge(c);
                Some(s)
            })
            .collect(),
    )
}

fn property_type(p: &Json) -> String {
    let ty = jstr(p, "type").unwrap_or_else(|| "ANY".into()).to_uppercase();
    match jstr(p, "linkedClass").or_else(|| jstr(p, "linkedType")) {
        Some(linked) => format!("{ty} {linked}"),
        None => ty,
    }
}

fn property_ddl(class: &str, p: &Json) -> Option<String> {
    let name = jstr(p, "name")?;
    let mut attrs = Vec::new();
    for (key, label) in [("mandatory", "MANDATORY"), ("notNull", "NOTNULL"), ("readonly", "READONLY")] {
        if jflag(p, key) {
            attrs.push(format!("{label} TRUE"));
        }
    }
    for (key, label) in [("min", "MIN"), ("max", "MAX"), ("regexp", "REGEXP"), ("collate", "COLLATE"), ("defaultValue", "DEFAULT")] {
        if let Some(v) = jstr(p, key) {
            attrs.push(format!("{label} {v}"));
        }
    }
    let tail = if attrs.is_empty() { String::new() } else { format!(" ({})", attrs.join(", ")) };
    Some(format!("CREATE PROPERTY {class}.{name} {}{tail}", property_type(p)))
}

// WHAT:  The class rebuilt as DDL: OrientDB's REST catalog reports the schema
//        as JSON only, so the definition pane shows the statements that made it.
fn class_ddl(c: &Json) -> String {
    let Some(name) = jstr(c, "name") else { return String::new() };
    let mut head = format!("CREATE CLASS {name}");
    let supers = match jstr(c, "superClass") {
        Some(s) => vec![s],
        None => jnames(c, "superClasses"),
    };
    if !supers.is_empty() {
        head.push_str(&format!(" EXTENDS {}", supers.join(", ")));
    }
    if jflag(c, "abstract") {
        head.push_str(" ABSTRACT");
    }
    let mut lines = vec![format!("{head};")];
    if jflag(c, "strictmode") {
        lines.push(format!("ALTER CLASS {name} STRICTMODE TRUE;"));
    }
    let mut properties: Vec<Json> = class_properties(c);
    properties.sort_by_key(|p| jstr(p, "name").unwrap_or_default());
    lines.extend(properties.iter().filter_map(|p| property_ddl(&name, p)).map(|s| format!("{s};")));
    lines.join("\n")
}

fn class_columns(c: &Json) -> Vec<ColumnInfo> {
    let mut properties = class_properties(c);
    properties.sort_by_key(|p| jstr(p, "name").unwrap_or_default());
    std::iter::once(ColumnInfo { name: "@rid".into(), data_type: "rid".into(), nullable: false, primary_key: true, ordinal: 1 })
        .chain(properties.iter().enumerate().filter_map(|(i, p)| {
            Some(ColumnInfo {
                name: jstr(p, "name")?,
                data_type: property_type(p).to_lowercase(),
                nullable: !jflag(p, "notNull") && !jflag(p, "mandatory"),
                primary_key: false,
                ordinal: u32::try_from(i + 2).unwrap_or(u32::MAX),
            })
        }))
        .collect()
}

fn class_detail(reference: &ObjectRef, c: &Json, indexes: Vec<ObjectSummary>) -> ObjectDetail {
    let name = reference.name.as_str();
    let mut d = ObjectDetail::empty(reference).definition(class_ddl(c), CodeLanguage::Sql);
    if let Some(r) = jint(c, "records") {
        d = d.property("records", format_number(r as f64));
    }
    d = d.property("properties", class_properties(c).len().to_string());
    for (key, label) in [("superClass", "super class"), ("alias", "alias"), ("clusterSelection", "cluster selection"), ("defaultCluster", "default cluster")] {
        if let Some(v) = jstr(c, key) {
            d = d.property(label, v);
        }
    }
    let clusters = jnames(c, "clusters");
    if !clusters.is_empty() {
        d = d.property("clusters", clusters.join(", "));
    }
    d = d.property("abstract", jflag(c, "abstract").to_string());
    d.columns = class_columns(c);
    let mut properties = class_properties(c);
    properties.sort_by_key(|p| jstr(p, "name").unwrap_or_default());
    d.rows = Some(ResultSet {
        columns: ["property", "type", "mandatory", "notNull"].iter().map(|n| ColumnMeta { name: (*n).into(), type_name: "string".into() }).collect(),
        rows: properties
            .iter()
            .map(|p| {
                vec![
                    Value::Text(jstr(p, "name").unwrap_or_default()),
                    Value::Text(property_type(p)),
                    Value::Bool(jflag(p, "mandatory")),
                    Value::Bool(jflag(p, "notNull")),
                ]
            })
            .collect(),
        truncated: false,
    });
    d.children = indexes;
    let id = ident(name);
    d.action(ObjectAction::new("sample", "Sample 20", format!("SELECT FROM {id} LIMIT 20")))
        .action(ObjectAction::new("count", "Count", format!("SELECT count(*) AS n FROM {id}")))
        .action(ObjectAction::destructive("truncate", "Truncate class", format!("TRUNCATE CLASS {id}")))
        .action(ObjectAction::destructive("drop", "Drop class", format!("DROP CLASS {id} UNSAFE")))
}

// ---- indexes ----------------------------------------------------------------------

// WHAT:  (class, fields) from an index document, covering the single-field
//        (`indexDefinition.field`) and composite (`indexDefinitions`) shapes.
fn index_target(idx: &Json) -> (Option<String>, Vec<String>) {
    let Some(def) = idx.get("indexDefinition") else {
        return (jstr(idx, "className"), jnames(idx, "fields"));
    };
    if let Some(parts) = def.get("indexDefinitions").and_then(Json::as_array) {
        let class = parts.iter().find_map(|p| jstr(p, "className"));
        let fields = parts.iter().filter_map(|p| jstr(p, "field")).collect();
        return (class, fields);
    }
    let fields = match jstr(def, "field") {
        Some(f) => vec![f],
        None => jnames(def, "fields"),
    };
    (jstr(def, "className"), fields)
}

fn index_summaries(indexes: &[Json], owner: Option<&str>) -> Vec<ObjectSummary> {
    finish(
        indexes
            .iter()
            .filter_map(|idx| {
                let name = jstr(idx, "name")?;
                let (class, fields) = index_target(idx);
                let class = class.or_else(|| name.split_once('.').map(|(c, _)| c.to_string()));
                if owner.is_some_and(|o| class.as_deref() != Some(o)) {
                    return None;
                }
                let mut detail = fields.join(", ");
                if let Some(size) = jint(idx, "size") {
                    detail = format!("{detail} · {} entries", format_number(size as f64));
                }
                let mut s = ObjectSummary { reference: ObjectRef { kind: ObjectKind::Index, name, parent: class }, detail: None, badge: jstr(idx, "type") };
                if !detail.trim().is_empty() {
                    s = s.with_detail(detail);
                }
                Some(s)
            })
            .collect(),
    )
}

fn index_detail(reference: &ObjectRef, idx: &Json) -> ObjectDetail {
    let (class, fields) = index_target(idx);
    let ty = jstr(idx, "type").unwrap_or_else(|| "NOTUNIQUE".into());
    let class = class.or_else(|| reference.parent.clone()).unwrap_or_default();
    let ddl = if class.is_empty() || fields.is_empty() {
        format!("-- {} {ty}", reference.name)
    } else {
        format!("CREATE INDEX {} ON {class} ({}) {ty}", reference.name, fields.join(", "))
    };
    let mut d = ObjectDetail::empty(reference).definition(ddl, CodeLanguage::Sql);
    d.properties = props_of(idx, &["name", "indexDefinition", "configuration"]);
    d = d.property("class", class).property("fields", fields.join(", "));
    d.rows = Some(ResultSet {
        columns: vec![ColumnMeta { name: "field".into(), type_name: "string".into() }],
        rows: fields.into_iter().map(|f| vec![Value::Text(f)]).collect(),
        truncated: false,
    });
    d.action(ObjectAction::destructive("rebuild", "Rebuild index", format!("REBUILD INDEX {}", reference.name)))
        .action(ObjectAction::destructive("drop", "Drop index", format!("DROP INDEX {}", reference.name)))
}

// ---- functions / users / roles / connections -------------------------------------------

fn function_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|f| {
                let name = jstr(f, "name")?;
                let params = jnames(f, "parameters");
                let mut s = ObjectSummary::new(ObjectKind::Function, name.clone(), None).with_detail(format!("{name}({})", params.join(", ")));
                s.badge = jstr(f, "language").map(|l| l.to_lowercase());
                Some(s)
            })
            .collect(),
    )
}

fn function_detail(reference: &ObjectRef, f: &Json) -> ObjectDetail {
    let language = jstr(f, "language").unwrap_or_else(|| "javascript".into());
    let mut d = ObjectDetail::empty(reference);
    if let Some(code) = jstr(f, "code") {
        d = d.definition(code, CodeLanguage::Text);
    }
    d = d.property("language", language).property("parameters", jnames(f, "parameters").join(", "));
    d = d.property("idempotent", jflag(f, "idempotent").to_string());
    d.action(ObjectAction::destructive("drop", "Delete function", format!("DELETE FROM OFunction WHERE name = {}", quote_str(&reference.name))))
}

fn user_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|u| {
                let name = jstr(u, "name")?;
                let roles = jnames(u, "roles");
                let mut s = ObjectSummary::new(ObjectKind::User, name, None);
                if !roles.is_empty() {
                    s = s.with_detail(roles.join(", "));
                }
                s.badge = jstr(u, "status").map(|s| s.to_lowercase());
                Some(s)
            })
            .collect(),
    )
}

fn user_detail(reference: &ObjectRef, u: &Json) -> ObjectDetail {
    let name = quote_str(&reference.name);
    let roles = jnames(u, "roles");
    let mut d = ObjectDetail::empty(reference)
        .property("status", jstr(u, "status").unwrap_or_else(|| "ACTIVE".into()))
        .property("roles", roles.join(", "));
    d.rows = Some(ResultSet {
        columns: vec![ColumnMeta { name: "role".into(), type_name: "string".into() }],
        rows: roles.into_iter().map(|r| vec![Value::Text(r)]).collect(),
        truncated: false,
    });
    d.action(ObjectAction::destructive("suspend", "Suspend user", format!("UPDATE OUser SET status = 'SUSPENDED' WHERE name = {name}")))
        .action(ObjectAction::new("activate", "Activate user", format!("UPDATE OUser SET status = 'ACTIVE' WHERE name = {name}")))
        .action(ObjectAction::destructive("drop", "Delete user", format!("DELETE FROM OUser WHERE name = {name}")))
}

fn role_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|r| {
                let name = jstr(r, "name")?;
                let rules = r.get("rules").and_then(Json::as_object).map(|m| m.len()).unwrap_or(0);
                let mut parts = Vec::new();
                if rules > 0 {
                    parts.push(format!("{rules} rule(s)"));
                }
                if let Some(inherited) = jstr(r, "inheritedRole") {
                    parts.push(format!("inherits {inherited}"));
                }
                let mut s = ObjectSummary::new(ObjectKind::Role, name, None);
                if !parts.is_empty() {
                    s = s.with_detail(parts.join(" · "));
                }
                s.badge = jstr(r, "mode").map(|m| m.to_lowercase());
                Some(s)
            })
            .collect(),
    )
}

fn role_detail(reference: &ObjectRef, r: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).property("mode", jstr(r, "mode").unwrap_or_default());
    if let Some(inherited) = jstr(r, "inheritedRole") {
        d = d.property("inherits", inherited);
    }
    if let Some(rules) = r.get("rules").and_then(Json::as_object) {
        let mut rows: Vec<(String, String)> = rules.iter().map(|(k, v)| (k.clone(), scalar_text(v))).collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let rows: Vec<Vec<Value>> = rows.into_iter().map(|(k, v)| vec![Value::Text(k), Value::Text(v)]).collect();
        d.rows = Some(ResultSet {
            columns: vec![ColumnMeta { name: "resource".into(), type_name: "string".into() }, ColumnMeta { name: "permission".into(), type_name: "string".into() }],
            rows,
            truncated: false,
        });
    }
    d.action(ObjectAction::destructive("drop", "Delete role", format!("DELETE FROM ORole WHERE name = {}", quote_str(&reference.name))))
}

// WHAT:  GET /server → `connections`, one row per open client session.
fn connection_summaries(server: &Json) -> Vec<ObjectSummary> {
    let items = server.get("connections").and_then(Json::as_array).cloned().unwrap_or_default();
    finish(
        items
            .iter()
            .filter_map(|c| {
                let id = jstr(c, "connectionId").or_else(|| jstr(c, "sessionId"))?;
                let mut parts = Vec::new();
                if let Some(u) = jstr(c, "user") {
                    parts.push(u);
                }
                if let Some(db) = jstr(c, "db") {
                    parts.push(db);
                }
                if let Some(addr) = jstr(c, "remoteAddress") {
                    parts.push(addr);
                }
                if let Some(cmd) = jstr(c, "commandInfo").or_else(|| jstr(c, "lastCommandInfo")) {
                    parts.push(preview(&cmd, 50));
                }
                let mut s = ObjectSummary::new(ObjectKind::Session, id, None);
                if !parts.is_empty() {
                    s = s.with_detail(parts.join(" · "));
                }
                s.badge = jstr(c, "protocol").map(|p| p.to_lowercase());
                Some(s)
            })
            .collect(),
    )
}

fn connection_detail(reference: &ObjectRef, c: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(cmd) = jstr(c, "lastCommandDetail").or_else(|| jstr(c, "commandDetail")) {
        d = d.definition(cmd, CodeLanguage::Sql);
    }
    d.properties = props_of(c, &["connectionId"]);
    // OrientDB has no SQL statement that closes another connection, and every
    // action must be runnable through `execute`, so this detail has none.
    d
}

// ---- stats -----------------------------------------------------------------------------

fn server_property(server: &Json, name: &str) -> Option<String> {
    server
        .get("properties")
        .and_then(Json::as_array)
        .and_then(|props| props.iter().find(|p| jstr(p, "name").as_deref() == Some(name)))
        .and_then(|p| jstr(p, "value"))
}

fn stat_groups(server: Option<&Json>, database: &str, info: &Json, allocation: Option<&Json>, indexes: usize) -> Vec<StatGroup> {
    let mut server_stats = Vec::new();
    if let Some(s) = server {
        let version = jstr(s, "version").or_else(|| server_property(s, "server.version")).unwrap_or_else(|| "OrientDB".into());
        server_stats.push(Stat::text("Version", version));
        for (key, label) in [("osName", "OS"), ("osVersion", "OS version"), ("javaVendor", "Java vendor"), ("javaVersion", "Java version")] {
            if let Some(v) = jstr(s, key) {
                server_stats.push(Stat::text(label, v));
            }
        }
        if let Some(conns) = s.get("connections").and_then(Json::as_array) {
            server_stats.push(Stat::number("Connections", conns.len() as f64, None));
        }
    }
    if let Some(user) = jstr(info, "currentUser") {
        server_stats.push(Stat::text("User", user));
    }
    server_stats.push(Stat::text("Database", database));
    let mut groups = vec![StatGroup { title: "Server".into(), stats: server_stats }];

    let classes = info.get("classes").and_then(Json::as_array).cloned().unwrap_or_default();
    let user_classes = classes.iter().filter(|c| jstr(c, "name").is_some_and(|n| !is_system_class(&n))).count();
    let records: i64 = classes.iter().filter_map(|c| jint(c, "records")).sum();
    groups.push(StatGroup {
        title: "Schema".into(),
        stats: vec![
            Stat::number("Classes", user_classes as f64, None).with_hint(format!("{} including system", classes.len())),
            Stat::number("Indexes", indexes as f64, None),
            Stat::number("Records", records as f64, None),
            Stat::number("Clusters", info.get("clusters").and_then(Json::as_array).map(Vec::len).unwrap_or(0) as f64, None),
        ],
    });

    if let Some(a) = allocation {
        let mut storage = Vec::new();
        if let Some(size) = a.get("size").and_then(Json::as_f64) {
            storage.push(Stat { label: "Allocated".into(), value: bytes_text(size), unit: None, hint: None, numeric: Some(size) });
        }
        if let Some(segments) = a.get("segments").and_then(Json::as_array) {
            let used: f64 = segments.iter().filter(|s| jstr(s, "type").as_deref() == Some("d")).filter_map(|s| s.get("size").and_then(Json::as_f64)).sum();
            let holes: f64 = segments.iter().filter(|s| jstr(s, "type").as_deref() == Some("h")).filter_map(|s| s.get("size").and_then(Json::as_f64)).sum();
            storage.push(Stat { label: "Data".into(), value: bytes_text(used), unit: None, hint: None, numeric: Some(used) });
            storage.push(Stat { label: "Holes".into(), value: bytes_text(holes), unit: None, hint: None, numeric: Some(holes) });
            storage.push(Stat::number("Segments", segments.len() as f64, None));
        }
        if !storage.is_empty() {
            groups.push(StatGroup { title: "Storage".into(), stats: storage });
        }
    }
    groups
}

impl OrientIntegration {
    async fn server_info(&self) -> Option<Json> {
        self.http.get_json::<Json>("/server").await.ok()
    }

    // WHAT:  Index documents from the index manager; falls back to the names the
    //        class entries carry when that metadata class is not readable.
    async fn index_docs(&self) -> Vec<Json> {
        if let Ok(rows) = self.sql_rows("SELECT expand(indexes) FROM metadata:indexmanager").await {
            if !rows.is_empty() {
                return rows;
            }
        }
        let Ok(info) = self.database_info().await else { return Vec::new() };
        Self::class_entries(&info)
            .iter()
            .flat_map(|c| {
                let class = jstr(c, "name").unwrap_or_default();
                jnames(c, "indexes").into_iter().map(move |name| json!({ "name": name, "className": class }))
            })
            .collect()
    }

    async fn metadata_rows(&self, sql: &str) -> Vec<Json> {
        self.sql_rows(sql).await.unwrap_or_default()
    }

    async fn explorer_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Database => {
                let names = self.databases().await?;
                Ok(finish(
                    names
                        .into_iter()
                        .map(|n| {
                            let mut s = ObjectSummary::new(ObjectKind::Database, n.clone(), None);
                            if n == self.database {
                                s = s.with_badge("current");
                            }
                            s
                        })
                        .collect(),
                ))
            }
            ObjectKind::Class => {
                let info = self.database_info().await?;
                Ok(class_summaries(&Self::class_entries(&info), false))
            }
            ObjectKind::Index => {
                let owner = parent.map(str::trim).filter(|p| !p.is_empty() && *p != self.database);
                Ok(index_summaries(&self.index_docs().await, owner))
            }
            ObjectKind::Function => Ok(function_summaries(&self.metadata_rows("SELECT name, code, language, parameters, idempotent FROM OFunction").await)),
            ObjectKind::User => Ok(user_summaries(&self.metadata_rows("SELECT name, status, roles.name AS roles FROM OUser").await)),
            ObjectKind::Role => Ok(role_summaries(&self.metadata_rows("SELECT name, mode, rules, inheritedRole.name AS inheritedRole FROM ORole").await)),
            ObjectKind::Session => Ok(self.server_info().await.map(|s| connection_summaries(&s)).unwrap_or_default()),
            _ => Ok(Vec::new()),
        }
    }

    async fn explorer_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let missing = || AppError::not_found(format!("`{name}` no longer exists in `{}`.", self.database));
        match reference.kind {
            ObjectKind::Database => {
                let info: Json = self.http.get_json(&format!("/database/{}", pct(name))).await?;
                let classes = Self::class_entries(&info);
                let mut d = ObjectDetail::empty(reference)
                    .property("classes", classes.iter().filter(|c| jstr(c, "name").is_some_and(|n| !is_system_class(&n))).count().to_string())
                    .property("clusters", info.get("clusters").and_then(Json::as_array).map(Vec::len).unwrap_or(0).to_string());
                if let Some(user) = jstr(&info, "currentUser") {
                    d = d.property("current user", user);
                }
                if let Some(server) = info.get("server") {
                    d.properties.extend(props_of(server, &[]));
                }
                d.children = class_summaries(&classes, false);
                Ok(d)
            }
            ObjectKind::Class => {
                let info = self.database_info().await?;
                let classes = Self::class_entries(&info);
                let c = classes.iter().find(|c| jstr(c, "name").as_deref() == Some(name)).ok_or_else(missing)?;
                let indexes = index_summaries(&self.index_docs().await, Some(name));
                Ok(class_detail(reference, c, indexes))
            }
            ObjectKind::Index => {
                let indexes = self.index_docs().await;
                let idx = indexes.iter().find(|i| jstr(i, "name").as_deref() == Some(name)).ok_or_else(missing)?;
                Ok(index_detail(reference, idx))
            }
            ObjectKind::Function => {
                let sql = format!("SELECT name, code, language, parameters, idempotent FROM OFunction WHERE name = {}", quote_str(name));
                let rows = self.sql_rows(&sql).await?;
                Ok(function_detail(reference, rows.first().ok_or_else(missing)?))
            }
            ObjectKind::User => {
                let sql = format!("SELECT name, status, roles.name AS roles FROM OUser WHERE name = {}", quote_str(name));
                let rows = self.sql_rows(&sql).await?;
                Ok(user_detail(reference, rows.first().ok_or_else(missing)?))
            }
            ObjectKind::Role => {
                let sql = format!("SELECT name, mode, rules, inheritedRole.name AS inheritedRole FROM ORole WHERE name = {}", quote_str(name));
                let rows = self.sql_rows(&sql).await?;
                Ok(role_detail(reference, rows.first().ok_or_else(missing)?))
            }
            ObjectKind::Session => {
                let server = self.server_info().await.ok_or_else(|| AppError::not_found("The server status is not available."))?;
                let conns = server.get("connections").and_then(Json::as_array).cloned().unwrap_or_default();
                let c = conns
                    .iter()
                    .find(|c| jstr(c, "connectionId").as_deref() == Some(name) || jstr(c, "sessionId").as_deref() == Some(name))
                    .ok_or_else(|| AppError::not_found(format!("Connection `{name}` is closed.")))?;
                Ok(connection_detail(reference, c))
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn explorer_stats(&self) -> AppResult<ServerStats> {
        let info = self.database_info().await?;
        let server = self.server_info().await;
        let allocation = self.http.get_json::<Json>(&format!("/allocation/{}", pct(&self.database))).await.ok();
        let indexes = self.index_docs().await.len();
        Ok(ServerStats::now(stat_groups(server.as_ref(), &self.database, &info, allocation.as_ref(), indexes)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { sql: true, namespaces: false, exact_estimate: true, ..Capabilities::DOCUMENT },
        object_kinds: vec![K::Database, K::Class, K::Index, K::Function, K::User, K::Role, K::Session],
        tools: vec![T::Stats, T::GraphView],
    }
}

#[async_trait]
impl Integration for OrientIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.database_info().await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v: Json = match self.http.get_json("/server").await {
            Ok(v) => v,
            Err(_) => return Ok(Some("OrientDB".into())),
        };
        let version = v
            .get("version")
            .and_then(Json::as_str)
            .map(str::to_string)
            .or_else(|| {
                v.get("properties")
                    .and_then(Json::as_array)
                    .and_then(|props| props.iter().find(|p| p.get("name").and_then(Json::as_str) == Some("server.version")))
                    .and_then(|p| p.get("value").and_then(Json::as_str))
                    .map(str::to_string)
            });
        Ok(Some(version.map(|s| format!("OrientDB {s}")).unwrap_or_else(|| "OrientDB".into())))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let v: Json = match self.http.get_json("/listDatabases").await {
            Ok(v) => v,
            Err(_) => return Ok(vec![self.database.clone()]),
        };
        let mut names: Vec<String> = v
            .get("databases")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        if names.is_empty() {
            names.push(self.database.clone());
        }
        names.sort();
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let info = self.database_info().await?;
        let mut tables: Vec<TableInfo> = Self::class_entries(&info)
            .iter()
            .filter_map(|c| {
                let name = c.get("name").and_then(Json::as_str)?;
                if is_system_class(name) {
                    return None;
                }
                let abstract_class = c.get("abstract").and_then(Json::as_bool).unwrap_or(false);
                Some(TableInfo {
                    schema: None,
                    name: name.to_string(),
                    kind: if abstract_class { TableKind::View } else { TableKind::Table },
                    row_estimate: c.get("records").and_then(Json::as_i64),
                })
            })
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let info = self.database_info().await?;
        let declared: Vec<(String, String)> = Self::class_entries(&info)
            .iter()
            .find(|c| c.get("name").and_then(Json::as_str) == Some(table.name.as_str()))
            .and_then(|c| c.get("properties").and_then(Json::as_array))
            .map(|props| {
                props
                    .iter()
                    .filter_map(|p| {
                        let name = p.get("name").and_then(Json::as_str)?;
                        let ty = p.get("type").and_then(Json::as_str).unwrap_or("ANY");
                        Some((name.to_string(), ty.to_ascii_lowercase()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let sample = self.sql_rows(&format!("SELECT FROM {} LIMIT {SAMPLE_SIZE}", ident(&table.name))).await?;
        Ok(union_columns(&declared, &sample))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let info = self.database_info().await?;
        Ok(Self::class_entries(&info)
            .iter()
            .find(|c| c.get("name").and_then(Json::as_str) == Some(table.name.as_str()))
            .and_then(|c| c.get("records").and_then(Json::as_i64)))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count(*) AS n FROM {}{}", ident(&table.name), where_clause(filters));
        let rows = self.sql_rows(&sql).await?;
        Ok(rows.first().and_then(|r| r.get("n")).and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let limit = query.limit.min(MAX_PAGE_ROWS);
        let sql = format!(
            "SELECT FROM {}{}{} SKIP {} LIMIT {limit}",
            ident(&table.name),
            where_clause(&query.filters),
            order_clause(&query.sort),
            query.offset
        );
        let rows = self.sql_rows(&sql).await?;
        let cleaned: Vec<Json> = rows.iter().map(clean_record).collect();
        Ok(docs_to_result_set(&cols, &cleaned, false))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let text = sql.trim();
        if text.is_empty() {
            return Err(AppError::invalid_input("Empty command."));
        }
        let language = if is_gremlin(text) { "gremlin" } else { "sql" };
        if self.read_only && language == "sql" && is_write_sql(text) {
            return Err(AppError::read_only(format!("This connection is read-only; `{}` is blocked.", first_word(text))));
        }
        if self.read_only && language == "gremlin" && (text.contains(".addV(") || text.contains(".addE(") || text.contains(".drop(") || text.contains(".property(")) {
            return Err(AppError::read_only("This connection is read-only; mutating Gremlin steps are blocked."));
        }
        let body = self.command(language, text).await?;
        if body.get("result").is_none() && !body.is_array() {
            return Ok(vec![StatementResult::Rows { result: json_result(body) }]);
        }
        Ok(vec![command_to_result(&body, max_rows.max(1))])
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
    fn where_and_order_render() {
        let w = where_clause(&[
            rule("age", FilterOp::Gte, "5"),
            rule("name", FilterOp::Contains, "O'B"),
            rule("tier", FilterOp::In, "gold, 2"),
            rule("@rid", FilterOp::Eq, "#12:0"),
            rule("note", FilterOp::IsNull, ""),
        ]);
        assert_eq!(
            w,
            " WHERE `age` >= 5 AND `name`.toString().toLowerCase() LIKE '%o\\'b%' AND `tier` IN ['gold', 2] AND @rid = #12:0 AND `note` IS NULL"
        );
        assert_eq!(order_clause(&[SortRule { column: "a".into(), desc: true }]), " ORDER BY `a` DESC");
        assert_eq!(literal("@rid", "not a rid"), "'not a rid'");
        assert_eq!(ident("we`ird"), "`weird`");
    }

    #[test]
    fn write_and_gremlin_detection() {
        assert!(is_write_sql("insert into V set a = 1"));
        assert!(is_write_sql(" DROP CLASS X"));
        assert!(!is_write_sql("SELECT FROM V"));
        assert!(is_gremlin("g.V().limit(5)"));
        assert!(!is_gremlin("SELECT g.V FROM x"));
        assert!(is_system_class("OUser"));
        assert!(is_system_class("_studio"));
        assert!(!is_system_class("Person"));
    }

    #[test]
    fn command_responses_map() {
        let rows = command_to_result(&json!({"result": [{"@rid": "#1:0", "@type": "d", "@version": 1, "name": "a"}]}), 10);
        match rows {
            StatementResult::Rows { result } => {
                let names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
                assert_eq!(names, vec!["@rid", "name"]);
            }
            _ => panic!("rows"),
        }
        let aff = command_to_result(&json!({"result": [{"@type": "d", "count": 3, "@version": 0}]}), 10);
        assert!(matches!(aff, StatementResult::Affected { rows_affected: 3 }));
        let scalar = command_to_result(&json!({"result": [1, 2]}), 10);
        assert!(matches!(scalar, StatementResult::Rows { .. }));
    }

    #[test]
    fn columns_union_declared_and_sampled() {
        let cols = union_columns(&[("name".into(), "string".into())], &[json!({"@rid": "#1:0", "@class": "P", "@version": 2, "age": 3})]);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["@rid", "@class", "name", "age"]);
        assert!(cols[0].primary_key);
        assert_eq!(cols[3].data_type, "integer");
    }

    fn person_class() -> Json {
        json!({
            "name": "Person",
            "superClass": "V",
            "superClasses": ["V"],
            "abstract": false,
            "strictmode": false,
            "records": 1234,
            "clusters": [12, 13],
            "defaultCluster": 12,
            "clusterSelection": "round-robin",
            "indexes": ["Person.name"],
            "properties": [
                {"name": "name", "type": "STRING", "mandatory": true, "notNull": true},
                {"name": "friend", "type": "LINK", "linkedClass": "Person", "mandatory": false, "notNull": false},
                {"name": "age", "type": "INTEGER", "min": "0"}
            ]
        })
    }

    #[test]
    fn classes_map_to_summaries_and_ddl() {
        let classes = vec![person_class(), json!({"name": "OUser", "records": 3, "properties": []}), json!({"name": "Abstract", "abstract": true, "properties": []})];
        let s = class_summaries(&classes, false);
        assert_eq!(s.iter().map(|c| c.reference.name.as_str()).collect::<Vec<_>>(), vec!["Abstract", "Person"]);
        assert_eq!(s[1].detail.as_deref(), Some("1,234 records · 3 properties"));
        assert_eq!(s[1].badge.as_deref(), Some("V"));
        assert_eq!(s[0].badge.as_deref(), Some("abstract"));
        assert_eq!(class_summaries(&classes, true).len(), 3);

        let ddl = class_ddl(&person_class());
        assert_eq!(
            ddl,
            "CREATE CLASS Person EXTENDS V;\nCREATE PROPERTY Person.age INTEGER (MIN 0);\nCREATE PROPERTY Person.friend LINK Person;\nCREATE PROPERTY Person.name STRING (MANDATORY TRUE, NOTNULL TRUE);"
        );
        let cols = class_columns(&person_class());
        assert_eq!(cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["@rid", "age", "friend", "name"]);
        assert!(cols[0].primary_key);
        assert_eq!(cols[2].data_type, "link person");
        assert!(!cols[3].nullable);

        let r = ObjectRef { kind: ObjectKind::Class, name: "Person".into(), parent: None };
        let d = class_detail(&r, &person_class(), vec![]);
        assert_eq!(d.language, CodeLanguage::Sql);
        assert!(d.properties.iter().any(|p| p.name == "records" && p.value == "1,234"));
        assert!(d.properties.iter().any(|p| p.name == "clusters" && p.value == "12, 13"));
        assert_eq!(d.rows.as_ref().map(|r| r.rows.len()), Some(3));
        assert_eq!(d.actions.len(), 4);
        assert_eq!(d.actions[3].statement, "DROP CLASS `Person` UNSAFE");
        assert!(d.actions[2].destructive && d.actions[3].destructive && !d.actions[0].destructive);
        assert!(is_write_sql(&d.actions[2].statement) && is_write_sql(&d.actions[3].statement));
        assert!(!is_write_sql(&d.actions[0].statement));
    }

    #[test]
    fn indexes_map_from_both_shapes() {
        let single = json!({"name": "Person.name", "type": "UNIQUE", "size": 1234, "indexDefinition": {"className": "Person", "field": "name", "keyType": "STRING"}});
        let composite = json!({"name": "Person.full", "type": "NOTUNIQUE", "indexDefinition": {"indexDefinitions": [{"className": "Person", "field": "first"}, {"className": "Person", "field": "last"}]}});
        let bare = json!({"name": "Post.title", "className": "Post"});
        let all = vec![single.clone(), composite.clone(), bare];
        let s = index_summaries(&all, None);
        assert_eq!(s.len(), 3);
        assert_eq!(s[0].detail.as_deref(), Some("first, last"));
        assert_eq!(s[1].badge.as_deref(), Some("UNIQUE"));
        assert_eq!(s[1].detail.as_deref(), Some("name · 1,234 entries"));
        assert_eq!(s[1].reference.parent.as_deref(), Some("Person"));
        assert_eq!(index_summaries(&all, Some("Person")).len(), 2);
        assert_eq!(index_summaries(&all, Some("Post"))[0].reference.name, "Post.title");

        let d = index_detail(&s[1].reference, &single);
        assert_eq!(d.definition.as_deref(), Some("CREATE INDEX Person.name ON Person (name) UNIQUE"));
        assert_eq!(d.actions.len(), 2);
        assert!(d.actions.iter().all(|a| a.destructive));
        assert!(is_write_sql("REBUILD INDEX Person.name") && is_write_sql("DROP INDEX Person.name"));
        let cd = index_detail(&s[0].reference, &composite);
        assert!(cd.definition.as_deref().is_some_and(|t| t.contains("(first, last)")));
    }

    #[test]
    fn functions_users_roles_connections_map() {
        let f = json!({"name": "sum", "code": "return a + b;", "language": "javascript", "parameters": ["a", "b"], "idempotent": true});
        let fs = function_summaries(std::slice::from_ref(&f));
        assert_eq!(fs[0].detail.as_deref(), Some("sum(a, b)"));
        assert_eq!(fs[0].badge.as_deref(), Some("javascript"));
        let d = function_detail(&fs[0].reference, &f);
        assert_eq!(d.definition.as_deref(), Some("return a + b;"));
        assert_eq!(d.actions[0].statement, "DELETE FROM OFunction WHERE name = 'sum'");
        assert!(is_write_sql(&d.actions[0].statement));

        let u = json!({"name": "admin", "status": "ACTIVE", "roles": ["admin", "reader"]});
        let us = user_summaries(std::slice::from_ref(&u));
        assert_eq!(us[0].detail.as_deref(), Some("admin, reader"));
        assert_eq!(us[0].badge.as_deref(), Some("active"));
        let d = user_detail(&us[0].reference, &u);
        assert_eq!(d.rows.as_ref().map(|r| r.rows.len()), Some(2));
        assert_eq!(d.actions.len(), 3);
        assert!(d.actions[0].destructive && !d.actions[1].destructive);
        assert!(d.actions.iter().all(|a| is_write_sql(&a.statement)));

        let r = json!({"name": "reader", "mode": "DENY_ALL_BUT", "inheritedRole": "base", "rules": {"database.class.*": 2, "database": 2}});
        let rs = role_summaries(std::slice::from_ref(&r));
        assert_eq!(rs[0].detail.as_deref(), Some("2 rule(s) · inherits base"));
        assert_eq!(rs[0].badge.as_deref(), Some("deny_all_but"));
        let d = role_detail(&rs[0].reference, &r);
        assert_eq!(d.rows.as_ref().map(|r| r.rows[0][0].clone()), Some(Value::Text("database".into())));
        assert_eq!(d.actions[0].statement, "DELETE FROM ORole WHERE name = 'reader'");

        let server = json!({"connections": [{"connectionId": "12", "user": "admin", "db": "demodb", "remoteAddress": "127.0.0.1:5000", "protocol": "HTTP-DB", "commandInfo": "Load record"}]});
        let cs = connection_summaries(&server);
        assert_eq!(cs[0].reference.name, "12");
        assert_eq!(cs[0].detail.as_deref(), Some("admin · demodb · 127.0.0.1:5000 · Load record"));
        assert_eq!(cs[0].badge.as_deref(), Some("http-db"));
        let cd = connection_detail(&cs[0].reference, &json!({"connectionId": "12", "user": "admin"}));
        assert!(cd.actions.is_empty());
        assert!(cd.properties.iter().any(|p| p.name == "user"));
    }

    #[test]
    fn stats_groups_from_server_and_allocation() {
        let server = json!({
            "connections": [{"connectionId": "1"}, {"connectionId": "2"}],
            "properties": [{"name": "server.version", "value": "3.2.24"}],
            "osName": "Linux"
        });
        let info = json!({"currentUser": "admin", "classes": [person_class(), json!({"name": "OUser", "records": 3})], "clusters": [1, 2, 3]});
        let allocation = json!({"size": 2048, "segments": [{"type": "d", "offset": 0, "size": 1024}, {"type": "h", "offset": 1024, "size": 1024}]});
        let groups = stat_groups(Some(&server), "demodb", &info, Some(&allocation), 4);
        assert_eq!(groups.iter().map(|g| g.title.as_str()).collect::<Vec<_>>(), vec!["Server", "Schema", "Storage"]);
        assert_eq!(groups[0].stats[0].value, "3.2.24");
        assert!(groups[0].stats.iter().any(|s| s.label == "Connections" && s.numeric == Some(2.0)));
        assert!(groups[0].stats.iter().any(|s| s.label == "User" && s.value == "admin"));
        let schema = &groups[1].stats;
        assert_eq!(schema[0].numeric, Some(1.0));
        assert_eq!(schema[0].hint.as_deref(), Some("2 including system"));
        assert_eq!(schema[1].numeric, Some(4.0));
        assert_eq!(schema[2].value, "1,237");
        assert_eq!(schema[3].numeric, Some(3.0));
        assert_eq!(groups[2].stats[0].value, "2.0 KB");
        assert_eq!(groups[2].stats[1].value, "1.0 KB");
        assert_eq!(stat_groups(None, "demodb", &json!({"classes": []}), None, 0).len(), 2);
        assert_eq!(bytes_text(0.0), "0 B");
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        use crate::model::{ConnectionSummary, Environment};
        let Ok(url) = std::env::var("DBFREE_TEST_ORIENTDB_URL") else { return };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Orientdb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: std::env::var("DBFREE_TEST_ORIENTDB_DB").ok(),
                username: std::env::var("DBFREE_TEST_ORIENTDB_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_ORIENTDB_PASSWORD").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("OrientDB"), "{version}");
        db.execute("CREATE CLASS DbfreeSmoke", 10).await.unwrap_or_else(|e| panic!("create class: {e}"));
        db.execute("INSERT INTO DbfreeSmoke SET n = 1", 10).await.unwrap_or_else(|e| panic!("insert: {e}"));
        db.execute("INSERT INTO DbfreeSmoke SET n = 2", 10).await.unwrap_or_else(|e| panic!("insert: {e}"));
        let cat = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == "DbfreeSmoke"));
        let t = TableRef { schema: None, name: "DbfreeSmoke".into() };
        let cols = db.columns(&t).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "n"));
        let page = db
            .fetch_page(&t, &PageQuery { sort: vec![SortRule { column: "n".into(), desc: true }], filters: vec![rule("n", FilterOp::Gte, "1")], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(db.count(&t, &[rule("n", FilterOp::Eq, "2")]).await.unwrap_or_default(), 1);
        db.execute("DROP CLASS DbfreeSmoke UNSAFE", 10).await.unwrap_or_else(|e| panic!("drop: {e}"));
    }
}
