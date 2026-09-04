// SOT: neo4j-integration, memgraph-integration, neo4rs-adapter, cypher, bolt-value-decoding, graph-label-catalog, neo4j-object-explorer, neo4j-server-stats, memgraph-catalog-fallback

use crate::error::{AppError, AppResult};
use crate::integrations::http::objects_to_result_set;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectProperty, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    ServerStats, SortRule, SslMode, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use neo4rs::{BoltType, ConfigBuilder, Graph, Query, Row};
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::sync::Arc;

// ============================================================================
// NEO4J / MEMGRAPH ADAPTER
//
// WHAT:  Maps a property graph onto the engine-neutral `Integration`.
// WHY:   The grid wants tables; a graph has labels and relationship types.
//        Nodes of one label become a table (schema = database name), each
//        relationship type becomes a table in a second schema `relationships`.
// HOW:   columns     = sampled union of property keys, plus synthetic `_id`
//                      (id(n)) marked primary key (`_start`/`_end` for rels)
//        fetch_page  = MATCH (n:`Label`) WHERE … RETURN … ORDER BY … SKIP … LIMIT …
//                      with every filter value passed as a Bolt parameter
//        count       = MATCH … RETURN count(n)
//        execute     = raw Cypher; each returned column → one grid column,
//                      nodes / relationships / paths → JSON
//        `neo4rs` is the only vendor crate used, and only in this file.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI meta)
// ============================================================================

const SAMPLE_SIZE: usize = 50;
const RELATIONSHIPS_SCHEMA: &str = "relationships";
const ID_FIELD: &str = "_id";
const START_FIELD: &str = "_start";
const END_FIELD: &str = "_end";
const DEFAULT_DATABASE: &str = "neo4j";
const MUTATING_KEYWORDS: &[&str] = &["create", "merge", "delete", "set", "remove", "drop", "detach", "load", "alter", "grant", "revoke", "deny"];

pub struct Neo4jIntegration {
    graph: Graph,
    engine: Engine,
    database: String,
    read_only: bool,
}

fn map_error(err: neo4rs::Error) -> AppError {
    match err {
        neo4rs::Error::AuthenticationError(msg) => AppError::not_connected(format!("Authentication failed: {msg}")),
        neo4rs::Error::ConnectionError => AppError::not_connected("Could not reach the Bolt server."),
        neo4rs::Error::IOError { detail } => AppError::not_connected(format!("Bolt connection failed: {detail}")),
        neo4rs::Error::Neo4j(e) => {
            let is_security = matches!(e.kind(), neo4rs::Neo4jErrorKind::Client(neo4rs::Neo4jClientErrorKind::Security(_)));
            let text = format!("{} ({})", e.message(), e.code());
            if is_security {
                AppError::not_connected(text)
            } else {
                AppError::driver(text)
            }
        }
        other => AppError::driver(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

// WHAT:  Bolt URI from host/port/ssl. `Require` uses the self-signed scheme
//        (+ssc), `VerifyCa`/`VerifyFull` the verified one (+s).
fn build_uri(conn: &ResolvedConnection) -> String {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("127.0.0.1");
    if host.contains("://") {
        return host.to_string();
    }
    let port = s.port.unwrap_or(7687);
    let scheme = match s.ssl_mode {
        SslMode::Disable | SslMode::Prefer => "bolt",
        SslMode::Require => "bolt+ssc",
        SslMode::VerifyCa | SslMode::VerifyFull => "bolt+s",
    };
    let host = if host.contains(':') && !host.starts_with('[') { format!("[{host}]") } else { host.to_string() };
    format!("{scheme}://{host}:{port}")
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let uri = build_uri(conn);
    let user = s.username.as_deref().map(str::trim).unwrap_or_default().to_string();
    let password = conn.secret.clone().unwrap_or_default();
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let mut builder = ConfigBuilder::default().uri(uri).user(user).password(password).fetch_size(500).max_connections(4);
    // Memgraph has a single database and ignores the Bolt `db` field; Neo4j 4+ routes on it.
    if s.engine != Engine::Memgraph {
        builder = builder.db(database.as_str());
    }
    let config = builder.build().map_err(map_error)?;
    let graph = Graph::connect(config).await.map_err(map_error)?;
    let integration = Neo4jIntegration { graph, engine: s.engine, database, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Bolt → model::Value
// ---------------------------------------------------------------------------

fn bolt_type_name(value: &BoltType) -> &'static str {
    match value {
        BoltType::String(_) => "string",
        BoltType::Boolean(_) => "boolean",
        BoltType::Map(_) => "map",
        BoltType::Null(_) => "null",
        BoltType::Integer(_) => "integer",
        BoltType::Float(_) => "float",
        BoltType::List(_) => "list",
        BoltType::Node(_) => "node",
        BoltType::Relation(_) | BoltType::UnboundedRelation(_) => "relationship",
        BoltType::Point2D(_) | BoltType::Point3D(_) => "point",
        BoltType::Bytes(_) => "bytes",
        BoltType::Path(_) => "path",
        BoltType::Duration(_) => "duration",
        BoltType::Date(_) => "date",
        BoltType::Time(_) | BoltType::LocalTime(_) => "time",
        BoltType::DateTime(_) | BoltType::LocalDateTime(_) | BoltType::DateTimeZoneId(_) => "datetime",
    }
}

fn map_to_json(map: &neo4rs::BoltMap) -> serde_json::Value {
    let mut out: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for (k, v) in &map.value {
        out.insert(k.value.clone(), bolt_to_json(v));
    }
    serde_json::Value::Object(out.into_iter().collect())
}

fn temporal_text(value: &BoltType) -> Option<String> {
    match value {
        BoltType::Date(d) => chrono::NaiveDate::try_from(d).ok().map(|d| d.to_string()),
        BoltType::Time(t) => {
            let (time, offset): (chrono::NaiveTime, chrono::FixedOffset) = t.into();
            Some(format!("{time}{offset}"))
        }
        BoltType::LocalTime(t) => {
            let time: chrono::NaiveTime = t.into();
            Some(time.to_string())
        }
        BoltType::DateTime(dt) => chrono::DateTime::<chrono::FixedOffset>::try_from(dt).ok().map(|d| d.to_rfc3339()),
        BoltType::LocalDateTime(dt) => chrono::NaiveDateTime::try_from(dt).ok().map(|d| d.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
        BoltType::DateTimeZoneId(dt) => chrono::DateTime::<chrono::FixedOffset>::try_from(dt).ok().map(|d| format!("{}[{}]", d.to_rfc3339(), dt.tz_id())),
        BoltType::Duration(d) => {
            let std: std::time::Duration = d.clone().into();
            Some(format!("PT{}S", std.as_secs_f64()))
        }
        _ => None,
    }
}

fn bolt_to_json(value: &BoltType) -> serde_json::Value {
    use serde_json::Value as J;
    match value {
        BoltType::String(s) => J::String(s.value.clone()),
        BoltType::Boolean(b) => J::Bool(b.value),
        BoltType::Map(m) => map_to_json(m),
        BoltType::Null(_) => J::Null,
        BoltType::Integer(i) => J::from(i.value),
        BoltType::Float(f) => serde_json::Number::from_f64(f.value).map(J::Number).unwrap_or(J::Null),
        BoltType::List(l) => J::Array(l.value.iter().map(bolt_to_json).collect()),
        BoltType::Bytes(b) => J::String(base64::engine::general_purpose::STANDARD.encode(&b.value)),
        BoltType::Node(n) => serde_json::json!({
            "id": n.id.value,
            "labels": n.labels.value.iter().map(bolt_to_json).collect::<Vec<_>>(),
            "properties": map_to_json(&n.properties),
        }),
        BoltType::Relation(r) => serde_json::json!({
            "id": r.id.value,
            "type": r.typ.value,
            "start": r.start_node_id.value,
            "end": r.end_node_id.value,
            "properties": map_to_json(&r.properties),
        }),
        BoltType::UnboundedRelation(r) => serde_json::json!({
            "id": r.id.value,
            "type": r.typ.value,
            "properties": map_to_json(&r.properties),
        }),
        BoltType::Path(p) => serde_json::json!({
            "nodes": p.nodes.value.iter().map(bolt_to_json).collect::<Vec<_>>(),
            "relationships": p.rels.value.iter().map(bolt_to_json).collect::<Vec<_>>(),
        }),
        BoltType::Point2D(p) => serde_json::json!({ "srid": p.sr_id.value, "x": p.x.value, "y": p.y.value }),
        BoltType::Point3D(p) => serde_json::json!({ "srid": p.sr_id.value, "x": p.x.value, "y": p.y.value, "z": p.z.value }),
        temporal => J::String(temporal_text(temporal).unwrap_or_default()),
    }
}

fn bolt_to_value(value: &BoltType) -> Value {
    match value {
        BoltType::Null(_) => Value::Null,
        BoltType::String(s) => Value::Text(s.value.clone()),
        BoltType::Boolean(b) => Value::Bool(b.value),
        BoltType::Integer(i) => Value::Int(i.value),
        BoltType::Float(f) => Value::Float(f.value),
        BoltType::Bytes(b) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(&b.value)),
        BoltType::DateTime(_) | BoltType::LocalDateTime(_) | BoltType::DateTimeZoneId(_) => {
            Value::DateTime(temporal_text(value).unwrap_or_default())
        }
        BoltType::Date(_) | BoltType::Time(_) | BoltType::LocalTime(_) | BoltType::Duration(_) => {
            Value::Text(temporal_text(value).unwrap_or_default())
        }
        BoltType::Map(_)
        | BoltType::List(_)
        | BoltType::Node(_)
        | BoltType::Relation(_)
        | BoltType::UnboundedRelation(_)
        | BoltType::Path(_)
        | BoltType::Point2D(_)
        | BoltType::Point3D(_) => Value::Json(bolt_to_json(value)),
    }
}

// WHAT:  A row's columns as (name, BoltType) pairs. neo4rs keeps them in a
//        HashMap, so order is restored from the query text (see `order_columns`).
fn row_entries(row: &Row) -> AppResult<Vec<(String, BoltType)>> {
    let map: neo4rs::BoltMap = row.to_strict::<neo4rs::BoltMap>().map_err(|e| AppError::driver(format!("Malformed Bolt row: {e}")))?;
    Ok(map.value.into_iter().map(|(k, v)| (k.value, v)).collect())
}

// WHAT:  Orders column names by where they appear after the last RETURN (or
//        YIELD) in the query; unknown names go last, alphabetically.
fn order_columns(mut names: Vec<String>, cypher: &str) -> Vec<String> {
    let lower = cypher.to_ascii_lowercase();
    let tail_start = lower.rfind("return").or_else(|| lower.rfind("yield")).unwrap_or(0);
    let tail = &lower[tail_start..];
    let position = |name: &str| -> Option<usize> {
        let needle = name.to_ascii_lowercase();
        let mut from = 0;
        while let Some(idx) = tail[from..].find(&needle) {
            let abs = from + idx;
            let before = tail[..abs].chars().next_back();
            let after = tail[abs + needle.len()..].chars().next();
            let boundary = |c: Option<char>| c.is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
            if boundary(before) && boundary(after) {
                return Some(abs);
            }
            from = abs + needle.len().max(1);
        }
        None
    };
    names.sort_by(|a, b| match (position(a), position(b)) {
        (Some(x), Some(y)) => x.cmp(&y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.cmp(b),
    });
    names
}

fn rows_to_result(rows: &[Vec<(String, BoltType)>], cypher: &str, truncated: bool) -> ResultSet {
    let mut names: Vec<String> = Vec::new();
    for row in rows {
        for (name, _) in row {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
    }
    let names = order_columns(names, cypher);
    let columns = names
        .iter()
        .map(|name| {
            let type_name = rows
                .iter()
                .find_map(|row| row.iter().find(|(n, v)| n == name && !matches!(v, BoltType::Null(_))).map(|(_, v)| bolt_type_name(v)))
                .unwrap_or("null")
                .to_string();
            ColumnMeta { name: name.clone(), type_name }
        })
        .collect();
    let rows = rows
        .iter()
        .map(|row| names.iter().map(|n| row.iter().find(|(k, _)| k == n).map(|(_, v)| bolt_to_value(v)).unwrap_or(Value::Null)).collect())
        .collect();
    ResultSet { columns, rows, truncated }
}

// ---------------------------------------------------------------------------
// Cypher building
// ---------------------------------------------------------------------------

fn backtick(raw: &str) -> String {
    format!("`{}`", raw.replace('`', "``"))
}

// WHAT:  Parses a filter value the way a person types it: int, float, bool, else text.
fn lenient_param(raw: &str) -> BoltType {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return BoltType::Boolean(neo4rs::BoltBoolean::new(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return BoltType::Boolean(neo4rs::BoltBoolean::new(false));
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return BoltType::Integer(neo4rs::BoltInteger::new(i));
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return BoltType::Float(neo4rs::BoltFloat::new(f));
    }
    BoltType::String(neo4rs::BoltString::new(trimmed))
}

fn id_param(raw: &str) -> BoltType {
    raw.trim()
        .parse::<i64>()
        .map(|i| BoltType::Integer(neo4rs::BoltInteger::new(i)))
        .unwrap_or_else(|_| BoltType::String(neo4rs::BoltString::new(raw.trim())))
}

// WHAT:  Column reference in Cypher: synthetic ids map to functions, everything else to `n.prop`.
fn column_expr(column: &str) -> String {
    match column {
        ID_FIELD => "id(n)".to_string(),
        START_FIELD => "id(startNode(n))".to_string(),
        END_FIELD => "id(endNode(n))".to_string(),
        other => format!("n.{}", backtick(other)),
    }
}

struct WhereClause {
    text: String,
    params: Vec<(String, BoltType)>,
}

fn where_clause(filters: &[FilterRule]) -> WhereClause {
    let mut parts = Vec::new();
    let mut params = Vec::new();
    for (i, rule) in filters.iter().enumerate() {
        let expr = column_expr(&rule.column);
        let key = format!("p{i}");
        let is_id = matches!(rule.column.as_str(), ID_FIELD | START_FIELD | END_FIELD);
        let scalar = |v: &str| if is_id { id_param(v) } else { lenient_param(v) };
        let text = rule.value.trim();
        let (clause, value) = match rule.op {
            FilterOp::Eq => (format!("{expr} = ${key}"), Some(scalar(text))),
            FilterOp::Ne => (format!("{expr} <> ${key}"), Some(scalar(text))),
            FilterOp::Gt => (format!("{expr} > ${key}"), Some(scalar(text))),
            FilterOp::Gte => (format!("{expr} >= ${key}"), Some(scalar(text))),
            FilterOp::Lt => (format!("{expr} < ${key}"), Some(scalar(text))),
            FilterOp::Lte => (format!("{expr} <= ${key}"), Some(scalar(text))),
            FilterOp::Contains => (format!("toLower(toString({expr})) CONTAINS toLower(${key})"), Some(BoltType::String(neo4rs::BoltString::new(text)))),
            FilterOp::StartsWith => (format!("toLower(toString({expr})) STARTS WITH toLower(${key})"), Some(BoltType::String(neo4rs::BoltString::new(text)))),
            FilterOp::EndsWith => (format!("toLower(toString({expr})) ENDS WITH toLower(${key})"), Some(BoltType::String(neo4rs::BoltString::new(text)))),
            FilterOp::In => {
                let items: Vec<BoltType> = text.split(',').map(str::trim).filter(|v| !v.is_empty()).map(scalar).collect();
                let mut list = neo4rs::BoltList::with_capacity(items.len());
                for item in items {
                    list.push(item);
                }
                (format!("{expr} IN ${key}"), Some(BoltType::List(list)))
            }
            FilterOp::IsNull => (format!("{expr} IS NULL"), None),
            FilterOp::IsNotNull => (format!("{expr} IS NOT NULL"), None),
        };
        parts.push(clause);
        if let Some(v) = value {
            params.push((key, v));
        }
    }
    let text = if parts.is_empty() { String::new() } else { format!(" WHERE {}", parts.join(" AND ")) };
    WhereClause { text, params }
}

fn order_by(sort: &[SortRule]) -> String {
    if sort.is_empty() {
        return " ORDER BY id(n)".to_string();
    }
    let parts: Vec<String> = sort.iter().map(|s| format!("{}{}", column_expr(&s.column), if s.desc { " DESC" } else { "" })).collect();
    format!(" ORDER BY {}", parts.join(", "))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Label,
    RelType,
}

fn target_for(table: &TableRef) -> Target {
    if table.schema.as_deref() == Some(RELATIONSHIPS_SCHEMA) {
        Target::RelType
    } else {
        Target::Label
    }
}

fn match_clause(table: &TableRef) -> String {
    match target_for(table) {
        Target::Label => format!("MATCH (n:{})", backtick(&table.name)),
        Target::RelType => format!("MATCH ()-[n:{}]->()", backtick(&table.name)),
    }
}

fn return_clause(table: &TableRef) -> &'static str {
    match target_for(table) {
        Target::Label => "RETURN id(n) AS _id, n AS _node",
        Target::RelType => "RETURN id(n) AS _id, id(startNode(n)) AS _start, id(endNode(n)) AS _end, n AS _node",
    }
}

// WHAT:  A read-only session refuses Cypher that starts a mutating clause, and
//        the administration commands the object explorer offers as actions
//        (terminate a transaction, stop / start a database).
fn looks_mutating(cypher: &str) -> bool {
    let head: String = cypher.split_whitespace().take(2).collect::<Vec<_>>().join(" ").to_ascii_lowercase();
    if ["terminate transaction", "stop database", "start database"].iter().any(|p| head.starts_with(p)) {
        return true;
    }
    cypher
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .any(|w| MUTATING_KEYWORDS.contains(&w.to_ascii_lowercase().as_str()))
}

fn has_return(cypher: &str) -> bool {
    let lower = cypher.to_ascii_lowercase();
    ["return", "yield", "show", "call", "explain", "profile", "unwind"].iter().any(|k| {
        lower.split(|c: char| !(c.is_alphanumeric() || c == '_')).any(|w| w == *k)
    })
}

// WHAT:  Splits console input into statements on `;` outside quotes/backticks.
pub fn split_statements(script: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = script.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                current.push(c);
                if c == '\\' && q != '`' {
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                } else if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    current.push(c);
                }
                '/' if chars.peek() == Some(&'/') => {
                    for cc in chars.by_ref() {
                        if cc == '\n' {
                            break;
                        }
                    }
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    let mut prev = '\0';
                    for cc in chars.by_ref() {
                        if prev == '*' && cc == '/' {
                            break;
                        }
                        prev = cc;
                    }
                }
                ';' => {
                    if !current.trim().is_empty() {
                        out.push(current.trim().to_string());
                    }
                    current.clear();
                }
                other => current.push(other),
            },
        }
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }
    out
}

// ---------------------------------------------------------------------------
// Node rows → grid
// ---------------------------------------------------------------------------

struct Entity {
    id: i64,
    start: Option<i64>,
    end: Option<i64>,
    properties: Vec<(String, BoltType)>,
}

fn entity_from_row(row: &Row) -> AppResult<Entity> {
    let entries = row_entries(row)?;
    let int = |name: &str| -> Option<i64> {
        entries.iter().find(|(k, _)| k == name).and_then(|(_, v)| match v {
            BoltType::Integer(i) => Some(i.value),
            _ => None,
        })
    };
    let id = int(ID_FIELD).ok_or_else(|| AppError::driver("Row without id"))?;
    let properties = match entries.iter().find(|(k, _)| k == "_node").map(|(_, v)| v) {
        Some(BoltType::Node(n)) => n.properties.value.iter().map(|(k, v)| (k.value.clone(), v.clone())).collect(),
        Some(BoltType::Relation(r)) => r.properties.value.iter().map(|(k, v)| (k.value.clone(), v.clone())).collect(),
        Some(BoltType::UnboundedRelation(r)) => r.properties.value.iter().map(|(k, v)| (k.value.clone(), v.clone())).collect(),
        Some(BoltType::Map(m)) => m.value.iter().map(|(k, v)| (k.value.clone(), v.clone())).collect(),
        _ => Vec::new(),
    };
    Ok(Entity { id, start: int(START_FIELD), end: int(END_FIELD), properties })
}

// WHAT:  Union of property keys across entities (sorted), synthetic ids first.
fn union_columns(entities: &[Entity], target: Target) -> Vec<ColumnInfo> {
    let mut types: BTreeMap<String, &'static str> = BTreeMap::new();
    for e in entities {
        for (k, v) in &e.properties {
            let entry = types.entry(k.clone()).or_insert("null");
            if *entry == "null" && !matches!(v, BoltType::Null(_)) {
                *entry = bolt_type_name(v);
            }
        }
    }
    let mut columns = vec![ColumnInfo { name: ID_FIELD.into(), data_type: "integer".into(), nullable: false, primary_key: true, ordinal: 1 }];
    if target == Target::RelType {
        columns.push(ColumnInfo { name: START_FIELD.into(), data_type: "integer".into(), nullable: false, primary_key: false, ordinal: 2 });
        columns.push(ColumnInfo { name: END_FIELD.into(), data_type: "integer".into(), nullable: false, primary_key: false, ordinal: 3 });
    }
    for (name, ty) in types {
        let ordinal = u32::try_from(columns.len() + 1).unwrap_or(u32::MAX);
        columns.push(ColumnInfo { name, data_type: ty.to_string(), nullable: true, primary_key: false, ordinal });
    }
    columns
}

fn entity_rows(columns: &[ColumnInfo], entities: &[Entity]) -> Vec<Vec<Value>> {
    entities
        .iter()
        .map(|e| {
            columns
                .iter()
                .map(|c| match c.name.as_str() {
                    ID_FIELD => Value::Int(e.id),
                    START_FIELD => e.start.map(Value::Int).unwrap_or(Value::Null),
                    END_FIELD => e.end.map(Value::Int).unwrap_or(Value::Null),
                    name => e.properties.iter().find(|(k, _)| k == name).map(|(_, v)| bolt_to_value(v)).unwrap_or(Value::Null),
                })
                .collect()
        })
        .collect()
}

fn metas(columns: &[ColumnInfo]) -> Vec<ColumnMeta> {
    columns.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect()
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl Neo4jIntegration {
    fn query(&self, cypher: &str, params: Vec<(String, BoltType)>) -> Query {
        let mut q = neo4rs::query(cypher);
        for (k, v) in params {
            q = q.param(&k, v);
        }
        q
    }

    async fn collect(&self, cypher: &str, params: Vec<(String, BoltType)>, max_rows: usize) -> AppResult<(Vec<Row>, bool)> {
        let mut stream = self.graph.execute(self.query(cypher, params)).await.map_err(map_error)?;
        let mut rows = Vec::new();
        let mut truncated = false;
        while let Some(row) = stream.next().await.map_err(map_error)? {
            if rows.len() >= max_rows {
                truncated = true;
                break;
            }
            rows.push(row);
        }
        Ok((rows, truncated))
    }

    async fn strings(&self, cypher: &str, column: &str) -> AppResult<Vec<String>> {
        let (rows, _) = self.collect(cypher, vec![], 10_000).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            if let Ok(s) = row.get::<String>(column) {
                out.push(s);
            }
        }
        Ok(out)
    }

    // WHAT:  Procedure first (Neo4j, Memgraph), scan fallback for engines without it.
    async fn labels(&self) -> AppResult<Vec<String>> {
        match self.strings("CALL db.labels() YIELD label RETURN label", "label").await {
            Ok(v) => Ok(v),
            Err(_) => self.strings("MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label", "label").await,
        }
    }

    async fn relationship_types(&self) -> AppResult<Vec<String>> {
        match self.strings("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType", "relationshipType").await {
            Ok(v) => Ok(v),
            Err(_) => self.strings("MATCH ()-[r]->() RETURN DISTINCT type(r) AS relationshipType", "relationshipType").await,
        }
    }

    async fn entities(&self, table: &TableRef, query: &PageQuery) -> AppResult<Vec<Entity>> {
        let clause = where_clause(&query.filters);
        let cypher = format!(
            "{}{} {}{} SKIP {} LIMIT {}",
            match_clause(table),
            clause.text,
            return_clause(table),
            order_by(&query.sort),
            query.offset,
            query.limit
        );
        let (rows, _) = self.collect(&cypher, clause.params, query.limit as usize).await?;
        rows.iter().map(entity_from_row).collect()
    }

    fn guard_write(&self, cypher: &str) -> AppResult<()> {
        if self.read_only && looks_mutating(cypher) {
            return Err(AppError::invalid_input("This connection is read-only; mutating Cypher is blocked."));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Catalog rows (`SHOW …`, `CALL db.* / dbms.* / mg.*`) become JSON
//        objects and pure mappers turn them into ObjectSummary / ObjectDetail /
//        Stat, so offline tests can feed literal fixtures.
// WHY:   Neo4j 5, Neo4j 4.x and Memgraph spell the same catalog differently
//        (`SHOW INDEXES YIELD *` / `CALL db.indexes()` / `SHOW INDEX INFO`).
//        Every listing tries the dialects in order, the engine's own first,
//        and the mappers read whichever column names came back.
// HOW:   Actions are Cypher administration commands (DROP INDEX, DROP
//        CONSTRAINT, TERMINATE TRANSACTIONS, DROP USER…) that run through
//        `execute`, so the read-only lock and the destructive confirmation
//        apply unchanged.
// ---------------------------------------------------------------------------

const LIST_CAP: usize = 2_000;
const COUNTED_ENTITIES: usize = 100;
const PROPERTY_SAMPLE: usize = 200;

fn jget<'a>(row: &'a Json, keys: &[&str]) -> Option<&'a Json> {
    keys.iter().find_map(|k| row.get(*k)).filter(|v| !v.is_null())
}

// WHAT:  A JSON value as display text: strings bare, lists comma-joined,
//        everything else compact JSON.
fn text_of(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        Json::Array(items) => items.iter().map(text_of).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

fn jtext(row: &Json, keys: &[&str]) -> Option<String> {
    jget(row, keys).map(text_of).filter(|s| !s.is_empty())
}

fn jbool(row: &Json, keys: &[&str]) -> bool {
    jget(row, keys).and_then(Json::as_bool).unwrap_or(false)
}

fn jint(row: &Json, keys: &[&str]) -> Option<i64> {
    jget(row, keys).and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)).or_else(|| v.as_str().and_then(|s| s.parse().ok())))
}

fn jlist(row: &Json, keys: &[&str]) -> Vec<String> {
    match jget(row, keys) {
        Some(Json::Array(items)) => items.iter().map(text_of).filter(|s| !s.is_empty()).collect(),
        Some(other) => vec![text_of(other)].into_iter().filter(|s| !s.is_empty()).collect(),
        None => Vec::new(),
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

fn cypher_string(raw: &str) -> String {
    format!("\"{}\"", raw.replace('\\', "\\\\").replace('"', "\\\""))
}

fn backtick_labels(labels: &str) -> String {
    labels.split('|').map(backtick).collect::<Vec<_>>().join("|")
}

// WHAT:  ISO-8601 duration ("PT12.5S", what Bolt durations render as) or a
//        millisecond count (`dbms.listTransactions`) → "12.5 s".
fn duration_text(raw: &str) -> String {
    let t = raw.trim();
    let secs = t
        .strip_prefix("PT")
        .and_then(|r| r.strip_suffix('S'))
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| t.parse::<f64>().ok().map(|ms| ms / 1000.0));
    match secs {
        None => t.to_string(),
        Some(s) if s < 1.0 => format!("{:.0} ms", s * 1000.0),
        Some(s) if s < 60.0 => format!("{s:.1} s"),
        Some(s) if s < 3600.0 => format!("{}m {}s", (s / 60.0).floor(), (s % 60.0).floor()),
        Some(s) => format!("{}h {}m", (s / 3600.0).floor(), ((s % 3600.0) / 60.0).floor()),
    }
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

fn finish(mut out: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    out.truncate(LIST_CAP);
    out
}

// WHAT:  Every scalar / short field of a catalog row as a property sheet, skipping
//        the ones already shown elsewhere (the definition, the name).
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
                other => text_of(other),
            };
            (!text.is_empty()).then(|| ObjectProperty { name: k.clone(), value: preview(&text, 400) })
        })
        .collect()
}

fn row_to_json(row: &Row) -> AppResult<Json> {
    let entries = row_entries(row)?;
    Ok(Json::Object(entries.iter().map(|(k, v)| (k.clone(), bolt_to_json(v))).collect()))
}

fn is_memgraph_row(row: &Json) -> bool {
    row.get("index type").is_some() || row.get("constraint type").is_some() || row.get("current_value").is_some() || row.get("transaction_id").is_some()
}

// ---- graph counts -----------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq)]
struct GraphCounts {
    nodes: BTreeMap<String, i64>,
    rels: BTreeMap<String, i64>,
    total_nodes: Option<i64>,
    total_rels: Option<i64>,
}

// WHAT:  `CALL db.stats.retrieve('GRAPH COUNTS')` data: `nodes` entries carry
//        `label` (absent = grand total), `relationships` carry `relationshipType`
//        (absent = total; with startLabel / endLabel = per-pair breakdown).
fn counts_from_graph_counts(data: &Json) -> GraphCounts {
    let mut out = GraphCounts::default();
    for n in data.get("nodes").and_then(Json::as_array).into_iter().flatten() {
        let count = jint(n, &["count"]).unwrap_or(0);
        match jtext(n, &["label"]) {
            Some(label) => {
                out.nodes.insert(label, count);
            }
            None => out.total_nodes = Some(count),
        }
    }
    for r in data.get("relationships").and_then(Json::as_array).into_iter().flatten() {
        let count = jint(r, &["count"]).unwrap_or(0);
        if r.get("startLabel").is_some() || r.get("endLabel").is_some() {
            continue;
        }
        match jtext(r, &["relationshipType"]) {
            Some(t) => {
                out.rels.insert(t, count);
            }
            None => out.total_rels = Some(count),
        }
    }
    out
}

// WHAT:  `CALL apoc.meta.stats()`: `labels` / `relTypesCount` are name → count maps.
fn counts_from_apoc(row: &Json) -> GraphCounts {
    let map = |key: &str| -> BTreeMap<String, i64> {
        row.get(key)
            .and_then(Json::as_object)
            .map(|m| m.iter().filter_map(|(k, v)| v.as_i64().map(|n| (k.clone(), n))).collect())
            .unwrap_or_default()
    };
    GraphCounts { nodes: map("labels"), rels: map("relTypesCount"), total_nodes: jint(row, &["nodeCount"]), total_rels: jint(row, &["relCount"]) }
}

fn entity_summaries(kind: ObjectKind, names: &[String], counts: &BTreeMap<String, i64>, database: &str) -> Vec<ObjectSummary> {
    let noun = if kind == ObjectKind::RelationshipType { "relationships" } else { "nodes" };
    finish(
        names
            .iter()
            .map(|n| {
                let mut s = ObjectSummary::new(kind, n.clone(), Some(database.to_string()));
                if let Some(c) = counts.get(n) {
                    s = s.with_detail(format!("{} {noun}", crate::model::objects::format_number(*c as f64)));
                }
                s
            })
            .collect(),
    )
}

// ---- databases ----------------------------------------------------------------

fn database_summaries(rows: &[Json], current: &str) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|row| {
                let name = jtext(row, &["name", "Name"])?;
                let mut parts = Vec::new();
                if let Some(t) = jtext(row, &["type"]).filter(|t| t != "standard") {
                    parts.push(t);
                }
                if jbool(row, &["default"]) {
                    parts.push("default".into());
                }
                if jbool(row, &["home"]) {
                    parts.push("home".into());
                }
                if name == current {
                    parts.push("current".into());
                }
                if let Some(role) = jtext(row, &["role"]) {
                    parts.push(role);
                }
                if let Some(addr) = jtext(row, &["address"]) {
                    parts.push(addr);
                }
                let badge = jtext(row, &["currentStatus", "status", "Status"]).map(|s| s.to_lowercase());
                let mut s = ObjectSummary::new(ObjectKind::Database, name, None);
                if !parts.is_empty() {
                    s = s.with_detail(parts.join(" · "));
                }
                s.badge = badge;
                Some(s)
            })
            .collect(),
    )
}

fn database_detail(reference: &ObjectRef, row: Option<&Json>, memgraph: bool) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(row) = row {
        d = d.definition(serde_json::to_string_pretty(row).unwrap_or_default(), CodeLanguage::Json);
        d.properties = props_of(row, &["name", "Name"]);
    }
    if !memgraph && reference.name != "system" {
        let name = backtick(&reference.name);
        d = d
            .action(ObjectAction::new("start", "Start database", format!("START DATABASE {name}")))
            .action(ObjectAction::destructive("stop", "Stop database", format!("STOP DATABASE {name}")));
    }
    d
}

// ---- labels / relationship types ---------------------------------------------

fn entity_detail(reference: &ObjectRef, count: Option<i64>, properties: Option<ResultSet>, children: Vec<ObjectSummary>) -> ObjectDetail {
    let rel = reference.kind == ObjectKind::RelationshipType;
    let name = backtick(&reference.name);
    let pattern = if rel { format!("()-[n:{name}]->()") } else { format!("(n:{name})") };
    let mut d = ObjectDetail::empty(reference).definition(format!("MATCH {pattern} RETURN n LIMIT 25"), CodeLanguage::Text);
    if let Some(c) = count {
        d = d.property(if rel { "relationships" } else { "nodes" }, crate::model::objects::format_number(c as f64));
    }
    if let Some(rows) = &properties {
        d = d.property("property keys", rows.rows.len().to_string());
    }
    d.rows = properties;
    d.children = children;
    d.action(ObjectAction::new("sample", "Sample 25", format!("MATCH {pattern} RETURN n LIMIT 25"))).action(if rel {
        ObjectAction::destructive("delete-all", "Delete all relationships", format!("MATCH {pattern} DELETE n"))
    } else {
        ObjectAction::destructive("delete-all", "Delete all nodes", format!("MATCH {pattern} DETACH DELETE n"))
    })
}

// ---- indexes / constraints ------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaTarget {
    label: String,
    properties: Vec<String>,
    relationship: bool,
}

// WHAT:  Which label / type and properties a schema object covers, from either
//        Neo4j (`entityType`, `labelsOrTypes`, `properties`) or Memgraph
//        (`label`, `property`, `index type` = edge-type…) columns.
fn schema_target(row: &Json) -> SchemaTarget {
    let labels = jlist(row, &["labelsOrTypes", "label", "labels"]);
    let properties = jlist(row, &["properties", "property"]);
    let entity = jtext(row, &["entityType"]).unwrap_or_default();
    let index_type = jtext(row, &["index type"]).unwrap_or_default();
    let relationship = entity.eq_ignore_ascii_case("RELATIONSHIP") || index_type.starts_with("edge");
    SchemaTarget { label: labels.join("|"), properties, relationship }
}

fn target_text(t: &SchemaTarget) -> String {
    let head = if t.relationship { format!("[:{}]", t.label) } else { format!(":{}", t.label) };
    if t.properties.is_empty() {
        head
    } else {
        format!("{head}({})", t.properties.join(", "))
    }
}

// WHAT:  Neo4j names every index / constraint; Memgraph does not, so a stable
//        name is synthesised from kind + label + properties.
fn schema_object_name(row: &Json, t: &SchemaTarget) -> String {
    if let Some(n) = jtext(row, &["name"]) {
        return n;
    }
    let mut n = t.label.replace('|', "_");
    if !t.properties.is_empty() {
        n.push('_');
        n.push_str(&t.properties.join("_"));
    }
    match jtext(row, &["constraint type", "index type"]) {
        Some(kind) => format!("{}_{n}", kind.replace([' ', '+', '-'], "_")),
        None => n,
    }
}

fn owner_matches(t: &SchemaTarget, owner: Option<&str>) -> bool {
    owner.is_none_or(|o| t.label.split('|').any(|l| l == o))
}

fn index_summaries(rows: &[Json], database: &str, owner: Option<&str>) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|row| {
                let t = schema_target(row);
                if !owner_matches(&t, owner) {
                    return None;
                }
                let name = schema_object_name(row, &t);
                let mut detail = target_text(&t);
                if let Some(state) = jtext(row, &["state"]) {
                    detail = format!("{detail} · {}", state.to_lowercase());
                }
                if let Some(count) = jint(row, &["count"]) {
                    detail = format!("{detail} · {count} entries");
                }
                let badge = jtext(row, &["type", "index type", "uniqueness"]).map(|b| b.to_lowercase());
                Some(ObjectSummary { reference: ObjectRef { kind: ObjectKind::Index, name, parent: Some(database.to_string()) }, detail: Some(detail), badge })
            })
            .collect(),
    )
}

fn constraint_summaries(rows: &[Json], database: &str, owner: Option<&str>) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|row| {
                let t = schema_target(row);
                if !owner_matches(&t, owner) {
                    return None;
                }
                let name = schema_object_name(row, &t);
                let detail = if t.label.is_empty() { jtext(row, &["description"]).unwrap_or_default() } else { target_text(&t) };
                let badge = jtext(row, &["type", "constraint type"]).map(|b| b.to_lowercase());
                Some(ObjectSummary { reference: ObjectRef { kind: ObjectKind::Constraint, name, parent: Some(database.to_string()) }, detail: (!detail.is_empty()).then_some(detail), badge })
            })
            .collect(),
    )
}

fn memgraph_on_clause(t: &SchemaTarget) -> String {
    if t.properties.is_empty() {
        format!(":{}", t.label)
    } else {
        format!(":{}({})", t.label, t.properties.join(", "))
    }
}

// WHAT:  The CREATE statement: the server's own `createStatement` when it has
//        one (Neo4j 4.4+ with YIELD *), else rebuilt from the row.
fn index_ddl(row: &Json, name: &str) -> String {
    if let Some(s) = jtext(row, &["createStatement"]) {
        return s;
    }
    let t = schema_target(row);
    if is_memgraph_row(row) {
        let on = memgraph_on_clause(&t);
        return match jtext(row, &["index type"]).unwrap_or_default().as_str() {
            k if k.starts_with("edge") => format!("CREATE EDGE INDEX ON {on};"),
            "point" => format!("CREATE POINT INDEX ON {on};"),
            "text" => format!("CREATE TEXT INDEX {name} ON {on};"),
            _ => format!("CREATE INDEX ON {on};"),
        };
    }
    let ty = jtext(row, &["type"]).unwrap_or_else(|| "RANGE".into()).to_uppercase();
    let (var, pattern) = if t.relationship { ("r", format!("()-[r:{}]-()", backtick_labels(&t.label))) } else { ("n", format!("(n:{})", backtick_labels(&t.label))) };
    let props: Vec<String> = t.properties.iter().map(|p| format!("{var}.{}", backtick(p))).collect();
    let name = backtick(name);
    match ty.as_str() {
        "LOOKUP" if t.relationship => format!("CREATE LOOKUP INDEX {name} FOR ()-[r]-() ON EACH type(r)"),
        "LOOKUP" => format!("CREATE LOOKUP INDEX {name} FOR (n) ON EACH labels(n)"),
        "FULLTEXT" => format!("CREATE FULLTEXT INDEX {name} FOR {pattern} ON EACH [{}]", props.join(", ")),
        "VECTOR" => format!("CREATE VECTOR INDEX {name} FOR {pattern} ON {}", props.join(", ")),
        "BTREE" | "RANGE" => format!("CREATE INDEX {name} FOR {pattern} ON ({})", props.join(", ")),
        other => format!("CREATE {other} INDEX {name} FOR {pattern} ON ({})", props.join(", ")),
    }
}

fn index_drop(row: &Json, name: &str) -> String {
    if !is_memgraph_row(row) {
        return format!("DROP INDEX {}", backtick(name));
    }
    let t = schema_target(row);
    let on = memgraph_on_clause(&t);
    match jtext(row, &["index type"]).unwrap_or_default().as_str() {
        k if k.starts_with("edge") => format!("DROP EDGE INDEX ON {on};"),
        "point" => format!("DROP POINT INDEX ON {on};"),
        "text" => format!("DROP TEXT INDEX {name};"),
        _ => format!("DROP INDEX ON {on};"),
    }
}

fn memgraph_constraint(row: &Json, verb: &str) -> String {
    let t = schema_target(row);
    let props: Vec<String> = t.properties.iter().map(|p| format!("n.{p}")).collect();
    let assertion = match jtext(row, &["constraint type"]).unwrap_or_default().as_str() {
        "exists" => format!("EXISTS ({})", props.join(", ")),
        "data_type" => format!("{} IS TYPED {}", props.join(", "), jtext(row, &["data_type"]).unwrap_or_else(|| "STRING".into())),
        _ => format!("{} IS UNIQUE", props.join(", ")),
    };
    format!("{verb} CONSTRAINT ON (n:{}) ASSERT {assertion};", t.label)
}

fn constraint_ddl(row: &Json, name: &str) -> String {
    if let Some(s) = jtext(row, &["createStatement"]) {
        return s;
    }
    if is_memgraph_row(row) {
        return memgraph_constraint(row, "CREATE");
    }
    let t = schema_target(row);
    if t.label.is_empty() {
        return jtext(row, &["description"]).unwrap_or_default();
    }
    let ty = jtext(row, &["type"]).unwrap_or_default().to_uppercase();
    let (var, pattern) = if t.relationship { ("r", format!("()-[r:{}]-()", backtick_labels(&t.label))) } else { ("n", format!("(n:{})", backtick_labels(&t.label))) };
    let props: Vec<String> = t.properties.iter().map(|p| format!("{var}.{}", backtick(p))).collect();
    let subject = if props.len() == 1 { props.join("") } else { format!("({})", props.join(", ")) };
    let requirement = if ty.contains("KEY") {
        if t.relationship { "IS RELATIONSHIP KEY".to_string() } else { "IS NODE KEY".to_string() }
    } else if ty.contains("EXISTENCE") {
        "IS NOT NULL".to_string()
    } else if ty.contains("TYPE") {
        format!("IS :: {}", jtext(row, &["propertyType"]).unwrap_or_else(|| "STRING".into()))
    } else {
        "IS UNIQUE".to_string()
    };
    format!("CREATE CONSTRAINT {} FOR {pattern} REQUIRE {subject} {requirement}", backtick(name))
}

fn constraint_drop(row: &Json, name: &str) -> String {
    if is_memgraph_row(row) {
        memgraph_constraint(row, "DROP")
    } else {
        format!("DROP CONSTRAINT {}", backtick(name))
    }
}

fn schema_detail(reference: &ObjectRef, row: &Json) -> ObjectDetail {
    let (ddl, drop) = if reference.kind == ObjectKind::Index {
        (index_ddl(row, &reference.name), index_drop(row, &reference.name))
    } else {
        (constraint_ddl(row, &reference.name), constraint_drop(row, &reference.name))
    };
    let mut d = ObjectDetail::empty(reference).definition(ddl, CodeLanguage::Text);
    d.properties = props_of(row, &["name", "createStatement"]);
    let t = schema_target(row);
    if !t.label.is_empty() {
        d.rows = Some(ResultSet {
            columns: vec![ColumnMeta { name: "property".into(), type_name: "string".into() }],
            rows: t.properties.iter().map(|p| vec![Value::Text(p.clone())]).collect(),
            truncated: false,
        });
    }
    let label = if reference.kind == ObjectKind::Index { "Drop index" } else { "Drop constraint" };
    d.action(ObjectAction::destructive("drop", label, drop))
}

// ---- procedures / functions ------------------------------------------------------

fn routine_summaries(kind: ObjectKind, rows: &[Json], database: &str) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|row| {
                let name = jtext(row, &["name"])?;
                let badge = jtext(row, &["mode", "category"])
                    .map(|b| b.to_lowercase())
                    .or_else(|| jget(row, &["is_write"]).and_then(Json::as_bool).map(|w| if w { "write".to_string() } else { "read".to_string() }))
                    .or_else(|| jbool(row, &["aggregating"]).then(|| "aggregating".to_string()))
                    .or_else(|| jbool(row, &["isBuiltIn"]).then(|| "builtin".to_string()));
                let detail = jtext(row, &["signature"]).or_else(|| jtext(row, &["description"])).map(|d| preview(&d, 140));
                Some(ObjectSummary { reference: ObjectRef { kind, name, parent: Some(database.to_string()) }, detail, badge })
            })
            .collect(),
    )
}

fn routine_detail(reference: &ObjectRef, row: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(sig) = jtext(row, &["signature"]) {
        d = d.definition(sig, CodeLanguage::Text);
    }
    d.properties = props_of(row, &["name", "signature", "argumentDescription", "returnDescription"]);
    if let Some(args) = row.get("argumentDescription").and_then(Json::as_array).filter(|a| !a.is_empty()) {
        d.rows = Some(objects_to_result_set(args, Some("name"), LIST_CAP));
    }
    d
}

// ---- users / roles ----------------------------------------------------------------

fn user_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|row| {
                let name = jtext(row, &["user", "username", "name"])?;
                let mut parts = jlist(row, &["roles"]);
                if jbool(row, &["passwordChangeRequired"]) {
                    parts.push("password change required".into());
                }
                if let Some(home) = jtext(row, &["home"]) {
                    parts.push(format!("home: {home}"));
                }
                let mut s = ObjectSummary::new(ObjectKind::User, name, None);
                if !parts.is_empty() {
                    s = s.with_detail(parts.join(", "));
                }
                if jbool(row, &["suspended"]) {
                    s = s.with_badge("suspended");
                }
                Some(s)
            })
            .collect(),
    )
}

fn user_detail(reference: &ObjectRef, row: Option<&Json>, privileges: Option<ResultSet>, memgraph: bool) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(row) = row {
        d.properties = props_of(row, &["user", "username"]);
    }
    d.rows = privileges;
    let name = backtick(&reference.name);
    if !memgraph {
        d = d
            .action(ObjectAction::destructive("suspend", "Suspend user", format!("ALTER USER {name} SET STATUS SUSPENDED")))
            .action(ObjectAction::new("activate", "Activate user", format!("ALTER USER {name} SET STATUS ACTIVE")));
    }
    d.action(ObjectAction::destructive("drop", "Drop user", format!("DROP USER {name}")))
}

// WHAT:  `SHOW ROLES WITH USERS` repeats a role per member; fold to one row each.
fn role_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    let mut members: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let Some(role) = jtext(row, &["role", "name"]) else { continue };
        let entry = members.entry(role).or_default();
        if let Some(m) = jtext(row, &["member"]) {
            entry.push(m);
        }
    }
    finish(
        members
            .into_iter()
            .map(|(role, m)| {
                let mut s = ObjectSummary::new(ObjectKind::Role, role, None);
                if !m.is_empty() {
                    s = s.with_detail(format!("{} member(s): {}", m.len(), m.join(", ")));
                }
                s
            })
            .collect(),
    )
}

fn role_detail(reference: &ObjectRef, members: &[String], privileges: Option<ResultSet>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if !members.is_empty() {
        d = d.property("members", members.join(", "));
    }
    d.rows = privileges;
    d.action(ObjectAction::destructive("drop", "Drop role", format!("DROP ROLE {}", backtick(&reference.name))))
}

// ---- transactions -----------------------------------------------------------------

fn transaction_query(row: &Json) -> String {
    jget(row, &["currentQuery", "query"]).map(|q| match q {
        Json::Array(items) => items.iter().map(text_of).collect::<Vec<_>>().join("; "),
        other => text_of(other),
    })
    .unwrap_or_default()
}

fn transaction_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|row| {
                let id = jtext(row, &["transactionId", "transaction_id", "id"])?;
                let mut parts = Vec::new();
                if let Some(u) = jtext(row, &["username", "user"]) {
                    parts.push(u);
                }
                if let Some(e) = jtext(row, &["elapsedTime", "elapsedTimeMillis"]) {
                    parts.push(duration_text(&e));
                }
                let query = transaction_query(row);
                if !query.is_empty() {
                    parts.push(preview(&query, 60));
                }
                let mut s = ObjectSummary::new(ObjectKind::Transaction, id, None);
                if !parts.is_empty() {
                    s = s.with_detail(parts.join(" · "));
                }
                s.badge = jtext(row, &["status"]).map(|b| b.to_lowercase());
                Some(s)
            })
            .collect(),
    )
}

fn transaction_detail(reference: &ObjectRef, row: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    let query = transaction_query(row);
    if !query.is_empty() {
        d = d.definition(query, CodeLanguage::Text);
    }
    d.properties = props_of(row, &["currentQuery", "query", "transactionId", "transaction_id"]);
    d.action(ObjectAction::destructive("terminate", "Terminate transaction", format!("TERMINATE TRANSACTIONS {}", cypher_string(&reference.name))))
}

// ---- settings ---------------------------------------------------------------------

fn setting_summaries(rows: &[Json]) -> Vec<ObjectSummary> {
    finish(
        rows.iter()
            .filter_map(|row| {
                let name = jtext(row, &["name"])?;
                let mut s = ObjectSummary::new(ObjectKind::Setting, name, None);
                if let Some(v) = jtext(row, &["value", "current_value"]) {
                    s = s.with_detail(preview(&v, 80));
                }
                if jbool(row, &["dynamic"]) {
                    s = s.with_badge("dynamic");
                } else if jbool(row, &["explicitlySet"]) {
                    s = s.with_badge("explicit");
                }
                Some(s)
            })
            .collect(),
    )
}

fn setting_detail(reference: &ObjectRef, row: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(v) = jtext(row, &["value", "current_value"]) {
        d = d.definition(v, CodeLanguage::Text);
    }
    d.properties = props_of(row, &["name", "value", "current_value"]);
    d
}

// ---- stats --------------------------------------------------------------------------

// WHAT:  `CALL dbms.queryJmx('java.lang:type=Memory')` attributes → heap figures.
fn jmx_memory_stats(attrs: &Json) -> Vec<Stat> {
    let usage = |pool: &str, field: &str| -> Option<f64> { attrs.get(pool)?.get("value")?.get(field)?.as_f64() };
    let mut out = Vec::new();
    if let Some(used) = usage("HeapMemoryUsage", "used") {
        out.push(stat_bytes("Heap used", used));
    }
    if let Some(committed) = usage("HeapMemoryUsage", "committed") {
        out.push(stat_bytes("Heap committed", committed));
    }
    if let Some(max) = usage("HeapMemoryUsage", "max").filter(|m| *m > 0.0) {
        out.push(stat_bytes("Heap max", max));
    }
    if let Some(used) = usage("NonHeapMemoryUsage", "used") {
        out.push(stat_bytes("Off-heap used", used));
    }
    out
}

fn jmx_uptime(attrs: &Json) -> Option<String> {
    let ms = attrs.get("Uptime")?.get("value")?.as_f64()?;
    Some(duration_text(&format!("{ms}")))
}

// WHAT:  Memgraph `SHOW STORAGE INFO` rows (`storage info`, `value`) → figures.
fn storage_info_stats(rows: &[Json]) -> (Vec<Stat>, Vec<Stat>) {
    let map: BTreeMap<String, &Json> = rows.iter().filter_map(|r| jtext(r, &["storage info"]).zip(r.get("value"))).collect();
    let num = |k: &str| map.get(k).and_then(|v| v.as_f64());
    let mut graph = Vec::new();
    let mut storage = Vec::new();
    if let Some(v) = num("vertex_count") {
        graph.push(Stat::number("Nodes", v, None));
    }
    if let Some(v) = num("edge_count") {
        graph.push(Stat::number("Relationships", v, None));
    }
    if let Some(v) = num("average_degree") {
        graph.push(Stat::number("Average degree", (v * 100.0).round() / 100.0, None));
    }
    for (key, label) in [("memory_res", "Memory resident"), ("memory_usage", "Memory usage"), ("memory_tracked", "Memory tracked"), ("allocation_limit", "Allocation limit"), ("disk_usage", "Disk usage")] {
        if let Some(v) = num(key) {
            storage.push(stat_bytes(label, v));
        }
    }
    for (key, label) in [("storage_mode", "Storage mode"), ("global_isolation_level", "Isolation level")] {
        if let Some(v) = map.get(key).map(|v| text_of(v)) {
            storage.push(Stat::text(label, v));
        }
    }
    (graph, storage)
}

fn find_row<'a>(rows: &'a [Json], name: &str, name_of: impl Fn(&Json) -> Option<String>) -> Option<&'a Json> {
    rows.iter().find(|r| name_of(r).as_deref() == Some(name))
}

impl Neo4jIntegration {
    fn is_memgraph(&self) -> bool {
        self.engine == Engine::Memgraph
    }

    async fn json_rows(&self, cypher: &str, max_rows: usize) -> AppResult<Vec<Json>> {
        let (rows, _) = self.collect(cypher, vec![], max_rows).await?;
        rows.iter().map(row_to_json).collect()
    }

    // WHAT:  Runs the first dialect the server accepts (the engine's own first);
    //        the last error wins when none does.
    async fn first_ok(&self, neo4j: &[&str], memgraph: &[&str], max_rows: usize) -> AppResult<Vec<Json>> {
        let order: Vec<&str> = if self.is_memgraph() { memgraph.iter().chain(neo4j).copied().collect() } else { neo4j.iter().chain(memgraph).copied().collect() };
        let mut last = AppError::driver("No statement to run.");
        for cypher in order {
            match self.json_rows(cypher, max_rows).await {
                Ok(rows) => return Ok(rows),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    async fn catalog_rows(&self, kind: ObjectKind) -> AppResult<Vec<Json>> {
        match kind {
            ObjectKind::Database => self.first_ok(&["SHOW DATABASES YIELD *", "SHOW DATABASES"], &["SHOW DATABASES"], LIST_CAP).await,
            ObjectKind::Constraint => {
                self.first_ok(
                    &["SHOW CONSTRAINTS YIELD *", "SHOW CONSTRAINTS", "CALL db.constraints() YIELD name, description RETURN name, description"],
                    &["SHOW CONSTRAINT INFO"],
                    LIST_CAP,
                )
                .await
            }
            ObjectKind::Index => self.first_ok(&["SHOW INDEXES YIELD *", "SHOW INDEXES", "CALL db.indexes()"], &["SHOW INDEX INFO"], LIST_CAP).await,
            ObjectKind::Procedure => {
                self.first_ok(
                    &["SHOW PROCEDURES YIELD *", "SHOW PROCEDURES", "CALL dbms.procedures()"],
                    &["CALL mg.procedures() YIELD name, signature, is_write, path RETURN name, signature, is_write, path"],
                    LIST_CAP,
                )
                .await
            }
            ObjectKind::Function => {
                self.first_ok(
                    &["SHOW FUNCTIONS YIELD *", "SHOW FUNCTIONS", "CALL dbms.functions()"],
                    &["CALL mg.functions() YIELD name, signature, path RETURN name, signature, path"],
                    LIST_CAP,
                )
                .await
            }
            ObjectKind::User => self.first_ok(&["SHOW USERS YIELD *", "SHOW USERS"], &["SHOW USERS"], LIST_CAP).await,
            ObjectKind::Role => self.first_ok(&["SHOW ROLES WITH USERS", "SHOW ROLES"], &["SHOW ROLES"], LIST_CAP).await,
            ObjectKind::Transaction => {
                self.first_ok(&["SHOW TRANSACTIONS YIELD *", "SHOW TRANSACTIONS", "CALL dbms.listTransactions()"], &["SHOW TRANSACTIONS"], LIST_CAP).await
            }
            ObjectKind::Setting => {
                self.first_ok(
                    &[
                        "CALL dbms.listConfig() YIELD name, value, description, dynamic, defaultValue, startupValue, explicitlySet RETURN name, value, description, dynamic, defaultValue, startupValue, explicitlySet",
                        "CALL dbms.listConfig()",
                    ],
                    &["SHOW CONFIG"],
                    LIST_CAP,
                )
                .await
            }
            _ => Ok(Vec::new()),
        }
    }

    // WHAT:  Per-label / per-type counts from the count store (Neo4j) or APOC;
    //        empty when neither procedure exists (Memgraph), see `fill_counts`.
    async fn graph_counts(&self) -> GraphCounts {
        if !self.is_memgraph() {
            if let Ok(rows) = self.json_rows("CALL db.stats.retrieve('GRAPH COUNTS') YIELD data RETURN data", 1).await {
                if let Some(data) = rows.first().and_then(|r| r.get("data")) {
                    return counts_from_graph_counts(data);
                }
            }
            if let Ok(rows) = self
                .json_rows("CALL apoc.meta.stats() YIELD labels, relTypesCount, nodeCount, relCount RETURN labels, relTypesCount, nodeCount, relCount", 1)
                .await
            {
                if let Some(row) = rows.first() {
                    return counts_from_apoc(row);
                }
            }
        }
        GraphCounts::default()
    }

    // WHAT:  Counts the first `COUNTED_ENTITIES` names the stats did not cover
    //        (O(1) in Neo4j's count store, a scan elsewhere, hence the cap).
    async fn fill_counts(&self, names: &[String], mut known: BTreeMap<String, i64>, rel: bool) -> BTreeMap<String, i64> {
        // Collect first: the filter borrows `known`, which the body then mutates.
        let pending: Vec<String> = names.iter().filter(|n| !known.contains_key(*n)).take(COUNTED_ENTITIES).cloned().collect();
        for name in &pending {
            let cypher = if rel { format!("MATCH ()-[n:{}]->() RETURN count(n) AS n", backtick(name)) } else { format!("MATCH (n:{}) RETURN count(n) AS n", backtick(name)) };
            if let Ok(rows) = self.json_rows(&cypher, 1).await {
                if let Some(c) = rows.first().and_then(|r| jint(r, &["n"])) {
                    known.insert(name.clone(), c);
                }
            }
        }
        known
    }

    async fn property_stats(&self, name: &str, rel: bool) -> Option<ResultSet> {
        let pattern = if rel { format!("()-[n:{}]->()", backtick(name)) } else { format!("(n:{})", backtick(name)) };
        let cypher = format!("MATCH {pattern} WITH n LIMIT {PROPERTY_SAMPLE} UNWIND keys(n) AS key RETURN key AS property, count(*) AS occurrences ORDER BY key");
        let rows = self.json_rows(&cypher, LIST_CAP).await.ok()?;
        Some(objects_to_result_set(&rows, Some("property"), LIST_CAP))
    }

    async fn grid(&self, neo4j: &[&str], memgraph: &[&str]) -> Option<ResultSet> {
        let rows = self.first_ok(neo4j, memgraph, LIST_CAP).await.ok()?;
        (!rows.is_empty()).then(|| objects_to_result_set(&rows, None, LIST_CAP))
    }

    async fn scalar(&self, cypher: &str) -> Option<i64> {
        self.json_rows(cypher, 1).await.ok()?.first().and_then(|r| jint(r, &["n"]))
    }

    async fn explorer_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let db = self.database.as_str();
        // A parent other than the session database is a label / relationship
        // type asking for its own indexes and constraints.
        let owner = parent.filter(|p| *p != db);
        match kind {
            ObjectKind::Database => Ok(match self.catalog_rows(kind).await {
                Ok(rows) if !rows.is_empty() => database_summaries(&rows, db),
                _ => vec![ObjectSummary::new(ObjectKind::Database, db, None).with_badge("current")],
            }),
            ObjectKind::Label | ObjectKind::RelationshipType => {
                let rel = kind == ObjectKind::RelationshipType;
                let names = if rel { self.relationship_types().await? } else { self.labels().await? };
                let counts = self.graph_counts().await;
                let known = if rel { counts.rels } else { counts.nodes };
                let counts = self.fill_counts(&names, known, rel).await;
                Ok(entity_summaries(kind, &names, &counts, db))
            }
            ObjectKind::Constraint => Ok(constraint_summaries(&self.catalog_rows(kind).await?, db, owner)),
            ObjectKind::Index => Ok(index_summaries(&self.catalog_rows(kind).await?, db, owner)),
            ObjectKind::Procedure | ObjectKind::Function => Ok(routine_summaries(kind, &self.catalog_rows(kind).await?, db)),
            ObjectKind::User => Ok(user_summaries(&self.catalog_rows(kind).await?)),
            ObjectKind::Role => Ok(role_summaries(&self.catalog_rows(kind).await?)),
            ObjectKind::Transaction => Ok(transaction_summaries(&self.catalog_rows(kind).await?)),
            // Listing the configuration needs admin rights; an empty list beats an error here.
            ObjectKind::Setting => Ok(self.catalog_rows(kind).await.map(|rows| setting_summaries(&rows)).unwrap_or_default()),
            _ => Ok(Vec::new()),
        }
    }

    async fn explorer_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let memgraph = self.is_memgraph();
        let name = reference.name.as_str();
        match reference.kind {
            ObjectKind::Database => {
                let rows = self.catalog_rows(reference.kind).await.unwrap_or_default();
                Ok(database_detail(reference, find_row(&rows, name, |r| jtext(r, &["name", "Name"])), memgraph))
            }
            ObjectKind::Label | ObjectKind::RelationshipType => {
                let rel = reference.kind == ObjectKind::RelationshipType;
                let counts = self.graph_counts().await;
                let known = if rel { counts.rels } else { counts.nodes };
                let count = self.fill_counts(std::slice::from_ref(&reference.name), known, rel).await.get(name).copied();
                let properties = self.property_stats(name, rel).await;
                let mut children = self.explorer_objects(ObjectKind::Index, Some(name)).await.unwrap_or_default();
                children.extend(self.explorer_objects(ObjectKind::Constraint, Some(name)).await.unwrap_or_default());
                Ok(entity_detail(reference, count, properties, children))
            }
            ObjectKind::Constraint | ObjectKind::Index => {
                let rows = self.catalog_rows(reference.kind).await?;
                let row = find_row(&rows, name, |r| Some(schema_object_name(r, &schema_target(r))))
                    .ok_or_else(|| AppError::not_found(format!("No {} named `{name}`.", if reference.kind == ObjectKind::Index { "index" } else { "constraint" })))?;
                Ok(schema_detail(reference, row))
            }
            ObjectKind::Procedure | ObjectKind::Function => {
                let rows = self.catalog_rows(reference.kind).await?;
                let row = find_row(&rows, name, |r| jtext(r, &["name"])).ok_or_else(|| AppError::not_found(format!("No routine named `{name}`.")))?;
                Ok(routine_detail(reference, row))
            }
            ObjectKind::User => {
                let rows = self.catalog_rows(reference.kind).await.unwrap_or_default();
                let row = find_row(&rows, name, |r| jtext(r, &["user", "username", "name"]));
                let neo4j = format!("SHOW USER {} PRIVILEGES", backtick(name));
                let mg = format!("SHOW PRIVILEGES FOR {}", backtick(name));
                let privileges = self.grid(&[neo4j.as_str()], &[mg.as_str()]).await;
                Ok(user_detail(reference, row, privileges, memgraph))
            }
            ObjectKind::Role => {
                let rows = self.catalog_rows(reference.kind).await.unwrap_or_default();
                let members: Vec<String> = rows.iter().filter(|r| jtext(r, &["role", "name"]).as_deref() == Some(name)).filter_map(|r| jtext(r, &["member"])).collect();
                let neo4j = format!("SHOW ROLE {} PRIVILEGES", backtick(name));
                let privileges = self.grid(&[neo4j.as_str()], &[]).await;
                Ok(role_detail(reference, &members, privileges))
            }
            ObjectKind::Transaction => {
                let rows = self.catalog_rows(reference.kind).await?;
                let row = find_row(&rows, name, |r| jtext(r, &["transactionId", "transaction_id", "id"]))
                    .ok_or_else(|| AppError::not_found(format!("Transaction `{name}` is no longer running.")))?;
                Ok(transaction_detail(reference, row))
            }
            ObjectKind::Setting => {
                let rows = self.catalog_rows(reference.kind).await?;
                let row = find_row(&rows, name, |r| jtext(r, &["name"])).ok_or_else(|| AppError::not_found(format!("No setting named `{name}`.")))?;
                Ok(setting_detail(reference, row))
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn explorer_stats(&self) -> AppResult<ServerStats> {
        let memgraph = self.is_memgraph();
        let mut server = Vec::new();
        if let Ok(Some(v)) = self.server_version().await {
            server.push(Stat::text("Version", v));
        }
        if let Ok(rows) = self.json_rows("CALL dbms.components() YIELD edition RETURN edition", 1).await {
            if let Some(e) = rows.first().and_then(|r| jtext(r, &["edition"])) {
                server.push(Stat::text("Edition", e));
            }
        }
        server.push(Stat::text("Database", self.database.clone()));
        if let Ok(rows) = self.json_rows("CALL dbms.queryJmx('java.lang:type=Runtime') YIELD attributes RETURN attributes", 1).await {
            if let Some(up) = rows.first().and_then(|r| r.get("attributes")).and_then(jmx_uptime) {
                server.push(Stat::text("Uptime", up));
            }
        }
        if let Ok(rows) = self.catalog_rows(ObjectKind::Database).await {
            let dbs = database_summaries(&rows, &self.database);
            let online = dbs.iter().filter(|d| d.badge.as_deref() == Some("online")).count();
            server.push(Stat::number("Databases", dbs.len() as f64, None).with_hint(format!("{online} online")));
        }
        let mut groups = vec![StatGroup { title: "Server".into(), stats: server }];

        let counts = self.graph_counts().await;
        let labels = self.labels().await.unwrap_or_default();
        let rel_types = self.relationship_types().await.unwrap_or_default();
        let (mut graph, storage) = if memgraph {
            self.json_rows("SHOW STORAGE INFO", LIST_CAP).await.map(|rows| storage_info_stats(&rows)).unwrap_or_default()
        } else {
            (Vec::new(), Vec::new())
        };
        if graph.is_empty() {
            let nodes = match counts.total_nodes {
                Some(n) => Some(n),
                None => self.scalar("MATCH (n) RETURN count(n) AS n").await,
            };
            let rels = match counts.total_rels {
                Some(n) => Some(n),
                None => self.scalar("MATCH ()-[r]->() RETURN count(r) AS n").await,
            };
            if let Some(n) = nodes {
                graph.push(Stat::number("Nodes", n as f64, None));
            }
            if let Some(n) = rels {
                graph.push(Stat::number("Relationships", n as f64, None));
            }
        }
        graph.push(Stat::number("Labels", labels.len() as f64, None));
        graph.push(Stat::number("Relationship types", rel_types.len() as f64, None));
        if let Some(n) = self.scalar("CALL db.propertyKeys() YIELD propertyKey RETURN count(*) AS n").await {
            graph.push(Stat::number("Property keys", n as f64, None));
        }
        groups.push(StatGroup { title: "Graph".into(), stats: graph });

        let mut schema = Vec::new();
        if let Ok(rows) = self.catalog_rows(ObjectKind::Index).await {
            let failed = rows.iter().filter(|r| jtext(r, &["state"]).is_some_and(|s| s.eq_ignore_ascii_case("FAILED"))).count();
            let populating = rows.iter().filter(|r| jtext(r, &["state"]).is_some_and(|s| s.eq_ignore_ascii_case("POPULATING"))).count();
            let mut stat = Stat::number("Indexes", rows.len() as f64, None);
            if failed + populating > 0 {
                stat = stat.with_hint(format!("{failed} failed, {populating} populating"));
            }
            schema.push(stat);
        }
        if let Ok(rows) = self.catalog_rows(ObjectKind::Constraint).await {
            schema.push(Stat::number("Constraints", rows.len() as f64, None));
        }
        if !schema.is_empty() {
            groups.push(StatGroup { title: "Schema".into(), stats: schema });
        }

        if let Ok(rows) = self.catalog_rows(ObjectKind::Transaction).await {
            let running = rows.iter().filter(|r| jtext(r, &["status"]).is_none_or(|s| s.to_lowercase().starts_with("running"))).count();
            groups.push(StatGroup {
                title: "Transactions".into(),
                stats: vec![Stat::number("Open", rows.len() as f64, None), Stat::number("Running", running as f64, None)],
            });
        }

        if !storage.is_empty() {
            groups.push(StatGroup { title: "Storage".into(), stats: storage });
        } else if let Ok(rows) = self.json_rows("CALL dbms.queryJmx('java.lang:type=Memory') YIELD attributes RETURN attributes", 1).await {
            let memory = rows.first().and_then(|r| r.get("attributes")).map(jmx_memory_stats).unwrap_or_default();
            if !memory.is_empty() {
                groups.push(StatGroup { title: "Memory".into(), stats: memory });
            }
        }
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
        capabilities: Capabilities { namespaces: true, paging: true, fixed_columns: false, ..Capabilities::DOCUMENT },
        object_kinds: vec![K::Database, K::Label, K::RelationshipType, K::Constraint, K::Index, K::Procedure, K::Function, K::User, K::Role, K::Transaction, K::Setting],
        tools: vec![T::Stats, T::GraphView],
    }
}

#[async_trait]
impl Integration for Neo4jIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        self.collect("RETURN 1 AS ok", vec![], 1).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        if let Ok((rows, _)) = self.collect("CALL dbms.components() YIELD name, versions RETURN name, versions[0] AS version", vec![], 5).await {
            for row in rows {
                let name: String = row.get("name").unwrap_or_default();
                let version: String = row.get("version").unwrap_or_default();
                if !version.is_empty() {
                    return Ok(Some(format!("{name} {version}")));
                }
            }
        }
        if let Ok((rows, _)) = self.collect("SHOW VERSION", vec![], 1).await {
            if let Some(version) = rows.first().and_then(|r| r.get::<String>("version").ok()) {
                return Ok(Some(format!("Memgraph {version}")));
            }
        }
        Ok(None)
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let mut names = self.strings("SHOW DATABASES YIELD name RETURN name", "name").await.unwrap_or_default();
        names.retain(|n| n != "system");
        if !names.contains(&self.database) {
            names.push(self.database.clone());
        }
        names.sort();
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut labels = self.labels().await?;
        labels.sort();
        let mut rels = self.relationship_types().await.unwrap_or_default();
        rels.sort();
        let nodes = SchemaInfo {
            name: self.database.clone(),
            tables: labels
                .into_iter()
                .map(|name| TableInfo { schema: Some(self.database.clone()), name, kind: TableKind::Table, row_estimate: None })
                .collect(),
        };
        let relationships = SchemaInfo {
            name: RELATIONSHIPS_SCHEMA.to_string(),
            tables: rels
                .into_iter()
                .map(|name| TableInfo { schema: Some(RELATIONSHIPS_SCHEMA.to_string()), name, kind: TableKind::Table, row_estimate: None })
                .collect(),
        };
        let mut schemas = vec![nodes];
        if !relationships.tables.is_empty() {
            schemas.push(relationships);
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let sample = PageQuery { sort: vec![], filters: vec![], offset: 0, limit: SAMPLE_SIZE as u32 };
        let entities = self.entities(table, &sample).await?;
        Ok(union_columns(&entities, target_for(table)))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let clause = where_clause(filters);
        let cypher = format!("{}{} RETURN count(n) AS total", match_clause(table), clause.text);
        let (rows, _) = self.collect(&cypher, clause.params, 1).await?;
        Ok(rows.first().and_then(|r| r.get::<i64>("total").ok()).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let entities = self.entities(table, query).await?;
        let mut columns = self.columns(table).await?;
        for extra in union_columns(&entities, target_for(table)) {
            if !columns.iter().any(|c| c.name == extra.name) {
                columns.push(extra);
            }
        }
        Ok(ResultSet { rows: entity_rows(&columns, &entities), columns: metas(&columns), truncated: false })
    }

    async fn execute(&self, script: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let statements = split_statements(script);
        if statements.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        let mut out = Vec::with_capacity(statements.len());
        for cypher in statements {
            self.guard_write(&cypher)?;
            if looks_mutating(&cypher) && !has_return(&cypher) {
                self.graph.run(neo4rs::query(&cypher)).await.map_err(map_error)?;
                out.push(StatementResult::Affected { rows_affected: 0 });
                continue;
            }
            let (rows, truncated) = self.collect(&cypher, vec![], max_rows).await?;
            let entries: Vec<Vec<(String, BoltType)>> = rows.iter().map(row_entries).collect::<AppResult<_>>()?;
            out.push(StatementResult::Rows { result: rows_to_result(&entries, &cypher, truncated) });
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
    use crate::model::{ConnectionInput, ConnectionSummary, Environment};

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    fn input(engine: Engine, host: Option<&str>, port: Option<u16>, ssl: SslMode) -> ResolvedConnection {
        let input = ConnectionInput {
            name: "t".into(),
            engine,
            environment: Environment::Local,
            read_only: false,
            host: host.map(str::to_string),
            port,
            database: None,
            username: Some("neo4j".into()),
            password: None,
            file_path: None,
            ssl_mode: ssl,
        };
        ResolvedConnection { summary: ConnectionSummary::draft(&input, true), secret: Some("pw".into()) }
    }

    #[test]
    fn uri_reflects_ssl_mode() {
        assert_eq!(build_uri(&input(Engine::Neo4j, Some("db.example"), Some(7687), SslMode::Disable)), "bolt://db.example:7687");
        assert_eq!(build_uri(&input(Engine::Neo4j, None, None, SslMode::Require)), "bolt+ssc://127.0.0.1:7687");
        assert_eq!(build_uri(&input(Engine::Neo4j, Some("h"), Some(1), SslMode::VerifyFull)), "bolt+s://h:1");
        assert_eq!(build_uri(&input(Engine::Neo4j, Some("neo4j+s://x.databases.neo4j.io"), None, SslMode::Prefer)), "neo4j+s://x.databases.neo4j.io");
    }

    #[test]
    fn filters_become_parameters() {
        let clause = where_clause(&[
            rule("_id", FilterOp::Eq, "7"),
            rule("name", FilterOp::Contains, "an"),
            rule("age", FilterOp::Gte, "30"),
            rule("tier", FilterOp::In, "gold, 2"),
            rule("note", FilterOp::IsNull, ""),
        ]);
        assert_eq!(
            clause.text,
            " WHERE id(n) = $p0 AND toLower(toString(n.`name`)) CONTAINS toLower($p1) AND n.`age` >= $p2 AND n.`tier` IN $p3 AND n.`note` IS NULL"
        );
        assert_eq!(clause.params.len(), 4);
        assert!(matches!(clause.params[0].1, BoltType::Integer(ref i) if i.value == 7));
        assert!(matches!(clause.params[2].1, BoltType::Integer(ref i) if i.value == 30));
        assert!(matches!(clause.params[3].1, BoltType::List(ref l) if l.len() == 2));
        assert_eq!(where_clause(&[]).text, "");
        assert_eq!(order_by(&[]), " ORDER BY id(n)");
        assert_eq!(order_by(&[SortRule { column: "a`b".into(), desc: true }]), " ORDER BY n.`a``b` DESC");
    }

    #[test]
    fn match_clauses_pick_target() {
        let label = TableRef { schema: Some("neo4j".into()), name: "Person".into() };
        let rel = TableRef { schema: Some("relationships".into()), name: "KNOWS".into() };
        assert_eq!(match_clause(&label), "MATCH (n:`Person`)");
        assert_eq!(match_clause(&rel), "MATCH ()-[n:`KNOWS`]->()");
        assert!(return_clause(&rel).contains("_start"));
    }

    #[test]
    fn statements_and_mutation_detection() {
        let parts = split_statements("MATCH (n) RETURN n; // c;\nCREATE (:A {s: 'x;y'}) ; /* ; */ RETURN `a;b`");
        assert_eq!(parts, vec!["MATCH (n) RETURN n", "CREATE (:A {s: 'x;y'})", "RETURN `a;b`"]);
        assert!(looks_mutating("MATCH (n) DETACH DELETE n"));
        assert!(looks_mutating("merge (a:A)"));
        assert!(!looks_mutating("MATCH (n) WHERE n.settings = 1 RETURN n"));
        assert!(has_return("CREATE (a) RETURN a"));
        assert!(!has_return("CREATE (a)"));
    }

    #[test]
    fn bolt_values_decode() {
        assert_eq!(bolt_to_value(&BoltType::Integer(neo4rs::BoltInteger::new(3))), Value::Int(3));
        assert_eq!(bolt_to_value(&BoltType::Boolean(neo4rs::BoltBoolean::new(true))), Value::Bool(true));
        assert_eq!(bolt_to_value(&BoltType::Null(neo4rs::BoltNull)), Value::Null);
        assert_eq!(bolt_to_value(&BoltType::String(neo4rs::BoltString::new("x"))), Value::Text("x".into()));
        let mut list = neo4rs::BoltList::new();
        list.push(BoltType::Integer(neo4rs::BoltInteger::new(1)));
        list.push(BoltType::String(neo4rs::BoltString::new("a")));
        assert_eq!(bolt_to_value(&BoltType::List(list)), Value::Json(serde_json::json!([1, "a"])));
        let date = neo4rs::BoltDate::from(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap_or_default());
        assert_eq!(bolt_to_value(&BoltType::Date(date)), Value::Text("2024-02-29".into()));
        let mut props = neo4rs::BoltMap::new();
        props.put(neo4rs::BoltString::new("name"), BoltType::String(neo4rs::BoltString::new("ann")));
        let mut labels = neo4rs::BoltList::new();
        labels.push(BoltType::String(neo4rs::BoltString::new("Person")));
        let node = neo4rs::BoltNode::new(neo4rs::BoltInteger::new(5), labels, props);
        assert_eq!(
            bolt_to_value(&BoltType::Node(node)),
            Value::Json(serde_json::json!({ "id": 5, "labels": ["Person"], "properties": { "name": "ann" } }))
        );
    }

    #[test]
    fn columns_follow_return_order() {
        let ordered = order_columns(vec!["z".into(), "name".into(), "id".into()], "MATCH (n) RETURN n.id AS id, n.name AS name, z");
        assert_eq!(ordered, vec!["id", "name", "z"]);
        let unknown = order_columns(vec!["b".into(), "a".into()], "CALL db.labels()");
        assert_eq!(unknown, vec!["a", "b"]);
    }

    #[test]
    fn entity_columns_put_ids_first() {
        let entities = vec![
            Entity { id: 1, start: None, end: None, properties: vec![("name".into(), BoltType::String(neo4rs::BoltString::new("a")))] },
            Entity { id: 2, start: None, end: None, properties: vec![("age".into(), BoltType::Integer(neo4rs::BoltInteger::new(3)))] },
        ];
        let columns = union_columns(&entities, Target::Label);
        assert_eq!(columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["_id", "age", "name"]);
        assert!(columns[0].primary_key);
        let rows = entity_rows(&columns, &entities);
        assert_eq!(rows[0], vec![Value::Int(1), Value::Null, Value::Text("a".into())]);
        let rel_columns = union_columns(&[], Target::RelType);
        assert_eq!(rel_columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["_id", "_start", "_end"]);
    }

    #[test]
    fn graph_counts_parse_both_sources() {
        let data = serde_json::json!({
            "nodes": [{"count": 171}, {"count": 133, "label": "Person"}, {"count": 38, "label": "Movie"}],
            "relationships": [{"count": 253}, {"count": 172, "relationshipType": "ACTED_IN"}, {"count": 172, "relationshipType": "ACTED_IN", "startLabel": "Person"}, {"count": 81, "relationshipType": "DIRECTED"}]
        });
        let c = counts_from_graph_counts(&data);
        assert_eq!(c.total_nodes, Some(171));
        assert_eq!(c.total_rels, Some(253));
        assert_eq!(c.nodes.get("Movie"), Some(&38));
        assert_eq!(c.rels.get("ACTED_IN"), Some(&172));
        assert_eq!(c.rels.len(), 2);
        let apoc = counts_from_apoc(&serde_json::json!({"labels": {"A": 2}, "relTypesCount": {"R": 1}, "nodeCount": 2, "relCount": 1}));
        assert_eq!(apoc.nodes.get("A"), Some(&2));
        assert_eq!(apoc.total_rels, Some(1));
        let s = entity_summaries(ObjectKind::Label, &["Movie".into(), "Zed".into()], &c.nodes, "neo4j");
        assert_eq!(s[0].detail.as_deref(), Some("38 nodes"));
        assert_eq!(s[0].reference.parent.as_deref(), Some("neo4j"));
        assert!(s[1].detail.is_none());
    }

    #[test]
    fn database_rows_map_status_and_flags() {
        let rows = vec![
            serde_json::json!({"name": "neo4j", "type": "standard", "currentStatus": "online", "default": true, "home": true, "address": "localhost:7687"}),
            serde_json::json!({"name": "system", "type": "system", "currentStatus": "online", "default": false}),
        ];
        let s = database_summaries(&rows, "neo4j");
        assert_eq!(s[0].reference.name, "neo4j");
        assert_eq!(s[0].badge.as_deref(), Some("online"));
        assert_eq!(s[0].detail.as_deref(), Some("default · home · current · localhost:7687"));
        assert_eq!(s[1].detail.as_deref(), Some("system"));
        let r = ObjectRef { kind: ObjectKind::Database, name: "neo4j".into(), parent: None };
        let d = database_detail(&r, rows.first(), false);
        assert_eq!(d.language, CodeLanguage::Json);
        assert!(d.actions.iter().any(|a| a.destructive && a.statement == "STOP DATABASE `neo4j`"));
        assert!(database_detail(&r, None, true).actions.is_empty());
    }

    #[test]
    fn neo4j_index_and_constraint_rows() {
        let idx = serde_json::json!({"name": "person_name", "type": "RANGE", "state": "ONLINE", "entityType": "NODE", "labelsOrTypes": ["Person"], "properties": ["name"], "populationPercent": 100.0});
        let s = index_summaries(std::slice::from_ref(&idx), "neo4j", None);
        assert_eq!(s[0].reference.name, "person_name");
        assert_eq!(s[0].badge.as_deref(), Some("range"));
        assert_eq!(s[0].detail.as_deref(), Some(":Person(name) · online"));
        assert!(index_summaries(std::slice::from_ref(&idx), "neo4j", Some("Movie")).is_empty());
        assert_eq!(index_ddl(&idx, "person_name"), "CREATE INDEX `person_name` FOR (n:`Person`) ON (n.`name`)");
        assert_eq!(index_drop(&idx, "person_name"), "DROP INDEX `person_name`");
        let ft = serde_json::json!({"name": "ft", "type": "FULLTEXT", "entityType": "RELATIONSHIP", "labelsOrTypes": ["KNOWS", "LIKES"], "properties": ["since", "note"]});
        assert_eq!(index_ddl(&ft, "ft"), "CREATE FULLTEXT INDEX `ft` FOR ()-[r:`KNOWS`|`LIKES`]-() ON EACH [r.`since`, r.`note`]");
        let lookup = serde_json::json!({"name": "lk", "type": "LOOKUP", "entityType": "NODE"});
        assert_eq!(index_ddl(&lookup, "lk"), "CREATE LOOKUP INDEX `lk` FOR (n) ON EACH labels(n)");
        let given = serde_json::json!({"name": "x", "type": "RANGE", "createStatement": "CREATE INDEX x FOR (n:A) ON (n.b)"});
        assert_eq!(index_ddl(&given, "x"), "CREATE INDEX x FOR (n:A) ON (n.b)");

        let uniq = serde_json::json!({"name": "book_isbn", "type": "UNIQUENESS", "entityType": "NODE", "labelsOrTypes": ["Book"], "properties": ["isbn"]});
        let c = constraint_summaries(std::slice::from_ref(&uniq), "neo4j", Some("Book"));
        assert_eq!(c[0].badge.as_deref(), Some("uniqueness"));
        assert_eq!(c[0].detail.as_deref(), Some(":Book(isbn)"));
        assert_eq!(constraint_ddl(&uniq, "book_isbn"), "CREATE CONSTRAINT `book_isbn` FOR (n:`Book`) REQUIRE n.`isbn` IS UNIQUE");
        let key = serde_json::json!({"name": "k", "type": "NODE_KEY", "entityType": "NODE", "labelsOrTypes": ["A"], "properties": ["x", "y"]});
        assert_eq!(constraint_ddl(&key, "k"), "CREATE CONSTRAINT `k` FOR (n:`A`) REQUIRE (n.`x`, n.`y`) IS NODE KEY");
        let exists = serde_json::json!({"name": "e", "type": "RELATIONSHIP_PROPERTY_EXISTENCE", "entityType": "RELATIONSHIP", "labelsOrTypes": ["R"], "properties": ["p"]});
        assert_eq!(constraint_ddl(&exists, "e"), "CREATE CONSTRAINT `e` FOR ()-[r:`R`]-() REQUIRE r.`p` IS NOT NULL");
        let legacy = serde_json::json!({"name": "old", "description": "CONSTRAINT ON ( a:A ) ASSERT (a.x) IS UNIQUE"});
        assert_eq!(constraint_summaries(std::slice::from_ref(&legacy), "neo4j", None)[0].detail.as_deref(), Some("CONSTRAINT ON ( a:A ) ASSERT (a.x) IS UNIQUE"));
        let r = ObjectRef { kind: ObjectKind::Constraint, name: "book_isbn".into(), parent: Some("neo4j".into()) };
        let d = schema_detail(&r, &uniq);
        assert_eq!(d.actions[0].statement, "DROP CONSTRAINT `book_isbn`");
        assert!(d.actions[0].destructive);
        assert_eq!(d.rows.as_ref().map(|r| r.rows.len()), Some(1));
    }

    #[test]
    fn memgraph_index_and_constraint_rows() {
        let idx = serde_json::json!({"index type": "label+property", "label": "Person", "property": "name", "count": 12});
        let s = index_summaries(std::slice::from_ref(&idx), "memgraph", None);
        assert_eq!(s[0].reference.name, "label_property_Person_name");
        assert_eq!(s[0].badge.as_deref(), Some("label+property"));
        assert_eq!(s[0].detail.as_deref(), Some(":Person(name) · 12 entries"));
        assert_eq!(index_ddl(&idx, "label_property_Person_name"), "CREATE INDEX ON :Person(name);");
        assert_eq!(index_drop(&idx, "label_property_Person_name"), "DROP INDEX ON :Person(name);");
        let edge = serde_json::json!({"index type": "edge-type", "label": "KNOWS", "property": null, "count": 3});
        assert_eq!(index_summaries(std::slice::from_ref(&edge), "memgraph", None)[0].detail.as_deref(), Some("[:KNOWS] · 3 entries"));
        assert_eq!(index_drop(&edge, "x"), "DROP EDGE INDEX ON :KNOWS;");
        let uniq = serde_json::json!({"constraint type": "unique", "label": "Person", "properties": ["email", "id"]});
        assert_eq!(constraint_ddl(&uniq, "x"), "CREATE CONSTRAINT ON (n:Person) ASSERT n.email, n.id IS UNIQUE;");
        assert_eq!(constraint_drop(&uniq, "x"), "DROP CONSTRAINT ON (n:Person) ASSERT n.email, n.id IS UNIQUE;");
        let exists = serde_json::json!({"constraint type": "exists", "label": "Person", "properties": "name"});
        assert_eq!(constraint_drop(&exists, "x"), "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);");
        let typed = serde_json::json!({"constraint type": "data_type", "label": "P", "properties": "age", "data_type": "INTEGER"});
        assert_eq!(constraint_ddl(&typed, "x"), "CREATE CONSTRAINT ON (n:P) ASSERT n.age IS TYPED INTEGER;");
        assert_eq!(constraint_summaries(std::slice::from_ref(&uniq), "memgraph", None)[0].reference.name, "unique_Person_email_id");
    }

    #[test]
    fn routines_users_roles_map() {
        let rows = vec![
            serde_json::json!({"name": "db.labels", "mode": "READ", "signature": "db.labels() :: (label :: STRING)", "description": "List labels", "argumentDescription": []}),
            serde_json::json!({"name": "apoc.do", "is_write": true, "signature": "apoc.do()", "path": "/mods/apoc.py"}),
        ];
        let s = routine_summaries(ObjectKind::Procedure, &rows, "neo4j");
        assert_eq!(s[0].reference.name, "apoc.do");
        assert_eq!(s[0].badge.as_deref(), Some("write"));
        assert_eq!(s[1].badge.as_deref(), Some("read"));
        assert_eq!(s[1].detail.as_deref(), Some("db.labels() :: (label :: STRING)"));
        let d = routine_detail(&s[1].reference, &rows[0]);
        assert_eq!(d.definition.as_deref(), Some("db.labels() :: (label :: STRING)"));
        assert!(d.properties.iter().any(|p| p.name == "description" && p.value == "List labels"));
        assert!(d.rows.is_none());
        let with_args = serde_json::json!({"name": "f", "argumentDescription": [{"name": "x", "type": "INTEGER", "description": "n"}]});
        assert_eq!(routine_detail(&s[1].reference, &with_args).rows.map(|r| r.rows.len()), Some(1));

        let users = vec![serde_json::json!({"user": "neo4j", "roles": ["admin", "PUBLIC"], "suspended": false, "passwordChangeRequired": true}), serde_json::json!({"user": "bob", "roles": [], "suspended": true})];
        let u = user_summaries(&users);
        assert_eq!(u[0].reference.name, "bob");
        assert_eq!(u[0].badge.as_deref(), Some("suspended"));
        assert_eq!(u[1].detail.as_deref(), Some("admin, PUBLIC, password change required"));
        let d = user_detail(&u[1].reference, users.first(), None, false);
        assert_eq!(d.actions.len(), 3);
        assert_eq!(d.actions[0].statement, "ALTER USER `neo4j` SET STATUS SUSPENDED");
        assert_eq!(user_detail(&u[1].reference, None, None, true).actions.len(), 1);

        let roles = vec![serde_json::json!({"role": "admin", "member": "neo4j"}), serde_json::json!({"role": "admin", "member": "ann"}), serde_json::json!({"role": "reader", "member": null})];
        let r = role_summaries(&roles);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].detail.as_deref(), Some("2 member(s): neo4j, ann"));
        assert!(r[1].detail.is_none());
        assert_eq!(role_detail(&r[0].reference, &["neo4j".into()], None).actions[0].statement, "DROP ROLE `admin`");
    }

    #[test]
    fn transactions_and_settings_map() {
        let rows = vec![
            serde_json::json!({"transactionId": "neo4j-transaction-12", "username": "neo4j", "currentQuery": "MATCH (n)\n  RETURN n", "status": "Running", "elapsedTime": "PT2.5S"}),
            serde_json::json!({"transaction_id": "7", "username": "mg", "query": ["MATCH (n) RETURN n", "RETURN 1"], "metadata": {}}),
        ];
        let s = transaction_summaries(&rows);
        assert_eq!(s[0].reference.name, "7");
        assert_eq!(s[0].detail.as_deref(), Some("mg · MATCH (n) RETURN n; RETURN 1"));
        assert_eq!(s[1].badge.as_deref(), Some("running"));
        assert_eq!(s[1].detail.as_deref(), Some("neo4j · 2.5 s · MATCH (n) RETURN n"));
        let d = transaction_detail(&s[1].reference, &rows[0]);
        assert_eq!(d.definition.as_deref(), Some("MATCH (n)\n  RETURN n"));
        assert_eq!(d.actions[0].statement, "TERMINATE TRANSACTIONS \"neo4j-transaction-12\"");
        assert!(looks_mutating("TERMINATE TRANSACTIONS \"x\""));
        assert!(looks_mutating("stop database `neo4j`"));
        assert_eq!(duration_text("PT0.123S"), "123 ms");
        assert_eq!(duration_text("125000"), "2m 5s");
        assert_eq!(duration_text("PT7200S"), "2h 0m");
        assert_eq!(duration_text("weird"), "weird");

        let cfg = vec![serde_json::json!({"name": "dbms.memory.heap.max_size", "value": "1G", "dynamic": false, "explicitlySet": true, "description": "Heap"}), serde_json::json!({"name": "tx.timeout", "current_value": "600", "default_value": "600"})];
        let s = setting_summaries(&cfg);
        assert_eq!(s[0].badge.as_deref(), Some("explicit"));
        assert_eq!(s[0].detail.as_deref(), Some("1G"));
        assert_eq!(s[1].detail.as_deref(), Some("600"));
        let d = setting_detail(&s[1].reference, &cfg[1]);
        assert_eq!(d.definition.as_deref(), Some("600"));
        assert!(d.properties.iter().any(|p| p.name == "default_value"));
    }

    #[test]
    fn stats_helpers_format() {
        let attrs = serde_json::json!({"HeapMemoryUsage": {"value": {"used": 1073741824.0, "max": 2147483648.0, "committed": 1610612736.0}}, "NonHeapMemoryUsage": {"value": {"used": 1024.0}}});
        let m = jmx_memory_stats(&attrs);
        assert_eq!(m.len(), 4);
        assert_eq!(m[0].value, "1.0 GB");
        assert_eq!(m[0].numeric, Some(1073741824.0));
        assert_eq!(jmx_uptime(&serde_json::json!({"Uptime": {"value": 90000.0}})).as_deref(), Some("1m 30s"));
        let (graph, storage) = storage_info_stats(&[
            serde_json::json!({"storage info": "vertex_count", "value": 10}),
            serde_json::json!({"storage info": "edge_count", "value": 4}),
            serde_json::json!({"storage info": "memory_res", "value": 2048}),
            serde_json::json!({"storage info": "storage_mode", "value": "IN_MEMORY_TRANSACTIONAL"}),
        ]);
        assert_eq!(graph.len(), 2);
        assert_eq!(storage[0].value, "2.0 KB");
        assert_eq!(storage[1].value, "IN_MEMORY_TRANSACTIONAL");
        assert_eq!(bytes_text(512.0), "512 B");
        assert_eq!(preview("a   b\nc", 3), "a b…");
        assert_eq!(cypher_string("a\"b"), "\"a\\\"b\"");
        let r = ObjectRef { kind: ObjectKind::RelationshipType, name: "KNOWS".into(), parent: Some("neo4j".into()) };
        let d = entity_detail(&r, Some(3), None, vec![]);
        assert_eq!(d.properties[0].value, "3");
        assert_eq!(d.actions[1].statement, "MATCH ()-[n:`KNOWS`]->() DELETE n");
    }

    fn resolved(engine: Engine, host: String) -> ResolvedConnection {
        let input = ConnectionInput {
            name: "live".into(),
            engine,
            environment: Environment::Local,
            read_only: false,
            host: Some(host),
            port: std::env::var("DBFREE_TEST_NEO4J_PORT").ok().and_then(|p| p.parse().ok()),
            database: None,
            username: std::env::var("DBFREE_TEST_NEO4J_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Disable,
        };
        ResolvedConnection { summary: ConnectionSummary::draft(&input, true), secret: std::env::var("DBFREE_TEST_NEO4J_PASSWORD").ok() }
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_NEO4J_HOST is set
    //        (e.g. `docker run --rm -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:5`).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(host) = std::env::var("DBFREE_TEST_NEO4J_HOST") else {
            return;
        };
        let graph = connect(&resolved(Engine::Neo4j, host)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        graph.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        let version = graph.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(!version.is_empty(), "version");
        graph
            .execute(
                "MATCH (n:DbfreeTest) DETACH DELETE n;
                 CREATE (a:DbfreeTest {name: 'ann', age: 30})-[:DBFREE_KNOWS {since: 2020}]->(b:DbfreeTest {name: 'bob', age: 25});
                 CREATE (:DbfreeTest {name: 'cyd', tags: ['x', 'y']})",
                10,
            )
            .await
            .unwrap_or_else(|e| panic!("setup: {e}"));
        let table = TableRef { schema: Some("neo4j".into()), name: "DbfreeTest".into() };
        let columns = graph.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["_id", "age", "name", "tags"]);
        let catalog = graph.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(catalog.schemas.iter().any(|s| s.tables.iter().any(|t| t.name == "DbfreeTest")));
        assert!(catalog.schemas.iter().any(|s| s.name == "relationships" && s.tables.iter().any(|t| t.name == "DBFREE_KNOWS")));
        let page = graph
            .fetch_page(
                &table,
                &PageQuery {
                    sort: vec![SortRule { column: "age".into(), desc: true }],
                    filters: vec![rule("name", FilterOp::Contains, "N")],
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap_or_else(|e| panic!("fetch_page: {e}"));
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0][2], Value::Text("ann".into()));
        assert_eq!(graph.count(&table, &[]).await.unwrap_or_default(), 3);
        assert_eq!(graph.count(&table, &[rule("age", FilterOp::IsNull, "")]).await.unwrap_or_default(), 1);
        let rel = TableRef { schema: Some("relationships".into()), name: "DBFREE_KNOWS".into() };
        let rels = graph.fetch_page(&rel, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 5 }).await.unwrap_or_else(|e| panic!("rels: {e}"));
        assert_eq!(rels.rows.len(), 1);
        assert_eq!(rels.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["_id", "_start", "_end", "since"]);
        let out = graph
            .execute("MATCH (a:DbfreeTest)-[r]->(b) RETURN a.name AS from, r, b.name AS to", 10)
            .await
            .unwrap_or_else(|e| panic!("execute: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => {
                assert_eq!(result.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["from", "r", "to"]);
                assert!(matches!(result.rows[0][1], Value::Json(_)));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        graph.execute("MATCH (n:DbfreeTest) DETACH DELETE n", 10).await.unwrap_or_else(|e| panic!("cleanup: {e}"));
    }
}
