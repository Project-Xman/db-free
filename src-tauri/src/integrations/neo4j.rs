// SOT: neo4j-integration, memgraph-integration, neo4rs-adapter, cypher, bolt-value-decoding, graph-label-catalog

use crate::error::{AppError, AppResult};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SortRule, SslMode, StatementResult, TableInfo, TableKind, TableRef,
    Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use neo4rs::{BoltType, ConfigBuilder, Graph, Query, Row};
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

// WHAT:  A read-only session refuses Cypher that starts a mutating clause.
fn looks_mutating(cypher: &str) -> bool {
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

#[async_trait]
impl Integration for Neo4jIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { namespaces: true, paging: true, fixed_columns: false, ..Capabilities::DOCUMENT }
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
