// SOT: redis-integration, redis-adapter, key-value-mapping, redis-command-parser, redis-object-explorer, redis-server-stats, redis-info-parser

use crate::error::{AppError, AppResult};
use crate::integrations::{Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats,
    SortRule, SslMode, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use redis::aio::MultiplexedConnection;
use redis::Client;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

// ============================================================================
// REDIS ADAPTER
//
// WHAT:  Maps a key-value store onto the engine-neutral `Integration` contract.
// WHY:   The UI (tables panel, grid, query tab) is written once against the
//        trait; Redis has to look like "one schema whose tables are keys".
// HOW:   catalog     = keys from SCAN (capped), one schema "db<N>"
//        columns     = fixed per key type (string/hash/list/set/zset/stream)
//        fetch_page  = whole key loaded, filters + sort applied client-side,
//                      then offset/limit sliced (Redis has no server-side paging)
//        execute     = one Redis command per line, tokenised like redis-cli
//        objects     = one admin command per kind (INFO keyspace, SCAN TYPE
//                      stream, XINFO, PUBSUB, FUNCTION LIST, ACL LIST, CONFIG
//                      GET, CLIENT LIST, SLOWLOG GET, CLUSTER NODES, INFO
//                      replication); commands a server lacks yield empty lists
//        stats       = one INFO call parsed into grouped figures
//        actions     = Redis command lines, run later through `execute`, so
//                      the guard's read-only / destructive gates apply
//        `redis` is the only vendor crate used, and only in this file.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI meta)
// ============================================================================

impl From<redis::RedisError> for AppError {
    fn from(err: redis::RedisError) -> Self {
        AppError::driver(err)
    }
}

const SCAN_BATCH: usize = 500;
const MAX_KEYS: usize = 5_000;
const MAX_STREAM_ENTRIES: usize = 5_000;
const DEFAULT_DATABASES: u32 = 16;

pub struct RedisIntegration {
    conn: MultiplexedConnection,
    db: i64,
    /// host:port — the node name when the server is not a cluster.
    addr: String,
}

// WHAT:  Percent-encodes a userinfo component so `:`/`@`/`/` in passwords survive.
fn encode_userinfo(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        let keep = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~');
        if keep {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn build_url(conn: &ResolvedConnection) -> (String, i64) {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("127.0.0.1");
    let port = s.port.unwrap_or(6379);
    let db: i64 = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .and_then(|d| d.parse().ok())
        .unwrap_or(0);
    let (scheme, fragment) = match s.ssl_mode {
        SslMode::Disable | SslMode::Prefer => ("redis", ""),
        // "require" = encrypted but without certificate verification (self-signed friendly).
        SslMode::Require => ("rediss", "#insecure"),
        SslMode::VerifyCa | SslMode::VerifyFull => ("rediss", ""),
    };
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    let pass = conn.secret.as_deref().filter(|p| !p.is_empty());
    let userinfo = match (user, pass) {
        (Some(u), Some(p)) => format!("{}:{}@", encode_userinfo(u), encode_userinfo(p)),
        (Some(u), None) => format!("{}@", encode_userinfo(u)),
        (None, Some(p)) => format!(":{}@", encode_userinfo(p)),
        (None, None) => String::new(),
    };
    (format!("{scheme}://{userinfo}{host}:{port}/{db}{fragment}"), db)
}

// WHAT:  `host:port` with the same defaults as the URL.
fn address(conn: &ResolvedConnection) -> String {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("127.0.0.1");
    format!("{host}:{}", s.port.unwrap_or(6379))
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let (url, db) = build_url(conn);
    let client = Client::open(url)?;
    let connection = client.get_multiplexed_async_connection().await?;
    Ok(Arc::new(RedisIntegration { conn: connection, db, addr: address(conn) }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Str,
    Hash,
    List,
    Set,
    ZSet,
    Stream,
}

impl KeyKind {
    fn parse(raw: &str) -> Option<KeyKind> {
        match raw {
            "string" => Some(KeyKind::Str),
            "hash" => Some(KeyKind::Hash),
            "list" => Some(KeyKind::List),
            "set" => Some(KeyKind::Set),
            "zset" => Some(KeyKind::ZSet),
            "stream" => Some(KeyKind::Stream),
            _ => None,
        }
    }

    // WHAT:  The fixed column set the grid shows for each key type.
    fn columns(self) -> Vec<ColumnInfo> {
        let col = |name: &str, data_type: &str, primary_key: bool, ordinal: u32| ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: false,
            primary_key,
            ordinal,
        };
        match self {
            KeyKind::Str => vec![col("value", "string", false, 1)],
            KeyKind::Hash => vec![col("field", "text", true, 1), col("value", "text", false, 2)],
            KeyKind::List => vec![col("index", "int", true, 1), col("value", "text", false, 2)],
            KeyKind::Set => vec![col("member", "text", true, 1)],
            KeyKind::ZSet => vec![col("member", "text", true, 1), col("score", "float", false, 2)],
            KeyKind::Stream => vec![col("id", "text", true, 1), col("fields", "json", false, 2)],
        }
    }
}

// WHAT:  Text → Value, promoting JSON objects/arrays so the inspector can tree-view them.
fn text_value(text: String) -> Value {
    let trimmed = text.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
            if json.is_object() || json.is_array() {
                return Value::Json(json);
            }
        }
    }
    Value::Text(text)
}

fn bulk_to_value(bytes: Vec<u8>) -> Value {
    match String::from_utf8(bytes) {
        Ok(text) => text_value(text),
        Err(err) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(err.into_bytes())),
    }
}

// WHAT:  Full RESP value → JSON, for nested replies that do not fit a scalar cell.
fn reply_to_json(reply: redis::Value) -> serde_json::Value {
    use serde_json::Value as J;
    match reply {
        redis::Value::Nil => J::Null,
        redis::Value::Int(i) => J::from(i),
        redis::Value::Double(f) => serde_json::Number::from_f64(f).map(J::Number).unwrap_or(J::Null),
        redis::Value::Boolean(b) => J::Bool(b),
        redis::Value::Okay => J::String("OK".into()),
        redis::Value::BulkString(bytes) => J::String(String::from_utf8_lossy(&bytes).into_owned()),
        redis::Value::SimpleString(s) => J::String(s),
        redis::Value::VerbatimString { text, .. } => J::String(text),
        redis::Value::BigNumber(bytes) => J::String(String::from_utf8_lossy(&bytes).into_owned()),
        redis::Value::Array(items) | redis::Value::Set(items) => J::Array(items.into_iter().map(reply_to_json).collect()),
        redis::Value::Map(pairs) => J::Object(
            pairs
                .into_iter()
                .map(|(k, v)| (json_key(reply_to_json(k)), reply_to_json(v)))
                .collect(),
        ),
        redis::Value::Attribute { data, .. } => reply_to_json(*data),
        redis::Value::Push { data, .. } => J::Array(data.into_iter().map(reply_to_json).collect()),
        redis::Value::ServerError(err) => J::String(format!("ERR {err}")),
        // `redis::Value` is #[non_exhaustive]; future variants degrade to their debug text.
        other => J::String(format!("{other:?}")),
    }
}

fn json_key(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

// WHAT:  One reply element → one grid cell.
fn reply_to_cell(reply: redis::Value) -> Value {
    match reply {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(i) => Value::Int(i),
        redis::Value::Double(f) => Value::Float(f),
        redis::Value::Boolean(b) => Value::Bool(b),
        redis::Value::Okay => Value::Text("OK".into()),
        redis::Value::BulkString(bytes) => bulk_to_value(bytes),
        redis::Value::SimpleString(s) => Value::Text(s),
        redis::Value::VerbatimString { text, .. } => Value::Text(text),
        redis::Value::BigNumber(bytes) => Value::Decimal(String::from_utf8_lossy(&bytes).into_owned()),
        redis::Value::ServerError(err) => Value::Text(format!("ERR {err}")),
        nested @ (redis::Value::Array(_)
        | redis::Value::Set(_)
        | redis::Value::Map(_)
        | redis::Value::Attribute { .. }
        | redis::Value::Push { .. }) => Value::Json(reply_to_json(nested)),
        other => Value::Unsupported(format!("{other:?}")),
    }
}

fn meta(names: &[&str]) -> Vec<ColumnMeta> {
    names.iter().map(|n| ColumnMeta { name: (*n).to_string(), type_name: "text".to_string() }).collect()
}

// WHAT:  Whole reply → one StatementResult, capped at `max_rows`.
fn reply_to_statement(reply: redis::Value, max_rows: usize) -> StatementResult {
    let (columns, mut rows): (Vec<ColumnMeta>, Vec<Vec<Value>>) = match reply {
        redis::Value::Array(items) | redis::Value::Set(items) => {
            (meta(&["reply"]), items.into_iter().map(|item| vec![reply_to_cell(item)]).collect())
        }
        redis::Value::Map(pairs) => (
            meta(&["field", "value"]),
            pairs.into_iter().map(|(k, v)| vec![reply_to_cell(k), reply_to_cell(v)]).collect(),
        ),
        redis::Value::Attribute { data, .. } => return reply_to_statement(*data, max_rows),
        scalar => (meta(&["reply"]), vec![vec![reply_to_cell(scalar)]]),
    };
    let truncated = rows.len() > max_rows;
    rows.truncate(max_rows);
    StatementResult::Rows { result: ResultSet { columns, rows, truncated } }
}

// WHAT:  redis-cli style tokenizer: whitespace-separated, "double" quotes with
//        \n \r \t \\ \" \xHH escapes, 'single' quotes with \' only, # comments.
pub fn parse_commands(script: &str) -> AppResult<Vec<Vec<String>>> {
    let mut commands = Vec::new();
    for (line_no, line) in script.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let tokens = tokenize(trimmed).map_err(|msg| AppError::invalid_input(format!("Line {}: {msg}", line_no + 1)))?;
        if !tokens.is_empty() {
            commands.push(tokens);
        }
    }
    Ok(commands)
}

fn tokenize(line: &str) -> Result<Vec<String>, String> {
    let chars: Vec<char> = line.chars().collect();
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_token = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '"' => {
                in_token = true;
                i += 1;
                loop {
                    let Some(&ch) = chars.get(i) else {
                        return Err("unterminated double quote".to_string());
                    };
                    match ch {
                        '"' => {
                            i += 1;
                            break;
                        }
                        '\\' => {
                            let Some(&esc) = chars.get(i + 1) else {
                                return Err("dangling backslash".to_string());
                            };
                            match esc {
                                'n' => current.push('\n'),
                                'r' => current.push('\r'),
                                't' => current.push('\t'),
                                'a' => current.push('\u{7}'),
                                'b' => current.push('\u{8}'),
                                'x' => {
                                    let hex: String = chars.get(i + 2..i + 4).map(|h| h.iter().collect()).unwrap_or_default();
                                    match u8::from_str_radix(&hex, 16) {
                                        Ok(byte) if hex.len() == 2 => {
                                            current.push(char::from(byte));
                                            i += 2;
                                        }
                                        _ => current.push('x'),
                                    }
                                }
                                other => current.push(other),
                            }
                            i += 2;
                        }
                        other => {
                            current.push(other);
                            i += 1;
                        }
                    }
                }
            }
            '\'' => {
                in_token = true;
                i += 1;
                loop {
                    let Some(&ch) = chars.get(i) else {
                        return Err("unterminated single quote".to_string());
                    };
                    if ch == '\'' {
                        i += 1;
                        break;
                    }
                    if ch == '\\' && chars.get(i + 1) == Some(&'\'') {
                        current.push('\'');
                        i += 2;
                        continue;
                    }
                    current.push(ch);
                    i += 1;
                }
            }
            c if c.is_whitespace() => {
                if in_token {
                    tokens.push(std::mem::take(&mut current));
                    in_token = false;
                }
                i += 1;
            }
            other => {
                in_token = true;
                current.push(other);
                i += 1;
            }
        }
    }
    if in_token {
        tokens.push(current);
    }
    Ok(tokens)
}

// ---------------------------------------------------------------------------
// Client-side filter / sort (Redis cannot do either server-side per key).
// ---------------------------------------------------------------------------

fn cell_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => s.clone(),
        Value::Json(j) => j.to_string(),
    }
}

fn compare_cells(a: &Value, b: &Value) -> Ordering {
    let (ta, tb) = (cell_text(a), cell_text(b));
    match (ta.parse::<f64>(), tb.parse::<f64>()) {
        (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
        _ => ta.cmp(&tb),
    }
}

fn column_index(columns: &[ColumnInfo], name: &str) -> AppResult<usize> {
    columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| AppError::invalid_input(format!("Unknown column \"{name}\" for this key.")))
}

fn matches_rule(cell: &Value, rule: &FilterRule) -> bool {
    let text = cell_text(cell);
    let needle = rule.value.trim();
    let lower = text.to_lowercase();
    let needle_lower = needle.to_lowercase();
    let ordering = || {
        let probe = Value::Text(needle.to_string());
        compare_cells(cell, &probe)
    };
    match rule.op {
        FilterOp::Eq => text == needle,
        FilterOp::Ne => text != needle,
        FilterOp::Gt => ordering() == Ordering::Greater,
        FilterOp::Gte => ordering() != Ordering::Less,
        FilterOp::Lt => ordering() == Ordering::Less,
        FilterOp::Lte => ordering() != Ordering::Greater,
        FilterOp::Contains => lower.contains(&needle_lower),
        FilterOp::StartsWith => lower.starts_with(&needle_lower),
        FilterOp::EndsWith => lower.ends_with(&needle_lower),
        FilterOp::In => needle.split(',').map(str::trim).any(|item| item == text),
        FilterOp::IsNull => matches!(cell, Value::Null) || text.is_empty(),
        FilterOp::IsNotNull => !matches!(cell, Value::Null) && !text.is_empty(),
    }
}

fn apply_filters(columns: &[ColumnInfo], rows: Vec<Vec<Value>>, filters: &[FilterRule]) -> AppResult<Vec<Vec<Value>>> {
    if filters.is_empty() {
        return Ok(rows);
    }
    let indexed: Vec<(usize, &FilterRule)> = filters
        .iter()
        .map(|f| column_index(columns, &f.column).map(|i| (i, f)))
        .collect::<AppResult<_>>()?;
    Ok(rows
        .into_iter()
        .filter(|row| indexed.iter().all(|(i, rule)| row.get(*i).is_some_and(|cell| matches_rule(cell, rule))))
        .collect())
}

fn apply_sort(columns: &[ColumnInfo], rows: &mut [Vec<Value>], sort: &[SortRule]) -> AppResult<()> {
    if sort.is_empty() {
        return Ok(());
    }
    let indexed: Vec<(usize, bool)> = sort
        .iter()
        .map(|s| column_index(columns, &s.column).map(|i| (i, s.desc)))
        .collect::<AppResult<_>>()?;
    rows.sort_by(|a, b| {
        for (i, desc) in &indexed {
            let ord = match (a.get(*i), b.get(*i)) {
                (Some(x), Some(y)) => compare_cells(x, y),
                _ => Ordering::Equal,
            };
            let ord = if *desc { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
    Ok(())
}

impl RedisIntegration {
    fn con(&self) -> MultiplexedConnection {
        self.conn.clone()
    }

    async fn key_kind(&self, key: &str) -> AppResult<KeyKind> {
        let mut con = self.con();
        let raw: String = redis::cmd("TYPE").arg(key).query_async(&mut con).await?;
        KeyKind::parse(&raw).ok_or_else(|| match raw.as_str() {
            "none" => AppError::not_found(format!("Key \"{key}\" does not exist.")),
            other => AppError::driver(format!("Unsupported key type \"{other}\" for \"{key}\".")),
        })
    }

    async fn scan_keys(&self) -> AppResult<Vec<String>> {
        self.scan(None).await
    }

    // WHAT:  Loads every entry of one key as grid rows, shaped per `KeyKind::columns`.
    async fn load_entries(&self, key: &str, kind: KeyKind) -> AppResult<Vec<Vec<Value>>> {
        let mut con = self.con();
        let rows = match kind {
            KeyKind::Str => {
                let value: Option<Vec<u8>> = redis::cmd("GET").arg(key).query_async(&mut con).await?;
                vec![vec![value.map(bulk_to_value).unwrap_or(Value::Null)]]
            }
            KeyKind::Hash => {
                let pairs: Vec<(String, Vec<u8>)> = redis::cmd("HGETALL").arg(key).query_async(&mut con).await?;
                pairs.into_iter().map(|(field, value)| vec![Value::Text(field), bulk_to_value(value)]).collect()
            }
            KeyKind::List => {
                let items: Vec<Vec<u8>> = redis::cmd("LRANGE").arg(key).arg(0).arg(-1).query_async(&mut con).await?;
                items
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| vec![Value::Int(i64::try_from(i).unwrap_or(i64::MAX)), bulk_to_value(value)])
                    .collect()
            }
            KeyKind::Set => {
                let mut members: Vec<String> = redis::cmd("SMEMBERS").arg(key).query_async(&mut con).await?;
                members.sort();
                members.into_iter().map(|m| vec![text_value(m)]).collect()
            }
            KeyKind::ZSet => {
                let pairs: Vec<(String, f64)> = redis::cmd("ZRANGE")
                    .arg(key)
                    .arg(0)
                    .arg(-1)
                    .arg("WITHSCORES")
                    .query_async(&mut con)
                    .await?;
                pairs.into_iter().map(|(member, score)| vec![text_value(member), Value::Float(score)]).collect()
            }
            KeyKind::Stream => {
                let reply: redis::Value = redis::cmd("XRANGE")
                    .arg(key)
                    .arg("-")
                    .arg("+")
                    .arg("COUNT")
                    .arg(MAX_STREAM_ENTRIES)
                    .query_async(&mut con)
                    .await?;
                stream_rows(reply)
            }
        };
        Ok(rows)
    }

    async fn entry_count(&self, key: &str, kind: KeyKind) -> AppResult<i64> {
        let mut con = self.con();
        let command = match kind {
            KeyKind::Str => return Ok(1),
            KeyKind::Hash => "HLEN",
            KeyKind::List => "LLEN",
            KeyKind::Set => "SCARD",
            KeyKind::ZSet => "ZCARD",
            KeyKind::Stream => "XLEN",
        };
        let n: i64 = redis::cmd(command).arg(key).query_async(&mut con).await?;
        Ok(n)
    }
}

// WHAT:  XRANGE reply → rows [id, {field: value}].
fn stream_rows(reply: redis::Value) -> Vec<Vec<Value>> {
    let entries = match reply {
        redis::Value::Array(items) => items,
        _ => return Vec::new(),
    };
    entries
        .into_iter()
        .filter_map(|entry| {
            let redis::Value::Array(mut parts) = entry else {
                return None;
            };
            if parts.len() < 2 {
                return None;
            }
            let fields = parts.pop()?;
            let id = parts.pop()?;
            let id_cell = reply_to_cell(id);
            let mut object = serde_json::Map::new();
            if let redis::Value::Array(flat) = fields {
                let mut iter = flat.into_iter();
                while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                    object.insert(json_key(reply_to_json(k)), reply_to_json(v));
                }
            } else if let redis::Value::Map(pairs) = fields {
                for (k, v) in pairs {
                    object.insert(json_key(reply_to_json(k)), reply_to_json(v));
                }
            }
            Some(vec![id_cell, Value::Json(serde_json::Value::Object(object))])
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Object explorer / admin / stats
//
// WHAT:  Pure parsers for the admin replies (INFO, CLIENT LIST, CLUSTER NODES,
//        ACL LIST, SLOWLOG GET, XINFO, CONFIG GET) and the builders that turn
//        them into ObjectSummary / ObjectDetail / StatGroup.
// WHY:   Kept as free functions so they are unit-tested offline; the async
//        methods further down only fetch and delegate. Every action statement
//        is a Redis command line, i.e. this adapter's `execute` language, so it
//        passes the guard (read-only lock, destructive confirmation, history).
// ---------------------------------------------------------------------------

const MAX_OBJECTS: usize = 2_000;
const STREAM_PREVIEW: usize = 20;
const SLOWLOG_ENTRIES: usize = 128;
const DETAIL_PREVIEW: usize = 80;
const XTRIM_DEFAULT: usize = 1_000;
const NUMSUB_BATCH: usize = 100;

// WHAT:  `INFO` text → flat field map. Section headers (`# Server`) and blank
//        lines are skipped; field names are unique across the default sections.
fn parse_info(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| l.split_once(':').map(|(k, v)| (k.trim().to_string(), v.trim().to_string())))
        .collect()
}

// WHAT:  `k=v,k=v` (keyspace lines, replica lines) → map.
fn parse_kv_list(text: &str) -> BTreeMap<String, String> {
    text.split(',')
        .filter_map(|p| p.split_once('=').map(|(k, v)| (k.trim().to_string(), v.trim().to_string())))
        .collect()
}

fn info_num(info: &BTreeMap<String, String>, key: &str) -> Option<f64> {
    info.get(key).and_then(|v| v.parse::<f64>().ok())
}

fn info_int(info: &BTreeMap<String, String>, key: &str) -> Option<i64> {
    info.get(key).and_then(|v| v.parse::<i64>().ok())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyspaceEntry {
    db: u32,
    keys: i64,
    expires: i64,
    avg_ttl: i64,
}

// WHAT:  `dbN:keys=…,expires=…,avg_ttl=…` lines of INFO keyspace, by db index.
//        Databases without keys are not reported by the server.
fn parse_keyspace(info: &BTreeMap<String, String>) -> Vec<KeyspaceEntry> {
    let mut out: Vec<KeyspaceEntry> = info
        .iter()
        .filter_map(|(k, v)| {
            let db = k.strip_prefix("db")?.parse::<u32>().ok()?;
            let fields = parse_kv_list(v);
            let num = |name: &str| fields.get(name).and_then(|n| n.parse::<i64>().ok()).unwrap_or(0);
            Some(KeyspaceEntry { db, keys: num("keys"), expires: num("expires"), avg_ttl: num("avg_ttl") })
        })
        .collect();
    out.sort_by_key(|e| e.db);
    out
}

// WHAT:  `CLIENT LIST` → one field map per client (`id=3 addr=… cmd=client|list`).
fn parse_client_list(text: &str) -> Vec<BTreeMap<String, String>> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|line| {
            line.split_whitespace()
                .filter_map(|tok| tok.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClusterNode {
    id: String,
    addr: String,
    flags: Vec<String>,
    master: Option<String>,
    config_epoch: String,
    link_state: String,
    slots: Vec<String>,
}

impl ClusterNode {
    fn role(&self) -> &'static str {
        if self.flags.iter().any(|f| f == "master") {
            "master"
        } else if self.flags.iter().any(|f| f == "slave") {
            "replica"
        } else {
            "unknown"
        }
    }
    fn slots_text(&self) -> String {
        if self.slots.is_empty() {
            "no slots".to_string()
        } else {
            self.slots.join(" ")
        }
    }
}

// WHAT:  `CLUSTER NODES` → nodes. One per line:
//        `<id> <ip:port@cport[,host]> <flags> <master|-> <ping> <pong> <epoch> <link> <slots…>`.
fn parse_cluster_nodes(text: &str) -> Vec<ClusterNode> {
    text.lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let id = parts.next()?.to_string();
            let raw_addr = parts.next()?;
            let addr = raw_addr.split(['@', ',']).next().unwrap_or(raw_addr).to_string();
            let flags: Vec<String> = parts.next()?.split(',').filter(|f| !f.is_empty()).map(str::to_string).collect();
            let master = parts.next().filter(|m| *m != "-").map(str::to_string);
            let _ping_sent = parts.next();
            let _pong_recv = parts.next();
            let config_epoch = parts.next().unwrap_or("").to_string();
            let link_state = parts.next().unwrap_or("").to_string();
            let slots = parts.map(str::to_string).collect();
            Some(ClusterNode { id, addr, flags, master, config_epoch, link_state, slots })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AclUser {
    name: String,
    enabled: bool,
    rules: String,
}

// WHAT:  One `ACL LIST` line (`user <name> on nopass ~* &* +@all`) → user.
//        Password hashes (`#<sha256>`) are masked so no credential material
//        reaches the UI.
fn parse_acl_line(line: &str) -> Option<AclUser> {
    let mut parts = line.split_whitespace();
    if parts.next()? != "user" {
        return None;
    }
    let name = parts.next()?.to_string();
    let rest: Vec<String> = parts
        .map(|tok| if tok.starts_with('#') && tok.len() > 1 { "#<hash>".to_string() } else { tok.to_string() })
        .collect();
    let enabled = rest.iter().any(|t| t == "on");
    Some(AclUser { name, enabled, rules: rest.join(" ") })
}

// WHAT:  Config values that are credentials are never shown.
fn mask_config(name: &str, value: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if !value.is_empty() && (lower.contains("pass") || lower.contains("auth")) {
        "••••••".to_string()
    } else {
        value.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SlowEntry {
    id: i64,
    timestamp: i64,
    duration_us: i64,
    argv: Vec<String>,
    client: String,
    name: String,
}

impl SlowEntry {
    fn command_line(&self) -> String {
        self.argv.join(" ")
    }
}

// WHAT:  `SLOWLOG GET` reply → entries. Each is
//        `[id, unix seconds, µs, argv[], client addr, client name]` (the last
//        two exist since 4.0).
fn parse_slowlog(reply: redis::Value) -> Vec<SlowEntry> {
    let items = match reply {
        redis::Value::Array(items) => items,
        _ => return Vec::new(),
    };
    items
        .into_iter()
        .filter_map(|entry| {
            let serde_json::Value::Array(fields) = reply_to_json(entry) else {
                return None;
            };
            let int = |i: usize| fields.get(i).and_then(serde_json::Value::as_i64);
            let text = |i: usize| fields.get(i).map(json_text).unwrap_or_default();
            let argv = match fields.get(3) {
                Some(serde_json::Value::Array(a)) => a.iter().map(json_text).collect(),
                _ => Vec::new(),
            };
            Some(SlowEntry {
                id: int(0)?,
                timestamp: int(1).unwrap_or(0),
                duration_us: int(2).unwrap_or(0),
                argv,
                client: text(4),
                name: text(5),
            })
        })
        .collect()
}

fn json_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn json_int(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str().and_then(|s| s.parse().ok()))
}

type JsonObject = serde_json::Map<String, serde_json::Value>;

// WHAT:  A map-shaped reply (RESP3 Map, or the RESP2 flat key/value array that
//        XINFO, CONFIG GET, ACL GETUSER and FUNCTION LIST return) → JSON object.
fn reply_to_object(reply: redis::Value) -> JsonObject {
    let mut object = JsonObject::new();
    match reply {
        redis::Value::Map(pairs) => {
            for (k, v) in pairs {
                object.insert(json_key(reply_to_json(k)), reply_to_json(v));
            }
        }
        redis::Value::Array(items) => {
            let mut iter = items.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                object.insert(json_key(reply_to_json(k)), reply_to_json(v));
            }
        }
        redis::Value::Attribute { data, .. } => return reply_to_object(*data),
        _ => {}
    }
    object
}

// WHAT:  An array of map-shaped replies (XINFO GROUPS / CONSUMERS, FUNCTION LIST).
fn reply_to_objects(reply: redis::Value) -> Vec<JsonObject> {
    match reply {
        redis::Value::Array(items) | redis::Value::Set(items) => items.into_iter().map(reply_to_object).collect(),
        redis::Value::Attribute { data, .. } => reply_to_objects(*data),
        _ => Vec::new(),
    }
}

// WHAT:  Bulk / simple string replies (INFO, CLIENT LIST, CLUSTER NODES) → text.
fn reply_text(reply: redis::Value) -> String {
    json_text(&reply_to_json(reply))
}

fn reply_strings(reply: redis::Value) -> Vec<String> {
    match reply_to_json(reply) {
        serde_json::Value::Array(items) => items.iter().map(json_text).collect(),
        serde_json::Value::Null => Vec::new(),
        other => vec![json_text(&other)],
    }
}

fn field_text(object: &JsonObject, name: &str) -> String {
    object.get(name).map(json_text).unwrap_or_default()
}

fn field_int(object: &JsonObject, name: &str) -> Option<i64> {
    object.get(name).and_then(json_int)
}

// WHAT:  The id of an XINFO STREAM `first-entry` / `last-entry` (`[id, [f, v…]]`).
fn entry_id(value: Option<&serde_json::Value>) -> String {
    value.and_then(serde_json::Value::as_array).and_then(|a| a.first()).map(json_text).unwrap_or_default()
}

fn pretty_json(object: &JsonObject) -> String {
    serde_json::to_string_pretty(&serde_json::Value::Object(object.clone())).unwrap_or_default()
}

// WHAT:  Seconds → `3d 4h 5m 6s`.
fn format_duration(secs: f64) -> String {
    let total = secs.max(0.0).round() as u64;
    let (days, hours, minutes, seconds) = (total / 86_400, (total % 86_400) / 3_600, (total % 3_600) / 60, total % 60);
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 || days > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 || hours > 0 || days > 0 {
        parts.push(format!("{minutes}m"));
    }
    parts.push(format!("{seconds}s"));
    parts.join(" ")
}

// WHAT:  Unix seconds → ISO-8601 (UTC); the raw number when out of range.
fn format_unix(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0).map(|d| d.to_rfc3339()).unwrap_or_else(|| ts.to_string())
}

fn mib(bytes: f64) -> f64 {
    (bytes / 1_048_576.0 * 100.0).round() / 100.0
}

fn preview(text: &str, max: usize) -> String {
    let flat = text.replace(['\n', '\r'], " ");
    if flat.chars().count() > max {
        format!("{}…", flat.chars().take(max).collect::<String>())
    } else {
        flat
    }
}

// WHAT:  A key / argument written so the command tokenizer reads it back
//        verbatim (double-quoted when it holds whitespace, quotes or backslashes).
fn quote_arg(raw: &str) -> String {
    if !raw.is_empty() && !raw.chars().any(|c| c.is_whitespace() || matches!(c, '"' | '\'' | '\\')) {
        return raw.to_string();
    }
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for c in raw.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

fn result_set(columns: &[&str], rows: Vec<Vec<Value>>) -> ResultSet {
    ResultSet { columns: meta(columns), rows, truncated: false }
}

fn int_cell(value: Option<i64>) -> Value {
    value.map(Value::Int).unwrap_or(Value::Null)
}

// WHAT:  Errors that mean "this server does not have that command" (older
//        Redis, Dragonfly, cluster support off) rather than a real failure.
fn is_unsupported(err: &AppError) -> bool {
    let m = err.message().to_ascii_lowercase();
    m.contains("unknown command")
        || m.contains("unknown subcommand")
        || m.contains("cluster support disabled")
        || m.contains("not supported")
        || m.contains("syntax error")
        || m.contains("wrong number of arguments")
}

fn group(title: &str, stats: Vec<Stat>) -> StatGroup {
    StatGroup { title: title.to_string(), stats }
}

fn push_number(stats: &mut Vec<Stat>, label: &str, value: Option<f64>, unit: Option<&str>) {
    if let Some(v) = value {
        stats.push(Stat::number(label, v, unit));
    }
}

fn push_bytes(stats: &mut Vec<Stat>, label: &str, bytes: Option<f64>) {
    if let Some(b) = bytes {
        stats.push(Stat::number(label, mib(b), Some("MiB")).with_hint(format!("{} bytes", format_number(b))));
    }
}

// WHAT:  INFO fields (+ `maxclients` from CONFIG) → the Stats tab groups.
fn stats_groups(info: &BTreeMap<String, String>, maxclients: Option<f64>) -> Vec<StatGroup> {
    let num = |key: &str| info_num(info, key);
    let text = |key: &str| info.get(key).cloned().unwrap_or_default();
    let flag = |key: &str| text(key) == "1";

    let mut server = vec![Stat::text("Version", text("redis_version")), Stat::text("Mode", text("redis_mode"))];
    if let Some(up) = num("uptime_in_seconds") {
        server.push(Stat::text("Uptime", format_duration(up)).with_hint(format!("{} s", format_number(up))));
    }
    if info.contains_key("os") {
        server.push(Stat::text("OS", text("os")));
    }

    let mut clients = Vec::new();
    push_number(&mut clients, "Connected", num("connected_clients"), None);
    push_number(&mut clients, "Blocked", num("blocked_clients"), None);
    push_number(&mut clients, "Tracking", num("tracking_clients"), None);
    push_number(&mut clients, "Max clients", maxclients, None);

    let mut memory = Vec::new();
    push_bytes(&mut memory, "Used", num("used_memory"));
    push_bytes(&mut memory, "Peak", num("used_memory_peak"));
    push_bytes(&mut memory, "RSS", num("used_memory_rss"));
    push_number(&mut memory, "Fragmentation", num("mem_fragmentation_ratio"), Some("ratio"));
    match num("maxmemory") {
        Some(limit) if limit > 0.0 => push_bytes(&mut memory, "Max memory", Some(limit)),
        Some(_) => memory.push(Stat::text("Max memory", "unlimited")),
        None => {}
    }
    if info.contains_key("maxmemory_policy") {
        memory.push(Stat::text("Eviction policy", text("maxmemory_policy")));
    }
    push_number(&mut memory, "Evicted keys", num("evicted_keys"), None);
    push_number(&mut memory, "Expired keys", num("expired_keys"), None);

    let mut throughput = Vec::new();
    push_number(&mut throughput, "Ops / sec", num("instantaneous_ops_per_sec"), Some("ops/s"));
    push_number(&mut throughput, "Commands processed", num("total_commands_processed"), None);
    push_number(&mut throughput, "Connections received", num("total_connections_received"), None);
    push_number(&mut throughput, "Keyspace hits", num("keyspace_hits"), None);
    push_number(&mut throughput, "Keyspace misses", num("keyspace_misses"), None);
    if let (Some(hits), Some(misses)) = (num("keyspace_hits"), num("keyspace_misses")) {
        if hits + misses > 0.0 {
            throughput.push(Stat::number("Hit ratio", (hits / (hits + misses) * 10_000.0).round() / 100.0, Some("%")));
        }
    }
    push_bytes(&mut throughput, "Network in", num("total_net_input_bytes"));
    push_bytes(&mut throughput, "Network out", num("total_net_output_bytes"));

    let mut persistence = Vec::new();
    if let Some(ts) = info_int(info, "rdb_last_save_time") {
        persistence.push(Stat::text("Last RDB save", format_unix(ts)));
    }
    push_number(&mut persistence, "Changes since save", num("rdb_changes_since_last_save"), None);
    if info.contains_key("rdb_bgsave_in_progress") {
        persistence.push(Stat::text("RDB save", if flag("rdb_bgsave_in_progress") { "in progress" } else { "idle" }));
    }
    if info.contains_key("aof_enabled") {
        persistence.push(Stat::text("AOF", if flag("aof_enabled") { "on" } else { "off" }));
    }
    if info.contains_key("aof_rewrite_in_progress") {
        persistence.push(Stat::text("AOF rewrite", if flag("aof_rewrite_in_progress") { "in progress" } else { "idle" }));
    }

    let mut replication = Vec::new();
    if info.contains_key("role") {
        replication.push(Stat::text("Role", text("role")));
    }
    push_number(&mut replication, "Replicas", num("connected_slaves"), None);
    push_number(&mut replication, "Replication offset", num("master_repl_offset"), None);
    if info.contains_key("master_link_status") {
        replication.push(Stat::text("Master link", text("master_link_status")));
        replication.push(Stat::text("Master", format!("{}:{}", text("master_host"), text("master_port"))));
    }

    let keyspace = parse_keyspace(info);
    let keys: i64 = keyspace.iter().map(|e| e.keys).sum();
    let expiring: i64 = keyspace.iter().map(|e| e.expires).sum();
    let keyspace_stats = vec![
        Stat::number("Keys", keys as f64, None),
        Stat::number("Expiring", expiring as f64, None),
        Stat::number("Databases in use", keyspace.len() as f64, None),
    ];

    vec![
        group("Server", server),
        group("Clients", clients),
        group("Memory", memory),
        group("Throughput", throughput),
        group("Persistence", persistence),
        group("Replication", replication),
        group("Keyspace", keyspace_stats),
    ]
    .into_iter()
    .filter(|g| !g.stats.is_empty())
    .collect()
}

impl RedisIntegration {
    // WHAT:  One command from string parts → raw reply.
    async fn raw(&self, parts: &[&str]) -> AppResult<redis::Value> {
        let Some((name, args)) = parts.split_first() else {
            return Err(AppError::invalid_input("Empty command."));
        };
        let mut cmd = redis::cmd(name);
        for arg in args {
            cmd.arg(*arg);
        }
        let mut con = self.con();
        Ok(cmd.query_async(&mut con).await?)
    }

    // WHAT:  Same, but a command this server does not know yields Ok(None), so
    //        Redis 5, Dragonfly or a non-cluster node degrade to "nothing here".
    async fn tolerant(&self, parts: &[&str]) -> AppResult<Option<redis::Value>> {
        match self.raw(parts).await {
            Ok(value) => Ok(Some(value)),
            Err(err) if is_unsupported(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn info(&self, section: Option<&str>) -> AppResult<BTreeMap<String, String>> {
        let reply = match section {
            Some(s) => self.raw(&["INFO", s]).await?,
            None => self.raw(&["INFO"]).await?,
        };
        Ok(parse_info(&reply_text(reply)))
    }

    async fn config_value(&self, name: &str) -> AppResult<Option<String>> {
        let Some(reply) = self.tolerant(&["CONFIG", "GET", name]).await? else {
            return Ok(None);
        };
        Ok(reply_to_object(reply).get(name).map(json_text))
    }

    // WHAT:  SCAN the keyspace (capped), optionally by TYPE (Redis 6+).
    async fn scan(&self, type_filter: Option<&str>) -> AppResult<Vec<String>> {
        let mut con = self.con();
        let mut cursor: u64 = 0;
        let mut keys: Vec<String> = Vec::new();
        loop {
            let mut cmd = redis::cmd("SCAN");
            cmd.arg(cursor).arg("MATCH").arg("*").arg("COUNT").arg(SCAN_BATCH);
            if let Some(kind) = type_filter {
                cmd.arg("TYPE").arg(kind);
            }
            let (next, batch): (u64, Vec<String>) = cmd.query_async(&mut con).await?;
            keys.extend(batch);
            cursor = next;
            if cursor == 0 || keys.len() >= MAX_KEYS {
                break;
            }
        }
        keys.sort();
        keys.dedup();
        keys.truncate(MAX_KEYS);
        Ok(keys)
    }

    async fn stream_keys(&self) -> AppResult<Vec<String>> {
        match self.scan(Some("stream")).await {
            Ok(keys) => Ok(keys),
            // Redis < 6 has no TYPE filter: classify the (capped) key list instead.
            Err(err) if is_unsupported(&err) => {
                let keys = self.scan(None).await?;
                let kinds = self.pipeline_strings("TYPE", &keys).await?;
                Ok(keys.into_iter().zip(kinds).filter(|(_, kind)| kind == "stream").map(|(key, _)| key).collect())
            }
            Err(err) => Err(err),
        }
    }

    // WHAT:  `<command> <key>` for every key in one pipeline, replies as text.
    async fn pipeline_strings(&self, command: &str, keys: &[String]) -> AppResult<Vec<String>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.cmd(command).arg(key);
        }
        let mut con = self.con();
        let replies: Vec<redis::Value> = pipe.query_async(&mut con).await?;
        Ok(replies.into_iter().map(reply_text).collect())
    }

    // ---- objects per kind --------------------------------------------------

    async fn database_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let keyspace = parse_keyspace(&self.info(Some("keyspace")).await?);
        let mut out: Vec<ObjectSummary> = keyspace
            .iter()
            .map(|e| {
                let mut summary = ObjectSummary::new(ObjectKind::Database, format!("db{}", e.db), None)
                    .with_detail(format!("{} keys · {} expiring", format_number(e.keys as f64), format_number(e.expires as f64)));
                if i64::from(e.db) == self.db {
                    summary = summary.with_badge("current");
                }
                summary
            })
            .collect();
        if !keyspace.iter().any(|e| i64::from(e.db) == self.db) {
            out.push(ObjectSummary::new(ObjectKind::Database, format!("db{}", self.db), None).with_detail("empty").with_badge("current"));
        }
        out.sort_by_key(|s| s.reference.name.trim_start_matches("db").parse::<u32>().unwrap_or(u32::MAX));
        Ok(out)
    }

    async fn stream_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut keys = self.stream_keys().await?;
        keys.truncate(MAX_OBJECTS);
        let lengths = self.pipeline_strings("XLEN", &keys).await?;
        Ok(keys
            .into_iter()
            .zip(lengths.into_iter().chain(std::iter::repeat(String::new())))
            .map(|(key, len)| {
                let detail = len.parse::<f64>().map(|n| format!("{} entries", format_number(n))).unwrap_or_default();
                ObjectSummary::new(ObjectKind::Stream, key, None).with_detail(detail)
            })
            .collect())
    }

    async fn groups_of(&self, stream: &str) -> AppResult<Vec<ObjectSummary>> {
        let groups = reply_to_objects(self.raw(&["XINFO", "GROUPS", stream]).await?);
        let mut out: Vec<ObjectSummary> = groups
            .iter()
            .map(|g| {
                let consumers = field_int(g, "consumers").unwrap_or(0);
                let pending = field_int(g, "pending").unwrap_or(0);
                let lag = field_int(g, "lag").map(|l| format_number(l as f64)).unwrap_or_else(|| "?".to_string());
                ObjectSummary::new(ObjectKind::ConsumerGroup, field_text(g, "name"), Some(stream.to_string()))
                    .with_detail(format!("{consumers} consumers · {} pending · lag {lag}", format_number(pending as f64)))
                    .with_badge(if consumers > 0 { "active" } else { "idle" })
            })
            .collect();
        out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        Ok(out)
    }

    async fn group_objects(&self, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        if let Some(stream) = parent {
            return self.groups_of(stream).await;
        }
        let mut out = Vec::new();
        for stream in self.stream_keys().await? {
            out.extend(self.groups_of(&stream).await?);
            if out.len() >= MAX_OBJECTS {
                break;
            }
        }
        Ok(out)
    }

    // WHAT:  Channel → subscriber count, `PUBSUB NUMSUB` in batches.
    async fn subscriber_counts(&self, subcommand: &str, channels: &[String]) -> AppResult<BTreeMap<String, i64>> {
        let mut counts = BTreeMap::new();
        for chunk in channels.chunks(NUMSUB_BATCH) {
            let mut parts = vec!["PUBSUB", subcommand];
            parts.extend(chunk.iter().map(String::as_str));
            if let Some(reply) = self.tolerant(&parts).await? {
                for (name, count) in reply_to_object(reply) {
                    counts.insert(name, json_int(&count).unwrap_or(0));
                }
            }
        }
        Ok(counts)
    }

    async fn channel_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut out = Vec::new();
        for (list, numsub, badge) in [("CHANNELS", "NUMSUB", None), ("SHARDCHANNELS", "SHARDNUMSUB", Some("shard"))] {
            let Some(reply) = self.tolerant(&["PUBSUB", list]).await? else {
                continue;
            };
            let mut channels = reply_strings(reply);
            channels.sort();
            channels.truncate(MAX_OBJECTS);
            let counts = self.subscriber_counts(numsub, &channels).await?;
            for channel in channels {
                let subscribers = counts.get(&channel).copied().unwrap_or(0);
                let mut summary = ObjectSummary::new(ObjectKind::Channel, channel, None).with_detail(format!("{subscribers} subscribers"));
                if let Some(b) = badge {
                    summary = summary.with_badge(b);
                }
                out.push(summary);
            }
        }
        Ok(out)
    }

    async fn function_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let Some(reply) = self.tolerant(&["FUNCTION", "LIST"]).await? else {
            return Ok(Vec::new());
        };
        let mut out: Vec<ObjectSummary> = reply_to_objects(reply)
            .iter()
            .map(|lib| {
                let functions: Vec<String> = match lib.get("functions") {
                    Some(serde_json::Value::Array(items)) => items.iter().filter_map(|f| f.get("name")).map(json_text).collect(),
                    _ => Vec::new(),
                };
                ObjectSummary::new(ObjectKind::Function, field_text(lib, "library_name"), None)
                    .with_detail(format!("{} functions: {}", functions.len(), preview(&functions.join(", "), DETAIL_PREVIEW)))
                    .with_badge(field_text(lib, "engine").to_ascii_lowercase())
            })
            .collect();
        out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        Ok(out)
    }

    async fn user_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let Some(reply) = self.tolerant(&["ACL", "LIST"]).await? else {
            return Ok(Vec::new());
        };
        let mut out: Vec<ObjectSummary> = reply_strings(reply)
            .iter()
            .filter_map(|line| parse_acl_line(line))
            .map(|u| {
                ObjectSummary::new(ObjectKind::User, u.name, None)
                    .with_detail(preview(&u.rules, DETAIL_PREVIEW))
                    .with_badge(if u.enabled { "on" } else { "off" })
            })
            .collect();
        out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        Ok(out)
    }

    async fn setting_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let Some(reply) = self.tolerant(&["CONFIG", "GET", "*"]).await? else {
            return Ok(Vec::new());
        };
        let config: BTreeMap<String, String> = reply_to_object(reply).iter().map(|(k, v)| (k.clone(), json_text(v))).collect();
        Ok(config
            .into_iter()
            .take(MAX_OBJECTS)
            .map(|(name, value)| {
                let shown = mask_config(&name, &value);
                ObjectSummary::new(ObjectKind::Setting, name, None).with_detail(preview(&shown, DETAIL_PREVIEW))
            })
            .collect())
    }

    async fn session_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let text = reply_text(self.raw(&["CLIENT", "LIST"]).await?);
        let mut clients = parse_client_list(&text);
        clients.sort_by_key(|c| c.get("id").and_then(|id| id.parse::<i64>().ok()).unwrap_or(i64::MAX));
        Ok(clients
            .iter()
            .take(MAX_OBJECTS)
            .map(|c| {
                let get = |k: &str| c.get(k).cloned().unwrap_or_default();
                let mut summary = ObjectSummary::new(ObjectKind::Session, get("id"), None)
                    .with_detail(format!("{} · {} · idle {}s", get("addr"), get("cmd"), get("idle")));
                let badge = [get("name"), get("user")].into_iter().find(|s| !s.is_empty());
                if let Some(b) = badge {
                    summary = summary.with_badge(b);
                }
                summary
            })
            .collect())
    }

    async fn slowlog_entries(&self) -> AppResult<Vec<SlowEntry>> {
        Ok(parse_slowlog(self.raw(&["SLOWLOG", "GET", &SLOWLOG_ENTRIES.to_string()]).await?))
    }

    async fn slowlog_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        // Server order is newest first; kept, since that is how a slow log reads.
        Ok(self
            .slowlog_entries()
            .await?
            .iter()
            .map(|e| {
                let mut summary = ObjectSummary::new(ObjectKind::SlowQuery, e.id.to_string(), None)
                    .with_detail(format!("{} µs · {}", format_number(e.duration_us as f64), preview(&e.command_line(), DETAIL_PREVIEW)));
                if let Some(verb) = e.argv.first() {
                    summary = summary.with_badge(verb.to_ascii_uppercase());
                }
                summary
            })
            .collect())
    }

    async fn cluster_nodes(&self) -> AppResult<Option<Vec<ClusterNode>>> {
        Ok(self.tolerant(&["CLUSTER", "NODES"]).await?.map(|reply| parse_cluster_nodes(&reply_text(reply))))
    }

    async fn node_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        if let Some(mut nodes) = self.cluster_nodes().await? {
            nodes.sort_by(|a, b| a.addr.cmp(&b.addr));
            return Ok(nodes
                .iter()
                .map(|n| {
                    let myself = if n.flags.iter().any(|f| f == "myself") { " · this node" } else { "" };
                    ObjectSummary::new(ObjectKind::Node, n.addr.clone(), None)
                        .with_detail(format!("{} · {}{myself}", n.slots_text(), n.link_state))
                        .with_badge(n.role())
                })
                .collect());
        }
        let info = self.info(Some("server")).await?;
        let get = |k: &str| info.get(k).cloned().unwrap_or_default();
        let uptime = info_num(&info, "uptime_in_seconds").map(format_duration).unwrap_or_default();
        Ok(vec![ObjectSummary::new(ObjectKind::Node, self.addr.clone(), None)
            .with_detail(format!("Redis {} · up {uptime}", get("redis_version")))
            .with_badge(get("redis_mode"))])
    }

    async fn replica_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let info = self.info(Some("replication")).await?;
        let mut out = Vec::new();
        for (key, value) in &info {
            if !key.starts_with("slave") || !key[5..].chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            let fields = parse_kv_list(value);
            let get = |k: &str| fields.get(k).cloned().unwrap_or_default();
            out.push(
                ObjectSummary::new(ObjectKind::Replica, format!("{}:{}", get("ip"), get("port")), None)
                    .with_detail(format!("offset {} · lag {}s", get("offset"), get("lag")))
                    .with_badge(get("state")),
            );
        }
        if let (Some(host), Some(port)) = (info.get("master_host"), info.get("master_port")) {
            let link = info.get("master_link_status").cloned().unwrap_or_default();
            let offset = info.get("slave_repl_offset").cloned().unwrap_or_default();
            out.push(
                ObjectSummary::new(ObjectKind::Replica, format!("{host}:{port}"), None)
                    .with_detail(format!("link {link} · offset {offset}"))
                    .with_badge("master"),
            );
        }
        out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        Ok(out)
    }

    // ---- details per kind --------------------------------------------------

    async fn database_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let index = reference
            .name
            .trim_start_matches("db")
            .parse::<u32>()
            .map_err(|_| AppError::invalid_input(format!("\"{}\" is not a database name (expected db<N>).", reference.name)))?;
        let keyspace = parse_keyspace(&self.info(Some("keyspace")).await?);
        let entry = keyspace.iter().find(|e| e.db == index);
        let current = i64::from(index) == self.db;
        let mut detail = ObjectDetail::empty(reference)
            .property("index", index.to_string())
            .property("keys", entry.map(|e| format_number(e.keys as f64)).unwrap_or_else(|| "0".to_string()))
            .property("expiring keys", entry.map(|e| format_number(e.expires as f64)).unwrap_or_else(|| "0".to_string()))
            .property("average TTL", entry.map(|e| format!("{} ms", format_number(e.avg_ttl as f64))).unwrap_or_else(|| "—".to_string()))
            .property("session database", if current { "yes" } else { "no" });
        if current {
            detail = detail.action(ObjectAction::destructive("flushdb", "Flush this database (FLUSHDB)", "FLUSHDB"));
        }
        Ok(detail)
    }

    async fn stream_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let object = reply_to_object(self.raw(&["XINFO", "STREAM", name]).await?);
        let mut detail = ObjectDetail::empty(reference).definition(pretty_json(&object), CodeLanguage::Json);
        for field in ["length", "radix-tree-keys", "radix-tree-nodes", "last-generated-id", "max-deleted-entry-id", "entries-added", "recorded-first-entry-id", "groups"] {
            if let Some(v) = object.get(field) {
                detail = detail.property(field, json_text(v));
            }
        }
        detail = detail.property("first-entry", entry_id(object.get("first-entry"))).property("last-entry", entry_id(object.get("last-entry")));
        detail.columns = KeyKind::Stream.columns();
        let latest = self.raw(&["XREVRANGE", name, "+", "-", "COUNT", &STREAM_PREVIEW.to_string()]).await?;
        detail.rows = Some(result_set(&["id", "fields"], stream_rows(latest)));
        detail.children = self.groups_of(name).await?;
        let key = quote_arg(name);
        detail.actions = vec![
            ObjectAction::destructive("xtrim", &format!("Trim to {} entries (XTRIM MAXLEN)", format_number(XTRIM_DEFAULT as f64)), format!("XTRIM {key} MAXLEN {XTRIM_DEFAULT}")),
            ObjectAction::destructive("del", "Delete stream (DEL)", format!("DEL {key}")),
        ];
        Ok(detail)
    }

    async fn group_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let stream = reference
            .parent
            .as_deref()
            .ok_or_else(|| AppError::invalid_input("A consumer group needs its stream as parent."))?;
        let groups = reply_to_objects(self.raw(&["XINFO", "GROUPS", stream]).await?);
        let group = groups
            .into_iter()
            .find(|g| field_text(g, "name") == reference.name)
            .ok_or_else(|| AppError::not_found(format!("Consumer group \"{}\" does not exist on stream \"{stream}\".", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference).definition(pretty_json(&group), CodeLanguage::Json).property("stream", stream);
        for field in ["name", "consumers", "pending", "last-delivered-id", "entries-read", "lag"] {
            if let Some(v) = group.get(field) {
                detail = detail.property(field, json_text(v));
            }
        }
        let consumers = reply_to_objects(self.raw(&["XINFO", "CONSUMERS", stream, &reference.name]).await?);
        let rows = consumers
            .iter()
            .map(|c| vec![Value::Text(field_text(c, "name")), int_cell(field_int(c, "pending")), int_cell(field_int(c, "idle")), int_cell(field_int(c, "inactive"))])
            .collect();
        detail.rows = Some(result_set(&["consumer", "pending", "idle_ms", "inactive_ms"], rows));
        detail.actions = vec![ObjectAction::destructive(
            "xgroup-destroy",
            "Destroy consumer group (XGROUP DESTROY)",
            format!("XGROUP DESTROY {} {}", quote_arg(stream), quote_arg(&reference.name)),
        )];
        Ok(detail)
    }

    async fn channel_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let regular = self.subscriber_counts("NUMSUB", std::slice::from_ref(&reference.name)).await?.get(name).copied();
        let shard = self.subscriber_counts("SHARDNUMSUB", std::slice::from_ref(&reference.name)).await?.get(name).copied();
        let patterns = self.tolerant(&["PUBSUB", "NUMPAT"]).await?.map(reply_text).unwrap_or_default();
        let mut detail = ObjectDetail::empty(reference).property("subscribers", regular.unwrap_or(0).to_string());
        if let Some(n) = shard {
            detail = detail.property("shard subscribers", n.to_string());
        }
        if !patterns.is_empty() {
            detail = detail.property("pattern subscriptions (server-wide)", patterns);
        }
        Ok(detail)
    }

    async fn function_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let libraries = reply_to_objects(self.raw(&["FUNCTION", "LIST", "LIBRARYNAME", name, "WITHCODE"]).await?);
        let library = libraries
            .into_iter()
            .find(|l| field_text(l, "library_name") == name)
            .ok_or_else(|| AppError::not_found(format!("Function library \"{name}\" does not exist.")))?;
        let functions: Vec<&JsonObject> = match library.get("functions") {
            Some(serde_json::Value::Array(items)) => items.iter().filter_map(serde_json::Value::as_object).collect(),
            _ => Vec::new(),
        };
        let mut detail = ObjectDetail::empty(reference)
            .property("library", field_text(&library, "library_name"))
            .property("engine", field_text(&library, "engine"))
            .property("functions", functions.len().to_string());
        let code = field_text(&library, "library_code");
        if !code.is_empty() {
            detail = detail.definition(code, CodeLanguage::Text);
        }
        let rows = functions
            .iter()
            .map(|f| {
                let flags = match f.get("flags") {
                    Some(serde_json::Value::Array(items)) => items.iter().map(json_text).collect::<Vec<_>>().join(", "),
                    _ => String::new(),
                };
                vec![Value::Text(field_text(f, "name")), Value::Text(field_text(f, "description")), Value::Text(flags)]
            })
            .collect();
        detail.rows = Some(result_set(&["function", "description", "flags"], rows));
        detail.actions = vec![ObjectAction::destructive("function-delete", "Delete library (FUNCTION DELETE)", format!("FUNCTION DELETE {}", quote_arg(name)))];
        Ok(detail)
    }

    async fn user_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let user = reply_to_object(self.raw(&["ACL", "GETUSER", name]).await?);
        if user.is_empty() {
            return Err(AppError::not_found(format!("ACL user \"{name}\" does not exist.")));
        }
        let list = |v: Option<&serde_json::Value>| match v {
            Some(serde_json::Value::Array(items)) => items.iter().map(json_text).collect::<Vec<_>>().join(" "),
            Some(other) => json_text(other),
            None => String::new(),
        };
        let passwords = user.get("passwords").and_then(serde_json::Value::as_array).map(Vec::len).unwrap_or(0);
        let selectors = user.get("selectors").and_then(serde_json::Value::as_array).map(Vec::len).unwrap_or(0);
        let mut detail = ObjectDetail::empty(reference)
            .property("flags", list(user.get("flags")))
            .property("passwords", passwords.to_string())
            .property("commands", list(user.get("commands")))
            .property("keys", list(user.get("keys")))
            .property("channels", list(user.get("channels")))
            .property("selectors", selectors.to_string());
        if let Some(reply) = self.tolerant(&["ACL", "LIST"]).await? {
            if let Some(line) = reply_strings(reply).iter().filter_map(|l| parse_acl_line(l)).find(|u| u.name == name) {
                detail = detail.definition(format!("user {} {}", line.name, line.rules), CodeLanguage::Text);
            }
        }
        detail.actions = vec![ObjectAction::destructive("acl-deluser", "Delete user (ACL DELUSER)", format!("ACL DELUSER {}", quote_arg(name)))];
        Ok(detail)
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let value = self
            .config_value(name)
            .await?
            .ok_or_else(|| AppError::not_found(format!("Config parameter \"{name}\" does not exist.")))?;
        let shown = mask_config(name, &value);
        let template = if shown == value { quote_arg(&value) } else { "<value>".to_string() };
        Ok(ObjectDetail::empty(reference)
            .property("parameter", name)
            .property("value", shown)
            .action(ObjectAction::destructive("config-set", "Set value (CONFIG SET)", format!("CONFIG SET {} {template}", quote_arg(name)))))
    }

    async fn session_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let id = reference.name.as_str();
        let reply = match self.tolerant(&["CLIENT", "LIST", "ID", id]).await? {
            Some(reply) => reply,
            None => self.raw(&["CLIENT", "LIST"]).await?,
        };
        let client = parse_client_list(&reply_text(reply))
            .into_iter()
            .find(|c| c.get("id").is_some_and(|v| v == id))
            .ok_or_else(|| AppError::not_found(format!("Client {id} is no longer connected.")))?;
        let mut detail = ObjectDetail::empty(reference);
        for field in ["id", "addr", "laddr", "name", "user", "db", "age", "idle", "flags", "cmd", "lib-name", "lib-ver", "resp"] {
            if let Some(v) = client.get(field) {
                detail = detail.property(field, v.clone());
            }
        }
        let rows = client.iter().map(|(k, v)| vec![Value::Text(k.clone()), Value::Text(v.clone())]).collect();
        detail.rows = Some(result_set(&["field", "value"], rows));
        detail.actions = vec![ObjectAction::destructive("client-kill", "Kill connection (CLIENT KILL)", format!("CLIENT KILL ID {id}"))];
        Ok(detail)
    }

    async fn slowlog_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let entry = self
            .slowlog_entries()
            .await?
            .into_iter()
            .find(|e| e.id.to_string() == reference.name)
            .ok_or_else(|| AppError::not_found(format!("Slow log entry {} is no longer in the log.", reference.name)))?;
        let mut detail = ObjectDetail::empty(reference)
            .definition(entry.command_line(), CodeLanguage::Text)
            .property("id", entry.id.to_string())
            .property("time", format_unix(entry.timestamp))
            .property("duration", format!("{} µs", format_number(entry.duration_us as f64)))
            .property("client", entry.client.clone())
            .property("client name", entry.name.clone());
        let rows = entry.argv.iter().enumerate().map(|(i, a)| vec![Value::Int(i64::try_from(i).unwrap_or(i64::MAX)), Value::Text(a.clone())]).collect();
        detail.rows = Some(result_set(&["arg", "value"], rows));
        detail.actions = vec![ObjectAction::destructive("slowlog-reset", "Clear the slow log (SLOWLOG RESET)", "SLOWLOG RESET")];
        Ok(detail)
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        if let Some(nodes) = self.cluster_nodes().await? {
            let node = nodes
                .into_iter()
                .find(|n| n.addr == reference.name)
                .ok_or_else(|| AppError::not_found(format!("Cluster node \"{}\" is not known.", reference.name)))?;
            return Ok(ObjectDetail::empty(reference)
                .property("id", node.id.clone())
                .property("address", node.addr.clone())
                .property("role", node.role())
                .property("flags", node.flags.join(", "))
                .property("master", node.master.clone().unwrap_or_else(|| "—".to_string()))
                .property("config epoch", node.config_epoch.clone())
                .property("link state", node.link_state.clone())
                .property("slots", node.slots_text()));
        }
        let info = self.info(Some("server")).await?;
        let mut detail = ObjectDetail::empty(reference);
        for (k, v) in &info {
            detail = detail.property(k, v.clone());
        }
        Ok(detail)
    }

    async fn replica_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let info = self.info(Some("replication")).await?;
        let mut detail = ObjectDetail::empty(reference);
        let line = info.iter().find(|(k, v)| {
            k.starts_with("slave") && {
                let fields = parse_kv_list(v);
                format!("{}:{}", fields.get("ip").cloned().unwrap_or_default(), fields.get("port").cloned().unwrap_or_default()) == reference.name
            }
        });
        if let Some((_, v)) = line {
            for (k, v) in parse_kv_list(v) {
                detail = detail.property(&k, v);
            }
        }
        for (k, v) in &info {
            if !k.starts_with("slave") || !k[5..].chars().all(|c| c.is_ascii_digit()) {
                detail = detail.property(k, v.clone());
            }
        }
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
        capabilities: Capabilities { describes_fields: true, sql: false, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true },
        object_kinds: vec![K::Database, K::Stream, K::ConsumerGroup, K::Channel, K::Function, K::User, K::Setting, K::Session, K::SlowQuery, K::Node, K::Replica],
        tools: vec![T::Stats, T::KeyBrowser, T::PubSub],
    }
}

#[async_trait]
impl Integration for RedisIntegration {
    fn engine(&self) -> Engine {
        Engine::Redis
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let mut con = self.con();
        let reply: String = redis::cmd("PING").query_async(&mut con).await?;
        if reply.eq_ignore_ascii_case("PONG") {
            Ok(())
        } else {
            Err(AppError::driver(format!("Unexpected PING reply: {reply}")))
        }
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let mut con = self.con();
        let info: String = redis::cmd("INFO").arg("server").query_async(&mut con).await?;
        Ok(info
            .lines()
            .find_map(|line| line.strip_prefix("redis_version:"))
            .map(|v| format!("Redis {}", v.trim())))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.db.to_string())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let mut con = self.con();
        let configured: Option<Vec<String>> = redis::cmd("CONFIG").arg("GET").arg("databases").query_async(&mut con).await.ok();
        let total = configured
            .and_then(|pair| pair.get(1).and_then(|n| n.parse::<u32>().ok()))
            .unwrap_or(DEFAULT_DATABASES)
            .clamp(1, 1_024);
        Ok((0..total).map(|n| n.to_string()).collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let keys = self.scan_keys().await?;
        let tables = keys
            .into_iter()
            .map(|name| TableInfo { schema: None, name, kind: TableKind::Table, row_estimate: None })
            .collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: format!("db{}", self.db), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(self.key_kind(&table.name).await?.columns())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let kind = self.key_kind(&table.name).await?;
        Ok(Some(self.entry_count(&table.name, kind).await?))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let kind = self.key_kind(&table.name).await?;
        if filters.is_empty() {
            return self.entry_count(&table.name, kind).await;
        }
        let rows = self.load_entries(&table.name, kind).await?;
        let filtered = apply_filters(&kind.columns(), rows, filters)?;
        Ok(i64::try_from(filtered.len()).unwrap_or(i64::MAX))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let kind = self.key_kind(&table.name).await?;
        let columns = kind.columns();
        let rows = self.load_entries(&table.name, kind).await?;
        let mut rows = apply_filters(&columns, rows, &query.filters)?;
        apply_sort(&columns, &mut rows, &query.sort)?;
        let offset = usize::try_from(query.offset).unwrap_or(usize::MAX);
        let limit = query.limit as usize;
        let page: Vec<Vec<Value>> = rows.into_iter().skip(offset).take(limit).collect();
        Ok(ResultSet {
            columns: columns.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect(),
            rows: page,
            truncated: false,
        })
    }

    async fn execute(&self, script: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let commands = parse_commands(script)?;
        if commands.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        let mut con = self.con();
        let mut out = Vec::with_capacity(commands.len());
        for tokens in commands {
            let Some((name, args)) = tokens.split_first() else {
                continue;
            };
            let mut cmd = redis::cmd(name);
            for arg in args {
                cmd.arg(arg.as_str());
            }
            let reply: redis::Value = cmd.query_async(&mut con).await?;
            out.push(reply_to_statement(reply, max_rows));
        }
        Ok(out)
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut out = match kind {
            ObjectKind::Database => self.database_objects().await?,
            ObjectKind::Stream => self.stream_objects().await?,
            ObjectKind::ConsumerGroup => self.group_objects(parent).await?,
            ObjectKind::Channel => self.channel_objects().await?,
            ObjectKind::Function => self.function_objects().await?,
            ObjectKind::User => self.user_objects().await?,
            ObjectKind::Setting => self.setting_objects().await?,
            ObjectKind::Session => self.session_objects().await?,
            ObjectKind::SlowQuery => self.slowlog_objects().await?,
            ObjectKind::Node => self.node_objects().await?,
            ObjectKind::Replica => self.replica_objects().await?,
            _ => Vec::new(),
        };
        out.truncate(MAX_OBJECTS);
        Ok(out)
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Database => self.database_detail(reference).await,
            ObjectKind::Stream => self.stream_detail(reference).await,
            ObjectKind::ConsumerGroup => self.group_detail(reference).await,
            ObjectKind::Channel => self.channel_detail(reference).await,
            ObjectKind::Function => self.function_detail(reference).await,
            ObjectKind::User => self.user_detail(reference).await,
            ObjectKind::Setting => self.setting_detail(reference).await,
            ObjectKind::Session => self.session_detail(reference).await,
            ObjectKind::SlowQuery => self.slowlog_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
            ObjectKind::Replica => self.replica_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let info = self.info(None).await?;
        let maxclients = self.config_value("maxclients").await.unwrap_or(None).and_then(|v| v.parse::<f64>().ok());
        Ok(ServerStats::now(stats_groups(&info, maxclients)))
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment};

    #[test]
    fn tokenizer_handles_quotes_escapes_and_comments() {
        let cmds = parse_commands("SET a \"hello world\"\n\n# comment\nHSET h 'it\\'s' \"tab\\tnew\\nline\" \"\\x41\"\n   ").unwrap_or_default();
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0], vec!["SET", "a", "hello world"]);
        assert_eq!(cmds[1], vec!["HSET", "h", "it's", "tab\tnew\nline", "A"]);
        assert!(parse_commands("GET \"unterminated").is_err());
        assert!(parse_commands("").unwrap_or_default().is_empty());
    }

    #[test]
    fn replies_become_rows() {
        let array = redis::Value::Array(vec![
            redis::Value::BulkString(b"a".to_vec()),
            redis::Value::Int(3),
            redis::Value::Nil,
            redis::Value::Array(vec![redis::Value::Int(1), redis::Value::Int(2)]),
        ]);
        match reply_to_statement(array, 10) {
            StatementResult::Rows { result } => {
                assert_eq!(result.columns.len(), 1);
                assert_eq!(result.rows.len(), 4);
                assert_eq!(result.rows[0][0], Value::Text("a".into()));
                assert_eq!(result.rows[1][0], Value::Int(3));
                assert_eq!(result.rows[2][0], Value::Null);
                assert!(matches!(result.rows[3][0], Value::Json(_)));
                assert!(!result.truncated);
            }
            other => panic!("expected rows, got {other:?}"),
        }
        let map = redis::Value::Map(vec![(redis::Value::SimpleString("k".into()), redis::Value::BulkString(b"{\"x\":1}".to_vec()))]);
        match reply_to_statement(map, 10) {
            StatementResult::Rows { result } => {
                assert_eq!(result.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["field", "value"]);
                assert!(matches!(result.rows[0][1], Value::Json(_)));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        match reply_to_statement(redis::Value::Okay, 10) {
            StatementResult::Rows { result } => assert_eq!(result.rows[0][0], Value::Text("OK".into())),
            other => panic!("expected rows, got {other:?}"),
        }
        let big = redis::Value::Array((0..5).map(redis::Value::Int).collect());
        match reply_to_statement(big, 2) {
            StatementResult::Rows { result } => {
                assert_eq!(result.rows.len(), 2);
                assert!(result.truncated);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn filters_and_sort_apply_client_side() {
        let columns = KeyKind::ZSet.columns();
        let rows = vec![
            vec![Value::Text("bob".into()), Value::Float(2.0)],
            vec![Value::Text("Ann".into()), Value::Float(10.0)],
            vec![Value::Text("carl".into()), Value::Float(1.5)],
        ];
        let filtered = apply_filters(
            &columns,
            rows.clone(),
            &[FilterRule { column: "member".into(), op: FilterOp::Contains, value: "A".into() }],
        )
        .unwrap_or_default();
        assert_eq!(filtered.len(), 2, "case-insensitive contains: Ann, carl");
        let mut sorted = rows;
        apply_sort(&columns, &mut sorted, &[SortRule { column: "score".into(), desc: true }]).unwrap_or_default();
        assert_eq!(sorted[0][1], Value::Float(10.0));
        assert_eq!(sorted[2][1], Value::Float(1.5));
        let gt = apply_filters(&columns, sorted, &[FilterRule { column: "score".into(), op: FilterOp::Gt, value: "1.9".into() }]).unwrap_or_default();
        assert_eq!(gt.len(), 2);
        assert!(apply_filters(&columns, Vec::new(), &[FilterRule { column: "nope".into(), op: FilterOp::Eq, value: "x".into() }]).is_err());
    }

    #[test]
    fn url_builder_encodes_credentials_and_tls() {
        let input = ConnectionInput {
            name: "r".into(),
            engine: Engine::Redis,
            environment: Environment::Local,
            read_only: false,
            host: Some("cache.local".into()),
            port: Some(6380),
            database: Some("3".into()),
            username: Some("app".into()),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Require,
        };
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, true), secret: Some("p@ss:w/rd".into()) };
        let (url, db) = build_url(&resolved);
        assert_eq!(url, "rediss://app:p%40ss%3Aw%2Frd@cache.local:6380/3#insecure");
        assert_eq!(db, 3);
    }

    #[test]
    fn info_parser_keyspace_and_durations() {
        let text = "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\nuptime_in_seconds:90061\r\n\r\n# Keyspace\r\ndb0:keys=12,expires=3,avg_ttl=5000\r\ndb3:keys=1,expires=0,avg_ttl=0\r\n";
        let info = parse_info(text);
        assert_eq!(info.get("redis_version").map(String::as_str), Some("7.2.4"));
        assert_eq!(info.len(), 5);
        assert_eq!(
            parse_keyspace(&info),
            vec![
                KeyspaceEntry { db: 0, keys: 12, expires: 3, avg_ttl: 5000 },
                KeyspaceEntry { db: 3, keys: 1, expires: 0, avg_ttl: 0 }
            ]
        );
        assert_eq!(format_duration(90061.0), "1d 1h 1m 1s");
        assert_eq!(format_duration(59.0), "59s");
        assert_eq!(format_duration(3600.0), "1h 0m 0s");
        assert_eq!(format_unix(1_700_000_000), "2023-11-14T22:13:20+00:00");
    }

    #[test]
    fn stats_groups_from_info() {
        let text = "redis_version:7.2.4\nredis_mode:standalone\nuptime_in_seconds:120\nconnected_clients:3\nblocked_clients:0\n\
                    used_memory:1048576\nused_memory_peak:2097152\nmem_fragmentation_ratio:1.5\nmaxmemory:0\nevicted_keys:7\n\
                    instantaneous_ops_per_sec:12\ntotal_commands_processed:1000\nkeyspace_hits:75\nkeyspace_misses:25\n\
                    total_net_input_bytes:2048\ntotal_net_output_bytes:4096\nrdb_last_save_time:1700000000\nrdb_changes_since_last_save:5\n\
                    aof_enabled:0\naof_rewrite_in_progress:0\nrole:master\nconnected_slaves:1\nmaster_repl_offset:42\ndb0:keys=10,expires=2,avg_ttl=0\n";
        let groups = stats_groups(&parse_info(text), Some(10_000.0));
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Clients", "Memory", "Throughput", "Persistence", "Replication", "Keyspace"]);
        let find = |group: &str, label: &str| {
            groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label)).cloned()
        };
        assert_eq!(find("Memory", "Used").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Memory", "Used").and_then(|s| s.hint), Some("1,048,576 bytes".into()));
        assert_eq!(find("Memory", "Max memory").map(|s| s.value), Some("unlimited".into()));
        assert_eq!(find("Throughput", "Hit ratio").and_then(|s| s.numeric), Some(75.0));
        assert_eq!(find("Clients", "Max clients").and_then(|s| s.numeric), Some(10_000.0));
        assert_eq!(find("Keyspace", "Keys").and_then(|s| s.numeric), Some(10.0));
        assert_eq!(find("Keyspace", "Expiring").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Persistence", "Last RDB save").map(|s| s.value), Some("2023-11-14T22:13:20+00:00".into()));
        assert_eq!(find("Persistence", "AOF").map(|s| s.value), Some("off".into()));
        assert_eq!(find("Server", "Uptime").map(|s| s.value), Some("2m 0s".into()));
        assert_eq!(find("Replication", "Role").map(|s| s.value), Some("master".into()));
        assert!(stats_groups(&BTreeMap::new(), None).iter().all(|g| !g.stats.is_empty()));
    }

    #[test]
    fn client_list_cluster_nodes_and_acl_lines() {
        let clients = parse_client_list(
            "id=3 addr=127.0.0.1:52555 laddr=127.0.0.1:6379 fd=8 name= age=5 idle=0 flags=N db=0 cmd=client|list user=default\n\
             id=4 addr=10.0.0.9:1234 name=worker idle=12 cmd=get user=app\n",
        );
        assert_eq!(clients.len(), 2);
        assert_eq!(clients[0].get("cmd").map(String::as_str), Some("client|list"));
        assert_eq!(clients[0].get("name").map(String::as_str), Some(""));
        assert_eq!(clients[1].get("idle").map(String::as_str), Some("12"));

        let nodes = parse_cluster_nodes(
            "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004,hostname4 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected\n\
             e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-5460 10000\n",
        );
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].addr, "127.0.0.1:30004");
        assert_eq!(nodes[0].role(), "replica");
        assert_eq!(nodes[0].master.as_deref(), Some("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca"));
        assert_eq!(nodes[0].slots_text(), "no slots");
        assert_eq!(nodes[1].role(), "master");
        assert_eq!(nodes[1].slots, vec!["0-5460", "10000"]);
        assert_eq!(nodes[1].config_epoch, "1");
        assert_eq!(nodes[1].link_state, "connected");
        assert!(nodes[1].flags.iter().any(|f| f == "myself"));

        let user = parse_acl_line("user app on #5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8 ~cache:* &* -@all +get +set")
            .unwrap_or_else(|| panic!("acl line"));
        assert_eq!(user.name, "app");
        assert!(user.enabled);
        assert_eq!(user.rules, "on #<hash> ~cache:* &* -@all +get +set");
        assert!(!parse_acl_line("user off_user off nopass").unwrap_or_else(|| panic!("acl line")).enabled);
        assert!(parse_acl_line("nonsense").is_none());
        assert_eq!(mask_config("requirepass", "hunter2"), "••••••");
        assert_eq!(mask_config("masterauth", ""), "");
        assert_eq!(mask_config("maxmemory", "0"), "0");
    }

    #[test]
    fn slowlog_and_xinfo_replies() {
        let bulk = |s: &str| redis::Value::BulkString(s.as_bytes().to_vec());
        let entry = redis::Value::Array(vec![
            redis::Value::Int(14),
            redis::Value::Int(1_700_000_000),
            redis::Value::Int(15_000),
            redis::Value::Array(vec![bulk("SET"), bulk("k"), bulk("v")]),
            bulk("127.0.0.1:5000"),
            bulk("worker"),
        ]);
        let entries = parse_slowlog(redis::Value::Array(vec![entry, redis::Value::Nil]));
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0],
            SlowEntry {
                id: 14,
                timestamp: 1_700_000_000,
                duration_us: 15_000,
                argv: vec!["SET".into(), "k".into(), "v".into()],
                client: "127.0.0.1:5000".into(),
                name: "worker".into()
            }
        );
        assert_eq!(entries[0].command_line(), "SET k v");
        assert!(parse_slowlog(redis::Value::Nil).is_empty());

        // RESP2 XINFO STREAM: flat key/value array with a nested first-entry.
        let stream = redis::Value::Array(vec![
            bulk("length"),
            redis::Value::Int(2),
            bulk("last-generated-id"),
            bulk("1700000000000-1"),
            bulk("groups"),
            redis::Value::Int(1),
            bulk("first-entry"),
            redis::Value::Array(vec![bulk("1700000000000-0"), redis::Value::Array(vec![bulk("f"), bulk("v")])]),
        ]);
        let object = reply_to_object(stream);
        assert_eq!(field_int(&object, "length"), Some(2));
        assert_eq!(field_text(&object, "last-generated-id"), "1700000000000-1");
        assert_eq!(entry_id(object.get("first-entry")), "1700000000000-0");
        assert_eq!(entry_id(object.get("last-entry")), "");
        assert!(pretty_json(&object).contains("\"length\": 2"));

        // RESP3 map shape, as XINFO GROUPS returns it.
        let groups = reply_to_objects(redis::Value::Array(vec![redis::Value::Map(vec![
            (bulk("name"), bulk("g1")),
            (bulk("consumers"), redis::Value::Int(2)),
            (bulk("lag"), redis::Value::Nil),
        ])]));
        assert_eq!(groups.len(), 1);
        assert_eq!(field_text(&groups[0], "name"), "g1");
        assert_eq!(field_int(&groups[0], "consumers"), Some(2));
        assert_eq!(field_int(&groups[0], "lag"), None);
        assert_eq!(reply_strings(redis::Value::Array(vec![bulk("a"), bulk("b")])), vec!["a", "b"]);
        assert!(reply_strings(redis::Value::Nil).is_empty());
        assert_eq!(reply_text(bulk("id=1 addr=x")), "id=1 addr=x");
    }

    #[test]
    fn quoting_masking_and_previews() {
        assert_eq!(quote_arg("plain:key"), "plain:key");
        assert_eq!(quote_arg("has space"), "\"has space\"");
        assert_eq!(quote_arg("q\"uote\\"), "\"q\\\"uote\\\\\"");
        assert_eq!(quote_arg(""), "\"\"");
        let cmds = parse_commands(&format!("DEL {}", quote_arg("has \"space\""))).unwrap_or_default();
        assert_eq!(cmds[0], vec!["DEL", "has \"space\""]);
        assert_eq!(preview("abcdef", 3), "abc…");
        assert_eq!(preview("a\nb", 10), "a b");
        assert_eq!(mib(1_572_864.0), 1.5);
        assert!(is_unsupported(&AppError::driver("ERR unknown command 'FUNCTION', with args beginning with: 'LIST'")));
        assert!(is_unsupported(&AppError::driver("ERR This instance has cluster support disabled")));
        assert!(!is_unsupported(&AppError::driver("NOAUTH Authentication required.")));
        assert!(!profile().object_kinds.contains(&ObjectKind::Script), "Redis has no command that lists loaded scripts");
    }

    // WHAT:  Live round trip. Skipped unless DB_FREE_REDIS_URL is set (e.g. redis://127.0.0.1:6379/15).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DB_FREE_REDIS_URL") else {
            return;
        };
        // Minimal redis[s]://[user[:pass]@]host[:port][/db] parser (no url crate dependency).
        let (scheme, rest) = url.split_once("://").unwrap_or(("redis", url.as_str()));
        let (userinfo, hostpart) = match rest.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, rest),
        };
        let (hostport, path) = hostpart.split_once('/').unwrap_or((hostpart, "0"));
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse::<u16>().ok()),
            None => (hostport.to_string(), None),
        };
        let (username, password) = match userinfo.map(|u| u.split_once(':').unwrap_or((u, ""))) {
            Some((u, p)) => (Some(u.to_string()).filter(|u| !u.is_empty()), Some(p.to_string()).filter(|p| !p.is_empty())),
            None => (None, None),
        };
        let db: i64 = path.parse().unwrap_or(0);
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Redis,
            environment: Environment::Local,
            read_only: false,
            host: Some(host),
            port,
            database: Some(db.to_string()),
            username,
            password: None,
            file_path: None,
            ssl_mode: if scheme == "rediss" { SslMode::VerifyFull } else { SslMode::Disable },
        };
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, false), secret: password };
        let redis = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        redis.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert!(redis.server_version().await.unwrap_or_default().is_some_and(|v| v.starts_with("Redis")));
        assert_eq!(redis.current_database(), Some(db.to_string()));
        assert!(!redis.databases().await.unwrap_or_default().is_empty());

        let setup = "DEL dbfree_test:s dbfree_test:h dbfree_test:l dbfree_test:z\n\
                     SET dbfree_test:s \"{\\\"ok\\\":true}\"\n\
                     HSET dbfree_test:h alpha 1 beta two\n\
                     RPUSH dbfree_test:l x y z\n\
                     ZADD dbfree_test:z 1.5 low 10 high";
        let results = redis.execute(setup, 100).await.unwrap_or_else(|e| panic!("setup: {e}"));
        assert_eq!(results.len(), 5);

        let catalog = redis.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let names: Vec<&str> = catalog.schemas.iter().flat_map(|s| s.tables.iter().map(|t| t.name.as_str())).collect();
        for key in ["dbfree_test:s", "dbfree_test:h", "dbfree_test:l", "dbfree_test:z"] {
            assert!(names.contains(&key), "{key} missing from {names:?}");
        }

        let hash = TableRef { schema: None, name: "dbfree_test:h".into() };
        let cols = redis.columns(&hash).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["field", "value"]);
        assert_eq!(redis.row_estimate(&hash).await.unwrap_or_default(), Some(2));
        let page = redis
            .fetch_page(&hash, &PageQuery { sort: vec![SortRule { column: "field".into(), desc: true }], filters: Vec::new(), offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.rows[0][0], Value::Text("beta".into()));
        let filtered = redis
            .count(&hash, &[FilterRule { column: "value".into(), op: FilterOp::Eq, value: "1".into() }])
            .await
            .unwrap_or_default();
        assert_eq!(filtered, 1);

        let string = TableRef { schema: None, name: "dbfree_test:s".into() };
        let page = redis.fetch_page(&string, &PageQuery { sort: Vec::new(), filters: Vec::new(), offset: 0, limit: 10 }).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(page.rows[0][0], Value::Json(_)));

        let list = TableRef { schema: None, name: "dbfree_test:l".into() };
        let page = redis.fetch_page(&list, &PageQuery { sort: Vec::new(), filters: Vec::new(), offset: 1, limit: 1 }).await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0][0], Value::Int(1));
        assert_eq!(page.rows[0][1], Value::Text("y".into()));

        let zset = TableRef { schema: None, name: "dbfree_test:z".into() };
        let page = redis.fetch_page(&zset, &PageQuery { sort: vec![SortRule { column: "score".into(), desc: true }], filters: Vec::new(), offset: 0, limit: 10 }).await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(page.rows[0][0], Value::Text("high".into()));

        let out = redis.execute("HGETALL dbfree_test:h", 100).await.unwrap_or_else(|e| panic!("hgetall: {e}"));
        match out.first() {
            Some(StatementResult::Rows { result }) => assert!(result.rows.len() == 4 || result.rows.len() == 2, "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        assert!(matches!(redis.columns(&TableRef { schema: None, name: "dbfree_test:missing".into() }).await, Err(AppError::NotFound { .. })));

        let cleanup = redis.execute("DEL dbfree_test:s dbfree_test:h dbfree_test:l dbfree_test:z", 10).await.unwrap_or_else(|e| panic!("cleanup: {e}"));
        match cleanup.first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows[0][0], Value::Int(4)),
            other => panic!("expected rows, got {other:?}"),
        }
        redis.close().await;
    }
}
