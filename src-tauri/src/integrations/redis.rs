// SOT: redis-integration, redis-adapter, key-value-mapping, redis-command-parser

use crate::error::{AppError, AppResult};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SortRule, SslMode, StatementResult, TableInfo, TableKind, TableRef,
    Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use redis::aio::MultiplexedConnection;
use redis::Client;
use std::cmp::Ordering;
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

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let (url, db) = build_url(conn);
    let client = Client::open(url)?;
    let connection = client.get_multiplexed_async_connection().await?;
    Ok(Arc::new(RedisIntegration { conn: connection, db }))
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
        let mut con = self.con();
        let mut cursor: u64 = 0;
        let mut keys: Vec<String> = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("*")
                .arg("COUNT")
                .arg(SCAN_BATCH)
                .query_async(&mut con)
                .await?;
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

#[async_trait]
impl Integration for RedisIntegration {
    fn engine(&self) -> Engine {
        Engine::Redis
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true }
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
