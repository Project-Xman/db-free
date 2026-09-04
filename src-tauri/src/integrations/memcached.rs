// SOT: memcached-integration, memcached-text-protocol, memcached-adapter, memcached-command-parser, memcached-object-explorer, memcached-server-stats, memcached-stats-parser

use crate::error::{AppError, AppResult};
use crate::integrations::http::local;
use crate::integrations::{Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, ObjectDetail, ObjectKind, ObjectRef, ObjectSummary, PageQuery,
    ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat, StatGroup, StatementResult,
    TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

// ============================================================================
// MEMCACHED ADAPTER
//
// WHAT:  Memcached over its text protocol (port 11211), no vendor crate.
// WHY:   The protocol is a handful of line commands; a TcpStream and a small
//        reader are enough and keep the dependency list flat.
// HOW:   catalog     = schema "memcached" with two tables:
//                        keys  (Table) – best-effort enumeration through
//                                        `stats items` + `stats cachedump`
//                        stats (View)  – the `stats` output as name/value
//        columns     = fixed per table
//        fetch_page  = keys: cachedump → multi-get, then client-side paging
//                      stats: `stats` → rows
//        execute     = one raw protocol command per line; write commands are
//                      refused on read-only connections
//        objects     = Setting from `stats settings`, Session from `stats conns`
//                      (empty on builds without it); no actions, the protocol
//                      has none that target one setting or connection
//        stats       = `stats` + `stats slabs` as grouped figures
//        Memcached has NO key scan. `stats cachedump` is capped by the server
//        (and removed in some builds, which answer CLIENT_ERROR); in that case
//        the keys table is simply empty while stats still work.
//        One connection behind a tokio Mutex; it reconnects on I/O errors.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI meta)
// ============================================================================

const DEFAULT_PORT: u16 = 11211;
const SCHEMA_NAME: &str = "memcached";
const KEYS_TABLE: &str = "keys";
const STATS_TABLE: &str = "stats";
const MAX_KEYS: usize = 5_000;
const CACHEDUMP_PER_SLAB: usize = 1_000;
const GET_BATCH: usize = 100;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

pub struct MemcachedIntegration {
    addr: String,
    stream: Mutex<Option<TcpStream>>,
    read_only: bool,
}

pub fn address(conn: &ResolvedConnection) -> String {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("127.0.0.1");
    // Accept "host:port" pasted into the host field.
    if host.rsplit_once(':').is_some_and(|(_, p)| p.parse::<u16>().is_ok()) && !host.starts_with('[') {
        return host.to_string();
    }
    format!("{host}:{}", s.port.unwrap_or(DEFAULT_PORT))
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let addr = address(conn);
    let stream = open_stream(&addr).await?;
    let integration = MemcachedIntegration { addr, stream: Mutex::new(Some(stream)), read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

async fn open_stream(addr: &str) -> AppResult<TcpStream> {
    let stream = tokio::time::timeout(IO_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| AppError::not_connected(format!("Timed out connecting to memcached at {addr}.")))?
        .map_err(|e| AppError::not_connected(format!("Could not connect to memcached at {addr}: {e}")))?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

// WHAT:  How a command's reply ends, so the reader knows when to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyShape {
    /// Single line (`STORED`, `DELETED`, `VERSION x`, `OK`, `NOT_FOUND`, `123`, errors).
    Line,
    /// Multi-line, terminated by a line `END` (`get`, `gets`, `stats …`).
    UntilEnd,
    /// No reply at all (`noreply` variants, `quit`).
    None,
}

fn is_terminal_line(line: &str) -> bool {
    let word = line.split_whitespace().next().unwrap_or("");
    matches!(word, "END" | "ERROR" | "CLIENT_ERROR" | "SERVER_ERROR" | "RESET" | "OK" | "BUSY" | "BADCLASS" | "NOSPARE" | "NOTFULL" | "UNSAFE" | "SAME")
}

// WHAT:  Which reply shape a raw command line produces.
fn reply_shape(command: &str) -> ReplyShape {
    let mut words = command.split_whitespace();
    let verb = words.next().unwrap_or("").to_ascii_lowercase();
    let noreply = command.split_whitespace().last().is_some_and(|w| w.eq_ignore_ascii_case("noreply"));
    if noreply || verb == "quit" {
        return ReplyShape::None;
    }
    match verb.as_str() {
        "get" | "gets" | "gat" | "gats" | "stats" | "mg" | "lru_crawler" => ReplyShape::UntilEnd,
        _ => ReplyShape::Line,
    }
}

// WHAT:  Storage commands carry a data block after the command line.
fn is_storage_command(verb: &str) -> bool {
    matches!(verb, "set" | "add" | "replace" | "append" | "prepend" | "cas")
}

fn is_write_command(verb: &str) -> bool {
    is_storage_command(verb) || matches!(verb, "delete" | "incr" | "decr" | "touch" | "flush_all" | "ms" | "md" | "ma" | "verbosity" | "slabs" | "lru")
}

// WHAT:  Reads reply lines until the shape says stop. `get` replies have
//        `VALUE key flags bytes` headers followed by raw data blocks, which
//        may contain newlines, so those are read by length.
async fn read_reply(stream: &mut TcpStream, shape: ReplyShape) -> AppResult<Vec<String>> {
    if shape == ReplyShape::None {
        return Ok(Vec::new());
    }
    let mut reader = LineReader::new(stream);
    let mut lines = Vec::new();
    loop {
        let line = reader.read_line().await?;
        if line.starts_with("VALUE ") {
            let bytes: usize = line.split_whitespace().nth(3).and_then(|n| n.parse().ok()).unwrap_or(0);
            let data = reader.read_exact(bytes + 2).await?;
            let body = String::from_utf8_lossy(&data[..bytes]).into_owned();
            lines.push(line);
            lines.push(body);
            continue;
        }
        let done = match shape {
            ReplyShape::Line => true,
            ReplyShape::UntilEnd => is_terminal_line(&line),
            ReplyShape::None => true,
        };
        lines.push(line);
        if done {
            break;
        }
    }
    Ok(lines)
}

struct LineReader<'a> {
    stream: &'a mut TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl<'a> LineReader<'a> {
    fn new(stream: &'a mut TcpStream) -> Self {
        LineReader { stream, buf: Vec::with_capacity(8192), pos: 0 }
    }

    async fn fill(&mut self) -> AppResult<()> {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let mut chunk = [0u8; 8192];
        let n = tokio::time::timeout(IO_TIMEOUT, self.stream.read(&mut chunk))
            .await
            .map_err(|_| AppError::driver("Timed out waiting for memcached."))?
            .map_err(|e| AppError::driver(format!("memcached read failed: {e}")))?;
        if n == 0 {
            return Err(AppError::driver("memcached closed the connection."));
        }
        self.buf.extend_from_slice(&chunk[..n]);
        if self.buf.len() > MAX_RESPONSE_BYTES {
            return Err(AppError::driver("memcached reply exceeded the 64 MiB cap."));
        }
        Ok(())
    }

    async fn read_line(&mut self) -> AppResult<String> {
        loop {
            if let Some(i) = self.buf[self.pos..].iter().position(|&b| b == b'\n') {
                let end = self.pos + i;
                let line = String::from_utf8_lossy(&self.buf[self.pos..end]).trim_end_matches('\r').to_string();
                self.pos = end + 1;
                return Ok(line);
            }
            self.fill().await?;
        }
    }

    async fn read_exact(&mut self, n: usize) -> AppResult<Vec<u8>> {
        while self.buf.len() - self.pos < n {
            self.fill().await?;
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(out)
    }
}

// WHAT:  One raw command (with optional data block) → reply lines.
async fn roundtrip(stream: &mut TcpStream, command: &str, data: Option<&str>) -> AppResult<Vec<String>> {
    let mut payload = String::with_capacity(command.len() + 4);
    payload.push_str(command.trim());
    payload.push_str("\r\n");
    if let Some(block) = data {
        payload.push_str(block);
        payload.push_str("\r\n");
    }
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(payload.as_bytes()))
        .await
        .map_err(|_| AppError::driver("Timed out writing to memcached."))?
        .map_err(|e| AppError::driver(format!("memcached write failed: {e}")))?;
    read_reply(stream, reply_shape(command)).await
}

// WHAT:  `stats …` reply → ordered name → value map.
fn parse_stats(lines: &[String]) -> BTreeMap<String, String> {
    lines
        .iter()
        .filter_map(|line| {
            let mut parts = line.splitn(3, ' ');
            if parts.next()? != "STAT" {
                return None;
            }
            let name = parts.next()?.to_string();
            Some((name, parts.next().unwrap_or("").to_string()))
        })
        .collect()
}

// WHAT:  `get` reply → (key, value) pairs in server order.
fn parse_values(lines: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if let Some(rest) = lines[i].strip_prefix("VALUE ") {
            let key = rest.split_whitespace().next().unwrap_or("").to_string();
            let body = lines.get(i + 1).cloned().unwrap_or_default();
            out.push((key, body));
            i += 2;
        } else {
            i += 1;
        }
    }
    out
}

// WHAT:  Slab class ids from `stats items` (`STAT items:<id>:number N`).
fn parse_slab_ids(stats: &BTreeMap<String, String>) -> Vec<u32> {
    let mut ids: Vec<u32> = stats
        .keys()
        .filter_map(|k| {
            let mut parts = k.split(':');
            if parts.next()? != "items" {
                return None;
            }
            let id = parts.next()?.parse::<u32>().ok()?;
            if parts.next()? == "number" {
                Some(id)
            } else {
                None
            }
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

// WHAT:  `stats cachedump` reply → keys (`ITEM key [bytes b; secs s]`).
fn parse_cachedump(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|line| line.strip_prefix("ITEM ").and_then(|rest| rest.split_whitespace().next()).map(str::to_string))
        .collect()
}

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

fn meta(names: &[&str]) -> Vec<ColumnMeta> {
    names.iter().map(|n| ColumnMeta { name: (*n).to_string(), type_name: "text".to_string() }).collect()
}

fn key_columns() -> Vec<ColumnInfo> {
    let col = |name: &str, data_type: &str, primary_key: bool, ordinal: u32| ColumnInfo {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: false,
        primary_key,
        ordinal,
    };
    vec![col("key", "text", true, 1), col("value", "text", false, 2), col("size", "int", false, 3)]
}

fn stats_columns() -> Vec<ColumnInfo> {
    vec![
        ColumnInfo { name: "name".into(), data_type: "text".into(), nullable: false, primary_key: true, ordinal: 1 },
        ColumnInfo { name: "value".into(), data_type: "text".into(), nullable: false, primary_key: false, ordinal: 2 },
    ]
}

// WHAT:  Reply lines → one StatementResult for the query tab.
fn reply_to_statement(command: &str, lines: Vec<String>, max_rows: usize) -> AppResult<StatementResult> {
    if let Some(first) = lines.first() {
        let word = first.split_whitespace().next().unwrap_or("");
        if matches!(word, "ERROR" | "CLIENT_ERROR" | "SERVER_ERROR") {
            let detail = first.trim();
            return Err(AppError::invalid_input(format!("memcached: {detail}")));
        }
    }
    let verb = command.split_whitespace().next().unwrap_or("").to_ascii_lowercase();
    if lines.iter().any(|l| l.starts_with("VALUE ")) {
        let mut rows: Vec<Vec<Value>> = parse_values(&lines)
            .into_iter()
            .map(|(k, v)| {
                let size = i64::try_from(v.len()).unwrap_or(i64::MAX);
                vec![Value::Text(k), text_value(v), Value::Int(size)]
            })
            .collect();
        let truncated = rows.len() > max_rows;
        rows.truncate(max_rows);
        return Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["key", "value", "size"]), rows, truncated } });
    }
    if verb == "stats" && lines.iter().any(|l| l.starts_with("STAT ")) {
        let mut rows: Vec<Vec<Value>> = lines
            .iter()
            .filter_map(|line| {
                let mut parts = line.splitn(3, ' ');
                if parts.next()? != "STAT" {
                    return None;
                }
                let name = parts.next()?.to_string();
                let value = parts.next().unwrap_or("").to_string();
                Some(vec![Value::Text(name), text_value(value)])
            })
            .collect();
        let truncated = rows.len() > max_rows;
        rows.truncate(max_rows);
        return Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["name", "value"]), rows, truncated } });
    }
    if lines.iter().any(|l| l.starts_with("ITEM ")) {
        let mut rows: Vec<Vec<Value>> = lines
            .iter()
            .filter(|l| l.starts_with("ITEM "))
            .map(|l| vec![Value::Text(l.trim_start_matches("ITEM ").to_string())])
            .collect();
        let truncated = rows.len() > max_rows;
        rows.truncate(max_rows);
        return Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["item"]), rows, truncated } });
    }
    let first = lines.first().map(String::as_str).unwrap_or("");
    match first.split_whitespace().next().unwrap_or("") {
        "STORED" | "DELETED" | "TOUCHED" | "OK" => Ok(StatementResult::Affected { rows_affected: 1 }),
        "NOT_STORED" | "NOT_FOUND" | "EXISTS" if is_write_command(&verb) => Ok(StatementResult::Affected { rows_affected: 0 }),
        _ => {
            let rows: Vec<Vec<Value>> = lines
                .into_iter()
                .filter(|l| l != "END")
                .map(|l| vec![text_value(l)])
                .collect();
            if rows.is_empty() {
                return Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["reply"]), rows: Vec::new(), truncated: false } });
            }
            Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["reply"]), rows, truncated: false } })
        }
    }
}

// WHAT:  Script → (command line, optional data block) pairs. Storage commands
//        take their data from the next line; a missing `bytes` field is filled
//        in from the data length so `set key 0 0` followed by the value works.
pub fn parse_script(script: &str) -> AppResult<Vec<(String, Option<String>)>> {
    let lines: Vec<&str> = script.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i].trim();
        i += 1;
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut words: Vec<String> = line.split_whitespace().map(str::to_string).collect();
        let verb = words.first().map(|w| w.to_ascii_lowercase()).unwrap_or_default();
        if is_storage_command(&verb) {
            let data = lines.get(i).map(|l| l.trim_end_matches('\r').to_string()).unwrap_or_default();
            i += 1;
            let has_noreply = words.last().is_some_and(|w| w.eq_ignore_ascii_case("noreply"));
            let fixed = if verb == "cas" { 6 } else { 5 };
            let body_len = data.len().to_string();
            let count = words.len() - usize::from(has_noreply);
            // set key flags exptime [bytes]
            if count == fixed - 1 {
                let at = words.len() - usize::from(has_noreply);
                words.insert(at, body_len);
            } else if count == fixed {
                let at = words.len() - 1 - usize::from(has_noreply);
                if verb == "cas" {
                    words[at - 1] = body_len;
                } else {
                    words[at] = body_len;
                }
            } else if count == 2 {
                // `set key` + data: default flags/exptime.
                words.push("0".into());
                words.push("0".into());
                words.push(body_len);
            } else {
                return Err(AppError::invalid_input(format!("`{line}`: expected `{verb} <key> <flags> <exptime>` followed by the value on the next line.")));
            }
            out.push((words.join(" "), Some(data)));
        } else {
            out.push((line.to_string(), None));
        }
    }
    Ok(out)
}

impl MemcachedIntegration {
    // WHAT:  Runs one command on the shared connection, reconnecting once on
    //        an I/O failure so a dropped socket does not poison the session.
    async fn command(&self, command: &str, data: Option<&str>) -> AppResult<Vec<String>> {
        let mut guard = self.stream.lock().await;
        if guard.is_none() {
            *guard = Some(open_stream(&self.addr).await?);
        }
        let first = match guard.as_mut() {
            Some(stream) => roundtrip(stream, command, data).await,
            None => Err(AppError::not_connected("memcached is not connected.")),
        };
        match first {
            Ok(lines) => Ok(lines),
            Err(_) => {
                *guard = None;
                let mut stream = open_stream(&self.addr).await?;
                let lines = roundtrip(&mut stream, command, data).await?;
                *guard = Some(stream);
                Ok(lines)
            }
        }
    }

    async fn stats(&self, arg: Option<&str>) -> AppResult<BTreeMap<String, String>> {
        let command = match arg {
            Some(a) => format!("stats {a}"),
            None => "stats".to_string(),
        };
        let lines = self.command(&command, None).await?;
        if let Some(first) = lines.first() {
            if first.starts_with("CLIENT_ERROR") || first.starts_with("SERVER_ERROR") || first == "ERROR" {
                return Err(AppError::driver(format!("memcached: {first}")));
            }
        }
        Ok(parse_stats(&lines))
    }

    // WHAT:  Best-effort key enumeration. Returns an empty list when the server
    //        refuses `stats cachedump` (removed in newer builds).
    async fn list_keys(&self, cap: usize) -> AppResult<Vec<String>> {
        let items = self.stats(Some("items")).await?;
        let mut keys: Vec<String> = Vec::new();
        for slab in parse_slab_ids(&items) {
            if keys.len() >= cap {
                break;
            }
            let lines = self.command(&format!("stats cachedump {slab} {CACHEDUMP_PER_SLAB}"), None).await?;
            if lines.first().is_some_and(|l| l.starts_with("CLIENT_ERROR") || l.starts_with("SERVER_ERROR") || l == "ERROR") {
                break;
            }
            keys.extend(parse_cachedump(&lines));
        }
        keys.sort();
        keys.dedup();
        keys.truncate(cap);
        Ok(keys)
    }

    async fn get_many(&self, keys: &[String]) -> AppResult<Vec<(String, String)>> {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(GET_BATCH) {
            let lines = self.command(&format!("get {}", chunk.join(" ")), None).await?;
            out.extend(parse_values(&lines));
        }
        Ok(out)
    }

    async fn key_rows(&self, cap: usize) -> AppResult<Vec<Vec<Value>>> {
        let keys = self.list_keys(cap).await?;
        let values = self.get_many(&keys).await?;
        Ok(values
            .into_iter()
            .map(|(k, v)| {
                let size = i64::try_from(v.len()).unwrap_or(i64::MAX);
                vec![Value::Text(k), text_value(v), Value::Int(size)]
            })
            .collect())
    }

    async fn stats_rows(&self) -> AppResult<Vec<Vec<Value>>> {
        let stats = self.stats(None).await?;
        Ok(stats.into_iter().map(|(k, v)| vec![Value::Text(k), text_value(v)]).collect())
    }
}

// ---------------------------------------------------------------------------
// Object explorer / stats
//
// WHAT:  `stats settings` → Setting objects, `stats conns` → Session objects,
//        `stats` + `stats slabs` → the Stats tab groups.
// WHY:   Memcached has no catalog beyond its stats families, and no command
//        that targets one setting or one connection, so these views are
//        read-only: there are no actions to offer.
// ---------------------------------------------------------------------------

const MAX_OBJECTS: usize = 2_000;
const DETAIL_PREVIEW: usize = 80;

#[derive(Debug, Clone, Default, PartialEq)]
struct SlabSummary {
    slabs: u64,
    pages: u64,
    malloced: Option<f64>,
}

// WHAT:  `stats slabs` (`STAT <id>:total_pages N`, `STAT active_slabs N`,
//        `STAT total_malloced N`) → slab class count / page total / bytes.
fn summarise_slabs(stats: &BTreeMap<String, String>) -> SlabSummary {
    let mut ids = std::collections::BTreeSet::new();
    let mut pages = 0u64;
    for (key, value) in stats {
        if let Some((id, field)) = key.split_once(':') {
            if let Ok(id) = id.parse::<u64>() {
                ids.insert(id);
                if field == "total_pages" {
                    pages += value.parse::<u64>().unwrap_or(0);
                }
            }
        }
    }
    let slabs = stats
        .get("active_slabs")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or_else(|| u64::try_from(ids.len()).unwrap_or(u64::MAX));
    SlabSummary { slabs, pages, malloced: stats.get("total_malloced").and_then(|v| v.parse::<f64>().ok()) }
}

// WHAT:  `stats conns` (`STAT <fd>:addr tcp:0.0.0.0:11211`, `STAT <fd>:state
//        conn_listening`, `STAT <fd>:secs_since_last_cmd 4`) → one field map
//        per connection, by descriptor.
fn parse_conns(stats: &BTreeMap<String, String>) -> BTreeMap<u64, BTreeMap<String, String>> {
    let mut out: BTreeMap<u64, BTreeMap<String, String>> = BTreeMap::new();
    for (key, value) in stats {
        if let Some((fd, field)) = key.split_once(':') {
            if let Ok(fd) = fd.parse::<u64>() {
                out.entry(fd).or_default().insert(field.to_string(), value.clone());
            }
        }
    }
    out
}

fn stat_num(stats: &BTreeMap<String, String>, key: &str) -> Option<f64> {
    stats.get(key).and_then(|v| v.parse::<f64>().ok())
}

fn mib(bytes: f64) -> f64 {
    (bytes / 1_048_576.0 * 100.0).round() / 100.0
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

fn preview(text: &str, max: usize) -> String {
    if text.chars().count() > max {
        format!("{}…", text.chars().take(max).collect::<String>())
    } else {
        text.to_string()
    }
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

fn ratio_percent(hits: Option<f64>, misses: Option<f64>) -> Option<f64> {
    match (hits, misses) {
        (Some(h), Some(m)) if h + m > 0.0 => Some((h / (h + m) * 10_000.0).round() / 100.0),
        _ => None,
    }
}

// WHAT:  `stats` + summarised `stats slabs` → Stats tab groups.
fn stats_groups(stats: &BTreeMap<String, String>, slabs: &SlabSummary) -> Vec<StatGroup> {
    let num = |key: &str| stat_num(stats, key);
    let text = |key: &str| stats.get(key).cloned().unwrap_or_default();

    let mut server = Vec::new();
    if stats.contains_key("version") {
        server.push(Stat::text("Version", text("version")));
    }
    if let Some(up) = num("uptime") {
        server.push(Stat::text("Uptime", format_duration(up)).with_hint(format!("{} s", format_number(up))));
    }
    push_number(&mut server, "Threads", num("threads"), None);
    push_number(&mut server, "CPU user", num("rusage_user"), Some("s"));
    push_number(&mut server, "CPU system", num("rusage_system"), Some("s"));

    let mut connections = Vec::new();
    push_number(&mut connections, "Current", num("curr_connections"), None);
    push_number(&mut connections, "Total", num("total_connections"), None);
    push_number(&mut connections, "Rejected", num("rejected_connections"), None);
    push_number(&mut connections, "Max simultaneous", num("max_connections"), None);
    push_number(&mut connections, "Listen disabled", num("listen_disabled_num"), None);

    let mut memory = Vec::new();
    push_bytes(&mut memory, "Used", num("bytes"));
    push_bytes(&mut memory, "Limit", num("limit_maxbytes"));
    if let (Some(used), Some(limit)) = (num("bytes"), num("limit_maxbytes")) {
        if limit > 0.0 {
            memory.push(Stat::number("Usage", (used / limit * 10_000.0).round() / 100.0, Some("%")));
        }
    }

    let mut items = Vec::new();
    push_number(&mut items, "Current", num("curr_items"), None);
    push_number(&mut items, "Total stored", num("total_items"), None);
    push_number(&mut items, "Evictions", num("evictions"), None);
    push_number(&mut items, "Reclaimed", num("reclaimed"), None);
    push_number(&mut items, "Expired unfetched", num("expired_unfetched"), None);
    push_number(&mut items, "Evicted unfetched", num("evicted_unfetched"), None);

    let mut cache = Vec::new();
    push_number(&mut cache, "Gets", num("cmd_get"), None);
    push_number(&mut cache, "Sets", num("cmd_set"), None);
    push_number(&mut cache, "Hits", num("get_hits"), None);
    push_number(&mut cache, "Misses", num("get_misses"), None);
    push_number(&mut cache, "Hit ratio", ratio_percent(num("get_hits"), num("get_misses")), Some("%"));
    push_number(&mut cache, "Flushes", num("cmd_flush"), None);
    push_number(&mut cache, "Touches", num("cmd_touch"), None);
    push_bytes(&mut cache, "Read", num("bytes_read"));
    push_bytes(&mut cache, "Written", num("bytes_written"));

    let mut slab_stats = vec![
        Stat::number("Slab classes", slabs.slabs as f64, None),
        Stat::number("Pages", slabs.pages as f64, None),
    ];
    push_bytes(&mut slab_stats, "Malloced", slabs.malloced);

    vec![
        group("Server", server),
        group("Connections", connections),
        group("Memory", memory),
        group("Items", items),
        group("Cache", cache),
        group("Slabs", slab_stats),
    ]
    .into_iter()
    .filter(|g| !g.stats.is_empty())
    .collect()
}

// WHAT:  A `stats <family>` the server answers with an error line (older
//        builds without `stats conns`, builds compiled without slabs stats).
fn is_unsupported(err: &AppError) -> bool {
    err.message().contains("ERROR")
}

impl MemcachedIntegration {
    // WHAT:  `stats <family>` that this build does not know → Ok(None).
    async fn stats_opt(&self, arg: &str) -> AppResult<Option<BTreeMap<String, String>>> {
        match self.stats(Some(arg)).await {
            Ok(map) => Ok(Some(map)),
            Err(err) if is_unsupported(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn session_summary(fd: u64, fields: &BTreeMap<String, String>) -> ObjectSummary {
        let get = |k: &str| fields.get(k).cloned().unwrap_or_default();
        let (addr, state, idle) = (get("addr"), get("state"), get("secs_since_last_cmd"));
        let detail = if idle.is_empty() { addr } else { format!("{addr} · last command {idle}s ago") };
        let mut summary = ObjectSummary::new(ObjectKind::Session, fd.to_string(), None).with_detail(detail);
        if !state.is_empty() {
            summary = summary.with_badge(state.trim_start_matches("conn_").to_string());
        }
        summary
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { views: true, ..Capabilities::KEY_VALUE },
        object_kinds: vec![K::Setting, K::Session],
        tools: vec![T::Stats, T::KeyBrowser],
    }
}

#[async_trait]
impl Integration for MemcachedIntegration {
    fn engine(&self) -> Engine {
        Engine::Memcached
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let lines = self.command("version", None).await?;
        match lines.first() {
            Some(l) if l.starts_with("VERSION") => Ok(()),
            Some(other) => Err(AppError::driver(format!("Unexpected memcached reply: {other}"))),
            None => Err(AppError::driver("memcached did not answer `version`.")),
        }
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let lines = self.command("version", None).await?;
        Ok(lines.first().and_then(|l| l.strip_prefix("VERSION ")).map(|v| format!("Memcached {}", v.trim())))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.addr.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.addr.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let stats = self.stats(None).await?;
        let curr_items = stats.get("curr_items").and_then(|v| v.parse::<i64>().ok());
        let tables = vec![
            TableInfo { schema: Some(SCHEMA_NAME.into()), name: KEYS_TABLE.into(), kind: TableKind::Table, row_estimate: curr_items },
            TableInfo {
                schema: Some(SCHEMA_NAME.into()),
                name: STATS_TABLE.into(),
                kind: TableKind::View,
                row_estimate: i64::try_from(stats.len()).ok(),
            },
        ];
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: SCHEMA_NAME.into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        match table.name.as_str() {
            KEYS_TABLE => Ok(key_columns()),
            STATS_TABLE => Ok(stats_columns()),
            other => Err(AppError::not_found(format!("Unknown memcached table \"{other}\"."))),
        }
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let stats = self.stats(None).await?;
        match table.name.as_str() {
            KEYS_TABLE => Ok(stats.get("curr_items").and_then(|v| v.parse::<i64>().ok())),
            STATS_TABLE => Ok(i64::try_from(stats.len()).ok()),
            other => Err(AppError::not_found(format!("Unknown memcached table \"{other}\"."))),
        }
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (names, rows): (Vec<String>, Vec<Vec<Value>>) = match table.name.as_str() {
            KEYS_TABLE => {
                if filters.is_empty() {
                    if let Some(n) = self.stats(None).await?.get("curr_items").and_then(|v| v.parse::<i64>().ok()) {
                        return Ok(n);
                    }
                }
                (key_columns().into_iter().map(|c| c.name).collect(), self.key_rows(MAX_KEYS).await?)
            }
            STATS_TABLE => (stats_columns().into_iter().map(|c| c.name).collect(), self.stats_rows().await?),
            other => return Err(AppError::not_found(format!("Unknown memcached table \"{other}\"."))),
        };
        let kept = local::apply_filters(&names, rows, filters);
        Ok(i64::try_from(kept.len()).unwrap_or(i64::MAX))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let (columns, rows) = match table.name.as_str() {
            KEYS_TABLE => (key_columns(), self.key_rows(MAX_KEYS).await?),
            STATS_TABLE => (stats_columns(), self.stats_rows().await?),
            other => return Err(AppError::not_found(format!("Unknown memcached table \"{other}\"."))),
        };
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let truncated = rows.len() >= MAX_KEYS;
        let rows = local::page(&names, rows, query);
        let columns = columns.into_iter().map(|c| ColumnMeta { name: c.name, type_name: c.data_type }).collect();
        Ok(ResultSet { columns, rows, truncated })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let commands = parse_script(sql)?;
        if commands.is_empty() {
            return Err(AppError::invalid_input("Nothing to run. Try `stats`, `get <key>` or `set <key> 0 0` with the value on the next line."));
        }
        let mut out = Vec::with_capacity(commands.len());
        for (command, data) in commands {
            let verb = command.split_whitespace().next().unwrap_or("").to_ascii_lowercase();
            if self.read_only && is_write_command(&verb) {
                return Err(AppError::invalid_input(format!("This connection is read-only; `{verb}` is refused.")));
            }
            if verb == "quit" {
                return Err(AppError::invalid_input("`quit` would close the session; disconnect from the sidebar instead."));
            }
            let lines = self.command(&command, data.as_deref()).await?;
            out.push(reply_to_statement(&command, lines, max_rows)?);
        }
        Ok(out)
    }

    async fn objects(&self, kind: ObjectKind, _parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Setting => {
                let settings = self.stats(Some("settings")).await?;
                Ok(settings
                    .into_iter()
                    .take(MAX_OBJECTS)
                    .map(|(name, value)| ObjectSummary::new(ObjectKind::Setting, name, None).with_detail(preview(&value, DETAIL_PREVIEW)))
                    .collect())
            }
            ObjectKind::Session => {
                let Some(conns) = self.stats_opt("conns").await? else {
                    return Ok(Vec::new());
                };
                Ok(parse_conns(&conns).iter().take(MAX_OBJECTS).map(|(fd, fields)| Self::session_summary(*fd, fields)).collect())
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Setting => {
                let settings = self.stats(Some("settings")).await?;
                let value = settings
                    .get(&reference.name)
                    .ok_or_else(|| AppError::not_found(format!("Setting \"{}\" is not reported by this server.", reference.name)))?;
                Ok(ObjectDetail::empty(reference).property("setting", reference.name.clone()).property("value", value.clone()))
            }
            ObjectKind::Session => {
                let fd = reference
                    .name
                    .parse::<u64>()
                    .map_err(|_| AppError::invalid_input(format!("\"{}\" is not a connection descriptor.", reference.name)))?;
                let conns = self
                    .stats_opt("conns")
                    .await?
                    .ok_or_else(|| AppError::not_found("This memcached build does not report `stats conns`."))?;
                let fields = parse_conns(&conns)
                    .remove(&fd)
                    .ok_or_else(|| AppError::not_found(format!("Connection {fd} is no longer open.")))?;
                let mut detail = ObjectDetail::empty(reference).property("descriptor", fd.to_string());
                for name in ["addr", "state", "secs_since_last_cmd"] {
                    if let Some(v) = fields.get(name) {
                        detail = detail.property(name, v.clone());
                    }
                }
                let rows = fields.iter().map(|(k, v)| vec![Value::Text(k.clone()), Value::Text(v.clone())]).collect();
                detail.rows = Some(ResultSet { columns: meta(&["field", "value"]), rows, truncated: false });
                Ok(detail)
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let stats = self.stats(None).await?;
        let slabs = self.stats_opt("slabs").await?.map(|s| summarise_slabs(&s)).unwrap_or_default();
        Ok(ServerStats::now(stats_groups(&stats, &slabs)))
    }

    async fn close(&self) {
        let mut guard = self.stream.lock().await;
        if let Some(mut stream) = guard.take() {
            let _ = stream.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, FilterOp, SortRule, SslMode};

    fn lines(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    fn resolved(host: &str, port: Option<u16>) -> ResolvedConnection {
        ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Memcached,
                environment: Environment::Local,
                read_only: false,
                host: Some(host.into()),
                port,
                database: None,
                username: None,
                file_path: None,
                ssl_mode: SslMode::Disable,
                has_secret: false,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: None,
        }
    }

    #[test]
    fn builds_address() {
        assert_eq!(address(&resolved("cache.local", None)), "cache.local:11211");
        assert_eq!(address(&resolved("cache.local", Some(11212))), "cache.local:11212");
        assert_eq!(address(&resolved("cache.local:5000", Some(1))), "cache.local:5000");
        assert_eq!(address(&resolved("  ", None)), "127.0.0.1:11211");
    }

    #[test]
    fn reply_shapes() {
        assert_eq!(reply_shape("get a b"), ReplyShape::UntilEnd);
        assert_eq!(reply_shape("stats items"), ReplyShape::UntilEnd);
        assert_eq!(reply_shape("set a 0 0 1"), ReplyShape::Line);
        assert_eq!(reply_shape("delete a noreply"), ReplyShape::None);
        assert_eq!(reply_shape("version"), ReplyShape::Line);
        assert!(is_write_command("flush_all"));
        assert!(!is_write_command("get"));
    }

    #[test]
    fn parses_protocol_replies() {
        let stats = parse_stats(&lines(&["STAT pid 1", "STAT curr_items 3", "STAT items:1:number 2", "STAT items:5:number 1", "END"]));
        assert_eq!(stats.get("curr_items").map(String::as_str), Some("3"));
        assert_eq!(parse_slab_ids(&stats), vec![1, 5]);
        let values = parse_values(&lines(&["VALUE a 0 3", "one", "VALUE b 0 7", "{\"x\":1}", "END"]));
        assert_eq!(values, vec![("a".to_string(), "one".to_string()), ("b".to_string(), "{\"x\":1}".to_string())]);
        assert_eq!(parse_cachedump(&lines(&["ITEM a [3 b; 0 s]", "ITEM b [7 b; 0 s]", "END"])), vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn maps_replies_to_statements() {
        let get = reply_to_statement("get a b", lines(&["VALUE a 0 3", "one", "VALUE b 0 7", "{\"x\":1}", "END"]), 10).unwrap_or_else(|e| panic!("{e}"));
        let StatementResult::Rows { result } = get else { panic!("rows") };
        assert_eq!(result.rows[1], vec![Value::Text("b".into()), Value::Json(serde_json::json!({"x": 1})), Value::Int(7)]);
        assert!(matches!(reply_to_statement("set a 0 0 1", lines(&["STORED"]), 10), Ok(StatementResult::Affected { rows_affected: 1 })));
        assert!(matches!(reply_to_statement("delete a", lines(&["NOT_FOUND"]), 10), Ok(StatementResult::Affected { rows_affected: 0 })));
        assert!(reply_to_statement("bogus", lines(&["ERROR"]), 10).is_err());
        let stats = reply_to_statement("stats", lines(&["STAT pid 1", "STAT uptime 5", "END"]), 1).unwrap_or_else(|e| panic!("{e}"));
        let StatementResult::Rows { result } = stats else { panic!("rows") };
        assert_eq!(result.rows.len(), 1);
        assert!(result.truncated);
        let incr = reply_to_statement("incr a 1", lines(&["6"]), 10).unwrap_or_else(|e| panic!("{e}"));
        let StatementResult::Rows { result } = incr else { panic!("rows") };
        assert_eq!(result.rows[0][0], Value::Text("6".into()));
        let empty = reply_to_statement("get missing", lines(&["END"]), 10).unwrap_or_else(|e| panic!("{e}"));
        let StatementResult::Rows { result } = empty else { panic!("rows") };
        assert!(result.rows.is_empty());
    }

    #[test]
    fn parses_scripts() {
        let script = parse_script("# comment\nset a 0 0\nhello world\nget a\nset b 0 0 999\n{\"k\":1}\ndelete a\nadd c\nxyz\ncas d 0 0 1 42\nzz").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(script[0], ("set a 0 0 11".to_string(), Some("hello world".to_string())));
        assert_eq!(script[1], ("get a".to_string(), None));
        assert_eq!(script[2], ("set b 0 0 7".to_string(), Some("{\"k\":1}".to_string())));
        assert_eq!(script[3], ("delete a".to_string(), None));
        assert_eq!(script[4], ("add c 0 0 3".to_string(), Some("xyz".to_string())));
        assert_eq!(script[5], ("cas d 0 0 2 42".to_string(), Some("zz".to_string())));
        assert!(parse_script("set a\nb").is_ok());
        assert!(parse_script("set\nb").is_err());
    }

    #[test]
    fn slab_and_conn_stats_summaries() {
        let slabs = parse_stats(&lines(&[
            "STAT 1:chunk_size 96",
            "STAT 1:total_pages 2",
            "STAT 5:chunk_size 240",
            "STAT 5:total_pages 3",
            "STAT active_slabs 2",
            "STAT total_malloced 5242880",
            "END",
        ]));
        assert_eq!(summarise_slabs(&slabs), SlabSummary { slabs: 2, pages: 5, malloced: Some(5_242_880.0) });
        assert_eq!(summarise_slabs(&BTreeMap::new()), SlabSummary::default());
        let conns = parse_stats(&lines(&[
            "STAT 26:addr tcp:0.0.0.0:11211",
            "STAT 26:state conn_listening",
            "STAT 28:addr tcp:127.0.0.1:52345",
            "STAT 28:state conn_waiting",
            "STAT 28:secs_since_last_cmd 4",
            "END",
        ]));
        let parsed = parse_conns(&conns);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.get(&28).and_then(|f| f.get("state")).map(String::as_str), Some("conn_waiting"));
        let listening = parsed.get(&26).map(|f| MemcachedIntegration::session_summary(26, f)).unwrap_or_else(|| panic!("fd 26"));
        assert_eq!(listening.badge.as_deref(), Some("listening"));
        assert_eq!(listening.detail.as_deref(), Some("tcp:0.0.0.0:11211"));
        let waiting = parsed.get(&28).map(|f| MemcachedIntegration::session_summary(28, f)).unwrap_or_else(|| panic!("fd 28"));
        assert_eq!(waiting.detail.as_deref(), Some("tcp:127.0.0.1:52345 · last command 4s ago"));
        assert!(is_unsupported(&AppError::driver("memcached: CLIENT_ERROR bad command line format")));
        assert!(is_unsupported(&AppError::driver("memcached: ERROR")));
        assert!(!is_unsupported(&AppError::driver("memcached closed the connection.")));
    }

    #[test]
    fn stats_groups_from_stats() {
        let stats = parse_stats(&lines(&[
            "STAT pid 1",
            "STAT uptime 3661",
            "STAT version 1.6.22",
            "STAT curr_connections 3",
            "STAT total_connections 10",
            "STAT cmd_get 100",
            "STAT cmd_set 40",
            "STAT get_hits 80",
            "STAT get_misses 20",
            "STAT bytes 524288",
            "STAT limit_maxbytes 67108864",
            "STAT curr_items 5",
            "STAT total_items 45",
            "STAT evictions 2",
            "STAT threads 4",
            "END",
        ]));
        let groups = stats_groups(&stats, &SlabSummary { slabs: 3, pages: 7, malloced: Some(1_048_576.0) });
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Connections", "Memory", "Items", "Cache", "Slabs"]);
        let find = |group: &str, label: &str| {
            groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label)).cloned()
        };
        assert_eq!(find("Server", "Uptime").map(|s| s.value), Some("1h 1m 1s".into()));
        assert_eq!(find("Server", "Threads").and_then(|s| s.numeric), Some(4.0));
        assert_eq!(find("Cache", "Hit ratio").and_then(|s| s.numeric), Some(80.0));
        assert_eq!(find("Memory", "Used").and_then(|s| s.numeric), Some(0.5));
        assert_eq!(find("Memory", "Usage").and_then(|s| s.numeric), Some(0.78));
        assert_eq!(find("Items", "Evictions").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Slabs", "Pages").and_then(|s| s.numeric), Some(7.0));
        assert_eq!(find("Slabs", "Malloced").and_then(|s| s.numeric), Some(1.0));
        assert!(find("Connections", "Rejected").is_none(), "absent stats are skipped, not zeroed");
        // A bare `stats` with nothing numeric still yields the slab group only.
        let empty = stats_groups(&BTreeMap::new(), &SlabSummary::default());
        assert_eq!(empty.iter().map(|g| g.title.as_str()).collect::<Vec<_>>(), vec!["Slabs"]);
    }

    // Live test: DBFREE_TEST_MEMCACHED_URL=host:port (e.g. `docker run --rm -d -p 11211:11211 memcached:alpine`).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_MEMCACHED_URL") else {
            return;
        };
        let mc = connect(&resolved(&url, None)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        mc.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        let version = mc.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("Memcached"), "{version}");
        let out = mc
            .execute("set dbfree:a 0 0\nhello\nset dbfree:b 0 0\n{\"n\": 2}\nget dbfree:a dbfree:b\ndelete dbfree:zzz\nstats", 500)
            .await
            .unwrap_or_else(|e| panic!("execute: {e}"));
        assert_eq!(out.len(), 5);
        assert!(matches!(out[0], StatementResult::Affected { rows_affected: 1 }));
        let StatementResult::Rows { result } = &out[2] else { panic!("rows") };
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[1][1], Value::Json(serde_json::json!({"n": 2})));
        assert!(matches!(out[3], StatementResult::Affected { rows_affected: 0 }));

        let catalog = mc.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert_eq!(catalog.schemas[0].tables.len(), 2);
        assert!(catalog.schemas[0].tables[0].row_estimate.unwrap_or_default() >= 2);

        let stats_table = TableRef { schema: Some(SCHEMA_NAME.into()), name: STATS_TABLE.into() };
        let page = mc
            .fetch_page(&stats_table, &PageQuery { sort: vec![SortRule { column: "name".into(), desc: false }], filters: vec![FilterRule { column: "name".into(), op: FilterOp::Eq, value: "pid".into() }], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("stats page: {e}"));
        assert_eq!(page.rows.len(), 1);
        assert!(mc.count(&stats_table, &[]).await.unwrap_or_default() > 10);

        let keys_table = TableRef { schema: Some(SCHEMA_NAME.into()), name: KEYS_TABLE.into() };
        let page = mc
            .fetch_page(&keys_table, &PageQuery { sort: vec![], filters: vec![FilterRule { column: "key".into(), op: FilterOp::StartsWith, value: "dbfree:".into() }], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("keys page: {e}"));
        // cachedump may be unsupported on this build; when it works both keys must show up.
        assert!(page.rows.is_empty() || page.rows.len() == 2, "{:?}", page.rows);
        let _ = mc.execute("delete dbfree:a\ndelete dbfree:b", 1).await;
        mc.close().await;
    }
}
