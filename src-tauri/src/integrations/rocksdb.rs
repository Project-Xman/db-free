// SOT: rocksdb-integration, rocksdb-adapter, column-families, rocksdb-command-parser, rocksdb-object-explorer, rocksdb-server-stats, rocksdb-properties

use crate::error::{AppError, AppResult};
use crate::integrations::http::local;
use crate::integrations::{Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat, StatGroup,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use rocksdb::{Direction, IteratorMode, Options, DB};
use std::sync::Arc;

// ============================================================================
// ROCKSDB ADAPTER
//
// WHAT:  Maps an embedded RocksDB directory onto the `Integration` contract.
// WHY:   RocksDB has no query language and no schema; the UI still needs
//        tables (column families), columns (key / value / size) and paging.
// HOW:   catalog     = one schema "column_families", one table per CF
//        columns     = fixed: key (pk), value (text / json auto), size (int)
//        fetch_page  = iterate from start (or from the key prefix when the
//                      filter on `key` allows it), cap at 5 000 entries, then
//                      client-side filter / sort / slice via http::local::page
//        execute     = one command per line: GET / PUT / DELETE / SCAN / KEYS / CF,
//                      optionally prefixed by `--cf <name>`
//        objects     = ColumnFamily (estimated keys, SST / memtable / cache
//                      sizes, files per level) and Setting (the options the
//                      directory was opened with + DB properties / reports)
//        stats       = totals across column families + compaction state, all
//                      via `property_value[_cf]` / `property_int_value[_cf]`
//        `rocksdb` is the only vendor crate used, and only in this file.
//        DB (single-threaded CF mode) is Send + Sync, so an Arc<DB> is shared
//        and every call runs on the blocking pool.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI meta)
// ============================================================================

impl From<rocksdb::Error> for AppError {
    fn from(err: rocksdb::Error) -> Self {
        AppError::driver(err)
    }
}

const SCHEMA_NAME: &str = "column_families";
const DEFAULT_CF: &str = "default";
const MAX_SCAN: usize = 5_000;
const MAX_COUNT: usize = 100_000;
const DEFAULT_SCAN_LIMIT: usize = 100;

pub struct RocksdbIntegration {
    db: Arc<DB>,
    dir_name: String,
    path: String,
    read_only: bool,
    cf_names: Vec<String>,
}

fn open(path: &str, read_only: bool) -> AppResult<(DB, Vec<String>)> {
    let mut opts = Options::default();
    let existing = DB::list_cf(&opts, path).unwrap_or_default();
    let mut cfs: Vec<String> = existing;
    if cfs.is_empty() {
        cfs.push(DEFAULT_CF.to_string());
    }
    let db = if read_only {
        if !std::path::Path::new(path).is_dir() {
            return Err(AppError::not_found(format!("RocksDB directory \"{path}\" does not exist (read-only connections do not create it).")));
        }
        DB::open_cf_for_read_only(&opts, path, &cfs, false)?
    } else {
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        DB::open_cf(&opts, path, &cfs)?
    };
    Ok((db, cfs))
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let path = conn
        .summary
        .file_path
        .clone()
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .ok_or_else(|| AppError::invalid_input("RocksDB connection has no directory path."))?;
    let read_only = conn.summary.read_only;
    let open_path = path.clone();
    let (db, cf_names) = tokio::task::spawn_blocking(move || open(&open_path, read_only))
        .await
        .map_err(AppError::internal)??;
    let dir_name = std::path::Path::new(&path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.clone());
    Ok(Arc::new(RocksdbIntegration { db: Arc::new(db), dir_name, path, read_only, cf_names }))
}

// ---------------------------------------------------------------------------
// Value decoding
// ---------------------------------------------------------------------------

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

fn bytes_to_value(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => text_value(text.to_string()),
        Err(_) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

fn key_to_value(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => Value::Text(text.to_string()),
        Err(_) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
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

fn column_names() -> Vec<String> {
    key_columns().into_iter().map(|c| c.name).collect()
}

fn meta(names: &[&str]) -> Vec<ColumnMeta> {
    names.iter().map(|n| ColumnMeta { name: (*n).to_string(), type_name: "text".to_string() }).collect()
}

// WHAT:  The key prefix implied by the page filters, so the scan can seek
//        instead of walking the whole column family.
fn key_prefix(filters: &[FilterRule]) -> Option<String> {
    filters
        .iter()
        .filter(|f| f.column == "key" && matches!(f.op, FilterOp::StartsWith | FilterOp::Eq))
        .map(|f| f.value.trim().to_string())
        .filter(|v| !v.is_empty())
        .max_by_key(String::len)
}

// ---------------------------------------------------------------------------
// Command language for the query tab
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Get { cf: Option<String>, key: String },
    Put { cf: Option<String>, key: String, value: String },
    Delete { cf: Option<String>, key: String },
    Scan { cf: Option<String>, prefix: Option<String>, limit: usize },
    Keys { cf: Option<String>, prefix: Option<String>, limit: usize },
    ListCfs,
}

impl Command {
    fn is_write(&self) -> bool {
        matches!(self, Command::Put { .. } | Command::Delete { .. })
    }
}

// WHAT:  `[--cf NAME] VERB args…`; PUT takes the rest of the line verbatim as
//        the value so JSON documents with spaces round-trip unchanged.
pub fn parse_command(line: &str) -> AppResult<Command> {
    let mut rest = line.trim();
    let mut cf: Option<String> = None;
    if let Some(after) = rest.strip_prefix("--cf") {
        let after = after.trim_start();
        let (name, tail) = split_word(after);
        if name.is_empty() {
            return Err(AppError::invalid_input("`--cf` needs a column family name."));
        }
        cf = Some(name.to_string());
        rest = tail.trim_start();
    }
    let (verb, tail) = split_word(rest);
    let tail = tail.trim_start();
    match verb.to_ascii_uppercase().as_str() {
        "GET" => {
            let (key, _) = split_word(tail);
            if key.is_empty() {
                return Err(AppError::invalid_input("GET needs a key."));
            }
            Ok(Command::Get { cf, key: key.to_string() })
        }
        "PUT" | "SET" => {
            let (key, value) = split_word(tail);
            let value = value.trim_start();
            if key.is_empty() {
                return Err(AppError::invalid_input("PUT needs a key and a value."));
            }
            Ok(Command::Put { cf, key: key.to_string(), value: value.to_string() })
        }
        "DELETE" | "DEL" => {
            let (key, _) = split_word(tail);
            if key.is_empty() {
                return Err(AppError::invalid_input("DELETE needs a key."));
            }
            Ok(Command::Delete { cf, key: key.to_string() })
        }
        "SCAN" | "KEYS" => {
            let mut words = tail.split_whitespace();
            let mut prefix: Option<String> = None;
            let mut limit = DEFAULT_SCAN_LIMIT;
            if let Some(first) = words.next() {
                match first.parse::<usize>() {
                    Ok(n) => limit = n,
                    Err(_) => {
                        prefix = Some(first.to_string());
                        if let Some(second) = words.next() {
                            limit = second
                                .parse()
                                .map_err(|_| AppError::invalid_input(format!("Limit \"{second}\" is not a number.")))?;
                        }
                    }
                }
            }
            let limit = limit.clamp(1, MAX_SCAN);
            if verb.eq_ignore_ascii_case("SCAN") {
                Ok(Command::Scan { cf, prefix, limit })
            } else {
                Ok(Command::Keys { cf, prefix, limit })
            }
        }
        "CF" | "CFS" | "COLUMNFAMILIES" => Ok(Command::ListCfs),
        "" => Err(AppError::invalid_input("Empty command.")),
        other => Err(AppError::invalid_input(format!(
            "Unknown command \"{other}\". Use GET <key>, PUT <key> <value>, DELETE <key>, SCAN [prefix] [limit], KEYS [prefix] [limit] or CF; prefix any of them with --cf <name>."
        ))),
    }
}

fn split_word(input: &str) -> (&str, &str) {
    let trimmed = input.trim_start();
    match trimmed.find(char::is_whitespace) {
        Some(i) => (&trimmed[..i], &trimmed[i..]),
        None => (trimmed, ""),
    }
}

pub fn parse_script(script: &str) -> AppResult<Vec<Command>> {
    let mut out = Vec::new();
    for (line_no, line) in script.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("--") && !trimmed.starts_with("--cf") {
            continue;
        }
        out.push(parse_command(trimmed).map_err(|e| AppError::invalid_input(format!("Line {}: {e}", line_no + 1)))?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Blocking helpers (run inside spawn_blocking)
// ---------------------------------------------------------------------------

fn cf_of<'a>(db: &'a DB, name: &str) -> AppResult<&'a rocksdb::ColumnFamily> {
    db.cf_handle(name).ok_or_else(|| AppError::not_found(format!("Column family \"{name}\" does not exist.")))
}

// WHAT:  Walks a column family from `prefix` (or the start), returning at most
//        `cap` (key, value) pairs that share the prefix.
// WHAT:  One raw key/value pair as RocksDB hands it back.
type Entry = (Box<[u8]>, Box<[u8]>);
fn scan_cf(db: &DB, cf_name: &str, prefix: Option<&str>, cap: usize) -> AppResult<Vec<Entry>> {
    let cf = cf_of(db, cf_name)?;
    let prefix_bytes = prefix.unwrap_or("").as_bytes();
    let mode = if prefix_bytes.is_empty() { IteratorMode::Start } else { IteratorMode::From(prefix_bytes, Direction::Forward) };
    let mut out = Vec::new();
    for item in db.iterator_cf(cf, mode) {
        let (key, value) = item?;
        if !key.starts_with(prefix_bytes) {
            break;
        }
        out.push((key, value));
        if out.len() >= cap {
            break;
        }
    }
    Ok(out)
}

fn count_cf(db: &DB, cf_name: &str, prefix: Option<&str>, cap: usize) -> AppResult<usize> {
    let cf = cf_of(db, cf_name)?;
    let prefix_bytes = prefix.unwrap_or("").as_bytes();
    let mode = if prefix_bytes.is_empty() { IteratorMode::Start } else { IteratorMode::From(prefix_bytes, Direction::Forward) };
    let mut n = 0usize;
    for item in db.iterator_cf(cf, mode) {
        let (key, _) = item?;
        if !key.starts_with(prefix_bytes) {
            break;
        }
        n += 1;
        if n >= cap {
            break;
        }
    }
    Ok(n)
}

fn estimate_cf(db: &DB, cf_name: &str) -> AppResult<Option<i64>> {
    let cf = cf_of(db, cf_name)?;
    let n = db.property_int_value_cf(cf, "rocksdb.estimate-num-keys")?;
    Ok(n.and_then(|v| i64::try_from(v).ok()))
}

fn entry_row(key: &[u8], value: &[u8]) -> Vec<Value> {
    vec![key_to_value(key), bytes_to_value(value), Value::Int(i64::try_from(value.len()).unwrap_or(i64::MAX))]
}

fn run_command(db: &DB, cf_names: &[String], read_only: bool, command: Command, max_rows: usize) -> AppResult<StatementResult> {
    if read_only && command.is_write() {
        return Err(AppError::invalid_input("This connection is read-only; PUT and DELETE are refused."));
    }
    let cf_name = |cf: &Option<String>| cf.clone().unwrap_or_else(|| DEFAULT_CF.to_string());
    match command {
        Command::Get { cf, key } => {
            let handle = cf_of(db, &cf_name(&cf))?;
            let value = db.get_cf(handle, key.as_bytes())?;
            let rows = match value {
                Some(bytes) => vec![entry_row(key.as_bytes(), &bytes)],
                None => Vec::new(),
            };
            Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["key", "value", "size"]), rows, truncated: false } })
        }
        Command::Put { cf, key, value } => {
            let handle = cf_of(db, &cf_name(&cf))?;
            db.put_cf(handle, key.as_bytes(), value.as_bytes())?;
            Ok(StatementResult::Affected { rows_affected: 1 })
        }
        Command::Delete { cf, key } => {
            let handle = cf_of(db, &cf_name(&cf))?;
            let existed = db.get_cf(handle, key.as_bytes())?.is_some();
            db.delete_cf(handle, key.as_bytes())?;
            Ok(StatementResult::Affected { rows_affected: u64::from(existed) })
        }
        Command::Scan { cf, prefix, limit } => {
            let cap = limit.min(max_rows.max(1));
            let entries = scan_cf(db, &cf_name(&cf), prefix.as_deref(), cap + 1)?;
            let truncated = entries.len() > cap;
            let rows: Vec<Vec<Value>> = entries.iter().take(cap).map(|(k, v)| entry_row(k, v)).collect();
            Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["key", "value", "size"]), rows, truncated } })
        }
        Command::Keys { cf, prefix, limit } => {
            let cap = limit.min(max_rows.max(1));
            let entries = scan_cf(db, &cf_name(&cf), prefix.as_deref(), cap + 1)?;
            let truncated = entries.len() > cap;
            let rows: Vec<Vec<Value>> = entries.iter().take(cap).map(|(k, _)| vec![key_to_value(k)]).collect();
            Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["key"]), rows, truncated } })
        }
        Command::ListCfs => {
            let mut rows = Vec::new();
            for name in cf_names {
                let estimate = estimate_cf(db, name).unwrap_or(None);
                rows.push(vec![Value::Text(name.clone()), estimate.map(Value::Int).unwrap_or(Value::Null)]);
            }
            Ok(StatementResult::Rows { result: ResultSet { columns: meta(&["column_family", "estimated_keys"]), rows, truncated: false } })
        }
    }
}

impl RocksdbIntegration {
    async fn blocking<T, F>(&self, f: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&DB) -> AppResult<T> + Send + 'static,
    {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || f(&db)).await.map_err(AppError::internal)?
    }
}

// ---------------------------------------------------------------------------
// Object explorer / stats
//
// WHAT:  Column families and the open options as objects; DB properties as
//        the Stats tab. Everything comes from `property_value[_cf]` /
//        `property_int_value[_cf]`; a property this build does not answer is
//        simply absent, never an error.
// WHY:   RocksDB has no admin command language (the query tab speaks
//        GET / PUT / DELETE / SCAN / KEYS / CF), so there are no actions; the
//        value is seeing sizes, memtables, cache and compaction state without
//        leaving the app.
// ---------------------------------------------------------------------------

const MAX_LEVELS: usize = 7;
const DETAIL_PREVIEW: usize = 80;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CfFigures {
    keys: Option<u64>,
    sst_bytes: Option<u64>,
    live_sst_bytes: Option<u64>,
    live_data_bytes: Option<u64>,
    memtable_bytes: Option<u64>,
    readers_bytes: Option<u64>,
    block_cache_bytes: Option<u64>,
    pending_compaction_bytes: Option<u64>,
    immutable_memtables: Option<u64>,
    files_per_level: Vec<u64>,
}

impl CfFigures {
    fn level0_files(&self) -> u64 {
        self.files_per_level.first().copied().unwrap_or(0)
    }
    fn total_files(&self) -> u64 {
        self.files_per_level.iter().sum()
    }
}

fn cf_int(db: &DB, cf: &rocksdb::ColumnFamily, property: &str) -> Option<u64> {
    db.property_int_value_cf(cf, property).ok().flatten()
}

fn db_int(db: &DB, property: &str) -> Option<u64> {
    db.property_int_value(property).ok().flatten()
}

fn db_text(db: &DB, property: &str) -> Option<String> {
    db.property_value(property).ok().flatten().filter(|t| !t.trim().is_empty())
}

// WHAT:  Every per-column-family figure the explorer shows, in one pass.
fn cf_figures(db: &DB, name: &str) -> AppResult<CfFigures> {
    let cf = cf_of(db, name)?;
    let mut files_per_level = Vec::new();
    for level in 0..MAX_LEVELS {
        match cf_int(db, cf, &format!("rocksdb.num-files-at-level{level}")) {
            Some(n) => files_per_level.push(n),
            None => break,
        }
    }
    Ok(CfFigures {
        keys: cf_int(db, cf, "rocksdb.estimate-num-keys"),
        sst_bytes: cf_int(db, cf, "rocksdb.total-sst-files-size"),
        live_sst_bytes: cf_int(db, cf, "rocksdb.live-sst-files-size"),
        live_data_bytes: cf_int(db, cf, "rocksdb.estimate-live-data-size"),
        memtable_bytes: cf_int(db, cf, "rocksdb.cur-size-all-mem-tables"),
        readers_bytes: cf_int(db, cf, "rocksdb.estimate-table-readers-mem"),
        block_cache_bytes: cf_int(db, cf, "rocksdb.block-cache-usage"),
        pending_compaction_bytes: cf_int(db, cf, "rocksdb.estimate-pending-compaction-bytes"),
        immutable_memtables: cf_int(db, cf, "rocksdb.num-immutable-mem-table"),
        files_per_level,
    })
}

fn mib(bytes: f64) -> f64 {
    (bytes / 1_048_576.0 * 100.0).round() / 100.0
}

fn bytes_text(bytes: Option<u64>) -> String {
    match bytes {
        Some(b) => format!("{} MiB ({} bytes)", format_number(mib(b as f64)), format_number(b as f64)),
        None => "—".to_string(),
    }
}

fn count_text(count: Option<u64>) -> String {
    count.map(|n| format_number(n as f64)).unwrap_or_else(|| "—".to_string())
}

fn preview(text: &str, max: usize) -> String {
    let flat = text.replace(['\n', '\r'], " ");
    if flat.chars().count() > max {
        format!("{}…", flat.chars().take(max).collect::<String>())
    } else {
        flat
    }
}

fn group(title: &str, stats: Vec<Stat>) -> StatGroup {
    StatGroup { title: title.to_string(), stats }
}

fn push_number(stats: &mut Vec<Stat>, label: &str, value: Option<u64>, unit: Option<&str>) {
    if let Some(v) = value {
        stats.push(Stat::number(label, v as f64, unit));
    }
}

fn push_bytes(stats: &mut Vec<Stat>, label: &str, bytes: Option<u64>) {
    if let Some(b) = bytes {
        stats.push(Stat::number(label, mib(b as f64), Some("MiB")).with_hint(format!("{} bytes", format_number(b as f64))));
    }
}

fn push_flag(stats: &mut Vec<Stat>, label: &str, value: Option<u64>) {
    if let Some(v) = value {
        stats.push(Stat::text(label, if v == 0 { "no" } else { "yes" }));
    }
}

// WHAT:  The options this adapter opened the directory with (`open`).
fn open_options(path: &str, read_only: bool, cf_names: &[String]) -> Vec<(String, String)> {
    let mut out = vec![
        ("path".to_string(), path.to_string()),
        ("mode".to_string(), if read_only { "read-only" } else { "read-write" }.to_string()),
        ("create_if_missing".to_string(), (!read_only).to_string()),
        ("create_missing_column_families".to_string(), (!read_only).to_string()),
        ("compression".to_string(), "snappy (library default; the adapter sets none)".to_string()),
        ("column_families".to_string(), cf_names.join(", ")),
    ];
    if read_only {
        out.push(("error_if_log_file_exist".to_string(), "false".to_string()));
    }
    out
}

// WHAT:  DB-wide properties that read like settings / state, listed as-is.
const DB_PROPERTIES: &[&str] = &[
    "rocksdb.background-errors",
    "rocksdb.num-running-compactions",
    "rocksdb.num-running-flushes",
    "rocksdb.compaction-pending",
    "rocksdb.mem-table-flush-pending",
    "rocksdb.block-cache-capacity",
    "rocksdb.block-cache-usage",
    "rocksdb.block-cache-pinned-usage",
    "rocksdb.num-snapshots",
    "rocksdb.num-live-versions",
    "rocksdb.is-write-stopped",
    "rocksdb.actual-delayed-write-rate",
    "rocksdb.min-log-number-to-keep",
    "rocksdb.is-file-deletions-enabled",
];

// WHAT:  Multi-line report properties: listed with a one-line summary, full
//        text in the detail's definition.
const REPORT_PROPERTIES: &[&str] = &["rocksdb.stats", "rocksdb.dbstats", "rocksdb.levelstats", "rocksdb.sstables", "rocksdb.options-statistics"];

// WHAT:  One-line summary of a report property: the `Uptime(secs)` line of
//        `rocksdb.stats` when present, else the first non-empty line.
fn report_summary(text: &str) -> String {
    let lines: Vec<&str> = text.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
    let pick = lines.iter().find(|l| l.starts_with("Uptime(secs)")).or_else(|| lines.first());
    match pick {
        Some(line) => format!("{} · {} lines", preview(line, DETAIL_PREVIEW), lines.len()),
        None => "empty".to_string(),
    }
}

fn setting_objects(db: &DB, path: &str, read_only: bool, cf_names: &[String]) -> Vec<ObjectSummary> {
    let mut out: Vec<ObjectSummary> = open_options(path, read_only, cf_names)
        .into_iter()
        .map(|(name, value)| ObjectSummary::new(ObjectKind::Setting, name, None).with_detail(preview(&value, DETAIL_PREVIEW)).with_badge("option"))
        .collect();
    for property in DB_PROPERTIES {
        if let Some(value) = db_text(db, property) {
            out.push(ObjectSummary::new(ObjectKind::Setting, *property, None).with_detail(preview(&value, DETAIL_PREVIEW)).with_badge("property"));
        }
    }
    for property in REPORT_PROPERTIES {
        if let Some(value) = db_text(db, property) {
            out.push(ObjectSummary::new(ObjectKind::Setting, *property, None).with_detail(report_summary(&value)).with_badge("report"));
        }
    }
    out
}

fn setting_detail(db: &DB, reference: &ObjectRef, path: &str, read_only: bool, cf_names: &[String]) -> AppResult<ObjectDetail> {
    let name = reference.name.as_str();
    if let Some((_, value)) = open_options(path, read_only, cf_names).into_iter().find(|(n, _)| n == name) {
        return Ok(ObjectDetail::empty(reference).property("option", name).property("value", value).property("source", "adapter open options"));
    }
    let value = db_text(db, name).ok_or_else(|| AppError::not_found(format!("Property \"{name}\" is not available on this build.")))?;
    let detail = ObjectDetail::empty(reference).property("property", name).property("source", "rocksdb property");
    if REPORT_PROPERTIES.contains(&name) {
        Ok(detail.property("summary", report_summary(&value)).definition(value, CodeLanguage::Text))
    } else {
        Ok(detail.property("value", value))
    }
}

fn column_family_objects(db: &DB, cf_names: &[String]) -> Vec<ObjectSummary> {
    let mut out: Vec<ObjectSummary> = cf_names
        .iter()
        .map(|name| {
            let figures = cf_figures(db, name).unwrap_or_default();
            let size = format_number(mib(figures.sst_bytes.unwrap_or(0) as f64));
            let mut summary = ObjectSummary::new(ObjectKind::ColumnFamily, name.clone(), None)
                .with_detail(format!("~{} keys · {size} MiB on disk", count_text(figures.keys)));
            if name == DEFAULT_CF {
                summary = summary.with_badge("default");
            }
            summary
        })
        .collect();
    out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    out
}

fn column_family_detail(db: &DB, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = reference.name.as_str();
    let figures = cf_figures(db, name)?;
    let mut detail = ObjectDetail::empty(reference)
        .property("estimated keys", count_text(figures.keys))
        .property("SST files size", bytes_text(figures.sst_bytes))
        .property("live SST files size", bytes_text(figures.live_sst_bytes))
        .property("estimated live data", bytes_text(figures.live_data_bytes))
        .property("memtables", bytes_text(figures.memtable_bytes))
        .property("immutable memtables", count_text(figures.immutable_memtables))
        .property("table readers memory", bytes_text(figures.readers_bytes))
        .property("block cache usage", bytes_text(figures.block_cache_bytes))
        .property("pending compaction", bytes_text(figures.pending_compaction_bytes));
    for (level, files) in figures.files_per_level.iter().enumerate() {
        detail = detail.property(&format!("files at level {level}"), format_number(*files as f64));
    }
    if let Some(cf) = db.cf_handle(name) {
        if let Some(text) = db.property_value_cf(cf, "rocksdb.cfstats-no-file-histogram").ok().flatten().filter(|t| !t.trim().is_empty()) {
            detail = detail.definition(text, CodeLanguage::Text);
        }
    }
    detail.columns = key_columns();
    Ok(detail)
}

// WHAT:  Totals across column families plus DB-wide compaction / cache state.
fn stats_groups(db: &DB, path: &str, read_only: bool, cf_names: &[String]) -> Vec<StatGroup> {
    let figures: Vec<CfFigures> = cf_names.iter().filter_map(|name| cf_figures(db, name).ok()).collect();
    let sum = |pick: fn(&CfFigures) -> Option<u64>| -> Option<u64> {
        let values: Vec<u64> = figures.iter().filter_map(pick).collect();
        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum())
        }
    };

    let mut storage = vec![Stat::number("Column families", cf_names.len() as f64, None)];
    push_number(&mut storage, "Estimated keys", sum(|f| f.keys), None);
    push_bytes(&mut storage, "SST files", sum(|f| f.sst_bytes));
    push_bytes(&mut storage, "Live SST files", sum(|f| f.live_sst_bytes));
    push_bytes(&mut storage, "Live data", sum(|f| f.live_data_bytes));
    storage.push(Stat::number("SST file count", figures.iter().map(CfFigures::total_files).sum::<u64>() as f64, None));
    storage.push(Stat::number("Files at level 0", figures.iter().map(CfFigures::level0_files).sum::<u64>() as f64, None));
    push_bytes(&mut storage, "Pending compaction", sum(|f| f.pending_compaction_bytes));

    let mut memory = Vec::new();
    push_bytes(&mut memory, "Memtables", sum(|f| f.memtable_bytes));
    push_number(&mut memory, "Immutable memtables", sum(|f| f.immutable_memtables), None);
    push_bytes(&mut memory, "Table readers", sum(|f| f.readers_bytes));
    push_bytes(&mut memory, "Block cache usage", db_int(db, "rocksdb.block-cache-usage"));
    push_bytes(&mut memory, "Block cache pinned", db_int(db, "rocksdb.block-cache-pinned-usage"));
    push_bytes(&mut memory, "Block cache capacity", db_int(db, "rocksdb.block-cache-capacity"));

    let mut background = Vec::new();
    push_number(&mut background, "Running compactions", db_int(db, "rocksdb.num-running-compactions"), None);
    push_number(&mut background, "Running flushes", db_int(db, "rocksdb.num-running-flushes"), None);
    push_flag(&mut background, "Compaction pending", db_int(db, "rocksdb.compaction-pending"));
    push_flag(&mut background, "Flush pending", db_int(db, "rocksdb.mem-table-flush-pending"));
    push_number(&mut background, "Background errors", db_int(db, "rocksdb.background-errors"), None);
    push_flag(&mut background, "Write stopped", db_int(db, "rocksdb.is-write-stopped"));
    push_number(&mut background, "Delayed write rate", db_int(db, "rocksdb.actual-delayed-write-rate"), Some("B/s"));

    let mut server = vec![
        Stat::text("Directory", path),
        Stat::text("Mode", if read_only { "read-only" } else { "read-write" }),
        Stat::number("Latest sequence number", db.latest_sequence_number() as f64, None),
    ];
    push_number(&mut server, "Snapshots", db_int(db, "rocksdb.num-snapshots"), None);
    push_number(&mut server, "Live versions", db_int(db, "rocksdb.num-live-versions"), None);

    vec![group("Storage", storage), group("Memory", memory), group("Background", background), group("Server", server)]
        .into_iter()
        .filter(|g| !g.stats.is_empty())
        .collect()
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, namespaces: true, exact_estimate: false, ..Capabilities::KEY_VALUE },
        object_kinds: vec![K::ColumnFamily, K::Setting],
        tools: vec![T::Stats, T::KeyBrowser],
    }
}

#[async_trait]
impl Integration for RocksdbIntegration {
    fn engine(&self) -> Engine {
        Engine::Rocksdb
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        self.blocking(|db| {
            db.property_value("rocksdb.num-files-at-level0")?;
            Ok(())
        })
        .await
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some("RocksDB (embedded)".to_string()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.dir_name.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.dir_name.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let names = self.cf_names.clone();
        self.blocking(move |db| {
            let tables = names
                .iter()
                .map(|name| TableInfo {
                    schema: Some(SCHEMA_NAME.to_string()),
                    name: name.clone(),
                    kind: TableKind::Table,
                    row_estimate: estimate_cf(db, name).unwrap_or(None),
                })
                .collect();
            Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: SCHEMA_NAME.to_string(), tables }] })
        })
        .await
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        if !self.cf_names.iter().any(|n| n == &table.name) {
            return Err(AppError::not_found(format!("Column family \"{}\" does not exist.", table.name)));
        }
        Ok(key_columns())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let name = table.name.clone();
        self.blocking(move |db| estimate_cf(db, &name)).await
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let name = table.name.clone();
        if filters.is_empty() {
            return self
                .blocking(move |db| match estimate_cf(db, &name)? {
                    Some(n) => Ok(n),
                    None => count_cf(db, &name, None, MAX_COUNT).map(|n| i64::try_from(n).unwrap_or(i64::MAX)),
                })
                .await;
        }
        let prefix = key_prefix(filters);
        let filters = filters.to_vec();
        self.blocking(move |db| {
            let entries = scan_cf(db, &name, prefix.as_deref(), MAX_COUNT)?;
            let rows: Vec<Vec<Value>> = entries.iter().map(|(k, v)| entry_row(k, v)).collect();
            let kept = local::apply_filters(&column_names(), rows, &filters);
            Ok(i64::try_from(kept.len()).unwrap_or(i64::MAX))
        })
        .await
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let name = table.name.clone();
        let query = query.clone();
        self.blocking(move |db| {
            let prefix = key_prefix(&query.filters);
            let window = (query.offset as usize).saturating_add(query.limit as usize).clamp(1, MAX_SCAN);
            // Sorting or non-prefix filters need the full (capped) window; a bare
            // key-ordered page only needs offset+limit entries.
            let key_ordered = query.sort.len() == 1 && query.sort[0].column == "key" && !query.sort[0].desc;
            let needs_all = (!query.sort.is_empty() && !key_ordered)
                || query.filters.iter().any(|f| f.column != "key" || f.op != FilterOp::StartsWith);
            let cap = if needs_all { MAX_SCAN } else { window };
            let entries = scan_cf(db, &name, prefix.as_deref(), cap)?;
            let rows: Vec<Vec<Value>> = entries.iter().map(|(k, v)| entry_row(k, v)).collect();
            let truncated = rows.len() >= MAX_SCAN;
            let rows = local::page(&column_names(), rows, &query);
            let columns = vec![
                ColumnMeta { name: "key".into(), type_name: "text".into() },
                ColumnMeta { name: "value".into(), type_name: "text".into() },
                ColumnMeta { name: "size".into(), type_name: "int".into() },
            ];
            Ok(ResultSet { columns, rows, truncated })
        })
        .await
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let commands = parse_script(sql)?;
        if commands.is_empty() {
            return Err(AppError::invalid_input("Nothing to run. Try `SCAN`, `GET <key>` or `CF`."));
        }
        let read_only = self.read_only;
        let cf_names = self.cf_names.clone();
        self.blocking(move |db| {
            let mut out = Vec::with_capacity(commands.len());
            for command in commands {
                out.push(run_command(db, &cf_names, read_only, command, max_rows)?);
            }
            Ok(out)
        })
        .await
    }

    async fn objects(&self, kind: ObjectKind, _parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let names = self.cf_names.clone();
        let path = self.path.clone();
        let read_only = self.read_only;
        match kind {
            ObjectKind::ColumnFamily => self.blocking(move |db| Ok(column_family_objects(db, &names))).await,
            ObjectKind::Setting => self.blocking(move |db| Ok(setting_objects(db, &path, read_only, &names))).await,
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let reference = reference.clone();
        let names = self.cf_names.clone();
        let path = self.path.clone();
        let read_only = self.read_only;
        match reference.kind {
            ObjectKind::ColumnFamily => self.blocking(move |db| column_family_detail(db, &reference)).await,
            ObjectKind::Setting => self.blocking(move |db| setting_detail(db, &reference, &path, read_only, &names)).await,
            _ => Ok(ObjectDetail::empty(&reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let names = self.cf_names.clone();
        let path = self.path.clone();
        let read_only = self.read_only;
        let groups = self.blocking(move |db| Ok(stats_groups(db, &path, read_only, &names))).await?;
        Ok(ServerStats::now(groups))
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SortRule, SslMode};

    fn resolved(path: &str, read_only: bool) -> ResolvedConnection {
        ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Rocksdb,
                environment: Environment::Local,
                read_only,
                host: None,
                port: None,
                database: None,
                username: None,
                file_path: Some(path.into()),
                ssl_mode: SslMode::Disable,
                has_secret: false,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: None,
        }
    }

    fn temp_dir(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir()
            .join(format!("dbfree-rocksdb-{tag}-{}-{nanos}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    fn rows_of(result: &StatementResult) -> &ResultSet {
        match result {
            StatementResult::Rows { result } => result,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse_command("GET user:1"), Ok(Command::Get { cf: None, key: "user:1".into() }));
        assert_eq!(
            parse_command("--cf users put user:1 {\"name\": \"Ada Lovelace\"}"),
            Ok(Command::Put { cf: Some("users".into()), key: "user:1".into(), value: "{\"name\": \"Ada Lovelace\"}".into() })
        );
        assert_eq!(parse_command("del k"), Ok(Command::Delete { cf: None, key: "k".into() }));
        assert_eq!(parse_command("SCAN"), Ok(Command::Scan { cf: None, prefix: None, limit: DEFAULT_SCAN_LIMIT }));
        assert_eq!(parse_command("SCAN 5"), Ok(Command::Scan { cf: None, prefix: None, limit: 5 }));
        assert_eq!(parse_command("KEYS user: 20"), Ok(Command::Keys { cf: None, prefix: Some("user:".into()), limit: 20 }));
        assert_eq!(parse_command("cf"), Ok(Command::ListCfs));
        assert!(parse_command("GET").is_err());
        assert!(parse_command("--cf").is_err());
        assert!(parse_command("FROB x").is_err());
        assert!(parse_command("SCAN a b").is_err());
        let script = parse_script("# comment\nGET a\n\nSCAN x 2\n").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(script.len(), 2);
        assert!(parse_script("GET a\nNOPE").is_err());
    }

    #[test]
    fn value_decoding_and_prefix() {
        assert_eq!(bytes_to_value(b"plain"), Value::Text("plain".into()));
        assert_eq!(bytes_to_value(b"{\"a\":1}"), Value::Json(serde_json::json!({"a": 1})));
        assert_eq!(bytes_to_value(b"[1,2]"), Value::Json(serde_json::json!([1, 2])));
        assert_eq!(bytes_to_value(b"{not json"), Value::Text("{not json".into()));
        assert_eq!(bytes_to_value(&[0xff, 0x00]), Value::Bytes("/wA=".into()));
        assert_eq!(key_to_value(b"[1]"), Value::Text("[1]".into()));
        let rule = |op: FilterOp, value: &str| FilterRule { column: "key".into(), op, value: value.into() };
        assert_eq!(key_prefix(&[rule(FilterOp::StartsWith, "user:")]), Some("user:".into()));
        assert_eq!(key_prefix(&[rule(FilterOp::Eq, "user:1"), rule(FilterOp::StartsWith, "u")]), Some("user:1".into()));
        assert_eq!(key_prefix(&[rule(FilterOp::Contains, "x")]), None);
        assert_eq!(key_prefix(&[]), None);
    }

    #[test]
    fn settings_reports_and_formatting() {
        let options = open_options("/data/db", true, &["default".to_string(), "users".to_string()]);
        let get = |name: &str| options.iter().find(|(n, _)| n == name).map(|(_, v)| v.clone());
        assert_eq!(get("mode").as_deref(), Some("read-only"));
        assert_eq!(get("create_if_missing").as_deref(), Some("false"));
        assert_eq!(get("column_families").as_deref(), Some("default, users"));
        assert_eq!(get("error_if_log_file_exist").as_deref(), Some("false"));
        let rw = open_options("/data/db", false, &[]);
        assert!(rw.iter().any(|(n, v)| n == "create_missing_column_families" && v == "true"));
        assert!(!rw.iter().any(|(n, _)| n == "error_if_log_file_exist"));

        let stats = "\n** DB Stats **\nUptime(secs): 12.3 total, 12.3 interval\nCumulative writes: 0 writes\n";
        assert_eq!(report_summary(stats), "Uptime(secs): 12.3 total, 12.3 interval · 3 lines");
        assert_eq!(report_summary("first\nsecond"), "first · 2 lines");
        assert_eq!(report_summary("  \n"), "empty");
        assert_eq!(mib(1_572_864.0), 1.5);
        assert_eq!(bytes_text(Some(1_048_576)), "1 MiB (1,048,576 bytes)");
        assert_eq!(bytes_text(None), "—");
        assert_eq!(count_text(Some(1234)), "1,234");
        assert_eq!(preview("a\nbcdef", 3), "a b…");
        let figures = CfFigures { files_per_level: vec![2, 0, 5], ..CfFigures::default() };
        assert_eq!(figures.level0_files(), 2);
        assert_eq!(figures.total_files(), 7);
        assert_eq!(CfFigures::default().level0_files(), 0);
    }

    #[tokio::test]
    async fn round_trip_on_temp_dir() {
        let path = temp_dir("rt");
        let db = connect(&resolved(&path, false)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        db.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert_eq!(db.engine(), Engine::Rocksdb);
        assert!(db.server_version().await.unwrap_or_default().unwrap_or_default().starts_with("RocksDB"));

        let script = "PUT user:1 {\"name\": \"Ada\", \"score\": 9}\nPUT user:2 Linus\nPUT user:3 Grace\nPUT cfg:theme dark\nGET user:2\nGET nope\nKEYS user:\nSCAN cfg 10\nCF";
        let results = db.execute(script, 100).await.unwrap_or_else(|e| panic!("execute: {e}"));
        assert_eq!(results.len(), 9);
        assert!(matches!(results[0], StatementResult::Affected { rows_affected: 1 }));
        let got = rows_of(&results[4]);
        assert_eq!(got.rows, vec![vec![Value::Text("user:2".into()), Value::Text("Linus".into()), Value::Int(5)]]);
        assert!(rows_of(&results[5]).rows.is_empty());
        assert_eq!(rows_of(&results[6]).rows.len(), 3);
        assert_eq!(rows_of(&results[7]).rows.len(), 1);
        assert_eq!(rows_of(&results[8]).rows[0][0], Value::Text("default".into()));

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert_eq!(catalog.schemas[0].name, SCHEMA_NAME);
        assert_eq!(catalog.schemas[0].tables[0].name, "default");

        let table = TableRef { schema: Some(SCHEMA_NAME.into()), name: "default".into() };
        let columns = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(columns.len(), 3);
        assert!(columns[0].primary_key);
        assert!(db.columns(&TableRef { schema: None, name: "missing".into() }).await.is_err());

        let prefix = FilterRule { column: "key".into(), op: FilterOp::StartsWith, value: "user:".into() };
        assert_eq!(db.count(&table, std::slice::from_ref(&prefix)).await.unwrap_or_default(), 3);
        let contains = FilterRule { column: "value".into(), op: FilterOp::Contains, value: "ada".into() };
        assert_eq!(db.count(&table, std::slice::from_ref(&contains)).await.unwrap_or_default(), 1);
        assert!(db.count(&table, &[]).await.unwrap_or_default() >= 0);

        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![SortRule { column: "key".into(), desc: true }], filters: vec![prefix.clone()], offset: 0, limit: 2 })
            .await
            .unwrap_or_else(|e| panic!("fetch_page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.rows[0][0], Value::Text("user:3".into()));
        assert_eq!(page.rows[1][0], Value::Text("user:2".into()));

        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![prefix], offset: 2, limit: 5 })
            .await
            .unwrap_or_else(|e| panic!("fetch_page: {e}"));
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0][0], Value::Text("user:3".into()));

        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("fetch_page: {e}"));
        assert_eq!(page.rows.len(), 4);
        assert_eq!(page.rows[1][1], Value::Json(serde_json::json!({"name": "Ada", "score": 9})));

        let cfs = db.objects(ObjectKind::ColumnFamily, None).await.unwrap_or_else(|e| panic!("cf objects: {e}"));
        assert_eq!(cfs.len(), 1);
        assert_eq!(cfs[0].reference.name, "default");
        assert_eq!(cfs[0].badge.as_deref(), Some("default"));
        assert!(cfs[0].detail.as_deref().is_some_and(|d| d.contains("keys")));
        let settings = db.objects(ObjectKind::Setting, None).await.unwrap_or_else(|e| panic!("settings: {e}"));
        assert!(settings.iter().any(|s| s.reference.name == "path" && s.badge.as_deref() == Some("option")));
        assert!(settings.iter().any(|s| s.reference.name == "rocksdb.stats" && s.badge.as_deref() == Some("report")));
        assert!(settings.iter().any(|s| s.reference.name == "rocksdb.num-running-compactions" && s.badge.as_deref() == Some("property")));
        assert!(db.objects(ObjectKind::Session, None).await.unwrap_or_default().is_empty());
        let cf_ref = |name: &str| ObjectRef { kind: ObjectKind::ColumnFamily, name: name.into(), parent: None };
        let cf_detail = db.object_detail(&cf_ref("default")).await.unwrap_or_else(|e| panic!("cf detail: {e}"));
        assert_eq!(cf_detail.columns.len(), 3);
        assert!(cf_detail.properties.iter().any(|p| p.name == "estimated keys"));
        assert!(cf_detail.properties.iter().any(|p| p.name == "files at level 0"));
        assert!(cf_detail.actions.is_empty());
        assert!(db.object_detail(&cf_ref("nope")).await.is_err());
        let setting_ref = |name: &str| ObjectRef { kind: ObjectKind::Setting, name: name.into(), parent: None };
        let report = db.object_detail(&setting_ref("rocksdb.stats")).await.unwrap_or_else(|e| panic!("stats detail: {e}"));
        assert_eq!(report.language, CodeLanguage::Text);
        assert!(report.definition.is_some());
        let mode = db.object_detail(&setting_ref("mode")).await.unwrap_or_else(|e| panic!("mode detail: {e}"));
        assert!(mode.properties.iter().any(|p| p.name == "value" && p.value == "read-write"));
        assert!(db.object_detail(&setting_ref("rocksdb.no-such-property")).await.is_err());
        let stats = db.server_stats().await.unwrap_or_else(|e| panic!("server stats: {e}"));
        let titles: Vec<&str> = stats.groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Storage", "Memory", "Background", "Server"], "{titles:?}");
        let keys = stats.groups[0].stats.iter().find(|s| s.label == "Estimated keys").and_then(|s| s.numeric);
        assert!(keys.is_some(), "estimate-num-keys must be answered: {:?}", stats.groups[0]);
        assert!(!stats.collected_at.is_empty());

        let del = db.execute("DELETE user:1\nDELETE user:1", 10).await.unwrap_or_else(|e| panic!("delete: {e}"));
        assert!(matches!(del[0], StatementResult::Affected { rows_affected: 1 }));
        assert!(matches!(del[1], StatementResult::Affected { rows_affected: 0 }));
        assert!(db.execute("--cf nope GET x", 10).await.is_err());
        drop(db);

        let ro = connect(&resolved(&path, true)).await.unwrap_or_else(|e| panic!("connect ro: {e}"));
        assert!(ro.execute("PUT a b", 10).await.is_err());
        let got = ro.execute("GET user:2", 10).await.unwrap_or_else(|e| panic!("get ro: {e}"));
        assert_eq!(rows_of(&got[0]).rows.len(), 1);
        drop(ro);
        let _ = std::fs::remove_dir_all(&path);

        let missing = temp_dir("missing");
        assert!(connect(&resolved(&missing, true)).await.is_err());
        assert!(!std::path::Path::new(&missing).exists());
    }
}
