// SOT: sqlite-integration, rusqlite-adapter, sqlite-value-decoding, sqlite-catalog-queries, sqlite-object-explorer, sqlite-pragma-settings, sqlite-server-stats

use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::error::{AppError, AppResult};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, ObjectAction, ObjectDetail, ObjectKind,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats,
    Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use rusqlite::fallible_iterator::FallibleIterator;
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, OpenFlags};
use std::sync::{Arc, Mutex};

impl From<rusqlite::Error> for AppError {
    fn from(err: rusqlite::Error) -> Self {
        AppError::driver(err)
    }
}

// WHAT:  SQLite adapter on rusqlite (sync), run on the blocking pool.
// WHY:   rusqlite exposes exact dynamic cell types, and sharing one libsqlite3
//        with the local store avoids two bundled SQLite builds.
// HOW:   A read-only connection opens the file with SQLITE_OPEN_READ_ONLY so the
//        engine itself refuses writes, on top of the guard's own check.
pub struct SqliteIntegration {
    conn: Arc<Mutex<Connection>>,
    file_name: String,
    /// Path as given by the connection; used to size the file and its WAL on disk.
    path: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let path = conn
        .summary
        .file_path
        .clone()
        .ok_or_else(|| AppError::invalid_input("SQLite connection has no file path."))?;
    let read_only = conn.summary.read_only;
    let open_path = path.clone();
    let connection = tokio::task::spawn_blocking(move || {
        let mut flags = OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI;
        flags |= if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        };
        Connection::open_with_flags(open_path, flags).map_err(AppError::from)
    })
    .await
    .map_err(AppError::internal)??;
    let file_name = std::path::Path::new(&path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.clone());
    Ok(Arc::new(SqliteIntegration { conn: Arc::new(Mutex::new(connection)), file_name, path }))
}

fn decode_cell(value: ValueRef<'_>, decl_type: &str) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => {
            if decl_type.eq_ignore_ascii_case("boolean") || decl_type.eq_ignore_ascii_case("bool") {
                Value::Bool(i != 0)
            } else {
                Value::Int(i)
            }
        }
        ValueRef::Real(f) => Value::Float(f),
        ValueRef::Text(bytes) => {
            let text = String::from_utf8_lossy(bytes).into_owned();
            if decl_type.to_ascii_uppercase().contains("JSON") {
                serde_json::from_str(&text).map(Value::Json).unwrap_or(Value::Text(text))
            } else if matches!(
                decl_type.to_ascii_uppercase().as_str(),
                "DATE" | "DATETIME" | "TIMESTAMP" | "TIME"
            ) {
                Value::DateTime(text)
            } else {
                Value::Text(text)
            }
        }
        ValueRef::Blob(bytes) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

impl SqliteIntegration {
    async fn blocking<T, F>(&self, f: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> AppResult<T> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| AppError::internal("sqlite session lock poisoned"))?;
            f(&guard)
        })
        .await
        .map_err(AppError::internal)?
    }
}

// WHAT:  Runs every statement in `sql` in order, collecting rows or change counts.
// HOW:   rusqlite::Batch walks the statement tail correctly (strings, comments).
fn run_batch(conn: &Connection, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
    let mut batch = rusqlite::Batch::new(conn, sql);
    let mut out = Vec::new();
    while let Some(mut stmt) = batch.next()? {
        if stmt.column_count() == 0 {
            let changed = stmt.execute([])?;
            out.push(StatementResult::Affected { rows_affected: changed as u64 });
            continue;
        }
        let columns: Vec<ColumnMeta> = stmt
            .columns()
            .iter()
            .map(|c| ColumnMeta {
                name: c.name().to_string(),
                type_name: c.decl_type().unwrap_or("").to_ascii_lowercase(),
            })
            .collect();
        let decl: Vec<String> = columns.iter().map(|c| c.type_name.clone()).collect();
        let mut rows = stmt.query([])?;
        let mut collected: Vec<Vec<Value>> = Vec::new();
        let mut truncated = false;
        while let Some(row) = rows.next()? {
            if collected.len() >= max_rows {
                truncated = true;
                break;
            }
            let mut cells = Vec::with_capacity(decl.len());
            for (i, decl_type) in decl.iter().enumerate() {
                cells.push(decode_cell(row.get_ref(i)?, decl_type));
            }
            collected.push(cells);
        }
        out.push(StatementResult::Rows { result: ResultSet { columns, rows: collected, truncated } });
    }
    Ok(out)
}

fn table_columns(conn: &Connection, name: &str) -> AppResult<Vec<ColumnInfo>> {
    let mut stmt = conn.prepare("SELECT cid, name, type, \"notnull\", pk FROM pragma_table_info(?1) ORDER BY cid")?;
    let rows = stmt
        .query_map(params![name], |row| {
            let cid: i64 = row.get(0)?;
            let notnull: i64 = row.get(3)?;
            let pk: i64 = row.get(4)?;
            Ok(ColumnInfo {
                name: row.get(1)?,
                data_type: row.get::<_, String>(2)?.to_ascii_lowercase(),
                nullable: notnull == 0,
                primary_key: pk > 0,
                ordinal: u32::try_from(cid).unwrap_or_default(),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// ============================================================================
// OBJECT EXPLORER
//
// WHAT:  Databases (PRAGMA database_list), tables / views / virtual tables /
//        indexes / triggers (sqlite_master), the useful PRAGMAs as settings,
//        per-object detail with admin actions, and server stats.
// WHY:   SQLite has no information_schema: sqlite_master plus the PRAGMA
//        table-valued functions are the whole catalog, so the explorer is a
//        handful of small queries over them.
// HOW:   `parent` is a database name (main / temp / attached) for table-like
//        kinds and the owning table for indexes and triggers; a parent that is
//        not an attached database is read as a table name. Identifiers are
//        quoted with quote_ident, names are bound as parameters, and PRAGMA
//        names only ever come from the constant lists below.
// WHERE: src-tauri/src/model/objects.rs, src/features/objects/ObjectTab.tsx
// ============================================================================

const OBJECT_CAP: usize = 2000;

#[derive(Debug, Clone)]
struct DatabaseEntry {
    name: String,
    file: String,
}

#[derive(Debug, Clone)]
struct MasterRow {
    db: String,
    kind: String,
    name: String,
    tbl_name: String,
    sql: Option<String>,
}

// WHAT:  One PRAGMA the Setting kind exposes. `options` are the values the
//        detail offers as one-click actions; numeric PRAGMAs get a definition
//        template instead (open it in a query tab, edit, run).
struct PragmaSpec {
    name: &'static str,
    writable: bool,
    options: &'static [&'static str],
    hint: &'static str,
}

const SETTINGS: &[PragmaSpec] = &[
    PragmaSpec { name: "journal_mode", writable: true, options: &["delete", "truncate", "persist", "memory", "wal", "off"], hint: "How the rollback journal / write-ahead log is kept." },
    PragmaSpec { name: "synchronous", writable: true, options: &["off", "normal", "full", "extra"], hint: "How aggressively SQLite fsyncs; lower is faster, less durable." },
    PragmaSpec { name: "foreign_keys", writable: true, options: &["on", "off"], hint: "Whether foreign-key constraints are enforced on this connection." },
    PragmaSpec { name: "page_size", writable: true, options: &[], hint: "Bytes per page. Changing it only takes effect on the next VACUUM." },
    PragmaSpec { name: "cache_size", writable: true, options: &[], hint: "Page cache: pages when positive, KiB when negative." },
    PragmaSpec { name: "auto_vacuum", writable: true, options: &["none", "full", "incremental"], hint: "Whether freed pages are reclaimed automatically (needs VACUUM to switch)." },
    PragmaSpec { name: "temp_store", writable: true, options: &["default", "file", "memory"], hint: "Where temporary tables and indexes live." },
    PragmaSpec { name: "mmap_size", writable: true, options: &[], hint: "Maximum bytes mapped into memory instead of read()." },
    PragmaSpec { name: "locking_mode", writable: true, options: &["normal", "exclusive"], hint: "Whether the file lock is released after each transaction." },
    PragmaSpec { name: "secure_delete", writable: true, options: &["on", "off", "fast"], hint: "Overwrite deleted content with zeros." },
    PragmaSpec { name: "recursive_triggers", writable: true, options: &["on", "off"], hint: "Allow triggers to fire themselves recursively." },
    PragmaSpec { name: "query_only", writable: true, options: &["on", "off"], hint: "Reject every data change on this connection." },
    PragmaSpec { name: "user_version", writable: true, options: &[], hint: "Application-defined schema version stored in the header." },
    PragmaSpec { name: "application_id", writable: true, options: &[], hint: "Application-defined file type id stored in the header." },
    PragmaSpec { name: "wal_autocheckpoint", writable: true, options: &[], hint: "WAL pages written before an automatic checkpoint." },
    PragmaSpec { name: "busy_timeout", writable: true, options: &[], hint: "Milliseconds to wait for a locked database." },
    PragmaSpec { name: "encoding", writable: false, options: &[], hint: "Text encoding, fixed once the database is created." },
    PragmaSpec { name: "page_count", writable: false, options: &[], hint: "Pages in the main database file." },
    PragmaSpec { name: "freelist_count", writable: false, options: &[], hint: "Unused pages reclaimable by VACUUM." },
    PragmaSpec { name: "schema_version", writable: false, options: &[], hint: "Bumped by every schema change." },
    PragmaSpec { name: "data_version", writable: false, options: &[], hint: "Changes when another connection commits." },
];

// WHAT:  `PRAGMA name = value;` with bare keywords / numbers and quoted text.
fn pragma_statement(name: &str, value: &str) -> String {
    let bare = !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if bare {
        format!("PRAGMA {name} = {value};")
    } else {
        format!("PRAGMA {name} = {};", quote_literal(value))
    }
}

// WHAT:  Text rendering of a decoded cell for property sheets and summaries.
fn value_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => s.clone(),
        Value::Json(j) => j.to_string(),
    }
}

fn is_virtual(sql: Option<&str>) -> bool {
    sql.is_some_and(|s| s.trim_start().to_ascii_uppercase().starts_with("CREATE VIRTUAL TABLE"))
}

// WHAT:  `CREATE VIRTUAL TABLE t USING fts5(a, b)` → (Some("fts5"), Some("a, b")).
fn virtual_module(sql: &str) -> (Option<String>, Option<String>) {
    let upper = sql.to_ascii_uppercase();
    let Some(pos) = upper.find(" USING ") else { return (None, None) };
    let rest = sql[pos + 7..].trim_start();
    let module: String = rest.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
    let args = rest
        .find('(')
        .and_then(|open| rest.rfind(')').filter(|close| *close > open).map(|close| rest[open + 1..close].trim().to_string()))
        .filter(|a| !a.is_empty());
    (Some(module).filter(|m| !m.is_empty()), args)
}

// WHAT:  Table options after the column list: WITHOUT ROWID / STRICT.
fn table_flags(sql: Option<&str>) -> Vec<&'static str> {
    let upper = sql.unwrap_or("").to_ascii_uppercase();
    let tail = upper.rsplit(')').next().unwrap_or("");
    let mut out = Vec::new();
    if tail.contains("WITHOUT ROWID") {
        out.push("without rowid");
    }
    if tail.split(|c: char| !c.is_ascii_alphabetic()).any(|w| w == "STRICT") {
        out.push("strict");
    }
    out
}

// WHAT:  (timing, event) of a trigger from its CREATE statement head.
fn trigger_facts(sql: &str) -> (Option<&'static str>, Option<&'static str>) {
    let upper = sql.to_ascii_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();
    let end = tokens.iter().position(|t| *t == "BEGIN").unwrap_or(tokens.len());
    let head = &tokens[..end];
    let timing = if head.contains(&"INSTEAD") {
        Some("INSTEAD OF")
    } else if head.contains(&"BEFORE") {
        Some("BEFORE")
    } else if head.contains(&"AFTER") {
        Some("AFTER")
    } else {
        None
    };
    let event = head.iter().find_map(|t| match *t {
        "INSERT" => Some("INSERT"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        _ => None,
    });
    (timing, event)
}

// WHAT:  The virtual table a shadow table (`docs_data`, `docs_idx`…) belongs to.
fn shadow_owner<'a>(name: &str, virtual_names: &'a [String]) -> Option<&'a str> {
    virtual_names
        .iter()
        .filter(|v| name.strip_prefix(v.as_str()).is_some_and(|rest| rest.starts_with('_')))
        .max_by_key(|v| v.len())
        .map(String::as_str)
}

fn human_bytes(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", value.round())
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

// WHAT:  A byte figure shown human-readable but still numeric for sparklines.
fn bytes_stat(label: &str, bytes: f64) -> Stat {
    let mut stat = Stat::number(label, bytes, None);
    stat.value = human_bytes(bytes);
    stat
}

fn qualified(db: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(db), quote_ident(name))
}

// WHAT:  Runs one read-only catalog query into a ResultSet (rows for the detail tab).
fn query_set(conn: &Connection, sql: &str, params: &[&dyn rusqlite::ToSql]) -> AppResult<ResultSet> {
    let mut stmt = conn.prepare(sql)?;
    let columns: Vec<ColumnMeta> = stmt
        .columns()
        .iter()
        .map(|c| ColumnMeta { name: c.name().to_string(), type_name: c.decl_type().unwrap_or("").to_ascii_lowercase() })
        .collect();
    let decl: Vec<String> = columns.iter().map(|c| c.type_name.clone()).collect();
    let mut rows = stmt.query(params)?;
    let mut collected: Vec<Vec<Value>> = Vec::new();
    while let Some(row) = rows.next()? {
        if collected.len() >= OBJECT_CAP {
            break;
        }
        let mut cells = Vec::with_capacity(decl.len());
        for (i, decl_type) in decl.iter().enumerate() {
            cells.push(decode_cell(row.get_ref(i)?, decl_type));
        }
        collected.push(cells);
    }
    Ok(ResultSet { columns, rows: collected, truncated: false })
}

// WHAT:  `PRAGMA [db.]name` as text; None when the build has no such pragma.
fn pragma_value(conn: &Connection, db: Option<&str>, name: &str) -> Option<String> {
    let sql = match db {
        Some(d) => format!("PRAGMA {}.{name}", quote_ident(d)),
        None => format!("PRAGMA {name}"),
    };
    conn.query_row(&sql, [], |row| Ok(decode_cell(row.get_ref(0)?, ""))).ok().map(|v| value_text(&v))
}

fn pragma_number(conn: &Connection, db: Option<&str>, name: &str) -> Option<f64> {
    pragma_value(conn, db, name).and_then(|v| v.parse::<f64>().ok())
}

fn database_list(conn: &Connection) -> AppResult<Vec<DatabaseEntry>> {
    let mut stmt = conn.prepare("PRAGMA database_list")?;
    let rows = stmt
        .query_map([], |row| Ok(DatabaseEntry { name: row.get(1)?, file: row.get::<_, Option<String>>(2)?.unwrap_or_default() }))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// WHAT:  sqlite_master rows of one database, optionally only those owned by `table`.
fn master_rows(conn: &Connection, db: &str, types: &[&str], table: Option<&str>) -> AppResult<Vec<MasterRow>> {
    let type_list = types.iter().map(|t| format!("'{t}'")).collect::<Vec<_>>().join(", ");
    let sql = format!(
        "SELECT type, name, tbl_name, sql FROM {}.sqlite_master WHERE type IN ({type_list}) \
         AND (name NOT LIKE 'sqlite_%' OR (type = 'index' AND name LIKE 'sqlite_autoindex_%')){} ORDER BY name LIMIT {OBJECT_CAP}",
        quote_ident(db),
        if table.is_some() { " AND tbl_name = ?1" } else { "" }
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = match table {
        Some(t) => stmt.query(params![t])?,
        None => stmt.query([])?,
    };
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(MasterRow {
            db: db.to_string(),
            kind: row.get(0)?,
            name: row.get(1)?,
            tbl_name: row.get(2)?,
            sql: row.get(3)?,
        });
    }
    Ok(out)
}

// WHAT:  Finds one named object, looking in the hinted database first, then main, then the rest.
fn find_master(conn: &Connection, types: &[&str], name: &str, hint: Option<&str>) -> AppResult<Option<MasterRow>> {
    let mut databases = database_list(conn)?;
    databases.sort_by_key(|d| match (hint, d.name.as_str()) {
        (Some(h), n) if h == n => 0,
        (_, "main") => 1,
        _ => 2,
    });
    let type_list = types.iter().map(|t| format!("'{t}'")).collect::<Vec<_>>().join(", ");
    for db in databases {
        let sql = format!("SELECT type, name, tbl_name, sql FROM {}.sqlite_master WHERE type IN ({type_list}) AND name = ?1", quote_ident(&db.name));
        let found = conn
            .query_row(&sql, params![name], |row| {
                Ok(MasterRow { db: db.name.clone(), kind: row.get(0)?, name: row.get(1)?, tbl_name: row.get(2)?, sql: row.get(3)? })
            })
            .ok();
        if found.is_some() {
            return Ok(found);
        }
    }
    Ok(None)
}

fn table_columns_in(conn: &Connection, db: &str, name: &str) -> AppResult<Vec<ColumnInfo>> {
    let mut stmt = conn.prepare("SELECT cid, name, type, \"notnull\", pk FROM pragma_table_info(?1, ?2) ORDER BY cid")?;
    let rows = stmt
        .query_map(params![name, db], |row| {
            let cid: i64 = row.get(0)?;
            let notnull: i64 = row.get(3)?;
            let pk: i64 = row.get(4)?;
            Ok(ColumnInfo {
                name: row.get(1)?,
                data_type: row.get::<_, String>(2)?.to_ascii_lowercase(),
                nullable: notnull == 0,
                primary_key: pk > 0,
                ordinal: u32::try_from(cid).unwrap_or_default(),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn table_summary(row: &MasterRow, virtual_names: &[String]) -> ObjectSummary {
    let mut summary = ObjectSummary::new(ObjectKind::Table, &row.name, Some(row.db.clone()));
    if let Some(owner) = shadow_owner(&row.name, virtual_names) {
        summary = summary.with_badge("shadow").with_detail(format!("shadow of {owner}"));
    } else {
        let flags = table_flags(row.sql.as_deref());
        if !flags.is_empty() {
            summary = summary.with_detail(flags.join(", "));
        }
    }
    summary
}

fn virtual_summary(row: &MasterRow) -> ObjectSummary {
    let (module, args) = row.sql.as_deref().map(virtual_module).unwrap_or((None, None));
    let mut summary = ObjectSummary::new(ObjectKind::VirtualTable, &row.name, Some(row.db.clone()));
    if let Some(m) = module {
        summary = summary.with_badge(m);
    }
    if let Some(a) = args {
        summary = summary.with_detail(a);
    }
    summary
}

fn index_summary(row: &MasterRow) -> ObjectSummary {
    let badge = match row.sql.as_deref() {
        None => "auto",
        Some(sql) if sql.to_ascii_uppercase().contains("UNIQUE") => "unique",
        Some(_) => "index",
    };
    ObjectSummary::new(ObjectKind::Index, &row.name, Some(row.tbl_name.clone()))
        .with_detail(format!("on {}", row.tbl_name))
        .with_badge(badge)
}

fn trigger_summary(row: &MasterRow) -> ObjectSummary {
    let (timing, event) = row.sql.as_deref().map(trigger_facts).unwrap_or((None, None));
    let mut summary = ObjectSummary::new(ObjectKind::Trigger, &row.name, Some(row.tbl_name.clone()))
        .with_detail(format!("{} on {}", timing.unwrap_or("").to_ascii_lowercase(), row.tbl_name).trim().to_string());
    if let Some(e) = event {
        summary = summary.with_badge(e.to_ascii_lowercase());
    }
    summary
}

fn database_summary(db: &DatabaseEntry) -> ObjectSummary {
    let badge = match db.name.as_str() {
        "main" => "main",
        "temp" => "temp",
        _ => "attached",
    };
    let detail = if db.file.is_empty() { "in-memory" } else { db.file.as_str() };
    ObjectSummary::new(ObjectKind::Database, &db.name, None).with_detail(detail).with_badge(badge)
}

fn setting_summary(spec: &PragmaSpec, value: &str) -> ObjectSummary {
    ObjectSummary::new(ObjectKind::Setting, spec.name, None)
        .with_detail(value.to_string())
        .with_badge(if spec.writable { "writable" } else { "read-only" })
}

// WHAT:  Which databases a `parent` selects: the named one, or all of them.
fn scope<'a>(databases: &'a [DatabaseEntry], parent: Option<&str>) -> Vec<&'a DatabaseEntry> {
    match parent {
        Some(p) if databases.iter().any(|d| d.name == p) => databases.iter().filter(|d| d.name == p).collect(),
        _ => databases.iter().collect(),
    }
}

fn list_objects(conn: &Connection, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
    let databases = database_list(conn)?;
    let parent_is_db = parent.is_some_and(|p| databases.iter().any(|d| d.name == p));
    let mut out: Vec<ObjectSummary> = Vec::new();
    match kind {
        ObjectKind::Database => out.extend(databases.iter().map(database_summary)),
        ObjectKind::Table | ObjectKind::VirtualTable => {
            for db in scope(&databases, parent) {
                let rows = master_rows(conn, &db.name, &["table"], None)?;
                let virtual_names: Vec<String> = rows.iter().filter(|r| is_virtual(r.sql.as_deref())).map(|r| r.name.clone()).collect();
                for row in &rows {
                    match (kind, is_virtual(row.sql.as_deref())) {
                        (ObjectKind::Table, false) => out.push(table_summary(row, &virtual_names)),
                        (ObjectKind::VirtualTable, true) => out.push(virtual_summary(row)),
                        _ => {}
                    }
                }
            }
        }
        ObjectKind::View => {
            for db in scope(&databases, parent) {
                out.extend(master_rows(conn, &db.name, &["view"], None)?.iter().map(|r| ObjectSummary::new(ObjectKind::View, &r.name, Some(r.db.clone()))));
            }
        }
        ObjectKind::Index | ObjectKind::Trigger => {
            let table = parent.filter(|_| !parent_is_db);
            let type_name = if kind == ObjectKind::Index { "index" } else { "trigger" };
            for db in scope(&databases, parent) {
                for row in master_rows(conn, &db.name, &[type_name], table)? {
                    out.push(if kind == ObjectKind::Index { index_summary(&row) } else { trigger_summary(&row) });
                }
            }
        }
        ObjectKind::Setting => {
            for spec in SETTINGS {
                if let Some(value) = pragma_value(conn, None, spec.name) {
                    out.push(setting_summary(spec, &value));
                }
            }
        }
        _ => {}
    }
    if kind != ObjectKind::Setting {
        out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    }
    out.truncate(OBJECT_CAP);
    Ok(out)
}

fn not_found(reference: &ObjectRef) -> AppError {
    AppError::not_found(format!("{:?} \"{}\" was not found.", reference.kind, reference.name))
}

fn table_detail(conn: &Connection, row: &MasterRow, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let target = qualified(&row.db, &row.name);
    let mut detail = ObjectDetail::empty(reference);
    if let Some(sql) = &row.sql {
        detail = detail.definition(sql, CodeLanguage::Sql);
    }
    detail.columns = table_columns_in(conn, &row.db, &row.name)?;
    let owned = master_rows(conn, &row.db, &["index", "trigger"], Some(row.name.as_str()))?;
    let (indexes, triggers): (Vec<&MasterRow>, Vec<&MasterRow>) = owned.iter().partition(|r| r.kind == "index");
    let count: Option<i64> = conn.query_row(&format!("SELECT count(*) FROM {target}"), [], |r| r.get(0)).ok();
    detail = detail.property("Database", row.db.clone());
    if let Some(n) = count {
        detail = detail.property("Rows", crate::model::objects::format_number(n as f64));
    }
    let column_count = detail.columns.len();
    detail = detail
        .property("Columns", column_count.to_string())
        .property("Indexes", indexes.len().to_string())
        .property("Triggers", triggers.len().to_string());
    for flag in table_flags(row.sql.as_deref()) {
        detail = detail.property("Option", flag);
    }
    let fks = query_set(
        conn,
        "SELECT id, seq, \"table\" AS references_table, \"from\" AS column_name, \"to\" AS referenced_column, on_update, on_delete, \"match\" \
         FROM pragma_foreign_key_list(?1, ?2) ORDER BY id, seq",
        &[&row.name, &row.db],
    )?;
    if !fks.rows.is_empty() {
        detail.rows = Some(fks);
    }
    detail.children = indexes.into_iter().map(index_summary).chain(triggers.into_iter().map(trigger_summary)).collect();
    Ok(detail
        .action(ObjectAction::new("analyze", "Analyze", format!("ANALYZE {target};")))
        .action(ObjectAction::new("reindex", "Reindex", format!("REINDEX {target};")))
        .action(ObjectAction::new("vacuum", "Vacuum database", format!("VACUUM {};", quote_ident(&row.db))))
        .action(ObjectAction::destructive("truncate", "Delete all rows", format!("DELETE FROM {target};")))
        .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {target};"))))
}

fn view_detail(conn: &Connection, row: &MasterRow, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let target = qualified(&row.db, &row.name);
    let mut detail = ObjectDetail::empty(reference).property("Database", row.db.clone());
    if let Some(sql) = &row.sql {
        detail = detail.definition(sql, CodeLanguage::Sql);
    }
    detail.columns = table_columns_in(conn, &row.db, &row.name).unwrap_or_default();
    detail.children = master_rows(conn, &row.db, &["trigger"], Some(row.name.as_str()))?.iter().map(trigger_summary).collect();
    Ok(detail.action(ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {target};"))))
}

fn virtual_table_detail(conn: &Connection, row: &MasterRow, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let target = qualified(&row.db, &row.name);
    let (module, args) = row.sql.as_deref().map(virtual_module).unwrap_or((None, None));
    let mut detail = ObjectDetail::empty(reference).property("Database", row.db.clone());
    if let Some(sql) = &row.sql {
        detail = detail.definition(sql, CodeLanguage::Sql);
    }
    if let Some(m) = &module {
        detail = detail.property("Module", m.clone());
    }
    if let Some(a) = args {
        detail = detail.property("Arguments", a);
    }
    // Columns need the module loaded; a missing module must not hide the definition.
    detail.columns = table_columns_in(conn, &row.db, &row.name).unwrap_or_default();
    let prefix = format!("{}_", row.name);
    detail.children = master_rows(conn, &row.db, &["table"], None)?
        .iter()
        .filter(|r| r.name.starts_with(&prefix))
        .map(|r| ObjectSummary::new(ObjectKind::Table, &r.name, Some(r.db.clone())).with_badge("shadow"))
        .collect();
    let is_fts = module.as_deref().is_some_and(|m| m.to_ascii_lowercase().starts_with("fts"));
    if is_fts {
        let command = |verb: &str| format!("INSERT INTO {target}({}) VALUES('{verb}');", quote_ident(&row.name));
        detail = detail
            .action(ObjectAction::new("optimize", "Optimize full-text index", command("optimize")))
            .action(ObjectAction::new("integrity-check", "Check full-text index", command("integrity-check")))
            .action(ObjectAction::destructive("rebuild", "Rebuild full-text index", command("rebuild")));
    }
    Ok(detail.action(ObjectAction::destructive("drop", "Drop virtual table", format!("DROP TABLE {target};"))))
}

fn index_detail(conn: &Connection, row: &MasterRow, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let target = qualified(&row.db, &row.name);
    let mut detail = ObjectDetail::empty(reference).property("Database", row.db.clone()).property("Table", row.tbl_name.clone());
    detail = match &row.sql {
        Some(sql) => detail.definition(sql, CodeLanguage::Sql),
        None => detail.definition("-- automatic index created for a PRIMARY KEY or UNIQUE constraint", CodeLanguage::Sql),
    };
    let facts: Option<(i64, String, i64)> = conn
        .query_row(
            "SELECT \"unique\", origin, partial FROM pragma_index_list(?1, ?2) WHERE name = ?3",
            params![row.tbl_name, row.db, row.name],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .ok();
    if let Some((unique, origin, partial)) = facts {
        let origin_label = match origin.as_str() {
            "c" => "CREATE INDEX",
            "u" => "UNIQUE constraint",
            "pk" => "PRIMARY KEY",
            other => other,
        };
        detail = detail
            .property("Unique", (unique != 0).to_string())
            .property("Origin", origin_label)
            .property("Partial", (partial != 0).to_string());
    }
    let columns = query_set(
        conn,
        // key = 1 keeps the declared index columns; xinfo also returns the implicit
        // rowid / included entries (key = 0), which are not part of the index key.
        "SELECT seqno, cid, name, \"desc\", coll, key FROM pragma_index_xinfo(?1, ?2) WHERE key = 1 ORDER BY seqno",
        &[&row.name, &row.db],
    )
    .or_else(|_| query_set(conn, "SELECT seqno, cid, name FROM pragma_index_info(?1, ?2) ORDER BY seqno", &[&row.name, &row.db]))?;
    detail.rows = Some(columns);
    detail = detail.action(ObjectAction::new("reindex", "Reindex", format!("REINDEX {target};")));
    if row.sql.is_some() {
        detail = detail.action(ObjectAction::destructive("drop", "Drop index", format!("DROP INDEX {target};")));
    }
    Ok(detail)
}

fn trigger_detail(row: &MasterRow, reference: &ObjectRef) -> ObjectDetail {
    let target = qualified(&row.db, &row.name);
    let (timing, event) = row.sql.as_deref().map(trigger_facts).unwrap_or((None, None));
    let mut detail = ObjectDetail::empty(reference).property("Database", row.db.clone()).property("Table", row.tbl_name.clone());
    if let Some(sql) = &row.sql {
        detail = detail.definition(sql, CodeLanguage::Sql);
    }
    if let Some(t) = timing {
        detail = detail.property("Timing", t);
    }
    if let Some(e) = event {
        detail = detail.property("Event", e);
    }
    detail.action(ObjectAction::destructive("drop", "Drop trigger", format!("DROP TRIGGER {target};")))
}

fn database_detail(conn: &Connection, db: &DatabaseEntry, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &db.name;
    let quoted = quote_ident(name);
    let mut detail = ObjectDetail::empty(reference).property("File", if db.file.is_empty() { "in-memory".to_string() } else { db.file.clone() });
    let page_size = pragma_number(conn, Some(name), "page_size");
    let page_count = pragma_number(conn, Some(name), "page_count");
    if let (Some(size), Some(count)) = (page_size, page_count) {
        detail = detail.property("Size", human_bytes(size * count)).property("Pages", crate::model::objects::format_number(count)).property("Page size", human_bytes(size));
    }
    for pragma in ["freelist_count", "journal_mode", "encoding", "user_version", "application_id"] {
        if let Some(v) = pragma_value(conn, Some(name), pragma) {
            detail = detail.property(pragma, v);
        }
    }
    let tables = master_rows(conn, name, &["table", "view"], None)?;
    let virtual_names: Vec<String> = tables.iter().filter(|r| is_virtual(r.sql.as_deref())).map(|r| r.name.clone()).collect();
    detail.children = tables
        .iter()
        .map(|r| match (r.kind.as_str(), is_virtual(r.sql.as_deref())) {
            ("view", _) => ObjectSummary::new(ObjectKind::View, &r.name, Some(r.db.clone())),
            (_, true) => virtual_summary(r),
            _ => table_summary(r, &virtual_names),
        })
        .collect();
    detail = detail
        .action(ObjectAction::new("integrity-check", "Integrity check", format!("PRAGMA {quoted}.integrity_check;")))
        .action(ObjectAction::new("fk-check", "Foreign key check", format!("PRAGMA {quoted}.foreign_key_check;")))
        .action(ObjectAction::new("analyze", "Analyze", format!("ANALYZE {quoted};")))
        .action(ObjectAction::new("optimize", "Optimize", "PRAGMA optimize;"))
        .action(ObjectAction::new("checkpoint", "Checkpoint WAL", format!("PRAGMA {quoted}.wal_checkpoint(TRUNCATE);")))
        .action(ObjectAction::new("vacuum", "Vacuum", format!("VACUUM {quoted};")));
    if name != "main" && name != "temp" {
        detail = detail.action(ObjectAction::destructive("detach", "Detach database", format!("DETACH DATABASE {quoted};")));
    }
    Ok(detail)
}

fn setting_detail(conn: &Connection, spec: &PragmaSpec, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let value = pragma_value(conn, None, spec.name).ok_or_else(|| not_found(reference))?;
    let definition = if spec.writable { pragma_statement(spec.name, &value) } else { format!("PRAGMA {};", spec.name) };
    let mut detail = ObjectDetail::empty(reference)
        .definition(definition, CodeLanguage::Sql)
        .property("Value", value.clone())
        .property("Writable", spec.writable.to_string())
        .property("Description", spec.hint);
    for option in spec.options {
        if !option.eq_ignore_ascii_case(&value) {
            detail = detail.action(ObjectAction::destructive(
                &format!("set-{option}"),
                &format!("Set {} = {option}", spec.name),
                pragma_statement(spec.name, option),
            ));
        }
    }
    Ok(detail)
}

fn describe_object(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let hint = reference.parent.as_deref();
    match reference.kind {
        ObjectKind::Database => {
            let db = database_list(conn)?.into_iter().find(|d| d.name == reference.name).ok_or_else(|| not_found(reference))?;
            database_detail(conn, &db, reference)
        }
        ObjectKind::Table | ObjectKind::VirtualTable => {
            let row = find_master(conn, &["table"], &reference.name, hint)?.ok_or_else(|| not_found(reference))?;
            if is_virtual(row.sql.as_deref()) {
                virtual_table_detail(conn, &row, reference)
            } else {
                table_detail(conn, &row, reference)
            }
        }
        ObjectKind::View => {
            let row = find_master(conn, &["view"], &reference.name, hint)?.ok_or_else(|| not_found(reference))?;
            view_detail(conn, &row, reference)
        }
        ObjectKind::Index => {
            let row = find_master(conn, &["index"], &reference.name, hint)?.ok_or_else(|| not_found(reference))?;
            index_detail(conn, &row, reference)
        }
        ObjectKind::Trigger => {
            let row = find_master(conn, &["trigger"], &reference.name, hint)?.ok_or_else(|| not_found(reference))?;
            Ok(trigger_detail(&row, reference))
        }
        ObjectKind::Setting => {
            let spec = SETTINGS.iter().find(|s| s.name == reference.name).ok_or_else(|| not_found(reference))?;
            setting_detail(conn, spec, reference)
        }
        _ => Ok(ObjectDetail::empty(reference)),
    }
}

fn file_size(path: &str) -> Option<f64> {
    std::fs::metadata(path).ok().filter(|m| m.is_file()).map(|m| m.len() as f64)
}

fn collect_stats(conn: &Connection, path: &str) -> AppResult<ServerStats> {
    let version: String = conn.query_row("SELECT sqlite_version()", [], |r| r.get(0))?;
    let text = |name: &str| pragma_value(conn, None, name).unwrap_or_default();
    let mut server = vec![Stat::text("SQLite version", version), Stat::text("File", path.to_string()), Stat::text("Journal mode", text("journal_mode"))];
    if let Some(v) = pragma_value(conn, None, "encoding") {
        server.push(Stat::text("Encoding", v));
    }
    let attached = database_list(conn)?.len();
    server.push(Stat::number("Attached databases", attached as f64, None));

    let page_size = pragma_number(conn, None, "page_size").unwrap_or(0.0);
    let page_count = pragma_number(conn, None, "page_count").unwrap_or(0.0);
    let freelist = pragma_number(conn, None, "freelist_count").unwrap_or(0.0);
    let mut storage = vec![
        bytes_stat("Database size", page_size * page_count).with_hint("page_count × page_size"),
        Stat::number("Pages", page_count, None),
        bytes_stat("Page size", page_size),
        Stat::number("Free pages", freelist, None),
        bytes_stat("Reclaimable", page_size * freelist).with_hint("Freed by VACUUM"),
    ];
    if let Some(size) = file_size(path) {
        storage.push(bytes_stat("File on disk", size));
    }
    if let Some(size) = file_size(&format!("{path}-wal")) {
        storage.push(bytes_stat("WAL file", size));
    }

    let mut cache = Vec::new();
    if let Some(v) = pragma_number(conn, None, "cache_size") {
        // Negative values are KiB, positive ones are pages.
        let bytes = if v < 0.0 { -v * 1024.0 } else { v * page_size };
        cache.push(bytes_stat("Page cache", bytes).with_hint(format!("cache_size = {v}")));
    }
    if let Some(v) = pragma_number(conn, None, "mmap_size") {
        cache.push(bytes_stat("Memory map", v));
    }
    for name in ["synchronous", "temp_store", "foreign_keys", "auto_vacuum", "busy_timeout", "wal_autocheckpoint", "user_version"] {
        if let Some(v) = pragma_value(conn, None, name) {
            cache.push(Stat::text(name, v));
        }
    }

    let mut counts = std::collections::BTreeMap::new();
    let mut stmt = conn.prepare("SELECT type, count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' GROUP BY type")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let kind: String = row.get(0)?;
        let n: i64 = row.get(1)?;
        counts.insert(kind, n as f64);
    }
    let schema = ["table", "index", "view", "trigger"]
        .iter()
        .map(|k| Stat::number(&format!("{}s", k[..1].to_ascii_uppercase() + &k[1..]), counts.get(*k).copied().unwrap_or(0.0), None))
        .collect();

    Ok(ServerStats::now(vec![
        StatGroup { title: "Server".into(), stats: server },
        StatGroup { title: "Storage".into(), stats: storage },
        StatGroup { title: "Schema".into(), stats: schema },
        StatGroup { title: "Settings".into(), stats: cache },
    ]))
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { sql: true, namespaces: false, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: true, exact_estimate: true },
        object_kinds: vec![K::Database, K::Table, K::View, K::VirtualTable, K::Index, K::Trigger, K::Setting],
        tools: vec![T::Stats, T::Erd],
    }
}

#[async_trait]
impl Integration for SqliteIntegration {
    fn engine(&self) -> Engine {
        Engine::Sqlite
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        self.blocking(|conn| {
            let v: String = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
            Ok(Some(format!("SQLite {v}")))
        })
        .await
    }

    fn current_database(&self) -> Option<String> {
        Some(self.file_name.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.file_name.clone()])
    }

    async fn ping(&self) -> AppResult<()> {
        self.blocking(|conn| {
            conn.query_row("SELECT 1", [], |_| Ok(()))?;
            Ok(())
        })
        .await
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        self.blocking(|conn| {
            let mut stmt = conn.prepare(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') \
                 AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )?;
            let tables = stmt
                .query_map([], |row| {
                    let kind: String = row.get(1)?;
                    Ok(TableInfo {
                        schema: None,
                        name: row.get(0)?,
                        kind: if kind == "view" { TableKind::View } else { TableKind::Table },
                        row_estimate: None,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: "main".to_string(), tables }] })
        })
        .await
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let name = table.name.clone();
        self.blocking(move |conn| table_columns(conn, &name)).await
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let sql = format!("SELECT count(*) FROM {}", quote_ident(&table.name));
        self.blocking(move |conn| {
            let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
            Ok(Some(count))
        })
        .await
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count(*) FROM {}{}", quote_ident(&table.name), where_clause(Engine::Sqlite, filters));
        self.blocking(move |conn| {
            let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
            Ok(count)
        })
        .await
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let order = if query.sort.is_empty() { " ORDER BY rowid".to_string() } else { order_clause(Engine::Sqlite, &query.sort) };
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            quote_ident(&table.name),
            where_clause(Engine::Sqlite, &query.filters),
            order,
            query.limit,
            query.offset
        );
        let max_rows = query.limit as usize;
        let mut statements = self.blocking(move |conn| run_batch(conn, &sql, max_rows)).await?;
        match statements.pop() {
            Some(StatementResult::Rows { result }) => Ok(result),
            _ => Ok(ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }),
        }
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let sql = sql.to_string();
        self.blocking(move |conn| run_batch(conn, &sql, max_rows)).await
    }

    async fn close(&self) {}

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        self.blocking(|conn| {
            let mut tables = conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?;
            let names = tables.query_map([], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut out: Vec<ForeignKey> = Vec::new();
            for name in names {
                let mut stmt = conn.prepare("SELECT id, seq, \"table\", \"from\", \"to\" FROM pragma_foreign_key_list(?1) ORDER BY id, seq")?;
                let rows = stmt
                    .query_map(params![name], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?, row.get::<_, Option<String>>(4)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for (id, to_table, from_col, to_col) in rows {
                    let key_name = format!("{name}_fk_{id}");
                    match out.iter_mut().find(|fk| fk.name == key_name) {
                        Some(fk) => {
                            fk.from_columns.push(from_col);
                            fk.to_columns.push(to_col.unwrap_or_default());
                        }
                        None => out.push(ForeignKey {
                            name: key_name,
                            from_schema: None,
                            from_table: name.clone(),
                            from_columns: vec![from_col],
                            to_schema: None,
                            to_table,
                            to_columns: vec![to_col.unwrap_or_default()],
                        }),
                    }
                }
            }
            Ok(out)
        })
        .await
    }

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let name = table.name.clone();
        self.blocking(move |conn| {
            let sql: Option<String> = conn
                .query_row("SELECT sql FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1", params![name], |row| row.get(0))
                .ok();
            Ok(sql)
        })
        .await
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let parent = parent.map(str::to_string);
        self.blocking(move |conn| list_objects(conn, kind, parent.as_deref())).await
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let reference = reference.clone();
        self.blocking(move |conn| describe_object(conn, &reference)).await
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let path = self.path.clone();
        self.blocking(move |conn| collect_stats(conn, &path)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode};

    fn resolved(path: &str, read_only: bool) -> ResolvedConnection {
        let input = ConnectionInput {
            name: "t".into(),
            engine: Engine::Sqlite,
            environment: Environment::Local,
            read_only,
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
            file_path: Some(path.into()),
            ssl_mode: SslMode::Disable,
        };
        ResolvedConnection { summary: ConnectionSummary::draft(&input, false), secret: None }
    }

    #[tokio::test]
    async fn end_to_end_on_a_temp_file() {
        let dir = std::env::temp_dir().join(format!("db-free-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        let path = dir.join("t.db").to_string_lossy().into_owned();
        let integration = connect(&resolved(&path, false)).await.unwrap_or_else(|e| panic!("{e}"));
        integration.ping().await.unwrap_or_else(|e| panic!("{e}"));

        let created = integration
            .execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, meta JSON, flag BOOLEAN); \
                 INSERT INTO users VALUES (1, 'ann', '{\"a\":1}', 1), (2, 'bob', NULL, 0); \
                 SELECT * FROM users ORDER BY id;",
                100,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(created.len(), 3);
        match created.get(2) {
            Some(StatementResult::Rows { result }) => {
                assert_eq!(result.rows.len(), 2);
                assert_eq!(result.columns.len(), 4);
                let first = result.rows.first().cloned().unwrap_or_default();
                assert_eq!(first.first(), Some(&Value::Int(1)));
                assert_eq!(first.get(1), Some(&Value::Text("ann".into())));
                assert!(matches!(first.get(2), Some(Value::Json(_))));
                assert_eq!(first.get(3), Some(&Value::Bool(true)));
            }
            other => panic!("expected rows, got {other:?}"),
        }

        let catalog = integration.catalog().await.unwrap_or_else(|e| panic!("{e}"));
        let names: Vec<&str> = catalog
            .schemas
            .iter()
            .flat_map(|s| s.tables.iter().map(|t| t.name.as_str()))
            .collect();
        assert_eq!(names, vec!["users"]);

        let table = TableRef { schema: None, name: "users".into() };
        let cols = integration.columns(&table).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(cols.iter().any(|c| c.name == "id" && c.primary_key));
        assert_eq!(integration.row_estimate(&table).await.unwrap_or_default(), Some(2));

        let query = PageQuery {
            sort: vec![crate::model::SortRule { column: "id".into(), desc: true }],
            filters: vec![crate::model::FilterRule { column: "name".into(), op: crate::model::FilterOp::Contains, value: "n".into() }],
            offset: 0,
            limit: 10,
        };
        let page = integration.fetch_page(&table, &query).await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(page.rows.len(), 1, "only 'ann' contains n");
        assert_eq!(integration.count(&table, &query.filters).await.unwrap_or_default(), 1);

        integration
            .execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))", 10)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let fks = integration.foreign_keys().await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(fks.len(), 1);
        assert_eq!(fks.first().map(|f| (f.from_table.as_str(), f.to_table.as_str())), Some(("orders", "users")));
        assert!(integration.ddl(&table).await.unwrap_or_default().is_some_and(|d| d.starts_with("CREATE TABLE")));
        let truncated = integration.execute("SELECT * FROM users", 1).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(truncated.first(), Some(StatementResult::Rows { result }) if result.truncated));

        // Read-only flag is enforced by SQLite itself.
        let ro = connect(&resolved(&path, true)).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(ro.execute("DELETE FROM users", 10).await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn catalog_text_parsers() {
        assert!(is_virtual(Some("CREATE VIRTUAL TABLE docs USING fts5(title, body)")));
        assert!(!is_virtual(Some("CREATE TABLE t (a)")));
        assert!(!is_virtual(None));
        assert_eq!(virtual_module("CREATE VIRTUAL TABLE docs USING fts5(title, body)"), (Some("fts5".into()), Some("title, body".into())));
        assert_eq!(virtual_module("create virtual table r using rtree(id, minx, maxx)"), (Some("rtree".into()), Some("id, minx, maxx".into())));
        assert_eq!(virtual_module("CREATE VIRTUAL TABLE x USING dbstat"), (Some("dbstat".into()), None));
        assert_eq!(virtual_module("CREATE TABLE t (a)"), (None, None));
        assert_eq!(table_flags(Some("CREATE TABLE t (a INT PRIMARY KEY, strict TEXT) WITHOUT ROWID, STRICT")), vec!["without rowid", "strict"]);
        assert!(table_flags(Some("CREATE TABLE t (a, strict)")).is_empty());
        assert_eq!(trigger_facts("CREATE TRIGGER audit AFTER UPDATE OF name ON users BEGIN INSERT INTO log VALUES (1); END"), (Some("AFTER"), Some("UPDATE")));
        assert_eq!(trigger_facts("CREATE TRIGGER v INSTEAD OF INSERT ON view_t BEGIN SELECT 1; END"), (Some("INSTEAD OF"), Some("INSERT")));
        assert_eq!(trigger_facts("CREATE TRIGGER d BEFORE DELETE ON t BEGIN SELECT 1; END"), (Some("BEFORE"), Some("DELETE")));
        let virtual_names = vec!["docs".to_string(), "docs_v2".to_string()];
        assert_eq!(shadow_owner("docs_data", &virtual_names), Some("docs"));
        assert_eq!(shadow_owner("docs_v2_idx", &virtual_names), Some("docs_v2"));
        assert_eq!(shadow_owner("documents", &virtual_names), None);
        assert_eq!(shadow_owner("docs", &virtual_names), None);
        assert_eq!(pragma_statement("journal_mode", "wal"), "PRAGMA journal_mode = wal;");
        assert_eq!(pragma_statement("cache_size", "-2000"), "PRAGMA cache_size = -2000;");
        assert_eq!(pragma_statement("encoding", "UTF-16le"), "PRAGMA encoding = UTF-16le;");
        assert_eq!(pragma_statement("x", "it's"), "PRAGMA x = 'it''s';");
        assert_eq!(human_bytes(0.0), "0 B");
        assert_eq!(human_bytes(512.0), "512 B");
        assert_eq!(human_bytes(4096.0), "4.0 KiB");
        assert_eq!(human_bytes(1.5 * 1024.0 * 1024.0), "1.5 MiB");
        let stat = bytes_stat("Size", 2048.0);
        assert_eq!(stat.value, "2.0 KiB");
        assert_eq!(stat.numeric, Some(2048.0));
        assert_eq!(value_text(&Value::Int(3)), "3");
        assert_eq!(value_text(&Value::Null), "");
    }

    #[tokio::test]
    async fn explorer_lists_and_describes_objects() {
        let dir = std::env::temp_dir().join(format!("db-free-explorer-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        let path = dir.join("x.db").to_string_lossy().into_owned();
        let db = connect(&resolved(&path, false)).await.unwrap_or_else(|e| panic!("{e}"));
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT) STRICT; \
             CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE, total REAL); \
             CREATE INDEX orders_user ON orders(user_id); \
             CREATE VIEW big_orders AS SELECT * FROM orders WHERE total > 100; \
             CREATE TRIGGER orders_audit AFTER INSERT ON orders BEGIN UPDATE users SET name = name WHERE id = NEW.user_id; END; \
             CREATE VIRTUAL TABLE docs USING fts5(title, body); \
             INSERT INTO users VALUES (1, 'a@x', 'ann'), (2, 'b@x', 'bob'); \
             PRAGMA journal_mode = wal;",
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("{e}"));

        let names = |items: &[ObjectSummary]| items.iter().map(|o| o.reference.name.clone()).collect::<Vec<_>>();

        let databases = db.objects(ObjectKind::Database, None).await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(names(&databases), vec!["main"]);
        assert_eq!(databases[0].badge.as_deref(), Some("main"));
        assert!(databases[0].detail.as_deref().is_some_and(|d| d.ends_with("x.db")));

        let tables = db.objects(ObjectKind::Table, None).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(names(&tables).contains(&"users".to_string()) && names(&tables).contains(&"orders".to_string()));
        assert!(!names(&tables).contains(&"docs".to_string()), "virtual tables are their own kind");
        let users = tables.iter().find(|t| t.reference.name == "users").unwrap_or_else(|| panic!("users"));
        assert_eq!(users.reference.parent.as_deref(), Some("main"));
        assert_eq!(users.detail.as_deref(), Some("strict"));
        let shadow = tables.iter().find(|t| t.reference.name == "docs_data").unwrap_or_else(|| panic!("fts5 shadow table"));
        assert_eq!(shadow.badge.as_deref(), Some("shadow"));

        let virtuals = db.objects(ObjectKind::VirtualTable, Some("main")).await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(names(&virtuals), vec!["docs"]);
        assert_eq!(virtuals[0].badge.as_deref(), Some("fts5"));
        assert_eq!(virtuals[0].detail.as_deref(), Some("title, body"));

        assert_eq!(names(&db.objects(ObjectKind::View, None).await.unwrap_or_default()), vec!["big_orders"]);

        let indexes = db.objects(ObjectKind::Index, None).await.unwrap_or_else(|e| panic!("{e}"));
        let orders_user = indexes.iter().find(|i| i.reference.name == "orders_user").unwrap_or_else(|| panic!("orders_user"));
        assert_eq!(orders_user.reference.parent.as_deref(), Some("orders"));
        assert_eq!(orders_user.badge.as_deref(), Some("index"));
        assert!(indexes.iter().any(|i| i.reference.name.starts_with("sqlite_autoindex_users") && i.badge.as_deref() == Some("auto")));
        let by_table = db.objects(ObjectKind::Index, Some("orders")).await.unwrap_or_default();
        assert_eq!(names(&by_table), vec!["orders_user"]);

        let triggers = db.objects(ObjectKind::Trigger, Some("orders")).await.unwrap_or_default();
        assert_eq!(names(&triggers), vec!["orders_audit"]);
        assert_eq!(triggers[0].badge.as_deref(), Some("insert"));
        assert_eq!(triggers[0].detail.as_deref(), Some("after on orders"));

        let settings = db.objects(ObjectKind::Setting, None).await.unwrap_or_default();
        let journal = settings.iter().find(|s| s.reference.name == "journal_mode").unwrap_or_else(|| panic!("journal_mode"));
        assert_eq!(journal.detail.as_deref(), Some("wal"));
        assert_eq!(journal.badge.as_deref(), Some("writable"));
        assert!(settings.iter().any(|s| s.reference.name == "page_count" && s.badge.as_deref() == Some("read-only")));

        let table_ref = ObjectRef { kind: ObjectKind::Table, name: "orders".into(), parent: Some("main".into()) };
        let detail = db.object_detail(&table_ref).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(detail.definition.as_deref().is_some_and(|d| d.starts_with("CREATE TABLE")));
        assert_eq!(detail.language, CodeLanguage::Sql);
        assert_eq!(detail.columns.len(), 3);
        let fk_rows = detail.rows.as_ref().unwrap_or_else(|| panic!("foreign keys as rows"));
        assert_eq!(fk_rows.rows.len(), 1);
        assert!(fk_rows.columns.iter().any(|c| c.name == "references_table"));
        assert_eq!(detail.children.len(), 2, "{:?}", detail.children);
        assert!(detail.children.iter().any(|c| c.reference.kind == ObjectKind::Index && c.reference.name == "orders_user"));
        assert!(detail.children.iter().any(|c| c.reference.kind == ObjectKind::Trigger));
        assert!(detail.properties.iter().any(|p| p.name == "Rows" && p.value == "0"));
        assert!(detail.actions.iter().any(|a| a.id == "drop" && a.destructive && a.statement == "DROP TABLE \"main\".\"orders\";"));
        assert!(detail.actions.iter().any(|a| a.id == "analyze" && !a.destructive));

        let users_detail = db.object_detail(&ObjectRef { kind: ObjectKind::Table, name: "users".into(), parent: None }).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(users_detail.properties.iter().any(|p| p.name == "Rows" && p.value == "2"));
        assert!(users_detail.properties.iter().any(|p| p.name == "Option" && p.value == "strict"));
        assert!(users_detail.rows.is_none());

        let index_detail = db.object_detail(&ObjectRef { kind: ObjectKind::Index, name: "orders_user".into(), parent: Some("orders".into()) }).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(index_detail.properties.iter().any(|p| p.name == "Origin" && p.value == "CREATE INDEX"));
        assert!(index_detail.properties.iter().any(|p| p.name == "Unique" && p.value == "false"));
        let cols = index_detail.rows.as_ref().unwrap_or_else(|| panic!("index columns"));
        assert_eq!(cols.rows.len(), 1);
        assert!(index_detail.actions.iter().any(|a| a.id == "drop"));
        let auto = db.object_detail(&ObjectRef { kind: ObjectKind::Index, name: "sqlite_autoindex_users_1".into(), parent: None }).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(!auto.actions.iter().any(|a| a.id == "drop"), "auto indexes cannot be dropped");

        let trigger = db.object_detail(&ObjectRef { kind: ObjectKind::Trigger, name: "orders_audit".into(), parent: None }).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(trigger.properties.iter().any(|p| p.name == "Timing" && p.value == "AFTER"));
        assert!(trigger.properties.iter().any(|p| p.name == "Event" && p.value == "INSERT"));

        let view = db.object_detail(&ObjectRef { kind: ObjectKind::View, name: "big_orders".into(), parent: None }).await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(view.columns.len(), 3);
        assert!(view.actions.iter().any(|a| a.statement == "DROP VIEW \"main\".\"big_orders\";"));

        let virtual_detail = db.object_detail(&ObjectRef { kind: ObjectKind::VirtualTable, name: "docs".into(), parent: None }).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(virtual_detail.properties.iter().any(|p| p.name == "Module" && p.value == "fts5"));
        assert!(virtual_detail.children.iter().all(|c| c.badge.as_deref() == Some("shadow")) && !virtual_detail.children.is_empty());
        assert!(virtual_detail.actions.iter().any(|a| a.id == "optimize" && a.statement.contains("VALUES('optimize')")));

        let database = db.object_detail(&ObjectRef { kind: ObjectKind::Database, name: "main".into(), parent: None }).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(database.properties.iter().any(|p| p.name == "journal_mode" && p.value == "wal"));
        assert!(database.children.iter().any(|c| c.reference.kind == ObjectKind::VirtualTable));
        assert!(!database.actions.iter().any(|a| a.id == "detach"), "main cannot be detached");

        let setting = db.object_detail(&ObjectRef { kind: ObjectKind::Setting, name: "journal_mode".into(), parent: None }).await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(setting.definition.as_deref(), Some("PRAGMA journal_mode = wal;"));
        assert!(setting.actions.iter().all(|a| a.destructive));
        assert!(setting.actions.iter().any(|a| a.statement == "PRAGMA journal_mode = delete;"));
        assert!(!setting.actions.iter().any(|a| a.statement == "PRAGMA journal_mode = wal;"), "current value is not offered");

        assert!(db.object_detail(&ObjectRef { kind: ObjectKind::Table, name: "nope".into(), parent: None }).await.is_err());

        let stats = db.server_stats().await.unwrap_or_else(|e| panic!("{e}"));
        let titles: Vec<&str> = stats.groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Storage", "Schema", "Settings"]);
        let schema = &stats.groups[2];
        assert!(schema.stats.iter().any(|s| s.label == "Tables" && s.numeric.is_some_and(|n| n >= 2.0)));
        assert!(stats.groups[1].stats.iter().any(|s| s.label == "File on disk" && s.numeric.is_some_and(|n| n > 0.0)));
        assert!(stats.groups[0].stats.iter().any(|s| s.label == "Journal mode" && s.value == "wal"));

        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
