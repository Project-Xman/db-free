// SOT: mysql-integration, mariadb, sqlx-mysql-adapter, mysql-value-decoding, mysql-catalog-queries, mysql-object-explorer, mysql-server-stats, mysql-admin-actions

use crate::error::{AppError, AppResult};
use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{qualified_name_for, quote_ident_for, Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, ObjectAction, ObjectDetail, ObjectKind,
    ObjectProperty, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    ServerStats, SslMode, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use futures::StreamExt;
use sqlx::mysql::{MySql, MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow, MySqlSslMode};
use sqlx::{Column, Decode, Either, Executor, Row, TypeInfo, ValueRef};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

// WHAT:  MySQL and MariaDB adapter on sqlx (MariaDB speaks the MySQL protocol).
// WHY:   One adapter, two engines: the struct remembers which one the user
//        picked so `engine()` reports it faithfully and the UI labels match.
// HOW:   Every query — including catalog lookups — runs through the text
//        protocol (`raw_sql`), so cells arrive as text and decode by column
//        type name. That sidesteps sqlx's per-type compatibility checks and the
//        unsigned/signed integer split. Databases are the namespaces here
//        (MySQL has no schema layer), so `TableRef.schema` is always None and
//        switching database means reconnecting.
// WHERE: src-tauri/src/integrations/mod.rs (trait, quoting), sql.rs (WHERE/ORDER builders)
pub struct MysqlIntegration {
    pool: MySqlPool,
    engine: Engine,
    database: Option<String>,
}

const SYSTEM_DATABASES: [&str; 4] = ["information_schema", "performance_schema", "mysql", "sys"];

fn ssl_mode(mode: SslMode) -> MySqlSslMode {
    match mode {
        SslMode::Disable => MySqlSslMode::Disabled,
        SslMode::Prefer => MySqlSslMode::Preferred,
        SslMode::Require => MySqlSslMode::Required,
        SslMode::VerifyCa => MySqlSslMode::VerifyCa,
        SslMode::VerifyFull => MySqlSslMode::VerifyIdentity,
    }
}

// WHAT:  Opens a small pool (4). An empty database connects server-wide; the
//        sidebar then offers `databases()` for switching.
pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .map(str::to_string);
    let mut opts = MySqlConnectOptions::new()
        .host(s.host.as_deref().unwrap_or("localhost"))
        .port(s.port.unwrap_or(3306))
        .ssl_mode(ssl_mode(s.ssl_mode));
    if let Some(db) = database.as_deref() {
        opts = opts.database(db);
    }
    if let Some(user) = s.username.as_deref() {
        opts = opts.username(user);
    }
    if let Some(secret) = conn.secret.as_deref() {
        opts = opts.password(secret);
    }
    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(15))
        .connect_with(opts)
        .await?;
    let engine = s.engine;
    Ok(Arc::new(MysqlIntegration { pool, engine, database }))
}

// WHAT:  "8.4.0" → "MySQL 8.4.0"; "11.4.2-MariaDB-ubu2404" → "MariaDB 11.4.2-MariaDB-ubu2404".
fn describe_version(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.to_ascii_lowercase().contains("mariadb") {
        format!("MariaDB {trimmed}")
    } else {
        format!("MySQL {trimmed}")
    }
}

// WHAT:  Column types that always carry raw bytes.
// WHY:   sqlx names a column BINARY/VARBINARY from the wire-level BINARY flag,
//        which MySQL also sets for `_bin` collated text and for SHOW output, so
//        those two are decoded as text when the payload is valid UTF-8 instead.
fn is_blob_type(type_name: &str) -> bool {
    matches!(type_name, "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BIT" | "GEOMETRY")
}

// WHAT:  True when a column the driver calls binary may really hold JSON text.
// WHY:   MariaDB has no distinct JSON type: `JSON` is an alias for LONGTEXT
//        with a CHECK constraint, and sqlx surfaces that column as BLOB. Without
//        this, a MariaDB JSON column renders as an opaque base64 blob instead of
//        a browsable document. MySQL reports JSON properly and is unaffected.
fn may_hold_json(type_name: &str) -> bool {
    matches!(
        type_name,
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "VARCHAR" | "CHAR" | "BLOB" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB"
    )
}

// WHAT:  Promotes a text payload that is a JSON object or array to Value::Json.
// WHY:   So MariaDB JSON columns get the same tree inspector as MySQL's.
fn promote_json(text: &str) -> Option<Value> {
    let trimmed = text.trim_start();
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return None;
    }
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(json) if json.is_object() || json.is_array() => Some(Value::Json(json)),
        _ => None,
    }
}

fn is_binary_flagged_string(type_name: &str) -> bool {
    matches!(type_name, "BINARY" | "VARBINARY")
}

// WHAT:  Text-protocol cell → Value, driven by sqlx's column type name
//        ("BIGINT UNSIGNED", "DECIMAL", "JSON", "DATETIME", ...).
fn text_to_value(type_name: &str, text: &str) -> Value {
    let upper = type_name.to_ascii_uppercase();
    let base = upper.split_whitespace().next().unwrap_or_default();
    match base {
        "BOOLEAN" => Value::Bool(text == "1" || text.eq_ignore_ascii_case("true")),
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" | "YEAR" => text
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or_else(|_| Value::Decimal(text.to_string())),
        "FLOAT" | "DOUBLE" => text
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or_else(|_| Value::Text(text.to_string())),
        "DECIMAL" | "NEWDECIMAL" | "NUMERIC" => Value::Decimal(text.to_string()),
        "JSON" => serde_json::from_str(text)
            .map(Value::Json)
            .unwrap_or_else(|_| Value::Text(text.to_string())),
        "DATE" | "DATETIME" | "TIMESTAMP" | "TIME" => Value::DateTime(text.to_string()),
        "NULL" => Value::Null,
        _ => Value::Text(text.to_string()),
    }
}

fn decode_cell(row: &MySqlRow, index: usize) -> Value {
    let raw = match row.try_get_raw(index) {
        Ok(raw) => raw,
        Err(err) => return Value::Unsupported(err.to_string()),
    };
    if raw.is_null() {
        return Value::Null;
    }
    let type_name = raw.type_info().name().to_ascii_uppercase();
    if is_blob_type(&type_name) {
        return match <Vec<u8> as Decode<'_, MySql>>::decode(raw) {
            Ok(bytes) => {
                // A MariaDB JSON column arrives here as BLOB; promote it when the
                // payload really is a JSON document, otherwise keep it binary.
                if may_hold_json(&type_name) {
                    if let Ok(text) = std::str::from_utf8(&bytes) {
                        if let Some(json) = promote_json(text) {
                            return json;
                        }
                    }
                }
                Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes))
            }
            Err(_) => Value::Unsupported(format!("<{}>", type_name.to_ascii_lowercase())),
        };
    }
    if is_binary_flagged_string(&type_name) {
        return match <Vec<u8> as Decode<'_, MySql>>::decode(raw) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(text) => Value::Text(text),
                Err(err) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(err.into_bytes())),
            },
            Err(_) => Value::Unsupported(format!("<{}>", type_name.to_ascii_lowercase())),
        };
    }
    match <String as Decode<'_, MySql>>::decode(raw) {
        Ok(text) => {
            if may_hold_json(&type_name) {
                if let Some(json) = promote_json(&text) {
                    return json;
                }
            }
            text_to_value(&type_name, &text)
        }
        Err(_) => match row.try_get_raw(index) {
            // Non-UTF-8 payload in a text column: keep it as bytes rather than fail the row.
            Ok(raw) => <Vec<u8> as Decode<'_, MySql>>::decode(raw)
                .map(|b| Value::Bytes(base64::engine::general_purpose::STANDARD.encode(b)))
                .unwrap_or_else(|_| Value::Unsupported(format!("<{}>", type_name.to_ascii_lowercase()))),
            Err(_) => Value::Unsupported(format!("<{}>", type_name.to_ascii_lowercase())),
        },
    }
}

// WHAT:  Decoder for the adapter's own catalog queries (information_schema, SHOW …).
// WHY:   Those result columns are text by construction, yet MySQL flags many of
//        them BINARY, so the user-data decoder would base64 them.
fn decode_meta_cell(row: &MySqlRow, index: usize) -> Value {
    let raw = match row.try_get_raw(index) {
        Ok(raw) => raw,
        Err(err) => return Value::Unsupported(err.to_string()),
    };
    if raw.is_null() {
        return Value::Null;
    }
    let type_name = raw.type_info().name().to_ascii_uppercase();
    match <String as Decode<'_, MySql>>::decode(raw) {
        Ok(text) => {
            if may_hold_json(&type_name) {
                if let Some(json) = promote_json(&text) {
                    return json;
                }
            }
            text_to_value(&type_name, &text)
        }
        Err(_) => Value::Unsupported(format!("<{}>", type_name.to_ascii_lowercase())),
    }
}

fn columns_of(row: &MySqlRow) -> Vec<ColumnMeta> {
    row.columns()
        .iter()
        .map(|c| ColumnMeta {
            name: c.name().to_string(),
            type_name: c.type_info().name().to_ascii_lowercase(),
        })
        .collect()
}

fn cell_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::Text(t)) | Some(Value::DateTime(t)) | Some(Value::Decimal(t)) => t.clone(),
        Some(Value::Int(i)) => i.to_string(),
        Some(Value::Float(f)) => f.to_string(),
        Some(Value::Bool(b)) => if *b { "1".to_string() } else { "0".to_string() },
        Some(Value::Json(j)) => j.to_string(),
        Some(Value::Bytes(b)) | Some(Value::Unsupported(b)) => b.clone(),
        Some(Value::Null) | None => String::new(),
    }
}

fn cell_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Int(i)) => Some(*i),
        Some(Value::Decimal(t)) | Some(Value::Text(t)) => t.parse::<i64>().ok(),
        Some(Value::Float(f)) => Some(*f as i64),
        _ => None,
    }
}

impl MysqlIntegration {
    fn require_database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    // WHAT:  Runs `sql` through the text protocol and groups rows per statement.
    // HOW:   sqlx yields Right(row) per row and Left(result) when a statement ends.
    async fn run(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        self.run_with(sql, max_rows, decode_cell).await
    }

    async fn run_with(
        &self,
        sql: &str,
        max_rows: usize,
        decode: fn(&MySqlRow, usize) -> Value,
    ) -> AppResult<Vec<StatementResult>> {
        let mut stream = (&self.pool).fetch_many(sqlx::raw_sql(sql));
        let mut out = Vec::new();
        let mut current: Option<ResultSet> = None;
        while let Some(item) = stream.next().await {
            match item? {
                Either::Right(row) => {
                    let set = current.get_or_insert_with(|| ResultSet {
                        columns: columns_of(&row),
                        rows: Vec::new(),
                        truncated: false,
                    });
                    if set.rows.len() < max_rows {
                        let width = set.columns.len();
                        set.rows.push((0..width).map(|i| decode(&row, i)).collect());
                    } else {
                        set.truncated = true;
                    }
                }
                Either::Left(done) => match current.take() {
                    Some(result) => out.push(StatementResult::Rows { result }),
                    None => out.push(StatementResult::Affected { rows_affected: done.rows_affected() }),
                },
            }
        }
        if let Some(result) = current.take() {
            out.push(StatementResult::Rows { result });
        }
        Ok(out)
    }

    // WHAT:  First result set (columns + rows) of one of the adapter's own metadata queries.
    async fn query_set(&self, sql: &str) -> AppResult<ResultSet> {
        let statements = self.run_with(sql, usize::MAX, decode_meta_cell).await?;
        Ok(statements
            .into_iter()
            .find_map(|s| match s {
                StatementResult::Rows { result } => Some(result),
                StatementResult::Affected { .. } => None,
            })
            .unwrap_or_else(|| ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }))
    }

    // WHAT:  Rows of the first result set of one of the adapter's own metadata queries.
    async fn query_rows(&self, sql: &str) -> AppResult<Vec<Vec<Value>>> {
        Ok(self.query_set(sql).await?.rows)
    }
}

// ============================================================================
// OBJECT EXPLORER / ADMINISTRATION
//
// WHAT:  Lists and describes everything MySQL exposes beyond rows: databases,
//        tables, views, partitions, indexes, constraints, routines, triggers,
//        events, accounts and grants, sessions, InnoDB locks, replication,
//        global variables and the statement digest table.
// WHY:   The object explorer and the admin page are generic; this block turns
//        information_schema / performance_schema / SHOW output into the neutral
//        `ObjectSummary` / `ObjectDetail` / `ServerStats` shapes.
// HOW:   Pure SQL builders (`object_list_sql`) and row mappers (`summarize`)
//        are unit-tested offline; the async methods only run them. Nested
//        kinds (index, constraint, trigger, partition) carry `db.table` in
//        `reference.parent` so a detail request knows both halves; MySQL
//        forbids `.` in database and table names, so the split is unambiguous.
//        Everything privilege-gated (mysql.user, performance_schema, SHOW
//        REPLICA STATUS) has a fallback or degrades to a clear message.
// WHERE: src-tauri/src/model/objects.rs (contract), src/features/objects (UI)
// ============================================================================

const OBJECT_CAP: usize = 2000;
const PREVIEW_CHARS: usize = 80;

fn system_db_list() -> String {
    SYSTEM_DATABASES.iter().map(|d| quote_literal(d)).collect::<Vec<_>>().join(", ")
}

// WHAT:  `col = 'db'` for one database, `col NOT IN (system…)` for every user database.
fn db_scope(column: &str, db: Option<&str>) -> String {
    match db {
        Some(d) => format!("{column} = {}", quote_literal(d)),
        None => format!("{column} NOT IN ({})", system_db_list()),
    }
}

// WHAT:  `reference.parent` → (database, table). "db" → (db, None); "db.table" → both.
fn split_owner(parent: Option<&str>) -> (Option<&str>, Option<&str>) {
    match parent.map(str::trim).filter(|p| !p.is_empty()) {
        None => (None, None),
        Some(p) => match p.split_once('.') {
            Some((db, table)) => (Some(db), Some(table)),
            None => (Some(p), None),
        },
    }
}

fn owner_key(db: &str, table: &str) -> String {
    format!("{db}.{table}")
}

fn ident(name: &str) -> String {
    quote_ident_for(Engine::Mysql, name)
}

fn qualified(db: &str, name: &str) -> String {
    format!("{}.{}", ident(db), ident(name))
}

fn account_literal(user: &str, host: &str) -> String {
    format!("{}@{}", quote_literal(user), quote_literal(host))
}

pub fn human_bytes(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", value as u64)
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub fn human_duration(secs: u64) -> String {
    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3_600;
    let minutes = (secs % 3_600) / 60;
    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {}s", secs % 60)
    } else {
        format!("{secs}s")
    }
}

// WHAT:  Whitespace-collapsed, character-safe prefix with an ellipsis.
pub fn preview(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    let cut: String = collapsed.chars().take(max_chars).collect();
    format!("{}…", cut.trim_end())
}

// WHAT:  `TABLE_ROWS` → `table rows`, `Innodb_buffer_pool_size` → `innodb buffer pool size`.
fn pretty_label(raw: &str) -> String {
    raw.replace('_', " ").to_ascii_lowercase()
}

// WHAT:  A bytes figure: human text for display, raw bytes for the sparkline.
fn bytes_stat(label: &str, bytes: f64) -> Stat {
    Stat { label: label.to_string(), value: human_bytes(bytes), unit: None, hint: None, numeric: Some(bytes) }
}

fn cell_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Int(i)) => Some(*i as f64),
        Some(Value::Float(f)) => Some(*f),
        Some(Value::Decimal(t)) | Some(Value::Text(t)) => t.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn cell_opt(value: Option<&Value>) -> Option<String> {
    let text = cell_text(value);
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

// WHAT:  Column index by (case-insensitive) name in one of our own result sets.
fn column_index(set: &ResultSet, name: &str) -> Option<usize> {
    set.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
}

fn set_text(set: &ResultSet, row: &[Value], name: &str) -> String {
    column_index(set, name).map(|i| cell_text(row.get(i))).unwrap_or_default()
}

// WHAT:  First row of a result set as a property sheet (nulls / blanks skipped).
fn properties_of(set: &ResultSet) -> Vec<ObjectProperty> {
    let Some(row) = set.rows.first() else {
        return Vec::new();
    };
    set.columns
        .iter()
        .zip(row.iter())
        .filter_map(|(c, v)| cell_opt(Some(v)).map(|text| ObjectProperty { name: pretty_label(&c.name), value: text }))
        .collect()
}

// WHAT:  The statement column of a `SHOW CREATE …` row, whatever the object kind.
fn create_column_index(columns: &[ColumnMeta]) -> Option<usize> {
    columns
        .iter()
        .position(|c| c.name.starts_with("Create ") || c.name == "SQL Original Statement")
}

// WHAT:  Gives duplicate names a ` (2)`, ` (3)`… suffix so list keys stay unique.
fn dedupe_names(items: &mut [ObjectSummary]) {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for item in items.iter_mut() {
        let count = seen.entry(item.reference.name.clone()).or_insert(0);
        *count += 1;
        if *count > 1 {
            item.reference.name = format!("{} ({count})", item.reference.name);
        }
    }
}

// WHAT:  SQL for the single-query kinds. `db` scopes to one database (None =
//        every user database); `table` narrows nested kinds to one owner.
pub fn object_list_sql(kind: ObjectKind, db: Option<&str>, table: Option<&str>) -> Option<String> {
    let table_filter = |col: &str| table.map(|t| format!(" AND {col} = {}", quote_literal(t))).unwrap_or_default();
    let sql = match kind {
        ObjectKind::Database => format!(
            "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA \
             WHERE SCHEMA_NAME NOT IN ({}) ORDER BY SCHEMA_NAME LIMIT {OBJECT_CAP}",
            system_db_list()
        ),
        ObjectKind::Table => format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH, TABLE_COMMENT \
             FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND {} \
             ORDER BY TABLE_SCHEMA, TABLE_NAME LIMIT {OBJECT_CAP}",
            db_scope("TABLE_SCHEMA", db)
        ),
        ObjectKind::View => format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, DEFINER, IS_UPDATABLE, SECURITY_TYPE FROM information_schema.VIEWS \
             WHERE {} ORDER BY TABLE_SCHEMA, TABLE_NAME LIMIT {OBJECT_CAP}",
            db_scope("TABLE_SCHEMA", db)
        ),
        ObjectKind::Partition => format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, PARTITION_METHOD, PARTITION_DESCRIPTION, TABLE_ROWS, \
             DATA_LENGTH + INDEX_LENGTH FROM information_schema.PARTITIONS \
             WHERE PARTITION_NAME IS NOT NULL AND {}{} \
             ORDER BY TABLE_SCHEMA, TABLE_NAME, PARTITION_ORDINAL_POSITION LIMIT {OBJECT_CAP}",
            db_scope("TABLE_SCHEMA", db),
            table_filter("TABLE_NAME")
        ),
        ObjectKind::Index => format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, MIN(NON_UNIQUE), MIN(INDEX_TYPE), \
             GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ') \
             FROM information_schema.STATISTICS WHERE {}{} \
             GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME \
             ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME LIMIT {OBJECT_CAP}",
            db_scope("TABLE_SCHEMA", db),
            table_filter("TABLE_NAME")
        ),
        ObjectKind::Constraint => format!(
            "SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE, \
             GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION SEPARATOR ', '), \
             MAX(kcu.REFERENCED_TABLE_NAME), MAX(rc.DELETE_RULE), MAX(rc.UPDATE_RULE) \
             FROM information_schema.TABLE_CONSTRAINTS tc \
             LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu \
               ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.TABLE_NAME = tc.TABLE_NAME \
             LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc \
               ON rc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND rc.TABLE_NAME = tc.TABLE_NAME \
             WHERE {}{} GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE \
             ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME LIMIT {OBJECT_CAP}",
            db_scope("tc.TABLE_SCHEMA", db),
            table_filter("tc.TABLE_NAME")
        ),
        ObjectKind::Function | ObjectKind::Procedure => format!(
            "SELECT r.ROUTINE_SCHEMA, r.ROUTINE_NAME, r.DTD_IDENTIFIER, r.DEFINER, r.IS_DETERMINISTIC, r.SQL_DATA_ACCESS, \
             r.CREATED, r.LAST_ALTERED, \
             (SELECT GROUP_CONCAT(CONCAT_WS(' ', p.PARAMETER_MODE, p.PARAMETER_NAME, p.DTD_IDENTIFIER) ORDER BY p.ORDINAL_POSITION SEPARATOR ', ') \
              FROM information_schema.PARAMETERS p \
              WHERE p.SPECIFIC_SCHEMA = r.ROUTINE_SCHEMA AND p.SPECIFIC_NAME = r.SPECIFIC_NAME AND p.ORDINAL_POSITION > 0) \
             FROM information_schema.ROUTINES r WHERE r.ROUTINE_TYPE = '{}' AND {} \
             ORDER BY r.ROUTINE_SCHEMA, r.ROUTINE_NAME LIMIT {OBJECT_CAP}",
            if kind == ObjectKind::Function { "FUNCTION" } else { "PROCEDURE" },
            db_scope("r.ROUTINE_SCHEMA", db)
        ),
        ObjectKind::Trigger => format!(
            "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, DEFINER, CREATED \
             FROM information_schema.TRIGGERS WHERE {}{} \
             ORDER BY TRIGGER_SCHEMA, EVENT_OBJECT_TABLE, TRIGGER_NAME LIMIT {OBJECT_CAP}",
            db_scope("TRIGGER_SCHEMA", db),
            table_filter("EVENT_OBJECT_TABLE")
        ),
        ObjectKind::Event => format!(
            "SELECT EVENT_SCHEMA, EVENT_NAME, STATUS, EVENT_TYPE, INTERVAL_VALUE, INTERVAL_FIELD, EXECUTE_AT, LAST_EXECUTED, DEFINER \
             FROM information_schema.EVENTS WHERE {} ORDER BY EVENT_SCHEMA, EVENT_NAME LIMIT {OBJECT_CAP}",
            db_scope("EVENT_SCHEMA", db)
        ),
        ObjectKind::Setting => "SHOW GLOBAL VARIABLES".to_string(),
        ObjectKind::Session => "SHOW FULL PROCESSLIST".to_string(),
        _ => return None,
    };
    Some(sql)
}

const DATA_LOCKS_SQL: &str = "SELECT ENGINE_TRANSACTION_ID, OBJECT_SCHEMA, OBJECT_NAME, INDEX_NAME, LOCK_TYPE, LOCK_MODE, LOCK_STATUS, LOCK_DATA, THREAD_ID \
    FROM performance_schema.data_locks ORDER BY ENGINE_TRANSACTION_ID, OBJECT_SCHEMA, OBJECT_NAME LIMIT 2000";
const INNODB_LOCKS_SQL: &str = "SELECT lock_trx_id, NULL, lock_table, lock_index, lock_type, lock_mode, NULL, lock_data, NULL \
    FROM information_schema.INNODB_LOCKS ORDER BY lock_trx_id LIMIT 2000";
const DIGEST_SQL: &str = "SELECT DIGEST, SCHEMA_NAME, DIGEST_TEXT, COUNT_STAR, AVG_TIMER_WAIT / 1000000000000, SUM_TIMER_WAIT / 1000000000000, \
    MAX_TIMER_WAIT / 1000000000000, SUM_ROWS_EXAMINED, SUM_ROWS_SENT, FIRST_SEEN, LAST_SEEN \
    FROM performance_schema.events_statements_summary_by_digest WHERE DIGEST_TEXT IS NOT NULL \
    ORDER BY AVG_TIMER_WAIT DESC LIMIT 200";
const SLOW_LOG_SQL: &str = "SELECT NULL, db, sql_text, 1, TIME_TO_SEC(query_time), TIME_TO_SEC(query_time), TIME_TO_SEC(query_time), \
    rows_examined, rows_sent, start_time, start_time FROM mysql.slow_log ORDER BY start_time DESC LIMIT 200";

// WHAT:  `GRANT SELECT, INSERT ON `db`.* TO `u`@`h`` → (privileges, object, grantee).
//        Role grants (`GRANT `r`@`%` TO …`) have no ON clause: object is empty.
pub fn parse_grant(line: &str) -> Option<(String, String, String)> {
    let rest = line.trim().strip_prefix("GRANT ")?;
    let (head, grantee) = match rest.rsplit_once(" TO ") {
        Some((h, g)) => (h, g),
        None => (rest, ""),
    };
    let grantee = grantee.split(" WITH ").next().unwrap_or(grantee).trim().to_string();
    match head.rsplit_once(" ON ") {
        Some((privs, object)) => Some((privs.trim().to_string(), object.trim().to_string(), grantee)),
        None => Some((head.trim().to_string(), String::new(), grantee)),
    }
}

fn grant_badge(object: &str, privileges: &str) -> &'static str {
    if privileges.starts_with("PROXY") {
        "proxy"
    } else if object.is_empty() {
        "role"
    } else if object == "*.*" {
        "global"
    } else if object.ends_with(".*") {
        "database"
    } else if privileges.contains('(') {
        "column"
    } else {
        "table"
    }
}

// WHAT:  Lock-looking lines of the TRANSACTIONS section of SHOW ENGINE INNODB STATUS,
//        the last resort when neither performance_schema.data_locks nor
//        information_schema.INNODB_LOCKS exists.
pub fn parse_innodb_status_locks(status: &str) -> Vec<String> {
    let mut in_transactions = false;
    let mut out = Vec::new();
    for line in status.lines() {
        let trimmed = line.trim();
        if trimmed == "TRANSACTIONS" {
            in_transactions = true;
            continue;
        }
        if in_transactions && (trimmed == "FILE I/O" || trimmed.starts_with("INSERT BUFFER") || trimmed == "LOG") {
            break;
        }
        if in_transactions && (trimmed.contains(" lock ") || trimmed.contains(" LOCK ") || trimmed.starts_with("RECORD LOCKS") || trimmed.starts_with("TABLE LOCK")) {
            out.push(trimmed.to_string());
        }
    }
    out
}

// WHAT:  One listing row → ObjectSummary, per kind (column order from `object_list_sql`).
pub fn summarize(kind: ObjectKind, row: &[Value]) -> ObjectSummary {
    let t = |i: usize| cell_text(row.get(i));
    match kind {
        ObjectKind::Database => {
            let charset = t(1);
            let collation = t(2);
            ObjectSummary::new(kind, t(0), None).with_detail(format!("{charset} · {collation}"))
        }
        ObjectKind::Table => {
            let mut parts = Vec::new();
            if let Some(rows) = cell_f64(row.get(3)) {
                parts.push(format!("~{} rows", format_number(rows)));
            }
            if let Some(size) = cell_f64(row.get(4)) {
                parts.push(human_bytes(size));
            }
            let comment = t(5);
            if !comment.is_empty() {
                parts.push(preview(&comment, 40));
            }
            let mut summary = ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(parts.join(" · "));
            if let Some(engine) = cell_opt(row.get(2)) {
                summary = summary.with_badge(engine.to_ascii_lowercase());
            }
            summary
        }
        ObjectKind::View => {
            let mut summary = ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(t(2));
            if t(3).eq_ignore_ascii_case("YES") {
                summary = summary.with_badge("updatable");
            }
            summary
        }
        ObjectKind::Partition => {
            let mut parts = vec![format!("{} {}", t(3), t(4)).trim().to_string()];
            if let Some(rows) = cell_f64(row.get(5)) {
                parts.push(format!("~{} rows", format_number(rows)));
            }
            if let Some(size) = cell_f64(row.get(6)) {
                parts.push(human_bytes(size));
            }
            ObjectSummary::new(kind, t(2), Some(owner_key(&t(0), &t(1))))
                .with_detail(parts.join(" · "))
                .with_badge(t(3).to_ascii_lowercase())
        }
        ObjectKind::Index => {
            let name = t(2);
            let index_type = t(4).to_ascii_uppercase();
            let badge = if name == "PRIMARY" {
                "primary".to_string()
            } else if t(3) == "0" {
                "unique".to_string()
            } else {
                index_type.to_ascii_lowercase()
            };
            ObjectSummary::new(kind, name, Some(owner_key(&t(0), &t(1))))
                .with_detail(format!("{} ({})", t(1), t(5)))
                .with_badge(badge)
        }
        ObjectKind::Constraint => {
            let kind_text = t(3).to_ascii_uppercase();
            let badge = match kind_text.as_str() {
                "PRIMARY KEY" => "primary",
                "FOREIGN KEY" => "foreign",
                "UNIQUE" => "unique",
                "CHECK" => "check",
                _ => "constraint",
            };
            let mut detail = format!("{} ({})", t(1), t(4));
            if let Some(referenced) = cell_opt(row.get(5)) {
                detail.push_str(&format!(" → {referenced}"));
                if let Some(rule) = cell_opt(row.get(6)) {
                    detail.push_str(&format!(" · on delete {}", rule.to_ascii_lowercase()));
                }
            }
            ObjectSummary::new(kind, t(2), Some(owner_key(&t(0), &t(1)))).with_detail(detail).with_badge(badge)
        }
        ObjectKind::Function | ObjectKind::Procedure => {
            let params = t(8);
            let mut detail = format!("({params})");
            if kind == ObjectKind::Function {
                if let Some(returns) = cell_opt(row.get(2)) {
                    detail.push_str(&format!(" → {returns}"));
                }
            }
            let mut summary = ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(detail);
            if t(4).eq_ignore_ascii_case("YES") {
                summary = summary.with_badge("deterministic");
            }
            summary
        }
        ObjectKind::Trigger => ObjectSummary::new(kind, t(1), Some(owner_key(&t(0), &t(2))))
            .with_detail(format!("{} {} ON {}", t(3), t(4), t(2)))
            .with_badge(t(4).to_ascii_lowercase()),
        ObjectKind::Event => {
            let schedule = if t(3).eq_ignore_ascii_case("RECURRING") {
                format!("every {} {}", t(4), t(5).to_ascii_lowercase())
            } else {
                format!("at {}", t(6))
            };
            let mut detail = schedule;
            if let Some(last) = cell_opt(row.get(7)) {
                detail.push_str(&format!(" · last {last}"));
            }
            ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(detail).with_badge(t(2).to_ascii_lowercase())
        }
        ObjectKind::Setting => ObjectSummary::new(kind, t(0), None).with_detail(preview(&t(1), 100)),
        ObjectKind::Session => {
            // Id, User, Host, db, Command, Time, State, Info
            let mut parts = vec![format!("{}@{}", t(1), t(2))];
            if let Some(db) = cell_opt(row.get(3)) {
                parts.push(db);
            }
            if let Some(secs) = cell_f64(row.get(5)) {
                parts.push(human_duration(secs as u64));
            }
            if let Some(state) = cell_opt(row.get(6)) {
                parts.push(state);
            }
            if let Some(info) = cell_opt(row.get(7)) {
                parts.push(preview(&info, PREVIEW_CHARS));
            }
            ObjectSummary::new(kind, t(0), None).with_detail(parts.join(" · ")).with_badge(t(4).to_ascii_lowercase())
        }
        ObjectKind::Lock => {
            // trx, schema, table, index, type, mode, status, data, thread
            let table = match (cell_opt(row.get(1)), cell_opt(row.get(2))) {
                (Some(s), Some(t)) => format!("{s}.{t}"),
                (_, Some(t)) => t,
                _ => "(unknown)".to_string(),
            };
            let mut parts = vec![t(4).to_ascii_lowercase()];
            if let Some(status) = cell_opt(row.get(6)) {
                parts.push(status.to_ascii_lowercase());
            }
            if let Some(index) = cell_opt(row.get(3)) {
                parts.push(format!("index {index}"));
            }
            if let Some(data) = cell_opt(row.get(7)) {
                parts.push(preview(&data, 40));
            }
            ObjectSummary::new(kind, format!("{table} #{}", t(0)), None)
                .with_detail(parts.join(" · "))
                .with_badge(t(5).to_ascii_lowercase())
        }
        ObjectKind::SlowQuery => {
            // digest, schema, text, count, avg_s, sum_s, max_s, rows_examined, rows_sent, first, last
            let mut parts = Vec::new();
            if let Some(avg) = cell_f64(row.get(4)) {
                parts.push(format!("avg {avg:.3} s"));
            }
            if let Some(count) = cell_f64(row.get(3)) {
                parts.push(format!("{} calls", format_number(count)));
            }
            if let Some(examined) = cell_f64(row.get(7)) {
                parts.push(format!("{} rows examined", format_number(examined)));
            }
            let mut summary = ObjectSummary::new(kind, preview(&t(2), PREVIEW_CHARS), cell_opt(row.first())).with_detail(parts.join(" · "));
            if let Some(schema) = cell_opt(row.get(1)) {
                summary = summary.with_badge(schema);
            }
            summary
        }
        _ => ObjectSummary::new(kind, t(0), None),
    }
}

fn user_summary(row: &[Value]) -> ObjectSummary {
    let user = cell_text(row.first());
    let host = cell_text(row.get(1));
    let mut parts = Vec::new();
    if let Some(plugin) = cell_opt(row.get(2)) {
        parts.push(plugin);
    }
    if cell_text(row.get(3)).eq_ignore_ascii_case("Y") {
        parts.push("locked".to_string());
    }
    ObjectSummary::new(ObjectKind::User, user, Some(host.clone())).with_detail(parts.join(" · ")).with_badge(host)
}

fn grant_summary(line: &str) -> ObjectSummary {
    match parse_grant(line) {
        Some((privs, object, grantee)) => {
            let badge = grant_badge(&object, &privs);
            let name = if object.is_empty() { privs.clone() } else { format!("{privs} ON {object}") };
            ObjectSummary::new(ObjectKind::Grant, preview(&name, 120), Some(grantee.clone())).with_detail(format!("TO {grantee}")).with_badge(badge)
        }
        None => ObjectSummary::new(ObjectKind::Grant, preview(line, 120), None),
    }
}

fn replica_summary(set: &ResultSet, row: &[Value]) -> ObjectSummary {
    let pick = |a: &str, b: &str| {
        let first = set_text(set, row, a);
        if first.is_empty() { set_text(set, row, b) } else { first }
    };
    let host = pick("Source_Host", "Master_Host");
    let port = pick("Source_Port", "Master_Port");
    let io = pick("Replica_IO_Running", "Slave_IO_Running");
    let sql = pick("Replica_SQL_Running", "Slave_SQL_Running");
    let lag = pick("Seconds_Behind_Source", "Seconds_Behind_Master");
    let channel = set_text(set, row, "Channel_Name");
    let mut name = format!("{host}:{port}");
    if !channel.is_empty() {
        name.push_str(&format!(" [{channel}]"));
    }
    let running = io.eq_ignore_ascii_case("Yes") && sql.eq_ignore_ascii_case("Yes");
    let mut parts = Vec::new();
    if !lag.is_empty() {
        parts.push(format!("lag {lag} s"));
    }
    parts.push(format!("IO {io}"));
    parts.push(format!("SQL {sql}"));
    let error = pick("Last_Error", "Last_Error");
    if !error.is_empty() {
        parts.push(preview(&error, 60));
    }
    ObjectSummary::new(ObjectKind::Replica, name, None)
        .with_detail(parts.join(" · "))
        .with_badge(if running { "running" } else { "stopped" })
}

fn is_privilege_error(err: &AppError) -> bool {
    let text = err.to_string().to_ascii_lowercase();
    text.contains("denied") || text.contains("privilege") || text.contains("doesn't exist") || text.contains("unknown table") || text.contains("unknown column")
}

// ---- server statistics -----------------------------------------------------

fn stat_f64(map: &BTreeMap<String, String>, key: &str) -> Option<f64> {
    map.get(key).and_then(|v| v.trim().parse::<f64>().ok())
}

// WHAT:  SHOW GLOBAL STATUS + SHOW GLOBAL VARIABLES → grouped figures.
pub fn build_stats(status: &BTreeMap<String, String>, variables: &BTreeMap<String, String>) -> Vec<StatGroup> {
    let s = |k: &str| stat_f64(status, k);
    let v = |k: &str| stat_f64(variables, k);
    let num = |label: &str, key: &str, unit: Option<&str>| s(key).map(|n| Stat::number(label, n, unit));

    let mut server = Vec::new();
    if let Some(version) = variables.get("version") {
        server.push(Stat::text("Version", describe_version(version)));
    }
    let uptime = s("Uptime");
    if let Some(up) = uptime {
        server.push(Stat::text("Uptime", human_duration(up as u64)));
    }
    if let (Some(up), Some(questions)) = (uptime, s("Questions")) {
        server.push(Stat::number("Queries / s (avg)", questions / up.max(1.0), None));
    }
    if let Some(hostname) = variables.get("hostname") {
        server.push(Stat::text("Host", hostname.clone()));
    }

    let mut connections = Vec::new();
    connections.extend(num("Connected", "Threads_connected", None));
    connections.extend(num("Running", "Threads_running", None));
    if let Some(max_used) = s("Max_used_connections") {
        let mut stat = Stat::number("Max used", max_used, None);
        if let Some(limit) = v("max_connections") {
            stat = stat.with_hint(format!("of {} allowed", format_number(limit)));
        }
        connections.push(stat);
    }
    if let Some(limit) = v("max_connections") {
        connections.push(Stat::number("Max connections", limit, None));
    }
    connections.extend(num("Total connections", "Connections", None));
    connections.extend(num("Aborted connects", "Aborted_connects", None));
    connections.extend(num("Aborted clients", "Aborted_clients", None));

    let mut queries = Vec::new();
    queries.extend(num("Questions", "Questions", None));
    queries.extend(num("SELECT", "Com_select", None));
    queries.extend(num("INSERT", "Com_insert", None));
    queries.extend(num("UPDATE", "Com_update", None));
    queries.extend(num("DELETE", "Com_delete", None));
    queries.extend(num("Slow queries", "Slow_queries", None));
    queries.extend(num("Temp tables on disk", "Created_tmp_disk_tables", None));
    queries.extend(num("Table locks waited", "Table_locks_waited", None));

    let mut cache = Vec::new();
    if let (Some(requests), Some(reads)) = (s("Innodb_buffer_pool_read_requests"), s("Innodb_buffer_pool_reads")) {
        let ratio = if requests > 0.0 { (1.0 - reads / requests) * 100.0 } else { 100.0 };
        cache.push(Stat::number("Buffer pool hit ratio", (ratio * 100.0).round() / 100.0, Some("%")).with_hint("logical reads served without a disk read"));
        cache.push(Stat::number("Read requests", requests, None));
        cache.push(Stat::number("Disk reads", reads, None));
    }
    if let Some(size) = v("innodb_buffer_pool_size") {
        cache.push(bytes_stat("Buffer pool size", size));
    }
    cache.extend(num("Pages total", "Innodb_buffer_pool_pages_total", None));
    cache.extend(num("Pages free", "Innodb_buffer_pool_pages_free", None));
    cache.extend(num("Pages dirty", "Innodb_buffer_pool_pages_dirty", None));
    cache.extend(num("Open tables", "Open_tables", None));
    cache.extend(num("Opened tables", "Opened_tables", None));

    let mut throughput = Vec::new();
    if let Some(sent) = s("Bytes_sent") {
        throughput.push(bytes_stat("Bytes sent", sent));
    }
    if let Some(received) = s("Bytes_received") {
        throughput.push(bytes_stat("Bytes received", received));
    }
    if let Some(written) = s("Innodb_data_written") {
        throughput.push(bytes_stat("InnoDB data written", written));
    }
    if let Some(read) = s("Innodb_data_read") {
        throughput.push(bytes_stat("InnoDB data read", read));
    }
    throughput.extend(num("Rows read", "Innodb_rows_read", None));
    throughput.extend(num("Rows inserted", "Innodb_rows_inserted", None));
    throughput.extend(num("Rows updated", "Innodb_rows_updated", None));
    throughput.extend(num("Rows deleted", "Innodb_rows_deleted", None));
    throughput.extend(num("Row lock waits", "Innodb_row_lock_waits", None));

    let groups = [
        ("Server", server),
        ("Connections", connections),
        ("Queries", queries),
        ("InnoDB cache", cache),
        ("Throughput", throughput),
    ];
    groups
        .into_iter()
        .filter(|(_, stats)| !stats.is_empty())
        .map(|(title, stats)| StatGroup { title: title.to_string(), stats })
        .collect()
}

impl MysqlIntegration {
    // WHAT:  Database a request refers to: the reference's parent, else the session's.
    fn resolve_db(&self, parent: Option<&str>) -> AppResult<String> {
        let (db, _) = split_owner(parent);
        db.map(str::to_string)
            .or_else(|| self.database.clone())
            .ok_or_else(|| AppError::invalid_input("Select a database first."))
    }

    async fn columns_in(&self, db: &str, table: &str) -> AppResult<Vec<ColumnInfo>> {
        let sql = format!(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, ORDINAL_POSITION, COLUMN_KEY \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} \
             ORDER BY ORDINAL_POSITION",
            quote_literal(db),
            quote_literal(table)
        );
        let rows = self.query_rows(&sql).await?;
        Ok(rows
            .iter()
            .map(|r| ColumnInfo {
                name: cell_text(r.first()),
                data_type: cell_text(r.get(1)).to_ascii_lowercase(),
                nullable: cell_text(r.get(2)).eq_ignore_ascii_case("YES"),
                primary_key: cell_text(r.get(4)).eq_ignore_ascii_case("PRI"),
                ordinal: cell_i64(r.get(3)).and_then(|n| u32::try_from(n).ok()).unwrap_or_default(),
            })
            .collect())
    }

    async fn list_simple(&self, kind: ObjectKind, sql: &str) -> AppResult<Vec<ObjectSummary>> {
        let rows = self.query_rows(sql).await?;
        let mut items: Vec<ObjectSummary> = rows.iter().map(|r| summarize(kind, r)).collect();
        if matches!(kind, ObjectKind::Session | ObjectKind::Lock | ObjectKind::SlowQuery) {
            dedupe_names(&mut items);
        } else {
            items.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        }
        Ok(items)
    }

    // WHAT:  Full account list when mysql.user is readable, else just this session's account.
    async fn list_users(&self) -> AppResult<Vec<ObjectSummary>> {
        const FULL: &str = "SELECT User, Host, plugin, account_locked FROM mysql.user ORDER BY User, Host";
        const BASIC: &str = "SELECT User, Host, plugin, NULL FROM mysql.user ORDER BY User, Host";
        let rows = match self.query_rows(FULL).await {
            Ok(rows) => rows,
            Err(_) => match self.query_rows(BASIC).await {
                Ok(rows) => rows,
                Err(_) => {
                    let me = self.query_rows("SELECT CURRENT_USER()").await?;
                    let account = me.first().map(|r| cell_text(r.first())).unwrap_or_default();
                    let (user, host) = account.rsplit_once('@').unwrap_or((account.as_str(), "%"));
                    vec![vec![Value::Text(user.to_string()), Value::Text(host.to_string())]]
                }
            },
        };
        Ok(rows.iter().map(|r| user_summary(r)).collect())
    }

    async fn show_grants(&self, account: Option<&str>) -> AppResult<Vec<String>> {
        let sql = match account {
            Some(a) => format!("SHOW GRANTS FOR {a}"),
            None => "SHOW GRANTS".to_string(),
        };
        let rows = self.query_rows(&sql).await?;
        Ok(rows.iter().map(|r| cell_text(r.first())).filter(|g| !g.is_empty()).collect())
    }

    async fn list_locks(&self) -> AppResult<Vec<ObjectSummary>> {
        if let Ok(rows) = self.query_rows(DATA_LOCKS_SQL).await {
            let mut items: Vec<ObjectSummary> = rows.iter().map(|r| summarize(ObjectKind::Lock, r)).collect();
            dedupe_names(&mut items);
            return Ok(items);
        }
        if let Ok(rows) = self.query_rows(INNODB_LOCKS_SQL).await {
            let mut items: Vec<ObjectSummary> = rows.iter().map(|r| summarize(ObjectKind::Lock, r)).collect();
            dedupe_names(&mut items);
            return Ok(items);
        }
        let rows = self.query_rows("SHOW ENGINE INNODB STATUS").await?;
        let status = rows.first().map(|r| cell_text(r.get(2))).unwrap_or_default();
        let mut items: Vec<ObjectSummary> = parse_innodb_status_locks(&status)
            .into_iter()
            .map(|line| ObjectSummary::new(ObjectKind::Lock, preview(&line, PREVIEW_CHARS), None).with_detail(line).with_badge("innodb status"))
            .collect();
        dedupe_names(&mut items);
        Ok(items)
    }

    async fn replica_status(&self) -> AppResult<ResultSet> {
        match self.query_set("SHOW REPLICA STATUS").await {
            Ok(set) => Ok(set),
            Err(_) => match self.query_set("SHOW SLAVE STATUS").await {
                Ok(set) => Ok(set),
                Err(err) if is_privilege_error(&err) => Ok(ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }),
                Err(err) => Err(err),
            },
        }
    }

    async fn list_replicas(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self.replica_status().await?;
        let mut items: Vec<ObjectSummary> = set.rows.iter().map(|r| replica_summary(&set, r)).collect();
        dedupe_names(&mut items);
        Ok(items)
    }

    async fn slow_query_rows(&self) -> AppResult<Vec<Vec<Value>>> {
        match self.query_rows(DIGEST_SQL).await {
            Ok(rows) => Ok(rows),
            Err(first) => match self.query_rows(SLOW_LOG_SQL).await {
                Ok(rows) => Ok(rows),
                Err(_) => Err(AppError::invalid_input(format!(
                    "Statement statistics need performance_schema (events_statements_summary_by_digest) or log_output=TABLE with mysql.slow_log: {first}"
                ))),
            },
        }
    }

    async fn show_create(&self, statement: &str) -> AppResult<Option<String>> {
        let set = self.query_set(statement).await?;
        let Some(index) = create_column_index(&set.columns) else {
            return Ok(None);
        };
        Ok(set.rows.first().and_then(|r| cell_opt(r.get(index))))
    }

    async fn property_sheet(&self, sql: &str) -> Vec<ObjectProperty> {
        match self.query_set(sql).await {
            Ok(set) => properties_of(&set),
            Err(_) => Vec::new(),
        }
    }

    async fn table_detail(&self, reference: &ObjectRef, db: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(db, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE TABLE {target}")).await? {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT ENGINE, ROW_FORMAT, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, DATA_FREE, AUTO_INCREMENT, TABLE_COLLATION, \
                 CREATE_TIME, UPDATE_TIME, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                quote_literal(db),
                quote_literal(name)
            ))
            .await;
        detail.columns = self.columns_in(db, name).await?;
        let owner = owner_key(db, name);
        for kind in [ObjectKind::Index, ObjectKind::Constraint, ObjectKind::Trigger, ObjectKind::Partition] {
            if let Some(sql) = object_list_sql(kind, Some(db), Some(name)) {
                if let Ok(children) = self.list_simple(kind, &sql).await {
                    detail.children.extend(children.into_iter().filter(|c| c.reference.parent.as_deref() == Some(owner.as_str())));
                }
            }
        }
        Ok(detail
            .action(ObjectAction::new("analyze", "Analyze table", format!("ANALYZE TABLE {target}")))
            .action(ObjectAction::new("optimize", "Optimize table", format!("OPTIMIZE TABLE {target}")))
            .action(ObjectAction::destructive("truncate", "Truncate table", format!("TRUNCATE TABLE {target}")))
            .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {target}"))))
    }

    async fn view_detail(&self, reference: &ObjectRef, db: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(db, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE VIEW {target}")).await? {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT DEFINER, SECURITY_TYPE, IS_UPDATABLE, CHECK_OPTION, CHARACTER_SET_CLIENT, COLLATION_CONNECTION \
                 FROM information_schema.VIEWS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                quote_literal(db),
                quote_literal(name)
            ))
            .await;
        detail.columns = self.columns_in(db, name).await?;
        Ok(detail.action(ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {target}"))))
    }

    async fn routine_detail(&self, reference: &ObjectRef, db: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(db, name);
        let word = if reference.kind == ObjectKind::Function { "FUNCTION" } else { "PROCEDURE" };
        let mut detail = ObjectDetail::empty(reference);
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE {word} {target}")).await? {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT DTD_IDENTIFIER, DEFINER, IS_DETERMINISTIC, SQL_DATA_ACCESS, SECURITY_TYPE, CREATED, LAST_ALTERED, ROUTINE_COMMENT \
                 FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = {} AND ROUTINE_NAME = {} AND ROUTINE_TYPE = '{word}'",
                quote_literal(db),
                quote_literal(name)
            ))
            .await;
        let params = format!(
            "SELECT ORDINAL_POSITION, PARAMETER_MODE, PARAMETER_NAME, DTD_IDENTIFIER FROM information_schema.PARAMETERS \
             WHERE SPECIFIC_SCHEMA = {} AND SPECIFIC_NAME = {} ORDER BY ORDINAL_POSITION",
            quote_literal(db),
            quote_literal(name)
        );
        detail.rows = self.query_set(&params).await.ok();
        Ok(detail.action(ObjectAction::destructive("drop", &format!("Drop {}", word.to_ascii_lowercase()), format!("DROP {word} {target}"))))
    }

    async fn trigger_detail(&self, reference: &ObjectRef, db: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(db, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE TRIGGER {target}")).await? {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_ORIENTATION, DEFINER, CREATED \
                 FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = {} AND TRIGGER_NAME = {}",
                quote_literal(db),
                quote_literal(name)
            ))
            .await;
        Ok(detail.action(ObjectAction::destructive("drop", "Drop trigger", format!("DROP TRIGGER {target}"))))
    }

    async fn event_detail(&self, reference: &ObjectRef, db: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(db, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE EVENT {target}")).await? {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT STATUS, EVENT_TYPE, INTERVAL_VALUE, INTERVAL_FIELD, EXECUTE_AT, STARTS, ENDS, ON_COMPLETION, LAST_EXECUTED, DEFINER, EVENT_COMMENT \
                 FROM information_schema.EVENTS WHERE EVENT_SCHEMA = {} AND EVENT_NAME = {}",
                quote_literal(db),
                quote_literal(name)
            ))
            .await;
        Ok(detail
            .action(ObjectAction::destructive("enable", "Enable event", format!("ALTER EVENT {target} ENABLE")))
            .action(ObjectAction::destructive("disable", "Disable event", format!("ALTER EVENT {target} DISABLE")))
            .action(ObjectAction::destructive("drop", "Drop event", format!("DROP EVENT {target}"))))
    }

    async fn index_detail(&self, reference: &ObjectRef, db: &str, table: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(db, table);
        let mut detail = ObjectDetail::empty(reference);
        let rows = self
            .query_set(&format!(
                "SELECT SEQ_IN_INDEX, COLUMN_NAME, COLLATION, CARDINALITY, SUB_PART, NULLABLE, NON_UNIQUE, INDEX_TYPE, INDEX_COMMENT \
                 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} AND INDEX_NAME = {} ORDER BY SEQ_IN_INDEX",
                quote_literal(db),
                quote_literal(table),
                quote_literal(name)
            ))
            .await?;
        if let Some(first) = rows.rows.first() {
            let unique = set_text(&rows, first, "NON_UNIQUE") == "0";
            let index_type = set_text(&rows, first, "INDEX_TYPE");
            let columns: Vec<String> = rows.rows.iter().map(|r| ident(&set_text(&rows, r, "COLUMN_NAME"))).collect();
            let definition = if name == "PRIMARY" {
                format!("ALTER TABLE {target} ADD PRIMARY KEY ({})", columns.join(", "))
            } else {
                let prefix = match index_type.to_ascii_uppercase().as_str() {
                    "FULLTEXT" => "FULLTEXT ",
                    "SPATIAL" => "SPATIAL ",
                    _ if unique => "UNIQUE ",
                    _ => "",
                };
                format!("CREATE {prefix}INDEX {} ON {target} ({})", ident(name), columns.join(", "))
            };
            detail = detail
                .definition(definition, CodeLanguage::Sql)
                .property("table", table)
                .property("type", index_type.to_ascii_lowercase())
                .property("unique", if unique { "yes" } else { "no" });
            if let Some(comment) = cell_opt(first.get(8)) {
                detail = detail.property("comment", comment);
            }
        }
        detail.rows = Some(rows);
        let drop = if name == "PRIMARY" {
            format!("ALTER TABLE {target} DROP PRIMARY KEY")
        } else {
            format!("DROP INDEX {} ON {target}", ident(name))
        };
        Ok(detail.action(ObjectAction::destructive("drop", "Drop index", drop)))
    }

    async fn constraint_detail(&self, reference: &ObjectRef, db: &str, table: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(db, table);
        let mut detail = ObjectDetail::empty(reference);
        let sql = object_list_sql(ObjectKind::Constraint, Some(db), Some(table)).unwrap_or_default();
        let rows = self.query_rows(&sql).await?;
        let Some(row) = rows.iter().find(|r| cell_text(r.get(2)) == name) else {
            return Err(AppError::not_found(format!("Constraint {name} on {table} was not found.")));
        };
        let kind_text = cell_text(row.get(3)).to_ascii_uppercase();
        let columns = cell_text(row.get(4));
        let referenced = cell_opt(row.get(5));
        detail = detail.property("table", table).property("type", kind_text.to_ascii_lowercase());
        if !columns.is_empty() {
            detail = detail.property("columns", columns.clone());
        }
        let quoted_cols: Vec<String> = columns.split(", ").filter(|c| !c.is_empty()).map(ident).collect();
        let drop = match kind_text.as_str() {
            "PRIMARY KEY" => {
                detail = detail.definition(format!("ALTER TABLE {target} ADD PRIMARY KEY ({})", quoted_cols.join(", ")), CodeLanguage::Sql);
                format!("ALTER TABLE {target} DROP PRIMARY KEY")
            }
            "FOREIGN KEY" => {
                let ref_table = referenced.clone().unwrap_or_default();
                if let Some(r) = referenced.as_deref() {
                    detail = detail.property("references", r.to_string());
                }
                if let Some(rule) = cell_opt(row.get(6)) {
                    detail = detail.property("on delete", rule);
                }
                if let Some(rule) = cell_opt(row.get(7)) {
                    detail = detail.property("on update", rule);
                }
                let ref_cols = self
                    .query_rows(&format!(
                        "SELECT REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
                         WHERE CONSTRAINT_SCHEMA = {} AND TABLE_NAME = {} AND CONSTRAINT_NAME = {} ORDER BY ORDINAL_POSITION",
                        quote_literal(db),
                        quote_literal(table),
                        quote_literal(name)
                    ))
                    .await
                    .unwrap_or_default();
                let ref_cols: Vec<String> = ref_cols.iter().map(|r| ident(&cell_text(r.first()))).collect();
                detail = detail.definition(
                    format!(
                        "ALTER TABLE {target} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
                        ident(name),
                        quoted_cols.join(", "),
                        qualified(db, &ref_table),
                        ref_cols.join(", ")
                    ),
                    CodeLanguage::Sql,
                );
                format!("ALTER TABLE {target} DROP FOREIGN KEY {}", ident(name))
            }
            "UNIQUE" => {
                detail = detail.definition(format!("ALTER TABLE {target} ADD CONSTRAINT {} UNIQUE ({})", ident(name), quoted_cols.join(", ")), CodeLanguage::Sql);
                format!("ALTER TABLE {target} DROP INDEX {}", ident(name))
            }
            _ => {
                if let Ok(rows) = self
                    .query_rows(&format!(
                        "SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = {} AND CONSTRAINT_NAME = {}",
                        quote_literal(db),
                        quote_literal(name)
                    ))
                    .await
                {
                    if let Some(clause) = rows.first().and_then(|r| cell_opt(r.first())) {
                        detail = detail.definition(format!("ALTER TABLE {target} ADD CONSTRAINT {} CHECK ({clause})", ident(name)), CodeLanguage::Sql);
                    }
                }
                format!("ALTER TABLE {target} DROP CHECK {}", ident(name))
            }
        };
        Ok(detail.action(ObjectAction::destructive("drop", "Drop constraint", drop)))
    }

    async fn partition_detail(&self, reference: &ObjectRef, db: &str, table: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(db, table);
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = self
            .property_sheet(&format!(
                "SELECT TABLE_NAME, PARTITION_ORDINAL_POSITION, PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_DESCRIPTION, \
                 SUBPARTITION_METHOD, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, CREATE_TIME, UPDATE_TIME, PARTITION_COMMENT \
                 FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} AND PARTITION_NAME = {} LIMIT 1",
                quote_literal(db),
                quote_literal(table),
                quote_literal(name)
            ))
            .await;
        Ok(detail
            .action(ObjectAction::destructive("truncate", "Truncate partition", format!("ALTER TABLE {target} TRUNCATE PARTITION {}", ident(name))))
            .action(ObjectAction::destructive("drop", "Drop partition", format!("ALTER TABLE {target} DROP PARTITION {}", ident(name)))))
    }

    async fn database_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let db = reference.name.as_str();
        let mut detail = ObjectDetail::empty(reference);
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE DATABASE {}", ident(db))).await? {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT s.DEFAULT_CHARACTER_SET_NAME, s.DEFAULT_COLLATION_NAME, \
                 (SELECT COUNT(*) FROM information_schema.TABLES t WHERE t.TABLE_SCHEMA = s.SCHEMA_NAME AND t.TABLE_TYPE = 'BASE TABLE') AS tables, \
                 (SELECT COUNT(*) FROM information_schema.VIEWS v WHERE v.TABLE_SCHEMA = s.SCHEMA_NAME) AS views, \
                 (SELECT SUM(DATA_LENGTH + INDEX_LENGTH) FROM information_schema.TABLES t WHERE t.TABLE_SCHEMA = s.SCHEMA_NAME) AS size_bytes \
                 FROM information_schema.SCHEMATA s WHERE s.SCHEMA_NAME = {}",
                quote_literal(db)
            ))
            .await;
        Ok(detail.action(ObjectAction::destructive("drop", "Drop database", format!("DROP DATABASE {}", ident(db)))))
    }

    async fn user_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let user = reference.name.as_str();
        let host = reference.parent.as_deref().unwrap_or("%");
        let account = account_literal(user, host);
        let mut detail = ObjectDetail::empty(reference).property("user", user).property("host", host);
        let extra = self
            .property_sheet(&format!(
                "SELECT plugin, account_locked, password_expired, max_connections, max_user_connections, max_questions, max_updates \
                 FROM mysql.user WHERE User = {} AND Host = {}",
                quote_literal(user),
                quote_literal(host)
            ))
            .await;
        detail.properties.extend(extra);
        let grants = self.show_grants(Some(&account)).await?;
        detail = detail.definition(grants.join(";\n"), CodeLanguage::Sql);
        detail.rows = Some(ResultSet {
            columns: vec![ColumnMeta { name: "grant".into(), type_name: "text".into() }],
            rows: grants.into_iter().map(|g| vec![Value::Text(g)]).collect(),
            truncated: false,
        });
        Ok(detail
            .action(ObjectAction::destructive("lock", "Lock account", format!("ALTER USER {account} ACCOUNT LOCK")))
            .action(ObjectAction::destructive("unlock", "Unlock account", format!("ALTER USER {account} ACCOUNT UNLOCK")))
            .action(ObjectAction::destructive("drop", "Drop user", format!("DROP USER {account}"))))
    }

    async fn grant_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let grantee = reference.parent.clone();
        let lines = match grantee.as_deref() {
            Some(g) => self.show_grants(Some(g)).await.or_else(|_| Ok::<_, AppError>(Vec::new()))?,
            None => self.show_grants(None).await?,
        };
        let wanted = reference.name.trim_end_matches('…');
        let line = lines
            .iter()
            .find(|l| parse_grant(l).map(|(p, o, _)| if o.is_empty() { p } else { format!("{p} ON {o}") }).is_some_and(|n| n.starts_with(wanted)))
            .cloned()
            .unwrap_or_else(|| reference.name.clone());
        let mut detail = ObjectDetail::empty(reference).definition(line.clone(), CodeLanguage::Sql);
        if let Some((privs, object, to)) = parse_grant(&line) {
            detail = detail.property("privileges", privs.clone()).property("grantee", to.clone());
            if !object.is_empty() {
                detail = detail.property("on", object.clone());
                detail = detail.action(ObjectAction::destructive("revoke", "Revoke", format!("REVOKE {privs} ON {object} FROM {to}")));
            } else {
                detail = detail.action(ObjectAction::destructive("revoke", "Revoke", format!("REVOKE {privs} FROM {to}")));
            }
        }
        Ok(detail)
    }

    async fn session_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let id: i64 = reference.name.trim().parse().map_err(|_| AppError::invalid_input("Session ids are numeric."))?;
        let set = self
            .query_set(&format!("SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO FROM information_schema.PROCESSLIST WHERE ID = {id}"))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(row) = set.rows.first() {
            let info = set_text(&set, row, "INFO");
            if !info.is_empty() {
                detail = detail.definition(info, CodeLanguage::Sql);
            }
        }
        detail.properties = properties_of(&set).into_iter().filter(|p| p.name != "info").collect();
        Ok(detail
            .action(ObjectAction::new("kill-query", "Kill current query", format!("KILL QUERY {id}")))
            .action(ObjectAction::destructive("kill", "Kill connection", format!("KILL {id}"))))
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let pattern = quote_literal(&name.replace('%', "\\%").replace('_', "\\_"));
        let global = self.query_rows(&format!("SHOW GLOBAL VARIABLES LIKE {pattern}")).await?;
        let session = self.query_rows(&format!("SHOW SESSION VARIABLES LIKE {pattern}")).await.unwrap_or_default();
        let global_value = global.first().map(|r| cell_text(r.get(1))).unwrap_or_default();
        let session_value = session.first().map(|r| cell_text(r.get(1))).unwrap_or_default();
        let mut detail = ObjectDetail::empty(reference)
            .definition(format!("SET GLOBAL {} = {}", ident(name), quote_literal(&global_value)), CodeLanguage::Sql)
            .property("global value", global_value.clone());
        if !session_value.is_empty() && session_value != global_value {
            detail = detail.property("session value", session_value);
        }
        let upper = global_value.to_ascii_uppercase();
        if upper == "ON" || upper == "OFF" {
            let flipped = if upper == "ON" { "OFF" } else { "ON" };
            detail = detail.action(ObjectAction::destructive("toggle", &format!("Set {flipped}"), format!("SET GLOBAL {} = {flipped}", ident(name))));
        }
        Ok(detail.action(ObjectAction::destructive("default", "Reset to default", format!("SET GLOBAL {} = DEFAULT", ident(name)))))
    }

    async fn lock_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let items = self.list_locks().await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(item) = items.iter().find(|i| i.reference.name == reference.name) {
            if let Some(text) = &item.detail {
                detail = detail.definition(text.clone(), CodeLanguage::Text);
            }
            if let Some(mode) = &item.badge {
                detail = detail.property("mode", mode.clone());
            }
        }
        Ok(detail)
    }

    async fn replica_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let set = self.replica_status().await?;
        let mut detail = ObjectDetail::empty(reference);
        let row = set.rows.iter().find(|r| replica_summary(&set, r).reference.name == reference.name).or_else(|| set.rows.first());
        if let Some(row) = row {
            detail.properties = set
                .columns
                .iter()
                .zip(row.iter())
                .filter_map(|(c, v)| cell_opt(Some(v)).map(|text| ObjectProperty { name: pretty_label(&c.name), value: text }))
                .collect();
        }
        let channel = row.map(|r| set_text(&set, r, "Channel_Name")).unwrap_or_default();
        let suffix = if channel.is_empty() { String::new() } else { format!(" FOR CHANNEL {}", quote_literal(&channel)) };
        let modern = column_index(&set, "Replica_IO_Running").is_some();
        let word = if modern { "REPLICA" } else { "SLAVE" };
        Ok(detail
            .action(ObjectAction::destructive("stop", "Stop replication", format!("STOP {word}{suffix}")))
            .action(ObjectAction::destructive("start", "Start replication", format!("START {word}{suffix}"))))
    }

    async fn slow_query_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let rows = self.slow_query_rows().await?;
        let wanted = reference.name.trim_end_matches('…');
        let row = rows
            .iter()
            .find(|r| reference.parent.as_deref().is_some_and(|d| cell_text(r.first()) == d))
            .or_else(|| rows.iter().find(|r| preview(&cell_text(r.get(2)), PREVIEW_CHARS).starts_with(wanted)));
        let Some(row) = row else {
            return Err(AppError::not_found("That statement is no longer in the digest table."));
        };
        let mut detail = ObjectDetail::empty(reference).definition(cell_text(row.get(2)), CodeLanguage::Sql);
        let labels = ["digest", "schema", "", "calls", "avg seconds", "total seconds", "max seconds", "rows examined", "rows sent", "first seen", "last seen"];
        for (i, label) in labels.iter().enumerate() {
            if label.is_empty() {
                continue;
            }
            if let Some(value) = cell_opt(row.get(i)) {
                detail = detail.property(label, value);
            }
        }
        Ok(detail.action(ObjectAction::destructive(
            "reset",
            "Reset digest statistics",
            "TRUNCATE TABLE performance_schema.events_statements_summary_by_digest",
        )))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, sql: true, namespaces: false, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: true, exact_estimate: false },
        object_kinds: vec![K::Database, K::Table, K::View, K::Partition, K::Index, K::Constraint, K::Function, K::Procedure, K::Trigger, K::Event, K::User, K::Grant, K::Session, K::Lock, K::Replica, K::Setting, K::SlowQuery],
        tools: vec![T::Stats, T::Erd],
    }
}

#[async_trait]
impl Integration for MysqlIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let rows = self.query_rows("SELECT VERSION()").await?;
        Ok(rows.first().map(|r| describe_version(&cell_text(r.first()))))
    }

    fn current_database(&self) -> Option<String> {
        self.database.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let rows = self.query_rows("SHOW DATABASES").await?;
        Ok(rows
            .iter()
            .map(|r| cell_text(r.first()))
            .filter(|name| !name.is_empty() && !SYSTEM_DATABASES.contains(&name.as_str()))
            .collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let Some(db) = self.require_database() else {
            return Ok(SchemaCatalog { schemas: Vec::new() });
        };
        let sql = format!(
            "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = {} ORDER BY TABLE_NAME",
            quote_literal(db)
        );
        let rows = self.query_rows(&sql).await?;
        let tables = rows
            .iter()
            .map(|r| TableInfo {
                schema: None,
                name: cell_text(r.first()),
                kind: if cell_text(r.get(1)).eq_ignore_ascii_case("VIEW") { TableKind::View } else { TableKind::Table },
                row_estimate: cell_i64(r.get(2)),
            })
            .collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: db.to_string(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let Some(db) = self.require_database() else {
            return Err(AppError::invalid_input("Select a database first."));
        };
        self.columns_in(db, &table.name).await
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let Some(db) = self.require_database() else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
            quote_literal(db),
            quote_literal(&table.name)
        );
        let rows = self.query_rows(&sql).await?;
        let estimate = rows.first().and_then(|r| cell_i64(r.first()));
        match estimate {
            // InnoDB statistics lag behind fresh writes; an exact count is cheap when it says 0.
            Some(n) if n > 0 => Ok(Some(n)),
            _ => Ok(Some(self.count(table, &[]).await?)),
        }
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!(
            "SELECT COUNT(*) FROM {}{}",
            qualified_name_for(Engine::Mysql, table),
            where_clause(Engine::Mysql, filters)
        );
        let rows = self.query_rows(&sql).await?;
        Ok(rows.first().and_then(|r| cell_i64(r.first())).unwrap_or_default())
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            qualified_name_for(Engine::Mysql, table),
            where_clause(Engine::Mysql, &query.filters),
            order_clause(Engine::Mysql, &query.sort),
            query.limit,
            query.offset
        );
        let mut statements = self.run(&sql, query.limit as usize).await?;
        match statements.pop() {
            Some(StatementResult::Rows { result }) => Ok(result),
            _ => Ok(ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }),
        }
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        self.run(sql, max_rows).await
    }

    async fn close(&self) {
        self.pool.close().await;
    }

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        let Some(db) = self.require_database() else {
            return Ok(Vec::new());
        };
        let sql = format!(
            "SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
             FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = {} AND REFERENCED_TABLE_NAME IS NOT NULL \
             ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION",
            quote_literal(db)
        );
        let rows = self.query_rows(&sql).await?;
        let mut grouped: BTreeMap<(String, String), ForeignKey> = BTreeMap::new();
        for r in &rows {
            let name = cell_text(r.first());
            let from_table = cell_text(r.get(1));
            let entry = grouped.entry((from_table.clone(), name.clone())).or_insert_with(|| ForeignKey {
                name,
                from_schema: None,
                from_table,
                from_columns: Vec::new(),
                to_schema: None,
                to_table: cell_text(r.get(3)),
                to_columns: Vec::new(),
            });
            entry.from_columns.push(cell_text(r.get(2)));
            entry.to_columns.push(cell_text(r.get(4)));
        }
        Ok(grouped.into_values().collect())
    }

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let sql = format!("SHOW CREATE TABLE {}", qualified_name_for(Engine::Mysql, table));
        let rows = self.query_rows(&sql).await?;
        Ok(rows.first().and_then(|r| r.get(1)).map(|v| cell_text(Some(v))).filter(|d| !d.is_empty()))
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let (db, table) = split_owner(parent);
        // A scoped ask with no explicit database means the session's database, if any.
        let scope = if kind.scoped() { db.map(str::to_string).or_else(|| self.database.clone()) } else { db.map(str::to_string) };
        match kind {
            ObjectKind::User => self.list_users().await,
            ObjectKind::Grant => Ok(self.show_grants(None).await?.iter().map(|g| grant_summary(g)).collect()),
            ObjectKind::Lock => self.list_locks().await,
            ObjectKind::Replica => self.list_replicas().await,
            ObjectKind::SlowQuery => {
                let rows = self.slow_query_rows().await?;
                let mut items: Vec<ObjectSummary> = rows.iter().map(|r| summarize(kind, r)).collect();
                dedupe_names(&mut items);
                Ok(items)
            }
            _ => match object_list_sql(kind, scope.as_deref(), table) {
                Some(sql) => self.list_simple(kind, &sql).await,
                None => Ok(Vec::new()),
            },
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let (db, table) = split_owner(reference.parent.as_deref());
        match reference.kind {
            ObjectKind::Database => self.database_detail(reference).await,
            ObjectKind::User => self.user_detail(reference).await,
            ObjectKind::Grant => self.grant_detail(reference).await,
            ObjectKind::Session => self.session_detail(reference).await,
            ObjectKind::Setting => self.setting_detail(reference).await,
            ObjectKind::Lock => self.lock_detail(reference).await,
            ObjectKind::Replica => self.replica_detail(reference).await,
            ObjectKind::SlowQuery => self.slow_query_detail(reference).await,
            ObjectKind::Table => {
                let db = self.resolve_db(db)?;
                self.table_detail(reference, &db).await
            }
            ObjectKind::View => {
                let db = self.resolve_db(db)?;
                self.view_detail(reference, &db).await
            }
            ObjectKind::Function | ObjectKind::Procedure => {
                let db = self.resolve_db(db)?;
                self.routine_detail(reference, &db).await
            }
            ObjectKind::Trigger => {
                let db = self.resolve_db(db)?;
                self.trigger_detail(reference, &db).await
            }
            ObjectKind::Event => {
                let db = self.resolve_db(db)?;
                self.event_detail(reference, &db).await
            }
            ObjectKind::Index | ObjectKind::Constraint | ObjectKind::Partition => {
                let db = self.resolve_db(db)?;
                let Some(table) = table else {
                    return Err(AppError::invalid_input("Open this object from its table so the owner is known."));
                };
                match reference.kind {
                    ObjectKind::Index => self.index_detail(reference, &db, table).await,
                    ObjectKind::Constraint => self.constraint_detail(reference, &db, table).await,
                    _ => self.partition_detail(reference, &db, table).await,
                }
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let to_map = |rows: Vec<Vec<Value>>| -> BTreeMap<String, String> {
            rows.iter().map(|r| (cell_text(r.first()), cell_text(r.get(1)))).collect()
        };
        let status = to_map(self.query_rows("SHOW GLOBAL STATUS").await?);
        let variables = to_map(
            self.query_rows("SHOW GLOBAL VARIABLES WHERE Variable_name IN ('version', 'hostname', 'max_connections', 'innodb_buffer_pool_size')")
                .await
                .unwrap_or_default(),
        );
        Ok(ServerStats::now(build_stats(&status, &variables)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, FilterOp, SortRule};

    #[test]
    fn version_strings_name_the_flavour() {
        assert_eq!(describe_version("8.4.0"), "MySQL 8.4.0");
        assert_eq!(describe_version("11.4.2-MariaDB-ubu2404"), "MariaDB 11.4.2-MariaDB-ubu2404");
    }

    #[test]
    fn text_cells_decode_by_type_name() {
        assert_eq!(text_to_value("BIGINT", "42"), Value::Int(42));
        assert_eq!(text_to_value("BIGINT UNSIGNED", "18446744073709551615"), Value::Decimal("18446744073709551615".into()));
        assert_eq!(text_to_value("INT UNSIGNED", "7"), Value::Int(7));
        assert_eq!(text_to_value("BOOLEAN", "1"), Value::Bool(true));
        assert_eq!(text_to_value("BOOLEAN", "0"), Value::Bool(false));
        assert_eq!(text_to_value("TINYINT", "1"), Value::Int(1));
        assert_eq!(text_to_value("DOUBLE", "1.5"), Value::Float(1.5));
        assert_eq!(text_to_value("DECIMAL", "12.50"), Value::Decimal("12.50".into()));
        assert!(matches!(text_to_value("JSON", "{\"a\":1}"), Value::Json(_)));
        assert_eq!(text_to_value("JSON", "not json"), Value::Text("not json".into()));
        assert_eq!(text_to_value("DATETIME", "2026-09-04 01:02:03"), Value::DateTime("2026-09-04 01:02:03".into()));
        assert_eq!(text_to_value("VARCHAR", "hi"), Value::Text("hi".into()));
        assert!(is_blob_type("LONGBLOB") && !is_blob_type("VARBINARY") && !is_blob_type("TEXT"));
        assert!(is_binary_flagged_string("VARBINARY") && !is_binary_flagged_string("BLOB"));
    }

    #[test]
    fn ssl_modes_map() {
        assert!(matches!(ssl_mode(SslMode::Disable), MySqlSslMode::Disabled));
        assert!(matches!(ssl_mode(SslMode::VerifyFull), MySqlSslMode::VerifyIdentity));
    }


    // ---- object explorer (offline) --------------------------------------------

    fn text(s: &str) -> Value {
        Value::Text(s.into())
    }

    #[test]
    fn owner_keys_split_on_the_first_dot() {
        assert_eq!(split_owner(None), (None, None));
        assert_eq!(split_owner(Some("")), (None, None));
        assert_eq!(split_owner(Some("shop")), (Some("shop"), None));
        assert_eq!(split_owner(Some("shop.orders")), (Some("shop"), Some("orders")));
        assert_eq!(owner_key("shop", "orders"), "shop.orders");
    }

    #[test]
    fn list_sql_scopes_to_one_or_every_user_database() {
        let all = object_list_sql(ObjectKind::Table, None, None).unwrap_or_default();
        assert!(all.contains("TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')"), "{all}");
        assert!(all.ends_with("LIMIT 2000"));
        let one = object_list_sql(ObjectKind::Table, Some("it's"), None).unwrap_or_default();
        assert!(one.contains("TABLE_SCHEMA = 'it''s'"), "{one}");
        let nested = object_list_sql(ObjectKind::Index, Some("shop"), Some("orders")).unwrap_or_default();
        assert!(nested.contains("TABLE_SCHEMA = 'shop' AND TABLE_NAME = 'orders'"), "{nested}");
        assert!(nested.contains("GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME"));
        let routines = object_list_sql(ObjectKind::Procedure, None, None).unwrap_or_default();
        assert!(routines.contains("ROUTINE_TYPE = 'PROCEDURE'"));
        assert!(object_list_sql(ObjectKind::Function, None, None).unwrap_or_default().contains("ROUTINE_TYPE = 'FUNCTION'"));
        assert_eq!(object_list_sql(ObjectKind::Session, None, None).as_deref(), Some("SHOW FULL PROCESSLIST"));
        assert_eq!(object_list_sql(ObjectKind::Setting, None, None).as_deref(), Some("SHOW GLOBAL VARIABLES"));
        assert!(object_list_sql(ObjectKind::User, None, None).is_none());
        assert!(object_list_sql(ObjectKind::Lock, None, None).is_none());
    }

    #[test]
    fn rows_become_summaries() {
        let table = summarize(ObjectKind::Table, &[text("shop"), text("orders"), text("InnoDB"), Value::Int(1500), Value::Int(3_145_728), text("")]);
        assert_eq!(table.reference.parent.as_deref(), Some("shop"));
        assert_eq!(table.reference.name, "orders");
        assert_eq!(table.badge.as_deref(), Some("innodb"));
        assert_eq!(table.detail.as_deref(), Some("~1,500 rows · 3.0 MB"));

        let index = summarize(ObjectKind::Index, &[text("shop"), text("orders"), text("PRIMARY"), text("0"), text("BTREE"), text("id")]);
        assert_eq!(index.reference.parent.as_deref(), Some("shop.orders"));
        assert_eq!(index.badge.as_deref(), Some("primary"));
        let unique = summarize(ObjectKind::Index, &[text("shop"), text("orders"), text("ux_code"), text("0"), text("BTREE"), text("code")]);
        assert_eq!(unique.badge.as_deref(), Some("unique"));
        let fulltext = summarize(ObjectKind::Index, &[text("shop"), text("orders"), text("ft"), text("1"), text("FULLTEXT"), text("notes")]);
        assert_eq!(fulltext.badge.as_deref(), Some("fulltext"));
        assert_eq!(fulltext.detail.as_deref(), Some("orders (notes)"));

        let fk = summarize(
            ObjectKind::Constraint,
            &[text("shop"), text("orders"), text("fk_customer"), text("FOREIGN KEY"), text("customer_id"), text("customers"), text("CASCADE"), text("RESTRICT")],
        );
        assert_eq!(fk.badge.as_deref(), Some("foreign"));
        assert_eq!(fk.detail.as_deref(), Some("orders (customer_id) → customers · on delete cascade"));

        let func = summarize(
            ObjectKind::Function,
            &[text("shop"), text("total"), text("decimal(10,2)"), text("root@%"), text("YES"), text("READS SQL DATA"), text(""), text(""), text("IN order_id int")],
        );
        assert_eq!(func.detail.as_deref(), Some("(IN order_id int) → decimal(10,2)"));
        assert_eq!(func.badge.as_deref(), Some("deterministic"));

        let trigger = summarize(ObjectKind::Trigger, &[text("shop"), text("trg"), text("orders"), text("BEFORE"), text("INSERT"), text("root@%"), text("")]);
        assert_eq!(trigger.reference.parent.as_deref(), Some("shop.orders"));
        assert_eq!(trigger.detail.as_deref(), Some("BEFORE INSERT ON orders"));

        let event = summarize(ObjectKind::Event, &[text("shop"), text("nightly"), text("ENABLED"), text("RECURRING"), text("1"), text("DAY"), Value::Null, text("2026-09-03 02:00:00"), text("root@%")]);
        assert_eq!(event.detail.as_deref(), Some("every 1 day · last 2026-09-03 02:00:00"));
        assert_eq!(event.badge.as_deref(), Some("enabled"));

        let session = summarize(ObjectKind::Session, &[Value::Int(42), text("app"), text("10.0.0.5:5555"), text("shop"), text("Query"), Value::Int(3), text("executing"), text("SELECT   *\nFROM orders")]);
        assert_eq!(session.reference.name, "42");
        assert_eq!(session.badge.as_deref(), Some("query"));
        assert_eq!(session.detail.as_deref(), Some("app@10.0.0.5:5555 · shop · 3s · executing · SELECT * FROM orders"));

        let lock = summarize(ObjectKind::Lock, &[text("1234"), text("shop"), text("orders"), text("PRIMARY"), text("RECORD"), text("X"), text("GRANTED"), text("5"), Value::Int(9)]);
        assert_eq!(lock.reference.name, "shop.orders #1234");
        assert_eq!(lock.badge.as_deref(), Some("x"));
        assert_eq!(lock.detail.as_deref(), Some("record · granted · index PRIMARY · 5"));

        let slow = summarize(
            ObjectKind::SlowQuery,
            &[text("abc123"), text("shop"), text("SELECT * FROM `orders` WHERE `id` = ?"), Value::Int(12), Value::Float(0.25), Value::Float(3.0), Value::Float(1.0), Value::Int(4000), Value::Int(12), text(""), text("")],
        );
        assert_eq!(slow.reference.parent.as_deref(), Some("abc123"));
        assert_eq!(slow.badge.as_deref(), Some("shop"));
        assert_eq!(slow.detail.as_deref(), Some("avg 0.250 s · 12 calls · 4,000 rows examined"));

        let user = user_summary(&[text("app"), text("10.%"), text("caching_sha2_password"), text("Y")]);
        assert_eq!(user.reference.name, "app");
        assert_eq!(user.badge.as_deref(), Some("10.%"));
        assert_eq!(user.detail.as_deref(), Some("caching_sha2_password · locked"));
    }

    #[test]
    fn grants_parse_and_classify() {
        assert_eq!(
            parse_grant("GRANT SELECT, INSERT ON `shop`.* TO `app`@`%`"),
            Some(("SELECT, INSERT".into(), "`shop`.*".into(), "`app`@`%`".into()))
        );
        assert_eq!(
            parse_grant("GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost` WITH GRANT OPTION"),
            Some(("ALL PRIVILEGES".into(), "*.*".into(), "`root`@`localhost`".into()))
        );
        assert_eq!(parse_grant("GRANT `reader`@`%` TO `app`@`%`"), Some(("`reader`@`%`".into(), String::new(), "`app`@`%`".into())));
        assert_eq!(parse_grant("REVOKE X"), None);
        assert_eq!(grant_badge("*.*", "SELECT"), "global");
        assert_eq!(grant_badge("`shop`.*", "SELECT"), "database");
        assert_eq!(grant_badge("`shop`.`orders`", "SELECT (id)"), "column");
        assert_eq!(grant_badge("`shop`.`orders`", "SELECT"), "table");
        assert_eq!(grant_badge("", "`reader`@`%`"), "role");
        let summary = grant_summary("GRANT SELECT ON `shop`.* TO `app`@`%`");
        assert_eq!(summary.reference.name, "SELECT ON `shop`.*");
        assert_eq!(summary.reference.parent.as_deref(), Some("`app`@`%`"));
        assert_eq!(summary.badge.as_deref(), Some("database"));
    }

    #[test]
    fn replica_rows_read_both_column_generations() {
        let set = ResultSet {
            columns: ["Source_Host", "Source_Port", "Replica_IO_Running", "Replica_SQL_Running", "Seconds_Behind_Source", "Channel_Name", "Last_Error"]
                .iter()
                .map(|n| ColumnMeta { name: (*n).into(), type_name: "text".into() })
                .collect(),
            rows: vec![vec![text("db1"), Value::Int(3306), text("Yes"), text("Yes"), Value::Int(2), text("main"), text("")]],
            truncated: false,
        };
        let summary = replica_summary(&set, &set.rows[0]);
        assert_eq!(summary.reference.name, "db1:3306 [main]");
        assert_eq!(summary.badge.as_deref(), Some("running"));
        assert_eq!(summary.detail.as_deref(), Some("lag 2 s · IO Yes · SQL Yes"));
        let legacy = ResultSet {
            columns: ["Master_Host", "Master_Port", "Slave_IO_Running", "Slave_SQL_Running", "Seconds_Behind_Master"]
                .iter()
                .map(|n| ColumnMeta { name: (*n).into(), type_name: "text".into() })
                .collect(),
            rows: vec![vec![text("db1"), Value::Int(3306), text("No"), text("Yes"), Value::Null]],
            truncated: false,
        };
        let summary = replica_summary(&legacy, &legacy.rows[0]);
        assert_eq!(summary.reference.name, "db1:3306");
        assert_eq!(summary.badge.as_deref(), Some("stopped"));
    }

    #[test]
    fn innodb_status_lock_lines_are_extracted() {
        let status = "=====================================\nTRANSACTIONS\n------------\nTrx id counter 1234\n---TRANSACTION 1230, ACTIVE 5 sec\n2 lock struct(s), heap size 1136\nTABLE LOCK table `shop`.`orders` trx id 1230 lock mode IX\nRECORD LOCKS space id 5 page no 4 n bits 72 index PRIMARY of table `shop`.`orders` trx id 1230 lock_mode X\n--------\nFILE I/O\n--------\nI/O thread 0 state: waiting\n";
        let locks = parse_innodb_status_locks(status);
        assert_eq!(locks.len(), 3, "{locks:?}");
        assert!(locks[1].starts_with("TABLE LOCK"));
        assert!(locks[2].starts_with("RECORD LOCKS"));
        assert!(parse_innodb_status_locks("no transactions section").is_empty());
    }

    #[test]
    fn helpers_format_and_dedupe() {
        assert_eq!(human_bytes(512.0), "512 B");
        assert_eq!(human_bytes(1536.0), "1.5 KB");
        assert_eq!(human_bytes(3.0 * 1024.0 * 1024.0 * 1024.0), "3.0 GB");
        assert_eq!(human_duration(45), "45s");
        assert_eq!(human_duration(125), "2m 5s");
        assert_eq!(human_duration(3_700), "1h 1m");
        assert_eq!(human_duration(90_061), "1d 1h 1m");
        assert_eq!(preview("select   *\n  from t", 100), "select * from t");
        assert_eq!(preview("abcdefghij", 5), "abcde…");
        assert_eq!(pretty_label("TABLE_ROWS"), "table rows");
        let cols = vec![
            ColumnMeta { name: "Trigger".into(), type_name: "text".into() },
            ColumnMeta { name: "sql_mode".into(), type_name: "text".into() },
            ColumnMeta { name: "SQL Original Statement".into(), type_name: "text".into() },
        ];
        assert_eq!(create_column_index(&cols), Some(2));
        let cols = vec![ColumnMeta { name: "View".into(), type_name: "text".into() }, ColumnMeta { name: "Create View".into(), type_name: "text".into() }];
        assert_eq!(create_column_index(&cols), Some(1));
        let mut items = vec![
            ObjectSummary::new(ObjectKind::Lock, "a", None),
            ObjectSummary::new(ObjectKind::Lock, "a", None),
            ObjectSummary::new(ObjectKind::Lock, "b", None),
            ObjectSummary::new(ObjectKind::Lock, "a", None),
        ];
        dedupe_names(&mut items);
        let names: Vec<&str> = items.iter().map(|i| i.reference.name.as_str()).collect();
        assert_eq!(names, vec!["a", "a (2)", "b", "a (3)"]);
        let set = ResultSet {
            columns: vec![ColumnMeta { name: "ENGINE".into(), type_name: "text".into() }, ColumnMeta { name: "TABLE_ROWS".into(), type_name: "int".into() }, ColumnMeta { name: "TABLE_COMMENT".into(), type_name: "text".into() }],
            rows: vec![vec![text("InnoDB"), Value::Int(10), Value::Null]],
            truncated: false,
        };
        let props = properties_of(&set);
        assert_eq!(props.len(), 2);
        assert_eq!(props[0].name, "engine");
        assert_eq!(props[1].value, "10");
        assert_eq!(account_literal("app", "10.%"), "'app'@'10.%'");
    }

    #[test]
    fn stats_derive_ratios_and_units() {
        let status: BTreeMap<String, String> = [
            ("Uptime", "7200"),
            ("Questions", "14400"),
            ("Threads_connected", "5"),
            ("Threads_running", "2"),
            ("Max_used_connections", "20"),
            ("Innodb_buffer_pool_read_requests", "1000"),
            ("Innodb_buffer_pool_reads", "50"),
            ("Bytes_sent", "2048"),
            ("Com_select", "100"),
        ]
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
        let variables: BTreeMap<String, String> =
            [("version", "8.4.0"), ("max_connections", "151")].iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
        let groups = build_stats(&status, &variables);
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Connections", "Queries", "InnoDB cache", "Throughput"]);
        let find = |title: &str, label: &str| groups.iter().find(|g| g.title == title).and_then(|g| g.stats.iter().find(|s| s.label == label)).cloned();
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("MySQL 8.4.0".into()));
        assert_eq!(find("Server", "Uptime").map(|s| s.value), Some("2h 0m".into()));
        assert_eq!(find("Server", "Queries / s (avg)").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Connections", "Max used").and_then(|s| s.hint), Some("of 151 allowed".into()));
        assert_eq!(find("InnoDB cache", "Buffer pool hit ratio").and_then(|s| s.numeric), Some(95.0));
        let sent = find("Throughput", "Bytes sent").unwrap_or_else(|| panic!("bytes sent missing"));
        assert_eq!(sent.value, "2.0 KB");
        assert_eq!(sent.numeric, Some(2048.0));
        assert!(find("Queries", "INSERT").is_none(), "absent counters are skipped");
        assert!(build_stats(&BTreeMap::new(), &BTreeMap::new()).is_empty());
    }

    #[test]
    fn profile_kinds_all_have_a_listing_path() {
        for kind in profile().object_kinds {
            let handled = object_list_sql(kind, None, None).is_some()
                || matches!(kind, ObjectKind::User | ObjectKind::Grant | ObjectKind::Lock | ObjectKind::Replica | ObjectKind::SlowQuery);
            assert!(handled, "{kind:?} is declared but has no listing");
        }
    }

    // WHAT:  Live round trip against a real server. Skipped unless DB_FREE_MYSQL_HOST is set.
    // HOW:   DB_FREE_MYSQL_HOST / PORT / USER / PASSWORD / DB, e.g. a throwaway docker mysql:8.4.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(host) = std::env::var("DB_FREE_MYSQL_HOST") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Mysql,
            environment: Environment::Local,
            read_only: false,
            host: Some(host),
            port: std::env::var("DB_FREE_MYSQL_PORT").ok().and_then(|p| p.parse().ok()),
            database: std::env::var("DB_FREE_MYSQL_DB").ok(),
            username: std::env::var("DB_FREE_MYSQL_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary::draft(&input, true),
            secret: std::env::var("DB_FREE_MYSQL_PASSWORD").ok(),
        };
        let my = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        my.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert_eq!(my.engine(), Engine::Mysql);
        let version = my.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("MySQL") || version.starts_with("MariaDB"), "{version}");
        let dbs = my.databases().await.unwrap_or_default();
        assert!(dbs.iter().any(|d| Some(d) == my.current_database().as_ref()), "{dbs:?}");

        my.execute("DROP TABLE IF EXISTS dbfree_t", 10).await.unwrap_or_else(|e| panic!("drop: {e}"));
        let out = my
            .execute(
                "CREATE TABLE dbfree_t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), meta JSON, raw BLOB, ts DATETIME, n DECIMAL(10,2), ok TINYINT(1)); \
                 INSERT INTO dbfree_t (name, meta, raw, ts, n, ok) VALUES ('ann', '{\"a\": 1}', X'6869', '2026-09-04 01:02:03', 12.50, 1), ('bob', NULL, NULL, NULL, NULL, 0); \
                 SELECT * FROM dbfree_t ORDER BY id;",
                100,
            )
            .await
            .unwrap_or_else(|e| panic!("execute: {e}"));
        assert_eq!(out.len(), 3, "{out:?}");
        match out.get(2) {
            Some(StatementResult::Rows { result }) => {
                assert_eq!(result.rows.len(), 2);
                let first = result.rows.first().cloned().unwrap_or_default();
                assert_eq!(first.first(), Some(&Value::Int(1)));
                assert_eq!(first.get(1), Some(&Value::Text("ann".into())));
                assert!(matches!(first.get(2), Some(Value::Json(_))), "{first:?}");
                assert_eq!(first.get(3), Some(&Value::Bytes("aGk=".into())));
                assert_eq!(first.get(4), Some(&Value::DateTime("2026-09-04 01:02:03".into())));
                assert_eq!(first.get(5), Some(&Value::Decimal("12.50".into())));
                assert_eq!(first.get(6), Some(&Value::Bool(true)));
                let second = result.rows.get(1).cloned().unwrap_or_default();
                assert_eq!(second.get(2), Some(&Value::Null));
                assert_eq!(second.get(6), Some(&Value::Bool(false)));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        match out.first() {
            Some(StatementResult::Affected { rows_affected }) => assert_eq!(*rows_affected, 0),
            other => panic!("expected affected, got {other:?}"),
        }

        let catalog = my.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let table_ref = TableRef { schema: None, name: "dbfree_t".into() };
        assert!(catalog.schemas.iter().flat_map(|s| s.tables.iter()).any(|t| t.name == "dbfree_t" && t.kind == TableKind::Table));
        let cols = my.columns(&table_ref).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(cols.len(), 7);
        assert!(cols.iter().any(|c| c.name == "id" && c.primary_key && !c.nullable));
        assert!(cols.iter().any(|c| c.name == "name" && c.data_type == "varchar(50)" && c.nullable));
        assert_eq!(my.row_estimate(&table_ref).await.unwrap_or_default(), Some(2));
        assert_eq!(my.count(&table_ref, &[]).await.unwrap_or_default(), 2);

        let query = PageQuery {
            sort: vec![SortRule { column: "id".into(), desc: true }],
            filters: vec![FilterRule { column: "name".into(), op: FilterOp::Contains, value: "N".into() }],
            offset: 0,
            limit: 10,
        };
        let page = my.fetch_page(&table_ref, &query).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1, "only 'ann' contains n: {page:?}");
        assert_eq!(page.rows.first().and_then(|r| r.get(1)), Some(&Value::Text("ann".into())));
        assert_eq!(my.count(&table_ref, &query.filters).await.unwrap_or_default(), 1);

        let fks = my.foreign_keys().await.unwrap_or_else(|e| panic!("foreign_keys: {e}"));
        assert!(fks.iter().all(|fk| !fk.from_columns.is_empty()));
        let ddl = my.ddl(&table_ref).await.unwrap_or_else(|e| panic!("ddl: {e}"));
        assert!(ddl.is_some_and(|d| d.contains("CREATE TABLE")));

        my.execute("DROP TABLE dbfree_t", 10).await.unwrap_or_else(|e| panic!("drop: {e}"));
        my.close().await;
    }

}
