// SOT: duckdb-integration, duckdb-adapter, duckdb-value-decoding, duckdb-catalog-queries, duckdb-object-explorer, duckdb-settings, duckdb-server-stats

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{qualified_name_for, quote_ident, Capabilities, Integration};
use crate::model::{
    objects::format_number, CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, ObjectAction,
    ObjectDetail, ObjectKind, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, ServerStats, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use duckdb::types::{TimeUnit, ValueRef};
use duckdb::{params, AccessMode, Config, Connection, Row};
use std::sync::{Arc, Mutex};

// ============================================================================
// DUCKDB ADAPTER
//
// WHAT:  DuckDB (embedded analytics) on the bundled `duckdb` crate.
// WHY:   Same shape as sqlite.rs: a sync driver on the blocking pool so the
//        async trait never stalls the runtime while a scan runs.
// HOW:   `duckdb::Connection` is Send but not Sync, so it lives behind
//        Arc<Mutex<>> and every call goes through `spawn_blocking`.
//        A read-only connection opens with access_mode=READ_ONLY so the engine
//        itself refuses writes, on top of the guard's own check.
//        Identifiers are ANSI double-quoted (Engine::Duckdb → quote_ident).
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI meta)
// ============================================================================

impl From<duckdb::Error> for AppError {
    fn from(err: duckdb::Error) -> Self {
        AppError::driver(err)
    }
}

const SYSTEM_SCHEMAS: &str = "('information_schema', 'pg_catalog')";

pub struct DuckdbIntegration {
    conn: Arc<Mutex<Connection>>,
    file_name: String,
}

// WHAT:  Whether `path` asks for an in-memory database rather than a file.
fn is_memory_path(path: &str) -> bool {
    let trimmed = path.trim();
    trimmed.is_empty() || trimmed == ":memory:" || trimmed.eq_ignore_ascii_case("memory")
}

fn display_name(path: &str) -> String {
    if is_memory_path(path) {
        return "memory".to_string();
    }
    std::path::Path::new(path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string())
}

fn open(path: &str, read_only: bool) -> AppResult<Connection> {
    if is_memory_path(path) {
        return Connection::open_in_memory().map_err(AppError::from);
    }
    let mut config = Config::default();
    if read_only {
        if !std::path::Path::new(path).exists() {
            return Err(AppError::not_found(format!("DuckDB file \"{path}\" does not exist (read-only connections do not create it).")));
        }
        config = config.access_mode(AccessMode::ReadOnly)?;
    }
    Connection::open_with_flags(path, config).map_err(AppError::from)
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let path = conn.summary.file_path.clone().unwrap_or_default();
    let read_only = conn.summary.read_only;
    let open_path = path.clone();
    let connection = tokio::task::spawn_blocking(move || open(&open_path, read_only))
        .await
        .map_err(AppError::internal)??;
    Ok(Arc::new(DuckdbIntegration { conn: Arc::new(Mutex::new(connection)), file_name: display_name(&path) }))
}

// ---------------------------------------------------------------------------
// Value decoding
// ---------------------------------------------------------------------------

fn split_time_unit(unit: TimeUnit, raw: i64) -> (i64, u32) {
    let (secs, nanos) = match unit {
        TimeUnit::Second => (raw, 0),
        TimeUnit::Millisecond => (raw.div_euclid(1_000), raw.rem_euclid(1_000) * 1_000_000),
        TimeUnit::Microsecond => (raw.div_euclid(1_000_000), raw.rem_euclid(1_000_000) * 1_000),
        TimeUnit::Nanosecond => (raw.div_euclid(1_000_000_000), raw.rem_euclid(1_000_000_000)),
    };
    (secs, u32::try_from(nanos).unwrap_or_default())
}

fn timestamp_text(unit: TimeUnit, raw: i64) -> String {
    let (secs, nanos) = split_time_unit(unit, raw);
    chrono::DateTime::from_timestamp(secs, nanos)
        .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.f").to_string())
        .unwrap_or_else(|| raw.to_string())
}

fn date_text(days: i32) -> String {
    chrono::DateTime::from_timestamp(i64::from(days) * 86_400, 0)
        .map(|dt| dt.date_naive().format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| days.to_string())
}

fn time_text(unit: TimeUnit, raw: i64) -> String {
    let (secs, nanos) = split_time_unit(unit, raw);
    let secs_of_day = u32::try_from(secs.rem_euclid(86_400)).unwrap_or_default();
    chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs_of_day, nanos)
        .map(|t| t.format("%H:%M:%S%.f").to_string())
        .unwrap_or_else(|| raw.to_string())
}

fn i128_value(i: i128) -> Value {
    i64::try_from(i).map(Value::Int).unwrap_or_else(|_| Value::Decimal(i.to_string()))
}

// WHAT:  Nested (list / struct / map / array / union) → JSON for the inspector.
fn owned_to_json(value: duckdb::types::Value) -> serde_json::Value {
    use duckdb::types::Value as D;
    use serde_json::Value as J;
    match value {
        D::Null => J::Null,
        D::Boolean(b) => J::Bool(b),
        D::TinyInt(i) => J::from(i),
        D::SmallInt(i) => J::from(i),
        D::Int(i) => J::from(i),
        D::BigInt(i) => J::from(i),
        D::HugeInt(i) => i64::try_from(i).map(J::from).unwrap_or_else(|_| J::String(i.to_string())),
        D::UTinyInt(i) => J::from(i),
        D::USmallInt(i) => J::from(i),
        D::UInt(i) => J::from(i),
        D::UBigInt(i) => J::from(i),
        D::Float(f) => serde_json::Number::from_f64(f64::from(f)).map(J::Number).unwrap_or(J::Null),
        D::Double(f) => serde_json::Number::from_f64(f).map(J::Number).unwrap_or(J::Null),
        D::Decimal(d) => J::String(d.to_string()),
        D::Timestamp(unit, raw) => J::String(timestamp_text(unit, raw)),
        D::Text(s) | D::Enum(s) => J::String(s),
        D::Blob(bytes) => J::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
        D::Date32(d) => J::String(date_text(d)),
        D::Time64(unit, raw) => J::String(time_text(unit, raw)),
        D::Interval { months, days, nanos } => J::String(format!("{months} months {days} days {nanos} ns")),
        D::List(items) | D::Array(items) => J::Array(items.into_iter().map(owned_to_json).collect()),
        D::Struct(map) => J::Object(map.iter().map(|(k, v)| (k.clone(), owned_to_json(v.clone()))).collect()),
        D::Map(map) => J::Object(
            map.iter()
                .map(|(k, v)| {
                    let key = match owned_to_json(k.clone()) {
                        J::String(s) => s,
                        other => other.to_string(),
                    };
                    (key, owned_to_json(v.clone()))
                })
                .collect(),
        ),
        D::Union(inner) => owned_to_json(*inner),
        // `duckdb::types::Value` is #[non_exhaustive]: future variants degrade to text.
        other => J::String(format!("{other:?}")),
    }
}

// WHAT:  One DuckDB cell → the engine-neutral Value.
fn decode_cell(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(i) => Value::Int(i64::from(i)),
        ValueRef::SmallInt(i) => Value::Int(i64::from(i)),
        ValueRef::Int(i) => Value::Int(i64::from(i)),
        ValueRef::BigInt(i) => Value::Int(i),
        ValueRef::HugeInt(i) => i128_value(i),
        ValueRef::UTinyInt(i) => Value::Int(i64::from(i)),
        ValueRef::USmallInt(i) => Value::Int(i64::from(i)),
        ValueRef::UInt(i) => Value::Int(i64::from(i)),
        ValueRef::UBigInt(i) => i64::try_from(i).map(Value::Int).unwrap_or_else(|_| Value::Decimal(i.to_string())),
        ValueRef::Float(f) => Value::Float(f64::from(f)),
        ValueRef::Double(f) => Value::Float(f),
        ValueRef::Decimal(d) => Value::Decimal(d.to_string()),
        ValueRef::Timestamp(unit, raw) => Value::DateTime(timestamp_text(unit, raw)),
        ValueRef::Date32(days) => Value::DateTime(date_text(days)),
        ValueRef::Time64(unit, raw) => Value::DateTime(time_text(unit, raw)),
        ValueRef::Text(bytes) => Value::Text(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(bytes) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes)),
        ValueRef::Interval { months, days, nanos } => Value::Text(format!("{months} months {days} days {nanos} ns")),
        ValueRef::Enum(..) => match value.to_owned() {
            duckdb::types::Value::Enum(s) => Value::Text(s),
            other => Value::Json(owned_to_json(other)),
        },
        ValueRef::List(..) | ValueRef::Struct(..) | ValueRef::Array(..) | ValueRef::Map(..) | ValueRef::Union(..) => {
            Value::Json(owned_to_json(value.to_owned()))
        }
        // `duckdb::types::ValueRef` is #[non_exhaustive]: future variants degrade to JSON.
        _ => Value::Json(owned_to_json(value.to_owned())),
    }
}

fn type_label(data_type: &duckdb::arrow::datatypes::DataType) -> String {
    use duckdb::arrow::datatypes::DataType as T;
    match data_type {
        T::Boolean => "boolean".into(),
        T::Int8 => "tinyint".into(),
        T::Int16 => "smallint".into(),
        T::Int32 => "integer".into(),
        T::Int64 => "bigint".into(),
        T::UInt8 => "utinyint".into(),
        T::UInt16 => "usmallint".into(),
        T::UInt32 => "uinteger".into(),
        T::UInt64 => "ubigint".into(),
        T::Float32 => "float".into(),
        T::Float64 => "double".into(),
        T::Utf8 | T::LargeUtf8 | T::Utf8View => "varchar".into(),
        T::Binary | T::LargeBinary | T::BinaryView => "blob".into(),
        T::Date32 | T::Date64 => "date".into(),
        T::Time32(_) | T::Time64(_) => "time".into(),
        T::Timestamp(_, Some(_)) => "timestamptz".into(),
        T::Timestamp(_, None) => "timestamp".into(),
        T::Decimal128(p, s) | T::Decimal256(p, s) => format!("decimal({p},{s})"),
        T::List(_) | T::LargeList(_) | T::FixedSizeList(..) => "list".into(),
        T::Struct(_) => "struct".into(),
        T::Map(..) => "map".into(),
        T::Union(..) => "union".into(),
        T::Dictionary(..) => "enum".into(),
        T::Interval(_) => "interval".into(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

// WHAT:  Statements whose arrow result is a synthetic "Count" column, not data.
fn is_dml_without_returning(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    let first = upper.split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("");
    let dml = matches!(first, "INSERT" | "UPDATE" | "DELETE" | "COPY" | "CREATE" | "DROP" | "ALTER" | "TRUNCATE" | "ATTACH" | "DETACH" | "SET" | "RESET" | "BEGIN" | "COMMIT" | "ROLLBACK" | "VACUUM" | "ANALYZE" | "CHECKPOINT" | "INSTALL" | "LOAD" | "IMPORT" | "EXPORT" | "USE" | "MERGE");
    dml && !upper.contains("RETURNING")
}

// WHAT:  Runs one statement, collecting rows or the change count.
fn run_statement(conn: &Connection, sql: &str, max_rows: usize) -> AppResult<StatementResult> {
    let mut stmt = conn.prepare(sql)?;
    let changes = stmt.execute([])?;
    if stmt.column_count() == 0 || is_dml_without_returning(sql) {
        return Ok(StatementResult::Affected { rows_affected: changes as u64 });
    }
    let schema = stmt.schema();
    let columns: Vec<ColumnMeta> = schema
        .fields()
        .iter()
        .map(|f| ColumnMeta { name: f.name().clone(), type_name: type_label(f.data_type()) })
        .collect();
    let width = columns.len();
    let mut rows = stmt.raw_query();
    let mut collected: Vec<Vec<Value>> = Vec::new();
    let mut truncated = false;
    while let Some(row) = rows.next()? {
        if collected.len() >= max_rows {
            truncated = true;
            break;
        }
        let mut cells = Vec::with_capacity(width);
        for i in 0..width {
            cells.push(decode_cell(row.get_ref(i)?));
        }
        collected.push(cells);
    }
    Ok(StatementResult::Rows { result: ResultSet { columns, rows: collected, truncated } })
}

fn run_script(conn: &Connection, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
    let mut out = Vec::new();
    for statement in split_statements(sql) {
        if statement.trim().is_empty() {
            continue;
        }
        out.push(run_statement(conn, &statement, max_rows)?);
    }
    Ok(out)
}

fn schema_of(table: &TableRef) -> String {
    table.schema.clone().unwrap_or_else(|| "main".to_string())
}

fn table_columns(conn: &Connection, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
    let schema = schema_of(table);
    let mut pk_stmt = conn.prepare(
        "SELECT constraint_column_names FROM duckdb_constraints() \
         WHERE schema_name = ? AND table_name = ? AND constraint_type = 'PRIMARY KEY'",
    )?;
    let mut pk_cols: Vec<String> = Vec::new();
    let mut pk_rows = pk_stmt.query(params![schema, table.name])?;
    while let Some(row) = pk_rows.next()? {
        if let Value::Json(serde_json::Value::Array(items)) = decode_cell(row.get_ref(0)?) {
            pk_cols.extend(items.into_iter().filter_map(|v| v.as_str().map(str::to_string)));
        }
    }
    let mut stmt = conn.prepare(
        "SELECT column_name, data_type, is_nullable, ordinal_position FROM information_schema.columns \
         WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
    )?;
    let rows = stmt
        .query_map(params![schema, table.name], |row| {
            let name: String = row.get(0)?;
            let nullable: String = row.get(2)?;
            let ordinal: i64 = row.get(3)?;
            Ok(ColumnInfo {
                primary_key: pk_cols.iter().any(|c| c == &name),
                name,
                data_type: row.get::<_, String>(1)?.to_ascii_lowercase(),
                nullable: nullable.eq_ignore_ascii_case("YES"),
                ordinal: u32::try_from(ordinal).unwrap_or_default(),
            })
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;
    Ok(rows)
}

impl DuckdbIntegration {
    async fn blocking<T, F>(&self, f: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> AppResult<T> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| AppError::internal("duckdb session lock poisoned"))?;
            f(&guard)
        })
        .await
        .map_err(AppError::internal)?
    }

    async fn scalar_i64(&self, sql: String) -> AppResult<i64> {
        self.blocking(move |conn| {
            let n: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
            Ok(n)
        })
        .await
    }
}

// ============================================================================
// OBJECT EXPLORER
//
// WHAT:  DuckDB's catalog table functions (duckdb_databases / _schemas /
//        _tables / _views / _sequences / _types / _functions / _indexes /
//        _constraints / _extensions / _settings) behind the generic object
//        explorer, plus admin figures from pragma_database_size() and
//        duckdb_memory().
// WHY:   Every catalog question in DuckDB is one table function call, so a kind
//        is one query; information_schema only covers tables and columns and
//        knows nothing about macros, extensions or settings.
// HOW:   `parent` is a schema for scoped kinds and the owning table when the
//        explorer drills into a table's indexes or constraints (see `Scope`).
//        Names reaching SQL go through quote_literal / quote_ident, never
//        string interpolation.
// WHERE: src-tauri/src/model/objects.rs, src/features/objects/ObjectTab.tsx
// ============================================================================

const OBJECT_CAP: usize = 2000;

// WHAT:  What a `parent` selects: every user schema, one schema, or one table.
enum Scope {
    UserSchemas,
    Schema(String),
    Table(String),
}

// WHAT:  A `parent` that names a schema scopes by schema; anything else is the
//        owning table (indexes / constraints opened from a table's children).
fn resolve_scope(conn: &Connection, parent: Option<&str>) -> AppResult<Scope> {
    let Some(name) = parent else { return Ok(Scope::UserSchemas) };
    let known: i64 = conn.query_row("SELECT count(*) FROM duckdb_schemas() WHERE schema_name = ?", params![name], |row| row.get(0))?;
    Ok(if known > 0 { Scope::Schema(name.to_string()) } else { Scope::Table(name.to_string()) })
}

impl Scope {
    // WHAT:  WHERE fragment for a catalog function. `table_column` is the name of
    //        its table column, when it has one (duckdb_indexes, duckdb_constraints).
    fn predicate(&self, table_column: Option<&str>) -> String {
        match self {
            Scope::UserSchemas => format!("schema_name NOT IN {SYSTEM_SCHEMAS}"),
            Scope::Schema(schema) => format!("schema_name = {}", quote_literal(schema)),
            Scope::Table(table) => match table_column {
                Some(column) => format!("{column} = {}", quote_literal(table)),
                None => format!("schema_name NOT IN {SYSTEM_SCHEMAS}"),
            },
        }
    }
}

fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

// WHAT:  Runs one catalog query into summaries, capped like every other family.
fn summaries<F>(conn: &Connection, sql: &str, build: F) -> AppResult<Vec<ObjectSummary>>
where
    F: Fn(&Row<'_>) -> duckdb::Result<ObjectSummary>,
{
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        if out.len() >= OBJECT_CAP {
            break;
        }
        out.push(build(row)?);
    }
    Ok(out)
}

// WHAT:  A LIST(VARCHAR) catalog column as plain strings.
fn list_text(value: Value) -> Vec<String> {
    match value {
        Value::Json(serde_json::Value::Array(items)) => items.into_iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
        Value::Text(text) => vec![text],
        _ => Vec::new(),
    }
}

// WHAT:  `name(a, b) → RETURN_TYPE`, the signature shown next to a function.
fn signature(name: &str, parameters: &[String], return_type: Option<&str>) -> String {
    let head = format!("{name}({})", parameters.join(", "));
    match return_type.filter(|r| !r.is_empty()) {
        Some(returns) => format!("{head} → {returns}"),
        None => head,
    }
}

// WHAT:  Constraints are not always named; fall back to a stable synthetic name.
fn constraint_name(name: Option<String>, table: &str, kind: &str, index: i64) -> String {
    name.filter(|n| !n.is_empty())
        .unwrap_or_else(|| format!("{table}_{}_{index}", kind.to_ascii_lowercase().replace(' ', "_")))
}

// WHAT:  A DuckDB SET value: bare for numbers and booleans, quoted otherwise.
fn set_statement(name: &str, value: &str) -> String {
    let bare = !value.is_empty()
        && (value.eq_ignore_ascii_case("true")
            || value.eq_ignore_ascii_case("false")
            || value.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-'));
    if bare {
        format!("SET {name} = {value};")
    } else {
        format!("SET {name} = {};", quote_literal(value))
    }
}

fn table_summary(row: &Row<'_>) -> duckdb::Result<ObjectSummary> {
    let name: String = row.get(0)?;
    let schema: String = row.get(1)?;
    let estimated: Option<i64> = row.get(2)?;
    let columns: Option<i64> = row.get(3)?;
    let temporary: Option<bool> = row.get(4)?;
    let mut detail = Vec::new();
    if let Some(n) = columns {
        detail.push(format!("{n} columns"));
    }
    if let Some(n) = estimated {
        detail.push(format!("~{} rows", format_number(n as f64)));
    }
    let mut summary = ObjectSummary::new(ObjectKind::Table, name, Some(schema)).with_detail(detail.join(" · "));
    if temporary == Some(true) {
        summary = summary.with_badge("temporary");
    }
    Ok(summary)
}

fn index_summary(row: &Row<'_>) -> duckdb::Result<ObjectSummary> {
    let name: String = row.get(0)?;
    let table: String = row.get(1)?;
    let unique: Option<bool> = row.get(2)?;
    let primary: Option<bool> = row.get(3)?;
    let badge = match (primary, unique) {
        (Some(true), _) => "primary",
        (_, Some(true)) => "unique",
        _ => "index",
    };
    Ok(ObjectSummary::new(ObjectKind::Index, name, Some(table.clone())).with_detail(format!("on {table}")).with_badge(badge))
}

fn constraint_summary(row: &Row<'_>) -> duckdb::Result<ObjectSummary> {
    let table: String = row.get(0)?;
    let kind: String = row.get(1)?;
    let text: Option<String> = row.get(2)?;
    let raw_name: Option<String> = row.get(3)?;
    let index: i64 = row.get(4)?;
    let name = constraint_name(raw_name, &table, &kind, index);
    Ok(ObjectSummary::new(ObjectKind::Constraint, name, Some(table))
        .with_detail(text.unwrap_or_else(|| kind.clone()))
        .with_badge(kind.to_ascii_lowercase()))
}

fn list_objects(conn: &Connection, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
    let scope = resolve_scope(conn, parent)?;
    match kind {
        ObjectKind::Database => summaries(
            conn,
            "SELECT database_name, path, type, readonly FROM duckdb_databases() WHERE NOT internal ORDER BY database_name",
            |row| {
                let name: String = row.get(0)?;
                let path: Option<String> = row.get(1)?;
                let engine: Option<String> = row.get(2)?;
                let readonly: Option<bool> = row.get(3)?;
                let mut summary = ObjectSummary::new(ObjectKind::Database, name, None)
                    .with_detail(path.filter(|p| !p.is_empty()).unwrap_or_else(|| "in-memory".into()));
                summary = if readonly == Some(true) {
                    summary.with_badge("read-only")
                } else {
                    match engine.filter(|e| !e.is_empty()) {
                        Some(e) => summary.with_badge(e),
                        None => summary,
                    }
                };
                Ok(summary)
            },
        ),
        ObjectKind::Schema => summaries(
            conn,
            // DuckDB flags every `main` as internal (memory, system and temp all
            // have one), so `NOT internal` would hide the schema users work in.
            // Excluding the system-owned databases keeps `main` and drops the rest.
            &format!(
                "SELECT schema_name, database_name FROM duckdb_schemas() \
                 WHERE schema_name NOT IN {SYSTEM_SCHEMAS} AND database_name NOT IN ('system', 'temp') ORDER BY schema_name"
            ),
            |row| {
                let name: String = row.get(0)?;
                let database: String = row.get(1)?;
                Ok(ObjectSummary::new(ObjectKind::Schema, name, None).with_detail(database))
            },
        ),
        ObjectKind::Table => summaries(
            conn,
            &format!(
                "SELECT table_name, schema_name, estimated_size, column_count, temporary FROM duckdb_tables() \
                 WHERE {} AND NOT internal ORDER BY schema_name, table_name",
                scope.predicate(Some("table_name"))
            ),
            table_summary,
        ),
        ObjectKind::View => summaries(
            conn,
            &format!(
                "SELECT view_name, schema_name, column_count, temporary FROM duckdb_views() \
                 WHERE {} AND NOT internal ORDER BY schema_name, view_name",
                scope.predicate(None)
            ),
            |row| {
                let name: String = row.get(0)?;
                let schema: String = row.get(1)?;
                let columns: Option<i64> = row.get(2)?;
                let temporary: Option<bool> = row.get(3)?;
                let mut summary = ObjectSummary::new(ObjectKind::View, name, Some(schema));
                if let Some(n) = columns {
                    summary = summary.with_detail(format!("{n} columns"));
                }
                if temporary == Some(true) {
                    summary = summary.with_badge("temporary");
                }
                Ok(summary)
            },
        ),
        ObjectKind::Sequence => summaries(
            conn,
            &format!(
                "SELECT sequence_name, schema_name, start_value, increment_by, last_value, cycle FROM duckdb_sequences() \
                 WHERE {} ORDER BY schema_name, sequence_name",
                scope.predicate(None)
            ),
            |row| {
                let name: String = row.get(0)?;
                let schema: String = row.get(1)?;
                let start: Option<i64> = row.get(2)?;
                let step: Option<i64> = row.get(3)?;
                let last: Option<i64> = row.get(4)?;
                let cycle: Option<bool> = row.get(5)?;
                let detail = format!(
                    "start {} · step {} · last {}",
                    start.unwrap_or(1),
                    step.unwrap_or(1),
                    last.map(|v| v.to_string()).unwrap_or_else(|| "—".into())
                );
                let mut summary = ObjectSummary::new(ObjectKind::Sequence, name, Some(schema)).with_detail(detail);
                if cycle == Some(true) {
                    summary = summary.with_badge("cycle");
                }
                Ok(summary)
            },
        ),
        ObjectKind::Type => summaries(
            conn,
            &format!(
                "SELECT type_name, schema_name, logical_type, type_category, labels FROM duckdb_types() \
                 WHERE {} AND NOT internal ORDER BY schema_name, type_name",
                scope.predicate(None)
            ),
            |row| {
                let name: String = row.get(0)?;
                let schema: String = row.get(1)?;
                // `logical_type` is the type id (ENUM, STRUCT…); `type_category`
                // is NULL for user-defined enums, so the id is the better badge.
                let logical: Option<String> = row.get(2)?;
                let category: Option<String> = row.get(3)?;
                let labels = list_text(decode_cell(row.get_ref(4)?));
                let mut summary = ObjectSummary::new(ObjectKind::Type, name, Some(schema));
                let detail = if labels.is_empty() { category.unwrap_or_default() } else { labels.join(", ") };
                if !detail.is_empty() {
                    summary = summary.with_detail(detail);
                }
                if let Some(l) = logical.filter(|l| !l.is_empty()) {
                    summary = summary.with_badge(l.to_ascii_lowercase());
                }
                Ok(summary)
            },
        ),
        ObjectKind::Function | ObjectKind::Macro => {
            let is_macro = kind == ObjectKind::Macro;
            summaries(
                conn,
                &format!(
                    "SELECT function_name, schema_name, function_type, return_type, parameters FROM duckdb_functions() \
                     WHERE {} AND NOT internal AND function_type {} '%macro%' ORDER BY schema_name, function_name",
                    scope.predicate(None),
                    if is_macro { "LIKE" } else { "NOT LIKE" }
                ),
                move |row| {
                    let name: String = row.get(0)?;
                    let schema: String = row.get(1)?;
                    let function_type: Option<String> = row.get(2)?;
                    let return_type: Option<String> = row.get(3)?;
                    let parameters = list_text(decode_cell(row.get_ref(4)?));
                    let mut summary = ObjectSummary::new(if is_macro { ObjectKind::Macro } else { ObjectKind::Function }, &name, Some(schema))
                        .with_detail(signature(&name, &parameters, return_type.as_deref()));
                    if let Some(t) = function_type.filter(|t| !t.is_empty()) {
                        summary = summary.with_badge(t);
                    }
                    Ok(summary)
                },
            )
        }
        ObjectKind::Index => summaries(
            conn,
            &format!(
                "SELECT index_name, table_name, is_unique, is_primary FROM duckdb_indexes() WHERE {} ORDER BY table_name, index_name",
                scope.predicate(Some("table_name"))
            ),
            index_summary,
        ),
        ObjectKind::Constraint => summaries(
            conn,
            &format!(
                "SELECT table_name, constraint_type, constraint_text, constraint_name, constraint_index FROM duckdb_constraints() \
                 WHERE {} ORDER BY table_name, constraint_index",
                scope.predicate(Some("table_name"))
            ),
            constraint_summary,
        ),
        ObjectKind::Extension => summaries(
            conn,
            "SELECT extension_name, loaded, installed, extension_version, description FROM duckdb_extensions() ORDER BY extension_name",
            |row| {
                let name: String = row.get(0)?;
                let loaded: Option<bool> = row.get(1)?;
                let installed: Option<bool> = row.get(2)?;
                let version: Option<String> = row.get(3)?;
                let description: Option<String> = row.get(4)?;
                let badge = match (loaded, installed) {
                    (Some(true), _) => "loaded",
                    (_, Some(true)) => "installed",
                    _ => "available",
                };
                let mut detail = description.unwrap_or_default();
                if let Some(v) = version.filter(|v| !v.is_empty()) {
                    detail = if detail.is_empty() { v } else { format!("{detail} ({v})") };
                }
                Ok(ObjectSummary::new(ObjectKind::Extension, name, None).with_detail(detail).with_badge(badge))
            },
        ),
        ObjectKind::Setting => summaries(
            conn,
            "SELECT name, value, input_type, scope FROM duckdb_settings() ORDER BY name",
            |row| {
                let name: String = row.get(0)?;
                let value: Option<String> = row.get(1)?;
                let input_type: Option<String> = row.get(2)?;
                let scope: Option<String> = row.get(3)?;
                let mut summary = ObjectSummary::new(ObjectKind::Setting, name, None).with_detail(value.unwrap_or_default());
                let badge = scope.filter(|s| !s.is_empty()).or(input_type).unwrap_or_default();
                if !badge.is_empty() {
                    summary = summary.with_badge(badge.to_ascii_lowercase());
                }
                Ok(summary)
            },
        ),
        _ => Ok(Vec::new()),
    }
}

fn not_found(reference: &ObjectRef) -> AppError {
    AppError::not_found(format!("{:?} \"{}\" was not found.", reference.kind, reference.name))
}

// WHAT:  The schema a detail request lands in: the hint when it is a schema, else
//        the one the catalog reports for that object.
fn locate(conn: &Connection, sql: &str, name: &str) -> AppResult<Option<String>> {
    let found: Option<String> = conn.query_row(sql, params![name], |row| row.get(0)).ok();
    Ok(found)
}

fn table_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    // Read the catalog row, then let the statement go: the children below query
    // the same connection.
    let (schema, database, sql, estimated, columns, indexes, temporary, has_pk) = {
        let mut stmt = conn.prepare(
            "SELECT schema_name, database_name, sql, estimated_size, column_count, index_count, temporary, has_primary_key \
             FROM duckdb_tables() WHERE table_name = ? ORDER BY schema_name LIMIT 1",
        )?;
        let mut found = stmt.query(params![name])?;
        let Some(row) = found.next()? else { return Err(not_found(reference)) };
        (
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, Option<bool>>(6)?,
            row.get::<_, Option<bool>>(7)?,
        )
    };
    let target = qualified(&schema, name);
    let mut detail = ObjectDetail::empty(reference).property("Database", database).property("Schema", schema.clone());
    if let Some(text) = sql.filter(|s| !s.is_empty()) {
        detail = detail.definition(text, CodeLanguage::Sql);
    }
    if let Some(n) = estimated {
        detail = detail.property("Estimated rows", format_number(n as f64));
    }
    if let Some(n) = columns {
        detail = detail.property("Columns", n.to_string());
    }
    if let Some(n) = indexes {
        detail = detail.property("Indexes", n.to_string());
    }
    detail = detail
        .property("Temporary", temporary.unwrap_or(false).to_string())
        .property("Primary key", has_pk.unwrap_or(false).to_string());
    detail.columns = table_columns(conn, &TableRef { schema: Some(schema.clone()), name: name.clone() })?;
    let mut children = list_objects(conn, ObjectKind::Index, Some(name.as_str()))?;
    children.extend(list_objects(conn, ObjectKind::Constraint, Some(name.as_str()))?);
    detail.children = children;
    Ok(detail
        .action(ObjectAction::new("analyze", "Analyze", format!("ANALYZE {target};")))
        .action(ObjectAction::new("checkpoint", "Checkpoint", "CHECKPOINT;"))
        .action(ObjectAction::destructive("truncate", "Delete all rows", format!("DELETE FROM {target};")))
        .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {target};"))))
}

fn view_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let found: Option<(String, String, Option<String>)> = conn
        .query_row(
            "SELECT schema_name, database_name, sql FROM duckdb_views() WHERE view_name = ? ORDER BY schema_name LIMIT 1",
            params![name],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .ok();
    let Some((schema, database, sql)) = found else { return Err(not_found(reference)) };
    let target = qualified(&schema, name);
    let mut detail = ObjectDetail::empty(reference).property("Database", database).property("Schema", schema.clone());
    if let Some(text) = sql.filter(|s| !s.is_empty()) {
        detail = detail.definition(text, CodeLanguage::Sql);
    }
    detail.columns = table_columns(conn, &TableRef { schema: Some(schema), name: name.clone() }).unwrap_or_default();
    Ok(detail.action(ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {target};"))))
}

fn schema_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let database = locate(conn, "SELECT database_name FROM duckdb_schemas() WHERE schema_name = ? LIMIT 1", name)?
        .ok_or_else(|| not_found(reference))?;
    let mut detail = ObjectDetail::empty(reference).property("Database", database);
    let mut children = list_objects(conn, ObjectKind::Table, Some(name.as_str()))?;
    children.extend(list_objects(conn, ObjectKind::View, Some(name.as_str()))?);
    detail = detail.property("Tables", children.iter().filter(|c| c.reference.kind == ObjectKind::Table).count().to_string());
    detail.children = children;
    Ok(detail.action(ObjectAction::destructive("drop", "Drop schema", format!("DROP SCHEMA {} CASCADE;", quote_ident(name)))))
}

fn database_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let found: Option<(Option<String>, Option<String>, Option<bool>)> = conn
        .query_row("SELECT path, type, readonly FROM duckdb_databases() WHERE database_name = ? LIMIT 1", params![name], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .ok();
    let Some((path, engine, readonly)) = found else { return Err(not_found(reference)) };
    let mut detail = ObjectDetail::empty(reference)
        .property("Path", path.filter(|p| !p.is_empty()).unwrap_or_else(|| "in-memory".into()))
        .property("Type", engine.unwrap_or_default())
        .property("Read only", readonly.unwrap_or(false).to_string());
    let size: Option<(String, i64, i64, i64)> = conn
        .query_row(
            "SELECT database_size, block_size, total_blocks, used_blocks FROM pragma_database_size() WHERE database_name = ? LIMIT 1",
            params![name],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .ok();
    if let Some((total, block, blocks, used)) = size {
        detail = detail
            .property("Size", total)
            .property("Block size", format_number(block as f64))
            .property("Blocks", format_number(blocks as f64))
            .property("Used blocks", format_number(used as f64));
    }
    detail.children = summaries(
        conn,
        &format!(
            "SELECT schema_name, database_name FROM duckdb_schemas() WHERE database_name = {} AND schema_name NOT IN {SYSTEM_SCHEMAS} ORDER BY schema_name",
            quote_literal(name)
        ),
        |row| {
            let schema: String = row.get(0)?;
            let database: String = row.get(1)?;
            Ok(ObjectSummary::new(ObjectKind::Schema, schema, None).with_detail(database))
        },
    )?;
    detail = detail.action(ObjectAction::new("checkpoint", "Checkpoint", format!("CHECKPOINT {};", quote_ident(name))));
    let current: Option<String> = conn.query_row("SELECT current_database()", [], |row| row.get(0)).ok();
    if current.as_deref() != Some(name.as_str()) {
        detail = detail.action(ObjectAction::destructive("detach", "Detach database", format!("DETACH {};", quote_ident(name))));
    }
    Ok(detail)
}

fn sequence_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let mut stmt = conn.prepare(
        "SELECT schema_name, sql, start_value, min_value, max_value, increment_by, last_value, cycle \
         FROM duckdb_sequences() WHERE sequence_name = ? LIMIT 1",
    )?;
    let mut found = stmt.query(params![name])?;
    let Some(row) = found.next()? else { return Err(not_found(reference)) };
    let schema: String = row.get(0)?;
    let sql: Option<String> = row.get(1)?;
    let start: Option<i64> = row.get(2)?;
    let min: Option<i64> = row.get(3)?;
    let max: Option<i64> = row.get(4)?;
    let step: Option<i64> = row.get(5)?;
    let last: Option<i64> = row.get(6)?;
    let cycle: Option<bool> = row.get(7)?;
    let target = qualified(&schema, name);
    let mut detail = ObjectDetail::empty(reference).property("Schema", schema);
    if let Some(text) = sql.filter(|s| !s.is_empty()) {
        detail = detail.definition(text, CodeLanguage::Sql);
    }
    for (label, value) in [("Start", start), ("Minimum", min), ("Maximum", max), ("Increment", step), ("Last value", last)] {
        if let Some(v) = value {
            detail = detail.property(label, v.to_string());
        }
    }
    detail = detail.property("Cycle", cycle.unwrap_or(false).to_string());
    Ok(detail.action(ObjectAction::destructive("drop", "Drop sequence", format!("DROP SEQUENCE {target};"))))
}

fn type_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let mut stmt = conn.prepare(
        "SELECT schema_name, logical_type, type_category, type_size, labels FROM duckdb_types() WHERE type_name = ? AND NOT internal LIMIT 1",
    )?;
    let mut found = stmt.query(params![name])?;
    let Some(row) = found.next()? else { return Err(not_found(reference)) };
    let schema: String = row.get(0)?;
    let logical: Option<String> = row.get(1)?;
    let category: Option<String> = row.get(2)?;
    let size: Option<i64> = row.get(3)?;
    let labels = list_text(decode_cell(row.get_ref(4)?));
    let target = qualified(&schema, name);
    let mut detail = ObjectDetail::empty(reference).property("Schema", schema);
    // Only the type id survives in the catalog; an enum's body is its labels.
    let body = if labels.is_empty() {
        logical.clone().filter(|l| !l.is_empty())
    } else {
        Some(format!("ENUM ({})", labels.iter().map(|l| quote_literal(l)).collect::<Vec<_>>().join(", ")))
    };
    if let Some(text) = body {
        detail = detail.definition(format!("CREATE TYPE {} AS {text};", quote_ident(name)), CodeLanguage::Sql);
    }
    if let Some(l) = logical.filter(|l| !l.is_empty()) {
        detail = detail.property("Type", l);
    }
    if !labels.is_empty() {
        detail = detail.property("Labels", labels.join(", "));
    }
    if let Some(c) = category.filter(|c| !c.is_empty()) {
        detail = detail.property("Category", c);
    }
    if let Some(n) = size {
        detail = detail.property("Size", format_number(n as f64));
    }
    Ok(detail.action(ObjectAction::destructive("drop", "Drop type", format!("DROP TYPE {target};"))))
}

fn function_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let is_macro = reference.kind == ObjectKind::Macro;
    let mut stmt = conn.prepare(
        "SELECT schema_name, function_type, return_type, parameters, macro_definition, description, has_side_effects \
         FROM duckdb_functions() WHERE function_name = ? AND NOT internal LIMIT 1",
    )?;
    let mut rows = stmt.query(params![name])?;
    let Some(row) = rows.next()? else { return Err(not_found(reference)) };
    let schema: String = row.get(0)?;
    let function_type: Option<String> = row.get(1)?;
    let return_type: Option<String> = row.get(2)?;
    let parameters = list_text(decode_cell(row.get_ref(3)?));
    let macro_definition: Option<String> = row.get(4)?;
    let description: Option<String> = row.get(5)?;
    let side_effects: Option<bool> = row.get(6)?;
    let target = qualified(&schema, name);
    let mut detail = ObjectDetail::empty(reference)
        .property("Schema", schema)
        .property("Signature", signature(name, &parameters, return_type.as_deref()));
    if let Some(t) = function_type.filter(|t| !t.is_empty()) {
        detail = detail.property("Kind", t);
    }
    if let Some(d) = description.filter(|d| !d.is_empty()) {
        detail = detail.property("Description", d);
    }
    detail = detail.property("Side effects", side_effects.unwrap_or(false).to_string());
    if let Some(body) = macro_definition.filter(|b| !b.is_empty()) {
        detail = detail.definition(format!("CREATE OR REPLACE MACRO {target}({}) AS {body};", parameters.join(", ")), CodeLanguage::Sql);
    }
    if is_macro {
        detail = detail.action(ObjectAction::destructive("drop", "Drop macro", format!("DROP MACRO {target};")));
    }
    Ok(detail)
}

fn index_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let mut stmt = conn.prepare(
        "SELECT schema_name, table_name, is_unique, is_primary, expressions, sql FROM duckdb_indexes() WHERE index_name = ? LIMIT 1",
    )?;
    let mut found = stmt.query(params![name])?;
    let Some(row) = found.next()? else { return Err(not_found(reference)) };
    let schema: String = row.get(0)?;
    let table: String = row.get(1)?;
    let unique: Option<bool> = row.get(2)?;
    let primary: Option<bool> = row.get(3)?;
    let expressions: Option<String> = row.get(4)?;
    let sql: Option<String> = row.get(5)?;
    let target = qualified(&schema, name);
    let mut detail = ObjectDetail::empty(reference)
        .property("Schema", schema)
        .property("Table", table)
        .property("Unique", unique.unwrap_or(false).to_string())
        .property("Primary", primary.unwrap_or(false).to_string());
    if let Some(text) = expressions.filter(|e| !e.is_empty()) {
        detail = detail.property("Expressions", text);
    }
    if let Some(text) = sql.filter(|s| !s.is_empty()) {
        detail = detail.definition(text, CodeLanguage::Sql);
    }
    Ok(detail.action(ObjectAction::destructive("drop", "Drop index", format!("DROP INDEX {target};"))))
}

fn constraint_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let table = reference.parent.clone().unwrap_or_default();
    let mut stmt = conn.prepare(
        "SELECT schema_name, table_name, constraint_type, constraint_text, constraint_column_names, constraint_name, constraint_index, \
         referenced_table, referenced_column_names FROM duckdb_constraints() WHERE table_name = ? OR constraint_name = ?",
    )?;
    let mut rows = stmt.query(params![table, reference.name])?;
    while let Some(row) = rows.next()? {
        let schema: String = row.get(0)?;
        let owner: String = row.get(1)?;
        let kind: String = row.get(2)?;
        let text: Option<String> = row.get(3)?;
        let columns = list_text(decode_cell(row.get_ref(4)?));
        let raw_name: Option<String> = row.get(5)?;
        let index: i64 = row.get(6)?;
        if constraint_name(raw_name, &owner, &kind, index) != reference.name {
            continue;
        }
        let referenced: Option<String> = row.get(7)?;
        let referenced_columns = list_text(decode_cell(row.get_ref(8)?));
        let mut detail = ObjectDetail::empty(reference)
            .property("Schema", schema)
            .property("Table", owner)
            .property("Type", kind)
            .property("Columns", columns.join(", "));
        if let Some(text) = text.filter(|t| !t.is_empty()) {
            detail = detail.definition(text, CodeLanguage::Sql);
        }
        if let Some(target) = referenced.filter(|t| !t.is_empty()) {
            detail = detail.property("References", format!("{target}({})", referenced_columns.join(", ")));
        }
        return Ok(detail);
    }
    Err(not_found(reference))
}

fn extension_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let mut stmt = conn.prepare(
        "SELECT loaded, installed, extension_version, install_mode, install_path, description \
         FROM duckdb_extensions() WHERE extension_name = ? LIMIT 1",
    )?;
    let mut found = stmt.query(params![name])?;
    let Some(row) = found.next()? else { return Err(not_found(reference)) };
    let loaded: Option<bool> = row.get(0)?;
    let installed: Option<bool> = row.get(1)?;
    let version: Option<String> = row.get(2)?;
    let mode: Option<String> = row.get(3)?;
    let path: Option<String> = row.get(4)?;
    let description: Option<String> = row.get(5)?;
    let literal = quote_literal(name);
    let mut detail = ObjectDetail::empty(reference)
        .property("Loaded", loaded.unwrap_or(false).to_string())
        .property("Installed", installed.unwrap_or(false).to_string());
    for (label, value) in [("Version", version), ("Install mode", mode), ("Install path", path), ("Description", description)] {
        if let Some(v) = value.filter(|v| !v.is_empty()) {
            detail = detail.property(label, v);
        }
    }
    Ok(detail
        .definition(format!("INSTALL {literal};\nLOAD {literal};"), CodeLanguage::Sql)
        .action(ObjectAction::new("install", "Install", format!("INSTALL {literal};")))
        .action(ObjectAction::new("load", "Load", format!("LOAD {literal};")))
        .action(ObjectAction::new("update", "Force install (update)", format!("FORCE INSTALL {literal};"))))
}

/// value, description, input_type, scope — every column of `duckdb_settings()` is nullable.
type SettingRow = (Option<String>, Option<String>, Option<String>, Option<String>);

fn setting_detail(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    let name = &reference.name;
    let found: Option<SettingRow> = conn
        .query_row("SELECT value, description, input_type, scope FROM duckdb_settings() WHERE name = ? LIMIT 1", params![name], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .ok();
    let Some((value, description, input_type, scope)) = found else { return Err(not_found(reference)) };
    let current = value.unwrap_or_default();
    let mut detail = ObjectDetail::empty(reference)
        .definition(set_statement(name, &current), CodeLanguage::Sql)
        .property("Value", current.clone());
    for (label, text) in [("Description", description), ("Input type", input_type.clone()), ("Scope", scope)] {
        if let Some(t) = text.filter(|t| !t.is_empty()) {
            detail = detail.property(label, t);
        }
    }
    if input_type.as_deref().is_some_and(|t| t.eq_ignore_ascii_case("BOOLEAN")) {
        for option in ["true", "false"] {
            if !current.eq_ignore_ascii_case(option) {
                detail = detail.action(ObjectAction::destructive(&format!("set-{option}"), &format!("Set {name} = {option}"), set_statement(name, option)));
            }
        }
    } else {
        detail = detail.action(ObjectAction::destructive("set", &format!("Set {name}"), set_statement(name, &current)));
    }
    Ok(detail.action(ObjectAction::destructive("reset", &format!("Reset {name}"), format!("RESET {name};"))))
}

fn describe_object(conn: &Connection, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    match reference.kind {
        ObjectKind::Database => database_detail(conn, reference),
        ObjectKind::Schema => schema_detail(conn, reference),
        ObjectKind::Table => table_detail(conn, reference),
        ObjectKind::View => view_detail(conn, reference),
        ObjectKind::Sequence => sequence_detail(conn, reference),
        ObjectKind::Type => type_detail(conn, reference),
        ObjectKind::Function | ObjectKind::Macro => function_detail(conn, reference),
        ObjectKind::Index => index_detail(conn, reference),
        ObjectKind::Constraint => constraint_detail(conn, reference),
        ObjectKind::Extension => extension_detail(conn, reference),
        ObjectKind::Setting => setting_detail(conn, reference),
        _ => Ok(ObjectDetail::empty(reference)),
    }
}

// WHAT:  One `SELECT count(*)` over a catalog function, 0 when it is unavailable.
fn catalog_count(conn: &Connection, function: &str, predicate: &str) -> f64 {
    conn.query_row(&format!("SELECT count(*) FROM {function} WHERE {predicate}"), [], |row| row.get::<_, i64>(0))
        .map(|n| n as f64)
        .unwrap_or(0.0)
}

fn collect_stats(conn: &Connection, database: &str) -> AppResult<ServerStats> {
    let version: String = conn.query_row("SELECT version()", [], |row| row.get(0))?;
    let mut server = vec![Stat::text("DuckDB version", version), Stat::text("Database", database.to_string())];
    if let Ok(threads) = conn.query_row("SELECT current_setting('threads')", [], |row| row.get::<_, i64>(0)) {
        server.push(Stat::number("Threads", threads as f64, None));
    }
    for setting in ["memory_limit", "max_memory", "temp_directory", "access_mode"] {
        if let Ok(value) = conn.query_row(&format!("SELECT current_setting('{setting}')"), [], |row| row.get::<_, String>(0)) {
            if !value.is_empty() {
                server.push(Stat::text(setting, value));
            }
        }
    }

    let mut storage = Vec::new();
    let mut memory = Vec::new();
    // pragma_database_size() is unavailable on some builds / attachments: skip it then.
    if let Ok(mut stmt) = conn.prepare(
        "SELECT database_size, block_size, total_blocks, used_blocks, free_blocks, wal_size, memory_usage, memory_limit \
         FROM pragma_database_size() WHERE database_name = ? LIMIT 1",
    ) {
        if let Ok(mut sized) = stmt.query(params![database]) {
            if let Ok(Some(row)) = sized.next() {
                let text = |index: usize| row.get::<_, String>(index).unwrap_or_default();
                let number = |index: usize| row.get::<_, i64>(index).unwrap_or_default() as f64;
                storage.push(Stat::text("Database size", text(0)));
                storage.push(Stat::number("Block size", number(1), Some("bytes")));
                storage.push(Stat::number("Total blocks", number(2), None));
                storage.push(Stat::number("Used blocks", number(3), None));
                storage.push(Stat::number("Free blocks", number(4), None));
                storage.push(Stat::text("WAL size", text(5)));
                memory.push(Stat::text("Memory usage", text(6)));
                memory.push(Stat::text("Memory limit", text(7)));
            }
        }
    }

    let mut stmt = conn.prepare("SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory() ORDER BY memory_usage_bytes DESC")?;
    let mut rows = stmt.query([])?;
    let mut total_bytes = 0.0;
    let mut temporary_bytes = 0.0;
    let mut tags: Vec<Stat> = Vec::new();
    while let Some(row) = rows.next()? {
        let tag: String = row.get(0)?;
        let used: i64 = row.get(1)?;
        let temporary: i64 = row.get(2)?;
        total_bytes += used as f64;
        temporary_bytes += temporary as f64;
        if used > 0 && tags.len() < 12 {
            tags.push(Stat::number(&tag, used as f64, Some("bytes")));
        }
    }
    memory.push(Stat::number("Buffer manager", total_bytes, Some("bytes")).with_hint("Sum of duckdb_memory() tags"));
    memory.push(Stat::number("Temporary storage", temporary_bytes, Some("bytes")));
    memory.extend(tags);

    let schema = vec![
        Stat::number("Schemas", catalog_count(conn, "duckdb_schemas()", &format!("schema_name NOT IN {SYSTEM_SCHEMAS} AND NOT internal")), None),
        Stat::number("Tables", catalog_count(conn, "duckdb_tables()", "NOT internal"), None),
        Stat::number("Views", catalog_count(conn, "duckdb_views()", "NOT internal"), None),
        Stat::number("Indexes", catalog_count(conn, "duckdb_indexes()", "TRUE"), None),
        Stat::number("Sequences", catalog_count(conn, "duckdb_sequences()", "TRUE"), None),
        Stat::number("Constraints", catalog_count(conn, "duckdb_constraints()", "TRUE"), None),
        Stat::number("Macros", catalog_count(conn, "duckdb_functions()", "NOT internal AND function_type LIKE '%macro%'"), None),
        Stat::number("Extensions loaded", catalog_count(conn, "duckdb_extensions()", "loaded"), None),
    ];

    Ok(ServerStats::now(vec![
        StatGroup { title: "Server".into(), stats: server },
        StatGroup { title: "Storage".into(), stats: storage },
        StatGroup { title: "Memory".into(), stats: memory },
        StatGroup { title: "Schema".into(), stats: schema },
    ]))
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { transactions: true, exact_estimate: false, namespaces: true, ..Capabilities::SQL },
        object_kinds: vec![K::Database, K::Schema, K::Table, K::View, K::Sequence, K::Type, K::Function, K::Macro, K::Index, K::Constraint, K::Extension, K::Setting],
        tools: vec![T::Stats, T::Erd],
    }
}

#[async_trait]
impl Integration for DuckdbIntegration {
    fn engine(&self) -> Engine {
        Engine::Duckdb
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        self.blocking(|conn| {
            conn.query_row("SELECT 1", [], |_| Ok(()))?;
            Ok(())
        })
        .await
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        self.blocking(|conn| {
            let v: String = conn.query_row("SELECT version()", [], |row| row.get(0))?;
            Ok(Some(format!("DuckDB {v}")))
        })
        .await
    }

    fn current_database(&self) -> Option<String> {
        Some(self.file_name.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        self.blocking(|conn| {
            let mut stmt = conn.prepare("SELECT database_name FROM duckdb_databases() WHERE NOT internal ORDER BY database_name")?;
            let names = stmt.query_map([], |row| row.get::<_, String>(0))?.collect::<duckdb::Result<Vec<_>>>()?;
            Ok(names)
        })
        .await
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        self.blocking(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT t.table_schema, t.table_name, t.table_type, \
                 (SELECT estimated_size FROM duckdb_tables() d WHERE d.schema_name = t.table_schema AND d.table_name = t.table_name AND NOT d.internal LIMIT 1) \
                 FROM information_schema.tables t \
                 WHERE t.table_schema NOT IN {SYSTEM_SCHEMAS} ORDER BY t.table_schema, t.table_name"
            ))?;
            let rows = stmt
                .query_map([], |row| {
                    let schema: String = row.get(0)?;
                    let name: String = row.get(1)?;
                    let kind: String = row.get(2)?;
                    let estimate: Option<i64> = row.get(3)?;
                    Ok((schema, name, kind, estimate))
                })?
                .collect::<duckdb::Result<Vec<_>>>()?;
            let mut schemas: Vec<SchemaInfo> = Vec::new();
            for (schema, name, kind, estimate) in rows {
                let is_view = kind.to_ascii_uppercase().contains("VIEW");
                let info = TableInfo {
                    schema: Some(schema.clone()),
                    name,
                    kind: if is_view { TableKind::View } else { TableKind::Table },
                    row_estimate: if is_view { None } else { estimate },
                };
                match schemas.iter_mut().find(|s| s.name == schema) {
                    Some(existing) => existing.tables.push(info),
                    None => schemas.push(SchemaInfo { name: schema, tables: vec![info] }),
                }
            }
            if schemas.is_empty() {
                schemas.push(SchemaInfo { name: "main".to_string(), tables: Vec::new() });
            }
            Ok(SchemaCatalog { schemas })
        })
        .await
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let table = table.clone();
        self.blocking(move |conn| table_columns(conn, &table)).await
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let table = table.clone();
        self.blocking(move |conn| {
            let schema = schema_of(&table);
            let estimate: Option<i64> = conn
                .query_row(
                    "SELECT estimated_size FROM duckdb_tables() WHERE schema_name = ? AND table_name = ? LIMIT 1",
                    params![schema, table.name],
                    |row| row.get(0),
                )
                .ok();
            if let Some(n) = estimate {
                return Ok(Some(n));
            }
            let sql = format!("SELECT count(*) FROM {}", qualified_name_for(Engine::Duckdb, &table));
            let n: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
            Ok(Some(n))
        })
        .await
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!(
            "SELECT count(*) FROM {}{}",
            qualified_name_for(Engine::Duckdb, table),
            where_clause(Engine::Duckdb, filters)
        );
        self.scalar_i64(sql).await
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            qualified_name_for(Engine::Duckdb, table),
            where_clause(Engine::Duckdb, &query.filters),
            order_clause(Engine::Duckdb, &query.sort),
            query.limit,
            query.offset
        );
        let max_rows = query.limit as usize;
        match self.blocking(move |conn| run_statement(conn, &sql, max_rows)).await? {
            StatementResult::Rows { result } => Ok(result),
            StatementResult::Affected { .. } => Ok(ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }),
        }
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let sql = sql.to_string();
        self.blocking(move |conn| run_script(conn, &sql, max_rows)).await
    }

    async fn close(&self) {}

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        self.blocking(|conn| {
            let mut stmt = conn.prepare(
                "SELECT schema_name, table_name, constraint_name, constraint_column_names, \
                 referenced_table, referenced_column_names \
                 FROM duckdb_constraints() WHERE constraint_type = 'FOREIGN KEY' \
                 ORDER BY schema_name, table_name, constraint_index",
            )?;
            let mut rows = stmt.query([])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                let schema: String = row.get(0)?;
                let from_table: String = row.get(1)?;
                let name: Option<String> = row.get(2)?;
                let from_columns = list_text(decode_cell(row.get_ref(3)?));
                let to_table: String = row.get(4)?;
                let to_columns = list_text(decode_cell(row.get_ref(5)?));
                out.push(ForeignKey {
                    name: name.unwrap_or_else(|| format!("{from_table}_{to_table}_fkey")),
                    from_schema: Some(schema.clone()),
                    from_table,
                    from_columns,
                    to_schema: Some(schema),
                    to_table,
                    to_columns,
                });
            }
            Ok(out)
        })
        .await
    }

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let table = table.clone();
        self.blocking(move |conn| {
            let schema = schema_of(&table);
            let from_tables: Option<String> = conn
                .query_row(
                    "SELECT sql FROM duckdb_tables() WHERE schema_name = ? AND table_name = ? LIMIT 1",
                    params![schema, table.name],
                    |row| row.get(0),
                )
                .ok();
            if from_tables.is_some() {
                return Ok(from_tables);
            }
            let from_views: Option<String> = conn
                .query_row(
                    "SELECT sql FROM duckdb_views() WHERE schema_name = ? AND view_name = ? LIMIT 1",
                    params![schema, table.name],
                    |row| row.get(0),
                )
                .ok();
            Ok(from_views)
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
        self.blocking(|conn| {
            let database: String = conn.query_row("SELECT current_database()", [], |row| row.get(0))?;
            collect_stats(conn, &database)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, FilterOp, SortRule, SslMode};

    fn resolved(path: &str, read_only: bool) -> ResolvedConnection {
        ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Duckdb,
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

    fn temp_file(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir()
            .join(format!("dbfree-duckdb-{tag}-{}-{nanos}.duckdb", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    fn rows_of(results: &[StatementResult]) -> &ResultSet {
        match results.last() {
            Some(StatementResult::Rows { result }) => result,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn temporal_formatting() {
        assert_eq!(date_text(0), "1970-01-01");
        assert_eq!(date_text(19_723), "2024-01-01");
        assert_eq!(timestamp_text(TimeUnit::Microsecond, 1_704_067_200_000_000), "2024-01-01 00:00:00");
        assert_eq!(timestamp_text(TimeUnit::Millisecond, -1), "1969-12-31 23:59:59.999");
        assert_eq!(time_text(TimeUnit::Microsecond, 3_661_000_000), "01:01:01");
        assert_eq!(i128_value(5), Value::Int(5));
        assert_eq!(i128_value(i128::from(i64::MAX) + 1), Value::Decimal("9223372036854775808".into()));
    }

    #[test]
    fn dml_detection() {
        assert!(is_dml_without_returning("INSERT INTO t VALUES (1)"));
        assert!(is_dml_without_returning("  create table t(a int)"));
        assert!(!is_dml_without_returning("INSERT INTO t VALUES (1) RETURNING *"));
        assert!(!is_dml_without_returning("SELECT 1"));
        assert!(!is_dml_without_returning("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(is_memory_path(":memory:"));
        assert!(!is_memory_path("/tmp/a.duckdb"));
        assert_eq!(display_name("/tmp/a.duckdb"), "a.duckdb");
    }

    #[tokio::test]
    async fn round_trip_on_temp_file() {
        let path = temp_file("rt");
        let db = connect(&resolved(&path, false)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        db.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert_eq!(db.engine(), Engine::Duckdb);
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("DuckDB"), "{version}");

        let script = "CREATE TABLE people (id INTEGER PRIMARY KEY, name VARCHAR, score DOUBLE, born DATE, tags VARCHAR[], meta STRUCT(a INTEGER, b VARCHAR), big HUGEINT, dec DECIMAL(10,2), blob BLOB);\
                      INSERT INTO people VALUES (1, 'Ada', 9.5, DATE '1815-12-10', ['math','code'], {'a': 1, 'b': 'x'}, 170141183460469231731687303715884105727, 12.34, '\\x00\\x01'::BLOB);\
                      INSERT INTO people VALUES (2, 'Linus', 8.0, DATE '1969-12-28', [], NULL, 5, NULL, NULL);\
                      INSERT INTO people VALUES (3, 'Grace', 9.9, DATE '1906-12-09', ['cobol'], {'a': 3, 'b': 'z'}, -1, 0.5, NULL);\
                      CREATE VIEW top AS SELECT id, name FROM people WHERE score > 9;";
        let results = db.execute(script, 100).await.unwrap_or_else(|e| panic!("execute: {e}"));
        assert_eq!(results.len(), 5);
        assert!(matches!(results[1], StatementResult::Affected { rows_affected: 1 }), "{:?}", results[1]);

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let main = catalog.schemas.iter().find(|s| s.name == "main").unwrap_or_else(|| panic!("no main schema: {catalog:?}"));
        let people = main.tables.iter().find(|t| t.name == "people").unwrap_or_else(|| panic!("no people table"));
        assert_eq!(people.kind, TableKind::Table);
        let top = main.tables.iter().find(|t| t.name == "top").unwrap_or_else(|| panic!("no top view"));
        assert_eq!(top.kind, TableKind::View);

        let table = TableRef { schema: Some("main".into()), name: "people".into() };
        let columns = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(columns.len(), 9);
        assert!(columns[0].primary_key && columns[0].name == "id");
        assert!(!columns[1].primary_key);
        assert_eq!(columns[1].data_type, "varchar");

        assert_eq!(db.row_estimate(&table).await.unwrap_or_default(), Some(3));
        assert_eq!(db.count(&table, &[]).await.unwrap_or_default(), 3);
        let filter = FilterRule { column: "name".into(), op: FilterOp::Contains, value: "a".into() };
        assert_eq!(db.count(&table, std::slice::from_ref(&filter)).await.unwrap_or_default(), 2);

        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![SortRule { column: "score".into(), desc: true }], filters: vec![filter], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("fetch_page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.rows[0][1], Value::Text("Grace".into()));
        assert_eq!(page.rows[1][1], Value::Text("Ada".into()));
        assert_eq!(page.columns[3].type_name, "date");

        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![SortRule { column: "id".into(), desc: false }], filters: vec![], offset: 1, limit: 1 })
            .await
            .unwrap_or_else(|e| panic!("fetch_page: {e}"));
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0][0], Value::Int(2));

        let all = db.execute("SELECT * FROM people ORDER BY id", 100).await.unwrap_or_else(|e| panic!("select: {e}"));
        let rs = rows_of(&all);
        let ada = &rs.rows[0];
        assert_eq!(ada[2], Value::Float(9.5));
        assert_eq!(ada[3], Value::DateTime("1815-12-10".into()));
        assert_eq!(ada[4], Value::Json(serde_json::json!(["math", "code"])));
        assert_eq!(ada[5], Value::Json(serde_json::json!({"a": 1, "b": "x"})));
        assert_eq!(ada[6], Value::Decimal("170141183460469231731687303715884105727".into()));
        assert_eq!(ada[7], Value::Decimal("12.34".into()));
        assert_eq!(ada[8], Value::Bytes("AAE=".into()));
        assert_eq!(rs.rows[1][5], Value::Null);
        assert_eq!(rs.rows[1][6], Value::Int(5));

        let truncated = db.execute("SELECT * FROM people", 2).await.unwrap_or_else(|e| panic!("select: {e}"));
        assert!(rows_of(&truncated).truncated);

        let ddl = db.ddl(&table).await.unwrap_or_default().unwrap_or_default();
        assert!(ddl.to_ascii_uppercase().contains("CREATE TABLE"), "{ddl}");
        let ddl = db.ddl(&TableRef { schema: Some("main".into()), name: "top".into() }).await.unwrap_or_default().unwrap_or_default();
        assert!(ddl.to_ascii_uppercase().contains("CREATE VIEW"), "{ddl}");

        db.execute("CREATE TABLE orders (id INTEGER, person_id INTEGER REFERENCES people(id))", 10)
            .await
            .unwrap_or_else(|e| panic!("fk: {e}"));
        let fks = db.foreign_keys().await.unwrap_or_default();
        assert_eq!(fks.len(), 1, "{fks:?}");
        assert_eq!(fks[0].from_table, "orders");
        assert_eq!(fks[0].to_table, "people");
        assert_eq!(fks[0].from_columns, vec!["person_id".to_string()]);
        assert_eq!(fks[0].to_columns, vec!["id".to_string()]);

        let dbs = db.databases().await.unwrap_or_default();
        assert!(!dbs.is_empty());
        db.close().await;
        drop(db);

        let ro = connect(&resolved(&path, true)).await.unwrap_or_else(|e| panic!("connect ro: {e}"));
        assert!(ro.execute("INSERT INTO people VALUES (9, 'x', 1, NULL, NULL, NULL, NULL, NULL, NULL)", 1).await.is_err());
        assert_eq!(ro.count(&table, &[]).await.unwrap_or_default(), 3);
        drop(ro);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.wal"));
    }

    #[test]
    fn catalog_text_helpers() {
        assert_eq!(list_text(Value::Json(serde_json::json!(["a", "b"]))), vec!["a".to_string(), "b".to_string()]);
        assert_eq!(list_text(Value::Text("solo".into())), vec!["solo".to_string()]);
        assert!(list_text(Value::Null).is_empty());
        assert_eq!(signature("f", &["a INTEGER".into(), "b VARCHAR".into()], Some("BIGINT")), "f(a INTEGER, b VARCHAR) → BIGINT");
        assert_eq!(signature("f", &[], None), "f()");
        assert_eq!(signature("f", &[], Some("")), "f()");
        assert_eq!(constraint_name(Some("pk_users".into()), "users", "PRIMARY KEY", 0), "pk_users");
        assert_eq!(constraint_name(None, "users", "PRIMARY KEY", 2), "users_primary_key_2");
        assert_eq!(constraint_name(Some(String::new()), "t", "CHECK", 1), "t_check_1");
        assert_eq!(set_statement("threads", "4"), "SET threads = 4;");
        assert_eq!(set_statement("enable_progress_bar", "true"), "SET enable_progress_bar = true;");
        assert_eq!(set_statement("memory_limit", "1.0 GiB"), "SET memory_limit = '1.0 GiB';");
        assert_eq!(set_statement("x", "it's"), "SET x = 'it''s';");
        assert_eq!(Scope::UserSchemas.predicate(Some("table_name")), "schema_name NOT IN ('information_schema', 'pg_catalog')");
        assert_eq!(Scope::Schema("main".into()).predicate(None), "schema_name = 'main'");
        assert_eq!(Scope::Table("users".into()).predicate(Some("table_name")), "table_name = 'users'");
        assert_eq!(Scope::Table("users".into()).predicate(None), "schema_name NOT IN ('information_schema', 'pg_catalog')");
    }

    #[tokio::test]
    async fn explorer_lists_and_describes_objects() {
        let db = connect(&resolved(":memory:", false)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR UNIQUE, name VARCHAR);\
             CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id), total DOUBLE CHECK (total >= 0));\
             INSERT INTO users VALUES (1, 'a@x', 'ann'), (2, 'b@x', 'bob');\
             CREATE INDEX orders_user ON orders(user_id);\
             CREATE VIEW big_orders AS SELECT * FROM orders WHERE total > 100;\
             CREATE SEQUENCE order_seq START 5 INCREMENT 2;\
             CREATE TYPE mood AS ENUM ('ok', 'great');\
             CREATE MACRO add_one(a) AS a + 1;\
             CREATE SCHEMA reporting;",
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("setup: {e}"));

        let names = |items: &[ObjectSummary]| items.iter().map(|o| o.reference.name.clone()).collect::<Vec<_>>();

        let databases = db.objects(ObjectKind::Database, None).await.unwrap_or_else(|e| panic!("databases: {e}"));
        assert!(!databases.is_empty(), "at least the attached memory database");
        assert!(databases.iter().all(|d| d.detail.is_some()));

        let schemas = db.objects(ObjectKind::Schema, None).await.unwrap_or_else(|e| panic!("schemas: {e}"));
        assert!(names(&schemas).contains(&"main".to_string()), "{:?}", names(&schemas));
        assert!(names(&schemas).contains(&"reporting".to_string()));

        let tables = db.objects(ObjectKind::Table, Some("main")).await.unwrap_or_else(|e| panic!("tables: {e}"));
        assert_eq!(names(&tables), vec!["orders", "users"]);
        let users = tables.iter().find(|t| t.reference.name == "users").unwrap_or_else(|| panic!("users"));
        assert_eq!(users.reference.parent.as_deref(), Some("main"));
        assert!(users.detail.as_deref().is_some_and(|d| d.contains("3 columns")), "{:?}", users.detail);

        assert_eq!(names(&db.objects(ObjectKind::View, None).await.unwrap_or_default()), vec!["big_orders"]);

        let sequences = db.objects(ObjectKind::Sequence, None).await.unwrap_or_else(|e| panic!("sequences: {e}"));
        assert_eq!(names(&sequences), vec!["order_seq"]);
        assert!(sequences[0].detail.as_deref().is_some_and(|d| d.contains("start 5") && d.contains("step 2")), "{:?}", sequences[0].detail);

        let types = db.objects(ObjectKind::Type, None).await.unwrap_or_else(|e| panic!("types: {e}"));
        assert_eq!(names(&types), vec!["mood"]);
        assert_eq!(types[0].badge.as_deref(), Some("enum"));
        assert_eq!(types[0].detail.as_deref(), Some("ok, great"));

        let macros = db.objects(ObjectKind::Macro, None).await.unwrap_or_else(|e| panic!("macros: {e}"));
        assert_eq!(names(&macros), vec!["add_one"]);
        assert!(macros[0].detail.as_deref().is_some_and(|d| d.starts_with("add_one(")), "{:?}", macros[0].detail);
        let functions = db.objects(ObjectKind::Function, None).await.unwrap_or_else(|e| panic!("functions: {e}"));
        assert!(!names(&functions).contains(&"add_one".to_string()), "macros are their own kind");

        let indexes = db.objects(ObjectKind::Index, None).await.unwrap_or_else(|e| panic!("indexes: {e}"));
        let orders_user = indexes.iter().find(|i| i.reference.name == "orders_user").unwrap_or_else(|| panic!("orders_user"));
        assert_eq!(orders_user.reference.parent.as_deref(), Some("orders"));
        assert_eq!(names(&db.objects(ObjectKind::Index, Some("orders")).await.unwrap_or_default()), vec!["orders_user"]);

        let constraints = db.objects(ObjectKind::Constraint, Some("orders")).await.unwrap_or_else(|e| panic!("constraints: {e}"));
        assert!(!constraints.is_empty(), "orders has a CHECK and a FOREIGN KEY");
        assert!(constraints.iter().all(|c| c.reference.parent.as_deref() == Some("orders")));
        assert!(constraints.iter().any(|c| c.badge.as_deref() == Some("check")), "{:?}", constraints);

        let extensions = db.objects(ObjectKind::Extension, None).await.unwrap_or_else(|e| panic!("extensions: {e}"));
        assert!(!extensions.is_empty());
        assert!(extensions.iter().all(|e| matches!(e.badge.as_deref(), Some("loaded" | "installed" | "available"))));

        let settings = db.objects(ObjectKind::Setting, None).await.unwrap_or_else(|e| panic!("settings: {e}"));
        assert!(settings.iter().any(|s| s.reference.name == "threads"));

        let table = db.object_detail(&ObjectRef { kind: ObjectKind::Table, name: "orders".into(), parent: Some("main".into()) }).await.unwrap_or_else(|e| panic!("table detail: {e}"));
        assert!(table.definition.as_deref().is_some_and(|d| d.to_ascii_uppercase().contains("CREATE TABLE")), "{:?}", table.definition);
        assert_eq!(table.columns.len(), 3);
        assert!(table.children.iter().any(|c| c.reference.kind == ObjectKind::Index && c.reference.name == "orders_user"));
        assert!(table.children.iter().any(|c| c.reference.kind == ObjectKind::Constraint));
        assert!(table.actions.iter().any(|a| a.id == "analyze" && !a.destructive));
        assert!(table.actions.iter().any(|a| a.id == "checkpoint" && a.statement == "CHECKPOINT;"));
        assert!(table.actions.iter().any(|a| a.id == "drop" && a.destructive && a.statement == "DROP TABLE \"main\".\"orders\";"));

        let view = db.object_detail(&ObjectRef { kind: ObjectKind::View, name: "big_orders".into(), parent: None }).await.unwrap_or_else(|e| panic!("view: {e}"));
        assert!(view.actions.iter().any(|a| a.statement == "DROP VIEW \"main\".\"big_orders\";"));

        let schema = db.object_detail(&ObjectRef { kind: ObjectKind::Schema, name: "main".into(), parent: None }).await.unwrap_or_else(|e| panic!("schema: {e}"));
        assert!(schema.children.iter().any(|c| c.reference.name == "users"));
        assert!(schema.actions.iter().any(|a| a.destructive && a.statement.contains("DROP SCHEMA")));

        let sequence = db.object_detail(&ObjectRef { kind: ObjectKind::Sequence, name: "order_seq".into(), parent: None }).await.unwrap_or_else(|e| panic!("sequence: {e}"));
        assert!(sequence.properties.iter().any(|p| p.name == "Start" && p.value == "5"));
        assert!(sequence.properties.iter().any(|p| p.name == "Increment" && p.value == "2"));

        let user_type = db.object_detail(&ObjectRef { kind: ObjectKind::Type, name: "mood".into(), parent: None }).await.unwrap_or_else(|e| panic!("type: {e}"));
        assert_eq!(user_type.definition.as_deref(), Some("CREATE TYPE \"mood\" AS ENUM ('ok', 'great');"));
        assert!(user_type.properties.iter().any(|p| p.name == "Labels" && p.value == "ok, great"));

        let user_macro = db.object_detail(&ObjectRef { kind: ObjectKind::Macro, name: "add_one".into(), parent: None }).await.unwrap_or_else(|e| panic!("macro: {e}"));
        assert!(user_macro.actions.iter().any(|a| a.id == "drop" && a.destructive));
        assert!(user_macro.properties.iter().any(|p| p.name == "Signature"));

        let index = db.object_detail(&ObjectRef { kind: ObjectKind::Index, name: "orders_user".into(), parent: Some("orders".into()) }).await.unwrap_or_else(|e| panic!("index: {e}"));
        assert!(index.properties.iter().any(|p| p.name == "Table" && p.value == "orders"));
        assert!(index.actions.iter().any(|a| a.destructive && a.statement.contains("DROP INDEX")));

        let constraint_ref = constraints.first().map(|c| c.reference.clone()).unwrap_or_else(|| panic!("a constraint"));
        let constraint = db.object_detail(&constraint_ref).await.unwrap_or_else(|e| panic!("constraint: {e}"));
        assert!(constraint.properties.iter().any(|p| p.name == "Type"));

        let extension_ref = extensions.first().map(|e| e.reference.clone()).unwrap_or_else(|| panic!("an extension"));
        let extension = db.object_detail(&extension_ref).await.unwrap_or_else(|e| panic!("extension: {e}"));
        assert!(extension.actions.iter().any(|a| a.id == "install" && !a.destructive && a.statement.starts_with("INSTALL ")));
        assert!(extension.actions.iter().any(|a| a.id == "load" && !a.destructive));

        let setting = db.object_detail(&ObjectRef { kind: ObjectKind::Setting, name: "threads".into(), parent: None }).await.unwrap_or_else(|e| panic!("setting: {e}"));
        assert!(setting.definition.as_deref().is_some_and(|d| d.starts_with("SET threads = ")), "{:?}", setting.definition);
        assert!(setting.actions.iter().all(|a| a.destructive));
        assert!(setting.actions.iter().any(|a| a.statement == "RESET threads;"));

        assert!(db.object_detail(&ObjectRef { kind: ObjectKind::Table, name: "nope".into(), parent: None }).await.is_err());

        let stats = db.server_stats().await.unwrap_or_else(|e| panic!("stats: {e}"));
        let titles: Vec<&str> = stats.groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Storage", "Memory", "Schema"]);
        assert!(stats.groups[0].stats.iter().any(|s| s.label == "DuckDB version"));
        assert!(stats.groups[0].stats.iter().any(|s| s.label == "Threads" && s.numeric.is_some_and(|n| n >= 1.0)));
        assert!(stats.groups[2].stats.iter().any(|s| s.label == "Buffer manager" && s.numeric.is_some()));
        let schema_group = &stats.groups[3];
        assert!(schema_group.stats.iter().any(|s| s.label == "Tables" && s.numeric.is_some_and(|n| n >= 2.0)));
        assert!(schema_group.stats.iter().any(|s| s.label == "Macros" && s.numeric.is_some_and(|n| n >= 1.0)));
    }

    #[tokio::test]
    async fn in_memory_and_missing_read_only() {
        let mem = connect(&resolved(":memory:", false)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let out = mem.execute("SELECT 1 + 1 AS two, 'a' AS s, TRUE AS b", 10).await.unwrap_or_else(|e| panic!("execute: {e}"));
        let rs = rows_of(&out);
        assert_eq!(rs.rows[0], vec![Value::Int(2), Value::Text("a".into()), Value::Bool(true)]);
        assert_eq!(mem.current_database(), Some("memory".into()));
        let missing = temp_file("missing");
        assert!(connect(&resolved(&missing, true)).await.is_err());
        assert!(!std::path::Path::new(&missing).exists());
    }
}

