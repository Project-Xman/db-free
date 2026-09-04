// SOT: duckdb-integration, duckdb-adapter, duckdb-value-decoding, duckdb-catalog-queries

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, where_clause};
use crate::integrations::{qualified_name_for, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, PageQuery, ResolvedConnection,
    ResultSet, SchemaCatalog, SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use duckdb::types::{TimeUnit, ValueRef};
use duckdb::{params, AccessMode, Config, Connection};
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

#[async_trait]
impl Integration for DuckdbIntegration {
    fn engine(&self) -> Engine {
        Engine::Duckdb
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { transactions: true, exact_estimate: false, namespaces: true, ..Capabilities::SQL }
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
            let list = |v: Value| -> Vec<String> {
                match v {
                    Value::Json(serde_json::Value::Array(items)) => {
                        items.into_iter().filter_map(|x| x.as_str().map(str::to_string)).collect()
                    }
                    _ => Vec::new(),
                }
            };
            while let Some(row) = rows.next()? {
                let schema: String = row.get(0)?;
                let from_table: String = row.get(1)?;
                let name: Option<String> = row.get(2)?;
                let from_columns = list(decode_cell(row.get_ref(3)?));
                let to_table: String = row.get(4)?;
                let to_columns = list(decode_cell(row.get_ref(5)?));
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
