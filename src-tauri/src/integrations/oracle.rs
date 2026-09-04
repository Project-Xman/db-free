// SOT: oracle-integration, oracle-adapter, oracle-value-decoding, oracle-catalog-queries

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
use oracle::sql_type::{OracleType, Timestamp};
use oracle::{Connection, ErrorKind, SqlValue};
use std::sync::{Arc, Mutex};

// ============================================================================
// ORACLE ADAPTER
//
// WHAT:  Oracle Database through the `oracle` crate (ODPI-C over Oracle Instant
//        Client, which must be installed on the machine at runtime).
// WHY:   Same shape as sqlite.rs: a sync driver behind Arc<Mutex<>> run on
//        the blocking pool.
// HOW:   Catalog comes from ALL_TABLES / ALL_VIEWS (owner = schema) minus the
//        Oracle-internal schemas. Paging uses `OFFSET n ROWS FETCH NEXT m ROWS
//        ONLY` (12c+). `execute` splits the script, strips the trailing `;`
//        Oracle rejects, and decodes SqlValue per column type.
//        A missing Instant Client (DPI-1047) is turned into a friendly
//        not_connected error with install instructions.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI meta)
// ============================================================================

impl From<oracle::Error> for AppError {
    fn from(err: oracle::Error) -> Self {
        map_error(&err)
    }
}

const DEFAULT_PORT: u16 = 1521;
const DEFAULT_SERVICE: &str = "FREEPDB1";

const SYSTEM_SCHEMAS: &[&str] = &[
    "SYS", "SYSTEM", "XDB", "MDSYS", "CTXSYS", "OUTLN", "DBSNMP", "APPQOSSYS", "WMSYS", "ORDSYS",
    "OLAPSYS", "LBACSYS", "GSMADMIN_INTERNAL", "DVSYS", "AUDSYS", "OJVMSYS", "DBSFWUSER", "GGSYS",
    "ANONYMOUS", "REMOTE_SCHEDULER_AGENT", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC", "SYS$UMF", "DIP",
    "ORACLE_OCM", "XS$NULL", "PDBADMIN", "ORDDATA", "ORDPLUGINS", "SI_INFORMTN_SCHEMA", "DGPDB_INT",
];

fn system_schema_list() -> String {
    SYSTEM_SCHEMAS.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(", ")
}

pub struct OracleIntegration {
    conn: Arc<Mutex<Connection>>,
    service: String,
    current_schema: String,
}

// WHAT:  Friendly error mapping. DPI-1047 = Instant Client not found; ORA-01017
//        = bad credentials; ORA-12541/12514 = listener / service problems.
fn map_error(err: &oracle::Error) -> AppError {
    let text = err.to_string();
    if text.contains("DPI-1047") {
        return AppError::not_connected(
            "Oracle Instant Client was not found. Install it from oracle.com (Instant Client Basic or Basic Light) \
             and point DYLD_LIBRARY_PATH (macOS) / LD_LIBRARY_PATH (Linux) / PATH (Windows) at the directory \
             containing libclntsh, then reconnect.",
        );
    }
    if text.contains("ORA-01017") || text.contains("ORA-28000") || text.contains("ORA-01005") {
        return AppError::not_connected(format!("Oracle rejected the credentials: {text}"));
    }
    if text.contains("ORA-12541") || text.contains("ORA-12514") || text.contains("ORA-12154") || text.contains("DPI-1080") {
        return AppError::not_connected(format!("Could not reach the Oracle listener / service: {text}"));
    }
    AppError::driver(text)
}

// WHAT:  Easy-connect string `//host:port/service` from the connection form.
pub fn connect_string(conn: &ResolvedConnection) -> String {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("127.0.0.1");
    // Allow the user to paste a full easy-connect / TNS string in the host field.
    if host.contains('/') || host.contains("(DESCRIPTION") {
        return host.to_string();
    }
    let port = s.port.unwrap_or(DEFAULT_PORT);
    let service = s.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).unwrap_or(DEFAULT_SERVICE);
    format!("//{host}:{port}/{service}")
}

fn service_name(conn: &ResolvedConnection) -> String {
    conn.summary
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_SERVICE)
        .to_string()
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let user = conn.summary.username.clone().unwrap_or_default();
    let password = conn.secret.clone().unwrap_or_default();
    let target = connect_string(conn);
    let service = service_name(conn);
    let (connection, current_schema) = tokio::task::spawn_blocking(move || -> AppResult<(Connection, String)> {
        let c = Connection::connect(&user, &password, &target).map_err(|e| map_error(&e))?;
        let schema: String = c
            .query_row_as("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL", &[])
            .unwrap_or_else(|_| user.to_ascii_uppercase());
        Ok((c, schema))
    })
    .await
    .map_err(AppError::internal)??;
    Ok(Arc::new(OracleIntegration { conn: Arc::new(Mutex::new(connection)), service, current_schema }))
}

// ---------------------------------------------------------------------------
// SQL builders (unit-tested; no live server needed)
// ---------------------------------------------------------------------------

// WHAT:  Oracle rejects a trailing `;` on plain SQL but requires it inside
//        PL/SQL blocks; keep it only when the statement is a block.
pub fn normalize_statement(statement: &str) -> String {
    let trimmed = statement.trim();
    let upper = trimmed.to_ascii_uppercase();
    let is_block = upper.starts_with("BEGIN") || upper.starts_with("DECLARE") || upper.starts_with("CREATE OR REPLACE") && (upper.contains("PROCEDURE") || upper.contains("FUNCTION") || upper.contains("TRIGGER") || upper.contains("PACKAGE") || upper.contains("TYPE"));
    if is_block {
        return trimmed.to_string();
    }
    trimmed.trim_end_matches(';').trim_end().to_string()
}

// WHAT:  Re-joins statements that the `;` splitter cut inside a PL/SQL block,
//        so `BEGIN … END;` runs as one unit.
pub fn split_oracle_script(sql: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut block: Option<String> = None;
    for piece in split_statements(sql) {
        let text = piece.trim();
        if text.is_empty() {
            continue;
        }
        match block.as_mut() {
            Some(buf) => {
                buf.push_str(text);
                buf.push(';');
                let upper = buf.to_ascii_uppercase();
                if text.to_ascii_uppercase().trim_end().ends_with("END") || upper.trim_end().ends_with("END;") {
                    if let Some(done) = block.take() {
                        out.push(done);
                    }
                } else {
                    buf.push('\n');
                }
            }
            None => {
                let upper = text.to_ascii_uppercase();
                let opens_block = upper.starts_with("BEGIN") || upper.starts_with("DECLARE") || (upper.starts_with("CREATE") && (upper.contains(" PROCEDURE ") || upper.contains(" FUNCTION ") || upper.contains(" TRIGGER ") || upper.contains(" PACKAGE ")));
                let self_contained = upper.trim_end().ends_with(" END") || upper == "END";
                if opens_block && !self_contained {
                    block = Some(format!("{text};\n"));
                } else {
                    out.push(text.to_string());
                }
            }
        }
    }
    if let Some(rest) = block.take() {
        out.push(rest);
    }
    out
}

fn owner_of(table: &TableRef, current_schema: &str) -> String {
    table.schema.clone().unwrap_or_else(|| current_schema.to_string())
}

fn quoted_table(table: &TableRef, current_schema: &str) -> String {
    let full = TableRef { schema: Some(owner_of(table, current_schema)), name: table.name.clone() };
    qualified_name_for(Engine::Oracle, &full)
}

pub fn page_sql(table: &TableRef, current_schema: &str, query: &PageQuery) -> String {
    format!(
        "SELECT * FROM {}{}{} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
        quoted_table(table, current_schema),
        where_clause(Engine::Oracle, &query.filters),
        order_clause(Engine::Oracle, &query.sort),
        query.offset,
        query.limit
    )
}

pub fn count_sql(table: &TableRef, current_schema: &str, filters: &[FilterRule]) -> String {
    format!("SELECT COUNT(*) FROM {}{}", quoted_table(table, current_schema), where_clause(Engine::Oracle, filters))
}

fn catalog_sql() -> String {
    format!(
        "SELECT owner, table_name, 'TABLE' AS kind, num_rows FROM all_tables WHERE owner NOT IN ({0}) \
         UNION ALL \
         SELECT owner, view_name, 'VIEW', NULL FROM all_views WHERE owner NOT IN ({0}) \
         ORDER BY 1, 2",
        system_schema_list()
    )
}

const COLUMNS_SQL: &str = "SELECT c.column_name, c.data_type, c.data_length, c.data_precision, c.data_scale, c.nullable, c.column_id, \
    CASE WHEN EXISTS (\
        SELECT 1 FROM all_constraints k JOIN all_cons_columns kc \
          ON kc.owner = k.owner AND kc.constraint_name = k.constraint_name \
        WHERE k.owner = c.owner AND k.table_name = c.table_name AND k.constraint_type = 'P' AND kc.column_name = c.column_name\
    ) THEN 1 ELSE 0 END AS is_pk \
    FROM all_tab_columns c WHERE c.owner = :1 AND c.table_name = :2 ORDER BY c.column_id";

const FOREIGN_KEYS_SQL: &str = "SELECT k.owner, k.table_name, k.constraint_name, kc.column_name, \
    r.owner, r.table_name, rc.column_name, kc.position \
    FROM all_constraints k \
    JOIN all_cons_columns kc ON kc.owner = k.owner AND kc.constraint_name = k.constraint_name \
    JOIN all_constraints r ON r.owner = k.r_owner AND r.constraint_name = k.r_constraint_name \
    JOIN all_cons_columns rc ON rc.owner = r.owner AND rc.constraint_name = r.constraint_name AND rc.position = kc.position \
    WHERE k.constraint_type = 'R' AND k.owner = :1 \
    ORDER BY k.owner, k.table_name, k.constraint_name, kc.position";

// WHAT:  Oracle's DATA_TYPE + precision/scale → a compact lowercase label.
pub fn column_type_label(data_type: &str, length: Option<i64>, precision: Option<i64>, scale: Option<i64>) -> String {
    let upper = data_type.to_ascii_uppercase();
    match upper.as_str() {
        "NUMBER" => match (precision, scale) {
            (Some(p), Some(0)) => format!("number({p})"),
            (Some(p), Some(s)) => format!("number({p},{s})"),
            (Some(p), None) => format!("number({p})"),
            _ => "number".to_string(),
        },
        "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" | "RAW" => match length {
            Some(n) => format!("{}({n})", upper.to_ascii_lowercase()),
            None => upper.to_ascii_lowercase(),
        },
        other => other.to_ascii_lowercase(),
    }
}

// ---------------------------------------------------------------------------
// Value decoding
// ---------------------------------------------------------------------------

fn timestamp_text(ts: &Timestamp) -> String {
    let mut out = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        ts.year(),
        ts.month(),
        ts.day(),
        ts.hour(),
        ts.minute(),
        ts.second()
    );
    if ts.nanosecond() > 0 {
        let frac = format!("{:09}", ts.nanosecond());
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    if ts.with_tz() {
        let offset = ts.tz_offset();
        let sign = if offset < 0 { '-' } else { '+' };
        let abs = offset.abs();
        out.push_str(&format!("{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60));
    }
    out
}

// WHAT:  Numeric text → Int when integral, else Decimal (keeps precision).
fn number_value(text: &str) -> Value {
    let trimmed = text.trim();
    if let Ok(i) = trimmed.parse::<i64>() {
        return Value::Int(i);
    }
    Value::Decimal(trimmed.to_string())
}

// WHAT:  One Oracle cell → the engine-neutral Value.
fn decode_cell(value: &SqlValue<'_>, oracle_type: &OracleType) -> Value {
    if value.is_null().unwrap_or(true) {
        return Value::Null;
    }
    match oracle_type {
        OracleType::Number(..) | OracleType::Float(_) => value
            .get::<String>()
            .map(|s| number_value(&s))
            .unwrap_or_else(|e| Value::Unsupported(e.to_string())),
        OracleType::Int64 | OracleType::UInt64 => value.get::<i64>().map(Value::Int).unwrap_or_else(|e| Value::Unsupported(e.to_string())),
        OracleType::BinaryFloat | OracleType::BinaryDouble => {
            value.get::<f64>().map(Value::Float).unwrap_or_else(|e| Value::Unsupported(e.to_string()))
        }
        OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Long
        | OracleType::Rowid
        | OracleType::Xml => value.get::<String>().map(Value::Text).unwrap_or_else(|e| Value::Unsupported(e.to_string())),
        OracleType::Json => value
            .get::<String>()
            .map(|s| serde_json::from_str(&s).map(Value::Json).unwrap_or(Value::Text(s)))
            .unwrap_or_else(|e| Value::Unsupported(e.to_string())),
        OracleType::Raw(_) | OracleType::BLOB | OracleType::LongRaw => value
            .get::<Vec<u8>>()
            .map(|b| Value::Bytes(base64::engine::general_purpose::STANDARD.encode(b)))
            .unwrap_or_else(|e| Value::Unsupported(e.to_string())),
        OracleType::Date | OracleType::Timestamp(_) | OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => value
            .get::<Timestamp>()
            .map(|ts| Value::DateTime(timestamp_text(&ts)))
            .unwrap_or_else(|e| Value::Unsupported(e.to_string())),
        OracleType::IntervalDS(..) | OracleType::IntervalYM(_) => {
            value.get::<String>().map(Value::Text).unwrap_or_else(|e| Value::Unsupported(e.to_string()))
        }
        OracleType::Boolean => value.get::<bool>().map(Value::Bool).unwrap_or_else(|e| Value::Unsupported(e.to_string())),
        other => value.get::<String>().map(Value::Text).unwrap_or_else(|_| Value::Unsupported(other.to_string())),
    }
}

fn oracle_type_label(t: &OracleType) -> String {
    t.to_string().to_ascii_lowercase()
}

// WHAT:  Runs one statement (already normalised) and collects rows / row count.
fn run_statement(conn: &Connection, sql: &str, max_rows: usize) -> AppResult<StatementResult> {
    let mut stmt = conn.statement(sql).build()?;
    if stmt.is_query() {
        let rows = stmt.query(&[])?;
        let columns: Vec<ColumnMeta> = rows
            .column_info()
            .iter()
            .map(|c| ColumnMeta { name: c.name().to_string(), type_name: oracle_type_label(c.oracle_type()) })
            .collect();
        let types: Vec<OracleType> = rows.column_info().iter().map(|c| c.oracle_type().clone()).collect();
        let mut collected: Vec<Vec<Value>> = Vec::new();
        let mut truncated = false;
        for row in rows {
            let row = row?;
            if collected.len() >= max_rows {
                truncated = true;
                break;
            }
            let cells = row.sql_values().iter().zip(types.iter()).map(|(v, t)| decode_cell(v, t)).collect();
            collected.push(cells);
        }
        return Ok(StatementResult::Rows { result: ResultSet { columns, rows: collected, truncated } });
    }
    stmt.execute(&[])?;
    let affected = if stmt.is_dml() { stmt.row_count().unwrap_or(0) } else { 0 };
    Ok(StatementResult::Affected { rows_affected: affected })
}

fn run_script(conn: &Connection, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
    let mut out = Vec::new();
    for statement in split_oracle_script(sql) {
        let normalized = normalize_statement(&statement);
        if normalized.is_empty() {
            continue;
        }
        out.push(run_statement(conn, &normalized, max_rows)?);
    }
    Ok(out)
}

fn opt_i64(value: &SqlValue<'_>) -> Option<i64> {
    if value.is_null().unwrap_or(true) {
        return None;
    }
    value.get::<i64>().ok()
}

impl OracleIntegration {
    async fn blocking<T, F>(&self, f: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> AppResult<T> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| AppError::internal("oracle session lock poisoned"))?;
            f(&guard)
        })
        .await
        .map_err(AppError::internal)?
    }

    async fn scalar_i64(&self, sql: String) -> AppResult<i64> {
        self.blocking(move |conn| {
            let n: i64 = conn.query_row_as(&sql, &[])?;
            Ok(n)
        })
        .await
    }
}

#[async_trait]
impl Integration for OracleIntegration {
    fn engine(&self) -> Engine {
        Engine::Oracle
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::SQL
    }

    async fn ping(&self) -> AppResult<()> {
        self.blocking(|conn| {
            conn.ping()?;
            Ok(())
        })
        .await
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        self.blocking(|conn| {
            let (version, banner) = conn.server_version()?;
            let short = banner.lines().next().unwrap_or("").trim().to_string();
            Ok(Some(if short.is_empty() { format!("Oracle {version}") } else { short }))
        })
        .await
    }

    fn current_database(&self) -> Option<String> {
        Some(self.service.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.service.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let current = self.current_schema.clone();
        self.blocking(move |conn| {
            let mut schemas: Vec<SchemaInfo> = Vec::new();
            let rows = conn.query(&catalog_sql(), &[])?;
            for row in rows {
                let row = row?;
                let values = row.sql_values();
                let owner: String = values.first().and_then(|v| v.get::<String>().ok()).unwrap_or_default();
                let name: String = values.get(1).and_then(|v| v.get::<String>().ok()).unwrap_or_default();
                let kind: String = values.get(2).and_then(|v| v.get::<String>().ok()).unwrap_or_default();
                let estimate = values.get(3).and_then(opt_i64);
                let info = TableInfo {
                    schema: Some(owner.clone()),
                    name,
                    kind: if kind == "VIEW" { TableKind::View } else { TableKind::Table },
                    row_estimate: estimate,
                };
                match schemas.iter_mut().find(|s| s.name == owner) {
                    Some(existing) => existing.tables.push(info),
                    None => schemas.push(SchemaInfo { name: owner, tables: vec![info] }),
                }
            }
            // Current schema first so the sidebar opens on the user's own objects.
            if let Some(pos) = schemas.iter().position(|s| s.name == current) {
                let own = schemas.remove(pos);
                schemas.insert(0, own);
            } else {
                schemas.insert(0, SchemaInfo { name: current, tables: Vec::new() });
            }
            Ok(SchemaCatalog { schemas })
        })
        .await
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let owner = owner_of(table, &self.current_schema);
        let name = table.name.clone();
        self.blocking(move |conn| {
            let rows = conn.query(COLUMNS_SQL, &[&owner, &name])?;
            let mut out = Vec::new();
            for row in rows {
                let row = row?;
                let v = row.sql_values();
                let col_name: String = v.first().and_then(|x| x.get::<String>().ok()).unwrap_or_default();
                let data_type: String = v.get(1).and_then(|x| x.get::<String>().ok()).unwrap_or_default();
                let length = v.get(2).and_then(opt_i64);
                let precision = v.get(3).and_then(opt_i64);
                let scale = v.get(4).and_then(opt_i64);
                let nullable: String = v.get(5).and_then(|x| x.get::<String>().ok()).unwrap_or_else(|| "Y".into());
                let ordinal = v.get(6).and_then(opt_i64).unwrap_or(0);
                let is_pk = v.get(7).and_then(opt_i64).unwrap_or(0) == 1;
                out.push(ColumnInfo {
                    name: col_name,
                    data_type: column_type_label(&data_type, length, precision, scale),
                    nullable: nullable == "Y",
                    primary_key: is_pk,
                    ordinal: u32::try_from(ordinal).unwrap_or_default(),
                });
            }
            if out.is_empty() {
                return Err(AppError::not_found(format!("Table \"{owner}\".\"{name}\" was not found or has no visible columns.")));
            }
            Ok(out)
        })
        .await
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let owner = owner_of(table, &self.current_schema);
        let name = table.name.clone();
        self.blocking(move |conn| {
            let row = conn.query_row("SELECT num_rows FROM all_tables WHERE owner = :1 AND table_name = :2", &[&owner, &name]);
            match row {
                Ok(r) => Ok(r.sql_values().first().and_then(opt_i64)),
                Err(e) if e.kind() == ErrorKind::NoDataFound => Ok(None),
                Err(e) => Err(map_error(&e)),
            }
        })
        .await
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        self.scalar_i64(count_sql(table, &self.current_schema, filters)).await
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let sql = page_sql(table, &self.current_schema, query);
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

    async fn close(&self) {
        let conn = Arc::clone(&self.conn);
        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(guard) = conn.lock() {
                let _ = guard.close();
            }
        })
        .await;
    }

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        let owner = self.current_schema.clone();
        self.blocking(move |conn| {
            let rows = conn.query(FOREIGN_KEYS_SQL, &[&owner])?;
            let mut out: Vec<ForeignKey> = Vec::new();
            for row in rows {
                let row = row?;
                let v = row.sql_values();
                let text = |i: usize| v.get(i).and_then(|x| x.get::<String>().ok()).unwrap_or_default();
                let (from_schema, from_table, name, from_col, to_schema, to_table, to_col) =
                    (text(0), text(1), text(2), text(3), text(4), text(5), text(6));
                match out.iter_mut().find(|fk| fk.name == name && fk.from_table == from_table && fk.from_schema.as_deref() == Some(&from_schema)) {
                    Some(existing) => {
                        existing.from_columns.push(from_col);
                        existing.to_columns.push(to_col);
                    }
                    None => out.push(ForeignKey {
                        name,
                        from_schema: Some(from_schema),
                        from_table,
                        from_columns: vec![from_col],
                        to_schema: Some(to_schema),
                        to_table,
                        to_columns: vec![to_col],
                    }),
                }
            }
            Ok(out)
        })
        .await
    }

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let owner = owner_of(table, &self.current_schema);
        let name = table.name.clone();
        self.blocking(move |conn| {
            let sql = "SELECT DBMS_METADATA.GET_DDL(:1, :2, :3) FROM DUAL";
            let mut object_type = "TABLE";
            let is_view = conn
                .query_row_as::<i64>("SELECT COUNT(*) FROM all_views WHERE owner = :1 AND view_name = :2", &[&owner, &name])
                .unwrap_or(0)
                > 0;
            if is_view {
                object_type = "VIEW";
            }
            let ddl: String = match conn.query_row_as::<String>(sql, &[&object_type, &name, &owner]) {
                Ok(s) => s,
                Err(e) if e.kind() == ErrorKind::NoDataFound => return Ok(None),
                Err(e) => return Err(map_error(&e)),
            };
            Ok(Some(ddl.trim().to_string()))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, FilterOp, SortRule, SslMode};

    fn resolved(host: Option<&str>, port: Option<u16>, database: Option<&str>) -> ResolvedConnection {
        ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Oracle,
                environment: Environment::Local,
                read_only: false,
                host: host.map(str::to_string),
                port,
                database: database.map(str::to_string),
                username: Some("app".into()),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: Some("pw".into()),
        }
    }

    #[test]
    fn builds_connect_strings() {
        assert_eq!(connect_string(&resolved(None, None, None)), "//127.0.0.1:1521/FREEPDB1");
        assert_eq!(connect_string(&resolved(Some("db.example"), Some(1522), Some("ORCLPDB"))), "//db.example:1522/ORCLPDB");
        assert_eq!(connect_string(&resolved(Some("db.example/XE"), None, None)), "db.example/XE");
        assert_eq!(service_name(&resolved(None, None, Some(" X "))), "X");
    }

    #[test]
    fn builds_page_and_count_sql() {
        let table = TableRef { schema: None, name: "EMP".into() };
        let query = PageQuery {
            sort: vec![SortRule { column: "EMPNO".into(), desc: true }],
            filters: vec![FilterRule { column: "ENAME".into(), op: FilterOp::Contains, value: "a".into() }],
            offset: 20,
            limit: 10,
        };
        assert_eq!(
            page_sql(&table, "SCOTT", &query),
            "SELECT * FROM \"SCOTT\".\"EMP\" WHERE CAST(\"ENAME\" AS VARCHAR2(4000)) LIKE '%a%' ORDER BY \"EMPNO\" DESC OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY"
        );
        let other = TableRef { schema: Some("HR".into()), name: "JOBS".into() };
        assert_eq!(count_sql(&other, "SCOTT", &[]), "SELECT COUNT(*) FROM \"HR\".\"JOBS\"");
        assert!(catalog_sql().contains("'SYS', 'SYSTEM'"));
    }

    #[test]
    fn normalises_statements() {
        assert_eq!(normalize_statement("SELECT 1 FROM DUAL;"), "SELECT 1 FROM DUAL");
        assert_eq!(normalize_statement("  SELECT 1 FROM DUAL ; "), "SELECT 1 FROM DUAL");
        assert_eq!(normalize_statement("BEGIN NULL; END;"), "BEGIN NULL; END;");
        let script = split_oracle_script("CREATE TABLE t (id NUMBER); BEGIN INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); END; SELECT * FROM t;");
        assert_eq!(script.len(), 3, "{script:?}");
        assert_eq!(script[0], "CREATE TABLE t (id NUMBER)");
        assert!(script[1].starts_with("BEGIN") && script[1].trim_end().ends_with("END;"), "{}", script[1]);
        assert_eq!(script[2], "SELECT * FROM t");
        let plain = split_oracle_script("SELECT 1 FROM DUAL;\nSELECT 2 FROM DUAL");
        assert_eq!(plain, vec!["SELECT 1 FROM DUAL".to_string(), "SELECT 2 FROM DUAL".to_string()]);
    }

    #[test]
    fn decodes_numbers_and_types() {
        assert_eq!(number_value("42"), Value::Int(42));
        assert_eq!(number_value("-7"), Value::Int(-7));
        assert_eq!(number_value("3.14"), Value::Decimal("3.14".into()));
        assert_eq!(number_value("99999999999999999999"), Value::Decimal("99999999999999999999".into()));
        assert_eq!(column_type_label("NUMBER", None, Some(10), Some(0)), "number(10)");
        assert_eq!(column_type_label("NUMBER", None, Some(10), Some(2)), "number(10,2)");
        assert_eq!(column_type_label("NUMBER", None, None, None), "number");
        assert_eq!(column_type_label("VARCHAR2", Some(100), None, None), "varchar2(100)");
        assert_eq!(column_type_label("DATE", Some(7), None, None), "date");
        assert_eq!(column_type_label("TIMESTAMP(6)", None, None, None), "timestamp(6)");
        let ts = Timestamp::new(2024, 3, 5, 7, 8, 9, 500_000_000).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(timestamp_text(&ts), "2024-03-05 07:08:09.5");
        let tz = ts.and_tz_hm_offset(-5, -30).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(timestamp_text(&tz), "2024-03-05 07:08:09.5-05:30");
        let plain = Timestamp::new(2024, 1, 1, 0, 0, 0, 0).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(timestamp_text(&plain), "2024-01-01 00:00:00");
    }

    #[test]
    fn maps_missing_client_error() {
        let err = AppError::not_connected("x");
        let mapped = map_error(&oracle::Error::new(ErrorKind::DpiError, "DPI-1047: Cannot locate a 64-bit Oracle Client library"));
        assert_eq!(std::mem::discriminant(&mapped), std::mem::discriminant(&err));
        assert!(mapped.to_string().contains("Instant Client"), "{mapped}");
    }

    // Live test: set DBFREE_TEST_ORACLE_URL=//host:port/service plus
    // DBFREE_TEST_ORACLE_USER / DBFREE_TEST_ORACLE_PASSWORD, and have Instant Client on the library path.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_ORACLE_URL") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Oracle,
            environment: Environment::Local,
            read_only: false,
            host: Some(url),
            port: None,
            database: std::env::var("DBFREE_TEST_ORACLE_SERVICE").ok(),
            username: std::env::var("DBFREE_TEST_ORACLE_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary::draft(&input, true),
            secret: std::env::var("DBFREE_TEST_ORACLE_PASSWORD").ok(),
        };
        let ora = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        ora.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        let version = ora.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.to_ascii_lowercase().contains("oracle"), "{version}");
        let _ = ora.execute("DROP TABLE dbfree_smoke", 1).await;
        let out = ora
            .execute(
                "CREATE TABLE dbfree_smoke (id NUMBER PRIMARY KEY, name VARCHAR2(50), amt NUMBER(10,2), at DATE);\
                 INSERT INTO dbfree_smoke VALUES (1, 'Ada', 12.5, DATE '2024-01-02');\
                 INSERT INTO dbfree_smoke VALUES (2, 'Linus', 3, NULL);\
                 SELECT * FROM dbfree_smoke ORDER BY id",
                10,
            )
            .await
            .unwrap_or_else(|e| panic!("execute: {e}"));
        assert_eq!(out.len(), 4);
        assert!(matches!(out[1], StatementResult::Affected { rows_affected: 1 }));
        let StatementResult::Rows { result } = &out[3] else { panic!("expected rows") };
        assert_eq!(result.rows[0][0], Value::Int(1));
        assert_eq!(result.rows[0][2], Value::Decimal("12.5".into()));
        assert_eq!(result.rows[0][3], Value::DateTime("2024-01-02 00:00:00".into()));
        let table = TableRef { schema: None, name: "DBFREE_SMOKE".into() };
        let cols = ora.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols[0].primary_key);
        assert_eq!(ora.count(&table, &[]).await.unwrap_or_default(), 2);
        let page = ora
            .fetch_page(&table, &PageQuery { sort: vec![SortRule { column: "ID".into(), desc: true }], filters: vec![], offset: 0, limit: 1 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows[0][1], Value::Text("Linus".into()));
        assert!(ora.ddl(&table).await.unwrap_or_default().unwrap_or_default().contains("DBFREE_SMOKE"));
        let _ = ora.execute("DROP TABLE dbfree_smoke", 1).await;
    }
}
