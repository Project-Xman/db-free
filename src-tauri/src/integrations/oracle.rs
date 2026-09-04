// SOT: oracle-integration, oracle-adapter, oracle-value-decoding, oracle-catalog-queries, oracle-object-explorer, oracle-server-stats, oracle-admin-actions

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{qualified_name_for, quote_ident_for, Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, ObjectAction, ObjectDetail, ObjectKind,
    ObjectProperty, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    ServerStats, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use oracle::sql_type::{OracleType, Timestamp};
use oracle::{Connection, ErrorKind, SqlValue};
use std::collections::BTreeMap;
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

// ============================================================================
// OBJECT EXPLORER / ADMINISTRATION
//
// WHAT:  Lists and describes Oracle's dictionary beyond rows: schemas, tables,
//        views, materialized views, indexes, constraints, sequences, types,
//        PL/SQL units, triggers, synonyms, tablespaces, users, roles, grants,
//        sessions, locks, parameters and scheduler jobs.
// WHY:   The object explorer and the admin page are generic; this block turns
//        ALL_* / DBA_* / V$ views into the neutral `ObjectSummary` /
//        `ObjectDetail` / `ServerStats` shapes.
// HOW:   Pure SQL builders (`object_list_sql`, `object_list_fallback_sql`) and
//        row mappers (`summarize`) are unit-tested offline. Every DBA_ / V$
//        query has an ALL_ / USER_ fallback or a `privilege_hint`, so a plain
//        application account gets what it can see and a clear error otherwise.
//        Nested kinds (index, constraint, trigger) carry `OWNER.TABLE` in
//        `reference.parent`; Oracle forbids `.` in an unquoted name, and the
//        detail re-reads the dictionary for the exact owner anyway.
//        Definitions come from DBMS_METADATA.GET_DDL when the account may call
//        it, else from ALL_SOURCE / a rebuilt statement.
// WHERE: src-tauri/src/model/objects.rs (contract), src/features/objects (UI)
// ============================================================================

const OBJECT_CAP: usize = 2000;
const PREVIEW_CHARS: usize = 80;

fn top(n: usize) -> String {
    format!(" FETCH FIRST {n} ROWS ONLY")
}

// WHAT:  `col = 'HR'` for one schema, else every schema Oracle does not maintain.
fn owner_scope(column: &str, owner: Option<&str>) -> String {
    match owner {
        Some(o) => format!("{column} = {}", quote_literal(o)),
        None => format!("{column} NOT IN ({})", system_schema_list()),
    }
}

// WHAT:  `reference.parent` → (owner, table). "HR" → (HR, None); "HR.EMP" → both.
fn split_owner(parent: Option<&str>) -> (Option<&str>, Option<&str>) {
    match parent.map(str::trim).filter(|p| !p.is_empty()) {
        None => (None, None),
        Some(p) => match p.split_once('.') {
            Some((owner, table)) => (Some(owner), Some(table)),
            None => (Some(p), None),
        },
    }
}

fn owner_key(owner: &str, table: &str) -> String {
    format!("{owner}.{table}")
}

fn ident(name: &str) -> String {
    quote_ident_for(Engine::Oracle, name)
}

fn qualified(owner: &str, name: &str) -> String {
    format!("{}.{}", ident(owner), ident(name))
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

pub fn preview(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    let cut: String = collapsed.chars().take(max_chars).collect();
    format!("{}…", cut.trim_end())
}

fn pretty_label(raw: &str) -> String {
    raw.replace('_', " ").to_ascii_lowercase()
}

fn bytes_stat(label: &str, bytes: f64) -> Stat {
    Stat { label: label.to_string(), value: human_bytes(bytes), unit: None, hint: None, numeric: Some(bytes) }
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

fn cell_opt(value: Option<&Value>) -> Option<String> {
    let text = cell_text(value);
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn cell_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Int(i)) => Some(*i as f64),
        Some(Value::Float(f)) => Some(*f),
        Some(Value::Decimal(t)) | Some(Value::Text(t)) => t.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn column_index(set: &ResultSet, name: &str) -> Option<usize> {
    set.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
}

fn set_text(set: &ResultSet, row: &[Value], name: &str) -> String {
    column_index(set, name).map(|i| cell_text(row.get(i))).unwrap_or_default()
}

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

// WHAT:  ORA codes that mean "your account may not read that view".
pub fn is_privilege_error(err: &AppError) -> bool {
    let text = err.to_string();
    text.contains("ORA-00942")
        || text.contains("ORA-01031")
        || text.contains("ORA-00904")
        || text.contains("ORA-31603")
        || text.to_ascii_lowercase().contains("insufficient privileges")
}

// WHAT:  The dictionary view a kind needs and what to ask a DBA for, used when
//        both the primary and the fallback query are refused.
pub fn privilege_hint(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Session => "V$SESSION is not readable by this account. Ask for SELECT_CATALOG_ROLE or SELECT ON V_$SESSION.",
        ObjectKind::Lock => "V$LOCK / V$SESSION are not readable by this account. Ask for SELECT_CATALOG_ROLE.",
        ObjectKind::Setting => "V$PARAMETER is not readable by this account. Ask for SELECT_CATALOG_ROLE or SELECT ON V_$PARAMETER.",
        ObjectKind::Tablespace => "Neither DBA_TABLESPACES nor USER_TABLESPACES is readable by this account.",
        ObjectKind::User => "Neither DBA_USERS nor ALL_USERS is readable by this account.",
        ObjectKind::Role => "Neither DBA_ROLES nor USER_ROLE_PRIVS is readable by this account.",
        ObjectKind::Grant => "Neither DBA_TAB_PRIVS nor USER_TAB_PRIVS is readable by this account.",
        ObjectKind::Job => "Neither DBA_SCHEDULER_JOBS nor USER_SCHEDULER_JOBS is readable by this account.",
        _ => "This dictionary view is not readable by this account.",
    }
}

const SESSION_LIST_SQL: &str = "SELECT s.sid, s.serial#, s.username, s.status, s.osuser, s.machine, s.program, s.event, \
    s.seconds_in_wait, s.logon_time, s.sql_id, (SELECT q.sql_text FROM v$sql q WHERE q.sql_id = s.sql_id AND ROWNUM = 1) \
    FROM v$session s WHERE s.type = 'USER' ORDER BY s.sid";
const LOCK_LIST_SQL: &str = "SELECT l.sid, s.username, l.type, l.lmode, l.request, l.ctime, l.block, o.owner, o.object_name \
    FROM v$lock l JOIN v$session s ON s.sid = l.sid \
    LEFT JOIN all_objects o ON l.type = 'TM' AND o.object_id = l.id1 \
    WHERE l.type IN ('TM', 'TX', 'UL') ORDER BY l.sid, l.type";
const SETTING_LIST_SQL: &str = "SELECT name, value, isdefault, issys_modifiable, isses_modifiable, description FROM v$parameter ORDER BY name";

// WHAT:  Lock mode number → Oracle's own name (V$LOCK.LMODE / REQUEST).
pub fn lock_mode(code: i64) -> &'static str {
    match code {
        0 => "none",
        1 => "null",
        2 => "row-S",
        3 => "row-X",
        4 => "share",
        5 => "S/row-X",
        6 => "exclusive",
        _ => "unknown",
    }
}

pub fn constraint_kind(code: &str) -> &'static str {
    match code.trim().to_ascii_uppercase().as_str() {
        "P" => "primary",
        "R" => "foreign",
        "U" => "unique",
        "C" => "check",
        "V" => "view check",
        "O" => "read only",
        _ => "constraint",
    }
}

// WHAT:  SQL for one kind, preferring the widest dictionary view the account may
//        hold. `owner` scopes to one schema (None = every non-Oracle schema);
//        `table` narrows nested kinds to one owner table.
pub fn object_list_sql(kind: ObjectKind, owner: Option<&str>, table: Option<&str>) -> Option<String> {
    let cap = top(OBJECT_CAP);
    let table_filter = |col: &str| table.map(|t| format!(" AND {col} = {}", quote_literal(t))).unwrap_or_default();
    let sql = match kind {
        ObjectKind::Schema => format!(
            "SELECT username, created, oracle_maintained FROM all_users WHERE oracle_maintained = 'N' ORDER BY username{cap}"
        ),
        ObjectKind::Table => format!(
            "SELECT owner, table_name, num_rows, tablespace_name, partitioned, temporary, last_analyzed \
             FROM all_tables WHERE {} ORDER BY owner, table_name{cap}",
            owner_scope("owner", owner)
        ),
        ObjectKind::View => format!(
            "SELECT owner, view_name, text_length FROM all_views WHERE {} ORDER BY owner, view_name{cap}",
            owner_scope("owner", owner)
        ),
        ObjectKind::MaterializedView => format!(
            "SELECT owner, mview_name, refresh_mode, refresh_method, last_refresh_date, staleness \
             FROM all_mviews WHERE {} ORDER BY owner, mview_name{cap}",
            owner_scope("owner", owner)
        ),
        ObjectKind::Index => format!(
            "SELECT i.owner, i.index_name, i.table_name, i.index_type, i.uniqueness, i.status, i.tablespace_name, \
             (SELECT LISTAGG(c.column_name, ', ') WITHIN GROUP (ORDER BY c.column_position) FROM all_ind_columns c \
              WHERE c.index_owner = i.owner AND c.index_name = i.index_name) \
             FROM all_indexes i WHERE {}{} ORDER BY i.owner, i.table_name, i.index_name{cap}",
            owner_scope("i.owner", owner),
            table_filter("i.table_name")
        ),
        ObjectKind::Constraint => format!(
            "SELECT c.owner, c.constraint_name, c.table_name, c.constraint_type, c.status, \
             (SELECT LISTAGG(cc.column_name, ', ') WITHIN GROUP (ORDER BY cc.position) FROM all_cons_columns cc \
              WHERE cc.owner = c.owner AND cc.constraint_name = c.constraint_name), r.table_name, c.delete_rule \
             FROM all_constraints c LEFT JOIN all_constraints r ON r.owner = c.r_owner AND r.constraint_name = c.r_constraint_name \
             WHERE {}{} ORDER BY c.owner, c.table_name, c.constraint_name{cap}",
            owner_scope("c.owner", owner),
            table_filter("c.table_name")
        ),
        ObjectKind::Sequence => format!(
            "SELECT sequence_owner, sequence_name, last_number, increment_by, min_value, max_value, cycle_flag, cache_size \
             FROM all_sequences WHERE {} ORDER BY sequence_owner, sequence_name{cap}",
            owner_scope("sequence_owner", owner)
        ),
        ObjectKind::Type => format!(
            "SELECT owner, type_name, typecode, attributes, incomplete FROM all_types WHERE {} ORDER BY owner, type_name{cap}",
            owner_scope("owner", owner)
        ),
        ObjectKind::Function | ObjectKind::Procedure | ObjectKind::Package => format!(
            "SELECT owner, object_name, status, created, last_ddl_time FROM all_objects \
             WHERE object_type = '{}' AND {} ORDER BY owner, object_name{cap}",
            match kind {
                ObjectKind::Function => "FUNCTION",
                ObjectKind::Procedure => "PROCEDURE",
                _ => "PACKAGE",
            },
            owner_scope("owner", owner)
        ),
        ObjectKind::Trigger => format!(
            "SELECT owner, trigger_name, table_name, trigger_type, triggering_event, status, table_owner \
             FROM all_triggers WHERE {}{} ORDER BY owner, table_name, trigger_name{cap}",
            owner_scope("owner", owner),
            table_filter("table_name")
        ),
        ObjectKind::Alias => format!(
            "SELECT owner, synonym_name, table_owner, table_name, db_link FROM all_synonyms \
             WHERE {}{} ORDER BY owner, synonym_name{cap}",
            owner_scope("owner", owner),
            if owner.is_none() { " AND owner <> 'PUBLIC'" } else { "" }
        ),
        ObjectKind::Tablespace => format!(
            "SELECT t.tablespace_name, t.status, t.contents, t.extent_management, \
             (SELECT SUM(d.bytes) FROM dba_data_files d WHERE d.tablespace_name = t.tablespace_name) \
             FROM dba_tablespaces t ORDER BY t.tablespace_name{cap}"
        ),
        ObjectKind::User => format!(
            "SELECT username, account_status, default_tablespace, created, profile FROM dba_users ORDER BY username{cap}"
        ),
        ObjectKind::Role => format!("SELECT role, authentication_type, password_required FROM dba_roles ORDER BY role{cap}"),
        ObjectKind::Grant => format!(
            "SELECT * FROM (\
             SELECT grantee, privilege, owner || '.' || table_name AS object_name, grantable, 'OBJECT' AS grant_kind FROM dba_tab_privs \
             UNION ALL SELECT grantee, granted_role, NULL, admin_option, 'ROLE' FROM dba_role_privs) ORDER BY 1, 2{cap}"
        ),
        ObjectKind::Session => SESSION_LIST_SQL.to_string(),
        ObjectKind::Lock => LOCK_LIST_SQL.to_string(),
        ObjectKind::Setting => SETTING_LIST_SQL.to_string(),
        ObjectKind::Job => format!(
            "SELECT owner, job_name, enabled, state, repeat_interval, last_start_date, next_run_date, run_count, failure_count \
             FROM dba_scheduler_jobs ORDER BY owner, job_name{cap}"
        ),
        _ => return None,
    };
    Some(sql)
}

// WHAT:  The query to try when the primary one is refused (older releases without
//        ORACLE_MAINTAINED, or an account with no DBA_ views).
pub fn object_list_fallback_sql(kind: ObjectKind, _owner: Option<&str>) -> Option<String> {
    let cap = top(OBJECT_CAP);
    let sql = match kind {
        ObjectKind::Schema => format!(
            "SELECT username, created, NULL FROM all_users WHERE username NOT IN ({}) ORDER BY username{cap}",
            system_schema_list()
        ),
        ObjectKind::Tablespace => format!("SELECT tablespace_name, status, contents, extent_management, NULL FROM user_tablespaces ORDER BY tablespace_name{cap}"),
        ObjectKind::User => format!("SELECT username, NULL, NULL, created, NULL FROM all_users ORDER BY username{cap}"),
        ObjectKind::Role => format!("SELECT granted_role, NULL, admin_option FROM user_role_privs ORDER BY granted_role{cap}"),
        ObjectKind::Grant => format!(
            "SELECT * FROM (\
             SELECT grantee, privilege, owner || '.' || table_name AS object_name, grantable, 'OBJECT' AS grant_kind FROM user_tab_privs \
             UNION ALL SELECT username, granted_role, NULL, admin_option, 'ROLE' FROM user_role_privs) ORDER BY 1, 2{cap}"
        ),
        ObjectKind::Job => format!(
            "SELECT USER, job_name, enabled, state, repeat_interval, last_start_date, next_run_date, run_count, failure_count \
             FROM user_scheduler_jobs ORDER BY job_name{cap}"
        ),
        // Every other kind lives in an ALL_ view every account may read.
        _ => return None,
    };
    Some(sql)
}

// WHAT:  One listing row → ObjectSummary, per kind (column order from `object_list_sql`).
pub fn summarize(kind: ObjectKind, row: &[Value]) -> ObjectSummary {
    let t = |i: usize| cell_text(row.get(i));
    match kind {
        ObjectKind::Schema => ObjectSummary::new(kind, t(0), None).with_detail(format!("created {}", t(1))),
        ObjectKind::Table => {
            let mut parts = Vec::new();
            if let Some(rows) = cell_f64(row.get(2)) {
                parts.push(format!("~{} rows", format_number(rows)));
            }
            if let Some(space) = cell_opt(row.get(3)) {
                parts.push(space);
            }
            let badge = if t(5).eq_ignore_ascii_case("Y") {
                "temporary"
            } else if t(4).eq_ignore_ascii_case("YES") {
                "partitioned"
            } else {
                "table"
            };
            ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(parts.join(" · ")).with_badge(badge)
        }
        ObjectKind::View => {
            let detail = cell_f64(row.get(2)).map(|n| format!("{} chars", format_number(n))).unwrap_or_default();
            ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(detail)
        }
        ObjectKind::MaterializedView => {
            let mut parts = vec![format!("{} refresh", t(3).to_ascii_lowercase())];
            if let Some(last) = cell_opt(row.get(4)) {
                parts.push(format!("last {last}"));
            }
            ObjectSummary::new(kind, t(1), Some(t(0)))
                .with_detail(parts.join(" · "))
                .with_badge(t(5).to_ascii_lowercase())
        }
        ObjectKind::Index => {
            let unique = t(4).eq_ignore_ascii_case("UNIQUE");
            let badge = if unique { "unique".to_string() } else { t(3).to_ascii_lowercase() };
            let mut detail = format!("{} ({})", t(2), t(7));
            if !t(5).eq_ignore_ascii_case("VALID") && !t(5).is_empty() {
                detail.push_str(&format!(" · {}", t(5).to_ascii_lowercase()));
            }
            ObjectSummary::new(kind, t(1), Some(owner_key(&t(0), &t(2)))).with_detail(detail).with_badge(badge)
        }
        ObjectKind::Constraint => {
            let badge = constraint_kind(&t(3));
            let mut detail = format!("{} ({})", t(2), t(5));
            if let Some(referenced) = cell_opt(row.get(6)) {
                detail.push_str(&format!(" → {referenced}"));
                if let Some(rule) = cell_opt(row.get(7)) {
                    detail.push_str(&format!(" · on delete {}", rule.to_ascii_lowercase()));
                }
            }
            if t(4).eq_ignore_ascii_case("DISABLED") {
                detail.push_str(" · disabled");
            }
            ObjectSummary::new(kind, t(1), Some(owner_key(&t(0), &t(2)))).with_detail(detail).with_badge(badge)
        }
        ObjectKind::Sequence => ObjectSummary::new(kind, t(1), Some(t(0)))
            .with_detail(format!("last {} · by {}", t(2), t(3)))
            .with_badge(if t(6).eq_ignore_ascii_case("Y") { "cycle" } else { "nocycle" }),
        ObjectKind::Type => {
            let mut summary = ObjectSummary::new(kind, t(1), Some(t(0))).with_badge(t(2).to_ascii_lowercase());
            if let Some(attributes) = cell_f64(row.get(3)) {
                summary = summary.with_detail(format!("{} attributes", format_number(attributes)));
            }
            summary
        }
        ObjectKind::Function | ObjectKind::Procedure | ObjectKind::Package => ObjectSummary::new(kind, t(1), Some(t(0)))
            .with_detail(format!("changed {}", t(4)))
            .with_badge(t(2).to_ascii_lowercase()),
        ObjectKind::Trigger => {
            let badge = if t(5).eq_ignore_ascii_case("DISABLED") { "disabled".to_string() } else { t(3).to_ascii_lowercase() };
            ObjectSummary::new(kind, t(1), Some(owner_key(&t(0), &t(2))))
                .with_detail(format!("{} {} ON {}", t(3), t(4), t(2)))
                .with_badge(badge)
        }
        ObjectKind::Alias => {
            let target = format!("{}.{}", t(2), t(3));
            let mut detail = format!("→ {target}");
            if let Some(link) = cell_opt(row.get(4)) {
                detail.push_str(&format!("@{link}"));
            }
            ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(detail).with_badge("synonym")
        }
        ObjectKind::Tablespace => {
            let mut parts = vec![t(2).to_ascii_lowercase()];
            if let Some(size) = cell_f64(row.get(4)) {
                parts.push(human_bytes(size));
            }
            ObjectSummary::new(kind, t(0), None).with_detail(parts.join(" · ")).with_badge(t(1).to_ascii_lowercase())
        }
        ObjectKind::User => {
            let mut parts = Vec::new();
            if let Some(space) = cell_opt(row.get(2)) {
                parts.push(space);
            }
            if let Some(profile) = cell_opt(row.get(4)) {
                parts.push(profile.to_ascii_lowercase());
            }
            let mut summary = ObjectSummary::new(kind, t(0), None).with_detail(parts.join(" · "));
            if let Some(status) = cell_opt(row.get(1)) {
                summary = summary.with_badge(status.to_ascii_lowercase());
            }
            summary
        }
        ObjectKind::Role => {
            let mut summary = ObjectSummary::new(kind, t(0), None);
            if let Some(auth) = cell_opt(row.get(1)) {
                summary = summary.with_badge(auth.to_ascii_lowercase());
            }
            if t(2).eq_ignore_ascii_case("YES") {
                summary = summary.with_detail("with admin option");
            }
            summary
        }
        ObjectKind::Grant => {
            // grantee, privilege, object, grantable, kind
            let is_role = t(4).eq_ignore_ascii_case("ROLE");
            let name = if is_role { t(1) } else { format!("{} ON {}", t(1), t(2)) };
            let mut detail = format!("TO {}", t(0));
            if t(3).eq_ignore_ascii_case("YES") {
                detail.push_str(" · grantable");
            }
            ObjectSummary::new(kind, name, Some(t(0))).with_detail(detail).with_badge(if is_role { "role" } else { "object" })
        }
        ObjectKind::Session => {
            // sid, serial#, username, status, osuser, machine, program, event, wait, logon, sql_id, sql_text
            let mut parts = Vec::new();
            if let Some(user) = cell_opt(row.get(2)) {
                parts.push(format!("{user}@{}", t(5)));
            }
            if let Some(program) = cell_opt(row.get(6)) {
                parts.push(preview(&program, 30));
            }
            if let Some(event) = cell_opt(row.get(7)) {
                parts.push(format!("{event} {}s", t(8)));
            }
            if let Some(sql) = cell_opt(row.get(11)) {
                parts.push(preview(&sql, PREVIEW_CHARS));
            }
            ObjectSummary::new(kind, format!("{},{}", t(0), t(1)), None)
                .with_detail(parts.join(" · "))
                .with_badge(t(3).to_ascii_lowercase())
        }
        ObjectKind::Lock => {
            // sid, username, type, lmode, request, ctime, block, object owner, object name
            let target = match (cell_opt(row.get(7)), cell_opt(row.get(8))) {
                (Some(o), Some(n)) => format!("{o}.{n}"),
                (_, Some(n)) => n,
                _ => t(2),
            };
            let mut parts = vec![format!("held {}", lock_mode(cell_f64(row.get(3)).unwrap_or(0.0) as i64))];
            if cell_f64(row.get(4)).unwrap_or(0.0) > 0.0 {
                parts.push(format!("waiting {}", lock_mode(cell_f64(row.get(4)).unwrap_or(0.0) as i64)));
            }
            if let Some(user) = cell_opt(row.get(1)) {
                parts.push(user);
            }
            if let Some(secs) = cell_f64(row.get(5)) {
                parts.push(human_duration(secs.max(0.0) as u64));
            }
            if cell_f64(row.get(6)).unwrap_or(0.0) > 0.0 {
                parts.push("blocking others".to_string());
            }
            ObjectSummary::new(kind, format!("{} {} {target}", t(0), t(2)), None)
                .with_detail(parts.join(" · "))
                .with_badge(t(2).to_ascii_lowercase())
        }
        ObjectKind::Setting => {
            let mut summary = ObjectSummary::new(kind, t(0), None).with_detail(preview(&t(1), 100));
            if t(2).eq_ignore_ascii_case("FALSE") {
                summary = summary.with_badge("modified");
            }
            summary
        }
        ObjectKind::Job => {
            // owner, job_name, enabled, state, repeat_interval, last_start, next_run, run_count, failure_count
            let mut parts = Vec::new();
            if let Some(interval) = cell_opt(row.get(4)) {
                parts.push(preview(&interval, 40));
            }
            if let Some(next) = cell_opt(row.get(6)) {
                parts.push(format!("next {next}"));
            }
            if let Some(failures) = cell_f64(row.get(8)) {
                if failures > 0.0 {
                    parts.push(format!("{} failures", format_number(failures)));
                }
            }
            let badge = if t(2).eq_ignore_ascii_case("FALSE") { "disabled".to_string() } else { t(3).to_ascii_lowercase() };
            ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(parts.join(" · ")).with_badge(badge)
        }
        _ => ObjectSummary::new(kind, t(0), None),
    }
}

// ---- server statistics -----------------------------------------------------

const STATS_INSTANCE_SQL: &str = "SELECT version, instance_name, host_name, status, database_status, startup_time, \
    (SYSDATE - startup_time) * 86400 FROM v$instance";
const STATS_SYSSTAT_SQL: &str = "SELECT name, value FROM v$sysstat WHERE name IN ('user commits', 'user rollbacks', 'execute count', \
    'parse count (total)', 'parse count (hard)', 'physical reads', 'physical writes', 'session logical reads', 'redo size', \
    'bytes sent via SQL*Net to client', 'bytes received via SQL*Net from client', 'sorts (disk)', 'sorts (memory)')";
const STATS_SGA_SQL: &str = "SELECT name, value FROM v$sga";
const STATS_PGA_SQL: &str = "SELECT name, value FROM v$pgastat WHERE name IN ('total PGA allocated', 'total PGA inuse', 'aggregate PGA target parameter')";
const STATS_SESSIONS_SQL: &str = "SELECT COUNT(*), SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END), \
    SUM(CASE WHEN type = 'BACKGROUND' THEN 1 ELSE 0 END), \
    (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'sessions') FROM v$session";
const STATS_TABLESPACE_SQL: &str = "SELECT df.tablespace_name, df.bytes, NVL(fs.bytes, 0) FROM \
    (SELECT tablespace_name, SUM(bytes) bytes FROM dba_data_files GROUP BY tablespace_name) df \
    LEFT JOIN (SELECT tablespace_name, SUM(bytes) bytes FROM dba_free_space GROUP BY tablespace_name) fs \
    ON fs.tablespace_name = df.tablespace_name ORDER BY df.tablespace_name";

// WHAT:  Raw rows of the stats queries, so `build_stats` is testable offline.
#[derive(Default)]
pub struct StatsInput {
    /// version, instance, host, status, database_status, startup_time, uptime_seconds
    pub instance: Vec<Value>,
    pub sysstat: BTreeMap<String, f64>,
    pub sga: BTreeMap<String, f64>,
    pub pga: BTreeMap<String, f64>,
    /// total sessions, active, background, sessions parameter
    pub sessions: Option<Vec<Value>>,
    /// tablespace, bytes, free bytes
    pub tablespaces: Vec<Vec<Value>>,
}

pub fn build_stats(input: &StatsInput) -> Vec<StatGroup> {
    let stat = |label: &str, key: &str, unit: Option<&str>| input.sysstat.get(key).map(|v| Stat::number(label, *v, unit));

    let mut server = Vec::new();
    if let Some(version) = cell_opt(input.instance.first()) {
        let instance = cell_text(input.instance.get(1));
        server.push(Stat::text("Version", if instance.is_empty() { format!("Oracle {version}") } else { format!("Oracle {version} ({instance})") }));
    }
    if let Some(host) = cell_opt(input.instance.get(2)) {
        server.push(Stat::text("Host", host));
    }
    if let Some(status) = cell_opt(input.instance.get(3)) {
        server.push(Stat::text("Status", status));
    }
    if let Some(uptime) = cell_f64(input.instance.get(6)) {
        server.push(Stat::text("Uptime", human_duration(uptime.max(0.0) as u64)));
    }
    if let Some(started) = cell_opt(input.instance.get(5)) {
        server.push(Stat::text("Started", started));
    }

    let mut sessions = Vec::new();
    if let Some(counts) = &input.sessions {
        for (i, label) in ["Sessions", "Active", "Background"].iter().enumerate() {
            if let Some(n) = cell_f64(counts.get(i)) {
                sessions.push(Stat::number(label, n, None));
            }
        }
        if let Some(limit) = cell_f64(counts.get(3)) {
            sessions.push(Stat::number("Sessions parameter", limit, None).with_hint("the `sessions` initialisation parameter"));
        }
    }

    let mut memory = Vec::new();
    let sga_total: f64 = input.sga.values().sum();
    if sga_total > 0.0 {
        memory.push(bytes_stat("SGA total", sga_total));
        for (name, value) in &input.sga {
            memory.push(bytes_stat(name, *value));
        }
    }
    for (name, value) in &input.pga {
        memory.push(bytes_stat(name, *value));
    }

    let mut queries = Vec::new();
    queries.extend(stat("User commits", "user commits", None));
    queries.extend(stat("User rollbacks", "user rollbacks", None));
    queries.extend(stat("Executions", "execute count", None));
    queries.extend(stat("Parses", "parse count (total)", None));
    queries.extend(stat("Hard parses", "parse count (hard)", None));
    queries.extend(stat("Sorts (memory)", "sorts (memory)", None));
    queries.extend(stat("Sorts (disk)", "sorts (disk)", None));

    let mut io = Vec::new();
    io.extend(stat("Logical reads", "session logical reads", Some("blocks")));
    io.extend(stat("Physical reads", "physical reads", Some("blocks")));
    io.extend(stat("Physical writes", "physical writes", Some("blocks")));
    if let (Some(logical), Some(physical)) = (input.sysstat.get("session logical reads"), input.sysstat.get("physical reads")) {
        let ratio = if *logical > 0.0 { (1.0 - physical / logical) * 100.0 } else { 100.0 };
        io.push(Stat::number("Buffer cache hit ratio", (ratio * 100.0).round() / 100.0, Some("%")).with_hint("logical reads served without a physical read"));
    }
    if let Some(redo) = input.sysstat.get("redo size") {
        io.push(bytes_stat("Redo written", *redo));
    }
    if let Some(sent) = input.sysstat.get("bytes sent via SQL*Net to client") {
        io.push(bytes_stat("Bytes sent", *sent));
    }
    if let Some(received) = input.sysstat.get("bytes received via SQL*Net from client") {
        io.push(bytes_stat("Bytes received", *received));
    }

    let mut storage = Vec::new();
    if !input.tablespaces.is_empty() {
        let mut total = 0.0;
        let mut free = 0.0;
        for row in &input.tablespaces {
            let size = cell_f64(row.get(1)).unwrap_or(0.0);
            let unused = cell_f64(row.get(2)).unwrap_or(0.0);
            total += size;
            free += unused;
            let used = (size - unused).max(0.0);
            let percent = if size > 0.0 { used / size * 100.0 } else { 0.0 };
            storage.push(
                Stat::number(&cell_text(row.first()), (percent * 100.0).round() / 100.0, Some("% used"))
                    .with_hint(format!("{} of {}", human_bytes(used), human_bytes(size))),
            );
        }
        storage.insert(0, bytes_stat("Allocated", total));
        storage.insert(1, bytes_stat("Free", free));
    }

    let groups = [
        ("Server", server),
        ("Sessions", sessions),
        ("Memory", memory),
        ("Queries", queries),
        ("I/O", io),
        ("Storage", storage),
    ];
    groups
        .into_iter()
        .filter(|(_, stats)| !stats.is_empty())
        .map(|(title, stats)| StatGroup { title: title.to_string(), stats })
        .collect()
}

impl OracleIntegration {
    async fn query_set(&self, sql: String) -> AppResult<ResultSet> {
        match self.blocking(move |conn| run_statement(conn, &sql, usize::MAX)).await? {
            StatementResult::Rows { result } => Ok(result),
            StatementResult::Affected { .. } => Ok(ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }),
        }
    }

    async fn query_rows(&self, sql: String) -> AppResult<Vec<Vec<Value>>> {
        Ok(self.query_set(sql).await?.rows)
    }

    async fn scalar_text(&self, sql: String) -> Option<String> {
        self.query_rows(sql).await.ok().and_then(|rows| rows.first().and_then(|r| cell_opt(r.first())))
    }

    async fn property_sheet(&self, sql: String) -> Vec<ObjectProperty> {
        match self.query_set(sql).await {
            Ok(set) => properties_of(&set),
            Err(_) => Vec::new(),
        }
    }

    // WHAT:  Primary dictionary query, its fallback, then a hint naming the view.
    async fn rows_with_fallback(&self, kind: ObjectKind, primary: String, fallback: Option<String>) -> AppResult<Vec<Vec<Value>>> {
        match self.query_rows(primary).await {
            Ok(rows) => Ok(rows),
            Err(err) if is_privilege_error(&err) => match fallback {
                Some(sql) => match self.query_rows(sql).await {
                    Ok(rows) => Ok(rows),
                    Err(second) if is_privilege_error(&second) => Err(AppError::invalid_input(format!("{} ({second})", privilege_hint(kind)))),
                    Err(second) => Err(second),
                },
                None => Err(AppError::invalid_input(format!("{} ({err})", privilege_hint(kind)))),
            },
            Err(err) => Err(err),
        }
    }

    async fn list_kind(&self, kind: ObjectKind, owner: Option<&str>, table: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let Some(primary) = object_list_sql(kind, owner, table) else {
            return Ok(Vec::new());
        };
        let rows = self.rows_with_fallback(kind, primary, object_list_fallback_sql(kind, owner)).await?;
        let mut items: Vec<ObjectSummary> = rows.iter().map(|r| summarize(kind, r)).collect();
        if matches!(kind, ObjectKind::Session | ObjectKind::Lock | ObjectKind::Grant) {
            dedupe_names(&mut items);
        } else {
            items.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        }
        Ok(items)
    }

    // WHAT:  DBMS_METADATA DDL, or None when the account may not call it.
    async fn metadata_ddl(&self, object_type: &str, name: &str, owner: &str) -> Option<String> {
        let (object_type, name, owner) = (object_type.to_string(), name.to_string(), owner.to_string());
        self.blocking(move |conn| {
            let ddl: String = conn
                .query_row_as::<String>("SELECT DBMS_METADATA.GET_DDL(:1, :2, :3) FROM DUAL", &[&object_type, &name, &owner])
                .map_err(|e| map_error(&e))?;
            Ok(ddl.trim().to_string())
        })
        .await
        .ok()
        .filter(|d| !d.is_empty())
    }

    // WHAT:  ALL_SOURCE body of a PL/SQL unit, the fallback when DBMS_METADATA is refused.
    async fn source_text(&self, owner: &str, name: &str, object_type: &str) -> Option<String> {
        let sql = format!(
            "SELECT text FROM all_source WHERE owner = {} AND name = {} AND type = {} ORDER BY line",
            quote_literal(owner),
            quote_literal(name),
            quote_literal(object_type)
        );
        let rows = self.query_rows(sql).await.ok()?;
        let text: String = rows.iter().map(|r| cell_text(r.first())).collect();
        if text.trim().is_empty() {
            None
        } else {
            Some(format!("CREATE OR REPLACE {}", text.trim_start()))
        }
    }

    async fn schema_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let literal = quote_literal(name);
        let mut detail = ObjectDetail::empty(reference).property("schema", name);
        detail.properties.extend(
            self.property_sheet(format!(
                "SELECT (SELECT COUNT(*) FROM all_tables t WHERE t.owner = u.username) AS tables, \
                 (SELECT COUNT(*) FROM all_views v WHERE v.owner = u.username) AS views, \
                 (SELECT COUNT(*) FROM all_objects o WHERE o.owner = u.username AND o.object_type IN ('FUNCTION', 'PROCEDURE', 'PACKAGE')) AS code_units, \
                 u.created FROM all_users u WHERE u.username = {literal}"
            ))
            .await,
        );
        Ok(detail.action(ObjectAction::destructive("drop", "Drop schema (cascade)", format!("DROP USER {} CASCADE", ident(name)))))
    }

    async fn table_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(ddl) = self.metadata_ddl("TABLE", name, owner).await {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(format!(
                "SELECT tablespace_name, num_rows, blocks, avg_row_len, partitioned, temporary, logging, last_analyzed \
                 FROM all_tables WHERE owner = {} AND table_name = {}",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await;
        detail.columns = self.columns(&TableRef { schema: Some(owner.to_string()), name: name.to_string() }).await?;
        for kind in [ObjectKind::Index, ObjectKind::Constraint, ObjectKind::Trigger] {
            if let Ok(children) = self.list_kind(kind, Some(owner), Some(name)).await {
                detail.children.extend(children);
            }
        }
        Ok(detail
            .action(ObjectAction::new("analyze", "Analyze table", format!("ANALYZE TABLE {target} COMPUTE STATISTICS")))
            .action(ObjectAction::destructive("truncate", "Truncate table", format!("TRUNCATE TABLE {target}")))
            .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {target} CASCADE CONSTRAINTS"))))
    }

    async fn view_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let mut detail = ObjectDetail::empty(reference);
        let ddl = match self.metadata_ddl("VIEW", name, owner).await {
            Some(ddl) => Some(ddl),
            None => self
                .scalar_text(format!(
                    "SELECT text FROM all_views WHERE owner = {} AND view_name = {}",
                    quote_literal(owner),
                    quote_literal(name)
                ))
                .await
                .map(|text| format!("CREATE OR REPLACE VIEW {target} AS\n{text}")),
        };
        if let Some(ddl) = ddl {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.columns = self.columns(&TableRef { schema: Some(owner.to_string()), name: name.to_string() }).await.unwrap_or_default();
        Ok(detail.action(ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {target}"))))
    }

    async fn mview_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(ddl) = self.metadata_ddl("MATERIALIZED_VIEW", name, owner).await {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(format!(
                "SELECT refresh_mode, refresh_method, build_mode, last_refresh_type, last_refresh_date, staleness, compile_state \
                 FROM all_mviews WHERE owner = {} AND mview_name = {}",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await;
        detail.columns = self.columns(&TableRef { schema: Some(owner.to_string()), name: name.to_string() }).await.unwrap_or_default();
        Ok(detail
            .action(ObjectAction::destructive(
                "refresh",
                "Refresh materialized view",
                format!("BEGIN DBMS_MVIEW.REFRESH({}); END;", quote_literal(&format!("{owner}.{name}"))),
            ))
            .action(ObjectAction::destructive("drop", "Drop materialized view", format!("DROP MATERIALIZED VIEW {target}"))))
    }

    async fn index_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let set = self
            .query_set(format!(
                "SELECT table_owner, table_name, index_type, uniqueness, status, tablespace_name, num_rows, distinct_keys, last_analyzed \
                 FROM all_indexes WHERE owner = {} AND index_name = {}",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = properties_of(&set);
        let columns = self
            .query_set(format!(
                "SELECT column_position, column_name, descend FROM all_ind_columns \
                 WHERE index_owner = {} AND index_name = {} ORDER BY column_position",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await
            .ok();
        if let (Some(row), Some(cols)) = (set.rows.first(), columns.as_ref()) {
            let table_owner = set_text(&set, row, "TABLE_OWNER");
            let table_name = set_text(&set, row, "TABLE_NAME");
            let unique = set_text(&set, row, "UNIQUENESS").eq_ignore_ascii_case("UNIQUE");
            let names: Vec<String> = cols
                .rows
                .iter()
                .map(|r| {
                    let column = ident(&cell_text(r.get(1)));
                    if cell_text(r.get(2)).eq_ignore_ascii_case("DESC") {
                        format!("{column} DESC")
                    } else {
                        column
                    }
                })
                .collect();
            detail = detail.definition(
                format!(
                    "CREATE {}INDEX {target} ON {} ({})",
                    if unique { "UNIQUE " } else { "" },
                    qualified(&table_owner, &table_name),
                    names.join(", ")
                ),
                CodeLanguage::Sql,
            );
        }
        detail.rows = columns;
        Ok(detail
            .action(ObjectAction::destructive("rebuild", "Rebuild index", format!("ALTER INDEX {target} REBUILD")))
            .action(ObjectAction::destructive("drop", "Drop index", format!("DROP INDEX {target}"))))
    }

    async fn constraint_detail(&self, reference: &ObjectRef, owner: &str, table: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, table);
        let quoted = ident(name);
        let sql = object_list_sql(ObjectKind::Constraint, Some(owner), Some(table)).unwrap_or_default();
        let rows = self.query_rows(sql).await?;
        let Some(row) = rows.iter().find(|r| cell_text(r.get(1)) == name) else {
            return Err(AppError::not_found(format!("Constraint {name} on {table} was not found.")));
        };
        let code = cell_text(row.get(3));
        let kind_text = constraint_kind(&code);
        let columns = cell_text(row.get(5));
        let quoted_cols: Vec<String> = columns.split(", ").filter(|c| !c.is_empty()).map(ident).collect();
        let mut detail = ObjectDetail::empty(reference)
            .property("table", table)
            .property("type", kind_text)
            .property("status", cell_text(row.get(4)).to_ascii_lowercase());
        if !columns.is_empty() {
            detail = detail.property("columns", columns.clone());
        }
        match code.as_str() {
            "P" => detail = detail.definition(format!("ALTER TABLE {target} ADD CONSTRAINT {quoted} PRIMARY KEY ({})", quoted_cols.join(", ")), CodeLanguage::Sql),
            "U" => detail = detail.definition(format!("ALTER TABLE {target} ADD CONSTRAINT {quoted} UNIQUE ({})", quoted_cols.join(", ")), CodeLanguage::Sql),
            "R" => {
                let referenced = cell_text(row.get(6));
                let ref_cols = self
                    .query_rows(format!(
                        "SELECT rc.column_name FROM all_constraints c JOIN all_cons_columns rc \
                         ON rc.owner = c.r_owner AND rc.constraint_name = c.r_constraint_name \
                         WHERE c.owner = {} AND c.constraint_name = {} ORDER BY rc.position",
                        quote_literal(owner),
                        quote_literal(name)
                    ))
                    .await
                    .unwrap_or_default();
                let ref_cols: Vec<String> = ref_cols.iter().map(|r| ident(&cell_text(r.first()))).collect();
                let mut definition = format!(
                    "ALTER TABLE {target} ADD CONSTRAINT {quoted} FOREIGN KEY ({}) REFERENCES {} ({})",
                    quoted_cols.join(", "),
                    qualified(owner, &referenced),
                    ref_cols.join(", ")
                );
                if let Some(rule) = cell_opt(row.get(7)) {
                    if !rule.eq_ignore_ascii_case("NO ACTION") {
                        definition.push_str(&format!(" ON DELETE {rule}"));
                    }
                    detail = detail.property("on delete", rule.to_ascii_lowercase());
                }
                detail = detail.property("references", referenced).definition(definition, CodeLanguage::Sql);
            }
            _ => {
                // SEARCH_CONDITION is a LONG; 12.2+ mirrors it as SEARCH_CONDITION_VC.
                if let Some(condition) = self
                    .scalar_text(format!(
                        "SELECT search_condition_vc FROM all_constraints WHERE owner = {} AND constraint_name = {}",
                        quote_literal(owner),
                        quote_literal(name)
                    ))
                    .await
                {
                    detail = detail.definition(format!("ALTER TABLE {target} ADD CONSTRAINT {quoted} CHECK ({condition})"), CodeLanguage::Sql);
                }
            }
        }
        Ok(detail
            .action(ObjectAction::destructive("enable", "Enable constraint", format!("ALTER TABLE {target} ENABLE CONSTRAINT {quoted}")))
            .action(ObjectAction::destructive("disable", "Disable constraint", format!("ALTER TABLE {target} DISABLE CONSTRAINT {quoted}")))
            .action(ObjectAction::destructive("drop", "Drop constraint", format!("ALTER TABLE {target} DROP CONSTRAINT {quoted}"))))
    }

    async fn sequence_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let set = self
            .query_set(format!(
                "SELECT min_value, max_value, increment_by, cycle_flag, order_flag, cache_size, last_number \
                 FROM all_sequences WHERE sequence_owner = {} AND sequence_name = {}",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = properties_of(&set);
        if let Some(row) = set.rows.first() {
            detail = detail.definition(
                format!(
                    "CREATE SEQUENCE {target} START WITH {} INCREMENT BY {} MINVALUE {} MAXVALUE {} {} CACHE {}",
                    set_text(&set, row, "LAST_NUMBER"),
                    set_text(&set, row, "INCREMENT_BY"),
                    set_text(&set, row, "MIN_VALUE"),
                    set_text(&set, row, "MAX_VALUE"),
                    if set_text(&set, row, "CYCLE_FLAG").eq_ignore_ascii_case("Y") { "CYCLE" } else { "NOCYCLE" },
                    set_text(&set, row, "CACHE_SIZE")
                ),
                CodeLanguage::Sql,
            );
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop sequence", format!("DROP SEQUENCE {target}"))))
    }

    async fn type_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let mut detail = ObjectDetail::empty(reference);
        let ddl = match self.metadata_ddl("TYPE", name, owner).await {
            Some(ddl) => Some(ddl),
            None => self.source_text(owner, name, "TYPE").await,
        };
        if let Some(ddl) = ddl {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(format!(
                "SELECT typecode, attributes, methods, predefined, incomplete FROM all_types WHERE owner = {} AND type_name = {}",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await;
        detail.rows = self
            .query_set(format!(
                "SELECT attr_no, attr_name, attr_type_name, length, precision, scale FROM all_type_attrs \
                 WHERE owner = {} AND type_name = {} ORDER BY attr_no",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await
            .ok();
        Ok(detail.action(ObjectAction::destructive("drop", "Drop type", format!("DROP TYPE {target}"))))
    }

    async fn code_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let word = match reference.kind {
            ObjectKind::Function => "FUNCTION",
            ObjectKind::Procedure => "PROCEDURE",
            _ => "PACKAGE",
        };
        let mut detail = ObjectDetail::empty(reference);
        let ddl = match self.metadata_ddl(word, name, owner).await {
            Some(ddl) => Some(ddl),
            None => self.source_text(owner, name, word).await,
        };
        if let Some(ddl) = ddl {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(format!(
                "SELECT status, created, last_ddl_time, temporary, oracle_maintained FROM all_objects \
                 WHERE owner = {} AND object_name = {} AND object_type = {}",
                quote_literal(owner),
                quote_literal(name),
                quote_literal(word)
            ))
            .await;
        detail.rows = self
            .query_set(format!(
                "SELECT position, argument_name, data_type, in_out, defaulted FROM all_arguments \
                 WHERE owner = {} AND object_name = {} ORDER BY position",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await
            .ok();
        Ok(detail
            .action(ObjectAction::new("compile", "Compile", format!("ALTER {word} {target} COMPILE")))
            .action(ObjectAction::destructive("drop", &format!("Drop {}", word.to_ascii_lowercase()), format!("DROP {word} {target}"))))
    }

    async fn trigger_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let mut detail = ObjectDetail::empty(reference);
        let ddl = match self.metadata_ddl("TRIGGER", name, owner).await {
            Some(ddl) => Some(ddl),
            None => self
                .scalar_text(format!(
                    "SELECT trigger_body FROM all_triggers WHERE owner = {} AND trigger_name = {}",
                    quote_literal(owner),
                    quote_literal(name)
                ))
                .await,
        };
        if let Some(ddl) = ddl {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(format!(
                "SELECT table_owner, table_name, trigger_type, triggering_event, status, base_object_type, when_clause \
                 FROM all_triggers WHERE owner = {} AND trigger_name = {}",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await;
        Ok(detail
            .action(ObjectAction::destructive("enable", "Enable trigger", format!("ALTER TRIGGER {target} ENABLE")))
            .action(ObjectAction::destructive("disable", "Disable trigger", format!("ALTER TRIGGER {target} DISABLE")))
            .action(ObjectAction::destructive("drop", "Drop trigger", format!("DROP TRIGGER {target}"))))
    }

    async fn synonym_detail(&self, reference: &ObjectRef, owner: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(owner, name);
        let set = self
            .query_set(format!(
                "SELECT table_owner, table_name, db_link FROM all_synonyms WHERE owner = {} AND synonym_name = {}",
                quote_literal(owner),
                quote_literal(name)
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = properties_of(&set);
        if let Some(row) = set.rows.first() {
            let referenced = qualified(&set_text(&set, row, "TABLE_OWNER"), &set_text(&set, row, "TABLE_NAME"));
            let link = set_text(&set, row, "DB_LINK");
            let suffix = if link.is_empty() { String::new() } else { format!("@{}", ident(&link)) };
            detail = detail.definition(format!("CREATE OR REPLACE SYNONYM {target} FOR {referenced}{suffix}"), CodeLanguage::Sql);
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop synonym", format!("DROP SYNONYM {target}"))))
    }

    async fn tablespace_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let literal = quote_literal(name);
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = self
            .property_sheet(format!(
                "SELECT status, contents, extent_management, segment_space_management, block_size, logging, force_logging \
                 FROM dba_tablespaces WHERE tablespace_name = {literal}"
            ))
            .await;
        if detail.properties.is_empty() {
            detail.properties = self
                .property_sheet(format!(
                    "SELECT status, contents, extent_management, segment_space_management, block_size FROM user_tablespaces WHERE tablespace_name = {literal}"
                ))
                .await;
        }
        detail.rows = self
            .query_set(format!(
                "SELECT file_name, bytes, maxbytes, autoextensible, status FROM dba_data_files WHERE tablespace_name = {literal} ORDER BY file_name"
            ))
            .await
            .ok();
        Ok(detail
            .action(ObjectAction::destructive("offline", "Take offline", format!("ALTER TABLESPACE {} OFFLINE", ident(name))))
            .action(ObjectAction::destructive("online", "Bring online", format!("ALTER TABLESPACE {} ONLINE", ident(name)))))
    }

    async fn user_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let literal = quote_literal(name);
        let quoted = ident(name);
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = self
            .property_sheet(format!(
                "SELECT account_status, default_tablespace, temporary_tablespace, profile, created, lock_date, expiry_date \
                 FROM dba_users WHERE username = {literal}"
            ))
            .await;
        if detail.properties.is_empty() {
            detail.properties = self.property_sheet(format!("SELECT created FROM all_users WHERE username = {literal}")).await;
        }
        let grants = self
            .query_set(format!(
                "SELECT * FROM (\
                 SELECT 'ROLE' AS grant_kind, granted_role AS privilege, NULL AS object_name, admin_option FROM dba_role_privs WHERE grantee = {literal} \
                 UNION ALL SELECT 'SYSTEM', privilege, NULL, admin_option FROM dba_sys_privs WHERE grantee = {literal} \
                 UNION ALL SELECT 'OBJECT', privilege, owner || '.' || table_name, grantable FROM dba_tab_privs WHERE grantee = {literal}) ORDER BY 1, 2"
            ))
            .await
            .ok();
        if let Some(set) = &grants {
            let lines: Vec<String> = set
                .rows
                .iter()
                .map(|r| format!("GRANT {}{} TO {quoted}", cell_text(r.get(1)), cell_opt(r.get(2)).map(|o| format!(" ON {o}")).unwrap_or_default()))
                .collect();
            if !lines.is_empty() {
                detail = detail.definition(lines.join(";\n"), CodeLanguage::Sql);
            }
        }
        detail.rows = grants;
        Ok(detail
            .action(ObjectAction::destructive("lock", "Lock account", format!("ALTER USER {quoted} ACCOUNT LOCK")))
            .action(ObjectAction::destructive("unlock", "Unlock account", format!("ALTER USER {quoted} ACCOUNT UNLOCK")))
            .action(ObjectAction::destructive("drop", "Drop user (cascade)", format!("DROP USER {quoted} CASCADE"))))
    }

    async fn role_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let literal = quote_literal(name);
        let quoted = ident(name);
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = self.property_sheet(format!("SELECT authentication_type, password_required FROM dba_roles WHERE role = {literal}")).await;
        detail.rows = self
            .query_set(format!(
                "SELECT * FROM (\
                 SELECT 'SYSTEM' AS grant_kind, privilege, NULL AS object_name FROM dba_sys_privs WHERE grantee = {literal} \
                 UNION ALL SELECT 'OBJECT', privilege, owner || '.' || table_name FROM dba_tab_privs WHERE grantee = {literal} \
                 UNION ALL SELECT 'MEMBER', grantee, NULL FROM dba_role_privs WHERE granted_role = {literal}) ORDER BY 1, 2"
            ))
            .await
            .ok();
        Ok(detail.action(ObjectAction::destructive("drop", "Drop role", format!("DROP ROLE {quoted}"))))
    }

    async fn grant_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let grantee = reference.parent.clone().unwrap_or_default();
        let Some(primary) = object_list_sql(ObjectKind::Grant, None, None) else {
            return Ok(ObjectDetail::empty(reference));
        };
        let rows = self.rows_with_fallback(ObjectKind::Grant, primary, object_list_fallback_sql(ObjectKind::Grant, None)).await?;
        let wanted = reference.name.split(" (").next().unwrap_or(&reference.name);
        let Some(row) = rows
            .iter()
            .find(|r| cell_text(r.first()) == grantee && summarize(ObjectKind::Grant, r).reference.name.starts_with(wanted))
        else {
            return Err(AppError::not_found("That grant no longer exists."));
        };
        let privilege = cell_text(row.get(1));
        let object = cell_opt(row.get(2));
        let quoted_grantee = ident(&grantee);
        let on = object
            .as_deref()
            .map(|o| {
                o.split_once('.')
                    .map(|(owner, name)| format!(" ON {}", qualified(owner, name)))
                    .unwrap_or_else(|| format!(" ON {}", ident(o)))
            })
            .unwrap_or_default();
        let mut detail = ObjectDetail::empty(reference)
            .definition(format!("GRANT {privilege}{on} TO {quoted_grantee}"), CodeLanguage::Sql)
            .property("grantee", grantee)
            .property("privilege", privilege.clone())
            .property("kind", cell_text(row.get(4)).to_ascii_lowercase());
        if let Some(object) = object {
            detail = detail.property("on", object);
        }
        Ok(detail.action(ObjectAction::destructive("revoke", "Revoke", format!("REVOKE {privilege}{on} FROM {quoted_grantee}"))))
    }

    async fn session_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let (sid, serial) = reference
            .name
            .split_once(',')
            .ok_or_else(|| AppError::invalid_input("A session is identified by `sid,serial#`."))?;
        let sid: i64 = sid.trim().parse().map_err(|_| AppError::invalid_input("Session ids are numeric."))?;
        let serial: i64 = serial.trim().parse().map_err(|_| AppError::invalid_input("Session serial numbers are numeric."))?;
        let set = self
            .query_set(format!(
                "SELECT s.sid, s.serial#, s.username, s.status, s.osuser, s.machine, s.program, s.module, s.type, \
                 s.logon_time, s.last_call_et, s.event, s.wait_class, s.seconds_in_wait, s.blocking_session, s.sql_id, \
                 (SELECT q.sql_fulltext FROM v$sql q WHERE q.sql_id = s.sql_id AND ROWNUM = 1) AS sql_text \
                 FROM v$session s WHERE s.sid = {sid} AND s.serial# = {serial}"
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(row) = set.rows.first() {
            let sql = set_text(&set, row, "SQL_TEXT");
            if !sql.is_empty() {
                detail = detail.definition(sql, CodeLanguage::Sql);
            }
        }
        detail.properties = properties_of(&set).into_iter().filter(|p| p.name != "sql text").collect();
        let session = format!("{sid},{serial}");
        Ok(detail
            .action(ObjectAction::destructive("kill", "Kill session", format!("ALTER SYSTEM KILL SESSION {}", quote_literal(&session))))
            .action(ObjectAction::destructive(
                "disconnect",
                "Disconnect session",
                format!("ALTER SYSTEM DISCONNECT SESSION {} IMMEDIATE", quote_literal(&session)),
            )))
    }

    async fn lock_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let items = self.list_kind(ObjectKind::Lock, None, None).await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(item) = items.iter().find(|i| i.reference.name == reference.name) {
            if let Some(text) = &item.detail {
                detail = detail.definition(text.clone(), CodeLanguage::Text);
            }
            if let Some(badge) = &item.badge {
                detail = detail.property("type", badge.clone());
            }
        }
        if let Some(sid) = reference.name.split(' ').next().and_then(|s| s.parse::<i64>().ok()) {
            detail = detail.property("session", sid.to_string());
            if let Some(serial) = self.scalar_text(format!("SELECT serial# FROM v$session WHERE sid = {sid}")).await {
                let session = format!("{sid},{serial}");
                detail = detail.action(ObjectAction::destructive("kill", "Kill holding session", format!("ALTER SYSTEM KILL SESSION {}", quote_literal(&session))));
            }
        }
        Ok(detail)
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let set = self
            .query_set(format!(
                "SELECT value, display_value, isdefault, isses_modifiable, issys_modifiable, ismodified, description \
                 FROM v$parameter WHERE name = {}",
                quote_literal(name)
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = properties_of(&set);
        if let Some(row) = set.rows.first() {
            let value = set_text(&set, row, "VALUE");
            detail = detail.definition(format!("ALTER SYSTEM SET {name} = {}", quote_literal(&value)), CodeLanguage::Sql);
            let modifiable = set_text(&set, row, "ISSYS_MODIFIABLE");
            if !modifiable.eq_ignore_ascii_case("FALSE") {
                detail = detail.action(ObjectAction::destructive("set", "Apply this value", format!("ALTER SYSTEM SET {name} = {}", quote_literal(&value))));
            }
        }
        Ok(detail)
    }

    async fn job_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let owner = reference.parent.clone().unwrap_or_else(|| self.current_schema.clone());
        let full = format!("{owner}.{name}");
        let literal = quote_literal(&full);
        let mut detail = ObjectDetail::empty(reference);
        let set = self
            .query_set(format!(
                "SELECT job_action, job_type, enabled, state, repeat_interval, start_date, last_start_date, next_run_date, \
                 run_count, failure_count, last_run_duration, comments \
                 FROM all_scheduler_jobs WHERE owner = {} AND job_name = {}",
                quote_literal(&owner),
                quote_literal(name)
            ))
            .await
            .unwrap_or(ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false });
        detail.properties = properties_of(&set).into_iter().filter(|p| p.name != "job action").collect();
        if let Some(row) = set.rows.first() {
            let action = set_text(&set, row, "JOB_ACTION");
            if !action.is_empty() {
                detail = detail.definition(action, CodeLanguage::Sql);
            }
        }
        Ok(detail
            .action(ObjectAction::new("run", "Run now", format!("BEGIN DBMS_SCHEDULER.RUN_JOB({literal}); END;")))
            .action(ObjectAction::destructive("enable", "Enable job", format!("BEGIN DBMS_SCHEDULER.ENABLE({literal}); END;")))
            .action(ObjectAction::destructive("disable", "Disable job", format!("BEGIN DBMS_SCHEDULER.DISABLE({literal}); END;")))
            .action(ObjectAction::destructive("drop", "Drop job", format!("BEGIN DBMS_SCHEDULER.DROP_JOB({literal}); END;"))))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities::SQL,
        object_kinds: vec![K::Schema, K::Table, K::View, K::MaterializedView, K::Index, K::Constraint, K::Sequence, K::Type, K::Function, K::Procedure, K::Package, K::Trigger, K::Alias, K::Tablespace, K::User, K::Role, K::Grant, K::Session, K::Lock, K::Setting, K::Job],
        tools: vec![T::Stats, T::Erd],
    }
}

#[async_trait]
impl Integration for OracleIntegration {
    fn engine(&self) -> Engine {
        Engine::Oracle
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
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

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let (owner, table) = split_owner(parent);
        self.list_kind(kind, owner, table).await
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let (owner, table) = split_owner(reference.parent.as_deref());
        let owner = owner.unwrap_or(self.current_schema.as_str()).to_string();
        match reference.kind {
            ObjectKind::Schema => self.schema_detail(reference).await,
            ObjectKind::Table => self.table_detail(reference, &owner).await,
            ObjectKind::View => self.view_detail(reference, &owner).await,
            ObjectKind::MaterializedView => self.mview_detail(reference, &owner).await,
            ObjectKind::Index => self.index_detail(reference, &owner).await,
            ObjectKind::Sequence => self.sequence_detail(reference, &owner).await,
            ObjectKind::Type => self.type_detail(reference, &owner).await,
            ObjectKind::Function | ObjectKind::Procedure | ObjectKind::Package => self.code_detail(reference, &owner).await,
            ObjectKind::Trigger => self.trigger_detail(reference, &owner).await,
            ObjectKind::Alias => self.synonym_detail(reference, &owner).await,
            ObjectKind::Tablespace => self.tablespace_detail(reference).await,
            ObjectKind::User => self.user_detail(reference).await,
            ObjectKind::Role => self.role_detail(reference).await,
            ObjectKind::Grant => self.grant_detail(reference).await,
            ObjectKind::Session => self.session_detail(reference).await,
            ObjectKind::Lock => self.lock_detail(reference).await,
            ObjectKind::Setting => self.setting_detail(reference).await,
            ObjectKind::Job => self.job_detail(reference).await,
            ObjectKind::Constraint => {
                let Some(table) = table else {
                    return Err(AppError::invalid_input("Open this constraint from its table so the owner is known."));
                };
                self.constraint_detail(reference, &owner, table).await
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        // V$ views need SELECT_CATALOG_ROLE; each part is optional so a plain
        // account still gets whatever it can read.
        let name_value = |rows: Vec<Vec<Value>>| -> BTreeMap<String, f64> {
            rows.iter()
                .filter_map(|r| cell_f64(r.get(1)).map(|v| (cell_text(r.first()), v)))
                .collect()
        };
        let instance = self.query_rows(STATS_INSTANCE_SQL.to_string()).await;
        if let Err(err) = &instance {
            if is_privilege_error(err) {
                return Err(AppError::invalid_input(format!(
                    "V$INSTANCE is not readable by this account, so server statistics are unavailable. Ask for SELECT_CATALOG_ROLE. ({err})"
                )));
            }
        }
        let input = StatsInput {
            instance: instance?.into_iter().next().unwrap_or_default(),
            sysstat: name_value(self.query_rows(STATS_SYSSTAT_SQL.to_string()).await.unwrap_or_default()),
            sga: name_value(self.query_rows(STATS_SGA_SQL.to_string()).await.unwrap_or_default()),
            pga: name_value(self.query_rows(STATS_PGA_SQL.to_string()).await.unwrap_or_default()),
            sessions: self.query_rows(STATS_SESSIONS_SQL.to_string()).await.ok().and_then(|rows| rows.into_iter().next()),
            tablespaces: self.query_rows(STATS_TABLESPACE_SQL.to_string()).await.unwrap_or_default(),
        };
        Ok(ServerStats::now(build_stats(&input)))
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


    // ---- object explorer (offline) --------------------------------------------

    fn text(s: &str) -> Value {
        Value::Text(s.into())
    }

    #[test]
    fn owner_keys_split_on_the_first_dot() {
        assert_eq!(split_owner(None), (None, None));
        assert_eq!(split_owner(Some(" ")), (None, None));
        assert_eq!(split_owner(Some("HR")), (Some("HR"), None));
        assert_eq!(split_owner(Some("HR.EMPLOYEES")), (Some("HR"), Some("EMPLOYEES")));
        assert_eq!(owner_key("HR", "EMPLOYEES"), "HR.EMPLOYEES");
        assert_eq!(qualified("HR", "EMPLOYEES"), "\"HR\".\"EMPLOYEES\"");
    }

    #[test]
    fn list_sql_scopes_to_one_or_every_user_schema() {
        let all = object_list_sql(ObjectKind::Table, None, None).unwrap_or_default();
        assert!(all.contains("owner NOT IN ('SYS', 'SYSTEM'"), "{all}");
        assert!(all.ends_with(" FETCH FIRST 2000 ROWS ONLY"), "{all}");
        let one = object_list_sql(ObjectKind::Table, Some("HR"), None).unwrap_or_default();
        assert!(one.contains("owner = 'HR'"), "{one}");
        let nested = object_list_sql(ObjectKind::Index, Some("HR"), Some("EMPLOYEES")).unwrap_or_default();
        assert!(nested.contains("i.owner = 'HR' AND i.table_name = 'EMPLOYEES'"), "{nested}");
        assert!(nested.contains("LISTAGG(c.column_name"), "{nested}");
        // Public synonyms are hidden only when no schema was picked.
        assert!(object_list_sql(ObjectKind::Alias, None, None).unwrap_or_default().contains("owner <> 'PUBLIC'"));
        assert!(!object_list_sql(ObjectKind::Alias, Some("HR"), None).unwrap_or_default().contains("owner <> 'PUBLIC'"));
        assert!(object_list_sql(ObjectKind::Function, None, None).unwrap_or_default().contains("object_type = 'FUNCTION'"));
        assert!(object_list_sql(ObjectKind::Package, None, None).unwrap_or_default().contains("object_type = 'PACKAGE'"));
        assert!(object_list_sql(ObjectKind::Schema, None, None).unwrap_or_default().contains("oracle_maintained = 'N'"));
        assert!(object_list_sql(ObjectKind::Keyspace, None, None).is_none());
    }

    #[test]
    fn privileged_kinds_have_a_fallback_or_a_hint() {
        for kind in [ObjectKind::Schema, ObjectKind::Tablespace, ObjectKind::User, ObjectKind::Role, ObjectKind::Grant, ObjectKind::Job] {
            let fallback = object_list_fallback_sql(kind, None).unwrap_or_default();
            assert!(!fallback.is_empty(), "{kind:?} has no fallback");
            assert!(!fallback.to_ascii_lowercase().contains("dba_"), "{kind:?} fallback still reads a DBA_ view: {fallback}");
        }
        for kind in [ObjectKind::Session, ObjectKind::Lock, ObjectKind::Setting] {
            assert!(object_list_fallback_sql(kind, None).is_none(), "{kind:?}");
            assert!(privilege_hint(kind).contains("V$"), "{kind:?}: {}", privilege_hint(kind));
        }
        assert!(object_list_fallback_sql(ObjectKind::Table, None).is_none());
        assert!(is_privilege_error(&AppError::driver("ORA-00942: table or view does not exist")));
        assert!(is_privilege_error(&AppError::driver("ORA-01031: insufficient privileges")));
        assert!(!is_privilege_error(&AppError::driver("ORA-00933: SQL command not properly ended")));
    }

    #[test]
    fn rows_become_summaries() {
        let table = summarize(ObjectKind::Table, &[text("HR"), text("EMPLOYEES"), Value::Int(107), text("USERS"), text("NO"), text("N"), text("2026-09-01")]);
        assert_eq!(table.reference.parent.as_deref(), Some("HR"));
        assert_eq!(table.detail.as_deref(), Some("~107 rows · USERS"));
        assert_eq!(table.badge.as_deref(), Some("table"));
        let part = summarize(ObjectKind::Table, &[text("HR"), text("SALES"), Value::Null, text("USERS"), text("YES"), text("N"), Value::Null]);
        assert_eq!(part.badge.as_deref(), Some("partitioned"));

        let index = summarize(
            ObjectKind::Index,
            &[text("HR"), text("EMP_PK"), text("EMPLOYEES"), text("NORMAL"), text("UNIQUE"), text("VALID"), text("USERS"), text("EMPLOYEE_ID")],
        );
        assert_eq!(index.reference.parent.as_deref(), Some("HR.EMPLOYEES"));
        assert_eq!(index.badge.as_deref(), Some("unique"));
        assert_eq!(index.detail.as_deref(), Some("EMPLOYEES (EMPLOYEE_ID)"));
        let unusable = summarize(
            ObjectKind::Index,
            &[text("HR"), text("EMP_IX"), text("EMPLOYEES"), text("BITMAP"), text("NONUNIQUE"), text("UNUSABLE"), text("USERS"), text("DEPT_ID")],
        );
        assert_eq!(unusable.badge.as_deref(), Some("bitmap"));
        assert_eq!(unusable.detail.as_deref(), Some("EMPLOYEES (DEPT_ID) · unusable"));

        let fk = summarize(
            ObjectKind::Constraint,
            &[text("HR"), text("EMP_DEPT_FK"), text("EMPLOYEES"), text("R"), text("ENABLED"), text("DEPARTMENT_ID"), text("DEPARTMENTS"), text("CASCADE")],
        );
        assert_eq!(fk.badge.as_deref(), Some("foreign"));
        assert_eq!(fk.detail.as_deref(), Some("EMPLOYEES (DEPARTMENT_ID) → DEPARTMENTS · on delete cascade"));
        let check = summarize(ObjectKind::Constraint, &[text("HR"), text("SAL_CK"), text("EMPLOYEES"), text("C"), text("DISABLED"), text("SALARY"), Value::Null, Value::Null]);
        assert_eq!(check.badge.as_deref(), Some("check"));
        assert!(check.detail.unwrap_or_default().ends_with("· disabled"));

        let seq = summarize(ObjectKind::Sequence, &[text("HR"), text("EMP_SEQ"), Value::Int(220), Value::Int(1), Value::Int(1), Value::Decimal("1e28".into()), text("N"), Value::Int(20)]);
        assert_eq!(seq.detail.as_deref(), Some("last 220 · by 1"));
        assert_eq!(seq.badge.as_deref(), Some("nocycle"));

        let syn = summarize(ObjectKind::Alias, &[text("HR"), text("EMP"), text("HR"), text("EMPLOYEES"), Value::Null]);
        assert_eq!(syn.detail.as_deref(), Some("→ HR.EMPLOYEES"));
        assert_eq!(syn.badge.as_deref(), Some("synonym"));

        let mview = summarize(ObjectKind::MaterializedView, &[text("HR"), text("EMP_MV"), text("DEMAND"), text("COMPLETE"), text("2026-09-01"), text("FRESH")]);
        assert_eq!(mview.badge.as_deref(), Some("fresh"));
        assert_eq!(mview.detail.as_deref(), Some("complete refresh · last 2026-09-01"));

        let trigger = summarize(ObjectKind::Trigger, &[text("HR"), text("EMP_TRG"), text("EMPLOYEES"), text("BEFORE EACH ROW"), text("INSERT OR UPDATE"), text("ENABLED"), text("HR")]);
        assert_eq!(trigger.reference.parent.as_deref(), Some("HR.EMPLOYEES"));
        assert_eq!(trigger.detail.as_deref(), Some("BEFORE EACH ROW INSERT OR UPDATE ON EMPLOYEES"));
        let off = summarize(ObjectKind::Trigger, &[text("HR"), text("T2"), text("EMPLOYEES"), text("AFTER STATEMENT"), text("DELETE"), text("DISABLED"), text("HR")]);
        assert_eq!(off.badge.as_deref(), Some("disabled"));

        let tbs = summarize(ObjectKind::Tablespace, &[text("USERS"), text("ONLINE"), text("PERMANENT"), text("LOCAL"), Value::Int(1_048_576)]);
        assert_eq!(tbs.detail.as_deref(), Some("permanent · 1.0 MB"));
        assert_eq!(tbs.badge.as_deref(), Some("online"));

        let user = summarize(ObjectKind::User, &[text("HR"), text("OPEN"), text("USERS"), text("2026-01-01"), text("DEFAULT")]);
        assert_eq!(user.badge.as_deref(), Some("open"));
        assert_eq!(user.detail.as_deref(), Some("USERS · default"));

        let grant = summarize(ObjectKind::Grant, &[text("APP"), text("SELECT"), text("HR.EMPLOYEES"), text("NO"), text("OBJECT")]);
        assert_eq!(grant.reference.name, "SELECT ON HR.EMPLOYEES");
        assert_eq!(grant.reference.parent.as_deref(), Some("APP"));
        assert_eq!(grant.badge.as_deref(), Some("object"));
        let role_grant = summarize(ObjectKind::Grant, &[text("APP"), text("RESOURCE"), Value::Null, text("YES"), text("ROLE")]);
        assert_eq!(role_grant.reference.name, "RESOURCE");
        assert_eq!(role_grant.detail.as_deref(), Some("TO APP · grantable"));

        let session = summarize(
            ObjectKind::Session,
            &[Value::Int(42), Value::Int(7), text("HR"), text("ACTIVE"), text("app"), text("web1"), text("dbfree"), text("db file sequential read"), Value::Int(3), text(""), text("abc"), text("SELECT *\n FROM EMPLOYEES")],
        );
        assert_eq!(session.reference.name, "42,7");
        assert_eq!(session.badge.as_deref(), Some("active"));
        assert_eq!(session.detail.as_deref(), Some("HR@web1 · dbfree · db file sequential read 3s · SELECT * FROM EMPLOYEES"));

        let lock = summarize(ObjectKind::Lock, &[Value::Int(42), text("HR"), text("TM"), Value::Int(3), Value::Int(0), Value::Int(90), Value::Int(1), text("HR"), text("EMPLOYEES")]);
        assert_eq!(lock.reference.name, "42 TM HR.EMPLOYEES");
        assert_eq!(lock.detail.as_deref(), Some("held row-X · HR · 1m 30s · blocking others"));
        assert_eq!(lock.badge.as_deref(), Some("tm"));

        let setting = summarize(ObjectKind::Setting, &[text("open_cursors"), text("300"), text("FALSE"), text("IMMEDIATE"), text("FALSE"), text("max cursors")]);
        assert_eq!(setting.detail.as_deref(), Some("300"));
        assert_eq!(setting.badge.as_deref(), Some("modified"));

        let job = summarize(
            ObjectKind::Job,
            &[text("HR"), text("NIGHTLY"), text("TRUE"), text("SCHEDULED"), text("FREQ=DAILY"), text("2026-09-03"), text("2026-09-04"), Value::Int(10), Value::Int(2)],
        );
        assert_eq!(job.badge.as_deref(), Some("scheduled"));
        assert_eq!(job.detail.as_deref(), Some("FREQ=DAILY · next 2026-09-04 · 2 failures"));
        let disabled = summarize(ObjectKind::Job, &[text("HR"), text("ADHOC"), text("FALSE"), text("DISABLED"), Value::Null, Value::Null, Value::Null, Value::Int(0), Value::Int(0)]);
        assert_eq!(disabled.badge.as_deref(), Some("disabled"));
    }

    #[test]
    fn helpers_format_and_classify() {
        assert_eq!(human_bytes(512.0), "512 B");
        assert_eq!(human_bytes(1536.0), "1.5 KB");
        assert_eq!(human_duration(90), "1m 30s");
        assert_eq!(human_duration(90_061), "1d 1h 1m");
        assert_eq!(preview("select  *\n from dual", 100), "select * from dual");
        assert_eq!(preview("abcdefghij", 3), "abc…");
        assert_eq!(pretty_label("LAST_DDL_TIME"), "last ddl time");
        assert_eq!(lock_mode(6), "exclusive");
        assert_eq!(lock_mode(3), "row-X");
        assert_eq!(lock_mode(99), "unknown");
        assert_eq!(constraint_kind("P"), "primary");
        assert_eq!(constraint_kind("r"), "foreign");
        assert_eq!(constraint_kind("Z"), "constraint");
        let mut items = vec![
            ObjectSummary::new(ObjectKind::Lock, "42 TM T", None),
            ObjectSummary::new(ObjectKind::Lock, "42 TM T", None),
        ];
        dedupe_names(&mut items);
        assert_eq!(items[1].reference.name, "42 TM T (2)");
        let set = ResultSet {
            columns: vec![
                ColumnMeta { name: "STATUS".into(), type_name: "varchar2".into() },
                ColumnMeta { name: "NUM_ROWS".into(), type_name: "number".into() },
                ColumnMeta { name: "LAST_ANALYZED".into(), type_name: "date".into() },
            ],
            rows: vec![vec![text("VALID"), Value::Int(7), Value::Null]],
            truncated: false,
        };
        let props = properties_of(&set);
        assert_eq!(props.len(), 2);
        assert_eq!(props[0].name, "status");
        assert_eq!(set_text(&set, &set.rows[0], "num_rows"), "7");
    }

    #[test]
    fn stats_derive_ratios_and_units() {
        let named = |pairs: &[(&str, f64)]| -> BTreeMap<String, f64> { pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect() };
        let input = StatsInput {
            instance: vec![text("19.0.0.0.0"), text("FREE"), text("oradb"), text("OPEN"), text("ACTIVE"), text("2026-09-03 10:00:00"), Value::Int(7200)],
            sysstat: named(&[
                ("user commits", 1200.0),
                ("session logical reads", 1000.0),
                ("physical reads", 40.0),
                ("parse count (total)", 300.0),
                ("redo size", 2048.0),
            ]),
            sga: named(&[("Fixed Size", 1024.0), ("Variable Size", 2048.0)]),
            pga: named(&[("total PGA allocated", 4096.0)]),
            sessions: Some(vec![Value::Int(30), Value::Int(4), Value::Int(20), Value::Int(200)]),
            tablespaces: vec![vec![text("USERS"), Value::Int(1_000_000), Value::Int(250_000)]],
        };
        let groups = build_stats(&input);
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Sessions", "Memory", "Queries", "I/O", "Storage"]);
        let find = |title: &str, label: &str| groups.iter().find(|g| g.title == title).and_then(|g| g.stats.iter().find(|s| s.label == label)).cloned();
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("Oracle 19.0.0.0.0 (FREE)".into()));
        assert_eq!(find("Server", "Uptime").map(|s| s.value), Some("2h 0m".into()));
        assert_eq!(find("Sessions", "Active").and_then(|s| s.numeric), Some(4.0));
        assert_eq!(find("Memory", "SGA total").map(|s| s.value), Some("3.0 KB".into()));
        assert_eq!(find("Memory", "total PGA allocated").map(|s| s.value), Some("4.0 KB".into()));
        assert_eq!(find("Queries", "User commits").and_then(|s| s.numeric), Some(1200.0));
        assert_eq!(find("I/O", "Buffer cache hit ratio").and_then(|s| s.numeric), Some(96.0));
        assert_eq!(find("I/O", "Redo written").map(|s| s.value), Some("2.0 KB".into()));
        let users = find("Storage", "USERS").unwrap_or_else(|| panic!("no tablespace stat"));
        assert_eq!(users.numeric, Some(75.0));
        assert!(users.hint.unwrap_or_default().contains("of 976.6 KB"));
        assert!(build_stats(&StatsInput::default()).is_empty());
    }

    #[test]
    fn profile_kinds_all_have_a_listing_path() {
        for kind in profile().object_kinds {
            assert!(object_list_sql(kind, None, None).is_some(), "{kind:?} is declared but has no listing");
        }
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
