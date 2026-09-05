// SOT: mssql-integration, mssql-adapter, tiberius-driver, mssql-catalog-queries, mssql-object-explorer, mssql-server-stats, mssql-admin-actions

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, validate_columns, where_clause};
use crate::integrations::{Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, ObjectAction, ObjectDetail, ObjectKind,
    ObjectProperty, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    ServerStats, SslMode, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use std::collections::BTreeMap;
use std::sync::Arc;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

impl From<tiberius::error::Error> for AppError {
    fn from(err: tiberius::error::Error) -> Self {
        AppError::driver(err.to_string())
    }
}

const DEFAULT_PORT: u16 = 1433;
const DEFAULT_DATABASE: &str = "master";
const DEFAULT_USER: &str = "sa";

pub struct MssqlIntegration {
    client: Arc<Mutex<Client<Compat<TcpStream>>>>,
    database: String,
}

pub fn quote_ident(raw: &str) -> String {
    format!("[{}]", raw.replace(']', "]]"))
}

pub fn qualified_name(table: &TableRef) -> String {
    match &table.schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
        None => quote_ident(&table.name),
    }
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("localhost");
    let port = s.port.unwrap_or(DEFAULT_PORT);
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let user = s
        .username
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .unwrap_or(DEFAULT_USER);
    let password = conn.secret.as_deref().unwrap_or_default();

    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(&database);
    config.authentication(AuthMethod::sql_server(user, password));

    match s.ssl_mode {
        SslMode::Disable => config.encryption(EncryptionLevel::NotSupported),
        SslMode::Prefer => {
            config.encryption(EncryptionLevel::Off);
            config.trust_cert();
        }
        SslMode::Require => {
            config.encryption(EncryptionLevel::Required);
            config.trust_cert();
        }
        SslMode::VerifyCa | SslMode::VerifyFull => {
            config.encryption(EncryptionLevel::Required);
        }
    }

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|e| AppError::driver(format!("Could not reach SQL Server at {host}:{port}: {e}")))?;
    let _ = tcp.set_nodelay(true);

    let client = Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| AppError::driver(format!("SQL Server connection failed: {e}")))?;

    Ok(Arc::new(MssqlIntegration {
        client: Arc::new(Mutex::new(client)),
        database,
    }))
}

fn decode_column_data(data: &ColumnData<'_>) -> Value {
    match data {
        ColumnData::U8(opt) => opt.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I16(opt) => opt.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I32(opt) => opt.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I64(opt) => opt.map(Value::Int).unwrap_or(Value::Null),
        ColumnData::F32(opt) => opt.map(|v| Value::Float(v as f64)).unwrap_or(Value::Null),
        ColumnData::F64(opt) => opt.map(Value::Float).unwrap_or(Value::Null),
        ColumnData::Bit(opt) => opt.map(Value::Bool).unwrap_or(Value::Null),
        ColumnData::String(opt) => opt.as_deref().map(|s| Value::Text(s.to_string())).unwrap_or(Value::Null),
        ColumnData::Guid(opt) => opt.map(|g| Value::Text(g.to_string())).unwrap_or(Value::Null),
        ColumnData::Binary(opt) => opt
            .as_deref()
            .map(|b| Value::Bytes(base64::engine::general_purpose::STANDARD.encode(b)))
            .unwrap_or(Value::Null),
        ColumnData::Numeric(opt) => opt.map(|n| Value::Text(n.to_string())).unwrap_or(Value::Null),
        ColumnData::Xml(opt) => opt.as_deref().map(|s| Value::Text(s.to_string())).unwrap_or(Value::Null),
        ColumnData::DateTime(opt) => opt.map(|dt| Value::DateTime(format!("{dt:?}"))).unwrap_or(Value::Null),
        ColumnData::SmallDateTime(opt) => opt.map(|dt| Value::DateTime(format!("{dt:?}"))).unwrap_or(Value::Null),
        ColumnData::Time(opt) => opt.map(|t| Value::DateTime(format!("{t:?}"))).unwrap_or(Value::Null),
        ColumnData::Date(opt) => opt.map(|d| Value::DateTime(format!("{d:?}"))).unwrap_or(Value::Null),
        ColumnData::DateTime2(opt) => opt.map(|dt| Value::DateTime(format!("{dt:?}"))).unwrap_or(Value::Null),
        ColumnData::DateTimeOffset(opt) => opt.map(|dto| Value::DateTime(format!("{dto:?}"))).unwrap_or(Value::Null),
    }
}

// ============================================================================
// OBJECT EXPLORER / ADMINISTRATION
//
// WHAT:  Lists and describes SQL Server's catalog beyond rows: databases,
//        schemas, tables, views, partitions, indexes, constraints, sequences,
//        user-defined types, functions, procedures, triggers, principals and
//        permissions, sessions, locks, server configuration and Agent jobs.
// WHY:   The object explorer and the admin page are generic; this block turns
//        `sys.*` catalog views and DMVs into the neutral `ObjectSummary` /
//        `ObjectDetail` / `ServerStats` shapes.
// HOW:   Pure T-SQL builders (`object_list_sql`) and row mappers (`summarize`)
//        are unit-tested offline. Nested kinds (index, constraint, trigger,
//        partition) carry `schema.table` in `reference.parent`. Anything that
//        needs VIEW SERVER STATE / msdb access degrades: table row counts fall
//        back to a plain catalog query, Agent jobs to an empty list.
//        SQL Server has no SHOW CREATE, so table DDL is rebuilt from
//        sys.columns; code objects use OBJECT_DEFINITION.
// WHERE: src-tauri/src/model/objects.rs (contract), src/features/objects (UI)
// ============================================================================

const OBJECT_CAP: usize = 2000;
const PREVIEW_CHARS: usize = 80;
const SYSTEM_SCHEMAS: [&str; 3] = ["sys", "INFORMATION_SCHEMA", "guest"];

fn quote_literal(raw: &str) -> String {
    format!("N'{}'", raw.replace('\'', "''"))
}

fn system_schema_list() -> String {
    SYSTEM_SCHEMAS.iter().map(|s| quote_literal(s)).collect::<Vec<_>>().join(", ")
}

// WHAT:  `col = N'x'` for one schema, else every schema that is not a system or `db_*` role schema.
fn schema_scope(column: &str, schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("{column} = {}", quote_literal(s)),
        None => format!("{column} NOT IN ({}) AND {column} NOT LIKE 'db[_]%'", system_schema_list()),
    }
}

// WHAT:  `reference.parent` → (schema, table). "dbo" → (dbo, None); "dbo.orders" → both.
fn split_owner(parent: Option<&str>) -> (Option<&str>, Option<&str>) {
    match parent.map(str::trim).filter(|p| !p.is_empty()) {
        None => (None, None),
        Some(p) => match p.split_once('.') {
            Some((schema, table)) => (Some(schema), Some(table)),
            None => (Some(p), None),
        },
    }
}

fn owner_key(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
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
        Some(Value::Bool(b)) => Some(if *b { 1.0 } else { 0.0 }),
        Some(Value::Decimal(t)) | Some(Value::Text(t)) => t.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn cell_bool(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(b)) => *b,
        Some(Value::Int(i)) => *i != 0,
        Some(Value::Text(t)) => t == "1" || t.eq_ignore_ascii_case("true"),
        _ => false,
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

fn is_privilege_error(err: &AppError) -> bool {
    let text = err.to_string().to_ascii_lowercase();
    text.contains("permission") || text.contains("denied") || text.contains("invalid object name") || text.contains("does not exist") || text.contains("view server state") || text.contains("view database state")
}

// WHAT:  sys.columns type facts → T-SQL type text (`nvarchar(50)`, `decimal(10,2)`, `varbinary(max)`).
pub fn column_type_sql(type_name: &str, max_length: i64, precision: i64, scale: i64) -> String {
    let lower = type_name.to_ascii_lowercase();
    match lower.as_str() {
        "nvarchar" | "nchar" => {
            if max_length < 0 { format!("{lower}(max)") } else { format!("{lower}({})", max_length / 2) }
        }
        "varchar" | "char" | "varbinary" | "binary" => {
            if max_length < 0 { format!("{lower}(max)") } else { format!("{lower}({max_length})") }
        }
        "decimal" | "numeric" => format!("{lower}({precision},{scale})"),
        "datetime2" | "datetimeoffset" | "time" => format!("{lower}({scale})"),
        "float" if precision != 53 => format!("float({precision})"),
        _ => lower,
    }
}

pub struct TableColumn {
    pub name: String,
    pub type_sql: String,
    pub nullable: bool,
    pub identity: bool,
    pub computed: Option<String>,
}

// WHAT:  CREATE TABLE text rebuilt from the catalog (SQL Server has no SHOW CREATE).
pub fn build_create_table(schema: &str, table: &str, columns: &[TableColumn], primary_key: &[String]) -> String {
    let mut lines: Vec<String> = columns
        .iter()
        .map(|c| match &c.computed {
            Some(expr) => format!("    {} AS {expr}", quote_ident(&c.name)),
            None => format!(
                "    {} {}{}{}",
                quote_ident(&c.name),
                c.type_sql,
                if c.identity { " IDENTITY(1,1)" } else { "" },
                if c.nullable { " NULL" } else { " NOT NULL" }
            ),
        })
        .collect();
    if !primary_key.is_empty() {
        let cols: Vec<String> = primary_key.iter().map(|c| quote_ident(c)).collect();
        lines.push(format!("    PRIMARY KEY ({})", cols.join(", ")));
    }
    format!("CREATE TABLE {} (\n{}\n);", qualified(schema, table), lines.join(",\n"))
}

// WHAT:  Agent's `last_run_date` (YYYYMMDD) + `last_run_time` (HHMMSS) → ISO-ish text.
pub fn agent_datetime(date: i64, time: i64) -> Option<String> {
    if date <= 0 {
        return None;
    }
    Some(format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        date / 10_000,
        (date / 100) % 100,
        date % 100,
        time / 10_000,
        (time / 100) % 100,
        time % 100
    ))
}

fn run_outcome(code: Option<f64>) -> &'static str {
    match code.map(|c| c as i64) {
        Some(0) => "failed",
        Some(1) => "succeeded",
        Some(2) => "retry",
        Some(3) => "canceled",
        Some(4) => "in progress",
        _ => "never run",
    }
}

fn permission_class_badge(class_desc: &str) -> String {
    match class_desc.to_ascii_uppercase().as_str() {
        "DATABASE" => "database".into(),
        "OBJECT_OR_COLUMN" => "object".into(),
        "SCHEMA" => "schema".into(),
        "DATABASE_PRINCIPAL" => "principal".into(),
        "TYPE" => "type".into(),
        other => other.to_ascii_lowercase().replace('_', " "),
    }
}

// WHAT:  `ON` clause for a permission: `OBJECT::[s].[t]`, `SCHEMA::[s]`, nothing for the database itself.
fn permission_target(class_desc: &str, target: &str, column: Option<&str>) -> String {
    let quoted_target = || {
        target
            .split_once('.')
            .map(|(a, b)| qualified(a, b))
            .unwrap_or_else(|| quote_ident(target))
    };
    match class_desc.to_ascii_uppercase().as_str() {
        "DATABASE" => String::new(),
        "OBJECT_OR_COLUMN" => match column {
            Some(c) => format!(" ON OBJECT::{} ({})", quoted_target(), quote_ident(c)),
            None => format!(" ON OBJECT::{}", quoted_target()),
        },
        "SCHEMA" => format!(" ON SCHEMA::{}", quote_ident(target)),
        "DATABASE_PRINCIPAL" => format!(" ON USER::{}", quote_ident(target)),
        "TYPE" => format!(" ON TYPE::{}", quoted_target()),
        other => format!(" ON {other}::{}", quote_ident(target)),
    }
}

const TABLE_LIST_SQL: &str = "SELECT TOP (2000) s.name, t.name, \
    SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.row_count ELSE 0 END), SUM(CONVERT(BIGINT, ps.used_page_count)) * 8192, \
    MAX(CASE WHEN ps.index_id = 1 THEN 1 ELSE 0 END), t.create_date, t.modify_date \
    FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id \
    LEFT JOIN sys.dm_db_partition_stats ps ON ps.object_id = t.object_id \
    WHERE t.is_ms_shipped = 0 AND {scope} GROUP BY s.name, t.name, t.create_date, t.modify_date ORDER BY s.name, t.name";
const TABLE_LIST_FALLBACK_SQL: &str = "SELECT TOP (2000) s.name, t.name, NULL, NULL, \
    CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.index_id = 1) THEN 1 ELSE 0 END, \
    t.create_date, t.modify_date \
    FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0 AND {scope} ORDER BY s.name, t.name";

const GRANT_LIST_SQL: &str = "SELECT TOP (2000) pr.name, pe.state_desc, pe.permission_name, pe.class_desc, \
    CASE pe.class WHEN 0 THEN DB_NAME() \
        WHEN 1 THEN OBJECT_SCHEMA_NAME(pe.major_id) + '.' + OBJECT_NAME(pe.major_id) \
        WHEN 3 THEN SCHEMA_NAME(pe.major_id) \
        WHEN 4 THEN (SELECT dp.name FROM sys.database_principals dp WHERE dp.principal_id = pe.major_id) \
        WHEN 6 THEN TYPE_NAME(pe.major_id) \
        ELSE CONVERT(NVARCHAR(20), pe.major_id) END, \
    CASE WHEN pe.class = 1 AND pe.minor_id > 0 THEN COL_NAME(pe.major_id, pe.minor_id) ELSE NULL END \
    FROM sys.database_permissions pe JOIN sys.database_principals pr ON pr.principal_id = pe.grantee_principal_id \
    ORDER BY pr.name, pe.class, pe.major_id, pe.permission_name";

const SESSION_LIST_SQL: &str = "SELECT TOP (2000) s.session_id, s.login_name, s.host_name, s.program_name, s.status, DB_NAME(s.database_id), \
    r.command, r.wait_type, r.blocking_session_id, s.cpu_time, s.total_elapsed_time, SUBSTRING(t.text, 1, 400) \
    FROM sys.dm_exec_sessions s LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id \
    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t WHERE s.is_user_process = 1 ORDER BY s.session_id";

const LOCK_LIST_SQL: &str = "SELECT TOP (2000) l.request_session_id, l.resource_type, DB_NAME(l.resource_database_id), \
    CASE WHEN l.resource_type = 'OBJECT' THEN OBJECT_NAME(l.resource_associated_entity_id, l.resource_database_id) ELSE '' END, \
    l.resource_description, l.request_mode, l.request_status, l.request_owner_type \
    FROM sys.dm_tran_locks l WHERE l.resource_type <> 'DATABASE' ORDER BY l.request_session_id, l.resource_type";

const JOB_LIST_SQL: &str = "SELECT j.name, j.enabled, j.description, c.name, j.date_created, j.date_modified, \
    js.last_run_outcome, js.last_run_date, js.last_run_time \
    FROM msdb.dbo.sysjobs j LEFT JOIN msdb.dbo.syscategories c ON c.category_id = j.category_id \
    LEFT JOIN msdb.dbo.sysjobservers js ON js.job_id = j.job_id AND js.server_id = 0 ORDER BY j.name";

// WHAT:  T-SQL for the single-query kinds. `schema` scopes to one schema (None =
//        every user schema); `table` narrows nested kinds to one owner.
pub fn object_list_sql(kind: ObjectKind, schema: Option<&str>, table: Option<&str>) -> Option<String> {
    let table_filter = |col: &str| table.map(|t| format!(" AND {col} = {}", quote_literal(t))).unwrap_or_default();
    let sql = match kind {
        ObjectKind::Database => "SELECT name, state_desc, recovery_model_desc, compatibility_level, create_date, collation_name FROM sys.databases ORDER BY name".to_string(),
        ObjectKind::Schema => format!(
            "SELECT s.name, p.name, (SELECT COUNT(*) FROM sys.objects o WHERE o.schema_id = s.schema_id) \
             FROM sys.schemas s JOIN sys.database_principals p ON p.principal_id = s.principal_id \
             WHERE {} ORDER BY s.name",
            schema_scope("s.name", schema)
        ),
        ObjectKind::Table => TABLE_LIST_SQL.replace("{scope}", &schema_scope("s.name", schema)),
        ObjectKind::View => format!(
            "SELECT TOP ({OBJECT_CAP}) s.name, v.name, v.create_date, v.modify_date, OBJECTPROPERTY(v.object_id, 'IsIndexed') \
             FROM sys.views v JOIN sys.schemas s ON s.schema_id = v.schema_id WHERE v.is_ms_shipped = 0 AND {} ORDER BY s.name, v.name",
            schema_scope("s.name", schema)
        ),
        ObjectKind::Partition => format!(
            "SELECT TOP ({OBJECT_CAP}) s.name, t.name, p.partition_number, p.rows, ps.name, pf.name, p.data_compression_desc, i.name \
             FROM sys.partitions p JOIN sys.tables t ON t.object_id = p.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id \
             JOIN sys.indexes i ON i.object_id = p.object_id AND i.index_id = p.index_id \
             LEFT JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id \
             LEFT JOIN sys.partition_functions pf ON pf.function_id = ps.function_id \
             WHERE p.index_id IN (0, 1) AND {}{}{} ORDER BY s.name, t.name, p.partition_number",
            schema_scope("s.name", schema),
            table_filter("t.name"),
            if table.is_none() { " AND ps.name IS NOT NULL" } else { "" }
        ),
        ObjectKind::Index => format!(
            "SELECT TOP ({OBJECT_CAP}) s.name, o.name, i.name, i.type_desc, i.is_unique, i.is_primary_key, i.is_unique_constraint, i.is_disabled, \
             STUFF((SELECT ', ' + c.name FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
                    WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0 ORDER BY ic.key_ordinal FOR XML PATH('')), 1, 2, '') \
             FROM sys.indexes i JOIN sys.objects o ON o.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE i.name IS NOT NULL AND o.type IN ('U', 'V') AND o.is_ms_shipped = 0 AND {}{} ORDER BY s.name, o.name, i.name",
            schema_scope("s.name", schema),
            table_filter("o.name")
        ),
        ObjectKind::Constraint => format!(
            "SELECT TOP ({OBJECT_CAP}) x.schema_name, x.table_name, x.constraint_name, x.kind, x.definition FROM (\
             SELECT s.name AS schema_name, t.name AS table_name, k.name AS constraint_name, \
                    CASE k.type WHEN 'PK' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS kind, CAST(NULL AS NVARCHAR(MAX)) AS definition \
             FROM sys.key_constraints k JOIN sys.tables t ON t.object_id = k.parent_object_id JOIN sys.schemas s ON s.schema_id = t.schema_id \
             UNION ALL SELECT s.name, t.name, f.name, 'FOREIGN KEY', OBJECT_SCHEMA_NAME(f.referenced_object_id) + '.' + OBJECT_NAME(f.referenced_object_id) \
             FROM sys.foreign_keys f JOIN sys.tables t ON t.object_id = f.parent_object_id JOIN sys.schemas s ON s.schema_id = t.schema_id \
             UNION ALL SELECT s.name, t.name, c.name, 'CHECK', c.definition \
             FROM sys.check_constraints c JOIN sys.tables t ON t.object_id = c.parent_object_id JOIN sys.schemas s ON s.schema_id = t.schema_id \
             UNION ALL SELECT s.name, t.name, d.name, 'DEFAULT', d.definition \
             FROM sys.default_constraints d JOIN sys.tables t ON t.object_id = d.parent_object_id JOIN sys.schemas s ON s.schema_id = t.schema_id\
             ) x WHERE {}{} ORDER BY x.schema_name, x.table_name, x.constraint_name",
            schema_scope("x.schema_name", schema),
            table_filter("x.table_name")
        ),
        ObjectKind::Sequence => format!(
            "SELECT TOP ({OBJECT_CAP}) s.name, q.name, TYPE_NAME(q.user_type_id), CONVERT(NVARCHAR(40), q.current_value), CONVERT(NVARCHAR(40), q.increment), \
             CONVERT(NVARCHAR(40), q.minimum_value), CONVERT(NVARCHAR(40), q.maximum_value), q.is_cycling, q.cache_size, CONVERT(NVARCHAR(40), q.start_value) \
             FROM sys.sequences q JOIN sys.schemas s ON s.schema_id = q.schema_id WHERE {} ORDER BY s.name, q.name",
            schema_scope("s.name", schema)
        ),
        ObjectKind::Type => format!(
            "SELECT TOP ({OBJECT_CAP}) s.name, t.name, TYPE_NAME(t.system_type_id), t.max_length, t.precision, t.scale, t.is_nullable, t.is_table_type \
             FROM sys.types t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_user_defined = 1 AND {} ORDER BY s.name, t.name",
            schema_scope("s.name", schema)
        ),
        ObjectKind::Function | ObjectKind::Procedure => format!(
            "SELECT TOP ({OBJECT_CAP}) s.name, o.name, o.type, o.create_date, o.modify_date \
             FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE o.type IN ({}) AND o.is_ms_shipped = 0 AND {} ORDER BY s.name, o.name",
            if kind == ObjectKind::Function { "'FN', 'IF', 'TF', 'FS', 'FT'" } else { "'P', 'PC'" },
            schema_scope("s.name", schema)
        ),
        ObjectKind::Trigger => format!(
            "SELECT TOP ({OBJECT_CAP}) s.name, o.name, tr.name, tr.is_disabled, tr.is_instead_of_trigger, \
             STUFF((SELECT ', ' + te.type_desc FROM sys.trigger_events te WHERE te.object_id = tr.object_id FOR XML PATH('')), 1, 2, '') \
             FROM sys.triggers tr JOIN sys.objects o ON o.object_id = tr.parent_id JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE tr.parent_class = 1 AND {}{} ORDER BY s.name, o.name, tr.name",
            schema_scope("s.name", schema),
            table_filter("o.name")
        ),
        ObjectKind::User => "SELECT name, type_desc, default_schema_name, create_date, authentication_type_desc FROM sys.database_principals \
             WHERE type IN ('S', 'U', 'G', 'E', 'X', 'C', 'K') AND name NOT IN ('sys', 'INFORMATION_SCHEMA') ORDER BY name"
            .to_string(),
        ObjectKind::Role => "SELECT p.name, p.is_fixed_role, p.create_date, \
             (SELECT COUNT(*) FROM sys.database_role_members m WHERE m.role_principal_id = p.principal_id) \
             FROM sys.database_principals p WHERE p.type = 'R' ORDER BY p.name"
            .to_string(),
        ObjectKind::Grant => GRANT_LIST_SQL.to_string(),
        ObjectKind::Session => SESSION_LIST_SQL.to_string(),
        ObjectKind::Lock => LOCK_LIST_SQL.to_string(),
        ObjectKind::Setting => "SELECT name, CONVERT(NVARCHAR(50), value), CONVERT(NVARCHAR(50), value_in_use), CONVERT(NVARCHAR(50), minimum), \
             CONVERT(NVARCHAR(50), maximum), description, is_dynamic, is_advanced FROM sys.configurations ORDER BY name"
            .to_string(),
        ObjectKind::Job => JOB_LIST_SQL.to_string(),
        _ => return None,
    };
    Some(sql)
}

fn function_badge(type_code: &str) -> &'static str {
    match type_code.trim() {
        "FN" => "scalar",
        "IF" => "inline table",
        "TF" => "table",
        "FS" | "FT" | "PC" => "clr",
        _ => "sql",
    }
}

// WHAT:  One listing row → ObjectSummary, per kind (column order from `object_list_sql`).
pub fn summarize(kind: ObjectKind, row: &[Value]) -> ObjectSummary {
    let t = |i: usize| cell_text(row.get(i));
    match kind {
        ObjectKind::Database => {
            let mut parts = vec![format!("{} recovery", t(2).to_ascii_lowercase())];
            if let Some(level) = cell_opt(row.get(3)) {
                parts.push(format!("compat {level}"));
            }
            if let Some(collation) = cell_opt(row.get(5)) {
                parts.push(collation);
            }
            ObjectSummary::new(kind, t(0), None).with_detail(parts.join(" · ")).with_badge(t(1).to_ascii_lowercase())
        }
        ObjectKind::Schema => ObjectSummary::new(kind, t(0), None).with_detail(format!("owner {} · {} objects", t(1), t(2))),
        ObjectKind::Table => {
            let mut parts = Vec::new();
            if let Some(rows) = cell_f64(row.get(2)) {
                parts.push(format!("~{} rows", format_number(rows)));
            }
            if let Some(size) = cell_f64(row.get(3)) {
                parts.push(human_bytes(size));
            }
            let badge = if cell_f64(row.get(4)).unwrap_or(0.0) > 0.0 { "clustered" } else { "heap" };
            ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(parts.join(" · ")).with_badge(badge)
        }
        ObjectKind::View => {
            let mut summary = ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(format!("modified {}", t(3)));
            if cell_f64(row.get(4)).unwrap_or(0.0) > 0.0 {
                summary = summary.with_badge("indexed");
            }
            summary
        }
        ObjectKind::Partition => {
            let mut parts = Vec::new();
            if let Some(rows) = cell_f64(row.get(3)) {
                parts.push(format!("{} rows", format_number(rows)));
            }
            if let Some(scheme) = cell_opt(row.get(4)) {
                parts.push(format!("{scheme} ({})", t(5)));
            }
            let compression = t(6);
            let mut summary = ObjectSummary::new(kind, format!("{} #{}", t(1), t(2)), Some(owner_key(&t(0), &t(1)))).with_detail(parts.join(" · "));
            if !compression.is_empty() && !compression.eq_ignore_ascii_case("NONE") {
                summary = summary.with_badge(compression.to_ascii_lowercase());
            }
            summary
        }
        ObjectKind::Index => {
            let badge = if cell_bool(row.get(5)) {
                "primary".to_string()
            } else if cell_bool(row.get(4)) {
                "unique".to_string()
            } else {
                t(3).to_ascii_lowercase()
            };
            let mut detail = format!("{} ({})", t(1), t(8));
            if cell_bool(row.get(7)) {
                detail.push_str(" · disabled");
            }
            ObjectSummary::new(kind, t(2), Some(owner_key(&t(0), &t(1)))).with_detail(detail).with_badge(badge)
        }
        ObjectKind::Constraint => {
            let badge = match t(3).to_ascii_uppercase().as_str() {
                "PRIMARY KEY" => "primary",
                "FOREIGN KEY" => "foreign",
                "UNIQUE" => "unique",
                "CHECK" => "check",
                "DEFAULT" => "default",
                _ => "constraint",
            };
            let mut detail = t(1);
            if let Some(definition) = cell_opt(row.get(4)) {
                let sep = if badge == "foreign" { " → " } else { " · " };
                detail.push_str(&format!("{sep}{}", preview(&definition, 60)));
            }
            ObjectSummary::new(kind, t(2), Some(owner_key(&t(0), &t(1)))).with_detail(detail).with_badge(badge)
        }
        ObjectKind::Sequence => {
            let mut summary = ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(format!("current {} · by {}", t(3), t(4)));
            if let Some(type_name) = cell_opt(row.get(2)) {
                summary = summary.with_badge(type_name);
            }
            summary
        }
        ObjectKind::Type => {
            let table_type = cell_bool(row.get(7));
            let detail = if table_type {
                "table type".to_string()
            } else {
                let length = cell_f64(row.get(3)).unwrap_or(0.0) as i64;
                let precision = cell_f64(row.get(4)).unwrap_or(0.0) as i64;
                let scale = cell_f64(row.get(5)).unwrap_or(0.0) as i64;
                format!("{}{}", column_type_sql(&t(2), length, precision, scale), if cell_bool(row.get(6)) { "" } else { " not null" })
            };
            ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(detail).with_badge(if table_type { "table" } else { "alias" })
        }
        ObjectKind::Function => ObjectSummary::new(kind, t(1), Some(t(0)))
            .with_detail(format!("modified {}", t(4)))
            .with_badge(function_badge(&t(2))),
        ObjectKind::Procedure => {
            let mut summary = ObjectSummary::new(kind, t(1), Some(t(0))).with_detail(format!("modified {}", t(4)));
            if t(2).trim() == "PC" {
                summary = summary.with_badge("clr");
            }
            summary
        }
        ObjectKind::Trigger => {
            let badge = if cell_bool(row.get(3)) {
                "disabled"
            } else if cell_bool(row.get(4)) {
                "instead of"
            } else {
                "after"
            };
            ObjectSummary::new(kind, t(2), Some(owner_key(&t(0), &t(1))))
                .with_detail(format!("{} ON {}", t(5), t(1)))
                .with_badge(badge)
        }
        ObjectKind::User => {
            let mut parts = Vec::new();
            if let Some(schema) = cell_opt(row.get(2)) {
                parts.push(format!("schema {schema}"));
            }
            if let Some(auth) = cell_opt(row.get(4)) {
                parts.push(auth.to_ascii_lowercase());
            }
            ObjectSummary::new(kind, t(0), None).with_detail(parts.join(" · ")).with_badge(t(1).to_ascii_lowercase())
        }
        ObjectKind::Role => ObjectSummary::new(kind, t(0), None)
            .with_detail(format!("{} members", t(3)))
            .with_badge(if cell_bool(row.get(1)) { "fixed" } else { "custom" }),
        ObjectKind::Grant => {
            // grantee, state, permission, class_desc, target, column
            let mut name = format!("{} ON {}", t(2), t(4));
            if let Some(column) = cell_opt(row.get(5)) {
                name.push_str(&format!(" ({column})"));
            }
            ObjectSummary::new(kind, name, Some(t(0)))
                .with_detail(format!("{} TO {}", t(1), t(0)))
                .with_badge(permission_class_badge(&t(3)))
        }
        ObjectKind::Session => {
            // session_id, login, host, program, status, db, command, wait_type, blocking, cpu, elapsed, sql
            let mut parts = vec![format!("{}@{}", t(1), t(2))];
            if let Some(program) = cell_opt(row.get(3)) {
                parts.push(preview(&program, 30));
            }
            if let Some(db) = cell_opt(row.get(5)) {
                parts.push(db);
            }
            if let Some(command) = cell_opt(row.get(6)) {
                parts.push(command);
            }
            if let Some(wait) = cell_opt(row.get(7)) {
                parts.push(format!("waiting {wait}"));
            }
            if cell_f64(row.get(8)).unwrap_or(0.0) > 0.0 {
                parts.push(format!("blocked by {}", t(8)));
            }
            if let Some(sql) = cell_opt(row.get(11)) {
                parts.push(preview(&sql, PREVIEW_CHARS));
            }
            ObjectSummary::new(kind, t(0), None).with_detail(parts.join(" · ")).with_badge(t(4).to_ascii_lowercase())
        }
        ObjectKind::Lock => {
            // session, resource_type, db, object, description, mode, status, owner_type
            let target = cell_opt(row.get(3)).or_else(|| cell_opt(row.get(4))).unwrap_or_default();
            let mut parts = vec![t(6).to_ascii_lowercase()];
            if let Some(db) = cell_opt(row.get(2)) {
                parts.push(db);
            }
            if let Some(owner) = cell_opt(row.get(7)) {
                parts.push(owner.to_ascii_lowercase());
            }
            ObjectSummary::new(kind, format!("{} {} {}", t(0), t(1).to_ascii_lowercase(), target).trim().to_string(), None)
                .with_detail(parts.join(" · "))
                .with_badge(t(5))
        }
        ObjectKind::Setting => {
            let in_use = t(2);
            let configured = t(1);
            let mut detail = in_use.clone();
            if !configured.is_empty() && configured != in_use {
                detail.push_str(&format!(" (pending {configured})"));
            }
            let mut summary = ObjectSummary::new(kind, t(0), None).with_detail(detail);
            if cell_bool(row.get(7)) {
                summary = summary.with_badge("advanced");
            }
            summary
        }
        ObjectKind::Job => {
            // name, enabled, description, category, created, modified, outcome, last_run_date, last_run_time
            let mut parts = Vec::new();
            if let Some(category) = cell_opt(row.get(3)) {
                parts.push(category);
            }
            let outcome = run_outcome(cell_f64(row.get(6)));
            match agent_datetime(cell_f64(row.get(7)).unwrap_or(0.0) as i64, cell_f64(row.get(8)).unwrap_or(0.0) as i64) {
                Some(when) => parts.push(format!("{outcome} {when}")),
                None => parts.push(outcome.to_string()),
            }
            ObjectSummary::new(kind, t(0), None)
                .with_detail(parts.join(" · "))
                .with_badge(if cell_bool(row.get(1)) { "enabled" } else { "disabled" })
        }
        _ => ObjectSummary::new(kind, t(0), None),
    }
}

// ---- server statistics -----------------------------------------------------

// WHAT:  Raw rows of the five stats queries, so `build_stats` is testable offline.
#[derive(Default)]
pub struct StatsInput {
    /// ProductVersion, Edition, ProductLevel, MachineName
    pub version: Vec<Value>,
    /// cpu_count, physical_memory_kb, committed_kb, committed_target_kb, start_time, uptime_seconds
    pub sys_info: Option<Vec<Value>>,
    /// connections, user sessions, running requests, blocked requests
    pub counts: Option<Vec<Value>>,
    /// object_name, counter_name, instance_name, cntr_value
    pub counters: Vec<Vec<Value>>,
    /// database, type_desc (ROWS / LOG), bytes
    pub files: Vec<Vec<Value>>,
}

const STATS_VERSION_SQL: &str = "SELECT CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductVersion')), CONVERT(NVARCHAR(128), SERVERPROPERTY('Edition')), \
    CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductLevel')), CONVERT(NVARCHAR(128), SERVERPROPERTY('MachineName'))";
const STATS_SYS_INFO_SQL: &str = "SELECT cpu_count, physical_memory_kb, committed_kb, committed_target_kb, sqlserver_start_time, \
    DATEDIFF(SECOND, sqlserver_start_time, GETDATE()) FROM sys.dm_os_sys_info";
const STATS_COUNTS_SQL: &str = "SELECT (SELECT COUNT(*) FROM sys.dm_exec_connections), \
    (SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1), \
    (SELECT COUNT(*) FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id = r.session_id WHERE s.is_user_process = 1), \
    (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id <> 0)";
const STATS_COUNTERS_SQL: &str = "SELECT RTRIM(object_name), RTRIM(counter_name), RTRIM(instance_name), cntr_value FROM sys.dm_os_performance_counters \
    WHERE counter_name IN ('Batch Requests/sec', 'Page life expectancy', 'Buffer cache hit ratio', 'Buffer cache hit ratio base', 'User Connections', \
    'Transactions/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec', 'Lock Waits/sec', 'Page reads/sec', 'Page writes/sec', 'Lazy writes/sec', \
    'Checkpoint pages/sec', 'Total Server Memory (KB)', 'Target Server Memory (KB)', 'Number of Deadlocks/sec', 'Lock Requests/sec') \
    AND (instance_name IN ('', '_Total') OR object_name LIKE '%Buffer Manager%')";
const STATS_FILES_SQL: &str = "SELECT DB_NAME(database_id), type_desc, SUM(CONVERT(BIGINT, size)) * 8192 FROM sys.master_files GROUP BY database_id, type_desc";

// WHAT:  One counter value, preferring the server-wide instance and the Buffer Manager object.
fn pick_counter(counters: &[Vec<Value>], name: &str) -> Option<f64> {
    let mut best: Option<(u8, f64)> = None;
    for row in counters {
        if !cell_text(row.get(1)).eq_ignore_ascii_case(name) {
            continue;
        }
        let object = cell_text(row.first());
        let instance = cell_text(row.get(2));
        let rank = if object.contains("Buffer Manager") {
            3
        } else if instance == "_Total" {
            2
        } else if instance.is_empty() {
            1
        } else {
            0
        };
        if let Some(value) = cell_f64(row.get(3)) {
            match best {
                Some((r, _)) if rank <= r => {}
                _ => best = Some((rank, value)),
            }
        }
    }
    best.map(|(_, v)| v)
}

pub fn build_stats(input: &StatsInput) -> Vec<StatGroup> {
    let counter = |label: &str, name: &str, unit: Option<&str>| pick_counter(&input.counters, name).map(|v| Stat::number(label, v, unit));

    let mut server = Vec::new();
    let version = cell_text(input.version.first());
    if !version.is_empty() {
        let edition = cell_text(input.version.get(1));
        let level = cell_text(input.version.get(2));
        let extra: Vec<String> = [edition, level].into_iter().filter(|s| !s.is_empty()).collect();
        server.push(Stat::text("Version", if extra.is_empty() { version } else { format!("{version} ({})", extra.join(", ")) }));
    }
    if let Some(host) = cell_opt(input.version.get(3)) {
        server.push(Stat::text("Host", host));
    }
    let mut memory = Vec::new();
    if let Some(info) = &input.sys_info {
        if let Some(up) = cell_f64(info.get(5)) {
            server.push(Stat::text("Uptime", human_duration(up.max(0.0) as u64)));
        }
        if let Some(started) = cell_opt(info.get(4)) {
            server.push(Stat::text("Started", started));
        }
        if let Some(cpus) = cell_f64(info.first()) {
            server.push(Stat::number("CPUs", cpus, None));
        }
        if let Some(kb) = cell_f64(info.get(1)) {
            memory.push(bytes_stat("Physical memory", kb * 1024.0));
        }
        if let Some(kb) = cell_f64(info.get(2)) {
            memory.push(bytes_stat("Committed", kb * 1024.0));
        }
        if let Some(kb) = cell_f64(info.get(3)) {
            memory.push(bytes_stat("Commit target", kb * 1024.0));
        }
    }
    if let Some(kb) = pick_counter(&input.counters, "Total Server Memory (KB)") {
        memory.push(bytes_stat("Total server memory", kb * 1024.0));
    }
    if let Some(kb) = pick_counter(&input.counters, "Target Server Memory (KB)") {
        memory.push(bytes_stat("Target server memory", kb * 1024.0));
    }

    let mut connections = Vec::new();
    if let Some(counts) = &input.counts {
        let labels = ["Connections", "User sessions", "Running requests", "Blocked requests"];
        for (i, label) in labels.iter().enumerate() {
            if let Some(n) = cell_f64(counts.get(i)) {
                connections.push(Stat::number(label, n, None));
            }
        }
    }
    connections.extend(counter("User connections (counter)", "User Connections", None));

    let mut throughput = Vec::new();
    throughput.extend(counter("Batch requests", "Batch Requests/sec", None).map(|s| s.with_hint("cumulative since start; the sparkline shows the rate")));
    throughput.extend(counter("Transactions", "Transactions/sec", None));
    throughput.extend(counter("SQL compilations", "SQL Compilations/sec", None));
    throughput.extend(counter("SQL re-compilations", "SQL Re-Compilations/sec", None));
    throughput.extend(counter("Lock requests", "Lock Requests/sec", None));
    throughput.extend(counter("Lock waits", "Lock Waits/sec", None));
    throughput.extend(counter("Deadlocks", "Number of Deadlocks/sec", None));

    let mut cache = Vec::new();
    cache.extend(counter("Page life expectancy", "Page life expectancy", Some("s")));
    if let (Some(ratio), Some(base)) = (pick_counter(&input.counters, "Buffer cache hit ratio"), pick_counter(&input.counters, "Buffer cache hit ratio base")) {
        let pct = if base > 0.0 { ratio / base * 100.0 } else { 100.0 };
        cache.push(Stat::number("Buffer cache hit ratio", (pct * 100.0).round() / 100.0, Some("%")));
    }
    cache.extend(counter("Page reads", "Page reads/sec", None));
    cache.extend(counter("Page writes", "Page writes/sec", None));
    cache.extend(counter("Lazy writes", "Lazy writes/sec", None));
    cache.extend(counter("Checkpoint pages", "Checkpoint pages/sec", None));

    let mut storage = Vec::new();
    if !input.files.is_empty() {
        let mut databases = std::collections::BTreeSet::new();
        let mut data = 0.0;
        let mut log = 0.0;
        for row in &input.files {
            databases.insert(cell_text(row.first()));
            let bytes = cell_f64(row.get(2)).unwrap_or(0.0);
            if cell_text(row.get(1)).eq_ignore_ascii_case("LOG") {
                log += bytes;
            } else {
                data += bytes;
            }
        }
        storage.push(Stat::number("Databases", databases.len() as f64, None));
        storage.push(bytes_stat("Data files", data));
        storage.push(bytes_stat("Log files", log));
    }

    let groups = [
        ("Server", server),
        ("Connections", connections),
        ("Memory", memory),
        ("Throughput", throughput),
        ("Cache", cache),
        ("Storage", storage),
    ];
    groups
        .into_iter()
        .filter(|(_, stats)| !stats.is_empty())
        .map(|(title, stats)| StatGroup { title: title.to_string(), stats })
        .collect()
}

impl MssqlIntegration {
    // WHAT:  First result set (columns + rows) of one of the adapter's own catalog queries.
    async fn query_set(&self, sql: &str) -> AppResult<ResultSet> {
        let mut client = self.client.lock().await;
        let stream = client.simple_query(sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;
        let columns = rows
            .first()
            .map(|r| r.columns().iter().map(|c| ColumnMeta { name: c.name().to_string(), type_name: format!("{:?}", c.column_type()).to_lowercase() }).collect())
            .unwrap_or_default();
        let rows = rows.into_iter().map(|r| r.cells().map(|(_, data)| decode_column_data(data)).collect()).collect();
        Ok(ResultSet { columns, rows, truncated: false })
    }

    async fn query_rows(&self, sql: &str) -> AppResult<Vec<Vec<Value>>> {
        Ok(self.query_set(sql).await?.rows)
    }

    async fn scalar_text(&self, sql: &str) -> Option<String> {
        self.query_rows(sql).await.ok().and_then(|rows| rows.first().and_then(|r| cell_opt(r.first())))
    }

    async fn property_sheet(&self, sql: &str) -> Vec<ObjectProperty> {
        match self.query_set(sql).await {
            Ok(set) => properties_of(&set),
            Err(_) => Vec::new(),
        }
    }

    async fn list_simple(&self, kind: ObjectKind, sql: &str) -> AppResult<Vec<ObjectSummary>> {
        let rows = self.query_rows(sql).await?;
        let mut items: Vec<ObjectSummary> = rows.iter().map(|r| summarize(kind, r)).collect();
        if matches!(kind, ObjectKind::Session | ObjectKind::Lock | ObjectKind::Grant) {
            dedupe_names(&mut items);
        } else {
            items.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        }
        Ok(items)
    }

    // WHAT:  Table list with row counts when VIEW DATABASE STATE allows it, plain otherwise.
    async fn list_tables(&self, schema: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let sql = TABLE_LIST_SQL.replace("{scope}", &schema_scope("s.name", schema));
        match self.list_simple(ObjectKind::Table, &sql).await {
            Ok(items) => Ok(items),
            Err(err) if is_privilege_error(&err) => {
                let fallback = TABLE_LIST_FALLBACK_SQL.replace("{scope}", &schema_scope("s.name", schema));
                self.list_simple(ObjectKind::Table, &fallback).await
            }
            Err(err) => Err(err),
        }
    }

    async fn list_jobs(&self) -> AppResult<Vec<ObjectSummary>> {
        match self.list_simple(ObjectKind::Job, JOB_LIST_SQL).await {
            Ok(items) => Ok(items),
            Err(err) if is_privilege_error(&err) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    async fn object_definition(&self, schema: &str, name: &str) -> Option<String> {
        self.scalar_text(&format!("SELECT OBJECT_DEFINITION(OBJECT_ID({}))", quote_literal(&qualified(schema, name)))).await
    }

    async fn table_columns_ddl(&self, schema: &str, table: &str) -> AppResult<String> {
        let target = quote_literal(&qualified(schema, table));
        let rows = self
            .query_rows(&format!(
                "SELECT c.name, TYPE_NAME(c.user_type_id), c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, cc.definition \
                 FROM sys.columns c LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id \
                 WHERE c.object_id = OBJECT_ID({target}) ORDER BY c.column_id"
            ))
            .await?;
        let columns: Vec<TableColumn> = rows
            .iter()
            .map(|r| TableColumn {
                name: cell_text(r.first()),
                type_sql: column_type_sql(
                    &cell_text(r.get(1)),
                    cell_f64(r.get(2)).unwrap_or(0.0) as i64,
                    cell_f64(r.get(3)).unwrap_or(0.0) as i64,
                    cell_f64(r.get(4)).unwrap_or(0.0) as i64,
                ),
                nullable: cell_bool(r.get(5)),
                identity: cell_bool(r.get(6)),
                computed: cell_opt(r.get(7)),
            })
            .collect();
        let pk = self
            .query_rows(&format!(
                "SELECT c.name FROM sys.key_constraints k JOIN sys.index_columns ic ON ic.object_id = k.parent_object_id AND ic.index_id = k.unique_index_id \
                 JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
                 WHERE k.type = 'PK' AND k.parent_object_id = OBJECT_ID({target}) ORDER BY ic.key_ordinal"
            ))
            .await
            .unwrap_or_default();
        let pk: Vec<String> = pk.iter().map(|r| cell_text(r.first())).collect();
        Ok(build_create_table(schema, table, &columns, &pk))
    }

    async fn table_detail(&self, reference: &ObjectRef, schema: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(schema, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Ok(ddl) = self.table_columns_ddl(schema, name).await {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT t.create_date, t.modify_date, \
                 (SELECT SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.row_count ELSE 0 END) FROM sys.dm_db_partition_stats ps WHERE ps.object_id = t.object_id) AS rows, \
                 (SELECT SUM(CONVERT(BIGINT, ps.used_page_count)) * 8192 FROM sys.dm_db_partition_stats ps WHERE ps.object_id = t.object_id) AS used_bytes, \
                 (SELECT SUM(CONVERT(BIGINT, ps.reserved_page_count)) * 8192 FROM sys.dm_db_partition_stats ps WHERE ps.object_id = t.object_id) AS reserved_bytes, \
                 t.temporal_type_desc, t.is_memory_optimized \
                 FROM sys.tables t WHERE t.object_id = OBJECT_ID({})",
                quote_literal(&target)
            ))
            .await;
        if detail.properties.is_empty() {
            detail.properties = self
                .property_sheet(&format!("SELECT t.create_date, t.modify_date FROM sys.tables t WHERE t.object_id = OBJECT_ID({})", quote_literal(&target)))
                .await;
        }
        detail.columns = self.columns(&TableRef { schema: Some(schema.to_string()), name: name.to_string() }).await?;
        for kind in [ObjectKind::Index, ObjectKind::Constraint, ObjectKind::Trigger, ObjectKind::Partition] {
            if let Some(sql) = object_list_sql(kind, Some(schema), Some(name)) {
                if let Ok(children) = self.list_simple(kind, &sql).await {
                    detail.children.extend(children);
                }
            }
        }
        Ok(detail
            .action(ObjectAction::destructive("statistics", "Update statistics", format!("UPDATE STATISTICS {target}")))
            .action(ObjectAction::destructive("rebuild", "Rebuild all indexes", format!("ALTER INDEX ALL ON {target} REBUILD")))
            .action(ObjectAction::destructive("truncate", "Truncate table", format!("TRUNCATE TABLE {target}")))
            .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {target}"))))
    }

    async fn view_detail(&self, reference: &ObjectRef, schema: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(schema, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(definition) = self.object_definition(schema, name).await {
            detail = detail.definition(definition, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT v.create_date, v.modify_date, v.is_ms_shipped, OBJECTPROPERTY(v.object_id, 'IsIndexed') AS is_indexed, OBJECTPROPERTY(v.object_id, 'IsSchemaBound') AS is_schema_bound \
                 FROM sys.views v WHERE v.object_id = OBJECT_ID({})",
                quote_literal(&target)
            ))
            .await;
        detail.columns = self.columns(&TableRef { schema: Some(schema.to_string()), name: name.to_string() }).await?;
        Ok(detail
            .action(ObjectAction::new("refresh", "Refresh view metadata", format!("EXEC sp_refreshview {}", quote_literal(&target))))
            .action(ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {target}"))))
    }

    async fn code_detail(&self, reference: &ObjectRef, schema: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(schema, name);
        let word = if reference.kind == ObjectKind::Function { "FUNCTION" } else { "PROCEDURE" };
        let mut detail = ObjectDetail::empty(reference);
        if let Some(definition) = self.object_definition(schema, name).await {
            detail = detail.definition(definition, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT o.type_desc, o.create_date, o.modify_date FROM sys.objects o WHERE o.object_id = OBJECT_ID({})",
                quote_literal(&target)
            ))
            .await;
        detail.rows = self
            .query_set(&format!(
                "SELECT p.parameter_id, p.name, TYPE_NAME(p.user_type_id) AS type_name, p.max_length, p.precision, p.scale, p.is_output, p.has_default_value \
                 FROM sys.parameters p WHERE p.object_id = OBJECT_ID({}) ORDER BY p.parameter_id",
                quote_literal(&target)
            ))
            .await
            .ok();
        Ok(detail.action(ObjectAction::destructive("drop", &format!("Drop {}", word.to_ascii_lowercase()), format!("DROP {word} {target}"))))
    }

    async fn trigger_detail(&self, reference: &ObjectRef, schema: &str, table: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let table_target = qualified(schema, table);
        let trigger_target = qualified(schema, name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(definition) = self.object_definition(schema, name).await {
            detail = detail.definition(definition, CodeLanguage::Sql);
        }
        detail.properties = self
            .property_sheet(&format!(
                "SELECT OBJECT_NAME(tr.parent_id) AS [table], tr.is_disabled, tr.is_instead_of_trigger, tr.create_date, tr.modify_date, \
                 STUFF((SELECT ', ' + te.type_desc FROM sys.trigger_events te WHERE te.object_id = tr.object_id FOR XML PATH('')), 1, 2, '') AS events \
                 FROM sys.triggers tr WHERE tr.object_id = OBJECT_ID({})",
                quote_literal(&trigger_target)
            ))
            .await;
        Ok(detail
            .action(ObjectAction::destructive("enable", "Enable trigger", format!("ENABLE TRIGGER {trigger_target} ON {table_target}")))
            .action(ObjectAction::destructive("disable", "Disable trigger", format!("DISABLE TRIGGER {trigger_target} ON {table_target}")))
            .action(ObjectAction::destructive("drop", "Drop trigger", format!("DROP TRIGGER {trigger_target}"))))
    }

    async fn index_detail(&self, reference: &ObjectRef, schema: &str, table: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(schema, table);
        let mut detail = ObjectDetail::empty(reference);
        let info = self
            .query_set(&format!(
                "SELECT i.type_desc, i.is_unique, i.is_primary_key, i.is_unique_constraint, i.is_disabled, i.fill_factor, i.has_filter, i.filter_definition, \
                 (SELECT SUM(ps.used_page_count) * 8192 FROM sys.dm_db_partition_stats ps WHERE ps.object_id = i.object_id AND ps.index_id = i.index_id) AS used_bytes \
                 FROM sys.indexes i WHERE i.object_id = OBJECT_ID({}) AND i.name = {}",
                quote_literal(&target),
                quote_literal(name)
            ))
            .await?;
        let columns = self
            .query_set(&format!(
                "SELECT ic.key_ordinal, c.name AS column_name, ic.is_descending_key, ic.is_included_column, TYPE_NAME(c.user_type_id) AS type_name \
                 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
                 JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
                 WHERE i.object_id = OBJECT_ID({}) AND i.name = {} ORDER BY ic.is_included_column, ic.key_ordinal",
                quote_literal(&target),
                quote_literal(name)
            ))
            .await?;
        let mut is_primary = false;
        let mut is_unique_constraint = false;
        if let Some(row) = info.rows.first() {
            let type_desc = set_text(&info, row, "type_desc");
            let unique = cell_bool(row.get(1));
            is_primary = cell_bool(row.get(2));
            is_unique_constraint = cell_bool(row.get(3));
            let keys: Vec<String> = columns
                .rows
                .iter()
                .filter(|r| !cell_bool(r.get(3)))
                .map(|r| format!("{}{}", quote_ident(&cell_text(r.get(1))), if cell_bool(r.get(2)) { " DESC" } else { "" }))
                .collect();
            let included: Vec<String> = columns.rows.iter().filter(|r| cell_bool(r.get(3))).map(|r| quote_ident(&cell_text(r.get(1)))).collect();
            let mut definition = format!(
                "CREATE {}{} INDEX {} ON {target} ({})",
                if unique { "UNIQUE " } else { "" },
                type_desc,
                quote_ident(name),
                keys.join(", ")
            );
            if !included.is_empty() {
                definition.push_str(&format!(" INCLUDE ({})", included.join(", ")));
            }
            if let Some(filter) = cell_opt(row.get(7)) {
                definition.push_str(&format!(" WHERE {filter}"));
            }
            detail = detail.definition(definition, CodeLanguage::Sql);
            detail.properties = properties_of(&info);
        }
        detail.rows = Some(columns);
        detail = detail
            .property("table", table)
            .action(ObjectAction::destructive("rebuild", "Rebuild index", format!("ALTER INDEX {} ON {target} REBUILD", quote_ident(name))))
            .action(ObjectAction::destructive("reorganize", "Reorganize index", format!("ALTER INDEX {} ON {target} REORGANIZE", quote_ident(name))));
        if is_primary || is_unique_constraint {
            detail = detail.action(ObjectAction::destructive("drop", "Drop constraint", format!("ALTER TABLE {target} DROP CONSTRAINT {}", quote_ident(name))));
        } else {
            detail = detail
                .action(ObjectAction::destructive("disable", "Disable index", format!("ALTER INDEX {} ON {target} DISABLE", quote_ident(name))))
                .action(ObjectAction::destructive("drop", "Drop index", format!("DROP INDEX {} ON {target}", quote_ident(name))));
        }
        Ok(detail)
    }

    async fn constraint_detail(&self, reference: &ObjectRef, schema: &str, table: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(schema, table);
        let sql = object_list_sql(ObjectKind::Constraint, Some(schema), Some(table)).unwrap_or_default();
        let rows = self.query_rows(&sql).await?;
        let Some(row) = rows.iter().find(|r| cell_text(r.get(2)) == name) else {
            return Err(AppError::not_found(format!("Constraint {name} on {table} was not found.")));
        };
        let kind_text = cell_text(row.get(3)).to_ascii_uppercase();
        let mut detail = ObjectDetail::empty(reference).property("table", table).property("type", kind_text.to_ascii_lowercase());
        let quoted_name = quote_ident(name);
        match kind_text.as_str() {
            "PRIMARY KEY" | "UNIQUE" => {
                let cols = self
                    .query_rows(&format!(
                        "SELECT c.name FROM sys.key_constraints k JOIN sys.index_columns ic ON ic.object_id = k.parent_object_id AND ic.index_id = k.unique_index_id \
                         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
                         WHERE k.parent_object_id = OBJECT_ID({}) AND k.name = {} ORDER BY ic.key_ordinal",
                        quote_literal(&target),
                        quote_literal(name)
                    ))
                    .await
                    .unwrap_or_default();
                let cols: Vec<String> = cols.iter().map(|r| quote_ident(&cell_text(r.first()))).collect();
                detail = detail
                    .property("columns", cols.join(", "))
                    .definition(format!("ALTER TABLE {target} ADD CONSTRAINT {quoted_name} {kind_text} ({})", cols.join(", ")), CodeLanguage::Sql);
            }
            "FOREIGN KEY" => {
                let referenced = cell_text(row.get(4));
                let pairs = self
                    .query_rows(&format!(
                        "SELECT pc.name, rc.name, f.delete_referential_action_desc, f.update_referential_action_desc, f.is_disabled \
                         FROM sys.foreign_keys f JOIN sys.foreign_key_columns fc ON fc.constraint_object_id = f.object_id \
                         JOIN sys.columns pc ON pc.object_id = fc.parent_object_id AND pc.column_id = fc.parent_column_id \
                         JOIN sys.columns rc ON rc.object_id = fc.referenced_object_id AND rc.column_id = fc.referenced_column_id \
                         WHERE f.parent_object_id = OBJECT_ID({}) AND f.name = {} ORDER BY fc.constraint_column_id",
                        quote_literal(&target),
                        quote_literal(name)
                    ))
                    .await
                    .unwrap_or_default();
                let from: Vec<String> = pairs.iter().map(|r| quote_ident(&cell_text(r.first()))).collect();
                let to: Vec<String> = pairs.iter().map(|r| quote_ident(&cell_text(r.get(1)))).collect();
                let ref_target = referenced.split_once('.').map(|(s, t)| qualified(s, t)).unwrap_or_else(|| quote_ident(&referenced));
                let mut definition = format!("ALTER TABLE {target} ADD CONSTRAINT {quoted_name} FOREIGN KEY ({}) REFERENCES {ref_target} ({})", from.join(", "), to.join(", "));
                if let Some(first) = pairs.first() {
                    let on_delete = cell_text(first.get(2));
                    let on_update = cell_text(first.get(3));
                    if !on_delete.is_empty() && on_delete != "NO_ACTION" {
                        definition.push_str(&format!(" ON DELETE {}", on_delete.replace('_', " ")));
                    }
                    if !on_update.is_empty() && on_update != "NO_ACTION" {
                        definition.push_str(&format!(" ON UPDATE {}", on_update.replace('_', " ")));
                    }
                    detail = detail.property("on delete", on_delete.to_ascii_lowercase().replace('_', " ")).property("on update", on_update.to_ascii_lowercase().replace('_', " "));
                    if cell_bool(first.get(4)) {
                        detail = detail.property("enabled", "no");
                    }
                }
                detail = detail.property("references", referenced).definition(definition, CodeLanguage::Sql);
                detail = detail
                    .action(ObjectAction::destructive("nocheck", "Disable constraint", format!("ALTER TABLE {target} NOCHECK CONSTRAINT {quoted_name}")))
                    .action(ObjectAction::destructive("check", "Enable constraint", format!("ALTER TABLE {target} WITH CHECK CHECK CONSTRAINT {quoted_name}")));
            }
            "DEFAULT" => {
                let expr = cell_text(row.get(4));
                let column = self
                    .scalar_text(&format!(
                        "SELECT c.name FROM sys.default_constraints d JOIN sys.columns c ON c.object_id = d.parent_object_id AND c.column_id = d.parent_column_id \
                         WHERE d.parent_object_id = OBJECT_ID({}) AND d.name = {}",
                        quote_literal(&target),
                        quote_literal(name)
                    ))
                    .await
                    .unwrap_or_default();
                detail = detail
                    .property("column", column.clone())
                    .definition(format!("ALTER TABLE {target} ADD CONSTRAINT {quoted_name} DEFAULT {expr} FOR {}", quote_ident(&column)), CodeLanguage::Sql);
            }
            _ => {
                let expr = cell_text(row.get(4));
                detail = detail.definition(format!("ALTER TABLE {target} ADD CONSTRAINT {quoted_name} CHECK {expr}"), CodeLanguage::Sql);
                detail = detail
                    .action(ObjectAction::destructive("nocheck", "Disable constraint", format!("ALTER TABLE {target} NOCHECK CONSTRAINT {quoted_name}")))
                    .action(ObjectAction::destructive("check", "Enable constraint", format!("ALTER TABLE {target} WITH CHECK CHECK CONSTRAINT {quoted_name}")));
            }
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop constraint", format!("ALTER TABLE {target} DROP CONSTRAINT {quoted_name}"))))
    }

    async fn partition_detail(&self, reference: &ObjectRef, schema: &str, table: &str) -> AppResult<ObjectDetail> {
        let number = reference.name.rsplit('#').next().and_then(|n| n.trim().parse::<i64>().ok()).unwrap_or(1);
        let target = qualified(schema, table);
        let mut detail = ObjectDetail::empty(reference).property("table", table).property("partition", number.to_string());
        let extra = self
            .property_sheet(&format!(
                "SELECT p.rows, ps.name AS partition_scheme, pf.name AS partition_function, p.data_compression_desc, i.name AS index_name, \
                 CONVERT(NVARCHAR(100), prv.value) AS boundary_value, \
                 (SELECT SUM(a.used_pages) * 8192 FROM sys.allocation_units a WHERE a.container_id = p.hobt_id) AS used_bytes \
                 FROM sys.partitions p JOIN sys.indexes i ON i.object_id = p.object_id AND i.index_id = p.index_id \
                 LEFT JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id \
                 LEFT JOIN sys.partition_functions pf ON pf.function_id = ps.function_id \
                 LEFT JOIN sys.partition_range_values prv ON prv.function_id = pf.function_id AND prv.boundary_id = p.partition_number \
                 WHERE p.object_id = OBJECT_ID({}) AND p.index_id IN (0, 1) AND p.partition_number = {number}",
                quote_literal(&target)
            ))
            .await;
        detail.properties.extend(extra);
        Ok(detail.action(ObjectAction::destructive(
            "truncate",
            "Truncate partition",
            format!("TRUNCATE TABLE {target} WITH (PARTITIONS ({number}))"),
        )))
    }

    async fn sequence_detail(&self, reference: &ObjectRef, schema: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(schema, name);
        let set = self
            .query_set(&format!(
                "SELECT TYPE_NAME(q.user_type_id) AS type_name, CONVERT(NVARCHAR(40), q.start_value) AS start_value, CONVERT(NVARCHAR(40), q.increment) AS increment, \
                 CONVERT(NVARCHAR(40), q.minimum_value) AS minimum_value, CONVERT(NVARCHAR(40), q.maximum_value) AS maximum_value, q.is_cycling, q.is_cached, q.cache_size, \
                 CONVERT(NVARCHAR(40), q.current_value) AS current_value, q.is_exhausted, q.create_date, q.modify_date \
                 FROM sys.sequences q WHERE q.object_id = OBJECT_ID({})",
                quote_literal(&target)
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(row) = set.rows.first() {
            let mut definition = format!(
                "CREATE SEQUENCE {target} AS {} START WITH {} INCREMENT BY {} MINVALUE {} MAXVALUE {}",
                set_text(&set, row, "type_name"),
                set_text(&set, row, "start_value"),
                set_text(&set, row, "increment"),
                set_text(&set, row, "minimum_value"),
                set_text(&set, row, "maximum_value")
            );
            if cell_bool(row.get(5)) {
                definition.push_str(" CYCLE");
            }
            if cell_bool(row.get(6)) {
                let size = set_text(&set, row, "cache_size");
                definition.push_str(&if size.is_empty() { " CACHE".to_string() } else { format!(" CACHE {size}") });
            }
            detail = detail.definition(definition, CodeLanguage::Sql);
            detail.properties = properties_of(&set);
        }
        Ok(detail
            .action(ObjectAction::destructive("restart", "Restart sequence", format!("ALTER SEQUENCE {target} RESTART")))
            .action(ObjectAction::destructive("drop", "Drop sequence", format!("DROP SEQUENCE {target}"))))
    }

    async fn type_detail(&self, reference: &ObjectRef, schema: &str) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let target = qualified(schema, name);
        let set = self
            .query_set(&format!(
                "SELECT TYPE_NAME(t.system_type_id) AS base_type, t.max_length, t.precision, t.scale, t.is_nullable, t.is_table_type, t.is_assembly_type \
                 FROM sys.types t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = {} AND t.name = {}",
                quote_literal(schema),
                quote_literal(name)
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(row) = set.rows.first() {
            detail.properties = properties_of(&set);
            if cell_bool(row.get(5)) {
                let columns = self
                    .query_rows(&format!(
                        "SELECT c.name, TYPE_NAME(c.user_type_id), c.max_length, c.precision, c.scale, c.is_nullable, c.column_id \
                         FROM sys.table_types tt JOIN sys.columns c ON c.object_id = tt.type_table_object_id \
                         JOIN sys.schemas s ON s.schema_id = tt.schema_id WHERE s.name = {} AND tt.name = {} ORDER BY c.column_id",
                        quote_literal(schema),
                        quote_literal(name)
                    ))
                    .await
                    .unwrap_or_default();
                detail.columns = columns
                    .iter()
                    .map(|r| ColumnInfo {
                        name: cell_text(r.first()),
                        data_type: column_type_sql(
                            &cell_text(r.get(1)),
                            cell_f64(r.get(2)).unwrap_or(0.0) as i64,
                            cell_f64(r.get(3)).unwrap_or(0.0) as i64,
                            cell_f64(r.get(4)).unwrap_or(0.0) as i64,
                        ),
                        nullable: cell_bool(r.get(5)),
                        primary_key: false,
                        ordinal: cell_f64(r.get(6)).unwrap_or(0.0) as u32,
                    })
                    .collect();
                let body: Vec<String> = detail
                    .columns
                    .iter()
                    .map(|c| format!("    {} {}{}", quote_ident(&c.name), c.data_type, if c.nullable { " NULL" } else { " NOT NULL" }))
                    .collect();
                detail = detail.definition(format!("CREATE TYPE {target} AS TABLE (\n{}\n);", body.join(",\n")), CodeLanguage::Sql);
            } else {
                let base = column_type_sql(
                    &cell_text(row.first()),
                    cell_f64(row.get(1)).unwrap_or(0.0) as i64,
                    cell_f64(row.get(2)).unwrap_or(0.0) as i64,
                    cell_f64(row.get(3)).unwrap_or(0.0) as i64,
                );
                detail = detail.definition(
                    format!("CREATE TYPE {target} FROM {base}{}", if cell_bool(row.get(4)) { " NULL" } else { " NOT NULL" }),
                    CodeLanguage::Sql,
                );
            }
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop type", format!("DROP TYPE {target}"))))
    }

    async fn schema_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = self
            .property_sheet(&format!(
                "SELECT p.name AS owner, \
                 (SELECT COUNT(*) FROM sys.tables t WHERE t.schema_id = s.schema_id) AS tables, \
                 (SELECT COUNT(*) FROM sys.views v WHERE v.schema_id = s.schema_id) AS views, \
                 (SELECT COUNT(*) FROM sys.procedures pr WHERE pr.schema_id = s.schema_id) AS procedures, \
                 (SELECT COUNT(*) FROM sys.objects o WHERE o.schema_id = s.schema_id AND o.type IN ('FN', 'IF', 'TF', 'FS', 'FT')) AS functions \
                 FROM sys.schemas s JOIN sys.database_principals p ON p.principal_id = s.principal_id WHERE s.name = {}",
                quote_literal(name)
            ))
            .await;
        detail = detail.definition(format!("CREATE SCHEMA {}", quote_ident(name)), CodeLanguage::Sql);
        Ok(detail.action(ObjectAction::destructive("drop", "Drop schema", format!("DROP SCHEMA {}", quote_ident(name)))))
    }

    async fn database_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = self
            .property_sheet(&format!(
                "SELECT d.state_desc, d.recovery_model_desc, d.compatibility_level, d.collation_name, d.create_date, d.user_access_desc, d.is_read_only, \
                 SUSER_SNAME(d.owner_sid) AS owner, \
                 (SELECT SUM(CONVERT(BIGINT, f.size)) * 8192 FROM sys.master_files f WHERE f.database_id = d.database_id AND f.type_desc = 'ROWS') AS data_bytes, \
                 (SELECT SUM(CONVERT(BIGINT, f.size)) * 8192 FROM sys.master_files f WHERE f.database_id = d.database_id AND f.type_desc = 'LOG') AS log_bytes \
                 FROM sys.databases d WHERE d.name = {}",
                quote_literal(name)
            ))
            .await;
        detail.rows = self
            .query_set(&format!(
                "SELECT f.name, f.type_desc, f.physical_name, CONVERT(BIGINT, f.size) * 8192 AS bytes, f.state_desc \
                 FROM sys.master_files f WHERE f.database_id = DB_ID({}) ORDER BY f.file_id",
                quote_literal(name)
            ))
            .await
            .ok();
        Ok(detail.action(ObjectAction::destructive("drop", "Drop database", format!("DROP DATABASE {}", quote_ident(name)))))
    }

    async fn principal_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let is_role = reference.kind == ObjectKind::Role;
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = self
            .property_sheet(&format!(
                "SELECT p.type_desc, p.default_schema_name, p.create_date, p.modify_date, p.is_fixed_role, p.authentication_type_desc \
                 FROM sys.database_principals p WHERE p.name = {}",
                quote_literal(name)
            ))
            .await;
        let members = self
            .query_set(&format!(
                "SELECT m.name AS member, r.name AS role FROM sys.database_role_members rm \
                 JOIN sys.database_principals r ON r.principal_id = rm.role_principal_id \
                 JOIN sys.database_principals m ON m.principal_id = rm.member_principal_id \
                 WHERE {} = {} ORDER BY 1, 2",
                if is_role { "r.name" } else { "m.name" },
                quote_literal(name)
            ))
            .await
            .ok();
        let mut lines = Vec::new();
        if let Some(set) = &members {
            for row in &set.rows {
                lines.push(if is_role {
                    format!("ALTER ROLE {} ADD MEMBER {}", quote_ident(name), quote_ident(&set_text(set, row, "member")))
                } else {
                    format!("ALTER ROLE {} ADD MEMBER {}", quote_ident(&set_text(set, row, "role")), quote_ident(name))
                });
            }
        }
        if let Ok(grants) = self.query_rows(GRANT_LIST_SQL).await {
            for row in grants.iter().filter(|r| cell_text(r.first()) == name) {
                let column = cell_opt(row.get(5));
                lines.push(format!(
                    "{} {}{} TO {}",
                    cell_text(row.get(1)),
                    cell_text(row.get(2)),
                    permission_target(&cell_text(row.get(3)), &cell_text(row.get(4)), column.as_deref()),
                    quote_ident(name)
                ));
            }
        }
        detail.rows = members;
        if !lines.is_empty() {
            detail = detail.definition(lines.join(";\n"), CodeLanguage::Sql);
        }
        let fixed = detail.properties.iter().any(|p| p.name == "is fixed role" && (p.value == "1" || p.value.eq_ignore_ascii_case("true")));
        if is_role && !fixed {
            detail = detail.action(ObjectAction::destructive("drop", "Drop role", format!("DROP ROLE {}", quote_ident(name))));
        } else if !is_role {
            detail = detail.action(ObjectAction::destructive("drop", "Drop user", format!("DROP USER {}", quote_ident(name))));
        }
        Ok(detail)
    }

    async fn grant_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let rows = self.query_rows(GRANT_LIST_SQL).await?;
        let wanted = reference.name.split(" (").next().unwrap_or(&reference.name);
        let grantee = reference.parent.as_deref().unwrap_or_default();
        let row = rows
            .iter()
            .find(|r| cell_text(r.first()) == grantee && summarize(ObjectKind::Grant, r).reference.name == reference.name)
            .or_else(|| rows.iter().find(|r| cell_text(r.first()) == grantee && summarize(ObjectKind::Grant, r).reference.name.starts_with(wanted)));
        let Some(row) = row else {
            return Err(AppError::not_found("That permission no longer exists."));
        };
        let state = cell_text(row.get(1));
        let permission = cell_text(row.get(2));
        let class_desc = cell_text(row.get(3));
        let target = cell_text(row.get(4));
        let column = cell_opt(row.get(5));
        let on = permission_target(&class_desc, &target, column.as_deref());
        let grantee_quoted = quote_ident(grantee);
        let verb = if state.eq_ignore_ascii_case("DENY") { "DENY" } else { "GRANT" };
        let mut definition = format!("{verb} {permission}{on} TO {grantee_quoted}");
        if state.eq_ignore_ascii_case("GRANT_WITH_GRANT_OPTION") {
            definition.push_str(" WITH GRANT OPTION");
        }
        Ok(ObjectDetail::empty(reference)
            .definition(definition, CodeLanguage::Sql)
            .property("grantee", grantee)
            .property("state", state.to_ascii_lowercase())
            .property("permission", permission.clone())
            .property("scope", permission_class_badge(&class_desc))
            .property("target", target)
            .action(ObjectAction::destructive("revoke", "Revoke", format!("REVOKE {permission}{on} FROM {grantee_quoted}"))))
    }

    async fn session_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let id: i64 = reference.name.trim().parse().map_err(|_| AppError::invalid_input("Session ids are numeric."))?;
        let set = self
            .query_set(&format!(
                "SELECT s.session_id, s.login_name, s.host_name, s.program_name, s.client_interface_name, s.status, DB_NAME(s.database_id) AS [database], \
                 s.login_time, s.last_request_start_time, s.last_request_end_time, s.cpu_time, s.memory_usage, s.reads, s.writes, s.logical_reads, s.row_count, \
                 r.status AS request_status, r.command, r.wait_type, r.wait_time, r.blocking_session_id, r.percent_complete, r.total_elapsed_time, t.text AS sql_text \
                 FROM sys.dm_exec_sessions s LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id \
                 OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t WHERE s.session_id = {id}"
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(row) = set.rows.first() {
            let sql = set_text(&set, row, "sql_text");
            if !sql.is_empty() {
                detail = detail.definition(sql, CodeLanguage::Sql);
            }
        }
        detail.properties = properties_of(&set).into_iter().filter(|p| p.name != "sql text").collect();
        Ok(detail.action(ObjectAction::destructive("kill", "Kill session", format!("KILL {id}"))))
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let set = self
            .query_set(&format!(
                "SELECT CONVERT(NVARCHAR(50), value) AS configured, CONVERT(NVARCHAR(50), value_in_use) AS in_use, CONVERT(NVARCHAR(50), minimum) AS minimum, \
                 CONVERT(NVARCHAR(50), maximum) AS maximum, description, is_dynamic, is_advanced FROM sys.configurations WHERE name = {}",
                quote_literal(name)
            ))
            .await?;
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = properties_of(&set);
        if let Some(row) = set.rows.first() {
            let in_use = set_text(&set, row, "in_use");
            let minimum = set_text(&set, row, "minimum");
            let maximum = set_text(&set, row, "maximum");
            detail = detail.definition(format!("EXEC sp_configure {}, {in_use};\nRECONFIGURE;", quote_literal(name)), CodeLanguage::Sql);
            if minimum == "0" && maximum == "1" {
                let flipped = if in_use == "1" { "0" } else { "1" };
                detail = detail.action(ObjectAction::destructive(
                    "toggle",
                    &format!("Set to {flipped}"),
                    format!("EXEC sp_configure {}, {flipped}; RECONFIGURE", quote_literal(name)),
                ));
            }
        }
        Ok(detail)
    }

    async fn lock_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let items = self.list_simple(ObjectKind::Lock, LOCK_LIST_SQL).await?;
        let mut detail = ObjectDetail::empty(reference);
        if let Some(item) = items.iter().find(|i| i.reference.name == reference.name) {
            if let Some(text) = &item.detail {
                detail = detail.definition(text.clone(), CodeLanguage::Text);
            }
            if let Some(mode) = &item.badge {
                detail = detail.property("mode", mode.clone());
            }
            if let Some(session) = reference.name.split(' ').next().and_then(|s| s.parse::<i64>().ok()) {
                detail = detail.property("session", session.to_string()).action(ObjectAction::destructive("kill", "Kill holding session", format!("KILL {session}")));
            }
        }
        Ok(detail)
    }

    async fn job_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let literal = quote_literal(name);
        let mut detail = ObjectDetail::empty(reference);
        detail.properties = self
            .property_sheet(&format!(
                "SELECT j.enabled, j.description, c.name AS category, SUSER_SNAME(j.owner_sid) AS owner, j.date_created, j.date_modified, \
                 js.last_run_outcome, js.last_run_date, js.last_run_time, js.last_outcome_message \
                 FROM msdb.dbo.sysjobs j LEFT JOIN msdb.dbo.syscategories c ON c.category_id = j.category_id \
                 LEFT JOIN msdb.dbo.sysjobservers js ON js.job_id = j.job_id AND js.server_id = 0 WHERE j.name = {literal}"
            ))
            .await;
        detail.rows = self
            .query_set(&format!(
                "SELECT s.step_id, s.step_name, s.subsystem, s.database_name, s.command, s.on_success_action, s.on_fail_action \
                 FROM msdb.dbo.sysjobsteps s JOIN msdb.dbo.sysjobs j ON j.job_id = s.job_id WHERE j.name = {literal} ORDER BY s.step_id"
            ))
            .await
            .ok();
        if let Some(rows) = &detail.rows {
            let commands: Vec<String> = rows
                .rows
                .iter()
                .map(|r| format!("-- step {}: {}\n{}", set_text(rows, r, "step_id"), set_text(rows, r, "step_name"), set_text(rows, r, "command")))
                .collect();
            if !commands.is_empty() {
                detail = detail.definition(commands.join("\n\n"), CodeLanguage::Sql);
            }
        }
        Ok(detail
            .action(ObjectAction::new("start", "Start job", format!("EXEC msdb.dbo.sp_start_job @job_name = {literal}")))
            .action(ObjectAction::destructive("stop", "Stop job", format!("EXEC msdb.dbo.sp_stop_job @job_name = {literal}")))
            .action(ObjectAction::destructive("enable", "Enable job", format!("EXEC msdb.dbo.sp_update_job @job_name = {literal}, @enabled = 1")))
            .action(ObjectAction::destructive("disable", "Disable job", format!("EXEC msdb.dbo.sp_update_job @job_name = {literal}, @enabled = 0")))
            .action(ObjectAction::destructive("delete", "Delete job", format!("EXEC msdb.dbo.sp_delete_job @job_name = {literal}"))))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true,
            sql: true,
            namespaces: true,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: true,
            transactions: true,
            exact_estimate: false,
        },
        object_kinds: vec![K::Database, K::Schema, K::Table, K::View, K::Partition, K::Index, K::Constraint, K::Sequence, K::Type, K::Function, K::Procedure, K::Trigger, K::User, K::Role, K::Grant, K::Session, K::Lock, K::Setting, K::Job],
        tools: vec![T::Stats, T::Erd],
    }
}

#[async_trait]
impl Integration for MssqlIntegration {
    fn engine(&self) -> Engine {
        Engine::Mssql
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let mut client = self.client.lock().await;
        client.simple_query("SELECT 1").await.map_err(AppError::from)?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let mut client = self.client.lock().await;
        let stream = client
            .simple_query("SELECT @@VERSION")
            .await
            .map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;
        if let Some(row) = rows.into_iter().next() {
            if let Some(ColumnData::String(Some(v))) = row.into_iter().next() {
                return Ok(Some(v.to_string()));
            }
        }
        Ok(Some("Microsoft SQL Server".into()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let mut client = self.client.lock().await;
        let stream = client
            .simple_query("SELECT name FROM sys.databases WHERE state = 0 ORDER BY name")
            .await
            .map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;
        let mut dbs = Vec::new();
        for row in rows {
            if let Some(ColumnData::String(Some(name))) = row.into_iter().next() {
                dbs.push(name.to_string());
            }
        }
        Ok(dbs)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut client = self.client.lock().await;
        let mut schemas_map: std::collections::BTreeMap<String, Vec<TableInfo>> =
            std::collections::BTreeMap::new();

        // WHAT:  Seed the map with every user schema before adding tables.
        // WHY:   Deriving schemas from table rows alone hides an empty schema —
        //        `dbo` on a fresh database — so the sidebar looks broken until
        //        the first table exists.
        let schema_sql = "SELECT s.name FROM sys.schemas s \
                          JOIN sys.database_principals p ON p.principal_id = s.principal_id \
                          WHERE p.is_fixed_role = 0 AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA') \
                          ORDER BY s.name";
        if let Ok(stream) = client.simple_query(schema_sql).await {
            if let Ok(rows) = stream.into_first_result().await {
                for row in rows {
                    if let Some(ColumnData::String(Some(name))) = row.into_iter().next() {
                        schemas_map.entry(name.to_string()).or_default();
                    }
                }
            }
        }

        let sql = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                   FROM INFORMATION_SCHEMA.TABLES \
                   WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
                   ORDER BY TABLE_SCHEMA, TABLE_NAME";
        let stream = client.simple_query(sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;

        for row in rows {
            let mut iter = row.into_iter();
            let schema = match iter.next() {
                Some(ColumnData::String(Some(s))) => s.to_string(),
                _ => continue,
            };
            let name = match iter.next() {
                Some(ColumnData::String(Some(n))) => n.to_string(),
                _ => continue,
            };
            let kind = match iter.next() {
                Some(ColumnData::String(Some(k))) if k.eq_ignore_ascii_case("VIEW") => {
                    TableKind::View
                }
                _ => TableKind::Table,
            };
            let schema_clone = schema.clone();
            schemas_map
                .entry(schema)
                .or_default()
                .push(TableInfo { schema: Some(schema_clone), name, kind, row_estimate: None });
        }

        let schemas = schemas_map
            .into_iter()
            .map(|(name, tables)| SchemaInfo { name, tables })
            .collect();
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let schema = table.schema.as_deref().unwrap_or("dbo");
        let name = &table.name;

        let sql = format!(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.ORDINAL_POSITION, \
             CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk \
             FROM INFORMATION_SCHEMA.COLUMNS c \
             LEFT JOIN ( \
                 SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME \
                 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
                 JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku \
                   ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME \
                  AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA \
                 WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
             ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME \
             WHERE c.TABLE_SCHEMA = '{}' AND c.TABLE_NAME = '{}' \
             ORDER BY c.ORDINAL_POSITION",
            schema.replace('\'', "''"),
            name.replace('\'', "''")
        );

        let mut client = self.client.lock().await;
        let stream = client.simple_query(&sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;

        let mut cols = Vec::new();
        for row in rows {
            let mut iter = row.into_iter();
            let col_name = match iter.next() {
                Some(ColumnData::String(Some(n))) => n.to_string(),
                _ => continue,
            };
            let data_type = match iter.next() {
                Some(ColumnData::String(Some(t))) => t.to_string(),
                _ => String::from("unknown"),
            };
            let nullable = match iter.next() {
                Some(ColumnData::String(Some(is_null))) => is_null.eq_ignore_ascii_case("YES"),
                _ => false,
            };
            let ordinal = match iter.next() {
                Some(ColumnData::I32(Some(p))) => u32::try_from(p).unwrap_or(0),
                _ => cols.len() as u32,
            };
            let is_pk = match iter.next() {
                Some(ColumnData::I32(Some(p))) => p == 1,
                _ => false,
            };

            cols.push(ColumnInfo {
                name: col_name,
                data_type,
                nullable,
                primary_key: is_pk,
                ordinal,
            });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let schema = table.schema.as_deref().unwrap_or("dbo");
        let name = &table.name;
        let sql = format!(
            "SELECT SUM(p.rows) \
             FROM sys.partitions p \
             JOIN sys.tables t ON p.object_id = t.object_id \
             JOIN sys.schemas s ON t.schema_id = s.schema_id \
             WHERE s.name = '{}' AND t.name = '{}' AND p.index_id IN (0, 1)",
            schema.replace('\'', "''"),
            name.replace('\'', "''")
        );

        let mut client = self.client.lock().await;
        let stream = client.simple_query(&sql).await.map_err(AppError::from)?;
        let row = stream.into_row().await.map_err(AppError::from)?;
        if let Some(r) = row {
            if let Some(ColumnData::I64(Some(count))) = r.into_iter().next() {
                return Ok(Some(count));
            }
        }
        Ok(None)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let target = qualified_name(table);
        let where_str = where_clause(Engine::Mssql, filters);
        let sql = format!("SELECT COUNT(*) FROM {target}{where_str}");

        let mut client = self.client.lock().await;
        let stream = client.simple_query(&sql).await.map_err(AppError::from)?;
        let row = stream.into_row().await.map_err(AppError::from)?;
        if let Some(r) = row {
            if let Some(ColumnData::I32(Some(c))) = r.into_iter().next() {
                return Ok(c as i64);
            }
        }
        Ok(0)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;

        let target = qualified_name(table);
        let where_str = where_clause(Engine::Mssql, &query.filters);
        let mut order_str = order_clause(Engine::Mssql, &query.sort);
        if order_str.is_empty() {
            // MSSQL OFFSET-FETCH requires an ORDER BY clause.
            let pk = cols.iter().find(|c| c.primary_key).map(|c| c.name.as_str());
            let fallback = pk.unwrap_or_else(|| cols.first().map(|c| c.name.as_str()).unwrap_or("1"));
            order_str = format!(" ORDER BY {}", quote_ident(fallback));
        }

        let sql = format!(
            "SELECT * FROM {target}{where_str}{order_str} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            query.offset, query.limit
        );

        let mut client = self.client.lock().await;
        let stream = client.simple_query(&sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;

        let mut result_columns = Vec::new();
        let mut result_rows = Vec::new();

        if let Some(first_row) = rows.first() {
            for col in first_row.columns() {
                result_columns.push(ColumnMeta {
                    name: col.name().to_string(),
                    type_name: format!("{:?}", col.column_type()).to_lowercase(),
                });
            }
        }

        for row in rows {
            let cells = row.cells().map(|(_, data)| decode_column_data(data)).collect();
            result_rows.push(cells);
        }

        let max_rows = query.limit as usize;
        let truncated = result_rows.len() > max_rows;
        if truncated {
            result_rows.truncate(max_rows);
        }

        Ok(ResultSet {
            columns: result_columns,
            rows: result_rows,
            truncated,
        })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let stmts = split_statements(sql);
        let mut results = Vec::new();
        let mut client = self.client.lock().await;

        for stmt in stmts {
            let stream = match client.simple_query(&stmt).await {
                Ok(s) => s,
                Err(e) => return Err(AppError::driver(e.to_string())),
            };

            let rows = match stream.into_first_result().await {
                Ok(r) => r,
                Err(_) => {
                    results.push(StatementResult::Affected { rows_affected: 0 });
                    continue;
                }
            };

            if rows.is_empty() {
                results.push(StatementResult::Affected { rows_affected: 0 });
                continue;
            }

            let mut cols = Vec::new();
            if let Some(first) = rows.first() {
                for col in first.columns() {
                    cols.push(ColumnMeta {
                        name: col.name().to_string(),
                        type_name: format!("{:?}", col.column_type()).to_lowercase(),
                    });
                }
            }

            let mut out_rows = Vec::new();
            let truncated = rows.len() > max_rows;
            for r in rows.into_iter().take(max_rows) {
                out_rows.push(r.cells().map(|(_, data)| decode_column_data(data)).collect());
            }

            results.push(StatementResult::Rows {
                result: ResultSet {
                    columns: cols,
                    rows: out_rows,
                    truncated,
                },
            });
        }

        Ok(results)
    }

    async fn close(&self) {}

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        let sql = "SELECT \
                   fk.name AS constraint_name, \
                   OBJECT_SCHEMA_NAME(fk.parent_object_id) AS from_schema, \
                   OBJECT_NAME(fk.parent_object_id) AS from_table, \
                   c1.name AS from_column, \
                   OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS to_schema, \
                   OBJECT_NAME(fk.referenced_object_id) AS to_table, \
                   c2.name AS to_column \
                   FROM sys.foreign_keys fk \
                   INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id \
                   INNER JOIN sys.columns c1 ON fkc.parent_object_id = c1.object_id AND fkc.parent_column_id = c1.column_id \
                   INNER JOIN sys.columns c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id";

        let mut client = self.client.lock().await;
        let stream = client.simple_query(sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;

        let mut fks = Vec::new();
        for row in rows {
            let mut iter = row.into_iter();
            let name = match iter.next() {
                Some(ColumnData::String(Some(n))) => n.to_string(),
                _ => continue,
            };
            let from_schema = match iter.next() {
                Some(ColumnData::String(Some(s))) => s.to_string(),
                _ => continue,
            };
            let from_table = match iter.next() {
                Some(ColumnData::String(Some(t))) => t.to_string(),
                _ => continue,
            };
            let from_column = match iter.next() {
                Some(ColumnData::String(Some(c))) => c.to_string(),
                _ => continue,
            };
            let to_schema = match iter.next() {
                Some(ColumnData::String(Some(s))) => s.to_string(),
                _ => continue,
            };
            let to_table = match iter.next() {
                Some(ColumnData::String(Some(t))) => t.to_string(),
                _ => continue,
            };
            let to_column = match iter.next() {
                Some(ColumnData::String(Some(c))) => c.to_string(),
                _ => continue,
            };

            fks.push(ForeignKey {
                name,
                from_schema: Some(from_schema),
                from_table,
                from_columns: vec![from_column],
                to_schema: Some(to_schema),
                to_table,
                to_columns: vec![to_column],
            });
        }
        Ok(fks)
    }

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let schema = table.schema.as_deref().unwrap_or("dbo");
        Ok(self.table_columns_ddl(schema, &table.name).await.ok())
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let (schema, table) = split_owner(parent);
        match kind {
            ObjectKind::Table => self.list_tables(schema).await,
            ObjectKind::Job => self.list_jobs().await,
            _ => match object_list_sql(kind, schema, table) {
                Some(sql) => self.list_simple(kind, &sql).await,
                None => Ok(Vec::new()),
            },
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, table) = split_owner(reference.parent.as_deref());
        let schema_or_dbo = schema.unwrap_or("dbo");
        match reference.kind {
            ObjectKind::Database => self.database_detail(reference).await,
            ObjectKind::Schema => self.schema_detail(reference).await,
            ObjectKind::Table => self.table_detail(reference, schema_or_dbo).await,
            ObjectKind::View => self.view_detail(reference, schema_or_dbo).await,
            ObjectKind::Function | ObjectKind::Procedure => self.code_detail(reference, schema_or_dbo).await,
            ObjectKind::Sequence => self.sequence_detail(reference, schema_or_dbo).await,
            ObjectKind::Type => self.type_detail(reference, schema_or_dbo).await,
            ObjectKind::User | ObjectKind::Role => self.principal_detail(reference).await,
            ObjectKind::Grant => self.grant_detail(reference).await,
            ObjectKind::Session => self.session_detail(reference).await,
            ObjectKind::Setting => self.setting_detail(reference).await,
            ObjectKind::Lock => self.lock_detail(reference).await,
            ObjectKind::Job => self.job_detail(reference).await,
            ObjectKind::Index | ObjectKind::Constraint | ObjectKind::Trigger | ObjectKind::Partition => {
                let Some(table) = table else {
                    return Err(AppError::invalid_input("Open this object from its table so the owner is known."));
                };
                match reference.kind {
                    ObjectKind::Index => self.index_detail(reference, schema_or_dbo, table).await,
                    ObjectKind::Constraint => self.constraint_detail(reference, schema_or_dbo, table).await,
                    ObjectKind::Trigger => self.trigger_detail(reference, schema_or_dbo, table).await,
                    _ => self.partition_detail(reference, schema_or_dbo, table).await,
                }
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        // Everything but the version needs VIEW SERVER STATE; each part degrades on its own.
        let first_row = |rows: Vec<Vec<Value>>| rows.into_iter().next();
        let input = StatsInput {
            version: self.query_rows(STATS_VERSION_SQL).await.ok().and_then(first_row).unwrap_or_default(),
            sys_info: self.query_rows(STATS_SYS_INFO_SQL).await.ok().and_then(first_row),
            counts: self.query_rows(STATS_COUNTS_SQL).await.ok().and_then(first_row),
            counters: self.query_rows(STATS_COUNTERS_SQL).await.unwrap_or_default(),
            files: self.query_rows(STATS_FILES_SQL).await.unwrap_or_default(),
        };
        Ok(ServerStats::now(build_stats(&input)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, FilterOp, FilterRule, PageQuery, SslMode};

    #[test]
    fn mssql_ident_quoting() {
        assert_eq!(quote_ident("users"), "[users]");
        assert_eq!(quote_ident("us]ers"), "[us]]ers]");
        let table = TableRef {
            schema: Some("sales".into()),
            name: "orders".into(),
        };
        assert_eq!(qualified_name(&table), "[sales].[orders]");
    }


    // ---- object explorer (offline) --------------------------------------------

    fn text(s: &str) -> Value {
        Value::Text(s.into())
    }

    #[test]
    fn owner_keys_split_on_the_first_dot() {
        assert_eq!(split_owner(None), (None, None));
        assert_eq!(split_owner(Some("  ")), (None, None));
        assert_eq!(split_owner(Some("dbo")), (Some("dbo"), None));
        assert_eq!(split_owner(Some("sales.orders")), (Some("sales"), Some("orders")));
        assert_eq!(owner_key("sales", "orders"), "sales.orders");
        assert_eq!(qualified("sales", "orders"), "[sales].[orders]");
        assert_eq!(quote_literal("it's"), "N'it''s'");
    }

    #[test]
    fn list_sql_scopes_to_one_or_every_user_schema() {
        let all = object_list_sql(ObjectKind::View, None, None).unwrap_or_default();
        assert!(all.contains("s.name NOT IN (N'sys', N'INFORMATION_SCHEMA', N'guest')"), "{all}");
        assert!(all.contains("s.name NOT LIKE 'db[_]%'"), "{all}");
        assert!(all.contains("TOP (2000)"));
        let one = object_list_sql(ObjectKind::View, Some("sales"), None).unwrap_or_default();
        assert!(one.contains("s.name = N'sales'"), "{one}");
        let nested = object_list_sql(ObjectKind::Index, Some("sales"), Some("orders")).unwrap_or_default();
        assert!(nested.contains("s.name = N'sales' AND o.name = N'orders'"), "{nested}");
        let table = object_list_sql(ObjectKind::Table, Some("dbo"), None).unwrap_or_default();
        assert!(table.contains("sys.dm_db_partition_stats") && table.contains("s.name = N'dbo'"), "{table}");
        assert!(!table.contains("{scope}"), "the scope placeholder must be substituted");
        let fallback = TABLE_LIST_FALLBACK_SQL.replace("{scope}", &schema_scope("s.name", None));
        assert!(!fallback.contains("dm_db_partition_stats") && !fallback.contains("{scope}"), "{fallback}");
        assert!(object_list_sql(ObjectKind::Function, None, None).unwrap_or_default().contains("'FN', 'IF', 'TF', 'FS', 'FT'"));
        assert!(object_list_sql(ObjectKind::Procedure, None, None).unwrap_or_default().contains("'P', 'PC'"));
        // Only the partition list for a whole schema hides unpartitioned tables.
        assert!(object_list_sql(ObjectKind::Partition, Some("dbo"), None).unwrap_or_default().contains("ps.name IS NOT NULL"));
        assert!(!object_list_sql(ObjectKind::Partition, Some("dbo"), Some("orders")).unwrap_or_default().contains("ps.name IS NOT NULL"));
        assert!(object_list_sql(ObjectKind::Keyspace, None, None).is_none());
    }

    #[test]
    fn rows_become_summaries() {
        let db = summarize(ObjectKind::Database, &[text("shop"), text("ONLINE"), text("FULL"), Value::Int(160), text("2026-01-01"), text("Latin1_General_CI_AS")]);
        assert_eq!(db.badge.as_deref(), Some("online"));
        assert_eq!(db.detail.as_deref(), Some("full recovery · compat 160 · Latin1_General_CI_AS"));

        let table = summarize(ObjectKind::Table, &[text("sales"), text("orders"), Value::Int(1500), Value::Int(3_145_728), Value::Int(1), text(""), text("")]);
        assert_eq!(table.reference.parent.as_deref(), Some("sales"));
        assert_eq!(table.badge.as_deref(), Some("clustered"));
        assert_eq!(table.detail.as_deref(), Some("~1,500 rows · 3.0 MB"));
        let heap = summarize(ObjectKind::Table, &[text("sales"), text("staging"), Value::Null, Value::Null, Value::Int(0), text(""), text("")]);
        assert_eq!(heap.badge.as_deref(), Some("heap"));
        assert_eq!(heap.detail.as_deref(), Some(""));

        let index = summarize(
            ObjectKind::Index,
            &[text("sales"), text("orders"), text("PK_orders"), text("CLUSTERED"), Value::Bool(true), Value::Bool(true), Value::Bool(false), Value::Bool(false), text("id")],
        );
        assert_eq!(index.reference.parent.as_deref(), Some("sales.orders"));
        assert_eq!(index.badge.as_deref(), Some("primary"));
        let nc = summarize(
            ObjectKind::Index,
            &[text("sales"), text("orders"), text("ix_code"), text("NONCLUSTERED"), Value::Bool(false), Value::Bool(false), Value::Bool(false), Value::Bool(true), text("code")],
        );
        assert_eq!(nc.badge.as_deref(), Some("nonclustered"));
        assert_eq!(nc.detail.as_deref(), Some("orders (code) · disabled"));

        let fk = summarize(ObjectKind::Constraint, &[text("sales"), text("orders"), text("FK_cust"), text("FOREIGN KEY"), text("sales.customers")]);
        assert_eq!(fk.badge.as_deref(), Some("foreign"));
        assert_eq!(fk.detail.as_deref(), Some("orders → sales.customers"));
        let check = summarize(ObjectKind::Constraint, &[text("sales"), text("orders"), text("CK_total"), text("CHECK"), text("([total]>(0))")]);
        assert_eq!(check.badge.as_deref(), Some("check"));
        assert_eq!(check.detail.as_deref(), Some("orders · ([total]>(0))"));

        let seq = summarize(ObjectKind::Sequence, &[text("dbo"), text("order_no"), text("bigint"), text("42"), text("1"), text("1"), text("999"), Value::Bool(false), Value::Int(50), text("1")]);
        assert_eq!(seq.badge.as_deref(), Some("bigint"));
        assert_eq!(seq.detail.as_deref(), Some("current 42 · by 1"));

        let udt = summarize(ObjectKind::Type, &[text("dbo"), text("email"), text("nvarchar"), Value::Int(200), Value::Int(0), Value::Int(0), Value::Bool(true), Value::Bool(false)]);
        assert_eq!(udt.detail.as_deref(), Some("nvarchar(100)"));
        assert_eq!(udt.badge.as_deref(), Some("alias"));
        let tvp = summarize(ObjectKind::Type, &[text("dbo"), text("id_list"), text("int"), Value::Int(4), Value::Int(10), Value::Int(0), Value::Bool(true), Value::Bool(true)]);
        assert_eq!(tvp.badge.as_deref(), Some("table"));

        let func = summarize(ObjectKind::Function, &[text("dbo"), text("f_total"), text("IF"), text("2026-01-01"), text("2026-02-02")]);
        assert_eq!(func.badge.as_deref(), Some("inline table"));
        let trigger = summarize(ObjectKind::Trigger, &[text("sales"), text("orders"), text("trg_audit"), Value::Bool(false), Value::Bool(false), text("INSERT, UPDATE")]);
        assert_eq!(trigger.reference.parent.as_deref(), Some("sales.orders"));
        assert_eq!(trigger.badge.as_deref(), Some("after"));
        assert_eq!(trigger.detail.as_deref(), Some("INSERT, UPDATE ON orders"));

        let user = summarize(ObjectKind::User, &[text("app"), text("SQL_USER"), text("dbo"), text("2026-01-01"), text("INSTANCE")]);
        assert_eq!(user.badge.as_deref(), Some("sql_user"));
        assert_eq!(user.detail.as_deref(), Some("schema dbo · instance"));
        let role = summarize(ObjectKind::Role, &[text("db_owner"), Value::Bool(true), text("2026-01-01"), Value::Int(2)]);
        assert_eq!(role.badge.as_deref(), Some("fixed"));

        let grant = summarize(ObjectKind::Grant, &[text("app"), text("GRANT"), text("SELECT"), text("OBJECT_OR_COLUMN"), text("sales.orders"), text("total")]);
        assert_eq!(grant.reference.name, "SELECT ON sales.orders (total)");
        assert_eq!(grant.reference.parent.as_deref(), Some("app"));
        assert_eq!(grant.badge.as_deref(), Some("object"));

        let session = summarize(
            ObjectKind::Session,
            &[Value::Int(55), text("app"), text("web1"), text("dbfree"), text("running"), text("shop"), text("SELECT"), text("PAGEIOLATCH_SH"), Value::Int(0), Value::Int(12), Value::Int(300), text("SELECT *\n  FROM orders")],
        );
        assert_eq!(session.reference.name, "55");
        assert_eq!(session.badge.as_deref(), Some("running"));
        assert_eq!(session.detail.as_deref(), Some("app@web1 · dbfree · shop · SELECT · waiting PAGEIOLATCH_SH · SELECT * FROM orders"));

        let lock = summarize(ObjectKind::Lock, &[Value::Int(55), text("OBJECT"), text("shop"), text("orders"), text(""), text("IX"), text("GRANT"), text("TRANSACTION")]);
        assert_eq!(lock.reference.name, "55 object orders");
        assert_eq!(lock.badge.as_deref(), Some("IX"));
        assert_eq!(lock.detail.as_deref(), Some("grant · shop · transaction"));

        let setting = summarize(ObjectKind::Setting, &[text("max degree of parallelism"), text("4"), text("2"), text("0"), text("32767"), text("maximum degree of parallelism"), Value::Bool(true), Value::Bool(true)]);
        assert_eq!(setting.detail.as_deref(), Some("2 (pending 4)"));
        assert_eq!(setting.badge.as_deref(), Some("advanced"));

        let job = summarize(
            ObjectKind::Job,
            &[text("nightly"), Value::Bool(true), text("rollup"), text("Maintenance"), text(""), text(""), Value::Int(1), Value::Int(20_260_903), Value::Int(23_015)],
        );
        assert_eq!(job.badge.as_deref(), Some("enabled"));
        assert_eq!(job.detail.as_deref(), Some("Maintenance · succeeded 2026-09-03 02:30:15"));
        let never = summarize(ObjectKind::Job, &[text("adhoc"), Value::Bool(false), text(""), Value::Null, text(""), text(""), Value::Null, Value::Int(0), Value::Int(0)]);
        assert_eq!(never.detail.as_deref(), Some("never run"));
        assert_eq!(never.badge.as_deref(), Some("disabled"));
    }

    #[test]
    fn types_and_ddl_render() {
        assert_eq!(column_type_sql("nvarchar", 100, 0, 0), "nvarchar(50)");
        assert_eq!(column_type_sql("NVARCHAR", -1, 0, 0), "nvarchar(max)");
        assert_eq!(column_type_sql("varchar", 50, 0, 0), "varchar(50)");
        assert_eq!(column_type_sql("varbinary", -1, 0, 0), "varbinary(max)");
        assert_eq!(column_type_sql("decimal", 9, 10, 2), "decimal(10,2)");
        assert_eq!(column_type_sql("datetime2", 8, 27, 7), "datetime2(7)");
        assert_eq!(column_type_sql("int", 4, 10, 0), "int");
        assert_eq!(column_type_sql("float", 8, 53, 0), "float");
        assert_eq!(column_type_sql("float", 4, 24, 0), "float(24)");

        let columns = vec![
            TableColumn { name: "id".into(), type_sql: "int".into(), nullable: false, identity: true, computed: None },
            TableColumn { name: "name".into(), type_sql: "nvarchar(50)".into(), nullable: true, identity: false, computed: None },
            TableColumn { name: "upper_name".into(), type_sql: "nvarchar(50)".into(), nullable: true, identity: false, computed: Some("(upper([name]))".into()) },
        ];
        let ddl = build_create_table("sales", "orders", &columns, &["id".to_string()]);
        assert_eq!(
            ddl,
            "CREATE TABLE [sales].[orders] (\n    [id] int IDENTITY(1,1) NOT NULL,\n    [name] nvarchar(50) NULL,\n    [upper_name] AS (upper([name])),\n    PRIMARY KEY ([id])\n);"
        );
        assert!(!build_create_table("dbo", "t", &columns, &[]).contains("PRIMARY KEY"));
    }

    #[test]
    fn permissions_and_agent_values_render() {
        assert_eq!(permission_class_badge("OBJECT_OR_COLUMN"), "object");
        assert_eq!(permission_class_badge("DATABASE"), "database");
        assert_eq!(permission_class_badge("XML_SCHEMA_COLLECTION"), "xml schema collection");
        assert_eq!(permission_target("DATABASE", "shop", None), "");
        assert_eq!(permission_target("OBJECT_OR_COLUMN", "sales.orders", None), " ON OBJECT::[sales].[orders]");
        assert_eq!(permission_target("OBJECT_OR_COLUMN", "sales.orders", Some("total")), " ON OBJECT::[sales].[orders] ([total])");
        assert_eq!(permission_target("SCHEMA", "sales", None), " ON SCHEMA::[sales]");
        assert_eq!(agent_datetime(20_260_903, 23_015), Some("2026-09-03 02:30:15".into()));
        assert_eq!(agent_datetime(0, 0), None);
        assert_eq!(run_outcome(Some(1.0)), "succeeded");
        assert_eq!(run_outcome(Some(0.0)), "failed");
        assert_eq!(run_outcome(None), "never run");
        assert_eq!(function_badge("FN"), "scalar");
        assert_eq!(function_badge("TF"), "table");
        assert_eq!(function_badge("P"), "sql");
    }

    #[test]
    fn helpers_format_and_dedupe() {
        assert_eq!(human_bytes(512.0), "512 B");
        assert_eq!(human_bytes(1536.0), "1.5 KB");
        assert_eq!(human_duration(45), "45s");
        assert_eq!(human_duration(3_700), "1h 1m");
        assert_eq!(human_duration(90_061), "1d 1h 1m");
        assert_eq!(preview("select   *\n from t", 100), "select * from t");
        assert_eq!(preview("abcdefghij", 4), "abcd…");
        assert_eq!(pretty_label("last_run_outcome"), "last run outcome");
        let mut items = vec![
            ObjectSummary::new(ObjectKind::Lock, "55 object orders", None),
            ObjectSummary::new(ObjectKind::Lock, "55 object orders", None),
        ];
        dedupe_names(&mut items);
        assert_eq!(items[1].reference.name, "55 object orders (2)");
        let set = ResultSet {
            columns: vec![
                ColumnMeta { name: "create_date".into(), type_name: "datetime".into() },
                ColumnMeta { name: "rows".into(), type_name: "int".into() },
                ColumnMeta { name: "temporal_type_desc".into(), type_name: "nvarchar".into() },
            ],
            rows: vec![vec![text("2026-01-01"), Value::Int(3), Value::Null]],
            truncated: false,
        };
        let props = properties_of(&set);
        assert_eq!(props.len(), 2);
        assert_eq!(props[0].name, "create date");
        assert_eq!(set_text(&set, &set.rows[0], "rows"), "3");
        assert!(is_privilege_error(&AppError::driver("The user does not have permission to perform this action.")));
        assert!(is_privilege_error(&AppError::driver("Invalid object name 'msdb.dbo.sysjobs'.")));
        assert!(!is_privilege_error(&AppError::driver("Timeout expired")));
    }

    #[test]
    fn stats_derive_ratios_and_units() {
        let counters = vec![
            vec![text("SQLServer:Buffer Manager"), text("Buffer cache hit ratio"), text(""), Value::Int(970)],
            vec![text("SQLServer:Buffer Manager"), text("Buffer cache hit ratio base"), text(""), Value::Int(1000)],
            vec![text("SQLServer:Buffer Manager"), text("Page life expectancy"), text(""), Value::Int(3600)],
            vec![text("SQLServer:SQL Statistics"), text("Batch Requests/sec"), text(""), Value::Int(120_000)],
            vec![text("SQLServer:Databases"), text("Transactions/sec"), text("shop"), Value::Int(5)],
            vec![text("SQLServer:Databases"), text("Transactions/sec"), text("_Total"), Value::Int(77)],
            vec![text("SQLServer:Memory Manager"), text("Total Server Memory (KB)"), text(""), Value::Int(2048)],
        ];
        let input = StatsInput {
            version: vec![text("16.0.4165.4"), text("Developer Edition (64-bit)"), text("RTM"), text("sqlbox")],
            sys_info: Some(vec![Value::Int(8), Value::Int(16_777_216), Value::Int(4_194_304), Value::Int(8_388_608), text("2026-09-03 10:00:00"), Value::Int(7200)]),
            counts: Some(vec![Value::Int(12), Value::Int(9), Value::Int(2), Value::Int(1)]),
            counters,
            files: vec![
                vec![text("shop"), text("ROWS"), Value::Int(1_048_576)],
                vec![text("shop"), text("LOG"), Value::Int(524_288)],
                vec![text("master"), text("ROWS"), Value::Int(1_048_576)],
            ],
        };
        let groups = build_stats(&input);
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Connections", "Memory", "Throughput", "Cache", "Storage"]);
        let find = |title: &str, label: &str| groups.iter().find(|g| g.title == title).and_then(|g| g.stats.iter().find(|s| s.label == label)).cloned();
        assert_eq!(find("Server", "Version").map(|s| s.value), Some("16.0.4165.4 (Developer Edition (64-bit), RTM)".into()));
        assert_eq!(find("Server", "Uptime").map(|s| s.value), Some("2h 0m".into()));
        assert_eq!(find("Server", "CPUs").and_then(|s| s.numeric), Some(8.0));
        assert_eq!(find("Connections", "Blocked requests").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Memory", "Physical memory").map(|s| s.value), Some("16.0 GB".into()));
        assert_eq!(find("Cache", "Buffer cache hit ratio").and_then(|s| s.numeric), Some(97.0));
        assert_eq!(find("Cache", "Page life expectancy").and_then(|s| s.unit), Some("s".into()));
        // `_Total` wins over a per-database instance for the same counter.
        assert_eq!(find("Throughput", "Transactions").and_then(|s| s.numeric), Some(77.0));
        assert_eq!(find("Storage", "Databases").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Data files").map(|s| s.value), Some("2.0 MB".into()));
        // No DMV access at all still yields the version group only.
        let bare = build_stats(&StatsInput { version: vec![text("16.0.0.0")], ..StatsInput::default() });
        assert_eq!(bare.len(), 1);
        assert!(build_stats(&StatsInput::default()).is_empty());
    }

    #[test]
    fn profile_kinds_all_have_a_listing_path() {
        for kind in profile().object_kinds {
            assert!(object_list_sql(kind, None, None).is_some(), "{kind:?} is declared but has no listing");
        }
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_MSSQL_HOST is set, e.g.
    //        the `mssql` service in docker-compose.test.yml.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(host) = std::env::var("DBFREE_TEST_MSSQL_HOST") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Mssql,
                environment: Environment::Local,
                read_only: false,
                host: Some(host),
                port: std::env::var("DBFREE_TEST_MSSQL_PORT").ok().and_then(|p| p.parse().ok()),
                database: Some(std::env::var("DBFREE_TEST_MSSQL_DB").unwrap_or_else(|_| "master".into())),
                username: Some(std::env::var("DBFREE_TEST_MSSQL_USER").unwrap_or_else(|_| "sa".into())),
                file_path: None,
                ssl_mode: SslMode::Require,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_MSSQL_PASSWORD").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        assert_eq!(db.engine(), Engine::Mssql);
        db.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(!version.is_empty(), "no version reported");

        let _ = db.execute("DROP TABLE IF EXISTS dbfree_smoke", 10).await;
        db.execute("CREATE TABLE dbfree_smoke (id INT PRIMARY KEY, name NVARCHAR(50))", 10)
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        db.execute("INSERT INTO dbfree_smoke (id, name) VALUES (1, 'ada'), (2, 'alan'), (3, 'grace')", 10)
            .await
            .unwrap_or_else(|e| panic!("insert: {e}"));

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(catalog.schemas.iter().any(|s| s.tables.iter().any(|t| t.name == "dbfree_smoke")), "{catalog:?}");
        let table = TableRef { schema: Some("dbo".into()), name: "dbfree_smoke".into() };
        let cols = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "id" && c.primary_key), "{cols:?}");

        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 3, "{page:?}");
        let filters = vec![FilterRule { column: "name".into(), op: FilterOp::StartsWith, value: "a".into() }];
        assert_eq!(db.count(&table, &filters).await.unwrap_or_default(), 2);

        match db.execute("SELECT name FROM dbfree_smoke ORDER BY name", 10).await.unwrap_or_default().first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows.len(), 3, "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        let _ = db.execute("DROP TABLE dbfree_smoke", 10).await;
        db.close().await;
    }

}
