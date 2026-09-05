// SOT: clickhouse-integration, clickhouse-adapter, clickhouse-http, jsoncompact-decoding, clickhouse-object-explorer, clickhouse-system-tables, clickhouse-server-stats, projection-parsing

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{qualified_name_for, quote_ident, Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, SslMode, Stat,
    StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use std::collections::BTreeMap;
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

// WHAT:  ClickHouse adapter over its HTTP interface (port 8123 / 8443).
// WHY:   No native driver crate is needed: every statement is a POST whose
//        result comes back as JSONCompact, so one decoder covers SELECT, SHOW,
//        EXPLAIN and DDL alike. `reqwest` is imported only here and in the AI
//        service (scripts/guardrail.py enforces the boundary).
// HOW:   Credentials travel in X-ClickHouse-User / X-ClickHouse-Key headers,
//        the session database in the `database` query parameter, and catalog
//        lookups use server-side query parameters (`{db:String}`) so identifiers
//        never get spliced into SQL text.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI entry)

impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            AppError::timeout("ClickHouse did not answer in time.")
        } else if err.is_connect() {
            AppError::driver(format!("Could not reach ClickHouse: {err}"))
        } else {
            AppError::driver(err)
        }
    }
}

const DEFAULT_DATABASE: &str = "default";
const DEFAULT_USER: &str = "default";
const DEFAULT_PORT: u16 = 8123;

pub struct ClickhouseIntegration {
    client: Client,
    base_url: String,
    user: String,
    password: Option<String>,
    database: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let scheme = match s.ssl_mode {
        SslMode::Disable | SslMode::Prefer => "http",
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => "https",
    };
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
        .unwrap_or(DEFAULT_USER)
        .to_string();
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(300))
        .build()?;
    let integration = ClickhouseIntegration {
        client,
        base_url: format!("{scheme}://{host}:{port}/"),
        user,
        password: conn.secret.clone(),
        database,
    };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// JSONCompact decoding (pure functions, unit-tested below)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CompactMeta {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

#[derive(Debug, Deserialize)]
struct CompactBody {
    meta: Vec<CompactMeta>,
    #[serde(default)]
    data: Vec<Vec<serde_json::Value>>,
}

/// Peels `Nullable(...)` / `LowCardinality(...)` wrappers so the inner type drives decoding.
fn base_type(type_name: &str) -> &str {
    let mut current = type_name.trim();
    loop {
        let stripped = current
            .strip_prefix("Nullable(")
            .or_else(|| current.strip_prefix("LowCardinality("))
            .and_then(|inner| inner.strip_suffix(')'));
        match stripped {
            Some(inner) => current = inner.trim(),
            None => return current,
        }
    }
}

fn is_integer_type(base: &str) -> bool {
    base.starts_with("Int") || base.starts_with("UInt")
}

fn is_float_type(base: &str) -> bool {
    base.starts_with("Float") || base == "BFloat16"
}

fn is_temporal_type(base: &str) -> bool {
    base.starts_with("Date") || base.starts_with("Time")
}

fn decode_cell(cell: &serde_json::Value, type_name: &str) -> Value {
    let base = base_type(type_name);
    match cell {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if base.starts_with("Decimal") {
                // Exact numerics stay textual; never round them through f64.
                Value::Decimal(n.to_string())
            } else if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                if is_integer_type(base) {
                    // u64 above i64::MAX: keep every digit instead of rounding through f64.
                    Value::Decimal(n.to_string())
                } else {
                    Value::Float(f)
                }
            } else {
                Value::Decimal(n.to_string())
            }
        }
        serde_json::Value::String(text) => {
            if is_temporal_type(base) {
                Value::DateTime(text.clone())
            } else if base.starts_with("Decimal") {
                Value::Decimal(text.clone())
            } else if is_integer_type(base) {
                text.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Decimal(text.clone()))
            } else if is_float_type(base) {
                // Quoted denormals ("nan", "inf", "-inf") arrive as strings.
                text.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::Text(text.clone()))
            } else if base == "Bool" {
                Value::Bool(text == "true" || text == "1")
            } else {
                Value::Text(text.clone())
            }
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(cell.clone()),
    }
}

/// Decodes a JSONCompact body into a result set, capping rows at `max_rows`.
fn decode_compact(body: &str, max_rows: usize) -> AppResult<ResultSet> {
    let parsed: CompactBody = serde_json::from_str(body).map_err(|e| AppError::driver(format!("ClickHouse returned unreadable JSON: {e}")))?;
    let columns: Vec<ColumnMeta> = parsed
        .meta
        .iter()
        .map(|m| ColumnMeta { name: m.name.clone(), type_name: m.type_name.clone() })
        .collect();
    let truncated = parsed.data.len() > max_rows;
    let rows: Vec<Vec<Value>> = parsed
        .data
        .iter()
        .take(max_rows)
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(i, col)| row.get(i).map(|cell| decode_cell(cell, &col.type_name)).unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    Ok(ResultSet { columns, rows, truncated })
}

/// Decodes any response body: JSONCompact when possible, otherwise one text row
/// per line (a statement that carries its own `FORMAT` clause, for example).
fn decode_body(body: &str, max_rows: usize) -> ResultSet {
    match decode_compact(body, max_rows) {
        Ok(set) => set,
        Err(_) => {
            let lines: Vec<&str> = body.lines().collect();
            let truncated = lines.len() > max_rows;
            ResultSet {
                columns: vec![ColumnMeta { name: "output".to_string(), type_name: "String".to_string() }],
                rows: lines.into_iter().take(max_rows).map(|l| vec![Value::Text(l.to_string())]).collect(),
                truncated,
            }
        }
    }
}

/// `written_rows` from the X-ClickHouse-Summary header (a JSON object of stringified numbers).
fn parse_written_rows(summary: &str) -> Option<u64> {
    let parsed: serde_json::Value = serde_json::from_str(summary).ok()?;
    match parsed.get("written_rows")? {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn first_cell(set: &ResultSet) -> Option<&Value> {
    set.rows.first().and_then(|r| r.first())
}

fn cell_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Int(i)) => Some(*i),
        Some(Value::Decimal(d)) | Some(Value::Text(d)) => d.parse().ok(),
        Some(Value::Float(f)) => Some(*f as i64),
        _ => None,
    }
}

fn cell_text(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Text(t)) | Some(Value::Decimal(t)) | Some(Value::DateTime(t)) => Some(t.clone()),
        Some(Value::Int(i)) => Some(i.to_string()),
        Some(Value::Float(f)) => Some(f.to_string()),
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(Value::Json(j)) => Some(j.to_string()),
        _ => None,
    }
}

struct RawResponse {
    body: String,
    written_rows: Option<u64>,
}

impl ClickhouseIntegration {
    // WHAT:  One HTTP round trip. `params` become `param_<name>` server-side
    //        query parameters referenced as `{name:Type}` inside the SQL.
    async fn post(&self, sql: &str, params: &[(&str, &str)]) -> AppResult<RawResponse> {
        let mut query: Vec<(String, String)> = vec![
            ("database".to_string(), self.database.clone()),
            ("default_format".to_string(), "JSONCompact".to_string()),
            ("output_format_json_quote_64bit_integers".to_string(), "0".to_string()),
            ("output_format_json_quote_denormals".to_string(), "1".to_string()),
            // Decimals as strings keep their declared scale ("12.50", not 12.5).
            ("output_format_json_quote_decimals".to_string(), "1".to_string()),
        ];
        for (name, value) in params {
            query.push((format!("param_{name}"), (*value).to_string()));
        }
        let mut request = self
            .client
            .post(&self.base_url)
            .query(&query)
            .header("X-ClickHouse-User", &self.user)
            .header("Content-Type", "text/plain; charset=utf-8");
        if let Some(password) = &self.password {
            request = request.header("X-ClickHouse-Key", password);
        }
        let response = request.body(sql.to_string()).send().await?;
        let status = response.status();
        let written_rows = response
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_written_rows);
        let body = response.text().await?;
        if !status.is_success() {
            let message = body.trim();
            return Err(AppError::driver(if message.is_empty() { format!("ClickHouse returned HTTP {status}") } else { message.to_string() }));
        }
        Ok(RawResponse { body, written_rows })
    }

    async fn query(&self, sql: &str, params: &[(&str, &str)], max_rows: usize) -> AppResult<ResultSet> {
        let raw = self.post(sql, params).await?;
        decode_compact(&raw.body, max_rows)
    }

    async fn run_statement(&self, sql: &str, max_rows: usize) -> AppResult<StatementResult> {
        let raw = self.post(sql, &[]).await?;
        if raw.body.trim().is_empty() {
            return Ok(StatementResult::Affected { rows_affected: raw.written_rows.unwrap_or(0) });
        }
        Ok(StatementResult::Rows { result: decode_body(&raw.body, max_rows) })
    }

    fn qualified(&self, table: &TableRef) -> String {
        let with_schema = TableRef {
            schema: Some(table.schema.clone().unwrap_or_else(|| self.database.clone())),
            name: table.name.clone(),
        };
        qualified_name_for(Engine::Clickhouse, &with_schema)
    }

    fn schema_of<'a>(&'a self, table: &'a TableRef) -> &'a str {
        table.schema.as_deref().unwrap_or(self.database.as_str())
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Databases, tables / views / materialized views, partitions,
//        dictionaries, projections, SQL UDFs, users / roles / quotas,
//        settings, running queries, replicas, cluster nodes and the slowest
//        queries of the last day — all read from `system.*` tables.
// WHY:   The generic explorer / admin UI; ClickHouse keeps everything it
//        knows about itself in system tables, so one SELECT per kind covers
//        the whole surface, and every action is a plain statement that runs
//        back through `execute` (guard read-only lock + destructive confirm).
// HOW:   Namespaced kinds take the database as `parent`; nested kinds
//        (partitions, projections) take `db.table` so their detail can find
//        the owner. Identifiers reach SQL only through server-side parameters
//        (`{db:String}`) for lookups and through `quote_ident` for actions.
// ---------------------------------------------------------------------------

const MAX_OBJECTS: usize = 2_000;
const SYSTEM_DATABASES: &str = "('system', 'INFORMATION_SCHEMA', 'information_schema')";

fn column_index(set: &ResultSet, name: &str) -> Option<usize> {
    set.columns.iter().position(|c| c.name == name)
}

fn named<'a>(set: &ResultSet, row: &'a [Value], name: &str) -> Option<&'a Value> {
    column_index(set, name).and_then(|i| row.get(i)).filter(|v| !matches!(v, Value::Null))
}

fn named_text(set: &ResultSet, row: &[Value], name: &str) -> String {
    cell_text(named(set, row, name)).unwrap_or_default()
}

fn named_i64(set: &ResultSet, row: &[Value], name: &str) -> Option<i64> {
    cell_i64(named(set, row, name))
}

fn cell_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Int(i)) => Some(*i as f64),
        Some(Value::Float(f)) => Some(*f),
        Some(Value::Decimal(d)) | Some(Value::Text(d)) => d.parse().ok(),
        Some(Value::Bool(b)) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", value as u64, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn format_duration(seconds: f64) -> String {
    let total = seconds.max(0.0) as u64;
    let (days, hours, minutes) = (total / 86_400, (total % 86_400) / 3_600, (total % 3_600) / 60);
    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m {}s", total % 60)
    }
}

/// First `max` characters of a query on one line, for list captions.
fn preview(query: &str, max: usize) -> String {
    let flat: String = query.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() > max {
        format!("{}…", flat.chars().take(max).collect::<String>())
    } else {
        flat
    }
}

/// system.tables.engine → explorer kind.
fn table_kind(engine: &str) -> ObjectKind {
    match engine {
        "MaterializedView" => ObjectKind::MaterializedView,
        "View" | "LiveView" | "WindowView" => ObjectKind::View,
        _ => ObjectKind::Table,
    }
}

fn size_caption(rows: Option<i64>, bytes: Option<i64>) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(r) = rows {
        parts.push(format!("{} rows", crate::model::objects::format_number(r as f64)));
    }
    if let Some(b) = bytes {
        parts.push(format_bytes(b as f64));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" · "))
    }
}

/// `PROJECTION name (SELECT …)` clauses of a `create_table_query`, for servers
/// without `system.projections`. Parentheses are matched with quotes honoured.
fn parse_projections(create: &str) -> Vec<(String, String)> {
    const KEYWORD: &str = "PROJECTION ";
    let mut out = Vec::new();
    let mut search = 0;
    while let Some(rel) = create.get(search..).and_then(|s| s.find(KEYWORD)) {
        let at = search + rel;
        search = at + KEYWORD.len();
        let boundary = create[..at].chars().next_back().is_none_or(|c| c.is_whitespace() || c == ',' || c == '(');
        if !boundary {
            continue;
        }
        let rest = create[search..].trim_start();
        let name_len = rest.find(|c: char| c.is_whitespace() || c == '(').unwrap_or(rest.len());
        let name = rest[..name_len].trim_matches('`').to_string();
        let after = rest[name_len..].trim_start();
        if name.is_empty() || !after.starts_with('(') {
            continue;
        }
        let mut depth = 0i32;
        let mut quote: Option<char> = None;
        let mut escaped = false;
        let mut end = None;
        for (i, c) in after.char_indices() {
            match quote {
                Some(q) => {
                    if escaped {
                        escaped = false;
                    } else if c == '\\' {
                        escaped = true;
                    } else if c == q {
                        quote = None;
                    }
                }
                None => match c {
                    '\'' | '`' | '"' => quote = Some(c),
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(i);
                            break;
                        }
                    }
                    _ => {}
                },
            }
        }
        let Some(end) = end else { break };
        out.push((name, after[1..end].trim().to_string()));
        search = create.len() - after.len() + end + 1;
    }
    out
}

/// `db.table` parents (children of a table) → (db, table); plain parents are databases.
fn split_parent(parent: Option<&str>) -> (Option<&str>, Option<&str>) {
    match parent.and_then(|p| p.split_once('.')) {
        Some((db, table)) => (Some(db), Some(table)),
        None => (parent, None),
    }
}

fn scope_sql(column: &str, db: Option<&str>) -> String {
    match db {
        Some(_) => format!("{column} = {{db:String}}"),
        None => format!("{column} NOT IN {SYSTEM_DATABASES}"),
    }
}

fn scope_params(db: Option<&str>) -> Vec<(&'static str, &str)> {
    db.map(|d| vec![("db", d)]).unwrap_or_default()
}

fn ident(db: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(db), quote_ident(name))
}

fn table_actions(kind: ObjectKind, name: &str) -> Vec<ObjectAction> {
    match kind {
        ObjectKind::Table => vec![
            ObjectAction::new("optimize", "Optimize (merge parts)", format!("OPTIMIZE TABLE {name} FINAL")),
            ObjectAction::destructive("truncate", "Truncate table", format!("TRUNCATE TABLE {name}")),
            ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {name}")),
        ],
        ObjectKind::MaterializedView => vec![ObjectAction::destructive("drop", "Drop materialized view", format!("DROP VIEW {name}"))],
        _ => vec![ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {name}"))],
    }
}

fn partition_actions(table: &str, partition_id: &str) -> Vec<ObjectAction> {
    let id = quote_literal(partition_id);
    vec![
        ObjectAction::new("optimize", "Optimize partition", format!("OPTIMIZE TABLE {table} PARTITION ID {id} FINAL")),
        ObjectAction::destructive("detach", "Detach partition", format!("ALTER TABLE {table} DETACH PARTITION ID {id}")),
        ObjectAction::destructive("drop", "Drop partition", format!("ALTER TABLE {table} DROP PARTITION ID {id}")),
    ]
}

fn projection_actions(table: &str, projection: &str) -> Vec<ObjectAction> {
    let p = quote_ident(projection);
    vec![
        ObjectAction::new("materialize", "Materialize projection", format!("ALTER TABLE {table} MATERIALIZE PROJECTION {p}")),
        ObjectAction::destructive("clear", "Clear projection data", format!("ALTER TABLE {table} CLEAR PROJECTION {p}")),
        ObjectAction::destructive("drop", "Drop projection", format!("ALTER TABLE {table} DROP PROJECTION {p}")),
    ]
}

impl ClickhouseIntegration {
    fn owner(&self, reference: &ObjectRef) -> AppResult<(String, String)> {
        match reference.parent.as_deref() {
            Some(p) => match p.split_once('.') {
                Some((db, table)) => Ok((db.to_string(), table.to_string())),
                None => Ok((self.database.clone(), p.to_string())),
            },
            None => Err(AppError::invalid_input(format!("{} {} needs its table as parent (db.table).", kind_label(reference.kind), reference.name))),
        }
    }

    fn database_of(&self, reference: &ObjectRef) -> String {
        reference.parent.clone().filter(|p| !p.is_empty()).unwrap_or_else(|| self.database.clone())
    }

    async fn one_row(&self, sql: &str, params: &[(&str, &str)]) -> AppResult<Option<(ResultSet, Vec<Value>)>> {
        let set = self.query(sql, params, 1).await?;
        Ok(set.rows.first().cloned().map(|row| (set, row)))
    }

    /// Every non-null scalar column of a row as a property (arrays / maps skipped).
    fn row_properties(mut detail: ObjectDetail, set: &ResultSet, row: &[Value], skip: &[&str]) -> ObjectDetail {
        for (i, column) in set.columns.iter().enumerate() {
            if skip.contains(&column.name.as_str()) {
                continue;
            }
            match row.get(i) {
                Some(Value::Null) | Some(Value::Json(_)) | None => {}
                Some(v) => detail = detail.property(&column.name, cell_text(Some(v)).unwrap_or_default()),
            }
        }
        detail
    }

    async fn show_create(&self, statement: &str) -> Option<String> {
        self.query(statement, &[], 1).await.ok().and_then(|set| cell_text(first_cell(&set)))
    }

    async fn list_database_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self.query(&format!("SELECT name, engine, comment FROM system.databases WHERE name NOT IN {SYSTEM_DATABASES} ORDER BY name"), &[], MAX_OBJECTS).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let mut s = ObjectSummary::new(ObjectKind::Database, named_text(&set, row, "name"), None).with_badge(named_text(&set, row, "engine"));
                let comment = named_text(&set, row, "comment");
                if !comment.is_empty() {
                    s = s.with_detail(comment);
                }
                s
            })
            .collect())
    }

    async fn list_table_objects(&self, kind: ObjectKind, db: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let sql = format!(
            "SELECT database, name, engine, total_rows, total_bytes, comment FROM system.tables WHERE {} AND NOT is_temporary ORDER BY database, name",
            scope_sql("database", db)
        );
        let set = self.query(&sql, &scope_params(db), 20_000).await?;
        Ok(set
            .rows
            .iter()
            .filter(|row| table_kind(&named_text(&set, row, "engine")) == kind)
            .map(|row| {
                let engine = named_text(&set, row, "engine");
                let mut s = ObjectSummary::new(kind, named_text(&set, row, "name"), Some(named_text(&set, row, "database"))).with_badge(engine);
                let comment = named_text(&set, row, "comment");
                match size_caption(named_i64(&set, row, "total_rows"), named_i64(&set, row, "total_bytes")) {
                    Some(caption) => s = s.with_detail(caption),
                    None if !comment.is_empty() => s = s.with_detail(comment),
                    None => {}
                }
                s
            })
            .collect())
    }

    async fn list_partition_objects(&self, db: Option<&str>, table: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut sql = format!(
            "SELECT database, table, partition, partition_id, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, count() AS parts \
             FROM system.parts WHERE active AND {}",
            scope_sql("database", db)
        );
        let mut params = scope_params(db);
        if let Some(t) = table {
            sql.push_str(" AND table = {t:String}");
            params.push(("t", t));
        }
        sql.push_str(&format!(" GROUP BY database, table, partition, partition_id ORDER BY database, table, partition LIMIT {MAX_OBJECTS}"));
        let set = self.query(&sql, &params, MAX_OBJECTS).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let owner = format!("{}.{}", named_text(&set, row, "database"), named_text(&set, row, "table"));
                let caption = format!(
                    "{} · {} · {} parts",
                    named_text(&set, row, "table"),
                    size_caption(named_i64(&set, row, "rows"), named_i64(&set, row, "bytes")).unwrap_or_default(),
                    named_i64(&set, row, "parts").unwrap_or(0)
                );
                ObjectSummary::new(ObjectKind::Partition, named_text(&set, row, "partition"), Some(owner)).with_detail(caption)
            })
            .collect())
    }

    async fn list_dictionary_objects(&self, db: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let (filter, params) = match db {
            Some(d) => (" WHERE database = {db:String}".to_string(), vec![("db", d)]),
            None => (String::new(), Vec::new()),
        };
        let sql = format!("SELECT database, name, status, type, element_count, bytes_allocated, last_exception FROM system.dictionaries{filter} ORDER BY database, name");
        let set = self.query(&sql, &params, MAX_OBJECTS).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let database = named_text(&set, row, "database");
                let mut caption = format!(
                    "{} · {} elements · {}",
                    named_text(&set, row, "type"),
                    crate::model::objects::format_number(named_i64(&set, row, "element_count").unwrap_or(0) as f64),
                    format_bytes(named_i64(&set, row, "bytes_allocated").unwrap_or(0) as f64)
                );
                let exception = named_text(&set, row, "last_exception");
                if !exception.is_empty() {
                    caption = format!("{caption} · {}", preview(&exception, 60));
                }
                ObjectSummary::new(ObjectKind::Dictionary, named_text(&set, row, "name"), Some(database).filter(|d| !d.is_empty()))
                    .with_detail(caption)
                    .with_badge(named_text(&set, row, "status").to_ascii_lowercase())
            })
            .collect())
    }

    async fn projections_from_ddl(&self, db: Option<&str>, table: Option<&str>) -> AppResult<Vec<(String, String, String, String)>> {
        let mut sql = format!(
            "SELECT database, name, create_table_query FROM system.tables WHERE {} AND create_table_query LIKE '%PROJECTION%'",
            scope_sql("database", db)
        );
        let mut params = scope_params(db);
        if let Some(t) = table {
            sql.push_str(" AND name = {t:String}");
            params.push(("t", t));
        }
        sql.push_str(" ORDER BY database, name");
        let set = self.query(&sql, &params, 20_000).await?;
        let mut out = Vec::new();
        for row in &set.rows {
            let database = named_text(&set, row, "database");
            let name = named_text(&set, row, "name");
            for (projection, body) in parse_projections(&named_text(&set, row, "create_table_query")) {
                out.push((database.clone(), name.clone(), projection, body));
            }
        }
        Ok(out)
    }

    // WHAT:  `system.projections` (24.x+) when present, else the PROJECTION
    //        clauses parsed out of each table's `create_table_query`.
    async fn list_projection_objects(&self, db: Option<&str>, table: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut sql = format!("SELECT database, table, name, type, query FROM system.projections WHERE {}", scope_sql("database", db));
        let mut params = scope_params(db);
        if let Some(t) = table {
            sql.push_str(" AND table = {t:String}");
            params.push(("t", t));
        }
        sql.push_str(" ORDER BY database, table, name");
        if let Ok(set) = self.query(&sql, &params, MAX_OBJECTS).await {
            return Ok(set
                .rows
                .iter()
                .map(|row| {
                    let owner = format!("{}.{}", named_text(&set, row, "database"), named_text(&set, row, "table"));
                    ObjectSummary::new(ObjectKind::Projection, named_text(&set, row, "name"), Some(owner))
                        .with_detail(preview(&named_text(&set, row, "query"), 80))
                        .with_badge(named_text(&set, row, "type").to_ascii_lowercase())
                })
                .collect());
        }
        Ok(self
            .projections_from_ddl(db, table)
            .await?
            .into_iter()
            .map(|(database, table, name, body)| {
                ObjectSummary::new(ObjectKind::Projection, name, Some(format!("{database}.{table}"))).with_detail(preview(&body, 80))
            })
            .collect())
    }

    async fn list_function_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        // Servers older than SQL UDFs (21.10) have no `origin` column: nothing to list.
        let Ok(set) = self.query("SELECT name, create_query FROM system.functions WHERE origin = 'SQLUserDefined' ORDER BY name", &[], MAX_OBJECTS).await else {
            return Ok(Vec::new());
        };
        Ok(set
            .rows
            .iter()
            .map(|row| {
                ObjectSummary::new(ObjectKind::Function, named_text(&set, row, "name"), None)
                    .with_detail(preview(&named_text(&set, row, "create_query"), 80))
                    .with_badge("sql")
            })
            .collect())
    }

    async fn list_user_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self
            .query("SELECT name, storage, toString(auth_type) AS auth_type, default_database FROM system.users ORDER BY name", &[], MAX_OBJECTS)
            .await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let mut caption = named_text(&set, row, "auth_type").trim_matches(|c| c == '[' || c == ']' || c == '\'').to_string();
                let default_db = named_text(&set, row, "default_database");
                if !default_db.is_empty() {
                    caption = format!("{caption} · default db {default_db}");
                }
                ObjectSummary::new(ObjectKind::User, named_text(&set, row, "name"), None).with_detail(caption).with_badge(named_text(&set, row, "storage"))
            })
            .collect())
    }

    async fn list_role_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self.query("SELECT name, storage FROM system.roles ORDER BY name", &[], MAX_OBJECTS).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| ObjectSummary::new(ObjectKind::Role, named_text(&set, row, "name"), None).with_badge(named_text(&set, row, "storage")))
            .collect())
    }

    async fn list_quota_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self
            .query("SELECT name, storage, toString(keys) AS keys, toString(durations) AS durations, apply_to_all, toString(apply_to_list) AS apply_to_list FROM system.quotas ORDER BY name", &[], MAX_OBJECTS)
            .await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let applies = if named_i64(&set, row, "apply_to_all").unwrap_or(0) != 0 { "all users".to_string() } else { named_text(&set, row, "apply_to_list") };
                let caption = format!("keyed by {} · durations {} · {applies}", named_text(&set, row, "keys"), named_text(&set, row, "durations"));
                ObjectSummary::new(ObjectKind::Quota, named_text(&set, row, "name"), None).with_detail(caption).with_badge(named_text(&set, row, "storage"))
            })
            .collect())
    }

    async fn list_setting_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self.query("SELECT name, value, changed, type FROM system.settings ORDER BY name", &[], MAX_OBJECTS).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let mut s = ObjectSummary::new(ObjectKind::Setting, named_text(&set, row, "name"), None).with_detail(preview(&named_text(&set, row, "value"), 60));
                if named_i64(&set, row, "changed").unwrap_or(0) != 0 {
                    s = s.with_badge("changed");
                }
                s
            })
            .collect())
    }

    async fn list_session_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self
            .query("SELECT query_id, user, address, elapsed, read_rows, memory_usage, query FROM system.processes ORDER BY elapsed DESC", &[], MAX_OBJECTS)
            .await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let caption = format!(
                    "{:.1}s · {} rows · {} · {}",
                    cell_f64(named(&set, row, "elapsed")).unwrap_or(0.0),
                    crate::model::objects::format_number(named_i64(&set, row, "read_rows").unwrap_or(0) as f64),
                    format_bytes(named_i64(&set, row, "memory_usage").unwrap_or(0) as f64),
                    preview(&named_text(&set, row, "query"), 60)
                );
                ObjectSummary::new(ObjectKind::Session, named_text(&set, row, "query_id"), None).with_detail(caption).with_badge(named_text(&set, row, "user"))
            })
            .collect())
    }

    async fn list_replica_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self
            .query(
                "SELECT database, table, is_leader, is_readonly, is_session_expired, absolute_delay, queue_size, total_replicas, active_replicas \
                 FROM system.replicas ORDER BY database, table",
                &[],
                MAX_OBJECTS,
            )
            .await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let caption = format!(
                    "queue {} · delay {}s · {}/{} replicas active",
                    named_i64(&set, row, "queue_size").unwrap_or(0),
                    named_i64(&set, row, "absolute_delay").unwrap_or(0),
                    named_i64(&set, row, "active_replicas").unwrap_or(0),
                    named_i64(&set, row, "total_replicas").unwrap_or(0)
                );
                let badge = if named_i64(&set, row, "is_session_expired").unwrap_or(0) != 0 {
                    "session expired"
                } else if named_i64(&set, row, "is_readonly").unwrap_or(0) != 0 {
                    "readonly"
                } else if named_i64(&set, row, "is_leader").unwrap_or(0) != 0 {
                    "leader"
                } else {
                    "replica"
                };
                ObjectSummary::new(ObjectKind::Replica, named_text(&set, row, "table"), Some(named_text(&set, row, "database"))).with_detail(caption).with_badge(badge)
            })
            .collect())
    }

    async fn list_node_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self
            .query(
                "SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local, errors_count, slowdowns_count \
                 FROM system.clusters ORDER BY cluster, shard_num, replica_num",
                &[],
                MAX_OBJECTS,
            )
            .await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let name = format!("{}:{}", named_text(&set, row, "host_name"), named_text(&set, row, "port"));
                let mut caption = format!("{} · errors {}", named_text(&set, row, "host_address"), named_i64(&set, row, "errors_count").unwrap_or(0));
                if named_i64(&set, row, "is_local").unwrap_or(0) != 0 {
                    caption.push_str(" · local");
                }
                let badge = format!("shard {} · replica {}", named_i64(&set, row, "shard_num").unwrap_or(0), named_i64(&set, row, "replica_num").unwrap_or(0));
                ObjectSummary::new(ObjectKind::Node, name, Some(named_text(&set, row, "cluster"))).with_detail(caption).with_badge(badge)
            })
            .collect())
    }

    async fn list_slow_query_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self
            .query(
                "SELECT query_id, event_time, query_duration_ms, read_rows, memory_usage, user, query FROM system.query_log \
                 WHERE type = 'QueryFinish' AND event_time > now() - INTERVAL 1 DAY ORDER BY query_duration_ms DESC LIMIT 100",
                &[],
                100,
            )
            .await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let caption = format!(
                    "{} ms · {} rows · {} · {}",
                    crate::model::objects::format_number(named_i64(&set, row, "query_duration_ms").unwrap_or(0) as f64),
                    crate::model::objects::format_number(named_i64(&set, row, "read_rows").unwrap_or(0) as f64),
                    named_text(&set, row, "event_time"),
                    preview(&named_text(&set, row, "query"), 60)
                );
                ObjectSummary::new(ObjectKind::SlowQuery, named_text(&set, row, "query_id"), None).with_detail(caption).with_badge(named_text(&set, row, "user"))
            })
            .collect())
    }

    async fn database_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let db = &reference.name;
        let Some((set, row)) = self.one_row("SELECT name, engine, uuid, data_path, metadata_path, comment FROM system.databases WHERE name = {db:String}", &[("db", db)]).await? else {
            return Err(AppError::not_found(format!("Database {db} not found.")));
        };
        let mut detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["name"]);
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE DATABASE {}", quote_ident(db))).await {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        let tables = self
            .query(
                "SELECT name, engine, total_rows, total_bytes FROM system.tables WHERE database = {db:String} AND NOT is_temporary ORDER BY name",
                &[("db", db)],
                MAX_OBJECTS,
            )
            .await
            .unwrap_or(ResultSet { columns: vec![], rows: vec![], truncated: false });
        detail.children = tables
            .rows
            .iter()
            .map(|row| {
                let engine = named_text(&tables, row, "engine");
                let mut s = ObjectSummary::new(table_kind(&engine), named_text(&tables, row, "name"), Some(db.clone())).with_badge(engine);
                if let Some(caption) = size_caption(named_i64(&tables, row, "total_rows"), named_i64(&tables, row, "total_bytes")) {
                    s = s.with_detail(caption);
                }
                s
            })
            .collect();
        Ok(detail.action(ObjectAction::destructive("drop", "Drop database", format!("DROP DATABASE {}", quote_ident(db)))))
    }

    async fn table_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let db = self.database_of(reference);
        let name = &reference.name;
        let Some((set, row)) = self
            .one_row(
                "SELECT engine, engine_full, total_rows, total_bytes, partition_key, sorting_key, primary_key, sampling_key, comment, \
                 metadata_modification_time, storage_policy, create_table_query FROM system.tables WHERE database = {db:String} AND name = {t:String}",
                &[("db", &db), ("t", name)],
            )
            .await?
        else {
            return Err(AppError::not_found(format!("Table {db}.{name} not found.")));
        };
        let mut detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["create_table_query", "engine_full"]);
        let ddl = named_text(&set, &row, "create_table_query");
        if !ddl.is_empty() {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        if let Some(bytes) = named_i64(&set, &row, "total_bytes") {
            detail = detail.property("size", format_bytes(bytes as f64));
        }
        detail.columns = self.columns(&TableRef { schema: Some(db.clone()), name: name.clone() }).await.unwrap_or_default();
        let kind = table_kind(&named_text(&set, &row, "engine"));
        if kind == ObjectKind::Table {
            let mut children = self.list_partition_objects(Some(&db), Some(name)).await.unwrap_or_default();
            children.extend(self.list_projection_objects(Some(&db), Some(name)).await.unwrap_or_default());
            detail.children = children;
        }
        detail.actions = table_actions(kind, &ident(&db, name));
        Ok(detail)
    }

    async fn partition_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let (db, table) = self.owner(reference)?;
        let partition = &reference.name;
        let set = self
            .query(
                "SELECT name, partition_id, rows, bytes_on_disk, modification_time, min_time, max_time, level, disk_name \
                 FROM system.parts WHERE active AND database = {db:String} AND table = {t:String} AND partition = {p:String} ORDER BY name",
                &[("db", &db), ("t", &table), ("p", partition)],
                MAX_OBJECTS,
            )
            .await?;
        let Some(first) = set.rows.first() else {
            return Err(AppError::not_found(format!("Partition {partition} of {db}.{table} has no active parts.")));
        };
        let partition_id = named_text(&set, first, "partition_id");
        let rows: i64 = set.rows.iter().filter_map(|r| named_i64(&set, r, "rows")).sum();
        let bytes: i64 = set.rows.iter().filter_map(|r| named_i64(&set, r, "bytes_on_disk")).sum();
        let mut detail = ObjectDetail::empty(reference)
            .property("table", format!("{db}.{table}"))
            .property("partition_id", partition_id.clone())
            .property("parts", set.rows.len().to_string())
            .property("rows", crate::model::objects::format_number(rows as f64))
            .property("size", format_bytes(bytes as f64));
        detail.rows = Some(set);
        detail.actions = partition_actions(&ident(&db, &table), &partition_id);
        Ok(detail)
    }

    async fn dictionary_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let db = reference.parent.clone().unwrap_or_default();
        let name = &reference.name;
        let Some((set, row)) = self.one_row("SELECT * FROM system.dictionaries WHERE database = {db:String} AND name = {n:String}", &[("db", &db), ("n", name)]).await? else {
            return Err(AppError::not_found(format!("Dictionary {name} not found.")));
        };
        let mut detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["name", "database", "uuid"]);
        if let Some(bytes) = named_i64(&set, &row, "bytes_allocated") {
            detail = detail.property("size", format_bytes(bytes as f64));
        }
        let qualified = if db.is_empty() { quote_ident(name) } else { ident(&db, name) };
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE DICTIONARY {qualified}")).await {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        detail = detail.action(ObjectAction::new("reload", "Reload dictionary", format!("SYSTEM RELOAD DICTIONARY {qualified}")));
        if !db.is_empty() {
            detail = detail.action(ObjectAction::destructive("drop", "Drop dictionary", format!("DROP DICTIONARY {qualified}")));
        }
        Ok(detail)
    }

    async fn projection_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let (db, table) = self.owner(reference)?;
        let name = &reference.name;
        let mut detail = ObjectDetail::empty(reference).property("table", format!("{db}.{table}"));
        let from_system = self
            .one_row(
                "SELECT type, sorting_key, query FROM system.projections WHERE database = {db:String} AND table = {t:String} AND name = {n:String}",
                &[("db", &db), ("t", &table), ("n", name)],
            )
            .await
            .ok()
            .flatten();
        match from_system {
            Some((set, row)) => {
                detail = Self::row_properties(detail, &set, &row, &["query"]).definition(named_text(&set, &row, "query"), CodeLanguage::Sql);
            }
            None => {
                let body = self
                    .projections_from_ddl(Some(&db), Some(&table))
                    .await?
                    .into_iter()
                    .find(|(_, _, n, _)| n == name)
                    .map(|(_, _, _, body)| body)
                    .ok_or_else(|| AppError::not_found(format!("Projection {name} not found on {db}.{table}.")))?;
                detail = detail.definition(body, CodeLanguage::Sql);
            }
        }
        detail.actions = projection_actions(&ident(&db, &table), name);
        Ok(detail)
    }

    async fn function_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = &reference.name;
        let Some((set, row)) = self.one_row("SELECT name, create_query, origin FROM system.functions WHERE name = {n:String}", &[("n", name)]).await? else {
            return Err(AppError::not_found(format!("Function {name} not found.")));
        };
        let detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["name", "create_query"]);
        Ok(detail
            .definition(named_text(&set, &row, "create_query"), CodeLanguage::Sql)
            .action(ObjectAction::destructive("drop", "Drop function", format!("DROP FUNCTION {}", quote_ident(name)))))
    }

    async fn principal_detail(&self, reference: &ObjectRef, table: &str, keyword: &str) -> AppResult<ObjectDetail> {
        let name = &reference.name;
        let Some((set, row)) = self.one_row(&format!("SELECT * FROM system.{table} WHERE name = {{n:String}}"), &[("n", name)]).await? else {
            return Err(AppError::not_found(format!("{keyword} {name} not found.")));
        };
        let mut detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["name", "id"]);
        if let Some(ddl) = self.show_create(&format!("SHOW CREATE {keyword} {}", quote_ident(name))).await {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        if keyword != "QUOTA" {
            if let Ok(grants) = self.query(&format!("SHOW GRANTS FOR {}", quote_ident(name)), &[], MAX_OBJECTS).await {
                detail.rows = Some(grants);
            }
        } else if let Ok(usage) = self.query("SELECT * FROM system.quotas_usage WHERE quota_name = {n:String}", &[("n", name)], MAX_OBJECTS).await {
            detail.rows = Some(usage);
        }
        let label = format!("Drop {}", keyword.to_ascii_lowercase());
        Ok(detail.action(ObjectAction::destructive("drop", &label, format!("DROP {keyword} {}", quote_ident(name)))))
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = &reference.name;
        let Some((set, row)) = self.one_row("SELECT * FROM system.settings WHERE name = {n:String}", &[("n", name)]).await? else {
            return Err(AppError::not_found(format!("Setting {name} not found.")));
        };
        let value = named_text(&set, &row, "value");
        let detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["name"]).definition(format!("SET {name} = {}", quote_literal(&value)), CodeLanguage::Sql);
        Ok(detail.action(ObjectAction::destructive("reset", "Reset to default (this session)", format!("SET {name} = DEFAULT"))))
    }

    async fn session_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let id = &reference.name;
        let Some((set, row)) = self.one_row("SELECT * FROM system.processes WHERE query_id = {q:String}", &[("q", id)]).await? else {
            return Err(AppError::not_found(format!("Query {id} is no longer running.")));
        };
        let detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["query", "query_id"]).definition(named_text(&set, &row, "query"), CodeLanguage::Sql);
        Ok(detail.action(ObjectAction::destructive("kill", "Kill query", format!("KILL QUERY WHERE query_id = {}", quote_literal(id)))))
    }

    async fn replica_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let db = self.database_of(reference);
        let table = &reference.name;
        let Some((set, row)) = self.one_row("SELECT * FROM system.replicas WHERE database = {db:String} AND table = {t:String}", &[("db", &db), ("t", table)]).await? else {
            return Err(AppError::not_found(format!("{db}.{table} is not a replicated table.")));
        };
        let detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["database", "table"]);
        let name = ident(&db, table);
        Ok(detail
            .action(ObjectAction::new("sync", "Sync replica", format!("SYSTEM SYNC REPLICA {name}")))
            .action(ObjectAction::destructive("restart", "Restart replica", format!("SYSTEM RESTART REPLICA {name}")))
            .action(ObjectAction::destructive("restore", "Restore replica", format!("SYSTEM RESTORE REPLICA {name}"))))
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let cluster = reference.parent.clone().unwrap_or_default();
        let set = self.query("SELECT * FROM system.clusters WHERE cluster = {c:String}", &[("c", &cluster)], MAX_OBJECTS).await?;
        let row = set
            .rows
            .iter()
            .find(|row| format!("{}:{}", named_text(&set, row, "host_name"), named_text(&set, row, "port")) == reference.name)
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Node {} not found in cluster {cluster}.", reference.name)))?;
        Ok(Self::row_properties(ObjectDetail::empty(reference), &set, &row, &[]))
    }

    async fn slow_query_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let id = &reference.name;
        let Some((set, row)) = self
            .one_row("SELECT * FROM system.query_log WHERE query_id = {q:String} AND type = 'QueryFinish' ORDER BY event_time DESC LIMIT 1", &[("q", id)])
            .await?
        else {
            return Err(AppError::not_found(format!("Query {id} is not in system.query_log.")));
        };
        let mut detail = Self::row_properties(ObjectDetail::empty(reference), &set, &row, &["query", "query_id", "formatted_query"]).definition(named_text(&set, &row, "query"), CodeLanguage::Sql);
        for column in ["read_bytes", "result_bytes", "memory_usage"] {
            if let Some(bytes) = named_i64(&set, &row, column) {
                detail = detail.property(&format!("{column} (human)"), format_bytes(bytes as f64));
            }
        }
        Ok(detail)
    }

    async fn metric_values(&self, sql: &str) -> BTreeMap<String, f64> {
        let Ok(set) = self.query(sql, &[], 1_000).await else {
            return BTreeMap::new();
        };
        set.rows
            .iter()
            .filter_map(|row| Some((cell_text(row.first())?, cell_f64(row.get(1))?)))
            .collect()
    }
}

fn kind_label(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Partition => "Partition",
        ObjectKind::Projection => "Projection",
        _ => "Object",
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, sql: true, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: false, exact_estimate: false },
        object_kinds: vec![K::Database, K::Table, K::View, K::MaterializedView, K::Partition, K::Dictionary, K::Projection, K::Function, K::User, K::Role, K::Quota, K::Setting, K::Session, K::Replica, K::Node, K::SlowQuery],
        tools: vec![T::Stats],
    }
}

#[async_trait]
impl Integration for ClickhouseIntegration {
    fn engine(&self) -> Engine {
        Engine::Clickhouse
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        self.post("SELECT 1", &[]).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let set = self.query("SELECT version()", &[], 1).await?;
        Ok(cell_text(first_cell(&set)).map(|v| format!("ClickHouse {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let set = self
            .query(
                "SELECT name FROM system.databases \
                 WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY name",
                &[],
                10_000,
            )
            .await?;
        Ok(set.rows.iter().filter_map(|r| cell_text(r.first())).collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let set = self
            .query(
                "SELECT name, engine, total_rows FROM system.tables WHERE database = {db:String} ORDER BY name",
                &[("db", self.database.as_str())],
                100_000,
            )
            .await?;
        let tables: Vec<TableInfo> = set
            .rows
            .iter()
            .filter_map(|row| {
                let name = cell_text(row.first())?;
                let engine = cell_text(row.get(1)).unwrap_or_default();
                let kind = if engine.contains("View") { TableKind::View } else { TableKind::Table };
                Some(TableInfo { schema: Some(self.database.clone()), name, kind, row_estimate: cell_i64(row.get(2)) })
            })
            .collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let set = self
            .query(
                "SELECT name, type, position, is_in_primary_key FROM system.columns \
                 WHERE database = {db:String} AND table = {t:String} ORDER BY position",
                &[("db", self.schema_of(table)), ("t", table.name.as_str())],
                10_000,
            )
            .await?;
        Ok(set
            .rows
            .iter()
            .filter_map(|row| {
                let name = cell_text(row.first())?;
                let data_type = cell_text(row.get(1)).unwrap_or_default();
                Some(ColumnInfo {
                    nullable: data_type.starts_with("Nullable("),
                    primary_key: cell_i64(row.get(3)).unwrap_or(0) != 0,
                    ordinal: u32::try_from(cell_i64(row.get(2)).unwrap_or(0)).unwrap_or_default(),
                    name,
                    data_type,
                })
            })
            .collect())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let set = self
            .query(
                "SELECT total_rows FROM system.tables WHERE database = {db:String} AND name = {t:String}",
                &[("db", self.schema_of(table)), ("t", table.name.as_str())],
                1,
            )
            .await?;
        Ok(cell_i64(first_cell(&set)))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count() FROM {}{}", self.qualified(table), where_clause(Engine::Clickhouse, filters));
        let set = self.query(&sql, &[], 1).await?;
        cell_i64(first_cell(&set)).ok_or_else(|| AppError::driver("count() returned no value"))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            self.qualified(table),
            where_clause(Engine::Clickhouse, &query.filters),
            order_clause(Engine::Clickhouse, &query.sort),
            query.limit,
            query.offset
        );
        self.query(&sql, &[], query.limit as usize).await
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut out = Vec::new();
        for statement in split_statements(sql) {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }
            out.push(self.run_statement(trimmed, max_rows).await?);
        }
        Ok(out)
    }

    async fn close(&self) {}

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let set = self.query(&format!("SHOW CREATE TABLE {}", self.qualified(table)), &[], 1).await?;
        Ok(cell_text(first_cell(&set)))
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let (db, table) = split_parent(parent);
        let mut out = match kind {
            ObjectKind::Database => self.list_database_objects().await?,
            ObjectKind::Table | ObjectKind::View | ObjectKind::MaterializedView => self.list_table_objects(kind, db).await?,
            ObjectKind::Partition => self.list_partition_objects(db, table).await?,
            ObjectKind::Dictionary => self.list_dictionary_objects(db).await?,
            ObjectKind::Projection => self.list_projection_objects(db, table).await?,
            ObjectKind::Function => self.list_function_objects().await?,
            ObjectKind::User => self.list_user_objects().await?,
            ObjectKind::Role => self.list_role_objects().await?,
            ObjectKind::Quota => self.list_quota_objects().await?,
            ObjectKind::Setting => self.list_setting_objects().await?,
            ObjectKind::Session => self.list_session_objects().await?,
            ObjectKind::Replica => self.list_replica_objects().await?,
            ObjectKind::Node => self.list_node_objects().await?,
            ObjectKind::SlowQuery => self.list_slow_query_objects().await?,
            _ => Vec::new(),
        };
        out.truncate(MAX_OBJECTS);
        Ok(out)
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Database => self.database_detail(reference).await,
            ObjectKind::Table | ObjectKind::View | ObjectKind::MaterializedView => self.table_detail(reference).await,
            ObjectKind::Partition => self.partition_detail(reference).await,
            ObjectKind::Dictionary => self.dictionary_detail(reference).await,
            ObjectKind::Projection => self.projection_detail(reference).await,
            ObjectKind::Function => self.function_detail(reference).await,
            ObjectKind::User => self.principal_detail(reference, "users", "USER").await,
            ObjectKind::Role => self.principal_detail(reference, "roles", "ROLE").await,
            ObjectKind::Quota => self.principal_detail(reference, "quotas", "QUOTA").await,
            ObjectKind::Setting => self.setting_detail(reference).await,
            ObjectKind::Session => self.session_detail(reference).await,
            ObjectKind::Replica => self.replica_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
            ObjectKind::SlowQuery => self.slow_query_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    // WHAT:  Gauges from system.metrics / system.asynchronous_metrics and
    //        counters from system.events, grouped the way the overview shows them.
    async fn server_stats(&self) -> AppResult<ServerStats> {
        let version = self.server_version().await?.unwrap_or_else(|| "ClickHouse".to_string());
        let metrics = self
            .metric_values(
                "SELECT metric, value FROM system.metrics WHERE metric IN \
                 ('Query', 'TCPConnection', 'HTTPConnection', 'InterserverConnection', 'MemoryTracking', 'BackgroundMergesAndMutationsPoolTask', 'PartsActive')",
            )
            .await;
        let async_metrics = self
            .metric_values(
                "SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN \
                 ('Uptime', 'OSMemoryTotal', 'MarkCacheBytes', 'UncompressedCacheBytes', 'TotalPartsOfMergeTreeTables', 'NumberOfTables', 'NumberOfDatabases')",
            )
            .await;
        let events = self
            .metric_values(
                "SELECT event, value FROM system.events WHERE event IN \
                 ('Query', 'SelectQuery', 'InsertQuery', 'FailedQuery', 'InsertedRows', 'SelectedRows', 'InsertedBytes', 'SelectedBytes')",
            )
            .await;
        let get = |map: &BTreeMap<String, f64>, key: &str| map.get(key).copied().unwrap_or(0.0);
        let mb = |bytes: f64| (bytes / 1_048_576.0 * 10.0).round() / 10.0;
        let uptime = get(&async_metrics, "Uptime");
        let groups = vec![
            StatGroup {
                title: "Server".into(),
                stats: vec![
                    Stat::text("Version", version),
                    Stat::text("Uptime", format_duration(uptime)).with_hint(format!("{uptime} s")),
                    Stat::number("Databases", get(&async_metrics, "NumberOfDatabases"), None),
                    Stat::number("Tables", get(&async_metrics, "NumberOfTables"), None),
                ],
            },
            StatGroup {
                title: "Connections".into(),
                stats: vec![
                    Stat::number("Running queries", get(&metrics, "Query"), None),
                    Stat::number("TCP connections", get(&metrics, "TCPConnection"), None),
                    Stat::number("HTTP connections", get(&metrics, "HTTPConnection"), None),
                    Stat::number("Interserver connections", get(&metrics, "InterserverConnection"), None),
                ],
            },
            StatGroup {
                title: "Memory".into(),
                stats: vec![
                    Stat::number("Tracked memory", mb(get(&metrics, "MemoryTracking")), Some("MB")).with_hint(format_bytes(get(&metrics, "MemoryTracking"))),
                    Stat::number("OS memory total", mb(get(&async_metrics, "OSMemoryTotal")), Some("MB")),
                    Stat::number("Mark cache", mb(get(&async_metrics, "MarkCacheBytes")), Some("MB")),
                    Stat::number("Uncompressed cache", mb(get(&async_metrics, "UncompressedCacheBytes")), Some("MB")),
                ],
            },
            StatGroup {
                title: "Storage".into(),
                stats: vec![
                    Stat::number("MergeTree parts", get(&async_metrics, "TotalPartsOfMergeTreeTables"), None),
                    Stat::number("Active parts", get(&metrics, "PartsActive"), None),
                    Stat::number("Background merges / mutations", get(&metrics, "BackgroundMergesAndMutationsPoolTask"), None),
                ],
            },
            StatGroup {
                title: "Throughput".into(),
                stats: vec![
                    Stat::number("Queries", get(&events, "Query"), None).with_hint("since start"),
                    Stat::number("SELECT queries", get(&events, "SelectQuery"), None),
                    Stat::number("INSERT queries", get(&events, "InsertQuery"), None),
                    Stat::number("Failed queries", get(&events, "FailedQuery"), None),
                    Stat::number("Rows inserted", get(&events, "InsertedRows"), None),
                    Stat::number("Rows selected", get(&events, "SelectedRows"), None),
                    Stat::number("Bytes inserted", mb(get(&events, "InsertedBytes")), Some("MB")),
                    Stat::number("Bytes selected", mb(get(&events, "SelectedBytes")), Some("MB")),
                ],
            },
        ];
        Ok(ServerStats::now(groups))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, FilterOp, SortRule};

    #[test]
    fn base_type_unwraps_wrappers() {
        assert_eq!(base_type("Nullable(UInt64)"), "UInt64");
        assert_eq!(base_type("LowCardinality(Nullable(String))"), "String");
        assert_eq!(base_type("DateTime('UTC')"), "DateTime('UTC')");
        assert_eq!(base_type("Decimal(10, 2)"), "Decimal(10, 2)");
    }

    #[test]
    fn jsoncompact_decodes_every_kind() {
        let body = r#"{
            "meta": [
                {"name":"id","type":"UInt64"},
                {"name":"name","type":"String"},
                {"name":"maybe","type":"Nullable(String)"},
                {"name":"ts","type":"DateTime"},
                {"name":"n","type":"Decimal(10, 2)"},
                {"name":"ok","type":"Bool"},
                {"name":"tags","type":"Array(String)"},
                {"name":"f","type":"Float64"},
                {"name":"big","type":"UInt64"},
                {"name":"quoted","type":"Int64"},
                {"name":"dnum","type":"Decimal(10, 2)"}
            ],
            "data": [
                [1, "ann", null, "2026-09-04 10:00:00", "12.50", true, ["a","b"], 1.5, 18446744073709551615, "42", 12.5],
                [2, "bob", "x", "2026-09-04 11:00:00", "0.10", false, [], "nan", 3, "7", 3]
            ],
            "rows": 2
        }"#;
        let set = decode_compact(body, 10).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(set.columns.len(), 11);
        assert_eq!(set.columns.first().map(|c| c.type_name.as_str()), Some("UInt64"));
        assert!(!set.truncated);
        let first = set.rows.first().cloned().unwrap_or_default();
        assert_eq!(first.first(), Some(&Value::Int(1)));
        assert_eq!(first.get(1), Some(&Value::Text("ann".into())));
        assert_eq!(first.get(2), Some(&Value::Null));
        assert_eq!(first.get(3), Some(&Value::DateTime("2026-09-04 10:00:00".into())));
        assert_eq!(first.get(4), Some(&Value::Decimal("12.50".into())));
        assert_eq!(first.get(5), Some(&Value::Bool(true)));
        assert!(matches!(first.get(6), Some(Value::Json(serde_json::Value::Array(a))) if a.len() == 2));
        assert_eq!(first.get(7), Some(&Value::Float(1.5)));
        assert_eq!(first.get(8), Some(&Value::Decimal("18446744073709551615".into())));
        assert_eq!(first.get(9), Some(&Value::Int(42)));
        assert_eq!(first.get(10), Some(&Value::Decimal("12.5".into())));
        let second = set.rows.get(1).cloned().unwrap_or_default();
        assert!(matches!(second.get(7), Some(Value::Float(f)) if f.is_nan()));
        assert_eq!(second.get(8), Some(&Value::Int(3)));
    }

    #[test]
    fn row_cap_marks_truncation() {
        let body = r#"{"meta":[{"name":"x","type":"UInt8"}],"data":[[1],[2],[3]],"rows":3}"#;
        let set = decode_compact(body, 2).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(set.rows.len(), 2);
        assert!(set.truncated);
    }

    #[test]
    fn non_json_bodies_become_text_rows() {
        let set = decode_body("line one\nline two", 10);
        assert_eq!(set.columns.first().map(|c| c.name.as_str()), Some("output"));
        assert_eq!(set.rows.len(), 2);
        assert_eq!(set.rows.get(1).and_then(|r| r.first()), Some(&Value::Text("line two".into())));
    }

    #[test]
    fn projections_parse_from_create_table_query() {
        let ddl = "CREATE TABLE db.t (`id` UInt64, `x` String, PROJECTION p_x (SELECT x, count() GROUP BY x), PROJECTION `p2` (SELECT concat(x, ')') ORDER BY x)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192";
        assert_eq!(
            parse_projections(ddl),
            vec![("p_x".to_string(), "SELECT x, count() GROUP BY x".to_string()), ("p2".to_string(), "SELECT concat(x, ')') ORDER BY x".to_string())]
        );
        assert!(parse_projections("CREATE TABLE t (`myPROJECTION x` String) ENGINE = Memory").is_empty());
        assert!(parse_projections("CREATE TABLE t (x String) ENGINE = Memory").is_empty());
        assert!(parse_projections("PROJECTION broken (SELECT x").is_empty());
    }

    #[test]
    fn explorer_helpers_shape_captions_and_statements() {
        assert_eq!(table_kind("MergeTree"), ObjectKind::Table);
        assert_eq!(table_kind("View"), ObjectKind::View);
        assert_eq!(table_kind("MaterializedView"), ObjectKind::MaterializedView);
        assert_eq!(size_caption(Some(1_234_567), Some(2_621_440)).as_deref(), Some("1,234,567 rows · 2.5 MB"));
        assert_eq!(size_caption(None, None), None);
        assert_eq!(format_bytes(0.0), "0 B");
        assert_eq!(format_bytes(1024.0 * 1024.0 * 1024.0 * 3.0), "3.0 GB");
        assert_eq!(format_duration(90_061.0), "1d 1h 1m");
        assert_eq!(format_duration(61.0), "1m 1s");
        assert_eq!(preview("SELECT\n  1,\n  2  FROM t", 8), "SELECT 1…");
        assert_eq!(preview("short", 80), "short");
        assert_eq!(split_parent(Some("db.t")), (Some("db"), Some("t")));
        assert_eq!(split_parent(Some("db")), (Some("db"), None));
        assert_eq!(split_parent(None), (None, None));
        assert_eq!(scope_sql("database", Some("x")), "database = {db:String}");
        assert_eq!(scope_sql("database", None), "database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')");
        assert_eq!(scope_params(Some("x")), vec![("db", "x")]);
        assert!(scope_params(None).is_empty());
        let actions = partition_actions("\"db\".\"t\"", "2024-09");
        assert_eq!(actions[2].statement, "ALTER TABLE \"db\".\"t\" DROP PARTITION ID '2024-09'");
        assert!(actions[2].destructive && !actions[0].destructive);
        let p = projection_actions("\"db\".\"t\"", "p1");
        assert_eq!(p[0].statement, "ALTER TABLE \"db\".\"t\" MATERIALIZE PROJECTION \"p1\"");
        let t = table_actions(ObjectKind::Table, "\"db\".\"t\"");
        assert_eq!(t.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(), vec!["optimize", "truncate", "drop"]);
        assert_eq!(table_actions(ObjectKind::View, "\"db\".\"v\"")[0].statement, "DROP VIEW \"db\".\"v\"");
        let set = ResultSet {
            columns: vec![ColumnMeta { name: "name".into(), type_name: "String".into() }, ColumnMeta { name: "n".into(), type_name: "UInt64".into() }],
            rows: vec![vec![Value::Text("a".into()), Value::Int(7)]],
            truncated: false,
        };
        assert_eq!(named_text(&set, &set.rows[0], "name"), "a");
        assert_eq!(named_i64(&set, &set.rows[0], "n"), Some(7));
        assert_eq!(named_i64(&set, &set.rows[0], "missing"), None);
        assert_eq!(cell_f64(Some(&Value::Decimal("2.5".into()))), Some(2.5));
    }

    #[test]
    fn summary_header_yields_written_rows() {
        assert_eq!(parse_written_rows(r#"{"read_rows":"0","read_bytes":"0","written_rows":"2","written_bytes":"64","total_rows_to_read":"0"}"#), Some(2));
        assert_eq!(parse_written_rows(r#"{"written_rows":5}"#), Some(5));
        assert_eq!(parse_written_rows("not json"), None);
        assert_eq!(parse_written_rows(r#"{"read_rows":"1"}"#), None);
    }

    // WHAT:  Live round trip. Skipped unless DB_FREE_CLICKHOUSE_URL is set, e.g.
    //        DB_FREE_CLICKHOUSE_URL=http://127.0.0.1:8124 DB_FREE_CLICKHOUSE_USER=default DB_FREE_CLICKHOUSE_PASSWORD=dbfree
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DB_FREE_CLICKHOUSE_URL") else {
            return;
        };
        let parsed = reqwest::Url::parse(&url).unwrap_or_else(|e| panic!("bad url: {e}"));
        let input = ConnectionInput {
            name: "live-ch".into(),
            engine: Engine::Clickhouse,
            environment: Environment::Local,
            read_only: false,
            host: parsed.host_str().map(str::to_string),
            port: parsed.port(),
            database: Some("default".into()),
            username: std::env::var("DB_FREE_CLICKHOUSE_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: if parsed.scheme() == "https" { SslMode::Require } else { SslMode::Disable },
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary::draft(&input, true),
            secret: std::env::var("DB_FREE_CLICKHOUSE_PASSWORD").ok(),
        };
        let ch = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        ch.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert!(ch.server_version().await.unwrap_or_default().is_some_and(|v| v.starts_with("ClickHouse ")));
        assert_eq!(ch.current_database(), Some("default".into()));
        assert!(ch.databases().await.unwrap_or_default().iter().any(|d| d == "default"));

        let _ = ch.execute("DROP TABLE IF EXISTS dbfree_t", 10).await;
        let out = ch
            .execute(
                "CREATE TABLE dbfree_t (id UInt32, name String, meta String, ts DateTime, n Decimal(10,2), ok Bool, tags Array(String)) \
                 ENGINE = MergeTree ORDER BY id; \
                 INSERT INTO dbfree_t VALUES (1, 'ann', '{\"a\":1}', '2026-09-04 10:00:00', 12.50, true, ['x','y']), \
                 (2, 'bob', '', '2026-09-04 11:00:00', 0.10, false, []); \
                 SELECT * FROM dbfree_t ORDER BY id;",
                100,
            )
            .await
            .unwrap_or_else(|e| panic!("execute: {e}"));
        assert_eq!(out.len(), 3, "{out:?}");
        assert!(matches!(out.first(), Some(StatementResult::Affected { .. })));
        assert!(matches!(out.get(1), Some(StatementResult::Affected { rows_affected: 2 })), "{out:?}");
        match out.get(2) {
            Some(StatementResult::Rows { result }) => {
                assert_eq!(result.rows.len(), 2);
                assert_eq!(result.columns.len(), 7);
                let first = result.rows.first().cloned().unwrap_or_default();
                assert_eq!(first.first(), Some(&Value::Int(1)));
                assert_eq!(first.get(1), Some(&Value::Text("ann".into())));
                assert_eq!(first.get(2), Some(&Value::Text("{\"a\":1}".into())));
                assert_eq!(first.get(3), Some(&Value::DateTime("2026-09-04 10:00:00".into())));
                // Servers differ on trailing zeros ("12.5" vs "12.50"); the kind and value are what matter.
                assert!(matches!(first.get(4), Some(Value::Decimal(d)) if d.parse::<f64>().ok() == Some(12.5)), "{first:?}");
                assert_eq!(first.get(5), Some(&Value::Bool(true)));
                assert!(matches!(first.get(6), Some(Value::Json(serde_json::Value::Array(a))) if a.len() == 2));
            }
            other => panic!("expected rows, got {other:?}"),
        }

        let catalog = ch.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let table = catalog
            .schemas
            .iter()
            .flat_map(|s| s.tables.iter())
            .find(|t| t.name == "dbfree_t")
            .cloned()
            .unwrap_or_else(|| panic!("dbfree_t missing from catalog"));
        assert_eq!(table.kind, TableKind::Table);
        let table_ref = TableRef { schema: table.schema.clone(), name: table.name.clone() };
        let cols = ch.columns(&table_ref).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(cols.len(), 7);
        assert!(cols.iter().any(|c| c.name == "id" && c.primary_key && c.data_type == "UInt32"));
        assert_eq!(ch.row_estimate(&table_ref).await.unwrap_or_default(), Some(2));
        assert_eq!(ch.count(&table_ref, &[]).await.unwrap_or_default(), 2);

        let query = PageQuery {
            sort: vec![SortRule { column: "id".into(), desc: true }],
            // Upper-case on purpose: ClickHouse LIKE is case-sensitive, so this row is
            // only found when the filter builder reaches for ILIKE (see sql.rs).
            filters: vec![FilterRule { column: "name".into(), op: FilterOp::Contains, value: "N".into() }],
            offset: 0,
            limit: 10,
        };
        let page = ch.fetch_page(&table_ref, &query).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1, "only 'ann' contains n: {page:?}");
        assert_eq!(ch.count(&table_ref, &query.filters).await.unwrap_or_default(), 1);

        let ddl = ch.ddl(&table_ref).await.unwrap_or_else(|e| panic!("ddl: {e}"));
        assert!(ddl.is_some_and(|d| d.contains("CREATE TABLE") && d.contains("dbfree_t")));

        let truncated = ch.execute("SELECT * FROM dbfree_t", 1).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(truncated.first(), Some(StatementResult::Rows { result }) if result.truncated));

        ch.execute("DROP TABLE dbfree_t", 10).await.unwrap_or_else(|e| panic!("drop: {e}"));
        ch.close().await;
    }
}
