// SOT: postgres-integration, sqlx-adapter, pg-value-decoding, pg-catalog-queries, pg-object-explorer, pg-object-detail, pg-server-stats, pgvector-search

use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{qualified_name, quote_ident, Capabilities, Integration};
use crate::error::{AppError, AppResult};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, ObjectAction, ObjectDetail,
    ObjectKind, ObjectProperty, ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, ServerStats, SslMode, Stat, StatGroup, StatementResult, TableInfo,
    TableKind, TableRef, Value, VectorSearchRequest,
};
use async_trait::async_trait;
use base64::Engine as _;
use futures::StreamExt;
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow, PgSslMode, PgValueFormat};
use sqlx::{Column, Either, Executor, Row, TypeInfo, ValueRef};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::Database(db) => AppError::driver(db.message()),
            sqlx::Error::PoolTimedOut => AppError::timeout("Timed out waiting for a connection."),
            other => AppError::driver(other),
        }
    }
}

pub struct PostgresIntegration {
    pool: PgPool,
    database: String,
}

const DEFAULT_DATABASE: &str = "postgres";

fn ssl_mode(mode: SslMode) -> PgSslMode {
    match mode {
        SslMode::Disable => PgSslMode::Disable,
        SslMode::Prefer => PgSslMode::Prefer,
        SslMode::Require => PgSslMode::Require,
        SslMode::VerifyCa => PgSslMode::VerifyCa,
        SslMode::VerifyFull => PgSslMode::VerifyFull,
    }
}

// WHAT:  Opens a small pool (4) — a workbench never needs more, and it keeps the
//        memory baseline low.
pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    // Empty database = the server's maintenance database; the UI then lists every
    // database so the user can switch (see `databases`).
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let mut opts = PgConnectOptions::new()
        .host(s.host.as_deref().unwrap_or("localhost"))
        .port(s.port.unwrap_or(5432))
        .ssl_mode(ssl_mode(s.ssl_mode))
        .application_name("db-free")
        .database(&database);
    if let Some(user) = s.username.as_deref() {
        opts = opts.username(user);
    }
    if let Some(secret) = conn.secret.as_deref() {
        opts = opts.password(secret);
    }
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(15))
        .connect_with(opts)
        .await?;
    Ok(Arc::new(PostgresIntegration { pool, database }))
}

// WHAT:  Decodes a cell from the simple-query (text format) protocol.
// WHY:   Text format is universal: every type prints itself, so unknown or
//        extension types degrade to their textual form instead of failing.
// HOW:   Type name drives parsing for the handful of kinds the grid treats specially.
// WHERE: src-tauri/src/model/value.rs
fn decode_cell(row: &PgRow, index: usize) -> Value {
    let raw = match row.try_get_raw(index) {
        Ok(raw) => raw,
        Err(err) => return Value::Unsupported(err.to_string()),
    };
    if raw.is_null() {
        return Value::Null;
    }
    let type_name = raw.type_info().name().to_ascii_uppercase();
    if raw.format() == PgValueFormat::Binary {
        return Value::Unsupported(format!("<{}>", type_name.to_ascii_lowercase()));
    }
    let text = match raw.as_str() {
        Ok(text) => text,
        Err(_) => return Value::Unsupported(format!("<{}>", type_name.to_ascii_lowercase())),
    };
    match type_name.as_str() {
        "INT2" | "INT4" | "INT8" | "OID" => text
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or_else(|_| Value::Decimal(text.to_string())),
        "FLOAT4" | "FLOAT8" => text
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or_else(|_| Value::Text(text.to_string())),
        "NUMERIC" | "MONEY" => Value::Decimal(text.to_string()),
        "BOOL" => Value::Bool(text == "t" || text == "true"),
        "JSON" | "JSONB" => serde_json::from_str(text)
            .map(Value::Json)
            .unwrap_or_else(|_| Value::Text(text.to_string())),
        "BYTEA" => Value::Bytes(bytea_to_base64(text)),
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATE" | "TIME" | "TIMETZ" | "INTERVAL" => {
            Value::DateTime(text.to_string())
        }
        _ => Value::Text(text.to_string()),
    }
}

fn bytea_to_base64(text: &str) -> String {
    let hex = text.strip_prefix("\\x").unwrap_or(text);
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .filter_map(|i| hex.get(i..i + 2).and_then(|pair| u8::from_str_radix(pair, 16).ok()))
        .collect();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn columns_of(row: &PgRow) -> Vec<ColumnMeta> {
    row.columns()
        .iter()
        .map(|c| ColumnMeta {
            name: c.name().to_string(),
            type_name: c.type_info().name().to_ascii_lowercase(),
        })
        .collect()
}

impl PostgresIntegration {
    // WHAT:  Runs `sql` through the simple protocol and groups rows per statement.
    // HOW:   sqlx yields Right(row) per row and Left(result) when a statement ends.
    async fn run(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
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
                        set.rows.push((0..width).map(|i| decode_cell(&row, i)).collect());
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
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { sql: true, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: true, exact_estimate: false },
        object_kinds: vec![K::Database, K::Schema, K::Table, K::View, K::MaterializedView, K::ForeignTable, K::Partition, K::Index, K::Constraint, K::Sequence, K::Type, K::Function, K::Procedure, K::Aggregate, K::Trigger, K::Rule, K::Policy, K::Extension, K::Publication, K::Subscription, K::ReplicationSlot, K::ForeignServer, K::ForeignDataWrapper, K::Role, K::Grant, K::Tablespace, K::Session, K::Lock, K::Replica, K::Setting, K::SlowQuery],
        tools: vec![T::Stats, T::Erd, T::VectorSearch],
    }
}

#[async_trait]
impl Integration for PostgresIntegration {
    fn engine(&self) -> Engine {
        Engine::Postgres
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let version: String = sqlx::query_scalar("SHOW server_version").fetch_one(&self.pool).await?;
        Ok(Some(format!("PostgreSQL {version}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn ORDER BY datname",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        // WHAT:  Every visible schema, then its tables and views.
        // WHY:   Listing schemas separately means an *empty* schema still shows
        //        in the sidebar. Deriving them from the table rows alone hid
        //        `public` on a fresh database, so the tree looked broken until
        //        the user created their first table.
        let schema_rows = sqlx::query(
            "SELECT n.nspname AS schema \
             FROM pg_namespace n \
             WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
               AND n.nspname NOT LIKE 'pg_temp%' AND n.nspname NOT LIKE 'pg_toast_temp%' \
               AND has_schema_privilege(n.oid, 'USAGE') \
             ORDER BY n.nspname",
        )
        .fetch_all(&self.pool)
        .await?;

        let rows = sqlx::query(
            "SELECT n.nspname AS schema, c.relname AS name, c.relkind::text AS kind, \
                    c.reltuples::bigint AS estimate \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
               AND n.nspname NOT LIKE 'pg_temp%' AND n.nspname NOT LIKE 'pg_toast_temp%' \
             ORDER BY n.nspname, c.relname",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut schemas: BTreeMap<String, Vec<TableInfo>> = BTreeMap::new();
        for row in schema_rows {
            let schema: String = row.try_get("schema")?;
            schemas.entry(schema).or_default();
        }
        for row in rows {
            let schema: String = row.try_get("schema")?;
            let name: String = row.try_get("name")?;
            let kind: String = row.try_get("kind")?;
            let estimate: i64 = row.try_get("estimate")?;
            schemas.entry(schema.clone()).or_default().push(TableInfo {
                schema: Some(schema),
                name,
                kind: if kind == "v" || kind == "m" { TableKind::View } else { TableKind::Table },
                row_estimate: (estimate >= 0).then_some(estimate),
            });
        }
        Ok(SchemaCatalog {
            schemas: schemas.into_iter().map(|(name, tables)| SchemaInfo { name, tables }).collect(),
        })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let schema = table.schema.clone().unwrap_or_else(|| "public".to_string());
        let rows = sqlx::query(
            "SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS data_type, \
                    NOT a.attnotnull AS nullable, a.attnum::int4 AS ordinal, \
                    EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = a.attrelid \
                            AND i.indisprimary AND a.attnum = ANY (i.indkey)) AS primary_key \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(&schema)
        .bind(&table.name)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let ordinal: i32 = row.try_get("ordinal")?;
            out.push(ColumnInfo {
                name: row.try_get("name")?,
                data_type: row.try_get("data_type")?,
                nullable: row.try_get("nullable")?,
                primary_key: row.try_get("primary_key")?,
                ordinal: u32::try_from(ordinal).unwrap_or_default(),
            });
        }
        Ok(out)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let schema = table.schema.clone().unwrap_or_else(|| "public".to_string());
        let estimate: Option<i64> = sqlx::query_scalar(
            "SELECT c.reltuples::bigint FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
        )
        .bind(&schema)
        .bind(&table.name)
        .fetch_optional(&self.pool)
        .await?;
        match estimate {
            // -1 means "never analyzed" (PG14+); an exact count is cheap enough there.
            Some(n) if n < 0 => {
                let exact: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {}", qualified_name(table)))
                    .fetch_one(&self.pool)
                    .await?;
                Ok(Some(exact))
            }
            other => Ok(other),
        }
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count(*) FROM {}{}", qualified_name(table), where_clause(Engine::Postgres, filters));
        let count: i64 = sqlx::query_scalar(&sql).fetch_one(&self.pool).await?;
        Ok(count)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            qualified_name(table),
            where_clause(Engine::Postgres, &query.filters),
            order_clause(Engine::Postgres, &query.sort),
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
        let rows = sqlx::query(
            "SELECT c.conname AS name, ns.nspname AS from_schema, cl.relname AS from_table, \
                    (SELECT array_agg(a.attname ORDER BY x.ord) FROM unnest(c.conkey) WITH ORDINALITY AS x(attnum, ord) \
                     JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = x.attnum) AS from_columns, \
                    fns.nspname AS to_schema, fcl.relname AS to_table, \
                    (SELECT array_agg(a.attname ORDER BY x.ord) FROM unnest(c.confkey) WITH ORDINALITY AS x(attnum, ord) \
                     JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = x.attnum) AS to_columns \
             FROM pg_constraint c \
             JOIN pg_class cl ON cl.oid = c.conrelid JOIN pg_namespace ns ON ns.oid = cl.relnamespace \
             JOIN pg_class fcl ON fcl.oid = c.confrelid JOIN pg_namespace fns ON fns.oid = fcl.relnamespace \
             WHERE c.contype = 'f' AND ns.nspname NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY ns.nspname, cl.relname, c.conname",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(ForeignKey {
                name: row.try_get("name")?,
                from_schema: Some(row.try_get("from_schema")?),
                from_table: row.try_get("from_table")?,
                from_columns: row.try_get::<Vec<String>, _>("from_columns")?,
                to_schema: Some(row.try_get("to_schema")?),
                to_table: row.try_get("to_table")?,
                to_columns: row.try_get::<Vec<String>, _>("to_columns")?,
            });
        }
        Ok(out)
    }

    // WHAT:  Postgres has no SHOW CREATE TABLE; this reconstructs the essential
    //        CREATE TABLE (columns, NOT NULL, primary key) from the catalog.
    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let columns = self.columns(table).await?;
        if columns.is_empty() {
            return Ok(None);
        }
        let mut lines: Vec<String> = columns
            .iter()
            .map(|c| format!("  {} {}{}", quote_ident(&c.name), c.data_type, if c.nullable { "" } else { " NOT NULL" }))
            .collect();
        let pk: Vec<String> = columns.iter().filter(|c| c.primary_key).map(|c| quote_ident(&c.name)).collect();
        if !pk.is_empty() {
            lines.push(format!("  PRIMARY KEY ({})", pk.join(", ")));
        }
        Ok(Some(format!("CREATE TABLE {} (\n{}\n)", qualified_name(table), lines.join(",\n"))))
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        self.list_objects(kind, parent).await
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        self.describe_object(reference).await
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        self.collect_server_stats().await
    }

    async fn vector_search(&self, req: &VectorSearchRequest) -> AppResult<ResultSet> {
        self.pgvector_search(req).await
    }
}

// ============================================================================
// OBJECT EXPLORER
//
// WHAT:  `objects` / `object_detail` for every kind `profile()` declares: one
//        pg_catalog query per kind returning (name, parent, detail, badge),
//        and per-kind detail queries whose text-cast columns become the
//        property sheet, plus DDL from the pg_get_*def family.
// WHY:   The explorer, the admin page and the object tab are generic; the
//        engine knowledge (which catalog, which badge, which DDL function,
//        which admin statement) has to live here and nowhere else.
// HOW:   Scoped kinds take `parent` as `schema` or `schema.table` (the owner
//        for nested lookups: a table's indexes, triggers, partitions); it is
//        split at the first dot. Kinds that are unique per table (constraint,
//        trigger, rule, policy, partition) always report `schema.table` as
//        their parent so the reference resolves later. Every listing is capped
//        at MAX_OBJECTS and every failure names the kind, so one catalog view
//        a fork lacks (Cockroach, Yugabyte, QuestDB) degrades that kind only.
// WHERE: src-tauri/src/model/objects.rs, src/features/objects, src/features/admin
// ============================================================================

const MAX_OBJECTS: usize = 2000;
const MAX_DETAIL_ROWS: usize = 500;
const SCOPE_FILTER: &str = "($1::text IS NULL OR n.nspname = $1)";

// WHAT:  Which schemas count as the user's. `column` is the schema-name column
//        of the query at hand (`n.nspname`, `s.schemaname`…).
fn user_schemas(column: &str) -> String {
    format!(
        "{column} NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
         AND {column} NOT LIKE 'pg_temp%' AND {column} NOT LIKE 'pg_toast_temp%'"
    )
}

// WHAT:  `parent` decoded: `schema`, or `schema.table` for nested lookups.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Scope {
    schema: Option<String>,
    table: Option<String>,
}

fn scope_of(parent: Option<&str>) -> Scope {
    let Some(parent) = parent.map(str::trim).filter(|p| !p.is_empty()) else {
        return Scope::default();
    };
    match parent.split_once('.') {
        Some((schema, table)) if !schema.is_empty() && !table.is_empty() => {
            Scope { schema: Some(schema.to_string()), table: Some(table.to_string()) }
        }
        _ => Scope { schema: Some(parent.to_string()), table: None },
    }
}

fn qualify(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

fn kind_name(kind: ObjectKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.replace('_', " ")))
        .unwrap_or_else(|| format!("{kind:?}"))
}

fn not_found(kind: ObjectKind, name: &str) -> AppError {
    AppError::not_found(format!("{} \"{name}\" was not found.", capitalise(&kind_name(kind))))
}

fn capitalise(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

fn listing_error(kind: ObjectKind, err: sqlx::Error) -> AppError {
    let mapped = AppError::from(err);
    AppError::driver(format!("Could not list {}: {}", kind_name(kind), mapped.message()))
}

// WHAT:  The catalog query behind one kind. Scoped queries take `$1` (schema)
//        and `$2` (owning table / object name); global ones take nothing.
enum Listing {
    Scoped(String),
    Global(String),
}

const RELATION_SIZE_DETAIL: &str = "pg_catalog.pg_size_pretty(pg_catalog.pg_total_relation_size(c.oid)) || ' · ' || \
    CASE WHEN c.reltuples < 0 THEN 'not analyzed' ELSE '~' || c.reltuples::bigint::text || ' rows' END";

// WHAT:  Shared shape for pg_class-backed kinds (tables, views, foreign tables).
fn relations_sql(relkinds: &str, detail: &str, badge: &str, join: &str) -> String {
    format!(
        "SELECT c.relname::text AS name, n.nspname::text AS parent, ({detail})::text AS detail, ({badge})::text AS badge \
         FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace {join} \
         WHERE c.relkind IN ({relkinds}) AND NOT c.relispartition AND {schemas} \
           AND {SCOPE_FILTER} AND ($2::text IS NULL OR c.relname = $2) \
         ORDER BY n.nspname, c.relname LIMIT {MAX_OBJECTS}",
        schemas = user_schemas("n.nspname")
    )
}

// WHAT:  pg_proc-backed kinds; `prokind` is f / p / a. Functions that belong
//        to an extension are skipped (PostGIS alone ships a thousand).
fn routines_sql(prokind: char) -> String {
    format!(
        "SELECT (pg_catalog.quote_ident(p.proname) || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')')::text AS name, \
                n.nspname::text AS parent, \
                (coalesce('→ ' || pg_catalog.pg_get_function_result(p.oid) || ' · ', '') || \
                 CASE p.provolatile WHEN 'i' THEN 'immutable' WHEN 's' THEN 'stable' ELSE 'volatile' END)::text AS detail, \
                l.lanname::text AS badge \
         FROM pg_catalog.pg_proc p \
         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
         JOIN pg_catalog.pg_language l ON l.oid = p.prolang \
         WHERE p.prokind = '{prokind}' AND {schemas} AND {SCOPE_FILTER} AND ($2::text IS NULL OR p.proname = $2) \
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d \
                           WHERE d.classid = 'pg_catalog.pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e') \
         ORDER BY n.nspname, p.proname, name LIMIT {MAX_OBJECTS}",
        schemas = user_schemas("n.nspname")
    )
}

const TRIGGER_EVENTS: &str = "CASE WHEN t.tgtype & 2 <> 0 THEN 'BEFORE' WHEN t.tgtype & 64 <> 0 THEN 'INSTEAD OF' ELSE 'AFTER' END || ' ' || \
    concat_ws(' OR ', CASE WHEN t.tgtype & 4 <> 0 THEN 'INSERT' END, CASE WHEN t.tgtype & 8 <> 0 THEN 'DELETE' END, \
                      CASE WHEN t.tgtype & 16 <> 0 THEN 'UPDATE' END, CASE WHEN t.tgtype & 32 <> 0 THEN 'TRUNCATE' END) || \
    CASE WHEN t.tgtype & 1 <> 0 THEN ' · row' ELSE ' · statement' END";

const TRIGGER_STATE: &str = "CASE t.tgenabled WHEN 'D' THEN 'disabled' WHEN 'A' THEN 'always' WHEN 'R' THEN 'replica' ELSE 'enabled' END";

const CONSTRAINT_TYPE: &str = "CASE con.contype WHEN 'p' THEN 'primary key' WHEN 'f' THEN 'foreign key' WHEN 'u' THEN 'unique' \
    WHEN 'c' THEN 'check' WHEN 'x' THEN 'exclusion' WHEN 't' THEN 'trigger' ELSE con.contype::text END";

const TYPE_CATEGORY: &str = "CASE t.typtype WHEN 'e' THEN 'enum' WHEN 'd' THEN 'domain' WHEN 'r' THEN 'range' \
    WHEN 'c' THEN 'composite' WHEN 'b' THEN 'base' ELSE t.typtype::text END";

const REPLICA_NAME: &str = "(coalesce(nullif(r.application_name, ''), 'replica') || ' #' || r.pid::text)";
const WAL_RECEIVER_NAME: &str = "(coalesce(nullif(w.sender_host, ''), w.slot_name, 'upstream') || ' #' || w.pid::text)";
const CURRENT_LSN: &str = "CASE WHEN pg_catalog.pg_is_in_recovery() THEN pg_catalog.pg_last_wal_replay_lsn() ELSE pg_catalog.pg_current_wal_lsn() END";
const QUERY_PREVIEW: &str = r"left(regexp_replace(a.query, '\s+', ' ', 'g'), 80)";

fn listing(kind: ObjectKind) -> Option<Listing> {
    use ObjectKind as K;
    let schemas = user_schemas("n.nspname");
    Some(match kind {
        K::Database => Listing::Global(
            "SELECT d.datname::text AS name, NULL::text AS parent, \
                    (CASE WHEN pg_catalog.has_database_privilege(d.oid, 'CONNECT') \
                          THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.oid)) ELSE '?' END \
                     || ' · ' || pg_catalog.pg_get_userbyid(d.datdba) || ' · ' || pg_catalog.pg_encoding_to_char(d.encoding))::text AS detail, \
                    (CASE WHEN d.datname = current_database() THEN 'current' WHEN d.datistemplate THEN 'template' END)::text AS badge \
             FROM pg_catalog.pg_database d WHERE d.datallowconn ORDER BY d.datname"
                .to_string(),
        ),
        K::Schema => Listing::Global(format!(
            "SELECT n.nspname::text AS name, NULL::text AS parent, \
                    (pg_catalog.pg_get_userbyid(n.nspowner) || ' · ' || \
                     (SELECT count(*) FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid AND c.relkind IN ('r', 'p', 'v', 'm', 'f'))::text || ' relations')::text AS detail, \
                    (CASE WHEN NOT pg_catalog.has_schema_privilege(n.oid, 'USAGE') THEN 'no usage' END)::text AS badge \
             FROM pg_catalog.pg_namespace n WHERE {schemas} ORDER BY n.nspname LIMIT {MAX_OBJECTS}"
        )),
        K::Table => Listing::Scoped(relations_sql(
            "'r', 'p'",
            RELATION_SIZE_DETAIL,
            "CASE WHEN c.relkind = 'p' THEN 'partitioned' WHEN c.relpersistence = 'u' THEN 'unlogged' WHEN c.relpersistence = 't' THEN 'temporary' END",
            "",
        )),
        K::View => Listing::Scoped(relations_sql("'v'", "pg_catalog.pg_get_userbyid(c.relowner)", "NULL", "")),
        K::MaterializedView => Listing::Scoped(relations_sql(
            "'m'",
            RELATION_SIZE_DETAIL,
            "CASE WHEN c.relispopulated THEN 'populated' ELSE 'not populated' END",
            "",
        )),
        K::ForeignTable => Listing::Scoped(relations_sql(
            "'f'",
            "pg_catalog.pg_get_userbyid(c.relowner) || coalesce(' · ' || array_to_string(ft.ftoptions, ' '), '')",
            "fs.srvname",
            "LEFT JOIN pg_catalog.pg_foreign_table ft ON ft.ftrelid = c.oid LEFT JOIN pg_catalog.pg_foreign_server fs ON fs.oid = ft.ftserver",
        )),
        K::Partition => Listing::Scoped(format!(
            "SELECT c.relname::text AS name, (pn.nspname || '.' || p.relname)::text AS parent, \
                    (coalesce(pg_catalog.pg_get_expr(c.relpartbound, c.oid), 'default') || ' · ' || \
                     pg_catalog.pg_size_pretty(pg_catalog.pg_total_relation_size(c.oid)))::text AS detail, \
                    lower(split_part(pg_catalog.pg_get_partkeydef(i.inhparent), ' ', 1))::text AS badge \
             FROM pg_catalog.pg_inherits i \
             JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_catalog.pg_class p ON p.oid = i.inhparent \
             JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace \
             WHERE c.relispartition AND {schemas} \
               AND ($1::text IS NULL OR (CASE WHEN $2::text IS NULL THEN n.nspname ELSE pn.nspname END) = $1) \
               AND ($2::text IS NULL OR p.relname = $2) \
             ORDER BY pn.nspname, p.relname, c.relname LIMIT {MAX_OBJECTS}"
        )),
        K::Index => Listing::Scoped(format!(
            "SELECT ic.relname::text AS name, n.nspname::text AS parent, \
                    (tc.relname || ' · ' || pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(i.indexrelid)) || \
                     CASE WHEN i.indisprimary THEN ' · primary' WHEN i.indisunique THEN ' · unique' ELSE '' END || \
                     CASE WHEN NOT i.indisvalid THEN ' · INVALID' ELSE '' END)::text AS detail, \
                    am.amname::text AS badge \
             FROM pg_catalog.pg_index i \
             JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid \
             JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = tc.relnamespace \
             LEFT JOIN pg_catalog.pg_am am ON am.oid = ic.relam \
             WHERE {schemas} AND {SCOPE_FILTER} AND ($2::text IS NULL OR tc.relname = $2) \
             ORDER BY n.nspname, tc.relname, ic.relname LIMIT {MAX_OBJECTS}"
        )),
        K::Constraint => Listing::Scoped(format!(
            "SELECT con.conname::text AS name, (n.nspname || '.' || tc.relname)::text AS parent, \
                    left(pg_catalog.pg_get_constraintdef(con.oid), 160)::text AS detail, \
                    ({CONSTRAINT_TYPE})::text AS badge \
             FROM pg_catalog.pg_constraint con \
             JOIN pg_catalog.pg_class tc ON tc.oid = con.conrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = tc.relnamespace \
             WHERE {schemas} AND {SCOPE_FILTER} AND ($2::text IS NULL OR tc.relname = $2) \
             ORDER BY n.nspname, tc.relname, con.conname LIMIT {MAX_OBJECTS}"
        )),
        K::Sequence => Listing::Scoped(format!(
            "SELECT s.sequencename::text AS name, s.schemaname::text AS parent, \
                    ('last ' || coalesce(s.last_value::text, '—') || ' · start ' || s.start_value::text || ' · by ' || s.increment_by::text || \
                     CASE WHEN s.cycle THEN ' · cycles' ELSE '' END)::text AS detail, \
                    s.data_type::text AS badge \
             FROM pg_catalog.pg_sequences s \
             WHERE {seq_schemas} AND ($1::text IS NULL OR s.schemaname = $1) AND ($2::text IS NULL OR s.sequencename = $2) \
             ORDER BY s.schemaname, s.sequencename LIMIT {MAX_OBJECTS}",
            seq_schemas = user_schemas("s.schemaname")
        )),
        K::Type => Listing::Scoped(format!(
            "SELECT t.typname::text AS name, n.nspname::text AS parent, \
                    left(CASE t.typtype \
                      WHEN 'e' THEN (SELECT string_agg(e.enumlabel, ', ' ORDER BY e.enumsortorder) FROM pg_catalog.pg_enum e WHERE e.enumtypid = t.oid) \
                      WHEN 'd' THEN pg_catalog.format_type(t.typbasetype, t.typtypmod) \
                      WHEN 'r' THEN (SELECT pg_catalog.format_type(rg.rngsubtype, NULL) FROM pg_catalog.pg_range rg WHERE rg.rngtypid = t.oid) \
                      WHEN 'c' THEN (SELECT count(*)::text || ' attributes' FROM pg_catalog.pg_attribute a WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped) \
                      ELSE pg_catalog.obj_description(t.oid, 'pg_type') END, 160)::text AS detail, \
                    ({TYPE_CATEGORY})::text AS badge \
             FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
             LEFT JOIN pg_catalog.pg_class c ON c.oid = t.typrelid \
             WHERE t.typtype IN ('e', 'c', 'd', 'r', 'b') AND t.typcategory <> 'A' AND (t.typrelid = 0 OR c.relkind = 'c') \
               AND {schemas} AND {SCOPE_FILTER} AND ($2::text IS NULL OR t.typname = $2) \
             ORDER BY n.nspname, t.typname LIMIT {MAX_OBJECTS}"
        )),
        K::Function => Listing::Scoped(routines_sql('f')),
        K::Procedure => Listing::Scoped(routines_sql('p')),
        K::Aggregate => Listing::Scoped(routines_sql('a')),
        K::Trigger => Listing::Scoped(format!(
            "SELECT t.tgname::text AS name, (n.nspname || '.' || c.relname)::text AS parent, \
                    ({TRIGGER_EVENTS} || ' · ' || p.proname || '()')::text AS detail, \
                    ({TRIGGER_STATE})::text AS badge \
             FROM pg_catalog.pg_trigger t \
             JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid \
             WHERE NOT t.tgisinternal AND {schemas} AND {SCOPE_FILTER} AND ($2::text IS NULL OR c.relname = $2) \
             ORDER BY n.nspname, c.relname, t.tgname LIMIT {MAX_OBJECTS}"
        )),
        K::Rule => Listing::Scoped(format!(
            "SELECT r.rulename::text AS name, (r.schemaname || '.' || r.tablename)::text AS parent, \
                    left(r.definition, 160)::text AS detail, \
                    lower(trim(split_part(split_part(r.definition, ' TO ', 1), 'ON ', 2)))::text AS badge \
             FROM pg_catalog.pg_rules r \
             WHERE {rule_schemas} AND ($1::text IS NULL OR r.schemaname = $1) AND ($2::text IS NULL OR r.tablename = $2) \
             ORDER BY r.schemaname, r.tablename, r.rulename LIMIT {MAX_OBJECTS}",
            rule_schemas = user_schemas("r.schemaname")
        )),
        K::Policy => Listing::Scoped(format!(
            "SELECT p.policyname::text AS name, (p.schemaname || '.' || p.tablename)::text AS parent, \
                    (array_to_string(p.roles, ', ') || coalesce(' · USING ' || left(p.qual, 100), '') || \
                     coalesce(' · CHECK ' || left(p.with_check, 100), ''))::text AS detail, \
                    (lower(p.cmd) || CASE WHEN p.permissive = 'RESTRICTIVE' THEN ' · restrictive' ELSE '' END)::text AS badge \
             FROM pg_catalog.pg_policies p \
             WHERE {policy_schemas} AND ($1::text IS NULL OR p.schemaname = $1) AND ($2::text IS NULL OR p.tablename = $2) \
             ORDER BY p.schemaname, p.tablename, p.policyname LIMIT {MAX_OBJECTS}",
            policy_schemas = user_schemas("p.schemaname")
        )),
        K::Extension => Listing::Scoped(
            "SELECT e.extname::text AS name, n.nspname::text AS parent, \
                    (coalesce(a.comment, '') || CASE WHEN a.default_version IS NOT NULL AND a.default_version <> e.extversion \
                                                    THEN ' · default ' || a.default_version ELSE '' END)::text AS detail, \
                    e.extversion::text AS badge \
             FROM pg_catalog.pg_extension e \
             JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace \
             LEFT JOIN pg_catalog.pg_available_extensions a ON a.name = e.extname \
             WHERE ($1::text IS NULL OR n.nspname = $1) AND ($2::text IS NULL OR e.extname = $2) \
             ORDER BY e.extname"
                .to_string(),
        ),
        K::Publication => Listing::Global(
            "SELECT p.pubname::text AS name, NULL::text AS parent, \
                    ((CASE WHEN p.puballtables THEN 'all tables' \
                           ELSE (SELECT count(*) FROM pg_catalog.pg_publication_rel pr WHERE pr.prpubid = p.oid)::text || ' tables' END) \
                     || ' · ' || concat_ws(', ', CASE WHEN p.pubinsert THEN 'insert' END, CASE WHEN p.pubupdate THEN 'update' END, \
                                                 CASE WHEN p.pubdelete THEN 'delete' END, CASE WHEN p.pubtruncate THEN 'truncate' END) \
                     || ' · ' || pg_catalog.pg_get_userbyid(p.pubowner))::text AS detail, \
                    (CASE WHEN p.puballtables THEN 'all tables' ELSE 'tables' END)::text AS badge \
             FROM pg_catalog.pg_publication p ORDER BY p.pubname"
                .to_string(),
        ),
        K::Subscription => Listing::Global(
            "SELECT s.subname::text AS name, NULL::text AS parent, \
                    (array_to_string(s.subpublications, ', ') || ' · ' || pg_catalog.pg_get_userbyid(s.subowner))::text AS detail, \
                    (CASE WHEN s.subenabled THEN 'enabled' ELSE 'disabled' END)::text AS badge \
             FROM pg_catalog.pg_subscription s \
             WHERE s.subdbid = (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname = current_database()) \
             ORDER BY s.subname"
                .to_string(),
        ),
        K::ReplicationSlot => Listing::Global(format!(
            "SELECT s.slot_name::text AS name, s.database::text AS parent, \
                    (s.slot_type || coalesce(' · ' || s.plugin, '') || \
                     coalesce(' · retains ' || pg_catalog.pg_size_pretty(pg_catalog.pg_wal_lsn_diff({CURRENT_LSN}, s.restart_lsn)), ''))::text AS detail, \
                    (CASE WHEN s.active THEN 'active' ELSE 'inactive' END)::text AS badge \
             FROM pg_catalog.pg_replication_slots s ORDER BY s.slot_name"
        )),
        K::ForeignServer => Listing::Global(
            "SELECT s.srvname::text AS name, NULL::text AS parent, \
                    (pg_catalog.pg_get_userbyid(s.srvowner) || coalesce(' · ' || array_to_string(s.srvoptions, ' '), ''))::text AS detail, \
                    w.fdwname::text AS badge \
             FROM pg_catalog.pg_foreign_server s \
             JOIN pg_catalog.pg_foreign_data_wrapper w ON w.oid = s.srvfdw \
             ORDER BY s.srvname"
                .to_string(),
        ),
        K::ForeignDataWrapper => Listing::Global(
            "SELECT w.fdwname::text AS name, NULL::text AS parent, \
                    (pg_catalog.pg_get_userbyid(w.fdwowner) || ' · ' || \
                     (SELECT count(*) FROM pg_catalog.pg_foreign_server s WHERE s.srvfdw = w.oid)::text || ' servers')::text AS detail, \
                    (CASE WHEN w.fdwhandler <> 0 THEN w.fdwhandler::regproc::text ELSE 'no handler' END)::text AS badge \
             FROM pg_catalog.pg_foreign_data_wrapper w ORDER BY w.fdwname"
                .to_string(),
        ),
        K::Role => Listing::Global(
            "SELECT r.rolname::text AS name, NULL::text AS parent, \
                    (coalesce('member of ' || (SELECT string_agg(g.rolname, ', ' ORDER BY g.rolname) \
                                               FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles g ON g.oid = m.roleid \
                                               WHERE m.member = r.oid), 'no memberships') || \
                     CASE WHEN r.rolvaliduntil IS NOT NULL THEN ' · until ' || r.rolvaliduntil::date::text ELSE '' END)::text AS detail, \
                    (CASE WHEN r.rolsuper THEN 'superuser' WHEN r.rolcanlogin THEN 'login' ELSE 'group' END)::text AS badge \
             FROM pg_catalog.pg_roles r WHERE left(r.rolname, 3) <> 'pg_' ORDER BY r.rolname"
                .to_string(),
        ),
        K::Grant => Listing::Scoped(format!(
            "SELECT g.grantee::text AS name, (g.table_schema || '.' || g.table_name)::text AS parent, \
                    (g.table_name::text || ': ' || string_agg(g.privilege_type::text, ', ' ORDER BY g.privilege_type))::text AS detail, \
                    (CASE WHEN bool_or(g.is_grantable = 'YES') THEN 'grantable' END)::text AS badge \
             FROM information_schema.role_table_grants g \
             WHERE g.table_schema NOT IN ('pg_catalog', 'information_schema') \
               AND ($1::text IS NULL OR g.table_schema = $1) AND ($2::text IS NULL OR g.table_name = $2) \
             GROUP BY g.grantee, g.table_schema, g.table_name \
             ORDER BY g.table_schema, g.table_name, g.grantee LIMIT {MAX_OBJECTS}"
        )),
        K::Tablespace => Listing::Global(
            "SELECT t.spcname::text AS name, NULL::text AS parent, \
                    (pg_catalog.pg_get_userbyid(t.spcowner) || coalesce(' · ' || nullif(pg_catalog.pg_tablespace_location(t.oid), ''), '') || \
                     CASE WHEN pg_catalog.has_tablespace_privilege(t.oid, 'CREATE') \
                          THEN ' · ' || pg_catalog.pg_size_pretty(pg_catalog.pg_tablespace_size(t.oid)) ELSE '' END)::text AS detail, \
                    NULL::text AS badge \
             FROM pg_catalog.pg_tablespace t ORDER BY t.spcname"
                .to_string(),
        ),
        K::Session => Listing::Global(format!(
            "SELECT a.pid::text AS name, a.datname::text AS parent, \
                    (coalesce(a.usename::text, '?') || coalesce('@' || a.datname, '') || coalesce(' · ' || nullif(a.application_name, ''), '') || \
                     coalesce(' · ' || a.client_addr::text, '') || coalesce(' · ' || {QUERY_PREVIEW}, ''))::text AS detail, \
                    coalesce(a.state, a.backend_type)::text AS badge \
             FROM pg_catalog.pg_stat_activity a \
             WHERE a.pid IS NOT NULL \
             ORDER BY (a.backend_type = 'client backend') DESC, a.pid LIMIT {MAX_OBJECTS}"
        )),
        K::Lock => Listing::Global(format!(
            "SELECT (l.pid::text || ':' || coalesce(l.relation::regclass::text, l.locktype) || ':' || l.mode)::text AS name, \
                    l.pid::text AS parent, \
                    (coalesce(a.usename::text, '?') || coalesce(' · ' || a.state, '') || coalesce(' · ' || {QUERY_PREVIEW}, ''))::text AS detail, \
                    (CASE WHEN l.granted THEN 'granted' ELSE 'waiting' END)::text AS badge \
             FROM pg_catalog.pg_locks l \
             LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid \
             WHERE l.pid <> pg_catalog.pg_backend_pid() AND l.locktype <> 'virtualxid' \
             ORDER BY l.granted, l.pid, name LIMIT {MAX_OBJECTS}"
        )),
        K::Setting => Listing::Global(
            "SELECT s.name::text AS name, s.category::text AS parent, \
                    (s.setting || coalesce(' ' || s.unit, '') || ' · ' || s.short_desc)::text AS detail, \
                    s.context::text AS badge \
             FROM pg_catalog.pg_settings s ORDER BY s.name"
                .to_string(),
        ),
        // Two-step kinds (extension probe, primary/standby union) are built in code.
        K::SlowQuery | K::Replica => return None,
        _ => return None,
    })
}

// WHAT:  Replication peers: walsenders on a primary, the walreceiver on a standby.
const REPLICAS_SQL: &str = "SELECT {REPLICA_NAME}::text AS name, NULL::text AS parent, \
        (r.state || coalesce(' · ' || r.client_addr::text, '') || \
         coalesce(' · replay lag ' || round(extract(epoch from r.replay_lag)::numeric, 3)::text || ' s', '') || \
         coalesce(' · ' || pg_catalog.pg_size_pretty(pg_catalog.pg_wal_lsn_diff({CURRENT_LSN}, r.replay_lsn)) || ' behind', ''))::text AS detail, \
        r.sync_state::text AS badge \
     FROM pg_catalog.pg_stat_replication r ORDER BY name";

const WAL_RECEIVER_SQL: &str = "SELECT {WAL_RECEIVER_NAME}::text AS name, NULL::text AS parent, \
        (w.status || coalesce(' · received ' || w.latest_end_lsn::text, '') || \
         coalesce(' · replay delay ' || round(extract(epoch from (now() - pg_catalog.pg_last_xact_replay_timestamp()))::numeric, 1)::text || ' s', ''))::text AS detail, \
        'upstream'::text AS badge \
     FROM pg_catalog.pg_stat_wal_receiver w";

fn replicas_sql() -> String {
    REPLICAS_SQL.replace("{REPLICA_NAME}", REPLICA_NAME).replace("{CURRENT_LSN}", CURRENT_LSN)
}

fn wal_receiver_sql() -> String {
    WAL_RECEIVER_SQL.replace("{WAL_RECEIVER_NAME}", WAL_RECEIVER_NAME)
}

// WHAT:  pg_stat_statements renamed its timing columns in 1.8 (PostgreSQL 13);
//        `modern` picks the spelling, and the extension may live in any schema.
fn slow_query_list_sql(schema: &str, modern: bool) -> String {
    let (mean, total) = if modern { ("mean_exec_time", "total_exec_time") } else { ("mean_time", "total_time") };
    format!(
        "SELECT coalesce(s.queryid::text, md5(s.query)) AS name, NULL::text AS parent, \
                (round(s.{mean}::numeric, 2)::text || ' ms avg · ' || s.calls::text || ' calls · ' || \
                 round(s.{total}::numeric)::text || ' ms total · ' || left(regexp_replace(s.query, '\\s+', ' ', 'g'), 100))::text AS detail, \
                pg_catalog.pg_get_userbyid(s.userid)::text AS badge \
         FROM {schema}.pg_stat_statements s \
         WHERE s.dbid = (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname = current_database()) \
         ORDER BY s.{mean} DESC LIMIT 200",
        schema = quote_ident(schema)
    )
}

fn slow_query_detail_sql(schema: &str, modern: bool) -> String {
    let (mean, total, min, max, stddev) = if modern {
        ("mean_exec_time", "total_exec_time", "min_exec_time", "max_exec_time", "stddev_exec_time")
    } else {
        ("mean_time", "total_time", "min_time", "max_time", "stddev_time")
    };
    format!(
        "SELECT s.query AS query, pg_catalog.pg_get_userbyid(s.userid)::text AS username, s.calls::text AS calls, \
                round(s.{total}::numeric, 2)::text AS total_ms, round(s.{mean}::numeric, 3)::text AS mean_ms, \
                round(s.{min}::numeric, 3)::text AS min_ms, round(s.{max}::numeric, 3)::text AS max_ms, \
                round(s.{stddev}::numeric, 3)::text AS stddev_ms, s.rows::text AS rows_returned, \
                s.shared_blks_hit::text AS shared_blocks_hit, s.shared_blks_read::text AS shared_blocks_read, \
                s.temp_blks_written::text AS temp_blocks_written \
         FROM {schema}.pg_stat_statements s \
         WHERE coalesce(s.queryid::text, md5(s.query)) = $1 \
           AND s.dbid = (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname = current_database()) \
         ORDER BY s.calls DESC LIMIT 1",
        schema = quote_ident(schema)
    )
}

fn summary_from_row(kind: ObjectKind, row: &PgRow, nested_parent: Option<&str>) -> AppResult<ObjectSummary> {
    let name: String = row.try_get("name")?;
    let parent: Option<String> = row.try_get("parent")?;
    let detail: Option<String> = row.try_get("detail")?;
    let badge: Option<String> = row.try_get("badge")?;
    let mut summary = ObjectSummary::new(kind, name, nested_parent.map(str::to_string).or(parent));
    summary.detail = detail.filter(|d| !d.trim().is_empty());
    summary.badge = badge.filter(|b| !b.trim().is_empty());
    Ok(summary)
}

// WHAT:  One catalog row with every column read as text: the property sheet
//        of a detail view is exactly the SELECT list of its query.
struct TextRow {
    cells: Vec<(String, Option<String>)>,
}

impl TextRow {
    fn from_row(row: &PgRow) -> AppResult<TextRow> {
        let mut cells = Vec::with_capacity(row.columns().len());
        for (index, column) in row.columns().iter().enumerate() {
            cells.push((column.name().to_string(), cell_text(row, index)?));
        }
        Ok(TextRow { cells })
    }

    fn get(&self, name: &str) -> Option<&str> {
        self.cells.iter().find(|(n, _)| n == name).and_then(|(_, v)| v.as_deref())
    }

    fn is_true(&self, name: &str) -> bool {
        self.get(name).is_some_and(|v| v == "true" || v == "t")
    }

    fn properties(&self, skip: &[&str]) -> Vec<ObjectProperty> {
        self.cells
            .iter()
            .filter(|(name, value)| !skip.contains(&name.as_str()) && value.as_deref().is_some_and(|v| !v.is_empty()))
            .map(|(name, value)| ObjectProperty { name: name.replace('_', " "), value: value.clone().unwrap_or_default() })
            .collect()
    }
}

// WHAT:  Reads a cell as text whatever its declared type (the queries cast to
//        text, this is the safety net for the odd untyped column).
fn cell_text(row: &PgRow, index: usize) -> AppResult<Option<String>> {
    if let Ok(v) = row.try_get::<Option<String>, _>(index) {
        return Ok(v);
    }
    if let Ok(v) = row.try_get::<Option<i64>, _>(index) {
        return Ok(v.map(|n| n.to_string()));
    }
    if let Ok(v) = row.try_get::<Option<i32>, _>(index) {
        return Ok(v.map(|n| n.to_string()));
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(index) {
        return Ok(v.map(|n| n.to_string()));
    }
    if let Ok(v) = row.try_get::<Option<bool>, _>(index) {
        return Ok(v.map(|b| b.to_string()));
    }
    let raw = row.try_get_raw(index)?;
    if raw.is_null() {
        return Ok(None);
    }
    Ok(Some(raw.as_str().map(str::to_string).unwrap_or_else(|_| format!("<{}>", raw.type_info().name().to_ascii_lowercase()))))
}

fn empty_result() -> ResultSet {
    ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }
}

// ---- pure DDL builders (unit-tested offline) --------------------------------

// WHAT:  The table script: CREATE TABLE (or PARTITION OF), then every
//        non-primary constraint and every standalone index as its own statement.
fn table_script_text(head: &str, qualified: &str, constraints: &[(String, String)], indexes: &[String]) -> String {
    let mut script = format!("{head};");
    for (name, definition) in constraints {
        script.push_str(&format!("\n\nALTER TABLE {qualified} ADD CONSTRAINT {} {definition};", quote_ident(name)));
    }
    for definition in indexes {
        script.push_str(&format!("\n\n{definition};"));
    }
    script
}

fn type_ddl(qualified: &str, row: &TextRow) -> Option<String> {
    match row.get("typtype")? {
        "e" => Some(format!("CREATE TYPE {qualified} AS ENUM ({})", row.get("enum_labels").unwrap_or_default())),
        "c" => Some(format!("CREATE TYPE {qualified} AS ({})", row.get("attributes").unwrap_or_default())),
        "r" => Some(format!("CREATE TYPE {qualified} AS RANGE (SUBTYPE = {})", row.get("subtype").unwrap_or("?"))),
        "d" => {
            let mut ddl = format!("CREATE DOMAIN {qualified} AS {}", row.get("base_type").unwrap_or("?"));
            if let Some(default) = row.get("default_value") {
                ddl.push_str(&format!(" DEFAULT {default}"));
            }
            if row.is_true("not_null") {
                ddl.push_str(" NOT NULL");
            }
            if let Some(constraints) = row.get("constraints") {
                ddl.push_str(&format!(" {constraints}"));
            }
            Some(ddl)
        }
        _ => None,
    }
}

fn sequence_ddl(qualified: &str, row: &TextRow) -> String {
    let mut ddl = format!("CREATE SEQUENCE {qualified}");
    if let Some(data_type) = row.get("data_type") {
        ddl.push_str(&format!("\n  AS {data_type}"));
    }
    for (clause, column) in [
        ("INCREMENT BY", "increment_by"),
        ("MINVALUE", "min_value"),
        ("MAXVALUE", "max_value"),
        ("START WITH", "start_value"),
        ("CACHE", "cache_size"),
    ] {
        if let Some(value) = row.get(column) {
            ddl.push_str(&format!("\n  {clause} {value}"));
        }
    }
    ddl.push_str(if row.is_true("cycle") { "\n  CYCLE" } else { "\n  NO CYCLE" });
    if let Some(owner) = row.get("owned_by") {
        ddl.push_str(&format!("\n  OWNED BY {owner}"));
    }
    ddl
}

fn policy_ddl(name: &str, on_table: &str, row: &TextRow) -> String {
    let mut ddl = format!("CREATE POLICY {} ON {on_table}", quote_ident(name));
    if row.get("permissive").is_some_and(|p| p.eq_ignore_ascii_case("restrictive")) {
        ddl.push_str("\n  AS RESTRICTIVE");
    }
    if let Some(command) = row.get("command") {
        ddl.push_str(&format!("\n  FOR {command}"));
    }
    if let Some(roles) = row.get("roles") {
        ddl.push_str(&format!("\n  TO {roles}"));
    }
    if let Some(using) = row.get("using_expression") {
        ddl.push_str(&format!("\n  USING ({using})"));
    }
    if let Some(check) = row.get("check_expression") {
        ddl.push_str(&format!("\n  WITH CHECK ({check})"));
    }
    ddl
}

fn publication_ddl(name: &str, row: &TextRow) -> String {
    let target = if row.is_true("all_tables") {
        "FOR ALL TABLES".to_string()
    } else {
        match row.get("tables") {
            Some(tables) => format!("FOR TABLE {tables}"),
            None => String::new(),
        }
    };
    let ops: Vec<&str> = [("publish_insert", "insert"), ("publish_update", "update"), ("publish_delete", "delete"), ("publish_truncate", "truncate")]
        .into_iter()
        .filter(|(column, _)| row.is_true(column))
        .map(|(_, op)| op)
        .collect();
    let mut ddl = format!("CREATE PUBLICATION {}", quote_ident(name));
    if !target.is_empty() {
        ddl.push_str(&format!("\n  {target}"));
    }
    ddl.push_str(&format!("\n  WITH (publish = '{}')", ops.join(", ")));
    ddl
}

fn role_ddl(name: &str, row: &TextRow) -> String {
    let flags = [
        ("superuser", "SUPERUSER", "NOSUPERUSER"),
        ("can_login", "LOGIN", "NOLOGIN"),
        ("inherit", "INHERIT", "NOINHERIT"),
        ("create_role", "CREATEROLE", "NOCREATEROLE"),
        ("create_db", "CREATEDB", "NOCREATEDB"),
        ("replication", "REPLICATION", "NOREPLICATION"),
        ("bypass_rls", "BYPASSRLS", "NOBYPASSRLS"),
    ];
    let mut parts: Vec<String> = flags.iter().map(|(column, on, off)| if row.is_true(column) { (*on).to_string() } else { (*off).to_string() }).collect();
    if let Some(limit) = row.get("connection_limit").filter(|l| *l != "unlimited") {
        parts.push(format!("CONNECTION LIMIT {limit}"));
    }
    if let Some(until) = row.get("valid_until") {
        parts.push(format!("VALID UNTIL {}", quote_literal(until)));
    }
    let mut ddl = format!("CREATE ROLE {} WITH\n  {}", quote_ident(name), parts.join("\n  "));
    if let Some(groups) = row.get("member_of") {
        for group in groups.split(", ").filter(|g| !g.is_empty()) {
            ddl.push_str(&format!(";\nGRANT {} TO {}", quote_ident(group), quote_ident(name)));
        }
    }
    ddl
}

// WHAT:  `key=value` server / wrapper options → `OPTIONS (key 'value', …)`.
fn options_clause(options: Option<&str>) -> String {
    let pairs: Vec<String> = options
        .unwrap_or_default()
        .split(", ")
        .filter(|o| !o.is_empty())
        .map(|option| match option.split_once('=') {
            Some((key, value)) => format!("{} {}", quote_ident(key), quote_literal(value)),
            None => quote_ident(option),
        })
        .collect();
    if pairs.is_empty() {
        String::new()
    } else {
        format!(" OPTIONS ({})", pairs.join(", "))
    }
}

fn foreign_server_ddl(name: &str, row: &TextRow) -> String {
    let mut ddl = format!("CREATE SERVER {}", quote_ident(name));
    if let Some(kind) = row.get("type") {
        ddl.push_str(&format!(" TYPE {}", quote_literal(kind)));
    }
    if let Some(version) = row.get("version") {
        ddl.push_str(&format!(" VERSION {}", quote_literal(version)));
    }
    ddl.push_str(&format!("\n  FOREIGN DATA WRAPPER {}", quote_ident(row.get("wrapper").unwrap_or("?"))));
    ddl.push_str(&options_clause(row.get("options")));
    ddl
}

fn foreign_table_ddl(qualified: &str, columns: &[ColumnInfo], server: Option<&str>, options: Option<&str>) -> String {
    let lines: Vec<String> = columns
        .iter()
        .map(|c| format!("  {} {}{}", quote_ident(&c.name), c.data_type, if c.nullable { "" } else { " NOT NULL" }))
        .collect();
    format!(
        "CREATE FOREIGN TABLE {qualified} (\n{}\n) SERVER {}{}",
        lines.join(",\n"),
        quote_ident(server.unwrap_or("?")),
        options_clause(options)
    )
}

fn routine_drop_statement(prokind: Option<&str>, schema: &str, signature: &str) -> String {
    let keyword = match prokind {
        Some("p") => "PROCEDURE",
        Some("a") => "AGGREGATE",
        _ => "FUNCTION",
    };
    format!("DROP {keyword} {}.{signature}", quote_ident(schema))
}

fn pid_of(name: &str) -> AppResult<i64> {
    name.split(':')
        .next()
        .and_then(|p| p.trim().parse::<i64>().ok())
        .ok_or_else(|| AppError::invalid_input(format!("\"{name}\" is not a backend pid.")))
}

fn session_actions(pid: i64) -> Vec<ObjectAction> {
    vec![
        ObjectAction::new("cancel", "Cancel running query", format!("SELECT pg_cancel_backend({pid})")),
        ObjectAction::destructive("terminate", "Terminate backend", format!("SELECT pg_terminate_backend({pid})")),
    ]
}

// WHAT:  Settings that ALTER SYSTEM can change; `internal` ones cannot be set
//        at all and `postmaster` ones only take effect after a restart.
fn setting_actions(name: &str, value: &str, context: Option<&str>) -> Vec<ObjectAction> {
    match context {
        None | Some("internal") => Vec::new(),
        Some(context) => {
            let restart = if context == "postmaster" { " (restart required)" } else { "" };
            vec![
                ObjectAction::destructive(
                    "set",
                    &format!("ALTER SYSTEM SET{restart}"),
                    format!("ALTER SYSTEM SET {} = {}", quote_ident(name), quote_literal(value)),
                ),
                ObjectAction::destructive("reset", "ALTER SYSTEM RESET", format!("ALTER SYSTEM RESET {}", quote_ident(name))),
                ObjectAction::new("reload", "Reload configuration", "SELECT pg_reload_conf()"),
            ]
        }
    }
}

// ---- detail queries ----------------------------------------------------------

const DATABASE_DETAIL_SQL: &str = "SELECT pg_catalog.pg_get_userbyid(d.datdba)::text AS owner, \
        pg_catalog.pg_encoding_to_char(d.encoding)::text AS encoding, d.datcollate::text AS collation, d.datctype::text AS ctype, \
        CASE WHEN pg_catalog.has_database_privilege(d.oid, 'CONNECT') THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.oid)) END AS size, \
        t.spcname::text AS tablespace, d.datistemplate::text AS template, d.datallowconn::text AS allows_connections, \
        CASE WHEN d.datconnlimit < 0 THEN 'unlimited' ELSE d.datconnlimit::text END AS connection_limit, \
        (SELECT count(*) FROM pg_catalog.pg_stat_activity a WHERE a.datname = d.datname)::text AS sessions, \
        pg_catalog.shobj_description(d.oid, 'pg_database') AS comment \
     FROM pg_catalog.pg_database d JOIN pg_catalog.pg_tablespace t ON t.oid = d.dattablespace \
     WHERE d.datname = $1";

const SCHEMA_DETAIL_SQL: &str = "SELECT pg_catalog.pg_get_userbyid(n.nspowner)::text AS owner, \
        (SELECT count(*) FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid AND c.relkind IN ('r', 'p'))::text AS tables, \
        (SELECT count(*) FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid AND c.relkind IN ('v', 'm'))::text AS views, \
        (SELECT count(*) FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid AND c.relkind = 'S')::text AS sequences, \
        (SELECT count(*) FROM pg_catalog.pg_proc p WHERE p.pronamespace = n.oid)::text AS routines, \
        (SELECT pg_catalog.pg_size_pretty(coalesce(sum(pg_catalog.pg_total_relation_size(c.oid)), 0)) \
           FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid AND c.relkind IN ('r', 'p', 'm')) AS size, \
        pg_catalog.obj_description(n.oid, 'pg_namespace') AS comment \
     FROM pg_catalog.pg_namespace n WHERE n.nspname = $1";

const RELATION_DETAIL_SQL: &str = "SELECT c.oid::text AS oid, c.relkind::text AS relkind, \
        CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' WHEN 'v' THEN 'view' \
                       WHEN 'm' THEN 'materialized view' WHEN 'f' THEN 'foreign table' ELSE c.relkind::text END AS kind, \
        pg_catalog.pg_get_userbyid(c.relowner)::text AS owner, \
        CASE WHEN c.relkind IN ('r', 'p', 'm', 'f') THEN pg_catalog.pg_size_pretty(pg_catalog.pg_total_relation_size(c.oid)) END AS total_size, \
        CASE WHEN c.relkind IN ('r', 'p', 'm') THEN pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid)) END AS heap_size, \
        CASE WHEN c.relkind IN ('r', 'p', 'm') THEN pg_catalog.pg_size_pretty(pg_catalog.pg_indexes_size(c.oid)) END AS indexes_size, \
        CASE WHEN c.relkind IN ('r', 'p', 'm', 'f') THEN (CASE WHEN c.reltuples < 0 THEN 'not analyzed' ELSE c.reltuples::bigint::text END) END AS row_estimate, \
        coalesce(ts.spcname, 'pg_default')::text AS tablespace, \
        CASE c.relpersistence WHEN 'u' THEN 'unlogged' WHEN 't' THEN 'temporary' ELSE 'permanent' END AS persistence, \
        CASE WHEN c.relkind IN ('r', 'p') THEN c.relrowsecurity::text END AS row_level_security, \
        CASE WHEN c.relkind = 'm' THEN c.relispopulated::text END AS populated, \
        CASE WHEN c.relkind IN ('v', 'm') THEN pg_catalog.pg_get_viewdef(c.oid, true) END AS view_definition, \
        fs.srvname::text AS foreign_server, array_to_string(ft.ftoptions, ', ') AS foreign_options, \
        pg_catalog.obj_description(c.oid, 'pg_class') AS comment \
     FROM pg_catalog.pg_class c \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = c.reltablespace \
     LEFT JOIN pg_catalog.pg_foreign_table ft ON ft.ftrelid = c.oid \
     LEFT JOIN pg_catalog.pg_foreign_server fs ON fs.oid = ft.ftserver \
     WHERE n.nspname = $1 AND c.relname = $2";

// WHAT:  Partitioning and pg_stat figures; optional, so a fork without them
//        still shows the core sheet.
const RELATION_STATS_SQL: &str = "SELECT pg_catalog.pg_get_partkeydef(c.oid) AS partition_key, \
        pg_catalog.pg_get_expr(c.relpartbound, c.oid) AS partition_bound, \
        pn.nspname::text AS parent_schema, p.relname::text AS parent_name, \
        CASE WHEN p.oid IS NOT NULL THEN pn.nspname || '.' || p.relname END AS parent_table, \
        st.n_live_tup::text AS live_rows, st.n_dead_tup::text AS dead_rows, \
        st.seq_scan::text AS sequential_scans, st.idx_scan::text AS index_scans, \
        st.last_vacuum::text AS last_vacuum, st.last_autovacuum::text AS last_autovacuum, \
        st.last_analyze::text AS last_analyze, st.last_autoanalyze::text AS last_autoanalyze \
     FROM pg_catalog.pg_class c \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     LEFT JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid \
     LEFT JOIN pg_catalog.pg_class p ON p.oid = i.inhparent \
     LEFT JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace \
     LEFT JOIN pg_catalog.pg_stat_all_tables st ON st.relid = c.oid \
     WHERE n.nspname = $1 AND c.relname = $2 LIMIT 1";

const PARTITION_SCHEMA_SQL: &str = "SELECT n.nspname::text AS schema \
     FROM pg_catalog.pg_inherits i \
     JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     JOIN pg_catalog.pg_class p ON p.oid = i.inhparent \
     JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace \
     WHERE pn.nspname = $1 AND p.relname = $2 AND c.relname = $3";

const TABLE_CONSTRAINTS_SQL: &str = "SELECT con.conname::text AS name, pg_catalog.pg_get_constraintdef(con.oid, true) AS definition \
     FROM pg_catalog.pg_constraint con \
     JOIN pg_catalog.pg_class c ON c.oid = con.conrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     WHERE n.nspname = $1 AND c.relname = $2 AND con.contype <> 'p' \
     ORDER BY con.contype, con.conname";

const TABLE_INDEXES_SQL: &str = "SELECT pg_catalog.pg_get_indexdef(i.indexrelid) AS definition \
     FROM pg_catalog.pg_index i \
     JOIN pg_catalog.pg_class c ON c.oid = i.indrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     WHERE n.nspname = $1 AND c.relname = $2 AND NOT i.indisprimary \
       AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint x WHERE x.conindid = i.indexrelid AND x.contype IN ('u', 'x')) \
     ORDER BY 1";

const INDEX_DETAIL_SQL: &str = "SELECT i.indexrelid::text AS oid, pg_catalog.pg_get_indexdef(i.indexrelid) AS definition, \
        (tn.nspname || '.' || tc.relname)::text AS on_table, am.amname::text AS access_method, \
        i.indisunique::text AS is_unique, i.indisprimary::text AS is_primary, i.indisvalid::text AS is_valid, i.indisclustered::text AS is_clustered, \
        pg_catalog.pg_get_expr(i.indpred, i.indrelid, true) AS predicate, \
        pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(i.indexrelid)) AS size, \
        coalesce(ts.spcname, 'pg_default')::text AS tablespace, \
        st.idx_scan::text AS scans, st.idx_tup_read::text AS tuples_read, st.idx_tup_fetch::text AS tuples_fetched, \
        pg_catalog.obj_description(i.indexrelid, 'pg_class') AS comment \
     FROM pg_catalog.pg_index i \
     JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = ic.relnamespace \
     JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid \
     JOIN pg_catalog.pg_namespace tn ON tn.oid = tc.relnamespace \
     LEFT JOIN pg_catalog.pg_am am ON am.oid = ic.relam \
     LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = ic.reltablespace \
     LEFT JOIN pg_catalog.pg_stat_all_indexes st ON st.indexrelid = i.indexrelid \
     WHERE n.nspname = $1 AND ic.relname = $2";

const CONSTRAINT_DETAIL_SQL: &str = "SELECT pg_catalog.pg_get_constraintdef(con.oid, true) AS definition, \
        (CASE con.contype WHEN 'p' THEN 'primary key' WHEN 'f' THEN 'foreign key' WHEN 'u' THEN 'unique' \
              WHEN 'c' THEN 'check' WHEN 'x' THEN 'exclusion' WHEN 't' THEN 'trigger' ELSE con.contype::text END)::text AS type, \
        (n.nspname || '.' || tc.relname)::text AS on_table, \
        CASE WHEN con.confrelid <> 0 THEN fn.nspname || '.' || fc.relname END AS references_table, \
        CASE con.confupdtype WHEN 'a' THEN 'no action' WHEN 'r' THEN 'restrict' WHEN 'c' THEN 'cascade' WHEN 'n' THEN 'set null' WHEN 'd' THEN 'set default' END AS on_update, \
        CASE con.confdeltype WHEN 'a' THEN 'no action' WHEN 'r' THEN 'restrict' WHEN 'c' THEN 'cascade' WHEN 'n' THEN 'set null' WHEN 'd' THEN 'set default' END AS on_delete, \
        con.condeferrable::text AS deferrable, con.condeferred::text AS initially_deferred, con.convalidated::text AS validated, \
        CASE WHEN con.conindid <> 0 THEN ic.relname::text END AS backing_index, \
        pg_catalog.obj_description(con.oid, 'pg_constraint') AS comment \
     FROM pg_catalog.pg_constraint con \
     JOIN pg_catalog.pg_class tc ON tc.oid = con.conrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = tc.relnamespace \
     LEFT JOIN pg_catalog.pg_class fc ON fc.oid = con.confrelid \
     LEFT JOIN pg_catalog.pg_namespace fn ON fn.oid = fc.relnamespace \
     LEFT JOIN pg_catalog.pg_class ic ON ic.oid = con.conindid \
     WHERE n.nspname = $1 AND tc.relname = $2 AND con.conname = $3";

const SEQUENCE_DETAIL_SQL: &str = "SELECT s.sequenceowner::text AS owner, s.data_type::text AS data_type, \
        s.start_value::text AS start_value, s.min_value::text AS min_value, s.max_value::text AS max_value, \
        s.increment_by::text AS increment_by, s.cycle::text AS cycle, s.cache_size::text AS cache_size, s.last_value::text AS last_value, \
        (SELECT pg_catalog.quote_ident(rn.nspname) || '.' || pg_catalog.quote_ident(rc.relname) || '.' || pg_catalog.quote_ident(a.attname) \
           FROM pg_catalog.pg_depend d \
           JOIN pg_catalog.pg_class rc ON rc.oid = d.refobjid \
           JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace \
           JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid \
          WHERE d.classid = 'pg_catalog.pg_class'::regclass AND d.objid = c.oid \
            AND d.refclassid = 'pg_catalog.pg_class'::regclass AND d.deptype IN ('a', 'i') LIMIT 1) AS owned_by, \
        pg_catalog.obj_description(c.oid, 'pg_class') AS comment \
     FROM pg_catalog.pg_sequences s \
     JOIN pg_catalog.pg_namespace n ON n.nspname = s.schemaname \
     JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = s.sequencename AND c.relkind = 'S' \
     WHERE s.schemaname = $1 AND s.sequencename = $2";

const TYPE_DETAIL_SQL: &str = "SELECT t.oid::text AS oid, t.typtype::text AS typtype, t.typrelid::text AS typrelid, \
        (CASE t.typtype WHEN 'e' THEN 'enum' WHEN 'd' THEN 'domain' WHEN 'r' THEN 'range' WHEN 'c' THEN 'composite' WHEN 'b' THEN 'base' ELSE t.typtype::text END)::text AS category, \
        pg_catalog.pg_get_userbyid(t.typowner)::text AS owner, \
        CASE WHEN t.typtype = 'd' THEN pg_catalog.format_type(t.typbasetype, t.typtypmod) END AS base_type, \
        CASE WHEN t.typtype = 'd' THEN t.typnotnull::text END AS not_null, \
        CASE WHEN t.typtype = 'd' THEN t.typdefault END AS default_value, \
        CASE WHEN t.typtype = 'd' THEN (SELECT string_agg(pg_catalog.pg_get_constraintdef(x.oid, true), ' ' ORDER BY x.conname) \
                                        FROM pg_catalog.pg_constraint x WHERE x.contypid = t.oid) END AS constraints, \
        CASE WHEN t.typtype = 'r' THEN (SELECT pg_catalog.format_type(rg.rngsubtype, NULL) FROM pg_catalog.pg_range rg WHERE rg.rngtypid = t.oid) END AS subtype, \
        CASE WHEN t.typtype = 'e' THEN (SELECT string_agg(pg_catalog.quote_literal(e.enumlabel), ', ' ORDER BY e.enumsortorder) \
                                        FROM pg_catalog.pg_enum e WHERE e.enumtypid = t.oid) END AS enum_labels, \
        CASE WHEN t.typtype = 'c' THEN (SELECT string_agg(pg_catalog.quote_ident(a.attname) || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum) \
                                        FROM pg_catalog.pg_attribute a WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped) END AS attributes, \
        (SELECT e.extname FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid \
          WHERE d.classid = 'pg_catalog.pg_type'::regclass AND d.objid = t.oid AND d.deptype = 'e' LIMIT 1)::text AS extension, \
        pg_catalog.obj_description(t.oid, 'pg_type') AS comment \
     FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
     WHERE n.nspname = $1 AND t.typname = $2";

const ROUTINE_DETAIL_SQL: &str = "SELECT p.oid::text AS oid, p.prokind::text AS prokind, \
        (CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure' WHEN 'a' THEN 'aggregate' WHEN 'w' THEN 'window function' END)::text AS kind, \
        l.lanname::text AS language, pg_catalog.pg_get_function_arguments(p.oid) AS arguments, \
        CASE WHEN p.prokind <> 'p' THEN pg_catalog.pg_get_function_result(p.oid) END AS returns, \
        (CASE p.provolatile WHEN 'i' THEN 'immutable' WHEN 's' THEN 'stable' ELSE 'volatile' END)::text AS volatility, \
        (CASE p.proparallel WHEN 's' THEN 'safe' WHEN 'r' THEN 'restricted' ELSE 'unsafe' END)::text AS parallel, \
        p.prosecdef::text AS security_definer, p.proisstrict::text AS strict, p.proleakproof::text AS leakproof, \
        p.procost::text AS cost, CASE WHEN p.proretset THEN p.prorows::text END AS estimated_rows, \
        pg_catalog.pg_get_userbyid(p.proowner)::text AS owner, pg_catalog.obj_description(p.oid, 'pg_proc') AS comment \
     FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid = p.prolang \
     WHERE p.oid = pg_catalog.to_regprocedure($1)";

const FUNCTION_DEF_SQL: &str = "SELECT pg_catalog.pg_get_functiondef($1::oid) AS definition";

const AGGREGATE_DEF_SQL: &str = "SELECT 'CREATE AGGREGATE ' || p.oid::regprocedure::text || ' (' || \
        E'\\n  SFUNC = ' || a.aggtransfn::regproc::text || \
        E',\\n  STYPE = ' || pg_catalog.format_type(a.aggtranstype, NULL) || \
        coalesce(E',\\n  FINALFUNC = ' || nullif(a.aggfinalfn::regproc::text, '-'), '') || \
        coalesce(E',\\n  INITCOND = ' || pg_catalog.quote_literal(a.agginitval), '') || \
        coalesce(E',\\n  SORTOP = ' || nullif(a.aggsortop::regoperator::text, '0'), '') || \
        E'\\n)' AS definition \
     FROM pg_catalog.pg_aggregate a JOIN pg_catalog.pg_proc p ON p.oid = a.aggfnoid WHERE a.aggfnoid = $1::oid";

const TRIGGER_DETAIL_SQL: &str = "SELECT pg_catalog.pg_get_triggerdef(t.oid, true) AS definition, \
        (n.nspname || '.' || c.relname)::text AS on_table, (pn.nspname || '.' || p.proname || '()')::text AS function, \
        (CASE t.tgenabled WHEN 'D' THEN 'disabled' WHEN 'A' THEN 'always' WHEN 'R' THEN 'replica' ELSE 'enabled' END)::text AS enabled, \
        (t.tgconstraint <> 0)::text AS constraint_trigger, t.tgdeferrable::text AS deferrable, t.tginitdeferred::text AS initially_deferred, \
        pg_catalog.obj_description(t.oid, 'pg_trigger') AS comment \
     FROM pg_catalog.pg_trigger t \
     JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid \
     JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace \
     WHERE n.nspname = $1 AND c.relname = $2 AND t.tgname = $3";

const RULE_DETAIL_SQL: &str = "SELECT r.definition AS definition, (r.schemaname || '.' || r.tablename)::text AS on_table \
     FROM pg_catalog.pg_rules r WHERE r.schemaname = $1 AND r.tablename = $2 AND r.rulename = $3";

const POLICY_DETAIL_SQL: &str = "SELECT (p.schemaname || '.' || p.tablename)::text AS on_table, p.permissive::text AS permissive, \
        array_to_string(p.roles, ', ') AS roles, p.cmd::text AS command, p.qual AS using_expression, p.with_check AS check_expression \
     FROM pg_catalog.pg_policies p WHERE p.schemaname = $1 AND p.tablename = $2 AND p.policyname = $3";

const EXTENSION_DETAIL_SQL: &str = "SELECT e.extversion::text AS version, a.default_version::text AS default_version, n.nspname::text AS schema, \
        e.extrelocatable::text AS relocatable, pg_catalog.pg_get_userbyid(e.extowner)::text AS owner, a.comment AS comment \
     FROM pg_catalog.pg_extension e \
     JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace \
     LEFT JOIN pg_catalog.pg_available_extensions a ON a.name = e.extname \
     WHERE e.extname = $1";

const PUBLICATION_DETAIL_SQL: &str = "SELECT pg_catalog.pg_get_userbyid(p.pubowner)::text AS owner, p.puballtables::text AS all_tables, \
        p.pubinsert::text AS publish_insert, p.pubupdate::text AS publish_update, p.pubdelete::text AS publish_delete, p.pubtruncate::text AS publish_truncate, \
        (SELECT string_agg(pg_catalog.quote_ident(pt.schemaname) || '.' || pg_catalog.quote_ident(pt.tablename), ', ' ORDER BY pt.schemaname, pt.tablename) \
           FROM pg_catalog.pg_publication_tables pt WHERE pt.pubname = p.pubname) AS tables \
     FROM pg_catalog.pg_publication p WHERE p.pubname = $1";

const SUBSCRIPTION_DETAIL_SQL: &str = "SELECT pg_catalog.pg_get_userbyid(s.subowner)::text AS owner, s.subenabled::text AS enabled, \
        array_to_string(s.subpublications, ', ') AS publications, s.subslotname::text AS slot_name, s.subsynccommit::text AS synchronous_commit \
     FROM pg_catalog.pg_subscription s \
     WHERE s.subname = $1 AND s.subdbid = (SELECT d.oid FROM pg_catalog.pg_database d WHERE d.datname = current_database())";

const SLOT_DETAIL_SQL: &str = "SELECT s.slot_type::text AS type, s.plugin::text AS plugin, s.database::text AS database, \
        s.temporary::text AS temporary, s.active::text AS active, s.active_pid::text AS active_pid, \
        s.restart_lsn::text AS restart_lsn, s.confirmed_flush_lsn::text AS confirmed_flush_lsn, \
        pg_catalog.pg_size_pretty(pg_catalog.pg_wal_lsn_diff({CURRENT_LSN}, s.restart_lsn)) AS retained_wal \
     FROM pg_catalog.pg_replication_slots s WHERE s.slot_name = $1";

const FOREIGN_SERVER_DETAIL_SQL: &str = "SELECT w.fdwname::text AS wrapper, pg_catalog.pg_get_userbyid(s.srvowner)::text AS owner, \
        s.srvtype::text AS type, s.srvversion::text AS version, array_to_string(s.srvoptions, ', ') AS options, \
        (SELECT count(*) FROM pg_catalog.pg_foreign_table ft WHERE ft.ftserver = s.oid)::text AS foreign_tables, \
        (SELECT count(*) FROM pg_catalog.pg_user_mappings um WHERE um.srvid = s.oid)::text AS user_mappings, \
        pg_catalog.obj_description(s.oid, 'pg_foreign_server') AS comment \
     FROM pg_catalog.pg_foreign_server s JOIN pg_catalog.pg_foreign_data_wrapper w ON w.oid = s.srvfdw \
     WHERE s.srvname = $1";

const FOREIGN_SERVER_TABLES_SQL: &str = "SELECT c.relname::text AS name, n.nspname::text AS parent, \
        coalesce(array_to_string(ft.ftoptions, ' '), '')::text AS detail, s.srvname::text AS badge \
     FROM pg_catalog.pg_foreign_table ft \
     JOIN pg_catalog.pg_class c ON c.oid = ft.ftrelid \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     JOIN pg_catalog.pg_foreign_server s ON s.oid = ft.ftserver \
     WHERE s.srvname = $1 ORDER BY n.nspname, c.relname";

const FDW_DETAIL_SQL: &str = "SELECT pg_catalog.pg_get_userbyid(w.fdwowner)::text AS owner, \
        CASE WHEN w.fdwhandler <> 0 THEN w.fdwhandler::regproc::text END AS handler, \
        CASE WHEN w.fdwvalidator <> 0 THEN w.fdwvalidator::regproc::text END AS validator, \
        array_to_string(w.fdwoptions, ', ') AS options, \
        (SELECT count(*) FROM pg_catalog.pg_foreign_server s WHERE s.srvfdw = w.oid)::text AS servers, \
        pg_catalog.obj_description(w.oid, 'pg_foreign_data_wrapper') AS comment \
     FROM pg_catalog.pg_foreign_data_wrapper w WHERE w.fdwname = $1";

const ROLE_DETAIL_SQL: &str = "SELECT r.rolsuper::text AS superuser, r.rolcanlogin::text AS can_login, r.rolinherit::text AS inherit, \
        r.rolcreaterole::text AS create_role, r.rolcreatedb::text AS create_db, r.rolreplication::text AS replication, r.rolbypassrls::text AS bypass_rls, \
        CASE WHEN r.rolconnlimit < 0 THEN 'unlimited' ELSE r.rolconnlimit::text END AS connection_limit, r.rolvaliduntil::text AS valid_until, \
        (SELECT string_agg(g.rolname, ', ' ORDER BY g.rolname) FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles g ON g.oid = m.roleid WHERE m.member = r.oid) AS member_of, \
        (SELECT string_agg(g.rolname, ', ' ORDER BY g.rolname) FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles g ON g.oid = m.member WHERE m.roleid = r.oid) AS members, \
        array_to_string(r.rolconfig, ', ') AS settings, \
        (SELECT count(*) FROM pg_catalog.pg_stat_activity a WHERE a.usename = r.rolname)::text AS active_sessions, \
        pg_catalog.shobj_description(r.oid, 'pg_authid') AS comment \
     FROM pg_catalog.pg_roles r WHERE r.rolname = $1";

const GRANT_DETAIL_SQL: &str = "SELECT string_agg(g.privilege_type::text, ', ' ORDER BY g.privilege_type) AS privileges, \
        bool_or(g.is_grantable = 'YES')::text AS grantable, string_agg(DISTINCT g.grantor::text, ', ') AS grantor \
     FROM information_schema.role_table_grants g \
     WHERE g.grantee = $1 AND g.table_schema = $2 AND g.table_name = $3";

const TABLESPACE_DETAIL_SQL: &str = "SELECT pg_catalog.pg_get_userbyid(t.spcowner)::text AS owner, \
        pg_catalog.pg_tablespace_location(t.oid) AS location, \
        CASE WHEN pg_catalog.has_tablespace_privilege(t.oid, 'CREATE') THEN pg_catalog.pg_size_pretty(pg_catalog.pg_tablespace_size(t.oid)) END AS size, \
        array_to_string(t.spcoptions, ', ') AS options, pg_catalog.shobj_description(t.oid, 'pg_tablespace') AS comment \
     FROM pg_catalog.pg_tablespace t WHERE t.spcname = $1";

const SESSION_DETAIL_SQL: &str = "SELECT a.usename::text AS username, a.datname::text AS database, a.application_name::text AS application, \
        a.client_addr::text AS client_address, a.client_hostname::text AS client_hostname, a.client_port::text AS client_port, \
        a.backend_type::text AS backend_type, a.state::text AS state, a.wait_event_type::text AS wait_event_type, a.wait_event::text AS wait_event, \
        a.backend_start::text AS backend_start, a.xact_start::text AS transaction_start, a.query_start::text AS query_start, \
        a.state_change::text AS state_change, a.backend_xid::text AS backend_xid, a.backend_xmin::text AS backend_xmin, a.query AS query \
     FROM pg_catalog.pg_stat_activity a WHERE a.pid = $1::int";

const REPLICA_DETAIL_SQL: &str = "SELECT r.pid::text AS pid, r.usename::text AS username, r.application_name::text AS application, \
        r.client_addr::text AS client_address, r.client_hostname::text AS client_hostname, r.backend_start::text AS backend_start, \
        r.state::text AS state, r.sync_state::text AS sync_state, r.sync_priority::text AS sync_priority, \
        r.sent_lsn::text AS sent_lsn, r.write_lsn::text AS write_lsn, r.flush_lsn::text AS flush_lsn, r.replay_lsn::text AS replay_lsn, \
        r.write_lag::text AS write_lag, r.flush_lag::text AS flush_lag, r.replay_lag::text AS replay_lag, \
        pg_catalog.pg_size_pretty(pg_catalog.pg_wal_lsn_diff({CURRENT_LSN}, r.replay_lsn)) AS replay_behind \
     FROM pg_catalog.pg_stat_replication r WHERE {REPLICA_NAME} = $1";

const WAL_RECEIVER_DETAIL_SQL: &str = "SELECT w.pid::text AS pid, w.status::text AS status, w.sender_host::text AS sender_host, w.sender_port::text AS sender_port, \
        w.slot_name::text AS slot_name, w.receive_start_lsn::text AS receive_start_lsn, w.received_tli::text AS received_timeline, \
        w.latest_end_lsn::text AS latest_end_lsn, w.latest_end_time::text AS latest_end_time, \
        w.last_msg_send_time::text AS last_message_sent, w.last_msg_receipt_time::text AS last_message_received \
     FROM pg_catalog.pg_stat_wal_receiver w WHERE {WAL_RECEIVER_NAME} = $1";

const SETTING_DETAIL_SQL: &str = "SELECT s.setting::text AS value, s.unit::text AS unit, s.vartype::text AS type, s.context::text AS context, \
        s.category::text AS category, s.source::text AS source, s.sourcefile::text AS source_file, s.sourceline::text AS source_line, \
        s.min_val::text AS min_value, s.max_val::text AS max_value, array_to_string(s.enumvals, ', ') AS allowed_values, \
        s.boot_val::text AS boot_value, s.reset_val::text AS reset_value, s.pending_restart::text AS pending_restart, \
        s.short_desc::text AS short_desc, s.extra_desc::text AS extra_desc \
     FROM pg_catalog.pg_settings s WHERE s.name = $1";

const STAT_STATEMENTS_SCHEMA_SQL: &str = "SELECT n.nspname::text AS schema FROM pg_catalog.pg_extension e \
     JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'pg_stat_statements'";

fn no_stat_statements() -> AppError {
    AppError::invalid_input(
        "pg_stat_statements is not installed in this database. Add it to shared_preload_libraries, restart, \
         then run CREATE EXTENSION pg_stat_statements.",
    )
}

impl PostgresIntegration {
    async fn text_rows(&self, sql: &str, binds: &[&str]) -> AppResult<Vec<TextRow>> {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = query.bind(*bind);
        }
        let rows = query.fetch_all(&self.pool).await?;
        rows.iter().map(TextRow::from_row).collect()
    }

    async fn text_row(&self, sql: &str, binds: &[&str]) -> AppResult<Option<TextRow>> {
        Ok(self.text_rows(sql, binds).await?.into_iter().next())
    }

    // WHAT:  A tabular payload for the detail view, decoded like query results.
    async fn result_rows(&self, sql: &str, max_rows: usize) -> AppResult<ResultSet> {
        let mut statements = self.run(sql, max_rows).await?;
        match statements.pop() {
            Some(StatementResult::Rows { result }) => Ok(result),
            _ => Ok(empty_result()),
        }
    }

    // WHAT:  The same payload, but absent when it would be empty or unreadable.
    // WHY:   `ObjectDetail.rows` drives a tab in the UI: a payload the server
    //        refused (a stat view this fork lacks, a sequence we cannot SELECT)
    //        or one with no rows must not open an empty "Data · 0" tab, and it
    //        must never fail the whole detail.
    async fn optional_rows(&self, sql: &str, max_rows: usize) -> Option<ResultSet> {
        self.result_rows(sql, max_rows).await.ok().filter(|r| !r.rows.is_empty())
    }

    async fn list_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let scope = scope_of(parent);
        let rows = match kind {
            ObjectKind::SlowQuery => return self.list_slow_queries().await,
            ObjectKind::Replica => return self.list_replicas().await,
            other => match listing(other) {
                Some(Listing::Scoped(sql)) => {
                    sqlx::query(&sql).bind(scope.schema.as_deref()).bind(scope.table.as_deref()).fetch_all(&self.pool).await
                }
                Some(Listing::Global(sql)) => sqlx::query(&sql).fetch_all(&self.pool).await,
                None => return Ok(Vec::new()),
            },
        };
        let rows = rows.map_err(|e| listing_error(kind, e))?;
        // Nested lookups (a table's indexes…) report the owner as parent so the
        // reference resolves the same way from the sidebar and from a detail.
        let nested = scope.table.as_ref().and_then(|_| parent.map(|p| p.trim().to_string()));
        rows.iter().map(|row| summary_from_row(kind, row, nested.as_deref())).collect()
    }

    async fn list_replicas(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut out: Vec<ObjectSummary> = Vec::new();
        let senders = sqlx::query(&replicas_sql()).fetch_all(&self.pool).await.map_err(|e| listing_error(ObjectKind::Replica, e))?;
        for row in &senders {
            out.push(summary_from_row(ObjectKind::Replica, row, None)?);
        }
        // The walreceiver view only has a row on a standby; a fork without the
        // view must not hide the walsenders listed above.
        if let Ok(receivers) = sqlx::query(&wal_receiver_sql()).fetch_all(&self.pool).await {
            for row in &receivers {
                out.push(summary_from_row(ObjectKind::Replica, row, None)?);
            }
        }
        Ok(out)
    }

    async fn stat_statements_schema(&self) -> AppResult<String> {
        let schema: Option<String> = sqlx::query_scalar(STAT_STATEMENTS_SCHEMA_SQL).fetch_optional(&self.pool).await?;
        schema.ok_or_else(no_stat_statements)
    }

    async fn list_slow_queries(&self) -> AppResult<Vec<ObjectSummary>> {
        let schema = self.stat_statements_schema().await?;
        let rows = match sqlx::query(&slow_query_list_sql(&schema, true)).fetch_all(&self.pool).await {
            Ok(rows) => rows,
            Err(modern_err) => sqlx::query(&slow_query_list_sql(&schema, false))
                .fetch_all(&self.pool)
                .await
                .map_err(|_| listing_error(ObjectKind::SlowQuery, modern_err))?,
        };
        rows.iter().map(|row| summary_from_row(ObjectKind::SlowQuery, row, None)).collect()
    }

    async fn describe_object(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        use ObjectKind as K;
        match r.kind {
            K::Database => self.detail_database(r).await,
            K::Schema => self.detail_schema(r).await,
            K::Table | K::Partition | K::View | K::MaterializedView | K::ForeignTable => self.detail_relation(r).await,
            K::Index => self.detail_index(r).await,
            K::Constraint => self.detail_constraint(r).await,
            K::Sequence => self.detail_sequence(r).await,
            K::Type => self.detail_type(r).await,
            K::Function | K::Procedure | K::Aggregate => self.detail_routine(r).await,
            K::Trigger => self.detail_trigger(r).await,
            K::Rule => self.detail_rule(r).await,
            K::Policy => self.detail_policy(r).await,
            K::Extension => self.detail_extension(r).await,
            K::Publication => self.detail_publication(r).await,
            K::Subscription => self.detail_subscription(r).await,
            K::ReplicationSlot => self.detail_slot(r).await,
            K::ForeignServer => self.detail_foreign_server(r).await,
            K::ForeignDataWrapper => self.detail_fdw(r).await,
            K::Role => self.detail_role(r).await,
            K::Grant => self.detail_grant(r).await,
            K::Tablespace => self.detail_tablespace(r).await,
            K::Session | K::Lock => self.detail_session(r).await,
            K::Replica => self.detail_replica(r).await,
            K::Setting => self.detail_setting(r).await,
            K::SlowQuery => self.detail_slow_query(r).await,
            _ => Ok(ObjectDetail::empty(r)),
        }
    }

    // WHAT:  Schema of a scoped reference; partitions may live in another
    //        schema than their parent, so those are looked up through pg_inherits.
    async fn resolve_schema(&self, r: &ObjectRef) -> AppResult<(String, Scope)> {
        let scope = scope_of(r.parent.as_deref());
        if r.kind == ObjectKind::Partition {
            if let (Some(schema), Some(table)) = (&scope.schema, &scope.table) {
                let found: Option<String> = sqlx::query_scalar(PARTITION_SCHEMA_SQL)
                    .bind(schema)
                    .bind(table)
                    .bind(&r.name)
                    .fetch_optional(&self.pool)
                    .await?;
                if let Some(own) = found {
                    return Ok((own, scope));
                }
            }
        }
        Ok((scope.schema.clone().unwrap_or_else(|| "public".to_string()), scope))
    }

    // WHAT:  The owning table of a per-table object (constraint, trigger, rule, policy).
    fn table_scope(r: &ObjectRef) -> AppResult<(String, String)> {
        let scope = scope_of(r.parent.as_deref());
        match (scope.schema, scope.table) {
            (Some(schema), Some(table)) => Ok((schema, table)),
            _ => Err(AppError::invalid_input(format!(
                "A {} reference needs its table as parent (schema.table); got {:?}.",
                kind_name(r.kind),
                r.parent
            ))),
        }
    }

    async fn children_of(&self, owner: &str, kinds: &[ObjectKind]) -> Vec<ObjectSummary> {
        let mut children = Vec::new();
        for kind in kinds {
            if let Ok(list) = self.list_objects(*kind, Some(owner)).await {
                children.extend(list);
            }
        }
        children
    }

    async fn detail_database(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(DATABASE_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut detail = ObjectDetail::empty(r);
        detail.properties = row.properties(&[]);
        detail.rows = self
            .optional_rows(
                &format!(
                    "SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, \
                            tup_inserted, tup_updated, tup_deleted, conflicts, temp_files, temp_bytes, deadlocks, stats_reset \
                     FROM pg_catalog.pg_stat_database WHERE datname = {}",
                    quote_literal(&r.name)
                ),
                1,
            )
            .await;
        Ok(detail)
    }

    async fn detail_schema(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(SCHEMA_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let owner = row.get("owner").unwrap_or("?");
        let mut detail = ObjectDetail::empty(r)
            .definition(format!("CREATE SCHEMA {} AUTHORIZATION {}", quote_ident(&r.name), quote_ident(owner)), CodeLanguage::Sql)
            .action(ObjectAction::destructive("drop", "Drop schema", format!("DROP SCHEMA {}", quote_ident(&r.name))));
        detail.properties = row.properties(&[]);
        detail.children = self.children_of(&r.name, &[ObjectKind::Table, ObjectKind::View, ObjectKind::MaterializedView]).await;
        Ok(detail)
    }

    async fn detail_relation(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, _) = self.resolve_schema(r).await?;
        let core = self.text_row(RELATION_DETAIL_SQL, &[&schema, &r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let extras = self.text_row(RELATION_STATS_SQL, &[&schema, &r.name]).await.ok().flatten();
        let table = TableRef { schema: Some(schema.clone()), name: r.name.clone() };
        let qualified = qualify(&schema, &r.name);
        let owner = format!("{schema}.{}", r.name);
        let relkind = core.get("relkind").unwrap_or_default();

        let mut detail = ObjectDetail::empty(r);
        detail.properties = core.properties(&["oid", "relkind", "view_definition", "foreign_options"]);
        if let Some(extras) = &extras {
            detail.properties.extend(extras.properties(&["parent_schema", "parent_name"]));
        }
        detail.columns = self.columns(&table).await?;

        match relkind {
            "v" => {
                let body = core.get("view_definition").unwrap_or("");
                detail = detail.definition(format!("CREATE OR REPLACE VIEW {qualified} AS\n{body}"), CodeLanguage::Sql);
                detail.children = self.children_of(&owner, &[ObjectKind::Trigger, ObjectKind::Rule]).await;
                detail = detail.action(ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {qualified}")));
            }
            "m" => {
                let body = core.get("view_definition").unwrap_or("");
                let data = if core.is_true("populated") { "WITH DATA" } else { "WITH NO DATA" };
                detail = detail.definition(format!("CREATE MATERIALIZED VIEW {qualified} AS\n{body}\n{data}"), CodeLanguage::Sql);
                detail.children = self.children_of(&owner, &[ObjectKind::Index]).await;
                detail = detail
                    .action(ObjectAction::new("refresh", "Refresh", format!("REFRESH MATERIALIZED VIEW {qualified}")))
                    .action(ObjectAction::new("refresh_concurrently", "Refresh concurrently", format!("REFRESH MATERIALIZED VIEW CONCURRENTLY {qualified}")))
                    .action(ObjectAction::destructive("drop", "Drop materialized view", format!("DROP MATERIALIZED VIEW {qualified}")));
            }
            "f" => {
                let ddl = foreign_table_ddl(&qualified, &detail.columns, core.get("foreign_server"), core.get("foreign_options"));
                detail = detail
                    .definition(ddl, CodeLanguage::Sql)
                    .action(ObjectAction::new("analyze", "Analyze", format!("ANALYZE {qualified}")))
                    .action(ObjectAction::destructive("drop", "Drop foreign table", format!("DROP FOREIGN TABLE {qualified}")));
            }
            _ => {
                let partition_bound = extras.as_ref().and_then(|e| e.get("partition_bound").map(str::to_string));
                let parent = extras
                    .as_ref()
                    .and_then(|e| Some((e.get("parent_schema")?.to_string(), e.get("parent_name")?.to_string())));
                let head = match (&partition_bound, &parent) {
                    (Some(bound), Some((ps, pn))) => format!("CREATE TABLE {qualified} PARTITION OF {}\n  {bound}", qualify(ps, pn)),
                    _ => {
                        let mut base = self.ddl(&table).await?.unwrap_or_else(|| format!("CREATE TABLE {qualified} ()"));
                        if let Some(key) = extras.as_ref().and_then(|e| e.get("partition_key")) {
                            base.push_str(&format!(" PARTITION BY {key}"));
                        }
                        base
                    }
                };
                let constraints: Vec<(String, String)> = self
                    .text_rows(TABLE_CONSTRAINTS_SQL, &[&schema, &r.name])
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|row| Some((row.get("name")?.to_string(), row.get("definition")?.to_string())))
                    .collect();
                let indexes: Vec<String> = self
                    .text_rows(TABLE_INDEXES_SQL, &[&schema, &r.name])
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|row| row.get("definition").map(str::to_string))
                    .collect();
                detail = detail.definition(table_script_text(&head, &qualified, &constraints, &indexes), CodeLanguage::Sql);
                let mut kinds = vec![ObjectKind::Index, ObjectKind::Constraint, ObjectKind::Trigger, ObjectKind::Policy];
                if relkind == "p" {
                    kinds.push(ObjectKind::Partition);
                }
                detail.children = self.children_of(&owner, &kinds).await;
                detail = detail
                    .action(ObjectAction::new("analyze", "Analyze", format!("ANALYZE {qualified}")))
                    .action(ObjectAction::new("vacuum", "Vacuum", format!("VACUUM {qualified}")))
                    .action(ObjectAction::new("vacuum_analyze", "Vacuum analyze", format!("VACUUM ANALYZE {qualified}")));
                if let (Some(_), Some((ps, pn))) = (&partition_bound, &parent) {
                    detail = detail.action(ObjectAction::destructive(
                        "detach",
                        "Detach partition",
                        format!("ALTER TABLE {} DETACH PARTITION {qualified}", qualify(ps, pn)),
                    ));
                }
                detail = detail
                    .action(ObjectAction::destructive("truncate", "Truncate", format!("TRUNCATE TABLE {qualified}")))
                    .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {qualified}")));
            }
        }
        Ok(detail)
    }

    async fn detail_index(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, _) = self.resolve_schema(r).await?;
        let row = self.text_row(INDEX_DETAIL_SQL, &[&schema, &r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let qualified = qualify(&schema, &r.name);
        let oid = row.get("oid").unwrap_or("0");
        let mut detail = ObjectDetail::empty(r)
            .definition(row.get("definition").unwrap_or_default(), CodeLanguage::Sql)
            .action(ObjectAction::new("reindex", "Reindex", format!("REINDEX INDEX {qualified}")))
            .action(ObjectAction::destructive("drop", "Drop index", format!("DROP INDEX {qualified}")));
        detail.properties = row.properties(&["oid", "definition"]);
        detail.rows = self
            .optional_rows(
                &format!(
                    "SELECT k.ord::int AS position, pg_catalog.pg_get_indexdef(i.indexrelid, k.ord::int, true) AS expression, \
                            a.attname AS attribute, CASE WHEN a.attnum IS NOT NULL THEN pg_catalog.format_type(a.atttypid, a.atttypmod) END AS type, \
                            (k.ord <= i.indnkeyatts) AS key_column \
                     FROM pg_catalog.pg_index i \
                     CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
                     LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum AND k.attnum > 0 \
                     WHERE i.indexrelid = {} ORDER BY k.ord",
                    quote_literal(oid)
                ),
                MAX_DETAIL_ROWS,
            )
            .await;
        Ok(detail)
    }

    async fn detail_constraint(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, table) = Self::table_scope(r)?;
        let row = self.text_row(CONSTRAINT_DETAIL_SQL, &[&schema, &table, &r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let on_table = qualify(&schema, &table);
        let name = quote_ident(&r.name);
        let definition = row.get("definition").unwrap_or_default();
        let mut detail = ObjectDetail::empty(r).definition(format!("ALTER TABLE {on_table} ADD CONSTRAINT {name} {definition}"), CodeLanguage::Sql);
        detail.properties = row.properties(&["definition"]);
        if row.get("validated").is_some_and(|v| v == "false") {
            detail = detail.action(ObjectAction::new("validate", "Validate", format!("ALTER TABLE {on_table} VALIDATE CONSTRAINT {name}")));
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop constraint", format!("ALTER TABLE {on_table} DROP CONSTRAINT {name}"))))
    }

    async fn detail_sequence(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, _) = self.resolve_schema(r).await?;
        let row = self.text_row(SEQUENCE_DETAIL_SQL, &[&schema, &r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let qualified = qualify(&schema, &r.name);
        let mut detail = ObjectDetail::empty(r)
            .definition(sequence_ddl(&qualified, &row), CodeLanguage::Sql)
            .action(ObjectAction::destructive("restart", "Restart", format!("ALTER SEQUENCE {qualified} RESTART")))
            .action(ObjectAction::destructive("drop", "Drop sequence", format!("DROP SEQUENCE {qualified}")));
        detail.properties = row.properties(&[]);
        detail.rows = self.optional_rows(&format!("SELECT last_value, log_cnt, is_called FROM {qualified}"), 1).await;
        Ok(detail)
    }

    async fn detail_type(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, _) = self.resolve_schema(r).await?;
        let row = self.text_row(TYPE_DETAIL_SQL, &[&schema, &r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let qualified = qualify(&schema, &r.name);
        let typtype = row.get("typtype").unwrap_or_default();
        let mut detail = ObjectDetail::empty(r);
        detail.properties = row.properties(&["oid", "typtype", "typrelid", "enum_labels", "attributes"]);
        if let Some(ddl) = type_ddl(&qualified, &row) {
            detail = detail.definition(ddl, CodeLanguage::Sql);
        }
        match typtype {
            "e" => {
                detail.rows = self
                    .optional_rows(
                        &format!(
                            "SELECT enumsortorder AS sort_order, enumlabel AS label FROM pg_catalog.pg_enum WHERE enumtypid = {} ORDER BY enumsortorder",
                            quote_literal(row.get("oid").unwrap_or("0"))
                        ),
                        MAX_DETAIL_ROWS,
                    )
                    .await;
                detail = detail.action(ObjectAction::destructive("add_value", "Add enum value", format!("ALTER TYPE {qualified} ADD VALUE 'new_value'")));
            }
            "c" => {
                detail.rows = self
                    .optional_rows(
                        &format!(
                            "SELECT a.attnum AS position, a.attname AS name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS type \
                             FROM pg_catalog.pg_attribute a WHERE a.attrelid = {} AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                            quote_literal(row.get("typrelid").unwrap_or("0"))
                        ),
                        MAX_DETAIL_ROWS,
                    )
                    .await;
            }
            _ => {}
        }
        let drop = if typtype == "d" { format!("DROP DOMAIN {qualified}") } else { format!("DROP TYPE {qualified}") };
        Ok(detail.action(ObjectAction::destructive("drop", "Drop type", drop)))
    }

    async fn detail_routine(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, _) = self.resolve_schema(r).await?;
        let identity = format!("{}.{}", quote_ident(&schema), r.name);
        let row = self.text_row(ROUTINE_DETAIL_SQL, &[&identity]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let oid = row.get("oid").unwrap_or("0");
        let prokind = row.get("prokind");
        let definition_sql = if prokind == Some("a") { AGGREGATE_DEF_SQL } else { FUNCTION_DEF_SQL };
        let definition = self
            .text_row(definition_sql, &[oid])
            .await
            .ok()
            .flatten()
            .and_then(|d| d.get("definition").map(str::to_string));
        let mut detail = ObjectDetail::empty(r);
        detail.properties = row.properties(&["oid", "prokind"]);
        if let Some(definition) = definition {
            detail = detail.definition(definition, CodeLanguage::Sql);
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop", routine_drop_statement(prokind, &schema, &r.name))))
    }

    async fn detail_trigger(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, table) = Self::table_scope(r)?;
        let row = self.text_row(TRIGGER_DETAIL_SQL, &[&schema, &table, &r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let on_table = qualify(&schema, &table);
        let name = quote_ident(&r.name);
        let mut detail = ObjectDetail::empty(r).definition(row.get("definition").unwrap_or_default(), CodeLanguage::Sql);
        detail.properties = row.properties(&["definition"]);
        if row.get("enabled") == Some("disabled") {
            detail = detail.action(ObjectAction::destructive("enable", "Enable trigger", format!("ALTER TABLE {on_table} ENABLE TRIGGER {name}")));
        } else {
            detail = detail.action(ObjectAction::destructive("disable", "Disable trigger", format!("ALTER TABLE {on_table} DISABLE TRIGGER {name}")));
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop trigger", format!("DROP TRIGGER {name} ON {on_table}"))))
    }

    async fn detail_rule(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, table) = Self::table_scope(r)?;
        let row = self.text_row(RULE_DETAIL_SQL, &[&schema, &table, &r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let on_table = qualify(&schema, &table);
        let mut detail = ObjectDetail::empty(r).definition(row.get("definition").unwrap_or_default(), CodeLanguage::Sql);
        detail.properties = row.properties(&["definition"]);
        Ok(detail.action(ObjectAction::destructive("drop", "Drop rule", format!("DROP RULE {} ON {on_table}", quote_ident(&r.name)))))
    }

    async fn detail_policy(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, table) = Self::table_scope(r)?;
        let row = self.text_row(POLICY_DETAIL_SQL, &[&schema, &table, &r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let on_table = qualify(&schema, &table);
        let mut detail = ObjectDetail::empty(r).definition(policy_ddl(&r.name, &on_table, &row), CodeLanguage::Sql);
        detail.properties = row.properties(&[]);
        Ok(detail.action(ObjectAction::destructive("drop", "Drop policy", format!("DROP POLICY {} ON {on_table}", quote_ident(&r.name)))))
    }

    async fn detail_extension(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(EXTENSION_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let name = quote_ident(&r.name);
        let mut detail = ObjectDetail::empty(r).definition(
            format!(
                "CREATE EXTENSION IF NOT EXISTS {name} WITH SCHEMA {} VERSION {}",
                quote_ident(row.get("schema").unwrap_or("public")),
                quote_literal(row.get("version").unwrap_or_default())
            ),
            CodeLanguage::Sql,
        );
        detail.properties = row.properties(&[]);
        detail.rows = self
            .optional_rows(
                &format!(
                    "SELECT version, installed, superuser, relocatable, requires FROM pg_catalog.pg_available_extension_versions WHERE name = {} ORDER BY version",
                    quote_literal(&r.name)
                ),
                MAX_DETAIL_ROWS,
            )
            .await;
        if row.get("default_version").is_some_and(|d| Some(d) != row.get("version")) {
            detail = detail.action(ObjectAction::destructive("update", "Update to default version", format!("ALTER EXTENSION {name} UPDATE")));
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop extension", format!("DROP EXTENSION {name}"))))
    }

    async fn detail_publication(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(PUBLICATION_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut detail = ObjectDetail::empty(r).definition(publication_ddl(&r.name, &row), CodeLanguage::Sql);
        detail.properties = row.properties(&["tables"]);
        detail.rows = self
            .optional_rows(
                &format!("SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = {} ORDER BY 1, 2", quote_literal(&r.name)),
                MAX_DETAIL_ROWS,
            )
            .await;
        Ok(detail.action(ObjectAction::destructive("drop", "Drop publication", format!("DROP PUBLICATION {}", quote_ident(&r.name)))))
    }

    async fn detail_subscription(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(SUBSCRIPTION_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let name = quote_ident(&r.name);
        let mut detail = ObjectDetail::empty(r);
        detail.properties = row.properties(&[]);
        detail.rows = self
            .optional_rows(&format!("SELECT * FROM pg_catalog.pg_stat_subscription WHERE subname = {}", quote_literal(&r.name)), MAX_DETAIL_ROWS)
            .await;
        if row.is_true("enabled") {
            detail = detail.action(ObjectAction::destructive("disable", "Disable", format!("ALTER SUBSCRIPTION {name} DISABLE")));
        } else {
            detail = detail.action(ObjectAction::destructive("enable", "Enable", format!("ALTER SUBSCRIPTION {name} ENABLE")));
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop subscription", format!("DROP SUBSCRIPTION {name}"))))
    }

    async fn detail_slot(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let sql = SLOT_DETAIL_SQL.replace("{CURRENT_LSN}", CURRENT_LSN);
        let row = self.text_row(&sql, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut detail = ObjectDetail::empty(r);
        detail.properties = row.properties(&[]);
        Ok(detail.action(ObjectAction::destructive("drop", "Drop replication slot", format!("SELECT pg_drop_replication_slot({})", quote_literal(&r.name)))))
    }

    async fn detail_foreign_server(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(FOREIGN_SERVER_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut detail = ObjectDetail::empty(r).definition(foreign_server_ddl(&r.name, &row), CodeLanguage::Sql);
        detail.properties = row.properties(&[]);
        let tables = sqlx::query(FOREIGN_SERVER_TABLES_SQL).bind(&r.name).fetch_all(&self.pool).await?;
        detail.children = tables.iter().map(|row| summary_from_row(ObjectKind::ForeignTable, row, None)).collect::<AppResult<Vec<_>>>()?;
        Ok(detail.action(ObjectAction::destructive("drop", "Drop server", format!("DROP SERVER {}", quote_ident(&r.name)))))
    }

    async fn detail_fdw(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(FDW_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut ddl = format!("CREATE FOREIGN DATA WRAPPER {}", quote_ident(&r.name));
        if let Some(handler) = row.get("handler") {
            ddl.push_str(&format!("\n  HANDLER {handler}"));
        }
        if let Some(validator) = row.get("validator") {
            ddl.push_str(&format!("\n  VALIDATOR {validator}"));
        }
        ddl.push_str(&options_clause(row.get("options")));
        let mut detail = ObjectDetail::empty(r).definition(ddl, CodeLanguage::Sql);
        detail.properties = row.properties(&[]);
        detail.children = self
            .list_objects(ObjectKind::ForeignServer, None)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|s| s.badge.as_deref() == Some(r.name.as_str()))
            .collect();
        Ok(detail.action(ObjectAction::destructive("drop", "Drop foreign data wrapper", format!("DROP FOREIGN DATA WRAPPER {}", quote_ident(&r.name)))))
    }

    async fn detail_role(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(ROLE_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut detail = ObjectDetail::empty(r).definition(role_ddl(&r.name, &row), CodeLanguage::Sql);
        detail.properties = row.properties(&[]);
        detail.rows = self
            .optional_rows(
                &format!(
                    "SELECT table_schema, table_name, string_agg(privilege_type::text, ', ' ORDER BY privilege_type) AS privileges \
                     FROM information_schema.role_table_grants WHERE grantee = {} GROUP BY 1, 2 ORDER BY 1, 2",
                    quote_literal(&r.name)
                ),
                MAX_DETAIL_ROWS,
            )
            .await;
        Ok(detail.action(ObjectAction::destructive("drop", "Drop role", format!("DROP ROLE {}", quote_ident(&r.name)))))
    }

    async fn detail_grant(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let (schema, table) = Self::table_scope(r)?;
        let row = self.text_row(GRANT_DETAIL_SQL, &[&r.name, &schema, &table]).await?.filter(|row| row.get("privileges").is_some());
        let row = row.ok_or_else(|| not_found(r.kind, &r.name))?;
        let on_table = qualify(&schema, &table);
        let grantee = quote_ident(&r.name);
        let mut detail = ObjectDetail::empty(r)
            .definition(format!("GRANT {} ON {on_table} TO {grantee}", row.get("privileges").unwrap_or("ALL")), CodeLanguage::Sql);
        detail.properties = row.properties(&[]);
        detail.rows = self
            .optional_rows(
                &format!(
                    "SELECT privilege_type, is_grantable, grantor FROM information_schema.role_table_grants \
                     WHERE grantee = {} AND table_schema = {} AND table_name = {} ORDER BY privilege_type",
                    quote_literal(&r.name),
                    quote_literal(&schema),
                    quote_literal(&table)
                ),
                MAX_DETAIL_ROWS,
            )
            .await;
        Ok(detail.action(ObjectAction::destructive("revoke", "Revoke all", format!("REVOKE ALL ON {on_table} FROM {grantee}"))))
    }

    async fn detail_tablespace(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(TABLESPACE_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut detail = ObjectDetail::empty(r);
        detail.properties = row.properties(&[]);
        detail.rows = self
            .optional_rows(
                &format!(
                    "SELECT n.nspname AS schema, c.relname AS relation, c.relkind AS kind, pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid)) AS size \
                     FROM pg_catalog.pg_class c \
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     JOIN pg_catalog.pg_tablespace t ON t.oid = c.reltablespace \
                     WHERE t.spcname = {} ORDER BY pg_catalog.pg_relation_size(c.oid) DESC LIMIT 100",
                    quote_literal(&r.name)
                ),
                MAX_DETAIL_ROWS,
            )
            .await;
        Ok(detail.action(ObjectAction::destructive("drop", "Drop tablespace", format!("DROP TABLESPACE {}", quote_ident(&r.name)))))
    }

    async fn detail_session(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let pid = pid_of(&r.name)?;
        let pid_text = pid.to_string();
        let row = self.text_row(SESSION_DETAIL_SQL, &[&pid_text]).await?.ok_or_else(|| not_found(ObjectKind::Session, &pid_text))?;
        let mut detail = ObjectDetail::empty(r);
        if r.kind == ObjectKind::Lock {
            detail = detail.property("lock", r.name.clone());
        }
        detail.properties.extend(row.properties(&["query"]));
        if let Some(query) = row.get("query").filter(|q| !q.trim().is_empty()) {
            detail = detail.definition(query, CodeLanguage::Sql);
        }
        detail.rows = self
            .optional_rows(
                &format!(
                    "SELECT l.locktype, l.relation::regclass::text AS relation, l.mode, l.granted, l.waitstart \
                     FROM pg_catalog.pg_locks l WHERE l.pid = {pid} AND l.locktype <> 'virtualxid' ORDER BY l.granted, relation"
                ),
                MAX_DETAIL_ROWS,
            )
            .await;
        if detail.rows.is_none() {
            // `waitstart` arrived in PostgreSQL 14; older servers get the classic columns.
            detail.rows = self
                .optional_rows(
                    &format!(
                        "SELECT l.locktype, l.relation::regclass::text AS relation, l.mode, l.granted \
                         FROM pg_catalog.pg_locks l WHERE l.pid = {pid} AND l.locktype <> 'virtualxid' ORDER BY l.granted, relation"
                    ),
                    MAX_DETAIL_ROWS,
                )
                .await;
        }
        detail.actions = session_actions(pid);
        Ok(detail)
    }

    async fn detail_replica(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let sender_sql = REPLICA_DETAIL_SQL.replace("{CURRENT_LSN}", CURRENT_LSN).replace("{REPLICA_NAME}", REPLICA_NAME);
        let mut detail = ObjectDetail::empty(r);
        if let Some(row) = self.text_row(&sender_sql, &[&r.name]).await? {
            detail.properties = row.properties(&[]);
            if let Some(pid) = row.get("pid").and_then(|p| p.parse::<i64>().ok()) {
                detail = detail.action(ObjectAction::destructive("terminate", "Terminate walsender", format!("SELECT pg_terminate_backend({pid})")));
            }
            return Ok(detail);
        }
        let receiver_sql = WAL_RECEIVER_DETAIL_SQL.replace("{WAL_RECEIVER_NAME}", WAL_RECEIVER_NAME);
        let row = self.text_row(&receiver_sql, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        detail.properties = row.properties(&[]);
        Ok(detail)
    }

    async fn detail_setting(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let row = self.text_row(SETTING_DETAIL_SQL, &[&r.name]).await?.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut detail = ObjectDetail::empty(r);
        let description = [row.get("short_desc"), row.get("extra_desc")].into_iter().flatten().collect::<Vec<_>>().join("\n\n");
        if !description.is_empty() {
            detail = detail.definition(description, CodeLanguage::Text);
        }
        detail.properties = row.properties(&["short_desc", "extra_desc"]);
        detail.actions = setting_actions(&r.name, row.get("value").unwrap_or_default(), row.get("context"));
        Ok(detail)
    }

    async fn detail_slow_query(&self, r: &ObjectRef) -> AppResult<ObjectDetail> {
        let schema = self.stat_statements_schema().await?;
        let row = match self.text_row(&slow_query_detail_sql(&schema, true), &[&r.name]).await {
            Ok(row) => row,
            Err(modern_err) => self.text_row(&slow_query_detail_sql(&schema, false), &[&r.name]).await.map_err(|_| modern_err)?,
        };
        let row = row.ok_or_else(|| not_found(r.kind, &r.name))?;
        let mut detail = ObjectDetail::empty(r).definition(row.get("query").unwrap_or_default(), CodeLanguage::Sql);
        detail.properties = row.properties(&["query"]);
        Ok(detail.action(ObjectAction::destructive("reset", "Reset pg_stat_statements", format!("SELECT {}.pg_stat_statements_reset()", quote_ident(&schema)))))
    }
}

// ============================================================================
// SERVER STATS
//
// WHAT:  Six groups read from pg_stat_database / pg_stat_activity /
//        pg_stat_replication and the size functions, every figure cast to
//        float8 or text server-side so decoding never depends on the version.
// WHY:   The admin page draws sparklines from `numeric`; cumulative counters
//        (commits, tuples) are fine for that — the slope is the throughput.
// HOW:   Each group is its own query; one that a fork lacks reports itself
//        "Unavailable" with the driver's message instead of failing the page.
// ============================================================================

const MIB: f64 = 1024.0 * 1024.0;

const STATS_SERVER_SQL: &str = "SELECT current_setting('server_version')::text AS version, \
        pg_catalog.pg_postmaster_start_time()::text AS started, \
        extract(epoch from now() - pg_catalog.pg_postmaster_start_time())::float8 AS uptime_seconds, \
        current_database()::text AS database, \
        pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(current_database()))::text AS database_size, \
        pg_catalog.pg_is_in_recovery()::text AS in_recovery, \
        (SELECT count(*) FROM pg_catalog.pg_database d WHERE d.datallowconn)::float8 AS databases, \
        current_setting('TimeZone')::text AS timezone";

const STATS_CONNECTIONS_SQL: &str = "SELECT (count(*) FILTER (WHERE a.backend_type = 'client backend'))::float8 AS total, \
        (count(*) FILTER (WHERE a.backend_type = 'client backend' AND a.state = 'active'))::float8 AS active, \
        (count(*) FILTER (WHERE a.backend_type = 'client backend' AND a.state = 'idle'))::float8 AS idle, \
        (count(*) FILTER (WHERE a.backend_type = 'client backend' AND a.state LIKE 'idle in transaction%'))::float8 AS idle_in_transaction, \
        (count(*) FILTER (WHERE a.wait_event_type = 'Lock'))::float8 AS waiting_for_locks, \
        (count(*) FILTER (WHERE a.backend_type <> 'client backend'))::float8 AS background, \
        current_setting('max_connections')::float8 AS max_connections, \
        (SELECT extract(epoch from max(now() - x.xact_start))::float8 FROM pg_catalog.pg_stat_activity x \
          WHERE x.backend_type = 'client backend' AND x.xact_start IS NOT NULL) AS longest_transaction_seconds \
     FROM pg_catalog.pg_stat_activity a";

const STATS_CACHE_SQL: &str = "SELECT d.blks_hit::float8 AS blocks_hit, d.blks_read::float8 AS blocks_read, \
        CASE WHEN d.blks_hit + d.blks_read > 0 THEN round(100.0 * d.blks_hit / (d.blks_hit + d.blks_read), 2)::float8 END AS hit_ratio, \
        d.temp_files::float8 AS temp_files, d.temp_bytes::float8 AS temp_bytes, d.deadlocks::float8 AS deadlocks, d.conflicts::float8 AS conflicts, \
        current_setting('shared_buffers')::text AS shared_buffers, current_setting('effective_cache_size')::text AS effective_cache_size, \
        current_setting('work_mem')::text AS work_mem \
     FROM pg_catalog.pg_stat_database d WHERE d.datname = current_database()";

const STATS_THROUGHPUT_SQL: &str = "SELECT d.xact_commit::float8 AS commits, d.xact_rollback::float8 AS rollbacks, \
        d.tup_inserted::float8 AS inserted, d.tup_updated::float8 AS updated, d.tup_deleted::float8 AS deleted, \
        d.tup_fetched::float8 AS fetched, d.tup_returned::float8 AS returned, d.stats_reset::text AS stats_reset \
     FROM pg_catalog.pg_stat_database d WHERE d.datname = current_database()";

const STATS_REPLICATION_SQL: &str = "SELECT pg_catalog.pg_is_in_recovery()::text AS in_recovery, \
        (SELECT count(*) FROM pg_catalog.pg_stat_replication)::float8 AS replicas, \
        (SELECT count(*) FROM pg_catalog.pg_stat_replication WHERE state = 'streaming')::float8 AS streaming, \
        (SELECT extract(epoch from max(replay_lag))::float8 FROM pg_catalog.pg_stat_replication) AS max_replay_lag_seconds, \
        (SELECT count(*) FROM pg_catalog.pg_replication_slots)::float8 AS slots, \
        (SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE NOT active)::float8 AS inactive_slots, \
        CASE WHEN pg_catalog.pg_is_in_recovery() THEN extract(epoch from now() - pg_catalog.pg_last_xact_replay_timestamp())::float8 END AS replay_delay_seconds, \
        ({CURRENT_LSN})::text AS wal_position, current_setting('wal_level')::text AS wal_level";

fn stats_storage_sql() -> String {
    let schemas = user_schemas("n.nspname");
    let relations = format!("FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE {schemas}");
    format!(
        "SELECT pg_catalog.pg_database_size(current_database())::float8 AS database_bytes, \
                (SELECT coalesce(sum(pg_catalog.pg_table_size(c.oid)), 0)::float8 {relations} AND c.relkind IN ('r', 'p', 'm')) AS tables_bytes, \
                (SELECT coalesce(sum(pg_catalog.pg_indexes_size(c.oid)), 0)::float8 {relations} AND c.relkind IN ('r', 'p', 'm')) AS indexes_bytes, \
                (SELECT count(*) {relations} AND c.relkind IN ('r', 'p'))::float8 AS tables, \
                (SELECT count(*) {relations} AND c.relkind IN ('i', 'I'))::float8 AS indexes, \
                (SELECT n.nspname || '.' || c.relname {relations} AND c.relkind IN ('r', 'p', 'm') \
                  ORDER BY pg_catalog.pg_total_relation_size(c.oid) DESC LIMIT 1)::text AS largest_relation, \
                (SELECT pg_catalog.pg_size_pretty(pg_catalog.pg_total_relation_size(c.oid)) {relations} AND c.relkind IN ('r', 'p', 'm') \
                  ORDER BY pg_catalog.pg_total_relation_size(c.oid) DESC LIMIT 1)::text AS largest_relation_size, \
                (SELECT coalesce(sum(st.n_dead_tup), 0)::float8 FROM pg_catalog.pg_stat_user_tables st) AS dead_rows"
    )
}

fn num(row: &PgRow, name: &str) -> Option<f64> {
    row.try_get::<Option<f64>, _>(name).ok().flatten()
}

fn txt(row: &PgRow, name: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(name).ok().flatten()
}

fn push_number(stats: &mut Vec<Stat>, row: &PgRow, column: &str, label: &str, unit: Option<&str>) {
    if let Some(value) = num(row, column) {
        stats.push(Stat::number(label, value, unit));
    }
}

fn megabytes(bytes: f64) -> f64 {
    (bytes / MIB * 10.0).round() / 10.0
}

// WHAT:  `93784` → `1d 2h 3m`; sub-minute spans keep the seconds.
fn human_duration(seconds: f64) -> String {
    let total = seconds.max(0.0).round() as u64;
    let (days, hours, minutes, secs) = (total / 86_400, (total % 86_400) / 3_600, (total % 3_600) / 60, total % 60);
    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {secs}s")
    } else {
        format!("{secs}s")
    }
}

fn server_group(row: &PgRow) -> Vec<Stat> {
    let mut stats = Vec::new();
    if let Some(version) = txt(row, "version") {
        stats.push(Stat::text("Version", format!("PostgreSQL {version}")));
    }
    if let Some(uptime) = num(row, "uptime_seconds") {
        let mut stat = Stat::text("Uptime", human_duration(uptime));
        if let Some(started) = txt(row, "started") {
            stat = stat.with_hint(format!("Started {started}"));
        }
        stats.push(stat);
    }
    stats.push(Stat::text("Role", if txt(row, "in_recovery").as_deref() == Some("true") { "standby" } else { "primary" }));
    if let Some(database) = txt(row, "database") {
        let mut stat = Stat::text("Database", database);
        if let Some(size) = txt(row, "database_size") {
            stat = stat.with_hint(size);
        }
        stats.push(stat);
    }
    push_number(&mut stats, row, "databases", "Databases", None);
    if let Some(timezone) = txt(row, "timezone") {
        stats.push(Stat::text("Time zone", timezone));
    }
    stats
}

fn connections_group(row: &PgRow) -> Vec<Stat> {
    let mut stats = Vec::new();
    push_number(&mut stats, row, "active", "Active", None);
    push_number(&mut stats, row, "idle", "Idle", None);
    push_number(&mut stats, row, "idle_in_transaction", "Idle in transaction", None);
    push_number(&mut stats, row, "waiting_for_locks", "Waiting for locks", None);
    push_number(&mut stats, row, "total", "Total", None);
    push_number(&mut stats, row, "max_connections", "Max connections", None);
    if let (Some(total), Some(max)) = (num(row, "total"), num(row, "max_connections")) {
        if max > 0.0 {
            stats.push(Stat::number("Used", (total / max * 1000.0).round() / 10.0, Some("%")));
        }
    }
    push_number(&mut stats, row, "background", "Background workers", None);
    if let Some(seconds) = num(row, "longest_transaction_seconds") {
        stats.push(Stat::number("Longest transaction", (seconds * 10.0).round() / 10.0, Some("s")));
    }
    stats
}

fn cache_group(row: &PgRow) -> Vec<Stat> {
    let mut stats = Vec::new();
    push_number(&mut stats, row, "hit_ratio", "Buffer hit ratio", Some("%"));
    push_number(&mut stats, row, "blocks_hit", "Blocks hit", None);
    push_number(&mut stats, row, "blocks_read", "Blocks read", None);
    push_number(&mut stats, row, "temp_files", "Temp files", None);
    if let Some(bytes) = num(row, "temp_bytes") {
        stats.push(Stat::number("Temp written", megabytes(bytes), Some("MB")));
    }
    push_number(&mut stats, row, "deadlocks", "Deadlocks", None);
    push_number(&mut stats, row, "conflicts", "Conflicts", None);
    for (column, label) in [("shared_buffers", "shared_buffers"), ("effective_cache_size", "effective_cache_size"), ("work_mem", "work_mem")] {
        if let Some(value) = txt(row, column) {
            stats.push(Stat::text(label, value));
        }
    }
    stats
}

fn throughput_group(row: &PgRow) -> Vec<Stat> {
    let mut stats = Vec::new();
    push_number(&mut stats, row, "commits", "Commits", None);
    push_number(&mut stats, row, "rollbacks", "Rollbacks", None);
    push_number(&mut stats, row, "inserted", "Rows inserted", None);
    push_number(&mut stats, row, "updated", "Rows updated", None);
    push_number(&mut stats, row, "deleted", "Rows deleted", None);
    push_number(&mut stats, row, "fetched", "Rows fetched", None);
    push_number(&mut stats, row, "returned", "Rows returned", None);
    if let Some(reset) = txt(row, "stats_reset") {
        stats.push(Stat::text("Stats since", reset));
    }
    stats
}

fn replication_group(row: &PgRow) -> Vec<Stat> {
    let mut stats = Vec::new();
    let standby = txt(row, "in_recovery").as_deref() == Some("true");
    stats.push(Stat::text("Role", if standby { "standby" } else { "primary" }));
    push_number(&mut stats, row, "replicas", "Replicas", None);
    push_number(&mut stats, row, "streaming", "Streaming", None);
    if let Some(lag) = num(row, "max_replay_lag_seconds") {
        stats.push(Stat::number("Max replay lag", (lag * 1000.0).round() / 1000.0, Some("s")));
    }
    if let Some(delay) = num(row, "replay_delay_seconds") {
        stats.push(Stat::number("Replay delay", (delay * 10.0).round() / 10.0, Some("s")));
    }
    push_number(&mut stats, row, "slots", "Replication slots", None);
    push_number(&mut stats, row, "inactive_slots", "Inactive slots", None);
    if let Some(lsn) = txt(row, "wal_position") {
        stats.push(Stat::text("WAL position", lsn));
    }
    if let Some(level) = txt(row, "wal_level") {
        stats.push(Stat::text("wal_level", level));
    }
    stats
}

fn storage_group(row: &PgRow) -> Vec<Stat> {
    let mut stats = Vec::new();
    for (column, label) in [("database_bytes", "Database size"), ("tables_bytes", "Tables"), ("indexes_bytes", "Indexes")] {
        if let Some(bytes) = num(row, column) {
            stats.push(Stat::number(label, megabytes(bytes), Some("MB")).with_hint(format!("{} bytes", crate::model::objects::format_number(bytes))));
        }
    }
    push_number(&mut stats, row, "tables", "Table count", None);
    push_number(&mut stats, row, "indexes", "Index count", None);
    push_number(&mut stats, row, "dead_rows", "Dead rows", None);
    if let Some(largest) = txt(row, "largest_relation") {
        let mut stat = Stat::text("Largest relation", largest);
        if let Some(size) = txt(row, "largest_relation_size") {
            stat = stat.with_hint(size);
        }
        stats.push(stat);
    }
    stats
}

impl PostgresIntegration {
    async fn stat_group(&self, title: &str, sql: &str, build: fn(&PgRow) -> Vec<Stat>) -> StatGroup {
        let stats = match sqlx::query(sql).fetch_one(&self.pool).await {
            Ok(row) => build(&row),
            Err(err) => vec![Stat::text("Unavailable", AppError::from(err).message().to_string())],
        };
        StatGroup { title: title.to_string(), stats }
    }

    async fn collect_server_stats(&self) -> AppResult<ServerStats> {
        let replication_sql = STATS_REPLICATION_SQL.replace("{CURRENT_LSN}", CURRENT_LSN);
        let groups = vec![
            self.stat_group("Server", STATS_SERVER_SQL, server_group).await,
            self.stat_group("Connections", STATS_CONNECTIONS_SQL, connections_group).await,
            self.stat_group("Cache", STATS_CACHE_SQL, cache_group).await,
            self.stat_group("Throughput", STATS_THROUGHPUT_SQL, throughput_group).await,
            self.stat_group("Replication", &replication_sql, replication_group).await,
            self.stat_group("Storage", &stats_storage_sql(), storage_group).await,
        ];
        Ok(ServerStats::now(groups))
    }
}

// ============================================================================
// VECTOR SEARCH (pgvector)
//
// WHAT:  Nearest-neighbour query over a table with a `vector` / `halfvec`
//        column: `SELECT pk…, col <-> '[…]'::vector AS distance, payload…
//        FROM t [WHERE …] ORDER BY col <-> '[…]'::vector LIMIT k`.
// WHY:   Supabase, Neon, Timescale and plain Postgres all ship pgvector; the
//        playground needs no adapter of its own.
// HOW:   `collection` is `table` or `schema.table` (a bare name resolves to
//        the first user schema owning it, `public` first). `filter` is a JSON
//        string holding a SQL WHERE fragment; a `;` in it is refused so the
//        statement stays a single SELECT. The vector literal is built from
//        finite numbers only.
// ============================================================================

const MAX_TOP_K: u32 = 1000;

fn split_collection(collection: &str) -> AppResult<(Option<String>, String)> {
    let raw = collection.trim();
    if raw.is_empty() {
        return Err(AppError::invalid_input("Pick a table to search."));
    }
    let unquote = |s: &str| s.trim().trim_matches('"').to_string();
    match raw.split_once('.') {
        Some((schema, table)) if !schema.trim().is_empty() && !table.trim().is_empty() => Ok((Some(unquote(schema)), unquote(table))),
        _ => Ok((None, unquote(raw))),
    }
}

// WHAT:  `vector(1536)` → `vector`, `extensions.halfvec(3)` → `extensions.halfvec`;
//        None for anything that is not a pgvector dense type.
fn vector_base_type(data_type: &str) -> Option<&str> {
    let base = data_type.split('(').next().unwrap_or(data_type).trim();
    let simple = base.rsplit('.').next().unwrap_or(base);
    matches!(simple, "vector" | "halfvec").then_some(base)
}

fn vector_literal(vector: &[f64]) -> AppResult<String> {
    if vector.is_empty() {
        return Err(AppError::invalid_input("The query vector is empty."));
    }
    if vector.iter().any(|v| !v.is_finite()) {
        return Err(AppError::invalid_input("The query vector contains NaN or infinite values."));
    }
    let parts: Vec<String> = vector.iter().map(|v| format!("{v}")).collect();
    Ok(format!("[{}]", parts.join(",")))
}

fn vector_filter(filter: Option<&JsonValue>) -> AppResult<Option<String>> {
    match filter {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(text)) => {
            let fragment = text.trim();
            let fragment = fragment.strip_prefix("WHERE ").or_else(|| fragment.strip_prefix("where ")).unwrap_or(fragment).trim();
            if fragment.is_empty() {
                return Ok(None);
            }
            if fragment.contains(';') {
                return Err(AppError::invalid_input("The filter must be a single WHERE fragment; `;` is not allowed."));
            }
            Ok(Some(fragment.to_string()))
        }
        Some(_) => Err(AppError::invalid_input(
            "For Postgres the filter is a JSON string holding a SQL WHERE fragment, e.g. \"category = 'books' AND price < 20\".",
        )),
    }
}

fn vector_search_sql(
    qualified: &str,
    columns: &[ColumnInfo],
    target: &ColumnInfo,
    literal: &str,
    filter: Option<&str>,
    top_k: u32,
    include_vectors: bool,
) -> String {
    let base_type = vector_base_type(&target.data_type).unwrap_or("vector");
    let distance = format!("{} <-> '{literal}'::{base_type}", quote_ident(&target.name));
    let mut select: Vec<String> = columns.iter().filter(|c| c.primary_key).map(|c| quote_ident(&c.name)).collect();
    select.push(format!("{distance} AS distance"));
    select.extend(columns.iter().filter(|c| !c.primary_key && vector_base_type(&c.data_type).is_none()).map(|c| quote_ident(&c.name)));
    if include_vectors {
        select.extend(columns.iter().filter(|c| !c.primary_key && vector_base_type(&c.data_type).is_some()).map(|c| quote_ident(&c.name)));
    }
    let where_clause = filter.map(|f| format!(" WHERE ({f})")).unwrap_or_default();
    format!(
        "SELECT {} FROM {qualified}{where_clause} ORDER BY {distance} LIMIT {}",
        select.join(", "),
        top_k.clamp(1, MAX_TOP_K)
    )
}

impl PostgresIntegration {
    // WHAT:  Schema of a bare table name: `public` when it has one, else the
    //        first user schema that does.
    async fn resolve_table_schema(&self, table: &str) -> AppResult<String> {
        let found: Option<String> = sqlx::query_scalar(&format!(
            "SELECT n.nspname::text FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = $1 AND c.relkind IN ('r', 'p', 'v', 'm', 'f') AND {} \
             ORDER BY (n.nspname = 'public') DESC, n.nspname LIMIT 1",
            user_schemas("n.nspname")
        ))
        .bind(table)
        .fetch_optional(&self.pool)
        .await?;
        found.ok_or_else(|| AppError::not_found(format!("Table \"{table}\" was not found in any schema.")))
    }

    async fn pgvector_search(&self, req: &VectorSearchRequest) -> AppResult<ResultSet> {
        let (schema, table) = split_collection(&req.collection)?;
        let schema = match schema {
            Some(schema) => schema,
            None => self.resolve_table_schema(&table).await?,
        };
        let columns = self.columns(&TableRef { schema: Some(schema.clone()), name: table.clone() }).await?;
        if columns.is_empty() {
            return Err(AppError::not_found(format!("Table {} was not found.", qualify(&schema, &table))));
        }
        let target = match req.vector_name.as_deref().map(str::trim).filter(|n| !n.is_empty()) {
            Some(name) => {
                let column = columns
                    .iter()
                    .find(|c| c.name == name)
                    .ok_or_else(|| AppError::invalid_input(format!("Column \"{name}\" does not exist on {}.", qualify(&schema, &table))))?;
                if vector_base_type(&column.data_type).is_none() {
                    return Err(AppError::invalid_input(format!(
                        "Column \"{name}\" is {}, not a pgvector `vector` / `halfvec` column.",
                        column.data_type
                    )));
                }
                column
            }
            None => columns.iter().find(|c| vector_base_type(&c.data_type).is_some()).ok_or_else(|| {
                AppError::invalid_input(format!(
                    "{} has no `vector` column. Vector search needs the pgvector extension (CREATE EXTENSION vector) \
                     and a column of type vector(n).",
                    qualify(&schema, &table)
                ))
            })?,
        };
        let literal = vector_literal(&req.vector)?;
        let filter = vector_filter(req.filter.as_ref())?;
        let sql = vector_search_sql(&qualify(&schema, &table), &columns, target, &literal, filter.as_deref(), req.top_k, req.include_vectors);
        self.result_rows(&sql, req.top_k.clamp(1, MAX_TOP_K) as usize).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment};

    #[test]
    fn bytea_hex_to_base64() {
        assert_eq!(bytea_to_base64("\\x68656c6c6f"), "aGVsbG8=");
        assert_eq!(bytea_to_base64("\\x"), "");
    }

    // ---- object explorer -----------------------------------------------------

    fn text_row(cells: &[(&str, Option<&str>)]) -> TextRow {
        TextRow {
            cells: cells.iter().map(|(name, value)| ((*name).to_string(), value.map(str::to_string))).collect(),
        }
    }

    fn column(name: &str, data_type: &str, primary_key: bool) -> ColumnInfo {
        ColumnInfo { name: name.into(), data_type: data_type.into(), nullable: true, primary_key, ordinal: 0 }
    }

    #[test]
    fn parent_splits_into_schema_and_owner() {
        assert_eq!(scope_of(None), Scope { schema: None, table: None });
        assert_eq!(scope_of(Some("  ")), Scope { schema: None, table: None });
        assert_eq!(scope_of(Some("public")), Scope { schema: Some("public".into()), table: None });
        assert_eq!(scope_of(Some("public.users")), Scope { schema: Some("public".into()), table: Some("users".into()) });
        // Only the first dot splits, so `sales.2024.orders` keeps its owner intact.
        assert_eq!(scope_of(Some("sales.2024.orders")), Scope { schema: Some("sales".into()), table: Some("2024.orders".into()) });
    }

    #[test]
    fn every_declared_kind_has_a_query() {
        for kind in profile().object_kinds {
            let two_step = matches!(kind, ObjectKind::SlowQuery | ObjectKind::Replica);
            assert_eq!(listing(kind).is_some(), !two_step, "{kind:?} is declared but has no listing");
        }
    }

    // Scoped listings must bind both parameters, or sqlx errors at execution time.
    #[test]
    fn scoped_listings_bind_two_parameters() {
        for kind in profile().object_kinds {
            if let Some(Listing::Scoped(sql)) = listing(kind) {
                assert!(sql.contains("$1"), "{kind:?} scoped without $1");
                assert!(sql.contains("$2"), "{kind:?} scoped without $2");
            }
        }
    }

    // A `\` at end of line continues a normal string literal but is *literal*
    // inside a raw one — a raw-string catalog query would ship `\` + newline to
    // the server and fail to parse. Every backslash that survives must be a
    // deliberate escape (`\s` in a regex, `\n` in an E'' literal).
    fn assert_sql_is_clean(sql: &str) {
        assert!(!sql.contains("\\\n"), "raw-string line continuation leaked into: {sql}");
        // The SQL carries multi-byte characters, so inspect the escaped char itself
        // rather than slicing a window around the backslash.
        let stray: Vec<char> = sql
            .match_indices('\\')
            .filter_map(|(i, _)| sql[i + 1..].chars().next())
            .filter(|c| !matches!(*c, 's' | 'n'))
            .collect();
        assert!(stray.is_empty(), "stray backslash escapes {stray:?} in {sql}");
    }

    fn all_listing_sql() -> Vec<String> {
        let mut out: Vec<String> = ObjectKind::ALL
            .into_iter()
            .filter_map(listing)
            .map(|l| match l {
                Listing::Scoped(sql) | Listing::Global(sql) => sql,
            })
            .collect();
        out.push(replicas_sql());
        out.push(wal_receiver_sql());
        out.push(slow_query_list_sql("public", true));
        out.push(slow_query_list_sql("public", false));
        out.push(FOREIGN_SERVER_TABLES_SQL.to_string());
        out
    }

    // Every listing must project exactly the four columns `summary_from_row` reads.
    #[test]
    fn listings_project_the_summary_columns() {
        for sql in all_listing_sql() {
            for column in ["AS name", "AS parent", "AS detail", "AS badge"] {
                assert!(sql.contains(column), "missing {column} in {sql}");
            }
            assert_sql_is_clean(&sql);
        }
    }

    #[test]
    fn detail_queries_carry_no_stray_backslashes() {
        for sql in [
            DATABASE_DETAIL_SQL, SCHEMA_DETAIL_SQL, RELATION_DETAIL_SQL, RELATION_STATS_SQL, PARTITION_SCHEMA_SQL,
            TABLE_CONSTRAINTS_SQL, TABLE_INDEXES_SQL, INDEX_DETAIL_SQL, CONSTRAINT_DETAIL_SQL, SEQUENCE_DETAIL_SQL,
            TYPE_DETAIL_SQL, ROUTINE_DETAIL_SQL, FUNCTION_DEF_SQL, AGGREGATE_DEF_SQL, TRIGGER_DETAIL_SQL,
            RULE_DETAIL_SQL, POLICY_DETAIL_SQL, EXTENSION_DETAIL_SQL, PUBLICATION_DETAIL_SQL, SUBSCRIPTION_DETAIL_SQL,
            FOREIGN_SERVER_DETAIL_SQL, FDW_DETAIL_SQL, ROLE_DETAIL_SQL, GRANT_DETAIL_SQL, TABLESPACE_DETAIL_SQL,
            SESSION_DETAIL_SQL, SETTING_DETAIL_SQL, STAT_STATEMENTS_SCHEMA_SQL, STATS_SERVER_SQL,
            STATS_CONNECTIONS_SQL, STATS_CACHE_SQL, STATS_THROUGHPUT_SQL, QUERY_PREVIEW,
        ] {
            assert_sql_is_clean(sql);
        }
        assert_sql_is_clean(&slow_query_detail_sql("public", true));
        assert_sql_is_clean(&stats_storage_sql());
        // The escapes we do keep: a POSIX whitespace class and E'' newlines.
        assert!(QUERY_PREVIEW.contains(r"'\s+'"));
        assert!(AGGREGATE_DEF_SQL.contains(r"E'\n)'"));
    }

    #[test]
    fn placeholders_are_all_substituted() {
        let sql = format!(
            "{}{}{}{}",
            SLOT_DETAIL_SQL.replace("{CURRENT_LSN}", CURRENT_LSN),
            REPLICA_DETAIL_SQL.replace("{CURRENT_LSN}", CURRENT_LSN).replace("{REPLICA_NAME}", REPLICA_NAME),
            WAL_RECEIVER_DETAIL_SQL.replace("{WAL_RECEIVER_NAME}", WAL_RECEIVER_NAME),
            STATS_REPLICATION_SQL.replace("{CURRENT_LSN}", CURRENT_LSN),
        );
        assert!(!sql.contains('{'), "unsubstituted placeholder in {sql}");
        assert!(!replicas_sql().contains('{'));
        assert!(!wal_receiver_sql().contains('{'));
        assert!(!stats_storage_sql().contains('{'));
    }

    #[test]
    fn system_schemas_are_excluded() {
        let clause = user_schemas("n.nspname");
        for system in ["pg_catalog", "information_schema", "pg_toast"] {
            assert!(clause.contains(system), "{system} not excluded");
        }
        assert!(clause.contains("NOT LIKE 'pg_temp%'"));
    }

    #[test]
    fn table_script_appends_constraints_and_indexes() {
        let script = table_script_text(
            "CREATE TABLE \"public\".\"t\" (\n  \"id\" integer NOT NULL\n)",
            "\"public\".\"t\"",
            &[("t_name_key".into(), "UNIQUE (name)".into())],
            &["CREATE INDEX t_name_idx ON public.t USING btree (name)".into()],
        );
        assert!(script.starts_with("CREATE TABLE \"public\".\"t\" ("));
        assert!(script.contains("ALTER TABLE \"public\".\"t\" ADD CONSTRAINT \"t_name_key\" UNIQUE (name);"));
        assert!(script.ends_with("CREATE INDEX t_name_idx ON public.t USING btree (name);"));
    }

    #[test]
    fn type_ddl_per_category() {
        let enum_row = text_row(&[("typtype", Some("e")), ("enum_labels", Some("'new', 'old'"))]);
        assert_eq!(type_ddl("\"public\".\"status\"", &enum_row).as_deref(), Some("CREATE TYPE \"public\".\"status\" AS ENUM ('new', 'old')"));

        let composite = text_row(&[("typtype", Some("c")), ("attributes", Some("a integer, b text"))]);
        assert_eq!(type_ddl("\"public\".\"pair\"", &composite).as_deref(), Some("CREATE TYPE \"public\".\"pair\" AS (a integer, b text)"));

        let range = text_row(&[("typtype", Some("r")), ("subtype", Some("integer"))]);
        assert_eq!(type_ddl("\"public\".\"ir\"", &range).as_deref(), Some("CREATE TYPE \"public\".\"ir\" AS RANGE (SUBTYPE = integer)"));

        let domain = text_row(&[
            ("typtype", Some("d")),
            ("base_type", Some("text")),
            ("default_value", Some("'x'::text")),
            ("not_null", Some("true")),
            ("constraints", Some("CHECK ((VALUE <> ''::text))")),
        ]);
        assert_eq!(
            type_ddl("\"public\".\"nonempty\"", &domain).as_deref(),
            Some("CREATE DOMAIN \"public\".\"nonempty\" AS text DEFAULT 'x'::text NOT NULL CHECK ((VALUE <> ''::text))")
        );

        assert_eq!(type_ddl("\"public\".\"b\"", &text_row(&[("typtype", Some("b"))])), None);
    }

    #[test]
    fn sequence_ddl_lists_every_clause() {
        let row = text_row(&[
            ("data_type", Some("bigint")),
            ("increment_by", Some("1")),
            ("min_value", Some("1")),
            ("max_value", Some("9223372036854775807")),
            ("start_value", Some("1")),
            ("cache_size", Some("1")),
            ("cycle", Some("false")),
            ("owned_by", Some("\"public\".\"t\".\"id\"")),
        ]);
        let ddl = sequence_ddl("\"public\".\"t_id_seq\"", &row);
        assert!(ddl.starts_with("CREATE SEQUENCE \"public\".\"t_id_seq\"\n  AS bigint"));
        assert!(ddl.contains("\n  INCREMENT BY 1"));
        assert!(ddl.contains("\n  START WITH 1"));
        assert!(ddl.contains("\n  NO CYCLE"));
        assert!(ddl.ends_with("\n  OWNED BY \"public\".\"t\".\"id\""));
        assert!(sequence_ddl("s", &text_row(&[("cycle", Some("true"))])).contains("\n  CYCLE"));
    }

    #[test]
    fn policy_ddl_reassembles_the_statement() {
        let row = text_row(&[
            ("permissive", Some("RESTRICTIVE")),
            ("command", Some("SELECT")),
            ("roles", Some("app_user")),
            ("using_expression", Some("(owner = CURRENT_USER)")),
            ("check_expression", Some("(owner = CURRENT_USER)")),
        ]);
        let ddl = policy_ddl("owner_only", "\"public\".\"docs\"", &row);
        assert!(ddl.starts_with("CREATE POLICY \"owner_only\" ON \"public\".\"docs\""));
        assert!(ddl.contains("\n  AS RESTRICTIVE"));
        assert!(ddl.contains("\n  FOR SELECT"));
        assert!(ddl.contains("\n  TO app_user"));
        assert!(ddl.contains("\n  USING ((owner = CURRENT_USER))"));
        assert!(ddl.contains("\n  WITH CHECK ((owner = CURRENT_USER))"));
        // A permissive policy with no WITH CHECK omits both clauses.
        let plain = policy_ddl("p", "t", &text_row(&[("permissive", Some("PERMISSIVE")), ("command", Some("ALL"))]));
        assert!(!plain.contains("RESTRICTIVE") && !plain.contains("WITH CHECK"));
    }

    #[test]
    fn publication_ddl_folds_the_operation_list() {
        let row = text_row(&[
            ("all_tables", Some("false")),
            ("publish_insert", Some("true")),
            ("publish_update", Some("true")),
            ("publish_delete", Some("false")),
            ("publish_truncate", Some("false")),
            ("tables", Some("\"public\".\"a\", \"public\".\"b\"")),
        ]);
        let ddl = publication_ddl("pub", &row);
        assert!(ddl.contains("FOR TABLE \"public\".\"a\", \"public\".\"b\""));
        assert!(ddl.ends_with("WITH (publish = 'insert, update')"));
        let all = publication_ddl("pub", &text_row(&[("all_tables", Some("true")), ("publish_insert", Some("true"))]));
        assert!(all.contains("FOR ALL TABLES"));
    }

    #[test]
    fn role_ddl_negates_the_flags_it_lacks() {
        let row = text_row(&[
            ("superuser", Some("false")),
            ("can_login", Some("true")),
            ("inherit", Some("true")),
            ("connection_limit", Some("5")),
            ("valid_until", Some("2030-01-01 00:00:00+00")),
            ("member_of", Some("readers, writers")),
        ]);
        let ddl = role_ddl("app", &row);
        assert!(ddl.starts_with("CREATE ROLE \"app\" WITH"));
        assert!(ddl.contains("NOSUPERUSER") && ddl.contains("LOGIN") && ddl.contains("INHERIT"));
        assert!(ddl.contains("NOCREATEROLE") && ddl.contains("NOBYPASSRLS"));
        assert!(ddl.contains("CONNECTION LIMIT 5"));
        assert!(ddl.contains("VALID UNTIL '2030-01-01 00:00:00+00'"));
        assert!(ddl.contains("GRANT \"readers\" TO \"app\"") && ddl.contains("GRANT \"writers\" TO \"app\""));
        // "unlimited" is a display value, not a number: it must not reach the DDL.
        assert!(!role_ddl("a", &text_row(&[("connection_limit", Some("unlimited"))])).contains("CONNECTION LIMIT"));
    }

    #[test]
    fn options_become_a_quoted_clause() {
        assert_eq!(options_clause(None), "");
        assert_eq!(options_clause(Some("")), "");
        assert_eq!(options_clause(Some("host=db.example, port=5432")), " OPTIONS (\"host\" 'db.example', \"port\" '5432')");
        // A value containing a quote can never terminate the literal.
        assert_eq!(options_clause(Some("k=it's")), " OPTIONS (\"k\" 'it''s')");
    }

    #[test]
    fn foreign_object_ddl() {
        let server = text_row(&[("wrapper", Some("postgres_fdw")), ("type", Some("pg")), ("version", Some("15")), ("options", Some("host=remote"))]);
        let ddl = foreign_server_ddl("remote", &server);
        assert!(ddl.starts_with("CREATE SERVER \"remote\" TYPE 'pg' VERSION '15'"));
        assert!(ddl.contains("FOREIGN DATA WRAPPER \"postgres_fdw\""));
        assert!(ddl.ends_with(" OPTIONS (\"host\" 'remote')"));

        let columns = [column("id", "integer", true), column("name", "text", false)];
        let table = foreign_table_ddl("\"public\".\"remote_t\"", &columns, Some("remote"), Some("table_name=t"));
        assert!(table.starts_with("CREATE FOREIGN TABLE \"public\".\"remote_t\" (\n  \"id\" integer,\n  \"name\" text\n)"));
        assert!(table.contains("SERVER \"remote\" OPTIONS (\"table_name\" 't')"));
    }

    #[test]
    fn routine_drop_matches_the_prokind() {
        assert_eq!(routine_drop_statement(Some("f"), "public", "f(integer)"), "DROP FUNCTION \"public\".f(integer)");
        assert_eq!(routine_drop_statement(Some("p"), "public", "p()"), "DROP PROCEDURE \"public\".p()");
        assert_eq!(routine_drop_statement(Some("a"), "public", "a(integer)"), "DROP AGGREGATE \"public\".a(integer)");
        assert_eq!(routine_drop_statement(None, "s", "x()"), "DROP FUNCTION \"s\".x()");
    }

    #[test]
    fn session_and_lock_names_yield_a_pid() {
        assert_eq!(pid_of("4242").unwrap_or_default(), 4242);
        // A lock is named `pid:relation:mode`; the pid is still the first field.
        assert_eq!(pid_of("4242:public.users:RowExclusiveLock").unwrap_or_default(), 4242);
        assert!(pid_of("not-a-pid").is_err());
    }

    #[test]
    fn session_actions_cancel_then_terminate() {
        let actions = session_actions(77);
        assert_eq!(actions[0].statement, "SELECT pg_cancel_backend(77)");
        assert!(!actions[0].destructive);
        assert_eq!(actions[1].statement, "SELECT pg_terminate_backend(77)");
        assert!(actions[1].destructive);
    }

    #[test]
    fn settings_are_only_alterable_when_the_context_allows() {
        assert!(setting_actions("block_size", "8192", Some("internal")).is_empty());
        assert!(setting_actions("x", "1", None).is_empty());
        let actions = setting_actions("work_mem", "4MB", Some("user"));
        assert_eq!(actions[0].statement, "ALTER SYSTEM SET \"work_mem\" = '4MB'");
        assert!(actions[0].destructive && actions[1].destructive);
        assert_eq!(actions[1].statement, "ALTER SYSTEM RESET \"work_mem\"");
        assert!(!actions[2].destructive);
        assert!(setting_actions("shared_buffers", "128MB", Some("postmaster"))[0].label.contains("restart"));
    }

    #[test]
    fn properties_drop_empty_cells_and_underscores() {
        let row = text_row(&[("owner", Some("postgres")), ("oid", Some("1")), ("comment", None), ("total_size", Some("8192 bytes"))]);
        let props = row.properties(&["oid"]);
        assert_eq!(props.len(), 2);
        assert_eq!(props[0].name, "owner");
        assert_eq!(props[1].name, "total size");
        assert!(!row.is_true("owner"));
        assert!(text_row(&[("a", Some("true"))]).is_true("a"));
        assert!(text_row(&[("a", Some("t"))]).is_true("a"));
    }

    #[test]
    fn kind_names_read_as_prose() {
        assert_eq!(kind_name(ObjectKind::MaterializedView), "materialized view");
        assert_eq!(kind_name(ObjectKind::SlowQuery), "slow query");
        assert!(not_found(ObjectKind::Table, "t").message().starts_with("Table \"t\""));
    }

    // ---- server stats --------------------------------------------------------

    #[test]
    fn durations_read_as_human_spans() {
        assert_eq!(human_duration(0.0), "0s");
        assert_eq!(human_duration(45.4), "45s");
        assert_eq!(human_duration(90.0), "1m 30s");
        assert_eq!(human_duration(3_700.0), "1h 1m");
        assert_eq!(human_duration(93_784.0), "1d 2h 3m");
        assert_eq!(human_duration(-5.0), "0s");
    }

    #[test]
    fn bytes_become_rounded_megabytes() {
        assert_eq!(megabytes(0.0), 0.0);
        assert_eq!(megabytes(MIB), 1.0);
        assert_eq!(megabytes(MIB * 1.5), 1.5);
        assert_eq!(megabytes(MIB / 3.0), 0.3);
    }

    // ---- pgvector ------------------------------------------------------------

    #[test]
    fn collection_splits_on_the_first_dot() {
        assert_eq!(split_collection("items").unwrap_or_default(), (None, "items".to_string()));
        assert_eq!(split_collection(" public.items ").unwrap_or_default(), (Some("public".into()), "items".to_string()));
        assert_eq!(split_collection("\"public\".\"items\"").unwrap_or_default(), (Some("public".into()), "items".to_string()));
        assert!(split_collection("  ").is_err());
    }

    #[test]
    fn only_pgvector_types_are_searchable() {
        assert_eq!(vector_base_type("vector(1536)"), Some("vector"));
        assert_eq!(vector_base_type("vector"), Some("vector"));
        assert_eq!(vector_base_type("halfvec(3)"), Some("halfvec"));
        assert_eq!(vector_base_type("extensions.vector(3)"), Some("extensions.vector"));
        assert_eq!(vector_base_type("text"), None);
        assert_eq!(vector_base_type("double precision[]"), None);
    }

    #[test]
    fn vector_literal_rejects_unusable_input() {
        assert_eq!(vector_literal(&[1.0, -0.5, 2.25]).unwrap_or_default(), "[1,-0.5,2.25]");
        assert!(vector_literal(&[]).is_err());
        assert!(vector_literal(&[1.0, f64::NAN]).is_err());
        assert!(vector_literal(&[f64::INFINITY]).is_err());
    }

    #[test]
    fn vector_filter_is_a_single_where_fragment() {
        assert_eq!(vector_filter(None).unwrap_or_default(), None);
        assert_eq!(vector_filter(Some(&JsonValue::Null)).unwrap_or_default(), None);
        assert_eq!(vector_filter(Some(&JsonValue::String("  ".into()))).unwrap_or_default(), None);
        let filter = JsonValue::String("WHERE price < 20".into());
        assert_eq!(vector_filter(Some(&filter)).unwrap_or_default(), Some("price < 20".to_string()));
        // A second statement can never be smuggled in through the filter.
        let injection = JsonValue::String("1=1; DROP TABLE items".into());
        assert!(vector_filter(Some(&injection)).is_err());
        // Postgres takes SQL text, not a JSON object like Qdrant.
        assert!(vector_filter(Some(&serde_json::json!({ "must": [] }))).is_err());
    }

    #[test]
    fn vector_search_orders_by_distance() {
        let columns = [
            column("id", "integer", true),
            column("title", "text", false),
            column("embedding", "vector(3)", false),
        ];
        let target = &columns[2];
        let sql = vector_search_sql("\"public\".\"items\"", &columns, target, "[1,2,3]", None, 5, false);
        assert_eq!(
            sql,
            "SELECT \"id\", \"embedding\" <-> '[1,2,3]'::vector AS distance, \"title\" \
             FROM \"public\".\"items\" ORDER BY \"embedding\" <-> '[1,2,3]'::vector LIMIT 5"
        );
        // The vector column is projected only when asked for.
        let with_vectors = vector_search_sql("\"public\".\"items\"", &columns, target, "[1,2,3]", Some("title <> ''"), 5, true);
        assert!(with_vectors.contains(", \"embedding\" FROM"));
        assert!(with_vectors.contains("WHERE (title <> '')"));
        // top_k is clamped into range whatever the UI sends.
        assert!(vector_search_sql("t", &columns, target, "[1]", None, 0, false).ends_with("LIMIT 1"));
        assert!(vector_search_sql("t", &columns, target, "[1]", None, 99_999, false).ends_with(&format!("LIMIT {MAX_TOP_K}")));
    }

    #[test]
    fn halfvec_keeps_its_own_cast() {
        let columns = [column("id", "integer", true), column("e", "halfvec(2)", false)];
        let sql = vector_search_sql("t", &columns, &columns[1], "[1,2]", None, 3, false);
        assert!(sql.contains("'[1,2]'::halfvec"), "{sql}");
    }

    // WHAT:  Live round trip against a real server. Skipped unless DB_FREE_PG_HOST is set.
    // HOW:   DB_FREE_PG_HOST / PORT / USER / PASSWORD / DB, e.g. against a local docker postgres.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(host) = std::env::var("DB_FREE_PG_HOST") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Postgres,
            environment: Environment::Local,
            read_only: false,
            host: Some(host),
            port: std::env::var("DB_FREE_PG_PORT").ok().and_then(|p| p.parse().ok()),
            database: std::env::var("DB_FREE_PG_DB").ok(),
            username: std::env::var("DB_FREE_PG_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary::draft(&input, true),
            secret: std::env::var("DB_FREE_PG_PASSWORD").ok(),
        };
        let pg = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        pg.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert!(pg.server_version().await.unwrap_or_default().is_some_and(|v| v.starts_with("PostgreSQL")));
        let dbs = pg.databases().await.unwrap_or_default();
        assert!(dbs.iter().any(|d| Some(d) == pg.current_database().as_ref()), "{dbs:?}");

        let out = pg
            .execute(
                "CREATE TEMP TABLE dbfree_t (id serial primary key, name text, meta jsonb, raw bytea, ts timestamptz, n numeric(10,2), ok bool); \
                 INSERT INTO dbfree_t (name, meta, raw, ts, n, ok) VALUES ('ann', '{\"a\":1}', '\\x6869', now(), 12.50, true), ('bob', NULL, NULL, NULL, NULL, false); \
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
                assert!(matches!(first.get(2), Some(Value::Json(_))));
                assert_eq!(first.get(3), Some(&Value::Bytes("aGk=".into())));
                assert!(matches!(first.get(4), Some(Value::DateTime(_))));
                assert_eq!(first.get(5), Some(&Value::Decimal("12.50".into())));
                assert_eq!(first.get(6), Some(&Value::Bool(true)));
                let second = result.rows.get(1).cloned().unwrap_or_default();
                assert_eq!(second.get(2), Some(&Value::Null));
            }
            other => panic!("expected rows, got {other:?}"),
        }

        let catalog = pg.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(catalog.schemas.iter().any(|s| s.name == "public"));
        if let Some(table) = catalog.schemas.iter().flat_map(|s| s.tables.iter()).find(|t| t.kind == TableKind::Table) {
            let table_ref = TableRef { schema: table.schema.clone(), name: table.name.clone() };
            let cols = pg.columns(&table_ref).await.unwrap_or_else(|e| panic!("columns: {e}"));
            assert!(!cols.is_empty());
            let sort: Vec<crate::model::SortRule> = cols
                .iter()
                .filter(|c| c.primary_key)
                .map(|c| crate::model::SortRule { column: c.name.clone(), desc: false })
                .collect();
            let query = PageQuery { sort, filters: Vec::new(), offset: 0, limit: 5 };
            let page = pg.fetch_page(&table_ref, &query).await.unwrap_or_else(|e| panic!("page: {e}"));
            assert!(page.rows.len() <= 5);
            let total = pg.count(&table_ref, &[]).await.unwrap_or_else(|e| panic!("count: {e}"));
            assert!(total >= page.rows.len() as i64);
            assert!(pg.ddl(&table_ref).await.unwrap_or_default().is_some_and(|d| d.starts_with("CREATE TABLE")));
            let _ = pg.foreign_keys().await.unwrap_or_else(|e| panic!("fks: {e}"));
            let _ = pg.row_estimate(&table_ref).await.unwrap_or_else(|e| panic!("estimate: {e}"));
        }
        pg.close().await;
    }
}
