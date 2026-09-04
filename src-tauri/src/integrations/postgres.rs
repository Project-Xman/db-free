// SOT: postgres-integration, sqlx-adapter, pg-value-decoding, pg-catalog-queries

use crate::integrations::sql::{order_clause, where_clause};
use crate::integrations::{qualified_name, quote_ident, Capabilities, Integration};
use crate::error::{AppError, AppResult};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, PageQuery, ResolvedConnection,
    ResultSet, SchemaCatalog, SchemaInfo, SslMode, StatementResult, TableInfo, TableKind, TableRef,
    Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use futures::StreamExt;
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

#[async_trait]
impl Integration for PostgresIntegration {
    fn engine(&self) -> Engine {
        Engine::Postgres
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: true, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: true, exact_estimate: false }
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
