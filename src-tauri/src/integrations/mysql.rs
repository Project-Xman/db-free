// SOT: mysql-integration, mariadb, sqlx-mysql-adapter, mysql-value-decoding, mysql-catalog-queries

use crate::error::{AppError, AppResult};
use crate::integrations::sql::{order_clause, quote_literal, where_clause};
use crate::integrations::{qualified_name_for, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, PageQuery, ResolvedConnection,
    ResultSet, SchemaCatalog, SchemaInfo, SslMode, StatementResult, TableInfo, TableKind, TableRef,
    Value,
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
            Ok(bytes) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes)),
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
        Ok(text) => text_to_value(&type_name, &text),
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
        Ok(text) => text_to_value(&type_name, &text),
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

    // WHAT:  Rows of the first result set of one of the adapter's own metadata queries.
    async fn query_rows(&self, sql: &str) -> AppResult<Vec<Vec<Value>>> {
        let statements = self.run_with(sql, usize::MAX, decode_meta_cell).await?;
        Ok(statements
            .into_iter()
            .find_map(|s| match s {
                StatementResult::Rows { result } => Some(result.rows),
                StatementResult::Affected { .. } => None,
            })
            .unwrap_or_default())
    }
}

#[async_trait]
impl Integration for MysqlIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: true, namespaces: false, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: true, exact_estimate: false }
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
        let sql = format!(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, ORDINAL_POSITION, COLUMN_KEY \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} \
             ORDER BY ORDINAL_POSITION",
            quote_literal(db),
            quote_literal(&table.name)
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
