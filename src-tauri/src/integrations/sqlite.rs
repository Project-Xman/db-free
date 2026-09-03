// SOT: sqlite-integration, rusqlite-adapter, sqlite-value-decoding, sqlite-catalog-queries

use crate::integrations::sql::{order_clause, where_clause};
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::error::{AppError, AppResult};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, PageQuery, ResolvedConnection,
    ResultSet, SchemaCatalog, SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use rusqlite::fallible_iterator::FallibleIterator;
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, OpenFlags};
use std::sync::{Arc, Mutex};

impl From<rusqlite::Error> for AppError {
    fn from(err: rusqlite::Error) -> Self {
        AppError::driver(err)
    }
}

// WHAT:  SQLite adapter on rusqlite (sync), run on the blocking pool.
// WHY:   rusqlite exposes exact dynamic cell types, and sharing one libsqlite3
//        with the local store avoids two bundled SQLite builds.
// HOW:   A read-only connection opens the file with SQLITE_OPEN_READ_ONLY so the
//        engine itself refuses writes, on top of the guard's own check.
pub struct SqliteIntegration {
    conn: Arc<Mutex<Connection>>,
    file_name: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let path = conn
        .summary
        .file_path
        .clone()
        .ok_or_else(|| AppError::invalid_input("SQLite connection has no file path."))?;
    let read_only = conn.summary.read_only;
    let open_path = path.clone();
    let connection = tokio::task::spawn_blocking(move || {
        let mut flags = OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI;
        flags |= if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        };
        Connection::open_with_flags(open_path, flags).map_err(AppError::from)
    })
    .await
    .map_err(AppError::internal)??;
    let file_name = std::path::Path::new(&path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.clone());
    Ok(Arc::new(SqliteIntegration { conn: Arc::new(Mutex::new(connection)), file_name }))
}

fn decode_cell(value: ValueRef<'_>, decl_type: &str) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => {
            if decl_type.eq_ignore_ascii_case("boolean") || decl_type.eq_ignore_ascii_case("bool") {
                Value::Bool(i != 0)
            } else {
                Value::Int(i)
            }
        }
        ValueRef::Real(f) => Value::Float(f),
        ValueRef::Text(bytes) => {
            let text = String::from_utf8_lossy(bytes).into_owned();
            if decl_type.to_ascii_uppercase().contains("JSON") {
                serde_json::from_str(&text).map(Value::Json).unwrap_or(Value::Text(text))
            } else if matches!(
                decl_type.to_ascii_uppercase().as_str(),
                "DATE" | "DATETIME" | "TIMESTAMP" | "TIME"
            ) {
                Value::DateTime(text)
            } else {
                Value::Text(text)
            }
        }
        ValueRef::Blob(bytes) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

impl SqliteIntegration {
    async fn blocking<T, F>(&self, f: F) -> AppResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> AppResult<T> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = conn.lock().map_err(|_| AppError::internal("sqlite session lock poisoned"))?;
            f(&guard)
        })
        .await
        .map_err(AppError::internal)?
    }
}

// WHAT:  Runs every statement in `sql` in order, collecting rows or change counts.
// HOW:   rusqlite::Batch walks the statement tail correctly (strings, comments).
fn run_batch(conn: &Connection, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
    let mut batch = rusqlite::Batch::new(conn, sql);
    let mut out = Vec::new();
    while let Some(mut stmt) = batch.next()? {
        if stmt.column_count() == 0 {
            let changed = stmt.execute([])?;
            out.push(StatementResult::Affected { rows_affected: changed as u64 });
            continue;
        }
        let columns: Vec<ColumnMeta> = stmt
            .columns()
            .iter()
            .map(|c| ColumnMeta {
                name: c.name().to_string(),
                type_name: c.decl_type().unwrap_or("").to_ascii_lowercase(),
            })
            .collect();
        let decl: Vec<String> = columns.iter().map(|c| c.type_name.clone()).collect();
        let mut rows = stmt.query([])?;
        let mut collected: Vec<Vec<Value>> = Vec::new();
        let mut truncated = false;
        while let Some(row) = rows.next()? {
            if collected.len() >= max_rows {
                truncated = true;
                break;
            }
            let mut cells = Vec::with_capacity(decl.len());
            for (i, decl_type) in decl.iter().enumerate() {
                cells.push(decode_cell(row.get_ref(i)?, decl_type));
            }
            collected.push(cells);
        }
        out.push(StatementResult::Rows { result: ResultSet { columns, rows: collected, truncated } });
    }
    Ok(out)
}

fn table_columns(conn: &Connection, name: &str) -> AppResult<Vec<ColumnInfo>> {
    let mut stmt = conn.prepare("SELECT cid, name, type, \"notnull\", pk FROM pragma_table_info(?1) ORDER BY cid")?;
    let rows = stmt
        .query_map(params![name], |row| {
            let cid: i64 = row.get(0)?;
            let notnull: i64 = row.get(3)?;
            let pk: i64 = row.get(4)?;
            Ok(ColumnInfo {
                name: row.get(1)?,
                data_type: row.get::<_, String>(2)?.to_ascii_lowercase(),
                nullable: notnull == 0,
                primary_key: pk > 0,
                ordinal: u32::try_from(cid).unwrap_or_default(),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

#[async_trait]
impl Integration for SqliteIntegration {
    fn engine(&self) -> Engine {
        Engine::Sqlite
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: true, namespaces: false, fixed_columns: true, paging: true, row_estimate: true, views: true }
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        self.blocking(|conn| {
            let v: String = conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
            Ok(Some(format!("SQLite {v}")))
        })
        .await
    }

    fn current_database(&self) -> Option<String> {
        Some(self.file_name.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.file_name.clone()])
    }

    async fn ping(&self) -> AppResult<()> {
        self.blocking(|conn| {
            conn.query_row("SELECT 1", [], |_| Ok(()))?;
            Ok(())
        })
        .await
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        self.blocking(|conn| {
            let mut stmt = conn.prepare(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') \
                 AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )?;
            let tables = stmt
                .query_map([], |row| {
                    let kind: String = row.get(1)?;
                    Ok(TableInfo {
                        schema: None,
                        name: row.get(0)?,
                        kind: if kind == "view" { TableKind::View } else { TableKind::Table },
                        row_estimate: None,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: "main".to_string(), tables }] })
        })
        .await
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let name = table.name.clone();
        self.blocking(move |conn| table_columns(conn, &name)).await
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let sql = format!("SELECT count(*) FROM {}", quote_ident(&table.name));
        self.blocking(move |conn| {
            let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
            Ok(Some(count))
        })
        .await
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count(*) FROM {}{}", quote_ident(&table.name), where_clause(Engine::Sqlite, filters));
        self.blocking(move |conn| {
            let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
            Ok(count)
        })
        .await
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let order = if query.sort.is_empty() { " ORDER BY rowid".to_string() } else { order_clause(Engine::Sqlite, &query.sort) };
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            quote_ident(&table.name),
            where_clause(Engine::Sqlite, &query.filters),
            order,
            query.limit,
            query.offset
        );
        let max_rows = query.limit as usize;
        let mut statements = self.blocking(move |conn| run_batch(conn, &sql, max_rows)).await?;
        match statements.pop() {
            Some(StatementResult::Rows { result }) => Ok(result),
            _ => Ok(ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }),
        }
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let sql = sql.to_string();
        self.blocking(move |conn| run_batch(conn, &sql, max_rows)).await
    }

    async fn close(&self) {}

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        self.blocking(|conn| {
            let mut tables = conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?;
            let names = tables.query_map([], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut out: Vec<ForeignKey> = Vec::new();
            for name in names {
                let mut stmt = conn.prepare("SELECT id, seq, \"table\", \"from\", \"to\" FROM pragma_foreign_key_list(?1) ORDER BY id, seq")?;
                let rows = stmt
                    .query_map(params![name], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?, row.get::<_, Option<String>>(4)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for (id, to_table, from_col, to_col) in rows {
                    let key_name = format!("{name}_fk_{id}");
                    match out.iter_mut().find(|fk| fk.name == key_name) {
                        Some(fk) => {
                            fk.from_columns.push(from_col);
                            fk.to_columns.push(to_col.unwrap_or_default());
                        }
                        None => out.push(ForeignKey {
                            name: key_name,
                            from_schema: None,
                            from_table: name.clone(),
                            from_columns: vec![from_col],
                            to_schema: None,
                            to_table,
                            to_columns: vec![to_col.unwrap_or_default()],
                        }),
                    }
                }
            }
            Ok(out)
        })
        .await
    }

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let name = table.name.clone();
        self.blocking(move |conn| {
            let sql: Option<String> = conn
                .query_row("SELECT sql FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1", params![name], |row| row.get(0))
                .ok();
            Ok(sql)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode};

    fn resolved(path: &str, read_only: bool) -> ResolvedConnection {
        let input = ConnectionInput {
            name: "t".into(),
            engine: Engine::Sqlite,
            environment: Environment::Local,
            read_only,
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
            file_path: Some(path.into()),
            ssl_mode: SslMode::Disable,
        };
        ResolvedConnection { summary: ConnectionSummary::draft(&input, false), secret: None }
    }

    #[tokio::test]
    async fn end_to_end_on_a_temp_file() {
        let dir = std::env::temp_dir().join(format!("db-free-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        let path = dir.join("t.db").to_string_lossy().into_owned();
        let integration = connect(&resolved(&path, false)).await.unwrap_or_else(|e| panic!("{e}"));
        integration.ping().await.unwrap_or_else(|e| panic!("{e}"));

        let created = integration
            .execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, meta JSON, flag BOOLEAN); \
                 INSERT INTO users VALUES (1, 'ann', '{\"a\":1}', 1), (2, 'bob', NULL, 0); \
                 SELECT * FROM users ORDER BY id;",
                100,
            )
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(created.len(), 3);
        match created.get(2) {
            Some(StatementResult::Rows { result }) => {
                assert_eq!(result.rows.len(), 2);
                assert_eq!(result.columns.len(), 4);
                let first = result.rows.first().cloned().unwrap_or_default();
                assert_eq!(first.first(), Some(&Value::Int(1)));
                assert_eq!(first.get(1), Some(&Value::Text("ann".into())));
                assert!(matches!(first.get(2), Some(Value::Json(_))));
                assert_eq!(first.get(3), Some(&Value::Bool(true)));
            }
            other => panic!("expected rows, got {other:?}"),
        }

        let catalog = integration.catalog().await.unwrap_or_else(|e| panic!("{e}"));
        let names: Vec<&str> = catalog
            .schemas
            .iter()
            .flat_map(|s| s.tables.iter().map(|t| t.name.as_str()))
            .collect();
        assert_eq!(names, vec!["users"]);

        let table = TableRef { schema: None, name: "users".into() };
        let cols = integration.columns(&table).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(cols.iter().any(|c| c.name == "id" && c.primary_key));
        assert_eq!(integration.row_estimate(&table).await.unwrap_or_default(), Some(2));

        let query = PageQuery {
            sort: vec![crate::model::SortRule { column: "id".into(), desc: true }],
            filters: vec![crate::model::FilterRule { column: "name".into(), op: crate::model::FilterOp::Contains, value: "n".into() }],
            offset: 0,
            limit: 10,
        };
        let page = integration.fetch_page(&table, &query).await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(page.rows.len(), 1, "only 'ann' contains n");
        assert_eq!(integration.count(&table, &query.filters).await.unwrap_or_default(), 1);

        integration
            .execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))", 10)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let fks = integration.foreign_keys().await.unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(fks.len(), 1);
        assert_eq!(fks.first().map(|f| (f.from_table.as_str(), f.to_table.as_str())), Some(("orders", "users")));
        assert!(integration.ddl(&table).await.unwrap_or_default().is_some_and(|d| d.starts_with("CREATE TABLE")));
        let truncated = integration.execute("SELECT * FROM users", 1).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(truncated.first(), Some(StatementResult::Rows { result }) if result.truncated));

        // Read-only flag is enforced by SQLite itself.
        let ro = connect(&resolved(&path, true)).await.unwrap_or_else(|e| panic!("{e}"));
        assert!(ro.execute("DELETE FROM users", 10).await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
