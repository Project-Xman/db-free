// SOT: integration-trait, integration-adapter-layer, engine-adapters, capabilities, connect-dispatch, quote-ident

use crate::error::AppResult;
use crate::model::{
    ColumnInfo, Engine, FilterRule, ForeignKey, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, StatementResult, TableRef,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ts_rs::TS;

pub mod clickhouse;
pub mod mongodb;
pub mod mysql;
pub mod postgres;
pub mod redis;
pub mod sql;
pub mod sqlite;

// ============================================================================
// INTEGRATION ADAPTER LAYER
//
// WHAT:  One adapter per database engine. Each lives in its own file and is the
//        only place that engine's client crate is imported.
// WHY:   Services, the guard and the UI see one contract (`Integration`) and
//        one value model (`model::Value`). Relational, document and key-value
//        stores all fit: a "table" is a table, a collection, or a key pattern;
//        a "row" is a record, a document, or a key/value pair; `execute` runs
//        the engine's native command language (SQL, Redis commands, Mongo shell).
// HOW:   To add an engine (e.g. Redis, MongoDB, MySQL, ClickHouse):
//          1. Add the variant to `model::Engine` and its `kind()`.
//          2. Create `integrations/<engine>.rs` implementing `Integration`,
//             mapping the client crate's errors to `AppError` at the boundary.
//          3. Add the match arm in `connect` below.
//          4. Add the crate to VENDOR_OWNERS in scripts/guardrail.py.
//          5. Add the UI entry in src/lib/engines.ts (fails `satisfies` until done).
//        Every other `match Engine` in the crate then fails to compile until
//        handled — that is the guardrail working, not a chore to bypass.
// WHERE: src-tauri/src/integrations/postgres.rs, src-tauri/src/integrations/sqlite.rs
// ============================================================================

// WHAT:  What an adapter can do, so the UI adapts (hide SQL-only affordances for
//        a key-value store, label the editor "Command" instead of "SQL", etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Capabilities {
    /// Accepts SQL text in `execute`.
    pub sql: bool,
    /// Groups collections under named schemas / databases / keyspaces.
    pub namespaces: bool,
    /// Has a stable column set per collection (false for schemaless documents).
    pub fixed_columns: bool,
    /// Supports offset paging in `fetch_page`.
    pub paging: bool,
    /// Can report an approximate row / key count cheaply.
    pub row_estimate: bool,
    /// Distinguishes tables from views.
    pub views: bool,
}

// WHAT:  Per-engine metadata reported once per session (status bar, editor label).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SessionInfo {
    pub engine: Engine,
    pub capabilities: Capabilities,
    pub server_version: Option<String>,
    /// Database this session is attached to (Postgres) or the file (SQLite).
    pub database: Option<String>,
    /// Every database the server exposes, for the sidebar switcher.
    pub databases: Vec<String>,
}

#[async_trait]
pub trait Integration: Send + Sync {
    fn engine(&self) -> Engine;
    fn capabilities(&self) -> Capabilities;
    async fn ping(&self) -> AppResult<()>;
    async fn server_version(&self) -> AppResult<Option<String>>;
    /// The database this session is attached to, if the engine has that notion.
    fn current_database(&self) -> Option<String>;
    /// All databases visible to this session (for switching). One entry for file engines.
    async fn databases(&self) -> AppResult<Vec<String>>;
    async fn catalog(&self) -> AppResult<SchemaCatalog>;
    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>>;
    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>>;
    /// Exact count, honouring filters. Used when the estimate is not good enough.
    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64>;
    /// `query.sort` is already defaulted (primary key) and validated by the service.
    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet>;
    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>>;
    async fn close(&self);
    /// Foreign keys visible to the session (ER diagram, FK traversal). Empty when unsupported.
    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        Ok(Vec::new())
    }
    /// CREATE statement for a table (export "include schema"). None when unsupported.
    async fn ddl(&self, _table: &TableRef) -> AppResult<Option<String>> {
        Ok(None)
    }
}

// WHAT:  The single dispatch point from a resolved connection to its adapter.
pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    match conn.summary.engine {
        Engine::Postgres => postgres::connect(conn).await,
        Engine::Mysql | Engine::Mariadb => mysql::connect(conn).await,
        Engine::Sqlite => sqlite::connect(conn).await,
        Engine::Clickhouse => clickhouse::connect(conn).await,
        Engine::Redis => self::redis::connect(conn).await,
        Engine::Mongodb => self::mongodb::connect(conn).await,
    }
}

pub async fn describe(integration: &dyn Integration) -> AppResult<SessionInfo> {
    Ok(SessionInfo {
        engine: integration.engine(),
        capabilities: integration.capabilities(),
        server_version: integration.server_version().await.unwrap_or(None),
        database: integration.current_database(),
        databases: integration.databases().await.unwrap_or_default(),
    })
}

// WHAT:  Double-quote an identifier (Postgres, SQLite, ClickHouse all accept "..").
pub fn quote_ident(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\"\""))
}

// WHAT:  Engine-aware identifier quoting: MySQL/MariaDB use backticks.
pub fn quote_ident_for(engine: Engine, raw: &str) -> String {
    match engine {
        Engine::Mysql | Engine::Mariadb => format!("`{}`", raw.replace('`', "``")),
        Engine::Postgres | Engine::Sqlite | Engine::Clickhouse | Engine::Redis | Engine::Mongodb => quote_ident(raw),
    }
}

pub fn qualified_name(table: &TableRef) -> String {
    qualified_name_for(Engine::Postgres, table)
}

pub fn qualified_name_for(engine: Engine, table: &TableRef) -> String {
    match &table.schema {
        Some(schema) => format!("{}.{}", quote_ident_for(engine, schema), quote_ident_for(engine, &table.name)),
        None => quote_ident_for(engine, &table.name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_escapes_embedded_quotes() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        let t = TableRef { schema: Some("public".into()), name: "users".into() };
        assert_eq!(qualified_name(&t), "\"public\".\"users\"");
        assert_eq!(quote_ident_for(Engine::Mysql, "we`ird"), "`we``ird`");
        assert_eq!(qualified_name_for(Engine::Mariadb, &t), "`public`.`users`");
    }
}
