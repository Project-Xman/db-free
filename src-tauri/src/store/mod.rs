// SOT: local-store, app-database, migrations, sqlite-state, store-handle

use crate::error::{AppError, AppResult};
use rusqlite::Connection;
use std::path::Path;

pub mod buffers;
pub mod connections;
pub mod documents;
pub mod history;
pub mod saved_queries;
pub mod settings;

// WHAT:  The app's own SQLite file: saved connections, query history, editor buffers.
// WHY:   One embedded store, no server; every sub-module is a table family.
// HOW:   `rusqlite` is imported only under src/store and src/integrations/sqlite.rs.
//        Migrations are numbered via PRAGMA user_version.
// WHERE: src-tauri/src/state.rs (owns the single instance behind a Mutex)
pub struct Store {
    conn: Connection,
}

const SCHEMA_VERSION: i64 = 2;

impl Store {
    pub fn open(path: &Path) -> AppResult<Store> {
        let conn = Connection::open(path).map_err(AppError::store)?;
        Store::init(conn)
    }

    pub fn open_in_memory() -> AppResult<Store> {
        let conn = Connection::open_in_memory().map_err(AppError::store)?;
        Store::init(conn)
    }

    fn init(conn: Connection) -> AppResult<Store> {
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")
            .map_err(AppError::store)?;
        let store = Store { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> AppResult<()> {
        let version: i64 = self
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(AppError::store)?;
        if version < 1 {
            self.conn
                .execute_batch(
                    "
                    CREATE TABLE IF NOT EXISTS connections (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        engine TEXT NOT NULL,
                        environment TEXT NOT NULL,
                        read_only INTEGER NOT NULL DEFAULT 0,
                        host TEXT,
                        port INTEGER,
                        database TEXT,
                        username TEXT,
                        file_path TEXT,
                        ssl_mode TEXT NOT NULL DEFAULT 'prefer',
                        secret_ciphertext BLOB,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS query_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        connection_id TEXT NOT NULL,
                        sql TEXT NOT NULL,
                        status TEXT NOT NULL,
                        error TEXT,
                        elapsed_ms INTEGER NOT NULL,
                        row_count INTEGER,
                        executed_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS query_history_connection_idx
                        ON query_history (connection_id, id DESC);
                    CREATE TABLE IF NOT EXISTS editor_buffers (
                        id TEXT PRIMARY KEY,
                        connection_id TEXT,
                        title TEXT NOT NULL,
                        content TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    ",
                )
                .map_err(AppError::store)?;
        }
        if version < 2 {
            self.conn
                .execute_batch(
                    "
                    ALTER TABLE query_history ADD COLUMN origin TEXT NOT NULL DEFAULT 'user';
                    CREATE TABLE IF NOT EXISTS settings (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS saved_queries (
                        id TEXT PRIMARY KEY,
                        connection_id TEXT,
                        name TEXT NOT NULL,
                        sql TEXT NOT NULL,
                        tags TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS documents (
                        id TEXT PRIMARY KEY,
                        kind TEXT NOT NULL,
                        connection_id TEXT,
                        name TEXT NOT NULL,
                        body TEXT NOT NULL,
                        tags TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS documents_kind_idx ON documents (kind, updated_at DESC);
                    ",
                )
                .map_err(AppError::store)?;
        }
        if version < SCHEMA_VERSION {
            self.conn
                .pragma_update(None, "user_version", SCHEMA_VERSION)
                .map_err(AppError::store)?;
        }
        Ok(())
    }

    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }
}

pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
