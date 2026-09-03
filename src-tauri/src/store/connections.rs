// SOT: connections-table, connection-persistence, secret-change

use crate::error::{AppError, AppResult};
use crate::model::{
    ConnectionInput, ConnectionRecord, ConnectionSummary, Engine, Environment, SslMode,
};
use crate::store::{now_rfc3339, Store};
use rusqlite::{params, OptionalExtension, Row};

// WHAT:  How an update treats the stored secret.
// WHY:   The UI never sees the secret, so "field left blank" must mean Keep.
pub enum SecretChange {
    Keep,
    Set(Vec<u8>),
    Clear,
}

const COLUMNS: &str = "id, name, engine, environment, read_only, host, port, database, username, \
                       file_path, ssl_mode, secret_ciphertext IS NOT NULL, created_at, updated_at";

fn summary_from_row(row: &Row<'_>) -> rusqlite::Result<ConnectionSummary> {
    let engine_raw: String = row.get(2)?;
    let env_raw: String = row.get(3)?;
    let ssl_raw: String = row.get(10)?;
    let port: Option<i64> = row.get(6)?;
    Ok(ConnectionSummary {
        id: row.get(0)?,
        name: row.get(1)?,
        engine: Engine::parse(&engine_raw).unwrap_or(Engine::Postgres),
        environment: Environment::parse(&env_raw).unwrap_or(Environment::Local),
        read_only: row.get::<_, i64>(4)? != 0,
        host: row.get(5)?,
        port: port.and_then(|p| u16::try_from(p).ok()),
        database: row.get(7)?,
        username: row.get(8)?,
        file_path: row.get(9)?,
        ssl_mode: SslMode::parse(&ssl_raw).unwrap_or(SslMode::Prefer),
        has_secret: row.get::<_, i64>(11)? != 0,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

impl Store {
    pub fn list_connections(&self) -> AppResult<Vec<ConnectionSummary>> {
        let sql = format!("SELECT {COLUMNS} FROM connections ORDER BY lower(name), created_at");
        let mut stmt = self.conn().prepare(&sql).map_err(AppError::store)?;
        let rows = stmt
            .query_map([], summary_from_row)
            .map_err(AppError::store)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(AppError::store)?;
        Ok(rows)
    }

    pub fn get_connection(&self, id: &str) -> AppResult<ConnectionSummary> {
        let sql = format!("SELECT {COLUMNS} FROM connections WHERE id = ?1");
        self.conn()
            .query_row(&sql, params![id], summary_from_row)
            .optional()
            .map_err(AppError::store)?
            .ok_or_else(|| AppError::not_found(format!("Connection {id} does not exist.")))
    }

    pub fn get_connection_record(&self, id: &str) -> AppResult<ConnectionRecord> {
        let summary = self.get_connection(id)?;
        let secret_ciphertext: Option<Vec<u8>> = self
            .conn()
            .query_row(
                "SELECT secret_ciphertext FROM connections WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .map_err(AppError::store)?;
        Ok(ConnectionRecord { summary, secret_ciphertext })
    }

    pub fn insert_connection(
        &self,
        input: &ConnectionInput,
        secret: Option<Vec<u8>>,
    ) -> AppResult<ConnectionSummary> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = now_rfc3339();
        self.conn()
            .execute(
                "INSERT INTO connections (id, name, engine, environment, read_only, host, port, \
                 database, username, file_path, ssl_mode, secret_ciphertext, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13)",
                params![
                    id,
                    input.name.trim(),
                    input.engine.as_str(),
                    input.environment.as_str(),
                    i64::from(input.read_only),
                    input.host,
                    input.port.map(i64::from),
                    input.database,
                    input.username,
                    input.file_path,
                    input.ssl_mode.as_str(),
                    secret,
                    now,
                ],
            )
            .map_err(AppError::store)?;
        self.get_connection(&id)
    }

    pub fn update_connection(
        &self,
        id: &str,
        input: &ConnectionInput,
        secret: SecretChange,
    ) -> AppResult<ConnectionSummary> {
        let now = now_rfc3339();
        let changed = self
            .conn()
            .execute(
                "UPDATE connections SET name = ?2, engine = ?3, environment = ?4, read_only = ?5, \
                 host = ?6, port = ?7, database = ?8, username = ?9, file_path = ?10, \
                 ssl_mode = ?11, updated_at = ?12 WHERE id = ?1",
                params![
                    id,
                    input.name.trim(),
                    input.engine.as_str(),
                    input.environment.as_str(),
                    i64::from(input.read_only),
                    input.host,
                    input.port.map(i64::from),
                    input.database,
                    input.username,
                    input.file_path,
                    input.ssl_mode.as_str(),
                    now,
                ],
            )
            .map_err(AppError::store)?;
        if changed == 0 {
            return Err(AppError::not_found(format!("Connection {id} does not exist.")));
        }
        match secret {
            SecretChange::Keep => {}
            SecretChange::Set(bytes) => {
                self.conn()
                    .execute(
                        "UPDATE connections SET secret_ciphertext = ?2 WHERE id = ?1",
                        params![id, bytes],
                    )
                    .map_err(AppError::store)?;
            }
            SecretChange::Clear => {
                self.conn()
                    .execute(
                        "UPDATE connections SET secret_ciphertext = NULL WHERE id = ?1",
                        params![id],
                    )
                    .map_err(AppError::store)?;
            }
        }
        self.get_connection(id)
    }

    pub fn delete_connection(&self, id: &str) -> AppResult<()> {
        let changed = self
            .conn()
            .execute("DELETE FROM connections WHERE id = ?1", params![id])
            .map_err(AppError::store)?;
        if changed == 0 {
            return Err(AppError::not_found(format!("Connection {id} does not exist.")));
        }
        self.conn()
            .execute("DELETE FROM query_history WHERE connection_id = ?1", params![id])
            .map_err(AppError::store)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(name: &str) -> ConnectionInput {
        ConnectionInput {
            name: name.into(),
            engine: Engine::Sqlite,
            environment: Environment::Production,
            read_only: true,
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
            file_path: Some("/tmp/a.db".into()),
            ssl_mode: SslMode::Disable,
        }
    }

    #[test]
    fn insert_update_delete_round_trip() {
        let store = Store::open_in_memory().unwrap_or_else(|e| panic!("{e}"));
        let created = store
            .insert_connection(&input("A"), Some(vec![1, 2, 3]))
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(created.has_secret);
        assert_eq!(created.environment, Environment::Production);
        assert!(created.read_only);

        let record = store.get_connection_record(&created.id).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(record.secret_ciphertext, Some(vec![1, 2, 3]));

        let updated = store
            .update_connection(&created.id, &input("B"), SecretChange::Keep)
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(updated.name, "B");
        assert!(updated.has_secret);

        let cleared = store
            .update_connection(&created.id, &input("B"), SecretChange::Clear)
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(!cleared.has_secret);

        assert_eq!(store.list_connections().unwrap_or_default().len(), 1);
        store.delete_connection(&created.id).unwrap_or_else(|e| panic!("{e}"));
        assert!(matches!(store.get_connection(&created.id), Err(AppError::NotFound { .. })));
    }
}
