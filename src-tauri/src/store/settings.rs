// SOT: settings-table, settings-persistence, key-value-store

use crate::error::{AppError, AppResult};
use crate::store::Store;
use rusqlite::{params, OptionalExtension};

impl Store {
    pub fn get_setting(&self, key: &str) -> AppResult<Option<String>> {
        self.conn()
            .query_row("SELECT value FROM settings WHERE key = ?1", params![key], |row| row.get(0))
            .optional()
            .map_err(AppError::store)
    }

    pub fn set_setting(&self, key: &str, value: &str) -> AppResult<()> {
        self.conn()
            .execute(
                "INSERT INTO settings (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![key, value],
            )
            .map_err(AppError::store)?;
        Ok(())
    }

    pub fn delete_setting(&self, key: &str) -> AppResult<()> {
        self.conn().execute("DELETE FROM settings WHERE key = ?1", params![key]).map_err(AppError::store)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_round_trip() {
        let store = Store::open_in_memory().unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(store.get_setting("x").unwrap_or_default(), None);
        store.set_setting("x", "1").unwrap_or_else(|e| panic!("{e}"));
        store.set_setting("x", "2").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(store.get_setting("x").unwrap_or_default(), Some("2".into()));
        store.delete_setting("x").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(store.get_setting("x").unwrap_or_default(), None);
    }
}
