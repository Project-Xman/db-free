// SOT: saved-queries-table, saved-query-persistence

use crate::error::{AppError, AppResult};
use crate::model::SavedQuery;
use crate::store::{now_rfc3339, Store};
use rusqlite::{params, Row};

fn from_row(row: &Row<'_>) -> rusqlite::Result<SavedQuery> {
    let tags: String = row.get(4)?;
    Ok(SavedQuery {
        id: row.get(0)?,
        connection_id: row.get(1)?,
        name: row.get(2)?,
        sql: row.get(3)?,
        tags: tags.split(',').map(str::trim).filter(|t| !t.is_empty()).map(String::from).collect(),
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

impl Store {
    pub fn list_saved_queries(&self) -> AppResult<Vec<SavedQuery>> {
        let mut stmt = self
            .conn()
            .prepare("SELECT id, connection_id, name, sql, tags, created_at, updated_at FROM saved_queries ORDER BY lower(name)")
            .map_err(AppError::store)?;
        let rows = stmt
            .query_map([], from_row)
            .map_err(AppError::store)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(AppError::store)?;
        Ok(rows)
    }

    pub fn upsert_saved_query(&self, query: &SavedQuery) -> AppResult<SavedQuery> {
        let now = now_rfc3339();
        let id = if query.id.is_empty() { uuid::Uuid::new_v4().to_string() } else { query.id.clone() };
        self.conn()
            .execute(
                "INSERT INTO saved_queries (id, connection_id, name, sql, tags, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6) \
                 ON CONFLICT(id) DO UPDATE SET connection_id = excluded.connection_id, name = excluded.name, \
                 sql = excluded.sql, tags = excluded.tags, updated_at = excluded.updated_at",
                params![id, query.connection_id, query.name.trim(), query.sql, query.tags.join(","), now],
            )
            .map_err(AppError::store)?;
        let mut stmt = self
            .conn()
            .prepare("SELECT id, connection_id, name, sql, tags, created_at, updated_at FROM saved_queries WHERE id = ?1")
            .map_err(AppError::store)?;
        stmt.query_row(params![id], from_row).map_err(AppError::store)
    }

    pub fn delete_saved_query(&self, id: &str) -> AppResult<()> {
        self.conn().execute("DELETE FROM saved_queries WHERE id = ?1", params![id]).map_err(AppError::store)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_list_delete() {
        let store = Store::open_in_memory().unwrap_or_else(|e| panic!("{e}"));
        let saved = store
            .upsert_saved_query(&SavedQuery {
                id: String::new(),
                connection_id: None,
                name: "Top users".into(),
                sql: "select 1".into(),
                tags: vec!["reports".into()],
                created_at: String::new(),
                updated_at: String::new(),
            })
            .unwrap_or_else(|e| panic!("{e}"));
        assert!(!saved.id.is_empty());
        assert_eq!(saved.tags, vec!["reports".to_string()]);
        let renamed = store
            .upsert_saved_query(&SavedQuery { name: "Top users v2".into(), ..saved.clone() })
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(renamed.id, saved.id);
        assert_eq!(store.list_saved_queries().unwrap_or_default().len(), 1);
        store.delete_saved_query(&saved.id).unwrap_or_else(|e| panic!("{e}"));
        assert!(store.list_saved_queries().unwrap_or_default().is_empty());
    }
}
