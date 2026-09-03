// SOT: editor-buffers-table, buffer-persistence

use crate::error::{AppError, AppResult};
use crate::model::EditorBuffer;
use crate::store::{now_rfc3339, Store};
use rusqlite::{params, Row};

fn buffer_from_row(row: &Row<'_>) -> rusqlite::Result<EditorBuffer> {
    Ok(EditorBuffer {
        id: row.get(0)?,
        connection_id: row.get(1)?,
        title: row.get(2)?,
        content: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

impl Store {
    pub fn list_buffers(&self) -> AppResult<Vec<EditorBuffer>> {
        let mut stmt = self
            .conn()
            .prepare("SELECT id, connection_id, title, content, updated_at FROM editor_buffers ORDER BY updated_at")
            .map_err(AppError::store)?;
        let rows = stmt
            .query_map([], buffer_from_row)
            .map_err(AppError::store)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(AppError::store)?;
        Ok(rows)
    }

    pub fn upsert_buffer(&self, buffer: &EditorBuffer) -> AppResult<EditorBuffer> {
        let now = now_rfc3339();
        self.conn()
            .execute(
                "INSERT INTO editor_buffers (id, connection_id, title, content, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(id) DO UPDATE SET connection_id = excluded.connection_id, \
                 title = excluded.title, content = excluded.content, updated_at = excluded.updated_at",
                params![buffer.id, buffer.connection_id, buffer.title, buffer.content, now],
            )
            .map_err(AppError::store)?;
        Ok(EditorBuffer { updated_at: now, ..buffer.clone() })
    }

    pub fn delete_buffer(&self, id: &str) -> AppResult<()> {
        self.conn()
            .execute("DELETE FROM editor_buffers WHERE id = ?1", params![id])
            .map_err(AppError::store)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_replaces_content() {
        let store = Store::open_in_memory().unwrap_or_else(|e| panic!("{e}"));
        let buffer = EditorBuffer {
            id: "default".into(),
            connection_id: None,
            title: "Query".into(),
            content: "select 1".into(),
            updated_at: String::new(),
        };
        store.upsert_buffer(&buffer).unwrap_or_else(|e| panic!("{e}"));
        store
            .upsert_buffer(&EditorBuffer { content: "select 2".into(), ..buffer.clone() })
            .unwrap_or_else(|e| panic!("{e}"));
        let all = store.list_buffers().unwrap_or_default();
        assert_eq!(all.len(), 1);
        assert_eq!(all.first().map(|b| b.content.as_str()), Some("select 2"));
        store.delete_buffer("default").unwrap_or_else(|e| panic!("{e}"));
        assert!(store.list_buffers().unwrap_or_default().is_empty());
    }
}
