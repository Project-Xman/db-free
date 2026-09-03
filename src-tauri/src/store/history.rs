// SOT: query-history-table, history-persistence

use crate::error::{AppError, AppResult};
use crate::model::{HistoryEntry, HistoryOrigin, HistoryStatus};
use crate::store::{now_rfc3339, Store};
use rusqlite::{params, Row};

pub struct NewHistoryEntry<'a> {
    pub connection_id: &'a str,
    pub sql: &'a str,
    pub status: HistoryStatus,
    pub origin: HistoryOrigin,
    pub error: Option<&'a str>,
    pub elapsed_ms: u64,
    pub row_count: Option<u64>,
}

fn entry_from_row(row: &Row<'_>) -> rusqlite::Result<HistoryEntry> {
    let status_raw: String = row.get(3)?;
    let elapsed: i64 = row.get(5)?;
    let rows: Option<i64> = row.get(6)?;
    let origin_raw: String = row.get(8)?;
    Ok(HistoryEntry {
        id: row.get(0)?,
        connection_id: row.get(1)?,
        sql: row.get(2)?,
        status: HistoryStatus::parse(&status_raw),
        origin: HistoryOrigin::parse(&origin_raw),
        error: row.get(4)?,
        elapsed_ms: u64::try_from(elapsed).unwrap_or_default(),
        row_count: rows.and_then(|r| u64::try_from(r).ok()),
        executed_at: row.get(7)?,
    })
}

impl Store {
    pub fn insert_history(&self, entry: &NewHistoryEntry<'_>) -> AppResult<()> {
        self.conn()
            .execute(
                "INSERT INTO query_history (connection_id, sql, status, error, elapsed_ms, \
                 row_count, executed_at, origin) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    entry.connection_id,
                    entry.sql,
                    entry.status.as_str(),
                    entry.error,
                    i64::try_from(entry.elapsed_ms).unwrap_or(i64::MAX),
                    entry.row_count.and_then(|r| i64::try_from(r).ok()),
                    now_rfc3339(),
                    entry.origin.as_str(),
                ],
            )
            .map_err(AppError::store)?;
        Ok(())
    }

    pub fn list_history(&self, connection_id: Option<&str>, origin: Option<HistoryOrigin>, limit: u32) -> AppResult<Vec<HistoryEntry>> {
        let limit = i64::from(limit.clamp(1, 2_000));
        let mut stmt = self
            .conn()
            .prepare(
                "SELECT id, connection_id, sql, status, error, elapsed_ms, row_count, executed_at, origin \
                 FROM query_history WHERE (?1 IS NULL OR connection_id = ?1) AND (?2 IS NULL OR origin = ?2) \
                 ORDER BY id DESC LIMIT ?3",
            )
            .map_err(AppError::store)?;
        let rows = stmt
            .query_map(params![connection_id, origin.map(HistoryOrigin::as_str), limit], entry_from_row)
            .map_err(AppError::store)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(AppError::store)?;
        Ok(rows)
    }

    pub fn clear_history(&self, connection_id: Option<&str>) -> AppResult<u64> {
        let changed = self
            .conn()
            .execute("DELETE FROM query_history WHERE (?1 IS NULL OR connection_id = ?1)", params![connection_id])
            .map_err(AppError::store)?;
        Ok(changed as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_is_newest_first_and_filterable() {
        let store = Store::open_in_memory().unwrap_or_else(|e| panic!("{e}"));
        for (conn, sql) in [("a", "select 1"), ("b", "select 2"), ("a", "select 3")] {
            store
                .insert_history(&NewHistoryEntry {
                    connection_id: conn,
                    sql,
                    status: HistoryStatus::Ok,
                    origin: if sql == "select 2" { HistoryOrigin::System } else { HistoryOrigin::User },
                    error: None,
                    elapsed_ms: 5,
                    row_count: Some(1),
                })
                .unwrap_or_else(|e| panic!("{e}"));
        }
        let all = store.list_history(None, None, 10).unwrap_or_default();
        assert_eq!(all.len(), 3);
        assert_eq!(all.first().map(|e| e.sql.as_str()), Some("select 3"));
        let only_a = store.list_history(Some("a"), None, 10).unwrap_or_default();
        assert_eq!(only_a.len(), 2);
        let system = store.list_history(None, Some(HistoryOrigin::System), 10).unwrap_or_default();
        assert_eq!(system.len(), 1);
        assert_eq!(store.clear_history(Some("a")).unwrap_or_default(), 2);
        assert_eq!(store.list_history(None, None, 10).unwrap_or_default().len(), 1);
    }
}
