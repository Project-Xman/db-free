// SOT: history-service, history-listing

use crate::error::AppResult;
use crate::model::{HistoryEntry, HistoryOrigin, HistoryStatus};
use crate::state::AppState;
use crate::store::history::NewHistoryEntry;

pub fn list(state: &AppState, connection_id: Option<&str>, origin: Option<HistoryOrigin>, limit: u32) -> AppResult<Vec<HistoryEntry>> {
    state.with_store(|store| store.list_history(connection_id, origin, limit))
}

pub fn clear(state: &AppState, connection_id: Option<&str>) -> AppResult<u64> {
    state.with_store(|store| store.clear_history(connection_id))
}

// WHAT:  Logs a statement the app issued on the user's behalf (table pages).
pub fn record_system(state: &AppState, connection_id: &str, sql: &str, ok: Result<u64, &str>, elapsed_ms: u64) {
    let entry = NewHistoryEntry {
        connection_id,
        sql,
        status: if ok.is_ok() { HistoryStatus::Ok } else { HistoryStatus::Error },
        origin: HistoryOrigin::System,
        error: ok.err(),
        elapsed_ms,
        row_count: ok.ok(),
    };
    if let Err(err) = state.with_store(|store| store.insert_history(&entry)) {
        log::warn!("system history log failed: {err}");
    }
}
