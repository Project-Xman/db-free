// SOT: saved-queries-service

use crate::error::{AppError, AppResult};
use crate::model::SavedQuery;
use crate::state::AppState;

pub fn list(state: &AppState) -> AppResult<Vec<SavedQuery>> {
    state.with_store(|store| store.list_saved_queries())
}

pub fn save(state: &AppState, query: &SavedQuery) -> AppResult<SavedQuery> {
    if query.name.trim().is_empty() {
        return Err(AppError::invalid_input("A saved query needs a name."));
    }
    state.with_store(|store| store.upsert_saved_query(query))
}

pub fn delete(state: &AppState, id: &str) -> AppResult<()> {
    state.with_store(|store| store.delete_saved_query(id))
}
