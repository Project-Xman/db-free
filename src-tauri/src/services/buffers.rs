// SOT: buffers-service, editor-buffer-persistence

use crate::error::AppResult;
use crate::model::EditorBuffer;
use crate::state::AppState;

pub fn list(state: &AppState) -> AppResult<Vec<EditorBuffer>> {
    state.with_store(|store| store.list_buffers())
}

pub fn save(state: &AppState, buffer: &EditorBuffer) -> AppResult<EditorBuffer> {
    state.with_store(|store| store.upsert_buffer(buffer))
}

pub fn delete(state: &AppState, id: &str) -> AppResult<()> {
    state.with_store(|store| store.delete_buffer(id))
}
