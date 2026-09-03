// SOT: query-commands, ipc-execute, ipc-history, ipc-buffers

use crate::error::AppResult;
use crate::guard;
use crate::model::{EditorBuffer, HistoryEntry, HistoryOrigin, QueryOutcome};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ExecuteQueryRequest {
    pub connection_id: String,
    pub sql: String,
    pub confirm_destructive: bool,
    pub max_rows: Option<u32>,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct HistoryRequest {
    pub connection_id: Option<String>,
    pub origin: Option<HistoryOrigin>,
    pub limit: u32,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SaveBufferRequest {
    pub buffer: EditorBuffer,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct BufferIdRequest {
    pub id: String,
}

#[tauri::command]
pub async fn execute_query(state: State<'_, AppState>, req: ExecuteQueryRequest) -> AppResult<QueryOutcome> {
    let max_rows = guard::clamp_result_rows(req.max_rows);
    let sql = req.sql.clone();
    guard::statement(
        &state,
        guard::StatementRequest {
            connection_id: &req.connection_id,
            sql: &req.sql,
            confirm_destructive: req.confirm_destructive,
        },
        |ctx| async move { services::query::execute(&ctx, &sql, max_rows).await },
    )
    .await
}

#[tauri::command]
pub async fn list_history(state: State<'_, AppState>, req: HistoryRequest) -> AppResult<Vec<HistoryEntry>> {
    guard::local("list_history", async {
        services::history::list(&state, req.connection_id.as_deref(), req.origin, req.limit)
    })
    .await
}

#[tauri::command]
pub async fn list_buffers(state: State<'_, AppState>) -> AppResult<Vec<EditorBuffer>> {
    guard::local("list_buffers", async { services::buffers::list(&state) }).await
}

#[tauri::command]
pub async fn save_buffer(state: State<'_, AppState>, req: SaveBufferRequest) -> AppResult<EditorBuffer> {
    guard::local("save_buffer", async { services::buffers::save(&state, &req.buffer) }).await
}

#[tauri::command]
pub async fn delete_buffer(state: State<'_, AppState>, req: BufferIdRequest) -> AppResult<()> {
    guard::local("delete_buffer", async { services::buffers::delete(&state, &req.id) }).await
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ClearHistoryRequest {
    pub connection_id: Option<String>,
}

#[tauri::command]
pub async fn clear_history(state: State<'_, AppState>, req: ClearHistoryRequest) -> AppResult<u64> {
    guard::local("clear_history", async { services::history::clear(&state, req.connection_id.as_deref()) }).await
}
