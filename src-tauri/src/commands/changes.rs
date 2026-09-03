// SOT: changes-commands, ipc-pending-changes, commit-changes

use crate::error::AppResult;
use crate::guard;
use crate::model::{ChangePreview, QueryOutcome, StagedChange};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ChangesRequest {
    pub connection_id: String,
    pub changes: Vec<StagedChange>,
}

// WHAT:  Exact SQL for the Pending Changes panel; nothing runs.
#[tauri::command]
pub async fn preview_changes(state: State<'_, AppState>, req: ChangesRequest) -> AppResult<ChangePreview> {
    guard::local("preview_changes", async {
        let engine = services::connection::engine_of(&state, &req.connection_id)?;
        services::changes::preview(engine, &req.changes)
    })
    .await
}

// WHAT:  Runs the commit script through the statement guard, so read-only locks,
//        classification and history logging apply exactly as for typed SQL.
#[tauri::command]
pub async fn commit_changes(state: State<'_, AppState>, req: ChangesRequest) -> AppResult<QueryOutcome> {
    let engine = services::connection::engine_of(&state, &req.connection_id)?;
    let preview = services::changes::preview(engine, &req.changes)?;
    let script = preview.script.clone();
    guard::statement(
        &state,
        guard::StatementRequest { connection_id: &req.connection_id, sql: &preview.script, confirm_destructive: false },
        |ctx| async move { services::query::execute(&ctx, &script, 100).await },
    )
    .await
}
