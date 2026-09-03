// SOT: connection-commands, ipc-connections

use crate::error::AppResult;
use crate::guard;
use crate::integrations::SessionInfo;
use crate::model::{ConnectionInput, ConnectionSummary};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SaveConnectionRequest {
    pub id: Option<String>,
    pub input: ConnectionInput,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ConnectionIdRequest {
    pub id: String,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ConnectRequest {
    pub id: String,
    /// Session-only override of the saved database (sidebar switcher).
    pub database: Option<String>,
}

#[tauri::command]
pub async fn list_connections(state: State<'_, AppState>) -> AppResult<Vec<ConnectionSummary>> {
    guard::local("list_connections", async { services::connection::list(&state) }).await
}

#[tauri::command]
pub async fn save_connection(
    state: State<'_, AppState>,
    req: SaveConnectionRequest,
) -> AppResult<ConnectionSummary> {
    req.input.validate()?;
    guard::local("save_connection", async {
        services::connection::save(&state, req.id.as_deref(), &req.input)
    })
    .await
}

#[tauri::command]
pub async fn delete_connection(state: State<'_, AppState>, req: ConnectionIdRequest) -> AppResult<()> {
    guard::local("delete_connection", services::connection::delete(&state, &req.id)).await
}

#[tauri::command]
pub async fn test_connection(state: State<'_, AppState>, req: SaveConnectionRequest) -> AppResult<()> {
    req.input.validate()?;
    guard::local(
        "test_connection",
        services::connection::test(&state, req.id.as_deref(), &req.input),
    )
    .await
}

#[tauri::command]
pub async fn connect(state: State<'_, AppState>, req: ConnectRequest) -> AppResult<ConnectionSummary> {
    guard::local("connect", services::connection::connect(&state, &req.id, req.database.as_deref())).await
}

#[tauri::command]
pub async fn disconnect(state: State<'_, AppState>, req: ConnectionIdRequest) -> AppResult<()> {
    guard::local("disconnect", services::connection::disconnect(&state, &req.id)).await
}

#[tauri::command]
pub async fn active_sessions(state: State<'_, AppState>) -> AppResult<Vec<String>> {
    guard::local("active_sessions", async { Ok(services::connection::active_sessions(&state).await) }).await
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SessionRequest {
    pub connection_id: String,
}

#[tauri::command]
pub async fn describe_session(state: State<'_, AppState>, req: SessionRequest) -> AppResult<SessionInfo> {
    guard::session(&state, &req.connection_id, |ctx| async move {
        services::connection::describe(&ctx).await
    })
    .await
}
