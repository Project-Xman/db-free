// SOT: schema-commands, ipc-schema

use crate::error::AppResult;
use crate::guard;
use crate::model::{ColumnInfo, ForeignKey, SchemaCatalog, TableRef};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct CatalogRequest {
    pub connection_id: String,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ColumnsRequest {
    pub connection_id: String,
    pub table: TableRef,
}

#[tauri::command]
pub async fn load_catalog(state: State<'_, AppState>, req: CatalogRequest) -> AppResult<SchemaCatalog> {
    guard::session(&state, &req.connection_id, |ctx| async move {
        services::schema::catalog(&ctx).await
    })
    .await
}

#[tauri::command]
pub async fn load_columns(state: State<'_, AppState>, req: ColumnsRequest) -> AppResult<Vec<ColumnInfo>> {
    guard::session(&state, &req.connection_id, |ctx| async move {
        services::schema::columns(&ctx, &req.table).await
    })
    .await
}

#[tauri::command]
pub async fn load_foreign_keys(state: State<'_, AppState>, req: CatalogRequest) -> AppResult<Vec<ForeignKey>> {
    guard::session(&state, &req.connection_id, |ctx| async move { services::schema::foreign_keys(&ctx).await }).await
}

#[tauri::command]
pub async fn load_ddl(state: State<'_, AppState>, req: ColumnsRequest) -> AppResult<Option<String>> {
    guard::session(&state, &req.connection_id, |ctx| async move { services::schema::ddl(&ctx, &req.table).await }).await
}
