// SOT: library-commands, ipc-saved-queries, ipc-documents

use crate::error::AppResult;
use crate::guard;
use crate::model::{Document, DocumentKind, SavedQuery};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SaveQueryRequest {
    pub query: SavedQuery,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct IdRequest {
    pub id: String,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ListDocumentsRequest {
    pub kind: DocumentKind,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SaveDocumentRequest {
    pub document: Document,
}

#[tauri::command]
pub async fn list_saved_queries(state: State<'_, AppState>) -> AppResult<Vec<SavedQuery>> {
    guard::local("list_saved_queries", async { services::saved_queries::list(&state) }).await
}

#[tauri::command]
pub async fn save_saved_query(state: State<'_, AppState>, req: SaveQueryRequest) -> AppResult<SavedQuery> {
    guard::local("save_saved_query", async { services::saved_queries::save(&state, &req.query) }).await
}

#[tauri::command]
pub async fn delete_saved_query(state: State<'_, AppState>, req: IdRequest) -> AppResult<()> {
    guard::local("delete_saved_query", async { services::saved_queries::delete(&state, &req.id) }).await
}

#[tauri::command]
pub async fn list_documents(state: State<'_, AppState>, req: ListDocumentsRequest) -> AppResult<Vec<Document>> {
    guard::local("list_documents", async { services::documents::list(&state, req.kind) }).await
}

#[tauri::command]
pub async fn save_document(state: State<'_, AppState>, req: SaveDocumentRequest) -> AppResult<Document> {
    guard::local("save_document", async { services::documents::save(&state, &req.document) }).await
}

#[tauri::command]
pub async fn delete_document(state: State<'_, AppState>, req: IdRequest) -> AppResult<()> {
    guard::local("delete_document", async { services::documents::delete(&state, &req.id) }).await
}
