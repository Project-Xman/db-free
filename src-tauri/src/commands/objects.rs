// SOT: objects-commands, ipc-objects, ipc-object-detail, ipc-server-stats, ipc-vector-search, ipc-search, ipc-query-range, ipc-history

use crate::error::AppResult;
use crate::guard;
use crate::model::{
    ObjectDetail, ObjectKind, ObjectRef, ObjectSummary, RangeQueryRequest, RangeResult, ResultSet, SearchRequest,
    SearchResult, ServerStats, VectorSearchRequest,
};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ObjectsRequest {
    pub connection_id: String,
    pub kind: ObjectKind,
    pub parent: Option<String>,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ObjectRequest {
    pub connection_id: String,
    pub reference: ObjectRef,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct VectorSearchCommand {
    pub connection_id: String,
    pub request: VectorSearchRequest,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SearchCommand {
    pub connection_id: String,
    pub request: SearchRequest,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct RangeQueryCommand {
    pub connection_id: String,
    pub request: RangeQueryRequest,
}

// WHAT:  Object explorer + admin + playground commands. None runs user SQL, so
//        they pass through guard::session; object *actions* run through
//        execute_query so the statement guard applies to them.
#[tauri::command]
pub async fn list_objects(state: State<'_, AppState>, req: ObjectsRequest) -> AppResult<Vec<ObjectSummary>> {
    guard::session(&state, &req.connection_id, |ctx| async move { services::objects::list(&ctx, req.kind, req.parent.as_deref()).await }).await
}

#[tauri::command]
pub async fn load_object(state: State<'_, AppState>, req: ObjectRequest) -> AppResult<ObjectDetail> {
    guard::session(&state, &req.connection_id, |ctx| async move { services::objects::detail(&ctx, &req.reference).await }).await
}

#[tauri::command]
pub async fn server_stats(state: State<'_, AppState>, req: crate::commands::connections::SessionRequest) -> AppResult<ServerStats> {
    guard::session(&state, &req.connection_id, |ctx| async move { services::objects::stats(&ctx).await }).await
}

#[tauri::command]
pub async fn vector_search(state: State<'_, AppState>, req: VectorSearchCommand) -> AppResult<ResultSet> {
    let top_k = req.request.top_k.clamp(1, guard::MAX_PAGE_LIMIT);
    let request = VectorSearchRequest { top_k, ..req.request };
    guard::session(&state, &req.connection_id, |ctx| async move { services::objects::vector_search(&ctx, &request).await }).await
}

#[tauri::command]
pub async fn search_documents(state: State<'_, AppState>, req: SearchCommand) -> AppResult<SearchResult> {
    let limit = guard::clamp_page_limit(req.request.limit);
    let request = SearchRequest { limit, ..req.request };
    guard::session(&state, &req.connection_id, |ctx| async move { services::objects::search(&ctx, &request).await }).await
}

#[tauri::command]
pub async fn query_range(state: State<'_, AppState>, req: RangeQueryCommand) -> AppResult<RangeResult> {
    // NaN fails both checks: partial_cmp is None, so the request is rejected.
    let step_ok = req.request.step_seconds.partial_cmp(&0.0) == Some(std::cmp::Ordering::Greater);
    let range_ok = req.request.end.partial_cmp(&req.request.start) == Some(std::cmp::Ordering::Greater);
    if !step_ok || !range_ok {
        return Err(crate::error::AppError::invalid_input("The range needs end > start and a positive step."));
    }
    guard::session(&state, &req.connection_id, |ctx| async move { services::objects::query_range(&ctx, &req.request).await }).await
}

#[tauri::command]
pub async fn load_history(state: State<'_, AppState>, req: ObjectRequest) -> AppResult<ResultSet> {
    guard::session(&state, &req.connection_id, |ctx| async move { services::objects::history(&ctx, &req.reference).await }).await
}
