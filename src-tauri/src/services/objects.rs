// SOT: objects-service, object-listing, object-detail-loading, server-stats-loading, vector-search-service, search-service, range-query-service, ledger-history-service

use crate::error::AppResult;
use crate::guard::SessionCtx;
use crate::model::{
    ObjectDetail, ObjectKind, ObjectRef, ObjectSummary, RangeQueryRequest, RangeResult, ResultSet, SearchRequest,
    SearchResult, ServerStats, VectorSearchRequest,
};

// WHAT:  Object explorer, administration and playground tools: one call each,
//        straight to the adapter. Every request already passed guard::session.
// WHY:   Adapters answer only for kinds / tools in their `profile()`; the UI
//        reads that profile from SessionInfo, so an unsupported ask is a UI bug,
//        not a runtime branch here.
// WHERE: src-tauri/src/integrations/mod.rs (Integration trait defaults)
pub async fn list(ctx: &SessionCtx, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
    ctx.integration.objects(kind, parent).await
}

pub async fn detail(ctx: &SessionCtx, reference: &ObjectRef) -> AppResult<ObjectDetail> {
    ctx.integration.object_detail(reference).await
}

pub async fn stats(ctx: &SessionCtx) -> AppResult<ServerStats> {
    ctx.integration.server_stats().await
}

pub async fn vector_search(ctx: &SessionCtx, req: &VectorSearchRequest) -> AppResult<ResultSet> {
    ctx.integration.vector_search(req).await
}

pub async fn search(ctx: &SessionCtx, req: &SearchRequest) -> AppResult<SearchResult> {
    ctx.integration.search(req).await
}

pub async fn query_range(ctx: &SessionCtx, req: &RangeQueryRequest) -> AppResult<RangeResult> {
    ctx.integration.query_range(req).await
}

pub async fn history(ctx: &SessionCtx, reference: &ObjectRef) -> AppResult<ResultSet> {
    ctx.integration.history(reference).await
}
