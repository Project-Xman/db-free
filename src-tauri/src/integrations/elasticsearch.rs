// SOT: elasticsearch-integration, opensearch-integration, query-dsl, es-sql

use crate::error::{AppError, AppResult};
use crate::integrations::Integration;
use crate::model::ResolvedConnection;
use std::sync::Arc;

// WHAT:  Elasticsearch / OpenSearch adapter.
// STATUS: scaffold — replaced by the real adapter in this change set.
pub async fn connect(_conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    Err(AppError::driver("Elasticsearch / OpenSearch adapter is not available in this build yet."))
}
