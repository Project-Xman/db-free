// SOT: neo4j-integration, memgraph-integration, neo4rs-adapter, cypher

use crate::error::{AppError, AppResult};
use crate::integrations::Integration;
use crate::model::ResolvedConnection;
use std::sync::Arc;

// WHAT:  Neo4j / Memgraph adapter.
// STATUS: scaffold — replaced by the real adapter in this change set.
pub async fn connect(_conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    Err(AppError::driver("Neo4j / Memgraph adapter is not available in this build yet."))
}
