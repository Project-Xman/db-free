// SOT: sparql-integration, rdf-triple-store, sparql-protocol, jena-graphdb-stardog-blazegraph-virtuoso

use crate::error::{AppError, AppResult};
use crate::integrations::Integration;
use crate::model::ResolvedConnection;
use std::sync::Arc;

// WHAT:  SPARQL endpoint adapter.
// STATUS: scaffold — replaced by the real adapter in this change set.
pub async fn connect(_conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    Err(AppError::driver("SPARQL endpoint adapter is not available in this build yet."))
}
