// SOT: memcached-integration, memcached-text-protocol

use crate::error::{AppError, AppResult};
use crate::integrations::Integration;
use crate::model::ResolvedConnection;
use std::sync::Arc;

// WHAT:  Memcached adapter.
// STATUS: scaffold — replaced by the real adapter in this change set.
pub async fn connect(_conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    Err(AppError::driver("Memcached adapter is not available in this build yet."))
}
