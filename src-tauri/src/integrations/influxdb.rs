// SOT: influxdb-integration, influxql, flux, influx-v2-api

use crate::error::{AppError, AppResult};
use crate::integrations::Integration;
use crate::model::ResolvedConnection;
use std::sync::Arc;

// WHAT:  InfluxDB adapter.
// STATUS: scaffold — replaced by the real adapter in this change set.
pub async fn connect(_conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    Err(AppError::driver("InfluxDB adapter is not available in this build yet."))
}
