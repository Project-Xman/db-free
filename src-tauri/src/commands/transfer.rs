// SOT: transfer-commands, ipc-export, ipc-import

use crate::error::{AppError, AppResult};
use crate::guard;
use crate::model::{ExportReport, ImportReport, TableRef, TransferFormat};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use std::path::Path;
use std::time::Instant;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ExportRequest {
    pub connection_id: String,
    pub tables: Vec<TableRef>,
    pub format: TransferFormat,
    pub include_schema: bool,
    pub directory: String,
    pub max_rows: Option<u32>,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ImportRequest {
    pub connection_id: String,
    pub table: TableRef,
    pub path: String,
    pub format: TransferFormat,
}

#[tauri::command]
pub async fn export_tables(state: State<'_, AppState>, req: ExportRequest) -> AppResult<ExportReport> {
    let max_rows = u64::from(req.max_rows.unwrap_or(1_000_000)).max(1);
    guard::session(&state, &req.connection_id, |ctx| async move {
        services::transfer::export_tables(&ctx, &req.tables, req.format, req.include_schema, Path::new(&req.directory), max_rows).await
    })
    .await
}

// WHAT:  Parses the file, validates columns against the table, then runs batched
//        INSERTs through the statement guard (read-only locks apply).
#[tauri::command]
pub async fn import_file(state: State<'_, AppState>, req: ImportRequest) -> AppResult<ImportReport> {
    let started = Instant::now();
    let (columns, rows) = services::transfer::parse_file(Path::new(&req.path), req.format)?;
    if rows.is_empty() {
        return Err(AppError::invalid_input("The file has no rows."));
    }
    let engine = services::connection::engine_of(&state, &req.connection_id)?;
    let table = req.table.clone();
    let known = guard::session(&state, &req.connection_id, |ctx| async move { ctx.integration.columns(&table).await }).await?;
    if let Some(missing) = columns.iter().find(|c| !known.iter().any(|k| &k.name == *c)) {
        return Err(AppError::invalid_input(format!("Column \"{missing}\" does not exist on {}.", req.table.name)));
    }
    let batches = services::changes::insert_batches(engine, &req.table, &columns, &rows, 200);
    let statements = batches.len() as u64;
    let transactional = !matches!(engine, crate::model::Engine::Clickhouse);
    let mut script = String::new();
    if transactional {
        script.push_str("BEGIN;\n");
    }
    for b in &batches {
        script.push_str(b);
        script.push_str(";\n");
    }
    if transactional {
        script.push_str("COMMIT;");
    }
    let script_for_run = script.clone();
    guard::statement(
        &state,
        guard::StatementRequest { connection_id: &req.connection_id, sql: &script, confirm_destructive: false },
        |ctx| async move { services::query::execute(&ctx, &script_for_run, 10).await },
    )
    .await?;
    Ok(ImportReport { rows_inserted: rows.len() as u64, statements, elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX) })
}
