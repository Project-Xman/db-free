// SOT: workflow-commands, ipc-run-workflow, workflow-runner

use crate::error::{AppError, AppResult};
use crate::guard;
use crate::model::{DocumentBody, WorkflowRunReport, WorkflowStepResult};
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct RunWorkflowRequest {
    pub id: String,
}

// WHAT:  Runs a workflow's steps in order, each through the statement guard on
//        its own connection (falling back to the workflow's connection).
// WHY:   Orchestration across several guarded statements lives in the command
//        layer, like a router coordinating service calls.
#[tauri::command]
pub async fn run_workflow(state: State<'_, AppState>, req: RunWorkflowRequest) -> AppResult<WorkflowRunReport> {
    let doc = services::documents::get(&state, &req.id)?;
    let DocumentBody::Workflow(body) = &doc.body else {
        return Err(AppError::invalid_input("Document is not a workflow."));
    };
    let mut steps = Vec::with_capacity(body.steps.len());
    let mut stopped_early = false;
    for step in &body.steps {
        let Some(connection_id) = step.connection_id.clone().or_else(|| doc.connection_id.clone()) else {
            steps.push(WorkflowStepResult { step_id: step.id.clone(), name: step.name.clone(), ok: false, elapsed_ms: 0, rows: None, error: Some("No connection set for this step.".into()) });
            if step.stop_on_error {
                stopped_early = true;
                break;
            }
            continue;
        };
        let sql = step.sql.clone();
        let outcome = guard::statement(
            &state,
            guard::StatementRequest { connection_id: &connection_id, sql: &step.sql, confirm_destructive: true },
            |ctx| async move { services::query::execute(&ctx, &sql, 1_000).await },
        )
        .await;
        match outcome {
            Ok(o) => steps.push(WorkflowStepResult { step_id: step.id.clone(), name: step.name.clone(), ok: true, elapsed_ms: o.elapsed_ms, rows: Some(o.total_rows()), error: None }),
            Err(err) => {
                steps.push(WorkflowStepResult { step_id: step.id.clone(), name: step.name.clone(), ok: false, elapsed_ms: 0, rows: None, error: Some(err.message().to_string()) });
                if step.stop_on_error {
                    stopped_early = true;
                    break;
                }
            }
        }
    }
    Ok(WorkflowRunReport { steps, stopped_early })
}
