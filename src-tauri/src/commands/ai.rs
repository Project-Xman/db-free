// SOT: ai-commands, ipc-ai, ipc-explain

use crate::error::AppResult;
use crate::guard;
use crate::model::{AiReply, PlanReport};
use crate::services;
use crate::services::ai::AiRequest;
use crate::state::AppState;
use serde::{Deserialize, Serialize};
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct AiGenerateRequest {
    pub connection_id: String,
    pub prompt: String,
    #[serde(default)]
    pub current_query: Option<String>,
    #[serde(default)]
    pub current_table: Option<String>,
    #[serde(default)]
    pub error_context: Option<String>,
    #[serde(default)]
    pub conversation_history: Option<Vec<ChatMessage>>,
}

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ExplainRequest {
    pub connection_id: String,
    pub sql: String,
}

#[tauri::command]
pub async fn ai_generate(state: State<'_, AppState>, req: AiGenerateRequest) -> AppResult<AiReply> {
    let settings = services::settings::get(&state)?;
    let api_key = services::settings::ai_api_key(&state)?;
    guard::session(&state, &req.connection_id, |ctx| async move {
        services::ai::generate(
            &ctx,
            &AiRequest { settings: &settings.ai, api_key: api_key.as_deref() },
            &req.prompt,
            req.current_query.as_deref(),
            req.current_table.as_deref(),
            req.error_context.as_deref(),
            req.conversation_history.as_deref(),
        )
        .await
    })
    .await
}

#[tauri::command]
pub async fn explain_query(state: State<'_, AppState>, req: ExplainRequest) -> AppResult<PlanReport> {
    let settings = services::settings::get(&state)?;
    let api_key = services::settings::ai_api_key(&state)?;
    guard::session(&state, &req.connection_id, |ctx| async move {
        services::ai::explain(&ctx, &AiRequest { settings: &settings.ai, api_key: api_key.as_deref() }, &req.sql, 500).await
    })
    .await
}
