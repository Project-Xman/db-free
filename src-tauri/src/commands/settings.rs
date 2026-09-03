// SOT: settings-commands, ipc-settings

use crate::error::AppResult;
use crate::guard;
use crate::model::AppSettings;
use crate::services;
use crate::state::AppState;
use serde::Deserialize;
use tauri::State;
use ts_rs::TS;

#[derive(Debug, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SaveSettingsRequest {
    pub settings: AppSettings,
    /// Some("") clears the sealed AI key; None keeps it; Some(key) replaces it.
    pub ai_api_key: Option<String>,
}

#[tauri::command]
pub async fn get_settings(state: State<'_, AppState>) -> AppResult<AppSettings> {
    guard::local("get_settings", async { services::settings::get(&state) }).await
}

#[tauri::command]
pub async fn save_settings(state: State<'_, AppState>, req: SaveSettingsRequest) -> AppResult<AppSettings> {
    guard::local("save_settings", async { services::settings::save(&state, &req.settings, req.ai_api_key.as_deref()) }).await
}
