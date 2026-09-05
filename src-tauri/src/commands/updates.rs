// SOT: update-commands, ipc-self-update, updater-endpoint

use crate::error::{AppError, AppResult};
use crate::guard;
use crate::model::UpdateStatus;
use tauri::AppHandle;
use tauri_plugin_updater::UpdaterExt;

// WHAT:  Self-update: what the signed release feed offers, and installing it.
// WHY:   Installers were download-by-hand; the feed is the latest.json the
//        release workflow publishes next to every bundle.
// HOW:   The plugin verifies each release against the public key in
//        tauri.conf.json before anything is offered, so a compromised endpoint
//        cannot serve an unsigned build. This lives in the command layer rather
//        than in a service because it orchestrates neither store nor
//        integrations — it is the Tauri runtime itself, which services may not
//        import (scripts/guardrail.py).
// WHERE: .github/workflows/release.yml (publishes latest.json), tauri.conf.json
#[tauri::command]
pub async fn check_update(app: AppHandle) -> AppResult<UpdateStatus> {
    guard::local("check_update", async move {
        let current = app.package_info().version.to_string();
        let updater = app.updater().map_err(|e| AppError::internal(format!("updater unavailable: {e}")))?;
        let found = updater.check().await.map_err(|e| AppError::internal(format!("update check failed: {e}")))?;
        Ok(match found {
            Some(update) => UpdateStatus {
                current,
                available: Some(update.version.clone()),
                notes: update.body.clone(),
                published: update.date.map(|d| d.to_string()),
            },
            None => UpdateStatus { current, available: None, notes: None, published: None },
        })
    })
    .await
}

#[tauri::command]
pub async fn install_update(app: AppHandle) -> AppResult<()> {
    guard::local("install_update", async move {
        let updater = app.updater().map_err(|e| AppError::internal(format!("updater unavailable: {e}")))?;
        let update = updater
            .check()
            .await
            .map_err(|e| AppError::internal(format!("update check failed: {e}")))?
            .ok_or_else(|| AppError::not_found("no update available"))?;
        update
            .download_and_install(|_chunk, _total| {}, || {})
            .await
            .map_err(|e| AppError::internal(format!("update install failed: {e}")))?;
        // Restarting is what makes the installed binary the running one, so it
        // belongs here rather than in something the UI must remember to call.
        app.restart();
    })
    .await
}
