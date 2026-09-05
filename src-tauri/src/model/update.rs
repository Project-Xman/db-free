// SOT: update-status, self-update-model

use serde::{Deserialize, Serialize};
use ts_rs::TS;

// WHAT:  What a self-update check found.
// WHY:   The UI shows the running version next to whatever the release feed
//        offers, so "you are up to date" is a statement about both.
// WHERE: src-tauri/src/services/updates.rs, src/features/settings/SettingsPage.tsx
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct UpdateStatus {
    /// Version this binary was built as.
    pub current: String,
    /// Newer version offered by the release feed, when there is one.
    pub available: Option<String>,
    /// Release notes for that version, as published.
    pub notes: Option<String>,
    /// Publication date, as published (RFC 3339).
    pub published: Option<String>,
}

// WHAT:  Download progress for an update being installed.
// WHY:   The download is tens of megabytes; a button that just sits there looks
//        like a hang. Emitted on the "update:progress" event as bytes arrive.
// WHERE: src-tauri/src/commands/updates.rs, src/lib/ipc.ts (onUpdateProgress)
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct UpdateProgress {
    pub downloaded: u64,
    /// None when the server does not send a content length.
    pub total: Option<u64>,
    /// The download finished; installation follows, then a restart.
    pub done: bool,
}
