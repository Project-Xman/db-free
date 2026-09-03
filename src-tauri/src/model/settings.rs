// SOT: app-settings, settings-model, ai-settings, execution-mode, run-scope

use serde::{Deserialize, Serialize};
use ts_rs::TS;

// WHAT:  Every user preference, as one typed document (stored as JSON under the
//        settings key "app"). Unknown fields are ignored; missing ones default.
// WHY:   The settings page edits this struct directly; the guard and the UI read
//        the same source (execution mode decides review vs direct edits).
// WHERE: src-tauri/src/services/settings.rs, src/features/settings/SettingsPage.tsx
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionMode {
    /// Edits queue in the Pending Changes panel until Commit.
    Review,
    /// Edits apply immediately.
    Direct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum RunScope {
    /// Run every statement in the editor when nothing is selected.
    All,
    /// Run only the statement under the cursor.
    Current,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AiProvider {
    None,
    Anthropic,
    Openai,
    Openrouter,
    Ollama,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct AiSettings {
    pub provider: AiProvider,
    pub model: String,
    /// Override for self-hosted / proxy endpoints (Ollama defaults to http://127.0.0.1:11434).
    pub base_url: Option<String>,
    /// Never contains the key itself; the key is sealed separately in the store.
    #[serde(default)]
    pub has_api_key: bool,
}

impl Default for AiSettings {
    fn default() -> Self {
        AiSettings { provider: AiProvider::None, model: "claude-opus-5".to_string(), base_url: None, has_api_key: false }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", default)]
#[ts(export)]
pub struct AppSettings {
    pub accent: String,
    pub ui_font_size: u8,
    pub editor_font_size: u8,
    pub grid_density: String,
    pub null_display: String,
    pub show_results_pane: bool,
    pub condense_sql_when_formatting: bool,
    pub run_scope: RunScope,
    pub execution_mode: ExecutionMode,
    pub command_menu_sections: Vec<String>,
    pub inspector_tabs: Vec<String>,
    pub confirm_destructive: bool,
    pub crash_reports_opt_in: bool,
    pub ai: AiSettings,
}

impl Default for AppSettings {
    fn default() -> Self {
        AppSettings {
            accent: "blue".to_string(),
            ui_font_size: 13,
            editor_font_size: 13,
            grid_density: "cozy".to_string(),
            null_display: "NULL".to_string(),
            show_results_pane: true,
            condense_sql_when_formatting: false,
            run_scope: RunScope::All,
            execution_mode: ExecutionMode::Review,
            command_menu_sections: ["create", "navigation", "connections", "tables", "saved_queries", "dashboards", "workflows", "diagrams", "settings"]
                .into_iter()
                .map(String::from)
                .collect(),
            inspector_tabs: ["fields", "json", "sql"].into_iter().map(String::from).collect(),
            confirm_destructive: true,
            crash_reports_opt_in: false,
            ai: AiSettings::default(),
        }
    }
}
