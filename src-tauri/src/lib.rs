// SOT: app-entry, tauri-builder, command-registration, module-tree
// Tests may panic on setup failure; production code may not (Cargo.toml [lints]).
#![cfg_attr(test, allow(clippy::panic, clippy::unwrap_used, clippy::expect_used))]

pub mod adapters;
pub mod commands;
pub mod integrations;
pub mod error;
pub mod guard;
pub mod model;
pub mod services;
pub mod state;
pub mod store;

use adapters::keyring::OsKeyring;
use state::AppState;
use store::Store;
use tauri::Manager;

// WHAT:  Builds the Tauri app: opens the local store, manages AppState, registers
//        every command.
// WHY:   Startup work is minimal on purpose (cold start budget < 150 ms): the
//        master key and integration sessions are created lazily on first use.
// WHERE: src-tauri/src/commands/mod.rs (CommandName must match this list)
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            std::fs::create_dir_all(&data_dir)?;
            let store = Store::open(&data_dir.join("db-free.sqlite"))?;
            app.manage(AppState::new(store, Box::new(OsKeyring::default())));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::connections::list_connections,
            commands::connections::save_connection,
            commands::connections::delete_connection,
            commands::connections::test_connection,
            commands::connections::connect,
            commands::connections::disconnect,
            commands::connections::active_sessions,
            commands::connections::describe_session,
            commands::schema::load_catalog,
            commands::schema::load_columns,
            commands::data::fetch_table_page,
            commands::query::execute_query,
            commands::query::list_history,
            commands::query::list_buffers,
            commands::query::save_buffer,
            commands::query::delete_buffer,
            commands::query::clear_history,
            commands::schema::load_foreign_keys,
            commands::schema::load_ddl,
            commands::settings::get_settings,
            commands::settings::save_settings,
            commands::library::list_saved_queries,
            commands::library::save_saved_query,
            commands::library::delete_saved_query,
            commands::library::list_documents,
            commands::library::save_document,
            commands::library::delete_document,
            commands::changes::preview_changes,
            commands::changes::commit_changes,
            commands::transfer::export_tables,
            commands::transfer::import_file,
            commands::ai::ai_generate,
            commands::ai::explain_query,
            commands::workflows::run_workflow,
        ])
        .run(tauri::generate_context!())?;
    Ok(())
}

// WHAT:  Writes the TS bindings for every `#[ts(export)]` type.
// HOW:   `pnpm bindings` sets TS_RS_EXPORT_DIR=../src/lib/bindings and runs this test.
#[cfg(test)]
mod export_bindings {
    use ts_rs::TS;

    // Roots only: `export_all` follows every dependency type.
    // i64/u64 map to `number` because serde_json emits plain JSON numbers.
    #[test]
    fn export_bindings() {
        let cfg = ts_rs::Config::from_env().with_large_int("number");
        let results = [
            crate::error::AppError::export_all(&cfg),
            crate::commands::CommandName::export_all(&cfg),
            crate::commands::connections::SaveConnectionRequest::export_all(&cfg),
            crate::commands::connections::ConnectionIdRequest::export_all(&cfg),
            crate::commands::connections::ConnectRequest::export_all(&cfg),
            crate::commands::connections::SessionRequest::export_all(&cfg),
            crate::commands::schema::CatalogRequest::export_all(&cfg),
            crate::commands::schema::ColumnsRequest::export_all(&cfg),
            crate::commands::data::TablePageRequest::export_all(&cfg),
            crate::commands::query::ExecuteQueryRequest::export_all(&cfg),
            crate::commands::query::HistoryRequest::export_all(&cfg),
            crate::commands::query::SaveBufferRequest::export_all(&cfg),
            crate::commands::query::BufferIdRequest::export_all(&cfg),
            crate::integrations::SessionInfo::export_all(&cfg),
            crate::model::EngineKind::export_all(&cfg),
            crate::model::FormKind::export_all(&cfg),
            crate::model::ConnectionSummary::export_all(&cfg),
            crate::model::SchemaCatalog::export_all(&cfg),
            crate::model::TablePage::export_all(&cfg),
            crate::model::QueryOutcome::export_all(&cfg),
            crate::model::HistoryEntry::export_all(&cfg),
            crate::model::EditorBuffer::export_all(&cfg),
            crate::commands::query::ClearHistoryRequest::export_all(&cfg),
            crate::commands::settings::SaveSettingsRequest::export_all(&cfg),
            crate::commands::library::SaveQueryRequest::export_all(&cfg),
            crate::commands::library::IdRequest::export_all(&cfg),
            crate::commands::library::ListDocumentsRequest::export_all(&cfg),
            crate::commands::library::SaveDocumentRequest::export_all(&cfg),
            crate::commands::changes::ChangesRequest::export_all(&cfg),
            crate::commands::transfer::ExportRequest::export_all(&cfg),
            crate::commands::transfer::ImportRequest::export_all(&cfg),
            crate::commands::ai::AiGenerateRequest::export_all(&cfg),
            crate::commands::ai::ExplainRequest::export_all(&cfg),
            crate::commands::workflows::RunWorkflowRequest::export_all(&cfg),
            crate::model::ForeignKey::export_all(&cfg),
            crate::model::ChangePreview::export_all(&cfg),
            crate::model::ExportReport::export_all(&cfg),
            crate::model::ImportReport::export_all(&cfg),
            crate::model::AiReply::export_all(&cfg),
            crate::model::PlanReport::export_all(&cfg),
            crate::model::WorkflowRunReport::export_all(&cfg),
            crate::model::AppSettings::export_all(&cfg),
            crate::model::SavedQuery::export_all(&cfg),
            crate::model::Document::export_all(&cfg),
        ];
        for result in results {
            result.unwrap_or_else(|e| panic!("{e}"));
        }
    }
}
