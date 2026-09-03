// SOT: command-registry, ipc-command-names, command-index

use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub mod ai;
pub mod changes;
pub mod connections;
pub mod data;
pub mod library;
pub mod query;
pub mod schema;
pub mod settings;
pub mod transfer;
pub mod workflows;

// WHAT:  Every IPC command name, as one enum.
// WHY:   Exported to TS so `src/lib/ipc.ts` must declare a signature for each —
//        adding a command here without wiring it there is a TS compile error,
//        and the test below fails if it is not registered in lib.rs.
// HOW:   serde snake_case matches the `#[tauri::command]` function names.
// WHERE: src-tauri/src/lib.rs (generate_handler!), src/lib/ipc.ts (CommandMap)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CommandName {
    ListConnections,
    SaveConnection,
    DeleteConnection,
    TestConnection,
    Connect,
    Disconnect,
    ActiveSessions,
    DescribeSession,
    LoadCatalog,
    LoadColumns,
    FetchTablePage,
    ExecuteQuery,
    ListHistory,
    ListBuffers,
    SaveBuffer,
    DeleteBuffer,
    ClearHistory,
    LoadForeignKeys,
    LoadDdl,
    GetSettings,
    SaveSettings,
    ListSavedQueries,
    SaveSavedQuery,
    DeleteSavedQuery,
    ListDocuments,
    SaveDocument,
    DeleteDocument,
    PreviewChanges,
    CommitChanges,
    ExportTables,
    ImportFile,
    AiGenerate,
    ExplainQuery,
    RunWorkflow,
}

impl CommandName {
    pub const ALL: [CommandName; 34] = [
        CommandName::ListConnections,
        CommandName::SaveConnection,
        CommandName::DeleteConnection,
        CommandName::TestConnection,
        CommandName::Connect,
        CommandName::Disconnect,
        CommandName::ActiveSessions,
        CommandName::DescribeSession,
        CommandName::LoadCatalog,
        CommandName::LoadColumns,
        CommandName::FetchTablePage,
        CommandName::ExecuteQuery,
        CommandName::ListHistory,
        CommandName::ListBuffers,
        CommandName::SaveBuffer,
        CommandName::DeleteBuffer,
        CommandName::ClearHistory,
        CommandName::LoadForeignKeys,
        CommandName::LoadDdl,
        CommandName::GetSettings,
        CommandName::SaveSettings,
        CommandName::ListSavedQueries,
        CommandName::SaveSavedQuery,
        CommandName::DeleteSavedQuery,
        CommandName::ListDocuments,
        CommandName::SaveDocument,
        CommandName::DeleteDocument,
        CommandName::PreviewChanges,
        CommandName::CommitChanges,
        CommandName::ExportTables,
        CommandName::ImportFile,
        CommandName::AiGenerate,
        CommandName::ExplainQuery,
        CommandName::RunWorkflow,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            CommandName::ListConnections => "list_connections",
            CommandName::SaveConnection => "save_connection",
            CommandName::DeleteConnection => "delete_connection",
            CommandName::TestConnection => "test_connection",
            CommandName::Connect => "connect",
            CommandName::Disconnect => "disconnect",
            CommandName::ActiveSessions => "active_sessions",
            CommandName::DescribeSession => "describe_session",
            CommandName::LoadCatalog => "load_catalog",
            CommandName::LoadColumns => "load_columns",
            CommandName::FetchTablePage => "fetch_table_page",
            CommandName::ExecuteQuery => "execute_query",
            CommandName::ListHistory => "list_history",
            CommandName::ListBuffers => "list_buffers",
            CommandName::SaveBuffer => "save_buffer",
            CommandName::DeleteBuffer => "delete_buffer",
            CommandName::ClearHistory => "clear_history",
            CommandName::LoadForeignKeys => "load_foreign_keys",
            CommandName::LoadDdl => "load_ddl",
            CommandName::GetSettings => "get_settings",
            CommandName::SaveSettings => "save_settings",
            CommandName::ListSavedQueries => "list_saved_queries",
            CommandName::SaveSavedQuery => "save_saved_query",
            CommandName::DeleteSavedQuery => "delete_saved_query",
            CommandName::ListDocuments => "list_documents",
            CommandName::SaveDocument => "save_document",
            CommandName::DeleteDocument => "delete_document",
            CommandName::PreviewChanges => "preview_changes",
            CommandName::CommitChanges => "commit_changes",
            CommandName::ExportTables => "export_tables",
            CommandName::ImportFile => "import_file",
            CommandName::AiGenerate => "ai_generate",
            CommandName::ExplainQuery => "explain_query",
            CommandName::RunWorkflow => "run_workflow",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_command_is_registered_in_lib_rs() {
        let lib = include_str!("../lib.rs");
        for name in CommandName::ALL {
            let needle = format!("commands::{}::{},", module_of(name), name.as_str());
            assert!(lib.contains(&needle), "{needle} missing from generate_handler! in lib.rs");
        }
    }

    #[test]
    fn serde_names_match_as_str() {
        for name in CommandName::ALL {
            let json = serde_json::to_string(&name).unwrap_or_default();
            assert_eq!(json, format!("\"{}\"", name.as_str()));
        }
    }

    fn module_of(name: CommandName) -> &'static str {
        match name {
            CommandName::ListConnections
            | CommandName::SaveConnection
            | CommandName::DeleteConnection
            | CommandName::TestConnection
            | CommandName::Connect
            | CommandName::Disconnect
            | CommandName::ActiveSessions
            | CommandName::DescribeSession => "connections",
            CommandName::LoadCatalog | CommandName::LoadColumns | CommandName::LoadForeignKeys | CommandName::LoadDdl => "schema",
            CommandName::FetchTablePage => "data",
            CommandName::ExecuteQuery
            | CommandName::ListHistory
            | CommandName::ClearHistory
            | CommandName::ListBuffers
            | CommandName::SaveBuffer
            | CommandName::DeleteBuffer => "query",
            CommandName::GetSettings | CommandName::SaveSettings => "settings",
            CommandName::ListSavedQueries
            | CommandName::SaveSavedQuery
            | CommandName::DeleteSavedQuery
            | CommandName::ListDocuments
            | CommandName::SaveDocument
            | CommandName::DeleteDocument => "library",
            CommandName::PreviewChanges | CommandName::CommitChanges => "changes",
            CommandName::ExportTables | CommandName::ImportFile => "transfer",
            CommandName::AiGenerate | CommandName::ExplainQuery => "ai",
            CommandName::RunWorkflow => "workflows",
        }
    }
}
