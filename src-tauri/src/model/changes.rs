// SOT: staged-change, pending-changes-model, cell-edit, row-insert, row-delete

use crate::model::{TableRef, Value};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

// WHAT:  One column/value pair; the primary-key parts that address a row, or the
//        values of a new row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct CellValue {
    pub column: String,
    pub value: Value,
}

// WHAT:  An edit queued in the Pending Changes panel (review mode) or applied at
//        once (direct mode). The SQL is generated on the Rust side so the UI
//        shows exactly what will run.
// WHERE: src-tauri/src/services/changes.rs (SQL generation + commit)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum StagedChange {
    #[serde(rename_all = "camelCase")]
    Update { id: String, table: TableRef, key: Vec<CellValue>, column: String, old: Value, new: Value },
    #[serde(rename_all = "camelCase")]
    Insert { id: String, table: TableRef, values: Vec<CellValue> },
    #[serde(rename_all = "camelCase")]
    Delete { id: String, table: TableRef, key: Vec<CellValue> },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ChangePreview {
    pub statements: Vec<String>,
    /// The full script including the transaction wrapper, exactly as it will run.
    pub script: String,
}
