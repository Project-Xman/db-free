// SOT: result-set, statement-result, query-outcome, table-page, page-query, sort-rule, filter-rule, filter-op, history-entry, history-origin, saved-query, editor-buffer

use crate::model::schema::ColumnInfo;
use crate::model::value::Value;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ColumnMeta {
    pub name: String,
    pub type_name: String,
}

// WHAT:  Rows returned by one statement. `truncated` is set when the row cap hit.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ResultSet {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<Value>>,
    pub truncated: bool,
}

impl ResultSet {
    pub fn row_count(&self) -> u64 {
        self.rows.len() as u64
    }
}

// WHAT:  Comparison operators the GUI filter builder offers (PRD §4.2).
// HOW:   `needs_value()` tells the UI which operators take an input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    StartsWith,
    EndsWith,
    In,
    IsNull,
    IsNotNull,
}

impl FilterOp {
    pub fn needs_value(self) -> bool {
        !matches!(self, FilterOp::IsNull | FilterOp::IsNotNull)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct FilterRule {
    pub column: String,
    pub op: FilterOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SortRule {
    pub column: String,
    pub desc: bool,
}

// WHAT:  Everything the browser needs to ask for one page: sort, filters, window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct PageQuery {
    pub sort: Vec<SortRule>,
    pub filters: Vec<FilterRule>,
    pub offset: u64,
    pub limit: u32,
}

// WHAT:  One page of a table for the grid.
// HOW:   `total` is exact when filters are applied (a count query runs), otherwise
//        the engine's cheap estimate; `total_exact` says which.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct TablePage {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<Value>>,
    pub offset: u64,
    pub total: Option<i64>,
    pub total_exact: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum StatementResult {
    #[serde(rename_all = "camelCase")]
    Rows { result: ResultSet },
    #[serde(rename_all = "camelCase")]
    Affected { rows_affected: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct QueryOutcome {
    pub statements: Vec<StatementResult>,
    pub elapsed_ms: u64,
}

impl QueryOutcome {
    pub fn total_rows(&self) -> u64 {
        self.statements
            .iter()
            .map(|s| match s {
                StatementResult::Rows { result } => result.row_count(),
                StatementResult::Affected { rows_affected } => *rows_affected,
            })
            .sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum HistoryStatus {
    Ok,
    Error,
}

impl HistoryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            HistoryStatus::Ok => "ok",
            HistoryStatus::Error => "error",
        }
    }

    pub fn parse(raw: &str) -> HistoryStatus {
        if raw == "ok" {
            HistoryStatus::Ok
        } else {
            HistoryStatus::Error
        }
    }
}

// WHAT:  Who issued a logged statement: the user (editor) or the app itself
//        (table pages, catalog probes). The history tab filters on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum HistoryOrigin {
    User,
    System,
}

impl HistoryOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            HistoryOrigin::User => "user",
            HistoryOrigin::System => "system",
        }
    }

    pub fn parse(raw: &str) -> HistoryOrigin {
        if raw == "system" {
            HistoryOrigin::System
        } else {
            HistoryOrigin::User
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct HistoryEntry {
    pub id: i64,
    pub connection_id: String,
    pub sql: String,
    pub status: HistoryStatus,
    pub origin: HistoryOrigin,
    pub error: Option<String>,
    pub elapsed_ms: u64,
    pub row_count: Option<u64>,
    pub executed_at: String,
}

// WHAT:  An editor tab's unsaved text, persisted so restarts never lose work.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct EditorBuffer {
    pub id: String,
    pub connection_id: Option<String>,
    pub title: String,
    pub content: String,
    pub updated_at: String,
}

// WHAT:  A named, reusable query. `connection_id` None = available everywhere.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SavedQuery {
    pub id: String,
    pub connection_id: Option<String>,
    pub name: String,
    pub sql: String,
    pub tags: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}
