// SOT: cell-value, value-model, ipc-value-shape

use serde::{Deserialize, Serialize};
use ts_rs::TS;

// WHAT:  Engine-agnostic cell value. Every integration decodes into this; the grid
//        renders from it.
// WHY:   Adjacently tagged (`t`/`v`) so TS gets a discriminated union and the
//        grid can format per kind without sniffing strings.
// HOW:   Exact numerics (NUMERIC/DECIMAL) stay as text to avoid f64 loss; binary
//        is base64; JSON is parsed so the inspector can tree-view it.
// WHERE: src/lib/format.ts (rendering), src-tauri/src/integrations/*.rs (producers)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
#[ts(export)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(String),
    Text(String),
    Bytes(String),
    Json(serde_json::Value),
    DateTime(String),
    Unsupported(String),
}
