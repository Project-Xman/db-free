// SOT: document-model, dashboard-model, widget-model, workflow-model, diagram-model

use serde::{Deserialize, Serialize};
use ts_rs::TS;

// WHAT:  JSON-bodied artefacts the user authors: dashboards, workflows, schema diagrams.
// WHY:   One envelope (`Document`) with a tagged body keeps persistence generic
//        while every body stays a typed, TS-exported shape.
// WHERE: src-tauri/src/store/documents.rs, src/features/{dashboards,workflows,diagrams}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum DocumentKind {
    Dashboard,
    Workflow,
    Diagram,
}

impl DocumentKind {
    pub const ALL: [DocumentKind; 3] = [DocumentKind::Dashboard, DocumentKind::Workflow, DocumentKind::Diagram];

    pub fn as_str(self) -> &'static str {
        match self {
            DocumentKind::Dashboard => "dashboard",
            DocumentKind::Workflow => "workflow",
            DocumentKind::Diagram => "diagram",
        }
    }

    pub fn parse(raw: &str) -> Option<DocumentKind> {
        DocumentKind::ALL.into_iter().find(|k| k.as_str() == raw)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Document {
    pub id: String,
    pub kind: DocumentKind,
    pub connection_id: Option<String>,
    pub name: String,
    pub body: DocumentBody,
    pub tags: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
#[ts(export)]
pub enum DocumentBody {
    Dashboard(DashboardBody),
    Workflow(WorkflowBody),
    Diagram(DiagramBody),
}

// ---------------------------------------------------------------- dashboards
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct DashboardBody {
    pub widgets: Vec<Widget>,
    pub variables: Vec<DashboardVariable>,
    #[serde(default)]
    pub refresh_seconds: u32,
}

// WHAT:  Widget kinds (DB Pro parity). `Metric` accepts the legacy "number" name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WidgetKind {
    Area,
    Line,
    Bar,
    Pie,
    Sankey,
    Table,
    #[serde(alias = "number")]
    Metric,
    Sparkline,
    Map,
    Progress,
    Text,
    Image,
    Gif,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Widget {
    pub id: String,
    pub title: String,
    pub kind: WidgetKind,
    pub sql: String,
    pub x: u32,
    pub y: u32,
    pub w: u32,
    pub h: u32,
    pub tint: Option<String>,
    #[serde(default)]
    pub show_change: bool,
    /// Progress: the value that means 100 %.
    #[serde(default)]
    pub max_value: Option<f64>,
    /// Text widgets: the body (no SQL).
    #[serde(default)]
    pub text: Option<String>,
    /// Image / GIF widgets: the source URL.
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub x_label: Option<String>,
    #[serde(default)]
    pub y_label: Option<String>,
    /// Bar: horizontal bars.
    #[serde(default)]
    pub horizontal: bool,
    /// Progress: show percentage / raw values.
    #[serde(default = "default_true")]
    pub show_percent: bool,
    #[serde(default)]
    pub show_values: bool,
    /// Map: pulse the markers.
    #[serde(default)]
    pub pulse: bool,
    /// Metric / Text / Image / GIF: evaluated against the first cell of the first row; last match wins.
    #[serde(default)]
    pub conditions: Vec<WidgetCondition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ConditionOp {
    Equals,
    NotEquals,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WidgetCondition {
    pub op: ConditionOp,
    pub value: String,
    /// Text / image URL / tint name to use when the condition matches.
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct DashboardVariable {
    pub name: String,
    pub value: String,
}

// ---------------------------------------------------------------- workflows
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WorkflowBody {
    pub steps: Vec<WorkflowStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WorkflowStep {
    pub id: String,
    pub name: String,
    /// None = the workflow's own connection.
    pub connection_id: Option<String>,
    pub sql: String,
    #[serde(default = "default_true")]
    pub stop_on_error: bool,
}

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------- diagrams (designer)
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct DiagramBody {
    pub tables: Vec<DiagramTable>,
    pub relations: Vec<DiagramRelation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct DiagramTable {
    pub id: String,
    pub name: String,
    pub x: f64,
    pub y: f64,
    pub columns: Vec<DiagramColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct DiagramColumn {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct DiagramRelation {
    pub id: String,
    pub from_table: String,
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
}

// WHAT:  Outcome of running a workflow: one entry per step in order.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WorkflowStepResult {
    pub step_id: String,
    pub name: String,
    pub ok: bool,
    pub elapsed_ms: u64,
    pub rows: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct WorkflowRunReport {
    pub steps: Vec<WorkflowStepResult>,
    pub stopped_early: bool,
}
