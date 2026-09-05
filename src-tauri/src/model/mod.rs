// SOT: model-index, shared-shapes

pub mod changes;
pub mod connection;
pub mod documents;
pub mod objects;
pub mod query;
pub mod schema;
pub mod settings;
pub mod transfer;
pub mod update;
pub mod value;

pub use connection::{
    ConnectionInput, ConnectionRecord, ConnectionSummary, Engine, EngineFacts, EngineKind, Environment, Family,
    FormKind, ResolvedConnection, SslMode,
};
pub use changes::{CellValue, ChangePreview, StagedChange};
pub use documents::{
    ConditionOp, DashboardBody, DashboardVariable, DiagramBody, DiagramColumn, DiagramRelation, DiagramTable,
    Document, DocumentBody, DocumentKind, Widget, WidgetCondition, WidgetKind, WorkflowBody, WorkflowRunReport,
    WorkflowStep, WorkflowStepResult,
};
pub use objects::{
    CodeLanguage, FacetCounts, FacetValue, ObjectAction, ObjectDetail, ObjectKind, ObjectProperty, ObjectRef, ObjectSummary,
    RangeQueryRequest, RangeResult, SearchRequest, SearchResult, Series, ServerStats, Stat, StatGroup, Tool,
    VectorSearchRequest,
};
pub use query::{
    ColumnMeta, EditorBuffer, FilterOp, FilterRule, HistoryEntry, HistoryOrigin, HistoryStatus,
    PageQuery, QueryOutcome, ResultSet, SavedQuery, SortRule, StatementResult, TablePage,
};
pub use settings::{AiProvider, AiSettings, AppSettings, ExecutionMode, RunScope};
pub use transfer::{AiReply, ExportReport, ExportedFile, ImportReport, PlanReport, TransferFormat};
pub use schema::{ColumnInfo, ForeignKey, SchemaCatalog, SchemaInfo, TableInfo, TableKind, TableRef};
pub use value::Value;
pub use update::UpdateStatus;
