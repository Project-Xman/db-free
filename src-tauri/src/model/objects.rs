// SOT: object-kind, object-ref, object-summary, object-detail, object-action, code-language, server-stats, engine-tool, vector-search-request, search-request, range-query, ledger-history-request

use crate::model::{ColumnInfo, ResultSet};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use ts_rs::TS;

// ============================================================================
// OBJECT REGISTRY
//
// WHAT:  Every first-class thing a database exposes beyond rows: schemas,
//        views, functions, triggers, indexes, roles, sessions, topics, streams,
//        labels, buckets, metrics, snapshots… One enum for every family.
// WHY:   The object explorer, the admin page and the capability matrix are
//        generic: an adapter declares which kinds it has (`FamilyProfile`) and
//        answers `objects(kind, parent)` / `object_detail(ref)`. Adding a kind
//        here fails `src/lib/objects.ts` (`satisfies Record<ObjectKind, …>`)
//        until the UI has a label and an icon for it.
// HOW:   Kinds are engine-neutral; the engine-specific meaning lives in the
//        summary `badge` / detail `properties` (a Postgres `Type` is an enum or
//        a domain, a Snowflake `Stream` is a CDC stream, a Redis `Stream` is
//        the data type).
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile, Integration::objects)
// ============================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ObjectKind {
    // ---- containers
    Database,
    Schema,
    Keyspace,
    Namespace,
    Bucket,
    Dataset,
    ColumnFamily,
    // ---- storage structures
    Table,
    View,
    MaterializedView,
    ForeignTable,
    VirtualTable,
    Partition,
    Collection,
    EdgeCollection,
    Graph,
    Label,
    RelationshipType,
    Class,
    Measurement,
    Metric,
    Topic,
    Stream,
    Channel,
    ConsumerGroup,
    Document,
    Index,
    Constraint,
    /// One column / property / attribute of a table, collection or class.
    Field,
    Sequence,
    Type,
    Dictionary,
    Projection,
    Alias,
    Snapshot,
    Prefix,
    // ---- code
    Function,
    Procedure,
    Aggregate,
    Trigger,
    Rule,
    Event,
    Macro,
    Package,
    Script,
    Task,
    Job,
    Pipeline,
    Analyzer,
    Synonym,
    Template,
    RecordingRule,
    AlertRule,
    // ---- security
    User,
    Role,
    Grant,
    Policy,
    ApiKey,
    Acl,
    // ---- server
    Session,
    Lock,
    Transaction,
    Replica,
    Node,
    Shard,
    Setting,
    Extension,
    Tablespace,
    Publication,
    Subscription,
    ReplicationSlot,
    ForeignServer,
    ForeignDataWrapper,
    Warehouse,
    Stage,
    Quota,
    SlowQuery,
    Target,
    Alert,
    Service,
    Backup,
}

impl ObjectKind {
    pub const ALL: [ObjectKind; 81] = [
        ObjectKind::Database,
        ObjectKind::Schema,
        ObjectKind::Keyspace,
        ObjectKind::Namespace,
        ObjectKind::Bucket,
        ObjectKind::Dataset,
        ObjectKind::ColumnFamily,
        ObjectKind::Table,
        ObjectKind::View,
        ObjectKind::MaterializedView,
        ObjectKind::ForeignTable,
        ObjectKind::VirtualTable,
        ObjectKind::Partition,
        ObjectKind::Collection,
        ObjectKind::EdgeCollection,
        ObjectKind::Graph,
        ObjectKind::Label,
        ObjectKind::RelationshipType,
        ObjectKind::Class,
        ObjectKind::Measurement,
        ObjectKind::Metric,
        ObjectKind::Topic,
        ObjectKind::Stream,
        ObjectKind::Channel,
        ObjectKind::ConsumerGroup,
        ObjectKind::Document,
        ObjectKind::Index,
        ObjectKind::Constraint,
        ObjectKind::Field,
        ObjectKind::Sequence,
        ObjectKind::Type,
        ObjectKind::Dictionary,
        ObjectKind::Projection,
        ObjectKind::Alias,
        ObjectKind::Snapshot,
        ObjectKind::Prefix,
        ObjectKind::Function,
        ObjectKind::Procedure,
        ObjectKind::Aggregate,
        ObjectKind::Trigger,
        ObjectKind::Rule,
        ObjectKind::Event,
        ObjectKind::Macro,
        ObjectKind::Package,
        ObjectKind::Script,
        ObjectKind::Task,
        ObjectKind::Job,
        ObjectKind::Pipeline,
        ObjectKind::Analyzer,
        ObjectKind::Synonym,
        ObjectKind::Template,
        ObjectKind::RecordingRule,
        ObjectKind::AlertRule,
        ObjectKind::User,
        ObjectKind::Role,
        ObjectKind::Grant,
        ObjectKind::Policy,
        ObjectKind::ApiKey,
        ObjectKind::Acl,
        ObjectKind::Session,
        ObjectKind::Lock,
        ObjectKind::Transaction,
        ObjectKind::Replica,
        ObjectKind::Node,
        ObjectKind::Shard,
        ObjectKind::Setting,
        ObjectKind::Extension,
        ObjectKind::Tablespace,
        ObjectKind::Publication,
        ObjectKind::Subscription,
        ObjectKind::ReplicationSlot,
        ObjectKind::ForeignServer,
        ObjectKind::ForeignDataWrapper,
        ObjectKind::Warehouse,
        ObjectKind::Stage,
        ObjectKind::Quota,
        ObjectKind::SlowQuery,
        ObjectKind::Target,
        ObjectKind::Alert,
        ObjectKind::Service,
        ObjectKind::Backup,
    ];

    // WHAT:  Kinds listed per namespace (the sidebar asks for them with the
    //        current schema / keyspace / database as `parent`). Everything else
    //        is listed once for the whole server.
    pub fn scoped(self) -> bool {
        matches!(
            self,
            ObjectKind::Table
                | ObjectKind::View
                | ObjectKind::MaterializedView
                | ObjectKind::ForeignTable
                | ObjectKind::VirtualTable
                | ObjectKind::Partition
                | ObjectKind::Collection
                | ObjectKind::EdgeCollection
                | ObjectKind::Index
                | ObjectKind::Constraint
                | ObjectKind::Field
                | ObjectKind::Sequence
                | ObjectKind::Type
                | ObjectKind::Function
                | ObjectKind::Procedure
                | ObjectKind::Aggregate
                | ObjectKind::Trigger
                | ObjectKind::Rule
                | ObjectKind::Event
                | ObjectKind::Macro
                | ObjectKind::Package
                | ObjectKind::Policy
                | ObjectKind::Measurement
                | ObjectKind::Document
        )
    }
}

// WHAT:  Playground-style tools that need their own tab, beyond the generic
//        grid / query / object views. Declared per family in `FamilyProfile`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum Tool {
    /// Server overview: `Integration::server_stats` groups, auto-refreshing.
    Stats,
    /// Existing per-key inspector for key-value engines.
    KeyBrowser,
    /// Existing auto ER diagram from foreign keys.
    Erd,
    /// Similarity search: vector + top-k + payload filter → scored hits.
    VectorSearch,
    /// Full-text search: query + filters + facets + highlights.
    SearchPlayground,
    /// Range query over a time window with a step → chart.
    MetricsExplorer,
    /// Topic / partition / offset message browser with a producer.
    MessageViewer,
    /// Stage-by-stage aggregation pipeline editor.
    PipelineBuilder,
    /// Immutable history of a key / row with transaction ids and proofs.
    LedgerHistory,
    /// Node / relationship rendering of query results.
    GraphView,
    /// Publish to channels and list active subscriptions.
    PubSub,
    /// Tree view of XML documents.
    XmlViewer,
}

impl Tool {
    pub const ALL: [Tool; 12] = [
        Tool::Stats,
        Tool::KeyBrowser,
        Tool::Erd,
        Tool::VectorSearch,
        Tool::SearchPlayground,
        Tool::MetricsExplorer,
        Tool::MessageViewer,
        Tool::PipelineBuilder,
        Tool::LedgerHistory,
        Tool::GraphView,
        Tool::PubSub,
        Tool::XmlViewer,
    ];
}

// WHAT:  Identifies one object. `parent` is the namespace for scoped kinds
//        (schema, keyspace, database, bucket) or the owning object for nested
//        ones (the table of an index, the topic of a consumer group).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ObjectRef {
    pub kind: ObjectKind,
    pub name: String,
    pub parent: Option<String>,
}

// WHAT:  One row of an object list: enough for the sidebar and admin tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ObjectSummary {
    pub reference: ObjectRef,
    /// Short secondary text: signature, owner, row count, state, TTL…
    pub detail: Option<String>,
    /// Engine-specific subtype shown as a chip: `enum`, `btree`, `gsi`, `idle`, `leader`…
    pub badge: Option<String>,
}

impl ObjectSummary {
    pub fn new(kind: ObjectKind, name: impl Into<String>, parent: Option<String>) -> ObjectSummary {
        ObjectSummary { reference: ObjectRef { kind, name: name.into(), parent }, detail: None, badge: None }
    }
    pub fn with_detail(mut self, detail: impl Into<String>) -> ObjectSummary {
        self.detail = Some(detail.into());
        self
    }
    pub fn with_badge(mut self, badge: impl Into<String>) -> ObjectSummary {
        self.badge = Some(badge.into());
        self
    }
}

// WHAT:  Syntax the definition text is in (drives highlighting in the UI).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CodeLanguage {
    Sql,
    Json,
    Xml,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ObjectProperty {
    pub name: String,
    pub value: String,
}

// WHAT:  Something the user can do to the object. `statement` is in the
//        engine's native command language and runs through `execute_query`,
//        so the read-only lock, the destructive confirmation and the history
//        log all apply without the adapter doing anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ObjectAction {
    pub id: String,
    pub label: String,
    pub destructive: bool,
    pub statement: String,
}

impl ObjectAction {
    pub fn new(id: &str, label: &str, statement: impl Into<String>) -> ObjectAction {
        ObjectAction { id: id.to_string(), label: label.to_string(), destructive: false, statement: statement.into() }
    }
    pub fn destructive(id: &str, label: &str, statement: impl Into<String>) -> ObjectAction {
        ObjectAction { id: id.to_string(), label: label.to_string(), destructive: true, statement: statement.into() }
    }
}

// WHAT:  Everything the object tab shows: definition source, a property
//        sheet, an optional column list (table-like kinds), an optional
//        tabular payload (partitions, members, entries, mapping fields),
//        nested objects (a table's indexes, a topic's consumer groups) and
//        the actions available.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ObjectDetail {
    pub reference: ObjectRef,
    pub definition: Option<String>,
    pub language: CodeLanguage,
    pub properties: Vec<ObjectProperty>,
    pub columns: Vec<ColumnInfo>,
    pub rows: Option<ResultSet>,
    pub children: Vec<ObjectSummary>,
    pub actions: Vec<ObjectAction>,
}

impl ObjectDetail {
    pub fn empty(reference: &ObjectRef) -> ObjectDetail {
        ObjectDetail {
            reference: reference.clone(),
            definition: None,
            language: CodeLanguage::Text,
            properties: Vec::new(),
            columns: Vec::new(),
            rows: None,
            children: Vec::new(),
            actions: Vec::new(),
        }
    }
    pub fn definition(mut self, text: impl Into<String>, language: CodeLanguage) -> ObjectDetail {
        self.definition = Some(text.into());
        self.language = language;
        self
    }
    pub fn property(mut self, name: &str, value: impl Into<String>) -> ObjectDetail {
        self.properties.push(ObjectProperty { name: name.to_string(), value: value.into() });
        self
    }
    pub fn action(mut self, action: ObjectAction) -> ObjectDetail {
        self.actions.push(action);
        self
    }
}

// WHAT:  One monitoring figure. `numeric` lets the UI draw a sparkline across
//        refreshes; `value` is the display text (already formatted by the adapter).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Stat {
    pub label: String,
    pub value: String,
    pub unit: Option<String>,
    pub hint: Option<String>,
    pub numeric: Option<f64>,
}

impl Stat {
    pub fn text(label: &str, value: impl Into<String>) -> Stat {
        Stat { label: label.to_string(), value: value.into(), unit: None, hint: None, numeric: None }
    }
    pub fn number(label: &str, value: f64, unit: Option<&str>) -> Stat {
        Stat { label: label.to_string(), value: format_number(value), unit: unit.map(str::to_string), hint: None, numeric: Some(value) }
    }
    pub fn with_hint(mut self, hint: impl Into<String>) -> Stat {
        self.hint = Some(hint.into());
        self
    }
}

// WHAT:  Grouped server statistics (Connections / Memory / Cache / Replication …).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct StatGroup {
    pub title: String,
    pub stats: Vec<Stat>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ServerStats {
    pub groups: Vec<StatGroup>,
    /// ISO-8601 timestamp the figures were read at.
    pub collected_at: String,
}

impl ServerStats {
    pub fn now(groups: Vec<StatGroup>) -> ServerStats {
        ServerStats { groups, collected_at: chrono::Utc::now().to_rfc3339() }
    }
}

// WHAT:  Plain `1234567.5` → `1,234,567.5`; integers lose the fraction.
pub fn format_number(value: f64) -> String {
    let whole = value.trunc().abs() as u64;
    let mut digits = whole.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    while digits.len() > 3 {
        let tail = digits.split_off(digits.len() - 3);
        grouped = if grouped.is_empty() { tail } else { format!("{tail},{grouped}") };
    }
    grouped = if grouped.is_empty() { digits } else { format!("{digits},{grouped}") };
    let sign = if value < 0.0 { "-" } else { "" };
    let fraction = value.fract().abs();
    if fraction < f64::EPSILON {
        format!("{sign}{grouped}")
    } else {
        format!("{sign}{grouped}.{}", format!("{fraction:.2}").trim_start_matches("0.").trim_end_matches('0'))
    }
}

// ---- playground requests ----------------------------------------------------

// WHAT:  Similarity search against one collection. `vector` is the query
//        embedding; `filter` is the engine's native payload filter (JSON).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct VectorSearchRequest {
    pub collection: String,
    pub vector: Vec<f64>,
    /// Named vector / field to search (engines with several per point).
    pub vector_name: Option<String>,
    pub top_k: u32,
    pub filter: Option<JsonValue>,
    pub include_vectors: bool,
}

// WHAT:  Full-text search against one index: free text plus the engine's
//        native filter syntax, optional facets and highlighting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SearchRequest {
    pub index: String,
    pub query: String,
    pub filter: Option<String>,
    pub facets: Vec<String>,
    pub sort: Vec<String>,
    pub highlight: bool,
    pub limit: u32,
    pub offset: u32,
}

// WHAT:  Hits plus per-facet counts, so the playground can render both.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SearchResult {
    pub hits: ResultSet,
    pub total: Option<u64>,
    pub facets: Vec<FacetCounts>,
    pub took_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct FacetCounts {
    pub field: String,
    pub values: Vec<FacetValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct FacetValue {
    pub value: String,
    pub count: u64,
}

// WHAT:  Range query for metrics engines: expression over [start, end] with a
//        step, all in seconds since the epoch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct RangeQueryRequest {
    pub query: String,
    pub start: f64,
    pub end: f64,
    pub step_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Series {
    /// Label set / tag set identifying the series, as `key=value` text.
    pub name: String,
    pub labels: Vec<ObjectProperty>,
    /// `[timestamp_seconds, value]` pairs, ascending.
    pub points: Vec<[f64; 2]>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct RangeResult {
    pub series: Vec<Series>,
    pub warnings: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_lists_every_kind_once() {
        let mut seen = std::collections::HashSet::new();
        for kind in ObjectKind::ALL {
            assert!(seen.insert(kind), "{kind:?} listed twice");
        }
        assert_eq!(seen.len(), ObjectKind::ALL.len());
    }

    #[test]
    fn numbers_group_thousands() {
        assert_eq!(format_number(0.0), "0");
        assert_eq!(format_number(999.0), "999");
        assert_eq!(format_number(1234567.0), "1,234,567");
        assert_eq!(format_number(1234.5), "1,234.5");
        assert_eq!(format_number(-42.25), "-42.25");
    }

    #[test]
    fn detail_builders_accumulate() {
        let r = ObjectRef { kind: ObjectKind::Table, name: "t".into(), parent: Some("public".into()) };
        let d = ObjectDetail::empty(&r)
            .definition("CREATE TABLE t ()", CodeLanguage::Sql)
            .property("owner", "postgres")
            .action(ObjectAction::destructive("drop", "Drop", "DROP TABLE t"));
        assert_eq!(d.language, CodeLanguage::Sql);
        assert_eq!(d.properties.len(), 1);
        assert!(d.actions[0].destructive);
    }
}
