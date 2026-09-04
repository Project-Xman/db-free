// SOT: object-registry, object-kind-labels, object-kind-icons, object-sections, tool-registry, family-profile-lookup, admin-kinds
import type { Engine, FamilyProfile, ObjectKind, Tool } from "./bindings";
import { ENGINE_FACTS } from "./bindings/EngineFacts.gen";
import { FAMILY_PROFILES, SCOPED_OBJECT_KINDS } from "./bindings/FamilyProfiles.gen";
import type { IconName } from "./icons";

// WHAT:  UI metadata per object kind, keyed by the Rust `ObjectKind` enum.
// WHY:   `satisfies Record<ObjectKind, …>` fails the build when Rust gains a
//        kind this file does not label — the registry pattern, not a lookup
//        with a fallback.
// WHERE: src-tauri/src/model/objects.rs (ObjectKind), FamilyProfiles.gen.ts
export type ObjectSection = "containers" | "structure" | "code" | "security" | "server";

export interface ObjectKindMeta {
  label: string;
  plural: string;
  icon: IconName;
  section: ObjectSection;
}

const k = (label: string, plural: string, icon: IconName, section: ObjectSection): ObjectKindMeta => ({ label, plural, icon, section });

export const OBJECT_KINDS = {
  // ---- containers
  database: k("Database", "Databases", "database", "containers"),
  schema: k("Schema", "Schemas", "folder", "containers"),
  keyspace: k("Keyspace", "Keyspaces", "folder", "containers"),
  namespace: k("Namespace", "Namespaces", "folder", "containers"),
  bucket: k("Bucket", "Buckets", "archive", "containers"),
  dataset: k("Dataset", "Datasets", "layers", "containers"),
  column_family: k("Column family", "Column families", "columns", "containers"),
  // ---- structure
  table: k("Table", "Tables", "table", "structure"),
  view: k("View", "Views", "view", "structure"),
  materialized_view: k("Materialized view", "Materialized views", "view", "structure"),
  foreign_table: k("Foreign table", "Foreign tables", "globe", "structure"),
  virtual_table: k("Virtual table", "Virtual tables", "table", "structure"),
  partition: k("Partition", "Partitions", "layers", "structure"),
  collection: k("Collection", "Collections", "braces", "structure"),
  edge_collection: k("Edge collection", "Edge collections", "link", "structure"),
  graph: k("Graph", "Graphs", "hierarchy", "structure"),
  label: k("Label", "Labels", "tag", "structure"),
  relationship_type: k("Relationship type", "Relationship types", "link", "structure"),
  class: k("Class", "Classes", "cube", "structure"),
  measurement: k("Measurement", "Measurements", "chart", "structure"),
  metric: k("Metric", "Metrics", "activity", "structure"),
  topic: k("Topic", "Topics", "rows", "structure"),
  stream: k("Stream", "Streams", "flow", "structure"),
  channel: k("Channel", "Channels", "rss", "structure"),
  consumer_group: k("Consumer group", "Consumer groups", "users", "structure"),
  document: k("Document", "Documents", "file", "structure"),
  index: k("Index", "Indexes", "list", "structure"),
  constraint: k("Constraint", "Constraints", "shield", "structure"),
  sequence: k("Sequence", "Sequences", "hash", "structure"),
  type: k("Type", "Types", "swatch", "structure"),
  dictionary: k("Dictionary", "Dictionaries", "book", "structure"),
  projection: k("Projection", "Projections", "view", "structure"),
  alias: k("Alias", "Aliases", "tag", "structure"),
  snapshot: k("Snapshot", "Snapshots", "camera", "structure"),
  prefix: k("Prefix", "Prefixes", "text", "structure"),
  // ---- code
  function: k("Function", "Functions", "function", "code"),
  procedure: k("Procedure", "Procedures", "code", "code"),
  aggregate: k("Aggregate", "Aggregates", "function", "code"),
  trigger: k("Trigger", "Triggers", "flash", "code"),
  rule: k("Rule", "Rules", "route", "code"),
  event: k("Event", "Events", "bell", "code"),
  macro: k("Macro", "Macros", "code", "code"),
  package: k("Package", "Packages", "package", "code"),
  script: k("Script", "Scripts", "terminal", "code"),
  task: k("Task", "Tasks", "task", "code"),
  job: k("Job", "Jobs", "task", "code"),
  pipeline: k("Pipeline", "Pipelines", "flow", "code"),
  analyzer: k("Analyzer", "Analyzers", "text", "code"),
  synonym: k("Synonym", "Synonyms", "text", "code"),
  template: k("Template", "Templates", "file", "code"),
  recording_rule: k("Recording rule", "Recording rules", "chart", "code"),
  alert_rule: k("Alert rule", "Alert rules", "alert", "code"),
  // ---- security
  user: k("User", "Users", "user", "security"),
  role: k("Role", "Roles", "users", "security"),
  grant: k("Grant", "Grants", "key", "security"),
  policy: k("Policy", "Policies", "shield", "security"),
  api_key: k("API key", "API keys", "lock-key", "security"),
  acl: k("ACL", "ACLs", "lock", "security"),
  // ---- server
  session: k("Session", "Sessions", "activity", "server"),
  lock: k("Lock", "Locks", "lock", "server"),
  transaction: k("Transaction", "Transactions", "repeat", "server"),
  replica: k("Replica", "Replication", "database-sync", "server"),
  node: k("Node", "Nodes", "server", "server"),
  shard: k("Shard", "Shards", "layers", "server"),
  setting: k("Setting", "Settings", "settings", "server"),
  extension: k("Extension", "Extensions", "package", "server"),
  tablespace: k("Tablespace", "Tablespaces", "archive", "server"),
  publication: k("Publication", "Publications", "send", "server"),
  subscription: k("Subscription", "Subscriptions", "rss", "server"),
  replication_slot: k("Replication slot", "Replication slots", "database-sync", "server"),
  foreign_server: k("Foreign server", "Foreign servers", "globe", "server"),
  foreign_data_wrapper: k("Foreign data wrapper", "Foreign data wrappers", "plug", "server"),
  warehouse: k("Warehouse", "Warehouses", "server", "server"),
  stage: k("Stage", "Stages", "archive", "server"),
  quota: k("Quota", "Quotas", "gauge", "server"),
  slow_query: k("Slow query", "Slow queries", "clock", "server"),
  target: k("Target", "Targets", "target", "server"),
  alert: k("Alert", "Alerts", "alert", "server"),
  service: k("Service", "Services", "cube", "server"),
  backup: k("Backup", "Backups", "archive", "server"),
} satisfies Record<ObjectKind, ObjectKindMeta>;

const KIND_REGISTRY: Record<ObjectKind, ObjectKindMeta> = OBJECT_KINDS;

export const SECTIONS: readonly { id: ObjectSection; label: string }[] = [
  { id: "containers", label: "Containers" },
  { id: "structure", label: "Structure" },
  { id: "code", label: "Code" },
  { id: "security", label: "Security" },
  { id: "server", label: "Server" },
];

export function kindMeta(kind: ObjectKind): ObjectKindMeta {
  return KIND_REGISTRY[kind];
}

export function isScopedKind(kind: ObjectKind): boolean {
  return SCOPED_OBJECT_KINDS.includes(kind);
}

// WHAT:  Kinds the admin page owns (server-wide state, security, monitoring);
//        the object explorer sidebar shows the rest.
export const ADMIN_KINDS: readonly ObjectKind[] = [
  "session",
  "lock",
  "transaction",
  "slow_query",
  "replica",
  "node",
  "shard",
  "database",
  "user",
  "role",
  "grant",
  "acl",
  "api_key",
  "setting",
  "quota",
  "tablespace",
  "warehouse",
  "backup",
  "snapshot",
  "task",
  "job",
  "target",
  "alert",
  "service",
];

export function isAdminKind(kind: ObjectKind): boolean {
  return ADMIN_KINDS.includes(kind);
}

// WHAT:  Playground tabs, keyed by the Rust `Tool` enum.
export interface ToolMeta {
  label: string;
  icon: IconName;
  hint: string;
}

export const TOOLS = {
  stats: { label: "Server overview", icon: "activity", hint: "Connections, memory, cache, replication" },
  key_browser: { label: "Key browser", icon: "hash", hint: "Keys with type, TTL and a per-type editor" },
  erd: { label: "ER diagram", icon: "view", hint: "Tables and foreign keys, drawn" },
  vector_search: { label: "Vector search", icon: "radar", hint: "Similarity search with a payload filter" },
  search_playground: { label: "Search playground", icon: "search-list", hint: "Full-text query, filters, facets, highlights" },
  metrics_explorer: { label: "Metrics explorer", icon: "chart", hint: "Range queries charted over a time window" },
  message_viewer: { label: "Message viewer", icon: "message", hint: "Browse and produce topic messages" },
  pipeline_builder: { label: "Pipeline builder", icon: "flow", hint: "Aggregation stages, run and preview" },
  ledger_history: { label: "Ledger history", icon: "git-branch", hint: "Immutable history with transaction ids" },
  graph_view: { label: "Graph view", icon: "chart-relationship", hint: "Nodes and relationships from a query" },
  pub_sub: { label: "Pub/Sub", icon: "rss", hint: "Channels, publish, subscriber counts" },
  xml_viewer: { label: "XML viewer", icon: "xml", hint: "Documents as a collapsible tree" },
} satisfies Record<Tool, ToolMeta>;

const TOOL_REGISTRY: Record<Tool, ToolMeta> = TOOLS;

export function toolMeta(tool: Tool): ToolMeta {
  return TOOL_REGISTRY[tool];
}

export const TOOL_ORDER: readonly Tool[] = [
  "stats",
  "key_browser",
  "erd",
  "vector_search",
  "search_playground",
  "metrics_explorer",
  "message_viewer",
  "pipeline_builder",
  "ledger_history",
  "graph_view",
  "pub_sub",
  "xml_viewer",
];

// WHAT:  The static profile of an engine's adapter family: what the explorer
//        can list and which tools apply. Available before connecting.
// WHERE: src-tauri/src/integrations/<family>.rs (`profile()`)
export function profileOf(engine: Engine): FamilyProfile {
  return FAMILY_PROFILES[ENGINE_FACTS[engine].family];
}

export function objectKindsOf(engine: Engine): readonly ObjectKind[] {
  return profileOf(engine).objectKinds;
}

export function toolsOf(engine: Engine): readonly Tool[] {
  return profileOf(engine).tools;
}

export function hasTool(engine: Engine, tool: Tool): boolean {
  return toolsOf(engine).includes(tool);
}

export function hasKind(engine: Engine, kind: ObjectKind): boolean {
  return objectKindsOf(engine).includes(kind);
}
