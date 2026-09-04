// SOT: ipc-client, invoke-wrapper, command-map, app-error-normalization, client-block
import { invoke } from "@tauri-apps/api/core";
import type {
  AiGenerateRequest,
  AiReply,
  AppError,
  AppSettings,
  BufferIdRequest,
  CatalogRequest,
  ChangePreview,
  ChangesRequest,
  ClearHistoryRequest,
  ColumnInfo,
  ColumnsRequest,
  CommandName,
  ConnectRequest,
  ConnectionIdRequest,
  ConnectionSummary,
  Document,
  EditorBuffer,
  ExecuteQueryRequest,
  ExplainRequest,
  ExportReport,
  ExportRequest,
  ForeignKey,
  HistoryEntry,
  HistoryRequest,
  IdRequest,
  ImportReport,
  ImportRequest,
  ListDocumentsRequest,
  ObjectDetail,
  ObjectRequest,
  ObjectSummary,
  ObjectsRequest,
  PlanReport,
  QueryOutcome,
  RangeQueryCommand,
  RangeResult,
  ResultSet,
  RunWorkflowRequest,
  SaveBufferRequest,
  SaveConnectionRequest,
  SaveDocumentRequest,
  SaveQueryRequest,
  SaveSettingsRequest,
  SavedQuery,
  SchemaCatalog,
  SearchCommand,
  SearchResult,
  ServerStats,
  SessionInfo,
  SessionRequest,
  TablePage,
  TablePageRequest,
  VectorSearchCommand,
  WorkflowRunReport,
} from "./bindings";

// WHAT:  The client-side block. Every call into the Rust core goes through `ipc`.
// WHY:   One place normalises errors into AppError, so components switch on
//        `kind` and never parse strings. ESLint + guardrail.py forbid `invoke`
//        anywhere else.
// HOW:   CommandMap is checked in both directions against the Rust CommandName
//        enum: a command added in Rust without a signature here fails `tsc`.
// WHERE: src-tauri/src/commands/mod.rs (CommandName), src/lib/bindings (types)
interface CommandMap {
  list_connections: { req: undefined; res: ConnectionSummary[] };
  save_connection: { req: SaveConnectionRequest; res: ConnectionSummary };
  delete_connection: { req: ConnectionIdRequest; res: null };
  test_connection: { req: SaveConnectionRequest; res: null };
  connect: { req: ConnectRequest; res: ConnectionSummary };
  disconnect: { req: ConnectionIdRequest; res: null };
  active_sessions: { req: undefined; res: string[] };
  describe_session: { req: SessionRequest; res: SessionInfo };
  load_catalog: { req: CatalogRequest; res: SchemaCatalog };
  load_columns: { req: ColumnsRequest; res: ColumnInfo[] };
  load_foreign_keys: { req: CatalogRequest; res: ForeignKey[] };
  load_ddl: { req: ColumnsRequest; res: string | null };
  fetch_table_page: { req: TablePageRequest; res: TablePage };
  execute_query: { req: ExecuteQueryRequest; res: QueryOutcome };
  list_history: { req: HistoryRequest; res: HistoryEntry[] };
  clear_history: { req: ClearHistoryRequest; res: number };
  list_buffers: { req: undefined; res: EditorBuffer[] };
  save_buffer: { req: SaveBufferRequest; res: EditorBuffer };
  delete_buffer: { req: BufferIdRequest; res: null };
  get_settings: { req: undefined; res: AppSettings };
  save_settings: { req: SaveSettingsRequest; res: AppSettings };
  list_saved_queries: { req: undefined; res: SavedQuery[] };
  save_saved_query: { req: SaveQueryRequest; res: SavedQuery };
  delete_saved_query: { req: IdRequest; res: null };
  list_documents: { req: ListDocumentsRequest; res: Document[] };
  save_document: { req: SaveDocumentRequest; res: Document };
  delete_document: { req: IdRequest; res: null };
  preview_changes: { req: ChangesRequest; res: ChangePreview };
  commit_changes: { req: ChangesRequest; res: QueryOutcome };
  export_tables: { req: ExportRequest; res: ExportReport };
  import_file: { req: ImportRequest; res: ImportReport };
  ai_generate: { req: AiGenerateRequest; res: AiReply };
  explain_query: { req: ExplainRequest; res: PlanReport };
  run_workflow: { req: RunWorkflowRequest; res: WorkflowRunReport };
  list_objects: { req: ObjectsRequest; res: ObjectSummary[] };
  load_object: { req: ObjectRequest; res: ObjectDetail };
  server_stats: { req: SessionRequest; res: ServerStats };
  vector_search: { req: VectorSearchCommand; res: ResultSet };
  search_documents: { req: SearchCommand; res: SearchResult };
  query_range: { req: RangeQueryCommand; res: RangeResult };
  load_history: { req: ObjectRequest; res: ResultSet };
}

type MissingFromMap = Exclude<CommandName, keyof CommandMap>;
type NotACommand = Exclude<keyof CommandMap, CommandName>;
export const commandMapIsExhaustive: [MissingFromMap, NotACommand] extends [never, never] ? true : never = true;

type Args<K extends CommandName> = CommandMap[K]["req"] extends undefined ? [] : [req: CommandMap[K]["req"]];

// WHAT:  Error class carrying the typed AppError, so `throw` always throws an Error.
export class IpcError extends Error {
  readonly error: AppError;
  constructor(error: AppError) {
    super(error.message);
    this.name = "IpcError";
    this.error = error;
  }
}

export async function ipc<K extends CommandName>(name: K, ...args: Args<K>): Promise<CommandMap[K]["res"]> {
  const req = args[0];
  try {
    return await invoke<CommandMap[K]["res"]>(name, req === undefined ? undefined : { req });
  } catch (raw) {
    throw new IpcError(normalizeError(raw));
  }
}

// WHAT:  The only narrowing from `unknown` in the UI: Tauri rejects with the
//        serialised AppError, or with a string if the command itself was not found.
export function isAppError(raw: unknown): raw is AppError {
  return (
    typeof raw === "object" &&
    raw !== null &&
    "kind" in raw &&
    typeof raw.kind === "string" &&
    "message" in raw &&
    typeof raw.message === "string"
  );
}

export function normalizeError(raw: unknown): AppError {
  if (raw instanceof IpcError) return raw.error;
  if (isAppError(raw)) return raw;
  if (raw instanceof Error) return { kind: "internal", message: raw.message };
  if (typeof raw === "string") return { kind: "internal", message: raw };
  return { kind: "internal", message: JSON.stringify(raw) };
}

export function errorMessage(error: AppError): string {
  switch (error.kind) {
    case "destructive_confirmation_required":
      return `${error.message} ${error.statements.join("; ")}`;
    case "not_found":
    case "not_connected":
    case "read_only":
    case "invalid_input":
    case "timeout":
    case "driver":
    case "store":
    case "crypto":
    case "keyring":
    case "internal":
      return error.message;
  }
}
