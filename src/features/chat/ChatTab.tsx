// SOT: chat-tab, ai-database-conversation, inline-query-execution
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, Button, Chip, CloseButton, ScrollShadow, TextArea, TextField } from "@heroui/react";
import type { ChatMessage, QueryOutcome, StatementResult } from "@/lib/bindings";
import { formatCell, formatCount } from "@/lib/format";
import { ipc, normalizeError } from "@/lib/ipc";
import { engineMeta } from "@/lib/engines";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

interface MessageItem {
  id: string;
  role: "user" | "assistant";
  content: string;
  sql?: string | null;
  timestamp: number;
  outcome?: QueryOutcome | null;
  runError?: string | null;
  running?: boolean;
}

const chatHistoryCache = new Map<string, MessageItem[]>();

const STARTER_PROMPTS = [
  "Summarize the database schema and key relationships",
  "Which tables have the most records?",
  "Show 5 sample records from the primary tables",
  "Find any foreign keys or table constraints",
  "Write an analytics query with aggregation and grouping",
] as const;

// WHAT:  Interactive conversational chat interface for the active database.
// WHY:   Enables natural language schema discovery, iterative multi-turn query building,
//        live inline execution of generated statements, and auto error-fixing.
export function ChatTab({ connectionId }: { connectionId: string }) {
  const connections = useWorkspace((s) => s.connections);
  const catalogs = useWorkspace((s) => s.catalogs);
  const foreignKeys = useWorkspace((s) => s.foreignKeys[connectionId] ?? []);
  const columnsCache = useWorkspace((s) => s.columnsCache);
  const loadColumns = useWorkspace((s) => s.loadColumns);
  const openTable = useWorkspace((s) => s.openTable);
  const settings = useWorkspace((s) => s.settings);
  const openQuery = useWorkspace((s) => s.openQuery);
  const goSettings = useWorkspace((s) => s.goSettings);
  const showInfo = useWorkspace((s) => s.showInfo);
  const showError = useWorkspace((s) => s.showError);

  const connection = useMemo(() => connections.find((c) => c.id === connectionId), [connections, connectionId]);
  const meta = useMemo(() => (connection ? engineMeta(connection.engine) : null), [connection]);
  const catalog = catalogs[connectionId];
  const tableCount = useMemo(() => catalog?.schemas.reduce((acc, s) => acc + s.tables.length, 0) ?? 0, [catalog]);
  const aiConfigured = Boolean(settings?.ai && settings.ai.provider !== "none");

  const [showSchemaPanel, setShowSchemaPanel] = useState(false);
  const [schemaSearch, setSchemaSearch] = useState("");
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

  const [messages, setMessages] = useState<MessageItem[]>(() => chatHistoryCache.get(connectionId) ?? []);

  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const endRef = useRef<HTMLDivElement>(null);

  // Persist messages in session memory
  useEffect(() => {
    chatHistoryCache.set(connectionId, messages);
  }, [connectionId, messages]);

  const scrollToBottom = useCallback(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, busy, scrollToBottom]);

  const copySchemaMarkdown = useCallback(async () => {
    if (!catalog || !connection || !meta) return;
    let md = `# Database Schema: ${connection.name}\n\n`;
    md += `- **Engine**: ${meta.label}\n`;
    md += `\n## Tables (${tableCount})\n\n`;
    for (const schema of catalog.schemas) {
      for (const table of schema.tables) {
        const full = schema.name ? `${schema.name}.${table.name}` : table.name;
        const rowEst = table.rowEstimate !== null ? ` (~${formatCount(table.rowEstimate)} rows)` : "";
        md += `### ${full}${table.kind === "view" ? " [VIEW]" : ""}${rowEst}\n`;
        const tRef = { schema: table.schema, name: table.name };
        const key = `${connection.id}:${schema.name}.${table.name}`;
        const cached = columnsCache[key];
        const cols = cached && cached.length > 0 ? cached : await loadColumns(connection.id, tRef);
        if (cols.length > 0) {
          md += "| Column | Type | Constraints |\n|---|---|---|\n";
          for (const col of cols) {
            const flags = [col.primaryKey ? "PK" : "", !col.nullable ? "NOT NULL" : ""].filter(Boolean).join(", ");
            md += `| \`${col.name}\` | \`${col.dataType}\` | ${flags || "—"} |\n`;
          }
          md += "\n";
        }
      }
    }
    if (foreignKeys.length > 0) {
      md += "## Foreign Keys & Relationships\n\n";
      for (const fk of foreignKeys) {
        const from = fk.fromSchema ? `${fk.fromSchema}.${fk.fromTable}` : fk.fromTable;
        const to = fk.toSchema ? `${fk.toSchema}.${fk.toTable}` : fk.toTable;
        md += `- \`${from}(${fk.fromColumns.join(", ")})\` -> \`${to}(${fk.toColumns.join(", ")})\`\n`;
      }
      md += "\n";
    }
    void navigator.clipboard.writeText(md);
    showInfo("Copied complete schema as Markdown to clipboard.");
  }, [catalog, columnsCache, connection, foreignKeys, loadColumns, meta, showInfo, tableCount]);

  const filteredTables = useMemo(() => {
    if (!catalog) return [];
    const query = schemaSearch.trim().toLowerCase();
    const list: { schema: string | null; name: string; kind: "table" | "view"; rowEstimate: number | null }[] = [];
    for (const s of catalog.schemas) {
      for (const t of s.tables) {
        if (query.length === 0 || t.name.toLowerCase().includes(query) || s.name.toLowerCase().includes(query)) {
          list.push({ schema: t.schema, name: t.name, kind: t.kind, rowEstimate: t.rowEstimate });
        }
      }
    }
    return list;
  }, [catalog, schemaSearch]);

  const toggleTableExpand = useCallback(
    async (tableKeyStr: string, schema: string | null, tableName: string) => {
      if (!connection) return;
      if (expandedTable === tableKeyStr) {
        setExpandedTable(null);
      } else {
        setExpandedTable(tableKeyStr);
        const cacheKey = `${connection.id}:${schema ?? ""}.${tableName}`;
        if (!columnsCache[cacheKey]) {
          await loadColumns(connection.id, { schema, name: tableName });
        }
      }
    },
    [columnsCache, connection, expandedTable, loadColumns],
  );

  const send = useCallback(
    async (overrideText?: string) => {
      const text = (overrideText ?? input).trim();
      if (text.length === 0 || busy || !connection) return;

      const userMsg: MessageItem = {
        id: `msg-${Date.now()}-user`,
        role: "user",
        content: text,
        timestamp: Date.now(),
      };

      const historyToSend: ChatMessage[] = messages.map((m) => ({
        role: m.role,
        content: m.content,
      }));

      const newMessages = [...messages, userMsg];
      setMessages(newMessages);
      if (!overrideText) setInput("");
      setBusy(true);

      try {
        const reply = await ipc("ai_generate", {
          connectionId: connection.id,
          prompt: text,
          currentQuery: null,
          currentTable: null,
          errorContext: null,
          conversationHistory: historyToSend,
        });

        const assistantMsg: MessageItem = {
          id: `msg-${Date.now()}-assistant`,
          role: "assistant",
          content: reply.text,
          sql: reply.sql,
          timestamp: Date.now(),
        };

        setMessages((prev) => [...prev, assistantMsg]);
      } catch (raw) {
        showError(normalizeError(raw));
      } finally {
        setBusy(false);
      }
    },
    [busy, connection, input, messages, showError],
  );

  const runStatement = useCallback(
    async (msgId: string, sql: string) => {
      if (!connection) return;
      setMessages((prev) => prev.map((m) => (m.id === msgId ? { ...m, running: true, runError: null } : m)));

      try {
        const outcome = await ipc("execute_query", {
          connectionId: connection.id,
          sql,
          confirmDestructive: false,
          maxRows: 50,
        });
        setMessages((prev) => prev.map((m) => (m.id === msgId ? { ...m, running: false, outcome, runError: null } : m)));
        showInfo(`Query executed in ${outcome.elapsedMs}ms.`);
      } catch (raw) {
        const err = normalizeError(raw);
        setMessages((prev) => prev.map((m) => (m.id === msgId ? { ...m, running: false, runError: err.message } : m)));
      }
    },
    [connection, showInfo],
  );

  const clearChat = useCallback(() => {
    setMessages([]);
    chatHistoryCache.delete(connectionId);
    showInfo("Conversation cleared.");
  }, [connectionId, showInfo]);

  if (!connection || !meta) {
    return (
      <div className="flex h-full items-center justify-center p-6 text-muted">
        <span>Connection not found.</span>
      </div>
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-1 flex-col bg-background select-none">
      {/* Header bar */}
      <div className="flex app-toolbar shrink-0 items-center justify-between border-b border-border/40 glass-header ">
        <div className="flex items-center gap-2">
          <div className="flex size-6 items-center justify-center rounded-lg bg-accent/15 text-accent">
            <Icon name="braces" size={13} />
          </div>
          <span className="text-xs font-semibold tracking-tight text-foreground">Chat with Database</span>
          <Chip size="sm" variant="secondary" className="h-5 px-2 text-[10px] font-medium glass-pill text-muted">
            {meta.label}
          </Chip>
          <span className="text-[11px] text-muted">
            {tableCount > 0 ? `${formatCount(tableCount)} tables loaded` : "Loading catalog…"}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <Button
            size="sm"
            variant={showSchemaPanel ? "secondary" : "ghost"}
            className={cn("h-7 px-2.5 rounded-lg text-xs font-medium liquid-hover", showSchemaPanel && "glass-pill text-accent")}
            onPress={() => setShowSchemaPanel((v) => !v)}
          >
            <Icon name="table" size={13} />
            Schema Structure
          </Button>
          <Button
            size="sm"
            variant="ghost"
            className="h-7 px-2.5 rounded-lg text-xs font-medium text-muted hover:text-foreground hover:bg-surface-secondary/70 liquid-hover"
            onPress={() => void copySchemaMarkdown()}
          >
            <Icon name="download" size={12} />
            Copy Schema
          </Button>
          <IconButton icon="file" label="Open in Query Studio" onPress={() => openQuery(connection.id)} size={14} />
          <IconButton icon="trash" label="Clear conversation" isDisabled={messages.length === 0} onPress={clearChat} size={14} />
        </div>
      </div>

      {!aiConfigured ? (
        <div className="p-4">
          <Alert status="warning" className="rounded-xl glass-modal border-warning/30">
            <Alert.Indicator />
            <Alert.Content>
              <Alert.Title className="text-xs font-semibold">AI Provider Not Configured</Alert.Title>
              <Alert.Description className="text-xs text-muted">
                To chat with your database, configure an AI provider (OpenAI, Anthropic, OpenRouter, or local Ollama) in Settings.
              </Alert.Description>
            </Alert.Content>
            <Button size="sm" variant="secondary" onPress={goSettings} className="ml-auto glass-pill text-xs">
              Open Settings
            </Button>
          </Alert>
        </div>
      ) : null}

      {/* Main chat & schema structure layout */}
      <div className="flex min-h-0 flex-1">
        {/* Left: Chat thread */}
        <div className="flex min-w-0 flex-1 flex-col">
          <ScrollShadow className="min-h-0 flex-1 p-4">
            {messages.length === 0 ? (
              <div className="mx-auto flex max-w-xl flex-col items-center justify-center pt-12 text-center">
                <div className="mb-4 flex size-14 items-center justify-center rounded-2xl bg-accent/15 text-accent shadow-lg shadow-accent/10">
                  <Icon name="database" size={26} />
                </div>
                <h2 className="text-base font-semibold tracking-tight text-foreground">
                  Chat with {connection.name}
                </h2>
                <p className="mt-1.5 max-w-md text-xs leading-relaxed text-muted">
                  Ask questions about your schema, explore data relations, generate production-grade {meta.commandLanguage} queries, and run them directly.
                </p>

                <div className="mt-6 flex w-full flex-col gap-2">
                  <span className="text-left text-[11px] font-medium text-muted">Suggested questions:</span>
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                    {STARTER_PROMPTS.map((prompt) => (
                      <button
                        key={prompt}
                        type="button"
                        onClick={() => void send(prompt)}
                        className="flex items-center justify-between rounded-xl border border-border/50 bg-surface-secondary/40 p-2.5 text-left text-xs text-foreground transition-all hover:border-accent/40 hover:bg-surface-secondary/80 liquid-hover active:scale-[0.99]"
                      >
                        <span className="line-clamp-2">{prompt}</span>
                        <Icon name="play" size={10} className="shrink-0 text-muted opacity-60 ml-2" />
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div className="mx-auto flex max-w-3xl flex-col gap-4 pb-4">
                {messages.map((m) => (
                  <div key={m.id} className={cn("flex flex-col gap-1.5", m.role === "user" ? "items-end" : "items-start")}>
                    <div className="flex items-center gap-1.5 px-1 text-[10px] text-muted">
                      <span className="font-medium">{m.role === "user" ? "You" : "Database Assistant"}</span>
                      <span>•</span>
                      <span>{new Date(m.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</span>
                    </div>

                    <div
                      className={cn(
                        "selectable max-w-[92%] rounded-2xl p-3.5 text-xs leading-relaxed transition-all",
                        m.role === "user"
                          ? "rounded-tr-sm bg-accent text-accent-foreground shadow-sm"
                          : "rounded-tl-sm border border-border/50 bg-surface/80 text-foreground glass-modal",
                      )}
                    >
                      <p className="whitespace-pre-wrap">{m.content}</p>

                      {m.sql ? (
                        <div className="mt-3 rounded-xl border border-border/60 bg-surface-secondary/80 p-2.5">
                          <div className="mb-2 flex items-center justify-between">
                            <span className="font-mono text-[10.5px] font-semibold text-muted uppercase tracking-wider">
                              {meta.commandLanguage}
                            </span>
                            <div className="flex items-center gap-1">
                              <Button
                                size="sm"
                                variant="ghost"
                                className="h-5 px-1.5 text-[10.5px] rounded text-muted hover:text-foreground liquid-hover"
                                onPress={() => {
                                  void navigator.clipboard.writeText(m.sql ?? "");
                                  showInfo("Query copied to clipboard.");
                                }}
                              >
                                Copy
                              </Button>
                              <Button
                                size="sm"
                                variant="ghost"
                                className="h-5 px-1.5 text-[10.5px] rounded text-muted hover:text-foreground liquid-hover"
                                onPress={() => openQuery(connection.id, m.sql ?? "", "AI Query")}
                              >
                                Open in Studio
                              </Button>
                              <Button
                                size="sm"
                                variant="secondary"
                                isPending={Boolean(m.running)}
                                className="h-5 px-2 text-[10.5px] rounded font-medium glass-pill text-accent liquid-hover"
                                onPress={() => void runStatement(m.id, m.sql ?? "")}
                              >
                                <Icon name="play" size={9} />
                                Run in DB
                              </Button>
                            </div>
                          </div>

                          <ScrollShadow className="max-h-48">
                            <pre className="selectable font-mono text-[11px] text-foreground whitespace-pre-wrap">{m.sql}</pre>
                          </ScrollShadow>

                          {/* Error feedback with auto-fix button */}
                          {m.runError ? (
                            <div className="mt-2.5 rounded-lg border border-danger/30 bg-danger-soft/60 p-2 text-danger">
                              <div className="flex items-start justify-between gap-2">
                                <span className="font-mono text-[11px] leading-tight">{m.runError}</span>
                                <Button
                                  size="sm"
                                  variant="outline"
                                  className="h-5 shrink-0 rounded border-danger/40 bg-danger-soft px-1.5 text-[10.5px] text-danger hover:bg-danger/20 liquid-hover"
                                  onPress={() => void send(`Fix the following query error:\n${m.runError}\nFor query:\n${m.sql}`)}
                                >
                                  <Icon name="refresh" size={9} />
                                  Fix with AI
                                </Button>
                              </div>
                            </div>
                          ) : null}

                          {/* Live query results preview */}
                          {m.outcome ? (
                            <div className="mt-3 rounded-lg border border-border/40 bg-surface/90 p-2">
                              <div className="mb-1.5 flex items-center justify-between text-[10px] text-muted">
                                <span>Result preview</span>
                                <span>{m.outcome.elapsedMs}ms elapsed</span>
                              </div>
                              <OutcomePreview outcome={m.outcome} />
                            </div>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  </div>
                ))}

                {busy ? (
                  <div className="flex items-start gap-2 text-muted">
                    <div className="flex size-5 items-center justify-center rounded bg-accent/15 text-accent animate-pulse">
                      <Icon name="braces" size={11} />
                    </div>
                    <span className="text-xs animate-pulse">Assistant is analyzing schema and generating answer…</span>
                  </div>
                ) : null}

                <div ref={endRef} />
              </div>
            )}
          </ScrollShadow>

          {/* Input container */}
          <div className="border-t border-border/40 glass-footer p-3">
            <div className="mx-auto max-w-3xl">
              <TextField
                value={input}
                onChange={setInput}
                aria-label="Message to database assistant"
                className="w-full"
              >
                <div className="relative flex w-full flex-col rounded-2xl border border-border/60 bg-surface-secondary/40 p-1.5 shadow-sm focus-within:border-accent/50 focus-within:ring-1 focus-within:ring-accent/30">
                  <TextArea
                    placeholder={`Ask about ${connection.name} tables, relations, or request queries… (⌘+Enter to send)`}
                    rows={2}
                    className="w-full resize-none border-none bg-transparent p-2 text-xs text-foreground focus:outline-none placeholder:text-muted"
                    onKeyDown={(e) => {
                      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
                        e.preventDefault();
                        void send();
                      }
                    }}
                  />
                  <div className="flex items-center justify-between px-2 pt-1 pb-0.5">
                    <span className="text-[10.5px] text-muted">
                      Schema context auto-injected • ⌘+Enter
                    </span>
                    <Button
                      size="sm"
                      variant="secondary"
                      isPending={busy}
                      isDisabled={input.trim().length === 0 || !aiConfigured}
                      onPress={() => void send()}
                      className="h-7 rounded-xl font-medium glass-pill text-accent px-3 liquid-hover"
                    >
                      <Icon name="play" size={10} />
                      Send
                    </Button>
                  </div>
                </div>
              </TextField>
            </div>
          </div>
        </div>
        {/* End of Left chat thread */}

        {/* Right: Schema Structure Inspector Panel */}
        {showSchemaPanel ? (
          <div className="flex w-84 shrink-0 flex-col border-l border-border/40 glass-sidebar">
            <div className="flex h-10 items-center justify-between border-b border-border/40 px-3">
              <div className="flex items-center gap-1.5">
                <Icon name="table" size={13} className="text-accent" />
                <span className="text-xs font-semibold tracking-tight text-foreground">Database Structure</span>
              </div>
              <CloseButton onPress={() => setShowSchemaPanel(false)} aria-label="Close schema panel" />
            </div>

            {/* Search */}
            <div className="p-2 border-b border-border/40">
              <input
                type="text"
                value={schemaSearch}
                onChange={(e) => setSchemaSearch(e.target.value)}
                placeholder="Filter tables & views…"
                className="w-full rounded-lg border border-border/50 bg-surface-secondary/50 px-2.5 py-1 text-xs text-foreground placeholder:text-muted focus:border-accent/60 focus:outline-none"
              />
            </div>

            {/* Tables & Columns tree */}
            <ScrollShadow className="min-h-0 flex-1 p-2">
              <div className="flex flex-col gap-1">
                {filteredTables.map((t) => {
                  const keyStr = `${t.schema ?? ""}.${t.name}`;
                  const isExpanded = expandedTable === keyStr;
                  const cols = columnsCache[`${connection.id}:${keyStr}`] ?? [];

                  return (
                    <div key={keyStr} className="rounded-lg border border-border/30 bg-surface-secondary/25 overflow-hidden">
                      <div
                        onClick={() => void toggleTableExpand(keyStr, t.schema, t.name)}
                        className="flex items-center justify-between p-2 hover:bg-surface-secondary/60 cursor-pointer transition-colors"
                      >
                        <div className="flex items-center gap-1.5 min-w-0 flex-1">
                          <Icon name={t.kind === "view" ? "view" : "table"} size={12} className="shrink-0 text-muted" />
                          <span className="truncate font-mono text-[11px] font-medium text-foreground">
                            {t.name}
                          </span>
                          {t.kind === "view" ? (
                            <span className="rounded bg-surface-secondary px-1 text-[9px] text-muted uppercase">View</span>
                          ) : null}
                        </div>

                        <div className="flex items-center gap-0.5 shrink-0 ml-1">
                          {t.rowEstimate !== null ? (
                            <span className="text-[10px] text-muted mr-1">~{formatCount(t.rowEstimate)}</span>
                          ) : null}
                          <IconButton
                            icon="braces"
                            label="Ask AI about table"
                            onPress={() => void send(`Explain the purpose, columns, and relations for table "${t.name}"`)}
                            size={11}
                          />
                          <IconButton
                            icon="table"
                            label="Browse table data"
                            onPress={() => openTable(connection.id, { schema: t.schema, name: t.name })}
                            size={11}
                          />
                        </div>
                      </div>

                      {/* Expanded Columns List */}
                      {isExpanded ? (
                        <div className="border-t border-border/25 bg-surface/50 p-2 text-[10.5px]">
                          {cols.length === 0 ? (
                            <span className="text-muted italic text-[10px]">Loading columns…</span>
                          ) : (
                            <div className="flex flex-col gap-1">
                              {cols.map((col) => (
                                <div
                                  key={col.name}
                                  className="flex items-center justify-between font-mono hover:text-accent cursor-pointer"
                                  onClick={() => void send(`How is column "${col.name}" used in table "${t.name}"?`)}
                                  title="Click to ask AI about this column"
                                >
                                  <div className="flex items-center gap-1 min-w-0">
                                    <span className={cn(col.primaryKey ? "font-bold text-accent" : "text-foreground", "truncate")}>
                                      {col.name}
                                    </span>
                                    {col.primaryKey ? (
                                      <span className="rounded bg-accent/20 px-1 py-0.2 text-[9px] font-bold text-accent">PK</span>
                                    ) : null}
                                    {!col.nullable && !col.primaryKey ? (
                                      <span className="text-[9px] text-muted">REQ</span>
                                    ) : null}
                                  </div>
                                  <span className="text-[10px] text-muted truncate ml-1">{col.dataType}</span>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ) : null}
                    </div>
                  );
                })}
              </div>

              {/* Foreign Keys relationships summary */}
              {foreignKeys.length > 0 ? (
                <div className="mt-4 pt-3 border-t border-border/40">
                  <span className="text-[10.5px] font-semibold text-foreground tracking-tight block mb-2">
                    Foreign Key Relations ({foreignKeys.length})
                  </span>
                  <div className="flex flex-col gap-1.5">
                    {foreignKeys.map((fk, i) => (
                      <div
                        key={i}
                        className="flex items-center justify-between rounded-lg border border-border/30 bg-surface-secondary/20 p-1.5 text-[10px] font-mono text-muted"
                      >
                        <span className="truncate">
                          {fk.fromTable}({fk.fromColumns.join(",")}) → {fk.toTable}({fk.toColumns.join(",")})
                        </span>
                        <IconButton
                          icon="braces"
                          label="Ask AI to join"
                          onPress={() => void send(`Write a query joining ${fk.fromTable} and ${fk.toTable} on ${fk.fromColumns.join(", ")} = ${fk.toColumns.join(", ")}`)}
                          size={11}
                        />
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
            </ScrollShadow>
          </div>
        ) : null}
      </div>
    </div>
  );
}

function OutcomePreview({ outcome }: { outcome: QueryOutcome }) {
  if (outcome.statements.length === 0) {
    return <span className="text-[11px] text-muted">Query executed successfully with no returned rows.</span>;
  }

  return (
    <div className="flex flex-col gap-2">
      {outcome.statements.map((stmt, idx) => (
        <StatementPreview key={idx} stmt={stmt} />
      ))}
    </div>
  );
}

function StatementPreview({ stmt }: { stmt: StatementResult }) {
  if (stmt.kind === "affected") {
    return (
      <div className="flex items-center gap-1.5 py-1 text-[11px] text-foreground">
        <Icon name="check" size={11} className="text-success" />
        <span>{formatCount(stmt.rowsAffected)} rows affected.</span>
      </div>
    );
  }

  const { columns, rows, truncated } = stmt.result;
  if (rows.length === 0) {
    return <span className="text-[11px] text-muted">0 rows returned.</span>;
  }

  return (
    <div className="flex flex-col gap-1">
      <ScrollShadow hideScrollBar className="max-h-44 overflow-x-auto rounded border border-border/30">
        <table className="w-full border-collapse font-mono text-[10.5px]">
          <thead className="sticky top-0 bg-surface-secondary/90 backdrop-blur-sm">
            <tr>
              {columns.map((col) => (
                <th key={col.name} className="border-b border-border/40 px-2 py-1 text-left font-semibold text-muted">
                  {col.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rIdx) => (
              <tr key={rIdx} className="hover:bg-surface-secondary/40 border-b border-border/20 last:border-none">
                {row.map((cell, cIdx) => {
                  const formatted = formatCell(cell);
                  return (
                    <td
                      key={cIdx}
                      className={cn(
                        "px-2 py-0.5 truncate max-w-[180px]",
                        formatted.align === "right" ? "text-right" : "text-left",
                      )}
                    >
                      {formatted.text}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </ScrollShadow>
      <div className="flex items-center justify-between text-[10px] text-muted px-0.5">
        <span>{formatCount(rows.length)} rows shown</span>
        {truncated ? <span>(Results capped at preview limit)</span> : null}
      </div>
    </div>
  );
}
