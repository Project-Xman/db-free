// SOT: query-pane, run-query-flow, destructive-confirm-flow, buffer-autosave, save-query-flow, ai-assist-flow, explain-flow, format-sql
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, Button, CloseButton, Modal, Popover, ScrollShadow, Spinner, TextArea, TextField } from "@heroui/react";
import { format as formatSql } from "sql-formatter";
import type { AppError, ConnectionSummary, PlanReport, QueryOutcome } from "@/lib/bindings";
import type { SQLNamespace } from "@codemirror/lang-sql";
import { ipc, normalizeError } from "@/lib/ipc";
import { engineMeta } from "@/lib/engines";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Field } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { Resizer } from "@/components/global/Resizer";
import { RunShortcut } from "@/components/global/Kbd";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";
import { SqlEditor } from "./SqlEditor";
import { ResultsPane } from "./ResultsPane";
import { HistoryPanel } from "./HistoryPanel";

/// Per-tab row cap. The default comes from Settings -> Max query rows, so the
/// app-wide answer is set once and a single tab can still override it.
const ROW_CAPS = [
  { value: "500", label: "500 rows" },
  { value: "1000", label: "1,000 rows" },
  { value: "5000", label: "5,000 rows" },
  { value: "10000", label: "10,000 rows" },
  { value: "20000", label: "20,000 rows" },
  { value: "50000", label: "50,000 rows" },
  { value: "100000", label: "100,000 rows" },
] satisfies readonly { value: string; label: string }[];

function defaultRowCap(max: number | undefined): (typeof ROW_CAPS)[number]["value"] {
  const wanted = String(max ?? 1000);
  return ROW_CAPS.find((c) => c.value === wanted)?.value ?? "1000";
}

interface QueryPaneProps {
  connection: ConnectionSummary;
  tabId: string;
  seedSql?: string | undefined;
}

// WHAT:  SQL workbench tab: editor, run, results, history, autosaved buffer,
//        save-as-query, AI assist and plan explanation.
// WHY:   PRD §4.3 / §4.5 — execution runs on the Rust side; the UI awaits the
//        outcome. Destructive statements bounce back as a typed error the user
//        confirms explicitly.
// WHERE: src-tauri/src/guard/mod.rs, src-tauri/src/services/ai.rs
export function QueryPane({ connection, tabId, seedSql }: QueryPaneProps) {
  const catalog = useWorkspace((s) => s.catalogs[connection.id]);
  const columnsCache = useWorkspace((s) => s.columnsCache);
  const settings = useWorkspace((s) => s.settings);
  const saveQuery = useWorkspace((s) => s.saveQuery);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const bufferId = tabId;
  const isSql = engineMeta(connection.engine).commandLanguage === "SQL";

  const [sql, setSql] = useState(seedSql ?? "");
  const [loaded, setLoaded] = useState(false);
  const [running, setRunning] = useState(false);
  const [outcome, setOutcome] = useState<QueryOutcome | null>(null);
  const [rowCap, setRowCap] = useState<(typeof ROW_CAPS)[number]["value"]>(() => defaultRowCap(settings?.maxQueryRows));
  const [confirm, setConfirm] = useState<{ statements: string[] } | null>(null);
  const [historyKey, setHistoryKey] = useState(0);
  const [showHistory, setShowHistory] = useState(false);
  const [saveOpen, setSaveOpen] = useState(false);
  const [saveName, setSaveName] = useState("");
  const [saveTags, setSaveTags] = useState("");
  const [aiOpen, setAiOpen] = useState(false);
  const [aiPrompt, setAiPrompt] = useState("");
  const [aiBusy, setAiBusy] = useState(false);
  const [aiText, setAiText] = useState<string | null>(null);
  const [aiGeneratedSql, setAiGeneratedSql] = useState<string | null>(null);
  const [lastError, setLastError] = useState<string | null>(null);
  const [plan, setPlan] = useState<PlanReport | null>(null);
  const [planBusy, setPlanBusy] = useState(false);
  const saveTimer = useRef<number | null>(null);

  const [editorHeight, setEditorHeight] = useState<number>(() => {
    try {
      const saved = localStorage.getItem("db-free:query-editor-height");
      return saved ? Math.max(100, Math.min(800, Number(saved))) : 260;
    } catch {
      return 260;
    }
  });

  const handleEditorResize = useCallback((delta: number) => {
    setEditorHeight((prev) => {
      const next = Math.max(100, Math.min(800, prev + delta));
      try {
        localStorage.setItem("db-free:query-editor-height", String(next));
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  const [historyWidth, setHistoryWidth] = useState<number>(() => {
    try {
      const saved = localStorage.getItem("db-free:query-history-width");
      return saved ? Math.max(220, Math.min(600, Number(saved))) : 288;
    } catch {
      return 288;
    }
  });

  const handleHistoryResize = useCallback((delta: number) => {
    setHistoryWidth((prev) => {
      const next = Math.max(220, Math.min(600, prev - delta));
      try {
        localStorage.setItem("db-free:query-history-width", String(next));
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  useEffect(() => {
    const token = { cancelled: false };
    void (async () => {
      try {
        const buffers = await ipc("list_buffers");
        if (token.cancelled) return;
        const mine = buffers.find((b) => b.id === bufferId);
        if (mine && seedSql === undefined) setSql(mine.content);
      } catch (raw) {
        if (!token.cancelled) showError(normalizeError(raw));
      } finally {
        if (!token.cancelled) setLoaded(true);
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [bufferId, seedSql, showError]);

  const onChange = useCallback(
    (next: string) => {
      setSql(next);
      if (saveTimer.current !== null) window.clearTimeout(saveTimer.current);
      saveTimer.current = window.setTimeout(() => {
        void (async () => {
          try {
            await ipc("save_buffer", { buffer: { id: bufferId, connectionId: connection.id, title: "Query", content: next, updatedAt: "" } });
          } catch (raw) {
            showError(normalizeError(raw));
          }
        })();
      }, 500);
    },
    [bufferId, connection.id, showError],
  );

  const run = useCallback(
    async (confirmDestructive: boolean) => {
      if (running || sql.trim().length === 0) return;
      setRunning(true);
      setConfirm(null);
      try {
        const result = await ipc("execute_query", { connectionId: connection.id, sql, confirmDestructive, maxRows: Number(rowCap) });
        setOutcome(result);
        setLastError(null);
      } catch (raw) {
        const error: AppError = normalizeError(raw);
        setLastError(error.message);
        if (error.kind === "destructive_confirmation_required") setConfirm({ statements: error.statements });
        else showError(error);
      } finally {
        setRunning(false);
        setHistoryKey((k) => k + 1);
      }
    },
    [connection.id, rowCap, running, showError, sql],
  );

  const doFormat = useCallback(() => {
    if (!isSql) return;
    try {
      const language = connection.engine === "postgres" ? "postgresql" : connection.engine === "mysql" || connection.engine === "mariadb" ? "mysql" : connection.engine === "sqlite" ? "sqlite" : "sql";
      onChange(formatSql(sql, { language, keywordCase: "upper", expressionWidth: settings?.condenseSqlWhenFormatting ? 120 : 50 }));
    } catch (raw) {
      showError(normalizeError(raw));
    }
  }, [connection.engine, isSql, onChange, settings?.condenseSqlWhenFormatting, showError, sql]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.shiftKey && e.altKey && e.key.toLowerCase() === "f") {
        e.preventDefault();
        doFormat();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [doFormat]);

  const doSave = async () => {
    try {
      await saveQuery({ id: "", connectionId: connection.id, name: saveName, sql, tags: saveTags.split(",").map((t) => t.trim()).filter((t) => t.length > 0), createdAt: "", updatedAt: "" });
      setSaveOpen(false);
      setSaveName("");
      setSaveTags("");
      showInfo(`Saved "${saveName}".`);
    } catch (raw) {
      showError(normalizeError(raw));
    }
  };

  const askAi = async (overridePrompt?: string) => {
    const promptText = (overridePrompt ?? aiPrompt).trim();
    if (promptText.length === 0) return;
    if (overridePrompt) setAiPrompt(overridePrompt);
    setAiBusy(true);
    setAiText(null);
    setAiGeneratedSql(null);
    try {
      const reply = await ipc("ai_generate", {
        connectionId: connection.id,
        prompt: promptText,
        currentQuery: sql.trim().length > 0 ? sql : null,
        currentTable: null,
        errorContext: lastError,
        conversationHistory: null,
      });
      setAiText(reply.text);
      setAiGeneratedSql(reply.sql);
      if (reply.sql !== null && sql.trim().length === 0) {
        onChange(reply.sql);
        showInfo("Generated query placed in editor.");
      }
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setAiBusy(false);
    }
  };

  const explain = async () => {
    if (sql.trim().length === 0) return;
    setPlanBusy(true);
    try {
      setPlan(await ipc("explain_query", { connectionId: connection.id, sql }));
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setPlanBusy(false);
    }
  };

  const schema = useMemo<SQLNamespace>(() => buildNamespace(connection.id, catalog, columnsCache), [connection.id, catalog, columnsCache]);
  const defaultSchema = connection.engine === "postgres" ? "public" : undefined;
  const aiEnabled = settings !== null && settings.ai.provider !== "none";

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex app-toolbar shrink-0 items-center gap-2 border-b border-border/40 glass-header ">
        <Button
          size="sm"
          isPending={running}
          onPress={() => void run(false)}
          isDisabled={!loaded || sql.trim().length === 0}
          className="glass-pill bg-accent text-accent-foreground font-semibold shadow-sm shadow-accent/30 liquid-hover"
        >
          <Icon name="play" size={12} />
          Run
        </Button>
        <RunShortcut />
        {isSql ? (
          <Button size="sm" variant="ghost" className="rounded-lg text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover" onPress={doFormat} isDisabled={sql.trim().length === 0}>
            Format
          </Button>
        ) : null}
        {isSql ? (
          <Button
            size="sm"
            variant="ghost"
            className="rounded-lg text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover"
            isPending={planBusy}
            onPress={() => void explain()}
            isDisabled={sql.trim().length === 0}
          >
            Explain
          </Button>
        ) : null}
        <Button size="sm" variant="ghost" className="rounded-lg text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover" onPress={() => setSaveOpen(true)} isDisabled={sql.trim().length === 0}>
          Save
        </Button>
        <Popover isOpen={aiOpen} onOpenChange={setAiOpen}>
          <Button size="sm" variant={aiEnabled ? "secondary" : "ghost"} className={cn("rounded-lg liquid-hover", aiEnabled ? "glass-pill text-accent" : "text-muted hover:bg-surface-secondary/70 hover:text-foreground")}>
            <Icon name="braces" size={12} />
            AI
          </Button>
          <Popover.Content className="w-[500px] glass-modal rounded-xl">
            <Popover.Dialog>
              <div className="flex items-center justify-between">
                <Popover.Heading className="text-sm font-semibold text-foreground">AI Database Assistant</Popover.Heading>
                <div className="flex items-center gap-1.5">
                  <span className="rounded px-1.5 py-0.5 text-[10px] font-medium bg-surface-secondary text-muted">
                    {engineMeta(connection.engine).label}
                  </span>
                  {sql.trim().length > 0 ? (
                    <span className="rounded px-1.5 py-0.5 text-[10px] font-medium bg-accent/15 text-accent">
                      Editor Query
                    </span>
                  ) : null}
                  {lastError !== null ? (
                    <span className="rounded px-1.5 py-0.5 text-[10px] font-medium bg-danger-soft text-danger">
                      Error Context
                    </span>
                  ) : null}
                </div>
              </div>
              {aiEnabled ? (
                <>
                  <div className="mt-2.5 flex flex-wrap gap-1.5">
                    {lastError !== null ? (
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-6 rounded-full border-danger/40 bg-danger-soft/40 px-2 text-[11px] text-danger hover:bg-danger-soft liquid-hover"
                        onPress={() => void askAi("Fix the error in my query")}
                      >
                        <Icon name="refresh" size={10} />
                        Fix Error
                      </Button>
                    ) : null}
                    {sql.trim().length > 0 ? (
                      <>
                        <Button
                          size="sm"
                          variant="outline"
                          className="h-6 rounded-full border-border/60 bg-surface-secondary/50 px-2 text-[11px] text-muted hover:text-foreground hover:bg-surface-secondary liquid-hover"
                          onPress={() => void askAi("Optimize this query for performance and explain")}
                        >
                          Optimize
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          className="h-6 rounded-full border-border/60 bg-surface-secondary/50 px-2 text-[11px] text-muted hover:text-foreground hover:bg-surface-secondary liquid-hover"
                          onPress={() => void askAi("Add pagination using LIMIT and OFFSET")}
                        >
                          Paginate
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          className="h-6 rounded-full border-border/60 bg-surface-secondary/50 px-2 text-[11px] text-muted hover:text-foreground hover:bg-surface-secondary liquid-hover"
                          onPress={() => void askAi("Explain what this query does in plain language")}
                        >
                          Explain
                        </Button>
                      </>
                    ) : (
                      <>
                        <Button
                          size="sm"
                          variant="outline"
                          className="h-6 rounded-full border-border/60 bg-surface-secondary/50 px-2 text-[11px] text-muted hover:text-foreground hover:bg-surface-secondary liquid-hover"
                          onPress={() => void askAi("List top 10 rows ordered by latest date")}
                        >
                          Top 10 Rows
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          className="h-6 rounded-full border-border/60 bg-surface-secondary/50 px-2 text-[11px] text-muted hover:text-foreground hover:bg-surface-secondary liquid-hover"
                          onPress={() => void askAi("Count total rows grouped by status")}
                        >
                          Count by Status
                        </Button>
                      </>
                    )}
                  </div>
                  <TextField value={aiPrompt} onChange={setAiPrompt} className="mt-2 w-full" aria-label="Prompt">
                    <TextArea placeholder="Ask to generate, modify, explain, or fix a query..." rows={3} className="w-full" />
                  </TextField>
                  <div className="mt-2 flex items-center justify-between">
                    <Button size="sm" isPending={aiBusy} onPress={() => void askAi()} isDisabled={aiPrompt.trim().length === 0} className="glass-pill bg-accent text-accent-foreground font-semibold liquid-hover">
                      Generate {engineMeta(connection.engine).commandLanguage}
                    </Button>
                    <span className="text-[11px] text-muted">Schema, context & prompt sent to {settings.ai.provider}.</span>
                  </div>
                  {aiGeneratedSql !== null ? (
                    <div className="mt-3 rounded-lg border border-border/60 bg-surface-secondary/70 p-2.5">
                      <div className="mb-1.5 flex items-center justify-between">
                        <span className="text-[11px] font-semibold text-foreground tracking-tight">Generated Statement</span>
                        <div className="flex items-center gap-1">
                          <Button
                            size="sm"
                            variant="ghost"
                            className="h-5 px-1.5 text-[10.5px] rounded text-muted hover:text-foreground liquid-hover"
                            onPress={() => {
                              void navigator.clipboard.writeText(aiGeneratedSql);
                              showInfo("Copied statement to clipboard.");
                            }}
                          >
                            Copy
                          </Button>
                          <Button
                            size="sm"
                            variant="ghost"
                            className="h-5 px-1.5 text-[10.5px] rounded text-muted hover:text-foreground liquid-hover"
                            onPress={() => {
                              onChange(sql.trim().length > 0 ? `${sql.trimEnd()}\n\n${aiGeneratedSql}` : aiGeneratedSql);
                              showInfo("Inserted statement below.");
                            }}
                          >
                            Insert Below
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            className="h-5 px-2 text-[10.5px] rounded font-medium glass-pill text-accent liquid-hover"
                            onPress={() => {
                              onChange(aiGeneratedSql);
                              showInfo("Replaced editor query.");
                            }}
                          >
                            Replace Editor
                          </Button>
                        </div>
                      </div>
                      <ScrollShadow className="max-h-32">
                        <pre className="selectable font-mono text-[11px] text-foreground whitespace-pre-wrap">{aiGeneratedSql}</pre>
                      </ScrollShadow>
                    </div>
                  ) : null}
                  {aiText !== null ? (
                    <ScrollShadow className="selectable mt-2 max-h-36 rounded-lg border border-border/40 bg-surface/60 p-2 text-xs whitespace-pre-wrap text-muted">
                      {aiText}
                    </ScrollShadow>
                  ) : null}
                </>
              ) : (
                <p className="mt-2 text-xs text-muted">Turn on a provider in Settings → AI (bring your own key).</p>
              )}
            </Popover.Dialog>
          </Popover.Content>
        </Popover>
        <div className="ml-auto flex items-center gap-2">
          <AppSelect ariaLabel="Row cap" value={rowCap} options={ROW_CAPS} onChange={setRowCap} size="sm" className="w-32" />
          <IconButton icon="history" label="Query history" active={showHistory} onPress={() => setShowHistory((v) => !v)} />
        </div>
      </div>
      <div className="flex min-h-0 flex-1">
        <div className="flex min-w-0 flex-1 flex-col">
          <div className="relative shrink-0 flex flex-col" style={{ height: editorHeight }}>
            {loaded ? <SqlEditor value={sql} onChange={onChange} onRun={() => void run(false)} engine={connection.engine} schema={schema} defaultSchema={defaultSchema} /> : null}
          </div>
          <Resizer direction="vertical" onResize={handleEditorResize} />
          <div className="min-h-0 flex-1">
            {plan ? (
              <div className="flex h-full min-h-0 flex-col">
                <div className="flex h-9 shrink-0 items-center gap-2 border-b border-border/40 glass-header px-3 text-xs">
                  <span className="font-semibold text-foreground tracking-tight">Execution plan</span>
                  <span className="ml-auto">
                    <CloseButton onPress={() => setPlan(null)} aria-label="Close plan" />
                  </span>
                </div>
                <div className="grid min-h-0 flex-1 grid-cols-2 gap-0">
                  <ScrollShadow className="overflow-x-auto border-r border-border/40 p-3">
                    <pre className="selectable font-mono text-[11px] text-foreground">{plan.plan}</pre>
                  </ScrollShadow>
                  <ScrollShadow className="selectable p-3 text-xs whitespace-pre-wrap text-muted">{plan.explanation ?? "Enable an AI provider in Settings to get a plain-language explanation of this plan."}</ScrollShadow>
                </div>
              </div>
            ) : (
              <ResultsPane outcome={outcome} />
            )}
          </div>
        </div>
        {showHistory ? (
          <div className="relative shrink-0 flex flex-col border-l border-border/40 glass-sidebar select-none" style={{ width: historyWidth }}>
            <Resizer direction="horizontal" onResize={handleHistoryResize} className="absolute -left-1 top-0 bottom-0" />
            <HistoryPanel connectionId={connection.id} refreshKey={historyKey} onPick={(picked) => onChange(picked)} />
          </div>
        ) : null}
      </div>

      <Modal isOpen={saveOpen} onOpenChange={setSaveOpen}>
        <Modal.Backdrop>
          <Modal.Container>
            <Modal.Dialog className="sm:max-w-[440px]">
              <Modal.CloseTrigger />
              <Modal.Header>
                <Modal.Heading>Save query</Modal.Heading>
              </Modal.Header>
              <Modal.Body className="flex flex-col gap-4">
                <Field label="Name" value={saveName} onChange={setSaveName} placeholder="Top customers" autoFocus />
                <Field label="Tags" optional value={saveTags} onChange={setSaveTags} placeholder="reports, finance" />
              </Modal.Body>
              <Modal.Footer>
                <Button variant="tertiary" onPress={() => setSaveOpen(false)}>Cancel</Button>
                <Button onPress={() => void doSave()} isDisabled={saveName.trim().length === 0}>Save</Button>
              </Modal.Footer>
            </Modal.Dialog>
          </Modal.Container>
        </Modal.Backdrop>
      </Modal>

      <Modal isOpen={confirm !== null} onOpenChange={(open) => !open && setConfirm(null)}>
        <Modal.Backdrop>
          <Modal.Container>
            <Modal.Dialog className="sm:max-w-[520px]">
              <Modal.CloseTrigger />
              <Modal.Header>
                <Modal.Icon className="bg-danger-soft text-danger">
                  <Icon name="trash" size={18} />
                </Modal.Icon>
                <Modal.Heading>Run destructive statements?</Modal.Heading>
              </Modal.Header>
              <Modal.Body className="space-y-3">
                <Alert status="danger" className="rounded-xl">
                  <Alert.Indicator />
                  <Alert.Content>
                    <Alert.Title className="font-semibold text-xs">Destructive Operations</Alert.Title>
                    <Alert.Description className="text-xs">
                      These statements change or remove data without a safety net. Review before continuing.
                    </Alert.Description>
                  </Alert.Content>
                </Alert>
                <ul className="flex flex-col gap-1.5">
                  {confirm?.statements.map((s) => (
                    <li key={s} className="selectable rounded-md bg-danger-soft px-2.5 py-1.5 font-mono text-[11px] text-danger">
                      {s}
                    </li>
                  ))}
                </ul>
              </Modal.Body>
              <Modal.Footer>
                <Button variant="tertiary" onPress={() => setConfirm(null)}>Cancel</Button>
                <Button variant="danger" onPress={() => void run(true)}>Run anyway</Button>
              </Modal.Footer>
            </Modal.Dialog>
          </Modal.Container>
        </Modal.Backdrop>
      </Modal>
      {aiBusy ? <span className="sr-only"><Spinner size="sm" /></span> : null}
    </div>
  );
}

function buildNamespace(
  connectionId: string,
  catalog: ReturnType<typeof useWorkspace.getState>["catalogs"][string] | undefined,
  columnsCache: ReturnType<typeof useWorkspace.getState>["columnsCache"],
): SQLNamespace {
  if (!catalog) return {};
  const out: Record<string, Record<string, string[]> | string[]> = {};
  for (const schema of catalog.schemas) {
    const tables: Record<string, string[]> = {};
    for (const table of schema.tables) {
      const key = `${connectionId}:${table.schema === null ? table.name : `${table.schema}.${table.name}`}`;
      tables[table.name] = (columnsCache[key] ?? []).map((c) => c.name);
    }
    if (schema.tables.some((t) => t.schema !== null)) {
      out[schema.name] = tables;
    } else {
      for (const [name, cols] of Object.entries(tables)) out[name] = cols;
    }
  }
  return out;
}
