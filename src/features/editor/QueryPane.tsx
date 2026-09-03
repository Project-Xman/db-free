// SOT: query-pane, run-query-flow, destructive-confirm-flow, buffer-autosave, save-query-flow, ai-assist-flow, explain-flow, format-sql
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button, Modal, Popover, Spinner, TextArea, TextField } from "@heroui/react";
import { format as formatSql } from "sql-formatter";
import type { AppError, ConnectionSummary, PlanReport, QueryOutcome } from "@/lib/bindings";
import type { SQLNamespace } from "@codemirror/lang-sql";
import { ipc, normalizeError } from "@/lib/ipc";
import { engineMeta } from "@/lib/engines";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Field } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { RunShortcut } from "@/components/global/Kbd";
import { Icon } from "@/lib/icons";
import { SqlEditor } from "./SqlEditor";
import { ResultsPane } from "./ResultsPane";
import { HistoryPanel } from "./HistoryPanel";

const ROW_CAPS = [
  { value: "500", label: "500 rows" },
  { value: "1000", label: "1,000 rows" },
  { value: "5000", label: "5,000 rows" },
  { value: "20000", label: "20,000 rows" },
] satisfies readonly { value: string; label: string }[];

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
  const [rowCap, setRowCap] = useState<(typeof ROW_CAPS)[number]["value"]>("1000");
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
  const [plan, setPlan] = useState<PlanReport | null>(null);
  const [planBusy, setPlanBusy] = useState(false);
  const saveTimer = useRef<number | null>(null);

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
      } catch (raw) {
        const error: AppError = normalizeError(raw);
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

  const askAi = async () => {
    if (aiPrompt.trim().length === 0) return;
    setAiBusy(true);
    setAiText(null);
    try {
      const reply = await ipc("ai_generate", { connectionId: connection.id, prompt: aiPrompt });
      setAiText(reply.text);
      if (reply.sql !== null) onChange(sql.trim().length > 0 ? `${sql.trimEnd()}\n\n${reply.sql}` : reply.sql);
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
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-border bg-surface px-2">
        <Button size="sm" isPending={running} onPress={() => void run(false)} isDisabled={!loaded || sql.trim().length === 0}>
          <Icon name="play" size={12} />
          Run
        </Button>
        <RunShortcut />
        {isSql ? <Button size="sm" variant="ghost" className="text-muted" onPress={doFormat} isDisabled={sql.trim().length === 0}>Format</Button> : null}
        {isSql ? (
          <Button size="sm" variant="ghost" className="text-muted" isPending={planBusy} onPress={() => void explain()} isDisabled={sql.trim().length === 0}>
            Explain
          </Button>
        ) : null}
        <Button size="sm" variant="ghost" className="text-muted" onPress={() => setSaveOpen(true)} isDisabled={sql.trim().length === 0}>
          Save
        </Button>
        <Popover isOpen={aiOpen} onOpenChange={setAiOpen}>
          <Button size="sm" variant={aiEnabled ? "secondary" : "ghost"} className={aiEnabled ? "" : "text-muted"}>
            <Icon name="braces" size={12} />
            AI
          </Button>
          <Popover.Content className="w-[460px]">
            <Popover.Dialog>
              <Popover.Heading className="text-sm">Ask in plain language</Popover.Heading>
              {aiEnabled ? (
                <>
                  <TextField value={aiPrompt} onChange={setAiPrompt} className="mt-2 w-full" aria-label="Prompt">
                    <TextArea placeholder="e.g. customers who ordered more than 3 times last month" rows={3} className="w-full" />
                  </TextField>
                  <div className="mt-2 flex items-center gap-2">
                    <Button size="sm" isPending={aiBusy} onPress={() => void askAi()} isDisabled={aiPrompt.trim().length === 0}>
                      Generate {engineMeta(connection.engine).commandLanguage}
                    </Button>
                    <span className="text-[11px] text-muted">Schema names and your prompt are sent to {settings.ai.provider}.</span>
                  </div>
                  {aiText !== null ? <p className="selectable mt-3 max-h-40 overflow-y-auto rounded-md bg-surface-secondary p-2 text-xs whitespace-pre-wrap text-muted">{aiText}</p> : null}
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
          <div className="h-[42%] min-h-[120px] border-b border-border">
            {loaded ? <SqlEditor value={sql} onChange={onChange} onRun={() => void run(false)} engine={connection.engine} schema={schema} defaultSchema={defaultSchema} /> : null}
          </div>
          <div className="min-h-0 flex-1">
            {plan ? (
              <div className="flex h-full min-h-0 flex-col">
                <div className="flex h-9 shrink-0 items-center gap-2 border-b border-border bg-surface px-3 text-xs">
                  <span className="font-medium text-foreground">Execution plan</span>
                  <span className="ml-auto">
                    <IconButton icon="x" label="Close plan" onPress={() => setPlan(null)} />
                  </span>
                </div>
                <div className="grid min-h-0 flex-1 grid-cols-2 gap-0">
                  <pre className="selectable overflow-auto border-r border-border p-3 font-mono text-[11px] text-foreground">{plan.plan}</pre>
                  <div className="selectable overflow-auto p-3 text-xs whitespace-pre-wrap text-muted">{plan.explanation ?? "Enable an AI provider in Settings to get a plain-language explanation of this plan."}</div>
                </div>
              </div>
            ) : (
              <ResultsPane outcome={outcome} />
            )}
          </div>
        </div>
        {showHistory ? (
          <div className="w-72 shrink-0 border-l border-border bg-surface">
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
              <Modal.Body>
                <p className="text-sm text-muted">These statements change or remove data without a safety net. Review before continuing.</p>
                <ul className="mt-3 flex flex-col gap-1.5">
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
