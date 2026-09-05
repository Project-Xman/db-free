// SOT: workflow-tab, workflow-steps, workflow-runner-ui
import { useState } from "react";
import { Alert, Button, Card, Chip, Input, ScrollShadow } from "@heroui/react";
import type { Document, WorkflowBody, WorkflowRunReport, WorkflowStep } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { formatMs } from "@/lib/format";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Field, Toggle } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { SqlEditor } from "@/features/editor/SqlEditor";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

let stepCounter = 0;

// WHAT:  Workflow = ordered SQL steps, each on a chosen connection; Run executes
//        them through the guard in order and shows a per-step report.
// WHERE: src-tauri/src/commands/workflows.rs
export function WorkflowTab({ document: doc }: { document: Document }) {
  const saveDocument = useWorkspace((s) => s.saveDocument);
  const connections = useWorkspace((s) => s.connections);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const initial: WorkflowBody = doc.body.kind === "workflow" ? doc.body.data : { steps: [] };
  const [steps, setSteps] = useState<WorkflowStep[]>(initial.steps);
  const [name, setName] = useState(doc.name);
  const [connectionId, setConnectionId] = useState<string>(doc.connectionId ?? "");
  const [dirty, setDirty] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(initial.steps[0]?.id ?? null);
  const [report, setReport] = useState<WorkflowRunReport | null>(null);
  const [running, setRunning] = useState(false);

  const patchStep = (id: string, partial: Partial<WorkflowStep>) => {
    setSteps((s) => s.map((x) => (x.id === id ? { ...x, ...partial } : x)));
    setDirty(true);
  };
  const addStep = () => {
    stepCounter += 1;
    const step: WorkflowStep = { id: `step-${Date.now().toString(36)}-${stepCounter}`, name: `Step ${steps.length + 1}`, connectionId: null, sql: "", stopOnError: true };
    setSteps((s) => [...s, step]);
    setSelectedId(step.id);
    setDirty(true);
  };
  const move = (i: number, dir: -1 | 1) => {
    const j = i + dir;
    if (j < 0 || j >= steps.length) return;
    const copy = [...steps];
    const [item] = copy.splice(i, 1);
    if (item) copy.splice(j, 0, item);
    setSteps(copy);
    setDirty(true);
  };

  const save = async (): Promise<boolean> => {
    try {
      await saveDocument({ ...doc, name, connectionId: connectionId.length > 0 ? connectionId : null, body: { kind: "workflow", data: { steps } } });
      setDirty(false);
      return true;
    } catch (raw) {
      showError(normalizeError(raw));
      return false;
    }
  };

  const run = async () => {
    if (dirty && !(await save())) return;
    setRunning(true);
    try {
      const r = await ipc("run_workflow", { id: doc.id });
      setReport(r);
      showInfo(r.stoppedEarly ? "Workflow stopped on an error." : `Workflow finished: ${r.steps.length} step(s).`);
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setRunning(false);
    }
  };

  const selected = steps.find((s) => s.id === selectedId) ?? null;
  const engine = connections.find((c) => c.id === (selected?.connectionId ?? connectionId))?.engine ?? "postgres";
  const connOptions = [{ value: "", label: "— pick a connection —" }, ...connections.map((c) => ({ value: c.id, label: c.name }))];

  return (
    <div className="flex h-full min-h-0">
      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex app-toolbar shrink-0 items-center gap-2 border-b border-border bg-surface ">
          <Input
            value={name}
            onChange={(e) => {
              setName(e.target.value);
              setDirty(true);
            }}
            className="h-7 w-48 rounded-md bg-transparent text-sm text-foreground"
            aria-label="Workflow name"
          />
          <AppSelect ariaLabel="Default connection" value={connectionId} options={connOptions} onChange={(v) => { setConnectionId(v); setDirty(true); }} size="sm" className="w-56" icon="database" />
          <Button size="sm" isPending={running} onPress={() => void run()} isDisabled={steps.length === 0}>
            <Icon name="play" size={12} />
            Run
          </Button>
          <div className="ml-auto flex items-center gap-1">
            <Button size="sm" onPress={() => void save()} isDisabled={!dirty}>Save{dirty ? " *" : ""}</Button>
          </div>
        </div>
        <ScrollShadow className="min-h-0 flex-1 p-6">
          <div className="mx-auto flex w-full max-w-[560px] flex-col items-center gap-0">
            {steps.map((step, i) => {
              const result = report?.steps.find((r) => r.stepId === step.id);
              return (
                <div key={step.id} className="flex w-full flex-col items-center">
                  <Card
                    role="button"
                    tabIndex={0}
                    onClick={() => setSelectedId(step.id)}
                    onKeyDown={(e) => { if (e.key === "Enter") setSelectedId(step.id); }}
                    className={cn("w-full cursor-pointer transition-all border glass-card", selectedId === step.id ? "border-accent ring-1 ring-accent/30" : "border-border/40 hover:border-border")}
                  >
                    <Card.Content className="flex items-center gap-3 p-3 w-full">
                      <span className="flex size-7 items-center justify-center rounded-full bg-surface-tertiary text-xs text-muted shrink-0">{i + 1}</span>
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-sm font-medium text-foreground">{step.name}</span>
                        <span className="block truncate font-mono text-[11px] text-muted">{step.sql.replace(/\s+/g, " ").slice(0, 80) || "no SQL yet"}</span>
                      </span>
                      {result ? (
                        <Chip size="sm" color={result.ok ? "success" : "danger"} variant="soft">{result.ok ? `ok · ${formatMs(result.elapsedMs)}${result.rows !== null ? ` · ${result.rows} rows` : ""}` : "error"}</Chip>
                      ) : null}
                      <span className="flex flex-col">
                        <IconButton icon="arrow-up" label="Move up" isDisabled={i === 0} onPress={() => move(i, -1)} size={12} />
                        <IconButton icon="arrow-down" label="Move down" isDisabled={i === steps.length - 1} onPress={() => move(i, 1)} size={12} />
                      </span>
                    </Card.Content>
                  </Card>
                  {result && !result.ok && result.error ? (
                    <Alert status="danger" className="mt-1 w-full rounded-xl text-xs">
                      <Alert.Indicator />
                      <Alert.Content>
                        <Alert.Description className="selectable font-mono text-[11px]">{result.error}</Alert.Description>
                      </Alert.Content>
                    </Alert>
                  ) : null}
                  <div className="h-6 w-px bg-border/40" />
                </div>
              );
            })}
            <Button
              isIconOnly
              onPress={addStep}
              className="flex size-14 min-w-14 items-center justify-center rounded-full bg-accent text-accent-foreground shadow-lg hover:brightness-110"
              aria-label="Add step"
            >
              <Icon name="plus" size={22} />
            </Button>
          </div>
        </ScrollShadow>
      </div>
      {selected ? (
        <aside className="flex w-[420px] shrink-0 flex-col border-l border-border bg-surface">
          <div className="flex flex-col gap-3 border-b border-border px-3 py-3">
            <Field label="Step name" value={selected.name} onChange={(v) => patchStep(selected.id, { name: v })} />
            <AppSelect label="Connection" value={selected.connectionId ?? ""} options={[{ value: "", label: "Workflow default" }, ...connections.map((c) => ({ value: c.id, label: c.name }))]} onChange={(v) => patchStep(selected.id, { connectionId: v.length > 0 ? v : null })} />
            <Toggle checked={selected.stopOnError} onChange={(v) => patchStep(selected.id, { stopOnError: v })} label="Stop workflow if this step fails" />
          </div>
          <div className="min-h-0 flex-1">
            <SqlEditor value={selected.sql} onChange={(sql) => patchStep(selected.id, { sql })} onRun={() => void run()} engine={engine} schema={{}} />
          </div>
          <div className="flex items-center border-t border-border px-3 py-2">
            <Button size="sm" variant="danger-soft" className="ml-auto" onPress={() => { setSteps((s) => s.filter((x) => x.id !== selected.id)); setSelectedId(null); setDirty(true); }}>
              <Icon name="trash" size={12} />
              Delete step
            </Button>
          </div>
        </aside>
      ) : null}
    </div>
  );
}
