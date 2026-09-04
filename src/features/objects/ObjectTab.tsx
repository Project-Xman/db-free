// SOT: object-tab, object-definition-view, object-properties-sheet, object-actions, object-children, object-rows
import { useEffect, useMemo, useState } from "react";
import { Button, Chip, Modal, ScrollShadow, Spinner } from "@heroui/react";
import type { ObjectAction, ObjectDetail, ObjectRef } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { DENSITIES, formatCount } from "@/lib/format";
import { kindMeta } from "@/lib/objects";
import { Icon, typeIcon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Segmented } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { DataGrid } from "@/features/grid/DataGrid";
import { ObjectRow } from "./ObjectList";
import { XmlTree } from "@/features/tools/XmlTree";
import { cn } from "@/lib/cn";

type Part = "definition" | "properties" | "columns" | "rows" | "children" | "actions";

// WHAT:  One object (view, function, index, stream, topic, role, session…):
//        its definition source, property sheet, columns, tabular payload,
//        nested objects and actions. Actions run through execute_query so the
//        read-only lock, destructive confirmation and history log apply.
// WHERE: src-tauri/src/model/objects.rs (ObjectDetail), src/features/objects/ObjectsPanel.tsx
export function ObjectTab({ connectionId, reference }: { connectionId: string; reference: ObjectRef }) {
  const density = useWorkspace((s) => s.density);
  const openQuery = useWorkspace((s) => s.openQuery);
  const invalidateObjects = useWorkspace((s) => s.invalidateObjects);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const [detail, setDetail] = useState<ObjectDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [part, setPart] = useState<Part | null>(null);
  const [confirming, setConfirming] = useState<ObjectAction | null>(null);
  const [running, setRunning] = useState(false);
  const meta = kindMeta(reference.kind);

  // `tick` is the reload trigger (Refresh, or after an action ran); the
  // effect fetches once per value and drops answers that arrive after a change.
  const [tick, setTick] = useState(0);
  const reload = () => {
    setLoading(true);
    setTick((t) => t + 1);
  };

  useEffect(() => {
    const token = { cancelled: false };
    void (async () => {
      try {
        const next = await ipc("load_object", { connectionId, reference });
        if (!token.cancelled) {
          setDetail(next);
          setError(null);
        }
      } catch (raw) {
        if (!token.cancelled) setError(normalizeError(raw).message);
      } finally {
        if (!token.cancelled) setLoading(false);
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connectionId, reference, tick]);

  const parts = useMemo<{ value: Part; label: string }[]>(() => {
    if (!detail) return [];
    const out: { value: Part; label: string }[] = [];
    if (detail.definition !== null) out.push({ value: "definition", label: "Definition" });
    if (detail.properties.length > 0) out.push({ value: "properties", label: "Properties" });
    if (detail.columns.length > 0) out.push({ value: "columns", label: `Columns · ${detail.columns.length}` });
    if (detail.rows !== null) out.push({ value: "rows", label: `Data · ${formatCount(detail.rows.rows.length)}` });
    if (detail.children.length > 0) out.push({ value: "children", label: `Contains · ${detail.children.length}` });
    if (detail.actions.length > 0) out.push({ value: "actions", label: "Actions" });
    return out;
  }, [detail]);
  const current: Part | null = part !== null && parts.some((p) => p.value === part) ? part : (parts[0]?.value ?? null);

  const run = async (action: ObjectAction) => {
    setRunning(true);
    try {
      const outcome = await ipc("execute_query", { connectionId, sql: action.statement, confirmDestructive: action.destructive, maxRows: 200 });
      showInfo(`${action.label}: done in ${outcome.elapsedMs} ms.`);
      invalidateObjects(connectionId);
      window.dispatchEvent(new Event("db-free:refresh-tables"));
      setTick((t) => t + 1);
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setRunning(false);
      setConfirming(null);
    }
  };

  const copyDefinition = async () => {
    if (!detail?.definition) return;
    try {
      await navigator.clipboard.writeText(detail.definition);
      showInfo("Definition copied.");
    } catch {
      showError("Clipboard is not available.");
    }
  };

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-border/40 glass-header px-3">
        <Icon name={meta.icon} size={15} className="shrink-0 text-accent" />
        <span className="truncate text-sm font-semibold tracking-tight text-foreground">{reference.name}</span>
        <Chip size="sm" variant="soft" className="font-mono text-[10px]">
          {meta.label.toLowerCase()}
        </Chip>
        {reference.parent ? (
          <Chip size="sm" variant="soft" className="font-mono text-[10px] text-muted">
            {reference.parent}
          </Chip>
        ) : null}
        <span className="ml-auto flex items-center gap-0.5">
          {detail?.definition ? <IconButton icon="terminal" label="Open definition in a query tab" onPress={() => openQuery(connectionId, detail.definition ?? "", reference.name)} /> : null}
          {detail?.definition ? <IconButton icon="copy" label="Copy definition" onPress={() => void copyDefinition()} /> : null}
          <IconButton icon="refresh" label="Reload" onPress={reload} />
        </span>
      </div>

      {parts.length > 1 ? (
        <div className="shrink-0 px-3 py-2">
          <Segmented label="Object view" value={current ?? "definition"} onChange={setPart} options={parts} />
        </div>
      ) : null}

      <div className="min-h-0 flex-1">
        {loading && detail === null ? (
          <div className="flex h-full items-center justify-center gap-2 text-xs text-muted">
            <Spinner size="sm" /> loading…
          </div>
        ) : error !== null ? (
          <EmptyState icon="alert" title="Could not load this object" body={error} action={<Button size="sm" onPress={reload}>Retry</Button>} />
        ) : detail === null || current === null ? (
          <EmptyState icon={meta.icon} title={reference.name} body={`The adapter reports nothing more about this ${meta.label.toLowerCase()}.`} />
        ) : current === "definition" ? (
          detail.language === "xml" ? (
            <ScrollShadow className="h-full p-3">
              <XmlTree source={detail.definition ?? ""} />
            </ScrollShadow>
          ) : (
            <ScrollShadow className="h-full">
              <pre className={cn("selectable min-h-full p-4 font-mono text-[12px] leading-relaxed whitespace-pre-wrap break-words text-foreground", detail.language === "json" ? "text-syntax-string" : "")}>{detail.definition}</pre>
            </ScrollShadow>
          )
        ) : current === "properties" ? (
          <ScrollShadow className="h-full">
            <dl className="grid grid-cols-[minmax(140px,max-content)_1fr] gap-x-6 gap-y-0 p-4 text-xs">
              {detail.properties.map((p) => (
                <div key={p.name} className="contents">
                  <dt className="border-b border-separator py-1.5 font-medium text-muted">{p.name}</dt>
                  <dd className="selectable min-w-0 border-b border-separator py-1.5 font-mono break-words text-foreground">{p.value}</dd>
                </div>
              ))}
            </dl>
          </ScrollShadow>
        ) : current === "columns" ? (
          <ScrollShadow className="h-full">
            <ul className="p-3 text-xs">
              {detail.columns.map((c) => (
                <li key={c.name} className="flex h-7 items-center gap-2 border-b border-separator px-1">
                  <Icon name={typeIcon(c.dataType, c.primaryKey)} size={12} className={c.primaryKey ? "text-warning" : "text-muted"} />
                  <span className="font-medium text-foreground">{c.name}</span>
                  <span className="font-mono text-[11px] text-muted">{c.dataType}</span>
                  {!c.nullable ? <Chip size="sm" variant="soft" className="h-4 px-1 text-[9px]">not null</Chip> : null}
                  <span className="ml-auto font-mono text-[10px] text-muted/60">#{c.ordinal}</span>
                </li>
              ))}
            </ul>
          </ScrollShadow>
        ) : current === "rows" && detail.rows !== null ? (
          detail.rows.columns.length === 0 ? (
            <EmptyState title="No rows" />
          ) : (
            <DataGrid columns={detail.rows.columns.map((c) => ({ name: c.name, typeName: c.typeName }))} rowCount={detail.rows.rows.length} getRow={(i) => detail.rows?.rows[i]} rowHeight={DENSITIES[density].rowHeight} />
          )
        ) : current === "children" ? (
          <ScrollShadow className="h-full p-2">
            {detail.children.map((child) => (
              <ObjectRow key={`${child.reference.kind}:${child.reference.parent ?? ""}:${child.reference.name}`} connectionId={connectionId} object={child} />
            ))}
          </ScrollShadow>
        ) : (
          <ScrollShadow className="h-full p-4">
            <ul className="flex max-w-xl flex-col gap-2">
              {detail.actions.map((action) => (
                <li key={action.id} className="flex items-center gap-3 rounded-xl glass-card border-border/40 p-3">
                  <div className="min-w-0 flex-1">
                    <div className="text-sm font-medium text-foreground">{action.label}</div>
                    <code className="block truncate font-mono text-[11px] text-muted" title={action.statement}>
                      {action.statement}
                    </code>
                  </div>
                  <Button size="sm" variant={action.destructive ? "danger-soft" : "secondary"} isDisabled={running} onPress={() => (action.destructive ? setConfirming(action) : void run(action))}>
                    {action.destructive ? <Icon name="alert" size={12} /> : <Icon name="play" size={12} />}
                    {action.destructive ? "Run…" : "Run"}
                  </Button>
                </li>
              ))}
            </ul>
          </ScrollShadow>
        )}
      </div>

      <Modal isOpen={confirming !== null} onOpenChange={(o) => !o && setConfirming(null)}>
        <Modal.Backdrop>
          <Modal.Container>
            <Modal.Dialog className="sm:max-w-[520px]">
              <Modal.CloseTrigger />
              <Modal.Header>
                <Modal.Heading className="flex items-center gap-2">
                  <Icon name="alert" size={15} className="text-danger" />
                  {confirming?.label}
                </Modal.Heading>
              </Modal.Header>
              <Modal.Body>
                <p className="text-sm text-muted">This cannot be undone. The following statement will run on the server:</p>
                <pre className="selectable mt-2 rounded-lg glass-card p-3 font-mono text-[11px] whitespace-pre-wrap text-foreground">{confirming?.statement}</pre>
              </Modal.Body>
              <Modal.Footer>
                <Button variant="tertiary" onPress={() => setConfirming(null)}>
                  Cancel
                </Button>
                <Button variant="danger" isDisabled={running} onPress={() => confirming && void run(confirming)}>
                  {running ? <Spinner size="sm" /> : null}
                  {confirming?.label}
                </Button>
              </Modal.Footer>
            </Modal.Dialog>
          </Modal.Container>
        </Modal.Backdrop>
      </Modal>
    </div>
  );
}
