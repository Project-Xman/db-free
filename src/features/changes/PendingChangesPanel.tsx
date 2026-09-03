// SOT: pending-changes-panel, review-mode-ui, commit-flow, visual-diff
import { useCallback, useEffect, useState } from "react";
import { Button, Card, Chip, CloseButton, Kbd, ScrollShadow } from "@heroui/react";
import type { ChangePreview, StagedChange, Value } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { formatCell } from "@/lib/format";
import { tableKey, useWorkspace } from "@/stores/workspace";
import { Segmented } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { Resizer } from "@/components/global/Resizer";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

// Stable empty list: a selector must return the same reference for unchanged state.
const EMPTY_CHANGES: StagedChange[] = [];

// WHAT:  Right-hand "Pending Changes" drawer: visual diff per staged change, the
//        exact SQL script, Commit All (⌘S) and Clear All.
// WHY:   PRD §4.2 staged commits; the SQL comes from Rust so what you see is
//        what runs, inside one transaction.
// WHERE: src-tauri/src/services/changes.rs
export function PendingChangesPanel({ connectionId }: { connectionId: string }) {
  const changes = useWorkspace((s) => s.pendingChanges[connectionId] ?? EMPTY_CHANGES);
  const unstage = useWorkspace((s) => s.unstageChange);
  const clearChanges = useWorkspace((s) => s.clearChanges);
  const setOpen = useWorkspace((s) => s.setChangesPanelOpen);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const [view, setView] = useState<"visual" | "sql">("visual");
  const [loadedPreview, setPreview] = useState<ChangePreview | null>(null);
  const preview = changes.length === 0 ? null : loadedPreview;
  const [committing, setCommitting] = useState(false);
  const [width, setWidth] = useState<number>(() => {
    try {
      const saved = localStorage.getItem("db-free:changes-panel-width");
      return saved ? Math.max(280, Math.min(750, Number(saved))) : 400;
    } catch {
      return 400;
    }
  });

  const handleResize = useCallback((delta: number) => {
    setWidth((prev) => {
      // Dragging left (negative delta) expands panel width
      const next = Math.max(280, Math.min(750, prev - delta));
      try {
        localStorage.setItem("db-free:changes-panel-width", String(next));
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  useEffect(() => {
    const token = { cancelled: false };
    if (changes.length === 0) return;
    void (async () => {
      try {
        const p = await ipc("preview_changes", { connectionId, changes });
        if (!token.cancelled) setPreview(p);
      } catch (raw) {
        if (!token.cancelled) showError(normalizeError(raw));
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [changes, connectionId, showError]);

  const commit = async () => {
    if (changes.length === 0) return;
    setCommitting(true);
    try {
      await ipc("commit_changes", { connectionId, changes });
      clearChanges(connectionId);
      showInfo(`Committed ${changes.length} change(s).`);
      window.dispatchEvent(new CustomEvent("db-free:refresh-tables"));
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setCommitting(false);
    }
  };

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") {
        e.preventDefault();
        void commit();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- commit reads latest state via closure each render
  }, [changes, connectionId]);

  return (
    <aside className="relative flex shrink-0 flex-col glass-sidebar select-none" style={{ width }}>
      <Resizer direction="horizontal" onResize={handleResize} className="absolute -left-1 top-0 bottom-0" />
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-border/40 glass-header px-3">
        <span className="text-sm font-semibold text-foreground tracking-tight">Pending Changes</span>
        <span className="ml-auto">
          <CloseButton onPress={() => setOpen(false)} aria-label="Hide panel" />
        </span>
      </div>
      <div className="px-3 py-2">
        <Segmented label="Changes view" value={view} onChange={setView} options={[{ value: "visual", label: "Visual" }, { value: "sql", label: "SQL" }]} />
      </div>
      <ScrollShadow className="min-h-0 flex-1 px-3 pb-3">
        {changes.length === 0 ? (
          <p className="py-8 text-center text-xs text-muted">No pending changes. Double-click a cell to edit, use Insert, or select rows and Delete.</p>
        ) : view === "sql" ? (
          <pre className="selectable rounded-xl glass-card p-3.5 font-mono text-[11px] leading-relaxed whitespace-pre-wrap text-foreground">{preview?.script ?? "…"}</pre>
        ) : (
          <ul className="flex flex-col gap-2.5">
            {changes.map((c) => (
              <li key={c.id}>
                <Card className="rounded-xl glass-card border-border/40 p-3 shadow-xs">
                  <Card.Content className="p-0">
                    <div className="flex items-center gap-2 text-xs">
                      <Chip size="sm" variant="soft" color={c.kind === "update" ? "warning" : c.kind === "insert" ? "success" : "danger"} className="font-bold text-[10px] size-5 min-w-5 p-0 justify-center">
                        {c.kind === "update" ? "U" : c.kind === "insert" ? "I" : "D"}
                      </Chip>
                      <span className="truncate font-medium text-foreground">{tableKey(c.table)}</span>
                      <Icon name="chevron-right" size={11} className="text-muted" />
                      <span className="truncate text-muted">{describeKey(c)}</span>
                      {c.kind === "update" ? (
                        <>
                          <Icon name="chevron-right" size={11} className="text-muted" />
                          <span className="truncate text-muted">{c.column}</span>
                        </>
                      ) : null}
                      <span className="ml-auto">
                        <IconButton icon="refresh" label="Undo this change" onPress={() => unstage(connectionId, c.id)} />
                      </span>
                    </div>
                    <div className="mt-2 flex flex-col gap-1 font-mono text-[11px]">
                      {c.kind === "update" ? (
                        <>
                          <Diff sign="-" tone="danger" text={cell(c.old)} />
                          <Diff sign="+" tone="success" text={cell(c.new)} />
                        </>
                      ) : c.kind === "insert" ? (
                        c.values.map((v) => <Diff key={v.column} sign="+" tone="success" text={`${v.column} = ${cell(v.value)}`} />)
                      ) : (
                        <Diff sign="-" tone="danger" text={describeKey(c)} />
                      )}
                    </div>
                  </Card.Content>
                </Card>
              </li>
            ))}
          </ul>
        )}
      </ScrollShadow>
      <div className="flex shrink-0 items-center gap-2 border-t border-border/40 glass-header p-3">
        <Button size="sm" variant="ghost" className="rounded-lg text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover" onPress={() => clearChanges(connectionId)} isDisabled={changes.length === 0}>
          Clear All
        </Button>
        <Button
          size="sm"
          className="flex-1 rounded-xl glass-pill bg-accent text-accent-foreground font-semibold shadow-xs liquid-hover"
          isPending={committing}
          onPress={() => void commit()}
          isDisabled={changes.length === 0}
        >
          Commit All ({changes.length})
          <Kbd className="ml-1 text-[10px]">
            <Kbd.Abbr keyValue="command" />
            <Kbd.Content>S</Kbd.Content>
          </Kbd>
        </Button>
      </div>
    </aside>
  );
}

function Diff({ sign, tone, text }: { sign: string; tone: "danger" | "success"; text: string }) {
  return (
    <div className={cn("selectable flex gap-2 rounded px-2 py-1", tone === "danger" ? "bg-danger-soft text-danger" : "bg-success-soft text-success")}>
      <span>{sign}</span>
      <span className="truncate">{text}</span>
    </div>
  );
}

function cell(v: Value): string {
  return v.t === "null" ? "NULL" : formatCell(v).text;
}

function describeKey(c: StagedChange): string {
  if (c.kind === "insert") return "new row";
  return c.key.map((k) => `${k.column}=${cell(k.value)}`).join(", ") || "row";
}
