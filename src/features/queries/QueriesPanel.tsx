// SOT: queries-panel, saved-queries-sidebar, recent-queries
import { useEffect, useMemo, useState } from "react";
import type { HistoryEntry } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { useActiveConnection, useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Segmented } from "@/components/global/Field";
import { Icon } from "@/lib/icons";
import { isMac } from "@/components/global/Kbd";
import { cn } from "@/lib/cn";
import { ConnectionSwitcher } from "@/features/shell/ConnectionSwitcher";

// WHAT:  Sidebar of saved queries (A-Z / Z-A) plus the most recent distinct
//        statements from history. Click opens a new query tab seeded with the SQL.
export function QueriesPanel() {
  const connection = useActiveConnection();
  const savedQueries = useWorkspace((s) => s.savedQueries);
  const loadSavedQueries = useWorkspace((s) => s.loadSavedQueries);
  const deleteSavedQuery = useWorkspace((s) => s.deleteSavedQuery);
  const openQuery = useWorkspace((s) => s.openQuery);
  const showError = useWorkspace((s) => s.showError);
  const [order, setOrder] = useState<"az" | "za">("az");
  const [recent, setRecent] = useState<HistoryEntry[]>([]);

  useEffect(() => {
    if (!connection) return;
    const token = { cancelled: false };
    void (async () => {
      try {
        const rows = await ipc("list_history", { connectionId: connection.id, origin: "user", limit: 200 });
        if (token.cancelled) return;
        const seen = new Set<string>();
        setRecent(rows.filter((r) => (seen.has(r.sql) ? false : (seen.add(r.sql), true))).slice(0, 8));
      } catch (raw) {
        if (!token.cancelled) showError(normalizeError(raw));
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connection, showError]);

  const sorted = useMemo(() => {
    const list = savedQueries.filter((q) => q.connectionId === null || q.connectionId === connection?.id);
    return [...list].sort((a, b) => (order === "az" ? a.name.localeCompare(b.name) : b.name.localeCompare(a.name)));
  }, [savedQueries, order, connection]);

  if (!connection) return null;
  const cid = connection.id;

  return (
    <aside className="flex w-[280px] shrink-0 flex-col border-r border-border bg-surface">
      <div className={cn("drag-region flex h-10 shrink-0 items-center gap-1 pr-2", isMac() ? "pl-9" : "pl-3")} data-tauri-drag-region>
        <ConnectionSwitcher caption="Saved queries" />
        <div className="drag-region h-full min-w-4 flex-1" data-tauri-drag-region />
        <span className="flex items-center">
          <IconButton icon="refresh" label="Refresh" onPress={() => void loadSavedQueries()} />
          <IconButton icon="plus" label="New query" onPress={() => openQuery(cid)} />
        </span>
      </div>
      <div className="px-3 pb-2">
        <Segmented label="Sort" value={order} onChange={setOrder} options={[{ value: "az", label: "A-Z" }, { value: "za", label: "Z-A" }]} />
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        {sorted.length === 0 ? <p className="px-3 py-2 text-xs text-muted">No saved queries yet. Use Save in a query tab.</p> : null}
        {sorted.map((q) => (
          <div key={q.id} className="group flex h-8 items-center gap-2 pr-2 pl-3 text-[13px] text-muted hover:bg-surface-secondary hover:text-foreground">
            <button type="button" onClick={() => openQuery(cid, q.sql, q.name)} className="flex min-w-0 flex-1 items-center gap-2 text-left" title={q.sql}>
              <Icon name="file" size={13} className="shrink-0" />
              <span className="truncate">{q.name}</span>
              {q.tags.length > 0 ? <span className="ml-auto truncate text-[10px] text-muted">{q.tags.join(", ")}</span> : null}
            </button>
            <span className="opacity-0 group-hover:opacity-100">
              <IconButton
                icon="trash"
                label="Delete saved query"
                onPress={() => {
                  void (async () => {
                    try {
                      await deleteSavedQuery(q.id);
                    } catch (raw) {
                      showError(normalizeError(raw));
                    }
                  })();
                }}
              />
            </span>
          </div>
        ))}
        <div className="mt-3 px-3 pb-1 text-xs font-medium text-muted">Recent</div>
        {recent.length === 0 ? <p className="px-3 py-1 text-xs text-muted">Nothing run yet.</p> : null}
        {recent.map((r) => (
          <button key={r.id} type="button" onClick={() => openQuery(cid, r.sql)} className="flex h-8 w-full items-center gap-2 px-3 text-left text-[12px] text-muted hover:bg-surface-secondary hover:text-foreground" title={r.sql}>
            <Icon name="file" size={13} className="shrink-0" />
            <span className="truncate font-mono">{r.sql.replace(/\s+/g, " ")}</span>
          </button>
        ))}
      </div>
    </aside>
  );
}
