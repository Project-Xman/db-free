// SOT: queries-panel, saved-queries-sidebar, recent-queries
import { useEffect, useMemo, useState } from "react";
import { Button, Chip, ScrollShadow } from "@heroui/react";
import type { HistoryEntry } from "@/lib/bindings";
import { ipc } from "@/lib/ipc";
import { useActiveConnection, useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Segmented } from "@/components/global/Field";
import { Icon } from "@/lib/icons";
import { ConnectionSwitcher } from "@/features/shell/ConnectionSwitcher";

// WHAT:  Sidebar of saved queries (A-Z / Z-A) plus the most recent distinct
//        statements from history. Click opens a new query tab seeded with the SQL.
export function QueriesPanel() {
  const connection = useActiveConnection();
  const savedQueries = useWorkspace((s) => s.savedQueries);
  const loadSavedQueries = useWorkspace((s) => s.loadSavedQueries);
  const deleteSavedQuery = useWorkspace((s) => s.deleteSavedQuery);
  const openQuery = useWorkspace((s) => s.openQuery);
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
        const deduped: HistoryEntry[] = [];
        for (const r of rows) {
          const key = r.sql.trim();
          if (seen.has(key)) continue;
          seen.add(key);
          deduped.push(r);
          if (deduped.length >= 20) break;
        }
        setRecent(deduped);
      } catch {
        // history unavailable; not fatal
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connection]);

  const cid = connection?.id;
  const sorted = useMemo(() => {
    const copy = [...savedQueries];
    copy.sort((a, b) => a.name.localeCompare(b.name));
    if (order === "za") copy.reverse();
    return copy;
  }, [savedQueries, order]);

  if (!cid) return null;

  return (
    <aside className="flex h-full w-full flex-col glass-sidebar select-none">
      <div className="flex h-11 shrink-0 items-center justify-between border-b border-border/40 glass-header px-3">
        <ConnectionSwitcher caption="Saved queries" />
        <span className="flex items-center gap-0.5">
          <IconButton icon="refresh" label="Refresh" onPress={() => void loadSavedQueries()} />
          <IconButton icon="plus" label="New query" onPress={() => openQuery(cid)} />
        </span>
      </div>
      <div className="px-3 py-2">
        <Segmented label="Sort" value={order} onChange={setOrder} options={[{ value: "az", label: "A-Z" }, { value: "za", label: "Z-A" }]} />
      </div>
      <ScrollShadow className="min-h-0 flex-1 px-1.5 py-1">
        {sorted.length === 0 ? <p className="px-3 py-2 text-xs text-muted">No saved queries yet. Use Save in a query tab.</p> : null}
        {sorted.map((q) => (
          <div key={q.id} title={q.sql} className="group flex h-8 items-center gap-2 rounded-lg px-2 text-[12.5px] text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover">
            <Button
              variant="ghost"
              size="sm"
              onPress={() => openQuery(cid, q.sql, q.name)}
              className="flex h-auto min-w-0 flex-1 items-center justify-start gap-2 p-0 text-left bg-transparent hover:bg-transparent"
            >
              <Icon name="file" size={13} className="shrink-0 text-accent" />
              <span className="truncate">{q.name}</span>
              {q.tags.length > 0 ? (
                <Chip size="sm" variant="soft" className="ml-auto text-[10px]">
                  {q.tags.join(", ")}
                </Chip>
              ) : null}
            </Button>
            <span className="opacity-0 group-hover:opacity-100 transition-opacity">
              <IconButton
                icon="trash"
                label="Delete saved query"
                onPress={() => void deleteSavedQuery(q.id)}
              />
            </span>
          </div>
        ))}
        <div className="mt-3 px-3 pb-1 text-[10px] font-semibold uppercase tracking-wider text-muted/80">Recent</div>
        {recent.length === 0 ? <p className="px-3 py-1 text-xs text-muted">Nothing run yet.</p> : null}
        {recent.map((r) => (
          <div key={r.id} title={r.sql} className="w-full">
            <Button
              variant="ghost"
              size="sm"
              onPress={() => openQuery(cid, r.sql)}
              className="flex h-7.5 w-full items-center justify-start gap-2 rounded-lg px-2 text-left text-[11.5px] text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover"
            >
              <Icon name="file" size={12} className="shrink-0 opacity-60" />
              <span className="truncate font-mono">{r.sql.replace(/\s+/g, " ")}</span>
            </Button>
          </div>
        ))}
      </ScrollShadow>
    </aside>
  );
}
