// SOT: history-tab, query-history-grid, history-filters
import { useCallback, useEffect, useMemo, useState } from "react";
import { Button } from "@heroui/react";
import type { HistoryEntry, HistoryOrigin, Value } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { DENSITIES, formatCount } from "@/lib/format";
import { useWorkspace } from "@/stores/workspace";
import { DataGrid, type GridColumn } from "@/features/grid/DataGrid";
import { Segmented } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { EmptyState } from "@/components/global/EmptyState";
import { Icon } from "@/lib/icons";

type Filter = "all" | "user" | "system";

const COLUMNS: readonly GridColumn[] = [
  { name: "time", typeName: "timestamp" },
  { name: "status", typeName: "text" },
  { name: "origin", typeName: "text" },
  { name: "query", typeName: "text" },
  { name: "rows", typeName: "int" },
  { name: "ms", typeName: "int" },
  { name: "error", typeName: "text" },
];

// WHAT:  Full-tab history grid (All / System / User), click a row to reopen the SQL.
export function HistoryTab({ connectionId }: { connectionId: string }) {
  const density = useWorkspace((s) => s.density);
  const openQuery = useWorkspace((s) => s.openQuery);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const [filter, setFilter] = useState<Filter>("all");
  const [entries, setEntries] = useState<HistoryEntry[]>([]);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    const token = { cancelled: false };
    const origin: HistoryOrigin | null = filter === "all" ? null : filter;
    void (async () => {
      try {
        const rows = await ipc("list_history", { connectionId, origin, limit: 1000 });
        if (!token.cancelled) setEntries(rows);
      } catch (raw) {
        if (!token.cancelled) showError(normalizeError(raw));
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connectionId, filter, refresh, showError]);

  const rows = useMemo<Value[][]>(
    () =>
      entries.map((e) => [
        { t: "date_time", v: new Date(e.executedAt).toLocaleString() },
        { t: "text", v: e.status === "ok" ? "Success" : "Error" },
        { t: "text", v: e.origin },
        { t: "text", v: e.sql.replace(/\s+/g, " ") },
        e.rowCount === null ? { t: "null" } : { t: "int", v: e.rowCount },
        { t: "int", v: e.elapsedMs },
        e.error === null ? { t: "null" } : { t: "text", v: e.error },
      ]),
    [entries],
  );
  const getRow = useCallback((i: number) => rows[i], [rows]);

  const clear = async () => {
    if (!window.confirm("Clear query history for this connection?")) return;
    try {
      const n = await ipc("clear_history", { connectionId });
      showInfo(`Removed ${formatCount(n)} history entries.`);
      setRefresh((r) => r + 1);
    } catch (raw) {
      showError(normalizeError(raw));
    }
  };

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-border bg-surface px-2">
        <Segmented label="History filter" value={filter} onChange={setFilter} options={[{ value: "all", label: "All" }, { value: "system", label: "System" }, { value: "user", label: "User" }]} />
        <IconButton icon="trash" label="Clear history" onPress={() => void clear()} />
        <IconButton icon="refresh" label="Refresh" onPress={() => setRefresh((r) => r + 1)} />
        <span className="ml-auto text-xs text-muted">{formatCount(entries.length)} rows</span>
      </div>
      <div className="min-h-0 flex-1">
        {entries.length === 0 ? (
          <EmptyState icon="history" title="No history yet" body="Statements you run (and the queries the app runs for table pages) appear here." action={<Button size="sm" variant="secondary" onPress={() => openQuery(connectionId)}><Icon name="terminal" size={13} />New query</Button>} />
        ) : (
          <DataGrid
            columns={COLUMNS}
            rowCount={rows.length}
            getRow={getRow}
            rowHeight={DENSITIES[density].rowHeight}
            onCellSelect={(row) => {
              const entry = entries[row];
              if (entry) openQuery(connectionId, entry.sql);
            }}
          />
        )}
      </div>
    </div>
  );
}
