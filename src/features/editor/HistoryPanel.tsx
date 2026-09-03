// SOT: history-panel, query-history-ui
import { useEffect, useState } from "react";
import type { HistoryEntry } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { formatMs } from "@/lib/format";
import { cn } from "@/lib/cn";

export function HistoryPanel({ connectionId, refreshKey, onPick }: { connectionId: string; refreshKey: number; onPick: (sql: string) => void }) {
  const [entries, setEntries] = useState<HistoryEntry[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const token = { cancelled: false };
    void (async () => {
      try {
        const rows = await ipc("list_history", { connectionId, origin: "user", limit: 100 });
        if (!token.cancelled) setEntries(rows);
      } catch (raw) {
        if (!token.cancelled) setError(normalizeError(raw).message);
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connectionId, refreshKey]);

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="flex h-9 shrink-0 items-center border-b border-border px-3 text-xs font-medium text-muted">History</div>
      {error ? <p className="p-3 text-xs text-danger">{error}</p> : null}
      <ul className="flex-1 overflow-y-auto">
        {entries.length === 0 ? <li className="p-3 text-xs text-muted">Nothing run yet.</li> : null}
        {entries.map((e) => (
          <li key={e.id}>
            <button type="button" onClick={() => onPick(e.sql)} className="flex w-full flex-col gap-0.5 border-b border-separator px-3 py-2 text-left hover:bg-surface-secondary" title={e.error ?? e.sql}>
              <span className="truncate font-mono text-[11px] text-foreground">{e.sql.replace(/\s+/g, " ")}</span>
              <span className="flex items-center gap-2 text-[10px] text-muted">
                <span className={cn(e.status === "ok" ? "text-success" : "text-danger")}>{e.status}</span>
                <span>{formatMs(e.elapsedMs)}</span>
                {e.rowCount !== null ? <span>{e.rowCount} rows</span> : null}
                <span className="ml-auto">{new Date(e.executedAt).toLocaleTimeString()}</span>
              </span>
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}
