// SOT: history-panel, query-history-ui
import { useEffect, useState } from "react";
import { Alert, Button, Chip, ScrollShadow } from "@heroui/react";
import type { HistoryEntry } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { formatMs } from "@/lib/format";

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
      <div className="flex h-9 shrink-0 items-center border-b border-border/40 px-3 text-xs font-semibold text-muted tracking-tight">History</div>
      {error ? (
        <Alert status="danger" className="m-2 rounded-lg text-xs">
          <Alert.Indicator />
          <Alert.Content>
            <Alert.Title>History Error</Alert.Title>
            <Alert.Description>{error}</Alert.Description>
          </Alert.Content>
        </Alert>
      ) : null}
      <ScrollShadow className="flex-1">
        <ul className="divide-y divide-border/20">
          {entries.length === 0 ? <li className="p-3 text-xs text-muted">Nothing run yet.</li> : null}
          {entries.map((e) => (
            <li key={e.id} title={e.error ?? e.sql}>
              <Button
                variant="ghost"
                onPress={() => onPick(e.sql)}
                className="h-auto w-full flex-col items-start justify-start gap-1 rounded-none px-3 py-2 text-left bg-transparent hover:bg-surface-secondary/70"
              >
                <span className="truncate font-mono text-[11px] text-foreground">{e.sql.replace(/\s+/g, " ")}</span>
                <span className="flex items-center gap-2 text-[10px] text-muted">
                  <Chip size="sm" variant="soft" color={e.status === "ok" ? "success" : "danger"} className="text-[9.5px]">
                    {e.status}
                  </Chip>
                  <span>{formatMs(e.elapsedMs)}</span>
                  {e.rowCount !== null ? <span>{e.rowCount} rows</span> : null}
                  <span className="ml-auto">{new Date(e.executedAt).toLocaleTimeString()}</span>
                </span>
              </Button>
            </li>
          ))}
        </ul>
      </ScrollShadow>
    </div>
  );
}
