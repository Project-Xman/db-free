// SOT: key-tab, redis-key-viewer, key-value-editor, ttl-editor
import { useCallback, useEffect, useState } from "react";
import { Button, Chip, TextArea } from "@heroui/react";
import type { QueryOutcome, TablePage, TableRef } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { DENSITIES } from "@/lib/format";
import { useWorkspace } from "@/stores/workspace";
import { DataGrid } from "@/features/grid/DataGrid";
import { AppSelect } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { Icon } from "@/lib/icons";

const TTLS = [
  { value: "-1", label: "No expiry" },
  { value: "60", label: "1 minute" },
  { value: "300", label: "5 minutes" },
  { value: "3600", label: "1 hour" },
  { value: "86400", label: "1 day" },
] satisfies readonly { value: string; label: string }[];

// WHAT:  Redis key viewer (DB Manager layout): key, type badge, TTL, value editor for
//        strings, a grid for hashes/lists/sets/zsets/streams, Save / Refresh / Delete.
// HOW:   Reads go through fetch_table_page (the Redis adapter maps a key to rows);
//        writes are plain commands through execute_query so the guard applies.
// WHERE: src-tauri/src/integrations/redis.rs
export function KeyTab({ connectionId, table }: { connectionId: string; table: TableRef }) {
  const density = useWorkspace((s) => s.density);
  const readOnly = useWorkspace((s) => s.connections.find((c) => c.id === connectionId)?.readOnly ?? false);
  const closeTab = useWorkspace((s) => s.closeTab);
  const loadCatalog = useWorkspace((s) => s.loadCatalog);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const [page, setPage] = useState<TablePage | null>(null);
  const [type, setType] = useState<string>("");
  const [ttl, setTtl] = useState<number>(-1);
  const [text, setText] = useState("");
  const [dirty, setDirty] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const key = table.name;

  const scalar = useCallback((outcome: QueryOutcome): string => {
    const rows = outcome.statements.find((s) => s.kind === "rows");
    const cell = rows?.kind === "rows" ? rows.result.rows[0]?.[0] : undefined;
    if (!cell) return "";
    return cell.t === "null" ? "" : cell.t === "json" ? JSON.stringify(cell.v, null, 2) : String(cell.v);
  }, []);

  useEffect(() => {
    const token = { cancelled: false };
    void (async () => {
      try {
        const [p, typeOut, ttlOut] = await Promise.all([
          ipc("fetch_table_page", { connectionId, table, query: { sort: [], filters: [], offset: 0, limit: 1000 } }),
          ipc("execute_query", { connectionId, sql: `TYPE ${quote(key)}`, confirmDestructive: false, maxRows: 1 }),
          ipc("execute_query", { connectionId, sql: `TTL ${quote(key)}`, confirmDestructive: false, maxRows: 1 }),
        ]);
        if (token.cancelled) return;
        setPage(p);
        setType(scalar(typeOut));
        setTtl(Number(scalar(ttlOut)) || -1);
        const first = p.rows[0]?.[0];
        if (first) setText(first.t === "json" ? JSON.stringify(first.v, null, 2) : first.t === "null" ? "" : String(first.v));
        setDirty(false);
      } catch (raw) {
        if (!token.cancelled) showError(normalizeError(raw));
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connectionId, table, key, refresh, scalar, showError]);

  const run = async (command: string, message: string) => {
    try {
      await ipc("execute_query", { connectionId, sql: command, confirmDestructive: true, maxRows: 1 });
      showInfo(message);
      setRefresh((r) => r + 1);
    } catch (raw) {
      showError(normalizeError(raw));
    }
  };

  const save = () => void run(`SET ${quote(key)} ${quote(text)}`, `Saved ${key}.`);
  const applyTtl = (seconds: number) => {
    setTtl(seconds);
    void run(seconds < 0 ? `PERSIST ${quote(key)}` : `EXPIRE ${quote(key)} ${seconds}`, seconds < 0 ? "Expiry removed." : `Expires in ${seconds}s.`);
  };
  const remove = async () => {
    if (!window.confirm(`Delete key "${key}"?`)) return;
    await run(`DEL ${quote(key)}`, `Deleted ${key}.`);
    closeTab(`table:${connectionId}:${key}`);
    void loadCatalog(connectionId);
  };

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-border bg-surface px-2">
        <Button size="sm" onPress={save} isDisabled={readOnly || type !== "string" || !dirty}>
          <Icon name="check" size={12} />
          Save
        </Button>
        <IconButton icon="refresh" label="Reload key" onPress={() => setRefresh((r) => r + 1)} />
        <IconButton icon="trash" label="Delete key" isDisabled={readOnly} onPress={() => void remove()} />
        <div className="ml-auto flex items-center gap-2">
          <Icon name="history" size={13} className="text-muted" />
          <AppSelect ariaLabel="TTL" value={ttl >= 0 ? (TTLS.find((t) => Number(t.value) === ttl)?.value ?? "custom") : "-1"} options={ttl >= 0 && !TTLS.some((t) => Number(t.value) === ttl) ? [...TTLS, { value: "custom", label: `${ttl}s` }] : TTLS} onChange={(v) => v !== "custom" && applyTtl(Number(v))} size="sm" className="w-32" isDisabled={readOnly} />
          {type ? <Chip size="sm" color="success" variant="soft">{type}</Chip> : null}
        </div>
      </div>
      <div className="flex h-9 shrink-0 items-center gap-2 border-b border-border px-3 text-xs">
        <span className="text-muted">Key:</span>
        <span className="selectable font-mono text-foreground">{key}</span>
      </div>
      <div className="min-h-0 flex-1">
        {type === "string" ? (
          <TextArea
            value={text}
            onChange={(e) => {
              setText(e.target.value);
              setDirty(true);
            }}
            readOnly={readOnly}
            className="h-full w-full font-mono text-[12px]"
            aria-label="Value"
            spellCheck={false}
          />
        ) : page ? (
          <DataGrid columns={page.columns.map((c) => ({ name: c.name, typeName: c.dataType, primaryKey: c.primaryKey }))} rowCount={page.rows.length} getRow={(i) => page.rows[i]} rowHeight={DENSITIES[density].rowHeight} />
        ) : null}
      </div>
    </div>
  );
}

function quote(raw: string): string {
  return `"${raw.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n")}"`;
}
