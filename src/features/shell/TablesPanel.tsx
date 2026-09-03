// SOT: tables-panel, sidebar-tables, database-switcher, schema-switcher, table-tree
import { useState } from "react";
import { Button, SearchField, Separator, Spinner } from "@heroui/react";
import type { ColumnInfo, TableInfo, TableRef } from "@/lib/bindings";
import { formatCount } from "@/lib/format";
import { Icon, typeIcon } from "@/lib/icons";
import { normalizeError } from "@/lib/ipc";
import { tableKey, useActiveConnection, useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { AppSelect, Segmented } from "@/components/global/Field";
import { EnvBadge } from "@/components/global/Badge";
import { ConnectionSwitcher } from "./ConnectionSwitcher";
import { KeyTree } from "./KeyTree";
import { isMac } from "@/components/global/Kbd";
import { cn } from "@/lib/cn";

// WHAT:  Sidebar listing the active connection's tables, with database and
//        schema switchers on top (the "saas_db / public" breadcrumb).
// WHY:   An empty Database on the connection connects to the default database;
//        every database the server exposes is offered here for switching.
// WHERE: src-tauri/src/integrations/mod.rs (SessionInfo.databases)
export function TablesPanel() {
  const connection = useActiveConnection();
  const catalog = useWorkspace((s) => (connection ? s.catalogs[connection.id] : undefined));
  const info = useWorkspace((s) => (connection ? s.sessionInfos[connection.id] : undefined));
  const schemaFilter = useWorkspace((s) => (connection ? (s.schemaFilter[connection.id] ?? null) : null));
  const setSchemaFilter = useWorkspace((s) => s.setSchemaFilter);
  const switchDatabase = useWorkspace((s) => s.switchDatabase);
  const loadCatalog = useWorkspace((s) => s.loadCatalog);
  const openQuery = useWorkspace((s) => s.openQuery);
  const openErd = useWorkspace((s) => s.openErd);
  const connecting = useWorkspace((s) => s.connecting);
  const [mode, setMode] = useState<"az" | "tags">("az");
  const [search, setSearch] = useState("");
  const [searchOpen, setSearchOpen] = useState(false);

  if (!connection) return null;
  const id = connection.id;
  const schemas = catalog?.schemas ?? [];
  const schemaOptions = [{ value: "*", label: "All schemas" }, ...schemas.map((s) => ({ value: s.name, label: s.name }))];
  const databases = info?.databases ?? [];
  const currentDb = info?.database ?? "";
  const dbOptions = (databases.length > 0 ? databases : [currentDb]).map((d) => ({ value: d, label: d }));
  const needle = search.trim().toLowerCase();
  const visible = schemas
    .filter((s) => schemaFilter === null || s.name === schemaFilter)
    .map((s) => ({ ...s, tables: s.tables.filter((t) => needle.length === 0 || t.name.toLowerCase().includes(needle)) }))
    .filter((s) => s.tables.length > 0);

  return (
    <aside className="flex w-[280px] shrink-0 flex-col border-r border-border bg-surface">
      {/* macOS traffic lights overhang the 48px rail; keep the title clear of them. */}
      <div className={cn("drag-region flex h-10 shrink-0 items-center gap-1 pr-2", isMac() ? "pl-9" : "pl-3")} data-tauri-drag-region>
        <ConnectionSwitcher caption={connection.engine === "redis" ? "Keys" : connection.engine === "mongodb" ? "Collections" : "Tables"} />
        {connection.readOnly ? <EnvBadge environment="none" readOnly /> : null}
        <div className="drag-region h-full min-w-4 flex-1" data-tauri-drag-region />
        <span className="flex items-center">
          <IconButton icon="refresh" label="Refresh schema" onPress={() => void loadCatalog(id)} />
          <IconButton icon="plus" label="New query" onPress={() => openQuery(id)} />
          <IconButton icon="view" label="ER diagram" isDisabled={connection.engine === "redis" || connection.engine === "mongodb"} onPress={() => openErd(id, schemaFilter)} />
          <IconButton icon="search" label="Search tables" active={searchOpen} onPress={() => setSearchOpen((v) => !v)} />
        </span>
      </div>

      <div className="flex items-center gap-0.5 px-2 pb-1.5 text-xs text-muted">
        <AppSelect ariaLabel="Database" value={currentDb} options={dbOptions} plain className="w-auto min-w-0" icon="database" onChange={(db) => void switchDatabase(id, db)} />
        {(info?.capabilities.namespaces ?? true) && (schemas.length > 1 || (schemas[0] !== undefined && schemas[0].name !== "main")) ? (
          <>
            <span className="px-0.5 text-muted">/</span>
            <AppSelect ariaLabel="Schema" value={schemaFilter ?? "*"} options={schemaOptions} plain className="w-auto min-w-0" icon="folder" onChange={(v) => setSchemaFilter(id, v === "*" ? null : v)} />
          </>
        ) : null}
      </div>

      <div className="px-3 pb-2">
        <Segmented label="Table list mode" value={mode} onChange={setMode} options={[{ value: "az", label: "A-Z" }, { value: "tags", label: "Tags", disabled: true }]} />
      </div>

      {searchOpen ? (
        <div className="px-3 pb-2">
          <SearchField value={search} onChange={setSearch} aria-label="Search tables" autoFocus>
            <SearchField.Group>
              <SearchField.SearchIcon />
              <SearchField.Input placeholder="Search tables" className="w-full" />
              <SearchField.ClearButton />
            </SearchField.Group>
          </SearchField>
        </div>
      ) : null}

      <Separator />

      <div className="min-h-0 flex-1 overflow-y-auto py-1">
        {connecting === id || !catalog ? (
          <div className="flex items-center gap-2 px-3 py-3 text-xs text-muted">
            <Spinner size="sm" /> Loading schema…
          </div>
        ) : visible.length === 0 ? (
          <p className="px-3 py-3 text-xs text-muted">{needle.length > 0 ? "No tables match." : "No tables in this database yet. Create one from a query tab and refresh."}</p>
        ) : connection.engine === "redis" ? (
          <KeyTree connectionId={id} keys={visible.flatMap((s) => s.tables)} />
        ) : (
          visible.map((schema) => (
            <div key={schema.name}>
              {schemaFilter === null && schemas.length > 1 ? (
                <div className="flex items-center gap-1.5 px-3 pt-2 pb-1 text-[11px] font-medium text-muted">
                  <Icon name="folder" size={12} />
                  {schema.name}
                  <span className="ml-auto">{schema.tables.length}</span>
                </div>
              ) : null}
              {schema.tables.map((t) => (
                <TableRow key={tableKey({ schema: t.schema, name: t.name })} connectionId={id} table={t} />
              ))}
            </div>
          ))
        )}
      </div>

      {info?.serverVersion ? <div className="truncate border-t border-border px-3 py-1.5 text-[11px] text-muted">{info.serverVersion}</div> : null}
    </aside>
  );
}

function TableRow({ connectionId, table }: { connectionId: string; table: TableInfo }) {
  const ref: TableRef = { schema: table.schema, name: table.name };
  const key = tableKey(ref);
  const isActive = useWorkspace((s) => s.activeTabId === `table:${connectionId}:${key}`);
  const columns = useWorkspace((s) => s.columnsCache[`${connectionId}:${key}`]);
  const loadColumns = useWorkspace((s) => s.loadColumns);
  const openTable = useWorkspace((s) => s.openTable);
  const showError = useWorkspace((s) => s.showError);
  const [expanded, setExpanded] = useState(false);

  const toggle = () => {
    const next = !expanded;
    setExpanded(next);
    if (next && !columns) {
      void (async () => {
        try {
          await loadColumns(connectionId, ref);
        } catch (raw) {
          showError(normalizeError(raw));
        }
      })();
    }
  };

  return (
    <div>
      <div
        className={cn(
          "group flex h-8 cursor-default items-center gap-1 pr-2 pl-2 text-[13px]",
          isActive ? "bg-surface-tertiary text-foreground" : "text-muted hover:bg-surface-secondary hover:text-foreground",
        )}
      >
        <Button isIconOnly variant="ghost" size="sm" aria-label={expanded ? "Collapse columns" : "Expand columns"} onPress={toggle} className="size-5 min-w-5 rounded-sm text-muted">
          <Icon name={expanded ? "chevron-down" : "chevron-right"} size={12} />
        </Button>
        <button type="button" onClick={() => openTable(connectionId, ref)} className="flex min-w-0 flex-1 items-center gap-2 text-left" title={table.kind === "view" ? "view" : "table"}>
          <Icon name={table.kind === "view" ? "view" : "table"} size={14} className="shrink-0 text-muted" />
          <span className="truncate">{table.name}</span>
          {table.rowEstimate !== null ? <span className="ml-auto text-[10px] tabular-nums text-muted opacity-0 group-hover:opacity-100">{formatCount(table.rowEstimate)}</span> : null}
        </button>
      </div>
      {expanded ? <ColumnList columns={columns} /> : null}
    </div>
  );
}

function ColumnList({ columns }: { columns: ColumnInfo[] | undefined }) {
  if (!columns) {
    return (
      <div className="flex items-center gap-2 py-1 pl-9 text-[11px] text-muted">
        <Spinner size="sm" /> loading…
      </div>
    );
  }
  return (
    <ul className="pb-1">
      {columns.map((c) => (
        <li key={c.name} className="flex h-6 items-center gap-2 pl-9 pr-3 text-[12px] text-muted" title={c.dataType}>
          <Icon name={typeIcon(c.dataType, c.primaryKey)} size={12} className={c.primaryKey ? "text-warning" : ""} />
          <span className="truncate text-foreground/80">{c.name}</span>
          <span className="ml-auto truncate font-mono text-[10px]">{c.dataType}</span>
        </li>
      ))}
    </ul>
  );
}
