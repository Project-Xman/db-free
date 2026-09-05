// SOT: tables-panel, sidebar-tables, database-switcher, schema-switcher, table-tree
import { useState, type MouseEvent as ReactMouseEvent } from "react";
import { Button, Chip, ScrollShadow, SearchField, Separator, Skeleton, Spinner } from "@heroui/react";
import type { ColumnInfo, Engine, TableInfo, TableRef } from "@/lib/bindings";
import { formatCount } from "@/lib/format";
import { collectionNoun, engineMeta, isKeyValueEngine, supportsErd } from "@/lib/engines";
import { Icon, typeIcon } from "@/lib/icons";
import { normalizeError } from "@/lib/ipc";
import { tableKey, useActiveConnection, useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { AppSelect, Segmented } from "@/components/global/Field";
import { useContextMenu, type ContextMenu, type MenuEntry } from "@/components/global/ContextMenu";
import { EnvBadge } from "@/components/global/Badge";
import { ConnectionSwitcher } from "./ConnectionSwitcher";
import { KeyTree } from "./KeyTree";
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
  const menu = useContextMenu();
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
    <aside className="flex h-full w-full min-w-0 flex-col glass-sidebar select-none">
      <div className="drag-region flex h-11 shrink-0 items-center gap-1.5 px-3 border-b border-border/40" data-tauri-drag-region>
        <ConnectionSwitcher caption={collectionNoun(connection.engine)} />
        {connection.readOnly ? <EnvBadge environment="none" readOnly /> : null}
        <div className="drag-region h-full min-w-4 flex-1" data-tauri-drag-region />
        <span className="flex items-center gap-0.5">
          <IconButton icon="refresh" label="Refresh schema" onPress={() => void loadCatalog(id)} />
          <IconButton icon="plus" label="New query" onPress={() => openQuery(id)} />
          <IconButton icon="view" label="ER diagram" isDisabled={!supportsErd(connection.engine)} onPress={() => openErd(id, schemaFilter)} />
          <IconButton icon="search" label="Search tables" active={searchOpen} onPress={() => setSearchOpen((v) => !v)} />
        </span>
      </div>

      <div className="flex items-center gap-1 px-3 py-2 text-xs text-muted">
        <AppSelect ariaLabel="Database" value={currentDb} options={dbOptions} plain className="w-auto min-w-0" icon="database" onChange={(db) => void switchDatabase(id, db)} />
        {(info?.capabilities.namespaces ?? true) && (schemas.length > 1 || (schemas[0] !== undefined && schemas[0].name !== "main")) ? (
          <>
            <span className="px-0.5 text-muted/60">/</span>
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
            <SearchField.Group className="glass-input rounded-lg h-8 px-2">
              <SearchField.SearchIcon />
              <SearchField.Input placeholder="Search tables…" className="w-full text-xs" />
              <SearchField.ClearButton />
            </SearchField.Group>
          </SearchField>
        </div>
      ) : null}

      <Separator className="opacity-50" />

      <ScrollShadow className="min-h-0 flex-1 px-1.5 py-1.5">
        {connecting === id || !catalog ? (
          <div className="space-y-2.5 p-3">
            <Skeleton className="h-4 w-3/4 rounded-md" />
            <Skeleton className="h-4 w-1/2 rounded-md" />
            <Skeleton className="h-4 w-5/6 rounded-md" />
            <Skeleton className="h-4 w-2/3 rounded-md" />
          </div>
        ) : visible.length === 0 ? (
          <p className="px-3 py-3 text-xs text-muted">{needle.length > 0 ? "No tables match." : "No tables in this database yet. Create one from a query tab and refresh."}</p>
        ) : isKeyValueEngine(connection.engine) ? (
          <KeyTree connectionId={id} keys={visible.flatMap((s) => s.tables)} />
        ) : (
          visible.map((schema) => (
            <div key={schema.name} className="mb-1">
              {schemaFilter === null && schemas.length > 1 ? (
                <div
                  className="flex items-center gap-1.5 px-2.5 pt-2 pb-1 text-[10px] font-semibold uppercase tracking-wider text-muted/80"
                  onContextMenu={(e) =>
                    menu.open(
                      e,
                      [
                        { id: "only", label: `Show only ${schema.name}`, icon: "folder" },
                        { id: "erd", label: "ER diagram of this schema", icon: "view", group: "open", disabled: !supportsErd(connection.engine) },
                        { id: "copy", label: "Copy schema name", icon: "copy", group: "copy" },
                        { id: "refresh", label: "Refresh schema", icon: "refresh", group: "copy" },
                      ],
                      (action) => {
                        // The header only renders while every schema is shown, so
                        // "show all" would be a no-op here; the picker above does it.
                        if (action === "only") setSchemaFilter(id, schema.name);
                        else if (action === "erd") openErd(id, schema.name);
                        else if (action === "copy") void navigator.clipboard.writeText(schema.name);
                        else if (action === "refresh") void loadCatalog(id);
                      },
                    )
                  }
                >
                  <Icon name="folder" size={11} />
                  {schema.name}
                  <Chip size="sm" variant="soft" className="ml-auto font-mono text-[9px]">
                    {schema.tables.length}
                  </Chip>
                </div>
              ) : null}
              {schema.tables.map((t) => (
                <TableRow key={tableKey({ schema: t.schema, name: t.name })} connectionId={id} table={t} menu={menu} engine={connection.engine} />
              ))}
            </div>
          ))
        )}
      </ScrollShadow>

      {info?.serverVersion ? <div className="truncate border-t border-border/40 px-3 py-1.5 text-[10px] font-mono text-muted/70">{info.serverVersion}</div> : null}
      {menu.node}
    </aside>
  );
}

function TableRow({ connectionId, table, menu, engine }: { connectionId: string; table: TableInfo; menu: ContextMenu; engine: Engine }) {
  const ref: TableRef = { schema: table.schema, name: table.name };
  const key = tableKey(ref);
  const isActive = useWorkspace((s) => s.activeTabId === `table:${connectionId}:${key}`);
  const columns = useWorkspace((s) => s.columnsCache[`${connectionId}:${key}`]);
  const loadColumns = useWorkspace((s) => s.loadColumns);
  const openTable = useWorkspace((s) => s.openTable);
  const openQuery = useWorkspace((s) => s.openQuery);
  const openTransfer = useWorkspace((s) => s.openTransfer);
  const loadCatalog = useWorkspace((s) => s.loadCatalog);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const [expanded, setExpanded] = useState(false);
  const qualified = table.schema === null ? table.name : `${table.schema}.${table.name}`;
  const speaksSql = engineMeta(engine).commandLanguage === "SQL";

  const entries: MenuEntry[] = [
    { id: "open", label: `Open ${table.kind === "view" ? "view" : "table"}`, icon: "table" },
    { id: "columns", label: expanded ? "Collapse columns" : "Show columns", icon: "columns" },
    { id: "select", label: "Query this table", icon: "terminal", group: "query", disabled: !speaksSql },
    { id: "count", label: "Count rows", icon: "hash", group: "query", disabled: !speaksSql },
    { id: "export", label: "Export / import…", icon: "download", group: "query" },
    { id: "copy-name", label: "Copy name", icon: "copy", group: "copy" },
    { id: "copy-qualified", label: "Copy qualified name", icon: "copy", group: "copy", disabled: table.schema === null },
    { id: "refresh", label: "Refresh schema", icon: "refresh", group: "copy" },
  ];

  const onMenu = (event: ReactMouseEvent) =>
    menu.open(event, entries, (action) => {
      if (action === "open") openTable(connectionId, ref);
      else if (action === "columns") toggle();
      else if (action === "select") openQuery(connectionId, `SELECT *\nFROM ${qualified}\nLIMIT 100;`, table.name);
      else if (action === "count") openQuery(connectionId, `SELECT count(*) FROM ${qualified};`, `count ${table.name}`);
      else if (action === "export") openTransfer(connectionId);
      else if (action === "copy-name") { void navigator.clipboard.writeText(table.name); showInfo("Table name copied."); }
      else if (action === "copy-qualified") { void navigator.clipboard.writeText(qualified); showInfo("Qualified name copied."); }
      else if (action === "refresh") void loadCatalog(connectionId);
    });

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
    <div className="py-0.5">
      <div
        className={cn(
          "group flex h-7.5 cursor-default items-center gap-1 rounded-lg px-2 text-[12.5px] liquid-hover",
          isActive ? "glass-pill text-accent font-medium shadow-xs" : "text-muted hover:bg-surface-secondary/70 hover:text-foreground",
        )}
        title={table.kind === "view" ? "view" : "table"}
        onContextMenu={onMenu}
      >
        <Button isIconOnly variant="ghost" size="sm" aria-label={expanded ? "Collapse columns" : "Expand columns"} onPress={toggle} className="size-4.5 min-w-4.5 rounded-sm text-muted">
          <Icon name={expanded ? "chevron-down" : "chevron-right"} size={11} />
        </Button>
        <Button
          variant="ghost"
          size="sm"
          onPress={() => openTable(connectionId, ref)}
          className="flex h-auto min-w-0 flex-1 items-center justify-start gap-2 p-0 text-left bg-transparent hover:bg-transparent"
        >
          <Icon name={table.kind === "view" ? "view" : "table"} size={13} className="shrink-0 opacity-70" />
          <span className="truncate">{table.name}</span>
          {table.rowEstimate !== null ? (
            <span className="ml-auto rounded-sm px-1 text-[10px] tabular-nums font-mono text-muted/70 opacity-0 group-hover:opacity-100 transition-opacity">
              {formatCount(table.rowEstimate)}
            </span>
          ) : null}
        </Button>
      </div>
      {expanded ? <ColumnList columns={columns} menu={menu} /> : null}
    </div>
  );
}

function ColumnList({ columns, menu }: { columns: ColumnInfo[] | undefined; menu: ContextMenu }) {
  if (!columns) {
    return (
      <div className="flex items-center gap-2 py-1 pl-8 text-[11px] text-muted">
        <Spinner size="sm" /> loading…
      </div>
    );
  }
  return (
    <ul className="py-0.5 pl-6 pr-1 space-y-0.5">
      {columns.map((c) => (
        <li
          key={c.name}
          className="flex h-5.5 items-center gap-1.5 rounded-md px-1.5 text-[11.5px] text-muted hover:bg-surface-secondary/40 hover:text-foreground transition-colors"
          title={c.dataType}
          onContextMenu={(e) =>
            menu.open(
              e,
              [
                { id: "name", label: "Copy column name", icon: "copy" },
                { id: "type", label: `Copy type (${c.dataType})`, icon: "copy" },
              ],
              (action) => void navigator.clipboard.writeText(action === "name" ? c.name : c.dataType),
            )
          }
        >
          <Icon name={typeIcon(c.dataType, c.primaryKey)} size={11} className={c.primaryKey ? "text-warning" : "opacity-60"} />
          <span className="truncate font-sans">{c.name}</span>
          <span className="ml-auto truncate font-mono text-[9.5px] text-muted/60">{c.dataType}</span>
        </li>
      ))}
    </ul>
  );
}
