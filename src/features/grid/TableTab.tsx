// SOT: table-tab, table-toolbar, page-based-browsing, sort-state, export-copy, row-inspector, insert-row-flow, delete-rows-flow, cell-edit-staging, foreign-key-traversal
import { useCallback, useEffect, useMemo, useState } from "react";
import { Button, Dropdown, Label, Modal, Separator, Tooltip } from "@heroui/react";
import type { CellValue, ColumnInfo, FilterRule, ForeignKey, SortRule, StagedChange, TablePage, TableRef, Value } from "@/lib/bindings";
import type { JsonValue } from "@/lib/bindings/serde_json/JsonValue";
import { ipc, normalizeError } from "@/lib/ipc";
import { DENSITIES, formatCell, formatCount } from "@/lib/format";
import { engineMeta } from "@/lib/engines";
import { tableKey, useWorkspace } from "@/stores/workspace";
import { DataGrid, parseEdited, type GridColumn } from "./DataGrid";
import { FilterPopover } from "./FilterPopover";
import { AppSelect, Field } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { EmptyState } from "@/components/global/EmptyState";
import { Segmented } from "@/components/global/Field";
import { Icon } from "@/lib/icons";

const PAGE_SIZES = [
  { value: "50", label: "50 rows" },
  { value: "100", label: "100 rows" },
  { value: "200", label: "200 rows" },
  { value: "500", label: "500 rows" },
  { value: "1000", label: "1,000 rows" },
] satisfies readonly { value: string; label: string }[];
type PageSize = (typeof PAGE_SIZES)[number]["value"];

interface Loaded {
  key: string;
  page: TablePage | null;
  error: string | null;
}

let changeCounter = 0;
function nextChangeId(): string {
  changeCounter += 1;
  return `chg-${Date.now()}-${changeCounter}`;
}

// Stable empty list: a selector must return the same reference for unchanged state.
const EMPTY_CHANGES: StagedChange[] = [];
const EMPTY_FKS: ForeignKey[] = [];

// WHAT:  One open table: toolbar (insert, refresh, filter, sort, export, delete),
//        pager, virtualized grid with inline editing, record inspector.
// WHY:   Page-based browsing with an exact/estimated total; edits are staged in
//        review mode (Pending Changes) or committed at once in direct mode.
// HOW:   A request key (table + sort + filters + page + size + refresh) drives
//        one effect; `loading` is derived from key mismatch, never set in render.
// WHERE: src-tauri/src/services/data.rs, src-tauri/src/services/changes.rs
export function TableTab({ connectionId, table, initialFilters }: { connectionId: string; table: TableRef; initialFilters?: FilterRule[] | undefined }) {
  const density = useWorkspace((s) => s.density);
  const foreignKeys = useWorkspace((s) => s.foreignKeys[connectionId] ?? EMPTY_FKS);
  const openTable = useWorkspace((s) => s.openTable);
  const settings = useWorkspace((s) => s.settings);
  const engine = useWorkspace((s) => s.connections.find((c) => c.id === connectionId)?.engine ?? "postgres");
  const readOnly = useWorkspace((s) => s.connections.find((c) => c.id === connectionId)?.readOnly ?? false);
  const pending = useWorkspace((s) => s.pendingChanges[connectionId] ?? EMPTY_CHANGES);
  const stageChange = useWorkspace((s) => s.stageChange);
  const unstageChange = useWorkspace((s) => s.unstageChange);
  const showInfo = useWorkspace((s) => s.showInfo);
  const showError = useWorkspace((s) => s.showError);
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState<PageSize>("50");
  const [sort, setSort] = useState<SortRule[]>([]);
  const [filters, setFilters] = useState<FilterRule[]>(initialFilters ?? []);
  const [refresh, setRefresh] = useState(0);
  const [loaded, setLoaded] = useState<Loaded>({ key: "", page: null, error: null });
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set());
  const [cell, setCell] = useState<{ row: number; col: number } | null>(null);
  const [insertOpen, setInsertOpen] = useState(false);
  const [inspectorTab, setInspectorTab] = useState<string>(settings?.inspectorTabs[0] ?? "fields");

  const editable = engineMeta(engine).commandLanguage === "SQL" && !readOnly;
  const limit = Number(pageSize);
  const requestKey = JSON.stringify({ table, sort, filters, pageIndex, limit, refresh });

  useEffect(() => {
    const token = { cancelled: false };
    void (async () => {
      try {
        const page = await ipc("fetch_table_page", { connectionId, table, query: { sort, filters, offset: pageIndex * limit, limit } });
        if (!token.cancelled) setLoaded({ key: requestKey, page, error: null });
      } catch (raw) {
        if (!token.cancelled) setLoaded({ key: requestKey, page: null, error: normalizeError(raw).message });
      }
    })();
    return () => {
      token.cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- requestKey encodes every input
  }, [connectionId, requestKey]);

  useEffect(() => {
    const onRefresh = () => setRefresh((r) => r + 1);
    window.addEventListener("db-free:refresh-tables", onRefresh);
    return () => window.removeEventListener("db-free:refresh-tables", onRefresh);
  }, []);

  const loading = loaded.key !== requestKey;
  const page = loaded.page;
  const rows = useMemo(() => page?.rows ?? [], [page]);
  const columns = useMemo(() => page?.columns ?? [], [page]);
  const total = page?.total ?? null;
  const pageCount = total !== null ? Math.max(1, Math.ceil(total / limit)) : null;
  const hasNext = pageCount !== null ? pageIndex + 1 < pageCount : rows.length === limit;

  // Foreign keys leaving this table, by source column (single-column keys only).
  const fkByColumn = useMemo(() => {
    const map = new Map<string, { table: TableRef; column: string }>();
    for (const fk of foreignKeys) {
      if (fk.fromTable !== table.name || (fk.fromSchema ?? null) !== (table.schema ?? null) || fk.fromColumns.length !== 1) continue;
      const from = fk.fromColumns[0];
      const to = fk.toColumns[0];
      if (from !== undefined && to !== undefined) map.set(from, { table: { schema: fk.toSchema, name: fk.toTable }, column: to });
    }
    return map;
  }, [foreignKeys, table]);

  const gridColumns: GridColumn[] = useMemo(
    () =>
      columns.map((c) => {
        const link = fkByColumn.get(c.name);
        return { name: c.name, typeName: c.dataType, primaryKey: c.primaryKey, ...(link ? { linkTo: tableKey(link.table) } : {}) };
      }),
    [columns, fkByColumn],
  );

  // WHAT:  FK traversal: open the referenced table filtered to the clicked value.
  const openLinked = (rowIndex: number, colIndex: number) => {
    const column = columns[colIndex];
    const value = rows[rowIndex]?.[colIndex];
    const link = column ? fkByColumn.get(column.name) : undefined;
    if (!link || !value || value.t === "null") return;
    const text = value.t === "json" ? JSON.stringify(value.v) : String(value.v);
    openTable(connectionId, link.table, [{ column: link.column, op: "eq", value: text }]);
  };
  const getRow = useCallback((i: number): readonly Value[] | undefined => rows[i], [rows]);
  const pkColumns = useMemo(() => columns.filter((c) => c.primaryKey), [columns]);

  // Staged updates for this table, keyed by "row:col" so the grid can highlight them.
  const staged = useMemo(() => {
    const map = new Map<string, Value>();
    for (const c of pending) {
      if (c.kind !== "update" || JSON.stringify(c.table) !== JSON.stringify(table)) continue;
      const rowIndex = rows.findIndex((r) => c.key.every((k) => JSON.stringify(r[columns.findIndex((col) => col.name === k.column)]) === JSON.stringify(k.value)));
      const colIndex = columns.findIndex((col) => col.name === c.column);
      if (rowIndex >= 0 && colIndex >= 0) map.set(`${rowIndex}:${colIndex}`, c.new);
    }
    return map;
  }, [pending, rows, columns, table]);

  const keyOf = useCallback(
    (row: readonly Value[]): CellValue[] => pkColumns.map((c) => ({ column: c.name, value: row[columns.findIndex((col) => col.name === c.name)] ?? { t: "null" } })),
    [pkColumns, columns],
  );

  const applyChanges = async (changes: StagedChange[]) => {
    if (settings?.executionMode === "direct") {
      try {
        await ipc("commit_changes", { connectionId, changes });
        showInfo(`Applied ${changes.length} change(s).`);
        setRefresh((r) => r + 1);
      } catch (raw) {
        showError(normalizeError(raw));
      }
    } else {
      for (const c of changes) stageChange(connectionId, c);
    }
  };

  const onCellEdit = (rowIndex: number, colIndex: number, next: Value) => {
    const row = rows[rowIndex];
    const column = columns[colIndex];
    if (!row || !column) return;
    if (pkColumns.length === 0) {
      showError("This table has no primary key, so rows cannot be edited safely.");
      return;
    }
    const old = row[colIndex] ?? { t: "null" };
    const key = keyOf(row);
    if (JSON.stringify(old) === JSON.stringify(next)) {
      // Back to the original value: drop any staged edit for this cell instead of adding one.
      const existing = pending.find((c) => c.kind === "update" && tableKey(c.table) === tableKey(table) && c.column === column.name && JSON.stringify(c.key) === JSON.stringify(key));
      if (existing) unstageChange(connectionId, existing.id);
      return;
    }
    void applyChanges([{ kind: "update", id: nextChangeId(), table, key, column: column.name, old, new: next }]);
  };

  const deleteSelected = () => {
    if (pkColumns.length === 0) {
      showError("This table has no primary key, so rows cannot be deleted safely.");
      return;
    }
    const changes: StagedChange[] = [...selectedRows]
      .map((i) => rows[i])
      .filter((r): r is Value[] => r !== undefined)
      .map((r) => ({ kind: "delete", id: nextChangeId(), table, key: keyOf(r) }));
    if (changes.length === 0) return;
    if (settings?.executionMode === "direct" && !window.confirm(`Delete ${changes.length} row(s) now?`)) return;
    setSelectedRows(new Set());
    void applyChanges(changes);
  };

  const toggleSort = (column: string) => {
    setPageIndex(0);
    setSort((current) => {
      const existing = current.find((s) => s.column === column);
      if (!existing) return [{ column, desc: false }];
      if (!existing.desc) return [{ column, desc: true }];
      return [];
    });
  };

  const copyAs = async (format: "csv" | "json") => {
    if (!page) return;
    const chosen = selectedRows.size > 0 ? rows.filter((_, i) => selectedRows.has(i)) : rows;
    const text = format === "json" ? toJson(page, chosen) : toCsv(page, chosen);
    await navigator.clipboard.writeText(text);
    showInfo(`Copied ${chosen.length} row(s) as ${format.toUpperCase()} to the clipboard.`);
  };

  const selectedRow = cell ? rows[cell.row] : undefined;
  const selectedValue = cell ? selectedRow?.[cell.col] : undefined;
  const selectedColumn = cell ? columns[cell.col] : undefined;

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-1 overflow-x-auto border-b border-border bg-surface px-2 [scrollbar-width:none]">
        <Tooltip delay={300}>
          <Button size="sm" isDisabled={!editable || columns.length === 0} onPress={() => setInsertOpen(true)}>
            <Icon name="plus" size={13} />
            Insert
          </Button>
          <Tooltip.Content>{readOnly ? "Connection is read-only." : editable ? "Insert a row" : "Editing is only available for SQL engines."}</Tooltip.Content>
        </Tooltip>
        <Button size="sm" variant="ghost" className="text-muted" onPress={() => setRefresh((r) => r + 1)}>
          <Icon name="refresh" size={13} />
          Refresh
        </Button>
        <FilterPopover columns={columns} filters={filters} onApply={(next) => { setPageIndex(0); setFilters(next); }} />
        <Button size="sm" variant={sort.length > 0 ? "primary" : "ghost"} className={sort.length > 0 ? "" : "text-muted"} onPress={() => setSort([])} isDisabled={sort.length === 0}>
          <Icon name="sort" size={13} />
          {sort.length > 0 ? `Sorted by ${sort.length} rule` : "Sort"}
        </Button>
        <Dropdown>
          <Button size="sm" variant="ghost" className="text-muted" isDisabled={rows.length === 0}>
            <Icon name="download" size={13} />
            Export
            <Icon name="chevron-down" size={12} />
          </Button>
          <Dropdown.Popover>
            <Dropdown.Menu onAction={(key) => void copyAs(String(key) === "json" ? "json" : "csv")}>
              <Dropdown.Item id="csv" textValue="Copy as CSV"><Label>Copy {selectedRows.size > 0 ? "selection" : "page"} as CSV</Label></Dropdown.Item>
              <Dropdown.Item id="json" textValue="Copy as JSON"><Label>Copy {selectedRows.size > 0 ? "selection" : "page"} as JSON</Label></Dropdown.Item>
            </Dropdown.Menu>
          </Dropdown.Popover>
        </Dropdown>
        {selectedRows.size > 0 && editable ? (
          <Button size="sm" variant="danger-soft" onPress={deleteSelected}>
            <Icon name="trash" size={13} />
            Delete {selectedRows.size}
          </Button>
        ) : null}

        <div className="ml-auto flex shrink-0 items-center gap-1.5 text-xs whitespace-nowrap text-muted">
          {loading ? <span className="text-accent">loading…</span> : null}
          {selectedRows.size > 0 ? <span>{selectedRows.size} selected</span> : null}
          <IconButton icon="columns" label={cell ? "Hide inspector" : "Inspect selected cell"} active={cell !== null} isDisabled={cell === null} onPress={() => setCell(null)} />
          <Separator orientation="vertical" className="mx-1 h-5" />
          <IconButton icon="chevron-left" label="Previous page" isDisabled={pageIndex === 0} onPress={() => setPageIndex((p) => Math.max(0, p - 1))} />
          <span className="px-1 tabular-nums text-foreground">
            {pageIndex + 1}
            <span className="text-muted"> of {pageCount ?? "…"}</span>
          </span>
          <IconButton icon="chevron-right" label="Next page" isDisabled={!hasNext} onPress={() => setPageIndex((p) => p + 1)} />
          <AppSelect ariaLabel="Rows per page" value={pageSize} options={PAGE_SIZES} size="sm" className="w-28 shrink-0" onChange={(v) => { setPageIndex(0); setPageSize(v); }} />
          <span className="min-w-16 text-right tabular-nums">{total !== null ? `${page?.totalExact ? "" : "≈ "}${formatCount(total)} rows` : `${formatCount(rows.length)} rows`}</span>
        </div>
      </div>

      <div className="flex min-h-0 flex-1">
        <div className="min-w-0 flex-1">
          {loaded.error !== null ? (
            <EmptyState icon="table" title="Could not load table" body={loaded.error} action={<Button size="sm" onPress={() => setRefresh((r) => r + 1)}>Retry</Button>} />
          ) : (
            <DataGrid
              columns={gridColumns}
              rowCount={rows.length}
              getRow={getRow}
              rowHeight={DENSITIES[density].rowHeight}
              sort={sort}
              onSortToggle={toggleSort}
              selectedRows={selectedRows}
              onToggleRow={(i) =>
                setSelectedRows((s) => {
                  const next = new Set(s);
                  if (next.has(i)) next.delete(i);
                  else next.add(i);
                  return next;
                })
              }
              onToggleAll={() => setSelectedRows((s) => (s.size === rows.length ? new Set() : new Set(rows.map((_, i) => i))))}
              onCellSelect={(row, col) => setCell({ row, col })}
              selected={cell}
              {...(editable ? { onCellEdit } : {})}
              staged={staged}
              nullDisplay={settings?.nullDisplay ?? "NULL"}
              onLinkOpen={openLinked}
            />
          )}
        </div>
        {selectedRow && selectedValue !== undefined && selectedColumn ? (
          <RecordInspector
            columns={columns}
            row={selectedRow}
            column={selectedColumn}
            value={selectedValue}
            table={table}
            tabs={settings?.inspectorTabs ?? ["fields", "json", "sql"]}
            activeTab={inspectorTab}
            onTab={setInspectorTab}
            onClose={() => setCell(null)}
          />
        ) : null}
      </div>

      <InsertRowModal open={insertOpen} onClose={() => setInsertOpen(false)} columns={columns} onSubmit={(values) => { setInsertOpen(false); void applyChanges([{ kind: "insert", id: nextChangeId(), table, values }]); }} />
    </div>
  );
}

// WHAT:  Insert form: one field per column; blank = omitted (defaults apply), "NULL" = null.
function InsertRowModal({ open, onClose, columns, onSubmit }: { open: boolean; onClose: () => void; columns: readonly ColumnInfo[]; onSubmit: (values: CellValue[]) => void }) {
  const [values, setValues] = useState<Record<string, string>>({});
  const submit = () => {
    const out: CellValue[] = [];
    for (const c of columns) {
      const text = values[c.name] ?? "";
      if (text.length === 0) continue;
      out.push({ column: c.name, value: parseEdited(text, c.dataType, undefined) });
    }
    setValues({});
    onSubmit(out);
  };
  return (
    <Modal isOpen={open} onOpenChange={(o) => !o && onClose()}>
      <Modal.Backdrop>
        <Modal.Container>
          <Modal.Dialog className="sm:max-w-[560px]">
            <Modal.CloseTrigger />
            <Modal.Header>
              <Modal.Heading>Insert row</Modal.Heading>
            </Modal.Header>
            <Modal.Body className="max-h-[60vh] overflow-y-auto">
              <div className="grid grid-cols-2 gap-3">
                {columns.map((c) => (
                  <Field
                    key={c.name}
                    label={`${c.name}${c.primaryKey ? " (PK)" : ""}`}
                    value={values[c.name] ?? ""}
                    onChange={(v) => setValues((s) => ({ ...s, [c.name]: v }))}
                    placeholder={c.nullable ? "default / NULL" : c.dataType}
                    description={c.dataType}
                    mono
                  />
                ))}
              </div>
            </Modal.Body>
            <Modal.Footer>
              <Button variant="tertiary" onPress={onClose}>Cancel</Button>
              <Button onPress={submit}>Add row</Button>
            </Modal.Footer>
          </Modal.Dialog>
        </Modal.Container>
      </Modal.Backdrop>
    </Modal>
  );
}

// WHAT:  Record inspector with Fields / JSON / SQL tabs (order from settings).
function RecordInspector({ columns, row, column, value, table, tabs, activeTab, onTab, onClose }: { columns: readonly ColumnInfo[]; row: readonly Value[]; column: ColumnInfo; value: Value; table: TableRef; tabs: readonly string[]; activeTab: string; onTab: (t: string) => void; onClose: () => void }) {
  const current = tabs.includes(activeTab) ? activeTab : (tabs[0] ?? "fields");
  const record = Object.fromEntries(columns.map((c, i) => [c.name, plainValue(row[i])]));
  const insertSql = `INSERT INTO ${table.schema ? `"${table.schema}".` : ""}"${table.name}" (${columns.map((c) => `"${c.name}"`).join(", ")})\nVALUES (${row.map((v) => sqlLiteral(v)).join(", ")});`;
  return (
    <aside className="flex w-96 shrink-0 flex-col border-l border-border bg-surface">
      <div className="flex h-9 items-center gap-2 border-b border-border px-3 text-xs">
        <span className="truncate font-medium text-foreground">{column.name}</span>
        <span className="font-mono text-muted">{column.dataType}</span>
        <span className="ml-auto">
          <IconButton icon="x" label="Close inspector" onPress={onClose} />
        </span>
      </div>
      <div className="px-3 py-2">
        <Segmented label="Inspector tab" value={current} onChange={onTab} options={tabs.map((t) => ({ value: t, label: t.toUpperCase() }))} />
      </div>
      <div className="min-h-0 flex-1 overflow-auto">
        {current === "fields" ? (
          <dl className="px-3 pb-3 text-xs">
            {columns.map((c, i) => (
              <div key={c.name} className={`flex flex-col gap-0.5 border-b border-separator py-1.5 ${c.name === column.name ? "text-foreground" : "text-muted"}`}>
                <dt className="flex items-center gap-1.5 font-medium"><Icon name={c.primaryKey ? "key" : "text"} size={11} />{c.name}<span className="ml-auto font-mono text-[10px]">{c.dataType}</span></dt>
                <dd className="selectable truncate font-mono">{cellText(row[i])}</dd>
              </div>
            ))}
          </dl>
        ) : current === "json" ? (
          <pre className="selectable p-3 font-mono text-[11px] leading-relaxed whitespace-pre-wrap text-foreground">{JSON.stringify(record, null, 2)}</pre>
        ) : current === "sql" ? (
          <pre className="selectable p-3 font-mono text-[11px] leading-relaxed break-all whitespace-pre-wrap text-foreground">{insertSql}</pre>
        ) : (
          <pre className="selectable p-3 font-mono text-[11px] whitespace-pre-wrap text-foreground">{inspectorBody(value)}</pre>
        )}
      </div>
    </aside>
  );
}

function inspectorBody(value: Value): string {
  switch (value.t) {
    case "json":
      return JSON.stringify(value.v, null, 2);
    case "bytes":
      return hexDump(value.v);
    case "null":
    case "bool":
    case "int":
    case "float":
    case "decimal":
    case "text":
    case "date_time":
    case "unsupported":
      return formatCell(value).text;
  }
}

function hexDump(base64: string): string {
  let binary = "";
  try {
    binary = atob(base64);
  } catch {
    return base64;
  }
  const lines: string[] = [];
  for (let i = 0; i < binary.length; i += 16) {
    const chunk = binary.slice(i, i + 16);
    const hex = Array.from(chunk, (ch) => ch.charCodeAt(0).toString(16).padStart(2, "0")).join(" ");
    const ascii = Array.from(chunk, (ch) => (ch.charCodeAt(0) >= 32 && ch.charCodeAt(0) < 127 ? ch : ".")).join("");
    lines.push(`${i.toString(16).padStart(8, "0")}  ${hex.padEnd(47)}  ${ascii}`);
  }
  return lines.join("\n");
}

function cellText(value: Value | undefined): string {
  if (value === undefined) return "";
  return value.t === "json" ? JSON.stringify(value.v) : value.t === "null" ? "NULL" : formatCell(value).text;
}

function sqlLiteral(value: Value | undefined): string {
  if (value === undefined || value.t === "null") return "NULL";
  switch (value.t) {
    case "bool":
      return value.v ? "TRUE" : "FALSE";
    case "int":
    case "float":
      return String(value.v);
    case "decimal":
      return value.v;
    case "json":
      return `'${JSON.stringify(value.v).replace(/'/g, "''")}'`;
    case "text":
    case "bytes":
    case "date_time":
    case "unsupported":
      return `'${value.v.replace(/'/g, "''")}'`;
  }
}

function toCsv(page: TablePage, rows: readonly (readonly Value[])[]): string {
  const escape = (s: string) => (/[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s);
  const header = page.columns.map((c) => escape(c.name)).join(",");
  const body = rows.map((r) => r.map((v) => escape(v.t === "null" ? "" : cellText(v))).join(","));
  return [header, ...body].join("\n");
}

function toJson(page: TablePage, rows: readonly (readonly Value[])[]): string {
  const objects = rows.map((r) => Object.fromEntries(page.columns.map((c, i) => [c.name, plainValue(r[i])])));
  return JSON.stringify(objects, null, 2);
}

function plainValue(value: Value | undefined): JsonValue {
  if (value === undefined) return null;
  switch (value.t) {
    case "null":
      return null;
    case "bool":
    case "int":
    case "float":
    case "json":
      return value.v;
    case "decimal":
    case "text":
    case "bytes":
    case "date_time":
    case "unsupported":
      return value.v;
  }
}
