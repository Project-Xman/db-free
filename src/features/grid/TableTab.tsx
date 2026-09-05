// SOT: table-tab, table-toolbar, page-based-browsing, sort-state, export-copy, row-inspector, inspector-collapse, insert-row-flow, delete-rows-flow, cell-edit-staging, foreign-key-traversal, staged-row-mapping
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button, Chip, CloseButton, Dropdown, Label, Modal, ScrollShadow, Separator, Tooltip } from "@heroui/react";
import type { CellValue, ColumnInfo, FilterRule, ForeignKey, SortRule, StagedChange, TablePage, TableRef, Value } from "@/lib/bindings";
import type { JsonValue } from "@/lib/bindings/serde_json/JsonValue";
import { ipc, normalizeError } from "@/lib/ipc";
import { DENSITIES, formatCell, formatCount } from "@/lib/format";
import { engineMeta } from "@/lib/engines";
import { tableKey, useWorkspace } from "@/stores/workspace";
import { DataGrid, type GridColumn, type StagedCell } from "./DataGrid";
import { FilterPopover } from "./FilterPopover";
import { AppSelect } from "@/components/global/Field";
import { FormValueField } from "@/components/global/ValueEditor";
import { JsonViewer } from "@/components/global/JsonViewer";
import { IconButton } from "@/components/global/Button";
import { EmptyState } from "@/components/global/EmptyState";
import { Segmented } from "@/components/global/Field";
import { Resizer } from "@/components/global/Resizer";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

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
const NULL_VALUE: Value = { t: "null" };
type InsertChange = Extract<StagedChange, { kind: "insert" }>;

const INSPECTOR_WIDTH_KEY = "db-free:inspector-width";
const INSPECTOR_COLLAPSED_KEY = "db-free:inspector-collapsed";
const INSPECTOR_MIN = 260;
const INSPECTOR_MAX = 650;
const INSPECTOR_DEFAULT = 384;
/// Dragging the splitter narrower than this folds the inspector into its rail.
const INSPECTOR_FOLD_AT = 200;

// localStorage can throw (blocked site data); a missing value just means "default".
function readStored(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeStored(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // ignore
  }
}

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
  /// Right-click "Filter: col = value" builds a rule from the clicked cell. The
  /// rule travels with the page request, so it works on every engine — SQL
  /// adapters compile it into WHERE, the others filter the fetched rows.
  const addFilter = useCallback((rule: FilterRule) => {
    setPageIndex(0);
    setFilters((prev) => [...prev.filter((f) => !(f.column === rule.column && f.op === rule.op)), rule]);
  }, []);
  const [refresh, setRefresh] = useState(0);
  const [loaded, setLoaded] = useState<Loaded>({ key: "", page: null, error: null });
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set());
  const [cell, setCell] = useState<{ row: number; col: number } | null>(null);
  const [insertOpen, setInsertOpen] = useState(false);
  const [inspectorTab, setInspectorTab] = useState<string>(settings?.inspectorTabs[0] ?? "fields");
  // Folded inspector = narrow rail; the selection survives so it reopens on the same cell.
  const [inspectorCollapsed, setInspectorCollapsed] = useState<boolean>(() => readStored(INSPECTOR_COLLAPSED_KEY) === "1");
  const toggleInspector = useCallback(() => setInspectorCollapsed((c) => !c), []);
  useEffect(() => writeStored(INSPECTOR_COLLAPSED_KEY, inspectorCollapsed ? "1" : "0"), [inspectorCollapsed]);

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
  const pkColumns = useMemo(() => columns.filter((c) => c.primaryKey), [columns]);

  // WHAT:  Staged changes for this table mapped onto grid rows: updated cells by
  //        "row:col" (with the old value for the tooltip), deleted rows by index,
  //        and staged inserts appended as editable ghost rows after the page.
  const tableChanges = useMemo(() => pending.filter((c) => tableKey(c.table) === tableKey(table)), [pending, table]);
  const rowIndexOf = useCallback(
    (key: readonly CellValue[]) => rows.findIndex((r) => key.every((k) => JSON.stringify(r[columns.findIndex((col) => col.name === k.column)]) === JSON.stringify(k.value))),
    [rows, columns],
  );
  const staged = useMemo(() => {
    const map = new Map<string, StagedCell>();
    for (const c of tableChanges) {
      if (c.kind !== "update") continue;
      const rowIndex = rowIndexOf(c.key);
      const colIndex = columns.findIndex((col) => col.name === c.column);
      if (rowIndex >= 0 && colIndex >= 0) map.set(`${rowIndex}:${colIndex}`, { value: c.new, old: c.old });
    }
    return map;
  }, [tableChanges, rowIndexOf, columns]);
  const deletedRows = useMemo(() => new Set(tableChanges.filter((c) => c.kind === "delete").map((c) => rowIndexOf(c.key)).filter((i) => i >= 0)), [tableChanges, rowIndexOf]);
  const inserts = useMemo(() => tableChanges.filter((c): c is InsertChange => c.kind === "insert"), [tableChanges]);
  const allRows = useMemo(
    () => [...rows, ...inserts.map((c) => columns.map((col) => c.values.find((v) => v.column === col.name)?.value ?? NULL_VALUE))],
    [rows, inserts, columns],
  );
  const getRow = useCallback((i: number): readonly Value[] | undefined => allRows[i], [allRows]);

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
    const column = columns[colIndex];
    if (!column) return;
    if (rowIndex >= rows.length) {
      // Ghost row: the edit rewrites the staged insert itself (same id → replaced in place).
      const insert = inserts[rowIndex - rows.length];
      if (!insert) return;
      const values = columns.flatMap((col) => {
        if (col.name === column.name) return [{ column: col.name, value: next }];
        const existing = insert.values.find((v) => v.column === col.name);
        return existing ? [existing] : [];
      });
      stageChange(connectionId, { ...insert, values });
      return;
    }
    if (deletedRows.has(rowIndex)) {
      showError("This row is staged for deletion. Undo the delete in Pending Changes first.");
      return;
    }
    const row = rows[rowIndex];
    if (!row) return;
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
    // Selected ghost rows: drop the staged insert instead of staging a delete.
    for (const i of selectedRows) {
      const insert = i >= rows.length ? inserts[i - rows.length] : undefined;
      if (insert) unstageChange(connectionId, insert.id);
    }
    const real = [...selectedRows].filter((i) => i < rows.length && !deletedRows.has(i));
    if (real.length > 0 && pkColumns.length === 0) {
      showError("This table has no primary key, so rows cannot be deleted safely.");
      return;
    }
    const changes: StagedChange[] = real
      .map((i) => rows[i])
      .filter((r): r is Value[] => r !== undefined)
      .map((r) => ({ kind: "delete", id: nextChangeId(), table, key: keyOf(r) }));
    if (changes.length === 0) {
      setSelectedRows(new Set());
      return;
    }
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

  const selectedRow = cell ? allRows[cell.row] : undefined;
  const selectedValue = cell ? selectedRow?.[cell.col] : undefined;
  const selectedColumn = cell ? columns[cell.col] : undefined;

  return (
    <div className="flex h-full min-h-0 flex-col">
      <ScrollShadow orientation="horizontal" hideScrollBar className="flex h-11 shrink-0 items-center gap-1.5 border-b border-border/40 glass-header px-3">
        <Tooltip delay={300}>
          <Button size="sm" isDisabled={!editable || columns.length === 0} onPress={() => setInsertOpen(true)} className="glass-pill text-foreground liquid-hover">
            <Icon name="plus" size={13} className="text-accent" />
            Insert
          </Button>
          <Tooltip.Content>{readOnly ? "Connection is read-only." : editable ? "Insert a row" : "Editing is only available for SQL engines."}</Tooltip.Content>
        </Tooltip>
        <Button size="sm" variant="ghost" className="text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover rounded-lg" onPress={() => setRefresh((r) => r + 1)}>
          <Icon name="refresh" size={13} />
          Refresh
        </Button>
        <FilterPopover columns={columns} filters={filters} onApply={(next) => { setPageIndex(0); setFilters(next); }} />
        <Button size="sm" variant={sort.length > 0 ? "primary" : "ghost"} className={cn("rounded-lg liquid-hover", sort.length > 0 ? "glass-pill text-accent" : "text-muted hover:bg-surface-secondary/70 hover:text-foreground")} onPress={() => setSort([])} isDisabled={sort.length === 0}>
          <Icon name="sort" size={13} />
          {sort.length > 0 ? `Sorted by ${sort.length} rule` : "Sort"}
        </Button>
        <Dropdown>
          <Button size="sm" variant="ghost" className="text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover rounded-lg" isDisabled={rows.length === 0}>
            <Icon name="download" size={13} />
            Export
            <Icon name="chevron-down" size={12} />
          </Button>
          <Dropdown.Popover className="glass-modal rounded-xl">
            <Dropdown.Menu onAction={(key) => void copyAs(String(key) === "json" ? "json" : "csv")}>
              <Dropdown.Item id="csv" textValue="Copy as CSV"><Label>Copy {selectedRows.size > 0 ? "selection" : "page"} as CSV</Label></Dropdown.Item>
              <Dropdown.Item id="json" textValue="Copy as JSON"><Label>Copy {selectedRows.size > 0 ? "selection" : "page"} as JSON</Label></Dropdown.Item>
            </Dropdown.Menu>
          </Dropdown.Popover>
        </Dropdown>
        {selectedRows.size > 0 && editable ? (
          <Button size="sm" variant="danger-soft" className="rounded-lg liquid-hover" onPress={deleteSelected}>
            <Icon name="trash" size={13} />
            Delete {selectedRows.size}
          </Button>
        ) : null}

        <div className="ml-auto flex shrink-0 items-center gap-2 text-xs whitespace-nowrap text-muted">
          {loading ? <span className="text-accent font-medium">loading…</span> : null}
          {selectedRows.size > 0 ? (
            <Chip size="sm" variant="soft" color="accent" className="font-medium">
              {selectedRows.size} selected
            </Chip>
          ) : null}
          <IconButton icon="columns" label={cell === null ? "Inspect selected cell" : inspectorCollapsed ? "Show inspector" : "Hide inspector"} active={cell !== null && !inspectorCollapsed} isDisabled={cell === null} onPress={toggleInspector} />
          <Separator orientation="vertical" className="mx-0.5 h-4 opacity-50" />
          <div className="flex items-center gap-1 rounded-lg glass-pill px-1.5 py-0.5">
            <IconButton icon="chevron-left" label="Previous page" isDisabled={pageIndex === 0} onPress={() => setPageIndex((p) => Math.max(0, p - 1))} size={13} className="size-5 min-w-5" />
            <span className="px-1 tabular-nums font-mono text-[11px] text-foreground">
              {pageIndex + 1}
              <span className="text-muted"> / {pageCount ?? "…"}</span>
            </span>
            <IconButton icon="chevron-right" label="Next page" isDisabled={!hasNext} onPress={() => setPageIndex((p) => p + 1)} size={13} className="size-5 min-w-5" />
          </div>
          <AppSelect ariaLabel="Rows per page" value={pageSize} options={PAGE_SIZES} size="sm" className="w-24 shrink-0" onChange={(v) => { setPageIndex(0); setPageSize(v); }} />
          <span className="min-w-16 text-right tabular-nums font-mono text-[11px] text-muted">{total !== null ? `${page?.totalExact ? "" : "≈ "}${formatCount(total)} rows` : `${formatCount(rows.length)} rows`}</span>
        </div>
      </ScrollShadow>

      <div className="flex min-h-0 flex-1">
        <div className="min-w-0 flex-1">
          {loaded.error !== null ? (
            <EmptyState icon="table" title="Could not load table" body={loaded.error} action={<Button size="sm" onPress={() => setRefresh((r) => r + 1)}>Retry</Button>} />
          ) : (
            <DataGrid
              columns={gridColumns}
              rowCount={allRows.length}
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
              onToggleAll={() => setSelectedRows((s) => (s.size === allRows.length ? new Set() : new Set(allRows.map((_, i) => i))))}
              onCellSelect={(row, col) => setCell({ row, col })}
              selected={cell}
              {...(editable ? { onCellEdit } : {})}
              staged={staged}
              deletedRows={deletedRows}
              insertedFrom={rows.length}
              nullDisplay={settings?.nullDisplay ?? "NULL"}
              onLinkOpen={openLinked}
              onFilter={addFilter}
              onSortSet={(rule) => { setPageIndex(0); setSort([rule]); }}
              onClearSort={() => setSort([])}
              onCopied={(what) => showInfo(`${what} copied to the clipboard.`)}
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
            collapsed={inspectorCollapsed}
            onToggle={toggleInspector}
            onClose={() => setCell(null)}
          />
        ) : null}
      </div>

      <InsertRowModal open={insertOpen} onClose={() => setInsertOpen(false)} columns={columns} onSubmit={(values) => { setInsertOpen(false); void applyChanges([{ kind: "insert", id: nextChangeId(), table, values }]); }} />
    </div>
  );
}

// WHAT:  Insert form: one typed control per column (number field, date / time
//        picker, JSON editor, true/false select, text). Empty = omitted so the
//        database default applies; the NULL toggle sends an explicit NULL.
function InsertRowModal({ open, onClose, columns, onSubmit }: { open: boolean; onClose: () => void; columns: readonly ColumnInfo[]; onSubmit: (values: CellValue[]) => void }) {
  const [values, setValues] = useState<Record<string, Value | undefined>>({});
  const submit = () => {
    const out: CellValue[] = [];
    for (const c of columns) {
      const value = values[c.name];
      if (value !== undefined) out.push({ column: c.name, value });
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
            <Modal.Body className="max-h-[60vh] p-0">
              <ScrollShadow className="max-h-[60vh] px-4 py-3">
                <div className="grid grid-cols-2 gap-3">
                  {columns.map((c) => (
                    <FormValueField key={c.name} column={c} value={values[c.name]} onChange={(v) => setValues((s) => ({ ...s, [c.name]: v }))} />
                  ))}
                </div>
              </ScrollShadow>
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
//        Folds into a narrow rail from the toolbar button, the header chevron,
//        or by dragging the splitter past the minimum width; the rail expands
//        it again. Width and folded state persist in localStorage.
function RecordInspector({ columns, row, column, value, table, tabs, activeTab, onTab, collapsed, onToggle, onClose }: { columns: readonly ColumnInfo[]; row: readonly Value[]; column: ColumnInfo; value: Value; table: TableRef; tabs: readonly string[]; activeTab: string; onTab: (t: string) => void; collapsed: boolean; onToggle: () => void; onClose: () => void }) {
  const current = tabs.includes(activeTab) ? activeTab : (tabs[0] ?? "fields");
  const record = Object.fromEntries(columns.map((c, i) => [c.name, plainValue(row[i])]));
  const insertSql = `INSERT INTO ${table.schema ? `"${table.schema}".` : ""}"${table.name}" (${columns.map((c) => `"${c.name}"`).join(", ")})\nVALUES (${row.map((v) => sqlLiteral(v)).join(", ")});`;
  const [width, setWidth] = useState<number>(() => {
    const saved = Number(readStored(INSPECTOR_WIDTH_KEY));
    return Number.isFinite(saved) && saved > 0 ? Math.max(INSPECTOR_MIN, Math.min(INSPECTOR_MAX, saved)) : INSPECTOR_DEFAULT;
  });
  // Mirrors `width` so a drag can decide to fold without a side effect inside a state updater.
  const widthRef = useRef(width);

  const handleResize = useCallback(
    (delta: number) => {
      const next = widthRef.current - delta;
      if (next < INSPECTOR_FOLD_AT) {
        onToggle();
        return;
      }
      const clamped = Math.max(INSPECTOR_MIN, Math.min(INSPECTOR_MAX, next));
      widthRef.current = clamped;
      setWidth(clamped);
      writeStored(INSPECTOR_WIDTH_KEY, String(clamped));
    },
    [onToggle],
  );

  if (collapsed) {
    return (
      <aside className="flex w-9 shrink-0 flex-col items-center gap-1 border-l border-border/40 glass-sidebar py-1.5 select-none">
        <IconButton icon="chevron-left" label="Expand inspector" onPress={onToggle} size={13} className="size-6 min-w-6" />
        <Tooltip delay={500}>
          <Button
            variant="ghost"
            size="sm"
            aria-label={`Expand inspector for ${column.name}`}
            onPress={onToggle}
            className="h-auto min-h-0 w-6 min-w-0 flex-1 overflow-hidden rounded-md px-0 py-2 font-mono text-[11px] whitespace-nowrap text-muted [writing-mode:vertical-rl] rotate-180 hover:text-foreground"
          >
            {column.name}
          </Button>
          <Tooltip.Content>
            {column.name} · {column.dataType}
          </Tooltip.Content>
        </Tooltip>
        <CloseButton onPress={onClose} aria-label="Close inspector" />
      </aside>
    );
  }

  return (
    <aside className="relative flex shrink-0 flex-col border-l border-border/40 glass-sidebar select-none" style={{ width }}>
      <Resizer direction="horizontal" onResize={handleResize} className="absolute -left-1 top-0 bottom-0" />
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-border/40 glass-header px-3 text-xs">
        <span className="truncate font-semibold text-foreground tracking-tight">{column.name}</span>
        <Chip size="sm" variant="soft" className="font-mono text-[10px]">
          {column.dataType}
        </Chip>
        <span className="ml-auto flex items-center gap-0.5">
          <IconButton icon="chevron-right" label="Collapse inspector" onPress={onToggle} size={13} className="size-6 min-w-6" />
          <CloseButton onPress={onClose} aria-label="Close inspector" />
        </span>
      </div>
      <div className="px-3 py-2">
        <Segmented label="Inspector tab" value={current} onChange={onTab} options={tabs.map((t) => ({ value: t, label: t.toUpperCase() }))} />
      </div>
      <ScrollShadow className="min-h-0 flex-1">
        {current === "fields" ? (
          <dl className="px-3 pb-3 text-xs">
            {columns.map((c, i) => (
              <div key={c.name} className={`flex flex-col gap-0.5 border-b border-separator py-1.5 ${c.name === column.name ? "text-foreground" : "text-muted"}`}>
                <dt className="flex items-center gap-1.5 font-medium"><Icon name={c.primaryKey ? "key" : "text"} size={11} />{c.name}<span className="ml-auto font-mono text-[10px]">{c.dataType}</span></dt>
                {row[i]?.t === "json" ? <JsonViewer bare value={row[i].v} defaultDepth={1} className="pl-3" /> : <dd className="selectable truncate font-mono">{cellText(row[i])}</dd>}
              </div>
            ))}
          </dl>
        ) : current === "json" ? (
          <JsonViewer value={record} className="p-3" />
        ) : current === "sql" ? (
          <pre className="selectable p-3 font-mono text-[11px] leading-relaxed break-all whitespace-pre-wrap text-foreground">{insertSql}</pre>
        ) : (
          <pre className="selectable p-3 font-mono text-[11px] whitespace-pre-wrap text-foreground">{inspectorBody(value)}</pre>
        )}
      </ScrollShadow>
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
