// SOT: data-grid, virtualized-grid, grid-cell-rendering, column-sort-header, row-selection, inline-cell-edit, foreign-key-link
import { useEffect, useMemo, useRef, useState } from "react";
import { Button, Input } from "@heroui/react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { SortRule, Value } from "@/lib/bindings";
import { parseJson } from "@/lib/json";
import { cellClass, formatCell } from "@/lib/format";
import { Icon, typeIcon } from "@/lib/icons";
import { Check } from "@/components/global/Field";
import { cn } from "@/lib/cn";

export interface GridColumn {
  name: string;
  typeName: string;
  primaryKey?: boolean;
  /// Set when the column is a foreign key; cells get a link button.
  linkTo?: string;
}

interface DataGridProps {
  columns: readonly GridColumn[];
  rowCount: number;
  getRow: (index: number) => readonly Value[] | undefined;
  rowHeight: number;
  onRangeChange?: (start: number, end: number) => void;
  onCellSelect?: (rowIndex: number, colIndex: number) => void;
  selected?: { row: number; col: number } | null;
  sort?: readonly SortRule[];
  onSortToggle?: (column: string) => void;
  selectedRows?: ReadonlySet<number>;
  onToggleRow?: (rowIndex: number) => void;
  onToggleAll?: () => void;
  /// Present = cells are editable (double-click). Receives the typed text or null.
  onCellEdit?: (rowIndex: number, colIndex: number, next: Value) => void;
  /// Cells with a staged edit render highlighted with the staged value.
  staged?: ReadonlyMap<string, Value>;
  nullDisplay?: string;
  /// Foreign-key traversal: called from the link button in a linked column's cell.
  onLinkOpen?: (rowIndex: number, colIndex: number) => void;
}

const HEADER_HEIGHT = 32;
const CHECK_WIDTH = 40;

// WHAT:  Two-axis virtualized grid: only visible rows AND columns are mounted.
// WHY:   PRD §4.2 — 10^6 rows at 60 FPS. Row data is pulled through `getRow`
//        so the owner keeps the data and fetches on demand.
// HOW:   TanStack Virtual for both axes; absolute positioning inside a single
//        scroll container; the header and the checkbox gutter are sticky.
//        Editing: double-click opens an inline input; Enter stages, Esc cancels;
//        typing `NULL` stages a null.
// WHERE: src/features/grid/TableTab.tsx, src/features/editor/ResultsPane.tsx
export function DataGrid({
  columns,
  rowCount,
  getRow,
  rowHeight,
  onRangeChange,
  onCellSelect,
  selected = null,
  sort = [],
  onSortToggle,
  selectedRows,
  onToggleRow,
  onToggleAll,
  onCellEdit,
  staged,
  nullDisplay = "NULL",
  onLinkOpen,
}: DataGridProps) {
  const parentRef = useRef<HTMLDivElement | null>(null);
  const rangeRef = useRef(onRangeChange);
  useEffect(() => {
    rangeRef.current = onRangeChange;
  });
  const [editing, setEditing] = useState<{ row: number; col: number; text: string } | null>(null);

  const widths = useMemo(() => columns.map((c) => estimateWidth(c)), [columns]);

  const rowVirtualizer = useVirtualizer({
    count: rowCount,
    getScrollElement: () => parentRef.current,
    estimateSize: () => rowHeight,
    overscan: 12,
    onChange: (instance) => {
      const range = instance.range;
      if (range && rangeRef.current) rangeRef.current(range.startIndex, range.endIndex);
    },
  });

  const colVirtualizer = useVirtualizer({
    horizontal: true,
    count: columns.length,
    getScrollElement: () => parentRef.current,
    estimateSize: (index) => widths[index] ?? 160,
    overscan: 4,
  });

  useEffect(() => {
    rowVirtualizer.measure();
  }, [rowHeight, rowVirtualizer]);

  const totalWidth = colVirtualizer.getTotalSize();
  const totalHeight = rowVirtualizer.getTotalSize();
  const virtualRows = rowVirtualizer.getVirtualItems();
  const virtualCols = colVirtualizer.getVirtualItems();
  const selectable = selectedRows !== undefined && onToggleRow !== undefined;
  const gutter = selectable ? CHECK_WIDTH : 0;
  const allSelected = selectable && rowCount > 0 && selectedRows.size === rowCount;
  const someSelected = selectable && selectedRows.size > 0 && !allSelected;

  const commitEdit = () => {
    if (!editing || !onCellEdit) return;
    const column = columns[editing.col];
    const original = getRow(editing.row)?.[editing.col];
    onCellEdit(editing.row, editing.col, parseEdited(editing.text, column?.typeName ?? "", original));
    setEditing(null);
  };

  return (
    <div ref={parentRef} className="relative h-full w-full overflow-auto bg-background/60 font-mono text-[12px] select-none">
      <div style={{ width: totalWidth + gutter, height: totalHeight + HEADER_HEIGHT, position: "relative" }}>
        <div className="sticky top-0 z-20 flex border-b border-border/50 glass-header" style={{ height: HEADER_HEIGHT, width: totalWidth + gutter }}>
          {selectable ? (
            <div className="sticky left-0 z-30 flex shrink-0 items-center justify-center border-r border-border/50 glass-header" style={{ width: CHECK_WIDTH }}>
              <Check label="Select all rows" checked={allSelected} indeterminate={someSelected} onChange={onToggleAll} />
            </div>
          ) : null}
          <div className="relative" style={{ width: totalWidth, height: HEADER_HEIGHT }}>
            {virtualCols.map((vc) => {
              const column = columns[vc.index];
              if (!column) return null;
              const rule = sort.find((s) => s.column === column.name);
              return (
                <Button
                  variant="ghost"
                  key={vc.key}
                  isDisabled={onSortToggle === undefined}
                  onPress={() => onSortToggle?.(column.name)}
                  className={cn("absolute top-0 flex h-full items-center justify-start gap-1.5 truncate border-r border-border/40 px-2.5 text-left font-sans liquid-hover rounded-none", onSortToggle ? "hover:bg-surface-secondary/70" : "cursor-default")}
                  style={{ left: vc.start, width: vc.size }}
                >
                  <Icon name={column.linkTo !== undefined && !column.primaryKey ? "link" : typeIcon(column.typeName, column.primaryKey)} size={12} className={cn("shrink-0", column.primaryKey ? "text-warning" : column.linkTo !== undefined ? "text-accent" : "text-muted")} />
                  <span className="truncate text-[12px] font-medium text-foreground">{column.name}</span>
                  {column.linkTo !== undefined ? <span className="truncate text-[10px] text-muted">→ {column.linkTo}</span> : null}
                  {rule ? <Icon name={rule.desc ? "arrow-down" : "arrow-up"} size={11} className="ml-auto shrink-0 text-accent" /> : null}
                </Button>
              );
            })}
          </div>
        </div>

        {virtualRows.map((vr) => {
          const row = getRow(vr.index);
          const isSelectedRow = selected?.row === vr.index;
          const isChecked = selectedRows?.has(vr.index) ?? false;
          return (
            <div
              key={vr.key}
              className={cn(
                "absolute left-0 flex border-b border-separator/40 transition-colors duration-100",
                isChecked ? "bg-accent/15" : isSelectedRow ? "bg-surface-secondary/80" : vr.index % 2 === 1 ? "bg-surface/20 hover:bg-surface-secondary/40" : "hover:bg-surface-secondary/40",
              )}
              style={{ top: vr.start + HEADER_HEIGHT, height: vr.size, width: totalWidth + gutter }}
            >
              {selectable ? (
                <div className="sticky left-0 z-10 flex shrink-0 items-center justify-center border-r border-separator/40 bg-surface/70 backdrop-blur-sm" style={{ width: CHECK_WIDTH }}>
                  <Check label={`Select row ${vr.index + 1}`} checked={isChecked} onChange={() => onToggleRow(vr.index)} />
                </div>
              ) : null}
              <div className="relative" style={{ width: totalWidth }}>
                {row === undefined ? (
                  <div className="absolute inset-y-0 left-2 flex items-center gap-2">
                    <span className="h-2 w-24 animate-pulse rounded-sm bg-surface-tertiary" />
                    <span className="h-2 w-40 animate-pulse rounded-sm bg-surface-tertiary" />
                  </div>
                ) : (
                  virtualCols.map((vc) => {
                    const stagedValue = staged?.get(`${vr.index}:${vc.index}`);
                    const value = stagedValue ?? row[vc.index];
                    const formatted = value === undefined ? null : formatCell(value);
                    const isSelected = isSelectedRow && selected.col === vc.index;
                    const isEditing = editing !== null && editing.row === vr.index && editing.col === vc.index;
                    const linked = columns[vc.index]?.linkTo !== undefined && onLinkOpen !== undefined && value !== undefined && value.t !== "null";
                    return (
                      <div
                        key={vc.key}
                        onClick={() => onCellSelect?.(vr.index, vc.index)}
                        onDoubleClick={() => {
                          if (onCellEdit && value !== undefined && value.t !== "bytes" && value.t !== "bool") setEditing({ row: vr.index, col: vc.index, text: editText(value) });
                        }}
                        className={cn(
                          "absolute top-0 flex h-full cursor-default items-center truncate border-r border-separator/40 px-2",
                          formatted ? cellClass(formatted.kind) : "",
                          isSelected ? "bg-accent/20 ring-1 ring-accent ring-inset" : "",
                          stagedValue !== undefined ? "bg-warning-soft text-warning" : "",
                        )}
                        style={{ left: vc.start, width: vc.size }}
                        title={formatted?.text}
                      >
                        {isEditing ? (
                          <Input
                            autoFocus
                            value={editing.text}
                            onChange={(e) => setEditing({ ...editing, text: e.target.value })}
                            onKeyDown={(e) => {
                              if (e.key === "Enter") commitEdit();
                              if (e.key === "Escape") setEditing(null);
                            }}
                            onBlur={() => setEditing(null)}
                            className="h-[calc(100%-4px)] w-full rounded-sm border border-accent bg-background px-1 font-mono text-[12px] text-foreground"
                            aria-label="Edit cell"
                          />
                        ) : value?.t === "bool" ? (
                          // Interactive when the grid is editable: toggling stages / applies the edit.
                          <Check
                            label={value.v ? "true" : "false"}
                            checked={value.v}
                            {...(onCellEdit ? { onChange: (next: boolean) => onCellEdit(vr.index, vc.index, { t: "bool", v: next }) } : {})}
                          />
                        ) : value?.t === "null" ? (
                          <span className="truncate">{nullDisplay}</span>
                        ) : (
                          <span className="truncate">{formatted?.text ?? ""}</span>
                        )}
                        {linked && !isEditing ? (
                          <Button
                            isIconOnly
                            variant="ghost"
                            size="sm"
                            aria-label={`Open ${columns[vc.index]?.linkTo ?? "related"} rows`}
                            onPress={() => {
                              onLinkOpen(vr.index, vc.index);
                            }}
                            className="ml-auto flex size-4.5 min-w-4.5 p-0 shrink-0 rounded-sm text-accent opacity-60 hover:bg-accent-soft hover:opacity-100"
                          >
                            <Icon name="link" size={11} />
                          </Button>
                        ) : null}
                      </div>
                    );
                  })
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function editText(value: Value): string {
  switch (value.t) {
    case "null":
      return "";
    case "json":
      return JSON.stringify(value.v);
    case "bool":
      return value.v ? "true" : "false";
    case "int":
    case "float":
      return String(value.v);
    case "decimal":
    case "text":
    case "bytes":
    case "date_time":
    case "unsupported":
      return value.v;
  }
}

// WHAT:  Turns typed text back into a Value using the column type as a hint.
export function parseEdited(text: string, typeName: string, original: Value | undefined): Value {
  const t = typeName.toLowerCase();
  if (text === "NULL") return { t: "null" };
  if (t.includes("bool") || original?.t === "bool") {
    const lower = text.trim().toLowerCase();
    if (lower === "true" || lower === "1" || lower === "t") return { t: "bool", v: true };
    if (lower === "false" || lower === "0" || lower === "f") return { t: "bool", v: false };
  }
  if (/int|serial/.test(t) || original?.t === "int") {
    const n = Number(text);
    if (Number.isInteger(n) && text.trim().length > 0) return { t: "int", v: n };
  }
  if (/numeric|decimal|money/.test(t) || original?.t === "decimal") {
    if (text.trim().length > 0 && !Number.isNaN(Number(text))) return { t: "decimal", v: text.trim() };
  }
  if (/real|float|double/.test(t) || original?.t === "float") {
    const n = Number(text);
    if (text.trim().length > 0 && !Number.isNaN(n)) return { t: "float", v: n };
  }
  if (t.includes("json") || original?.t === "json") {
    const parsed = parseJson(text);
    if (parsed !== undefined && typeof parsed === "object") return { t: "json", v: parsed };
  }
  if (/date|time/.test(t) || original?.t === "date_time") return { t: "date_time", v: text };
  return { t: "text", v: text };
}

function estimateWidth(column: GridColumn): number {
  const t = column.typeName.toLowerCase();
  if (t.includes("bool")) return 120;
  if (/int|serial|numeric|decimal|real|float|double/.test(t)) return 110;
  if (/timestamp|date|time/.test(t)) return 200;
  if (t.includes("uuid")) return 300;
  if (/json|text|blob|bytea/.test(t)) return 260;
  return Math.min(320, Math.max(140, column.name.length * 9 + 60));
}
