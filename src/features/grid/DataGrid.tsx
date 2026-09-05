// SOT: data-grid, virtualized-grid, grid-cell-rendering, column-sort-header, column-resize, row-selection, inline-cell-edit, foreign-key-link, change-highlighting
import { useEffect, useRef, useState } from "react";
import { Button, ScrollShadow } from "@heroui/react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { SortRule, Value } from "@/lib/bindings";
import { cellClass, formatCell } from "@/lib/format";
import { fieldKind } from "@/lib/fields";
import { Icon, typeIcon } from "@/lib/icons";
import { Check } from "@/components/global/Field";
import { CellEditor } from "@/components/global/ValueEditor";
import { Resizer } from "@/components/global/Resizer";
import { cn } from "@/lib/cn";

export interface GridColumn {
  name: string;
  typeName: string;
  primaryKey?: boolean;
  /// Set when the column is a foreign key; cells get a link button.
  linkTo?: string;
}

/// A cell with a staged (uncommitted) edit: what it shows now and what the database still holds.
export interface StagedCell {
  value: Value;
  old: Value;
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
  /// Present = cells are editable (double-click). Receives the edited Value.
  onCellEdit?: (rowIndex: number, colIndex: number, next: Value) => void;
  /// Cells with a staged edit render highlighted with the staged value ("row:col" keys).
  staged?: ReadonlyMap<string, StagedCell>;
  /// Rows staged for deletion: red tint, struck through, not editable.
  deletedRows?: ReadonlySet<number>;
  /// Rows at or after this index are staged inserts: green tint, edits update the insert.
  insertedFrom?: number;
  nullDisplay?: string;
  /// Foreign-key traversal: called from the link button in a linked column's cell.
  onLinkOpen?: (rowIndex: number, colIndex: number) => void;
}

const HEADER_HEIGHT = 32;
const CHECK_WIDTH = 40;
const MIN_COL_WIDTH = 56;
const MAX_COL_WIDTH = 1600;
/// Grab area straddling a column's right edge. Wide enough to hit without
/// aiming: the visible line inside it stays 1px.
const HANDLE_WIDTH = 12;

// WHAT:  Two-axis virtualized grid: only visible rows AND columns are mounted.
// WHY:   PRD §4.2 — 10^6 rows at 60 FPS. Row data is pulled through `getRow`
//        so the owner keeps the data and fetches on demand.
// HOW:   TanStack Virtual for both axes; absolute positioning inside a single
//        scroll container; the header and the checkbox gutter are sticky.
//        Editing: double-click opens the typed CellEditor for the column kind
//        (text, number, date/time picker, JSON modal); Enter commits, Esc cancels.
//        Change highlighting: staged cell = amber with a left bar and a
//        "was: …" tooltip, staged delete = red strike-through row, staged
//        insert = green row appended after the fetched page.
//        Column resize: a Resizer straddles each header's right edge; widths
//        are keyed by column name so they survive a refetch, double-click
//        restores the type-based estimate.
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
  deletedRows,
  insertedFrom,
  nullDisplay = "NULL",
  onLinkOpen,
}: DataGridProps) {
  const parentRef = useRef<HTMLDivElement | null>(null);
  const rangeRef = useRef(onRangeChange);
  useEffect(() => {
    rangeRef.current = onRangeChange;
  });
  const [editing, setEditing] = useState<{ row: number; col: number } | null>(null);
  // Dragged widths by column name, in a ref rather than state: a drag must not
  // wait for a state round-trip (setState -> memo -> measure()) to be visible,
  // and at one event per pixel that round-trip is what made the handle look
  // dead. The virtualizer owns the rendered width via resizeItem(); this map is
  // only what a re-measure (new page, reset) reads back.
  const widthsRef = useRef<Record<string, number>>({});
  // Unclamped width of the column being dragged, so dragging past the minimum
  // and back does not leave the handle lagging behind the pointer.
  const dragWidth = useRef<number | null>(null);

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
    estimateSize: (index) => {
      const column = columns[index];
      return column ? (widthsRef.current[column.name] ?? estimateWidth(column)) : 160;
    },
    overscan: 4,
  });

  useEffect(() => {
    rowVirtualizer.measure();
  }, [rowHeight, rowVirtualizer]);
  // A new page or a different table re-reads the widths map, so a column keeps
  // the width it was dragged to and a new column starts from its estimate.
  useEffect(() => {
    colVirtualizer.measure();
  }, [columns, colVirtualizer]);

  const resizeColumn = (column: GridColumn, index: number, current: number, delta: number) => {
    const raw = (dragWidth.current ?? current) + delta;
    dragWidth.current = raw;
    const next = Math.min(MAX_COL_WIDTH, Math.max(MIN_COL_WIDTH, raw));
    if (widthsRef.current[column.name] === next) return;
    widthsRef.current[column.name] = next;
    // Writes the measurement and notifies: the header and every cell of that
    // column reposition on this frame.
    colVirtualizer.resizeItem(index, next);
  };
  const resetColumn = (column: GridColumn, index: number) => {
    if (!(column.name in widthsRef.current)) return;
    widthsRef.current = Object.fromEntries(Object.entries(widthsRef.current).filter(([name]) => name !== column.name));
    colVirtualizer.resizeItem(index, estimateWidth(column));
  };

  const totalWidth = colVirtualizer.getTotalSize();
  const totalHeight = rowVirtualizer.getTotalSize();
  const virtualRows = rowVirtualizer.getVirtualItems();
  const virtualCols = colVirtualizer.getVirtualItems();
  const selectable = selectedRows !== undefined && onToggleRow !== undefined;
  const gutter = selectable ? CHECK_WIDTH : 0;
  const allSelected = selectable && rowCount > 0 && selectedRows.size === rowCount;
  const someSelected = selectable && selectedRows.size > 0 && !allSelected;

  return (
    <ScrollShadow ref={parentRef} orientation="horizontal" className="h-full w-full overflow-y-auto bg-background/60 font-mono text-[12px] select-none">
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
            {virtualCols.map((vc) => {
              const column = columns[vc.index];
              if (!column) return null;
              return (
                <div
                  key={`${vc.key}-resize`}
                  className="absolute top-0 z-30 h-full cursor-col-resize"
                  style={{ left: vc.end - HANDLE_WIDTH / 2, width: HANDLE_WIDTH }}
                  title="Drag to resize · double-click to reset"
                  onDoubleClick={() => resetColumn(column, vc.index)}
                >
                  <Resizer
                    direction="horizontal"
                    onResize={(delta) => resizeColumn(column, vc.index, vc.size, delta)}
                    onDragEnd={() => {
                      dragWidth.current = null;
                    }}
                    className="absolute inset-0 w-full rounded-none"
                  />
                </div>
              );
            })}
          </div>
        </div>

        {virtualRows.map((vr) => {
          const row = getRow(vr.index);
          const isSelectedRow = selected?.row === vr.index;
          const isChecked = selectedRows?.has(vr.index) ?? false;
          const isDeleted = deletedRows?.has(vr.index) ?? false;
          const isInserted = insertedFrom !== undefined && vr.index >= insertedFrom;
          return (
            <div
              key={vr.key}
              className={cn(
                "absolute left-0 flex border-b border-separator/40 transition-colors duration-100",
                isChecked
                  ? "bg-accent/15"
                  : isDeleted
                    ? "bg-danger-soft/50 line-through decoration-danger/60"
                    : isInserted
                      ? "bg-success-soft/40"
                      : isSelectedRow
                        ? "bg-surface-secondary/80"
                        : vr.index % 2 === 1
                          ? "bg-surface/20 hover:bg-surface-secondary/40"
                          : "hover:bg-surface-secondary/40",
              )}
              style={{ top: vr.start + HEADER_HEIGHT, height: vr.size, width: totalWidth + gutter }}
              title={isDeleted ? "Staged for deletion" : isInserted ? "Staged insert" : undefined}
            >
              {selectable ? (
                <div className={cn("sticky left-0 z-10 flex shrink-0 items-center justify-center border-r border-separator/40 backdrop-blur-sm", isDeleted ? "bg-danger-soft/70" : isInserted ? "bg-success-soft/70" : "bg-surface/70")} style={{ width: CHECK_WIDTH }}>
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
                    const column = columns[vc.index];
                    const stagedCell = staged?.get(`${vr.index}:${vc.index}`);
                    const value = stagedCell?.value ?? row[vc.index];
                    const formatted = value === undefined ? null : formatCell(value);
                    const isSelected = isSelectedRow && selected.col === vc.index;
                    const isEditing = editing !== null && editing.row === vr.index && editing.col === vc.index;
                    const kind = fieldKind(column?.typeName ?? "", value);
                    const editable = onCellEdit !== undefined && !isDeleted && value !== undefined && kind !== "bytes" && kind !== "bool";
                    const linked = column?.linkTo !== undefined && onLinkOpen !== undefined && value !== undefined && value.t !== "null";
                    // One colour per cell: a change state wins over the value-kind syntax colour.
                    const tone = stagedCell !== undefined ? "font-medium text-warning" : isDeleted ? "text-danger" : isInserted ? "text-success" : formatted ? cellClass(formatted.kind) : "";
                    return (
                      <div
                        key={vc.key}
                        onClick={() => onCellSelect?.(vr.index, vc.index)}
                        onDoubleClick={() => {
                          if (editable) setEditing({ row: vr.index, col: vc.index });
                        }}
                        className={cn(
                          "absolute top-0 flex h-full cursor-default items-center truncate border-r border-separator/40 px-2",
                          tone,
                          stagedCell !== undefined ? "border-l-2 border-l-warning bg-warning-soft" : "",
                          isSelected ? "ring-1 ring-accent ring-inset" : "",
                          isSelected && stagedCell === undefined ? "bg-accent/20" : "",
                          isEditing ? "select-text" : "",
                        )}
                        style={{ left: vc.start, width: vc.size }}
                        title={stagedCell !== undefined ? `${formatted?.text ?? ""}\nwas: ${formatCell(stagedCell.old).text}` : formatted?.text}
                      >
                        {isEditing && value !== undefined && onCellEdit ? (
                          <CellEditor
                            typeName={column?.typeName ?? ""}
                            value={value}
                            onCommit={(next) => {
                              setEditing(null);
                              onCellEdit(vr.index, vc.index, next);
                            }}
                            onCancel={() => setEditing(null)}
                          />
                        ) : value?.t === "bool" ? (
                          // Interactive when the grid is editable: toggling stages / applies the edit.
                          <Check
                            label={value.v ? "true" : "false"}
                            checked={value.v}
                            {...(onCellEdit && !isDeleted ? { onChange: (next: boolean) => onCellEdit(vr.index, vc.index, { t: "bool", v: next }) } : {})}
                          />
                        ) : value?.t === "null" ? (
                          <span className={cn("truncate", stagedCell === undefined && !isDeleted && !isInserted ? "" : "italic")}>{nullDisplay}</span>
                        ) : (
                          <span className="truncate">{formatted?.text ?? ""}</span>
                        )}
                        {stagedCell !== undefined && !isEditing ? <span aria-hidden="true" className="ml-auto size-1.5 shrink-0 rounded-full bg-warning" /> : null}
                        {linked && !isEditing ? (
                          <Button
                            isIconOnly
                            variant="ghost"
                            size="sm"
                            aria-label={`Open ${column.linkTo ?? "related"} rows`}
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
    </ScrollShadow>
  );
}

function estimateWidth(column: GridColumn): number {
  const t = column.typeName.toLowerCase();
  if (t.includes("bool")) return 120;
  if (/int|serial|numeric|decimal|real|float|double/.test(t)) return 110;
  if (/timestamp|date|time/.test(t)) return 220;
  if (t.includes("uuid")) return 300;
  if (/json|text|blob|bytea/.test(t)) return 260;
  return Math.min(320, Math.max(140, column.name.length * 9 + 60));
}
