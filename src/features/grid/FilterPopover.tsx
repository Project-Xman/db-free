// SOT: filter-builder, filter-popover, filter-operators
import { useState } from "react";
import { Button, Popover } from "@heroui/react";
import type { ColumnInfo, FilterOp, FilterRule } from "@/lib/bindings";
import { AppSelect, Field } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { Icon } from "@/lib/icons";
import { keysOf } from "@/lib/records";

// WHAT:  Operator registry for the GUI filter builder, keyed by the Rust enum.
// WHERE: src-tauri/src/model/query.rs (FilterOp), src-tauri/src/integrations/sql.rs
export const FILTER_OPS = {
  eq: { label: "equals", needsValue: true },
  ne: { label: "not equals", needsValue: true },
  gt: { label: ">", needsValue: true },
  gte: { label: "≥", needsValue: true },
  lt: { label: "<", needsValue: true },
  lte: { label: "≤", needsValue: true },
  contains: { label: "contains", needsValue: true },
  starts_with: { label: "starts with", needsValue: true },
  ends_with: { label: "ends with", needsValue: true },
  in: { label: "in list (a, b, c)", needsValue: true },
  is_null: { label: "is null", needsValue: false },
  is_not_null: { label: "is not null", needsValue: false },
} satisfies Record<FilterOp, { label: string; needsValue: boolean }>;

const OP_OPTIONS = keysOf(FILTER_OPS).map((op) => ({ value: op, label: FILTER_OPS[op].label }));

interface FilterPopoverProps {
  columns: readonly ColumnInfo[];
  filters: readonly FilterRule[];
  onApply: (filters: FilterRule[]) => void;
}

export function FilterPopover({ columns, filters, onApply }: FilterPopoverProps) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<FilterRule[]>([]);
  const firstColumn = columns[0]?.name ?? "";

  const openWith = (next: boolean) => {
    if (next) setDraft(filters.length > 0 ? filters.map((f) => ({ ...f })) : [{ column: firstColumn, op: "eq", value: "" }]);
    setOpen(next);
  };

  const update = (index: number, partial: Partial<FilterRule>) => setDraft((d) => d.map((f, i) => (i === index ? { ...f, ...partial } : f)));
  const remove = (index: number) => setDraft((d) => d.filter((_, i) => i !== index));
  const apply = () => {
    onApply(draft.filter((f) => f.column.length > 0 && (!FILTER_OPS[f.op].needsValue || f.value.trim().length > 0)));
    setOpen(false);
  };

  return (
    <Popover isOpen={open} onOpenChange={openWith}>
      <Button size="sm" variant={filters.length > 0 ? "primary" : "ghost"} className={filters.length > 0 ? "" : "text-muted"}>
        <Icon name="filter" size={13} />
        {filters.length > 0 ? `Filter (${filters.length})` : "Filter"}
      </Button>
      <Popover.Content className="w-[560px] max-w-[90vw]">
        <Popover.Dialog>
          <Popover.Heading className="text-sm">Filters</Popover.Heading>
          <div className="mt-3 flex flex-col gap-2">
            {draft.map((rule, index) => (
              <div key={index} className="grid grid-cols-[1fr_150px_1fr_28px] items-end gap-2">
                <AppSelect ariaLabel="Column" value={rule.column} options={columns.map((c) => ({ value: c.name, label: c.name }))} onChange={(column) => update(index, { column })} size="sm" />
                <AppSelect ariaLabel="Operator" value={rule.op} options={OP_OPTIONS} onChange={(op) => update(index, { op })} size="sm" />
                <Field label="" value={rule.value} onChange={(value) => update(index, { value })} isDisabled={!FILTER_OPS[rule.op].needsValue} placeholder="value" mono className="[&_label]:hidden" />
                <IconButton icon="x" label="Remove filter" onPress={() => remove(index)} />
              </div>
            ))}
            <Button size="sm" variant="ghost" className="self-start text-muted" onPress={() => setDraft((d) => [...d, { column: firstColumn, op: "eq", value: "" }])}>
              <Icon name="plus" size={13} />
              Add filter
            </Button>
          </div>
          <div className="mt-4 flex justify-end gap-2">
            <Button size="sm" variant="tertiary" onPress={() => { onApply([]); setOpen(false); }}>
              Clear
            </Button>
            <Button size="sm" onPress={apply}>
              Apply
            </Button>
          </div>
        </Popover.Dialog>
      </Popover.Content>
    </Popover>
  );
}
