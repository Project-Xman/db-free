// SOT: value-editor, typed-cell-editor, insert-form-field, json-editor-modal
import { useMemo, useRef, useState, type KeyboardEvent } from "react";
import { Button, Input, Modal, TextArea } from "@heroui/react";
import { ToggleButton } from "@heroui/react";
import type { ColumnInfo, Value } from "@/lib/bindings";
import type { JsonValue } from "@/lib/bindings/serde_json/JsonValue";
import { parseJson } from "@/lib/json";
import { editText, fieldKind, parseEdited, type FieldKind } from "@/lib/fields";
import { AppSelect, DateTimeField, Field, NumberInput } from "./Field";
import { JsonViewer } from "./JsonViewer";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

interface CellEditorProps {
  typeName: string;
  value: Value;
  onCommit: (next: Value) => void;
  onCancel: () => void;
}

// WHAT:  Inline editor for one grid cell, picked by field kind:
//          text / decimal        → Input (typing `NULL` stages a null)
//          int / float           → NumberInput
//          date / time / datetime → DateTimeField (calendar + segments)
//          json                  → JsonEditorModal (tree + raw)
//        Enter commits, Escape cancels. Text and number editors also cancel on
//        blur; the date editors keep focus in their popover, so they carry
//        explicit ✓ / ✕ buttons. The latest draft lives in a ref so an Enter
//        that also changes the value (calendar cell) commits the new value.
// WHERE: src/features/grid/DataGrid.tsx
export function CellEditor({ typeName, value, onCommit, onCancel }: CellEditorProps) {
  const kind = fieldKind(typeName, value);
  const draft = useRef<string>(editText(value));
  const number = useRef<number | null>(value.t === "int" || value.t === "float" ? value.v : null);
  const [text, setText] = useState(draft.current);

  const keys = (commit: () => void) => (e: KeyboardEvent<HTMLElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      commit();
    } else if (e.key === "Escape") {
      e.preventDefault();
      onCancel();
    }
  };
  const commitText = () => onCommit(parseEdited(draft.current, typeName, value));
  const commitNumber = () => onCommit(number.current === null ? { t: "null" } : { t: kind === "int" ? "int" : "float", v: number.current });
  const commitDate = () => onCommit(draft.current === "" ? { t: "null" } : { t: "date_time", v: draft.current });

  switch (kind) {
    case "bool":
    case "bytes":
      return null;
    case "int":
    case "float":
      return (
        <div className="h-[calc(100%-4px)] w-full" onKeyDown={keys(commitNumber)} onBlur={onCancel}>
          <NumberInput compact autoFocus integer={kind === "int"} ariaLabel="Edit number" value={number.current} onChange={(n) => (number.current = n)} />
        </div>
      );
    case "date":
    case "time":
    case "datetime":
      return (
        <div className="flex h-[calc(100%-4px)] w-full min-w-0 items-center gap-0.5" onKeyDown={keys(commitDate)}>
          <div className="h-full min-w-0 flex-1">
            <DateTimeField
              compact
              autoFocus
              kind={kind}
              ariaLabel={`Edit ${kind}`}
              value={text}
              onChange={(next) => {
                draft.current = next;
                setText(next);
              }}
            />
          </div>
          <Button isIconOnly size="sm" variant="ghost" aria-label="Apply" onPress={commitDate} className="size-5 min-w-5 shrink-0 rounded-sm p-0 text-success">
            <Icon name="check" size={12} />
          </Button>
          <Button isIconOnly size="sm" variant="ghost" aria-label="Cancel" onPress={onCancel} className="size-5 min-w-5 shrink-0 rounded-sm p-0 text-muted">
            <Icon name="x" size={12} />
          </Button>
        </div>
      );
    case "json":
      return (
        <JsonEditorModal
          open
          title={`Edit ${typeName}`}
          initial={value.t === "json" ? value.v : null}
          onSave={(v) => onCommit({ t: "json", v })}
          onClose={onCancel}
          secondaryAction={{ label: "Set NULL", onPress: () => onCommit({ t: "null" }) }}
        />
      );
    case "decimal":
    case "text":
      return (
        <Input
          autoFocus
          value={text}
          onChange={(e) => {
            draft.current = e.target.value;
            setText(e.target.value);
          }}
          onKeyDown={keys(commitText)}
          onBlur={onCancel}
          inputMode={kind === "decimal" ? "decimal" : undefined}
          className="h-[calc(100%-4px)] w-full rounded-sm border border-accent bg-background px-1 font-mono text-[12px] text-foreground"
          aria-label="Edit cell"
        />
      );
  }
}

interface FormValueFieldProps {
  column: ColumnInfo;
  /// undefined = omitted (database default applies); { t: "null" } = explicit NULL.
  value: Value | undefined;
  onChange: (next: Value | undefined) => void;
}

// WHAT:  Insert-form control for one column, same kind → control mapping as
//        the grid, plus a NULL toggle for nullable columns.
// WHERE: src/features/grid/TableTab.tsx (InsertRowModal)
export function FormValueField({ column, value, onChange }: FormValueFieldProps) {
  const kind = fieldKind(column.dataType);
  const isNull = value?.t === "null";
  const label = `${column.name}${column.primaryKey ? " (PK)" : ""} · ${column.dataType}`;
  return (
    <div className="flex items-end gap-1.5">
      <div className="min-w-0 flex-1">
        <FormControl kind={kind} column={column} label={label} value={value} isNull={isNull} onChange={onChange} />
      </div>
      {column.nullable ? (
        <ToggleButton size="sm" isSelected={isNull} onChange={(selected) => onChange(selected ? { t: "null" } : undefined)} aria-label={`Set ${column.name} to NULL`} className="mb-px h-9 min-w-0 shrink-0 px-2 font-mono text-[10px]">
          NULL
        </ToggleButton>
      ) : null}
    </div>
  );
}

function FormControl({ kind, column, label, value, isNull, onChange }: { kind: FieldKind; column: ColumnInfo; label: string; value: Value | undefined; isNull: boolean; onChange: (next: Value | undefined) => void }) {
  const [jsonOpen, setJsonOpen] = useState(false);
  switch (kind) {
    case "int":
    case "float":
      return <NumberInput label={label} integer={kind === "int"} isDisabled={isNull} value={value?.t === "int" || value?.t === "float" ? value.v : null} onChange={(n) => onChange(n === null ? undefined : { t: kind === "int" ? "int" : "float", v: n })} />;
    case "bool":
      return (
        <AppSelect
          label={label}
          isDisabled={isNull}
          value={value?.t === "bool" ? (value.v ? "true" : "false") : "default"}
          options={[
            { value: "default", label: column.nullable ? "default" : "—" },
            { value: "true", label: "true" },
            { value: "false", label: "false" },
          ]}
          onChange={(v) => onChange(v === "default" ? undefined : { t: "bool", v: v === "true" })}
        />
      );
    case "date":
    case "time":
    case "datetime":
      return <DateTimeField kind={kind} label={label} isDisabled={isNull} value={value?.t === "date_time" ? value.v : ""} onChange={(t) => onChange(t === "" ? undefined : { t: "date_time", v: t })} />;
    case "json": {
      const summary = value?.t === "json" ? JSON.stringify(value.v) : "";
      return (
        <div className="flex w-full flex-col gap-1">
          <span className="text-sm font-medium text-foreground">{label}</span>
          <Button variant="tertiary" isDisabled={isNull} onPress={() => setJsonOpen(true)} className="w-full justify-start truncate font-mono text-xs">
            <Icon name="braces" size={13} className="shrink-0 text-accent" />
            <span className={cn("truncate", summary ? "text-foreground" : "text-muted")}>{summary || "Edit JSON…"}</span>
          </Button>
          <JsonEditorModal
            open={jsonOpen}
            title={column.name}
            initial={value?.t === "json" ? value.v : null}
            onSave={(v) => {
              onChange({ t: "json", v });
              setJsonOpen(false);
            }}
            onClose={() => setJsonOpen(false)}
            secondaryAction={{
              label: "Use default",
              onPress: () => {
                onChange(undefined);
                setJsonOpen(false);
              },
            }}
          />
        </div>
      );
    }
    case "bytes":
    case "decimal":
    case "text":
      return (
        <Field
          label={label}
          mono
          isDisabled={isNull}
          value={isNull || value === undefined ? "" : editText(value)}
          onChange={(t) => onChange(t.length === 0 ? undefined : parseEdited(t, column.dataType, undefined))}
          placeholder={column.nullable ? "default" : column.dataType}
        />
      );
  }
}

interface JsonEditorModalProps {
  open: boolean;
  title: string;
  initial: JsonValue | null;
  onSave: (value: JsonValue) => void;
  onClose: () => void;
  secondaryAction?: { label: string; onPress: () => void };
}

// WHAT:  JSON editor: raw text on the left (Format / Minify), live collapsible
//        tree on the right; Save is disabled until the text parses.
export function JsonEditorModal({ open, title, initial, onSave, onClose, secondaryAction }: JsonEditorModalProps) {
  return (
    <Modal isOpen={open} onOpenChange={(o) => !o && onClose()}>
      <Modal.Backdrop>
        <Modal.Container>
          <Modal.Dialog className="sm:max-w-[940px]">
            <Modal.CloseTrigger />
            <Modal.Header>
              <Modal.Heading className="flex items-center gap-2">
                <Icon name="braces" size={15} className="text-accent" />
                {title}
              </Modal.Heading>
            </Modal.Header>
            {open ? <JsonEditorBody initial={initial} onSave={onSave} onClose={onClose} secondaryAction={secondaryAction} /> : null}
          </Modal.Dialog>
        </Modal.Container>
      </Modal.Backdrop>
    </Modal>
  );
}

function JsonEditorBody({ initial, onSave, onClose, secondaryAction }: Omit<JsonEditorModalProps, "open" | "title">) {
  const [text, setText] = useState(() => (initial === null ? "" : JSON.stringify(initial, null, 2)));
  const parsed = useMemo(() => (text.trim().length === 0 ? undefined : parseJson(text)), [text]);
  const valid = parsed !== undefined;
  return (
    <>
      <Modal.Body className="max-h-[70vh] overflow-hidden">
        <div className="grid h-[52vh] min-h-[320px] grid-cols-1 gap-3 md:grid-cols-2">
          <div className="flex min-h-0 flex-col gap-1.5">
            <div className="flex h-6 items-center gap-1 text-xs text-muted">
              <span>Raw</span>
              <span className="ml-auto" />
              <Button size="sm" variant="ghost" isDisabled={!valid} className="h-6 min-w-0 rounded-md px-1.5 text-[11px]" onPress={() => parsed !== undefined && setText(JSON.stringify(parsed, null, 2))}>
                Format
              </Button>
              <Button size="sm" variant="ghost" isDisabled={!valid} className="h-6 min-w-0 rounded-md px-1.5 text-[11px]" onPress={() => parsed !== undefined && setText(JSON.stringify(parsed))}>
                Minify
              </Button>
            </div>
            <TextArea
              aria-label="JSON source"
              value={text}
              onChange={(e) => setText(e.target.value)}
              spellCheck={false}
              placeholder='{ "key": "value" }'
              className={cn("min-h-0 flex-1 resize-none font-mono text-[12px] leading-relaxed", valid ? "" : "border-danger")}
            />
            <span className={cn("h-4 text-[11px]", valid ? "text-muted" : "text-danger")}>{valid ? (parsed === null ? "null" : Array.isArray(parsed) ? `array · ${parsed.length} items` : typeof parsed === "object" ? `object · ${Object.keys(parsed).length} keys` : typeof parsed) : "Not valid JSON."}</span>
          </div>
          <div className="flex min-h-0 min-w-0 flex-col gap-1.5">
            <div className="flex h-6 items-center text-xs text-muted">Tree</div>
            <div className="min-h-0 flex-1 overflow-auto rounded-lg border border-border/40 bg-background/60 p-2">
              {parsed !== undefined ? <JsonViewer value={parsed} defaultDepth={3} /> : <span className="text-xs text-muted">Fix the JSON on the left to preview it here.</span>}
            </div>
          </div>
        </div>
      </Modal.Body>
      <Modal.Footer>
        {secondaryAction ? (
          <Button variant="ghost" className="mr-auto text-muted hover:text-foreground" onPress={secondaryAction.onPress}>
            {secondaryAction.label}
          </Button>
        ) : null}
        <Button variant="tertiary" onPress={onClose}>
          Cancel
        </Button>
        <Button isDisabled={!valid} onPress={() => parsed !== undefined && onSave(parsed)}>
          Save
        </Button>
      </Modal.Footer>
    </>
  );
}
