// SOT: value-editor, typed-cell-editor, insert-form-field, json-editor-modal
import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { Button, Input, Modal, ScrollShadow, SearchField, TextArea, ToggleButton } from "@heroui/react";
import type { ColumnInfo, Value } from "@/lib/bindings";
import type { JsonValue } from "@/lib/bindings/serde_json/JsonValue";
import { parseJson } from "@/lib/json";
import { editText, fieldKind, parseEdited, type FieldKind } from "@/lib/fields";
import { AppSelect, DateTimeField, Field, NumberInput } from "./Field";
import { JsonViewer } from "./JsonViewer";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

/// One candidate row from the table a foreign key points at.
export interface LookupRow {
  /// The referenced column's value, as text — what the cell will hold.
  value: string;
  /// The rest of the row, so the choice is made on more than an opaque id.
  detail: string;
}

interface CellEditorProps {
  typeName: string;
  value: Value;
  onCommit: (next: Value) => void;
  onCancel: () => void;
  /// Present on a foreign-key column: searches the referenced table.
  lookup?: ((search: string) => Promise<LookupRow[]>) | undefined;
}

// WHAT:  Inline editor for one grid cell, picked by field kind:
//          text / decimal        → Input (typing `NULL` stages a null)
//          int / float           → NumberInput
//          date / time / datetime → DateTimeField (calendar + segments; the
//                                   calendar/clock popover opens on entry)
//          json                  → JsonEditorModal (tree + raw)
//        Enter commits, Escape cancels. Text and number editors also cancel on
//        blur; the date editors keep focus in their popover, so they carry
//        explicit ✓ / ✕ buttons. The latest draft lives in a ref so an Enter
//        that also changes the value (calendar cell) commits the new value.
// WHERE: src/features/grid/DataGrid.tsx
export function CellEditor({ typeName, value, onCommit, onCancel, lookup }: CellEditorProps) {
  const kind = fieldKind(typeName, value);
  const initialText = editText(value);
  const initialNumber = value.t === "int" || value.t === "float" ? value.v : null;
  // State drives the controls; the refs mirror it so a keydown that both changes
  // the value and commits (Enter on a calendar cell) commits the fresh draft.
  // Refs are written in event handlers only, never read during render.
  const draft = useRef<string>(initialText);
  const number = useRef<number | null>(initialNumber);
  const [text, setText] = useState(initialText);
  const [num, setNum] = useState<number | null>(initialNumber);

  const onKey = (e: KeyboardEvent<HTMLElement>, commit: () => void) => {
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

  // A foreign key is chosen from the rows it can point at, not typed from memory.
  if (lookup) {
    return <LookupPicker initial={initialText} load={lookup} onCommit={(next) => onCommit(parseEdited(next, typeName, value))} onNull={() => onCommit({ t: "null" })} onCancel={onCancel} />;
  }

  switch (kind) {
    case "bool":
    case "bytes":
      return null;
    case "int":
    case "float":
      return (
        <div className="h-[calc(100%-4px)] w-full" onKeyDown={(e) => onKey(e, commitNumber)} onBlur={onCancel}>
          <NumberInput
            compact
            autoFocus
            integer={kind === "int"}
            ariaLabel="Edit number"
            value={num}
            onChange={(n) => {
              number.current = n;
              setNum(n);
            }}
          />
        </div>
      );
    case "date":
    case "time":
    case "datetime":
      return (
        // No confirm buttons: picking a date is the decision. The edit is staged
        // like every other cell edit and reviewed once in Pending Changes, so a
        // second ✓ here would be asking the same question twice. Escape cancels.
        <div className="h-[calc(100%-4px)] w-full min-w-0" onKeyDown={(e) => onKey(e, commitDate)}>
          <DateTimeField
            compact
            autoFocus
            autoOpen
            kind={kind}
            ariaLabel={`Edit ${kind}`}
            value={text}
            onChange={(next) => {
              draft.current = next;
              setText(next);
            }}
            onDone={commitDate}
          />
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
      // A row-height input shows the tail of a user agent string and nothing
      // else, so anything long or multi-line is edited in a panel that shows all
      // of it. The threshold is the point where a grid cell stops being a
      // sensible place to read a value.
      if (kind === "text" && (text.length > LONG_TEXT || text.includes("\n"))) {
        return (
          <TextAreaEditor
            title={`Edit ${typeName}`}
            initial={text}
            onCommit={(next) => onCommit(parseEdited(next, typeName, value))}
            onNull={() => onCommit({ t: "null" })}
            onCancel={onCancel}
          />
        );
      }
      return (
        <Input
          autoFocus
          value={text}
          onChange={(e) => {
            draft.current = e.target.value;
            setText(e.target.value);
          }}
          onKeyDown={(e) => onKey(e, commitText)}
          onBlur={onCancel}
          inputMode={kind === "decimal" ? "decimal" : undefined}
          className="h-[calc(100%-4px)] w-full rounded-sm border border-accent bg-background px-1 font-mono text-[12px] text-foreground"
          aria-label="Edit cell"
        />
      );
  }
}

// WHAT:  Picks a foreign-key value from the referenced table, with each row's
//        own columns shown next to the id.
// WHY:   Editing an FK meant opening the other table, copying an id and coming
//        back; the id alone says nothing about which row it is.
// HOW:   The search runs against the referenced table on every keystroke the
//        caller allows, so it works on any engine that can filter a page.
function LookupPicker({ initial, load, onCommit, onNull, onCancel }: { initial: string; load: (search: string) => Promise<LookupRow[]>; onCommit: (next: string) => void; onNull: () => void; onCancel: () => void }) {
  // The field is both the search and the value: pick a row, or type a value the
  // list does not offer (a row created elsewhere, an id not yet inserted) and
  // press Enter. A foreign key the database has not seen yet is the database's
  // to reject, not this dialog's.
  const [search, setSearch] = useState(initial);
  // One state for the whole request, so a keystroke cannot leave "loading" and
  // "failed" describing different searches.
  const [result, setResult] = useState<{ rows: LookupRow[]; failed: boolean } | null>(null);

  useEffect(() => {
    let cancelled = false;
    void load(search)
      .then((rows) => {
        if (!cancelled) setResult({ rows, failed: false });
      })
      .catch(() => {
        if (!cancelled) setResult({ rows: [], failed: true });
      });
    return () => {
      cancelled = true;
    };
  }, [search, load]);

  const rows = result?.rows ?? null;
  const failed = result?.failed ?? false;

  return (
    <Modal isOpen onOpenChange={(open) => !open && onCancel()}>
      <Modal.Backdrop>
        <Modal.Container>
          <Modal.Dialog className="sm:max-w-[640px]">
            <Modal.CloseTrigger />
            <Modal.Header>
              <Modal.Heading>Choose a referenced row</Modal.Heading>
            </Modal.Header>
            <Modal.Body>
              <SearchField
                value={search}
                onChange={setSearch}
                aria-label="Search or type a referenced value"
                autoFocus
                onKeyDown={(e) => {
                  if (e.key === "Enter" && search.trim().length > 0) {
                    e.preventDefault();
                    onCommit(search.trim());
                  }
                }}
              >
                <SearchField.Group className="glass-input h-8 rounded-lg px-2">
                  <SearchField.SearchIcon />
                  <SearchField.Input placeholder="Search or type a value…" className="w-full font-mono text-xs" />
                  <SearchField.ClearButton />
                </SearchField.Group>
              </SearchField>
              <ScrollShadow hideScrollBar className="mt-2 max-h-72">
                {rows === null ? (
                  <p className="px-1 py-2 text-xs text-muted">Loading…</p>
                ) : rows.length === 0 ? (
                  <p className="px-1 py-2 text-xs text-muted">
                    {failed ? "Could not read the referenced table — the typed value is still usable." : "No matching rows. Press Enter to use what you typed."}
                  </p>
                ) : (
                  <ul className="flex flex-col gap-0.5">
                    {rows.map((row) => (
                      <li key={row.value}>
                        <Button
                          variant="ghost"
                          size="sm"
                          onPress={() => onCommit(row.value)}
                          className={cn(
                            "flex h-auto w-full min-w-0 flex-col items-start gap-0.5 rounded-md px-2 py-1.5 text-left",
                            row.value === initial ? "glass-pill text-accent" : "text-foreground hover:bg-surface-secondary/60",
                          )}
                        >
                          <span className="truncate font-mono text-[12px]">{row.value}</span>
                          {row.detail.length > 0 ? <span className="truncate text-[11px] text-muted">{row.detail}</span> : null}
                        </Button>
                      </li>
                    ))}
                  </ul>
                )}
              </ScrollShadow>
            </Modal.Body>
            <Modal.Footer>
              <Button size="sm" variant="tertiary" onPress={onNull}>Set NULL</Button>
              <Button size="sm" variant="tertiary" onPress={onCancel}>Cancel</Button>
              <Button size="sm" isDisabled={search.trim().length === 0} onPress={() => onCommit(search.trim())}>
                Use typed value
              </Button>
            </Modal.Footer>
          </Modal.Dialog>
        </Modal.Container>
      </Modal.Backdrop>
    </Modal>
  );
}

/// Length past which a value is edited in a panel rather than in the cell.
const LONG_TEXT = 80;

// WHAT:  Full-value editor for long or multi-line text: the whole value, wrapped,
//        resizable, with the length shown.
// WHY:   Tokens, user agents, URLs and JSON strings are common cell values and
//        unreadable in a 28px row.
// WHERE: CellEditor (text branch), src/features/grid/DataGrid.tsx
function TextAreaEditor({ title, initial, onCommit, onNull, onCancel }: { title: string; initial: string; onCommit: (next: string) => void; onNull: () => void; onCancel: () => void }) {
  const [text, setText] = useState(initial);
  return (
    <Modal isOpen onOpenChange={(open) => !open && onCancel()}>
      <Modal.Backdrop>
        <Modal.Container>
          <Modal.Dialog className="sm:max-w-[720px]">
            <Modal.CloseTrigger />
            <Modal.Header>
              <Modal.Heading>{title}</Modal.Heading>
            </Modal.Header>
            <Modal.Body>
              <TextArea
                autoFocus
                value={text}
                onChange={(e) => setText(e.target.value)}
                onKeyDown={(e) => {
                  // Enter is a newline here; Cmd/Ctrl+Enter commits.
                  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
                    e.preventDefault();
                    onCommit(text);
                  } else if (e.key === "Escape") {
                    e.preventDefault();
                    onCancel();
                  }
                }}
                className="selectable h-72 w-full resize-y rounded-lg border border-border/40 bg-background p-2 font-mono text-[12px] whitespace-pre-wrap text-foreground"
                aria-label={title}
              />
              <p className="mt-1 text-[11px] text-muted">{text.length.toLocaleString()} characters · \u2318\u21b5 to save</p>
            </Modal.Body>
            <Modal.Footer>
              <Button size="sm" variant="tertiary" onPress={onNull}>Set NULL</Button>
              <Button size="sm" variant="tertiary" onPress={onCancel}>Cancel</Button>
              <Button size="sm" onPress={() => onCommit(text)}>Save</Button>
            </Modal.Footer>
          </Modal.Dialog>
        </Modal.Container>
      </Modal.Backdrop>
    </Modal>
  );
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
            {open ? <JsonEditorBody initial={initial} onSave={onSave} onClose={onClose} {...(secondaryAction ? { secondaryAction } : {})} /> : null}
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
            <ScrollShadow className="min-h-0 flex-1 overflow-x-auto rounded-lg border border-border/40 bg-background/60 p-2">
              {parsed !== undefined ? <JsonViewer value={parsed} defaultDepth={3} /> : <span className="text-xs text-muted">Fix the JSON on the left to preview it here.</span>}
            </ScrollShadow>
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
