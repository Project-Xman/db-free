// SOT: field-kind-registry, column-type-classification, edited-text-parsing
import type { Value } from "./bindings";
import { parseJson } from "./json";
import { temporalKind } from "./datetime";

export type FieldKind = "bool" | "int" | "float" | "decimal" | "json" | "date" | "time" | "datetime" | "bytes" | "text";

// WHAT:  Picks the editor control for a column: declared SQL type first, then
//        the shape of the value the adapter returned.
// WHY:   One classification feeds the grid editor, the insert form and text
//        parsing, so a `timestamp` column gets the same picker everywhere.
// WHERE: src/components/global/ValueEditor.tsx, src/features/grid/DataGrid.tsx
export function fieldKind(typeName: string, sample?: Value): FieldKind {
  const t = typeName.toLowerCase();
  if (t.includes("bool")) return "bool";
  if (t.includes("json")) return "json";
  if (/bytea|blob|binary/.test(t)) return "bytes";
  const temporal = temporalKind(t, sample?.t === "date_time" ? sample.v : undefined);
  if (temporal) return temporal;
  if (!t.includes("interval") && /int|serial/.test(t)) return "int";
  if (/numeric|decimal|money/.test(t)) return "decimal";
  if (/real|float|double/.test(t)) return "float";
  switch (sample?.t) {
    case "bool":
      return "bool";
    case "int":
      return "int";
    case "float":
      return "float";
    case "decimal":
      return "decimal";
    case "json":
      return "json";
    case "bytes":
      return "bytes";
    case "date_time":
      return "datetime";
    case "null":
    case "text":
    case "unsupported":
    case undefined:
      return "text";
  }
}

// WHAT:  Turns typed text back into a Value using the column kind as a hint.
//        `NULL` (exact) is the null sentinel; anything unparseable stays text
//        so the database reports the real error.
export function parseEdited(text: string, typeName: string, original: Value | undefined): Value {
  if (text === "NULL") return { t: "null" };
  const trimmed = text.trim();
  switch (fieldKind(typeName, original)) {
    case "bool": {
      const lower = trimmed.toLowerCase();
      if (lower === "true" || lower === "1" || lower === "t") return { t: "bool", v: true };
      if (lower === "false" || lower === "0" || lower === "f") return { t: "bool", v: false };
      break;
    }
    case "int": {
      const n = Number(trimmed);
      if (trimmed.length > 0 && Number.isInteger(n)) return { t: "int", v: n };
      break;
    }
    case "decimal":
      if (trimmed.length > 0 && !Number.isNaN(Number(trimmed))) return { t: "decimal", v: trimmed };
      break;
    case "float": {
      const n = Number(trimmed);
      if (trimmed.length > 0 && !Number.isNaN(n)) return { t: "float", v: n };
      break;
    }
    case "json": {
      const parsed = parseJson(text);
      if (parsed !== undefined) return { t: "json", v: parsed };
      break;
    }
    case "date":
    case "time":
    case "datetime":
      return { t: "date_time", v: trimmed };
    case "bytes":
    case "text":
      break;
  }
  return { t: "text", v: text };
}

// WHAT:  Display text for an editable Value (what an inline text editor opens with).
export function editText(value: Value): string {
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
