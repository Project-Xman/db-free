// SOT: cell-formatting, value-rendering, density-registry
import type { Value } from "./bindings";

export type CellKind = "null" | "bool" | "number" | "text" | "bytes" | "json" | "datetime" | "unsupported";

export interface FormattedCell {
  text: string;
  kind: CellKind;
  align: "left" | "right";
}

// WHAT:  Turns a Value into display text plus a kind for styling.
// WHY:   One exhaustive switch over the Rust enum; a new variant fails `tsc`.
// WHERE: src-tauri/src/model/value.rs
export function formatCell(value: Value): FormattedCell {
  switch (value.t) {
    case "null":
      return { text: "NULL", kind: "null", align: "left" };
    case "bool":
      return { text: value.v ? "true" : "false", kind: "bool", align: "left" };
    case "int":
      return { text: String(value.v), kind: "number", align: "right" };
    case "float":
      return { text: Number.isFinite(value.v) ? String(value.v) : "NaN", kind: "number", align: "right" };
    case "decimal":
      return { text: value.v, kind: "number", align: "right" };
    case "text":
      return { text: value.v, kind: "text", align: "left" };
    case "bytes":
      return { text: `0x… (${bytesLength(value.v)} B)`, kind: "bytes", align: "left" };
    case "json":
      return { text: JSON.stringify(value.v), kind: "json", align: "left" };
    case "date_time":
      return { text: value.v, kind: "datetime", align: "left" };
    case "unsupported":
      return { text: value.v, kind: "unsupported", align: "left" };
  }
}

function bytesLength(base64: string): number {
  const padding = base64.endsWith("==") ? 2 : base64.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((base64.length * 3) / 4) - padding);
}

export function cellClass(kind: CellKind): string {
  switch (kind) {
    case "null":
      return "text-muted italic";
    case "bool":
      return "text-syntax-keyword";
    case "number":
      return "text-syntax-number tabular-nums";
    case "text":
      return "text-foreground";
    case "bytes":
      return "text-muted";
    case "json":
      return "text-syntax-type";
    case "datetime":
      return "text-syntax-string";
    case "unsupported":
      return "text-muted";
  }
}

export type Density = "compact" | "cozy" | "comfortable";

export const DENSITIES: Record<Density, { label: string; rowHeight: number }> = {
  compact: { label: "Compact", rowHeight: 24 },
  cozy: { label: "Cozy", rowHeight: 28 },
  comfortable: { label: "Comfortable", rowHeight: 34 },
};

export const DENSITY_ORDER: Density[] = ["compact", "cozy", "comfortable"];

export function formatMs(ms: number): string {
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

export function formatCount(n: number): string {
  return new Intl.NumberFormat().format(n);
}
