// SOT: json-boundary, safe-json-parse
import type { JsonValue } from "./bindings/serde_json/JsonValue";

// WHAT:  The one place `JSON.parse` (typed `any`) is narrowed to JsonValue.
// WHY:   Like the IPC boundary in src/lib/ipc.ts, a single audited escape beats
//        scattered escapes.
export function parseJson(text: string): JsonValue | undefined {
  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return -- JSON.parse only ever yields JSON values
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}
