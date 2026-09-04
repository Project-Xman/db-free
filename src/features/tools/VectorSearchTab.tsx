// SOT: vector-search-tab, similarity-search-playground, query-vector-parsing
import { useState } from "react";
import { Button, Spinner, TextArea } from "@heroui/react";
import type { ResultSet } from "@/lib/bindings";
import type { JsonValue } from "@/lib/bindings/serde_json/JsonValue";
import { ipc, normalizeError } from "@/lib/ipc";
import { parseJson } from "@/lib/json";
import { DENSITIES, formatCount } from "@/lib/format";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Check, Field, NumberInput } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { DataGrid } from "@/features/grid/DataGrid";
import { ToolBody, ToolShell, useCollectionOptions } from "./ToolShell";

// WHAT:  `[0.1, 0.2, …]`, `0.1, 0.2` or whitespace-separated numbers → vector.
export function parseVector(text: string): number[] | null {
  const trimmed = text.trim();
  if (trimmed.length === 0) return null;
  const json = parseJson(trimmed);
  const values = Array.isArray(json) ? json : trimmed.split(/[\s,;]+/).map((s) => Number(s));
  const numbers: number[] = [];
  for (const v of values) {
    if (typeof v !== "number" || Number.isNaN(v)) return null;
    numbers.push(v);
  }
  return numbers.length > 0 ? numbers : null;
}

// WHAT:  Vector search playground: collection, query vector, top-k, native
//        payload filter → scored hits in the grid.
// WHERE: src-tauri/src/integrations/mod.rs (vector_search), src/features/tools/ToolTab.tsx
export function VectorSearchTab({ connectionId }: { connectionId: string }) {
  const density = useWorkspace((s) => s.density);
  const options = useCollectionOptions(connectionId);
  const [collection, setCollection] = useState(options[0]?.value ?? "");
  const [vectorText, setVectorText] = useState("");
  const [vectorName, setVectorName] = useState("");
  const [topK, setTopK] = useState<number | null>(10);
  const [filterText, setFilterText] = useState("");
  const [includeVectors, setIncludeVectors] = useState(false);
  const [result, setResult] = useState<ResultSet | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const current = collection.length > 0 ? collection : (options[0]?.value ?? "");

  const run = async () => {
    const vector = parseVector(vectorText);
    if (vector === null) {
      setError("Enter the query vector as a JSON array or comma-separated numbers.");
      return;
    }
    let filter: JsonValue | null = null;
    if (filterText.trim().length > 0) {
      const parsed = parseJson(filterText);
      if (parsed === undefined) {
        setError("The filter is not valid JSON.");
        return;
      }
      filter = parsed;
    }
    setRunning(true);
    setError(null);
    const started = performance.now();
    try {
      const hits = await ipc("vector_search", { connectionId, request: { collection: current, vector, vectorName: vectorName.trim().length > 0 ? vectorName.trim() : null, topK: topK ?? 10, filter, includeVectors } });
      setResult(hits);
      setElapsed(Math.round(performance.now() - started));
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  return (
    <ToolShell tool="vector_search" right={result ? <span className="font-mono text-[10px] text-muted">{formatCount(result.rows.length)} hits · {elapsed} ms</span> : null}>
      <ToolBody
        form={
          <>
            <AppSelect label="Collection" value={current} options={options} onChange={setCollection} />
            <div className="flex flex-col gap-1">
              <span className="text-sm font-medium text-foreground">Query vector</span>
              <TextArea aria-label="Query vector" value={vectorText} onChange={(e) => setVectorText(e.target.value)} placeholder="[0.12, -0.4, 0.88, …]" spellCheck={false} className="min-h-28 font-mono text-[12px]" />
              <span className="text-[11px] text-muted">{parseVector(vectorText)?.length ?? 0} dimensions</span>
            </div>
            <Field label="Vector name" value={vectorName} onChange={setVectorName} optional placeholder="named vector / field" mono />
            <NumberInput label="Top K" integer value={topK} onChange={setTopK} />
            <div className="flex flex-col gap-1">
              <span className="text-sm font-medium text-foreground">Payload filter</span>
              <TextArea aria-label="Payload filter" value={filterText} onChange={(e) => setFilterText(e.target.value)} placeholder='{ "must": [ { "key": "city", "match": { "value": "Berlin" } } ] }' spellCheck={false} className="min-h-24 font-mono text-[12px]" />
              <span className="text-[11px] text-muted">Engine-native filter JSON, optional.</span>
            </div>
            <Check label="Include vectors in results" checked={includeVectors} onChange={setIncludeVectors} />
            <Button onPress={() => void run()} isDisabled={running || current.length === 0}>
              {running ? <Spinner size="sm" /> : <Icon name="radar" size={13} />}
              Search
            </Button>
            {error !== null ? <p className="text-xs text-danger">{error}</p> : null}
          </>
        }
      >
        {result === null ? (
          <EmptyState icon="radar" title="Similarity search" body="Pick a collection, paste a query vector and search. Hits come back with their score and payload." />
        ) : result.columns.length === 0 ? (
          <EmptyState title="No hits" />
        ) : (
          <DataGrid columns={result.columns.map((c) => ({ name: c.name, typeName: c.typeName }))} rowCount={result.rows.length} getRow={(i) => result.rows[i]} rowHeight={DENSITIES[density].rowHeight} />
        )}
      </ToolBody>
    </ToolShell>
  );
}
