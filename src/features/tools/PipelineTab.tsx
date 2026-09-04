// SOT: pipeline-tab, aggregation-pipeline-builder, pipeline-stages
import { useState } from "react";
import { Button, Chip, ScrollShadow, Spinner, TextArea } from "@heroui/react";
import type { ResultSet } from "@/lib/bindings";
import type { JsonValue } from "@/lib/bindings/serde_json/JsonValue";
import { ipc, normalizeError } from "@/lib/ipc";
import { parseJson } from "@/lib/json";
import { DENSITIES, formatCount } from "@/lib/format";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { AppSelect, NumberInput } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { DataGrid } from "@/features/grid/DataGrid";
import { isRows } from "@/features/dashboards/charts";
import { ToolShell, useCollectionOptions } from "./ToolShell";
import { cn } from "@/lib/cn";

interface Stage {
  id: number;
  op: string;
  body: string;
  enabled: boolean;
}

const OPERATORS: readonly { value: string; label: string; template: string }[] = [
  { value: "$match", label: "$match", template: '{ "status": "active" }' },
  { value: "$project", label: "$project", template: '{ "_id": 0, "name": 1 }' },
  { value: "$group", label: "$group", template: '{ "_id": "$field", "count": { "$sum": 1 } }' },
  { value: "$sort", label: "$sort", template: '{ "count": -1 }' },
  { value: "$limit", label: "$limit", template: "20" },
  { value: "$skip", label: "$skip", template: "0" },
  { value: "$lookup", label: "$lookup", template: '{ "from": "other", "localField": "id", "foreignField": "ref", "as": "joined" }' },
  { value: "$unwind", label: "$unwind", template: '"$items"' },
  { value: "$facet", label: "$facet", template: '{ "byStatus": [ { "$group": { "_id": "$status", "n": { "$sum": 1 } } } ] }' },
  { value: "$set", label: "$set", template: '{ "total": { "$multiply": [ "$price", "$qty" ] } }' },
  { value: "$unset", label: "$unset", template: '"internal"' },
  { value: "$replaceWith", label: "$replaceWith", template: '"$nested"' },
  { value: "$count", label: "$count", template: '"total"' },
  { value: "$sample", label: "$sample", template: '{ "size": 10 }' },
  { value: "$addFields", label: "$addFields", template: '{ "year": { "$year": "$createdAt" } }' },
];

let stageCounter = 0;
const newStage = (op = "$match"): Stage => {
  stageCounter += 1;
  return { id: stageCounter, op, body: OPERATORS.find((o) => o.value === op)?.template ?? "{}", enabled: true };
};

// WHAT:  Builds `{ "aggregate": coll, "pipeline": [ {op: body}, … ], "cursor": {…} }`
//        from the stage list; a stage whose body is not JSON aborts with its index.
export function buildPipeline(stages: readonly Stage[]): { pipeline: JsonValue[] } | { error: string } {
  const pipeline: JsonValue[] = [];
  for (const [i, stage] of stages.entries()) {
    if (!stage.enabled) continue;
    const body = parseJson(stage.body);
    if (body === undefined) return { error: `Stage ${i + 1} (${stage.op}) is not valid JSON.` };
    pipeline.push({ [stage.op]: body });
  }
  return { pipeline };
}

// WHAT:  Aggregation pipeline builder: ordered stages with an operator and a
//        JSON body, toggled on/off, reordered, then run against a collection
//        through the adapter's raw command surface.
// WHERE: src-tauri/src/integrations/mongodb.rs (execute: run_command), src/features/tools/ToolTab.tsx
export function PipelineTab({ connectionId }: { connectionId: string }) {
  const density = useWorkspace((s) => s.density);
  const openQuery = useWorkspace((s) => s.openQuery);
  const options = useCollectionOptions(connectionId);
  const [collection, setCollection] = useState(options[0]?.value ?? "");
  const [stages, setStages] = useState<Stage[]>(() => [newStage("$match"), newStage("$limit")]);
  const [batch, setBatch] = useState<number | null>(100);
  const [result, setResult] = useState<ResultSet | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const current = collection.length > 0 ? collection : (options[0]?.value ?? "");

  const update = (id: number, patch: Partial<Stage>) => setStages((s) => s.map((st) => (st.id === id ? { ...st, ...patch } : st)));
  const move = (index: number, delta: number) =>
    setStages((s) => {
      const next = [...s];
      const target = index + delta;
      const item = next[index];
      const other = next[target];
      if (item === undefined || other === undefined) return s;
      next[index] = other;
      next[target] = item;
      return next;
    });

  const commandText = (): string | null => {
    const built = buildPipeline(stages);
    if ("error" in built) {
      setError(built.error);
      return null;
    }
    return JSON.stringify({ aggregate: current, pipeline: built.pipeline, cursor: { batchSize: batch ?? 100 } }, null, 2);
  };

  const run = async () => {
    const command = commandText();
    if (command === null) return;
    setRunning(true);
    setError(null);
    try {
      const outcome = await ipc("execute_query", { connectionId, sql: command, confirmDestructive: false, maxRows: batch ?? 100 });
      const rows = outcome.statements.find(isRows);
      setResult(rows ? rows.result : null);
      setElapsed(outcome.elapsedMs);
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  return (
    <ToolShell
      tool="pipeline_builder"
      right={
        <>
          {result ? (
            <span className="font-mono text-[10px] text-muted">
              {formatCount(result.rows.length)} docs · {elapsed} ms
            </span>
          ) : null}
          <IconButton
            icon="terminal"
            label="Open as command"
            onPress={() => {
              const command = commandText();
              if (command !== null) openQuery(connectionId, command, `${current} pipeline`);
            }}
          />
        </>
      }
    >
      <div className="flex h-full min-h-0">
        <aside className="flex w-[380px] shrink-0 flex-col border-r border-border/40">
          <div className="flex items-end gap-2 p-3">
            <div className="min-w-0 flex-1">
              <AppSelect label="Collection" value={current} options={options} onChange={setCollection} />
            </div>
            <div className="w-24">
              <NumberInput label="Batch" integer value={batch} onChange={setBatch} />
            </div>
          </div>
          <ScrollShadow className="min-h-0 flex-1 px-3">
            <ol className="flex flex-col gap-2 pb-2">
              {stages.map((stage, i) => (
                <li key={stage.id} className={cn("rounded-xl glass-card border-border/40 p-2", stage.enabled ? "" : "opacity-50")}>
                  <div className="mb-1.5 flex items-center gap-1">
                    <Chip size="sm" variant="soft" className="h-5 min-w-5 px-1 font-mono text-[10px]">
                      {i + 1}
                    </Chip>
                    <div className="w-36">
                      <AppSelect ariaLabel="Stage operator" size="sm" value={stage.op} options={OPERATORS.map((o) => ({ value: o.value, label: o.label }))} onChange={(op) => update(stage.id, { op, body: stage.body.trim().length === 0 ? (OPERATORS.find((o) => o.value === op)?.template ?? "{}") : stage.body })} />
                    </div>
                    <span className="ml-auto flex items-center">
                      <IconButton icon="arrow-up" label="Move up" isDisabled={i === 0} onPress={() => move(i, -1)} size={12} className="size-6 min-w-6" />
                      <IconButton icon="arrow-down" label="Move down" isDisabled={i === stages.length - 1} onPress={() => move(i, 1)} size={12} className="size-6 min-w-6" />
                      <IconButton icon={stage.enabled ? "eye" : "eye-off"} label={stage.enabled ? "Disable stage" : "Enable stage"} onPress={() => update(stage.id, { enabled: !stage.enabled })} size={12} className="size-6 min-w-6" />
                      <IconButton icon="trash" label="Remove stage" onPress={() => setStages((s) => s.filter((st) => st.id !== stage.id))} size={12} className="size-6 min-w-6" />
                    </span>
                  </div>
                  <TextArea aria-label={`${stage.op} body`} value={stage.body} onChange={(e) => update(stage.id, { body: e.target.value })} spellCheck={false} className={cn("min-h-16 font-mono text-[12px]", parseJson(stage.body) === undefined ? "border-danger" : "")} />
                </li>
              ))}
            </ol>
          </ScrollShadow>
          <div className="flex items-center gap-2 border-t border-border/40 p-3">
            <div className="w-40">
              <AppSelect ariaLabel="Add stage" size="sm" value="$match" options={OPERATORS.map((o) => ({ value: o.value, label: `+ ${o.label}` }))} onChange={(op) => setStages((s) => [...s, newStage(op)])} />
            </div>
            <Button className="ml-auto" onPress={() => void run()} isDisabled={running || current.length === 0}>
              {running ? <Spinner size="sm" /> : <Icon name="play" size={13} />}
              Run
            </Button>
          </div>
          {error !== null ? <p className="px-3 pb-2 text-xs text-danger">{error}</p> : null}
        </aside>
        <div className="min-h-0 min-w-0 flex-1">
          {result === null ? (
            <EmptyState icon="flow" title="Pipeline builder" body="Add stages, edit their JSON bodies and run. Disabled stages are skipped, so you can preview the pipeline step by step." />
          ) : result.columns.length === 0 || result.rows.length === 0 ? (
            <EmptyState title="No documents" body="The pipeline produced nothing." />
          ) : (
            <DataGrid columns={result.columns.map((c) => ({ name: c.name, typeName: c.typeName }))} rowCount={result.rows.length} getRow={(i) => result.rows[i]} rowHeight={DENSITIES[density].rowHeight} />
          )}
        </div>
      </div>
    </ToolShell>
  );
}
