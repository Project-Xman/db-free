// SOT: message-viewer-tab, topic-browser, consume-form, produce-form
import { useState } from "react";
import { Button, Spinner, TextArea } from "@heroui/react";
import type { QueryOutcome, ResultSet } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { parseJson } from "@/lib/json";
import { DENSITIES, formatCount } from "@/lib/format";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Field, NumberInput } from "@/components/global/Field";
import { Segmented } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { DataGrid } from "@/features/grid/DataGrid";
import { ToolBody, ToolShell, useCollectionOptions } from "./ToolShell";

type Mode = "consume" | "produce";
type Start = "earliest" | "latest" | "offset";

function firstRows(outcome: QueryOutcome): ResultSet | null {
  for (const s of outcome.statements) if (s.kind === "rows") return s.result;
  return null;
}

// WHAT:  Topic message browser for Kafka-protocol engines: consume from a
//        partition at earliest / latest / an offset, and produce a message
//        with key, value and headers. Both go through the adapter's JSON
//        command language via execute_query, so the read-only lock and the
//        history log apply.
// WHERE: src-tauri/src/integrations/kafka.rs (parse_command), src/features/tools/ToolTab.tsx
export function MessageViewerTab({ connectionId }: { connectionId: string }) {
  const density = useWorkspace((s) => s.density);
  const showInfo = useWorkspace((s) => s.showInfo);
  const options = useCollectionOptions(connectionId);
  const [mode, setMode] = useState<Mode>("consume");
  const [topic, setTopic] = useState(options[0]?.value ?? "");
  const [partition, setPartition] = useState<number | null>(null);
  const [start, setStart] = useState<Start>("latest");
  const [offset, setOffset] = useState<number | null>(0);
  const [limit, setLimit] = useState<number | null>(100);
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [headers, setHeaders] = useState("");
  const [result, setResult] = useState<ResultSet | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const current = topic.length > 0 ? topic : (options[0]?.value ?? "");

  const consume = async () => {
    setRunning(true);
    setError(null);
    try {
      const command = { topic: current, ...(partition !== null ? { partition } : {}), offset: start === "offset" ? (offset ?? 0) : start, limit: limit ?? 100 };
      const outcome = await ipc("execute_query", { connectionId, sql: JSON.stringify(command), confirmDestructive: false, maxRows: limit ?? 100 });
      setResult(firstRows(outcome));
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  const produce = async () => {
    let headerObject: Record<string, string> = {};
    if (headers.trim().length > 0) {
      const parsed = parseJson(headers);
      if (parsed === undefined || parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        setError("Headers must be a JSON object of string values.");
        return;
      }
      headerObject = Object.fromEntries(Object.entries(parsed).map(([k, v]) => [k, typeof v === "string" ? v : JSON.stringify(v)]));
    }
    setRunning(true);
    setError(null);
    try {
      const command = { produce: { topic: current, value, ...(key.trim().length > 0 ? { key: key.trim() } : {}), ...(partition !== null ? { partition } : {}), ...(Object.keys(headerObject).length > 0 ? { headers: headerObject } : {}) } };
      await ipc("execute_query", { connectionId, sql: JSON.stringify(command), confirmDestructive: false, maxRows: 1 });
      showInfo(`Produced to ${current}.`);
      setValue("");
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  return (
    <ToolShell tool="message_viewer" right={result ? <span className="font-mono text-[10px] text-muted">{formatCount(result.rows.length)} messages</span> : null}>
      <ToolBody
        form={
          <>
            <Segmented label="Mode" value={mode} onChange={setMode} options={[{ value: "consume", label: "Consume" }, { value: "produce", label: "Produce" }]} />
            <AppSelect label="Topic" value={current} options={options} onChange={setTopic} />
            <NumberInput label="Partition" integer value={partition} onChange={setPartition} />
            {mode === "consume" ? (
              <>
                <div className="flex flex-col gap-1">
                  <span className="text-sm font-medium text-foreground">Start</span>
                  <Segmented label="Start position" value={start} onChange={setStart} options={[{ value: "earliest", label: "Earliest" }, { value: "latest", label: "Latest" }, { value: "offset", label: "Offset" }]} />
                </div>
                {start === "offset" ? <NumberInput label="Offset" integer value={offset} onChange={setOffset} /> : null}
                <NumberInput label="Max messages" integer value={limit} onChange={setLimit} />
                <Button onPress={() => void consume()} isDisabled={running || current.length === 0}>
                  {running ? <Spinner size="sm" /> : <Icon name="download" size={13} />}
                  Consume
                </Button>
              </>
            ) : (
              <>
                <Field label="Key" value={key} onChange={setKey} optional mono />
                <div className="flex flex-col gap-1">
                  <span className="text-sm font-medium text-foreground">Value</span>
                  <TextArea aria-label="Message value" value={value} onChange={(e) => setValue(e.target.value)} spellCheck={false} className="min-h-28 font-mono text-[12px]" placeholder='{"event": "signup", "user": 42}' />
                </div>
                <div className="flex flex-col gap-1">
                  <span className="text-sm font-medium text-foreground">Headers</span>
                  <TextArea aria-label="Headers" value={headers} onChange={(e) => setHeaders(e.target.value)} spellCheck={false} className="min-h-16 font-mono text-[12px]" placeholder='{"source": "db-free"}' />
                </div>
                <Button onPress={() => void produce()} isDisabled={running || current.length === 0 || value.length === 0}>
                  {running ? <Spinner size="sm" /> : <Icon name="send" size={13} />}
                  Produce
                </Button>
              </>
            )}
            {error !== null ? <p className="text-xs text-danger">{error}</p> : null}
          </>
        }
      >
        {result === null ? (
          <EmptyState icon="message" title="Message viewer" body="Pick a topic and consume from the earliest offset, the latest, or a specific one. Switch to Produce to publish a message." />
        ) : result.columns.length === 0 || result.rows.length === 0 ? (
          <EmptyState title="No messages" body="Nothing was read in this window. Try the earliest offset." />
        ) : (
          <DataGrid columns={result.columns.map((c) => ({ name: c.name, typeName: c.typeName }))} rowCount={result.rows.length} getRow={(i) => result.rows[i]} rowHeight={DENSITIES[density].rowHeight} />
        )}
      </ToolBody>
    </ToolShell>
  );
}
