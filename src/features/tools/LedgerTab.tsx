// SOT: ledger-tab, immutable-history-playground, ledger-history-view
import { useState } from "react";
import { Button, Spinner } from "@heroui/react";
import type { ObjectKind, ResultSet } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { DENSITIES, formatCount } from "@/lib/format";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Field } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { DataGrid } from "@/features/grid/DataGrid";
import { ToolBody, ToolShell, useCollectionOptions } from "./ToolShell";

type Subject = "table" | "document";
const SUBJECTS: readonly { value: Subject; label: string }[] = [
  { value: "table", label: "Table row history" },
  { value: "document", label: "Key history" },
];

// WHAT:  Ledger history: every version of a key (or the rows of a table)
//        with transaction ids, timestamps and the verification the engine
//        offers. `document` stands for "the value at a key".
// WHERE: src-tauri/src/integrations/mod.rs (history), src/features/tools/ToolTab.tsx
export function LedgerTab({ connectionId }: { connectionId: string }) {
  const density = useWorkspace((s) => s.density);
  const options = useCollectionOptions(connectionId);
  const [subject, setSubject] = useState<Subject>("document");
  const [table, setTable] = useState(options[0]?.value ?? "");
  const [key, setKey] = useState("");
  const [result, setResult] = useState<ResultSet | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const currentTable = table.length > 0 ? table : (options[0]?.value ?? "");

  const run = async () => {
    const kind: ObjectKind = subject;
    const name = subject === "table" ? currentTable : key.trim();
    if (name.length === 0) {
      setError(subject === "table" ? "Pick a table." : "Enter a key.");
      return;
    }
    setRunning(true);
    setError(null);
    try {
      const rows = await ipc("load_history", { connectionId, reference: { kind, name, parent: subject === "document" && currentTable.length > 0 ? currentTable : null } });
      setResult(rows);
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  return (
    <ToolShell tool="ledger_history" right={result ? <span className="font-mono text-[10px] text-muted">{formatCount(result.rows.length)} revisions</span> : null}>
      <ToolBody
        form={
          <>
            <AppSelect label="Subject" value={subject} options={SUBJECTS} onChange={setSubject} />
            {options.length > 0 ? <AppSelect label={subject === "table" ? "Table" : "Table (optional)"} value={currentTable} options={options} onChange={setTable} /> : null}
            {subject === "document" ? <Field label="Key" value={key} onChange={setKey} placeholder="customer:42" mono /> : null}
            <Button onPress={() => void run()} isDisabled={running}>
              {running ? <Spinner size="sm" /> : <Icon name="history" size={13} />}
              Load history
            </Button>
            {error !== null ? <p className="text-xs text-danger">{error}</p> : null}
          </>
        }
      >
        {result === null ? (
          <EmptyState icon="git-branch" title="Ledger history" body="Every write to a key stays: load its revisions with transaction ids and verify them against the ledger state." />
        ) : result.columns.length === 0 || result.rows.length === 0 ? (
          <EmptyState title="No history" body="No revisions were found for this subject." />
        ) : (
          <DataGrid columns={result.columns.map((c) => ({ name: c.name, typeName: c.typeName }))} rowCount={result.rows.length} getRow={(i) => result.rows[i]} rowHeight={DENSITIES[density].rowHeight} />
        )}
      </ToolBody>
    </ToolShell>
  );
}
