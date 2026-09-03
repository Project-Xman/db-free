// SOT: results-pane, statement-tabs, query-result-grid
import { useState } from "react";
import { Chip } from "@heroui/react";
import type { QueryOutcome } from "@/lib/bindings";
import { DENSITIES, formatCount, formatMs } from "@/lib/format";
import { useWorkspace } from "@/stores/workspace";
import { DataGrid } from "@/features/grid/DataGrid";
import { EmptyState } from "@/components/global/EmptyState";
import { cn } from "@/lib/cn";

export function ResultsPane({ outcome }: { outcome: QueryOutcome | null }) {
  const density = useWorkspace((s) => s.density);
  const [active, setActive] = useState(0);

  if (!outcome) {
    return <EmptyState icon="terminal" title="No results yet" body="Run a query with ⌘/Ctrl + Enter. Results appear here." />;
  }
  const statements = outcome.statements;
  const index = Math.min(active, Math.max(0, statements.length - 1));
  const current = statements[index];

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-9 shrink-0 items-center gap-1 border-b border-border bg-surface px-2 text-xs">
        {statements.map((s, i) => (
          <button
            key={i}
            type="button"
            onClick={() => setActive(i)}
            className={cn("rounded-md px-2 py-0.5", i === index ? "bg-surface-tertiary text-foreground" : "text-muted hover:text-foreground")}
          >
            {s.kind === "rows" ? `Result ${i + 1} · ${formatCount(s.result.rows.length)} rows` : `Statement ${i + 1}`}
          </button>
        ))}
        <span className="ml-auto flex items-center gap-2">
          <Chip size="sm" color="success" variant="soft">
            {formatMs(outcome.elapsedMs)}
          </Chip>
          {current?.kind === "rows" && current.result.truncated ? (
            <Chip size="sm" color="warning" variant="soft">
              truncated at row cap
            </Chip>
          ) : null}
        </span>
      </div>
      <div className="min-h-0 flex-1">
        {current === undefined ? (
          <EmptyState title="Statement executed" body="No result set was returned." />
        ) : current.kind === "affected" ? (
          <EmptyState icon="check" title="Statement OK" body={`${formatCount(current.rowsAffected)} row(s) affected.`} />
        ) : current.result.columns.length === 0 ? (
          <EmptyState title="Empty result" body="The statement returned no rows." />
        ) : (
          <DataGrid
            columns={current.result.columns.map((c) => ({ name: c.name, typeName: c.typeName }))}
            rowCount={current.result.rows.length}
            getRow={(i) => current.result.rows[i]}
            rowHeight={DENSITIES[density].rowHeight}
          />
        )}
      </div>
    </div>
  );
}
