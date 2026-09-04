// SOT: search-tab, full-text-search-playground, facet-panel
import { useState } from "react";
import { Button, Chip, ScrollShadow, Spinner } from "@heroui/react";
import type { SearchResult } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { DENSITIES, formatCount } from "@/lib/format";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Check, Field, NumberInput } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { DataGrid } from "@/features/grid/DataGrid";
import { ToolBody, ToolShell, useCollectionOptions } from "./ToolShell";

const splitList = (text: string): string[] =>
  text
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

// WHAT:  Full-text search playground: index, query text, native filter,
//        facets, sort, highlighting, paging → hits grid plus facet counts.
// WHERE: src-tauri/src/integrations/mod.rs (search), src/features/tools/ToolTab.tsx
export function SearchTab({ connectionId }: { connectionId: string }) {
  const density = useWorkspace((s) => s.density);
  const options = useCollectionOptions(connectionId);
  const [index, setIndex] = useState(options[0]?.value ?? "");
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("");
  const [facets, setFacets] = useState("");
  const [sort, setSort] = useState("");
  const [highlight, setHighlight] = useState(true);
  const [limit, setLimit] = useState<number | null>(20);
  const [offset, setOffset] = useState(0);
  const [result, setResult] = useState<SearchResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const current = index.length > 0 ? index : (options[0]?.value ?? "");
  const pageSize = limit ?? 20;

  const run = async (nextOffset = 0) => {
    setRunning(true);
    setError(null);
    try {
      const next = await ipc("search_documents", {
        connectionId,
        request: { index: current, query, filter: filter.trim().length > 0 ? filter.trim() : null, facets: splitList(facets), sort: splitList(sort), highlight, limit: pageSize, offset: nextOffset },
      });
      setResult(next);
      setOffset(nextOffset);
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  const total = result?.total ?? null;
  const hasNext = result !== null && (total !== null ? offset + pageSize < total : result.hits.rows.length === pageSize);

  return (
    <ToolShell
      tool="search_playground"
      right={
        result ? (
          <span className="flex items-center gap-2 font-mono text-[10px] text-muted">
            {total !== null ? `${formatCount(total)} total` : `${formatCount(result.hits.rows.length)} hits`}
            {result.tookMs !== null ? ` · ${result.tookMs} ms` : ""}
            <Button size="sm" variant="ghost" isDisabled={offset === 0 || running} onPress={() => void run(Math.max(0, offset - pageSize))} className="h-6 min-w-0 px-1.5">
              <Icon name="chevron-left" size={12} />
            </Button>
            <span>{offset + 1}–{offset + (result.hits.rows.length || 0)}</span>
            <Button size="sm" variant="ghost" isDisabled={!hasNext || running} onPress={() => void run(offset + pageSize)} className="h-6 min-w-0 px-1.5">
              <Icon name="chevron-right" size={12} />
            </Button>
          </span>
        ) : null
      }
    >
      <ToolBody
        form={
          <>
            <AppSelect label="Index" value={current} options={options} onChange={setIndex} />
            <Field label="Query" value={query} onChange={setQuery} placeholder="free text, or * for everything" />
            <Field label="Filter" value={filter} onChange={setFilter} optional placeholder='engine syntax, e.g. price > 10 AND brand = "acme"' mono />
            <Field label="Facets" value={facets} onChange={setFacets} optional placeholder="brand, category" mono description="Comma-separated field names." />
            <Field label="Sort" value={sort} onChange={setSort} optional placeholder="price:asc, name:desc" mono />
            <NumberInput label="Page size" integer value={limit} onChange={setLimit} />
            <Check label="Highlight matches" checked={highlight} onChange={setHighlight} />
            <Button onPress={() => void run(0)} isDisabled={running || current.length === 0}>
              {running ? <Spinner size="sm" /> : <Icon name="search" size={13} />}
              Search
            </Button>
            {error !== null ? <p className="text-xs text-danger">{error}</p> : null}
          </>
        }
      >
        {result === null ? (
          <EmptyState icon="search-list" title="Search playground" body="Pick an index and type a query. Facet counts appear on the right when you ask for facets." />
        ) : (
          <div className="flex h-full min-h-0">
            <div className="min-h-0 min-w-0 flex-1">
              {result.hits.columns.length === 0 || result.hits.rows.length === 0 ? (
                <EmptyState title="No hits" body="Nothing matched. Loosen the query or the filter." />
              ) : (
                <DataGrid columns={result.hits.columns.map((c) => ({ name: c.name, typeName: c.typeName }))} rowCount={result.hits.rows.length} getRow={(i) => result.hits.rows[i]} rowHeight={DENSITIES[density].rowHeight} />
              )}
            </div>
            {result.facets.length > 0 ? (
              <ScrollShadow className="w-60 shrink-0 border-l border-border/40 p-3">
                {result.facets.map((facet) => (
                  <div key={facet.field} className="mb-3">
                    <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">{facet.field}</div>
                    <ul className="flex flex-col gap-0.5">
                      {facet.values.map((v) => (
                        <li key={v.value} className="flex items-center gap-2 text-xs">
                          <span className="truncate text-foreground">{v.value}</span>
                          <Chip size="sm" variant="soft" className="ml-auto h-4 px-1 font-mono text-[9px]">
                            {formatCount(v.count)}
                          </Chip>
                        </li>
                      ))}
                    </ul>
                  </div>
                ))}
              </ScrollShadow>
            ) : null}
          </div>
        )}
      </ToolBody>
    </ToolShell>
  );
}
