// SOT: capability-matrix, engine-feature-matrix, capabilities-page
import { useMemo, useState } from "react";
import { Button, Chip, SearchField } from "@heroui/react";
import type { Capabilities, Engine, EngineKind } from "@/lib/bindings";
import { CATEGORIES, ENGINE_ORDER, engineMeta } from "@/lib/engines";
import { OBJECT_KINDS, SECTIONS, TOOL_ORDER, kindMeta, profileOf, toolMeta } from "@/lib/objects";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { EngineIcon } from "@/components/global/EngineIcon";
import { AppSelect, Check } from "@/components/global/Field";
import { cn } from "@/lib/cn";
import { keysOf } from "@/lib/records";

interface Row {
  id: string;
  label: string;
  section: string;
  has: (engine: Engine) => boolean;
}

const CAPABILITY_ROWS: readonly { key: keyof Capabilities; label: string }[] = [
  { key: "sql", label: "SQL in the query tab" },
  { key: "namespaces", label: "Schemas / namespaces" },
  { key: "fixedColumns", label: "Fixed columns per collection" },
  { key: "paging", label: "Server-side paging" },
  { key: "rowEstimate", label: "Row count estimate" },
  { key: "exactEstimate", label: "Exact counts" },
  { key: "views", label: "Views" },
  { key: "transactions", label: "Transactions" },
];

// WHAT:  Feature × engine matrix straight from every family's `profile()`:
//        base capabilities, playground tools, then every object kind by
//        section. The implementation roadmap and the "what can this app do
//        with my database" answer, on one page.
// WHERE: src/lib/objects.ts (profileOf), src-tauri/src/integrations/<family>.rs
export function CapabilityMatrixPage() {
  const goConnections = useWorkspace((s) => s.goConnections);
  const [category, setCategory] = useState<EngineKind | "all">("all");
  const [search, setSearch] = useState("");
  const [hideEmpty, setHideEmpty] = useState(true);

  const engines = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return ENGINE_ORDER.filter((e) => (category === "all" || engineMeta(e).kind === category) && (needle.length === 0 || engineMeta(e).label.toLowerCase().includes(needle) || e.includes(needle)));
  }, [category, search]);

  const rows = useMemo<Row[]>(() => {
    const out: Row[] = CAPABILITY_ROWS.map((c) => ({ id: `cap:${c.key}`, label: c.label, section: "Capabilities", has: (e) => profileOf(e).capabilities[c.key] }));
    for (const tool of TOOL_ORDER) {
      out.push({ id: `tool:${tool}`, label: toolMeta(tool).label, section: "Tools", has: (e) => profileOf(e).tools.includes(tool) });
    }
    for (const section of SECTIONS) {
      const kinds = keysOf(OBJECT_KINDS).filter((k) => kindMeta(k).section === section.id);
      for (const kind of kinds) {
        out.push({ id: `kind:${kind}`, label: kindMeta(kind).plural, section: section.label, has: (e) => profileOf(e).objectKinds.includes(kind) });
      }
    }
    return out;
  }, []);

  const visibleRows = hideEmpty ? rows.filter((r) => engines.some((e) => r.has(e))) : rows;
  const totals = useMemo(() => Object.fromEntries(engines.map((e) => [e, rows.filter((r) => r.has(e)).length])), [engines, rows]);
  const categoryOptions: { value: EngineKind | "all"; label: string }[] = [{ value: "all", label: "All categories" }, ...CATEGORIES.map((c) => ({ value: c.kind, label: c.label }))];

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="drag-region flex h-11 shrink-0 items-center gap-2 border-b border-border/40 glass-header px-3" data-tauri-drag-region>
        <Button isIconOnly size="sm" variant="ghost" aria-label="Back to connections" onPress={goConnections} className="size-7 min-w-7 rounded-lg text-muted">
          <Icon name="arrow-left" size={14} />
        </Button>
        <Icon name="grid" size={15} className="text-accent" />
        <span className="text-sm font-semibold tracking-tight text-foreground">Engine capabilities</span>
        <Chip size="sm" variant="soft" className="font-mono text-[10px]">
          {engines.length} engines · {visibleRows.length} features
        </Chip>
        <div className="drag-region h-full min-w-4 flex-1" data-tauri-drag-region />
        <AppSelect ariaLabel="Category" value={category} options={categoryOptions} size="sm" className="w-52" onChange={setCategory} />
        <SearchField value={search} onChange={setSearch} aria-label="Search engines" className="w-48">
          <SearchField.Group className="glass-input h-8 rounded-lg px-2">
            <SearchField.SearchIcon />
            <SearchField.Input placeholder="Engine…" className="w-full text-xs" />
            <SearchField.ClearButton />
          </SearchField.Group>
        </SearchField>
        <Check label="Hide empty rows" checked={hideEmpty} onChange={setHideEmpty} />
      </div>
      <div className="min-h-0 flex-1 overflow-auto">
        <table className="min-w-max border-separate border-spacing-0 text-xs">
          <thead className="sticky top-0 z-20">
            <tr>
              <th className="sticky left-0 z-30 border-b border-r border-border/40 bg-background px-3 py-2 text-left font-medium text-muted">Feature</th>
              {engines.map((engine) => (
                <th key={engine} className="border-b border-border/40 bg-background px-1 py-2 align-bottom">
                  <span className="flex flex-col items-center gap-1 px-1 py-1" title={`${engineMeta(engine).label} · ${engineMeta(engine).hint} · ${totals[engine]} features`}>
                    <EngineIcon engine={engine} size={18} />
                    <span className="w-14 truncate text-[9px] font-medium text-muted">{engineMeta(engine).label}</span>
                    <span className="font-mono text-[9px] text-muted/70">{totals[engine]}</span>
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, i) => {
              const newSection = i === 0 || visibleRows[i - 1]?.section !== row.section;
              return (
                <RowGroup key={row.id} row={row} engines={engines} newSection={newSection} />
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function RowGroup({ row, engines, newSection }: { row: Row; engines: readonly Engine[]; newSection: boolean }) {
  return (
    <>
      {newSection ? (
        <tr>
          <td colSpan={engines.length + 1} className="sticky left-0 border-b border-border/40 bg-surface px-3 pt-3 pb-1 text-[10px] font-semibold uppercase tracking-wider text-muted/80">
            {row.section}
          </td>
        </tr>
      ) : null}
      <tr className="group">
        <td className="sticky left-0 z-10 max-w-[240px] truncate border-b border-r border-separator bg-background px-3 py-1 text-foreground group-hover:bg-surface-secondary/60">{row.label}</td>
        {engines.map((engine) => {
          const has = row.has(engine);
          return (
            <td key={engine} className={cn("border-b border-separator px-1 py-1 text-center group-hover:bg-surface-secondary/40", has ? "text-success" : "text-muted/30")} title={`${engineMeta(engine).label}: ${has ? "yes" : "no"}`}>
              {has ? <Icon name="check" size={12} /> : "·"}
            </td>
          );
        })}
      </tr>
    </>
  );
}

