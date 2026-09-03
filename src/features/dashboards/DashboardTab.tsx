// SOT: dashboard-tab, widget-grid, widget-editor, widget-options, widget-conditions-editor, dashboard-variables, dashboard-refresh
import { useCallback, useEffect, useMemo, useState } from "react";
import { Button, CloseButton, Input, ScrollShadow, Spinner } from "@heroui/react";
import type { ConditionOp, DashboardBody, Document, QueryOutcome, Widget, WidgetCondition, WidgetKind } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { DENSITIES, formatCount, formatMs } from "@/lib/format";
import { useWorkspace } from "@/stores/workspace";
import { DataGrid } from "@/features/grid/DataGrid";
import { AppSelect, Field, Toggle } from "@/components/global/Field";
import { IconButton } from "@/components/global/Button";
import { Resizer } from "@/components/global/Resizer";
import { EmptyState } from "@/components/global/EmptyState";
import { SqlEditor } from "@/features/editor/SqlEditor";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";
import { BarChart, ImageWidget, LineChart, MapChart, PieChart, ProgressMeter, SERIES_COLORS, SankeyChart, SparklineWidget, StatTile, TextWidget, chartData, conditionMatches, isRows } from "./charts";

const KINDS: readonly { value: WidgetKind; label: string }[] = [
  { value: "area", label: "Area Chart" },
  { value: "line", label: "Line Chart" },
  { value: "bar", label: "Bar Chart" },
  { value: "pie", label: "Pie Chart" },
  { value: "sankey", label: "Sankey Diagram" },
  { value: "table", label: "Table" },
  { value: "metric", label: "Metric" },
  { value: "sparkline", label: "Sparkline" },
  { value: "map", label: "Map" },
  { value: "progress", label: "Progress" },
  { value: "text", label: "Text" },
  { value: "image", label: "Image" },
  { value: "gif", label: "GIF" },
];

// WHAT:  Tint registry: option → literal CSS variable (validated dark palette in globals.css).
const TINTS = [
  { value: "series-1", label: "Blue", css: "var(--color-series-1)" },
  { value: "series-2", label: "Orange", css: "var(--color-series-2)" },
  { value: "series-3", label: "Aqua", css: "var(--color-series-3)" },
  { value: "series-4", label: "Yellow", css: "var(--color-series-4)" },
  { value: "series-5", label: "Magenta", css: "var(--color-series-5)" },
  { value: "series-6", label: "Green", css: "var(--color-series-6)" },
  { value: "series-7", label: "Violet", css: "var(--color-series-7)" },
] satisfies readonly { value: string; label: string; css: string }[];

const CONDITION_OPS: readonly { value: ConditionOp; label: string }[] = [
  { value: "equals", label: "equals" },
  { value: "not_equals", label: "not equals" },
  { value: "gt", label: ">" },
  { value: "gte", label: "≥" },
  { value: "lt", label: "<" },
  { value: "lte", label: "≤" },
  { value: "contains", label: "contains" },
];

function tintCss(tint: string | null): string {
  return TINTS.find((t) => t.value === tint)?.css ?? "var(--color-series-1)";
}

/// Series colours: the widget tint first, then the fixed categorical order.
function seriesColors(tint: string | null): readonly string[] {
  const first = tintCss(tint);
  return [first, ...SERIES_COLORS.filter((c) => c !== first)];
}

const HAS_SQL: Record<WidgetKind, boolean> = { area: true, line: true, bar: true, pie: true, sankey: true, table: true, metric: true, sparkline: true, map: true, progress: true, text: true, image: true, gif: true };
const HAS_CONDITIONS: Record<WidgetKind, boolean> = { area: false, line: false, bar: false, pie: false, sankey: false, table: false, metric: true, sparkline: false, map: false, progress: false, text: true, image: true, gif: true };

let widgetCounter = 0;

// WHAT:  Dashboard = grid of widgets, each backed by one SQL statement on the
//        dashboard's connection. Variables (`{{name}}`) substitute into SQL.
// HOW:   Widget results are fetched through execute_query; the right panel edits
//        the selected widget (title, kind, options, conditions, SQL) with Run Query
//        and a result preview, mirroring DB Manager's editor.
// WHERE: src-tauri/src/model/documents.rs (DashboardBody), ./charts.tsx
export function DashboardTab({ document: doc, connectionId: initialConnectionId }: { document: Document; connectionId: string | null }) {
  const saveDocument = useWorkspace((s) => s.saveDocument);
  const density = useWorkspace((s) => s.density);
  const connections = useWorkspace((s) => s.connections);
  const sessions = useWorkspace((s) => s.sessions);
  const activeConnectionId = useWorkspace((s) => s.activeConnectionId);
  const connect = useWorkspace((s) => s.connect);
  const [chosenConnectionId, setChosenConnectionId] = useState<string | null>(initialConnectionId ?? activeConnectionId);
  const connectionId = chosenConnectionId ?? activeConnectionId;
  const catalog = useWorkspace((s) => (connectionId ? s.catalogs[connectionId] : undefined));
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const initial: DashboardBody = doc.body.kind === "dashboard" ? doc.body.data : { widgets: [], variables: [], refreshSeconds: 0 };
  const [body, setBody] = useState<DashboardBody>(initial);
  const [name, setName] = useState(doc.name);
  const [dirty, setDirty] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [results, setResults] = useState<Record<string, QueryOutcome | null>>({});
  const [running, setRunning] = useState<Set<string>>(new Set());
  const [showVariables, setShowVariables] = useState(false);
  const [panelWidth, setPanelWidth] = useState<number>(() => {
    try {
      const saved = localStorage.getItem("db-free:dashboard-panel-width");
      return saved ? Math.max(300, Math.min(750, Number(saved))) : 400;
    } catch {
      return 400;
    }
  });

  const handlePanelResize = useCallback((delta: number) => {
    setPanelWidth((prev) => {
      const next = Math.max(300, Math.min(750, prev - delta));
      try {
        localStorage.setItem("db-free:dashboard-panel-width", String(next));
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  const patchBody = (partial: Partial<DashboardBody>) => {
    setBody((b) => ({ ...b, ...partial }));
    setDirty(true);
  };
  const patchWidget = (id: string, partial: Partial<Widget>) => patchBody({ widgets: body.widgets.map((w) => (w.id === id ? { ...w, ...partial } : w)) });

  const substituteVars = useCallback((sql: string) => body.variables.reduce((acc, v) => acc.split(`{{${v.name}}}`).join(v.value), sql), [body.variables]);

  const runWidget = useCallback(
    async (w: Widget) => {
      if (!connectionId || w.sql.trim().length === 0) return;
      if (!sessions.includes(connectionId) && !(await connect(connectionId))) return;
      setRunning((r) => new Set(r).add(w.id));
      try {
        const outcome = await ipc("execute_query", { connectionId, sql: substituteVars(w.sql), confirmDestructive: false, maxRows: 2000 });
        setResults((r) => ({ ...r, [w.id]: outcome }));
      } catch (raw) {
        showError(normalizeError(raw));
      } finally {
        setRunning((r) => {
          const next = new Set(r);
          next.delete(w.id);
          return next;
        });
      }
    },
    [connectionId, sessions, connect, substituteVars, showError],
  );

  const runAll = useCallback(() => {
    for (const w of body.widgets) void runWidget(w);
  }, [body.widgets, runWidget]);

  useEffect(() => {
    const id = window.setTimeout(runAll, 0);
    return () => window.clearTimeout(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- run once on open
  }, []);

  useEffect(() => {
    if (body.refreshSeconds <= 0) return;
    const id = window.setInterval(runAll, body.refreshSeconds * 1000);
    return () => window.clearInterval(id);
  }, [body.refreshSeconds, runAll]);

  const addWidget = () => {
    widgetCounter += 1;
    const w: Widget = {
      id: `w-${Date.now().toString(36)}-${widgetCounter}`,
      title: "",
      kind: "line",
      sql: "",
      x: 0,
      y: 0,
      w: 4,
      h: 3,
      tint: "series-1",
      showChange: false,
      maxValue: null,
      text: null,
      url: null,
      xLabel: null,
      yLabel: null,
      horizontal: false,
      showPercent: true,
      showValues: false,
      pulse: false,
      conditions: [],
    };
    patchBody({ widgets: [...body.widgets, w] });
    setSelectedId(w.id);
  };

  const save = async () => {
    try {
      await saveDocument({ ...doc, name, connectionId, body: { kind: "dashboard", data: body } });
      setDirty(false);
      showInfo("Dashboard saved.");
    } catch (raw) {
      showError(normalizeError(raw));
    }
  };

  const selected = body.widgets.find((w) => w.id === selectedId) ?? null;
  const schema = useMemo(() => Object.fromEntries((catalog?.schemas ?? []).flatMap((s) => s.tables.map((t) => [t.name, new Array<string>()]))), [catalog]);
  const engine = useWorkspace((s) => s.connections.find((c) => c.id === connectionId)?.engine ?? "postgres");
  const selectedOutcome = selected ? (results[selected.id] ?? null) : null;
  const selectedRows = selectedOutcome?.statements.find(isRows);

  return (
    <div className="flex h-full min-h-0">
      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex h-11 shrink-0 items-center gap-1 overflow-x-auto border-b border-border bg-surface px-2 whitespace-nowrap [scrollbar-width:none]">
          <Input
            value={name}
            onChange={(e) => {
              setName(e.target.value);
              setDirty(true);
            }}
            className="h-7 w-40 shrink-0 rounded-md bg-transparent text-sm text-foreground"
            aria-label="Dashboard name"
          />
          <AppSelect ariaLabel="Connection" value={connectionId ?? ""} options={[{ value: "", label: "— connection —" }, ...connections.map((c) => ({ value: c.id, label: c.name }))]} onChange={(v) => { setChosenConnectionId(v.length > 0 ? v : null); setDirty(true); }} size="sm" className="w-44 shrink-0" icon="database" />
          <Button size="sm" variant="ghost" className="text-muted" onPress={runAll} isDisabled={!connectionId}>
            <Icon name="refresh" size={13} />
            Refresh
          </Button>
          <AppSelect ariaLabel="Auto refresh" value={String(body.refreshSeconds)} options={[{ value: "0", label: "Manual" }, { value: "30", label: "Every 30 s" }, { value: "60", label: "Every minute" }, { value: "300", label: "Every 5 min" }]} onChange={(v) => patchBody({ refreshSeconds: Number(v) })} size="sm" className="w-36 shrink-0" />
          <Button size="sm" variant="ghost" className="text-muted" onPress={() => setShowVariables((v) => !v)}>
            <Icon name="braces" size={13} />
            Variables ({body.variables.length})
          </Button>
          <div className="ml-auto flex shrink-0 items-center gap-1 pl-2">
            <Button size="sm" variant="ghost" className="text-muted" onPress={addWidget}>
              <Icon name="plus" size={13} />
              Widget
            </Button>
            <Button size="sm" onPress={() => void save()} isDisabled={!dirty}>
              Save{dirty ? " *" : ""}
            </Button>
          </div>
        </div>
        {showVariables ? (
          <div className="flex flex-wrap items-end gap-2 border-b border-border bg-surface px-3 py-2">
            {body.variables.map((v, i) => (
              <div key={i} className="flex items-end gap-1">
                <Field label="Name" value={v.name} onChange={(name2) => patchBody({ variables: body.variables.map((x, j) => (j === i ? { ...x, name: name2 } : x)) })} className="w-32" mono />
                <Field label="Value" value={v.value} onChange={(value) => patchBody({ variables: body.variables.map((x, j) => (j === i ? { ...x, value } : x)) })} className="w-40" mono />
                <IconButton icon="x" label="Remove variable" onPress={() => patchBody({ variables: body.variables.filter((_, j) => j !== i) })} />
              </div>
            ))}
            <Button size="sm" variant="ghost" className="text-muted" onPress={() => patchBody({ variables: [...body.variables, { name: `var${body.variables.length + 1}`, value: "" }] })}>
              <Icon name="plus" size={13} />
              Add variable
            </Button>
            <span className="text-[11px] text-muted">Use as {"{{name}}"} inside widget SQL.</span>
          </div>
        ) : null}
        <ScrollShadow className="min-h-0 flex-1 p-4">
          {!connectionId ? (
            <EmptyState icon="columns" title="Pick a connection" body="Choose the connection widgets should query in the toolbar above." />
          ) : body.widgets.length === 0 ? (
            <EmptyState icon="columns" title="Empty dashboard" body="Add a widget, give it a SQL query, and pick a chart type." action={<Button size="sm" onPress={addWidget}>Add widget</Button>} />
          ) : (
            <div className="grid auto-rows-[120px] grid-cols-12 gap-3">
              {body.widgets.map((w) => (
                <div
                  key={w.id}
                  role="button"
                  tabIndex={0}
                  onClick={() => setSelectedId(w.id)}
                  onKeyDown={(e) => { if (e.key === "Enter") setSelectedId(w.id); }}
                  className={cn("flex min-h-0 flex-col overflow-hidden rounded-xl border bg-surface", selectedId === w.id ? "border-accent" : "border-border")}
                  style={{ gridColumn: `span ${Math.min(12, Math.max(2, w.w))}`, gridRow: `span ${Math.max(1, w.h)}`, borderTopWidth: 2, borderTopColor: tintCss(w.tint) }}
                >
                  <div className="flex h-8 shrink-0 items-center gap-2 px-3 text-xs">
                    <span className="truncate text-muted">{w.title.length > 0 ? w.title : "Untitled widget"}</span>
                    {running.has(w.id) ? <Spinner size="sm" className="ml-auto" /> : null}
                  </div>
                  <div className="min-h-0 flex-1">
                    <WidgetBody widget={w} outcome={results[w.id] ?? null} rowHeight={DENSITIES[density].rowHeight} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </ScrollShadow>
      </div>

      {selected ? (
        <aside className="relative flex shrink-0 flex-col overflow-y-auto border-l border-border/40 glass-sidebar select-none" style={{ width: panelWidth }}>
          <Resizer direction="horizontal" onResize={handlePanelResize} className="absolute -left-1 top-0 bottom-0" />
          <div className="flex items-center gap-2 border-b border-border/40 px-3 py-2">
            <Field label="" value={selected.title} onChange={(title) => patchWidget(selected.id, { title })} placeholder="Widget title" className="[&_label]:hidden" />
            <AppSelect ariaLabel="Widget type" value={selected.kind} options={KINDS} onChange={(kind) => patchWidget(selected.id, { kind })} className="w-40 shrink-0" />
          </div>
          <div className="h-48 shrink-0 border-b border-border p-2" style={{ borderTopWidth: 2, borderTopColor: tintCss(selected.tint) }}>
            <div className="px-1 pb-1 text-xs text-muted">{selected.title.length > 0 ? selected.title : "Preview"}</div>
            <div className="h-[calc(100%-20px)]">
              <WidgetBody widget={selected} outcome={selectedOutcome} rowHeight={DENSITIES[density].rowHeight} />
            </div>
          </div>

          <div className="flex flex-col gap-2 border-b border-border px-3 py-2 text-xs">
            <span className="text-muted">Options</span>
            <div className="flex flex-wrap items-center gap-3">
              {selected.kind !== "table" && selected.kind !== "text" && selected.kind !== "image" && selected.kind !== "gif" ? (
                <span className="flex items-center gap-2">
                  <span className="text-foreground">{selected.kind === "sankey" ? "Flow color" : selected.kind === "line" || selected.kind === "area" ? "Line color" : "Tint color"}</span>
                  <AppSelect ariaLabel="Tint" value={selected.tint ?? "series-1"} options={TINTS} onChange={(tint) => patchWidget(selected.id, { tint })} size="sm" className="w-28" />
                </span>
              ) : null}
              {selected.kind === "metric" || selected.kind === "sparkline" ? <Toggle checked={selected.showChange} onChange={(v) => patchWidget(selected.id, { showChange: v })} label="Show change" /> : null}
              {selected.kind === "bar" ? <Toggle checked={selected.horizontal} onChange={(v) => patchWidget(selected.id, { horizontal: v })} label="Horizontal" /> : null}
              {selected.kind === "map" ? <Toggle checked={selected.pulse} onChange={(v) => patchWidget(selected.id, { pulse: v })} label="Pulse" /> : null}
              {selected.kind === "progress" ? (
                <>
                  <Toggle checked={selected.showPercent} onChange={(v) => patchWidget(selected.id, { showPercent: v })} label="Show %" />
                  <Toggle checked={selected.showValues} onChange={(v) => patchWidget(selected.id, { showValues: v })} label="Show values" />
                  <Field label="Max" type="number" value={selected.maxValue === null ? "" : String(selected.maxValue)} onChange={(v) => patchWidget(selected.id, { maxValue: v.trim().length > 0 && Number.isFinite(Number(v)) ? Number(v) : null })} placeholder="from 2nd column" className="w-36" />
                </>
              ) : null}
            </div>
            {selected.kind === "line" || selected.kind === "area" || selected.kind === "bar" ? (
              <div className="flex items-end gap-2">
                <Field label="X label" value={selected.xLabel ?? ""} onChange={(v) => patchWidget(selected.id, { xLabel: v.length > 0 ? v : null })} placeholder="e.g. Month" />
                <Field label="Y label" value={selected.yLabel ?? ""} onChange={(v) => patchWidget(selected.id, { yLabel: v.length > 0 ? v : null })} placeholder="e.g. Revenue" />
              </div>
            ) : null}
            {selected.kind === "text" ? <Field label="Content" value={selected.text ?? ""} onChange={(v) => patchWidget(selected.id, { text: v })} placeholder="Content to display" description="Use {{column_name}} to insert query values." /> : null}
            {selected.kind === "image" || selected.kind === "gif" ? <Field label={selected.kind === "gif" ? "GIF URL" : "Image URL"} value={selected.url ?? ""} onChange={(v) => patchWidget(selected.id, { url: v })} placeholder="https://example.com/image.png" description="Use {{column_name}} to build the URL from query values." mono /> : null}
            {selected.kind === "sankey" ? <p className="text-muted">The first three columns are read as source, target, and value. Each row is a flow from source to target; duplicate pairs are summed.</p> : null}
            {selected.kind === "map" ? <p className="text-muted">Return lat and lon columns plus an optional label column.</p> : null}
            {selected.kind === "table" ? <p className="font-mono text-muted">SELECT * FROM table LIMIT 100</p> : null}
            <div className="flex flex-wrap items-center gap-3">
              <AppSelect ariaLabel="Width" value={String(selected.w)} options={[{ value: "3", label: "¼ width" }, { value: "4", label: "⅓ width" }, { value: "6", label: "½ width" }, { value: "12", label: "Full width" }]} onChange={(v) => patchWidget(selected.id, { w: Number(v) })} size="sm" className="w-28" />
              <AppSelect ariaLabel="Height" value={String(selected.h)} options={[{ value: "2", label: "Short" }, { value: "3", label: "Medium" }, { value: "4", label: "Tall" }]} onChange={(v) => patchWidget(selected.id, { h: Number(v) })} size="sm" className="w-28" />
            </div>
          </div>

          {HAS_SQL[selected.kind] ? (
            <div className="flex h-56 shrink-0 flex-col border-b border-border">
              <div className="min-h-0 flex-1">
                <SqlEditor value={selected.sql} onChange={(sql) => patchWidget(selected.id, { sql })} onRun={() => void runWidget(selected)} engine={engine} schema={schema} />
              </div>
              <div className="flex items-center gap-2 border-t border-border px-2 py-1.5">
                <Button size="sm" onPress={() => void runWidget(selected)} isDisabled={!connectionId || selected.sql.trim().length === 0} isPending={running.has(selected.id)}>
                  <Icon name="play" size={12} />
                  Run Query
                </Button>
                {selectedRows ? <span className="ml-auto text-[11px] text-muted">{formatCount(selectedRows.result.rows.length)} row{selectedRows.result.rows.length === 1 ? "" : "s"}, {formatMs(selectedOutcome?.elapsedMs ?? 0)}</span> : null}
              </div>
              {selectedRows && selectedRows.result.columns.length > 0 ? (
                <div className="h-20 shrink-0 border-t border-border">
                  <DataGrid columns={selectedRows.result.columns.map((c) => ({ name: c.name, typeName: c.typeName }))} rowCount={Math.min(selectedRows.result.rows.length, 5)} getRow={(i) => selectedRows.result.rows[i]} rowHeight={22} />
                </div>
              ) : null}
            </div>
          ) : null}

          {HAS_CONDITIONS[selected.kind] ? (
            <ConditionsEditor widget={selected} onChange={(conditions) => patchWidget(selected.id, { conditions })} />
          ) : null}

          <div className="mt-auto flex items-center border-t border-border px-3 py-2">
            <Button size="sm" variant="danger-soft" className="ml-auto" onPress={() => { patchBody({ widgets: body.widgets.filter((w) => w.id !== selected.id) }); setSelectedId(null); }}>
              <Icon name="trash" size={12} />
              Delete widget
            </Button>
          </div>
        </aside>
      ) : null}
    </div>
  );
}

// WHAT:  "Evaluates the first column of the first row. Last match wins."
function ConditionsEditor({ widget, onChange }: { widget: Widget; onChange: (c: WidgetCondition[]) => void }) {
  const contentLabel = widget.kind === "metric" ? "Tint when matched" : widget.kind === "text" ? "Content to display" : "URL to display";
  const patch = (i: number, partial: Partial<WidgetCondition>) => onChange(widget.conditions.map((c, j) => (j === i ? { ...c, ...partial } : c)));
  return (
    <div className="flex flex-col gap-2 border-b border-border px-3 py-2 text-xs">
      <p className="text-muted">Evaluates the first column of the first row. Last match wins.</p>
      {widget.conditions.map((c, i) => (
        <div key={i} className="flex flex-col gap-2 rounded-lg border border-border bg-background p-2">
          <div className="flex items-center gap-2">
            <Icon name="sort" size={12} className="text-muted" />
            <span className="text-[10px] font-medium tracking-wide text-muted uppercase">Condition {i + 1}</span>
            <span className="ml-auto">
              <CloseButton onPress={() => onChange(widget.conditions.filter((_, j) => j !== i))} aria-label="Remove condition" />
            </span>
          </div>
          <div className="flex items-center gap-2">
            <AppSelect ariaLabel="Operator" value={c.op} options={CONDITION_OPS} onChange={(op) => patch(i, { op })} size="sm" className="w-32" />
            <Field label="" value={c.value} onChange={(value) => patch(i, { value })} placeholder="value" className="[&_label]:hidden" mono />
          </div>
          {widget.kind === "metric" ? (
            <AppSelect ariaLabel={contentLabel} value={TINTS.some((t) => t.value === c.content) ? c.content : "series-1"} options={TINTS} onChange={(content) => patch(i, { content })} size="sm" className="w-32" />
          ) : (
            <Field label="" value={c.content} onChange={(content) => patch(i, { content })} placeholder={contentLabel} description="Use {{column_name}} to insert query values" className="[&_label]:hidden" mono={widget.kind !== "text"} />
          )}
        </div>
      ))}
      <Button size="sm" variant="secondary" className="self-start" onPress={() => onChange([...widget.conditions, { op: "equals", value: "", content: widget.kind === "metric" ? "series-6" : "" }])}>
        <Icon name="plus" size={12} />
        Add Condition
      </Button>
    </div>
  );
}

function WidgetBody({ widget, outcome, rowHeight }: { widget: Widget; outcome: QueryOutcome | null; rowHeight: number }) {
  const data = useMemo(() => chartData(outcome), [outcome]);
  const matchedTint = useMemo(() => {
    if (widget.kind !== "metric" || widget.conditions.length === 0) return widget.tint;
    const cell = outcome?.statements.find(isRows)?.result.rows[0]?.[0];
    let tint = widget.tint;
    for (const c of widget.conditions) if (conditionMatches(c, cell)) tint = c.content;
    return tint;
  }, [widget, outcome]);
  const tint = tintCss(matchedTint);
  const colors = seriesColors(widget.tint);
  const needsData = widget.kind !== "text" && widget.kind !== "image" && widget.kind !== "gif";
  if (needsData && !outcome) return <div className="flex h-full items-center justify-center text-xs text-muted">Run the query to populate</div>;
  switch (widget.kind) {
    case "metric":
      return <StatTile data={data} widget={widget} tint={tint} />;
    case "sparkline":
      return <SparklineWidget data={data} widget={widget} tint={tint} />;
    case "line":
      return <div className="h-full p-2"><LineChart data={data} colors={colors} xLabel={widget.xLabel} yLabel={widget.yLabel} /></div>;
    case "area":
      return <div className="h-full p-2"><LineChart data={data} colors={colors} area xLabel={widget.xLabel} yLabel={widget.yLabel} /></div>;
    case "bar":
      return <div className="h-full p-2"><BarChart data={data} colors={colors} horizontal={widget.horizontal} xLabel={widget.xLabel} yLabel={widget.yLabel} /></div>;
    case "pie":
      return <div className="h-full p-2"><PieChart data={data} tint={tint} /></div>;
    case "sankey":
      return <div className="h-full p-2"><SankeyChart outcome={outcome} tint={tint} /></div>;
    case "map":
      return <MapChart outcome={outcome} tint={tint} pulse={widget.pulse} />;
    case "progress":
      return <ProgressMeter outcome={outcome} widget={widget} tint={tint} />;
    case "text":
      return <TextWidget widget={widget} outcome={outcome} />;
    case "image":
      return <ImageWidget widget={widget} outcome={outcome} gif={false} />;
    case "gif":
      return <ImageWidget widget={widget} outcome={outcome} gif />;
    case "table": {
      const rows = outcome?.statements.find(isRows);
      if (!rows) return <div className="flex h-full items-center justify-center text-xs text-muted">No rows</div>;
      return <DataGrid columns={rows.result.columns.map((c) => ({ name: c.name, typeName: c.typeName }))} rowCount={rows.result.rows.length} getRow={(i) => rows.result.rows[i]} rowHeight={rowHeight} />;
    }
  }
}
