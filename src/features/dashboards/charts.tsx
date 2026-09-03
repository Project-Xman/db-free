// SOT: dashboard-charts, chart-data-extraction, line-chart, area-chart, bar-chart, pie-chart, sankey, map-chart, progress-meter, stat-tile, text-widget, image-widget, widget-conditions, chart-tooltip, chart-legend
import { useMemo, useState } from "react";
import type { QueryOutcome, StatementResult, Value, Widget, WidgetCondition } from "@/lib/bindings";
import { formatCell, formatCount } from "@/lib/format";
import { cn } from "@/lib/cn";

// WHAT:  Inline-SVG charts for dashboard widgets, following the dataviz method:
//        2px lines, <=24px bars with 4px rounded data-ends and 2px surface gaps,
//        10% area wash, legend for >=2 series, hover tooltips, text in text tokens,
//        categorical hues assigned in fixed order (never cycled).
// WHY:   No chart library: small bundle, works offline, theme via CSS tokens.
// WHERE: src/styles/globals.css (--color-series-*), ./DashboardTab.tsx
export const SERIES_COLORS = ["var(--color-series-1)", "var(--color-series-2)", "var(--color-series-3)", "var(--color-series-4)", "var(--color-series-5)", "var(--color-series-6)", "var(--color-series-7)"] as const;

export interface ChartData {
  labels: string[];
  series: { name: string; values: number[] }[];
}

export function isRows(s: StatementResult): s is Extract<StatementResult, { kind: "rows" }> {
  return s.kind === "rows";
}

function cellText(v: Value | undefined): string {
  return v === undefined ? "" : v.t === "null" ? "" : formatCell(v).text;
}

function toNumber(v: Value | undefined): number {
  if (!v) return NaN;
  if (v.t === "int" || v.t === "float") return v.v;
  if (v.t === "decimal" || v.t === "text") return Number(v.v);
  if (v.t === "bool") return v.v ? 1 : 0;
  return NaN;
}

function isNumericColumn(rows: readonly (readonly Value[])[], i: number): boolean {
  const first = rows.find((r) => {
    const v = r[i];
    return v !== undefined && v.t !== "null";
  })?.[i];
  return first !== undefined && (first.t === "int" || first.t === "float" || first.t === "decimal");
}

// WHAT:  First non-numeric column = x labels; every numeric column = one series.
//        A single numeric column with no label column is indexed 1..n.
export function chartData(outcome: QueryOutcome | null): ChartData {
  const rows = outcome?.statements.find(isRows);
  if (!rows) return { labels: [], series: [] };
  const cols = rows.result.columns;
  const numeric = cols.map((_, i) => isNumericColumn(rows.result.rows, i));
  const labelIndex = numeric.findIndex((n) => !n);
  const seriesIndexes = numeric.map((n, i) => (n ? i : -1)).filter((i) => i >= 0).slice(0, SERIES_COLORS.length);
  const labels = rows.result.rows.map((r, i) => (labelIndex >= 0 ? cellText(r[labelIndex]) : String(i + 1)));
  const series = seriesIndexes.map((i) => ({ name: cols[i]?.name ?? `Series ${i + 1}`, values: rows.result.rows.map((r) => toNumber(r[i])) }));
  return { labels, series };
}

function fmt(n: number): string {
  if (!Number.isFinite(n)) return "-";
  return Number.isInteger(n) ? formatCount(n) : n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

// ---------------------------------------------------------------- conditions
function firstCell(outcome: QueryOutcome | null): Value | undefined {
  return outcome?.statements.find(isRows)?.result.rows[0]?.[0];
}

export function conditionMatches(cond: WidgetCondition, cell: Value | undefined): boolean {
  const text = cellText(cell);
  const num = toNumber(cell);
  const target = Number(cond.value);
  const bothNumeric = Number.isFinite(num) && Number.isFinite(target) && cond.value.trim().length > 0;
  switch (cond.op) {
    case "equals":
      return bothNumeric ? num === target : text === cond.value;
    case "not_equals":
      return bothNumeric ? num !== target : text !== cond.value;
    case "gt":
      return bothNumeric && num > target;
    case "gte":
      return bothNumeric && num >= target;
    case "lt":
      return bothNumeric && num < target;
    case "lte":
      return bothNumeric && num <= target;
    case "contains":
      return text.toLowerCase().includes(cond.value.toLowerCase());
  }
}

// WHAT:  "Evaluates the first column of the first row. Last match wins."
export function matchedContent(widget: Widget, outcome: QueryOutcome | null, fallback: string): string {
  const cell = firstCell(outcome);
  let content = fallback;
  for (const c of widget.conditions) if (conditionMatches(c, cell)) content = c.content;
  return substitute(content, outcome);
}

// WHAT:  Replaces {{column}} with the first row's value for that column.
export function substitute(text: string, outcome: QueryOutcome | null): string {
  const rows = outcome?.statements.find(isRows);
  if (!rows) return text;
  const first = rows.result.rows[0] ?? [];
  return text.replace(/\{\{\s*([^}\s]+)\s*\}\}/g, (_, name: string) => {
    const i = rows.result.columns.findIndex((c) => c.name === name);
    return i >= 0 ? cellText(first[i]) : "";
  });
}

// ---------------------------------------------------------------- shared bits
function Tooltip({ x, y, label, rows }: { x: number; y: number; label: string; rows: { name: string; value: string; color: string }[] }) {
  return (
    <div className="pointer-events-none absolute z-10 rounded-md border border-border bg-overlay px-2 py-1 text-[11px] text-foreground shadow" style={{ left: `${x}%`, top: `${y}%`, transform: "translate(-50%, -110%)" }}>
      <div className="mb-0.5 text-muted">{label}</div>
      {rows.map((r) => (
        <div key={r.name} className="flex items-center gap-1.5">
          <span className="size-2 rounded-sm" style={{ background: r.color }} />
          <span className="text-muted">{r.name}</span>
          <span className="ml-auto pl-3 tabular-nums">{r.value}</span>
        </div>
      ))}
    </div>
  );
}

function Legend({ data, colors }: { data: ChartData; colors: readonly string[] }) {
  if (data.series.length < 2) return null;
  return (
    <div className="flex flex-wrap items-center justify-center gap-3 px-2 pb-1 text-[11px] text-muted">
      {data.series.map((s, i) => (
        <span key={s.name} className="flex items-center gap-1.5">
          <span className="size-2 rounded-sm" style={{ background: colors[i % colors.length] }} />
          {s.name}
        </span>
      ))}
    </div>
  );
}

function Empty({ hint }: { hint: string }) {
  return <div className="flex h-full items-center justify-center px-4 text-center text-xs text-muted">{hint}</div>;
}

function AxisLabels({ x, y }: { x: string | null; y: string | null }) {
  if (!x && !y) return null;
  return (
    <div className="pointer-events-none absolute inset-0 text-[10px] text-muted">
      {x ? <span className="absolute bottom-0 left-1/2 -translate-x-1/2">{x}</span> : null}
      {y ? <span className="absolute top-1/2 left-0 -translate-y-1/2 -rotate-90">{y}</span> : null}
    </div>
  );
}

function ChangeBadge({ change }: { change: number }) {
  return (
    <span className={cn("rounded-md px-1.5 py-0.5 text-[11px] font-medium", change >= 0 ? "bg-success-soft text-success" : "bg-danger-soft text-danger")}>
      {change >= 0 ? "+" : ""}
      {change.toFixed(1)}%
    </span>
  );
}

function changeOf(values: number[]): number | null {
  const last = values.at(-1);
  const prev = values.at(-2);
  return last !== undefined && prev !== undefined && prev !== 0 ? ((last - prev) / Math.abs(prev)) * 100 : null;
}

// ---------------------------------------------------------------- metric / sparkline
export function StatTile({ data, widget, tint }: { data: ChartData; widget: Widget; tint: string }) {
  const values = data.series[0]?.values ?? [];
  const last = values.at(-1);
  const change = changeOf(values);
  return (
    <div className="flex h-full flex-col p-3">
      <div className="flex items-start justify-between">
        <span className="text-[40px] leading-none font-semibold tabular-nums" style={{ color: tint }}>{last === undefined ? "-" : fmt(last)}</span>
        {widget.showChange && change !== null ? <ChangeBadge change={change} /> : null}
      </div>
      {values.length > 1 ? (
        <div className="mt-auto h-10">
          <LineChart data={{ labels: data.labels, series: data.series.slice(0, 1) }} colors={[tint]} compact area />
        </div>
      ) : null}
    </div>
  );
}

export function SparklineWidget({ data, widget, tint }: { data: ChartData; widget: Widget; tint: string }) {
  const change = changeOf(data.series[0]?.values ?? []);
  return (
    <div className="relative h-full p-2">
      {widget.showChange && change !== null ? (
        <span className="absolute top-1 right-2 z-10">
          <ChangeBadge change={change} />
        </span>
      ) : null}
      <LineChart data={{ labels: data.labels, series: data.series.slice(0, 1) }} colors={[tint]} compact area />
    </div>
  );
}

// ---------------------------------------------------------------- line / area
interface LineProps {
  data: ChartData;
  colors: readonly string[];
  compact?: boolean;
  area?: boolean;
  xLabel?: string | null;
  yLabel?: string | null;
}

export function LineChart({ data, colors, compact = false, area = false, xLabel = null, yLabel = null }: LineProps) {
  const [hover, setHover] = useState<number | null>(null);
  const W = 400;
  const H = compact ? 60 : 200;
  const padX = compact ? 2 : 36;
  const padTop = compact ? 4 : 12;
  const padBottom = compact ? 4 : 26;
  const n = data.labels.length;
  const all = data.series.flatMap((s) => s.values).filter(Number.isFinite);
  const min = Math.min(...all, 0);
  const max = Math.max(...all, 0);
  const span = max - min || 1;
  if (n === 0 || data.series.length === 0) return <Empty hint="Return a label column plus one or more numeric columns." />;
  if (n === 1) {
    return (
      <div className="flex h-full items-center justify-center gap-2">
        <span className={compact ? "text-2xl font-semibold tabular-nums" : "text-[40px] font-semibold tabular-nums"} style={{ color: colors[0] }}>{fmt(data.series[0]?.values[0] ?? NaN)}</span>
      </div>
    );
  }
  const step = (W - padX * 2) / (n - 1);
  const px = (i: number) => padX + i * step;
  const py = (v: number) => padTop + (1 - ((Number.isFinite(v) ? v : min) - min) / span) * (H - padTop - padBottom);
  const tickEvery = Math.max(1, Math.ceil(n / 8));
  return (
    <div className="relative flex h-full w-full flex-col">
      <div className="relative min-h-0 flex-1">
        <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="h-full w-full" role="img" aria-label="Line chart">
          {!compact ? [0, 0.5, 1].map((t) => <line key={t} x1={padX} x2={W - padX} y1={padTop + (H - padTop - padBottom) * t} y2={padTop + (H - padTop - padBottom) * t} stroke="var(--separator)" strokeWidth={1} />) : null}
          {data.series.map((s, si) => {
            const color = colors[si % colors.length] ?? colors[0] ?? "currentColor";
            const path = s.values.map((v, i) => `${i === 0 ? "M" : "L"}${px(i).toFixed(1)},${py(v).toFixed(1)}`).join(" ");
            const areaPath = `${path} L${px(n - 1).toFixed(1)},${(H - padBottom).toFixed(1)} L${px(0).toFixed(1)},${(H - padBottom).toFixed(1)} Z`;
            return (
              <g key={s.name}>
                {area ? <path d={areaPath} fill={color} opacity={0.12} /> : null}
                <path d={path} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
                {hover !== null ? <circle cx={px(hover)} cy={py(s.values[hover] ?? min)} r={4} fill={color} stroke="var(--surface)" strokeWidth={2} /> : null}
              </g>
            );
          })}
          {!compact ? (
            <>
              <text x={padX - 4} y={padTop + 4} textAnchor="end" fontSize={10} fill="var(--muted)">{fmt(max)}</text>
              <text x={padX - 4} y={H - padBottom} textAnchor="end" fontSize={10} fill="var(--muted)">{fmt(min)}</text>
              {data.labels.map((l, i) => (i % tickEvery === 0 ? <text key={i} x={px(i)} y={H - padBottom + 14} textAnchor="middle" fontSize={10} fill="var(--muted)">{l.slice(0, 8)}</text> : null))}
            </>
          ) : null}
          {data.labels.map((_, i) => (
            <rect key={i} x={px(i) - step / 2} y={0} width={step} height={H} fill="transparent" onMouseEnter={() => setHover(i)} onMouseLeave={() => setHover(null)} />
          ))}
        </svg>
        {hover !== null ? (
          <Tooltip x={(px(hover) / W) * 100} y={Math.max(18, (py(data.series[0]?.values[hover] ?? min) / H) * 100)} label={data.labels[hover] ?? ""} rows={data.series.map((s, si) => ({ name: s.name, value: fmt(s.values[hover] ?? NaN), color: colors[si % colors.length] ?? "" }))} />
        ) : null}
        <AxisLabels x={compact ? null : xLabel} y={compact ? null : yLabel} />
      </div>
      {!compact ? <Legend data={data} colors={colors} /> : null}
    </div>
  );
}

// ---------------------------------------------------------------- bar
export function BarChart({ data, colors, horizontal = false, xLabel = null, yLabel = null }: { data: ChartData; colors: readonly string[]; horizontal?: boolean; xLabel?: string | null; yLabel?: string | null }) {
  const [hover, setHover] = useState<number | null>(null);
  const W = 400;
  const H = 200;
  const padX = horizontal ? 70 : 36;
  const padTop = 12;
  const padBottom = horizontal ? 12 : 26;
  const n = data.labels.length;
  const k = data.series.length;
  if (n === 0 || k === 0) return <Empty hint="Return a label column plus one or more numeric columns." />;
  const all = data.series.flatMap((s) => s.values).filter(Number.isFinite);
  const max = Math.max(...all, 0);
  const min = Math.min(...all, 0);
  const span = max - min || 1;
  const slot = horizontal ? (H - padTop - padBottom) / n : (W - padX * 2) / n;
  const thick = Math.min(24, (slot - 4) / k);
  const baselineV = horizontal ? padX + (W - padX * 2) * (-min / span) : padTop + (H - padTop - padBottom) * (max / span);
  return (
    <div className="relative flex h-full w-full flex-col">
      <div className="relative min-h-0 flex-1">
        <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="h-full w-full" role="img" aria-label="Bar chart">
          {horizontal ? <line x1={baselineV} x2={baselineV} y1={padTop} y2={H - padBottom} stroke="var(--separator)" strokeWidth={1} /> : <line x1={padX} x2={W - padX} y1={baselineV} y2={baselineV} stroke="var(--separator)" strokeWidth={1} />}
          {data.labels.map((label, i) =>
            data.series.map((s, si) => {
              const raw = s.values[i] ?? NaN;
              const v = Number.isFinite(raw) ? raw : 0;
              const len = (Math.abs(v) / span) * (horizontal ? W - padX * 2 : H - padTop - padBottom);
              const color = colors[si % colors.length] ?? "";
              const r = Math.min(4, thick / 2, len);
              const off = i * slot + (slot - thick * k - 2 * (k - 1)) / 2 + si * (thick + 2);
              let d: string;
              if (horizontal) {
                const y = padTop + off;
                const x0 = v >= 0 ? baselineV : baselineV - len;
                const x1 = v >= 0 ? baselineV + len : baselineV;
                d = v >= 0
                  ? `M${x0},${y} H${x1 - r} Q${x1},${y} ${x1},${y + r} V${y + thick - r} Q${x1},${y + thick} ${x1 - r},${y + thick} H${x0} Z`
                  : `M${x1},${y} H${x0 + r} Q${x0},${y} ${x0},${y + r} V${y + thick - r} Q${x0},${y + thick} ${x0 + r},${y + thick} H${x1} Z`;
              } else {
                const x = padX + off;
                const y = v >= 0 ? baselineV - len : baselineV;
                d = v >= 0
                  ? `M${x},${y + len} V${y + r} Q${x},${y} ${x + r},${y} H${x + thick - r} Q${x + thick},${y} ${x + thick},${y + r} V${y + len} Z`
                  : `M${x},${y} V${y + len - r} Q${x},${y + len} ${x + r},${y + len} H${x + thick - r} Q${x + thick},${y + len} ${x + thick},${y + len - r} V${y} Z`;
              }
              return <path key={`${label}-${s.name}`} d={d} fill={color} opacity={hover === null || hover === i ? 1 : 0.5} onMouseEnter={() => setHover(i)} onMouseLeave={() => setHover(null)} />;
            }),
          )}
          {horizontal
            ? data.labels.map((l, i) => <text key={i} x={padX - 6} y={padTop + i * slot + slot / 2 + 3} textAnchor="end" fontSize={10} fill="var(--muted)">{l.slice(0, 10)}</text>)
            : n <= 14
              ? data.labels.map((l, i) => <text key={i} x={padX + i * slot + slot / 2} y={H - 8} textAnchor="middle" fontSize={10} fill="var(--muted)">{l.slice(0, 8)}</text>)
              : null}
          {!horizontal ? <text x={padX - 4} y={padTop + 4} textAnchor="end" fontSize={10} fill="var(--muted)">{fmt(max)}</text> : null}
        </svg>
        {hover !== null ? (
          <Tooltip x={horizontal ? 50 : ((padX + hover * slot + slot / 2) / W) * 100} y={horizontal ? ((padTop + hover * slot) / H) * 100 : 20} label={data.labels[hover] ?? ""} rows={data.series.map((s, si) => ({ name: s.name, value: fmt(s.values[hover] ?? NaN), color: colors[si % colors.length] ?? "" }))} />
        ) : null}
        <AxisLabels x={xLabel} y={yLabel} />
      </div>
      <Legend data={data} colors={colors} />
    </div>
  );
}

// ---------------------------------------------------------------- pie (donut)
function sliceAngles(values: readonly number[], total: number): { a0: number; a1: number; v: number }[] {
  const out: { a0: number; a1: number; v: number }[] = [];
  let angle = -Math.PI / 2;
  for (const v of values) {
    const a1 = angle + (v / total) * Math.PI * 2;
    out.push({ a0: angle, a1, v });
    angle = a1;
  }
  return out;
}

export function PieChart({ data, tint }: { data: ChartData; tint: string }) {
  const [hover, setHover] = useState<number | null>(null);
  const values = (data.series[0]?.values ?? []).map((v) => (Number.isFinite(v) && v > 0 ? v : 0));
  const total = values.reduce((a, b) => a + b, 0);
  if (total <= 0) return <Empty hint="Return a label column and a positive numeric column." />;
  const R = 80;
  const r = 44;
  const slices = sliceAngles(values, total).map((s, i) => ({ ...s, i, label: data.labels[i] ?? "" }));
  const arc = (a0: number, a1: number) => {
    const gap = 0.02;
    const s0 = a0 + gap;
    const s1 = Math.max(s0, a1 - gap);
    const large = s1 - s0 > Math.PI ? 1 : 0;
    const p = (ang: number, rad: number) => `${(100 + rad * Math.cos(ang)).toFixed(2)},${(100 + rad * Math.sin(ang)).toFixed(2)}`;
    return `M${p(s0, R)} A${R},${R} 0 ${large} 1 ${p(s1, R)} L${p(s1, r)} A${r},${r} 0 ${large} 0 ${p(s0, r)} Z`;
  };
  const current = hover === null ? undefined : slices[hover];
  return (
    <div className="relative flex h-full w-full items-center justify-center">
      <svg viewBox="0 0 200 200" className="h-full max-h-full" role="img" aria-label="Pie chart">
        {slices.map((s) => (
          <path key={s.i} d={arc(s.a0, s.a1)} fill={tint} opacity={hover === null || hover === s.i ? 1 - (s.i % 6) * 0.13 : 0.35} onMouseEnter={() => setHover(s.i)} onMouseLeave={() => setHover(null)} />
        ))}
      </svg>
      {current ? (
        <div className="pointer-events-none absolute rounded-md border border-border bg-overlay px-2 py-1 text-[11px] text-foreground shadow">
          <span className="text-muted">{current.label}</span> {fmt(current.v)} ({((current.v / total) * 100).toFixed(1)}%)
        </div>
      ) : null}
    </div>
  );
}

// ---------------------------------------------------------------- sankey (source, target, value)
interface SankeyModel {
  nodes: { name: string; column: number; y: number; height: number }[];
  links: { source: number; target: number; value: number; width: number; sy: number; ty: number }[];
  columns: number;
}

export function SankeyChart({ outcome, tint }: { outcome: QueryOutcome | null; tint: string }) {
  const [hover, setHover] = useState<number | null>(null);
  const model = useMemo(() => buildSankey(outcome), [outcome]);
  if (!model) return <Empty hint="Return three columns: source, target, value. Each row is a flow; duplicate pairs are summed." />;
  const W = 400;
  const H = 200;
  const colW = 10;
  const colX = (c: number) => 16 + (c * (W - 32 - colW)) / Math.max(1, model.columns - 1);
  const hovered = hover === null ? undefined : model.links[hover];
  return (
    <div className="relative h-full w-full">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="h-full w-full" role="img" aria-label="Sankey diagram">
        {model.links.map((l, i) => {
          const s = model.nodes[l.source];
          const t = model.nodes[l.target];
          if (!s || !t) return null;
          const x0 = colX(s.column) + colW;
          const x1 = colX(t.column);
          const y0 = s.y + l.sy;
          const y1 = t.y + l.ty;
          const w = l.width;
          const c = (x0 + x1) / 2;
          const d = `M${x0},${y0} C${c},${y0} ${c},${y1} ${x1},${y1} L${x1},${y1 + w} C${c},${y1 + w} ${c},${y0 + w} ${x0},${y0 + w} Z`;
          return <path key={i} d={d} fill={tint} opacity={hover === null || hover === i ? 0.35 : 0.12} onMouseEnter={() => setHover(i)} onMouseLeave={() => setHover(null)} />;
        })}
        {model.nodes.map((n) => (
          <g key={n.name}>
            <rect x={colX(n.column)} y={n.y} width={colW} height={Math.max(2, n.height)} rx={2} fill={tint} />
            <text x={n.column === model.columns - 1 ? colX(n.column) - 4 : colX(n.column) + colW + 4} y={n.y + n.height / 2 + 3} textAnchor={n.column === model.columns - 1 ? "end" : "start"} fontSize={10} fill="var(--muted)">{n.name}</text>
          </g>
        ))}
      </svg>
      {hovered ? (
        <div className="pointer-events-none absolute top-1 left-1 rounded-md border border-border bg-overlay px-2 py-1 text-[11px] text-foreground shadow">
          <span className="text-muted">{model.nodes[hovered.source]?.name} to {model.nodes[hovered.target]?.name}</span> {fmt(hovered.value)}
        </div>
      ) : null}
    </div>
  );
}

function buildSankey(outcome: QueryOutcome | null): SankeyModel | null {
  const rows = outcome?.statements.find(isRows);
  if (!rows || rows.result.columns.length < 3) return null;
  const flows = new Map<string, { s: string; t: string; v: number }>();
  for (const r of rows.result.rows) {
    const s = cellText(r[0]);
    const t = cellText(r[1]);
    const v = toNumber(r[2]);
    if (!s || !t || !Number.isFinite(v) || v <= 0 || s === t) continue;
    const key = `${s} ${t}`;
    const existing = flows.get(key);
    if (existing) existing.v += v;
    else flows.set(key, { s, t, v });
  }
  if (flows.size === 0) return null;
  // Column = longest path from a pure source (cycles stop growing after n passes).
  const names = [...new Set([...flows.values()].flatMap((f) => [f.s, f.t]))];
  const column = new Map<string, number>(names.map((n) => [n, 0]));
  let passes = 0;
  let changed = true;
  while (changed && passes < names.length) {
    passes += 1;
    changed = false;
    for (const f of flows.values()) {
      const next = (column.get(f.s) ?? 0) + 1;
      if (next > (column.get(f.t) ?? 0) && next < names.length) {
        column.set(f.t, next);
        changed = true;
      }
    }
  }
  const columns = Math.max(...column.values()) + 1;
  const H = 200;
  const total = (n: string) =>
    Math.max(
      [...flows.values()].filter((f) => f.s === n).reduce((a, f) => a + f.v, 0),
      [...flows.values()].filter((f) => f.t === n).reduce((a, f) => a + f.v, 0),
    );
  const colTotals = Array.from({ length: columns }, (_, c) => names.filter((n) => column.get(n) === c).reduce((a, n) => a + total(n), 0));
  const scale = (H - 24) / Math.max(...colTotals, 1);
  const nodes: SankeyModel["nodes"] = [];
  for (let c = 0; c < columns; c++) {
    const inCol = names.filter((n) => column.get(n) === c);
    const used = inCol.reduce((a, n) => a + total(n) * scale, 0);
    const gap = inCol.length > 1 ? Math.min(12, (H - 24 - used) / (inCol.length - 1)) : 0;
    let y = 12;
    for (const n of inCol) {
      const h = total(n) * scale;
      nodes.push({ name: n, column: c, y, height: h });
      y += h + Math.max(2, gap);
    }
  }
  const index = new Map(nodes.map((n, i) => [n.name, i]));
  const outOffset = new Map<string, number>();
  const inOffset = new Map<string, number>();
  const links: SankeyModel["links"] = [];
  for (const f of [...flows.values()].sort((a, b) => (column.get(a.s) ?? 0) - (column.get(b.s) ?? 0))) {
    const width = f.v * scale;
    const sy = outOffset.get(f.s) ?? 0;
    const ty = inOffset.get(f.t) ?? 0;
    outOffset.set(f.s, sy + width);
    inOffset.set(f.t, ty + width);
    links.push({ source: index.get(f.s) ?? 0, target: index.get(f.t) ?? 0, value: f.v, width, sy, ty });
  }
  return { nodes, links, columns };
}

// ---------------------------------------------------------------- map (lat, lon[, label])
export function MapChart({ outcome, tint, pulse }: { outcome: QueryOutcome | null; tint: string; pulse: boolean }) {
  const points = useMemo(() => {
    const rows = outcome?.statements.find(isRows);
    if (!rows) return [];
    const cols = rows.result.columns.map((c) => c.name.toLowerCase());
    const latI = cols.findIndex((c) => c.startsWith("lat"));
    const lonI = cols.findIndex((c) => c.startsWith("lon") || c.startsWith("lng"));
    const li = latI >= 0 ? latI : 0;
    const lo = lonI >= 0 ? lonI : 1;
    const labelI = cols.findIndex((_, i) => i !== li && i !== lo);
    return rows.result.rows
      .map((r) => ({ lat: toNumber(r[li]), lon: toNumber(r[lo]), label: labelI >= 0 ? cellText(r[labelI]) : "" }))
      .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lon) && Math.abs(p.lat) <= 90 && Math.abs(p.lon) <= 180);
  }, [outcome]);
  if (points.length === 0) return <Empty hint="Return latitude and longitude columns (lat, lon) plus an optional label." />;
  const W = 360;
  const H = 180;
  const x = (lon: number) => ((lon + 180) / 360) * W;
  const y = (lat: number) => ((90 - lat) / 180) * H;
  return (
    <div className="h-full w-full">
      <svg viewBox={`0 0 ${W} ${H}`} className="h-full w-full" role="img" aria-label="Map">
        <rect width={W} height={H} fill="var(--surface-secondary)" />
        {[-60, -30, 0, 30, 60].map((lat) => <line key={`lat${lat}`} x1={0} x2={W} y1={y(lat)} y2={y(lat)} stroke="var(--separator)" strokeWidth={lat === 0 ? 1 : 0.5} />)}
        {[-150, -120, -90, -60, -30, 0, 30, 60, 90, 120, 150].map((lon) => <line key={`lon${lon}`} x1={x(lon)} x2={x(lon)} y1={0} y2={H} stroke="var(--separator)" strokeWidth={lon === 0 ? 1 : 0.5} />)}
        {points.map((p, i) => (
          <g key={i}>
            {pulse ? <circle cx={x(p.lon)} cy={y(p.lat)} r={8} fill={tint} opacity={0.25} className="animate-ping" style={{ transformOrigin: `${x(p.lon)}px ${y(p.lat)}px` }} /> : null}
            <circle cx={x(p.lon)} cy={y(p.lat)} r={3.5} fill={tint} stroke="var(--surface)" strokeWidth={1.5}>
              <title>{p.label ? `${p.label}: ` : ""}{p.lat.toFixed(3)}, {p.lon.toFixed(3)}</title>
            </circle>
          </g>
        ))}
      </svg>
    </div>
  );
}

// ---------------------------------------------------------------- progress
export function ProgressMeter({ outcome, widget, tint }: { outcome: QueryOutcome | null; widget: Widget; tint: string }) {
  const rows = outcome?.statements.find(isRows);
  const first = rows?.result.rows[0];
  const value = toNumber(first?.[0]);
  const max = widget.maxValue ?? toNumber(first?.[1]);
  if (!Number.isFinite(value) || !Number.isFinite(max) || max <= 0) return <Empty hint="Return a value (and optionally a target) or set the maximum in options." />;
  const pct = Math.max(0, Math.min(100, (value / max) * 100));
  return (
    <div className="flex h-full flex-col items-center justify-center gap-2 px-6">
      <div className="h-2.5 w-full overflow-hidden rounded-full bg-surface-tertiary">
        <div className="h-full rounded-full" style={{ width: `${pct}%`, background: tint }} />
      </div>
      <div className="flex items-center gap-3 text-sm">
        {widget.showPercent ? <span className="font-medium" style={{ color: tint }}>{pct.toFixed(0)}%</span> : null}
        {widget.showValues ? <span className="text-muted">{fmt(value)} / {fmt(max)}</span> : null}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------- text / image
export function TextWidget({ widget, outcome }: { widget: Widget; outcome: QueryOutcome | null }) {
  const text = matchedContent(widget, outcome, widget.text ?? "");
  if (text.trim().length === 0) return <Empty hint="Add content in the options. Use {{column}} to insert query values." />;
  return <div className="selectable h-full overflow-auto p-3 text-sm whitespace-pre-wrap text-foreground">{text}</div>;
}

export function ImageWidget({ widget, outcome, gif }: { widget: Widget; outcome: QueryOutcome | null; gif: boolean }) {
  const url = matchedContent(widget, outcome, widget.url ?? "").trim();
  if (url.length === 0) return <Empty hint={gif ? "Paste a GIF URL in the options." : "Paste an image URL in the options."} />;
  return (
    <div className="flex h-full items-center justify-center overflow-hidden p-2">
      <img src={url} alt={widget.title} className="max-h-full max-w-full rounded-md object-contain" />
    </div>
  );
}
