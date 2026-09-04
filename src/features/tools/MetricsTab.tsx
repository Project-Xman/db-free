// SOT: metrics-tab, range-query-playground, time-range-presets, series-chart
import { useEffect, useMemo, useRef, useState } from "react";
import { Button, Spinner, TextArea } from "@heroui/react";
import type { RangeResult, Series } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { engineMeta } from "@/lib/engines";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { DateTimeField, Segmented } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { SERIES_COLORS } from "@/features/dashboards/charts";
import { ToolBody, ToolShell } from "./ToolShell";
import { cn } from "@/lib/cn";

type Preset = "10m" | "1h" | "6h" | "24h" | "7d" | "30d" | "custom";
const PRESETS: readonly { value: Preset; label: string; seconds: number }[] = [
  { value: "10m", label: "10m", seconds: 600 },
  { value: "1h", label: "1h", seconds: 3600 },
  { value: "6h", label: "6h", seconds: 6 * 3600 },
  { value: "24h", label: "24h", seconds: 24 * 3600 },
  { value: "7d", label: "7d", seconds: 7 * 24 * 3600 },
  { value: "30d", label: "30d", seconds: 30 * 24 * 3600 },
  { value: "custom", label: "Custom", seconds: 0 },
];
const POINTS = 240;

function seed(language: string): string {
  if (language === "InfluxQL") return 'SELECT mean("value") FROM "cpu" WHERE time > now() - 1h GROUP BY time(30s)';
  return "up";
}

function toIso(seconds: number): string {
  return new Date(seconds * 1000).toISOString().replace("T", " ").slice(0, 19);
}

// WHAT:  Metrics explorer: an expression over a time window with a step,
//        charted as one line per series, with a legend of label sets.
// WHERE: src-tauri/src/integrations/mod.rs (query_range), src/features/tools/ToolTab.tsx
export function MetricsTab({ connectionId }: { connectionId: string }) {
  const engine = useWorkspace((s) => s.connections.find((c) => c.id === connectionId)?.engine ?? "prometheus");
  const language = engineMeta(engine).commandLanguage;
  const [query, setQuery] = useState(() => seed(language));
  const [preset, setPreset] = useState<Preset>("1h");
  const [start, setStart] = useState(() => toIso(Date.now() / 1000 - 3600));
  const [end, setEnd] = useState(() => toIso(Date.now() / 1000));
  const [result, setResult] = useState<RangeResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [hidden, setHidden] = useState<ReadonlySet<string>>(new Set());

  const window = (): { from: number; to: number } => {
    const now = Date.now() / 1000;
    const p = PRESETS.find((x) => x.value === preset);
    if (p && p.seconds > 0) return { from: now - p.seconds, to: now };
    const from = Date.parse(start.replace(" ", "T") + "Z") / 1000;
    const to = Date.parse(end.replace(" ", "T") + "Z") / 1000;
    return { from: Number.isFinite(from) ? from : now - 3600, to: Number.isFinite(to) ? to : now };
  };

  const run = async () => {
    const { from, to } = window();
    if (!(to > from)) {
      setError("The end of the range must be after its start.");
      return;
    }
    setRunning(true);
    setError(null);
    try {
      const step = Math.max(1, Math.round((to - from) / POINTS));
      const next = await ipc("query_range", { connectionId, request: { query, start: from, end: to, stepSeconds: step } });
      setResult(next);
      setHidden(new Set());
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  const visible = useMemo(() => (result?.series ?? []).filter((s) => !hidden.has(s.name)), [result, hidden]);

  return (
    <ToolShell tool="metrics_explorer" right={result ? <span className="font-mono text-[10px] text-muted">{result.series.length} series</span> : null}>
      <ToolBody
        form={
          <>
            <div className="flex flex-col gap-1">
              <span className="text-sm font-medium text-foreground">{language} expression</span>
              <TextArea
                aria-label="Query"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                spellCheck={false}
                className="min-h-28 font-mono text-[12px]"
                onKeyDown={(e) => {
                  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
                    e.preventDefault();
                    void run();
                  }
                }}
              />
            </div>
            <div className="flex flex-col gap-1">
              <span className="text-sm font-medium text-foreground">Range</span>
              <Segmented label="Time range" value={preset} onChange={setPreset} options={PRESETS.map((p) => ({ value: p.value, label: p.label }))} />
            </div>
            {preset === "custom" ? (
              <>
                <DateTimeField kind="datetime" label="From (UTC)" value={start} onChange={setStart} />
                <DateTimeField kind="datetime" label="To (UTC)" value={end} onChange={setEnd} />
              </>
            ) : null}
            <Button onPress={() => void run()} isDisabled={running || query.trim().length === 0}>
              {running ? <Spinner size="sm" /> : <Icon name="play" size={13} />}
              Run
            </Button>
            {error !== null ? <p className="text-xs text-danger">{error}</p> : null}
            {result?.warnings.map((w) => (
              <p key={w} className="text-xs text-warning">
                {w}
              </p>
            ))}
          </>
        }
      >
        {result === null ? (
          <EmptyState icon="chart" title="Metrics explorer" body="Write an expression, pick a window and run it (⌘↩). Each series becomes a line; click legend entries to hide them." />
        ) : result.series.length === 0 ? (
          <EmptyState title="No series" body="The expression returned no samples in this window." />
        ) : (
          <div className="flex h-full min-h-0 flex-col">
            <div className="min-h-0 flex-1 p-3">
              <SeriesChart series={visible} />
            </div>
            <ul className="flex max-h-40 shrink-0 flex-wrap gap-x-4 gap-y-1 overflow-y-auto border-t border-border/40 px-3 py-2 text-[11px]">
              {result.series.map((s, i) => (
                <li key={s.name}>
                  <Button
                    variant="ghost"
                    size="sm"
                    onPress={() =>
                      setHidden((prev) => {
                        const next = new Set(prev);
                        if (next.has(s.name)) next.delete(s.name);
                        else next.add(s.name);
                        return next;
                      })
                    }
                    className={cn("h-6 min-w-0 gap-1.5 rounded-md px-1.5 font-mono text-[11px]", hidden.has(s.name) ? "text-muted/50 line-through" : "text-foreground")}
                  >
                    <span className="inline-block size-2 rounded-full" style={{ background: SERIES_COLORS[i % SERIES_COLORS.length] }} />
                    {s.name}
                    <span className="text-muted">{s.points.length > 0 ? formatValue(s.points[s.points.length - 1]?.[1] ?? 0) : ""}</span>
                  </Button>
                </li>
              ))}
            </ul>
          </div>
        )}
      </ToolBody>
    </ToolShell>
  );
}

function formatValue(v: number): string {
  if (!Number.isFinite(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1e9) return `${(v / 1e9).toFixed(2)}G`;
  if (abs >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(v / 1e3).toFixed(2)}k`;
  if (abs >= 100) return v.toFixed(0);
  if (abs >= 1) return v.toFixed(2);
  return v.toPrecision(3);
}

function formatTime(seconds: number, spanSeconds: number): string {
  const d = new Date(seconds * 1000);
  if (spanSeconds > 2 * 24 * 3600) return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}${spanSeconds < 900 ? `:${String(d.getSeconds()).padStart(2, "0")}` : ""}`;
}

// WHAT:  Multi-series line chart: responsive SVG with axes, grid, one path
//        per series in the shared series palette and a hover readout.
export function SeriesChart({ series }: { series: readonly Series[] }) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(600);
  const [height, setHeight] = useState(300);
  const [hover, setHover] = useState<number | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (rect) {
        setWidth(Math.max(200, rect.width));
        setHeight(Math.max(160, rect.height));
      }
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const pad = { l: 56, r: 12, t: 10, b: 24 };
  const points = series.flatMap((s) => s.points);
  const xs = points.map((p) => p[0]);
  const ys = points.map((p) => p[1]).filter((v) => Number.isFinite(v));
  const x0 = xs.length > 0 ? Math.min(...xs) : 0;
  const x1 = xs.length > 0 ? Math.max(...xs) : 1;
  const yMin0 = ys.length > 0 ? Math.min(...ys) : 0;
  const yMax0 = ys.length > 0 ? Math.max(...ys) : 1;
  const yMin = yMin0 === yMax0 ? yMin0 - 1 : Math.min(0, yMin0) === 0 && yMin0 >= 0 ? 0 : yMin0;
  const yMax = yMin0 === yMax0 ? yMax0 + 1 : yMax0;
  const sx = (x: number) => pad.l + ((x - x0) / (x1 - x0 || 1)) * (width - pad.l - pad.r);
  const sy = (y: number) => pad.t + (1 - (y - yMin) / (yMax - yMin || 1)) * (height - pad.t - pad.b);
  const yTicks = Array.from({ length: 5 }, (_, i) => yMin + ((yMax - yMin) * i) / 4);
  const xTicks = Array.from({ length: 6 }, (_, i) => x0 + ((x1 - x0) * i) / 5);
  const hoverX = hover === null ? null : x0 + ((hover - pad.l) / (width - pad.l - pad.r)) * (x1 - x0);
  const readout = hoverX === null ? [] : series.map((s) => {
    let best = s.points[0];
    for (const p of s.points) if (best === undefined || Math.abs(p[0] - hoverX) < Math.abs(best[0] - hoverX)) best = p;
    return { name: s.name, point: best };
  });

  return (
    <div ref={ref} className="relative h-full w-full">
      <svg width={width} height={height} className="text-muted" onMouseMove={(e) => setHover(e.nativeEvent.offsetX)} onMouseLeave={() => setHover(null)}>
        {yTicks.map((t) => (
          <g key={t}>
            <line x1={pad.l} x2={width - pad.r} y1={sy(t)} y2={sy(t)} stroke="currentColor" strokeOpacity="0.15" />
            <text x={pad.l - 6} y={sy(t) + 3} textAnchor="end" fontSize="10" fill="currentColor" className="font-mono">
              {formatValue(t)}
            </text>
          </g>
        ))}
        {xTicks.map((t) => (
          <text key={t} x={sx(t)} y={height - 6} textAnchor="middle" fontSize="10" fill="currentColor" className="font-mono">
            {formatTime(t, x1 - x0)}
          </text>
        ))}
        {series.map((s, i) => (
          <path key={s.name} d={s.points.filter((p) => Number.isFinite(p[1])).map((p, j) => `${j === 0 ? "M" : "L"}${sx(p[0]).toFixed(1)},${sy(p[1]).toFixed(1)}`).join(" ")} fill="none" stroke={SERIES_COLORS[i % SERIES_COLORS.length]} strokeWidth="1.5" strokeLinejoin="round" />
        ))}
        {hover !== null && hover >= pad.l && hover <= width - pad.r ? <line x1={hover} x2={hover} y1={pad.t} y2={height - pad.b} stroke="currentColor" strokeOpacity="0.4" strokeDasharray="3 3" /> : null}
      </svg>
      {hoverX !== null && readout.length > 0 ? (
        <div className="pointer-events-none absolute top-2 right-3 max-w-xs rounded-lg glass-card border-border/40 p-2 font-mono text-[10px] text-foreground">
          <div className="mb-1 text-muted">{readout[0]?.point ? new Date(readout[0].point[0] * 1000).toLocaleString() : ""}</div>
          {readout.slice(0, 12).map((r, i) => (
            <div key={r.name} className="flex items-center gap-1.5">
              <span className="inline-block size-1.5 shrink-0 rounded-full" style={{ background: SERIES_COLORS[i % SERIES_COLORS.length] }} />
              <span className="truncate text-muted">{r.name}</span>
              <span className="ml-auto pl-2">{r.point ? formatValue(r.point[1]) : "—"}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}
