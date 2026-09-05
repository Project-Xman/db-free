// SOT: admin-tab, server-overview, server-stats-view, stat-sparkline, admin-object-lists, auto-refresh
import { useEffect, useMemo, useState } from "react";
import { Button, Chip, ScrollShadow, SearchField, Spinner } from "@heroui/react";
import type { ObjectKind, ServerStats, Stat } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { ADMIN_KINDS, kindMeta, hasTool } from "@/lib/objects";
import { Icon } from "@/lib/icons";
import { engineMeta } from "@/lib/engines";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Segmented } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { ObjectRow, useObjects } from "@/features/objects/ObjectList";
import { cn } from "@/lib/cn";

type Interval = "off" | "5" | "15" | "60";
type View = "overview" | ObjectKind;
const INTERVALS: readonly { value: Interval; label: string }[] = [
  { value: "off", label: "Manual" },
  { value: "5", label: "5s" },
  { value: "15", label: "15s" },
  { value: "60", label: "60s" },
];
const HISTORY = 40;

// WHAT:  Server administration for one connection: an Overview of stats
//        groups (auto-refreshing, with sparklines) and one list per admin
//        kind the family declares (sessions, locks, users, roles, settings,
//        replication, nodes, slow queries…). Rows open the object tab, where
//        actions such as Kill or Drop live.
// WHERE: src/lib/objects.ts (ADMIN_KINDS), src-tauri/src/integrations/mod.rs (server_stats)
export function AdminTab({ connectionId }: { connectionId: string }) {
  const connection = useWorkspace((s) => s.connections.find((c) => c.id === connectionId));
  const info = useWorkspace((s) => s.sessionInfos[connectionId]);
  const engine = connection?.engine ?? "postgres";
  const kinds: ObjectKind[] = useMemo(() => ADMIN_KINDS.filter((k) => (info?.objectKinds ?? []).includes(k)), [info]);
  const hasStats = info ? info.tools.includes("stats") : hasTool(engine, "stats");
  const views = useMemo<{ value: View; label: string }[]>(() => [...(hasStats ? [{ value: "overview" as const, label: "Overview" }] : []), ...kinds.map((k) => ({ value: k, label: kindMeta(k).plural }))], [hasStats, kinds]);
  const [view, setView] = useState<View>(views[0]?.value ?? "overview");
  const current: View = views.some((v) => v.value === view) ? view : (views[0]?.value ?? "overview");

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex app-toolbar shrink-0 items-center gap-2 border-b border-border/40 glass-header ">
        <Icon name="server" size={15} className="text-accent" />
        <span className="text-sm font-semibold tracking-tight text-foreground">Server</span>
        <Chip size="sm" variant="soft" className="font-mono text-[10px]">
          {engineMeta(engine).label}
        </Chip>
        {info?.serverVersion ? <span className="truncate font-mono text-[10px] text-muted">{info.serverVersion}</span> : null}
      </div>
      {views.length > 1 ? (
        <ScrollShadow orientation="horizontal" hideScrollBar className="shrink-0 px-3 py-2">
          <Segmented label="Admin view" value={current} onChange={setView} options={views} />
        </ScrollShadow>
      ) : null}
      <div className="min-h-0 flex-1">
        {views.length === 0 ? (
          <EmptyState icon="server" title="Nothing to administer" body="This engine's adapter exposes no server state." />
        ) : current === "overview" ? (
          <Overview connectionId={connectionId} />
        ) : (
          <KindList key={current} connectionId={connectionId} kind={current} />
        )}
      </div>
    </div>
  );
}

function Overview({ connectionId }: { connectionId: string }) {
  const [stats, setStats] = useState<ServerStats | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [interval, setIntervalMode] = useState<Interval>("15");
  // Numeric history per stat (last HISTORY refreshes), for the sparklines.
  const [history, setHistory] = useState<Map<string, number[]>>(() => new Map());

  // `tick` is the reload trigger: the timer and the Refresh button bump it,
  // the effect below fetches once per value and drops stale answers.
  const [tick, setTick] = useState(0);
  const refresh = () => {
    setLoading(true);
    setTick((t) => t + 1);
  };

  useEffect(() => {
    const token = { cancelled: false };
    void (async () => {
      try {
        const next = await ipc("server_stats", { connectionId });
        if (token.cancelled) return;
        setHistory((prev) => {
          const map = new Map(prev);
          for (const group of next.groups) {
            for (const stat of group.stats) {
              if (stat.numeric === null) continue;
              const key = `${group.title}/${stat.label}`;
              map.set(key, [...(map.get(key) ?? []), stat.numeric].slice(-HISTORY));
            }
          }
          return map;
        });
        setStats(next);
        setError(null);
      } catch (raw) {
        if (!token.cancelled) setError(normalizeError(raw).message);
      } finally {
        if (!token.cancelled) setLoading(false);
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connectionId, tick]);

  useEffect(() => {
    if (interval === "off") return;
    const handle = window.setInterval(() => setTick((t) => t + 1), Number(interval) * 1000);
    return () => window.clearInterval(handle);
  }, [interval]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex shrink-0 items-center gap-2 px-3 pb-1 text-xs text-muted">
        <Segmented label="Refresh interval" value={interval} onChange={setIntervalMode} options={INTERVALS} />
        <IconButton icon="refresh" label="Refresh now" onPress={refresh} />
        {loading ? <Spinner size="sm" /> : null}
        {stats ? <span className="ml-auto font-mono text-[10px]">read {new Date(stats.collectedAt).toLocaleTimeString()}</span> : null}
      </div>
      <ScrollShadow className="min-h-0 flex-1 p-3">
        {error !== null ? (
          <EmptyState icon="alert" title="Could not read server statistics" body={error} action={<Button size="sm" onPress={refresh}>Retry</Button>} />
        ) : stats === null ? null : stats.groups.length === 0 ? (
          <EmptyState icon="activity" title="No statistics" body="The adapter returned no figures for this server." />
        ) : (
          <div className="grid grid-cols-1 gap-3 lg:grid-cols-2 2xl:grid-cols-3">
            {stats.groups.map((group) => (
              <section key={group.title} className="rounded-xl glass-card border-border/40 p-3">
                <h3 className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-muted">{group.title}</h3>
                <ul className="grid grid-cols-2 gap-x-3 gap-y-2">
                  {group.stats.map((stat) => (
                    <StatTile key={stat.label} stat={stat} series={history.get(`${group.title}/${stat.label}`) ?? []} />
                  ))}
                </ul>
              </section>
            ))}
          </div>
        )}
      </ScrollShadow>
    </div>
  );
}

function StatTile({ stat, series }: { stat: Stat; series: number[] }) {
  return (
    <li className="min-w-0" title={stat.hint ?? undefined}>
      <div className="truncate text-[11px] text-muted">{stat.label}</div>
      <div className="flex items-end gap-2">
        <span className="truncate font-mono text-[15px] font-semibold tabular-nums text-foreground">
          {stat.value}
          {stat.unit ? <span className="ml-0.5 text-[10px] font-normal text-muted">{stat.unit}</span> : null}
        </span>
        {series.length > 1 ? <Sparkline values={series} /> : null}
      </div>
    </li>
  );
}

// WHAT:  Tiny inline trend of the last refreshes; the stroke uses the accent token.
function Sparkline({ values }: { values: number[] }) {
  const w = 56;
  const h = 18;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const points = values.map((v, i) => `${((i / (values.length - 1)) * (w - 2) + 1).toFixed(1)},${(h - 1 - ((v - min) / span) * (h - 2)).toFixed(1)}`).join(" ");
  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} className="mb-0.5 shrink-0 text-accent" aria-hidden="true">
      <polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.25" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

function KindList({ connectionId, kind }: { connectionId: string; kind: ObjectKind }) {
  const [search, setSearch] = useState("");
  const [refreshKey, setRefreshKey] = useState(0);
  const invalidateObjects = useWorkspace((s) => s.invalidateObjects);
  const { objects, error, loading } = useObjects(connectionId, kind, null, true, refreshKey);
  const meta = kindMeta(kind);
  const needle = search.trim().toLowerCase();
  const visible = objects?.filter((o) => needle.length === 0 || o.reference.name.toLowerCase().includes(needle) || (o.detail ?? "").toLowerCase().includes(needle)) ?? [];
  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex shrink-0 items-center gap-2 px-3 pb-2">
        <SearchField value={search} onChange={setSearch} aria-label={`Filter ${meta.plural.toLowerCase()}`} className="max-w-xs">
          <SearchField.Group className="glass-input h-8 rounded-lg px-2">
            <SearchField.SearchIcon />
            <SearchField.Input placeholder={`Filter ${meta.plural.toLowerCase()}…`} className="w-full text-xs" />
            <SearchField.ClearButton />
          </SearchField.Group>
        </SearchField>
        <IconButton
          icon="refresh"
          label="Reload"
          onPress={() => {
            invalidateObjects(connectionId);
            setRefreshKey((k) => k + 1);
          }}
        />
        {loading ? <Spinner size="sm" /> : null}
        {objects ? (
          <Chip size="sm" variant="soft" className="ml-auto font-mono text-[10px]">
            {objects.length}
          </Chip>
        ) : null}
      </div>
      <ScrollShadow className={cn("min-h-0 flex-1 px-2 pb-2")}>
        {error !== null ? (
          <EmptyState icon="alert" title={`Could not list ${meta.plural.toLowerCase()}`} body={error} />
        ) : objects !== null && visible.length === 0 ? (
          <EmptyState icon={meta.icon} title={`No ${meta.plural.toLowerCase()}`} {...(needle.length > 0 ? { body: "Nothing matches the filter." } : {})} />
        ) : (
          visible.map((o) => <ObjectRow key={`${o.reference.parent ?? ""}:${o.reference.name}`} connectionId={connectionId} object={o} />)
        )}
      </ScrollShadow>
    </div>
  );
}
