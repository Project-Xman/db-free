// SOT: graph-view-tab, graph-result-extraction, force-layout, graph-canvas
import { useEffect, useMemo, useRef, useState } from "react";
import { Button, Chip, ScrollShadow, Spinner, TextArea } from "@heroui/react";
import type { QueryOutcome, Value } from "@/lib/bindings";
import type { JsonValue } from "@/lib/bindings/serde_json/JsonValue";
import { ipc, normalizeError } from "@/lib/ipc";
import { engineMeta } from "@/lib/engines";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { EmptyState } from "@/components/global/EmptyState";
import { JsonViewer } from "@/components/global/JsonViewer";
import { SERIES_COLORS } from "@/features/dashboards/charts";
import { ToolShell } from "./ToolShell";
import { cn } from "@/lib/cn";

export interface GraphNode {
  id: string;
  label: string;
  caption: string;
  properties: JsonValue;
}

export interface GraphEdge {
  id: string;
  from: string;
  to: string;
  type: string;
  properties: JsonValue;
}

export interface Graph {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

const MAX_NODES = 400;
const CAPTION_KEYS = ["name", "title", "label", "username", "email", "key", "_key", "id"];

function isObject(v: JsonValue | undefined): v is Record<string, JsonValue> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function text(v: JsonValue | undefined): string {
  return typeof v === "string" ? v : typeof v === "number" || typeof v === "boolean" ? String(v) : "";
}

function caption(props: JsonValue, fallback: string): string {
  if (isObject(props)) {
    for (const key of CAPTION_KEYS) {
      const t = text(props[key]);
      if (t.length > 0) return t.length > 24 ? `${t.slice(0, 22)}…` : t;
    }
  }
  return fallback.length > 24 ? `${fallback.slice(0, 22)}…` : fallback;
}

// WHAT:  Finds nodes and relationships in query output, whatever the engine's
//        JSON shape: Bolt nodes/relationships/paths (Neo4j family), `_from`/
//        `_to` edge documents (ArangoDB), `@rid`/`in`/`out` (OrientDB), and
//        s/p/o triples (SPARQL).
export function extractGraph(outcome: QueryOutcome): Graph {
  const nodes = new Map<string, GraphNode>();
  const edges = new Map<string, GraphEdge>();
  const addNode = (id: string, label: string, props: JsonValue) => {
    if (!nodes.has(id) && nodes.size < MAX_NODES) nodes.set(id, { id, label, caption: caption(props, id), properties: props });
  };
  const addEdge = (id: string, from: string, to: string, type: string, props: JsonValue) => {
    if (!edges.has(id)) edges.set(id, { id, from, to, type, properties: props });
  };
  const visit = (v: JsonValue) => {
    if (Array.isArray(v)) {
      for (const item of v) visit(item);
      return;
    }
    if (!isObject(v)) return;
    if (Array.isArray(v.labels) && isObject(v.properties) && v.id !== undefined) {
      addNode(text(v.id), text(v.labels[0]) || "Node", v.properties);
      return;
    }
    if (typeof v.type === "string" && v.start !== undefined && v.end !== undefined) {
      const from = text(v.start);
      const to = text(v.end);
      addEdge(text(v.id) || `${from}-${v.type}-${to}`, from, to, v.type, v.properties ?? {});
      return;
    }
    if (Array.isArray(v.nodes) && Array.isArray(v.relationships)) {
      visit(v.nodes);
      visit(v.relationships);
      return;
    }
    if (typeof v._id === "string" && typeof v._from === "string" && typeof v._to === "string") {
      addNode(v._from, v._from.split("/")[0] ?? "Vertex", {});
      addNode(v._to, v._to.split("/")[0] ?? "Vertex", {});
      addEdge(v._id, v._from, v._to, v._id.split("/")[0] ?? "edge", v);
      return;
    }
    if (typeof v._id === "string" && typeof v._key === "string") {
      addNode(v._id, v._id.split("/")[0] ?? "Vertex", v);
      return;
    }
    // SurrealDB: a record id is "table:key"; a RELATE edge carries in / out.
    if (typeof v.id === "string" && v.id.includes(":") && !Array.isArray(v.labels)) {
      const rid = v.id;
      const table = rid.split(":")[0] ?? "record";
      if (typeof v.in === "string" && typeof v.out === "string") {
        addNode(v.in, v.in.split(":")[0] ?? "record", {});
        addNode(v.out, v.out.split(":")[0] ?? "record", {});
        addEdge(rid, v.in, v.out, table, v);
      } else {
        addNode(rid, table, v);
      }
      return;
    }
    if (typeof v["@rid"] === "string") {
      const rid = v["@rid"];
      const cls = text(v["@class"]) || "V";
      if (typeof v.in === "string" && typeof v.out === "string") {
        addNode(v.out, "V", {});
        addNode(v.in, "V", {});
        addEdge(rid, v.out, v.in, cls, v);
      } else {
        addNode(rid, cls, v);
      }
      return;
    }
    for (const child of Object.values(v)) visit(child);
  };
  for (const statement of outcome.statements) {
    if (statement.kind !== "rows") continue;
    const names = statement.result.columns.map((c) => c.name.toLowerCase());
    const s = names.findIndex((n) => n === "s" || n === "subject");
    const p = names.findIndex((n) => n === "p" || n === "predicate");
    const o = names.findIndex((n) => n === "o" || n === "object");
    for (const row of statement.result.rows) {
      if (s >= 0 && p >= 0 && o >= 0) {
        const subject = cellText(row[s]);
        const predicate = cellText(row[p]);
        const object = cellText(row[o]);
        if (subject && predicate && object) {
          addNode(subject, "resource", {});
          addNode(object, object.startsWith("http") || object.startsWith("_:") ? "resource" : "literal", {});
          addEdge(`${subject}|${predicate}|${object}`, subject, object, predicate.split(/[#/]/).pop() ?? predicate, {});
          continue;
        }
      }
      for (const cell of row) if (cell.t === "json") visit(cell.v);
    }
  }
  const known = new Set(nodes.keys());
  return { nodes: [...nodes.values()], edges: [...edges.values()].filter((e) => known.has(e.from) && known.has(e.to)) };
}

function cellText(value: Value | undefined): string {
  if (!value) return "";
  return value.t === "text" || value.t === "date_time" ? value.v : value.t === "int" || value.t === "float" ? String(value.v) : "";
}

interface Placed {
  x: number;
  y: number;
}

// WHAT:  Fruchterman–Reingold style layout, a few hundred synchronous
//        iterations (the node cap keeps it under a frame budget).
export function layout(graph: Graph, width: number, height: number): Map<string, Placed> {
  const pos = new Map<string, Placed>();
  const n = graph.nodes.length;
  graph.nodes.forEach((node, i) => {
    const angle = (i / Math.max(1, n)) * Math.PI * 2;
    const radius = Math.min(width, height) * 0.35;
    pos.set(node.id, { x: width / 2 + Math.cos(angle) * radius, y: height / 2 + Math.sin(angle) * radius });
  });
  const k = Math.sqrt((width * height) / Math.max(1, n)) * 0.6;
  let temperature = width / 8;
  const steps = n > 150 ? 120 : 260;
  for (let step = 0; step < steps; step += 1) {
    const disp = new Map<string, Placed>();
    for (const node of graph.nodes) disp.set(node.id, { x: 0, y: 0 });
    for (let i = 0; i < n; i += 1) {
      const a = graph.nodes[i];
      if (!a) continue;
      const pa = pos.get(a.id);
      const da = disp.get(a.id);
      if (!pa || !da) continue;
      for (let j = i + 1; j < n; j += 1) {
        const b = graph.nodes[j];
        if (!b) continue;
        const pb = pos.get(b.id);
        const db = disp.get(b.id);
        if (!pb || !db) continue;
        let dx = pa.x - pb.x;
        let dy = pa.y - pb.y;
        let dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 0.01) {
          dx = Math.random() - 0.5;
          dy = Math.random() - 0.5;
          dist = 1;
        }
        const force = (k * k) / dist;
        da.x += (dx / dist) * force;
        da.y += (dy / dist) * force;
        db.x -= (dx / dist) * force;
        db.y -= (dy / dist) * force;
      }
    }
    for (const edge of graph.edges) {
      const pa = pos.get(edge.from);
      const pb = pos.get(edge.to);
      const da = disp.get(edge.from);
      const db = disp.get(edge.to);
      if (!pa || !pb || !da || !db) continue;
      const dx = pa.x - pb.x;
      const dy = pa.y - pb.y;
      const dist = Math.max(0.01, Math.sqrt(dx * dx + dy * dy));
      const force = (dist * dist) / k;
      da.x -= (dx / dist) * force;
      da.y -= (dy / dist) * force;
      db.x += (dx / dist) * force;
      db.y += (dy / dist) * force;
    }
    for (const node of graph.nodes) {
      const p = pos.get(node.id);
      const d = disp.get(node.id);
      if (!p || !d) continue;
      // gravity towards the centre keeps disconnected pieces on screen
      d.x += (width / 2 - p.x) * 0.02;
      d.y += (height / 2 - p.y) * 0.02;
      const len = Math.max(0.01, Math.sqrt(d.x * d.x + d.y * d.y));
      p.x += (d.x / len) * Math.min(len, temperature);
      p.y += (d.y / len) * Math.min(len, temperature);
    }
    temperature = Math.max(1.5, temperature * 0.95);
  }
  return pos;
}

function seedFor(language: string): string {
  switch (language) {
    case "Cypher":
      return "MATCH (n)-[r]->(m)\nRETURN n, r, m\nLIMIT 50";
    case "AQL":
      return "// Return edge documents; their _from / _to become nodes.\nFOR e IN edges\n  LIMIT 50\n  RETURN e";
    case "SPARQL":
      return "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 100";
    case "SurrealQL":
      return "-- Edges created with RELATE carry in / out.\nSELECT * FROM knows LIMIT 50;";
    case "GSQL":
      return "// Installed query returning vertices and edges\nSELECT * FROM Person LIMIT 50";
    default:
      return "SELECT FROM E LIMIT 50";
  }
}

// WHAT:  The query that pulls one node's neighbours, in each engine's language.
// WHY:   Expanding is how a graph is actually explored: you start from a small
//        seed and walk outwards, rather than writing one query that returns the
//        whole shape up front.
// HOW:   Node ids are the engine's own — Neo4j element ids, ArangoDB
//        `collection/key`, OrientDB `#12:0`, an RDF IRI — so each language can
//        address the clicked node directly.
export function expandFor(language: string, id: string): string {
  const escaped = id.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  switch (language) {
    case "Cypher":
      return `MATCH (n)-[r]-(m) WHERE elementId(n) = '${escaped}' RETURN n, r, m LIMIT 50`;
    case "AQL":
      return `FOR e IN edges FILTER e._from == '${escaped}' OR e._to == '${escaped}' LIMIT 50 RETURN e`;
    case "SPARQL":
      return `SELECT ?s ?p ?o WHERE { { <${id}> ?p ?o . BIND(<${id}> AS ?s) } UNION { ?s ?p <${id}> . BIND(<${id}> AS ?o) } } LIMIT 100`;
    case "SurrealQL":
      return `SELECT * FROM ${id} FETCH in, out;`;
    default:
      return `SELECT expand(bothE()) FROM ${id} LIMIT 50`;
  }
}

// WHAT:  Adds `incoming` to `base` without losing what is already on screen.
// WHY:   Expanding must not reset the canvas: nodes already placed keep their
//        identity so the layout stays recognisable between clicks.
export function mergeGraph(base: Graph, incoming: Graph): Graph {
  const nodes = new Map(base.nodes.map((n) => [n.id, n]));
  for (const n of incoming.nodes) {
    if (!nodes.has(n.id) && nodes.size < MAX_NODES) nodes.set(n.id, n);
  }
  const edges = new Map(base.edges.map((e) => [e.id, e]));
  for (const e of incoming.edges) if (!edges.has(e.id)) edges.set(e.id, e);
  const known = new Set(nodes.keys());
  return { nodes: [...nodes.values()], edges: [...edges.values()].filter((e) => known.has(e.from) && known.has(e.to)) };
}

// WHAT:  Drops the labels and relationship types the user switched off.
// WHY:   A dense graph is unreadable until the noisy types are hidden, so the
//        legend doubles as the filter rather than being a static key.
export function visibleGraph(graph: Graph, hiddenLabels: ReadonlySet<string>, hiddenTypes: ReadonlySet<string>): Graph {
  const nodes = graph.nodes.filter((n) => !hiddenLabels.has(n.label));
  const known = new Set(nodes.map((n) => n.id));
  return { nodes, edges: graph.edges.filter((e) => !hiddenTypes.has(e.type) && known.has(e.from) && known.has(e.to)) };
}

// WHAT:  Graph view: run a native query, draw what came back as nodes and
//        relationships on a pannable, zoomable canvas; click for properties,
//        double-click to expand a node, click the legend to filter.
// WHERE: src/features/tools/ToolTab.tsx, src-tauri/src/integrations/neo4j.rs (bolt_to_json)
export function GraphViewTab({ connectionId }: { connectionId: string }) {
  const engine = useWorkspace((s) => s.connections.find((c) => c.id === connectionId)?.engine ?? "neo4j");
  const language = engineMeta(engine).commandLanguage;
  const [query, setQuery] = useState(() => seedFor(language));
  const [graph, setGraph] = useState<Graph | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [selected, setSelected] = useState<{ kind: "node"; node: GraphNode } | { kind: "edge"; edge: GraphEdge } | null>(null);
  const [layoutKey, setLayoutKey] = useState(0);
  const [hiddenLabels, setHiddenLabels] = useState<ReadonlySet<string>>(() => new Set<string>());
  const [hiddenTypes, setHiddenTypes] = useState<ReadonlySet<string>>(() => new Set<string>());
  const [expanding, setExpanding] = useState(false);

  const shown = useMemo(() => (graph === null ? null : visibleGraph(graph, hiddenLabels, hiddenTypes)), [graph, hiddenLabels, hiddenTypes]);

  const run = async () => {
    setRunning(true);
    setError(null);
    try {
      const outcome = await ipc("execute_query", { connectionId, sql: query, confirmDestructive: false, maxRows: 2000 });
      const next = extractGraph(outcome);
      setGraph(next);
      setSelected(null);
      // A new result is a new shape: old filters would hide parts of it silently.
      setHiddenLabels(new Set<string>());
      setHiddenTypes(new Set<string>());
      setLayoutKey((k) => k + 1);
      if (next.nodes.length === 0) setError("The result carried no nodes or relationships. Return whole nodes / edges rather than scalar columns.");
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  // WHAT:  Pulls one node's neighbours and merges them in, keeping the layout.
  const expand = async (node: GraphNode) => {
    if (expanding) return;
    setExpanding(true);
    setError(null);
    try {
      const outcome = await ipc("execute_query", { connectionId, sql: expandFor(language, node.id), confirmDestructive: false, maxRows: 2000 });
      const added = extractGraph(outcome);
      setGraph((current) => (current === null ? added : mergeGraph(current, added)));
      if (added.nodes.length === 0) setError(`${node.caption} has no neighbours in reach of that query.`);
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setExpanding(false);
    }
  };

  const toggle = (set: ReadonlySet<string>, key: string): ReadonlySet<string> => {
    const next = new Set(set);
    if (!next.delete(key)) next.add(key);
    return next;
  };

  return (
    <ToolShell
      tool="graph_view"
      right={
        shown !== null && graph !== null ? (
          <span className="font-mono text-[10px] text-muted">
            {shown.nodes.length} nodes · {shown.edges.length} relationships
            {shown.nodes.length !== graph.nodes.length ? ` · ${graph.nodes.length - shown.nodes.length} hidden` : ""}
            {expanding ? " · expanding…" : ""}
          </span>
        ) : null
      }
    >
      <div className="flex h-full min-h-0 flex-col">
        <div className="flex shrink-0 items-start gap-2 border-b border-border/40 p-3">
          <TextArea
            aria-label={`${language} query`}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            spellCheck={false}
            className="min-h-20 flex-1 font-mono text-[12px]"
            onKeyDown={(e) => {
              if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
                e.preventDefault();
                void run();
              }
            }}
          />
          <div className="flex flex-col gap-1.5">
            <Button onPress={() => void run()} isDisabled={running || query.trim().length === 0}>
              {running ? <Spinner size="sm" /> : <Icon name="play" size={13} />}
              Run
            </Button>
            <Button variant="tertiary" size="sm" isDisabled={graph === null} onPress={() => setLayoutKey((k) => k + 1)}>
              <Icon name="refresh" size={12} />
              Re-layout
            </Button>
          </div>
        </div>
        {error !== null ? <p className="shrink-0 px-3 py-1 text-xs text-danger">{error}</p> : null}
        <div className="flex min-h-0 flex-1">
          <div className="min-h-0 min-w-0 flex-1">
            {shown === null || graph === null || graph.nodes.length === 0 ? (
              <EmptyState icon="chart-relationship" title="Graph view" body="Run a query that returns nodes and relationships (⌘↩). Drag to pan, scroll to zoom, click a node or relationship for its properties, double-click a node to expand it." />
            ) : (
              <GraphCanvas key={layoutKey} graph={shown} selected={selected} onSelect={setSelected} onExpand={(n) => void expand(n)} />
            )}
          </div>
          {graph !== null && graph.nodes.length > 0 ? (
            <aside className="flex w-72 shrink-0 flex-col border-l border-border/40">
              <Legend
                graph={graph}
                hiddenLabels={hiddenLabels}
                hiddenTypes={hiddenTypes}
                onToggleLabel={(l) => setHiddenLabels((set) => toggle(set, l))}
                onToggleType={(t) => setHiddenTypes((set) => toggle(set, t))}
              />
              <ScrollShadow className="min-h-0 flex-1 border-t border-border/40 p-3">
                {selected === null ? (
                  <p className="text-xs text-muted">Select a node or a relationship.</p>
                ) : (
                  <>
                    <div className="mb-2 flex items-center gap-2">
                      <Chip size="sm" variant="soft" className="font-mono text-[10px]">
                        {selected.kind === "node" ? selected.node.label : selected.edge.type}
                      </Chip>
                      <span className="truncate font-mono text-[11px] text-muted">{selected.kind === "node" ? selected.node.id : selected.edge.id}</span>
                    </div>
                    {selected.kind === "edge" ? (
                      <p className="mb-2 font-mono text-[11px] text-muted">
                        {selected.edge.from} → {selected.edge.to}
                      </p>
                    ) : null}
                    <JsonViewer bare value={selected.kind === "node" ? selected.node.properties : selected.edge.properties} defaultDepth={2} />
                  </>
                )}
              </ScrollShadow>
            </aside>
          ) : null}
        </div>
      </div>
    </ToolShell>
  );
}

function labelColors(graph: Graph): Map<string, string> {
  const map = new Map<string, string>();
  for (const node of graph.nodes) {
    if (!map.has(node.label)) map.set(node.label, SERIES_COLORS[map.size % SERIES_COLORS.length] ?? SERIES_COLORS[0]);
  }
  return map;
}

function Legend({
  graph,
  hiddenLabels,
  hiddenTypes,
  onToggleLabel,
  onToggleType,
}: {
  graph: Graph;
  hiddenLabels: ReadonlySet<string>;
  hiddenTypes: ReadonlySet<string>;
  onToggleLabel: (label: string) => void;
  onToggleType: (type: string) => void;
}) {
  const colors = labelColors(graph);
  const labelCounts = new Map<string, number>();
  for (const n of graph.nodes) labelCounts.set(n.label, (labelCounts.get(n.label) ?? 0) + 1);
  const typeCounts = new Map<string, number>();
  for (const e of graph.edges) typeCounts.set(e.type, (typeCounts.get(e.type) ?? 0) + 1);
  return (
    <ScrollShadow hideScrollBar className="max-h-56 p-3 text-[11px]">
      <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">Labels</div>
      <ul className="mb-2 flex flex-wrap gap-1">
        {[...labelCounts.entries()].map(([label, count]) => {
          const off = hiddenLabels.has(label);
          return (
            <li key={label}>
              <button
                type="button"
                aria-pressed={!off}
                title={off ? `Show ${label}` : `Hide ${label}`}
                onClick={() => onToggleLabel(label)}
                className={cn("flex items-center gap-1 rounded-md glass-pill px-1.5 py-0.5", off && "opacity-40")}
              >
                <span className="inline-block size-2 rounded-full" style={{ background: colors.get(label) }} />
                <span className={cn("text-foreground", off && "line-through")}>{label}</span>
                <span className="font-mono text-muted">{count}</span>
              </button>
            </li>
          );
        })}
      </ul>
      <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">Relationships</div>
      <ul className="flex flex-wrap gap-1">
        {[...typeCounts.entries()].map(([type, count]) => {
          const off = hiddenTypes.has(type);
          return (
            <li key={type}>
              <button
                type="button"
                aria-pressed={!off}
                title={off ? `Show ${type}` : `Hide ${type}`}
                onClick={() => onToggleType(type)}
                className={cn("flex items-center gap-1 rounded-md glass-pill px-1.5 py-0.5", off && "opacity-40")}
              >
                <span className={cn("text-foreground", off && "line-through")}>{type}</span>
                <span className="font-mono text-muted">{count}</span>
              </button>
            </li>
          );
        })}
      </ul>
    </ScrollShadow>
  );
}

function GraphCanvas({
  graph,
  selected,
  onSelect,
  onExpand,
}: {
  graph: Graph;
  selected: { kind: "node"; node: GraphNode } | { kind: "edge"; edge: GraphEdge } | null;
  onSelect: (s: { kind: "node"; node: GraphNode } | { kind: "edge"; edge: GraphEdge } | null) => void;
  onExpand: (node: GraphNode) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ width: 800, height: 500 });
  // Base positions come from the layout; drags override single nodes. Both
  // reset when the graph or the canvas size changes (memo keys), never in an effect.
  const base = useMemo(() => layout(graph, size.width, size.height), [graph, size.width, size.height]);
  const [overrides, setOverrides] = useState<{ base: Map<string, Placed>; moved: Map<string, Placed> }>({ base, moved: new Map() });
  const positions = useMemo(() => {
    const moved = overrides.base === base ? overrides.moved : null;
    if (moved === null || moved.size === 0) return base;
    const merged = new Map(base);
    for (const [id, p] of moved) merged.set(id, p);
    return merged;
  }, [base, overrides]);
  const [view, setView] = useState({ x: 0, y: 0, scale: 1 });
  const drag = useRef<{ kind: "pan"; startX: number; startY: number; originX: number; originY: number } | { kind: "node"; id: string } | null>(null);
  const colors = useMemo(() => labelColors(graph), [graph]);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (rect && rect.width > 0 && rect.height > 0) setSize({ width: rect.width, height: rect.height });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const toWorld = (clientX: number, clientY: number) => {
    const rect = ref.current?.getBoundingClientRect();
    const px = clientX - (rect?.left ?? 0);
    const py = clientY - (rect?.top ?? 0);
    return { x: (px - view.x) / view.scale, y: (py - view.y) / view.scale };
  };

  return (
    <div
      ref={ref}
      className="relative h-full w-full cursor-grab overflow-hidden select-none"
      onWheel={(e) => {
        const rect = ref.current?.getBoundingClientRect();
        const px = e.clientX - (rect?.left ?? 0);
        const py = e.clientY - (rect?.top ?? 0);
        const factor = e.deltaY < 0 ? 1.1 : 1 / 1.1;
        setView((v) => {
          const scale = Math.min(4, Math.max(0.2, v.scale * factor));
          return { scale, x: px - (px - v.x) * (scale / v.scale), y: py - (py - v.y) * (scale / v.scale) };
        });
      }}
      onMouseDown={(e) => {
        if (e.button !== 0) return;
        drag.current = { kind: "pan", startX: e.clientX, startY: e.clientY, originX: view.x, originY: view.y };
      }}
      onMouseMove={(e) => {
        const d = drag.current;
        if (!d) return;
        if (d.kind === "pan") {
          setView((v) => ({ ...v, x: d.originX + (e.clientX - d.startX), y: d.originY + (e.clientY - d.startY) }));
        } else {
          const w = toWorld(e.clientX, e.clientY);
          setOverrides((o) => {
            const next = new Map(o.base === base ? o.moved : new Map<string, Placed>());
            next.set(d.id, { x: w.x, y: w.y });
            return { base, moved: next };
          });
        }
      }}
      onMouseUp={() => {
        drag.current = null;
      }}
      onMouseLeave={() => {
        drag.current = null;
      }}
    >
      <svg width={size.width} height={size.height} className="text-muted">
        <defs>
          <marker id="graph-arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor" />
          </marker>
        </defs>
        <g transform={`translate(${view.x} ${view.y}) scale(${view.scale})`}>
          {graph.edges.map((edge) => {
            const a = positions.get(edge.from);
            const b = positions.get(edge.to);
            if (!a || !b) return null;
            const dx = b.x - a.x;
            const dy = b.y - a.y;
            const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
            const r = 15;
            const x1 = a.x + (dx / dist) * r;
            const y1 = a.y + (dy / dist) * r;
            const x2 = b.x - (dx / dist) * (r + 1);
            const y2 = b.y - (dy / dist) * (r + 1);
            const active = selected?.kind === "edge" && selected.edge.id === edge.id;
            return (
              <g
                key={edge.id}
                onMouseDown={(e) => {
                  e.stopPropagation();
                  onSelect({ kind: "edge", edge });
                }}
                className="cursor-pointer"
              >
                <line x1={x1} y1={y1} x2={x2} y2={y2} stroke="currentColor" strokeOpacity={active ? 1 : 0.45} strokeWidth={active ? 2 : 1.2} markerEnd="url(#graph-arrow)" />
                <text x={(x1 + x2) / 2} y={(y1 + y2) / 2 - 4} textAnchor="middle" fontSize="9" fill="currentColor" className="font-mono" opacity={0.8}>
                  {edge.type}
                </text>
              </g>
            );
          })}
          {graph.nodes.map((node) => {
            const p = positions.get(node.id);
            if (!p) return null;
            const active = selected?.kind === "node" && selected.node.id === node.id;
            return (
              <g
                key={node.id}
                transform={`translate(${p.x} ${p.y})`}
                className={cn("cursor-pointer", active ? "" : "")}
                onMouseDown={(e) => {
                  e.stopPropagation();
                  drag.current = { kind: "node", id: node.id };
                  onSelect({ kind: "node", node });
                }}
                onDoubleClick={(e) => {
                  e.stopPropagation();
                  onExpand(node);
                }}
              >
                <circle r={14} fill={colors.get(node.label)} fillOpacity={0.9} stroke={active ? "var(--foreground)" : "var(--background)"} strokeWidth={active ? 2.5 : 1.5} />
                <text y={26} textAnchor="middle" fontSize="10" fill="var(--foreground)" className="font-sans">
                  {node.caption}
                </text>
              </g>
            );
          })}
        </g>
      </svg>
      <div className="pointer-events-none absolute right-2 bottom-2 font-mono text-[10px] text-muted">{Math.round(view.scale * 100)}%</div>
    </div>
  );
}
