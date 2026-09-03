// SOT: erd-tab, auto-er-diagram, foreign-key-graph, diagram-export
import { useCallback, useEffect, useMemo, useState } from "react";
import { Background, Controls, MiniMap, ReactFlow, type Edge, type Node, useEdgesState, useNodesState, MarkerType } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { Button, Spinner } from "@heroui/react";
import type { ColumnInfo, ForeignKey, TableRef } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { tableKey, useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { EmptyState } from "@/components/global/EmptyState";
import { Icon, typeIcon } from "@/lib/icons";
import { TableNode, VERTICAL_SOURCE_HANDLE, VERTICAL_TARGET_HANDLE, type TableNodeData } from "./TableNode";

const NODE_TYPES = { table: TableNode };

// WHAT:  ER diagram derived from information_schema foreign keys (PRD §4.4).
// HOW:   Loads columns for every table in the chosen schema, lays nodes out in
//        layers (top→bottom by FK direction), renders with React Flow; the
//        toolbar re-lays out, refreshes, and exports the diagram as SVG.
// WHERE: src-tauri/src/integrations/*.rs (foreign_keys), src/features/diagrams/TableNode.tsx
export function ErdTab({ connectionId, schema }: { connectionId: string; schema: string | null }) {
  const catalog = useWorkspace((s) => s.catalogs[connectionId]);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<TableNodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState(new Array<Edge>());
  const [loading, setLoading] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [fkCount, setFkCount] = useState(0);

  const tables = useMemo<TableRef[]>(
    () => (catalog?.schemas ?? []).filter((s) => schema === null || s.name === schema).flatMap((s) => s.tables.filter((t) => t.kind === "table").map((t) => ({ schema: t.schema, name: t.name }))),
    [catalog, schema],
  );

  useEffect(() => {
    const token = { cancelled: false };
    void (async () => {
      setLoading(true);
      try {
        const fks = await ipc("load_foreign_keys", { connectionId });
        const limited = tables.slice(0, 120);
        const columns = await Promise.all(limited.map((t) => ipc("load_columns", { connectionId, table: t }).catch((): ColumnInfo[] => [])));
        if (token.cancelled) return;
        const built = buildGraph(limited, columns, fks);
        setNodes(built.nodes);
        setEdges(built.edges);
        setFkCount(built.edges.length);
      } catch (raw) {
        if (!token.cancelled) showError(normalizeError(raw));
      } finally {
        if (!token.cancelled) setLoading(false);
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connectionId, tables, refresh, setNodes, setEdges, showError]);

  const relayout = useCallback(() => {
    setNodes((current) => layout(current, edges));
  }, [edges, setNodes]);

  const exportSvg = async () => {
    const svg = document.querySelector<SVGSVGElement>(".react-flow__edges");
    const viewport = document.querySelector<HTMLElement>(".react-flow__viewport");
    if (!viewport) return;
    const html = viewport.outerHTML;
    const doc = `<svg xmlns="http://www.w3.org/2000/svg" xmlns:xhtml="http://www.w3.org/1999/xhtml" width="2000" height="1400"><style>.react-flow__node{font-family:Inter,sans-serif;font-size:12px;color:#e8e8e8}</style><foreignObject width="2000" height="1400"><xhtml:div>${html}</xhtml:div></foreignObject>${svg ? svg.innerHTML : ""}</svg>`;
    await navigator.clipboard.writeText(doc);
    showInfo("Diagram SVG copied to the clipboard.");
  };

  if (tables.length === 0) {
    return <EmptyState icon="view" title="No tables to diagram" body="Connect to a database with tables, or pick another schema in the sidebar." />;
  }

  return (
    <div className="relative h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={NODE_TYPES}
        fitView
        minZoom={0.05}
        colorMode="dark"
        proOptions={{ hideAttribution: true }}
      >
        <Background gap={24} size={1} />
        <Controls showInteractive={false} />
        <MiniMap pannable zoomable className="!bg-surface" nodeColor="var(--accent)" maskColor="rgba(0,0,0,0.6)" />
      </ReactFlow>
      <div className="absolute top-3 right-3 flex items-center gap-1 rounded-lg border border-border bg-surface/90 p-1 backdrop-blur">
        <IconButton icon="refresh" label="Reload from database" onPress={() => setRefresh((r) => r + 1)} />
        <IconButton icon="sort" label="Auto layout" onPress={relayout} />
        <IconButton icon="download" label="Copy as SVG" onPress={() => void exportSvg()} />
      </div>
      <div className="absolute bottom-3 left-14 flex items-center gap-2 rounded-md border border-border bg-surface/90 px-2 py-1 text-[11px] text-muted backdrop-blur">
        {loading ? <Spinner size="sm" /> : <Icon name="view" size={12} />}
        {tables.length} tables · {fkCount} relations{tables.length > 120 ? " · showing first 120" : ""}
      </div>
      {tables.length > 0 && nodes.length === 0 && !loading ? (
        <div className="absolute inset-0 flex items-center justify-center">
          <Button size="sm" variant="secondary" onPress={() => setRefresh((r) => r + 1)}>Reload</Button>
        </div>
      ) : null}
    </div>
  );
}

function buildGraph(tables: TableRef[], columns: ColumnInfo[][], fks: ForeignKey[]): { nodes: Node<TableNodeData>[]; edges: Edge[] } {
  const nodes: Node<TableNodeData>[] = tables.map((t, i) => ({
    id: tableKey(t),
    type: "table",
    position: { x: 0, y: 0 },
    data: { title: tableKey(t), vertical: true, columns: (columns[i] ?? []).map((c) => ({ name: c.name, type: c.dataType, pk: c.primaryKey, icon: typeIcon(c.dataType, c.primaryKey) })) },
  }));
  const ids = new Set(nodes.map((n) => n.id));
  const edges: Edge[] = fks
    .map((fk) => {
      const source = tableKey({ schema: fk.fromSchema, name: fk.fromTable });
      const target = tableKey({ schema: fk.toSchema, name: fk.toTable });
      return { fk, source, target };
    })
    .filter(({ source, target }) => ids.has(source) && ids.has(target))
    .map(({ fk, source, target }) => ({
      id: fk.name,
      source,
      target,
      sourceHandle: VERTICAL_SOURCE_HANDLE,
      targetHandle: VERTICAL_TARGET_HANDLE,
      label: `${fk.fromColumns.join(",")} → ${fk.toColumns.join(",")}`,
      type: "smoothstep",
      markerEnd: { type: MarkerType.ArrowClosed },
      style: { stroke: "var(--accent)", strokeWidth: 1.5 },
      labelStyle: { fill: "var(--muted)", fontSize: 10 },
      labelBgStyle: { fill: "var(--surface)" },
    }));
  return { nodes: layout(nodes, edges), edges };
}

// WHAT:  Layered top→bottom layout: a table's rank is the longest FK path that
//        reaches it, so referenced tables sit below the tables that reference
//        them; nodes sit side by side within a rank, each row as tall as its
//        tallest card.
const NODE_WIDTH = 240;
const COLUMN_GAP = 60;
const ROW_GAP = 90;

function layout(nodes: Node<TableNodeData>[], edges: Edge[]): Node<TableNodeData>[] {
  const ids = nodes.map((n) => n.id);
  const outgoing = new Map<string, string[]>(ids.map((id) => [id, []]));
  const indegree = new Map<string, number>(ids.map((id) => [id, 0]));
  for (const e of edges) {
    if (e.source === e.target) continue;
    outgoing.get(e.source)?.push(e.target);
    indegree.set(e.target, (indegree.get(e.target) ?? 0) + 1);
  }
  const rank = new Map<string, number>(ids.map((id) => [id, 0]));
  const queue = ids.filter((id) => (indegree.get(id) ?? 0) === 0);
  const remaining = new Map(indegree);
  while (queue.length > 0) {
    const id = queue.shift();
    if (id === undefined) break;
    for (const next of outgoing.get(id) ?? []) {
      rank.set(next, Math.max(rank.get(next) ?? 0, (rank.get(id) ?? 0) + 1));
      remaining.set(next, (remaining.get(next) ?? 1) - 1);
      if ((remaining.get(next) ?? 0) === 0) queue.push(next);
    }
  }
  const height = (n: Node<TableNodeData>) => 36 + Math.min(n.data.columns.length, 14) * 22;
  const rows = new Map<number, Node<TableNodeData>[]>();
  for (const n of nodes) {
    const r = rank.get(n.id) ?? 0;
    rows.set(r, [...(rows.get(r) ?? []), n]);
  }
  const widest = Math.max(0, ...[...rows.values()].map((list) => list.length)) * (NODE_WIDTH + COLUMN_GAP) - COLUMN_GAP;
  const positioned = new Map<string, { x: number; y: number }>();
  let y = 0;
  for (const r of [...rows.keys()].sort((a, b) => a - b)) {
    const list = rows.get(r) ?? [];
    const rowWidth = list.length * (NODE_WIDTH + COLUMN_GAP) - COLUMN_GAP;
    let x = (widest - rowWidth) / 2;
    for (const n of list) {
      positioned.set(n.id, { x, y });
      x += NODE_WIDTH + COLUMN_GAP;
    }
    y += Math.max(0, ...list.map(height)) + ROW_GAP;
  }
  return nodes.map((n) => ({ ...n, position: positioned.get(n.id) ?? { x: 0, y: 0 } }));
}
