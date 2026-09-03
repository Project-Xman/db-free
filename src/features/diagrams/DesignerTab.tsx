// SOT: designer-tab, schema-diagram-designer, diagram-ddl-preview, diagram-versions
import { useCallback, useEffect, useMemo, useState } from "react";
import { Background, Controls, MiniMap, ReactFlow, addEdge, type Connection, type Edge, type Node, useEdgesState, useNodesState, MarkerType } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { Button, Input, Modal } from "@heroui/react";
import type { DiagramBody, DiagramColumn, DiagramTable, Document } from "@/lib/bindings";
import { normalizeError } from "@/lib/ipc";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Field, Toggle } from "@/components/global/Field";
import { Icon, typeIcon } from "@/lib/icons";
import { TableNode, type TableNodeData } from "./TableNode";

const NODE_TYPES = { table: TableNode };

let idCounter = 0;
function newId(prefix: string): string {
  idCounter += 1;
  return `${prefix}-${Date.now().toString(36)}-${idCounter}`;
}

// WHAT:  Free-form schema designer (DB Manager "Schema Diagrams"): add tables and
//        columns, drag to arrange, draw relations by connecting column handles,
//        preview the DDL, save the document.
// HOW:   The document body is the source of truth; React Flow state is derived
//        from it and written back on every change (positions included).
// WHERE: src-tauri/src/model/documents.rs (DiagramBody)
export function DesignerTab({ document: doc }: { document: Document }) {
  const saveDocument = useWorkspace((s) => s.saveDocument);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const body: DiagramBody = doc.body.kind === "diagram" ? doc.body.data : { tables: [], relations: [] };
  const [tables, setTables] = useState<DiagramTable[]>(body.tables);
  const [relations, setRelations] = useState(body.relations);
  const [name, setName] = useState(doc.name);
  const [dirty, setDirty] = useState(false);
  const [editing, setEditing] = useState<DiagramTable | null>(null);
  const [ddlOpen, setDdlOpen] = useState(false);
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<TableNodeData>>(toNodes(body.tables));
  const [edges, setEdges, onEdgesChange] = useEdgesState(toEdges(body.relations));

  useEffect(() => {
    setNodes(toNodes(tables));
  }, [tables, setNodes]);
  useEffect(() => {
    setEdges(toEdges(relations));
  }, [relations, setEdges]);

  const onConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target || !connection.sourceHandle || !connection.targetHandle) return;
      const relation = { id: newId("rel"), fromTable: connection.source, fromColumn: connection.sourceHandle, toTable: connection.target, toColumn: connection.targetHandle };
      setRelations((r) => [...r, relation]);
      setEdges((e) => addEdge({ ...connection, id: relation.id }, e));
      setDirty(true);
    },
    [setEdges],
  );

  const addTable = () => {
    const table: DiagramTable = { id: newId("tbl"), name: `table_${tables.length + 1}`, x: 80 + tables.length * 40, y: 80 + tables.length * 30, columns: [{ name: "id", dataType: "integer", primaryKey: true, nullable: false }] };
    setTables((t) => [...t, table]);
    setEditing(table);
    setDirty(true);
  };

  const removeSelected = () => {
    const ids = new Set(nodes.filter((n) => n.selected).map((n) => n.id));
    if (ids.size === 0) return;
    setTables((t) => t.filter((x) => !ids.has(x.id)));
    setRelations((r) => r.filter((x) => !ids.has(x.fromTable) && !ids.has(x.toTable)));
    setDirty(true);
  };

  const save = async () => {
    const positioned = tables.map((t) => {
      const node = nodes.find((n) => n.id === t.id);
      return node ? { ...t, x: node.position.x, y: node.position.y } : t;
    });
    try {
      await saveDocument({ ...doc, name, body: { kind: "diagram", data: { tables: positioned, relations } } });
      setTables(positioned);
      setDirty(false);
      showInfo("Diagram saved.");
    } catch (raw) {
      showError(normalizeError(raw));
    }
  };

  const ddl = useMemo(() => toDdl(tables, relations), [tables, relations]);

  return (
    <div className="relative flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-1 border-b border-border bg-surface px-2">
        <Input
          value={name}
          onChange={(e) => {
            setName(e.target.value);
            setDirty(true);
          }}
          className="h-7 w-48 rounded-md bg-transparent text-sm text-foreground"
          aria-label="Diagram name"
        />
        <IconButton icon="plus" label="Add table" onPress={addTable} />
        <IconButton icon="trash" label="Remove selected tables" onPress={removeSelected} />
        <IconButton icon="pencil" label="Edit selected table" onPress={() => { const sel = nodes.find((n) => n.selected); const t = sel ? tables.find((x) => x.id === sel.id) : undefined; if (t) setEditing(t); }} />
        <span className="mx-1 h-5 w-px bg-separator" />
        <span className="text-xs text-muted">{tables.length} tables · {relations.length} relations</span>
        <div className="ml-auto flex items-center gap-1">
          <Button size="sm" variant="ghost" className="text-muted" onPress={() => setDdlOpen(true)}>
            <Icon name="terminal" size={13} />
            DDL
          </Button>
          <Button size="sm" onPress={() => void save()} isDisabled={!dirty}>
            Save{dirty ? " *" : ""}
          </Button>
        </div>
      </div>
      <div className="min-h-0 flex-1">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={(changes) => { onNodesChange(changes); if (changes.some((c) => c.type === "position")) setDirty(true); }}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onNodeDoubleClick={(_, node) => { const t = tables.find((x) => x.id === node.id); if (t) setEditing(t); }}
          nodeTypes={NODE_TYPES}
          fitView
          colorMode="dark"
          proOptions={{ hideAttribution: true }}
        >
          <Background gap={24} size={1} />
          <Controls showInteractive={false} />
          <MiniMap pannable zoomable className="!bg-surface" nodeColor="var(--accent)" maskColor="rgba(0,0,0,0.6)" />
        </ReactFlow>
        {tables.length === 0 ? (
          <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
            <p className="rounded-md bg-surface/80 px-3 py-2 text-xs text-muted">Add a table to start. Drag from a column handle to another column to draw a relation.</p>
          </div>
        ) : null}
      </div>

      <TableEditor
        key={editing?.id ?? "none"}
        table={editing}
        onClose={() => setEditing(null)}
        onSave={(next) => {
          setTables((t) => t.map((x) => (x.id === next.id ? next : x)));
          setEditing(null);
          setDirty(true);
        }}
      />

      <Modal isOpen={ddlOpen} onOpenChange={setDdlOpen}>
        <Modal.Backdrop>
          <Modal.Container>
            <Modal.Dialog className="sm:max-w-[720px]">
              <Modal.CloseTrigger />
              <Modal.Header>
                <Modal.Heading>Generated DDL</Modal.Heading>
              </Modal.Header>
              <Modal.Body>
                <pre className="selectable max-h-[60vh] overflow-auto rounded-md bg-background p-3 font-mono text-[11px] whitespace-pre text-foreground">{ddl}</pre>
              </Modal.Body>
              <Modal.Footer>
                <Button variant="secondary" onPress={() => { void navigator.clipboard.writeText(ddl); showInfo("DDL copied."); }}>Copy</Button>
                <Button onPress={() => setDdlOpen(false)}>Close</Button>
              </Modal.Footer>
            </Modal.Dialog>
          </Modal.Container>
        </Modal.Backdrop>
      </Modal>
    </div>
  );
}

function TableEditor({ table, onClose, onSave }: { table: DiagramTable | null; onClose: () => void; onSave: (t: DiagramTable) => void }) {
  const [draft, setDraft] = useState<DiagramTable | null>(table);
  if (!draft) return null;
  const patchColumn = (i: number, partial: Partial<DiagramColumn>) => setDraft({ ...draft, columns: draft.columns.map((c, j) => (j === i ? { ...c, ...partial } : c)) });
  return (
    <Modal isOpen onOpenChange={(o) => !o && onClose()}>
      <Modal.Backdrop>
        <Modal.Container>
          <Modal.Dialog className="sm:max-w-[640px]">
            <Modal.CloseTrigger />
            <Modal.Header>
              <Modal.Heading>Edit table</Modal.Heading>
            </Modal.Header>
            <Modal.Body className="flex max-h-[60vh] flex-col gap-3 overflow-y-auto">
              <Field label="Table name" value={draft.name} onChange={(name) => setDraft({ ...draft, name })} mono autoFocus />
              <div className="flex flex-col gap-2">
                {draft.columns.map((c, i) => (
                  <div key={i} className="grid grid-cols-[1fr_1fr_auto_auto_28px] items-end gap-2">
                    <Field label={i === 0 ? "Column" : ""} value={c.name} onChange={(v) => patchColumn(i, { name: v })} mono className={i === 0 ? "" : "[&_label]:hidden"} />
                    <Field label={i === 0 ? "Type" : ""} value={c.dataType} onChange={(v) => patchColumn(i, { dataType: v })} mono className={i === 0 ? "" : "[&_label]:hidden"} />
                    <Toggle checked={c.primaryKey} onChange={(v) => patchColumn(i, { primaryKey: v, nullable: v ? false : c.nullable })} label="PK" />
                    <Toggle checked={c.nullable} onChange={(v) => patchColumn(i, { nullable: v })} label="Null" />
                    <IconButton icon="x" label="Remove column" onPress={() => setDraft({ ...draft, columns: draft.columns.filter((_, j) => j !== i) })} />
                  </div>
                ))}
                <Button size="sm" variant="ghost" className="self-start text-muted" onPress={() => setDraft({ ...draft, columns: [...draft.columns, { name: `column_${draft.columns.length + 1}`, dataType: "text", primaryKey: false, nullable: true }] })}>
                  <Icon name="plus" size={13} />
                  Add column
                </Button>
              </div>
            </Modal.Body>
            <Modal.Footer>
              <Button variant="tertiary" onPress={onClose}>Cancel</Button>
              <Button onPress={() => onSave(draft)} isDisabled={draft.name.trim().length === 0}>Apply</Button>
            </Modal.Footer>
          </Modal.Dialog>
        </Modal.Container>
      </Modal.Backdrop>
    </Modal>
  );
}

function toNodes(tables: readonly DiagramTable[]): Node<TableNodeData>[] {
  return tables.map((t) => ({
    id: t.id,
    type: "table",
    position: { x: t.x, y: t.y },
    data: { title: t.name, columns: t.columns.map((c) => ({ name: c.name, type: c.dataType, pk: c.primaryKey, icon: typeIcon(c.dataType, c.primaryKey) })) },
  }));
}

function toEdges(relations: readonly DiagramBody["relations"][number][]): Edge[] {
  return relations.map((r) => ({
    id: r.id,
    source: r.fromTable,
    target: r.toTable,
    sourceHandle: r.fromColumn,
    targetHandle: r.toColumn,
    type: "smoothstep",
    markerEnd: { type: MarkerType.ArrowClosed },
    style: { stroke: "var(--accent)", strokeWidth: 1.5 },
  }));
}

function toDdl(tables: readonly DiagramTable[], relations: readonly DiagramBody["relations"][number][]): string {
  const byId = new Map(tables.map((t) => [t.id, t]));
  const out: string[] = [];
  for (const t of tables) {
    const cols = t.columns.map((c) => `  "${c.name}" ${c.dataType}${c.nullable ? "" : " NOT NULL"}`);
    const pk = t.columns.filter((c) => c.primaryKey).map((c) => `"${c.name}"`);
    if (pk.length > 0) cols.push(`  PRIMARY KEY (${pk.join(", ")})`);
    for (const r of relations.filter((x) => x.fromTable === t.id)) {
      const target = byId.get(r.toTable);
      if (target) cols.push(`  FOREIGN KEY ("${r.fromColumn}") REFERENCES "${target.name}" ("${r.toColumn}")`);
    }
    out.push(`CREATE TABLE "${t.name}" (\n${cols.join(",\n")}\n);`);
  }
  return out.join("\n\n");
}
