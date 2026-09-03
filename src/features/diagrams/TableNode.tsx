// SOT: table-node, erd-node, diagram-node-rendering
import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import { Icon, type IconName } from "@/lib/icons";

// React Flow node data must be assignable to its record constraint; a type alias
// (implicit index signature) satisfies that where an interface cannot.
// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
export type TableNodeData = {
  title: string;
  columns: { name: string; type: string; pk: boolean; icon: IconName }[];
  /** Top→bottom diagrams attach relations to the card (top/bottom) instead of to a column row (left/right). */
  vertical?: boolean;
};

export const VERTICAL_TARGET_HANDLE = "__top";
export const VERTICAL_SOURCE_HANDLE = "__bottom";

const MAX_ROWS = 14;

// WHAT:  A table card on the ER / designer canvas: blue header, column rows with
//        type icons. Horizontal diagrams get a handle per column so relations
//        attach to the right row; vertical ones get one handle on top (incoming)
//        and one below (outgoing) so edges flow down the page.
export function TableNode({ data, selected }: NodeProps<Node<TableNodeData>>) {
  const shown = data.columns.slice(0, MAX_ROWS);
  const vertical = data.vertical === true;
  return (
    <div className={`relative w-[240px] overflow-hidden rounded-md border bg-surface text-[11px] shadow-lg ${selected ? "border-accent" : "border-border"}`}>
      {vertical ? <Handle type="target" position={Position.Top} id={VERTICAL_TARGET_HANDLE} className="!size-1.5 !border-0 !bg-accent" /> : null}
      <div className="flex items-center gap-1.5 bg-accent px-2 py-1 font-medium text-accent-foreground">
        <Icon name="table" size={12} />
        <span className="truncate">{data.title}</span>
      </div>
      <ul>
        {shown.map((c) => (
          <li key={c.name} className="relative flex h-[22px] items-center gap-1.5 border-t border-separator px-2 text-muted">
            {vertical ? null : <Handle type="target" position={Position.Left} id={c.name} className="!size-1.5 !border-0 !bg-accent" />}
            <Icon name={c.icon} size={11} className={c.pk ? "text-warning" : ""} />
            <span className="truncate text-foreground">{c.name}</span>
            <span className="ml-auto truncate font-mono text-[10px]">{c.type}</span>
            {vertical ? null : <Handle type="source" position={Position.Right} id={c.name} className="!size-1.5 !border-0 !bg-accent" />}
          </li>
        ))}
        {data.columns.length > MAX_ROWS ? <li className="border-t border-separator px-2 py-1 text-[10px] text-muted">+{data.columns.length - MAX_ROWS} more</li> : null}
      </ul>
      {vertical ? <Handle type="source" position={Position.Bottom} id={VERTICAL_SOURCE_HANDLE} className="!size-1.5 !border-0 !bg-accent" /> : null}
    </div>
  );
}
