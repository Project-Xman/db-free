// SOT: key-tree, redis-key-grouping, key-namespace-folders
import { useMemo, useState } from "react";
import { Button, Chip } from "@heroui/react";
import type { TableInfo, TableRef } from "@/lib/bindings";
import { useWorkspace } from "@/stores/workspace";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

interface KeyNode {
  name: string;
  path: string;
  children: Map<string, KeyNode>;
  /// Full key when this node is itself a key (a folder can also be a key).
  key: string | null;
  count: number;
}

// WHAT:  Redis keys grouped by `:` into collapsible folders with key counts
//        (DB Manager layout): `bull:queue:1` → bull › queue › 1.
// HOW:   Builds a trie from the flat key list every render of the catalog;
//        folders collapse by default, click a leaf to open the key tab.
export function KeyTree({ connectionId, keys }: { connectionId: string; keys: TableInfo[] }) {
  const root = useMemo(() => buildTree(keys), [keys]);
  return (
    <ul>
      {[...root.children.values()].map((node) => (
        <KeyRow key={node.path} connectionId={connectionId} node={node} depth={0} />
      ))}
    </ul>
  );
}

function buildTree(keys: TableInfo[]): KeyNode {
  const root: KeyNode = { name: "", path: "", children: new Map(), key: null, count: 0 };
  for (const t of keys) {
    const parts = t.name.split(":");
    let node = root;
    let path = "";
    parts.forEach((part, i) => {
      path = i === 0 ? part : `${path}:${part}`;
      let child = node.children.get(part);
      if (!child) {
        child = { name: part, path, children: new Map(), key: null, count: 0 };
        node.children.set(part, child);
      }
      child.count += 1;
      if (i === parts.length - 1) child.key = t.name;
      node = child;
    });
  }
  return root;
}

function KeyRow({ connectionId, node, depth }: { connectionId: string; node: KeyNode; depth: number }) {
  const openTable = useWorkspace((s) => s.openTable);
  const activeTabId = useWorkspace((s) => s.activeTabId);
  const [open, setOpen] = useState(false);
  const hasChildren = node.children.size > 0;
  const isKey = node.key !== null;
  const ref: TableRef | null = node.key === null ? null : { schema: null, name: node.key };
  const active = ref !== null && activeTabId === `table:${connectionId}:${ref.name}`;

  return (
    <li>
      <div
        className={cn("group flex h-8 cursor-default items-center gap-1 pr-2 text-[13px]", active ? "bg-surface-tertiary text-foreground" : "text-muted hover:bg-surface-secondary hover:text-foreground")}
        style={{ paddingLeft: 8 + depth * 16 }}
        title={node.path}
      >
        {hasChildren ? (
          <Button
            isIconOnly
            variant="ghost"
            size="sm"
            onPress={() => setOpen((v) => !v)}
            aria-label={open ? "Collapse" : "Expand"}
            className="flex size-5 min-w-5 p-0 items-center justify-center rounded-sm text-muted hover:text-foreground"
          >
            <Icon name={open ? "chevron-down" : "chevron-right"} size={12} />
          </Button>
        ) : (
          <span className="size-5" />
        )}
        <Button
          variant="ghost"
          size="sm"
          onPress={() => {
            if (ref) openTable(connectionId, ref);
            else setOpen((v) => !v);
          }}
          className="flex h-auto min-w-0 flex-1 items-center justify-start gap-2 p-0 text-left bg-transparent hover:bg-transparent"
        >
          <Icon name={hasChildren && !isKey ? "folder" : isKey && hasChildren ? "folder" : "hash"} size={13} className={cn("shrink-0", isKey ? "text-accent" : "")} />
          <span className="truncate">{node.name.length > 0 ? node.name : "(empty)"}</span>
          {hasChildren ? (
            <Chip size="sm" variant="soft" className="ml-auto font-mono text-[9px]">
              {node.count}
            </Chip>
          ) : null}
        </Button>
      </div>
      {open && hasChildren ? (
        <ul>
          {[...node.children.values()].map((child) => (
            <KeyRow key={child.path} connectionId={connectionId} node={child} depth={depth + 1} />
          ))}
        </ul>
      ) : null}
    </li>
  );
}
