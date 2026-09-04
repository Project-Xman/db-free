// SOT: json-viewer, json-tree, collapsible-json
import { useMemo, useState } from "react";
import { Button } from "@heroui/react";
import type { JsonValue } from "@/lib/bindings/serde_json/JsonValue";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

const INDENT = 14;

interface JsonViewerProps {
  value: JsonValue;
  /// Containers deeper than this start collapsed.
  defaultDepth?: number;
  className?: string;
  /// Hide the expand/collapse/copy toolbar (embedding in a tight space).
  bare?: boolean;
}

// WHAT:  Collapsible JSON tree. Objects and arrays fold behind a chevron and
//        show a `{3 keys}` / `[5 items]` summary while closed; scalars use the
//        syntax colours from globals.css.
// WHY:   Wide JSON columns are unreadable as one line; a tree with expand /
//        collapse is what the inspector and the JSON cell editor need.
// WHERE: src/features/grid/TableTab.tsx, src/components/global/ValueEditor.tsx
export function JsonViewer({ value, defaultDepth = 2, className, bare = false }: JsonViewerProps) {
  const containers = useMemo(() => containerPaths(value), [value]);
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(() => new Set(containerPaths(value, defaultDepth)));
  const toggle = (path: string) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  return (
    <div className={cn("selectable font-mono text-[11px] leading-relaxed", className)}>
      {!bare && containers.length > 0 ? (
        <div className="mb-1.5 flex items-center gap-1">
          <Button size="sm" variant="ghost" className="h-6 min-w-0 rounded-md px-1.5 text-[11px] text-muted hover:text-foreground" onPress={() => setCollapsed(new Set())}>
            <Icon name="expand" size={11} />
            Expand all
          </Button>
          <Button size="sm" variant="ghost" className="h-6 min-w-0 rounded-md px-1.5 text-[11px] text-muted hover:text-foreground" onPress={() => setCollapsed(new Set(containers))}>
            <Icon name="collapse" size={11} />
            Collapse all
          </Button>
          <Button size="sm" variant="ghost" className="ml-auto h-6 min-w-0 rounded-md px-1.5 text-[11px] text-muted hover:text-foreground" onPress={() => void navigator.clipboard.writeText(JSON.stringify(value, null, 2))}>
            <Icon name="copy" size={11} />
            Copy
          </Button>
        </div>
      ) : null}
      <Node value={value} path="$" depth={0} collapsed={collapsed} onToggle={toggle} last />
    </div>
  );
}

interface NodeProps {
  name?: string;
  value: JsonValue;
  path: string;
  depth: number;
  collapsed: ReadonlySet<string>;
  onToggle: (path: string) => void;
  last: boolean;
}

function Node({ name, value, path, depth, collapsed, onToggle, last }: NodeProps) {
  const comma = last ? null : <span className="text-muted">,</span>;
  const key = name === undefined ? null : (
    <>
      <span className="text-syntax-type">&quot;{name}&quot;</span>
      <span className="text-muted">:&nbsp;</span>
    </>
  );
  if (value === null || typeof value !== "object") {
    return (
      <div className="flex min-w-0 items-baseline whitespace-pre-wrap break-all" style={{ paddingLeft: depth * INDENT }}>
        {key}
        <Scalar value={value} />
        {comma}
      </div>
    );
  }
  const isArray = Array.isArray(value);
  const entries: readonly (readonly [string, JsonValue])[] = isArray ? value.map((v, i): readonly [string, JsonValue] => [String(i), v]) : Object.entries(value);
  const [open, close] = isArray ? ["[", "]"] : ["{", "}"];
  const expanded = !collapsed.has(path);
  return (
    <div>
      <div className="flex min-w-0 items-center" style={{ paddingLeft: depth * INDENT }}>
        <Button
          isIconOnly
          size="sm"
          variant="ghost"
          aria-label={expanded ? "Collapse" : "Expand"}
          onPress={() => onToggle(path)}
          className="-ml-4 size-4 min-w-4 rounded-sm p-0 text-muted hover:bg-surface-secondary hover:text-foreground"
        >
          <Icon name={expanded ? "chevron-down" : "chevron-right"} size={11} />
        </Button>
        {key}
        <span className="text-muted">{open}</span>
        {expanded ? null : (
          <>
            <Button size="sm" variant="ghost" onPress={() => onToggle(path)} className="mx-1 h-4 min-w-0 rounded-sm px-1 text-[10px] italic text-muted hover:text-foreground">
              {entries.length} {isArray ? (entries.length === 1 ? "item" : "items") : entries.length === 1 ? "key" : "keys"}
            </Button>
            <span className="text-muted">{close}</span>
            {comma}
          </>
        )}
      </div>
      {expanded ? (
        <>
          {entries.map(([k, v], i) => (
            <Node key={k} {...(isArray ? {} : { name: k })} value={v} path={`${path}.${k}`} depth={depth + 1} collapsed={collapsed} onToggle={onToggle} last={i === entries.length - 1} />
          ))}
          <div style={{ paddingLeft: depth * INDENT }}>
            <span className="text-muted">{close}</span>
            {comma}
          </div>
        </>
      ) : null}
    </div>
  );
}

function Scalar({ value }: { value: string | number | boolean | null }) {
  if (value === null) return <span className="text-syntax-keyword">null</span>;
  if (typeof value === "boolean") return <span className="text-syntax-keyword">{value ? "true" : "false"}</span>;
  if (typeof value === "number") return <span className="text-syntax-number">{String(value)}</span>;
  return <span className="text-syntax-string">&quot;{value}&quot;</span>;
}

// WHAT:  Paths of every object/array node, optionally only those at or below `minDepth`.
function containerPaths(value: JsonValue, minDepth = 0): string[] {
  const out: string[] = [];
  const walk = (v: JsonValue, path: string, depth: number) => {
    if (v === null || typeof v !== "object") return;
    if (depth >= minDepth) out.push(path);
    const entries: readonly (readonly [string, JsonValue])[] = Array.isArray(v) ? v.map((item, i): readonly [string, JsonValue] => [String(i), item]) : Object.entries(v);
    for (const [k, child] of entries) walk(child, `${path}.${k}`, depth + 1);
  };
  walk(value, "$", 0);
  return out;
}
