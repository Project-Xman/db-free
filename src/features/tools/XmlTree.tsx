// SOT: xml-tree, xml-viewer-component, collapsible-xml
import { useMemo, useState } from "react";
import { Button } from "@heroui/react";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

interface XmlNode {
  path: string;
  name: string;
  attributes: { name: string; value: string }[];
  text: string | null;
  children: XmlNode[];
}

// WHAT:  Parses XML text with the browser's DOMParser into a plain tree
//        (elements, attributes, text), which the component renders
//        collapsibly. A parse error is shown in place of the tree.
// WHERE: src/features/objects/ObjectTab.tsx (xml definitions), src/features/tools/XmlViewerTab.tsx
function toTree(element: Element, path: string): XmlNode {
  const children: XmlNode[] = [];
  const texts: string[] = [];
  let index = 0;
  for (const child of Array.from(element.childNodes)) {
    if (child.nodeType === Node.ELEMENT_NODE && child instanceof Element) {
      children.push(toTree(child, `${path}/${index}`));
      index += 1;
    } else if (child.nodeType === Node.TEXT_NODE || child.nodeType === Node.CDATA_SECTION_NODE) {
      const t = (child.textContent ?? "").trim();
      if (t.length > 0) texts.push(t);
    }
  }
  return {
    path,
    name: element.nodeName,
    attributes: Array.from(element.attributes).map((a) => ({ name: a.name, value: a.value })),
    text: texts.length > 0 ? texts.join(" ") : null,
    children,
  };
}

function parseXml(source: string): { root: XmlNode | null; error: string | null } {
  const doc = new DOMParser().parseFromString(source, "application/xml");
  const parseError = doc.getElementsByTagName("parsererror")[0];
  if (parseError) return { root: null, error: parseError.textContent.split("\n")[0] ?? "Invalid XML." };
  return { root: toTree(doc.documentElement, "$"), error: null };
}

function containerPaths(node: XmlNode, depth: number, maxDepth: number, out: string[]): string[] {
  if (node.children.length > 0 && depth >= maxDepth) out.push(node.path);
  for (const child of node.children) containerPaths(child, depth + 1, maxDepth, out);
  return out;
}

export function XmlTree({ source, defaultDepth = 2 }: { source: string; defaultDepth?: number }) {
  const parsed = useMemo(() => parseXml(source), [source]);
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(() => new Set(parsed.root ? containerPaths(parsed.root, 0, defaultDepth, []) : []));
  if (parsed.error !== null || parsed.root === null) {
    return <p className="text-xs text-danger">{parsed.error ?? "Invalid XML."}</p>;
  }
  const toggle = (path: string) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  return (
    <div className="selectable font-mono text-[12px] leading-relaxed">
      <div className="mb-1 flex gap-1">
        <Button size="sm" variant="ghost" className="h-6 min-w-0 rounded-md px-1.5 text-[11px] text-muted hover:text-foreground" onPress={() => setCollapsed(new Set())}>
          Expand all
        </Button>
        <Button size="sm" variant="ghost" className="h-6 min-w-0 rounded-md px-1.5 text-[11px] text-muted hover:text-foreground" onPress={() => setCollapsed(new Set(containerPaths(parsed.root ?? { path: "$", name: "", attributes: [], text: null, children: [] }, 0, 0, [])))}>
          Collapse all
        </Button>
      </div>
      <XmlNodeView node={parsed.root} collapsed={collapsed} onToggle={toggle} depth={0} />
    </div>
  );
}

function XmlNodeView({ node, collapsed, onToggle, depth }: { node: XmlNode; collapsed: ReadonlySet<string>; onToggle: (path: string) => void; depth: number }) {
  const hasChildren = node.children.length > 0;
  const open = !collapsed.has(node.path);
  return (
    <div style={{ paddingLeft: depth === 0 ? 0 : 14 }}>
      <div className="flex items-start gap-1">
        {hasChildren ? (
          <Button isIconOnly size="sm" variant="ghost" aria-label={open ? "Collapse" : "Expand"} onPress={() => onToggle(node.path)} className="mt-0.5 size-4 min-w-4 rounded-sm text-muted">
            <Icon name={open ? "chevron-down" : "chevron-right"} size={10} />
          </Button>
        ) : (
          <span className="inline-block w-4 shrink-0" />
        )}
        <span className="min-w-0 break-all">
          <span className="text-muted">&lt;</span>
          <span className="text-syntax-keyword">{node.name}</span>
          {node.attributes.map((a) => (
            <span key={a.name}>
              {" "}
              <span className="text-syntax-type">{a.name}</span>
              <span className="text-muted">=</span>
              <span className="text-syntax-string">"{a.value}"</span>
            </span>
          ))}
          <span className="text-muted">{hasChildren || node.text !== null ? ">" : " />"}</span>
          {!hasChildren && node.text !== null ? (
            <>
              <span className={cn("text-foreground", node.text.length > 120 ? "block whitespace-pre-wrap pl-4" : "")}>{node.text}</span>
              <span className="text-muted">&lt;/</span>
              <span className="text-syntax-keyword">{node.name}</span>
              <span className="text-muted">&gt;</span>
            </>
          ) : null}
          {hasChildren && !open ? <span className="text-muted/60"> … {node.children.length} </span> : null}
          {hasChildren && !open ? (
            <>
              <span className="text-muted">&lt;/</span>
              <span className="text-syntax-keyword">{node.name}</span>
              <span className="text-muted">&gt;</span>
            </>
          ) : null}
        </span>
      </div>
      {hasChildren && open ? (
        <>
          {node.text !== null ? <div className="pl-8 text-foreground">{node.text}</div> : null}
          {node.children.map((child) => (
            <XmlNodeView key={child.path} node={child} collapsed={collapsed} onToggle={onToggle} depth={depth + 1} />
          ))}
          <div className="pl-4">
            <span className="text-muted">&lt;/</span>
            <span className="text-syntax-keyword">{node.name}</span>
            <span className="text-muted">&gt;</span>
          </div>
        </>
      ) : null}
    </div>
  );
}
