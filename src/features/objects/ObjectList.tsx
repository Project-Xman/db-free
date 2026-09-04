// SOT: object-list, object-row, object-kind-node, lazy-object-loading
import { useEffect, useState } from "react";
import { Button, Chip, Spinner } from "@heroui/react";
import type { ObjectKind, ObjectRef, ObjectSummary } from "@/lib/bindings";
import { kindMeta } from "@/lib/objects";
import { Icon } from "@/lib/icons";
import { normalizeError } from "@/lib/ipc";
import { objectKey, useWorkspace } from "@/stores/workspace";
import { cn } from "@/lib/cn";

// WHAT:  One object row: kind icon, name, muted detail, optional badge chip.
//        Click opens (or focuses) the object tab.
// WHERE: src/features/objects/ObjectsPanel.tsx, src/features/admin/AdminTab.tsx
//        `onSelect` replaces that default (pickers such as the XML viewer).
export function ObjectRow({ connectionId, object, dense = false, onSelect }: { connectionId: string; object: ObjectSummary; dense?: boolean; onSelect?: (reference: ObjectRef) => void }) {
  const openObject = useWorkspace((s) => s.openObject);
  const tabId = `object:${connectionId}:${objectKey(object.reference)}`;
  const isActive = useWorkspace((s) => s.activeTabId === tabId);
  const meta = kindMeta(object.reference.kind);
  return (
    <Button
      variant="ghost"
      size="sm"
      onPress={() => (onSelect ? onSelect(object.reference) : openObject(connectionId, object.reference))}
      className={cn(
        "flex w-full min-w-0 items-center justify-start gap-2 rounded-lg px-2 text-left text-[12.5px] liquid-hover",
        dense ? "h-7 min-h-7" : "h-7.5 min-h-7.5",
        isActive ? "glass-pill font-medium text-accent shadow-xs" : "text-muted hover:bg-surface-secondary/70 hover:text-foreground",
      )}
    >
      <Icon name={meta.icon} size={13} className="shrink-0 opacity-70" />
      <span className="truncate">{object.reference.name}</span>
      {object.badge ? (
        <Chip size="sm" variant="soft" className="ml-1 h-4 shrink-0 px-1 font-mono text-[9px]">
          {object.badge}
        </Chip>
      ) : null}
      {object.detail ? <span className="ml-auto truncate pl-2 font-mono text-[10px] text-muted/70">{object.detail}</span> : null}
    </Button>
  );
}

// WHAT:  Loads one (kind, parent) listing through the store cache and renders
//        it; `refreshKey` changes force a reload.
export function useObjects(connectionId: string, kind: ObjectKind, parent: string | null, enabled: boolean, refreshKey = 0) {
  const loadObjects = useWorkspace((s) => s.loadObjects);
  // `loading` is derived: the request key changes before the answer arrives.
  const requestKey = `${connectionId}|${kind}|${parent ?? ""}|${refreshKey}`;
  const [state, setState] = useState<{ key: string; objects: ObjectSummary[] | null; error: string | null }>({ key: "", objects: null, error: null });
  useEffect(() => {
    if (!enabled) return;
    const token = { cancelled: false };
    void (async () => {
      try {
        const objects = await loadObjects(connectionId, kind, parent, refreshKey > 0);
        if (!token.cancelled) setState({ key: requestKey, objects, error: null });
      } catch (raw) {
        if (!token.cancelled) setState({ key: requestKey, objects: null, error: normalizeError(raw).message });
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [connectionId, kind, parent, enabled, refreshKey, requestKey, loadObjects]);
  const fresh = state.key === requestKey;
  return { objects: fresh ? state.objects : (state.objects ?? null), error: fresh ? state.error : null, loading: enabled && !fresh };
}

// WHAT:  Collapsible folder for one kind in the sidebar: header with count,
//        lazily loaded rows underneath, filtered by the panel search.
export function KindNode({ connectionId, kind, parent, needle, refreshKey, defaultOpen = false }: { connectionId: string; kind: ObjectKind; parent: string | null; needle: string; refreshKey: number; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen);
  const meta = kindMeta(kind);
  const { objects, error, loading } = useObjects(connectionId, kind, parent, open, refreshKey);
  const visible = objects?.filter((o) => needle.length === 0 || o.reference.name.toLowerCase().includes(needle)) ?? [];
  return (
    <div className="py-0.5">
      <Button
        variant="ghost"
        size="sm"
        onPress={() => setOpen((v) => !v)}
        className="flex h-7 min-h-7 w-full min-w-0 items-center justify-start gap-1.5 rounded-lg px-1.5 text-left text-[12px] text-muted hover:bg-surface-secondary/60 hover:text-foreground"
      >
        <Icon name={open ? "chevron-down" : "chevron-right"} size={11} className="shrink-0" />
        <Icon name={meta.icon} size={13} className="shrink-0 opacity-70" />
        <span className="truncate">{meta.plural}</span>
        {objects !== null ? (
          <Chip size="sm" variant="soft" className="ml-auto h-4 min-w-0 px-1 font-mono text-[9px]">
            {objects.length}
          </Chip>
        ) : null}
      </Button>
      {open ? (
        <div className="pl-3">
          {loading && objects === null ? (
            <div className="flex items-center gap-2 py-1 pl-2 text-[11px] text-muted">
              <Spinner size="sm" /> loading…
            </div>
          ) : error !== null ? (
            <p className="px-2 py-1 text-[11px] text-danger">{error}</p>
          ) : visible.length === 0 ? (
            <p className="px-2 py-1 text-[11px] text-muted/70">{needle.length > 0 ? "No match." : `No ${meta.plural.toLowerCase()}.`}</p>
          ) : (
            visible.map((o) => <ObjectRow key={objectKey(o.reference)} connectionId={connectionId} object={o} dense />)
          )}
        </div>
      ) : null}
    </div>
  );
}
