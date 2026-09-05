// SOT: tool-shell, playground-header, playground-layout, collection-options
import { useMemo, type ReactNode } from "react";
import { Chip, ScrollShadow } from "@heroui/react";
import type { Tool } from "@/lib/bindings";
import { toolMeta } from "@/lib/objects";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";

// WHAT:  Common frame for the playground tabs: header (icon, name, hint,
//        optional right-hand controls) above the tool body.
// WHERE: src/features/tools/ToolTab.tsx
export function ToolShell({ tool, right, children }: { tool: Tool; right?: ReactNode; children: ReactNode }) {
  const meta = toolMeta(tool);
  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-2 border-b border-border/40 glass-header px-3">
        <Icon name={meta.icon} size={15} className="text-accent" />
        <span className="text-sm font-semibold tracking-tight text-foreground">{meta.label}</span>
        <Chip size="sm" variant="soft" className="hidden text-[10px] text-muted md:inline-flex">
          {meta.hint}
        </Chip>
        <span className="ml-auto flex items-center gap-1.5">{right}</span>
      </div>
      <div className="min-h-0 flex-1">{children}</div>
    </div>
  );
}

// WHAT:  The catalog's collections / tables / indexes / topics for a
//        connection as select options (every playground picks one first).
export function useCollectionOptions(connectionId: string): { value: string; label: string }[] {
  // The catalog reference is stable between loads, so the options only rebuild on reload.
  const catalog = useWorkspace((s) => s.catalogs[connectionId]);
  return useMemo(() => {
    if (!catalog) return [];
    return catalog.schemas.flatMap((schema) => schema.tables.map((t) => ({ value: t.name, label: t.schema && catalog.schemas.length > 1 ? `${t.schema}.${t.name}` : t.name })));
  }, [catalog]);
}

// WHAT:  Two-pane playground body: a form column on the left, results filling the rest.
export function ToolBody({ form, children }: { form: ReactNode; children: ReactNode }) {
  return (
    <div className="flex h-full min-h-0">
      <aside className="flex w-80 shrink-0 border-r border-border/40">
        <ScrollShadow hideScrollBar className="flex min-w-0 flex-1 flex-col gap-3 p-3">{form}</ScrollShadow>
      </aside>
      <div className="min-h-0 min-w-0 flex-1">{children}</div>
    </div>
  );
}
