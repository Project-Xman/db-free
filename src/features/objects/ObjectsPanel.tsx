// SOT: objects-panel, object-explorer-sidebar, object-sections, scoped-kind-parent
import { useState } from "react";
import { ScrollShadow, SearchField, Separator, Skeleton } from "@heroui/react";
import type { ObjectKind } from "@/lib/bindings";
import { SECTIONS, isAdminKind, isScopedKind, kindMeta, objectKindsOf } from "@/lib/objects";
import { Icon } from "@/lib/icons";
import { useActiveConnection, useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { AppSelect } from "@/components/global/Field";
import { EnvBadge } from "@/components/global/Badge";
import { ConnectionSwitcher } from "@/features/shell/ConnectionSwitcher";
import { KindNode } from "./ObjectList";

// WHAT:  Sidebar listing every object kind the engine's family declares
//        (views, functions, triggers, indexes, streams, labels, buckets…),
//        grouped by section, each folder loading lazily. Server-wide admin
//        kinds (sessions, users, settings…) live on the Server tab instead.
// WHY:   One generic tree driven by `FamilyProfile.objectKinds`, so a new
//        engine gets an explorer by declaring what it has.
// WHERE: src/lib/objects.ts (registry), src-tauri/src/integrations/mod.rs (objects)
export function ObjectsPanel() {
  const connection = useActiveConnection();
  const info = useWorkspace((s) => (connection ? s.sessionInfos[connection.id] : undefined));
  const catalog = useWorkspace((s) => (connection ? s.catalogs[connection.id] : undefined));
  const schemaFilter = useWorkspace((s) => (connection ? (s.schemaFilter[connection.id] ?? null) : null));
  const setSchemaFilter = useWorkspace((s) => s.setSchemaFilter);
  const invalidateObjects = useWorkspace((s) => s.invalidateObjects);
  const openAdmin = useWorkspace((s) => s.openAdmin);
  const connecting = useWorkspace((s) => s.connecting);
  const [search, setSearch] = useState("");
  const [searchOpen, setSearchOpen] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);

  if (!connection) return null;
  const id = connection.id;
  const kinds: readonly ObjectKind[] = info?.objectKinds ?? objectKindsOf(connection.engine);
  const explorerKinds = kinds.filter((kind) => !isAdminKind(kind));
  const schemas = catalog?.schemas ?? [];
  const showSchemaSwitch = (info?.capabilities.namespaces ?? true) && explorerKinds.some(isScopedKind) && schemas.length > 1;
  const schemaOptions = [{ value: "*", label: "All schemas" }, ...schemas.map((s) => ({ value: s.name, label: s.name }))];
  const needle = search.trim().toLowerCase();
  const sections = SECTIONS.map((section) => ({ ...section, kinds: explorerKinds.filter((kind) => kindMeta(kind).section === section.id) })).filter((s) => s.kinds.length > 0);

  const refresh = () => {
    invalidateObjects(id);
    setRefreshKey((k) => k + 1);
  };

  return (
    <aside className="flex h-full w-full min-w-0 flex-col glass-sidebar select-none">
      <div className="drag-region flex h-11 shrink-0 items-center gap-1.5 border-b border-border/40 px-3" data-tauri-drag-region>
        <ConnectionSwitcher caption="Objects" />
        {connection.readOnly ? <EnvBadge environment="none" readOnly /> : null}
        <div className="drag-region h-full min-w-4 flex-1" data-tauri-drag-region />
        <span className="flex items-center gap-0.5">
          <IconButton icon="refresh" label="Reload objects" onPress={refresh} />
          <IconButton icon="server" label="Server admin" onPress={() => openAdmin(id)} />
          <IconButton icon="search" label="Search objects" active={searchOpen} onPress={() => setSearchOpen((v) => !v)} />
        </span>
      </div>

      {showSchemaSwitch ? (
        <div className="flex items-center gap-1 px-3 py-2 text-xs text-muted">
          <AppSelect ariaLabel="Schema" value={schemaFilter ?? "*"} options={schemaOptions} plain className="w-auto min-w-0" icon="folder" onChange={(v) => setSchemaFilter(id, v === "*" ? null : v)} />
        </div>
      ) : null}

      {searchOpen ? (
        <div className="px-3 pb-2 pt-1">
          <SearchField value={search} onChange={setSearch} aria-label="Search objects" autoFocus>
            <SearchField.Group className="glass-input h-8 rounded-lg px-2">
              <SearchField.SearchIcon />
              <SearchField.Input placeholder="Filter loaded objects…" className="w-full text-xs" />
              <SearchField.ClearButton />
            </SearchField.Group>
          </SearchField>
        </div>
      ) : null}

      <Separator className="opacity-50" />

      <ScrollShadow className="min-h-0 flex-1 px-1.5 py-1.5">
        {connecting === id ? (
          <div className="space-y-2.5 p-3">
            <Skeleton className="h-4 w-3/4 rounded-md" />
            <Skeleton className="h-4 w-1/2 rounded-md" />
            <Skeleton className="h-4 w-5/6 rounded-md" />
          </div>
        ) : sections.length === 0 ? (
          <p className="px-3 py-3 text-xs text-muted">This engine exposes no browsable objects beyond its {kinds.length === 0 ? "data" : "server state (see Server admin)"}.</p>
        ) : (
          sections.map((section) => (
            <div key={section.id} className="mb-1.5">
              <div className="flex items-center gap-1.5 px-2.5 pt-2 pb-1 text-[10px] font-semibold uppercase tracking-wider text-muted/80">
                <Icon name={section.id === "code" ? "code" : section.id === "security" ? "shield" : section.id === "server" ? "server" : section.id === "containers" ? "database" : "layers"} size={11} />
                {section.label}
              </div>
              {section.kinds.map((kind) => (
                <KindNode key={kind} connectionId={id} kind={kind} parent={isScopedKind(kind) ? schemaFilter : null} needle={needle} refreshKey={refreshKey} defaultOpen={kind === "view" || kind === "collection" || kind === "topic" || kind === "label"} />
              ))}
            </div>
          ))
        )}
      </ScrollShadow>

      {info?.serverVersion ? <div className="truncate border-t border-border/40 px-3 py-1.5 font-mono text-[10px] text-muted/70">{info.serverVersion}</div> : null}
    </aside>
  );
}
