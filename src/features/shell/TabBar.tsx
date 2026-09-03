// SOT: tab-bar, workspace-tabs, window-drag-region
import { Button } from "@heroui/react";
import { usePendingCount, useWorkspace, type Tab } from "@/stores/workspace";
import { Icon, type IconName } from "@/lib/icons";
import { cn } from "@/lib/cn";

// WHAT:  Open tabs above the main area (also a drag region) + the Changes button.
export function TabBar() {
  const tabs = useWorkspace((s) => s.tabs);
  const activeTabId = useWorkspace((s) => s.activeTabId);
  const activateTab = useWorkspace((s) => s.activateTab);
  const closeTab = useWorkspace((s) => s.closeTab);
  const activeId = useWorkspace((s) => s.activeConnectionId);
  const openQuery = useWorkspace((s) => s.openQuery);
  const pending = usePendingCount(activeId);
  const panelOpen = useWorkspace((s) => s.changesPanelOpen);
  const setPanelOpen = useWorkspace((s) => s.setChangesPanelOpen);

  return (
    <div className="drag-region flex h-10 shrink-0 items-end border-b border-border bg-surface" data-tauri-drag-region role="tablist" aria-label="Open tabs">
      <div className="flex h-full items-end overflow-x-auto [scrollbar-width:none]">
        {tabs.map((tab) => (
          <TabItem key={tab.id} tab={tab} active={tab.id === activeTabId} onActivate={() => activateTab(tab.id)} onClose={() => closeTab(tab.id)} />
        ))}
      </div>
      {activeId ? (
        <Button isIconOnly variant="ghost" size="sm" aria-label="New query tab" onPress={() => openQuery(activeId)} className="mb-1 ml-1 size-7 min-w-7 text-muted">
          <Icon name="plus" size={14} />
        </Button>
      ) : null}
      <div className="drag-region h-full min-w-6 flex-1" data-tauri-drag-region />
      {activeId ? (
        <Button size="sm" variant={pending > 0 ? "secondary" : "ghost"} className={cn("mr-2 mb-1.5", pending > 0 ? "" : "text-muted")} onPress={() => setPanelOpen(!panelOpen)}>
          Changes
          {pending > 0 ? <span className="ml-1 rounded-full bg-warning px-1.5 text-[10px] font-semibold text-warning-foreground">{pending}</span> : null}
        </Button>
      ) : null}
    </div>
  );
}

function tabPresentation(tab: Tab): { label: string; icon: IconName } {
  switch (tab.kind) {
    case "table":
      return { label: tab.table.name, icon: "table" };
    case "query":
      return { label: tab.title, icon: "terminal" };
    case "history":
      return { label: "Query History", icon: "history" };
    case "transfer":
      return { label: "Export / Import", icon: "download" };
    case "erd":
      return { label: `Diagram: ${tab.schema ?? "all"}`, icon: "view" };
    case "document":
      return { label: tab.documentKind === "dashboard" ? "Dashboard" : tab.documentKind === "workflow" ? "Workflow" : "Diagram", icon: tab.documentKind === "dashboard" ? "columns" : tab.documentKind === "workflow" ? "play" : "view" };
  }
}

function TabItem({ tab, active, onActivate, onClose }: { tab: Tab; active: boolean; onActivate: () => void; onClose: () => void }) {
  const docName = useWorkspace((s) => (tab.kind === "document" ? s.documents[tab.documentKind].find((d) => d.id === tab.documentId)?.name : undefined));
  const base = tabPresentation(tab);
  const label = docName ?? base.label;
  return (
    <div
      role="tab"
      aria-selected={active}
      tabIndex={0}
      onClick={onActivate}
      onKeyDown={(e) => {
        if (e.key === "Enter") onActivate();
      }}
      onAuxClick={(e) => {
        if (e.button === 1) onClose();
      }}
      className={cn(
        "group flex h-9 max-w-[220px] min-w-[120px] cursor-default items-center gap-2 border-r border-border px-3 text-[13px]",
        active ? "bg-background text-foreground" : "text-muted hover:bg-surface-secondary hover:text-foreground",
      )}
    >
      <Icon name={base.icon} size={13} className="shrink-0 text-muted" />
      <span className="min-w-0 flex-1 truncate">{label}</span>
      <button
        type="button"
        aria-label={`Close ${label}`}
        onClick={(e) => {
          e.stopPropagation();
          onClose();
        }}
        className={cn("rounded-sm p-0.5 text-muted hover:bg-surface-tertiary hover:text-foreground", active ? "" : "opacity-0 group-hover:opacity-100")}
      >
        <Icon name="x" size={12} />
      </button>
    </div>
  );
}
