// SOT: tab-bar, workspace-tabs, window-drag-region
import { Button, Chip, CloseButton } from "@heroui/react";
import { usePendingCount, useWorkspace, type Tab } from "@/stores/workspace";
import { Icon, type IconName } from "@/lib/icons";
import { OBJECT_KINDS, TOOLS } from "@/lib/objects";
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
    <div className="drag-region flex h-11 shrink-0 items-center border-b border-border/40 glass-header px-2" data-tauri-drag-region role="tablist" aria-label="Open tabs">
      <div className="flex h-full items-center gap-1 overflow-x-auto [scrollbar-width:none] py-1">
        {tabs.map((tab) => (
          <TabItem key={tab.id} tab={tab} active={tab.id === activeTabId} onActivate={() => activateTab(tab.id)} onClose={() => closeTab(tab.id)} />
        ))}
      </div>
      {activeId ? (
        <Button
          isIconOnly
          variant="ghost"
          size="sm"
          aria-label="New query tab"
          onPress={() => openQuery(activeId)}
          className="ml-1 size-7 min-w-7 rounded-lg text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover"
        >
          <Icon name="plus" size={13} />
        </Button>
      ) : null}
      <div className="drag-region h-full min-w-6 flex-1" data-tauri-drag-region />
      {activeId ? (
        <Button
          size="sm"
          variant={pending > 0 ? "secondary" : "ghost"}
          className={cn(
            "mr-2 h-7.5 rounded-lg text-xs font-medium liquid-hover",
            pending > 0 ? "glass-pill text-foreground border-warning/50" : "text-muted hover:bg-surface-secondary/70 hover:text-foreground",
          )}
          onPress={() => setPanelOpen(!panelOpen)}
        >
          Changes
          {pending > 0 ? (
            <Chip size="sm" variant="primary" color="warning" className="ml-1.5 font-bold text-[10px] h-4 min-w-4 p-0">
              {pending}
            </Chip>
          ) : null}
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
    case "chat":
      return { label: "Chat DB", icon: "braces" };
    case "object":
      return { label: tab.reference.name, icon: OBJECT_KINDS[tab.reference.kind].icon };
    case "admin":
      return { label: "Server", icon: "server" };
    case "tool":
      return { label: TOOLS[tab.tool].label, icon: TOOLS[tab.tool].icon };
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
        "group relative flex h-7.5 max-w-[200px] min-w-[110px] cursor-default items-center gap-2 rounded-lg px-2.5 text-[12.5px] font-medium liquid-hover",
        active
          ? "glass-pill text-foreground shadow-xs"
          : "text-muted hover:bg-surface-secondary/60 hover:text-foreground",
      )}
    >
      <Icon name={base.icon} size={12.5} className={cn("shrink-0", active ? "text-accent" : "text-muted")} />
      <span className="min-w-0 flex-1 truncate">{label}</span>
      <CloseButton
        aria-label={`Close ${label}`}
        onPress={() => onClose()}
        className={cn(
          "size-4 min-w-4 p-0 rounded-full text-muted hover:text-foreground transition-all",
          active ? "opacity-70 hover:opacity-100" : "opacity-0 group-hover:opacity-100",
        )}
      />
    </div>
  );
}
