// SOT: icon-rail, primary-navigation, window-drag-region
import { useWorkspace, type SidebarMode } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { isMac } from "@/components/global/Kbd";
import { Icon, type IconName } from "@/lib/icons";
import { cn } from "@/lib/cn";

const SIDEBARS: readonly { mode: SidebarMode; icon: IconName; label: string }[] = [
  { mode: "tables", icon: "table", label: "Tables" },
  { mode: "queries", icon: "file", label: "Saved queries" },
  { mode: "dashboards", icon: "columns", label: "Dashboards" },
  { mode: "workflows", icon: "play", label: "Workflows" },
  { mode: "diagrams", icon: "view", label: "Schema diagrams" },
];

// WHAT:  Left rail: connections home, one entry per sidebar, history/transfer
//        shortcuts, settings. Its top is the window drag region.
export function IconRail() {
  const page = useWorkspace((s) => s.page);
  const sidebar = useWorkspace((s) => s.sidebar);
  const activeId = useWorkspace((s) => s.activeConnectionId);
  const connected = useWorkspace((s) => (activeId ? s.sessions.includes(activeId) : false));
  const goConnections = useWorkspace((s) => s.goConnections);
  const goSettings = useWorkspace((s) => s.goSettings);
  const setSidebar = useWorkspace((s) => s.setSidebar);
  const openHistory = useWorkspace((s) => s.openHistory);
  const openTransfer = useWorkspace((s) => s.openTransfer);
  const setPaletteOpen = useWorkspace((s) => s.setPaletteOpen);
  const inWorkspace = page.kind === "workspace";

  return (
    <nav className="flex w-12 shrink-0 flex-col items-center border-r border-border bg-surface" aria-label="Primary">
      <div className={cn("drag-region w-full shrink-0", isMac() ? "h-9" : "h-2")} data-tauri-drag-region />
      <button type="button" onClick={() => setPaletteOpen(true)} className="mb-3 flex size-8 items-center justify-center rounded-lg bg-accent text-accent-foreground" aria-label="Command palette (⌘K)" title="Command palette (⌘K)">
        <Icon name="database" size={16} />
      </button>
      <div className="flex flex-col gap-1">
        <IconButton icon="plug" label="Connections" active={page.kind === "connections" || page.kind === "connection-picker" || page.kind === "connection-form"} onPress={goConnections} size={16} />
        {SIDEBARS.map((s) => (
          <IconButton key={s.mode} icon={s.icon} label={s.label} active={inWorkspace && sidebar === s.mode} isDisabled={!connected} onPress={() => setSidebar(s.mode)} size={16} />
        ))}
        <IconButton icon="history" label="Query history" isDisabled={!connected} onPress={() => activeId && openHistory(activeId)} size={16} />
        <IconButton icon="download" label="Export / Import" isDisabled={!connected} onPress={() => activeId && openTransfer(activeId)} size={16} />
      </div>
      <div className="mt-auto mb-2 flex flex-col gap-1">
        <IconButton icon="settings" label="Settings" active={page.kind === "settings"} onPress={goSettings} size={16} />
      </div>
    </nav>
  );
}
