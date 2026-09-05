// SOT: icon-rail, primary-navigation, window-drag-region
import { Button, Separator } from "@heroui/react";
import { useWorkspace, type SidebarMode } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { isMac } from "@/components/global/Kbd";
import { Icon, type IconName } from "@/lib/icons";
import { cn } from "@/lib/cn";

const SIDEBARS: readonly { mode: SidebarMode; icon: IconName; label: string }[] = [
  { mode: "tables", icon: "table", label: "Tables" },
  { mode: "objects", icon: "hierarchy", label: "Objects" },
  { mode: "queries", icon: "file", label: "Saved queries" },
  { mode: "dashboards", icon: "columns", label: "Dashboards" },
  { mode: "workflows", icon: "play", label: "Workflows" },
  { mode: "diagrams", icon: "view", label: "Schema diagrams" },
];

// On macOS the traffic lights sit inside this rail, so the two share a centre
// line: the cluster is 52px wide (three 12px buttons 20px apart), and
// tauri.conf.json insets it by 14px — (80 - 52) / 2 — so it is centred on the
// 80px rail exactly like the icon column below it. y=16 centres the 12px
// buttons in the 44px drag strip, which matches the header height.
const RAIL_WIDTH_MAC = "w-[80px]";

// WHAT:  Left rail: connections home, one entry per sidebar, history/transfer
//        shortcuts, settings. Its top is the window drag region and, on
//        macOS, it is exactly as wide as the traffic lights.
export function IconRail() {
  const page = useWorkspace((s) => s.page);
  const sidebar = useWorkspace((s) => s.sidebar);
  const activeId = useWorkspace((s) => s.activeConnectionId);
  const connected = useWorkspace((s) => (activeId ? s.sessions.includes(activeId) : false));
  const goConnections = useWorkspace((s) => s.goConnections);
  const goSettings = useWorkspace((s) => s.goSettings);
  const goCapabilities = useWorkspace((s) => s.goCapabilities);
  const setSidebar = useWorkspace((s) => s.setSidebar);
  const openHistory = useWorkspace((s) => s.openHistory);
  const openTransfer = useWorkspace((s) => s.openTransfer);
  const openChat = useWorkspace((s) => s.openChat);
  const openAdmin = useWorkspace((s) => s.openAdmin);
  const setPaletteOpen = useWorkspace((s) => s.setPaletteOpen);
  const inWorkspace = page.kind === "workspace";

  return (
    <nav className={cn("flex shrink-0 flex-col items-center glass-dock pb-2.5", isMac() ? RAIL_WIDTH_MAC : "w-11")} aria-label="Primary">
      <div className={cn("drag-region w-full shrink-0", isMac() ? "h-11" : "h-3")} data-tauri-drag-region />
      <Button
        isIconOnly
        onPress={() => setPaletteOpen(true)}
        className="group mb-2.5 flex size-8 min-w-8 items-center justify-center rounded-lg bg-accent text-accent-foreground shadow-md shadow-accent/25 liquid-hover hover:scale-105"
        aria-label="Command palette (⌘K)"
      >
        <Icon name="database" size={16} />
      </Button>
      <div className="flex flex-col items-center gap-1.5">
        <IconButton
          icon="plug"
          label="Connections"
          active={page.kind === "connections" || page.kind === "connection-picker" || page.kind === "connection-form"}
          onPress={goConnections}
          size={16}
        />
        <Separator className="my-1.5 w-5 opacity-40" />
        {SIDEBARS.map((s) => (
          <IconButton
            key={s.mode}
            icon={s.icon}
            label={s.label}
            active={inWorkspace && sidebar === s.mode}
            isDisabled={!connected}
            onPress={() => setSidebar(s.mode)}
            size={16}
          />
        ))}
        <Separator className="my-1.5 w-5 opacity-40" />
        <IconButton icon="history" label="Query history" isDisabled={!connected} onPress={() => activeId && openHistory(activeId)} size={16} />
        <IconButton icon="download" label="Export / Import" isDisabled={!connected} onPress={() => activeId && openTransfer(activeId)} size={16} />
        <IconButton icon="braces" label="Chat with database" isDisabled={!connected} onPress={() => activeId && openChat(activeId)} size={16} />
        <IconButton icon="server" label="Server admin" isDisabled={!connected} onPress={() => activeId && openAdmin(activeId)} size={16} />
      </div>
      <div className="mt-auto flex flex-col items-center gap-1">
        <IconButton icon="grid" label="Engine capabilities" active={page.kind === "capabilities"} onPress={goCapabilities} size={16} />
        <IconButton icon="settings" label="Settings" active={page.kind === "settings"} onPress={goSettings} size={16} />
      </div>
    </nav>
  );
}
