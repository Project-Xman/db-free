// SOT: app-shell, layout, page-routing, tab-routing, settings-css-vars
import { useEffect } from "react";
import { useActiveConnection, useActiveTab, useWorkspace } from "@/stores/workspace";
import { IconRail } from "@/features/shell/IconRail";
import { Sidebar } from "@/features/shell/Sidebar";
import { TabBar } from "@/features/shell/TabBar";
import { ConnectionsPage } from "@/features/connections/ConnectionsPage";
import { ConnectionPicker } from "@/features/connections/ConnectionPicker";
import { ConnectionForm } from "@/features/connections/ConnectionForm";
import { SettingsPage } from "@/features/settings/SettingsPage";
import { QueryPane } from "@/features/editor/QueryPane";
import { TableTab } from "@/features/grid/TableTab";
import { KeyTab } from "@/features/keys/KeyTab";
import { HistoryTab } from "@/features/history/HistoryTab";
import { TransferTab } from "@/features/transfer/TransferTab";
import { ErdTab } from "@/features/diagrams/ErdTab";
import { DocumentTab } from "@/features/documents/DocumentTab";
import { ChatTab } from "@/features/chat/ChatTab";
import { PendingChangesPanel } from "@/features/changes/PendingChangesPanel";
import { CommandPalette } from "@/features/palette/CommandPalette";
import { Toaster } from "@/components/global/Toaster";
import { EmptyState } from "@/components/global/EmptyState";
import { RunShortcut } from "@/components/global/Kbd";

const ACCENTS: Record<string, string> = {
  blue: "oklch(0.6 0.2 258)",
  violet: "oklch(0.62 0.2 295)",
  green: "oklch(0.68 0.17 150)",
  orange: "oklch(0.7 0.17 55)",
  rose: "oklch(0.64 0.2 10)",
};

export function App() {
  const bootstrap = useWorkspace((s) => s.bootstrap);
  const ready = useWorkspace((s) => s.ready);
  const page = useWorkspace((s) => s.page);
  const settings = useWorkspace((s) => s.settings);
  const connection = useActiveConnection();
  const connected = useWorkspace((s) => (connection ? s.sessions.includes(connection.id) : false));
  const tab = useActiveTab();
  const changesOpen = useWorkspace((s) => s.changesPanelOpen);

  useEffect(() => {
    void bootstrap();
  }, [bootstrap]);

  // WHAT:  Settings that are pure CSS (accent, font sizes) apply as root variables.
  useEffect(() => {
    if (!settings) return;
    const root = document.documentElement.style;
    root.setProperty("--accent", ACCENTS[settings.accent] ?? ACCENTS.blue ?? "");
    root.setProperty("--ui-font-size", `${settings.uiFontSize}px`);
    root.setProperty("--editor-font-size", `${settings.editorFontSize}px`);
    document.body.style.fontSize = `${settings.uiFontSize}px`;
  }, [settings]);

  return (
    <div className="grid-bg flex h-full text-foreground">
      <IconRail />
      {!ready ? null : page.kind === "settings" ? (
        <div className="flex h-full min-h-0 min-w-0 flex-1 flex-col">
          <SettingsPage />
        </div>
      ) : page.kind === "connection-picker" ? (
        <div className="flex h-full min-h-0 min-w-0 flex-1 flex-col">
          <ConnectionPicker />
        </div>
      ) : page.kind === "connection-form" ? (
        <div className="flex h-full min-h-0 min-w-0 flex-1 flex-col">
          <ConnectionForm />
        </div>
      ) : page.kind === "connections" || !connection || !connected ? (
        <div className="flex h-full min-h-0 min-w-0 flex-1 flex-col">
          <ConnectionsPage />
        </div>
      ) : (
        <>
          <Sidebar />
          <main className="flex h-full min-h-0 min-w-0 flex-1 flex-col">
            <TabBar />
            <div className="flex min-h-0 flex-1">
              <div className="min-w-0 flex-1">
                {tab === null ? (
                  <EmptyState icon="table" title="Pick a table" body="Select a table on the left to browse it, or open a query tab. Run with" action={<RunShortcut />} />
                ) : tab.kind === "table" ? (
                  connection.engine === "redis" ? <KeyTab key={tab.id} connectionId={tab.connectionId} table={tab.table} /> : <TableTab key={`${tab.id}:${tab.filterKey}`} connectionId={tab.connectionId} table={tab.table} initialFilters={tab.initialFilters} />
                ) : tab.kind === "query" ? (
                  <QueryPane key={tab.id} tabId={tab.id} connection={connection} seedSql={tab.seedSql} />
                ) : tab.kind === "history" ? (
                  <HistoryTab key={tab.id} connectionId={tab.connectionId} />
                ) : tab.kind === "transfer" ? (
                  <TransferTab key={tab.id} connectionId={tab.connectionId} />
                ) : tab.kind === "erd" ? (
                  <ErdTab key={tab.id} connectionId={tab.connectionId} schema={tab.schema} />
                ) : tab.kind === "chat" ? (
                  <ChatTab key={tab.id} connectionId={tab.connectionId} />
                ) : (
                  <DocumentTab key={tab.id} kind={tab.documentKind} documentId={tab.documentId} connectionId={tab.connectionId} />
                )}
              </div>
              {changesOpen ? <PendingChangesPanel connectionId={connection.id} /> : null}
            </div>
          </main>
        </>
      )}
      <CommandPalette />
      <Toaster />
    </div>
  );
}
