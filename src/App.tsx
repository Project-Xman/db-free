// SOT: app-shell, layout, page-routing, tab-routing, settings-css-vars
import { useEffect } from "react";
import { useActiveConnection, useActiveTab, useWorkspace } from "@/stores/workspace";
import { isKeyValueEngine } from "@/lib/engines";
import { fontStack } from "@/lib/fonts";
import { IconRail } from "@/features/shell/IconRail";
import { Sidebar } from "@/features/shell/Sidebar";
import { TabBar } from "@/features/shell/TabBar";
import { ConnectionsPage } from "@/features/connections/ConnectionsPage";
import { ConnectionPicker } from "@/features/connections/ConnectionPicker";
import { ConnectionForm } from "@/features/connections/ConnectionForm";
import { SettingsPage } from "@/features/settings/SettingsPage";
import { CapabilityMatrixPage } from "@/features/engines/CapabilityMatrixPage";
import { QueryPane } from "@/features/editor/QueryPane";
import { TableTab } from "@/features/grid/TableTab";
import { KeyTab } from "@/features/keys/KeyTab";
import { HistoryTab } from "@/features/history/HistoryTab";
import { TransferTab } from "@/features/transfer/TransferTab";
import { ErdTab } from "@/features/diagrams/ErdTab";
import { DocumentTab } from "@/features/documents/DocumentTab";
import { ChatTab } from "@/features/chat/ChatTab";
import { ObjectTab } from "@/features/objects/ObjectTab";
import { AdminTab } from "@/features/admin/AdminTab";
import { ToolTab } from "@/features/tools/ToolTab";
import { PendingChangesPanel } from "@/features/changes/PendingChangesPanel";
import { CommandPalette } from "@/features/palette/CommandPalette";
import { Toaster } from "@/components/global/Toaster";
import { EmptyState } from "@/components/global/EmptyState";
import { RunShortcut } from "@/components/global/Kbd";

/// Stacks used until the settings load (they match globals.css).
const UI_FONT_FALLBACK = '"JetBrains Mono Variable", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';
const EDITOR_FONT_FALLBACK = UI_FONT_FALLBACK;

/// Accent colour plus its oklch hue: the hue also drives every tinted surface
/// (window gradient, glass panels, selection) through the --accent-hue variable.
const ACCENTS: Record<string, { color: string; hue: number }> = {
  blue: { color: "oklch(0.6 0.2 258)", hue: 258 },
  violet: { color: "oklch(0.62 0.2 295)", hue: 295 },
  green: { color: "oklch(0.68 0.17 150)", hue: 150 },
  orange: { color: "oklch(0.7 0.17 55)", hue: 55 },
  rose: { color: "oklch(0.64 0.2 10)", hue: 10 },
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

  // WHAT:  Suppress the webview's own right-click menu (Cut/Copy/Paste, Speech,
  //        Inspect Element) app-wide.
  // WHY:   It is a browser menu in a desktop app: it offers nothing the app can
  //        honour and hides the app's own context menus behind a second click.
  // HOW:   Capture phase, preventDefault only — propagation continues, so the
  //        React handlers that open the app's menus still run.
  useEffect(() => {
    const suppress = (event: MouseEvent) => event.preventDefault();
    document.addEventListener("contextmenu", suppress, { capture: true });
    return () => document.removeEventListener("contextmenu", suppress, { capture: true });
  }, []);

  // WHAT:  Settings that are pure CSS (accent, font sizes) apply as root variables.
  useEffect(() => {
    if (!settings) return;
    const root = document.documentElement.style;
    const accent = ACCENTS[settings.accent] ?? ACCENTS.blue;
    if (accent) {
      root.setProperty("--accent", accent.color);
      root.setProperty("--accent-hue", String(accent.hue));
    }
    root.setProperty("--font-sans", fontStack(settings.uiFont, UI_FONT_FALLBACK));
    root.setProperty("--font-mono", fontStack(settings.editorFont, EDITOR_FONT_FALLBACK));
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
      ) : page.kind === "capabilities" ? (
        <div className="flex h-full min-h-0 min-w-0 flex-1 flex-col">
          <CapabilityMatrixPage />
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
                  isKeyValueEngine(connection.engine) ? <KeyTab key={tab.id} connectionId={tab.connectionId} table={tab.table} /> : <TableTab key={`${tab.id}:${tab.filterKey}`} connectionId={tab.connectionId} table={tab.table} initialFilters={tab.initialFilters} />
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
                ) : tab.kind === "object" ? (
                  <ObjectTab key={tab.id} connectionId={tab.connectionId} reference={tab.reference} />
                ) : tab.kind === "admin" ? (
                  <AdminTab key={tab.id} connectionId={tab.connectionId} />
                ) : tab.kind === "tool" ? (
                  <ToolTab key={tab.id} connectionId={tab.connectionId} tool={tab.tool} />
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
