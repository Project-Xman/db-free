// SOT: sidebar-switch, sidebar-modes
import { useWorkspace } from "@/stores/workspace";
import { TablesPanel } from "./TablesPanel";
import { QueriesPanel } from "@/features/queries/QueriesPanel";
import { DocumentsPanel } from "@/features/documents/DocumentsPanel";

export function Sidebar() {
  const mode = useWorkspace((s) => s.sidebar);
  switch (mode) {
    case "tables":
      return <TablesPanel />;
    case "queries":
      return <QueriesPanel />;
    case "dashboards":
      return <DocumentsPanel kind="dashboard" />;
    case "workflows":
      return <DocumentsPanel kind="workflow" />;
    case "diagrams":
      return <DocumentsPanel kind="diagram" />;
  }
}
