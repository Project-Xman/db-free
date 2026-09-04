// SOT: sidebar-switch, sidebar-modes, drag-to-expand
import { useCallback, useState } from "react";
import { useWorkspace, type SidebarMode } from "@/stores/workspace";
import { Resizer } from "@/components/global/Resizer";
import { TablesPanel } from "./TablesPanel";
import { ObjectsPanel } from "@/features/objects/ObjectsPanel";
import { QueriesPanel } from "@/features/queries/QueriesPanel";
import { DocumentsPanel } from "@/features/documents/DocumentsPanel";

function renderPanel(mode: SidebarMode) {
  switch (mode) {
    case "tables":
      return <TablesPanel />;
    case "objects":
      return <ObjectsPanel />;
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

export function Sidebar() {
  const mode = useWorkspace((s) => s.sidebar);
  const [width, setWidth] = useState<number>(() => {
    try {
      const saved = localStorage.getItem("db-free:sidebar-width");
      return saved ? Math.max(180, Math.min(520, Number(saved))) : 260;
    } catch {
      return 260;
    }
  });

  const handleResize = useCallback((delta: number) => {
    setWidth((prev) => {
      const next = Math.max(180, Math.min(520, prev + delta));
      try {
        localStorage.setItem("db-free:sidebar-width", String(next));
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  return (
    <div className="relative flex h-full shrink-0" style={{ width }}>
      <div className="flex h-full w-full min-w-0 flex-col">
        {renderPanel(mode)}
      </div>
      <Resizer direction="horizontal" onResize={handleResize} className="absolute -right-0.5 top-0 bottom-0" />
    </div>
  );
}
