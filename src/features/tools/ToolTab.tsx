// SOT: tool-tab, tool-dispatch, playground-routing
import { Button } from "@heroui/react";
import type { Tool } from "@/lib/bindings";
import { useWorkspace } from "@/stores/workspace";
import { EmptyState } from "@/components/global/EmptyState";
import { AdminTab } from "@/features/admin/AdminTab";
import { ErdTab } from "@/features/diagrams/ErdTab";
import { VectorSearchTab } from "./VectorSearchTab";
import { SearchTab } from "./SearchTab";
import { MetricsTab } from "./MetricsTab";
import { MessageViewerTab } from "./MessageViewerTab";
import { PipelineTab } from "./PipelineTab";
import { LedgerTab } from "./LedgerTab";
import { GraphViewTab } from "./GraphViewTab";
import { PubSubTab } from "./PubSubTab";
import { XmlViewerTab } from "./XmlViewerTab";

// WHAT:  Routes a `Tool` to its playground. Tools that already had a home
//        (server overview, ER diagram, key browser) reuse it.
// WHERE: src/lib/objects.ts (TOOLS), src/stores/workspace.ts (openTool)
export function ToolTab({ connectionId, tool }: { connectionId: string; tool: Tool }) {
  const setSidebar = useWorkspace((s) => s.setSidebar);
  switch (tool) {
    case "stats":
      return <AdminTab connectionId={connectionId} />;
    case "erd":
      return <ErdTab connectionId={connectionId} schema={null} />;
    case "key_browser":
      return <EmptyState icon="hash" title="Key browser" body="Keys are listed in the Tables sidebar; open one to inspect and edit it by type." action={<Button size="sm" onPress={() => setSidebar("tables")}>Show keys</Button>} />;
    case "vector_search":
      return <VectorSearchTab connectionId={connectionId} />;
    case "search_playground":
      return <SearchTab connectionId={connectionId} />;
    case "metrics_explorer":
      return <MetricsTab connectionId={connectionId} />;
    case "message_viewer":
      return <MessageViewerTab connectionId={connectionId} />;
    case "pipeline_builder":
      return <PipelineTab connectionId={connectionId} />;
    case "ledger_history":
      return <LedgerTab connectionId={connectionId} />;
    case "graph_view":
      return <GraphViewTab connectionId={connectionId} />;
    case "pub_sub":
      return <PubSubTab connectionId={connectionId} />;
    case "xml_viewer":
      return <XmlViewerTab connectionId={connectionId} />;
  }
}
