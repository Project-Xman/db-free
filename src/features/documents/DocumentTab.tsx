// SOT: document-tab, document-dispatch
import { useEffect } from "react";
import { Spinner } from "@heroui/react";
import type { DocumentKind } from "@/lib/bindings";
import { useWorkspace } from "@/stores/workspace";
import { DashboardTab } from "@/features/dashboards/DashboardTab";
import { WorkflowTab } from "@/features/workflows/WorkflowTab";
import { DesignerTab } from "@/features/diagrams/DesignerTab";
import { EmptyState } from "@/components/global/EmptyState";

// WHAT:  Resolves a document tab to its editor; loads the kind's list if needed.
export function DocumentTab({ kind, documentId, connectionId }: { kind: DocumentKind; documentId: string; connectionId: string | null }) {
  const docs = useWorkspace((s) => s.documents[kind]);
  const loadDocuments = useWorkspace((s) => s.loadDocuments);
  const doc = docs.find((d) => d.id === documentId);

  useEffect(() => {
    if (!doc) void loadDocuments(kind);
  }, [doc, kind, loadDocuments]);

  if (!doc) {
    return docs.length === 0 ? <div className="flex h-full items-center justify-center"><Spinner size="sm" /></div> : <EmptyState title="Document not found" body="It may have been deleted." />;
  }
  switch (kind) {
    case "dashboard":
      return <DashboardTab key={doc.id} document={doc} connectionId={doc.connectionId ?? connectionId} />;
    case "workflow":
      return <WorkflowTab key={doc.id} document={doc} />;
    case "diagram":
      return <DesignerTab key={doc.id} document={doc} />;
  }
}
