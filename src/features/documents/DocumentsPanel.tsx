// SOT: documents-panel, dashboards-sidebar, workflows-sidebar, diagrams-sidebar
import { useEffect } from "react";
import type { Document, DocumentBody, DocumentKind } from "@/lib/bindings";
import { normalizeError } from "@/lib/ipc";
import { useActiveConnection, useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Icon, type IconName } from "@/lib/icons";
import { isMac } from "@/components/global/Kbd";
import { cn } from "@/lib/cn";
import { ConnectionSwitcher } from "@/features/shell/ConnectionSwitcher";

const META: Record<DocumentKind, { title: string; icon: IconName; empty: string }> = {
  dashboard: { title: "Dashboards", icon: "columns", empty: "No dashboards yet. Create one to chart query results." },
  workflow: { title: "Workflows", icon: "play", empty: "No workflows yet. Chain SQL steps and run them in order." },
  diagram: { title: "Schema Diagrams", icon: "view", empty: "No diagrams yet. Design tables and relations on a canvas." },
};

function emptyBody(kind: DocumentKind): DocumentBody {
  switch (kind) {
    case "dashboard":
      return { kind: "dashboard", data: { widgets: [], variables: [], refreshSeconds: 0 } };
    case "workflow":
      return { kind: "workflow", data: { steps: [] } };
    case "diagram":
      return { kind: "diagram", data: { tables: [], relations: [] } };
  }
}

// WHAT:  Sidebar listing one document kind, grouped as "Untagged" like DB Pro.
export function DocumentsPanel({ kind }: { kind: DocumentKind }) {
  const connection = useActiveConnection();
  const docs = useWorkspace((s) => s.documents[kind]);
  const loadDocuments = useWorkspace((s) => s.loadDocuments);
  const saveDocument = useWorkspace((s) => s.saveDocument);
  const deleteDocument = useWorkspace((s) => s.deleteDocument);
  const openDocument = useWorkspace((s) => s.openDocument);
  const activeTabId = useWorkspace((s) => s.activeTabId);
  const showError = useWorkspace((s) => s.showError);
  const meta = META[kind];

  useEffect(() => {
    void (async () => {
      try {
        await loadDocuments(kind);
      } catch (raw) {
        showError(normalizeError(raw));
      }
    })();
  }, [kind, loadDocuments, showError]);

  const create = async () => {
    const name = `${meta.title.replace(/s$/, "")} ${docs.length + 1}`;
    const doc: Document = { id: "", kind, connectionId: connection?.id ?? null, name, body: emptyBody(kind), tags: [], createdAt: "", updatedAt: "" };
    try {
      const saved = await saveDocument(doc);
      openDocument(kind, saved.id, saved.connectionId);
    } catch (raw) {
      showError(normalizeError(raw));
    }
  };

  return (
    <aside className="flex w-[280px] shrink-0 flex-col border-r border-border bg-surface">
      <div className={cn("drag-region flex h-10 shrink-0 items-center gap-1 pr-2", isMac() ? "pl-9" : "pl-3")} data-tauri-drag-region>
        <ConnectionSwitcher caption={meta.title} />
        <div className="drag-region h-full min-w-4 flex-1" data-tauri-drag-region />
        <span className="flex items-center">
          <IconButton icon="refresh" label="Refresh" onPress={() => void loadDocuments(kind)} />
          <IconButton icon="plus" label={`New ${kind}`} onPress={() => void create()} />
        </span>
      </div>
      <div className="flex items-center px-3 py-1 text-xs text-muted">
        <span>Untagged</span>
        <span className="ml-auto">{docs.length}</span>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        {docs.length === 0 ? <p className="px-3 py-2 text-xs text-muted">{meta.empty}</p> : null}
        {docs.map((d) => {
          const active = activeTabId === `doc:${kind}:${d.id}`;
          return (
            <div key={d.id} className={cn("group flex h-8 items-center gap-2 pr-2 pl-3 text-[13px]", active ? "bg-surface-tertiary text-foreground" : "text-muted hover:bg-surface-secondary hover:text-foreground")}>
              <button type="button" onClick={() => openDocument(kind, d.id, d.connectionId)} className="flex min-w-0 flex-1 items-center gap-2 text-left">
                <Icon name={meta.icon} size={13} className="shrink-0" />
                <span className="truncate">{d.name}</span>
              </button>
              <span className="opacity-0 group-hover:opacity-100">
                <IconButton
                  icon="trash"
                  label="Delete"
                  onPress={() => {
                    void (async () => {
                      try {
                        await deleteDocument(kind, d.id);
                      } catch (raw) {
                        showError(normalizeError(raw));
                      }
                    })();
                  }}
                />
              </span>
            </div>
          );
        })}
      </div>
    </aside>
  );
}
