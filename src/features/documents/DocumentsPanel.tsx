// SOT: documents-panel, dashboards-sidebar, workflows-sidebar, diagrams-sidebar
import { useEffect } from "react";
import { Button, Chip, ScrollShadow } from "@heroui/react";
import type { Document, DocumentBody, DocumentKind } from "@/lib/bindings";
import { normalizeError } from "@/lib/ipc";
import { useActiveConnection, useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Icon, type IconName } from "@/lib/icons";
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

// WHAT:  Sidebar listing one document kind, grouped as "Untagged" like DB Manager.
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
    <aside className="flex h-full w-full min-w-0 flex-col glass-sidebar select-none">
      <div className="drag-region flex h-11 shrink-0 items-center gap-1.5 px-3 border-b border-border/40" data-tauri-drag-region>
        <ConnectionSwitcher caption={meta.title} />
        <div className="drag-region h-full min-w-4 flex-1" data-tauri-drag-region />
        <span className="flex items-center gap-0.5">
          <IconButton icon="refresh" label="Refresh" onPress={() => void loadDocuments(kind)} />
          <IconButton icon="plus" label={`New ${kind}`} onPress={() => void create()} />
        </span>
      </div>
      <div className="flex items-center px-3.5 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted/80">
        <span>Untagged</span>
        <Chip size="sm" variant="soft" className="ml-auto font-mono text-[9.5px]">
          {docs.length}
        </Chip>
      </div>
      <ScrollShadow className="min-h-0 flex-1 px-1.5 py-1">
        {docs.length === 0 ? <p className="px-3 py-4 text-xs text-muted">{meta.empty}</p> : null}
        {docs.map((d) => {
          const active = activeTabId === `doc:${kind}:${d.id}`;
          return (
            <div key={d.id} className={cn("group flex h-8 items-center gap-2 rounded-lg px-2 text-[12.5px] liquid-hover", active ? "glass-pill text-accent" : "text-muted hover:bg-surface-secondary/70 hover:text-foreground")}>
              <Button
                variant="ghost"
                size="sm"
                onPress={() => openDocument(kind, d.id, d.connectionId)}
                className="flex h-auto min-w-0 flex-1 items-center justify-start gap-2 p-0 text-left bg-transparent hover:bg-transparent"
              >
                <Icon name={meta.icon} size={13} className={cn("shrink-0", active ? "text-accent" : "text-muted")} />
                <span className="truncate font-medium">{d.name}</span>
              </Button>
              <span className="opacity-0 group-hover:opacity-100 transition-opacity">
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
      </ScrollShadow>
    </aside>
  );
}
