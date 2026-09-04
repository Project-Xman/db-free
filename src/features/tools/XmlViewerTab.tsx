// SOT: xml-viewer-tab, xml-document-browser
import { useState } from "react";
import { ScrollShadow, Spinner } from "@heroui/react";
import type { ObjectDetail, ObjectRef } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Field } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { IconButton } from "@/components/global/Button";
import { ObjectRow, useObjects } from "@/features/objects/ObjectList";
import { ToolShell, useCollectionOptions } from "./ToolShell";
import { XmlTree } from "./XmlTree";

// WHAT:  XML document browser: the documents of one database / collection
//        on the left, the selected one as a collapsible tree on the right.
// WHERE: src-tauri/src/integrations/mod.rs (objects: Document), src/features/tools/XmlTree.tsx
export function XmlViewerTab({ connectionId }: { connectionId: string }) {
  const options = useCollectionOptions(connectionId);
  const database = useWorkspace((s) => s.sessionInfos[connectionId]?.database ?? "");
  const invalidateObjects = useWorkspace((s) => s.invalidateObjects);
  const [collection, setCollection] = useState(options[0]?.value ?? database);
  const [customCollection, setCustomCollection] = useState("");
  const [refreshKey, setRefreshKey] = useState(0);
  const parent = customCollection.trim().length > 0 ? customCollection.trim() : collection.length > 0 ? collection : database;
  const { objects, error, loading } = useObjects(connectionId, "document", parent.length > 0 ? parent : null, true, refreshKey);
  const [selected, setSelected] = useState<ObjectRef | null>(null);
  const [detail, setDetail] = useState<ObjectDetail | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);

  // WHAT:  Selecting a document loads it; a later selection wins over an
  //        earlier in-flight load by comparing the reference that resolved.
  const select = (reference: ObjectRef) => {
    setSelected(reference);
    setDetail(null);
    setDetailError(null);
    void (async () => {
      try {
        const next = await ipc("load_object", { connectionId, reference });
        setSelected((current) => {
          if (current === reference) setDetail(next);
          return current;
        });
      } catch (raw) {
        const message = normalizeError(raw).message;
        setSelected((current) => {
          if (current === reference) setDetailError(message);
          return current;
        });
      }
    })();
  };

  return (
    <ToolShell
      tool="xml_viewer"
      right={
        <IconButton
          icon="refresh"
          label="Reload documents"
          onPress={() => {
            invalidateObjects(connectionId);
            setRefreshKey((k) => k + 1);
          }}
        />
      }
    >
      <div className="flex h-full min-h-0">
        <aside className="flex w-72 shrink-0 flex-col border-r border-border/40">
          <div className="flex flex-col gap-2 p-3">
            {options.length > 0 ? <AppSelect label="Collection" value={collection} options={options} onChange={setCollection} /> : null}
            <Field label="Path" value={customCollection} onChange={setCustomCollection} optional placeholder={parent || "/db/apps"} mono description="Overrides the selection above." />
          </div>
          <ScrollShadow className="min-h-0 flex-1 px-2 pb-2">
            {loading && objects === null ? (
              <div className="flex items-center gap-2 p-2 text-[11px] text-muted">
                <Spinner size="sm" /> loading…
              </div>
            ) : error !== null ? (
              <p className="p-2 text-xs text-danger">{error}</p>
            ) : objects === null || objects.length === 0 ? (
              <p className="p-2 text-xs text-muted">No documents here.</p>
            ) : (
              objects.map((o) => <ObjectRow key={o.reference.name} connectionId={connectionId} object={o} dense onSelect={select} />)
            )}
          </ScrollShadow>
        </aside>
        <div className="min-h-0 min-w-0 flex-1">
          {selected === null ? (
            <EmptyState icon="xml" title="XML viewer" body="Pick a document on the left to read it as a tree." />
          ) : detailError !== null ? (
            <EmptyState icon="alert" title="Could not load the document" body={detailError} />
          ) : detail === null ? (
            <div className="flex h-full items-center justify-center gap-2 text-xs text-muted">
              <Spinner size="sm" /> loading…
            </div>
          ) : detail.definition === null ? (
            <EmptyState title={selected.name} body="The adapter returned no content for this document." />
          ) : (
            <ScrollShadow className="h-full p-4">
              <XmlTree source={detail.definition} />
            </ScrollShadow>
          )}
        </div>
      </div>
    </ToolShell>
  );
}
