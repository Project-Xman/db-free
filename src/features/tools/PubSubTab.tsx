// SOT: pubsub-tab, channel-list, publish-form
import { useState } from "react";
import { Button, ScrollShadow, Spinner, TextArea } from "@heroui/react";
import { ipc, normalizeError } from "@/lib/ipc";
import { formatCell } from "@/lib/format";
import { Icon } from "@/lib/icons";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { Field } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { ObjectRow, useObjects } from "@/features/objects/ObjectList";
import { ToolBody, ToolShell } from "./ToolShell";

// WHAT:  Redis-style publish/subscribe: the active channels (with subscriber
//        counts from the adapter) and a publish form. Publishing goes through
//        execute_query (`PUBLISH channel message`), so a read-only connection
//        cannot publish.
// WHERE: src-tauri/src/integrations/redis.rs (objects: Channel), src/features/tools/ToolTab.tsx
export function PubSubTab({ connectionId }: { connectionId: string }) {
  const showInfo = useWorkspace((s) => s.showInfo);
  const invalidateObjects = useWorkspace((s) => s.invalidateObjects);
  const [refreshKey, setRefreshKey] = useState(0);
  const { objects, error: listError, loading } = useObjects(connectionId, "channel", null, true, refreshKey);
  const [channel, setChannel] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [log, setLog] = useState<{ at: string; channel: string; receivers: string }[]>([]);

  const refresh = () => {
    invalidateObjects(connectionId);
    setRefreshKey((k) => k + 1);
  };

  const quote = (text: string) => `"${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;

  const publish = async () => {
    if (channel.trim().length === 0) {
      setError("Enter a channel.");
      return;
    }
    setRunning(true);
    setError(null);
    try {
      const outcome = await ipc("execute_query", { connectionId, sql: `PUBLISH ${quote(channel.trim())} ${quote(message)}`, confirmDestructive: false, maxRows: 1 });
      const first = outcome.statements[0];
      const cell = first?.kind === "rows" ? first.result.rows[0]?.[0] : undefined;
      const receivers = cell !== undefined ? formatCell(cell).text : first?.kind === "affected" ? String(first.rowsAffected) : "?";
      setLog((l) => [{ at: new Date().toLocaleTimeString(), channel: channel.trim(), receivers }, ...l].slice(0, 50));
      showInfo(`Published to ${channel.trim()} (${receivers} receiver${receivers === "1" ? "" : "s"}).`);
      refresh();
    } catch (raw) {
      setError(normalizeError(raw).message);
    } finally {
      setRunning(false);
    }
  };

  return (
    <ToolShell tool="pub_sub" right={<IconButton icon="refresh" label="Reload channels" onPress={refresh} />}>
      <ToolBody
        form={
          <>
            <Field label="Channel" value={channel} onChange={setChannel} placeholder="events:orders" mono />
            <div className="flex flex-col gap-1">
              <span className="text-sm font-medium text-foreground">Message</span>
              <TextArea aria-label="Message" value={message} onChange={(e) => setMessage(e.target.value)} spellCheck={false} className="min-h-28 font-mono text-[12px]" placeholder='{"order": 42, "status": "paid"}' />
            </div>
            <Button onPress={() => void publish()} isDisabled={running}>
              {running ? <Spinner size="sm" /> : <Icon name="send" size={13} />}
              Publish
            </Button>
            {error !== null ? <p className="text-xs text-danger">{error}</p> : null}
            {log.length > 0 ? (
              <ul className="flex flex-col gap-0.5 border-t border-border/40 pt-2 font-mono text-[10px] text-muted">
                {log.map((entry, i) => (
                  <li key={`${entry.at}-${i}`} className="flex gap-2">
                    <span>{entry.at}</span>
                    <span className="truncate text-foreground">{entry.channel}</span>
                    <span className="ml-auto">{entry.receivers} rcv</span>
                  </li>
                ))}
              </ul>
            ) : null}
          </>
        }
      >
        <div className="flex h-full min-h-0 flex-col">
          <div className="flex h-9 shrink-0 items-center gap-2 border-b border-border/40 px-3 text-[11px] text-muted">
            Active channels {loading ? <Spinner size="sm" /> : null}
            <span className="ml-auto">Subscribers are counted server-side; a channel appears once someone subscribes.</span>
          </div>
          <ScrollShadow className="min-h-0 flex-1 p-2">
            {listError !== null ? (
              <EmptyState icon="alert" title="Could not list channels" body={listError} />
            ) : objects !== null && objects.length === 0 ? (
              <EmptyState icon="rss" title="No active channels" body="Nothing is subscribed right now. Publish anyway: Redis delivers to whoever is listening." />
            ) : (
              (objects ?? []).map((o) => <ObjectRow key={o.reference.name} connectionId={connectionId} object={o} onSelect={(ref) => setChannel(ref.name)} />)
            )}
          </ScrollShadow>
        </div>
      </ToolBody>
    </ToolShell>
  );
}
