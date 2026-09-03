// SOT: connections-page, connection-list, home-screen
import { Button, Card, Spinner } from "@heroui/react";
import { engineMeta } from "@/lib/engines";
import { environmentMeta } from "@/lib/environments";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { EnvBadge, EnvDot } from "@/components/global/Badge";
import { EmptyState } from "@/components/global/EmptyState";
import { Icon } from "@/lib/icons";

// WHAT:  Home: saved connections. Click a row to connect and enter the workspace.
export function ConnectionsPage() {
  const connections = useWorkspace((s) => s.connections);
  const sessions = useWorkspace((s) => s.sessions);
  const connecting = useWorkspace((s) => s.connecting);
  const select = useWorkspace((s) => s.selectConnection);
  const openForm = useWorkspace((s) => s.openForm);
  const disconnect = useWorkspace((s) => s.disconnect);

  return (
    <div className="grid-bg flex h-full min-h-0 flex-1 flex-col overflow-y-auto">
      <div className="drag-region h-10 shrink-0" data-tauri-drag-region />
      <div className="mx-auto w-full max-w-2xl px-6 pb-10">
        <div className="mb-6 flex items-center justify-between">
          <div>
            <h1 className="text-lg font-semibold text-foreground">Connections</h1>
            <p className="text-xs text-muted">Credentials are encrypted at rest; the key lives in your OS keychain.</p>
          </div>
          <Button onPress={() => openForm()}>
            <Icon name="plus" size={14} />
            New connection
          </Button>
        </div>

        {connections.length === 0 ? (
          <Card className="w-full">
            <Card.Content>
              <EmptyState
                icon="plug"
                title="No connections yet"
                body="Add a PostgreSQL or SQLite connection to start browsing tables and running SQL."
                action={<Button onPress={() => openForm()}>Add connection</Button>}
              />
            </Card.Content>
          </Card>
        ) : (
          <Card className="w-full p-0">
            <Card.Content className="p-0">
              <ul className="divide-y divide-separator">
                {connections.map((c) => {
                  const live = sessions.includes(c.id);
                  const meta = engineMeta(c.engine);
                  const target = meta.fileBased ? (c.filePath ?? "") : `${c.host ?? ""}${c.port !== null ? `:${c.port}` : ""}${c.database ? `/${c.database}` : ""}`;
                  return (
                    <li key={c.id} className="group flex items-center gap-3 px-4 py-3 hover:bg-surface-secondary">
                      <span className="flex size-9 items-center justify-center rounded-lg bg-surface-tertiary text-muted">
                        <Icon name={meta.fileBased ? "file" : "database"} size={16} />
                      </span>
                      <button type="button" onClick={() => select(c.id)} className="min-w-0 flex-1 text-left">
                        <span className="flex items-center gap-2">
                          <EnvDot environment={c.environment} live={live} />
                          <span className="truncate text-sm font-medium text-foreground">{c.name}</span>
                          <EnvBadge environment={c.environment} readOnly={c.readOnly} />
                        </span>
                        <span className="mt-0.5 flex items-center gap-2 text-xs text-muted">
                          <span>{meta.label}</span>
                          <span className="truncate font-mono">{target}</span>
                          {live ? <span className={environmentMeta(c.environment).text}>connected</span> : null}
                        </span>
                      </button>
                      {connecting === c.id ? <Spinner size="sm" /> : null}
                      <span className="flex items-center opacity-0 group-hover:opacity-100">
                        <IconButton icon="pencil" label="Edit" onPress={() => openForm(c.id)} />
                        {live ? <IconButton icon="x" label="Disconnect" onPress={() => void disconnect(c.id)} /> : null}
                      </span>
                      <Button size="sm" variant={live ? "secondary" : "primary"} onPress={() => select(c.id)}>
                        {live ? "Open" : "Connect"}
                      </Button>
                    </li>
                  );
                })}
              </ul>
            </Card.Content>
          </Card>
        )}
      </div>
    </div>
  );
}
