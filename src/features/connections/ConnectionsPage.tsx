// SOT: connections-page, connection-list, home-screen
import { Alert, Button, Card, Chip, ScrollShadow, Spinner } from "@heroui/react";
import { engineMeta } from "@/lib/engines";
import { cn } from "@/lib/cn";
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
    <div className="grid-bg flex h-full min-h-0 flex-1 flex-col">
      <div className="drag-region h-11 shrink-0" data-tauri-drag-region />
      <ScrollShadow className="min-h-0 flex-1">
        <div className="mx-auto w-full max-w-2xl px-6 pb-12">
          <div className="mb-6 flex items-center justify-between">
            <div className="flex items-center gap-2.5">
              <h1 className="text-xl font-bold tracking-tight text-foreground">Connections</h1>
              <Chip size="sm" variant="soft" color="accent" className="font-medium">
                {connections.length} saved
              </Chip>
            </div>
            <Button onPress={() => openForm()} className="glass-pill bg-accent text-accent-foreground font-semibold shadow-md shadow-accent/25 liquid-hover">
              <Icon name="plus" size={14} />
              New Connection
            </Button>
          </div>

          <Alert status="accent" className="mb-6 glass-card rounded-xl border-border/40">
            <Alert.Indicator />
            <Alert.Content>
              <Alert.Title className="text-xs font-semibold">Keychain Encryption</Alert.Title>
              <Alert.Description className="text-xs text-muted">
                Credentials encrypted at rest with AES-256-GCM via your native OS Keychain.
              </Alert.Description>
            </Alert.Content>
          </Alert>

        {connections.length === 0 ? (
          <Card className="w-full glass-card rounded-2xl border-border/60">
            <Card.Content>
              <EmptyState
                icon="plug"
                title="No connections yet"
                body="Add a PostgreSQL, SQLite, MySQL, Redis, or MongoDB connection to start browsing tables and running queries."
                action={<Button onPress={() => openForm()} className="glass-pill bg-accent text-accent-foreground font-semibold">Add connection</Button>}
              />
            </Card.Content>
          </Card>
        ) : (
          <div className="flex flex-col gap-2.5">
            {connections.map((c) => {
              const live = sessions.includes(c.id);
              const meta = engineMeta(c.engine);
              const target = meta.fileBased ? (c.filePath ?? "") : `${c.host ?? ""}${c.port !== null ? `:${c.port}` : ""}${c.database ? `/${c.database}` : ""}`;
              return (
                <Card
                  key={c.id}
                  className="group relative flex flex-row items-center gap-3.5 rounded-xl glass-card px-4 py-3.5 glass-card-hover border-border/40"
                >
                  <Card.Content className="flex flex-row items-center gap-3.5 w-full p-0">
                    <span className="flex size-10 items-center justify-center rounded-xl bg-surface-tertiary/70 text-accent shadow-xs border border-border/40 shrink-0">
                      <Icon name={meta.fileBased ? "file" : "database"} size={18} />
                    </span>
                    <Button
                      variant="ghost"
                      onPress={() => select(c.id)}
                      className="h-auto min-w-0 flex-1 flex-col items-start justify-start p-0 bg-transparent hover:bg-transparent text-left"
                    >
                      <span className="flex items-center gap-2">
                        <EnvDot environment={c.environment} live={live} />
                        <span className="truncate text-[14px] font-semibold text-foreground tracking-tight">{c.name}</span>
                        <EnvBadge environment={c.environment} readOnly={c.readOnly} />
                      </span>
                      <span className="mt-1 flex items-center gap-2 text-xs text-muted">
                        <span className="rounded bg-surface-secondary/70 px-1.5 py-0.5 text-[10px] font-medium text-foreground/80">{meta.label}</span>
                        <span className="truncate font-mono text-[11px] opacity-80">{target}</span>
                        {live ? <span className="text-[11px] font-medium text-success">● connected</span> : null}
                      </span>
                    </Button>
                    {connecting === c.id ? <Spinner size="sm" /> : null}
                    <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <IconButton icon="pencil" label="Edit" onPress={() => openForm(c.id)} />
                      {live ? <IconButton icon="x" label="Disconnect" onPress={() => void disconnect(c.id)} /> : null}
                    </div>
                    <Button
                      size="sm"
                      variant={live ? "secondary" : "primary"}
                      className={cn("rounded-lg font-medium liquid-hover", live ? "glass-pill text-foreground" : "bg-accent text-accent-foreground shadow-xs")}
                      onPress={() => select(c.id)}
                    >
                      {live ? "Open" : "Connect"}
                    </Button>
                  </Card.Content>
                </Card>
              );
            })}
          </div>
        )}
        </div>
      </ScrollShadow>
    </div>
  );
}
