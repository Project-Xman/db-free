// SOT: connection-switcher, quick-connect, sidebar-title
import { Button, Dropdown, Label } from "@heroui/react";
import { useActiveConnection, useWorkspace } from "@/stores/workspace";
import { engineMeta } from "@/lib/engines";
import { EnvDot } from "@/components/global/Badge";
import { Icon } from "@/lib/icons";

const NEW = "__new__";
const MANAGE = "__manage__";

// WHAT:  Sidebar title that doubles as a quick connection switcher: pick another
//        saved connection (connects on demand), add one, or open the list.
export function ConnectionSwitcher({ caption }: { caption: string }) {
  const connection = useActiveConnection();
  const connections = useWorkspace((s) => s.connections);
  const sessions = useWorkspace((s) => s.sessions);
  const select = useWorkspace((s) => s.selectConnection);
  const goPicker = useWorkspace((s) => s.goPicker);
  const goConnections = useWorkspace((s) => s.goConnections);
  if (!connection) return <span className="text-sm font-medium text-foreground">{caption}</span>;

  return (
    <Dropdown>
      <Button variant="ghost" size="sm" className="h-7 min-w-0 gap-1.5 rounded-md px-1.5 text-sm font-medium text-foreground" aria-label={`${caption} — switch connection`}>
        <EnvDot environment={connection.environment} live />
        <span className="truncate">{connection.name}</span>
        <Icon name="chevron-down" size={12} className="text-muted" />
      </Button>
      <Dropdown.Popover className="min-w-[300px]">
        <Dropdown.Menu
          onAction={(key) => {
            const id = String(key);
            if (id === NEW) goPicker();
            else if (id === MANAGE) goConnections();
            else select(id);
          }}
        >
          <Dropdown.Section>
            {connections.map((c) => {
              const meta = engineMeta(c.engine);
              const target = meta.fileBased ? (c.filePath ?? "") : `${c.host ?? ""}${c.port !== null ? `:${c.port}` : ""}${c.database ? `/${c.database}` : ""}`;
              return (
                <Dropdown.Item key={c.id} id={c.id} textValue={`${c.name} ${target}`}>
                  <EnvDot environment={c.environment} live={sessions.includes(c.id)} />
                  <span className="ml-2 flex min-w-0 flex-col">
                    <Label className="truncate">{c.name}</Label>
                    <span className="truncate font-mono text-[10px] text-muted">{meta.label} · {target}</span>
                  </span>
                  {c.id === connection.id ? <Icon name="check" size={13} className="ml-auto pl-3 text-accent" /> : null}
                </Dropdown.Item>
              );
            })}
          </Dropdown.Section>
          <Dropdown.Section>
            <Dropdown.Item id={NEW} textValue="New connection">
              <Icon name="plus" size={13} className="text-muted" />
              <Label className="ml-2">New connection…</Label>
            </Dropdown.Item>
            <Dropdown.Item id={MANAGE} textValue="Manage connections">
              <Icon name="plug" size={13} className="text-muted" />
              <Label className="ml-2">Manage connections…</Label>
            </Dropdown.Item>
          </Dropdown.Section>
        </Dropdown.Menu>
      </Dropdown.Popover>
    </Dropdown>
  );
}
