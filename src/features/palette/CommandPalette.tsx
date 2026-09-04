// SOT: command-palette, cmd-k, quick-actions
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Chip, Kbd, ListBox, Modal, ScrollShadow, SearchField } from "@heroui/react";
import { tableKey, useActiveConnection, useWorkspace } from "@/stores/workspace";
import { Icon, type IconName } from "@/lib/icons";
import { EngineIcon } from "@/components/global/EngineIcon";
import { engineMeta } from "@/lib/engines";
import { TOOL_ORDER, toolMeta, toolsOf } from "@/lib/objects";

const EMPTY_SECTIONS: string[] = [];

interface Action {
  id: string;
  section: string;
  label: string;
  hint?: string;
  icon: IconName;
  /// Replaces `icon` when set (engine logo for connections).
  leading?: ReactNode;
  run: () => void;
}

// WHAT:  ⌘/Ctrl+K palette: switch connection, jump to a table, open any tab, settings.
// WHY:   PRD §5 keyboard-first workflows.
export function CommandPalette() {
  const open = useWorkspace((s) => s.paletteOpen);
  const setOpen = useWorkspace((s) => s.setPaletteOpen);
  const connections = useWorkspace((s) => s.connections);
  const connection = useActiveConnection();
  const catalog = useWorkspace((s) => (connection ? s.catalogs[connection.id] : undefined));
  const savedQueries = useWorkspace((s) => s.savedQueries);
  const sections = useWorkspace((s) => s.settings?.commandMenuSections ?? EMPTY_SECTIONS);
  const store = useWorkspace;
  const [query, setQuery] = useState("");

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen(!store.getState().paletteOpen);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [setOpen, store]);

  const actions = useMemo<Action[]>(() => {
    const s = store.getState();
    const cid = connection?.id ?? null;
    const list: Action[] = [];
    list.push({ id: "new-connection", section: "create", label: "New connection", icon: "plus", run: () => s.goPicker() });
    if (cid) {
      list.push({ id: "new-query", section: "create", label: "New query tab", icon: "terminal", run: () => s.openQuery(cid) });
      list.push({ id: "new-dashboard", section: "create", label: "New dashboard", icon: "columns", run: () => s.setSidebar("dashboards") });
      list.push({ id: "new-workflow", section: "create", label: "New workflow", icon: "play", run: () => s.setSidebar("workflows") });
      list.push({ id: "new-diagram", section: "create", label: "New schema diagram", icon: "view", run: () => s.setSidebar("diagrams") });
      list.push({ id: "history", section: "navigation", label: "Query history", icon: "history", run: () => s.openHistory(cid) });
      list.push({ id: "transfer", section: "navigation", label: "Export / Import", icon: "download", run: () => s.openTransfer(cid) });
      list.push({ id: "erd", section: "navigation", label: "ER diagram of current schema", icon: "view", run: () => s.openErd(cid, s.schemaFilter[cid] ?? null) });
      list.push({ id: "chat", section: "navigation", label: "Chat with database", hint: "Ask questions, explore schema, generate queries", icon: "braces", run: () => s.openChat(cid) });
      list.push({ id: "tables", section: "navigation", label: "Tables sidebar", icon: "table", run: () => s.setSidebar("tables") });
      list.push({ id: "queries", section: "navigation", label: "Saved queries sidebar", icon: "file", run: () => s.setSidebar("queries") });
      list.push({ id: "objects", section: "navigation", label: "Objects sidebar", hint: "Views, functions, triggers, indexes, streams…", icon: "hierarchy", run: () => s.setSidebar("objects") });
      list.push({ id: "admin", section: "navigation", label: "Server admin", hint: "Overview, sessions, users, settings", icon: "server", run: () => s.openAdmin(cid) });
      const engine = s.connections.find((c) => c.id === cid)?.engine;
      const tools = s.sessionInfos[cid]?.tools ?? (engine ? toolsOf(engine) : []);
      for (const tool of TOOL_ORDER) {
        if (!tools.includes(tool) || tool === "stats" || tool === "erd" || tool === "key_browser") continue;
        const meta = toolMeta(tool);
        list.push({ id: `tool:${tool}`, section: "navigation", label: meta.label, hint: meta.hint, icon: meta.icon, run: () => s.openTool(cid, tool) });
      }
    }
    list.push({ id: "connections", section: "navigation", label: "Connections", icon: "plug", run: () => s.goConnections() });
    list.push({ id: "settings", section: "settings", label: "Settings", icon: "settings", run: () => s.goSettings() });
    list.push({ id: "capabilities", section: "settings", label: "Engine capabilities", hint: "Feature × engine matrix", icon: "grid", run: () => s.goCapabilities() });
    for (const c of connections) {
      list.push({ id: `conn:${c.id}`, section: "connections", label: c.name, hint: engineMeta(c.engine).label, icon: "database", leading: <EngineIcon engine={c.engine} size={16} />, run: () => s.selectConnection(c.id) });
    }
    if (cid && catalog) {
      for (const schema of catalog.schemas) {
        for (const t of schema.tables) {
          const ref = { schema: t.schema, name: t.name };
          list.push({ id: `table:${tableKey(ref)}`, section: "tables", label: tableKey(ref), hint: t.kind, icon: "table", run: () => s.openTable(cid, ref) });
        }
      }
    }
    if (cid) {
      for (const q of savedQueries) {
        list.push({ id: `saved:${q.id}`, section: "saved_queries", label: q.name, hint: q.sql.slice(0, 60), icon: "file", run: () => s.openQuery(cid, q.sql, q.name) });
      }
    }
    const order = new Map(sections.map((name, i) => [name, i]));
    return list.sort((a, b) => (order.get(a.section) ?? 99) - (order.get(b.section) ?? 99));
  }, [store, connection, connections, catalog, savedQueries, sections]);

  const needle = query.trim().toLowerCase();
  const visible = (needle.length === 0 ? actions : actions.filter((a) => a.label.toLowerCase().includes(needle) || (a.hint ?? "").toLowerCase().includes(needle))).slice(0, 60);

  return (
    <Modal isOpen={open} onOpenChange={setOpen}>
      <Modal.Backdrop className="backdrop-blur-md bg-backdrop/70">
        <Modal.Container className="items-start pt-[14vh]">
          <Modal.Dialog className="w-full sm:max-w-[620px] glass-modal rounded-2xl overflow-hidden shadow-2xl p-0 border border-border/60">
            <Modal.Body className="p-3">
              <SearchField value={query} onChange={setQuery} aria-label="Search or run commands" autoFocus className="w-full">
                <SearchField.Group className="w-full glass-input rounded-xl bg-surface-secondary/40 px-3.5 h-11 border border-border/40">
                  <SearchField.SearchIcon />
                  <SearchField.Input placeholder="Search commands, tables, queries…" className="w-full text-sm font-sans" />
                  <SearchField.ClearButton />
                </SearchField.Group>
              </SearchField>
              <ScrollShadow className="mt-2 max-h-[50vh] p-1">
                <ListBox
                  aria-label="Commands"
                  className="space-y-0.5"
                  onAction={(key) => {
                    const action = visible.find((a) => a.id === String(key));
                    if (action) {
                      setOpen(false);
                      setQuery("");
                      action.run();
                    }
                  }}
                >
                  {visible.map((a) => (
                    <ListBox.Item key={a.id} id={a.id} textValue={a.label} className="flex items-center rounded-lg px-2.5 py-2 text-xs liquid-hover cursor-default">
                      {a.leading ? <span className="mr-2.5 flex shrink-0 items-center">{a.leading}</span> : <Icon name={a.icon} size={15} className="mr-2.5 text-accent shrink-0" />}
                      <span className="truncate font-medium text-foreground">{a.label}</span>
                      {a.hint ? <span className="ml-2 truncate font-mono text-[10.5px] text-muted/80">{a.hint}</span> : null}
                      <Chip size="sm" variant="soft" className="ml-auto text-[9.5px] uppercase tracking-wider font-medium">
                        {a.section.replace("_", " ")}
                      </Chip>
                    </ListBox.Item>
                  ))}
                </ListBox>
              </ScrollShadow>
              <div className="mt-2.5 flex items-center justify-between border-t border-border/40 px-2 pt-2 text-[11px] text-muted">
                <div className="flex items-center gap-3">
                  <span className="flex items-center gap-1"><Kbd><Kbd.Abbr keyValue="enter" /></Kbd> select</span>
                  <span className="flex items-center gap-1"><Kbd><Kbd.Abbr keyValue="escape" /></Kbd> close</span>
                </div>
                <span className="font-mono text-[10px] text-muted/60">{visible.length} results</span>
              </div>
            </Modal.Body>
          </Modal.Dialog>
        </Modal.Container>
      </Modal.Backdrop>
    </Modal>
  );
}
