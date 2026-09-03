// SOT: command-palette, cmd-k, quick-actions
import { useEffect, useMemo, useState } from "react";
import { Kbd, ListBox, Modal, SearchField } from "@heroui/react";
import { tableKey, useActiveConnection, useWorkspace } from "@/stores/workspace";
import { Icon, type IconName } from "@/lib/icons";
import { engineMeta } from "@/lib/engines";

const EMPTY_SECTIONS: string[] = [];

interface Action {
  id: string;
  section: string;
  label: string;
  hint?: string;
  icon: IconName;
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
      list.push({ id: "tables", section: "navigation", label: "Tables sidebar", icon: "table", run: () => s.setSidebar("tables") });
      list.push({ id: "queries", section: "navigation", label: "Saved queries sidebar", icon: "file", run: () => s.setSidebar("queries") });
    }
    list.push({ id: "connections", section: "navigation", label: "Connections", icon: "plug", run: () => s.goConnections() });
    list.push({ id: "settings", section: "settings", label: "Settings", icon: "settings", run: () => s.goSettings() });
    for (const c of connections) {
      list.push({ id: `conn:${c.id}`, section: "connections", label: c.name, hint: engineMeta(c.engine).label, icon: "database", run: () => s.selectConnection(c.id) });
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
      <Modal.Backdrop>
        <Modal.Container className="items-start pt-[12vh]">
          <Modal.Dialog className="w-full sm:max-w-[640px]">
            <Modal.Body className="p-2">
              <SearchField value={query} onChange={setQuery} aria-label="Search or run commands" autoFocus className="w-full">
                <SearchField.Group className="w-full">
                  <SearchField.SearchIcon />
                  <SearchField.Input placeholder="Search or run commands…" className="w-full" />
                  <SearchField.ClearButton />
                </SearchField.Group>
              </SearchField>
              <ListBox
                aria-label="Commands"
                className="mt-2 max-h-[50vh] overflow-y-auto"
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
                  <ListBox.Item key={a.id} id={a.id} textValue={a.label}>
                    <Icon name={a.icon} size={14} className="mr-2 text-muted" />
                    <span className="truncate">{a.label}</span>
                    {a.hint ? <span className="ml-2 truncate text-xs text-muted">{a.hint}</span> : null}
                    <span className="ml-auto text-[10px] uppercase tracking-wide text-muted">{a.section.replace("_", " ")}</span>
                  </ListBox.Item>
                ))}
              </ListBox>
              <div className="mt-2 flex items-center gap-2 px-2 text-[11px] text-muted">
                <Kbd><Kbd.Abbr keyValue="enter" /></Kbd> run
                <Kbd><Kbd.Abbr keyValue="escape" /></Kbd> close
              </div>
            </Modal.Body>
          </Modal.Dialog>
        </Modal.Container>
      </Modal.Backdrop>
    </Modal>
  );
}
