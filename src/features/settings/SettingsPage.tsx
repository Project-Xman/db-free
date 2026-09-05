// SOT: settings-page, preferences-ui, ai-settings-ui, shortcuts-list
import { useState } from "react";
import { Button, Card, ScrollShadow, Separator } from "@heroui/react";
import type { AiProvider, AppSettings, ExecutionMode, RunScope, UpdateStatus } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Field, Toggle } from "@/components/global/Field";
import { Icon, type IconName } from "@/lib/icons";
import { cn } from "@/lib/cn";
import { EDITOR_FONT_OPTIONS, UI_FONT_OPTIONS } from "@/lib/fonts";

type Section = "general" | "themes" | "fonts" | "grid" | "editor" | "shortcuts" | "ai" | "security" | "updates" | "advanced";

const SECTIONS: readonly { id: Section; label: string; icon: IconName }[] = [
  { id: "general", label: "General", icon: "settings" },
  { id: "themes", label: "Themes", icon: "eye" },
  { id: "fonts", label: "Fonts", icon: "text" },
  { id: "grid", label: "Data Grid", icon: "table" },
  { id: "editor", label: "Editor", icon: "terminal" },
  { id: "shortcuts", label: "Shortcuts", icon: "hash" },
  { id: "ai", label: "AI", icon: "braces" },
  { id: "security", label: "Security & Privacy", icon: "lock" },
  { id: "updates", label: "Updates", icon: "download" },
  { id: "advanced", label: "Advanced", icon: "columns" },
];

const ACCENTS = [
  { value: "blue", label: "Blue" },
  { value: "violet", label: "Violet" },
  { value: "green", label: "Green" },
  { value: "orange", label: "Orange" },
  { value: "rose", label: "Rose" },
] satisfies readonly { value: string; label: string }[];

const PROVIDERS: readonly { value: AiProvider; label: string }[] = [
  { value: "none", label: "Off" },
  { value: "anthropic", label: "Anthropic" },
  { value: "openai", label: "OpenAI" },
  { value: "openrouter", label: "OpenRouter" },
  { value: "ollama", label: "Ollama (local)" },
];

const SHORTCUTS: readonly { keys: string; action: string }[] = [
  { keys: "⌘/Ctrl + K", action: "Command palette" },
  { keys: "⌘/Ctrl + Enter", action: "Run query" },
  { keys: "⌘/Ctrl + S", action: "Commit pending changes" },
  { keys: "⇧ + ⌥ + F", action: "Format SQL" },
  { keys: "Esc", action: "Close dialogs / cancel edit" },
  { keys: "Enter (in cell)", action: "Stage edit" },
  { keys: "Middle click (tab)", action: "Close tab" },
];

// WHAT:  Settings page mirroring DB Manager's sections; edits a draft of AppSettings.
//        A floating dock appears while the draft differs from the saved settings
//        (Reset / Save). The AI key is written separately and never echoed.
export function SettingsPage() {
  const settings = useWorkspace((s) => s.settings);
  if (!settings) return null;
  return <SettingsBody key={settings.ai.hasApiKey ? "k" : "n"} initial={settings} />;
}

function SettingsBody({ initial }: { initial: AppSettings }) {
  const saveSettings = useWorkspace((s) => s.saveSettings);
  const goConnections = useWorkspace((s) => s.goConnections);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const [section, setSection] = useState<Section>("general");
  const [draft, setDraft] = useState<AppSettings | null>(initial);
  const [apiKey, setApiKey] = useState("");
  const [clearKey, setClearKey] = useState(false);
  const [saving, setSaving] = useState(false);

  if (!draft) return null;
  const dirty = !sameSettings(draft, initial) || apiKey.trim().length > 0 || clearKey;
  const reset = () => {
    setDraft(initial);
    setApiKey("");
    setClearKey(false);
  };
  const patch = (partial: Partial<AppSettings>) => setDraft((d) => (d ? { ...d, ...partial } : d));
  const patchAi = (partial: Partial<AppSettings["ai"]>) => setDraft((d) => (d ? { ...d, ai: { ...d.ai, ...partial } } : d));

  const save = async () => {
    setSaving(true);
    try {
      await saveSettings(draft, clearKey ? "" : apiKey.trim().length > 0 ? apiKey.trim() : null);
      setApiKey("");
      setClearKey(false);
      showInfo("Settings saved.");
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="grid-bg relative flex h-full min-h-0 flex-1 flex-col">
      <div className="drag-region flex h-11 app-pad-x shrink-0 items-center gap-2 border-b border-border/40 glass-header" data-tauri-drag-region>
        <Button variant="ghost" size="sm" onPress={goConnections} className="rounded-lg text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover">
          <Icon name="chevron-left" size={14} />
          Back
        </Button>
        <span className="text-sm font-semibold text-foreground tracking-tight" data-tauri-drag-region>
          Preferences
        </span>
        <div className="drag-region h-full flex-1" data-tauri-drag-region />
      </div>
      <div className="flex min-h-0 flex-1">
        <nav className="w-48 shrink-0 py-4 px-2.5 glass-sidebar space-y-0.5" aria-label="Settings sections">
          {SECTIONS.map((s) => (
            <Button
              key={s.id}
              variant="ghost"
              size="sm"
              onPress={() => setSection(s.id)}
              className={cn(
                "flex h-8 w-full items-center justify-start gap-2.5 rounded-lg px-2.5 text-left text-[12.5px] font-medium liquid-hover",
                section === s.id ? "glass-pill text-accent" : "text-muted hover:bg-surface-secondary/60 hover:text-foreground",
              )}
            >
              <Icon name={s.icon} size={14} />
              {s.label}
            </Button>
          ))}
        </nav>
        <ScrollShadow className="min-h-0 flex-1">
          <div className="mx-auto flex w-full max-w-[720px] flex-col gap-4 px-8 py-6">
            {section === "general" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">General</h2>
                <Row title="Confirm destructive statements" body="DROP, TRUNCATE and DELETE/UPDATE without WHERE ask before running.">
                  <Toggle checked={draft.confirmDestructive} onChange={(v) => patch({ confirmDestructive: v })} label="" />
                </Row>
                <Row title="Default row density" body="Applies to new grids; the rail button cycles it per session.">
                  <AppSelect ariaLabel="Density" value={draft.gridDensity} options={[{ value: "compact", label: "Compact" }, { value: "cozy", label: "Cozy" }, { value: "comfortable", label: "Comfortable" }]} onChange={(v) => patch({ gridDensity: v })} className="w-44" />
                </Row>
              </>
            ) : null}
            {section === "themes" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Themes</h2>
                <p className="text-xs text-muted">The app is dark-only by design. Pick the accent colour.</p>
                <Row title="Accent" body="Buttons, selections, links.">
                  <AppSelect ariaLabel="Accent" value={draft.accent} options={ACCENTS} onChange={(v) => patch({ accent: v })} className="w-44" />
                </Row>
              </>
            ) : null}
            {section === "fonts" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Fonts</h2>
                <p className="text-xs text-muted">Every family is bundled with the app, so the picker works offline.</p>
                <Row title="Interface font" body="Menus, panels, labels.">
                  <AppSelect ariaLabel="Interface font" value={draft.uiFont} options={UI_FONT_OPTIONS} onChange={(v) => patch({ uiFont: v })} className="w-44" />
                </Row>
                <Row title="Interface font size" body="11–16 px.">
                  <Field label="" type="number" value={String(draft.uiFontSize)} onChange={(v) => patch({ uiFontSize: clamp(Number(v), 11, 16) })} className="w-24 [&_label]:hidden" />
                </Row>
                <Row title="Editor font" body="Query editor, grids and values; monospace only.">
                  <AppSelect ariaLabel="Editor font" value={draft.editorFont} options={EDITOR_FONT_OPTIONS} onChange={(v) => patch({ editorFont: v })} className="w-44" />
                </Row>
                <Row title="Editor font size" body="11–20 px.">
                  <Field label="" type="number" value={String(draft.editorFontSize)} onChange={(v) => patch({ editorFontSize: clamp(Number(v), 11, 20) })} className="w-24 [&_label]:hidden" />
                </Row>
              </>
            ) : null}
            {section === "grid" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Data Grid</h2>
                <Row title="Alternating row colours" body="Zebra striping in every grid, for reading long rows across.">
                  <Toggle checked={draft.alternatingRows} onChange={(v) => patch({ alternatingRows: v })} label="" />
                </Row>
                <Row title="Remember table sort and filter" body="Reopen each table with the sort and filters you last used, across restarts.">
                  <Toggle checked={draft.rememberTableState} onChange={(v) => patch({ rememberTableState: v })} label="" />
                </Row>
                <Row title="Table column preview" body="Expandable column list under each table in the sidebar.">
                  <Toggle checked={draft.columnPreview} onChange={(v) => patch({ columnPreview: v })} label="" />
                </Row>
                <Row title="NULL display" body="Text shown for NULL cells.">
                  <Field label="" value={draft.nullDisplay} onChange={(v) => patch({ nullDisplay: v })} className="w-32 [&_label]:hidden" mono />
                </Row>
              </>
            ) : null}
            {section === "editor" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Query Behaviour</h2>
                <Row title="Max query rows" body="Caps how many rows a query in the editor returns. Larger results are trimmed to this limit to keep the app fast; the results banner says when that happened.">
                  <AppSelect
                    ariaLabel="Max query rows"
                    value={String(draft.maxQueryRows)}
                    options={[
                      { value: "1000", label: "1,000 rows" },
                      { value: "5000", label: "5,000 rows" },
                      { value: "10000", label: "10,000 rows" },
                      { value: "50000", label: "50,000 rows" },
                      { value: "100000", label: "100,000 rows" },
                    ]}
                    onChange={(v) => patch({ maxQueryRows: Number(v) })}
                    className="w-40"
                  />
                </Row>
                <Row title="Show Results Pane When Running a Query" body="Automatically expand the SQL results pane when you run a query.">
                  <Toggle checked={draft.showResultsPane} onChange={(v) => patch({ showResultsPane: v })} label="" />
                </Row>
                <Row title="Condense SQL When Formatting" body="Keep lists and expressions on a single line when you use Format.">
                  <Toggle checked={draft.condenseSqlWhenFormatting} onChange={(v) => patch({ condenseSqlWhenFormatting: v })} label="" />
                </Row>
                <Row title="Run Without a Selection" body="Run every statement in the editor, or just the statement your cursor is in.">
                  <AppSelect<RunScope> ariaLabel="Run scope" value={draft.runScope} options={[{ value: "all", label: "All statements" }, { value: "current", label: "Current statement" }]} onChange={(v) => patch({ runScope: v })} className="w-48" />
                </Row>
                <Row title="Execution Mode" body="Review Mode queues changes for review before applying. Direct Mode applies changes immediately.">
                  <AppSelect<ExecutionMode> ariaLabel="Execution mode" value={draft.executionMode} options={[{ value: "review", label: "Review Mode" }, { value: "direct", label: "Direct Mode" }]} onChange={(v) => patch({ executionMode: v })} className="w-48" />
                </Row>
                <Separator />
                <h2 className="text-sm font-semibold text-foreground">Command Menu</h2>
                <p className="text-xs text-muted">Order of sections in the command menu (⌘K). Use the arrows to reorder.</p>
                <ul className="flex flex-col gap-1">
                  {draft.commandMenuSections.map((name, i) => (
                    <li key={name} className="flex items-center gap-2 rounded-md border border-border bg-surface px-3 py-1.5 text-[13px]">
                      <span className="capitalize">{name.replace("_", " ")}</span>
                      <span className="ml-auto flex gap-1">
                        <Button isIconOnly size="sm" variant="ghost" aria-label="Move up" isDisabled={i === 0} onPress={() => patch({ commandMenuSections: move(draft.commandMenuSections, i, i - 1) })}><Icon name="arrow-up" size={12} /></Button>
                        <Button isIconOnly size="sm" variant="ghost" aria-label="Move down" isDisabled={i === draft.commandMenuSections.length - 1} onPress={() => patch({ commandMenuSections: move(draft.commandMenuSections, i, i + 1) })}><Icon name="arrow-down" size={12} /></Button>
                      </span>
                    </li>
                  ))}
                </ul>
                <h2 className="text-sm font-semibold text-foreground">Inspector Tabs</h2>
                <p className="text-xs text-muted">Order of the tabs in the record inspector. The top tab is the default.</p>
                <ul className="flex flex-col gap-1">
                  {draft.inspectorTabs.map((name, i) => (
                    <li key={name} className="flex items-center gap-2 rounded-md border border-border bg-surface px-3 py-1.5 text-[13px]">
                      <span className="uppercase">{name}</span>
                      <span className="ml-auto flex gap-1">
                        <Button isIconOnly size="sm" variant="ghost" aria-label="Move up" isDisabled={i === 0} onPress={() => patch({ inspectorTabs: move(draft.inspectorTabs, i, i - 1) })}><Icon name="arrow-up" size={12} /></Button>
                        <Button isIconOnly size="sm" variant="ghost" aria-label="Move down" isDisabled={i === draft.inspectorTabs.length - 1} onPress={() => patch({ inspectorTabs: move(draft.inspectorTabs, i, i + 1) })}><Icon name="arrow-down" size={12} /></Button>
                      </span>
                    </li>
                  ))}
                </ul>
              </>
            ) : null}
            {section === "shortcuts" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Shortcuts</h2>
                <ul className="divide-y divide-separator rounded-md border border-border bg-surface">
                  {SHORTCUTS.map((s) => (
                    <li key={s.keys} className="flex items-center px-3 py-2 text-[13px]">
                      <span className="text-foreground">{s.action}</span>
                      <span className="ml-auto font-mono text-xs text-muted">{s.keys}</span>
                    </li>
                  ))}
                </ul>
              </>
            ) : null}
            {section === "ai" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">AI (bring your own key)</h2>
                <p className="text-xs text-muted">Natural-language to SQL and plan explanations. Only the schema (table and column names) and your prompt are sent to the provider. The key is encrypted at rest.</p>
                <Row title="Provider" body="Off keeps the app fully offline.">
                  <AppSelect<AiProvider> ariaLabel="Provider" value={draft.ai.provider} options={PROVIDERS} onChange={(v) => patchAi({ provider: v })} className="w-48" />
                </Row>
                <Row title="Model" body="e.g. claude-opus-5, gpt-4.1, llama3.1">
                  <Field label="" value={draft.ai.model} onChange={(v) => patchAi({ model: v })} className="w-64 [&_label]:hidden" mono />
                </Row>
                <Row title="Base URL" body="Optional. Proxies, OpenRouter, or a remote Ollama.">
                  <Field label="" value={draft.ai.baseUrl ?? ""} onChange={(v) => patchAi({ baseUrl: v.trim().length > 0 ? v.trim() : null })} className="w-72 [&_label]:hidden" mono />
                </Row>
                <Row title="API key" body={draft.ai.hasApiKey ? "A key is stored. Enter a new one to replace it." : "No key stored."}>
                  <div className="flex flex-col items-end gap-1">
                    <Field label="" type="password" value={apiKey} onChange={setApiKey} className="w-72 [&_label]:hidden" mono />
                    {draft.ai.hasApiKey ? <Toggle checked={clearKey} onChange={setClearKey} label="Remove stored key on save" /> : null}
                  </div>
                </Row>
              </>
            ) : null}
            {section === "security" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Security & Privacy</h2>
                <Row title="Telemetry" body="Zero telemetry, always. Nothing leaves this machine except your own database and AI requests.">
                  <span className="text-xs text-success">off</span>
                </Row>
                <Row title="Crash reports" body="Opt-in only. Off by default; this build keeps them local.">
                  <Toggle checked={draft.crashReportsOptIn} onChange={(v) => patch({ crashReportsOptIn: v })} label="" />
                </Row>
                <Row title="Credentials" body="Passwords and API keys are sealed with AES-256-GCM; the master key lives in the OS keychain.">
                  <Icon name="lock" size={16} className="text-muted" />
                </Row>
              </>
            ) : null}
            {section === "updates" ? <UpdatesSection /> : null}
            {section === "advanced" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Advanced</h2>
                <Row title="Reset preferences" body="Restores every setting to its default (connections and saved queries are kept).">
                  <Button size="sm" variant="danger-soft" onPress={() => setDraft((d) => (d ? { ...defaultSettings(), ai: d.ai } : d))}>
                    Reset
                  </Button>
                </Row>
              </>
            ) : null}
          </div>
        </ScrollShadow>
      </div>
      {dirty ? (
        <div className="pointer-events-none absolute inset-x-0 bottom-5 flex justify-center">
          <div role="status" className="pointer-events-auto flex items-center gap-3 rounded-2xl glass-modal py-2 pr-2 pl-4 shadow-2xl">
            <Icon name="info" size={15} className="text-accent" />
            <span className="mr-3 text-[13px] font-medium text-foreground">Unsaved changes</span>
            <Button size="sm" variant="danger-soft" onPress={reset} isDisabled={saving} className="rounded-lg liquid-hover">
              Reset
            </Button>
            <Button size="sm" isPending={saving} onPress={() => void save()} className="glass-pill bg-accent text-accent-foreground font-semibold shadow-xs liquid-hover">
              Save
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

// WHAT:  Structural equality for the draft vs saved settings; a fixed key list
//        keeps JSON.stringify order-independent (top level and `ai`).
function sameSettings(a: AppSettings, b: AppSettings): boolean {
  const keys = [...Object.keys(a), ...Object.keys(a.ai)].sort();
  return JSON.stringify(a, keys) === JSON.stringify(b, keys);
}

function Row({ title, body, children }: { title: string; body: string; children: React.ReactNode }) {
  return (
    <Card className="rounded-xl glass-card border-border/40 px-4 py-3.5 shadow-xs">
      <Card.Content className="flex flex-row items-center gap-6 p-0 w-full">
        <div className="min-w-0 flex-1">
          <p className="text-[13px] font-semibold text-foreground tracking-tight">{title}</p>
          <p className="text-xs text-muted mt-0.5">{body}</p>
        </div>
        <div className="shrink-0">{children}</div>
      </Card.Content>
    </Card>
  );
}

function clamp(n: number, lo: number, hi: number): number {
  return Number.isFinite(n) ? Math.min(hi, Math.max(lo, Math.round(n))) : lo;
}

function move<T>(list: readonly T[], from: number, to: number): T[] {
  const copy = [...list];
  const [item] = copy.splice(from, 1);
  if (item !== undefined) copy.splice(to, 0, item);
  return copy;
}

// WHAT:  Self-update panel: the running version, what the release feed offers,
//        and one button that installs it and restarts.
// WHY:   Installers were download-by-hand; the feed is the signed latest.json
//        published with each GitHub release.
// WHERE: src-tauri/src/services/updates.rs, .github/workflows/release.yml
function UpdatesSection() {
  const showError = useWorkspace((s) => s.showError);
  const [status, setStatus] = useState<UpdateStatus | null>(null);
  const [busy, setBusy] = useState<"check" | "install" | null>(null);

  const check = async () => {
    setBusy("check");
    try {
      setStatus(await ipc("check_update"));
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setBusy(null);
    }
  };

  const install = async () => {
    setBusy("install");
    try {
      // The app restarts into the new version, so nothing after this runs.
      await ipc("install_update");
    } catch (raw) {
      showError(normalizeError(raw));
      setBusy(null);
    }
  };

  return (
    <>
      <h2 className="text-sm font-semibold text-foreground">Updates</h2>
      <p className="text-xs text-muted">Signed builds are published for every commit on main; the app verifies the signature before installing.</p>
      <Row title="Version" body={status ? `Running ${status.current}` : "Check to compare this build against the latest release."}>
        <Button size="sm" variant="secondary" isPending={busy === "check"} onPress={() => void check()}>
          <Icon name="refresh" size={13} />
          Check for updates
        </Button>
      </Row>
      {status?.available ? (
        <Row title={`Version ${status.available} is available`} body={status.notes ?? status.published ?? "Installs and restarts the app."}>
          <Button size="sm" isPending={busy === "install"} onPress={() => void install()}>
            <Icon name="download" size={13} />
            Install and restart
          </Button>
        </Row>
      ) : status ? (
        <Row title="Up to date" body={`No release newer than ${status.current}.`}>
          <Icon name="check" size={15} className="text-success" />
        </Row>
      ) : null}
    </>
  );
}

function defaultSettings(): AppSettings {
  return {
    accent: "blue",
    uiFont: "jetbrains-mono",
    editorFont: "jetbrains-mono",
    uiFontSize: 13,
    editorFontSize: 13,
    gridDensity: "cozy",
    alternatingRows: true,
    rememberTableState: true,
    columnPreview: true,
    maxQueryRows: 10_000,
    nullDisplay: "NULL",
    showResultsPane: true,
    condenseSqlWhenFormatting: false,
    runScope: "all",
    executionMode: "review",
    commandMenuSections: ["create", "navigation", "connections", "tables", "saved_queries", "dashboards", "workflows", "diagrams", "settings"],
    inspectorTabs: ["fields", "json", "sql"],
    confirmDestructive: true,
    crashReportsOptIn: false,
    ai: { provider: "none", model: "claude-opus-5", baseUrl: null, hasApiKey: false },
  };
}
