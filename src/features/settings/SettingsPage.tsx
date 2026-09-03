// SOT: settings-page, preferences-ui, ai-settings-ui, shortcuts-list
import { useState } from "react";
import { Button, Separator } from "@heroui/react";
import type { AiProvider, AppSettings, ExecutionMode, RunScope } from "@/lib/bindings";
import { normalizeError } from "@/lib/ipc";
import { useWorkspace } from "@/stores/workspace";
import { AppSelect, Field, Toggle } from "@/components/global/Field";
import { Icon, type IconName } from "@/lib/icons";
import { isMac } from "@/components/global/Kbd";
import { cn } from "@/lib/cn";

type Section = "general" | "themes" | "fonts" | "grid" | "editor" | "shortcuts" | "ai" | "security" | "advanced";

const SECTIONS: readonly { id: Section; label: string; icon: IconName }[] = [
  { id: "general", label: "General", icon: "settings" },
  { id: "themes", label: "Themes", icon: "eye" },
  { id: "fonts", label: "Fonts", icon: "text" },
  { id: "grid", label: "Data Grid", icon: "table" },
  { id: "editor", label: "Editor", icon: "terminal" },
  { id: "shortcuts", label: "Shortcuts", icon: "hash" },
  { id: "ai", label: "AI", icon: "braces" },
  { id: "security", label: "Security & Privacy", icon: "lock" },
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

// WHAT:  Settings page mirroring DB Pro's sections; edits a draft of AppSettings.
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
      <div className={cn("drag-region flex h-10 shrink-0 items-center gap-2 pr-3", isMac() ? "pl-9" : "pl-3")} data-tauri-drag-region>
        <Button variant="ghost" size="sm" onPress={goConnections} className="text-muted">
          <Icon name="chevron-left" size={14} />
          Back
        </Button>
        <span className="text-sm font-medium text-foreground" data-tauri-drag-region>
          Settings
        </span>
        <div className="drag-region h-full flex-1" data-tauri-drag-region />
      </div>
      <div className="flex min-h-0 flex-1">
        <nav className="w-44 shrink-0 py-4 pl-4" aria-label="Settings sections">
          {SECTIONS.map((s) => (
            <button key={s.id} type="button" onClick={() => setSection(s.id)} className={cn("flex h-8 w-full items-center gap-2 rounded-md px-2 text-left text-[13px]", section === s.id ? "bg-surface-tertiary text-foreground" : "text-muted hover:text-foreground")}>
              <Icon name={s.icon} size={14} />
              {s.label}
            </button>
          ))}
        </nav>
        <div className="min-h-0 flex-1 overflow-y-auto">
          <div className="mx-auto flex w-full max-w-[720px] flex-col gap-6 px-8 py-6">
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
                <Row title="Interface font size" body="Inter, 11–16 px.">
                  <Field label="" type="number" value={String(draft.uiFontSize)} onChange={(v) => patch({ uiFontSize: clamp(Number(v), 11, 16) })} className="w-24 [&_label]:hidden" />
                </Row>
                <Row title="Editor font size" body="JetBrains Mono, 11–20 px.">
                  <Field label="" type="number" value={String(draft.editorFontSize)} onChange={(v) => patch({ editorFontSize: clamp(Number(v), 11, 20) })} className="w-24 [&_label]:hidden" />
                </Row>
              </>
            ) : null}
            {section === "grid" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Data Grid</h2>
                <Row title="NULL display" body="Text shown for NULL cells.">
                  <Field label="" value={draft.nullDisplay} onChange={(v) => patch({ nullDisplay: v })} className="w-32 [&_label]:hidden" mono />
                </Row>
              </>
            ) : null}
            {section === "editor" ? (
              <>
                <h2 className="text-sm font-semibold text-foreground">Query Behaviour</h2>
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
        </div>
      </div>
      {dirty ? (
        <div className="pointer-events-none absolute inset-x-0 bottom-5 flex justify-center">
          <div role="status" className="pointer-events-auto flex items-center gap-2.5 rounded-xl border border-border bg-surface/95 py-1.5 pr-1.5 pl-3 shadow-2xl backdrop-blur">
            <Icon name="info" size={15} className="text-muted" />
            <span className="mr-4 text-[13px] text-foreground">Unsaved changes</span>
            <Button size="sm" variant="danger" onPress={reset} isDisabled={saving}>
              Reset
            </Button>
            <Button size="sm" isPending={saving} onPress={() => void save()}>
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
    <div className="flex items-center gap-6">
      <div className="min-w-0 flex-1">
        <p className="text-[13px] text-foreground">{title}</p>
        <p className="text-xs text-muted">{body}</p>
      </div>
      <div className="shrink-0">{children}</div>
    </div>
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

function defaultSettings(): AppSettings {
  return {
    accent: "blue",
    uiFontSize: 13,
    editorFontSize: 13,
    gridDensity: "cozy",
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
