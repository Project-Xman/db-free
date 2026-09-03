// SOT: connection-form, connection-editor-page, test-connection-flow
import { useState } from "react";
import { Alert, Button, Card, ScrollShadow, Tabs } from "@heroui/react";
import type { ConnectionInput, ConnectionSummary, Engine, Environment, SslMode } from "@/lib/bindings";
import { ENGINE_ORDER, blankInput, engineMeta, type EnginePreset } from "@/lib/engines";
import { ENVIRONMENT_ORDER, environmentMeta } from "@/lib/environments";
import { ipc, normalizeError } from "@/lib/ipc";
import { pickSqliteFile } from "@/lib/native";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { AppSelect, Field, Toggle } from "@/components/global/Field";
import { Icon } from "@/lib/icons";

const SSL_MODES: readonly { value: SslMode; label: string }[] = [
  { value: "disable", label: "Disable" },
  { value: "prefer", label: "Prefer" },
  { value: "require", label: "Require" },
  { value: "verify_ca", label: "Verify CA" },
  { value: "verify_full", label: "Verify full" },
];

function fromSummary(summary: ConnectionSummary): ConnectionInput {
  return {
    name: summary.name,
    engine: summary.engine,
    environment: summary.environment,
    readOnly: summary.readOnly,
    host: summary.host,
    port: summary.port,
    database: summary.database,
    username: summary.username,
    password: null,
    filePath: summary.filePath,
    sslMode: summary.sslMode,
  };
}

type Status = { tone: "ok" | "error"; text: string } | null;

// WHAT:  Full-page connection editor (DB Manager layout): centered column on a grid
//        background, Back in the title strip, Test / Save footer.
// HOW:   Keyed by the connection being edited so state initialises from props
//        without an effect.
export function ConnectionForm() {
  const page = useWorkspace((s) => s.page);
  const connections = useWorkspace((s) => s.connections);
  if (page.kind !== "connection-form") return null;
  const editing = page.editingId === null ? null : (connections.find((c) => c.id === page.editingId) ?? null);
  return <ConnectionFormBody key={editing?.id ?? `new:${page.preset?.id ?? ""}:${page.draft?.engine ?? ""}`} editing={editing} preset={page.preset} draft={page.draft} />;
}

function ConnectionFormBody({ editing, preset, draft }: { editing: ConnectionSummary | null; preset?: EnginePreset | undefined; draft?: ConnectionInput | undefined }) {
  const goConnections = useWorkspace((s) => s.goConnections);
  const goPicker = useWorkspace((s) => s.goPicker);
  const saveConnection = useWorkspace((s) => s.saveConnection);
  const deleteConnection = useWorkspace((s) => s.deleteConnection);
  const showInfo = useWorkspace((s) => s.showInfo);

  const [input, setInput] = useState<ConnectionInput>(() => (editing ? fromSummary(editing) : (draft ?? blankInput("postgres", preset))));
  const [status, setStatus] = useState<Status>(null);
  const [testing, setTesting] = useState(false);
  const [saving, setSaving] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [section, setSection] = useState<"general" | "ssl">("general");

  const patch = (partial: Partial<ConnectionInput>) => setInput((prev) => ({ ...prev, ...partial }));
  const meta = engineMeta(input.engine);

  const changeEngine = (engine: Engine) => {
    const next = blankInput(engine);
    setInput((prev) => ({ ...next, name: prev.name, environment: prev.environment, readOnly: prev.readOnly }));
  };
  const changeEnvironment = (environment: Environment) => patch({ environment, readOnly: environmentMeta(environment).readOnlyDefault });

  const test = async () => {
    setTesting(true);
    setStatus(null);
    try {
      await ipc("test_connection", { id: editing?.id ?? null, input });
      setStatus({ tone: "ok", text: "Connection succeeded." });
    } catch (raw) {
      setStatus({ tone: "error", text: normalizeError(raw).message });
    } finally {
      setTesting(false);
    }
  };

  const save = async () => {
    setSaving(true);
    setStatus(null);
    try {
      const saved = await saveConnection(editing?.id ?? null, input);
      showInfo(`Saved "${saved.name}".`);
      goConnections();
    } catch (raw) {
      setStatus({ tone: "error", text: normalizeError(raw).message });
    } finally {
      setSaving(false);
    }
  };

  const remove = async () => {
    if (!editing) return;
    if (!window.confirm(`Delete connection "${editing.name}"? Saved credentials are removed too.`)) return;
    try {
      await deleteConnection(editing.id);
      goConnections();
    } catch (raw) {
      setStatus({ tone: "error", text: normalizeError(raw).message });
    }
  };

  const browse = async () => {
    const path = await pickSqliteFile();
    if (path) patch({ filePath: path });
  };

  return (
    <div className="grid-bg flex h-full min-h-0 flex-1 flex-col">
      <div className="drag-region flex h-11 shrink-0 items-center gap-2 px-4 border-b border-border/40 glass-header" data-tauri-drag-region>
        <Button variant="ghost" size="sm" onPress={editing ? goConnections : goPicker} className="rounded-lg text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover">
          <Icon name="chevron-left" size={14} />
          Back
        </Button>
        <span className="text-sm font-semibold text-foreground tracking-tight" data-tauri-drag-region>
          {editing ? "Edit Connection" : preset ? `New ${preset.label} Connection` : "New Connection"}
        </span>
        <div className="drag-region h-full flex-1" data-tauri-drag-region />
      </div>

      <ScrollShadow className="min-h-0 flex-1">
        <div className="mx-auto flex w-full max-w-[640px] flex-col gap-5 px-6 pt-8 pb-10">
          <Card className="glass-card rounded-2xl p-6 shadow-xl flex flex-col gap-5 border-border/40">
            <Card.Content className="flex flex-col gap-5 p-0">
              <AppSelect label="Type" value={input.engine} options={ENGINE_ORDER.map((e) => ({ value: e, label: engineMeta(e).label, icon: engineMeta(e).icon }))} onChange={changeEngine} />
              <Field label="Connection Name" value={input.name} onChange={(name) => patch({ name })} placeholder="local-db" autoFocus />
              <AppSelect label="Environment" value={input.environment} options={ENVIRONMENT_ORDER.map((e) => ({ value: e, label: environmentMeta(e).label }))} onChange={changeEnvironment} />

              {meta.fileBased ? (
                <Field
                  label="Database file"
                  value={input.filePath ?? ""}
                  onChange={(filePath) => patch({ filePath })}
                  placeholder="/path/to/database.sqlite"
                  suffix={<Button variant="secondary" size="sm" onPress={() => void browse()} className="rounded-lg">Browse…</Button>}
                />
              ) : input.engine === "val_town" ? (
                <div className="flex flex-col gap-5">
                  <Field
                    label="API Token"
                    type={showPassword ? "text" : "password"}
                    value={input.password ?? ""}
                    onChange={(password) => patch({ password })}
                    placeholder={editing && !input.password ? "•••••••• (unchanged)" : "vt_..."}
                    suffix={<IconButton icon={showPassword ? "eye-off" : "eye"} label={showPassword ? "Hide token" : "Show token"} onPress={() => setShowPassword((v) => !v)} />}
                  />
                  <Field label="Database Name" value={input.database ?? ""} onChange={(database) => patch({ database })} placeholder="main" optional />
                  <p className="-mt-3 flex items-center gap-1.5 text-xs text-muted">
                    <Icon name="lock" size={12} className="text-accent" />
                    Keychain enabled — your Val Town token is stored with AES-256-GCM.
                  </p>
                </div>
              ) : input.engine === "cloudflare_d1" ? (
                <div className="flex flex-col gap-5">
                  <Field label="Cloudflare Account ID" value={input.username ?? ""} onChange={(username) => patch({ username })} placeholder="Account ID (32 hex characters)" />
                  <Field label="D1 Database ID / Name" value={input.database ?? ""} onChange={(database) => patch({ database })} placeholder="Database UUID or Name" />
                  <Field
                    label="API Token"
                    type={showPassword ? "text" : "password"}
                    value={input.password ?? ""}
                    onChange={(password) => patch({ password })}
                    placeholder={editing && !input.password ? "•••••••• (unchanged)" : "Cloudflare API Token with D1 edit permission"}
                    suffix={<IconButton icon={showPassword ? "eye-off" : "eye"} label={showPassword ? "Hide token" : "Show token"} onPress={() => setShowPassword((v) => !v)} />}
                  />
                  <p className="-mt-3 flex items-center gap-1.5 text-xs text-muted">
                    <Icon name="lock" size={12} className="text-accent" />
                    Keychain enabled — your Cloudflare token is stored with AES-256-GCM.
                  </p>
                </div>
              ) : input.engine === "libsql" ? (
                <div className="flex flex-col gap-5">
                  <Field label="Database URL" value={input.host ?? ""} onChange={(host) => patch({ host })} placeholder="libsql://your-db.turso.io or https://..." />
                  <Field
                    label="Auth Token"
                    type={showPassword ? "text" : "password"}
                    value={input.password ?? ""}
                    onChange={(password) => patch({ password })}
                    placeholder={editing && !input.password ? "•••••••• (unchanged)" : "Turso JWT Auth Token"}
                    suffix={<IconButton icon={showPassword ? "eye-off" : "eye"} label={showPassword ? "Hide token" : "Show token"} onPress={() => setShowPassword((v) => !v)} />}
                  />
                  <p className="-mt-3 flex items-center gap-1.5 text-xs text-muted">
                    <Icon name="lock" size={12} className="text-accent" />
                    Keychain enabled — your Turso token is stored with AES-256-GCM.
                  </p>
                </div>
              ) : (
                <Tabs selectedKey={section} onSelectionChange={(k) => setSection(String(k) === "ssl" ? "ssl" : "general")} className="w-full">
                  <Tabs.List className="glass-pill border border-border/40 p-1">
                    <Tabs.Tab id="general">General</Tabs.Tab>
                    <Tabs.Tab id="ssl">SSL / TLS</Tabs.Tab>
                  </Tabs.List>
                  <Tabs.Panel id="general" className="flex flex-col gap-5 pt-4">
                    <div className="grid grid-cols-[1fr_120px] gap-3">
                      <Field label="Host" value={input.host ?? ""} onChange={(host) => patch({ host })} placeholder="localhost" />
                      <Field label="Port" type="number" value={input.port !== null ? String(input.port) : ""} onChange={(port) => patch({ port: port === "" ? null : Number(port) })} placeholder={String(meta.defaultPort ?? "")} />
                    </div>
                    {input.engine !== "redis" ? (
                      <Field label="Database" value={input.database ?? ""} onChange={(database) => patch({ database })} placeholder={input.engine === "mssql" ? "master" : "mydb"} />
                    ) : null}
                    {input.engine !== "sqlite" && input.engine !== "redis" ? (
                      <Field label="User" value={input.username ?? ""} onChange={(username) => patch({ username })} placeholder={input.engine === "mssql" ? "sa" : "postgres"} />
                    ) : null}
                    <Field
                      label="Password"
                      type={showPassword ? "text" : "password"}
                      value={input.password ?? ""}
                      onChange={(password) => patch({ password })}
                      placeholder={editing && !input.password ? "•••••••• (unchanged)" : "secret"}
                      suffix={<IconButton icon={showPassword ? "eye-off" : "eye"} label={showPassword ? "Hide password" : "Show password"} onPress={() => setShowPassword((v) => !v)} />}
                    />
                    <p className="-mt-3 flex items-center gap-1.5 text-xs text-muted">
                      <Icon name="lock" size={12} className="text-accent" />
                      Keychain enabled — stored with AES-256-GCM; the key never leaves your OS keychain.
                    </p>
                  </Tabs.Panel>
                  <Tabs.Panel id="ssl" className="flex flex-col gap-5 pt-4">
                    <AppSelect label="SSL mode" value={input.sslMode} options={SSL_MODES} onChange={(sslMode) => patch({ sslMode })} />
                    <p className="text-xs text-muted">SSH tunnelling (bastion hosts, key files, agent) arrives in Phase 2 of the roadmap.</p>
                  </Tabs.Panel>
                </Tabs>
              )}

              <Toggle checked={input.readOnly} onChange={(readOnly) => patch({ readOnly })} label="Read-only lock" description="Blocks every write and DDL statement on this connection. On by default for Production." />

              {status ? (
                <Alert status={status.tone === "ok" ? "success" : "danger"} className="rounded-xl font-mono text-xs">
                  <Alert.Indicator />
                  <Alert.Content>
                    <Alert.Title className="font-sans font-semibold">{status.tone === "ok" ? "Success" : "Connection Error"}</Alert.Title>
                    <Alert.Description className="selectable">{status.text}</Alert.Description>
                  </Alert.Content>
                </Alert>
              ) : null}
            </Card.Content>
          </Card>
        </div>
      </ScrollShadow>

      <div className="flex shrink-0 items-center justify-center gap-3 border-t border-border/40 glass-header px-6 py-3">
        {editing ? (
          <Button variant="danger-soft" onPress={() => void remove()} className="mr-auto rounded-xl liquid-hover">
            Delete
          </Button>
        ) : null}
        <Button variant="secondary" isPending={testing} onPress={() => void test()} className="w-[220px] rounded-xl glass-pill text-foreground liquid-hover">
          Test Connection
        </Button>
        <Button isPending={saving} onPress={() => void save()} className="w-[220px] rounded-xl glass-pill bg-accent text-accent-foreground font-semibold shadow-md shadow-accent/25 liquid-hover">
          {editing ? "Save Changes" : "Create Connection"}
        </Button>
      </div>
    </div>
  );
}
