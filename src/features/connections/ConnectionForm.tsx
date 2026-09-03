// SOT: connection-form, connection-editor-page, test-connection-flow
import { useState } from "react";
import { Button, Tabs } from "@heroui/react";
import type { ConnectionInput, ConnectionSummary, Engine, Environment, SslMode } from "@/lib/bindings";
import { ENGINE_ORDER, blankInput, engineMeta, type EnginePreset } from "@/lib/engines";
import { ENVIRONMENT_ORDER, environmentMeta } from "@/lib/environments";
import { ipc, normalizeError } from "@/lib/ipc";
import { pickSqliteFile } from "@/lib/native";
import { useWorkspace } from "@/stores/workspace";
import { IconButton } from "@/components/global/Button";
import { AppSelect, Field, Toggle } from "@/components/global/Field";
import { Icon } from "@/lib/icons";
import { isMac } from "@/components/global/Kbd";
import { cn } from "@/lib/cn";

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

// WHAT:  Full-page connection editor (DB Pro layout): centered column on a grid
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
  const isKeyValue = input.engine === "redis";
  const isDocument = input.engine === "mongodb";
  const isHttp = input.engine === "clickhouse";
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
      <div className={cn("drag-region flex h-10 shrink-0 items-center gap-2 pr-3", isMac() ? "pl-9" : "pl-3")} data-tauri-drag-region>
        <Button variant="ghost" size="sm" onPress={editing ? goConnections : goPicker} className="text-muted">
          <Icon name="chevron-left" size={14} />
          Back
        </Button>
        <span className="text-sm font-medium text-foreground" data-tauri-drag-region>
          {editing ? "Edit Connection" : preset ? `New ${preset.label} Connection` : "New Connection"}
        </span>
        <div className="drag-region h-full flex-1" data-tauri-drag-region />
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="mx-auto flex w-full max-w-[620px] flex-col gap-5 px-6 pt-10 pb-8">
          <AppSelect label="Type" value={input.engine} options={ENGINE_ORDER.map((e) => ({ value: e, label: engineMeta(e).label, icon: engineMeta(e).icon }))} onChange={changeEngine} />
          <Field label="Connection Name" value={input.name} onChange={(name) => patch({ name })} placeholder="local-db" autoFocus />
          <AppSelect label="Environment" value={input.environment} options={ENVIRONMENT_ORDER.map((e) => ({ value: e, label: environmentMeta(e).label }))} onChange={changeEnvironment} />

          {meta.fileBased ? (
            <Field
              label="Database file"
              value={input.filePath ?? ""}
              onChange={(filePath) => patch({ filePath })}
              placeholder="/path/to/app.sqlite"
              mono
              suffix={<IconButton icon="folder" label="Browse…" onPress={() => void browse()} />}
            />
          ) : (
            <Tabs
              selectedKey={section}
              onSelectionChange={(key) => setSection(String(key) === "ssl" ? "ssl" : "general")}
              className="w-full"
            >
              <Tabs.ListContainer>
                <Tabs.List aria-label="Connection settings">
                  <Tabs.Tab id="general">
                    General
                    <Tabs.Indicator />
                  </Tabs.Tab>
                  <Tabs.Tab id="ssl">
                    SSH / SSL
                    <Tabs.Indicator />
                  </Tabs.Tab>
                </Tabs.List>
              </Tabs.ListContainer>
              <Tabs.Panel id="general" className="flex flex-col gap-5 pt-5">
                <div className="grid grid-cols-[1fr_120px] gap-3">
                  <Field label="Host" value={input.host ?? ""} onChange={(host) => patch({ host })} placeholder="localhost" />
                  <Field label="Port" type="number" value={input.port === null ? "" : String(input.port)} onChange={(v) => patch({ port: v === "" ? null : Number(v) })} />
                </div>
                <Field
                  label={isKeyValue ? "Database index" : "Database"}
                  optional
                  value={input.database ?? ""}
                  onChange={(database) => patch({ database })}
                  placeholder={isKeyValue ? "0" : isDocument ? "test" : isHttp ? "default" : "Leave empty to select database after connecting"}
                  description={isKeyValue ? "Logical database 0–15." : "Leave empty to connect to the default database and pick one from the sidebar."}
                />
                <Field label="User" optional={isKeyValue || isDocument} value={input.username ?? ""} onChange={(username) => patch({ username })} placeholder={engineMeta(input.engine).defaultUser} />
                <Field
                  label="Password"
                  type={showPassword ? "text" : "password"}
                  value={input.password ?? ""}
                  onChange={(password) => patch({ password })}
                  placeholder={editing?.hasSecret ? "•••••••• (unchanged)" : ""}
                  description={editing?.hasSecret ? "Leave blank to keep the saved password." : undefined}
                  suffix={<IconButton icon={showPassword ? "eye-off" : "eye"} label={showPassword ? "Hide password" : "Show password"} onPress={() => setShowPassword((v) => !v)} />}
                />
                <p className="-mt-3 flex items-center gap-1.5 text-xs text-muted">
                  <Icon name="lock" size={12} />
                  Keychain enabled — stored with AES-256-GCM; the key never leaves your OS keychain.
                </p>
              </Tabs.Panel>
              <Tabs.Panel id="ssl" className="flex flex-col gap-5 pt-5">
                <AppSelect label="SSL mode" value={input.sslMode} options={SSL_MODES} onChange={(sslMode) => patch({ sslMode })} />
                <p className="text-xs text-muted">SSH tunnelling (bastion hosts, key files, agent) arrives in Phase 2 of the roadmap.</p>
              </Tabs.Panel>
            </Tabs>
          )}

          <Toggle checked={input.readOnly} onChange={(readOnly) => patch({ readOnly })} label="Read-only lock" description="Blocks every write and DDL statement on this connection. On by default for Production." />

          {status ? (
            <p className={cn("selectable rounded-md px-3 py-2 font-mono text-xs", status.tone === "ok" ? "bg-success-soft text-success" : "bg-danger-soft text-danger")}>{status.text}</p>
          ) : null}
        </div>
      </div>

      <div className="flex shrink-0 items-center justify-center gap-3 border-t border-border bg-surface/80 px-6 py-3 backdrop-blur">
        {editing ? (
          <Button variant="danger-soft" onPress={() => void remove()} className="mr-auto">
            Delete
          </Button>
        ) : null}
        <Button variant="secondary" isPending={testing} onPress={() => void test()} className="w-[240px]">
          Test Connection
        </Button>
        <Button isPending={saving} onPress={() => void save()} className="w-[240px]">
          {editing ? "Save Changes" : "Create Connection"}
        </Button>
      </div>
    </div>
  );
}
