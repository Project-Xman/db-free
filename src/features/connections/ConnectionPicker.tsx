// SOT: connection-picker, engine-grid, connection-string-detect
import { useState } from "react";
import { Button, Input, Label, TextField } from "@heroui/react";
import { COMING_SOON, ENGINE_ORDER, PRESETS, blankInput, engineMeta, parseConnectionString } from "@/lib/engines";
import { useWorkspace } from "@/stores/workspace";
import { Icon } from "@/lib/icons";
import { isMac } from "@/components/global/Kbd";
import { cn } from "@/lib/cn";

// WHAT:  First step of "New connection": paste a connection string (auto-detects
//        the engine) or pick an engine / hosted preset from the grid.
export function ConnectionPicker() {
  const openForm = useWorkspace((s) => s.openForm);
  const goConnections = useWorkspace((s) => s.goConnections);
  const [text, setText] = useState("");
  const parsed = parseConnectionString(text);

  return (
    <div className="grid-bg flex h-full min-h-0 flex-1 flex-col">
      <div className={cn("drag-region flex h-10 shrink-0 items-center gap-2 pr-3", isMac() ? "pl-9" : "pl-3")} data-tauri-drag-region>
        <Button variant="ghost" size="sm" onPress={goConnections} className="text-muted">
          <Icon name="chevron-left" size={14} />
          Back
        </Button>
        <span className="text-sm font-medium text-foreground" data-tauri-drag-region>
          New Connection
        </span>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="mx-auto flex w-full max-w-[560px] flex-col gap-6 px-6 pt-10 pb-10">
          <TextField value={text} onChange={setText} className="w-full">
            <Label>Connection String</Label>
            <Input placeholder="protocol://user:password@host:port/database" className="w-full font-mono" />
            <p className="mt-1 text-xs text-muted">Paste your connection string to auto-detect database type</p>
          </TextField>
          {parsed ? (
            <Button onPress={() => openForm(undefined, undefined, parsed)} className="self-start">
              Continue with {engineMeta(parsed.engine).label}
              <Icon name="chevron-right" size={14} />
            </Button>
          ) : text.trim().length > 0 ? (
            <p className="text-xs text-danger">Unrecognised scheme. Supported: {ENGINE_ORDER.flatMap((e) => engineMeta(e).schemes).join(", ")}.</p>
          ) : null}

          <div className="flex items-center gap-3 text-xs text-muted">
            <span className="h-px flex-1 bg-separator" />
            or select database
            <span className="h-px flex-1 bg-separator" />
          </div>

          <div className="grid grid-cols-2 gap-3">
            {ENGINE_ORDER.map((engine) => {
              const meta = engineMeta(engine);
              return (
                <button key={engine} type="button" onClick={() => openForm(undefined, undefined, blankInput(engine))} className="flex items-center gap-3 rounded-xl border border-border bg-surface px-3 py-3 text-left hover:border-border-secondary hover:bg-surface-secondary">
                  <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-surface-tertiary text-accent">
                    <Icon name={meta.icon} size={18} />
                  </span>
                  <span className="min-w-0">
                    <span className="block truncate text-sm text-foreground">{meta.label}</span>
                    <span className="block truncate text-[11px] text-muted">{meta.hint}</span>
                  </span>
                </button>
              );
            })}
            {PRESETS.map((preset) => (
              <button key={preset.id} type="button" onClick={() => openForm(undefined, preset, blankInput(preset.engine, preset))} className="flex items-center gap-3 rounded-xl border border-border bg-surface px-3 py-3 text-left hover:border-border-secondary hover:bg-surface-secondary">
                <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-surface-tertiary text-success">
                  <Icon name="database" size={18} />
                </span>
                <span className="min-w-0">
                  <span className="block truncate text-sm text-foreground">{preset.label}</span>
                  <span className="block truncate text-[11px] text-muted">{preset.hint}</span>
                </span>
              </button>
            ))}
            {COMING_SOON.map((item) => (
              <div key={item.label} className="flex items-center gap-3 rounded-xl border border-dashed border-border px-3 py-3 opacity-50" title={item.hint} aria-disabled="true">
                <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-surface-tertiary text-muted">
                  <Icon name="database" size={18} />
                </span>
                <span className="min-w-0">
                  <span className="block truncate text-sm text-foreground">{item.label}</span>
                  <span className="block truncate text-[11px] text-muted">coming soon</span>
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
