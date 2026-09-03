// SOT: connection-picker, engine-grid, connection-string-detect
import { useState } from "react";
import { Alert, Button, Card, Input, Label, ScrollShadow, Separator, TextField } from "@heroui/react";
import { ENGINE_ORDER, blankInput, engineMeta, parseConnectionString } from "@/lib/engines";
import { useWorkspace } from "@/stores/workspace";
import { Icon } from "@/lib/icons";
import { EngineIcon } from "@/components/global/EngineIcon";

// WHAT:  First step of "New connection": paste a connection string (auto-detects
//        the engine) or pick an engine / hosted preset from the grid.
export function ConnectionPicker() {
  const openForm = useWorkspace((s) => s.openForm);
  const goConnections = useWorkspace((s) => s.goConnections);
  const [text, setText] = useState("");
  const parsed = parseConnectionString(text);

  return (
    <div className="grid-bg flex h-full min-h-0 flex-1 flex-col">
      <div className="drag-region flex h-11 shrink-0 items-center gap-2 px-4 border-b border-border/40 glass-header" data-tauri-drag-region>
        <Button variant="ghost" size="sm" onPress={goConnections} className="rounded-lg text-muted hover:bg-surface-secondary/70 hover:text-foreground liquid-hover">
          <Icon name="chevron-left" size={14} />
          Back
        </Button>
        <span className="text-sm font-semibold text-foreground tracking-tight" data-tauri-drag-region>
          New Connection
        </span>
      </div>
      <ScrollShadow className="min-h-0 flex-1">
        <div className="mx-auto flex w-full max-w-[620px] flex-col gap-6 px-6 pt-8 pb-12">
          <div className="text-center">
            <h1 className="text-xl font-bold tracking-tight text-foreground">Select a Database</h1>
            <p className="mt-1 text-xs text-muted">Paste your connection string or select an engine to get started.</p>
          </div>

          <Card className="glass-card rounded-2xl p-5 shadow-lg border-border/40">
            <Card.Content className="p-0">
              <TextField value={text} onChange={setText} className="w-full">
                <Label className="text-xs font-semibold text-foreground tracking-tight">Connection String</Label>
                <Input placeholder="protocol://user:password@host:port/database" className="w-full font-mono text-xs mt-1.5" />
                <p className="mt-2 text-xs text-muted">Auto-detects database engine, user credentials, and host automatically.</p>
              </TextField>
              {parsed ? (
                <div className="mt-3.5 flex items-center justify-between border-t border-border/40 pt-3">
                  <span className="flex items-center gap-2 text-xs text-success font-medium">
                    <Icon name="check" size={13} />
                    Detected {engineMeta(parsed.engine).label}
                  </span>
                  <Button onPress={() => openForm(undefined, undefined, parsed)} className="glass-pill bg-accent text-accent-foreground font-semibold shadow-xs liquid-hover">
                    Continue
                    <Icon name="chevron-right" size={14} />
                  </Button>
                </div>
              ) : text.trim().length > 0 ? (
                <Alert status="danger" className="mt-3 text-xs rounded-xl">
                  <Alert.Indicator />
                  <Alert.Content>
                    <Alert.Title>Unrecognised Scheme</Alert.Title>
                    <Alert.Description>Supported: {ENGINE_ORDER.flatMap((e) => engineMeta(e).schemes).join(", ")}.</Alert.Description>
                  </Alert.Content>
                </Alert>
              ) : null}
            </Card.Content>
          </Card>

          <div className="flex items-center gap-3 text-xs text-muted">
            <Separator className="flex-1 opacity-50" />
            <span className="text-[11px] uppercase tracking-wider font-semibold text-muted/70">select database</span>
            <Separator className="flex-1 opacity-50" />
          </div>

          <div className="grid grid-cols-2 gap-3">
            {ENGINE_ORDER.map((engine) => {
              const meta = engineMeta(engine);
              return (
                <Button
                  key={engine}
                  variant="ghost"
                  onPress={() => openForm(undefined, undefined, blankInput(engine))}
                  className="group flex h-auto w-full items-center justify-start gap-3 rounded-xl glass-card px-3.5 py-3 text-left glass-card-hover border border-border/40"
                >
                  <span className="flex size-10 shrink-0 items-center justify-center rounded-xl bg-surface-tertiary/70 shadow-xs border border-border/40 group-hover:scale-105 transition-transform overflow-hidden">
                    <EngineIcon engine={engine} size={28} />
                  </span>
                  <span className="min-w-0">
                    <span className="block truncate text-sm font-semibold text-foreground tracking-tight">{meta.label}</span>
                    <span className="block truncate text-[11px] text-muted">{meta.hint}</span>
                  </span>
                </Button>
              );
            })}
          </div>
        </div>
      </ScrollShadow>
    </div>
  );
}
