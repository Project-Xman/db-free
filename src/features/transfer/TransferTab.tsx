// SOT: transfer-tab, export-ui, import-ui, table-picker, export-preview
import { useEffect, useMemo, useState } from "react";
import { Button, Chip, ScrollShadow, Separator, Spinner } from "@heroui/react";
import type { TableRef, TablePage, TransferFormat } from "@/lib/bindings";
import { ipc, normalizeError } from "@/lib/ipc";
import { pickDirectory, pickImportFile } from "@/lib/native";
import { DENSITIES, formatCount } from "@/lib/format";
import { tableKey, useWorkspace } from "@/stores/workspace";
import { DataGrid } from "@/features/grid/DataGrid";
import { AppSelect, Check, Segmented, Toggle } from "@/components/global/Field";
import { EmptyState } from "@/components/global/EmptyState";
import { Icon } from "@/lib/icons";
import { cn } from "@/lib/cn";

const FORMATS: readonly { value: TransferFormat; label: string }[] = [
  { value: "csv", label: "CSV" },
  { value: "json", label: "JSON" },
  { value: "sql", label: "SQL" },
];

// WHAT:  Export/Import tab: pick tables, format, include schema; preview the first
//        50 rows; export writes one file per table to a chosen folder. Import
//        loads CSV/JSON into a table through the statement guard.
// WHERE: src-tauri/src/services/transfer.rs
export function TransferTab({ connectionId }: { connectionId: string }) {
  const catalog = useWorkspace((s) => s.catalogs[connectionId]);
  const density = useWorkspace((s) => s.density);
  const showError = useWorkspace((s) => s.showError);
  const showInfo = useWorkspace((s) => s.showInfo);
  const [mode, setMode] = useState<"export" | "import">("export");
  const [format, setFormat] = useState<TransferFormat>("json");
  const [includeSchema, setIncludeSchema] = useState(true);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [previewKey, setPreviewKey] = useState<string | null>(null);
  const [loadedPreview, setPreview] = useState<TablePage | null>(null);
  const preview = previewKey === null ? null : loadedPreview;
  const [busy, setBusy] = useState(false);
  const [importTable, setImportTable] = useState<string>("");
  const [importFormat, setImportFormat] = useState<TransferFormat>("csv");

  const tables = useMemo<TableRef[]>(() => (catalog?.schemas ?? []).flatMap((s) => s.tables.filter((t) => t.kind === "table").map((t) => ({ schema: t.schema, name: t.name }))), [catalog]);
  const byKey = useMemo(() => new Map(tables.map((t) => [tableKey(t), t])), [tables]);

  useEffect(() => {
    if (previewKey === null) return;
    const table = byKey.get(previewKey);
    if (!table) return;
    const token = { cancelled: false };
    void (async () => {
      try {
        const page = await ipc("fetch_table_page", { connectionId, table, query: { sort: [], filters: [], offset: 0, limit: 50 } });
        if (!token.cancelled) setPreview(page);
      } catch (raw) {
        if (!token.cancelled) showError(normalizeError(raw));
      }
    })();
    return () => {
      token.cancelled = true;
    };
  }, [previewKey, byKey, connectionId, showError]);

  const toggle = (key: string) => {
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
    setPreviewKey(key);
  };

  const doExport = async () => {
    const chosen = [...selected].map((k) => byKey.get(k)).filter((t): t is TableRef => t !== undefined);
    if (chosen.length === 0) return;
    const directory = await pickDirectory();
    if (!directory) return;
    setBusy(true);
    try {
      const report = await ipc("export_tables", { connectionId, tables: chosen, format, includeSchema, directory, maxRows: null });
      const rows = report.files.reduce((n, f) => n + f.rows, 0);
      showInfo(`Exported ${report.files.length} file(s), ${formatCount(rows)} rows, to ${directory}.`);
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setBusy(false);
    }
  };

  const doImport = async () => {
    const table = byKey.get(importTable);
    if (!table) return;
    const path = await pickImportFile(importFormat);
    if (!path) return;
    setBusy(true);
    try {
      const report = await ipc("import_file", { connectionId, table, path, format: importFormat });
      showInfo(`Imported ${formatCount(report.rowsInserted)} rows into ${tableKey(table)} in ${report.statements} statement(s).`);
      window.dispatchEvent(new CustomEvent("db-free:refresh-tables"));
    } catch (raw) {
      showError(normalizeError(raw));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-3 border-b border-border bg-surface px-2">
        <Segmented label="Transfer mode" value={mode} onChange={setMode} options={[{ value: "export", label: "Export" }, { value: "import", label: "Import" }]} />
        {mode === "export" ? (
          <>
            <Button size="sm" isPending={busy} onPress={() => void doExport()} isDisabled={selected.size === 0}>
              <Icon name="download" size={13} />
              Export {selected.size > 0 ? `(${selected.size})` : ""}
            </Button>
            <Separator orientation="vertical" className="h-5 opacity-50" />
            <Segmented label="Format" value={format} onChange={setFormat} options={FORMATS} />
            <Toggle checked={includeSchema} onChange={setIncludeSchema} label="Include schema" />
          </>
        ) : (
          <>
            <AppSelect ariaLabel="Target table" value={importTable} options={[{ value: "", label: "Choose a table…" }, ...tables.map((t) => ({ value: tableKey(t), label: tableKey(t) }))]} onChange={setImportTable} size="sm" className="w-64" />
            <Segmented label="File format" value={importFormat} onChange={setImportFormat} options={FORMATS.filter((f) => f.value !== "sql")} />
            <Button size="sm" isPending={busy} onPress={() => void doImport()} isDisabled={importTable.length === 0}>
              <Icon name="folder" size={13} />
              Choose file & import
            </Button>
            <span className="text-xs text-muted">CSV: header row = column names. JSON: array of objects. Rows insert in one transaction.</span>
          </>
        )}
      </div>
      {mode === "export" ? (
        <div className="flex min-h-0 flex-1">
          <div className="flex w-[300px] shrink-0 flex-col border-r border-border/40">
            <div className="flex h-9 items-center px-3 text-xs text-muted">
              <Chip size="sm" variant="soft" className="font-mono text-[10px]">
                {tables.length} tables
              </Chip>
              <Button
                variant="ghost"
                size="sm"
                className="ml-auto h-6 px-1.5 text-xs text-accent"
                onPress={() => setSelected(selected.size === tables.length ? new Set() : new Set(tables.map(tableKey)))}
              >
                {selected.size === tables.length ? "Clear" : "Select All"}
              </Button>
            </div>
            <ScrollShadow className="min-h-0 flex-1">
              {tables.map((t) => {
                const key = tableKey(t);
                return (
                  <div key={key} className={cn("flex h-8 items-center gap-2 px-3 text-[13px]", previewKey === key ? "bg-surface-tertiary text-foreground" : "text-muted hover:bg-surface-secondary hover:text-foreground")}>
                    <Check label={`Select ${key}`} checked={selected.has(key)} onChange={() => toggle(key)} />
                    <Button
                      variant="ghost"
                      size="sm"
                      className="flex h-auto min-w-0 flex-1 items-center justify-start gap-2 p-0 text-left bg-transparent hover:bg-transparent"
                      onPress={() => setPreviewKey(key)}
                    >
                      <Icon name="table" size={13} className="shrink-0" />
                      <span className="truncate">{key}</span>
                    </Button>
                  </div>
                );
              })}
            </ScrollShadow>
          </div>
          <div className="flex min-w-0 flex-1 flex-col">
            <div className="flex h-9 items-center px-3 text-xs text-muted">Preview (first 50 rows per table){previewKey ? ` · ${previewKey}` : ""}</div>
            <div className="min-h-0 flex-1">
              {previewKey === null ? (
                <EmptyState title="Select tables to preview" />
              ) : preview === null ? (
                <div className="flex h-full items-center justify-center"><Spinner size="sm" /></div>
              ) : (
                <DataGrid columns={preview.columns.map((c) => ({ name: c.name, typeName: c.dataType, primaryKey: c.primaryKey }))} rowCount={preview.rows.length} getRow={(i) => preview.rows[i]} rowHeight={DENSITIES[density].rowHeight} />
              )}
            </div>
          </div>
        </div>
      ) : (
        <EmptyState icon="download" title="Import a file into a table" body="Pick the target table and file format above, then choose the file. Column names must match the table; read-only connections reject imports." />
      )}
    </div>
  );
}
