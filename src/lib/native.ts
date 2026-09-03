// SOT: native-dialogs, file-picker, directory-picker
import { open } from "@tauri-apps/plugin-dialog";
import type { TransferFormat } from "./bindings";

// WHAT:  Native pickers. Wrapped so components never import a Tauri plugin directly.
export async function pickSqliteFile(): Promise<string | null> {
  const picked = await open({
    multiple: false,
    directory: false,
    filters: [{ name: "Database file", extensions: ["db", "sqlite", "sqlite3", "db3", "duckdb", "ddb", "gpkg"] }],
  });
  return typeof picked === "string" ? picked : null;
}

export async function pickDirectory(): Promise<string | null> {
  const picked = await open({ multiple: false, directory: true });
  return typeof picked === "string" ? picked : null;
}

export async function pickImportFile(format: TransferFormat): Promise<string | null> {
  const extensions = format === "csv" ? ["csv", "txt"] : format === "json" ? ["json"] : ["sql"];
  const picked = await open({ multiple: false, directory: false, filters: [{ name: format.toUpperCase(), extensions }] });
  return typeof picked === "string" ? picked : null;
}
