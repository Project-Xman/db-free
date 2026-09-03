// SOT: engine-registry, engine-labels, engine-presets, connection-string-parser, coming-soon-engines
import type { ConnectionInput, Engine, SslMode } from "./bindings";
import type { IconName } from "./icons";

// WHAT:  UI metadata per engine, keyed by the Rust `Engine` enum.
// WHY:   Adding an engine in Rust makes this object fail `satisfies` until the
//        entry exists — the registry pattern, not a hand-written list.
// WHERE: src-tauri/src/model/connection.rs (Engine), src-tauri/src/integrations/mod.rs
export interface EngineMeta {
  label: string;
  fileBased: boolean;
  defaultPort: number | null;
  defaultDatabase: string;
  defaultUser: string;
  icon: IconName;
  /// Short description on the picker card.
  hint: string;
  /// Label for the editor tab when the engine does not speak SQL.
  commandLanguage: string;
  /// URL schemes that map to this engine when a connection string is pasted.
  schemes: readonly string[];
}

export const ENGINES = {
  postgres: { label: "PostgreSQL", fileBased: false, defaultPort: 5432, defaultDatabase: "", defaultUser: "postgres", icon: "database", hint: "Relational · SQL", commandLanguage: "SQL", schemes: ["postgres", "postgresql"] },
  mysql: { label: "MySQL", fileBased: false, defaultPort: 3306, defaultDatabase: "", defaultUser: "root", icon: "database", hint: "Relational · SQL", commandLanguage: "SQL", schemes: ["mysql"] },
  mariadb: { label: "MariaDB", fileBased: false, defaultPort: 3306, defaultDatabase: "", defaultUser: "root", icon: "database", hint: "Relational · SQL", commandLanguage: "SQL", schemes: ["mariadb"] },
  mssql: { label: "SQL Server", fileBased: false, defaultPort: 1433, defaultDatabase: "master", defaultUser: "sa", icon: "database", hint: "Relational · T-SQL", commandLanguage: "SQL", schemes: ["mssql", "sqlserver", "tds"] },
  clickhouse: { label: "ClickHouse", fileBased: false, defaultPort: 8123, defaultDatabase: "default", defaultUser: "default", icon: "columns", hint: "Analytical · SQL over HTTP", commandLanguage: "SQL", schemes: ["clickhouse"] },
  redis: { label: "Redis", fileBased: false, defaultPort: 6379, defaultDatabase: "0", defaultUser: "", icon: "hash", hint: "Key-value · commands", commandLanguage: "Command", schemes: ["redis", "rediss"] },
  mongodb: { label: "MongoDB", fileBased: false, defaultPort: 27017, defaultDatabase: "test", defaultUser: "", icon: "braces", hint: "Documents · JSON commands", commandLanguage: "Command", schemes: ["mongodb", "mongodb+srv"] },
  libsql: { label: "LibSQL / Turso", fileBased: false, defaultPort: null, defaultDatabase: "default", defaultUser: "", icon: "database", hint: "Embedded / Serverless · SQLite", commandLanguage: "SQL", schemes: ["libsql", "turso"] },
  val_town: { label: "Val Town", fileBased: false, defaultPort: null, defaultDatabase: "main", defaultUser: "", icon: "database", hint: "Serverless · SQLite over HTTP", commandLanguage: "SQL", schemes: ["valtown", "val_town"] },
  cloudflare_d1: { label: "Cloudflare D1", fileBased: false, defaultPort: null, defaultDatabase: "", defaultUser: "", icon: "database", hint: "Serverless · SQLite over REST", commandLanguage: "SQL", schemes: ["cloudflare", "d1"] },
  supabase: { label: "Supabase", fileBased: false, defaultPort: 5432, defaultDatabase: "postgres", defaultUser: "postgres", icon: "database", hint: "Postgres · Managed Cloud", commandLanguage: "SQL", schemes: ["supabase"] },
  planetscale: { label: "PlanetScale", fileBased: false, defaultPort: 3306, defaultDatabase: "", defaultUser: "", icon: "database", hint: "MySQL · Serverless Cloud", commandLanguage: "SQL", schemes: ["planetscale", "psdb"] },
  neon: { label: "Neon", fileBased: false, defaultPort: 5432, defaultDatabase: "neondb", defaultUser: "", icon: "database", hint: "Postgres · Serverless Cloud", commandLanguage: "SQL", schemes: ["neon"] },
  sqlite: { label: "SQLite", fileBased: true, defaultPort: null, defaultDatabase: "", defaultUser: "", icon: "file", hint: "Embedded file · SQL", commandLanguage: "SQL", schemes: ["sqlite", "file"] },
} satisfies Record<Engine, EngineMeta>;

export const ENGINE_ORDER: Engine[] = [
  "postgres",
  "libsql",
  "mysql",
  "val_town",
  "mariadb",
  "cloudflare_d1",
  "mssql",
  "supabase",
  "clickhouse",
  "planetscale",
  "redis",
  "neon",
  "mongodb",
  "sqlite",
];

export function engineMeta(engine: Engine): EngineMeta {
  return ENGINES[engine];
}

// WHAT:  Hosted services that are one of the engines above with fixed settings.
export interface EnginePreset {
  id: string;
  label: string;
  engine: Engine;
  sslMode: SslMode;
  hint: string;
  host?: string;
}

export const PRESETS: readonly EnginePreset[] = [
  { id: "supabase", label: "Supabase", engine: "supabase", sslMode: "require", hint: "Postgres · SSL required" },
  { id: "neon", label: "Neon", engine: "neon", sslMode: "require", hint: "Postgres · SSL required" },
  { id: "planetscale", label: "PlanetScale", engine: "planetscale", sslMode: "require", hint: "MySQL · SSL required" },
];

// WHAT:  Engines DB Manager lists that this build does not ship an adapter for yet.
export const COMING_SOON: readonly { label: string; hint: string }[] = [];

export function blankInput(engine: Engine, preset?: EnginePreset): ConnectionInput {
  const meta = engineMeta(engine);
  return {
    name: preset ? `${preset.label} database` : "",
    engine,
    environment: "none",
    readOnly: false,
    host: meta.fileBased ? null : (preset?.host ?? "localhost"),
    port: meta.defaultPort,
    database: meta.fileBased ? null : meta.defaultDatabase,
    username: meta.fileBased ? null : meta.defaultUser,
    password: null,
    filePath: meta.fileBased ? "" : null,
    sslMode: preset?.sslMode ?? "prefer",
  };
}

// WHAT:  Parses `scheme://user:pass@host:port/db?sslmode=…` into a ConnectionInput.
// WHY:   DB Manager's "paste a connection string to auto-detect" affordance.
export function parseConnectionString(raw: string): ConnectionInput | null {
  const text = raw.trim();
  if (text.length === 0) return null;
  const scheme = text.split("://")[0]?.toLowerCase() ?? "";
  const engine = ENGINE_ORDER.find((e) => ENGINES[e].schemes.includes(scheme));
  if (!engine) return null;
  if (engine === "sqlite") {
    const path = text.replace(/^(sqlite|file):\/\//i, "");
    return { ...blankInput("sqlite"), name: path.split("/").pop() ?? "sqlite", filePath: path };
  }
  let url: URL;
  try {
    // Custom schemes parse with the generic URL grammar when rewritten to http.
    url = new URL(text.replace(/^[a-z+]+:\/\//i, "http://"));
  } catch {
    return null;
  }
  const params = url.searchParams;
  const sslParam = (params.get("sslmode") ?? params.get("ssl") ?? "").toLowerCase();
  const sslMode: SslMode =
    scheme === "rediss" || scheme === "mongodb+srv" || sslParam === "require" || sslParam === "true"
      ? "require"
      : sslParam === "verify-ca" || sslParam === "verify_ca"
        ? "verify_ca"
        : sslParam === "verify-full" || sslParam === "verify_full"
          ? "verify_full"
          : sslParam === "disable"
            ? "disable"
            : "prefer";
  const base = blankInput(engine);
  const database = decodeURIComponent(url.pathname.replace(/^\//, ""));
  return {
    ...base,
    name: database.length > 0 ? database : url.hostname,
    host: url.hostname.length > 0 ? url.hostname : base.host,
    port: url.port.length > 0 ? Number(url.port) : base.port,
    database: database.length > 0 ? database : base.database,
    username: url.username.length > 0 ? decodeURIComponent(url.username) : base.username,
    password: url.password.length > 0 ? decodeURIComponent(url.password) : null,
    sslMode,
  };
}
