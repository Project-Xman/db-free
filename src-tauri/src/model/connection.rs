// SOT: engine, environment, ssl-mode, connection-input, connection-summary, connection-validation

use crate::error::{AppError, AppResult};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

// WHAT:  Supported database engines. Adding a variant is the whole "add a integration"
//        entry point: every `match` on Engine then fails to compile until handled.
// WHY:   The registry pattern — one enum, everything else derived from it.
// HOW:   integrations::connect dispatches on it; the UI reads Engine from bindings.
// WHERE: src-tauri/src/integrations/mod.rs, src/lib/engines.ts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum Engine {
    Postgres,
    Mysql,
    Mariadb,
    Mssql,
    Clickhouse,
    Redis,
    Mongodb,
    Libsql,
    ValTown,
    CloudflareD1,
    Supabase,
    Planetscale,
    Neon,
    Sqlite,
}

// WHAT:  Storage model of an engine; drives which UI affordances make sense.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum EngineKind {
    Relational,
    Analytical,
    Document,
    KeyValue,
}

impl Engine {
    pub const ALL: [Engine; 14] = [
        Engine::Postgres,
        Engine::Mysql,
        Engine::Mariadb,
        Engine::Mssql,
        Engine::Clickhouse,
        Engine::Redis,
        Engine::Mongodb,
        Engine::Libsql,
        Engine::ValTown,
        Engine::CloudflareD1,
        Engine::Supabase,
        Engine::Planetscale,
        Engine::Neon,
        Engine::Sqlite,
    ];

    pub fn kind(self) -> EngineKind {
        match self {
            Engine::Postgres
            | Engine::Mysql
            | Engine::Mariadb
            | Engine::Mssql
            | Engine::Sqlite
            | Engine::Libsql
            | Engine::ValTown
            | Engine::CloudflareD1
            | Engine::Supabase
            | Engine::Planetscale
            | Engine::Neon => EngineKind::Relational,
            Engine::Clickhouse => EngineKind::Analytical,
            Engine::Mongodb => EngineKind::Document,
            Engine::Redis => EngineKind::KeyValue,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Engine::Postgres => "postgres",
            Engine::Mysql => "mysql",
            Engine::Mariadb => "mariadb",
            Engine::Mssql => "mssql",
            Engine::Sqlite => "sqlite",
            Engine::Clickhouse => "clickhouse",
            Engine::Redis => "redis",
            Engine::Mongodb => "mongodb",
            Engine::Libsql => "libsql",
            Engine::ValTown => "val_town",
            Engine::CloudflareD1 => "cloudflare_d1",
            Engine::Supabase => "supabase",
            Engine::Planetscale => "planetscale",
            Engine::Neon => "neon",
        }
    }

    pub fn parse(raw: &str) -> Option<Engine> {
        Engine::ALL.into_iter().find(|e| e.as_str() == raw)
    }

    pub fn is_file_based(self) -> bool {
        matches!(self, Engine::Sqlite)
    }

    pub fn is_http_token_based(self) -> bool {
        matches!(self, Engine::Libsql | Engine::ValTown | Engine::CloudflareD1)
    }

    pub fn default_port(self) -> Option<u16> {
        match self {
            Engine::Postgres | Engine::Supabase | Engine::Neon => Some(5432),
            Engine::Mysql | Engine::Mariadb | Engine::Planetscale => Some(3306),
            Engine::Mssql => Some(1433),
            Engine::Clickhouse => Some(8123),
            Engine::Redis => Some(6379),
            Engine::Mongodb => Some(27017),
            Engine::Sqlite | Engine::Libsql | Engine::ValTown | Engine::CloudflareD1 => None,
        }
    }
}

// WHAT:  Deployment environment badge. Production defaults to read-only.
// WHERE: src/lib/environments.ts (colour tokens per variant)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum Environment {
    None,
    Local,
    Staging,
    Production,
}

impl Environment {
    pub const ALL: [Environment; 4] =
        [Environment::None, Environment::Local, Environment::Staging, Environment::Production];

    pub fn as_str(self) -> &'static str {
        match self {
            Environment::None => "none",
            Environment::Local => "local",
            Environment::Staging => "staging",
            Environment::Production => "production",
        }
    }

    pub fn parse(raw: &str) -> Option<Environment> {
        Environment::ALL.into_iter().find(|e| e.as_str() == raw)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    pub const ALL: [SslMode; 5] = [
        SslMode::Disable,
        SslMode::Prefer,
        SslMode::Require,
        SslMode::VerifyCa,
        SslMode::VerifyFull,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyCa => "verify_ca",
            SslMode::VerifyFull => "verify_full",
        }
    }

    pub fn parse(raw: &str) -> Option<SslMode> {
        SslMode::ALL.into_iter().find(|m| m.as_str() == raw)
    }
}

// WHAT:  What the UI sends to create or update a connection.
// WHY:   `password` is write-only: it is encrypted on arrival and never echoed
//        back. An empty/absent password on update keeps the stored secret.
// HOW:   `validate()` is the Zod analogue — commands call it before any service.
// WHERE: src-tauri/src/commands/connections.rs
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ConnectionInput {
    pub name: String,
    pub engine: Engine,
    pub environment: Environment,
    pub read_only: bool,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub file_path: Option<String>,
    pub ssl_mode: SslMode,
}

impl ConnectionInput {
    pub fn validate(&self) -> AppResult<()> {
        if self.name.trim().is_empty() {
            return Err(AppError::invalid_input("Connection name is required."));
        }
        if self.name.len() > 120 {
            return Err(AppError::invalid_input("Connection name is too long (max 120)."));
        }
        if self.engine.is_file_based() {
            if self.file_path.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                return Err(AppError::invalid_input("A database file path is required."));
            }
        } else if self.engine.is_http_token_based() {
            if self.engine == Engine::Libsql {
                if self.host.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                    return Err(AppError::invalid_input("Turso database URL or host is required."));
                }
            } else if self.engine == Engine::CloudflareD1 {
                if self.host.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                    return Err(AppError::invalid_input("Cloudflare Account ID is required in the host field."));
                }
                if self.database.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                    return Err(AppError::invalid_input("Cloudflare Database ID is required in the database field."));
                }
            }
        } else {
            if self.host.as_deref().map(str::trim).unwrap_or_default().is_empty() {
                return Err(AppError::invalid_input("Host is required."));
            }
            // Database is optional: an empty value connects to the server's default
            // database and the UI offers every database for switching.
            if self.port == Some(0) {
                return Err(AppError::invalid_input("Port must be between 1 and 65535."));
            }
        }
        Ok(())
    }

    /// Strips the write-only secret so the rest of the input can be logged or echoed.
    pub fn without_password(&self) -> ConnectionInput {
        ConnectionInput { password: None, ..self.clone() }
    }
}

// WHAT:  The connection as the UI sees it. Never carries a secret.
// WHERE: src-tauri/src/store/connections.rs (persisted form)
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct ConnectionSummary {
    pub id: String,
    pub name: String,
    pub engine: Engine,
    pub environment: Environment,
    pub read_only: bool,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub file_path: Option<String>,
    pub ssl_mode: SslMode,
    pub has_secret: bool,
    pub created_at: String,
    pub updated_at: String,
}

impl ConnectionSummary {
    /// Builds a summary from unsaved input — used for "Test connection" before save.
    pub fn draft(input: &ConnectionInput, has_secret: bool) -> ConnectionSummary {
        let now = chrono::Utc::now().to_rfc3339();
        ConnectionSummary {
            id: String::from("draft"),
            name: input.name.clone(),
            engine: input.engine,
            environment: input.environment,
            read_only: input.read_only,
            host: input.host.clone(),
            port: input.port,
            database: input.database.clone(),
            username: input.username.clone(),
            file_path: input.file_path.clone(),
            ssl_mode: input.ssl_mode,
            has_secret,
            created_at: now.clone(),
            updated_at: now,
        }
    }
}

// WHAT:  Store-layer row: summary plus the AES-GCM sealed secret. Not serializable
//        on purpose — it must never cross the IPC boundary.
#[derive(Debug, Clone)]
pub struct ConnectionRecord {
    pub summary: ConnectionSummary,
    pub secret_ciphertext: Option<Vec<u8>>,
}

// WHAT:  In-memory only: summary plus the decrypted secret, handed to a integration.
#[derive(Debug, Clone)]
pub struct ResolvedConnection {
    pub summary: ConnectionSummary,
    pub secret: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> ConnectionInput {
        ConnectionInput {
            name: "x".into(),
            engine: Engine::Postgres,
            environment: Environment::Local,
            read_only: false,
            host: Some("localhost".into()),
            port: Some(5432),
            database: Some("app".into()),
            username: Some("me".into()),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        }
    }

    #[test]
    fn postgres_requires_host_only() {
        let mut input = base();
        input.host = None;
        assert!(matches!(input.validate(), Err(AppError::InvalidInput { .. })));
        let mut input = base();
        input.database = Some("  ".into());
        assert!(input.validate().is_ok(), "database is optional");
        assert!(base().validate().is_ok());
    }

    #[test]
    fn sqlite_requires_file_path() {
        let mut input = base();
        input.engine = Engine::Sqlite;
        input.host = None;
        assert!(matches!(input.validate(), Err(AppError::InvalidInput { .. })));
        input.file_path = Some("/tmp/x.db".into());
        assert!(input.validate().is_ok());
    }

    #[test]
    fn enums_round_trip_through_strings() {
        for e in Engine::ALL {
            assert_eq!(Engine::parse(e.as_str()), Some(e));
        }
        for e in Environment::ALL {
            assert_eq!(Environment::parse(e.as_str()), Some(e));
        }
        for m in SslMode::ALL {
            assert_eq!(SslMode::parse(m.as_str()), Some(m));
        }
    }
}
