# Include All 14 Database Integration Adapters

This implementation plan adds full integration adapter support for all 14 database engines shown in the user's specification image:
1. **PostgreSQL**
2. **MySQL**
3. **MariaDB**
4. **SQL Server**
5. **ClickHouse**
6. **Redis**
7. **MongoDB**
8. **LibSQL / Turso**
9. **Val Town**
10. **Cloudflare D1**
11. **Supabase**
12. **PlanetScale**
13. **Neon**
14. **SQLite**

---

## User Review Required

> [!IMPORTANT]
> **No More "Coming Soon" Placeholders**: All 14 databases will be promoted from "coming soon" or separate preset lists into first-class active integration adapters across the entire stack:
> - **Rust Backend (`src-tauri`)**: First-class `Engine` enum variants, client connections, catalog discovery, paging, and query execution.
> - **TypeScript Client (`src/`)**: Unified 14-database picker grid matching the exact 2-column layout and authentic brand icons from the screenshot, with custom form fields per database type.
> - **Strict Architectural Guardrails**: All vendor boundaries (`sqlx`, `rusqlite`, `reqwest`, `tiberius`, `redis`, `mongodb`) will be maintained with zero leaking across layers.

---

## Architecture & Integration Strategy

### 1. Database Engine Classification & Adapters

| Database | Protocol / Transport | Implementation File | Credentials / Config |
|---|---|---|---|
| **PostgreSQL** | Native TCP / TLS (SQLx) | `integrations/postgres.rs` | Host, Port (5432), DB, User, Pass, SSL |
| **MySQL** | Native TCP / TLS (SQLx) | `integrations/mysql.rs` | Host, Port (3306), DB, User, Pass, SSL |
| **MariaDB** | Native TCP / TLS (SQLx) | `integrations/mysql.rs` | Host, Port (3306), DB, User, Pass, SSL |
| **SQL Server** | TDS over TCP / TLS (`tiberius`) | `integrations/mssql.rs` [NEW] | Host, Port (1433), DB, User, Pass, Encrypt |
| **ClickHouse** | HTTP JSONCompact (`reqwest`) | `integrations/clickhouse.rs` | Host, Port (8123), DB, User, Pass, SSL |
| **Redis** | RESP / TCP / TLS (`redis`) | `integrations/redis.rs` | Host, Port (6379), DB (index), Pass, SSL |
| **MongoDB** | Wire protocol (`mongodb`) | `integrations/mongodb.rs` | Host, Port (27017), DB, User, Pass, SSL |
| **LibSQL / Turso** | HTTP Pipeline API (`reqwest`) | `integrations/libsql.rs` [NEW] | Host / Turso URL, Auth Token |
| **Val Town** | HTTP SQLite API (`reqwest`) | `integrations/val_town.rs` [NEW] | API Token |
| **Cloudflare D1** | HTTP REST API (`reqwest`) | `integrations/cloudflare_d1.rs` [NEW] | Account ID, Database ID, API Token |
| **Supabase** | Postgres over SSL (`sqlx`) | `integrations/postgres.rs` | Host, Port (5432/6543), DB, User, Pass, SSL Require |
| **PlanetScale** | MySQL over SSL (`sqlx`) | `integrations/mysql.rs` | Host, Port (3306), DB, User, Pass, SSL Require |
| **Neon** | Serverless Postgres (`sqlx`) | `integrations/postgres.rs` | Host, Port (5432), DB, User, Pass, SSL Require |
| **SQLite** | Embedded file (`rusqlite`) | `integrations/sqlite.rs` | File path (.sqlite / .db) |

---

## Proposed Changes

### Rust Backend (`src-tauri`)

#### [MODIFY] [Cargo.toml](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/Cargo.toml)
- Add `tiberius = { version = "0.12", default-features = false, features = ["rustls", "tds73", "chrono"] }`
- Add `tokio-util = { version = "0.7", features = ["compat"] }`

#### [MODIFY] [model/connection.rs](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/src/model/connection.rs)
- Update `Engine` enum with variants: `Postgres`, `Mysql`, `Mariadb`, `Mssql`, `Clickhouse`, `Redis`, `Mongodb`, `Libsql`, `ValTown`, `CloudflareD1`, `Supabase`, `Planetscale`, `Neon`, `Sqlite`.
- Update `Engine::ALL`, `Engine::kind()`, `Engine::as_str()`, `Engine::parse()`, `Engine::is_file_based()`, `Engine::default_port()`.
- Update `ConnectionInput::validate()` to accommodate token-based HTTP databases (Val Town, Cloudflare D1, LibSQL).

#### [NEW] [integrations/mssql.rs](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/src/integrations/mssql.rs)
- Implements `Integration` for Microsoft SQL Server (MSSQL) using `tiberius`.
- Implements `ping`, `server_version`, `catalog`, `columns`, `row_estimate`, `count`, `fetch_page`, `execute`, `foreign_keys`.
- Handles T-SQL bracket identifier quoting `[schema].[table]`, paging with `OFFSET x ROWS FETCH NEXT y ROWS ONLY`.

#### [NEW] [integrations/libsql.rs](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/src/integrations/libsql.rs)
- Implements `Integration` for LibSQL / Turso over the Turso HTTP pipeline endpoint (`/v2/pipeline`).
- Implements SQLite SQL queries for catalog, columns, paging, and statement execution.

#### [NEW] [integrations/cloudflare_d1.rs](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/src/integrations/cloudflare_d1.rs)
- Implements `Integration` for Cloudflare D1 over Cloudflare REST API (`/client/v4/accounts/{account_id}/d1/database/{database_id}/query`).
- Decodes result arrays and metadata.

#### [NEW] [integrations/val_town.rs](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/src/integrations/val_town.rs)
- Implements `Integration` for Val Town SQLite over Val Town API (`/v1/sqlite/execute`).
- Decodes result columns and rows.

#### [MODIFY] [integrations/mod.rs](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/src/integrations/mod.rs)
- Register new modules: `mssql`, `libsql`, `cloudflare_d1`, `val_town`.
- Update `connect` dispatch to route all 14 engines.
- Update `quote_ident_for` for `Engine::Mssql` (brackets) and HTTP SQLite engines (double quotes).

#### [MODIFY] [services/changes.rs](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/src/services/changes.rs)
- Support literals and preview batch generation for `Mssql`, `Libsql`, `CloudflareD1`, `ValTown`, `Supabase`, `Planetscale`, `Neon`.

#### [MODIFY] [services/ai.rs](file:///Volumes/Vinu1TBSSD/Programs/db-free/src-tauri/src/services/ai.rs)
- Add explain query support and engine display labels for all engines.

---

### Guardrail Validator (`scripts/`)

#### [MODIFY] [scripts/guardrail.py](file:///Volumes/Vinu1TBSSD/Programs/db-free/scripts/guardrail.py)
- Update `VENDOR_OWNERS` to include:
  - `"tiberius": ("integrations/mssql.rs",)`
  - `"reqwest": ("integrations/clickhouse.rs", "integrations/libsql.rs", "integrations/cloudflare_d1.rs", "integrations/val_town.rs", "services/ai.rs")`

---

### Frontend UI & Bindings (`src/`)

#### [NEW] [components/global/EngineIcon.tsx](file:///Volumes/Vinu1TBSSD/Programs/db-free/src/components/global/EngineIcon.tsx)
- High-fidelity SVG brand icons matching the user's screenshot for all 14 databases:
  - PostgreSQL (elephant on blue)
  - MySQL (dolphin on orange)
  - MariaDB (sea lion on dark teal)
  - SQL Server (cylinder stack on blue/red)
  - ClickHouse (yellow bars)
  - Redis (R badge on red)
  - MongoDB (green leaf)
  - LibSQL / Turso (teal horned bull)
  - Val Town (white vt on dark box)
  - Cloudflare D1 (orange cylinder stack)
  - Supabase (emerald bolt)
  - PlanetScale (white orbital slash on black)
  - Neon (bright green N)
  - SQLite (blue feather / quill)

#### [MODIFY] [lib/engines.ts](file:///Volumes/Vinu1TBSSD/Programs/db-free/src/lib/engines.ts)
- Define metadata for all 14 engines in `ENGINES`.
- Update `ENGINE_ORDER` to match the exact 14 database ordering from the user's screenshot.
- Empty `COMING_SOON` array (all engines are supported).
- Expand connection string parsing schemes: `sqlserver://`, `mssql://`, `libsql://`, `turso://`, `valtown://`, `cloudflare://`, `d1://`, `supabase://`, `planetscale://`, `psdb://`, `neon://`.

#### [MODIFY] [features/connections/ConnectionPicker.tsx](file:///Volumes/Vinu1TBSSD/Programs/db-free/src/features/connections/ConnectionPicker.tsx)
- Reorganize into a clean 2-column, 7-row grid of 14 active database buttons matching the user's screenshot.
- Use `EngineIcon` for authentic brand logos on each tile.
- Remove the "coming soon" disabled cards section.

#### [MODIFY] [features/connections/ConnectionForm.tsx](file:///Volumes/Vinu1TBSSD/Programs/db-free/src/features/connections/ConnectionForm.tsx)
- Fix existing TypeScript issues (`cn` unused, `browse` function, `pickSqliteFile`).
- Customize input fields based on engine type:
  - **SQLite**: File picker browse button
  - **LibSQL / Turso**: Turso database URL / host and Auth Token
  - **Val Town**: API Token
  - **Cloudflare D1**: Account ID, Database ID, API Token
  - **SQL Server**: Host, Port (1433), Database, Username, Password, SSL/Trust Server Certificate
  - **Supabase / Neon / PlanetScale**: Tailored hints and SSL required badge
  - **Standard engines**: Host, Port, Database, Username, Password, SSL Mode

#### [MODIFY] [features/queries/QueriesPanel.tsx](file:///Volumes/Vinu1TBSSD/Programs/db-free/src/features/queries/QueriesPanel.tsx)
- Fix missing `caption="Queries"` prop on `<ConnectionSwitcher />`.

#### [MODIFY] [features/shell/TabBar.tsx](file:///Volumes/Vinu1TBSSD/Programs/db-free/src/features/shell/TabBar.tsx)
- Fix invalid HeroUI v3 `Chip` variant `"solid"` -> `"primary"`.

---

## Verification Plan

### Automated Tests
1. **TypeScript & Bindings**:
   - `pnpm bindings` (regenerate TypeScript type bindings from Rust `Engine` enum)
   - `pnpm typecheck` (verify 0 TypeScript compiler errors across all files)
   - `pnpm lint` (verify 0 ESLint errors)
2. **Guardrail Check**:
   - `python3 scripts/guardrail.py` (verify 0 vendor boundary, layering, or type safety violations)
3. **Rust Compilation & Unit Tests**:
   - `cargo check --manifest-path src-tauri/Cargo.toml`
   - `cargo test --manifest-path src-tauri/Cargo.toml` (run all unit tests including new adapter tests)

### Manual Verification
- Launch `pnpm build` to verify production Vite bundle builds cleanly.
- Inspect the Connection Picker UI to ensure all 14 database cards match the layout, brand icons, and styling shown in the user's reference image.
