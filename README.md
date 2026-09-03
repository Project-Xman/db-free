# DB Free

Lightweight, native database workbench. Rust core (Tauri v2, tokio, sqlx, rusqlite) with a React/TypeScript UI.
Sub-second cold start, small memory footprint, no telemetry, works fully offline.

**Engines:** PostgreSQL, MySQL/MariaDB, SQLite, ClickHouse, Redis, MongoDB (plus Supabase/Neon/PlanetScale presets).

**Features:** encrypted saved connections (AES-256-GCM, key in the OS keychain) · connection-string auto-detect ·
environment badges with read-only lock · virtualized table browser with sort, filter builder, pager and record inspector ·
inline editing with review-mode Pending Changes (visual diff + exact SQL, one transaction) or direct mode ·
SQL editor with schema-aware completion, format, explain plan, destructive-statement confirmation, history, saved queries and autosaved buffers ·
export/import (CSV, JSON, SQL dump) · ER diagrams from foreign keys · schema-diagram designer with DDL preview ·
dashboards (stat tiles, sparklines, line/bar/table widgets, variables, auto-refresh) · workflows (ordered SQL steps) ·
Redis key viewer · bring-your-own-key AI (Anthropic, OpenAI, OpenRouter, Ollama) · command palette (⌘K) · settings.

## Develop

Requirements: Rust (stable), Node 20+, pnpm. On Linux also `libwebkit2gtk-4.1-dev libappindicator3-dev librsvg2-dev patchelf libdbus-1-dev`.

```sh
pnpm install
pnpm tauri dev
```

## Build installers

```sh
pnpm tauri build            # host platform: .dmg (macOS), .msi/.exe (Windows), .AppImage/.deb (Linux)
```

Tagging `v*` runs `.github/workflows/release.yml`, which builds all three platforms and attaches them to a draft release.

## Quality gate

```sh
pnpm check                  # guardrail validator + tsc + eslint + clippy + cargo test
pnpm bindings               # regenerate TS types from Rust (#[ts(export)])
```

Architecture and rules: see `CLAUDE.md`.
