# DB Free

Lightweight, native database workbench. Rust core (Tauri v2, tokio, sqlx, rusqlite, scylla, neo4rs, rskafka, duckdb, rocksdb, reqwest) with a React/TypeScript UI.
Sub-second cold start, small memory footprint, no telemetry, works fully offline.

**Engines (69, in 22 categories):**

| Category | Engines |
|---|---|
| Relational / SQL | PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, Supabase, Neon, PlanetScale |
| Document | MongoDB, CouchDB, Firestore |
| Key-Value | Redis, Valkey, DynamoDB |
| Wide-Column | Cassandra, ScyllaDB, HBase |
| Graph | Neo4j, Memgraph, TigerGraph |
| Time-Series | TimescaleDB, InfluxDB, VictoriaMetrics, Prometheus, QuestDB |
| Vector | Qdrant, Milvus, Pinecone, Chroma, pgvector |
| Search / Full-Text | Elasticsearch, OpenSearch, Meilisearch, Typesense |
| Multi-Model | SurrealDB, OrientDB |
| Spatial | PostGIS, SpatiaLite |
| In-Memory | Memcached, Dragonfly |
| Columnar / OLAP | ClickHouse, DuckDB, Apache Druid, Snowflake, BigQuery |
| NewSQL / Distributed SQL | CockroachDB, TiDB, YugabyteDB |
| Embedded | SQLite, RocksDB, LibSQL / Turso, Val Town, Cloudflare D1 |
| Ledger | immudb, Amazon QLDB |
| Event / Streaming | Apache Kafka, Redpanda |
| Object | ObjectDB (via JPQL gateway) |
| Hierarchical / Network | IBM IMS, Raima RDM (via their SQL gateways) |
| XML | BaseX, eXist-db |
| Graph + Vector | Weaviate, ArangoDB |
| RDF / Triple Store | Apache Jena, GraphDB, Stardog, Blazegraph, Virtuoso |

Each engine speaks its native language in the query tab (SQL, CQL, Cypher, AQL, SurrealQL, PromQL, SPARQL, XQuery, Query DSL, Redis commands…) and its collections / indexes / labels / topics / graphs / keys appear in the table browser.

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

Pushing to `main` builds installers for all three platforms (macOS universal, Windows, Linux) and uploads them as workflow artifacts. Tagging `v*` attaches them to a draft GitHub release.

## Testing engines

`pnpm check` runs the guardrail, tsc, eslint, clippy and every unit + end-to-end
test. Adapter unit tests cover request/response shaping; the end-to-end suite
drives the real command path (connect → catalog → paged browse → filter → query,
plus the read-only lock and destructive confirmation) on the file engines.

Adapters also carry live round-trip tests gated on an environment variable, so
they are skipped unless a server is configured:

```
./scripts/live-tests.sh                 # every engine with a container image
./scripts/live-tests.sh qdrant neo4j    # just these
```

Each engine gets a throwaway container on a high port (5xxxx, so nothing
collides with your own stack) and is removed afterwards. Verified live so far:
memcached, meilisearch, typesense, qdrant, chroma, weaviate, elasticsearch,
couchdb, arangodb, surrealdb, orientdb, neo4j, cassandra, kafka/redpanda,
influxdb, prometheus, immudb, hbase, existdb, dynamodb (local), SPARQL (Fuseki),
duckdb, rocksdb, postgres, mysql, sqlite. Cloud-only engines (Snowflake,
BigQuery, Firestore, QLDB, Pinecone, Oracle) are unit-tested and carry live
tests that activate when you set their `DBFREE_TEST_*` variables.

## Quality gate

```sh
pnpm check                  # guardrail validator + tsc + eslint + clippy + cargo test
pnpm bindings               # regenerate TS types from Rust (#[ts(export)])
```

Architecture and rules: see `CLAUDE.md`.
