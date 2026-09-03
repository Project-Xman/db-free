# Adapter worker brief (db-free)

You are implementing database adapters in `/Volumes/Vinu1TBSSD/Programs/db-free` (Rust core, Tauri v2). Read `CLAUDE.md` first. The registry refactor is done; your job is to replace stub files in `src-tauri/src/integrations/` with real adapters.

## Ground rules (enforced by `pnpm check`)

- Every `.rs` file starts with a `// SOT: …` line (keep the one in the stub, extend it).
- Clippy denies `unwrap`, `expect`, `panic`, `todo`, `unimplemented` outside `#[cfg(test)]`. Use `?`, `ok_or_else`, `unwrap_or_default`, `map_err`.
- Vendor crates are confined per file (see `VENDOR_OWNERS` in `scripts/guardrail.py`). `reqwest` is allowed only via `crate::integrations::http::HttpClient` plus the files listed there. Do not add new crates to `Cargo.toml` without a very good reason; if you must, tell the coordinator in your report.
- Do NOT touch `src-tauri/src/model/connection.rs`, `integrations/mod.rs`, `services/`, `commands/`, or anything in `src/` (UI). Only your assigned `integrations/<name>.rs` files. If you find a bug elsewhere, report it instead of fixing.
- Do NOT run `cargo build`/`cargo test` for the whole crate repeatedly; it links DuckDB + RocksDB and is slow. Use `cargo check` (fast after the first time) and run only your own tests: `cargo test --lib integrations::<name>`.
- Commit your work when it compiles: `git add src-tauri/src/integrations/<your files> && git commit -m "feat(<name>): …"`. Other agents are committing in parallel; only add your own files. If `git commit` fails because of a lock, retry after a second.

## The contract

```rust
#[async_trait]
pub trait Integration: Send + Sync {
    fn engine(&self) -> Engine;                       // store the Engine from conn.summary.engine and return it
    fn capabilities(&self) -> Capabilities;           // use Capabilities::SQL / DOCUMENT / KEY_VALUE consts, override fields with struct update syntax
    async fn ping(&self) -> AppResult<()>;
    async fn server_version(&self) -> AppResult<Option<String>>;
    fn current_database(&self) -> Option<String>;
    async fn databases(&self) -> AppResult<Vec<String>>;   // for the sidebar switcher; one entry if not applicable
    async fn catalog(&self) -> AppResult<SchemaCatalog>;   // schemas -> tables (collections / indexes / keys / topics / graphs …)
    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>>;  // MUST be non-empty for a table to open (sample documents if schemaless)
    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>>;
    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64>;
    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet>;  // sort + filters + offset/limit
    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>>;   // the engine's native query language
    async fn close(&self);
    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> { Ok(vec![]) }   // optional
    async fn ddl(&self, _table: &TableRef) -> AppResult<Option<String>> { Ok(None) }  // optional
}
pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>>;
```

`ResolvedConnection { summary: ConnectionSummary { engine, host, port, database, username, file_path, ssl_mode, read_only, … }, secret: Option<String> }`. The secret is the password / API token / service-account JSON.

Model types live in `crate::model`: `Value` (Null/Bool/Int/Float/Decimal/Text/Bytes(base64)/Json/DateTime/Unsupported), `ResultSet { columns: Vec<ColumnMeta{name,type_name}>, rows, truncated }`, `StatementResult::{Rows{result}, Affected{rows_affected}}`, `SchemaCatalog { schemas: Vec<SchemaInfo{name, tables: Vec<TableInfo{schema, name, kind: TableKind::{Table,View}, row_estimate}>}> }`, `ColumnInfo { name, data_type, nullable, primary_key, ordinal }`, `TableRef { schema: Option<String>, name }`, `PageQuery { sort: Vec<SortRule{column,desc}>, filters: Vec<FilterRule{column, op: FilterOp, value}>, offset, limit }`.

## Shared helpers (use them)

- `crate::integrations::http::{HttpClient, Auth, base_url, json_to_value, json_type_name, objects_to_result_set, json_result, local}`:
  - `HttpClient::from_connection(conn, default_port, default_tls, auth)` builds the client from host/port/ssl_mode. `HttpClient::auth_from_connection(conn)` picks Basic (user+secret) / Bearer (secret only) / None. `Auth::Header{name,value}` for `api-key:` style.
  - `get_json / post_json / put_json / delete_json / get_text / post_raw(path, content_type, body, accept)`; errors already map to `AppError` with the server message.
  - `objects_to_result_set(&docs, Some("id"), max_rows)` turns a Vec of JSON objects into a grid (union of keys, id pinned first). `json_result(value)` shows any JSON verbatim.
  - `local::page(&column_names, rows, &query)` applies filter → sort → offset/limit client-side when the engine cannot.
- `crate::integrations::sql::{where_clause, order_clause, quote_literal, validate_columns}` and `crate::integrations::{quote_ident, quote_ident_for, qualified_name_for}` for SQL-speaking engines (Postgres-style double quotes by default).
- `crate::integrations::aws_sigv4::{AwsCredentials, sign_post}` (DynamoDB / QLDB).
- `crate::integrations::gcp_auth::GcpAuth` (Firestore / BigQuery): `GcpAuth::from_connection(conn, scope)?`, then `auth.bearer().await?` before each request (it caches).
- Look at `integrations/val_town.rs` (small REST SQL adapter), `integrations/redis.rs` (key-value mapped onto tables, client-side paging) and `integrations/mongodb.rs` (document store, sampled columns) as reference implementations.

## Field conventions per engine (what the UI puts where)

The connection form is data-driven (`src/lib/engines.ts`). Field meaning per engine:

| engine | host | port | database | username | secret |
|---|---|---|---|---|---|
| server-style (cassandra, neo4j, kafka, memcached, oracle) | host | port | keyspace / db / topic filter / service name | user | password |
| http_token (qdrant, weaviate, milvus, chroma, pinecone, elasticsearch, opensearch, meilisearch, typesense, influxdb, prometheus, victoriametrics, druid, couchdb, arangodb, surrealdb, orientdb, immudb, hbase, tigergraph, objectdb, basex, existdb, apache_jena, graphdb, stardog, blazegraph, virtuoso, snowflake) | base URL or host (use `HttpClient::from_connection` which handles both) | port (fallback) | engine-specific name (index / bucket / dataset / repository / namespace…) | user / org / tenant | API key / token / password |
| aws (dynamodb, qldb) | region | – | endpoint override (dynamodb) / ledger name (qldb) | access key id | secret key (optionally `SECRET|SESSION_TOKEN`) |
| gcp (firestore, bigquery) | emulator host (optional) | – | project id | database id (firestore) / dataset (bigquery) | service-account JSON or access token |
| file (duckdb, rocksdb) | – | – | – | – | – (`file_path`) |

Wire-compatible engines route to your adapter with a different `Engine` value: `Engine::Scylladb` → cassandra.rs, `Engine::Opensearch` → elasticsearch.rs, `Engine::Memgraph` → neo4j.rs, `Engine::Victoriametrics` → prometheus.rs, `Engine::Redpanda` → kafka.rs, five RDF stores → sparql.rs. Always return `conn.summary.engine` from `engine()` and branch on it only where the APIs actually differ.

## Mapping guidance

- Schemaless engines: `columns()` samples up to ~50 documents and unions the keys (see mongodb.rs). Mark the id field `primary_key: true` so the grid can address rows.
- `fetch_page` for engines without server-side sort/filter: fetch up to `offset+limit` (capped, e.g. 5 000) rows and use `http::local::page`.
- `execute` accepts the engine's native language. Accept JSON bodies where that is the natural API (vector DBs, Elasticsearch Query DSL, Mango). Also accept a small set of friendly shorthand commands where useful (e.g. `SCAN`, `GET key`). Return `Affected` for write operations.
- `read_only`: the guard already blocks writes for SQL. For non-SQL engines, refuse obvious mutations yourself when `conn.summary.read_only` is true (store the flag).
- Map TLS: `SslMode::Disable` = plain, `Prefer` = engine default, `Require` = TLS without cert verification, `VerifyCa/VerifyFull` = verified. `HttpClient::from_connection` already does this for HTTP.
- Errors: map every vendor error to `AppError::driver(...)`, auth failures to `AppError::not_connected`, bad input to `AppError::invalid_input`.
- Keep every request bounded (`max_rows`, sensible caps). Never load a whole collection.

## Tests

Add `#[cfg(test)] mod tests` with pure unit tests (URL building, request-body shaping, response → ResultSet decoding, command parsing). Live round-trip tests must be gated on an env var (`DBFREE_TEST_<ENGINE>_URL`) and skip silently when unset, following `live_round_trip_when_configured` in mysql.rs.

If Docker is available and the engine has an official image, you MAY run a quick smoke test (`docker run --rm -d -p …`), but stop the container afterwards and do not leave anything running. Report what you verified live vs. only via unit tests.

## Report format

When done, `swarm report` with: files touched, which engines are verified live vs unit-only, any crate additions, and anything the coordinator must change outside your files (with exact file:line suggestions).
