// SOT: integration-trait, integration-adapter-layer, engine-adapters, capabilities, connect-dispatch, quote-ident

use crate::error::{AppError, AppResult};
use crate::model::{
    ColumnInfo, Engine, Family, FilterRule, ForeignKey, ObjectDetail, ObjectKind, ObjectRef, ObjectSummary,
    PageQuery, RangeQueryRequest, RangeResult, ResolvedConnection, ResultSet, SchemaCatalog, SearchRequest,
    SearchResult, ServerStats, StatementResult, TableRef, Tool, VectorSearchRequest,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ts_rs::TS;

pub mod arangodb;
pub mod aws_sigv4;
pub mod basex;
pub mod bigquery;
pub mod cassandra;
pub mod chroma;
pub mod clickhouse;
pub mod cloudflare_d1;
pub mod couchdb;
pub mod druid;
pub mod duckdb;
pub mod dynamodb;
pub mod elasticsearch;
pub mod existdb;
pub mod firestore;
pub mod gcp_auth;
pub mod hbase;
pub mod http;
pub mod immudb;
pub mod influxdb;
pub mod kafka;
pub mod libsql;
pub mod meilisearch;
pub mod memcached;
pub mod milvus;
pub mod mongodb;
pub mod mssql;
pub mod mysql;
pub mod neo4j;
pub mod objectdb;
pub mod oracle;
pub mod orientdb;
pub mod pinecone;
pub mod postgres;
pub mod prometheus;
pub mod qdrant;
pub mod qldb;
pub mod redis;
pub mod rocksdb;
pub mod s3;
pub mod snowflake;
pub mod spacetimedb;
pub mod sparql;
pub mod sql;
pub mod sqlite;
pub mod surrealdb;
pub mod tigergraph;
pub mod typesense;
pub mod val_town;
pub mod weaviate;

// ============================================================================
// INTEGRATION ADAPTER LAYER
//
// WHAT:  One adapter per database engine. Each lives in its own file and is the
//        only place that engine's client crate is imported.
// WHY:   Services, the guard and the UI see one contract (`Integration`) and
//        one value model (`model::Value`). Relational, document and key-value
//        stores all fit: a "table" is a table, a collection, or a key pattern;
//        a "row" is a record, a document, or a key/value pair; `execute` runs
//        the engine's native command language (SQL, Redis commands, Mongo shell).
// HOW:   To add an engine:
//          1. Add the variant to `model::Engine` + `ALL`, and map it in
//             `family()`, `kind()`, `as_str()`, `default_port()`.
//             If it is wire-compatible with an existing family (Postgres,
//             MySQL, CQL, Elasticsearch REST, Kafka…) you are done in Rust.
//          2. Otherwise add a `Family` variant and create
//             `integrations/<family>.rs` implementing `Integration`, mapping
//             the client crate's errors to `AppError` at the boundary.
//             HTTP/REST engines share `integrations::http::HttpClient`.
//          3. Add the match arm in `connect` below.
//          4. Add the crate to VENDOR_OWNERS in scripts/guardrail.py.
//          5. Add the UI entry in src/lib/engines.ts (fails `satisfies` until done).
//        Every other `match Engine` / `match Family` in the crate then fails
//        to compile until handled — that is the guardrail working.
// WHERE: src-tauri/src/integrations/postgres.rs, src-tauri/src/integrations/sqlite.rs
// ============================================================================

// WHAT:  What an adapter can do, so the UI adapts (hide SQL-only affordances for
//        a key-value store, label the editor "Command" instead of "SQL", etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct Capabilities {
    /// Accepts SQL text in `execute`.
    pub sql: bool,
    /// Groups collections under named schemas / databases / keyspaces.
    pub namespaces: bool,
    /// Has a stable column set per collection (false for schemaless documents).
    pub fixed_columns: bool,
    /// Supports offset paging in `fetch_page`.
    pub paging: bool,
    /// Can report an approximate row / key count cheaply.
    pub row_estimate: bool,
    /// Distinguishes tables from views.
    pub views: bool,
    /// `BEGIN … COMMIT` wraps multi-statement scripts (imports, pending changes).
    #[serde(default)]
    pub transactions: bool,
    /// `row_estimate` is an exact count, not a statistics guess.
    #[serde(default)]
    pub exact_estimate: bool,
}

impl Capabilities {
    /// A relational SQL engine with schemas, views, paging and transactions.
    pub const SQL: Capabilities = Capabilities {
        sql: true,
        namespaces: true,
        fixed_columns: true,
        paging: true,
        row_estimate: true,
        views: true,
        transactions: true,
        exact_estimate: false,
    };
    /// A schemaless document / key-value store driven by its own command language.
    pub const DOCUMENT: Capabilities = Capabilities {
        sql: false,
        namespaces: true,
        fixed_columns: false,
        paging: true,
        row_estimate: true,
        views: false,
        transactions: false,
        exact_estimate: false,
    };
    /// A key-value store: one namespace, whole-key loads, client-side paging.
    pub const KEY_VALUE: Capabilities = Capabilities {
        sql: false,
        namespaces: false,
        fixed_columns: true,
        paging: true,
        row_estimate: true,
        views: false,
        transactions: false,
        exact_estimate: true,
    };
}

// WHAT:  What a family exposes beyond rows: the object kinds its explorer can
//        list and the playground tools that make sense for it. Static per
//        family (no session needed), so the capability matrix renders for every
//        engine and the sidebar lays itself out before the first catalog load.
// WHY:   The UI never asks `if engine === "postgres"`; it asks the profile.
//        Declaring a kind here is a promise that `objects(kind, …)` answers it.
// WHERE: src-tauri/src/integrations/<family>.rs (`profile()`), src/lib/objects.ts
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct FamilyProfile {
    pub capabilities: Capabilities,
    pub object_kinds: Vec<ObjectKind>,
    pub tools: Vec<Tool>,
}

// WHAT:  Per-engine metadata reported once per session (status bar, editor label).
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct SessionInfo {
    pub engine: Engine,
    pub capabilities: Capabilities,
    pub object_kinds: Vec<ObjectKind>,
    pub tools: Vec<Tool>,
    pub server_version: Option<String>,
    /// Database this session is attached to (Postgres) or the file (SQLite).
    pub database: Option<String>,
    /// Every database the server exposes, for the sidebar switcher.
    pub databases: Vec<String>,
}

#[async_trait]
pub trait Integration: Send + Sync {
    fn engine(&self) -> Engine;
    fn capabilities(&self) -> Capabilities;
    async fn ping(&self) -> AppResult<()>;
    async fn server_version(&self) -> AppResult<Option<String>>;
    /// The database this session is attached to, if the engine has that notion.
    fn current_database(&self) -> Option<String>;
    /// All databases visible to this session (for switching). One entry for file engines.
    async fn databases(&self) -> AppResult<Vec<String>>;
    async fn catalog(&self) -> AppResult<SchemaCatalog>;
    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>>;
    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>>;
    /// Exact count, honouring filters. Used when the estimate is not good enough.
    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64>;
    /// `query.sort` is already defaulted (primary key) and validated by the service.
    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet>;
    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>>;
    async fn close(&self);
    /// Foreign keys visible to the session (ER diagram, FK traversal). Empty when unsupported.
    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        Ok(Vec::new())
    }
    /// CREATE statement for a table (export "include schema"). None when unsupported.
    async fn ddl(&self, _table: &TableRef) -> AppResult<Option<String>> {
        Ok(None)
    }

    // ---- object explorer / administration ------------------------------------
    // WHAT:  Lists objects of one kind. `parent` is the namespace for scoped kinds
    //        (`ObjectKind::scoped`) — None means every user namespace, system
    //        ones excluded — and the owner (table, topic…) for nested ones. Only
    //        kinds declared in the family's `profile()` are ever asked for.
    async fn objects(&self, _kind: ObjectKind, _parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        Ok(Vec::new())
    }
    /// Definition, properties, nested objects and actions for one object.
    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        Ok(ObjectDetail::empty(reference))
    }
    /// Monitoring figures (connections, memory, cache, replication…), grouped.
    async fn server_stats(&self) -> AppResult<ServerStats> {
        Ok(ServerStats::default())
    }

    // ---- playground tools (declare the matching `Tool` in `profile()`) --------
    async fn vector_search(&self, _req: &VectorSearchRequest) -> AppResult<ResultSet> {
        Err(AppError::invalid_input("This engine has no vector search."))
    }
    async fn search(&self, _req: &SearchRequest) -> AppResult<SearchResult> {
        Err(AppError::invalid_input("This engine has no full-text search API."))
    }
    async fn query_range(&self, _req: &RangeQueryRequest) -> AppResult<RangeResult> {
        Err(AppError::invalid_input("This engine has no range query API."))
    }
    /// Immutable history of one key / row (ledger engines).
    async fn history(&self, _reference: &ObjectRef) -> AppResult<ResultSet> {
        Err(AppError::invalid_input("This engine keeps no verifiable history."))
    }
}

// WHAT:  Static profile per adapter family (see `FamilyProfile`).
pub fn profile(family: Family) -> FamilyProfile {
    match family {
        Family::Postgres => postgres::profile(),
        Family::Mysql => mysql::profile(),
        Family::Mssql => mssql::profile(),
        Family::Oracle => self::oracle::profile(),
        Family::Sqlite => sqlite::profile(),
        Family::Duckdb => self::duckdb::profile(),
        Family::Rocksdb => self::rocksdb::profile(),
        Family::Clickhouse => clickhouse::profile(),
        Family::Redis => self::redis::profile(),
        Family::Memcached => memcached::profile(),
        Family::Mongodb => self::mongodb::profile(),
        Family::Couchdb => couchdb::profile(),
        Family::Firestore => firestore::profile(),
        Family::Dynamodb => dynamodb::profile(),
        Family::Cassandra => cassandra::profile(),
        Family::Hbase => hbase::profile(),
        Family::Neo4j => neo4j::profile(),
        Family::Tigergraph => tigergraph::profile(),
        Family::Influxdb => influxdb::profile(),
        Family::Prometheus => prometheus::profile(),
        Family::Qdrant => qdrant::profile(),
        Family::Milvus => milvus::profile(),
        Family::Weaviate => weaviate::profile(),
        Family::Pinecone => pinecone::profile(),
        Family::Chroma => chroma::profile(),
        Family::Elasticsearch => elasticsearch::profile(),
        Family::Meilisearch => meilisearch::profile(),
        Family::Typesense => typesense::profile(),
        Family::Arangodb => arangodb::profile(),
        Family::Surrealdb => surrealdb::profile(),
        Family::Orientdb => orientdb::profile(),
        Family::Druid => druid::profile(),
        Family::Snowflake => snowflake::profile(),
        Family::Bigquery => bigquery::profile(),
        Family::Libsql => libsql::profile(),
        Family::ValTown => val_town::profile(),
        Family::CloudflareD1 => cloudflare_d1::profile(),
        Family::Immudb => immudb::profile(),
        Family::Qldb => qldb::profile(),
        Family::Kafka => kafka::profile(),
        Family::Objectdb => objectdb::profile(),
        Family::Sparql => sparql::profile(),
        Family::Basex => basex::profile(),
        Family::Existdb => existdb::profile(),
        Family::Spacetimedb => spacetimedb::profile(),
        Family::S3 => s3::profile(),
    }
}

// WHAT:  The single dispatch point from a resolved connection to its adapter.
pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    match conn.summary.engine.family() {
        Family::Postgres => postgres::connect(conn).await,
        Family::Mysql => mysql::connect(conn).await,
        Family::Mssql => mssql::connect(conn).await,
        Family::Oracle => oracle::connect(conn).await,
        Family::Sqlite => sqlite::connect(conn).await,
        Family::Duckdb => duckdb::connect(conn).await,
        Family::Rocksdb => rocksdb::connect(conn).await,
        Family::Clickhouse => clickhouse::connect(conn).await,
        Family::Redis => self::redis::connect(conn).await,
        Family::Memcached => memcached::connect(conn).await,
        Family::Mongodb => self::mongodb::connect(conn).await,
        Family::Couchdb => couchdb::connect(conn).await,
        Family::Firestore => firestore::connect(conn).await,
        Family::Dynamodb => dynamodb::connect(conn).await,
        Family::Cassandra => cassandra::connect(conn).await,
        Family::Hbase => hbase::connect(conn).await,
        Family::Neo4j => neo4j::connect(conn).await,
        Family::Tigergraph => tigergraph::connect(conn).await,
        Family::Influxdb => influxdb::connect(conn).await,
        Family::Prometheus => prometheus::connect(conn).await,
        Family::Qdrant => qdrant::connect(conn).await,
        Family::Milvus => milvus::connect(conn).await,
        Family::Weaviate => weaviate::connect(conn).await,
        Family::Pinecone => pinecone::connect(conn).await,
        Family::Chroma => chroma::connect(conn).await,
        Family::Elasticsearch => elasticsearch::connect(conn).await,
        Family::Meilisearch => meilisearch::connect(conn).await,
        Family::Typesense => typesense::connect(conn).await,
        Family::Arangodb => arangodb::connect(conn).await,
        Family::Surrealdb => surrealdb::connect(conn).await,
        Family::Orientdb => orientdb::connect(conn).await,
        Family::Druid => druid::connect(conn).await,
        Family::Snowflake => snowflake::connect(conn).await,
        Family::Bigquery => bigquery::connect(conn).await,
        Family::Libsql => libsql::connect(conn).await,
        Family::ValTown => val_town::connect(conn).await,
        Family::CloudflareD1 => cloudflare_d1::connect(conn).await,
        Family::Immudb => immudb::connect(conn).await,
        Family::Qldb => qldb::connect(conn).await,
        Family::Kafka => kafka::connect(conn).await,
        Family::Objectdb => objectdb::connect(conn).await,
        Family::Sparql => sparql::connect(conn).await,
        Family::Basex => basex::connect(conn).await,
        Family::Existdb => existdb::connect(conn).await,
        Family::Spacetimedb => spacetimedb::connect(conn).await,
        Family::S3 => s3::connect(conn).await,
    }
}

pub async fn describe(integration: &dyn Integration) -> AppResult<SessionInfo> {
    let family = profile(integration.engine().family());
    Ok(SessionInfo {
        engine: integration.engine(),
        capabilities: integration.capabilities(),
        object_kinds: family.object_kinds,
        tools: family.tools,
        server_version: integration.server_version().await.unwrap_or(None),
        database: integration.current_database(),
        databases: integration.databases().await.unwrap_or_default(),
    })
}

// WHAT:  Double-quote an identifier (Postgres, SQLite, ClickHouse all accept "..").
pub fn quote_ident(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\"\""))
}

// WHAT:  Engine-aware identifier quoting: MySQL family uses backticks, MSSQL
//        brackets, everything else double quotes (ANSI, also what CQL, DuckDB,
//        ClickHouse, Snowflake, Oracle and BigQuery-via-ANSI accept).
pub fn quote_ident_for(engine: Engine, raw: &str) -> String {
    match engine.family() {
        Family::Mysql => format!("`{}`", raw.replace('`', "``")),
        Family::Mssql => format!("[{}]", raw.replace(']', "]]")),
        Family::Bigquery => format!("`{}`", raw.replace('`', "\\`")),
        _ => quote_ident(raw),
    }
}

pub fn qualified_name(table: &TableRef) -> String {
    qualified_name_for(Engine::Postgres, table)
}

pub fn qualified_name_for(engine: Engine, table: &TableRef) -> String {
    match &table.schema {
        Some(schema) => format!("{}.{}", quote_ident_for(engine, schema), quote_ident_for(engine, &table.name)),
        None => quote_ident_for(engine, &table.name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_escapes_embedded_quotes() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        let t = TableRef { schema: Some("public".into()), name: "users".into() };
        assert_eq!(qualified_name(&t), "\"public\".\"users\"");
        assert_eq!(quote_ident_for(Engine::Mysql, "we`ird"), "`we``ird`");
        assert_eq!(qualified_name_for(Engine::Mariadb, &t), "`public`.`users`");
    }
}
