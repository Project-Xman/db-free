// SOT: engine, environment, ssl-mode, connection-input, connection-summary, connection-validation

use crate::error::{AppError, AppResult};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

// WHAT:  Every database this workbench can open. Adding a variant is the whole
//        "add an engine" entry point: every `match` on Engine then fails to
//        compile until handled.
// WHY:   The registry pattern — one enum, everything else derived from it.
//        Three derived views keep the rest of the crate small:
//          `family()`  which adapter module speaks to it (one adapter serves
//                      every wire-compatible engine: Postgres ⇒ Supabase, Neon,
//                      TimescaleDB, CockroachDB, YugabyteDB …)
//          `kind()`    the product category the picker groups by
//          `form()`    which connection fields the UI shows
// HOW:   integrations::connect dispatches on `family()`; the UI reads Engine
//        from bindings and ENGINES in src/lib/engines.ts must list every variant.
// WHERE: src-tauri/src/integrations/mod.rs, src/lib/engines.ts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum Engine {
    // Relational / SQL
    Postgres,
    Mysql,
    Mariadb,
    Mssql,
    Oracle,
    // Document
    Mongodb,
    Couchdb,
    Firestore,
    // Key-value
    Redis,
    Valkey,
    Dynamodb,
    // Wide-column
    Cassandra,
    Scylladb,
    Hbase,
    // Graph
    Neo4j,
    Memgraph,
    Tigergraph,
    // Time-series
    Timescaledb,
    Influxdb,
    Victoriametrics,
    Prometheus,
    Questdb,
    // Vector
    Qdrant,
    Milvus,
    Weaviate,
    Pinecone,
    Chroma,
    Pgvector,
    // Search / full-text
    Elasticsearch,
    Opensearch,
    Meilisearch,
    Typesense,
    // Multi-model
    Arangodb,
    Surrealdb,
    Orientdb,
    // Spatial
    Postgis,
    Spatialite,
    // In-memory
    Memcached,
    Dragonfly,
    // Columnar / OLAP
    Clickhouse,
    Duckdb,
    Druid,
    Snowflake,
    Bigquery,
    // NewSQL / distributed SQL
    Cockroachdb,
    Tidb,
    Yugabytedb,
    // Embedded / serverless SQL
    Sqlite,
    Rocksdb,
    Libsql,
    ValTown,
    CloudflareD1,
    // Hosted presets (wire-compatible with an engine above)
    Supabase,
    Planetscale,
    Neon,
    // Ledger
    Immudb,
    Qldb,
    // Event / streaming
    Kafka,
    Redpanda,
    // Object
    Objectdb,
    // Hierarchical / network (legacy, reached over their SQL gateways)
    IbmIms,
    RaimaRdm,
    // XML
    Basex,
    Existdb,
    // RDF / triple store
    ApacheJena,
    Graphdb,
    Stardog,
    Blazegraph,
    Virtuoso,
}

// WHAT:  The adapter module that speaks an engine's wire protocol.
// WHY:   Many products are wire-compatible (Postgres family, MySQL family,
//        Cassandra CQL, Elasticsearch REST, Kafka protocol…). One adapter per
//        protocol, many engines per adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    Postgres,
    Mysql,
    Mssql,
    Oracle,
    Sqlite,
    Duckdb,
    Rocksdb,
    Clickhouse,
    Redis,
    Memcached,
    Mongodb,
    Couchdb,
    Firestore,
    Dynamodb,
    Cassandra,
    Hbase,
    Neo4j,
    Tigergraph,
    Influxdb,
    Prometheus,
    Qdrant,
    Milvus,
    Weaviate,
    Pinecone,
    Chroma,
    Elasticsearch,
    Meilisearch,
    Typesense,
    Arangodb,
    Surrealdb,
    Orientdb,
    Druid,
    Snowflake,
    Bigquery,
    Libsql,
    ValTown,
    CloudflareD1,
    Immudb,
    Qldb,
    Kafka,
    Objectdb,
    Sparql,
    Basex,
    Existdb,
}

// WHAT:  Product category — the section the picker groups engines under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum EngineKind {
    Relational,
    Document,
    KeyValue,
    WideColumn,
    Graph,
    TimeSeries,
    Vector,
    Search,
    MultiModel,
    Spatial,
    InMemory,
    Analytical,
    NewSql,
    Embedded,
    Ledger,
    Streaming,
    Object,
    Hierarchical,
    Network,
    Xml,
    GraphVector,
    Rdf,
}

// WHAT:  Which set of fields the connection form renders for an engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum FormKind {
    /// host / port / database / user / password / SSL
    Server,
    /// local file or directory path
    File,
    /// base URL + API token (+ optional database / index name)
    HttpToken,
    /// region + access key + secret (AWS SigV4)
    Aws,
    /// project id + service-account JSON (Google)
    Gcp,
}

impl Engine {
    pub const ALL: [Engine; 69] = [
        Engine::Postgres,
        Engine::Mysql,
        Engine::Mariadb,
        Engine::Mssql,
        Engine::Oracle,
        Engine::Mongodb,
        Engine::Couchdb,
        Engine::Firestore,
        Engine::Redis,
        Engine::Valkey,
        Engine::Dynamodb,
        Engine::Cassandra,
        Engine::Scylladb,
        Engine::Hbase,
        Engine::Neo4j,
        Engine::Memgraph,
        Engine::Tigergraph,
        Engine::Timescaledb,
        Engine::Influxdb,
        Engine::Victoriametrics,
        Engine::Prometheus,
        Engine::Questdb,
        Engine::Qdrant,
        Engine::Milvus,
        Engine::Weaviate,
        Engine::Pinecone,
        Engine::Chroma,
        Engine::Pgvector,
        Engine::Elasticsearch,
        Engine::Opensearch,
        Engine::Meilisearch,
        Engine::Typesense,
        Engine::Arangodb,
        Engine::Surrealdb,
        Engine::Orientdb,
        Engine::Postgis,
        Engine::Spatialite,
        Engine::Memcached,
        Engine::Dragonfly,
        Engine::Clickhouse,
        Engine::Duckdb,
        Engine::Druid,
        Engine::Snowflake,
        Engine::Bigquery,
        Engine::Cockroachdb,
        Engine::Tidb,
        Engine::Yugabytedb,
        Engine::Sqlite,
        Engine::Rocksdb,
        Engine::Libsql,
        Engine::ValTown,
        Engine::CloudflareD1,
        Engine::Supabase,
        Engine::Planetscale,
        Engine::Neon,
        Engine::Immudb,
        Engine::Qldb,
        Engine::Kafka,
        Engine::Redpanda,
        Engine::Objectdb,
        Engine::IbmIms,
        Engine::RaimaRdm,
        Engine::Basex,
        Engine::Existdb,
        Engine::ApacheJena,
        Engine::Graphdb,
        Engine::Stardog,
        Engine::Blazegraph,
        Engine::Virtuoso,
    ];

    // WHAT:  The adapter that speaks this engine's protocol.
    pub fn family(self) -> Family {
        match self {
            Engine::Postgres
            | Engine::Supabase
            | Engine::Neon
            | Engine::Timescaledb
            | Engine::Questdb
            | Engine::Pgvector
            | Engine::Postgis
            | Engine::Cockroachdb
            | Engine::Yugabytedb
            | Engine::IbmIms
            | Engine::RaimaRdm => Family::Postgres,
            Engine::Mysql | Engine::Mariadb | Engine::Planetscale | Engine::Tidb => Family::Mysql,
            Engine::Mssql => Family::Mssql,
            Engine::Oracle => Family::Oracle,
            Engine::Sqlite | Engine::Spatialite => Family::Sqlite,
            Engine::Duckdb => Family::Duckdb,
            Engine::Rocksdb => Family::Rocksdb,
            Engine::Clickhouse => Family::Clickhouse,
            Engine::Redis | Engine::Valkey | Engine::Dragonfly => Family::Redis,
            Engine::Memcached => Family::Memcached,
            Engine::Mongodb => Family::Mongodb,
            Engine::Couchdb => Family::Couchdb,
            Engine::Firestore => Family::Firestore,
            Engine::Dynamodb => Family::Dynamodb,
            Engine::Cassandra | Engine::Scylladb => Family::Cassandra,
            Engine::Hbase => Family::Hbase,
            Engine::Neo4j | Engine::Memgraph => Family::Neo4j,
            Engine::Tigergraph => Family::Tigergraph,
            Engine::Influxdb => Family::Influxdb,
            Engine::Victoriametrics | Engine::Prometheus => Family::Prometheus,
            Engine::Qdrant => Family::Qdrant,
            Engine::Milvus => Family::Milvus,
            Engine::Weaviate => Family::Weaviate,
            Engine::Pinecone => Family::Pinecone,
            Engine::Chroma => Family::Chroma,
            Engine::Elasticsearch | Engine::Opensearch => Family::Elasticsearch,
            Engine::Meilisearch => Family::Meilisearch,
            Engine::Typesense => Family::Typesense,
            Engine::Arangodb => Family::Arangodb,
            Engine::Surrealdb => Family::Surrealdb,
            Engine::Orientdb => Family::Orientdb,
            Engine::Druid => Family::Druid,
            Engine::Snowflake => Family::Snowflake,
            Engine::Bigquery => Family::Bigquery,
            Engine::Libsql => Family::Libsql,
            Engine::ValTown => Family::ValTown,
            Engine::CloudflareD1 => Family::CloudflareD1,
            Engine::Immudb => Family::Immudb,
            Engine::Qldb => Family::Qldb,
            Engine::Kafka | Engine::Redpanda => Family::Kafka,
            Engine::Objectdb => Family::Objectdb,
            Engine::ApacheJena | Engine::Graphdb | Engine::Stardog | Engine::Blazegraph | Engine::Virtuoso => Family::Sparql,
            Engine::Basex => Family::Basex,
            Engine::Existdb => Family::Existdb,
        }
    }

    // WHAT:  Product category for the picker.
    pub fn kind(self) -> EngineKind {
        match self {
            Engine::Postgres
            | Engine::Mysql
            | Engine::Mariadb
            | Engine::Mssql
            | Engine::Oracle
            | Engine::Supabase
            | Engine::Planetscale
            | Engine::Neon => EngineKind::Relational,
            Engine::Mongodb | Engine::Couchdb | Engine::Firestore => EngineKind::Document,
            Engine::Redis | Engine::Valkey | Engine::Dynamodb => EngineKind::KeyValue,
            Engine::Cassandra | Engine::Scylladb | Engine::Hbase => EngineKind::WideColumn,
            Engine::Neo4j | Engine::Memgraph | Engine::Tigergraph => EngineKind::Graph,
            Engine::Timescaledb
            | Engine::Influxdb
            | Engine::Victoriametrics
            | Engine::Prometheus
            | Engine::Questdb => EngineKind::TimeSeries,
            Engine::Qdrant | Engine::Milvus | Engine::Pinecone | Engine::Chroma | Engine::Pgvector => EngineKind::Vector,
            Engine::Elasticsearch | Engine::Opensearch | Engine::Meilisearch | Engine::Typesense => EngineKind::Search,
            Engine::Surrealdb | Engine::Orientdb => EngineKind::MultiModel,
            Engine::Arangodb | Engine::Weaviate => EngineKind::GraphVector,
            Engine::Postgis | Engine::Spatialite => EngineKind::Spatial,
            Engine::Memcached | Engine::Dragonfly => EngineKind::InMemory,
            Engine::Clickhouse | Engine::Duckdb | Engine::Druid | Engine::Snowflake | Engine::Bigquery => EngineKind::Analytical,
            Engine::Cockroachdb | Engine::Tidb | Engine::Yugabytedb => EngineKind::NewSql,
            Engine::Sqlite | Engine::Rocksdb | Engine::Libsql | Engine::ValTown | Engine::CloudflareD1 => EngineKind::Embedded,
            Engine::Immudb | Engine::Qldb => EngineKind::Ledger,
            Engine::Kafka | Engine::Redpanda => EngineKind::Streaming,
            Engine::Objectdb => EngineKind::Object,
            Engine::IbmIms => EngineKind::Hierarchical,
            Engine::RaimaRdm => EngineKind::Network,
            Engine::Basex | Engine::Existdb => EngineKind::Xml,
            Engine::ApacheJena | Engine::Graphdb | Engine::Stardog | Engine::Blazegraph | Engine::Virtuoso => EngineKind::Rdf,
        }
    }

    // WHAT:  Which fields the connection form renders.
    pub fn form(self) -> FormKind {
        match self.family() {
            Family::Sqlite | Family::Duckdb | Family::Rocksdb => FormKind::File,
            Family::Dynamodb | Family::Qldb => FormKind::Aws,
            Family::Firestore | Family::Bigquery => FormKind::Gcp,
            Family::Libsql
            | Family::ValTown
            | Family::CloudflareD1
            | Family::Pinecone
            | Family::Snowflake
            | Family::Qdrant
            | Family::Weaviate
            | Family::Meilisearch
            | Family::Typesense
            | Family::Chroma
            | Family::Milvus
            | Family::Influxdb
            | Family::Prometheus
            | Family::Elasticsearch
            | Family::Druid
            | Family::Tigergraph
            | Family::Sparql
            | Family::Basex
            | Family::Existdb
            | Family::Hbase
            | Family::Couchdb
            | Family::Arangodb
            | Family::Surrealdb
            | Family::Orientdb
            | Family::Immudb
            | Family::Objectdb => FormKind::HttpToken,
            Family::Postgres
            | Family::Mysql
            | Family::Mssql
            | Family::Oracle
            | Family::Clickhouse
            | Family::Redis
            | Family::Memcached
            | Family::Mongodb
            | Family::Cassandra
            | Family::Neo4j
            | Family::Kafka => FormKind::Server,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Engine::Postgres => "postgres",
            Engine::Mysql => "mysql",
            Engine::Mariadb => "mariadb",
            Engine::Mssql => "mssql",
            Engine::Oracle => "oracle",
            Engine::Mongodb => "mongodb",
            Engine::Couchdb => "couchdb",
            Engine::Firestore => "firestore",
            Engine::Redis => "redis",
            Engine::Valkey => "valkey",
            Engine::Dynamodb => "dynamodb",
            Engine::Cassandra => "cassandra",
            Engine::Scylladb => "scylladb",
            Engine::Hbase => "hbase",
            Engine::Neo4j => "neo4j",
            Engine::Memgraph => "memgraph",
            Engine::Tigergraph => "tigergraph",
            Engine::Timescaledb => "timescaledb",
            Engine::Influxdb => "influxdb",
            Engine::Victoriametrics => "victoriametrics",
            Engine::Prometheus => "prometheus",
            Engine::Questdb => "questdb",
            Engine::Qdrant => "qdrant",
            Engine::Milvus => "milvus",
            Engine::Weaviate => "weaviate",
            Engine::Pinecone => "pinecone",
            Engine::Chroma => "chroma",
            Engine::Pgvector => "pgvector",
            Engine::Elasticsearch => "elasticsearch",
            Engine::Opensearch => "opensearch",
            Engine::Meilisearch => "meilisearch",
            Engine::Typesense => "typesense",
            Engine::Arangodb => "arangodb",
            Engine::Surrealdb => "surrealdb",
            Engine::Orientdb => "orientdb",
            Engine::Postgis => "postgis",
            Engine::Spatialite => "spatialite",
            Engine::Memcached => "memcached",
            Engine::Dragonfly => "dragonfly",
            Engine::Clickhouse => "clickhouse",
            Engine::Duckdb => "duckdb",
            Engine::Druid => "druid",
            Engine::Snowflake => "snowflake",
            Engine::Bigquery => "bigquery",
            Engine::Cockroachdb => "cockroachdb",
            Engine::Tidb => "tidb",
            Engine::Yugabytedb => "yugabytedb",
            Engine::Sqlite => "sqlite",
            Engine::Rocksdb => "rocksdb",
            Engine::Libsql => "libsql",
            Engine::ValTown => "val_town",
            Engine::CloudflareD1 => "cloudflare_d1",
            Engine::Supabase => "supabase",
            Engine::Planetscale => "planetscale",
            Engine::Neon => "neon",
            Engine::Immudb => "immudb",
            Engine::Qldb => "qldb",
            Engine::Kafka => "kafka",
            Engine::Redpanda => "redpanda",
            Engine::Objectdb => "objectdb",
            Engine::IbmIms => "ibm_ims",
            Engine::RaimaRdm => "raima_rdm",
            Engine::Basex => "basex",
            Engine::Existdb => "existdb",
            Engine::ApacheJena => "apache_jena",
            Engine::Graphdb => "graphdb",
            Engine::Stardog => "stardog",
            Engine::Blazegraph => "blazegraph",
            Engine::Virtuoso => "virtuoso",
        }
    }

    // WHAT:  Human name (AI prompts, history log). The UI has its own copy in
    //        src/lib/engines.ts so it can add hints and icons.
    pub fn label(self) -> &'static str {
        match self {
            Engine::Postgres => "PostgreSQL",
            Engine::Mysql => "MySQL",
            Engine::Mariadb => "MariaDB",
            Engine::Mssql => "SQL Server",
            Engine::Oracle => "Oracle Database",
            Engine::Mongodb => "MongoDB",
            Engine::Couchdb => "CouchDB",
            Engine::Firestore => "Firestore",
            Engine::Redis => "Redis",
            Engine::Valkey => "Valkey",
            Engine::Dynamodb => "DynamoDB",
            Engine::Cassandra => "Cassandra",
            Engine::Scylladb => "ScyllaDB",
            Engine::Hbase => "HBase",
            Engine::Neo4j => "Neo4j",
            Engine::Memgraph => "Memgraph",
            Engine::Tigergraph => "TigerGraph",
            Engine::Timescaledb => "TimescaleDB",
            Engine::Influxdb => "InfluxDB",
            Engine::Victoriametrics => "VictoriaMetrics",
            Engine::Prometheus => "Prometheus",
            Engine::Questdb => "QuestDB",
            Engine::Qdrant => "Qdrant",
            Engine::Milvus => "Milvus",
            Engine::Weaviate => "Weaviate",
            Engine::Pinecone => "Pinecone",
            Engine::Chroma => "Chroma",
            Engine::Pgvector => "pgvector",
            Engine::Elasticsearch => "Elasticsearch",
            Engine::Opensearch => "OpenSearch",
            Engine::Meilisearch => "Meilisearch",
            Engine::Typesense => "Typesense",
            Engine::Arangodb => "ArangoDB",
            Engine::Surrealdb => "SurrealDB",
            Engine::Orientdb => "OrientDB",
            Engine::Postgis => "PostGIS",
            Engine::Spatialite => "SpatiaLite",
            Engine::Memcached => "Memcached",
            Engine::Dragonfly => "Dragonfly",
            Engine::Clickhouse => "ClickHouse",
            Engine::Duckdb => "DuckDB",
            Engine::Druid => "Apache Druid",
            Engine::Snowflake => "Snowflake",
            Engine::Bigquery => "BigQuery",
            Engine::Cockroachdb => "CockroachDB",
            Engine::Tidb => "TiDB",
            Engine::Yugabytedb => "YugabyteDB",
            Engine::Sqlite => "SQLite",
            Engine::Rocksdb => "RocksDB",
            Engine::Libsql => "LibSQL / Turso",
            Engine::ValTown => "Val Town",
            Engine::CloudflareD1 => "Cloudflare D1",
            Engine::Supabase => "Supabase",
            Engine::Planetscale => "PlanetScale",
            Engine::Neon => "Neon",
            Engine::Immudb => "immudb",
            Engine::Qldb => "Amazon QLDB",
            Engine::Kafka => "Apache Kafka",
            Engine::Redpanda => "Redpanda",
            Engine::Objectdb => "ObjectDB",
            Engine::IbmIms => "IBM IMS",
            Engine::RaimaRdm => "Raima RDM",
            Engine::Basex => "BaseX",
            Engine::Existdb => "eXist-db",
            Engine::ApacheJena => "Apache Jena",
            Engine::Graphdb => "GraphDB",
            Engine::Stardog => "Stardog",
            Engine::Blazegraph => "Blazegraph",
            Engine::Virtuoso => "Virtuoso",
        }
    }

    pub fn parse(raw: &str) -> Option<Engine> {
        Engine::ALL.into_iter().find(|e| e.as_str() == raw)
    }

    pub fn is_file_based(self) -> bool {
        self.form() == FormKind::File
    }

    pub fn is_http_token_based(self) -> bool {
        self.form() == FormKind::HttpToken
    }

    // WHAT:  Engines whose `execute` accepts SQL text (drives editor mode + AI dialect).
    pub fn speaks_sql(self) -> bool {
        matches!(
            self.family(),
            Family::Postgres
                | Family::Mysql
                | Family::Mssql
                | Family::Oracle
                | Family::Sqlite
                | Family::Duckdb
                | Family::Clickhouse
                | Family::Libsql
                | Family::ValTown
                | Family::CloudflareD1
                | Family::Cassandra
                | Family::Druid
                | Family::Snowflake
                | Family::Bigquery
                | Family::Immudb
                | Family::Qldb
                | Family::Surrealdb
                | Family::Orientdb
                | Family::Influxdb
        )
    }

    pub fn default_port(self) -> Option<u16> {
        match self {
            Engine::Postgres
            | Engine::Supabase
            | Engine::Neon
            | Engine::Timescaledb
            | Engine::Pgvector
            | Engine::Postgis
            | Engine::IbmIms
            | Engine::RaimaRdm => Some(5432),
            Engine::Questdb => Some(8812),
            Engine::Cockroachdb => Some(26257),
            Engine::Yugabytedb => Some(5433),
            Engine::Mysql | Engine::Mariadb | Engine::Planetscale => Some(3306),
            Engine::Tidb => Some(4000),
            Engine::Mssql => Some(1433),
            Engine::Oracle => Some(1521),
            Engine::Clickhouse => Some(8123),
            Engine::Redis | Engine::Valkey | Engine::Dragonfly => Some(6379),
            Engine::Memcached => Some(11211),
            Engine::Mongodb => Some(27017),
            Engine::Couchdb => Some(5984),
            Engine::Cassandra | Engine::Scylladb => Some(9042),
            Engine::Hbase => Some(8080),
            Engine::Neo4j | Engine::Memgraph => Some(7687),
            Engine::Tigergraph => Some(14240),
            Engine::Influxdb => Some(8086),
            Engine::Victoriametrics => Some(8428),
            Engine::Prometheus => Some(9090),
            Engine::Qdrant => Some(6333),
            Engine::Milvus => Some(19530),
            Engine::Weaviate => Some(8080),
            Engine::Chroma => Some(8000),
            Engine::Elasticsearch | Engine::Opensearch => Some(9200),
            Engine::Meilisearch => Some(7700),
            Engine::Typesense => Some(8108),
            Engine::Arangodb => Some(8529),
            Engine::Surrealdb => Some(8000),
            Engine::Orientdb => Some(2480),
            Engine::Druid => Some(8888),
            // immudb's HTTP web-api port (3322 is gRPC, which this adapter does not speak).
            Engine::Immudb => Some(8080),
            Engine::Kafka | Engine::Redpanda => Some(9092),
            Engine::Objectdb => Some(6136),
            Engine::Basex => Some(8984),
            Engine::Existdb => Some(8080),
            Engine::ApacheJena => Some(3030),
            Engine::Graphdb => Some(7200),
            Engine::Stardog => Some(5820),
            Engine::Blazegraph => Some(9999),
            Engine::Virtuoso => Some(8890),
            Engine::Sqlite
            | Engine::Spatialite
            | Engine::Duckdb
            | Engine::Rocksdb
            | Engine::Libsql
            | Engine::ValTown
            | Engine::CloudflareD1
            | Engine::Firestore
            | Engine::Dynamodb
            | Engine::Pinecone
            | Engine::Snowflake
            | Engine::Bigquery
            | Engine::Qldb => None,
        }
    }
}

// WHAT:  The facts about an engine that Rust owns, exported so the UI registry
//        can be checked against them instead of restating them by hand.
// WHY:   `src/lib/engines.ts` carries labels, hints and field names the Rust
//        core has no opinion on, but `kind`, `form` and `defaultPort` are
//        decided here. Exporting them makes a disagreement a TypeScript error
//        rather than a wrong port in a connection dialog.
// HOW:   `pnpm bindings` writes EngineFacts.ts; engines.ts does
//        `satisfies Record<Engine, EngineMeta & EngineFacts[…]>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
#[ts(export)]
pub struct EngineFacts {
    pub kind: EngineKind,
    pub form: FormKind,
    pub default_port: Option<u16>,
}

impl Engine {
    pub fn facts(self) -> EngineFacts {
        EngineFacts { kind: self.kind(), form: self.form(), default_port: self.default_port() }
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
        let blank = |v: &Option<String>| v.as_deref().map(str::trim).unwrap_or_default().is_empty();
        match self.engine.form() {
            FormKind::File => {
                if blank(&self.file_path) {
                    return Err(AppError::invalid_input("A database file path is required."));
                }
            }
            FormKind::HttpToken => {
                // Hosted services without a self-hosted URL fall back to the vendor endpoint.
                let hosted = matches!(self.engine, Engine::ValTown | Engine::Pinecone | Engine::Snowflake);
                if blank(&self.host) && !hosted {
                    return Err(AppError::invalid_input("Server URL or host is required."));
                }
                if self.engine == Engine::CloudflareD1 && blank(&self.database) {
                    return Err(AppError::invalid_input("Cloudflare Database ID is required in the database field."));
                }
                if self.engine == Engine::Snowflake && blank(&self.host) {
                    return Err(AppError::invalid_input("Snowflake account identifier is required in the host field."));
                }
            }
            FormKind::Aws => {
                if blank(&self.host) {
                    return Err(AppError::invalid_input("AWS region is required (e.g. us-east-1)."));
                }
                if blank(&self.username) {
                    return Err(AppError::invalid_input("AWS access key ID is required."));
                }
            }
            FormKind::Gcp => {
                if blank(&self.database) {
                    return Err(AppError::invalid_input("Google Cloud project ID is required."));
                }
            }
            FormKind::Server => {
                if blank(&self.host) {
                    return Err(AppError::invalid_input("Host is required."));
                }
                // Database is optional: an empty value connects to the server's default
                // database and the UI offers every database for switching.
                if self.port == Some(0) {
                    return Err(AppError::invalid_input("Port must be between 1 and 65535."));
                }
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
        // ALL must list every variant exactly once (the registry contract).
        let mut seen = std::collections::HashSet::new();
        for e in Engine::ALL {
            assert!(seen.insert(e), "{e:?} listed twice in Engine::ALL");
        }
        for e in Environment::ALL {
            assert_eq!(Environment::parse(e.as_str()), Some(e));
        }
        for m in SslMode::ALL {
            assert_eq!(SslMode::parse(m.as_str()), Some(m));
        }
    }
}
