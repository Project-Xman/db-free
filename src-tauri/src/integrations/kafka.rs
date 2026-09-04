// SOT: kafka-integration, redpanda-integration, rskafka-adapter, topic-browser, kafka-record-decoding, kafka-console-commands

use crate::error::{AppError, AppResult};
use crate::integrations::http::local;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::{Record, RecordAndOffset};
use rskafka::BackoffConfig;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// KAFKA / REDPANDA ADAPTER
//
// WHAT:  Maps a log-structured message broker onto the engine-neutral
//        `Integration`: one schema `topics`, one table per topic, one row per
//        record with fixed columns (partition, offset, timestamp, key, value,
//        headers).
// WHY:   Browsing the tail of a topic and publishing a test message is the
//        whole workbench use case; consumer groups and streaming are out of scope.
// HOW:   catalog     = list_topics + watermark deltas as the row estimate
//        fetch_page  = for each partition, read the latest N records
//                      (bounded), then http::local::page for filter/sort/slice
//        count       = Σ (high − low watermark)
//        execute     = JSON {"topic", …} / {"produce": {…}} or the shorthands
//                      `TOPICS` and `CONSUME <topic> [n]`
//        `rskafka` is the only vendor crate used, and only in this file.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI meta)
// ============================================================================

const MAX_RECORDS: u64 = 1_000;
const FETCH_BYTES: i32 = 4 * 1024 * 1024;
const FETCH_WAIT_MS: i32 = 500;
const TOPICS_SCHEMA: &str = "topics";
const COLUMN_NAMES: [&str; 6] = ["partition", "offset", "timestamp", "key", "value", "headers"];

pub struct KafkaIntegration {
    client: Client,
    engine: Engine,
    topic_filter: Option<String>,
    read_only: bool,
}

fn map_error(err: rskafka::client::error::Error) -> AppError {
    use rskafka::client::error::Error as E;
    match err {
        E::Connection(inner) => {
            let text = inner.to_string();
            if text.to_ascii_lowercase().contains("sasl") {
                AppError::not_connected(format!("Authentication failed: {text}"))
            } else {
                AppError::not_connected(format!("Could not reach the broker: {text}"))
            }
        }
        E::ServerError { protocol_error, error_message, .. } => {
            let detail = error_message.unwrap_or_default();
            // `rskafka::protocol` is a private module, so the error code is only
            // reachable through its Debug name.
            let code = format!("{protocol_error:?}");
            let text = format!("{code}: {detail}");
            if code == "UnknownTopicOrPartition" {
                AppError::not_found(text)
            } else if code.contains("AuthenticationFailed") || code.contains("AuthorizationFailed") {
                AppError::not_connected(text)
            } else {
                AppError::driver(text)
            }
        }
        E::Timeout => AppError::timeout("The broker did not respond in time."),
        other => AppError::driver(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

// WHAT:  `h1:9092,h2` → bootstrap list; hosts without a port get the port field.
fn bootstrap_servers(host: Option<&str>, port: Option<u16>) -> Vec<String> {
    let port = port.unwrap_or(9092);
    let raw = host.map(str::trim).filter(|h| !h.is_empty()).unwrap_or("127.0.0.1");
    raw.split(',')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(|h| {
            let has_port = h.rsplit_once(':').is_some_and(|(_, p)| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()));
            if has_port {
                h.to_string()
            } else {
                format!("{h}:{port}")
            }
        })
        .collect()
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let servers = bootstrap_servers(s.host.as_deref(), s.port);
    let mut builder = ClientBuilder::new(servers).client_id("db-free").backoff_config(BackoffConfig {
        init_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(2),
        base: 2.0,
        deadline: Some(Duration::from_secs(10)),
    });
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    if let Some(user) = user {
        builder = builder.sasl_config(SaslConfig::Plain(Credentials::new(user.to_string(), conn.secret.clone().unwrap_or_default())));
    }
    if let Some(tls) = crate::integrations::cassandra::tls_config(s.ssl_mode)? {
        builder = builder.tls_config(tls);
    }
    let client = builder.build().await.map_err(map_error)?;
    let topic_filter = s.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    Ok(Arc::new(KafkaIntegration { client, engine: s.engine, topic_filter, read_only: s.read_only }))
}

// ---------------------------------------------------------------------------
// Records → rows
// ---------------------------------------------------------------------------

fn fixed_columns() -> Vec<ColumnInfo> {
    let col = |i: usize, data_type: &str, pk: bool| ColumnInfo {
        name: COLUMN_NAMES[i].to_string(),
        data_type: data_type.to_string(),
        nullable: !pk,
        primary_key: pk,
        ordinal: u32::try_from(i + 1).unwrap_or(u32::MAX),
    };
    vec![col(0, "int", true), col(1, "bigint", true), col(2, "datetime", false), col(3, "text", false), col(4, "text", false), col(5, "json", false)]
}

fn metas() -> Vec<ColumnMeta> {
    fixed_columns().into_iter().map(|c| ColumnMeta { name: c.name, type_name: c.data_type }).collect()
}

// WHAT:  Bytes → text (JSON promoted to a tree), base64 when not UTF-8.
fn bytes_to_value(bytes: Option<&[u8]>) -> Value {
    let Some(bytes) = bytes else {
        return Value::Null;
    };
    match std::str::from_utf8(bytes) {
        Ok(text) => {
            let trimmed = text.trim_start();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
                    if json.is_object() || json.is_array() {
                        return Value::Json(json);
                    }
                }
            }
            Value::Text(text.to_string())
        }
        Err(_) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

fn headers_to_value(headers: &BTreeMap<String, Vec<u8>>) -> Value {
    if headers.is_empty() {
        return Value::Null;
    }
    let map: serde_json::Map<String, serde_json::Value> = headers
        .iter()
        .map(|(k, v)| {
            let val = match std::str::from_utf8(v) {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(v)),
            };
            (k.clone(), val)
        })
        .collect();
    Value::Json(serde_json::Value::Object(map))
}

fn record_row(partition: i32, item: &RecordAndOffset) -> Vec<Value> {
    let r = &item.record;
    vec![
        Value::Int(i64::from(partition)),
        Value::Int(item.offset),
        Value::DateTime(r.timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        bytes_to_value(r.key.as_deref()),
        bytes_to_value(r.value.as_deref()),
        headers_to_value(&r.headers),
    ]
}

// ---------------------------------------------------------------------------
// Console commands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Start {
    Earliest,
    Latest,
    At(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Topics,
    Consume { topic: String, partition: Option<i32>, start: Start, limit: u64 },
    Produce { topic: String, partition: Option<i32>, key: Option<String>, value: String, headers: BTreeMap<String, String> },
}

fn json_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// WHAT:  JSON body or shorthand → Command. `max_rows` caps every consume.
pub fn parse_command(text: &str, max_rows: usize) -> AppResult<Command> {
    let text = text.trim();
    let cap = u64::try_from(max_rows).unwrap_or(u64::MAX).min(MAX_RECORDS);
    if text.starts_with('{') {
        let json: serde_json::Value =
            serde_json::from_str(text).map_err(|e| AppError::invalid_input(format!("Command is not valid JSON: {e}")))?;
        let obj = json.as_object().ok_or_else(|| AppError::invalid_input("Command must be a JSON object."))?;
        if let Some(produce) = obj.get("produce") {
            let p = produce.as_object().ok_or_else(|| AppError::invalid_input("\"produce\" must be an object."))?;
            let topic = p.get("topic").map(json_string).filter(|t| !t.is_empty()).ok_or_else(|| AppError::invalid_input("\"produce.topic\" is required."))?;
            let value = p.get("value").map(json_string).ok_or_else(|| AppError::invalid_input("\"produce.value\" is required."))?;
            let key = p.get("key").filter(|k| !k.is_null()).map(json_string);
            let partition = p.get("partition").and_then(serde_json::Value::as_i64).and_then(|v| i32::try_from(v).ok());
            let headers = p
                .get("headers")
                .and_then(serde_json::Value::as_object)
                .map(|h| h.iter().map(|(k, v)| (k.clone(), json_string(v))).collect())
                .unwrap_or_default();
            return Ok(Command::Produce { topic, partition, key, value, headers });
        }
        if obj.get("topics").is_some() && obj.get("topic").is_none() {
            return Ok(Command::Topics);
        }
        let topic = obj.get("topic").map(json_string).filter(|t| !t.is_empty()).ok_or_else(|| AppError::invalid_input("\"topic\" is required."))?;
        let partition = obj.get("partition").and_then(serde_json::Value::as_i64).and_then(|v| i32::try_from(v).ok());
        let start = match obj.get("offset") {
            None => Start::Latest,
            Some(serde_json::Value::Number(n)) => Start::At(n.as_i64().unwrap_or(0)),
            Some(serde_json::Value::String(s)) if s.eq_ignore_ascii_case("earliest") => Start::Earliest,
            Some(serde_json::Value::String(s)) if s.eq_ignore_ascii_case("latest") => Start::Latest,
            Some(serde_json::Value::String(s)) => Start::At(s.parse().map_err(|_| AppError::invalid_input("\"offset\" must be a number, \"earliest\" or \"latest\"."))?),
            Some(_) => return Err(AppError::invalid_input("\"offset\" must be a number, \"earliest\" or \"latest\".")),
        };
        let limit = obj.get("limit").and_then(serde_json::Value::as_u64).unwrap_or(cap.min(100)).clamp(1, cap);
        return Ok(Command::Consume { topic, partition, start, limit });
    }
    let mut words = text.split_whitespace();
    let verb = words.next().unwrap_or_default().to_ascii_lowercase();
    match verb.as_str() {
        "topics" => Ok(Command::Topics),
        "consume" => {
            let topic = words.next().ok_or_else(|| AppError::invalid_input("Usage: CONSUME <topic> [n]"))?.to_string();
            let limit = words.next().map(|n| n.parse::<u64>()).transpose().map_err(|_| AppError::invalid_input("Usage: CONSUME <topic> [n]"))?;
            Ok(Command::Consume { topic, partition: None, start: Start::Latest, limit: limit.unwrap_or(cap.min(100)).clamp(1, cap) })
        }
        _ => Err(AppError::invalid_input(
            "Enter `TOPICS`, `CONSUME <topic> [n]`, a JSON body {\"topic\": \"…\", \"offset\": \"earliest\", \"limit\": 50}, or {\"produce\": {\"topic\": \"…\", \"value\": \"…\"}}.",
        )),
    }
}

// WHAT:  Where to start reading so that at most `limit` records back from the
//        high watermark are returned (never below the low watermark).
fn tail_start(low: i64, high: i64, limit: u64, offset: u64) -> Option<(i64, u64)> {
    if high <= low {
        return None;
    }
    let available = (high - low) as u64;
    let skip = offset.min(available);
    let end = high - skip as i64;
    let take = limit.min(available - skip);
    if take == 0 {
        return None;
    }
    Some((end - take as i64, take))
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl KafkaIntegration {
    async fn topics(&self) -> AppResult<Vec<rskafka::topic::Topic>> {
        let mut topics = self.client.list_topics().await.map_err(map_error)?;
        if let Some(filter) = &self.topic_filter {
            topics.retain(|t| t.name.contains(filter.as_str()));
        }
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    async fn partitions_of(&self, topic: &str) -> AppResult<Vec<i32>> {
        let topics = self.client.list_topics().await.map_err(map_error)?;
        let found = topics.into_iter().find(|t| t.name == topic).ok_or_else(|| AppError::not_found(format!("Topic \"{topic}\" not found.")))?;
        Ok(found.partitions.into_iter().collect())
    }

    async fn partition(&self, topic: &str, partition: i32) -> AppResult<PartitionClient> {
        self.client.partition_client(topic, partition, UnknownTopicHandling::Error).await.map_err(map_error)
    }

    async fn watermarks(&self, pc: &PartitionClient) -> AppResult<(i64, i64)> {
        let low = pc.get_offset(OffsetAt::Earliest).await.map_err(map_error)?;
        let high = pc.get_offset(OffsetAt::Latest).await.map_err(map_error)?;
        Ok((low, high))
    }

    async fn topic_count(&self, topic: &str) -> AppResult<i64> {
        let mut total = 0i64;
        for p in self.partitions_of(topic).await? {
            let pc = self.partition(topic, p).await?;
            let (low, high) = self.watermarks(&pc).await?;
            total += (high - low).max(0);
        }
        Ok(total)
    }

    // WHAT:  Reads [start, start+take) from one partition, bounded by bytes and wait.
    async fn read_range(&self, pc: &PartitionClient, start: i64, take: u64, high: i64) -> AppResult<Vec<RecordAndOffset>> {
        let mut out = Vec::new();
        let mut cursor = start;
        while (out.len() as u64) < take && cursor < high {
            let (records, _) = pc.fetch_records(cursor, 1..FETCH_BYTES, FETCH_WAIT_MS).await.map_err(map_error)?;
            if records.is_empty() {
                break;
            }
            for r in records {
                cursor = r.offset + 1;
                if (out.len() as u64) < take {
                    out.push(r);
                }
            }
        }
        Ok(out)
    }

    // WHAT:  Latest `limit` records (after skipping `offset` from the tail) per partition.
    async fn tail(&self, topic: &str, partitions: &[i32], limit: u64, offset: u64) -> AppResult<Vec<Vec<Value>>> {
        let mut rows = Vec::new();
        for &p in partitions {
            let pc = self.partition(topic, p).await?;
            let (low, high) = self.watermarks(&pc).await?;
            if let Some((start, take)) = tail_start(low, high, limit, offset) {
                for r in self.read_range(&pc, start, take, high).await? {
                    rows.push(record_row(p, &r));
                }
            }
        }
        Ok(rows)
    }

    async fn consume(&self, topic: &str, partition: Option<i32>, start: &Start, limit: u64) -> AppResult<ResultSet> {
        let partitions = match partition {
            Some(p) => vec![p],
            None => self.partitions_of(topic).await?,
        };
        let mut rows = Vec::new();
        for &p in &partitions {
            let pc = self.partition(topic, p).await?;
            let (low, high) = self.watermarks(&pc).await?;
            let (from, take) = match start {
                Start::Earliest => (low, limit.min((high - low).max(0) as u64)),
                Start::Latest => match tail_start(low, high, limit, 0) {
                    Some(v) => v,
                    None => continue,
                },
                Start::At(o) => {
                    let from = (*o).clamp(low, high);
                    (from, limit.min((high - from).max(0) as u64))
                }
            };
            if take == 0 {
                continue;
            }
            for r in self.read_range(&pc, from, take, high).await? {
                rows.push(record_row(p, &r));
            }
        }
        let truncated = rows.len() as u64 > limit;
        rows.truncate(limit as usize);
        Ok(ResultSet { columns: metas(), rows, truncated })
    }

    async fn produce(&self, topic: &str, partition: Option<i32>, key: Option<String>, value: String, headers: BTreeMap<String, String>) -> AppResult<StatementResult> {
        if self.read_only {
            return Err(AppError::invalid_input("This connection is read-only; producing is blocked."));
        }
        let partition = match partition {
            Some(p) => p,
            None => self.partitions_of(topic).await?.into_iter().next().unwrap_or(0),
        };
        let pc = self.partition(topic, partition).await?;
        let record = Record {
            key: key.map(String::into_bytes),
            value: Some(value.into_bytes()),
            headers: headers.into_iter().map(|(k, v)| (k, v.into_bytes())).collect(),
            timestamp: chrono::Utc::now(),
        };
        pc.produce(vec![record], Compression::default()).await.map_err(map_error)?;
        Ok(StatementResult::Affected { rows_affected: 1 })
    }

    async fn topics_result(&self) -> AppResult<ResultSet> {
        let topics = self.topics().await?;
        let mut rows = Vec::with_capacity(topics.len());
        for t in topics {
            let count = self.topic_count(&t.name).await.ok();
            rows.push(vec![
                Value::Text(t.name.clone()),
                Value::Int(t.partitions.len() as i64),
                count.map(Value::Int).unwrap_or(Value::Null),
            ]);
        }
        let columns = [("topic", "text"), ("partitions", "int"), ("records", "bigint")]
            .iter()
            .map(|(n, t)| ColumnMeta { name: (*n).to_string(), type_name: (*t).to_string() })
            .collect();
        Ok(ResultSet { columns, rows, truncated: false })
    }
}

#[async_trait]
impl Integration for KafkaIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: false,
            namespaces: false,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        }
    }

    async fn ping(&self) -> AppResult<()> {
        self.client.list_topics().await.map_err(map_error)?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        // The Kafka wire protocol exposes API versions, not a product version.
        Ok(None)
    }

    fn current_database(&self) -> Option<String> {
        self.topic_filter.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(self.topic_filter.iter().cloned().collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let topics = self.topics().await?;
        let mut tables = Vec::with_capacity(topics.len());
        for t in topics {
            let row_estimate = self.topic_count(&t.name).await.ok();
            tables.push(TableInfo { schema: Some(TOPICS_SCHEMA.to_string()), name: t.name, kind: TableKind::Table, row_estimate });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: TOPICS_SCHEMA.to_string(), tables }] })
    }

    async fn columns(&self, _table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(fixed_columns())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.topic_count(&table.name).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        if filters.is_empty() {
            return self.topic_count(&table.name).await;
        }
        let partitions = self.partitions_of(&table.name).await?;
        let rows = self.tail(&table.name, &partitions, MAX_RECORDS, 0).await?;
        let names: Vec<String> = COLUMN_NAMES.iter().map(|n| (*n).to_string()).collect();
        Ok(local::apply_filters(&names, rows, filters).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let partitions = self.partitions_of(&table.name).await?;
        let names: Vec<String> = COLUMN_NAMES.iter().map(|n| (*n).to_string()).collect();
        let needs_local = !query.filters.is_empty() || query.sort.iter().any(|s| s.column != "offset" || !s.desc);
        let wanted = query.offset.saturating_add(u64::from(query.limit)).min(MAX_RECORDS);
        let (rows, local_query) = if needs_local {
            // Filters / custom sort need the bounded tail; page through it locally.
            (self.tail(&table.name, &partitions, MAX_RECORDS, 0).await?, query.clone())
        } else {
            // Default view: newest first. Read the last offset+limit per partition and slice.
            let rows = self.tail(&table.name, &partitions, wanted, 0).await?;
            (rows, PageQuery { sort: query.sort.clone(), filters: vec![], offset: query.offset, limit: query.limit })
        };
        let mut local_query = local_query;
        if local_query.sort.is_empty() {
            local_query.sort = vec![crate::model::SortRule { column: "offset".into(), desc: true }];
        }
        let rows = local::page(&names, rows, &local_query);
        Ok(ResultSet { columns: metas(), rows, truncated: false })
    }

    async fn execute(&self, script: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let statements: Vec<&str> = split_statements(script);
        if statements.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        let mut out = Vec::with_capacity(statements.len());
        for statement in statements {
            let result = match parse_command(statement, max_rows)? {
                Command::Topics => StatementResult::Rows { result: self.topics_result().await? },
                Command::Consume { topic, partition, start, limit } => {
                    StatementResult::Rows { result: self.consume(&topic, partition, &start, limit).await? }
                }
                Command::Produce { topic, partition, key, value, headers } => self.produce(&topic, partition, key, value, headers).await?,
            };
            out.push(result);
        }
        Ok(out)
    }

    async fn close(&self) {}
}

// WHAT:  JSON bodies are one statement each (balanced braces); shorthands are one per line.
fn split_statements(script: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = script.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let rest = &script[i..];
        let trimmed = rest.trim_start();
        let skipped = rest.len() - trimmed.len();
        if trimmed.is_empty() {
            break;
        }
        let start = i + skipped;
        if trimmed.starts_with('{') {
            let mut depth = 0i32;
            let mut in_str = false;
            let mut esc = false;
            let mut end = start;
            for (j, c) in script[start..].char_indices() {
                end = start + j + c.len_utf8();
                if in_str {
                    if esc {
                        esc = false;
                    } else if c == '\\' {
                        esc = true;
                    } else if c == '"' {
                        in_str = false;
                    }
                    continue;
                }
                match c {
                    '"' => in_str = true,
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
            }
            out.push(script[start..end].trim());
            i = end;
        } else {
            let line_end = trimmed.find('\n').map(|n| start + n).unwrap_or(script.len());
            let line = script[start..line_end].trim();
            if !line.is_empty() && !line.starts_with('#') && !line.starts_with("//") {
                out.push(line);
            }
            i = line_end + 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, FilterOp, SortRule, SslMode};

    #[test]
    fn bootstrap_list_gets_ports() {
        assert_eq!(bootstrap_servers(Some("a:1, b ,c:3"), Some(9092)), vec!["a:1", "b:9092", "c:3"]);
        assert_eq!(bootstrap_servers(None, None), vec!["127.0.0.1:9092"]);
    }

    #[test]
    fn commands_parse() {
        assert_eq!(parse_command("TOPICS", 10).unwrap_or(Command::Topics), Command::Topics);
        assert_eq!(
            parse_command("consume orders 5", 100).ok(),
            Some(Command::Consume { topic: "orders".into(), partition: None, start: Start::Latest, limit: 5 })
        );
        assert_eq!(
            parse_command(r#"{"topic": "t", "partition": 2, "offset": "earliest", "limit": 5000}"#, 100).ok(),
            Some(Command::Consume { topic: "t".into(), partition: Some(2), start: Start::Earliest, limit: 100 })
        );
        assert_eq!(
            parse_command(r#"{"topic": "t", "offset": 42}"#, 10).ok(),
            Some(Command::Consume { topic: "t".into(), partition: None, start: Start::At(42), limit: 10 })
        );
        let produce = parse_command(r#"{"produce": {"topic": "t", "key": 1, "value": {"a": 1}, "headers": {"h": "v"}}}"#, 10).ok();
        assert_eq!(
            produce,
            Some(Command::Produce {
                topic: "t".into(),
                partition: None,
                key: Some("1".into()),
                value: "{\"a\":1}".into(),
                headers: BTreeMap::from([("h".to_string(), "v".to_string())]),
            })
        );
        assert!(matches!(parse_command("SELECT 1", 10), Err(AppError::InvalidInput { .. })));
        assert!(matches!(parse_command(r#"{"offset": 1}"#, 10), Err(AppError::InvalidInput { .. })));
        assert!(matches!(parse_command(r#"{"topic": "t", "offset": true}"#, 10), Err(AppError::InvalidInput { .. })));
    }

    #[test]
    fn statements_split_json_and_lines() {
        let parts = split_statements("TOPICS\n{\"topic\": \"a\",\n \"limit\": 1}\n# c\nconsume b 2\n{\"produce\": {\"topic\": \"x\", \"value\": \"}\"}}");
        assert_eq!(parts, vec!["TOPICS", "{\"topic\": \"a\",\n \"limit\": 1}", "consume b 2", "{\"produce\": {\"topic\": \"x\", \"value\": \"}\"}}"]);
    }

    #[test]
    fn tail_window_stays_inside_watermarks() {
        assert_eq!(tail_start(0, 10, 3, 0), Some((7, 3)));
        assert_eq!(tail_start(5, 10, 100, 0), Some((5, 5)));
        assert_eq!(tail_start(0, 10, 3, 2), Some((5, 3)));
        assert_eq!(tail_start(0, 10, 3, 9), Some((0, 1)));
        assert_eq!(tail_start(0, 10, 3, 10), None);
        assert_eq!(tail_start(3, 3, 3, 0), None);
    }

    #[test]
    fn records_decode() {
        let mut headers = BTreeMap::new();
        headers.insert("h".to_string(), b"v".to_vec());
        let item = RecordAndOffset {
            record: Record {
                key: Some(b"k".to_vec()),
                value: Some(br#"{"a": 1}"#.to_vec()),
                headers,
                timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap_or_default(),
            },
            offset: 9,
        };
        let row = record_row(1, &item);
        assert_eq!(row[0], Value::Int(1));
        assert_eq!(row[1], Value::Int(9));
        assert_eq!(row[2], Value::DateTime("1970-01-01T00:00:00.000Z".into()));
        assert_eq!(row[3], Value::Text("k".into()));
        assert_eq!(row[4], Value::Json(serde_json::json!({"a": 1})));
        assert_eq!(row[5], Value::Json(serde_json::json!({"h": "v"})));
        assert_eq!(bytes_to_value(Some(&[0xff, 0xfe])), Value::Bytes("//4=".into()));
        assert_eq!(bytes_to_value(None), Value::Null);
        assert_eq!(fixed_columns().iter().filter(|c| c.primary_key).count(), 2);
    }

    fn resolved(engine: Engine, host: String) -> ResolvedConnection {
        let input = ConnectionInput {
            name: "live".into(),
            engine,
            environment: Environment::Local,
            read_only: false,
            host: Some(host),
            port: std::env::var("DBFREE_TEST_KAFKA_PORT").ok().and_then(|p| p.parse().ok()),
            database: None,
            username: std::env::var("DBFREE_TEST_KAFKA_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Disable,
        };
        ResolvedConnection { summary: ConnectionSummary::draft(&input, false), secret: std::env::var("DBFREE_TEST_KAFKA_PASSWORD").ok() }
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_KAFKA_HOST is set
    //        (e.g. `docker run --rm -p 9092:9092 redpandadata/redpanda redpanda start --mode dev-container`).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(host) = std::env::var("DBFREE_TEST_KAFKA_HOST") else {
            return;
        };
        let kafka = connect(&resolved(Engine::Kafka, host)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        kafka.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        let topic = "dbfree_test";
        let controller = std::env::var("DBFREE_TEST_KAFKA_CREATE_TOPIC").is_ok();
        if controller {
            // Optional: create the topic when auto-creation is off.
            if let Ok(cc) = kafka_client_for_test(&resolved(Engine::Kafka, std::env::var("DBFREE_TEST_KAFKA_HOST").unwrap_or_default())).await {
                if let Ok(ctrl) = cc.controller_client() {
                    let _ = ctrl.create_topic(topic, 1, 1, 5_000).await;
                }
            }
        }
        for i in 0..3 {
            let cmd = format!("{{\"produce\": {{\"topic\": \"{topic}\", \"key\": \"k{i}\", \"value\": {{\"n\": {i}}}, \"headers\": {{\"h\": \"{i}\"}}}}}}");
            kafka.execute(&cmd, 10).await.unwrap_or_else(|e| panic!("produce: {e}"));
        }
        let catalog = kafka.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let table = catalog.schemas[0].tables.iter().find(|t| t.name == topic).unwrap_or_else(|| panic!("topic missing: {catalog:?}"));
        assert!(table.row_estimate.unwrap_or_default() >= 3);
        let table = TableRef { schema: Some("topics".into()), name: topic.into() };
        let page = kafka
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 2 })
            .await
            .unwrap_or_else(|e| panic!("fetch_page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert!(matches!(page.rows[0][4], Value::Json(_)));
        let filtered = kafka
            .fetch_page(
                &table,
                &PageQuery {
                    sort: vec![SortRule { column: "offset".into(), desc: false }],
                    filters: vec![FilterRule { column: "key".into(), op: FilterOp::Eq, value: "k1".into() }],
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap_or_else(|e| panic!("fetch_page filtered: {e}"));
        assert!(!filtered.rows.is_empty());
        assert_eq!(filtered.rows[0][3], Value::Text("k1".into()));
        let out = kafka.execute(&format!("CONSUME {topic} 2"), 10).await.unwrap_or_else(|e| panic!("consume: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 2),
            other => panic!("expected rows, got {other:?}"),
        }
        let topics = kafka.execute("TOPICS", 10).await.unwrap_or_else(|e| panic!("topics: {e}"));
        assert!(matches!(&topics[0], StatementResult::Rows { result } if result.rows.iter().any(|r| r[0] == Value::Text(topic.into()))));
    }

    async fn kafka_client_for_test(conn: &ResolvedConnection) -> AppResult<Client> {
        let servers = bootstrap_servers(conn.summary.host.as_deref(), conn.summary.port);
        ClientBuilder::new(servers).build().await.map_err(map_error)
    }
}
