// SOT: cassandra-integration, scylla-adapter, cql, cql-value-decoding, cassandra-paging, system-schema-catalog, cql-ddl-reconstruction, cassandra-object-explorer, cassandra-server-stats

use crate::error::{AppError, AppResult};
use crate::integrations::http::local;
use crate::integrations::sql::{order_clause, quote_literal};
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats,
    SortRule, SslMode, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use scylla::client::session::{Session, TlsContext};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::frame::response::result::{CollectionType, ColumnType, NativeType};
use scylla::response::query_result::QueryResult;
use scylla::statement::Statement;
use scylla::value::{CqlValue, Row};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// CASSANDRA / SCYLLADB ADAPTER
//
// WHAT:  Maps a CQL wide-column store onto the engine-neutral `Integration`.
// WHY:   CQL looks like SQL but is not: no OFFSET, no arbitrary WHERE without
//        ALLOW FILTERING, ORDER BY only along the clustering key. The grid
//        still needs sort + filter + paging, so the adapter pushes down what
//        CQL accepts and finishes the rest client-side.
// HOW:   catalog     = system_schema.tables + system_schema.views per keyspace
//        columns     = system_schema.columns (partition/clustering → primary key)
//        fetch_page  = SELECT … [WHERE …] [ALLOW FILTERING], fetch offset+limit
//                      rows (capped) and slice; sort client-side unless it
//                      follows the clustering order
//        count       = SELECT COUNT(*) … ALLOW FILTERING
//        execute     = statements split on ';', each run unpaged (row cap)
//        `scylla` is the only vendor crate used, and only in this file.
// WHERE: src-tauri/src/integrations/mod.rs (trait), src/lib/engines.ts (UI meta)
// ============================================================================

const MAX_SCAN_ROWS: u64 = 5_000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
// WHAT:  Per-request ceiling, above the driver's 30 s default.
// WHY:   Schema changes (CREATE/DROP KEYSPACE) wait for agreement across the
//        cluster and routinely pass 30 s on a busy or single-node instance,
//        which surfaced as a spurious "client timeout" on a statement that in
//        fact succeeded. The guard still applies its own timeout on top.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const SYSTEM_KEYSPACES: &[&str] = &[
    "system",
    "system_auth",
    "system_distributed",
    "system_schema",
    "system_traces",
    "system_views",
    "system_virtual_schema",
    "system_replicated_keys",
];

pub struct CassandraIntegration {
    session: Session,
    engine: Engine,
    keyspace: Option<String>,
}

fn driver_error(err: impl std::fmt::Display) -> AppError {
    let text = err.to_string();
    let lower = text.to_ascii_lowercase();
    if lower.contains("authentication") || lower.contains("unauthorized") || lower.contains("credentials") {
        AppError::not_connected(text)
    } else {
        AppError::driver(text)
    }
}

fn is_system_keyspace(name: &str) -> bool {
    SYSTEM_KEYSPACES.contains(&name)
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

// WHAT:  Host field may hold `h1,h2,h3`; each without a port gets the port field.
fn known_nodes(host: Option<&str>, port: Option<u16>) -> Vec<String> {
    let port = port.unwrap_or(9042);
    let raw = host.map(str::trim).filter(|h| !h.is_empty()).unwrap_or("127.0.0.1");
    raw.split(',')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(|h| {
            let has_port = h.rsplit_once(':').is_some_and(|(_, p)| p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty());
            if has_port {
                h.to_string()
            } else {
                format!("{h}:{port}")
            }
        })
        .collect()
}

// WHAT:  Accept-everything verifier for `SslMode::Require` (encrypted, unverified).
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    rustls::crypto::CryptoProvider::get_default().cloned().unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()))
}

// WHAT:  rustls config for the requested SSL mode. `Require` skips verification,
//        `VerifyCa`/`VerifyFull` trust the OS certificate store. Shared with
//        kafka.rs (rskafka takes the same `Arc<rustls::ClientConfig>`).
pub(crate) fn tls_config(mode: SslMode) -> AppResult<Option<Arc<rustls::ClientConfig>>> {
    let provider = crypto_provider();
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| AppError::driver(format!("TLS setup failed: {e}")))?;
    let config = match mode {
        SslMode::Disable | SslMode::Prefer => return Ok(None),
        SslMode::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
            .with_no_client_auth(),
        SslMode::VerifyCa | SslMode::VerifyFull => {
            let mut roots = rustls::RootCertStore::empty();
            let native = rustls_native_certs::load_native_certs();
            roots.add_parsable_certificates(native.certs);
            if roots.is_empty() {
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            builder.with_root_certificates(roots).with_no_client_auth()
        }
    };
    Ok(Some(Arc::new(config)))
}

// WHAT:  Sends every peer address the cluster broadcasts back to the contact
//        point the user actually configured.
// WHY:   A node in Docker (or behind NAT / a bastion / port-forward) broadcasts
//        its own private `broadcast_rpc_address`, e.g. 192.168.215.11:9042. The
//        driver connects to the contact point, learns that address from
//        system.local, and then cannot reach it — "connection refused" on a
//        server that plainly works. Pinning the translation to the address the
//        user gave keeps single-node and forwarded clusters usable; a real
//        multi-node cluster reached over routable addresses is unaffected
//        because it broadcasts addresses that already resolve.
#[derive(Debug)]
struct ContactPointTranslator {
    contact: SocketAddr,
}

#[async_trait]
impl AddressTranslator for ContactPointTranslator {
    async fn translate_address(&self, _peer: &UntranslatedPeer) -> Result<SocketAddr, TranslationError> {
        Ok(self.contact)
    }
}

// WHAT:  Resolves the first contact point to a socket address for the translator.
async fn first_contact_addr(nodes: &[String]) -> Option<SocketAddr> {
    let first = nodes.first()?;
    if let Ok(addr) = first.parse::<SocketAddr>() {
        return Some(addr);
    }
    tokio::net::lookup_host(first).await.ok()?.next()
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let nodes = known_nodes(s.host.as_deref(), s.port);
    let profile = ExecutionProfile::builder().request_timeout(Some(REQUEST_TIMEOUT)).build();
    let mut builder = SessionBuilder::new()
        .known_nodes(nodes.clone())
        .connection_timeout(CONNECT_TIMEOUT)
        .default_execution_profile_handle(profile.into_handle());
    // Single contact point = a local / port-forwarded node whose broadcast
    // address is very likely unreachable; route peers back through it.
    if nodes.len() == 1 {
        if let Some(contact) = first_contact_addr(&nodes).await {
            builder = builder.address_translator(Arc::new(ContactPointTranslator { contact }));
        }
    }
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    if let Some(user) = user {
        builder = builder.user(user, conn.secret.clone().unwrap_or_default());
    }
    if let Some(tls) = tls_config(s.ssl_mode)? {
        builder = builder.tls_context(Some(TlsContext::from(tls)));
    }
    let session = builder.build().await.map_err(driver_error)?;
    let requested = s.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let keyspace = match requested {
        Some(ks) => Some(ks),
        None => list_keyspaces(&session).await.unwrap_or_default().into_iter().find(|k| !is_system_keyspace(k)),
    };
    Ok(Arc::new(CassandraIntegration { session, engine: s.engine, keyspace }))
}

async fn list_keyspaces(session: &Session) -> AppResult<Vec<String>> {
    let result = run(session, "SELECT keyspace_name FROM system_schema.keyspaces", usize::MAX).await?;
    let mut names: Vec<String> = result
        .rows
        .into_iter()
        .filter_map(|row| match row.into_iter().next() {
            Some(Value::Text(name)) => Some(name),
            _ => None,
        })
        .collect();
    names.sort();
    Ok(names)
}

// ---------------------------------------------------------------------------
// CqlValue → model::Value
// ---------------------------------------------------------------------------

fn native_type_name(t: &NativeType) -> &'static str {
    match t {
        NativeType::Ascii => "ascii",
        NativeType::Boolean => "boolean",
        NativeType::Blob => "blob",
        NativeType::Counter => "counter",
        NativeType::Date => "date",
        NativeType::Decimal => "decimal",
        NativeType::Double => "double",
        NativeType::Duration => "duration",
        NativeType::Float => "float",
        NativeType::Int => "int",
        NativeType::BigInt => "bigint",
        NativeType::Text => "text",
        NativeType::Timestamp => "timestamp",
        NativeType::Inet => "inet",
        NativeType::SmallInt => "smallint",
        NativeType::TinyInt => "tinyint",
        NativeType::Time => "time",
        NativeType::Timeuuid => "timeuuid",
        NativeType::Uuid => "uuid",
        NativeType::Varint => "varint",
        _ => "unknown",
    }
}

fn column_type_name(t: &ColumnType<'_>) -> String {
    match t {
        ColumnType::Native(n) => native_type_name(n).to_string(),
        ColumnType::Collection { typ, .. } => match typ {
            CollectionType::List(inner) => format!("list<{}>", column_type_name(inner)),
            CollectionType::Set(inner) => format!("set<{}>", column_type_name(inner)),
            CollectionType::Map(k, v) => format!("map<{}, {}>", column_type_name(k), column_type_name(v)),
            _ => "collection".to_string(),
        },
        ColumnType::Vector { typ, dimensions } => format!("vector<{}, {dimensions}>", column_type_name(typ)),
        ColumnType::UserDefinedType { definition, .. } => definition.name.to_string(),
        ColumnType::Tuple(items) => format!("tuple<{}>", items.iter().map(column_type_name).collect::<Vec<_>>().join(", ")),
        _ => "unknown".to_string(),
    }
}

fn timestamp_text(millis: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| millis.to_string())
}

fn date_text(days: u32) -> String {
    // CqlDate counts days from 2^31 days before the unix epoch.
    let offset = i64::from(days) - (1i64 << 31);
    chrono::DateTime::<chrono::Utc>::from_timestamp(offset * 86_400, 0)
        .map(|dt| dt.date_naive().to_string())
        .unwrap_or_else(|| days.to_string())
}

fn time_text(nanos: i64) -> String {
    let secs = nanos.div_euclid(1_000_000_000);
    let frac = nanos.rem_euclid(1_000_000_000);
    format!("{:02}:{:02}:{:02}.{:09}", secs / 3600, (secs / 60) % 60, secs % 60, frac)
}

// WHAT:  Two's-complement big-endian digits + scale → plain decimal text.
fn decimal_text(digits: &[u8], scale: i32) -> String {
    let negative = digits.first().is_some_and(|b| b & 0x80 != 0);
    let mut magnitude: Vec<u8> = digits.to_vec();
    if negative {
        // Two's complement negate: invert and add one.
        for b in magnitude.iter_mut() {
            *b = !*b;
        }
        let mut carry = 1u16;
        for b in magnitude.iter_mut().rev() {
            let sum = u16::from(*b) + carry;
            *b = (sum & 0xff) as u8;
            carry = sum >> 8;
        }
    }
    // Base-256 → base-10 by repeated division.
    let mut num = magnitude;
    let mut out_digits: Vec<u8> = Vec::new();
    while num.iter().any(|b| *b != 0) {
        let mut rem: u32 = 0;
        for b in num.iter_mut() {
            let cur = (rem << 8) | u32::from(*b);
            *b = (cur / 10) as u8;
            rem = cur % 10;
        }
        out_digits.push(rem as u8);
    }
    if out_digits.is_empty() {
        out_digits.push(0);
    }
    out_digits.reverse();
    let mut text: String = out_digits.iter().map(|d| char::from(b'0' + d)).collect();
    if scale > 0 {
        let scale = scale as usize;
        while text.len() <= scale {
            text.insert(0, '0');
        }
        text.insert(text.len() - scale, '.');
    } else if scale < 0 {
        text.extend(std::iter::repeat_n('0', scale.unsigned_abs() as usize));
    }
    if negative {
        text.insert(0, '-');
    }
    text
}

fn varint_text(digits: &[u8]) -> String {
    decimal_text(digits, 0)
}

fn cql_to_json(value: &CqlValue) -> serde_json::Value {
    use serde_json::Value as J;
    match value {
        CqlValue::Ascii(s) | CqlValue::Text(s) => J::String(s.clone()),
        CqlValue::Boolean(b) => J::Bool(*b),
        CqlValue::Blob(b) => J::String(base64::engine::general_purpose::STANDARD.encode(b)),
        CqlValue::Counter(c) => J::from(c.0),
        CqlValue::Decimal(d) => {
            let (digits, scale) = d.as_signed_be_bytes_slice_and_exponent();
            J::String(decimal_text(digits, scale))
        }
        CqlValue::Date(d) => J::String(date_text(d.0)),
        CqlValue::Double(f) => serde_json::Number::from_f64(*f).map(J::Number).unwrap_or(J::Null),
        CqlValue::Float(f) => serde_json::Number::from_f64(f64::from(*f)).map(J::Number).unwrap_or(J::Null),
        CqlValue::Duration(d) => J::String(format!("{}mo{}d{}ns", d.months, d.days, d.nanoseconds)),
        CqlValue::Empty => J::Null,
        CqlValue::Int(i) => J::from(*i),
        CqlValue::BigInt(i) => J::from(*i),
        CqlValue::SmallInt(i) => J::from(*i),
        CqlValue::TinyInt(i) => J::from(*i),
        CqlValue::Timestamp(ts) => J::String(timestamp_text(ts.0)),
        CqlValue::Time(t) => J::String(time_text(t.0)),
        CqlValue::Inet(ip) => J::String(ip.to_string()),
        CqlValue::List(items) | CqlValue::Set(items) | CqlValue::Vector(items) => J::Array(items.iter().map(cql_to_json).collect()),
        CqlValue::Map(pairs) => J::Object(pairs.iter().map(|(k, v)| (json_key(k), cql_to_json(v))).collect()),
        CqlValue::UserDefinedType { fields, .. } => J::Object(
            fields.iter().map(|(name, v)| (name.clone(), v.as_ref().map(cql_to_json).unwrap_or(J::Null))).collect(),
        ),
        CqlValue::Tuple(items) => J::Array(items.iter().map(|v| v.as_ref().map(cql_to_json).unwrap_or(J::Null)).collect()),
        CqlValue::Timeuuid(u) => J::String(u.to_string()),
        CqlValue::Uuid(u) => J::String(u.to_string()),
        CqlValue::Varint(v) => J::String(varint_text(v.as_signed_bytes_be_slice())),
        other => J::String(other.to_string()),
    }
}

fn json_key(value: &CqlValue) -> String {
    match cql_to_json(value) {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

fn cql_to_value(value: Option<&CqlValue>) -> Value {
    let Some(value) = value else {
        return Value::Null;
    };
    match value {
        CqlValue::Ascii(s) | CqlValue::Text(s) => Value::Text(s.clone()),
        CqlValue::Boolean(b) => Value::Bool(*b),
        CqlValue::Blob(b) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(b)),
        CqlValue::Counter(c) => Value::Int(c.0),
        CqlValue::Decimal(d) => {
            let (digits, scale) = d.as_signed_be_bytes_slice_and_exponent();
            Value::Decimal(decimal_text(digits, scale))
        }
        CqlValue::Varint(v) => Value::Decimal(varint_text(v.as_signed_bytes_be_slice())),
        CqlValue::Date(d) => Value::Text(date_text(d.0)),
        CqlValue::Double(f) => Value::Float(*f),
        CqlValue::Float(f) => Value::Float(f64::from(*f)),
        CqlValue::Duration(d) => Value::Text(format!("{}mo{}d{}ns", d.months, d.days, d.nanoseconds)),
        CqlValue::Empty => Value::Null,
        CqlValue::Int(i) => Value::Int(i64::from(*i)),
        CqlValue::BigInt(i) => Value::Int(*i),
        CqlValue::SmallInt(i) => Value::Int(i64::from(*i)),
        CqlValue::TinyInt(i) => Value::Int(i64::from(*i)),
        CqlValue::Timestamp(ts) => Value::DateTime(timestamp_text(ts.0)),
        CqlValue::Time(t) => Value::Text(time_text(t.0)),
        CqlValue::Inet(ip) => Value::Text(ip.to_string()),
        CqlValue::Timeuuid(u) => Value::Text(u.to_string()),
        CqlValue::Uuid(u) => Value::Text(u.to_string()),
        CqlValue::List(_)
        | CqlValue::Set(_)
        | CqlValue::Vector(_)
        | CqlValue::Map(_)
        | CqlValue::UserDefinedType { .. }
        | CqlValue::Tuple(_) => Value::Json(cql_to_json(value)),
        other => Value::Unsupported(other.to_string()),
    }
}

// WHAT:  Whole query result → grid, capped at `max_rows`. Non-row results
//        (DDL, INSERT, USE) become `Affected { 0 }` since CQL reports no counts.
fn result_to_statement(result: QueryResult, max_rows: usize) -> AppResult<StatementResult> {
    if result.is_rows() {
        let set = result_to_set(result, max_rows)?;
        Ok(StatementResult::Rows { result: set })
    } else {
        Ok(StatementResult::Affected { rows_affected: 0 })
    }
}

fn result_to_set(result: QueryResult, max_rows: usize) -> AppResult<ResultSet> {
    let rows_result = result.into_rows_result().map_err(driver_error)?;
    let columns: Vec<ColumnMeta> = rows_result
        .column_specs()
        .iter()
        .map(|spec| ColumnMeta { name: spec.name().to_string(), type_name: column_type_name(spec.typ()) })
        .collect();
    let mut rows = Vec::new();
    let mut truncated = false;
    let iter = rows_result.rows::<Row>().map_err(driver_error)?;
    for row in iter {
        if rows.len() >= max_rows {
            truncated = true;
            break;
        }
        let row = row.map_err(driver_error)?;
        rows.push(row.columns.iter().map(|c| cql_to_value(c.as_ref())).collect());
    }
    Ok(ResultSet { columns, rows, truncated })
}

async fn run(session: &Session, cql: &str, max_rows: usize) -> AppResult<ResultSet> {
    let result = session.query_unpaged(Statement::new(cql), ()).await.map_err(driver_error)?;
    if !result.is_rows() {
        return Ok(ResultSet { columns: vec![], rows: vec![], truncated: false });
    }
    result_to_set(result, max_rows)
}

// ---------------------------------------------------------------------------
// CQL statement building
// ---------------------------------------------------------------------------

fn qualified(keyspace: Option<&str>, table: &TableRef) -> String {
    match table.schema.as_deref().or(keyspace) {
        Some(ks) => format!("{}.{}", quote_ident(ks), quote_ident(&table.name)),
        None => quote_ident(&table.name),
    }
}

// WHAT:  Parses a filter value the way a person types it so the literal matches
//        the column's CQL type: numbers, booleans and UUIDs stay bare, else quoted.
fn cql_literal(raw: &str, data_type: &str) -> String {
    let trimmed = raw.trim();
    let numeric = matches!(data_type, "int" | "bigint" | "smallint" | "tinyint" | "counter" | "varint" | "float" | "double" | "decimal");
    if numeric && trimmed.parse::<f64>().is_ok() {
        return trimmed.to_string();
    }
    if data_type == "boolean" && (trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false")) {
        return trimmed.to_ascii_lowercase();
    }
    if matches!(data_type, "uuid" | "timeuuid") && uuid::Uuid::parse_str(trimmed).is_ok() {
        return trimmed.to_ascii_lowercase();
    }
    quote_literal(trimmed)
}

fn predicate(rule: &FilterRule, data_type: &str) -> Option<String> {
    let col = quote_ident(&rule.column);
    let lit = |v: &str| cql_literal(v, data_type);
    let text = rule.value.trim();
    Some(match rule.op {
        FilterOp::Eq => format!("{col} = {}", lit(text)),
        FilterOp::Ne => format!("{col} != {}", lit(text)),
        FilterOp::Gt => format!("{col} > {}", lit(text)),
        FilterOp::Gte => format!("{col} >= {}", lit(text)),
        FilterOp::Lt => format!("{col} < {}", lit(text)),
        FilterOp::Lte => format!("{col} <= {}", lit(text)),
        FilterOp::In => {
            let items: Vec<String> = text.split(',').map(str::trim).filter(|v| !v.is_empty()).map(lit).collect();
            if items.is_empty() {
                return None;
            }
            format!("{col} IN ({})", items.join(", "))
        }
        // CQL has no LIKE without a SASI index and no IS NULL; these run client-side.
        FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith | FilterOp::IsNull | FilterOp::IsNotNull => return None,
    })
}

// WHAT:  Splits filters into (server-side WHERE, needs ALLOW FILTERING, client-side leftovers).
struct Plan {
    where_sql: String,
    allow_filtering: bool,
    local_filters: Vec<FilterRule>,
}

fn plan_filters(filters: &[FilterRule], columns: &[ColumnInfo]) -> Plan {
    let mut parts = Vec::new();
    let mut local = Vec::new();
    let mut allow_filtering = false;
    for rule in filters {
        let Some(column) = columns.iter().find(|c| c.name == rule.column) else {
            local.push(rule.clone());
            continue;
        };
        match predicate(rule, &column.data_type) {
            Some(p) => {
                // Equality / IN on a primary-key column is native; anything else
                // (non-key column, range op) needs ALLOW FILTERING.
                let native = column.primary_key && matches!(rule.op, FilterOp::Eq | FilterOp::In);
                if !native {
                    allow_filtering = true;
                }
                parts.push(p);
            }
            None => local.push(rule.clone()),
        }
    }
    let where_sql = if parts.is_empty() { String::new() } else { format!(" WHERE {}", parts.join(" AND ")) };
    Plan { where_sql, allow_filtering, local_filters: local }
}

// WHAT:  ORDER BY is only pushed down when it is exactly a prefix of the
//        clustering columns (same order) and every partition key is pinned by
//        equality; anything else sorts client-side.
fn sort_is_native(sort: &[SortRule], clustering: &[String], partition: &[String], filters: &[FilterRule]) -> bool {
    if sort.is_empty() || clustering.is_empty() {
        return false;
    }
    let pinned = |pk: &String| filters.iter().any(|f| &f.column == pk && matches!(f.op, FilterOp::Eq | FilterOp::In));
    if !partition.iter().all(pinned) {
        return false;
    }
    let first_desc = sort.first().map(|s| s.desc).unwrap_or(false);
    sort.len() <= clustering.len()
        && sort.iter().zip(clustering).all(|(s, c)| &s.column == c && s.desc == first_desc)
}

// WHAT:  Statement splitter: `;` outside quotes, blank statements dropped.
pub fn split_statements(script: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = script.chars().peekable();
    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    if chars.peek() == Some(&q) {
                        if let Some(next) = chars.next() {
                            current.push(next);
                        }
                    } else {
                        quote = None;
                    }
                }
            }
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    current.push(c);
                }
                '-' if chars.peek() == Some(&'-') => {
                    // line comment
                    for cc in chars.by_ref() {
                        if cc == '\n' {
                            break;
                        }
                    }
                }
                '/' if chars.peek() == Some(&'*') => {
                    chars.next();
                    let mut prev = '\0';
                    for cc in chars.by_ref() {
                        if prev == '*' && cc == '/' {
                            break;
                        }
                        prev = cc;
                    }
                }
                ';' => {
                    if !current.trim().is_empty() {
                        out.push(current.trim().to_string());
                    }
                    current.clear();
                }
                other => current.push(other),
            },
        }
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }
    out
}

// ---------------------------------------------------------------------------
// Schema metadata
// ---------------------------------------------------------------------------

struct TableColumns {
    columns: Vec<ColumnInfo>,
    partition: Vec<String>,
    clustering: Vec<String>,
}

fn text_cell(row: &[Value], i: usize) -> String {
    match row.get(i) {
        Some(Value::Text(s)) => s.clone(),
        Some(Value::Int(i)) => i.to_string(),
        Some(other) => local_text(other),
        None => String::new(),
    }
}

fn local_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => s.clone(),
        Value::Json(j) => j.to_string(),
    }
}

fn int_cell(row: &[Value], i: usize) -> i64 {
    match row.get(i) {
        Some(Value::Int(i)) => *i,
        Some(Value::Text(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

// WHAT:  system_schema.columns rows → ColumnInfo, keys first in key order.
fn columns_from_rows(rows: &[Vec<Value>]) -> TableColumns {
    // row: column_name, kind, position, type
    let mut partition: Vec<(i64, String)> = Vec::new();
    let mut clustering: Vec<(i64, String)> = Vec::new();
    let mut regular: Vec<String> = Vec::new();
    let mut types: BTreeMap<String, String> = BTreeMap::new();
    for row in rows {
        let name = text_cell(row, 0);
        let kind = text_cell(row, 1);
        let position = int_cell(row, 2);
        types.insert(name.clone(), text_cell(row, 3));
        match kind.as_str() {
            "partition_key" => partition.push((position, name)),
            "clustering" => clustering.push((position, name)),
            _ => regular.push(name),
        }
    }
    partition.sort();
    clustering.sort();
    regular.sort();
    let partition: Vec<String> = partition.into_iter().map(|(_, n)| n).collect();
    let clustering: Vec<String> = clustering.into_iter().map(|(_, n)| n).collect();
    let mut columns = Vec::new();
    let mut ordinal = 0u32;
    let mut push = |name: &String, pk: bool| {
        ordinal += 1;
        columns.push(ColumnInfo {
            name: name.clone(),
            data_type: types.get(name).cloned().unwrap_or_default(),
            nullable: !pk,
            primary_key: pk,
            ordinal,
        });
    };
    for n in &partition {
        push(n, true);
    }
    for n in &clustering {
        push(n, true);
    }
    for n in &regular {
        push(n, false);
    }
    TableColumns { columns, partition, clustering }
}

impl CassandraIntegration {
    fn keyspace_for(&self, table: &TableRef) -> AppResult<String> {
        table
            .schema
            .clone()
            .or_else(|| self.keyspace.clone())
            .ok_or_else(|| AppError::invalid_input("No keyspace selected. Set the keyspace on the connection."))
    }

    async fn table_columns(&self, table: &TableRef) -> AppResult<TableColumns> {
        let ks = self.keyspace_for(table)?;
        let cql = format!(
            "SELECT column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = {} AND table_name = {}",
            quote_literal(&ks),
            quote_literal(&table.name)
        );
        let set = run(&self.session, &cql, usize::MAX).await?;
        let out = columns_from_rows(&set.rows);
        if out.columns.is_empty() {
            return Err(AppError::not_found(format!("Table {}.{} not found.", ks, table.name)));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Lists and describes keyspaces, tables, materialized views, indexes,
//        UDTs, UDFs / UDAs, roles, grants, nodes and settings straight from
//        the system keyspaces, and reconstructs the CQL DDL from them (there
//        is no SHOW CREATE in CQL).
// WHY:   The explorer / admin UI is generic; this is where the family maps
//        system_schema / system_auth / system / system_views onto it.
// HOW:   One SELECT per kind, filtered on the partition key (keyspace_name)
//        when a parent is given, client-side otherwise. Nested lookups
//        (indexes of a table, views of a base table) filter client-side too
//        because those are clustering columns. Every action is plain CQL that
//        runs back through `execute`, so the guard's read-only lock and
//        destructive confirmation apply unchanged.
// ---------------------------------------------------------------------------

const MAX_OBJECTS: usize = 2_000;

fn column_index(set: &ResultSet, name: &str) -> Option<usize> {
    set.columns.iter().position(|c| c.name == name)
}

fn named_value<'a>(set: &ResultSet, row: &'a [Value], name: &str) -> Option<&'a Value> {
    column_index(set, name).and_then(|i| row.get(i)).filter(|v| !matches!(v, Value::Null))
}

fn named_text(set: &ResultSet, row: &[Value], name: &str) -> String {
    named_value(set, row, name).map(local_text).unwrap_or_default()
}

fn named_i64(set: &ResultSet, row: &[Value], name: &str) -> Option<i64> {
    match named_value(set, row, name) {
        Some(Value::Int(i)) => Some(*i),
        Some(Value::Float(f)) => Some(*f as i64),
        Some(Value::Text(s)) | Some(Value::Decimal(s)) => s.parse().ok(),
        _ => None,
    }
}

fn named_bool(set: &ResultSet, row: &[Value], name: &str) -> Option<bool> {
    match named_value(set, row, name) {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::Text(s)) => Some(s.eq_ignore_ascii_case("true")),
        _ => None,
    }
}

fn json_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A `list<text>` / `set<text>` / `frozen<list<text>>` cell as strings.
fn named_list(set: &ResultSet, row: &[Value], name: &str) -> Vec<String> {
    match named_value(set, row, name) {
        Some(Value::Json(serde_json::Value::Array(items))) => items.iter().map(json_text).collect(),
        Some(Value::Text(s)) if !s.is_empty() => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// A `map<text, text>` cell as sorted pairs.
fn named_map(set: &ResultSet, row: &[Value], name: &str) -> BTreeMap<String, String> {
    match named_value(set, row, name) {
        Some(Value::Json(serde_json::Value::Object(obj))) => obj.iter().map(|(k, v)| (k.clone(), json_text(v))).collect(),
        _ => BTreeMap::new(),
    }
}

fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", value as u64, UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

// ---- CQL DDL reconstruction (pure, unit-tested) ----------------------------

/// `{'class': 'SimpleStrategy', 'replication_factor': '1'}`
fn cql_map_literal(map: &BTreeMap<String, String>) -> String {
    let pairs: Vec<String> = map.iter().map(|(k, v)| format!("{}: {}", quote_literal(k), quote_literal(v))).collect();
    format!("{{{}}}", pairs.join(", "))
}

/// `NetworkTopologyStrategy dc1=3, dc2=2` / `SimpleStrategy rf=1`.
fn replication_summary(map: &BTreeMap<String, String>) -> String {
    let class = map
        .get("class")
        .map(|c| c.rsplit('.').next().unwrap_or(c).to_string())
        .unwrap_or_else(|| "?".to_string());
    let factors: Vec<String> = map
        .iter()
        .filter(|(k, _)| k.as_str() != "class")
        .map(|(k, v)| if k == "replication_factor" { format!("rf={v}") } else { format!("{k}={v}") })
        .collect();
    if factors.is_empty() {
        class
    } else {
        format!("{class} {}", factors.join(", "))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CqlColumn {
    name: String,
    kind: String,
    position: i64,
    data_type: String,
    clustering_order: String,
}

fn kind_rank(kind: &str) -> u8 {
    match kind {
        "partition_key" => 0,
        "clustering" => 1,
        "static" => 2,
        _ => 3,
    }
}

/// system_schema.columns rows → key columns first, in key order, then static, then regular by name.
fn cql_columns(set: &ResultSet) -> Vec<CqlColumn> {
    let mut cols: Vec<CqlColumn> = set
        .rows
        .iter()
        .map(|row| CqlColumn {
            name: named_text(set, row, "column_name"),
            kind: named_text(set, row, "kind"),
            position: named_i64(set, row, "position").unwrap_or(-1),
            data_type: named_text(set, row, "type"),
            clustering_order: named_text(set, row, "clustering_order"),
        })
        .collect();
    cols.sort_by(|a, b| kind_rank(&a.kind).cmp(&kind_rank(&b.kind)).then(a.position.cmp(&b.position)).then(a.name.cmp(&b.name)));
    cols
}

fn column_infos(columns: &[CqlColumn]) -> Vec<ColumnInfo> {
    columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let pk = matches!(c.kind.as_str(), "partition_key" | "clustering");
            ColumnInfo { name: c.name.clone(), data_type: c.data_type.clone(), nullable: !pk, primary_key: pk, ordinal: i as u32 + 1 }
        })
        .collect()
}

fn primary_key_clause(columns: &[CqlColumn]) -> String {
    let names = |kind: &str| columns.iter().filter(|c| c.kind == kind).map(|c| quote_ident(&c.name)).collect::<Vec<_>>().join(", ");
    let partition = names("partition_key");
    let clustering = names("clustering");
    if clustering.is_empty() {
        format!("({partition})")
    } else {
        format!("(({partition}), {clustering})")
    }
}

/// `WITH CLUSTERING ORDER BY (…) AND option = literal …` or empty.
fn with_clause(columns: &[CqlColumn], options: &[(String, String)]) -> String {
    let mut with: Vec<String> = Vec::new();
    let order: Vec<String> = columns
        .iter()
        .filter(|c| c.kind == "clustering")
        .map(|c| format!("{} {}", quote_ident(&c.name), if c.clustering_order.eq_ignore_ascii_case("desc") { "DESC" } else { "ASC" }))
        .collect();
    if !order.is_empty() {
        with.push(format!("CLUSTERING ORDER BY ({})", order.join(", ")));
    }
    with.extend(options.iter().map(|(k, v)| format!("{k} = {v}")));
    if with.is_empty() {
        String::new()
    } else {
        format!("\nWITH {}", with.join("\n  AND "))
    }
}

// WHAT:  Table / view options worth echoing in DDL, in the order cqlsh prints them.
const TABLE_OPTIONS: &[&str] = &[
    "comment",
    "bloom_filter_fp_chance",
    "caching",
    "compaction",
    "compression",
    "crc_check_chance",
    "default_time_to_live",
    "gc_grace_seconds",
    "max_index_interval",
    "memtable_flush_period_in_ms",
    "min_index_interval",
    "speculative_retry",
    "read_repair",
    "cdc",
];

fn option_literal(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Int(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Decimal(s) => Some(s.clone()),
        Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => Some(quote_literal(s)),
        Value::Json(serde_json::Value::Object(obj)) => {
            Some(cql_map_literal(&obj.iter().map(|(k, v)| (k.clone(), json_text(v))).collect()))
        }
        Value::Json(other) => Some(quote_literal(&other.to_string())),
    }
}

fn table_options(set: &ResultSet, row: &[Value]) -> Vec<(String, String)> {
    TABLE_OPTIONS
        .iter()
        .filter_map(|name| {
            let value = named_value(set, row, name)?;
            if *name == "comment" && matches!(value, Value::Text(s) if s.is_empty()) {
                return None;
            }
            option_literal(value).map(|lit| ((*name).to_string(), lit))
        })
        .collect()
}

fn create_table_cql(keyspace: &str, table: &str, columns: &[CqlColumn], options: &[(String, String)]) -> String {
    let mut lines: Vec<String> = columns
        .iter()
        .map(|c| {
            let suffix = if c.kind == "static" { " STATIC" } else { "" };
            format!("  {} {}{suffix}", quote_ident(&c.name), c.data_type)
        })
        .collect();
    lines.push(format!("  PRIMARY KEY {}", primary_key_clause(columns)));
    format!(
        "CREATE TABLE {}.{} (\n{}\n){};",
        quote_ident(keyspace),
        quote_ident(table),
        lines.join(",\n"),
        with_clause(columns, options)
    )
}

fn create_view_cql(
    keyspace: &str,
    view: &str,
    base_table: &str,
    where_clause: &str,
    include_all_columns: bool,
    columns: &[CqlColumn],
    options: &[(String, String)],
) -> String {
    let select = if include_all_columns || columns.is_empty() {
        "*".to_string()
    } else {
        columns.iter().map(|c| quote_ident(&c.name)).collect::<Vec<_>>().join(", ")
    };
    let filter = if where_clause.trim().is_empty() { String::new() } else { format!("\n  WHERE {}", where_clause.trim()) };
    format!(
        "CREATE MATERIALIZED VIEW {}.{} AS\n  SELECT {select}\n  FROM {}.{}{filter}\n  PRIMARY KEY {}{};",
        quote_ident(keyspace),
        quote_ident(view),
        quote_ident(keyspace),
        quote_ident(base_table),
        primary_key_clause(columns),
        with_clause(columns, options)
    )
}

fn create_index_cql(keyspace: &str, table: &str, index: &str, kind: &str, options: &BTreeMap<String, String>) -> String {
    let target = options.get("target").cloned().unwrap_or_default();
    let on = format!("{}.{}", quote_ident(keyspace), quote_ident(table));
    match (kind.eq_ignore_ascii_case("CUSTOM"), options.get("class_name")) {
        (true, Some(class)) => {
            let extra: BTreeMap<String, String> =
                options.iter().filter(|(k, _)| k.as_str() != "target" && k.as_str() != "class_name").map(|(k, v)| (k.clone(), v.clone())).collect();
            let with = if extra.is_empty() { String::new() } else { format!(" WITH OPTIONS = {}", cql_map_literal(&extra)) };
            format!("CREATE CUSTOM INDEX {} ON {on} ({target}) USING {}{with};", quote_ident(index), quote_literal(class))
        }
        _ => format!("CREATE INDEX {} ON {on} ({target});", quote_ident(index)),
    }
}

fn create_type_cql(keyspace: &str, name: &str, fields: &[(String, String)]) -> String {
    let lines: Vec<String> = fields.iter().map(|(f, t)| format!("  {} {t}", quote_ident(f))).collect();
    format!("CREATE TYPE {}.{} (\n{}\n);", quote_ident(keyspace), quote_ident(name), lines.join(",\n"))
}

/// `"a" int, "b" text` — names are optional in system_schema (older servers).
fn function_args(arg_names: &[String], arg_types: &[String]) -> Vec<String> {
    arg_types
        .iter()
        .enumerate()
        .map(|(i, t)| match arg_names.get(i) {
            Some(n) => format!("{} {t}", quote_ident(n)),
            None => t.clone(),
        })
        .collect()
}

fn create_function_cql(keyspace: &str, name: &str, args: &[String], called_on_null: bool, return_type: &str, language: &str, body: &str) -> String {
    let null_clause = if called_on_null { "CALLED ON NULL INPUT" } else { "RETURNS NULL ON NULL INPUT" };
    format!(
        "CREATE FUNCTION {}.{}({})\n  {null_clause}\n  RETURNS {return_type}\n  LANGUAGE {language}\n  AS $${body}$$;",
        quote_ident(keyspace),
        quote_ident(name),
        args.join(", ")
    )
}

fn create_aggregate_cql(
    keyspace: &str,
    name: &str,
    arg_types: &[String],
    state_func: &str,
    state_type: &str,
    final_func: Option<&str>,
    initcond: Option<&str>,
) -> String {
    let mut text = format!(
        "CREATE AGGREGATE {}.{}({})\n  SFUNC {}\n  STYPE {state_type}",
        quote_ident(keyspace),
        quote_ident(name),
        arg_types.join(", "),
        quote_ident(state_func)
    );
    if let Some(f) = final_func.filter(|f| !f.is_empty()) {
        text.push_str(&format!("\n  FINALFUNC {}", quote_ident(f)));
    }
    if let Some(init) = initcond.filter(|i| !i.is_empty()) {
        text.push_str(&format!("\n  INITCOND {init}"));
    }
    text.push(';');
    text
}

/// `name(int, text)` ↔ (name, [int, text]); overloads share a name, so the
/// explorer reference carries the argument types.
fn signature(name: &str, arg_types: &[String]) -> String {
    format!("{name}({})", arg_types.join(", "))
}

fn parse_signature(text: &str) -> (String, Vec<String>) {
    match text.split_once('(') {
        Some((name, rest)) => {
            let inner = rest.trim_end().trim_end_matches(')');
            let mut args = Vec::new();
            let mut depth = 0i32;
            let mut current = String::new();
            for c in inner.chars() {
                match c {
                    '<' => {
                        depth += 1;
                        current.push(c);
                    }
                    '>' => {
                        depth -= 1;
                        current.push(c);
                    }
                    ',' if depth == 0 => {
                        args.push(current.trim().to_string());
                        current.clear();
                    }
                    _ => current.push(c),
                }
            }
            if !current.trim().is_empty() {
                args.push(current.trim().to_string());
            }
            (name.trim().to_string(), args)
        }
        None => (text.trim().to_string(), Vec::new()),
    }
}

/// system_auth resource (`data/ks/t`, `roles/x`, `functions/ks`) → CQL resource for GRANT / REVOKE.
fn cql_resource(resource: &str) -> Option<String> {
    let parts: Vec<&str> = resource.split('/').collect();
    match parts.as_slice() {
        ["data"] => Some("ALL KEYSPACES".to_string()),
        ["data", ks] => Some(format!("KEYSPACE {}", quote_ident(ks))),
        ["data", ks, table] => Some(format!("TABLE {}.{}", quote_ident(ks), quote_ident(table))),
        ["roles"] => Some("ALL ROLES".to_string()),
        ["roles", role] => Some(format!("ROLE {}", quote_ident(role))),
        ["functions"] => Some("ALL FUNCTIONS".to_string()),
        ["functions", ks] => Some(format!("ALL FUNCTIONS IN KEYSPACE {}", quote_ident(ks))),
        _ => None,
    }
}

fn dotted(keyspace: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(keyspace), quote_ident(name))
}

fn parent_keyspace(reference: &ObjectRef, fallback: Option<&str>) -> AppResult<String> {
    reference
        .parent
        .as_deref()
        .map(|p| p.split_once('.').map(|(ks, _)| ks).unwrap_or(p))
        .or(fallback)
        .map(str::to_string)
        .ok_or_else(|| AppError::invalid_input("No keyspace selected. Set the keyspace on the connection."))
}

fn short_class(class: &str) -> String {
    class.rsplit('.').next().unwrap_or(class).to_string()
}

impl CassandraIntegration {
    // WHAT:  `SELECT * FROM system_schema.<table>` for one keyspace or every user keyspace.
    async fn schema_rows(&self, table: &str, keyspace: Option<&str>) -> AppResult<ResultSet> {
        let cql = match keyspace {
            Some(ks) => format!("SELECT * FROM system_schema.{table} WHERE keyspace_name = {}", quote_literal(ks)),
            None => format!("SELECT * FROM system_schema.{table}"),
        };
        let mut set = run(&self.session, &cql, usize::MAX).await?;
        if keyspace.is_none() {
            let idx = column_index(&set, "keyspace_name");
            set.rows.retain(|row| idx.and_then(|i| row.get(i)).map(local_text).is_some_and(|ks| !is_system_keyspace(&ks)));
        }
        Ok(set)
    }

    async fn schema_row(&self, table: &str, keyspace: &str, name_column: &str, name: &str) -> AppResult<Option<(ResultSet, Vec<Value>)>> {
        let set = self.schema_rows(table, Some(keyspace)).await?;
        let row = set.rows.iter().find(|row| named_text(&set, row, name_column) == name).cloned();
        Ok(row.map(|row| (set, row)))
    }

    async fn columns_of(&self, keyspace: &str, table: &str) -> AppResult<Vec<CqlColumn>> {
        let cql = format!(
            "SELECT column_name, kind, position, type, clustering_order FROM system_schema.columns WHERE keyspace_name = {} AND table_name = {}",
            quote_literal(keyspace),
            quote_literal(table)
        );
        let set = run(&self.session, &cql, usize::MAX).await?;
        Ok(cql_columns(&set))
    }

    async fn list_keyspaces_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self.schema_rows("keyspaces", None).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let name = named_text(&set, row, "keyspace_name");
                let replication = named_map(&set, row, "replication");
                let mut summary = ObjectSummary::new(ObjectKind::Keyspace, name, None).with_detail(replication_summary(&replication));
                if named_bool(&set, row, "durable_writes") == Some(false) {
                    summary = summary.with_badge("no durable writes");
                }
                summary
            })
            .collect())
    }

    async fn list_tables_objects(&self, keyspace: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let set = self.schema_rows("tables", keyspace).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let ks = named_text(&set, row, "keyspace_name");
                let name = named_text(&set, row, "table_name");
                let mut summary = ObjectSummary::new(ObjectKind::Table, name, Some(ks));
                let comment = named_text(&set, row, "comment");
                if !comment.is_empty() {
                    summary = summary.with_detail(comment);
                }
                let ttl = named_i64(&set, row, "default_time_to_live").unwrap_or(0);
                if ttl > 0 {
                    summary = summary.with_badge(format!("ttl {ttl}s"));
                }
                summary
            })
            .collect())
    }

    async fn list_views_objects(&self, keyspace: Option<&str>, base_table: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let set = self.schema_rows("views", keyspace).await?;
        Ok(set
            .rows
            .iter()
            .filter(|row| base_table.is_none_or(|t| named_text(&set, row, "base_table_name") == t))
            .map(|row| {
                let ks = named_text(&set, row, "keyspace_name");
                let name = named_text(&set, row, "view_name");
                let base = named_text(&set, row, "base_table_name");
                ObjectSummary::new(ObjectKind::MaterializedView, name, Some(ks)).with_detail(format!("on {base}"))
            })
            .collect())
    }

    async fn list_indexes_objects(&self, keyspace: Option<&str>, table: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let set = self.schema_rows("indexes", keyspace).await?;
        Ok(set
            .rows
            .iter()
            .filter(|row| table.is_none_or(|t| named_text(&set, row, "table_name") == t))
            .map(|row| {
                let ks = named_text(&set, row, "keyspace_name");
                let name = named_text(&set, row, "index_name");
                let table = named_text(&set, row, "table_name");
                let options = named_map(&set, row, "options");
                let target = options.get("target").cloned().unwrap_or_default();
                ObjectSummary::new(ObjectKind::Index, name, Some(ks))
                    .with_detail(format!("{table} ({target})"))
                    .with_badge(named_text(&set, row, "kind").to_ascii_lowercase())
            })
            .collect())
    }

    async fn list_types_objects(&self, keyspace: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let set = self.schema_rows("types", keyspace).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let ks = named_text(&set, row, "keyspace_name");
                let name = named_text(&set, row, "type_name");
                let fields = named_list(&set, row, "field_names");
                ObjectSummary::new(ObjectKind::Type, name, Some(ks)).with_detail(format!("{} fields", fields.len())).with_badge("udt")
            })
            .collect())
    }

    async fn list_functions_objects(&self, keyspace: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let set = self.schema_rows("functions", keyspace).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let ks = named_text(&set, row, "keyspace_name");
                let name = named_text(&set, row, "function_name");
                let arg_types = named_list(&set, row, "argument_types");
                let returns = named_text(&set, row, "return_type");
                ObjectSummary::new(ObjectKind::Function, signature(&name, &arg_types), Some(ks))
                    .with_detail(format!("→ {returns}"))
                    .with_badge(named_text(&set, row, "language"))
            })
            .collect())
    }

    async fn list_aggregates_objects(&self, keyspace: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let set = self.schema_rows("aggregates", keyspace).await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let ks = named_text(&set, row, "keyspace_name");
                let name = named_text(&set, row, "aggregate_name");
                let arg_types = named_list(&set, row, "argument_types");
                let returns = named_text(&set, row, "return_type");
                ObjectSummary::new(ObjectKind::Aggregate, signature(&name, &arg_types), Some(ks)).with_detail(format!("→ {returns}"))
            })
            .collect())
    }

    async fn auth_rows(&self, cql: &str) -> AppResult<ResultSet> {
        run(&self.session, cql, MAX_OBJECTS).await.map_err(|e| {
            AppError::driver(format!("{e} — reading system_auth needs a superuser or SELECT permission on system_auth (roles created with authentication enabled)."))
        })
    }

    async fn list_roles_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self.auth_rows("SELECT role, can_login, is_superuser, member_of FROM system_auth.roles").await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let name = named_text(&set, row, "role");
                let members = named_list(&set, row, "member_of");
                let mut summary = ObjectSummary::new(ObjectKind::Role, name, None);
                if !members.is_empty() {
                    summary = summary.with_detail(format!("member of {}", members.join(", ")));
                }
                if named_bool(&set, row, "is_superuser") == Some(true) {
                    summary = summary.with_badge("superuser");
                } else if named_bool(&set, row, "can_login") == Some(true) {
                    summary = summary.with_badge("login");
                }
                summary
            })
            .collect())
    }

    async fn list_grants_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let set = self.auth_rows("SELECT role, resource, permissions FROM system_auth.role_permissions").await?;
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let role = named_text(&set, row, "role");
                let resource = named_text(&set, row, "resource");
                let permissions = named_list(&set, row, "permissions");
                ObjectSummary::new(ObjectKind::Grant, resource, Some(role)).with_detail(permissions.join(", "))
            })
            .collect())
    }

    async fn list_nodes_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let local = run(&self.session, "SELECT * FROM system.local", 1).await?;
        let mut out = Vec::new();
        for row in &local.rows {
            out.push(
                ObjectSummary::new(ObjectKind::Node, node_address(&local, row), None).with_detail(node_caption(&local, row)).with_badge("local"),
            );
        }
        let peers = run(&self.session, "SELECT * FROM system.peers", MAX_OBJECTS).await?;
        for row in &peers.rows {
            out.push(ObjectSummary::new(ObjectKind::Node, node_address(&peers, row), None).with_detail(node_caption(&peers, row)));
        }
        Ok(out)
    }

    // WHAT:  system_views.settings exists from Cassandra 4.0; older servers and
    //        ScyllaDB simply have no settings to list.
    async fn list_settings_objects(&self) -> AppResult<Vec<ObjectSummary>> {
        let Ok(set) = run(&self.session, "SELECT name, value FROM system_views.settings", MAX_OBJECTS).await else {
            return Ok(Vec::new());
        };
        Ok(set
            .rows
            .iter()
            .map(|row| {
                let name = named_text(&set, row, "name");
                let value = named_text(&set, row, "value");
                ObjectSummary::new(ObjectKind::Setting, name, None).with_detail(value)
            })
            .collect())
    }

    async fn keyspace_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ks = &reference.name;
        let Some((set, row)) = self.schema_row("keyspaces", ks, "keyspace_name", ks).await? else {
            return Err(AppError::not_found(format!("Keyspace {ks} not found.")));
        };
        let replication = named_map(&set, &row, "replication");
        let durable = named_bool(&set, &row, "durable_writes").unwrap_or(true);
        let ddl = format!("CREATE KEYSPACE {} WITH replication = {} AND durable_writes = {durable};", quote_ident(ks), cql_map_literal(&replication));
        let mut children = self.list_tables_objects(Some(ks)).await?;
        children.extend(self.list_views_objects(Some(ks), None).await.unwrap_or_default());
        children.extend(self.list_types_objects(Some(ks)).await.unwrap_or_default());
        let mut detail = ObjectDetail::empty(reference)
            .definition(ddl, CodeLanguage::Sql)
            .property("replication", replication_summary(&replication))
            .property("durable_writes", durable.to_string());
        for (k, v) in &replication {
            detail = detail.property(k, v.clone());
        }
        detail.children = children;
        Ok(detail.action(ObjectAction::destructive("drop", "Drop keyspace", format!("DROP KEYSPACE {}", quote_ident(ks)))))
    }

    async fn table_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ks = parent_keyspace(reference, self.keyspace.as_deref())?;
        let table = &reference.name;
        let Some((set, row)) = self.schema_row("tables", &ks, "table_name", table).await? else {
            return Err(AppError::not_found(format!("Table {ks}.{table} not found.")));
        };
        let columns = self.columns_of(&ks, table).await?;
        let options = table_options(&set, &row);
        let ddl = create_table_cql(&ks, table, &columns, &options);
        let mut detail = ObjectDetail::empty(reference).definition(ddl, CodeLanguage::Sql);
        detail.columns = column_infos(&columns);
        let id = named_text(&set, &row, "id");
        if !id.is_empty() {
            detail = detail.property("id", id);
        }
        let compaction = named_map(&set, &row, "compaction");
        if let Some(class) = compaction.get("class") {
            detail = detail.property("compaction", short_class(class));
        }
        let compression = named_map(&set, &row, "compression");
        if let Some(class) = compression.get("class") {
            detail = detail.property("compression", short_class(class));
        }
        for name in ["comment", "gc_grace_seconds", "default_time_to_live", "bloom_filter_fp_chance", "speculative_retry"] {
            let value = named_text(&set, &row, name);
            if !value.is_empty() {
                detail = detail.property(name, value);
            }
        }
        let flags = named_list(&set, &row, "flags");
        if !flags.is_empty() {
            detail = detail.property("flags", flags.join(", "));
        }
        let mut children = self.list_indexes_objects(Some(&ks), Some(table)).await.unwrap_or_default();
        children.extend(self.list_views_objects(Some(&ks), Some(table)).await.unwrap_or_default());
        detail.children = children;
        let name = dotted(&ks, table);
        Ok(detail
            .action(ObjectAction::destructive("truncate", "Truncate table", format!("TRUNCATE TABLE {name}")))
            .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {name}"))))
    }

    async fn view_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ks = parent_keyspace(reference, self.keyspace.as_deref())?;
        let view = &reference.name;
        let Some((set, row)) = self.schema_row("views", &ks, "view_name", view).await? else {
            return Err(AppError::not_found(format!("Materialized view {ks}.{view} not found.")));
        };
        let columns = self.columns_of(&ks, view).await?;
        let base = named_text(&set, &row, "base_table_name");
        let where_clause = named_text(&set, &row, "where_clause");
        let include_all = named_bool(&set, &row, "include_all_columns").unwrap_or(false);
        let options = table_options(&set, &row);
        let ddl = create_view_cql(&ks, view, &base, &where_clause, include_all, &columns, &options);
        let mut detail = ObjectDetail::empty(reference)
            .definition(ddl, CodeLanguage::Sql)
            .property("base_table", base)
            .property("where", where_clause)
            .property("include_all_columns", include_all.to_string());
        detail.columns = column_infos(&columns);
        Ok(detail.action(ObjectAction::destructive("drop", "Drop materialized view", format!("DROP MATERIALIZED VIEW {}", dotted(&ks, view)))))
    }

    async fn index_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ks = parent_keyspace(reference, self.keyspace.as_deref())?;
        let index = &reference.name;
        let Some((set, row)) = self.schema_row("indexes", &ks, "index_name", index).await? else {
            return Err(AppError::not_found(format!("Index {ks}.{index} not found.")));
        };
        let table = named_text(&set, &row, "table_name");
        let kind = named_text(&set, &row, "kind");
        let options = named_map(&set, &row, "options");
        let ddl = create_index_cql(&ks, &table, index, &kind, &options);
        let mut detail = ObjectDetail::empty(reference).definition(ddl, CodeLanguage::Sql).property("table", table).property("kind", kind);
        for (k, v) in &options {
            detail = detail.property(k, v.clone());
        }
        Ok(detail.action(ObjectAction::destructive("drop", "Drop index", format!("DROP INDEX {}", dotted(&ks, index)))))
    }

    async fn type_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ks = parent_keyspace(reference, self.keyspace.as_deref())?;
        let name = &reference.name;
        let Some((set, row)) = self.schema_row("types", &ks, "type_name", name).await? else {
            return Err(AppError::not_found(format!("Type {ks}.{name} not found.")));
        };
        let names = named_list(&set, &row, "field_names");
        let types = named_list(&set, &row, "field_types");
        let fields: Vec<(String, String)> = names.iter().cloned().zip(types.iter().cloned()).collect();
        let mut detail = ObjectDetail::empty(reference).definition(create_type_cql(&ks, name, &fields), CodeLanguage::Sql);
        detail.columns = fields
            .iter()
            .enumerate()
            .map(|(i, (f, t))| ColumnInfo { name: f.clone(), data_type: t.clone(), nullable: true, primary_key: false, ordinal: i as u32 + 1 })
            .collect();
        Ok(detail.action(ObjectAction::destructive("drop", "Drop type", format!("DROP TYPE {}", dotted(&ks, name)))))
    }

    async fn function_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ks = parent_keyspace(reference, self.keyspace.as_deref())?;
        let (name, args) = parse_signature(&reference.name);
        let set = self.schema_rows("functions", Some(&ks)).await?;
        let row = set
            .rows
            .iter()
            .find(|row| named_text(&set, row, "function_name") == name && (args.is_empty() || named_list(&set, row, "argument_types") == args))
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Function {ks}.{} not found.", reference.name)))?;
        let arg_types = named_list(&set, &row, "argument_types");
        let arg_names = named_list(&set, &row, "argument_names");
        let language = named_text(&set, &row, "language");
        let returns = named_text(&set, &row, "return_type");
        let body = named_text(&set, &row, "body");
        let called_on_null = named_bool(&set, &row, "called_on_null_input").unwrap_or(false);
        let ddl = create_function_cql(&ks, &name, &function_args(&arg_names, &arg_types), called_on_null, &returns, &language, &body);
        let drop = format!("DROP FUNCTION {}({})", dotted(&ks, &name), arg_types.join(", "));
        Ok(ObjectDetail::empty(reference)
            .definition(ddl, CodeLanguage::Sql)
            .property("language", language)
            .property("returns", returns)
            .property("called_on_null_input", called_on_null.to_string())
            .action(ObjectAction::destructive("drop", "Drop function", drop)))
    }

    async fn aggregate_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let ks = parent_keyspace(reference, self.keyspace.as_deref())?;
        let (name, args) = parse_signature(&reference.name);
        let set = self.schema_rows("aggregates", Some(&ks)).await?;
        let row = set
            .rows
            .iter()
            .find(|row| named_text(&set, row, "aggregate_name") == name && (args.is_empty() || named_list(&set, row, "argument_types") == args))
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Aggregate {ks}.{} not found.", reference.name)))?;
        let arg_types = named_list(&set, &row, "argument_types");
        let state_func = named_text(&set, &row, "state_func");
        let state_type = named_text(&set, &row, "state_type");
        let final_func = named_text(&set, &row, "final_func");
        let initcond = named_text(&set, &row, "initcond");
        let returns = named_text(&set, &row, "return_type");
        let ddl = create_aggregate_cql(&ks, &name, &arg_types, &state_func, &state_type, Some(&final_func), Some(&initcond));
        let drop = format!("DROP AGGREGATE {}({})", dotted(&ks, &name), arg_types.join(", "));
        Ok(ObjectDetail::empty(reference)
            .definition(ddl, CodeLanguage::Sql)
            .property("state_func", state_func)
            .property("state_type", state_type)
            .property("final_func", final_func)
            .property("returns", returns)
            .action(ObjectAction::destructive("drop", "Drop aggregate", drop)))
    }

    async fn role_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let role = &reference.name;
        let set = self.auth_rows(&format!("SELECT role, can_login, is_superuser, member_of FROM system_auth.roles WHERE role = {}", quote_literal(role))).await?;
        let row = set.rows.first().cloned().ok_or_else(|| AppError::not_found(format!("Role {role} not found.")))?;
        let login = named_bool(&set, &row, "can_login").unwrap_or(false);
        let superuser = named_bool(&set, &row, "is_superuser").unwrap_or(false);
        let members = named_list(&set, &row, "member_of");
        let ddl = format!("CREATE ROLE {} WITH LOGIN = {login} AND SUPERUSER = {superuser};", quote_ident(role));
        let grants = self
            .auth_rows(&format!("SELECT resource, permissions FROM system_auth.role_permissions WHERE role = {}", quote_literal(role)))
            .await
            .unwrap_or(ResultSet { columns: vec![], rows: vec![], truncated: false });
        let mut detail = ObjectDetail::empty(reference)
            .definition(ddl, CodeLanguage::Sql)
            .property("can_login", login.to_string())
            .property("is_superuser", superuser.to_string())
            .property("member_of", members.join(", "));
        detail.rows = Some(grants);
        Ok(detail.action(ObjectAction::destructive("drop", "Drop role", format!("DROP ROLE {}", quote_ident(role)))))
    }

    async fn grant_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let role = reference.parent.clone().ok_or_else(|| AppError::invalid_input("A grant reference needs its role as parent."))?;
        let resource = &reference.name;
        let set = self
            .auth_rows(&format!("SELECT resource, permissions FROM system_auth.role_permissions WHERE role = {}", quote_literal(&role)))
            .await?;
        let row = set
            .rows
            .iter()
            .find(|row| named_text(&set, row, "resource") == *resource)
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("No grant on {resource} for {role}.")))?;
        let permissions = named_list(&set, &row, "permissions");
        let mut detail = ObjectDetail::empty(reference).property("role", role.clone()).property("resource", resource.clone()).property("permissions", permissions.join(", "));
        if let Some(target) = cql_resource(resource) {
            let grant = format!("GRANT {} ON {target} TO {};", permissions.join(", "), quote_ident(&role));
            detail = detail
                .definition(grant, CodeLanguage::Sql)
                .action(ObjectAction::destructive("revoke", "Revoke all permissions", format!("REVOKE ALL PERMISSIONS ON {target} FROM {}", quote_ident(&role))));
        }
        Ok(detail)
    }

    async fn node_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let local = run(&self.session, "SELECT * FROM system.local", 1).await?;
        let found = local.rows.iter().find(|row| node_address(&local, row) == reference.name).map(|row| (local.clone(), row.clone(), true));
        let found = match found {
            Some(f) => Some(f),
            None => {
                let peers = run(&self.session, "SELECT * FROM system.peers", MAX_OBJECTS).await?;
                peers.rows.iter().find(|row| node_address(&peers, row) == reference.name).map(|row| (peers.clone(), row.clone(), false))
            }
        };
        let Some((set, row, is_local)) = found else {
            return Err(AppError::not_found(format!("Node {} not found in system.local / system.peers.", reference.name)));
        };
        let mut detail = ObjectDetail::empty(reference).property("local", is_local.to_string());
        for (i, column) in set.columns.iter().enumerate() {
            let Some(value) = row.get(i).filter(|v| !matches!(v, Value::Null)) else { continue };
            if column.name == "tokens" {
                if let Value::Json(serde_json::Value::Array(tokens)) = value {
                    detail = detail.property("tokens", format!("{} tokens", tokens.len()));
                }
                continue;
            }
            detail = detail.property(&column.name, local_text(value));
        }
        Ok(detail)
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let set = run(&self.session, &format!("SELECT name, value FROM system_views.settings WHERE name = {}", quote_literal(&reference.name)), 1).await?;
        let row = set.rows.first().cloned().ok_or_else(|| AppError::not_found(format!("Setting {} not found.", reference.name)))?;
        let value = named_text(&set, &row, "value");
        Ok(ObjectDetail::empty(reference).definition(format!("{} = {value}", reference.name), CodeLanguage::Text).property("value", value))
    }
}

fn node_address(set: &ResultSet, row: &[Value]) -> String {
    ["broadcast_address", "rpc_address", "peer", "listen_address"]
        .iter()
        .map(|c| named_text(set, row, c))
        .find(|v| !v.is_empty())
        .unwrap_or_else(|| named_text(set, row, "host_id"))
}

fn node_caption(set: &ResultSet, row: &[Value]) -> String {
    let dc = named_text(set, row, "data_center");
    let rack = named_text(set, row, "rack");
    let version = named_text(set, row, "release_version");
    let mut parts = Vec::new();
    if !dc.is_empty() || !rack.is_empty() {
        parts.push(format!("{dc} / {rack}"));
    }
    if !version.is_empty() {
        parts.push(version);
    }
    parts.join(" · ")
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, transactions: false, ..Capabilities::SQL },
        object_kinds: vec![K::Keyspace, K::Table, K::MaterializedView, K::Index, K::Type, K::Function, K::Aggregate, K::Role, K::Grant, K::Node, K::Setting],
        tools: vec![T::Stats, T::Erd],
    }
}

#[async_trait]
impl Integration for CassandraIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        run(&self.session, "SELECT now() FROM system.local", 1).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let set = run(&self.session, "SELECT release_version FROM system.local", 1).await?;
        let version = set.rows.first().map(|r| text_cell(r, 0)).filter(|v| !v.is_empty());
        let label = match self.engine {
            Engine::Scylladb => "ScyllaDB",
            _ => "Cassandra",
        };
        Ok(version.map(|v| format!("{label} {v}")))
    }

    fn current_database(&self) -> Option<String> {
        self.keyspace.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let all = list_keyspaces(&self.session).await?;
        let mut user: Vec<String> = all.iter().filter(|k| !is_system_keyspace(k)).cloned().collect();
        if let Some(current) = &self.keyspace {
            if !user.contains(current) {
                user.push(current.clone());
            }
        }
        user.sort();
        Ok(user)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut by_keyspace: BTreeMap<String, Vec<TableInfo>> = BTreeMap::new();
        let scope = |ks: &str| match &self.keyspace {
            Some(current) => ks == current,
            None => !is_system_keyspace(ks),
        };
        let tables = run(&self.session, "SELECT keyspace_name, table_name FROM system_schema.tables", usize::MAX).await?;
        for row in &tables.rows {
            let ks = text_cell(row, 0);
            if !scope(&ks) {
                continue;
            }
            let name = text_cell(row, 1);
            by_keyspace.entry(ks.clone()).or_default().push(TableInfo { schema: Some(ks), name, kind: TableKind::Table, row_estimate: None });
        }
        let views = run(&self.session, "SELECT keyspace_name, view_name FROM system_schema.views", usize::MAX).await.unwrap_or(ResultSet {
            columns: vec![],
            rows: vec![],
            truncated: false,
        });
        for row in &views.rows {
            let ks = text_cell(row, 0);
            if !scope(&ks) {
                continue;
            }
            let name = text_cell(row, 1);
            by_keyspace.entry(ks.clone()).or_default().push(TableInfo { schema: Some(ks), name, kind: TableKind::View, row_estimate: None });
        }
        if let Some(current) = &self.keyspace {
            by_keyspace.entry(current.clone()).or_default();
        }
        let schemas = by_keyspace
            .into_iter()
            .map(|(name, mut tables)| {
                tables.sort_by(|a, b| a.name.cmp(&b.name));
                SchemaInfo { name, tables }
            })
            .collect();
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(self.table_columns(table).await?.columns)
    }

    async fn row_estimate(&self, _table: &TableRef) -> AppResult<Option<i64>> {
        // Cassandra has no cheap cardinality statistics; COUNT(*) is a full scan.
        Ok(None)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let meta = self.table_columns(table).await?;
        let plan = plan_filters(filters, &meta.columns);
        if !plan.local_filters.is_empty() {
            // Client-side filters need the rows; count the bounded scan.
            let query = PageQuery { sort: vec![], filters: filters.to_vec(), offset: 0, limit: u32::MAX };
            let page = self.fetch_page(table, &query).await?;
            return Ok(page.rows.len() as i64);
        }
        let cql = format!("SELECT COUNT(*) FROM {}{} ALLOW FILTERING", qualified(self.keyspace.as_deref(), table), plan.where_sql);
        let set = run(&self.session, &cql, 1).await?;
        Ok(set.rows.first().map(|r| int_cell(r, 0)).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let meta = self.table_columns(table).await?;
        let plan = plan_filters(&query.filters, &meta.columns);
        let native_sort = sort_is_native(&query.sort, &meta.clustering, &meta.partition, &query.filters);
        let needs_local = !plan.local_filters.is_empty() || (!query.sort.is_empty() && !native_sort);
        let wanted = query.offset.saturating_add(u64::from(query.limit));
        // Local sort/filter needs the whole (bounded) set; native paging only offset+limit.
        let scan = if needs_local { MAX_SCAN_ROWS } else { wanted.min(MAX_SCAN_ROWS) };
        let order = if native_sort { order_clause(self.engine, &query.sort) } else { String::new() };
        let suffix = if plan.allow_filtering { " ALLOW FILTERING" } else { "" };
        let cql = format!(
            "SELECT * FROM {}{}{} LIMIT {}{}",
            qualified(self.keyspace.as_deref(), table),
            plan.where_sql,
            order,
            scan.max(1),
            suffix
        );
        let set = run(&self.session, &cql, scan as usize).await?;
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery {
            sort: if native_sort { vec![] } else { query.sort.clone() },
            filters: plan.local_filters,
            offset: query.offset,
            limit: query.limit,
        };
        let rows = local::page(&names, set.rows, &local_query);
        Ok(ResultSet { columns: set.columns, rows, truncated: false })
    }

    async fn execute(&self, script: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let statements = split_statements(script);
        if statements.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        let mut out = Vec::with_capacity(statements.len());
        for cql in statements {
            let result = self.session.query_unpaged(Statement::new(cql), ()).await.map_err(driver_error)?;
            out.push(result_to_statement(result, max_rows)?);
        }
        Ok(out)
    }

    async fn close(&self) {}

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let meta = self.table_columns(table).await?;
        let cols: Vec<String> = meta.columns.iter().map(|c| format!("  {} {}", quote_ident(&c.name), c.data_type)).collect();
        let pk = if meta.clustering.is_empty() {
            format!("({})", meta.partition.iter().map(|p| quote_ident(p)).collect::<Vec<_>>().join(", "))
        } else {
            format!(
                "(({}), {})",
                meta.partition.iter().map(|p| quote_ident(p)).collect::<Vec<_>>().join(", "),
                meta.clustering.iter().map(|p| quote_ident(p)).collect::<Vec<_>>().join(", ")
            )
        };
        Ok(Some(format!(
            "CREATE TABLE {} (\n{},\n  PRIMARY KEY {}\n);",
            qualified(self.keyspace.as_deref(), table),
            cols.join(",\n"),
            pk
        )))
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        // `ks.table` parents come from a table's children list; plain parents are keyspaces.
        let (keyspace, table) = match parent.and_then(|p| p.split_once('.')) {
            Some((ks, t)) => (Some(ks), Some(t)),
            None => (parent, None),
        };
        let mut out = match kind {
            ObjectKind::Keyspace => self.list_keyspaces_objects().await?,
            ObjectKind::Table => self.list_tables_objects(keyspace).await?,
            ObjectKind::MaterializedView => self.list_views_objects(keyspace, table).await?,
            ObjectKind::Index => self.list_indexes_objects(keyspace, table).await?,
            ObjectKind::Type => self.list_types_objects(keyspace).await?,
            ObjectKind::Function => self.list_functions_objects(keyspace).await?,
            ObjectKind::Aggregate => self.list_aggregates_objects(keyspace).await?,
            ObjectKind::Role => self.list_roles_objects().await?,
            ObjectKind::Grant => self.list_grants_objects().await?,
            ObjectKind::Node => self.list_nodes_objects().await?,
            ObjectKind::Setting => self.list_settings_objects().await?,
            _ => Vec::new(),
        };
        if kind != ObjectKind::Node {
            out.sort_by(|a, b| a.reference.parent.cmp(&b.reference.parent).then(a.reference.name.cmp(&b.reference.name)));
        }
        out.truncate(MAX_OBJECTS);
        Ok(out)
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Keyspace => self.keyspace_detail(reference).await,
            ObjectKind::Table => self.table_detail(reference).await,
            ObjectKind::MaterializedView => self.view_detail(reference).await,
            ObjectKind::Index => self.index_detail(reference).await,
            ObjectKind::Type => self.type_detail(reference).await,
            ObjectKind::Function => self.function_detail(reference).await,
            ObjectKind::Aggregate => self.aggregate_detail(reference).await,
            ObjectKind::Role => self.role_detail(reference).await,
            ObjectKind::Grant => self.grant_detail(reference).await,
            ObjectKind::Node => self.node_detail(reference).await,
            ObjectKind::Setting => self.setting_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    // WHAT:  Cluster identity from system.local, node count from system.peers,
    //        schema counts from system_schema and a size estimate for the
    //        current keyspace from system.size_estimates (partitions × mean size).
    async fn server_stats(&self) -> AppResult<ServerStats> {
        let local = run(
            &self.session,
            "SELECT cluster_name, release_version, partitioner, data_center, rack, cql_version, native_protocol_version FROM system.local",
            1,
        )
        .await?;
        let row = local.rows.first().cloned().unwrap_or_default();
        let peers = run(&self.session, "SELECT peer FROM system.peers", usize::MAX).await.map(|s| s.rows.len()).unwrap_or(0);
        let label = if matches!(self.engine, Engine::Scylladb) { "ScyllaDB" } else { "Cassandra" };
        let mut server = vec![
            Stat::text("Version", format!("{label} {}", named_text(&local, &row, "release_version"))),
            Stat::text("Cluster", named_text(&local, &row, "cluster_name")),
            Stat::text("Partitioner", short_class(&named_text(&local, &row, "partitioner"))),
        ];
        let cql = named_text(&local, &row, "cql_version");
        if !cql.is_empty() {
            server.push(Stat::text("CQL version", cql));
        }
        let cluster = vec![
            Stat::number("Nodes", (peers + 1) as f64, None).with_hint("system.local + system.peers"),
            Stat::number("Peers", peers as f64, None),
            Stat::text("Local DC / rack", format!("{} / {}", named_text(&local, &row, "data_center"), named_text(&local, &row, "rack"))),
        ];
        let count = |set: AppResult<ResultSet>| -> f64 {
            set.map(|s| {
                let idx = column_index(&s, "keyspace_name");
                s.rows.iter().filter(|r| idx.and_then(|i| r.get(i)).map(local_text).is_some_and(|ks| !is_system_keyspace(&ks))).count() as f64
            })
            .unwrap_or(0.0)
        };
        let keyspaces_all = run(&self.session, "SELECT keyspace_name FROM system_schema.keyspaces", usize::MAX).await;
        let system_keyspaces = keyspaces_all.as_ref().map(|s| s.rows.len()).unwrap_or(0) as f64;
        let user_keyspaces = count(keyspaces_all);
        let schema = vec![
            Stat::number("Keyspaces", user_keyspaces, None).with_hint(format!("{} including system", system_keyspaces)),
            Stat::number("Tables", count(run(&self.session, "SELECT keyspace_name FROM system_schema.tables", usize::MAX).await), None),
            Stat::number("Materialized views", count(run(&self.session, "SELECT keyspace_name FROM system_schema.views", usize::MAX).await), None),
            Stat::number("Indexes", count(run(&self.session, "SELECT keyspace_name FROM system_schema.indexes", usize::MAX).await), None),
            Stat::number("Types", count(run(&self.session, "SELECT keyspace_name FROM system_schema.types", usize::MAX).await), None),
            Stat::number("Functions", count(run(&self.session, "SELECT keyspace_name FROM system_schema.functions", usize::MAX).await), None),
        ];
        let mut groups = vec![
            StatGroup { title: "Server".into(), stats: server },
            StatGroup { title: "Cluster".into(), stats: cluster },
            StatGroup { title: "Schema".into(), stats: schema },
        ];
        if let Some(ks) = &self.keyspace {
            let cql = format!(
                "SELECT table_name, partitions_count, mean_partition_size FROM system.size_estimates WHERE keyspace_name = {}",
                quote_literal(ks)
            );
            if let Ok(set) = run(&self.session, &cql, usize::MAX).await {
                let mut partitions = 0f64;
                let mut bytes = 0f64;
                let mut tables: Vec<String> = Vec::new();
                for r in &set.rows {
                    let count = named_i64(&set, r, "partitions_count").unwrap_or(0) as f64;
                    let mean = named_i64(&set, r, "mean_partition_size").unwrap_or(0) as f64;
                    partitions += count;
                    bytes += count * mean;
                    let table = named_text(&set, r, "table_name");
                    if !tables.contains(&table) {
                        tables.push(table);
                    }
                }
                groups.push(StatGroup {
                    title: format!("Storage · {ks}"),
                    stats: vec![
                        Stat::number("Estimated partitions", partitions, None).with_hint("sum of system.size_estimates.partitions_count"),
                        Stat::number("Estimated size", (bytes / 1_048_576.0 * 10.0).round() / 10.0, Some("MB")).with_hint(format_bytes(bytes)),
                        Stat::number("Tables with estimates", tables.len() as f64, None),
                    ],
                });
            }
        }
        Ok(ServerStats::now(groups))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment};

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    fn col(name: &str, data_type: &str, pk: bool) -> ColumnInfo {
        ColumnInfo { name: name.into(), data_type: data_type.into(), nullable: !pk, primary_key: pk, ordinal: 1 }
    }

    #[test]
    fn nodes_get_ports() {
        assert_eq!(known_nodes(Some("a, b:9999 ,c"), Some(9042)), vec!["a:9042", "b:9999", "c:9042"]);
        assert_eq!(known_nodes(None, None), vec!["127.0.0.1:9042"]);
    }

    #[test]
    fn statements_split_on_semicolons_outside_quotes() {
        let parts = split_statements("SELECT 'a;b' FROM t; -- comment; here\nINSERT INTO t (x) VALUES (\"q;\"); /* c; */ USE ks");
        assert_eq!(parts, vec!["SELECT 'a;b' FROM t", "INSERT INTO t (x) VALUES (\"q;\")", "USE ks"]);
        assert!(split_statements("  ;; ").is_empty());
    }

    #[test]
    fn filters_plan_server_vs_client() {
        let columns = vec![col("id", "uuid", true), col("age", "int", false), col("name", "text", false)];
        let plan = plan_filters(
            &[
                rule("id", FilterOp::Eq, "6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                rule("age", FilterOp::Gte, "30"),
                rule("name", FilterOp::Contains, "an"),
                rule("name", FilterOp::In, "a, b"),
            ],
            &columns,
        );
        assert_eq!(
            plan.where_sql,
            " WHERE \"id\" = 6ba7b810-9dad-11d1-80b4-00c04fd430c8 AND \"age\" >= 30 AND \"name\" IN ('a', 'b')"
        );
        assert!(plan.allow_filtering);
        assert_eq!(plan.local_filters, vec![rule("name", FilterOp::Contains, "an")]);

        let native = plan_filters(&[rule("id", FilterOp::Eq, "x'y")], &columns);
        assert_eq!(native.where_sql, " WHERE \"id\" = 'x''y'");
        assert!(!native.allow_filtering);
        assert_eq!(plan_filters(&[], &columns).where_sql, "");
    }

    #[test]
    fn sort_pushdown_needs_pinned_partition_and_clustering_prefix() {
        let clustering = vec!["ts".to_string(), "seq".to_string()];
        let partition = vec!["id".to_string()];
        let sort = vec![SortRule { column: "ts".into(), desc: true }];
        assert!(!sort_is_native(&sort, &clustering, &partition, &[]));
        assert!(sort_is_native(&sort, &clustering, &partition, &[rule("id", FilterOp::Eq, "1")]));
        let wrong = vec![SortRule { column: "seq".into(), desc: false }];
        assert!(!sort_is_native(&wrong, &clustering, &partition, &[rule("id", FilterOp::Eq, "1")]));
        let mixed = vec![SortRule { column: "ts".into(), desc: true }, SortRule { column: "seq".into(), desc: false }];
        assert!(!sort_is_native(&mixed, &clustering, &partition, &[rule("id", FilterOp::Eq, "1")]));
    }

    #[test]
    fn schema_columns_put_keys_first() {
        let rows = vec![
            vec![Value::Text("body".into()), Value::Text("regular".into()), Value::Int(-1), Value::Text("text".into())],
            vec![Value::Text("ts".into()), Value::Text("clustering".into()), Value::Int(0), Value::Text("timestamp".into())],
            vec![Value::Text("id".into()), Value::Text("partition_key".into()), Value::Int(0), Value::Text("uuid".into())],
        ];
        let meta = columns_from_rows(&rows);
        let names: Vec<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "ts", "body"]);
        assert!(meta.columns[0].primary_key && meta.columns[1].primary_key && !meta.columns[2].primary_key);
        assert_eq!(meta.partition, vec!["id"]);
        assert_eq!(meta.clustering, vec!["ts"]);
    }

    #[test]
    fn values_decode() {
        assert_eq!(cql_to_value(None), Value::Null);
        assert_eq!(cql_to_value(Some(&CqlValue::Int(3))), Value::Int(3));
        assert_eq!(cql_to_value(Some(&CqlValue::Text("x".into()))), Value::Text("x".into()));
        assert_eq!(cql_to_value(Some(&CqlValue::Blob(vec![1, 2, 3]))), Value::Bytes("AQID".into()));
        assert_eq!(cql_to_value(Some(&CqlValue::Timestamp(scylla::value::CqlTimestamp(0)))), Value::DateTime("1970-01-01T00:00:00.000Z".into()));
        let list = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Text("a".into())]);
        assert_eq!(cql_to_value(Some(&list)), Value::Json(serde_json::json!([1, "a"])));
        let map = CqlValue::Map(vec![(CqlValue::Text("k".into()), CqlValue::Boolean(true))]);
        assert_eq!(cql_to_value(Some(&map)), Value::Json(serde_json::json!({"k": true})));
        assert_eq!(decimal_text(&[0x04, 0xD2], 2), "12.34");
        assert_eq!(decimal_text(&[0xFB, 0x2E], 2), "-12.34");
        assert_eq!(decimal_text(&[0x7B], 0), "123");
        assert_eq!(decimal_text(&[0x05], 3), "0.005");
        assert_eq!(decimal_text(&[0x05], -2), "500");
        assert_eq!(date_text(1 << 31), "1970-01-01");
        assert_eq!(time_text(3_723_000_000_004), "01:02:03.000000004");
        assert_eq!(cql_literal("42", "int"), "42");
        assert_eq!(cql_literal("42", "text"), "'42'");
        assert_eq!(cql_literal("TRUE", "boolean"), "true");
    }

    fn set(columns: &[&str], rows: Vec<Vec<Value>>) -> ResultSet {
        ResultSet {
            columns: columns.iter().map(|c| ColumnMeta { name: (*c).to_string(), type_name: "text".into() }).collect(),
            rows,
            truncated: false,
        }
    }

    fn t(s: &str) -> Value {
        Value::Text(s.into())
    }

    #[test]
    fn create_table_ddl_reconstructs_keys_order_and_options() {
        let columns = set(
            &["column_name", "kind", "position", "type", "clustering_order"],
            vec![
                vec![t("body"), t("regular"), Value::Int(-1), t("text"), t("none")],
                vec![t("ts"), t("clustering"), Value::Int(0), t("timestamp"), t("desc")],
                vec![t("id"), t("partition_key"), Value::Int(0), t("uuid"), t("none")],
                vec![t("bucket"), t("partition_key"), Value::Int(1), t("int"), t("none")],
                vec![t("owner"), t("static"), Value::Int(-1), t("text"), t("none")],
            ],
        );
        let cols = cql_columns(&columns);
        assert_eq!(cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id", "bucket", "ts", "owner", "body"]);
        let infos = column_infos(&cols);
        assert!(infos[0].primary_key && infos[2].primary_key && !infos[3].primary_key);
        let table_row = set(
            &["comment", "gc_grace_seconds", "compaction", "default_time_to_live", "bloom_filter_fp_chance"],
            vec![vec![
                t("it's events"),
                Value::Int(864000),
                Value::Json(serde_json::json!({"class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", "max_threshold": "32"})),
                Value::Int(0),
                Value::Float(0.01),
            ]],
        );
        let options = table_options(&table_row, &table_row.rows[0]);
        let ddl = create_table_cql("ks", "events", &cols, &options);
        assert_eq!(
            ddl,
            "CREATE TABLE \"ks\".\"events\" (\n  \"id\" uuid,\n  \"bucket\" int,\n  \"ts\" timestamp,\n  \"owner\" text STATIC,\n  \"body\" text,\n  PRIMARY KEY ((\"id\", \"bucket\"), \"ts\")\n)\nWITH CLUSTERING ORDER BY (\"ts\" DESC)\n  AND comment = 'it''s events'\n  AND bloom_filter_fp_chance = 0.01\n  AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}\n  AND default_time_to_live = 0\n  AND gc_grace_seconds = 864000;"
        );
        // Empty comment is skipped; no clustering column → no WITH clause at all.
        let simple = set(&["column_name", "kind", "position", "type", "clustering_order"], vec![vec![t("k"), t("partition_key"), Value::Int(0), t("int"), t("none")]]);
        let empty_comment = set(&["comment"], vec![vec![t("")]]);
        let ddl = create_table_cql("ks", "kv", &cql_columns(&simple), &table_options(&empty_comment, &empty_comment.rows[0]));
        assert_eq!(ddl, "CREATE TABLE \"ks\".\"kv\" (\n  \"k\" int,\n  PRIMARY KEY (\"k\")\n);");
    }

    #[test]
    fn view_index_type_function_ddl() {
        let columns = set(
            &["column_name", "kind", "position", "type", "clustering_order"],
            vec![
                vec![t("id"), t("clustering"), Value::Int(0), t("uuid"), t("asc")],
                vec![t("email"), t("partition_key"), Value::Int(0), t("text"), t("none")],
            ],
        );
        let cols = cql_columns(&columns);
        let view = create_view_cql("ks", "by_email", "users", "email IS NOT NULL AND id IS NOT NULL", false, &cols, &[]);
        assert_eq!(
            view,
            "CREATE MATERIALIZED VIEW \"ks\".\"by_email\" AS\n  SELECT \"email\", \"id\"\n  FROM \"ks\".\"users\"\n  WHERE email IS NOT NULL AND id IS NOT NULL\n  PRIMARY KEY ((\"email\"), \"id\")\nWITH CLUSTERING ORDER BY (\"id\" ASC);"
        );
        let mut opts = BTreeMap::new();
        opts.insert("target".to_string(), "email".to_string());
        assert_eq!(create_index_cql("ks", "users", "users_email_idx", "COMPOSITES", &opts), "CREATE INDEX \"users_email_idx\" ON \"ks\".\"users\" (email);");
        opts.insert("class_name".to_string(), "org.apache.cassandra.index.sasi.SASIIndex".to_string());
        opts.insert("mode".to_string(), "CONTAINS".to_string());
        assert_eq!(
            create_index_cql("ks", "users", "sasi", "CUSTOM", &opts),
            "CREATE CUSTOM INDEX \"sasi\" ON \"ks\".\"users\" (email) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'};"
        );
        assert_eq!(
            create_type_cql("ks", "address", &[("street".into(), "text".into()), ("zip".into(), "int".into())]),
            "CREATE TYPE \"ks\".\"address\" (\n  \"street\" text,\n  \"zip\" int\n);"
        );
        let f = create_function_cql("ks", "plus", &function_args(&["a".into(), "b".into()], &["int".into(), "int".into()]), true, "int", "java", "return a + b;");
        assert_eq!(f, "CREATE FUNCTION \"ks\".\"plus\"(\"a\" int, \"b\" int)\n  CALLED ON NULL INPUT\n  RETURNS int\n  LANGUAGE java\n  AS $$return a + b;$$;");
        let a = create_aggregate_cql("ks", "total", &["int".into()], "plus", "int", Some("fin"), Some("0"));
        assert_eq!(a, "CREATE AGGREGATE \"ks\".\"total\"(int)\n  SFUNC \"plus\"\n  STYPE int\n  FINALFUNC \"fin\"\n  INITCOND 0;");
        assert!(!create_aggregate_cql("ks", "total", &[], "plus", "int", Some(""), None).contains("FINALFUNC"));
    }

    #[test]
    fn signatures_resources_and_replication() {
        assert_eq!(signature("f", &["int".into(), "map<text, int>".into()]), "f(int, map<text, int>)");
        assert_eq!(parse_signature("f(int, map<text, int>)"), ("f".to_string(), vec!["int".to_string(), "map<text, int>".to_string()]));
        assert_eq!(parse_signature("f()"), ("f".to_string(), vec![]));
        assert_eq!(parse_signature("plain"), ("plain".to_string(), vec![]));
        assert_eq!(cql_resource("data").as_deref(), Some("ALL KEYSPACES"));
        assert_eq!(cql_resource("data/ks").as_deref(), Some("KEYSPACE \"ks\""));
        assert_eq!(cql_resource("data/ks/t").as_deref(), Some("TABLE \"ks\".\"t\""));
        assert_eq!(cql_resource("roles/bob").as_deref(), Some("ROLE \"bob\""));
        assert_eq!(cql_resource("functions/ks").as_deref(), Some("ALL FUNCTIONS IN KEYSPACE \"ks\""));
        assert_eq!(cql_resource("functions/ks/f[int]"), None);
        let mut nts = BTreeMap::new();
        nts.insert("class".to_string(), "org.apache.cassandra.locator.NetworkTopologyStrategy".to_string());
        nts.insert("dc1".to_string(), "3".to_string());
        nts.insert("dc2".to_string(), "2".to_string());
        assert_eq!(replication_summary(&nts), "NetworkTopologyStrategy dc1=3, dc2=2");
        let mut simple = BTreeMap::new();
        simple.insert("class".to_string(), "SimpleStrategy".to_string());
        simple.insert("replication_factor".to_string(), "1".to_string());
        assert_eq!(replication_summary(&simple), "SimpleStrategy rf=1");
        assert_eq!(cql_map_literal(&simple), "{'class': 'SimpleStrategy', 'replication_factor': '1'}");
        let r = ObjectRef { kind: ObjectKind::Index, name: "i".into(), parent: Some("ks.t".into()) };
        assert_eq!(parent_keyspace(&r, None).unwrap(), "ks");
        let bare = ObjectRef { kind: ObjectKind::Table, name: "t".into(), parent: None };
        assert_eq!(parent_keyspace(&bare, Some("cur")).unwrap(), "cur");
        assert!(parent_keyspace(&bare, None).is_err());
        assert_eq!(format_bytes(512.0), "512 B");
        assert_eq!(format_bytes(1_572_864.0), "1.5 MB");
    }

    #[test]
    fn node_rows_name_and_describe() {
        let local = set(&["broadcast_address", "data_center", "rack", "release_version", "tokens"], vec![vec![t("10.0.0.1"), t("dc1"), t("r1"), t("4.1.3"), Value::Json(serde_json::json!(["1", "2"]))]]);
        assert_eq!(node_address(&local, &local.rows[0]), "10.0.0.1");
        assert_eq!(node_caption(&local, &local.rows[0]), "dc1 / r1 · 4.1.3");
        let peer = set(&["peer", "rpc_address", "data_center", "rack"], vec![vec![t("10.0.0.2"), Value::Null, t("dc1"), t("r2")]]);
        assert_eq!(node_address(&peer, &peer.rows[0]), "10.0.0.2");
        assert_eq!(node_caption(&peer, &peer.rows[0]), "dc1 / r2");
    }

    fn resolved(engine: Engine) -> ResolvedConnection {
        let input = ConnectionInput {
            name: "live".into(),
            engine,
            environment: Environment::Local,
            read_only: false,
            host: std::env::var("DBFREE_TEST_CASSANDRA_HOST").ok(),
            port: std::env::var("DBFREE_TEST_CASSANDRA_PORT").ok().and_then(|p| p.parse().ok()),
            database: None,
            username: std::env::var("DBFREE_TEST_CASSANDRA_USER").ok(),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Disable,
        };
        ResolvedConnection { summary: ConnectionSummary::draft(&input, false), secret: std::env::var("DBFREE_TEST_CASSANDRA_PASSWORD").ok() }
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_CASSANDRA_HOST is set
    //        (e.g. `docker run --rm -p 9042:9042 cassandra:5`).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        if std::env::var("DBFREE_TEST_CASSANDRA_HOST").is_err() {
            return;
        }
        let cass = connect(&resolved(Engine::Cassandra)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        cass.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        let version = cass.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("Cassandra") || version.starts_with("ScyllaDB"), "{version}");
        cass.execute(
            "CREATE KEYSPACE IF NOT EXISTS dbfree_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
             CREATE TABLE IF NOT EXISTS dbfree_test.t (id int, ts int, body text, tags set<text>, PRIMARY KEY (id, ts));
             INSERT INTO dbfree_test.t (id, ts, body, tags) VALUES (1, 1, 'one', {'a'});
             INSERT INTO dbfree_test.t (id, ts, body, tags) VALUES (1, 2, 'two', {'b'});
             INSERT INTO dbfree_test.t (id, ts, body) VALUES (2, 1, 'three');",
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("setup: {e}"));
        let table = TableRef { schema: Some("dbfree_test".into()), name: "t".into() };
        let columns = cass.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id", "ts", "body", "tags"]);
        let dbs = cass.databases().await.unwrap_or_default();
        assert!(dbs.iter().any(|d| d == "dbfree_test"), "{dbs:?}");
        let catalog = cass.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(catalog.schemas.iter().any(|s| s.name == "dbfree_test" && s.tables.iter().any(|t| t.name == "t")));
        let page = cass
            .fetch_page(
                &table,
                &PageQuery {
                    sort: vec![SortRule { column: "ts".into(), desc: true }],
                    filters: vec![rule("id", FilterOp::Eq, "1")],
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap_or_else(|e| panic!("fetch_page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.rows[0][1], Value::Int(2));
        let filtered = cass
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![rule("body", FilterOp::Contains, "hre")], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("fetch_page local: {e}"));
        assert_eq!(filtered.rows.len(), 1);
        let count = cass.count(&table, &[rule("body", FilterOp::Eq, "two")]).await.unwrap_or_else(|e| panic!("count: {e}"));
        assert_eq!(count, 1);
        let total = cass.count(&table, &[]).await.unwrap_or_else(|e| panic!("count all: {e}"));
        assert_eq!(total, 3);
        let ddl = cass.ddl(&table).await.unwrap_or_default().unwrap_or_default();
        assert!(ddl.contains("PRIMARY KEY ((\"id\"), \"ts\")"), "{ddl}");
        cass.execute("DROP KEYSPACE dbfree_test", 10).await.unwrap_or_else(|e| panic!("drop: {e}"));
    }
}
