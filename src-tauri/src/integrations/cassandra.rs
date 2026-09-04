// SOT: cassandra-integration, scylla-adapter, cql, cql-value-decoding, cassandra-paging, system-schema-catalog

use crate::error::{AppError, AppResult};
use crate::integrations::http::local;
use crate::integrations::sql::{order_clause, quote_literal};
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SortRule, SslMode, StatementResult, TableInfo, TableKind, TableRef,
    Value,
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

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { transactions: false, ..Capabilities::SQL },
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
