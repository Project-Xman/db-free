// SOT: snowflake-integration, snowflake-sql-rest-api, key-pair-jwt, snowflake-row-decoder, snowflake-pat-auth

use crate::error::{AppError, AppResult};
use crate::integrations::sql::{order_clause, validate_columns, where_clause};
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// WHAT:  Snowflake adapter over the SQL API v2
//        (POST https://{account}.snowflakecomputing.com/api/v2/statements).
// WHY:   No native driver crate; the REST API covers everything the workbench
//        needs (catalog via SHOW / INFORMATION_SCHEMA, paging with standard
//        SQL, partitioned result sets).
// HOW:   `host` = account identifier (`xy12345.us-east-1`, `myorg-myaccount`)
//        or a full URL; `database` = "DB" or "DB.SCHEMA"; `username`; `secret`
//        is one of:
//          - a private-key PEM (`-----BEGIN …PRIVATE KEY-----`) → key-pair JWT
//            (RS256, 1 h, iss = ACCOUNT.USER.SHA256:<pubkey fingerprint>). The
//            fingerprint is derived from the unencrypted PKCS#8 / PKCS#1 key;
//            an explicit `SHA256:…` line after a blank line overrides it.
//            Encrypted keys are not supported (decrypt them first).
//          - a programmatic access token (PAT) or OAuth token → Bearer with
//            `X-Snowflake-Authorization-Token-Type` OAUTH when it looks like a
//            JWT (three dot-separated base64url parts) or is prefixed with
//            `oauth:`, else PROGRAMMATIC_ACCESS_TOKEN.
//        Optional `warehouse=…` / `role=…` suffixes on the database field
//        (`DB.SCHEMA;warehouse=WH;role=R`) are forwarded. 202 responses are
//        polled; extra partitions are fetched up to the row cap. Cells arrive
//        as strings and are decoded by column type (FIXED scale 0 → Int, REAL
//        → Float, BOOLEAN, BINARY hex → Bytes, DATE/TIME/TIMESTAMP_* →
//        RFC 3339, VARIANT/OBJECT/ARRAY → Json).
// WHERE: src-tauri/src/integrations/sql.rs (WHERE/ORDER builders), mod.rs (trait)
// ============================================================================

const STATEMENT_TIMEOUT_SECS: u64 = 60;
const POLL_INTERVAL_MS: u64 = 500;
const MAX_POLLS: usize = 240;
const JWT_LIFETIME_SECS: i64 = 3_600;

pub struct SnowflakeIntegration {
    engine: Engine,
    client: Client,
    base: String,
    auth: SnowflakeAuth,
    database: Option<String>,
    schema: Option<String>,
    warehouse: Option<String>,
    role: Option<String>,
}

#[derive(Debug, Clone)]
enum SnowflakeAuth {
    /// Static token with its `X-Snowflake-Authorization-Token-Type`.
    Token { token: String, kind: &'static str },
    /// Key-pair: a fresh JWT is minted per request from these inputs.
    KeyPair { account: String, user: String, pem: String, fingerprint: String },
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    iat: i64,
    exp: i64,
}

// ---------------------------------------------------------------------------
// Connection parsing
// ---------------------------------------------------------------------------

// WHAT:  Account identifier → base URL. Full URLs pass through.
fn account_base_url(host: &str) -> String {
    let h = host.trim().trim_end_matches('/');
    if h.starts_with("http://") || h.starts_with("https://") {
        return h.to_string();
    }
    if h.contains("snowflakecomputing.") {
        return format!("https://{h}");
    }
    format!("https://{h}.snowflakecomputing.com")
}

// WHAT:  JWT account name: the locator before the first `.` (region suffix), uppercased.
fn jwt_account(host: &str) -> String {
    let h = host.trim().trim_start_matches("https://").trim_start_matches("http://");
    let h = h.split("/").next().unwrap_or(h);
    let h = h.split(".snowflakecomputing").next().unwrap_or(h);
    h.split('.').next().unwrap_or(h).to_ascii_uppercase()
}

#[derive(Debug, Default, PartialEq)]
struct DbField {
    database: Option<String>,
    schema: Option<String>,
    warehouse: Option<String>,
    role: Option<String>,
}

// WHAT:  "DB.SCHEMA;warehouse=WH;role=R" → parts (all optional).
fn parse_database_field(raw: Option<&str>) -> DbField {
    let mut out = DbField::default();
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else { return out };
    let mut parts = raw.split(';');
    if let Some(first) = parts.next() {
        let first = first.trim();
        if !first.is_empty() {
            match first.split_once('.') {
                Some((db, schema)) => {
                    out.database = Some(db.trim().to_string()).filter(|s| !s.is_empty());
                    out.schema = Some(schema.trim().to_string()).filter(|s| !s.is_empty());
                }
                None => out.database = Some(first.to_string()),
            }
        }
    }
    for p in parts {
        if let Some((k, v)) = p.split_once('=') {
            let v = v.trim().to_string();
            match k.trim().to_ascii_lowercase().as_str() {
                "warehouse" => out.warehouse = Some(v),
                "role" => out.role = Some(v),
                "schema" => out.schema = Some(v),
                "database" | "db" => out.database = Some(v),
                _ => {}
            }
        }
    }
    out
}

fn looks_like_jwt(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    parts.len() == 3 && parts.iter().all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'))
}

// WHAT:  Splits the secret into PEM + optional explicit fingerprint line.
fn split_pem_and_fingerprint(secret: &str) -> (String, Option<String>) {
    let mut pem_lines = Vec::new();
    let mut fingerprint = None;
    for line in secret.lines() {
        let t = line.trim();
        if t.starts_with("SHA256:") {
            fingerprint = Some(t.to_string());
        } else if !t.is_empty() || !pem_lines.is_empty() {
            pem_lines.push(line);
        }
    }
    let pem = pem_lines.join("\n").trim().to_string();
    (pem, fingerprint)
}

// ---------------------------------------------------------------------------
// Minimal DER handling: private key PEM → RSA public key fingerprint
// ---------------------------------------------------------------------------

fn pem_body(pem: &str) -> AppResult<Vec<u8>> {
    let body: String = pem.lines().filter(|l| !l.starts_with("-----")).map(str::trim).collect();
    base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| AppError::invalid_input(format!("Private key PEM is not valid base64: {e}")))
}

// WHAT:  Reads one DER TLV at `pos`; returns (tag, content range, next pos).
fn der_tlv(bytes: &[u8], pos: usize) -> Option<(u8, std::ops::Range<usize>, usize)> {
    let tag = *bytes.get(pos)?;
    let first = *bytes.get(pos + 1)?;
    let (len, hdr) = if first & 0x80 == 0 {
        (first as usize, 2)
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 {
            return None;
        }
        let mut len = 0usize;
        for i in 0..n {
            len = (len << 8) | *bytes.get(pos + 2 + i)? as usize;
        }
        (len, 2 + n)
    };
    let start = pos + hdr;
    let end = start.checked_add(len)?;
    if end > bytes.len() {
        return None;
    }
    Some((tag, start..end, end))
}

fn der_children(bytes: &[u8], range: std::ops::Range<usize>) -> Vec<(u8, std::ops::Range<usize>)> {
    let mut out = Vec::new();
    let mut pos = range.start;
    while pos < range.end {
        match der_tlv(bytes, pos) {
            Some((tag, r, next)) => {
                out.push((tag, r));
                pos = next;
            }
            None => break,
        }
    }
    out
}

fn der_len(len: usize) -> Vec<u8> {
    if len < 0x80 {
        vec![len as u8]
    } else {
        let mut bytes = len.to_be_bytes().to_vec();
        while bytes.len() > 1 && bytes[0] == 0 {
            bytes.remove(0);
        }
        let mut out = vec![0x80 | bytes.len() as u8];
        out.extend(bytes);
        out
    }
}

fn der_encode(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend(der_len(content.len()));
    out.extend_from_slice(content);
    out
}

// WHAT:  Extracts (modulus, exponent) DER INTEGER contents from a PKCS#1 RSAPrivateKey
//        or a PKCS#8 PrivateKeyInfo wrapping one.
fn rsa_public_parts(der: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let (tag, outer, _) = der_tlv(der, 0)?;
    if tag != 0x30 {
        return None;
    }
    let children = der_children(der, outer);
    // PKCS#1: INTEGER version, INTEGER n, INTEGER e, INTEGER d, …
    if children.len() >= 3 && children.iter().take(3).all(|(t, _)| *t == 0x02) && children.len() >= 9 {
        return Some((der[children[1].1.clone()].to_vec(), der[children[2].1.clone()].to_vec()));
    }
    // PKCS#8: INTEGER version, SEQUENCE algorithm, OCTET STRING privateKey
    if children.len() >= 3 && children[0].0 == 0x02 && children[1].0 == 0x30 && children[2].0 == 0x04 {
        let inner = &der[children[2].1.clone()];
        return rsa_public_parts(inner);
    }
    None
}

// WHAT:  SubjectPublicKeyInfo DER for rsaEncryption + RSAPublicKey{n, e}.
fn spki_der(n: &[u8], e: &[u8]) -> Vec<u8> {
    let rsa_pub = der_encode(0x30, &[der_encode(0x02, n), der_encode(0x02, e)].concat());
    let mut bit_string = vec![0u8];
    bit_string.extend(rsa_pub);
    // OID 1.2.840.113549.1.1.1 (rsaEncryption) + NULL
    let alg = der_encode(0x30, &[der_encode(0x06, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01]), der_encode(0x05, &[])].concat());
    der_encode(0x30, &[alg, der_encode(0x03, &bit_string)].concat())
}

// WHAT:  `SHA256:<base64(sha256(spki der))>` as Snowflake prints it (DESCRIBE USER … RSA_PUBLIC_KEY_FP).
fn public_key_fingerprint(pem: &str) -> AppResult<String> {
    if pem.contains("ENCRYPTED") {
        return Err(AppError::invalid_input(
            "Encrypted private keys are not supported. Decrypt the key (openssl pkcs8 -nocrypt) or append a line `SHA256:<fingerprint>` after the PEM.",
        ));
    }
    let der = pem_body(pem)?;
    let (n, e) = rsa_public_parts(&der).ok_or_else(|| {
        AppError::invalid_input("Could not read an RSA private key from the PEM. Append a line `SHA256:<public key fingerprint>` after the PEM (from DESCRIBE USER … RSA_PUBLIC_KEY_FP).")
    })?;
    let spki = spki_der(&n, &e);
    let digest = Sha256::digest(&spki);
    Ok(format!("SHA256:{}", base64::engine::general_purpose::STANDARD.encode(digest)))
}

fn build_auth(conn: &ResolvedConnection, host: &str) -> AppResult<SnowflakeAuth> {
    let secret = conn.secret.as_deref().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| {
        AppError::invalid_input("Snowflake needs a secret: a programmatic access token, an OAuth token or a private-key PEM.")
    })?;
    if secret.contains("-----BEGIN") {
        let user = conn.summary.username.as_deref().map(str::trim).filter(|u| !u.is_empty()).ok_or_else(|| {
            AppError::invalid_input("Key-pair authentication needs the Snowflake user name.")
        })?;
        let (pem, explicit) = split_pem_and_fingerprint(secret);
        let fingerprint = match explicit {
            Some(fp) => fp,
            None => public_key_fingerprint(&pem)?,
        };
        return Ok(SnowflakeAuth::KeyPair { account: jwt_account(host), user: user.to_ascii_uppercase(), pem, fingerprint });
    }
    if let Some(t) = secret.strip_prefix("oauth:") {
        return Ok(SnowflakeAuth::Token { token: t.trim().to_string(), kind: "OAUTH" });
    }
    if let Some(t) = secret.strip_prefix("pat:") {
        return Ok(SnowflakeAuth::Token { token: t.trim().to_string(), kind: "PROGRAMMATIC_ACCESS_TOKEN" });
    }
    let kind = if looks_like_jwt(secret) { "OAUTH" } else { "PROGRAMMATIC_ACCESS_TOKEN" };
    Ok(SnowflakeAuth::Token { token: secret.to_string(), kind })
}

fn jwt_claims(account: &str, user: &str, fingerprint: &str, now: i64) -> JwtClaims {
    JwtClaims {
        iss: format!("{account}.{user}.{fingerprint}"),
        sub: format!("{account}.{user}"),
        iat: now,
        exp: now + JWT_LIFETIME_SECS,
    }
}

fn mint_jwt(account: &str, user: &str, pem: &str, fingerprint: &str) -> AppResult<String> {
    let claims = jwt_claims(account, user, fingerprint, chrono::Utc::now().timestamp());
    let key = EncodingKey::from_rsa_pem(pem.as_bytes()).map_err(|e| AppError::invalid_input(format!("Private key is not a valid RSA PEM: {e}")))?;
    jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &key).map_err(|e| AppError::crypto(e.to_string()))
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).ok_or_else(|| {
        AppError::invalid_input("Snowflake needs the account identifier (e.g. xy12345.us-east-1) in the host field.")
    })?;
    let base = account_base_url(host);
    let auth = build_auth(conn, host)?;
    let db = parse_database_field(s.database.as_deref());
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .user_agent("db-free")
        .build()
        .map_err(|e| AppError::driver(e.to_string()))?;
    let integration = SnowflakeIntegration {
        engine: s.engine,
        client,
        base,
        auth,
        database: db.database,
        schema: db.schema,
        warehouse: db.warehouse,
        role: db.role,
    };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Result decoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct RowType {
    name: String,
    type_name: String,
    scale: i64,
}

fn parse_row_types(meta: &Json) -> Vec<RowType> {
    meta.get("rowType")
        .and_then(Json::as_array)
        .map(|a| {
            a.iter()
                .map(|c| RowType {
                    name: c.get("name").and_then(Json::as_str).unwrap_or("?").to_string(),
                    type_name: c.get("type").and_then(Json::as_str).unwrap_or("text").to_ascii_lowercase(),
                    scale: c.get("scale").and_then(Json::as_i64).unwrap_or(0),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn epoch_to_rfc3339(seconds: f64) -> Option<String> {
    let secs = seconds.floor() as i64;
    let nanos = ((seconds - secs as f64) * 1e9).round().clamp(0.0, 999_999_999.0) as u32;
    chrono::DateTime::from_timestamp(secs, nanos).map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}

// WHAT:  One string cell → typed Value based on the Snowflake column type.
fn decode_cell(raw: &Json, ty: &RowType) -> Value {
    let Some(s) = raw.as_str() else {
        return match raw {
            Json::Null => Value::Null,
            other => crate::integrations::http::json_to_value(other),
        };
    };
    match ty.type_name.as_str() {
        "fixed" => {
            if ty.scale == 0 {
                s.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Decimal(s.to_string()))
            } else {
                Value::Decimal(s.to_string())
            }
        }
        "real" | "float" | "double" => s.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::Text(s.to_string())),
        "boolean" => match s.to_ascii_lowercase().as_str() {
            "true" | "1" => Value::Bool(true),
            "false" | "0" => Value::Bool(false),
            _ => Value::Text(s.to_string()),
        },
        "binary" => {
            let bytes: Vec<u8> = (0..s.len()).step_by(2).filter_map(|i| s.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok())).collect();
            Value::Bytes(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        "date" => s
            .parse::<i64>()
            .ok()
            .and_then(|days| chrono::DateTime::from_timestamp(days * 86_400, 0))
            .map(|dt| Value::DateTime(dt.format("%Y-%m-%d").to_string()))
            .unwrap_or_else(|| Value::Text(s.to_string())),
        "time" => s
            .parse::<f64>()
            .ok()
            .and_then(|secs| chrono::DateTime::from_timestamp(secs.floor() as i64, ((secs.fract()) * 1e9).round() as u32))
            .map(|dt| {
                let text = dt.format("%H:%M:%S%.f").to_string();
                // chrono pads to 3/6/9 digits; show only the digits the column carries.
                let trimmed = match text.split_once('.') {
                    Some((head, frac)) => {
                        let frac = frac.trim_end_matches('0');
                        if frac.is_empty() { head.to_string() } else { format!("{head}.{frac}") }
                    }
                    None => text,
                };
                Value::Text(trimmed)
            })
            .unwrap_or_else(|| Value::Text(s.to_string())),
        "timestamp_ntz" | "timestamp_ltz" | "timestamp_tz" => {
            // TIMESTAMP_TZ arrives as "epoch.fraction offsetMinutes"; the others as "epoch.fraction".
            let mut parts = s.split_whitespace();
            let epoch = parts.next().and_then(|p| p.parse::<f64>().ok());
            match epoch.and_then(epoch_to_rfc3339) {
                Some(mut iso) => {
                    if let Some(off) = parts.next().and_then(|p| p.parse::<i64>().ok()) {
                        // Snowflake encodes the offset as minutes + 1440.
                        let minutes = off - 1_440;
                        if let Some(dt) = chrono::DateTime::parse_from_rfc3339(&iso).ok().and_then(|dt| {
                            chrono::FixedOffset::east_opt((minutes * 60) as i32).map(|tz| dt.with_timezone(&tz))
                        }) {
                            iso = dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
                        }
                    }
                    Value::DateTime(iso)
                }
                None => Value::Text(s.to_string()),
            }
        }
        "variant" | "object" | "array" => serde_json::from_str::<Json>(s).map(Value::Json).unwrap_or_else(|_| Value::Text(s.to_string())),
        _ => Value::Text(s.to_string()),
    }
}

fn decode_rows(types: &[RowType], data: &[Json]) -> Vec<Vec<Value>> {
    data.iter()
        .map(|row| {
            let cells = row.as_array().cloned().unwrap_or_default();
            types.iter().enumerate().map(|(i, ty)| cells.get(i).map(|c| decode_cell(c, ty)).unwrap_or(Value::Null)).collect()
        })
        .collect()
}

fn metas(types: &[RowType]) -> Vec<ColumnMeta> {
    types.iter().map(|t| ColumnMeta { name: t.name.clone(), type_name: t.type_name.clone() }).collect()
}

fn is_ddl_dml_response(body: &Json) -> bool {
    body.get("resultSetMetaData")
        .and_then(|m| m.get("rowType"))
        .and_then(Json::as_array)
        .is_some_and(|rt| rt.len() == 1 && rt[0].get("name").and_then(Json::as_str).is_some_and(|n| n.starts_with("number of rows")))
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

impl SnowflakeIntegration {
    fn auth_headers(&self) -> AppResult<Vec<(&'static str, String)>> {
        match &self.auth {
            SnowflakeAuth::Token { token, kind } => Ok(vec![("Authorization", format!("Bearer {token}")), ("X-Snowflake-Authorization-Token-Type", (*kind).to_string())]),
            SnowflakeAuth::KeyPair { account, user, pem, fingerprint } => {
                let jwt = mint_jwt(account, user, pem, fingerprint)?;
                Ok(vec![("Authorization", format!("Bearer {jwt}")), ("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT".to_string())])
            }
        }
    }

    fn request(&self, method: Method, path: &str) -> AppResult<reqwest::RequestBuilder> {
        let mut req = self.client.request(method, format!("{}{}", self.base, path)).header("Accept", "application/json").header("Content-Type", "application/json");
        for (k, v) in self.auth_headers()? {
            req = req.header(k, v);
        }
        Ok(req)
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> AppResult<(StatusCode, Json)> {
        let resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                AppError::timeout("Snowflake did not respond in time.")
            } else if e.is_connect() {
                AppError::not_connected(format!("Could not reach Snowflake: {e}"))
            } else {
                AppError::driver(e.to_string())
            }
        })?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let body: Json = serde_json::from_str(&text).unwrap_or(Json::String(text.clone()));
        if status.is_success() || status == StatusCode::ACCEPTED {
            return Ok((status, body));
        }
        Err(crate::integrations::http::status_error(status, &text))
    }

    // WHAT:  Submit → poll until complete → gather partitions up to `max_rows`.
    async fn statement(&self, sql: &str, max_rows: usize) -> AppResult<(Vec<RowType>, Vec<Json>, bool, Json)> {
        let mut body = json!({ "statement": sql, "timeout": STATEMENT_TIMEOUT_SECS, "parameters": {} });
        if let Some(db) = &self.database {
            body["database"] = json!(db);
        }
        if let Some(schema) = &self.schema {
            body["schema"] = json!(schema);
        }
        if let Some(wh) = &self.warehouse {
            body["warehouse"] = json!(wh);
        }
        if let Some(role) = &self.role {
            body["role"] = json!(role);
        }
        let (mut status, mut resp) = self.send(self.request(Method::POST, "/api/v2/statements")?.json(&body)).await?;
        let handle = resp.get("statementHandle").and_then(Json::as_str).map(str::to_string);
        let mut polls = 0;
        while status == StatusCode::ACCEPTED {
            let Some(h) = &handle else { break };
            polls += 1;
            if polls > MAX_POLLS {
                let _ = self.send(self.request(Method::POST, &format!("/api/v2/statements/{h}/cancel"))?).await;
                return Err(AppError::timeout("Snowflake statement is still running; cancelled."));
            }
            tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            let (s, r) = self.send(self.request(Method::GET, &format!("/api/v2/statements/{h}"))?).await?;
            status = s;
            resp = r;
        }
        let meta = resp.get("resultSetMetaData").cloned().unwrap_or(Json::Null);
        let types = parse_row_types(&meta);
        let mut data: Vec<Json> = resp.get("data").and_then(Json::as_array).cloned().unwrap_or_default();
        let partitions = meta.get("partitionInfo").and_then(Json::as_array).map(Vec::len).unwrap_or(1);
        let mut truncated = false;
        if data.len() > max_rows {
            data.truncate(max_rows);
            truncated = true;
        } else if let Some(h) = &handle {
            for p in 1..partitions {
                if data.len() >= max_rows {
                    truncated = true;
                    break;
                }
                let (_, part) = self.send(self.request(Method::GET, &format!("/api/v2/statements/{h}?partition={p}"))?).await?;
                let mut rows = part.get("data").and_then(Json::as_array).cloned().unwrap_or_default();
                if data.len() + rows.len() > max_rows {
                    rows.truncate(max_rows - data.len());
                    truncated = true;
                }
                data.extend(rows);
            }
        }
        Ok((types, data, truncated, resp))
    }

    async fn rows(&self, sql: &str, max_rows: usize) -> AppResult<ResultSet> {
        let (types, data, truncated, _) = self.statement(sql, max_rows).await?;
        Ok(ResultSet { columns: metas(&types), rows: decode_rows(&types, &data), truncated })
    }

    fn table_name(&self, table: &TableRef) -> String {
        let schema = table.schema.clone().or_else(|| self.schema.clone());
        let mut parts = Vec::new();
        if let Some(db) = &self.database {
            parts.push(quote_ident(db));
        }
        if let Some(s) = schema {
            parts.push(quote_ident(&s));
        }
        parts.push(quote_ident(&table.name));
        parts.join(".")
    }

    fn col_text(rs: &ResultSet, row: &[Value], name: &str) -> Option<String> {
        let idx = rs.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))?;
        match row.get(idx)? {
            Value::Text(s) => Some(s.clone()),
            Value::Int(i) => Some(i.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Null => None,
            other => Some(format!("{other:?}")),
        }
    }
}

#[async_trait]
impl Integration for SnowflakeIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { transactions: false, exact_estimate: false, ..Capabilities::SQL }
    }

    async fn ping(&self) -> AppResult<()> {
        self.rows("SELECT 1", 1).await.map(|_| ())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let rs = self.rows("SELECT CURRENT_VERSION()", 1).await?;
        Ok(rs.rows.first().and_then(|r| r.first()).and_then(|v| match v {
            Value::Text(s) => Some(format!("Snowflake {s}")),
            _ => None,
        }))
    }

    fn current_database(&self) -> Option<String> {
        self.database.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let rs = self.rows("SHOW DATABASES", 1_000).await?;
        let mut names: Vec<String> = rs.rows.iter().filter_map(|r| Self::col_text(&rs, r, "name")).collect();
        if names.is_empty() {
            if let Some(db) = &self.database {
                names.push(db.clone());
            }
        }
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let db = self.database.clone().ok_or_else(|| AppError::invalid_input("Set the database field (DB or DB.SCHEMA) to browse tables."))?;
        let scope = format!("IN DATABASE {}", quote_ident(&db));
        let mut schemas: Vec<SchemaInfo> = Vec::new();
        let mut push = |schema: String, table: TableInfo| match schemas.iter_mut().find(|s| s.name == schema) {
            Some(s) => s.tables.push(table),
            None => schemas.push(SchemaInfo { name: schema, tables: vec![table] }),
        };
        let tables = self.rows(&format!("SHOW TABLES {scope}"), 10_000).await?;
        for row in &tables.rows {
            let (Some(schema), Some(name)) = (Self::col_text(&tables, row, "schema_name"), Self::col_text(&tables, row, "name")) else { continue };
            if let Some(want) = &self.schema {
                if !want.eq_ignore_ascii_case(&schema) {
                    continue;
                }
            }
            if schema.eq_ignore_ascii_case("INFORMATION_SCHEMA") {
                continue;
            }
            let row_estimate = Self::col_text(&tables, row, "rows").and_then(|s| s.parse::<i64>().ok());
            push(schema.clone(), TableInfo { schema: Some(schema), name, kind: TableKind::Table, row_estimate });
        }
        if let Ok(views) = self.rows(&format!("SHOW VIEWS {scope}"), 10_000).await {
            for row in &views.rows {
                let (Some(schema), Some(name)) = (Self::col_text(&views, row, "schema_name"), Self::col_text(&views, row, "name")) else { continue };
                if let Some(want) = &self.schema {
                    if !want.eq_ignore_ascii_case(&schema) {
                        continue;
                    }
                }
                if schema.eq_ignore_ascii_case("INFORMATION_SCHEMA") {
                    continue;
                }
                push(schema.clone(), TableInfo { schema: Some(schema), name, kind: TableKind::View, row_estimate: None });
            }
        }
        if schemas.is_empty() {
            let rs = self.rows(&format!("SHOW SCHEMAS {scope}"), 1_000).await?;
            for row in &rs.rows {
                if let Some(name) = Self::col_text(&rs, row, "name") {
                    if !name.eq_ignore_ascii_case("INFORMATION_SCHEMA") {
                        schemas.push(SchemaInfo { name, tables: vec![] });
                    }
                }
            }
        }
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        for s in &mut schemas {
            s.tables.sort_by(|a, b| a.name.cmp(&b.name));
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let db = self.database.clone().ok_or_else(|| AppError::invalid_input("Set the database field to inspect columns."))?;
        let schema = table.schema.clone().or_else(|| self.schema.clone()).unwrap_or_else(|| "PUBLIC".into());
        let sql = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
            quote_ident(&db),
            crate::integrations::sql::quote_literal(&schema),
            crate::integrations::sql::quote_literal(&table.name)
        );
        let rs = self.rows(&sql, 5_000).await?;
        let pks: Vec<String> = match self.rows(&format!("SHOW PRIMARY KEYS IN TABLE {}", self.table_name(&TableRef { schema: Some(schema.clone()), name: table.name.clone() })), 100).await {
            Ok(pk) => pk.rows.iter().filter_map(|r| Self::col_text(&pk, r, "column_name")).collect(),
            Err(_) => Vec::new(),
        };
        let cols: Vec<ColumnInfo> = rs
            .rows
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                let name = Self::col_text(&rs, row, "COLUMN_NAME")?;
                Some(ColumnInfo {
                    primary_key: pks.iter().any(|p| p.eq_ignore_ascii_case(&name)),
                    data_type: Self::col_text(&rs, row, "DATA_TYPE").unwrap_or_default(),
                    nullable: Self::col_text(&rs, row, "IS_NULLABLE").map(|s| s.eq_ignore_ascii_case("YES")).unwrap_or(true),
                    name,
                    ordinal: i as u32 + 1,
                })
            })
            .collect();
        if cols.is_empty() {
            return Err(AppError::not_found(format!("Table {}.{} has no columns or does not exist.", schema, table.name)));
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let db = self.database.clone().ok_or_else(|| AppError::invalid_input("Set the database field."))?;
        let schema = table.schema.clone().or_else(|| self.schema.clone()).unwrap_or_else(|| "PUBLIC".into());
        let sql = format!(
            "SELECT ROW_COUNT FROM {}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
            quote_ident(&db),
            crate::integrations::sql::quote_literal(&schema),
            crate::integrations::sql::quote_literal(&table.name)
        );
        let rs = self.rows(&sql, 1).await?;
        Ok(rs.rows.first().and_then(|r| r.first()).and_then(|v| match v {
            Value::Int(i) => Some(*i),
            _ => None,
        }))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT COUNT(*) FROM {}{}", self.table_name(table), where_clause(self.engine, filters));
        let rs = self.rows(&sql, 1).await?;
        Ok(match rs.rows.first().and_then(|r| r.first()) {
            Some(Value::Int(i)) => *i,
            _ => 0,
        })
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            self.table_name(table),
            where_clause(self.engine, &query.filters),
            order_clause(self.engine, &query.sort),
            query.limit,
            query.offset
        );
        self.rows(&sql, query.limit as usize).await
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let text = sql.trim().trim_end_matches(';').trim();
        if text.is_empty() {
            return Err(AppError::invalid_input("Empty statement."));
        }
        let (types, data, truncated, resp) = self.statement(text, max_rows.max(1)).await?;
        if is_ddl_dml_response(&resp) {
            let n = data.first().and_then(|r| r.get(0)).and_then(Json::as_str).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            return Ok(vec![StatementResult::Affected { rows_affected: n }]);
        }
        Ok(vec![StatementResult::Rows { result: ResultSet { columns: metas(&types), rows: decode_rows(&types, &data), truncated } }])
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ty(name: &str, type_name: &str, scale: i64) -> RowType {
        RowType { name: name.into(), type_name: type_name.into(), scale }
    }

    #[test]
    fn account_and_database_parsing() {
        assert_eq!(account_base_url("xy12345.us-east-1"), "https://xy12345.us-east-1.snowflakecomputing.com");
        assert_eq!(account_base_url("https://acme.snowflakecomputing.com/"), "https://acme.snowflakecomputing.com");
        assert_eq!(account_base_url("acme.snowflakecomputing.com"), "https://acme.snowflakecomputing.com");
        assert_eq!(jwt_account("xy12345.us-east-1"), "XY12345");
        assert_eq!(jwt_account("https://myorg-myaccount.snowflakecomputing.com"), "MYORG-MYACCOUNT");
        let f = parse_database_field(Some("ANALYTICS.PUBLIC;warehouse=WH1;role=SYSADMIN"));
        assert_eq!(f, DbField { database: Some("ANALYTICS".into()), schema: Some("PUBLIC".into()), warehouse: Some("WH1".into()), role: Some("SYSADMIN".into()) });
        assert_eq!(parse_database_field(Some("DB")).database.as_deref(), Some("DB"));
        assert_eq!(parse_database_field(None), DbField::default());
    }

    #[test]
    fn token_kind_detection() {
        assert!(looks_like_jwt("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln"));
        assert!(!looks_like_jwt("ver:1-hint:abc-ETMsDgAAA"));
        let (pem, fp) = split_pem_and_fingerprint("-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n\nSHA256:xyz=");
        assert_eq!(fp.as_deref(), Some("SHA256:xyz="));
        assert!(pem.ends_with("-----END PRIVATE KEY-----"));
    }

    #[test]
    fn jwt_claims_follow_snowflake_format() {
        let c = jwt_claims("XY12345", "ALICE", "SHA256:abc=", 1_000);
        assert_eq!(c.iss, "XY12345.ALICE.SHA256:abc=");
        assert_eq!(c.sub, "XY12345.ALICE");
        assert_eq!(c.exp - c.iat, JWT_LIFETIME_SECS);
    }

    #[test]
    fn der_roundtrip_and_fingerprint_shape() {
        // A tiny synthetic PKCS#1 key: version, n, e, d, p, q, dp, dq, qi (values are placeholders).
        let ints: Vec<Vec<u8>> = vec![vec![0], vec![0x00, 0xc3, 0x55], vec![0x01, 0x00, 0x01], vec![1], vec![2], vec![3], vec![4], vec![5], vec![6]];
        let body: Vec<u8> = ints.iter().flat_map(|i| der_encode(0x02, i)).collect();
        let pkcs1 = der_encode(0x30, &body);
        let (n, e) = rsa_public_parts(&pkcs1).unwrap();
        assert_eq!(n, vec![0x00, 0xc3, 0x55]);
        assert_eq!(e, vec![0x01, 0x00, 0x01]);
        // Wrap in PKCS#8.
        let alg = der_encode(0x30, &[der_encode(0x06, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01]), der_encode(0x05, &[])].concat());
        let pkcs8 = der_encode(0x30, &[der_encode(0x02, &[0]), alg, der_encode(0x04, &pkcs1)].concat());
        let (n2, _) = rsa_public_parts(&pkcs8).unwrap();
        assert_eq!(n2, n);
        let pem = format!("-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----", base64::engine::general_purpose::STANDARD.encode(&pkcs8));
        let fp = public_key_fingerprint(&pem).unwrap();
        assert!(fp.starts_with("SHA256:") && fp.len() > 20);
        assert!(public_key_fingerprint("-----BEGIN ENCRYPTED PRIVATE KEY-----\nAA==\n-----END ENCRYPTED PRIVATE KEY-----").is_err());
        assert_eq!(der_len(0x7f), vec![0x7f]);
        assert_eq!(der_len(0x1234), vec![0x82, 0x12, 0x34]);
    }

    #[test]
    fn cells_decode_by_type() {
        assert_eq!(decode_cell(&json!("42"), &ty("a", "fixed", 0)), Value::Int(42));
        assert_eq!(decode_cell(&json!("4.20"), &ty("a", "fixed", 2)), Value::Decimal("4.20".into()));
        assert_eq!(decode_cell(&json!("1.5"), &ty("a", "real", 0)), Value::Float(1.5));
        assert_eq!(decode_cell(&json!("true"), &ty("a", "boolean", 0)), Value::Bool(true));
        assert_eq!(decode_cell(&json!("0102"), &ty("a", "binary", 0)), Value::Bytes("AQI=".into()));
        assert_eq!(decode_cell(&json!("19723"), &ty("a", "date", 0)), Value::DateTime("2024-01-01".into()));
        assert_eq!(decode_cell(&json!("1704067200.000000000"), &ty("a", "timestamp_ntz", 9)), Value::DateTime("2024-01-01T00:00:00Z".into()));
        assert_eq!(decode_cell(&json!("1704067200.000000000 1500"), &ty("a", "timestamp_tz", 9)), Value::DateTime("2024-01-01T01:00:00+01:00".into()));
        assert_eq!(decode_cell(&json!("3661.5"), &ty("a", "time", 1)), Value::Text("01:01:01.5".into()));
        assert_eq!(decode_cell(&json!("{\"a\":1}"), &ty("a", "variant", 0)), Value::Json(json!({"a": 1})));
        assert_eq!(decode_cell(&Json::Null, &ty("a", "text", 0)), Value::Null);
        let types = parse_row_types(&json!({"rowType": [{"name": "ID", "type": "fixed", "scale": 0}, {"name": "N", "type": "text"}]}));
        let rows = decode_rows(&types, &[json!(["1", "x"]), json!(["2", null])]);
        assert_eq!(rows[1][1], Value::Null);
        assert_eq!(rows[0][0], Value::Int(1));
        assert!(is_ddl_dml_response(&json!({"resultSetMetaData": {"rowType": [{"name": "number of rows inserted"}]}})));
        assert!(!is_ddl_dml_response(&json!({"resultSetMetaData": {"rowType": [{"name": "ID"}]}})));
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        use crate::model::{ConnectionSummary, Environment, SslMode};
        let Ok(account) = std::env::var("DBFREE_TEST_SNOWFLAKE_ACCOUNT") else { return };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Snowflake,
                environment: Environment::Local,
                read_only: false,
                host: Some(account),
                port: None,
                database: std::env::var("DBFREE_TEST_SNOWFLAKE_DB").ok(),
                username: std::env::var("DBFREE_TEST_SNOWFLAKE_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_SNOWFLAKE_SECRET").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("Snowflake"), "{version}");
        let out = db.execute("SELECT 1 AS n, 'x' AS s", 10).await.unwrap_or_else(|e| panic!("execute: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows[0][0], Value::Int(1)),
            _ => panic!("rows"),
        }
    }
}
