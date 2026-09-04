// SOT: qldb-integration, aws-sigv4, partiql, ion, ion-binary-reader, qldb-hash, qldb-session-api

use crate::error::{AppError, AppResult};
use crate::integrations::aws_sigv4::{sign_post, AwsCredentials, SignRequest};
use crate::integrations::http::{local, Auth, HttpClient};
use crate::integrations::sql::quote_literal;
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use reqwest::Method;
use std::sync::Arc;
use tokio::sync::Mutex;

// ============================================================================
// WHAT:  Amazon QLDB adapter over the QLDB Session API (SigV4, JSON 1.0).
//        `host` = region, `database` = ledger. Statements are PartiQL; results
//        come back as Ion *binary* datagrams, decoded here by a small reader
//        (null, bool, int, float, decimal, timestamp, symbol, string, clob,
//        blob, list, sexp, struct, annotations and local symbol tables).
// WHY:   The AWS SDK + ion-rs would add a large dependency tree for one
//        engine; the subset of Ion QLDB emits is small and well specified.
// HOW:   Read-only work (catalog, pages, counts, SELECT) runs inside a
//        transaction that is aborted afterwards (no commit digest needed).
//        Writes (INSERT/UPDATE/DELETE/CREATE/DROP/UNDROP/FROM …) are committed
//        with a `CommitDigest` = QLDB hash chain (Ion-hash of the transaction
//        id, then `dot`-folded with the Ion-hash of every statement).
//        QLDB has no ORDER BY / OFFSET, so sorting and paging are client-side
//        over a bounded window.
// WHERE: src-tauri/src/integrations/aws_sigv4.rs, src-tauri/src/integrations/http.rs
// ============================================================================

const CONTENT_TYPE: &str = "application/x-amz-json-1.0";
const TARGET: &str = "QldbSession.SendCommand";
const SCAN_CAP: usize = 2_000;
const SAMPLE: usize = 50;
const ID: &str = "_id";

pub struct QldbIntegration {
    engine: Engine,
    http: HttpClient,
    creds: AwsCredentials,
    host: String,
    ledger: String,
    session: Mutex<Option<String>>,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let creds = AwsCredentials::from_connection(conn)?;
    let ledger = conn
        .summary
        .database
        .as_deref()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .ok_or_else(|| AppError::invalid_input("QLDB ledger name is required (database field)."))?
        .to_string();
    let host = format!("session.qldb.{}.amazonaws.com", creds.region);
    let http = HttpClient::new(format!("https://{host}"), Auth::None, false)?;
    let integration = QldbIntegration { engine: conn.summary.engine, http, creds, host, ledger, session: Mutex::new(None), read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// SHA-256 (local copy: the `sha2` crate is confined to aws_sigv4.rs / snowflake.rs)
// ---------------------------------------------------------------------------

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (slot, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *slot = slot.wrapping_add(v);
        }
    }
    let mut out = [0u8; 32];
    for (i, v) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&v.to_be_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// QLDB hash (Ion-hash of strings + the `dot` fold from the QLDB drivers)
// ---------------------------------------------------------------------------

// WHAT:  Ion-hash of an Ion string: H(0x0B ‖ TQ=0x80 ‖ escape(utf8) ‖ 0x0E).
pub fn ion_hash_string(s: &str) -> [u8; 32] {
    let mut bytes = vec![0x0B, 0x80];
    for b in s.as_bytes() {
        if matches!(b, 0x0B | 0x0C | 0x0E) {
            bytes.push(0x0C);
        }
        bytes.push(*b);
    }
    bytes.push(0x0E);
    sha256(&bytes)
}

// WHAT:  Compares two hashes as reversed signed-byte arrays (QldbHash.hashComparator).
fn hash_cmp(a: &[u8; 32], b: &[u8; 32]) -> std::cmp::Ordering {
    for i in (0..32).rev() {
        let x = a[i] as i8;
        let y = b[i] as i8;
        if x != y {
            return x.cmp(&y);
        }
    }
    std::cmp::Ordering::Equal
}

pub fn qldb_dot(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut joined = Vec::with_capacity(64);
    if hash_cmp(a, b) == std::cmp::Ordering::Less {
        joined.extend_from_slice(a);
        joined.extend_from_slice(b);
    } else {
        joined.extend_from_slice(b);
        joined.extend_from_slice(a);
    }
    sha256(&joined)
}

pub fn commit_digest(txn_id: &str, statements: &[&str]) -> [u8; 32] {
    let mut h = ion_hash_string(txn_id);
    for s in statements {
        h = qldb_dot(&h, &ion_hash_string(s));
    }
    h
}

// ---------------------------------------------------------------------------
// Ion binary reader
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Ion {
    Null(&'static str),
    Bool(bool),
    Int(i128),
    Float(f64),
    Decimal(String),
    Timestamp(String),
    Symbol(String),
    String(String),
    Clob(Vec<u8>),
    Blob(Vec<u8>),
    List(Vec<Ion>),
    Sexp(Vec<Ion>),
    Struct(Vec<(String, Ion)>),
}

const SYSTEM_SYMBOLS: [&str; 10] = ["$0", "$ion", "$ion_1_0", "$ion_symbol_table", "name", "version", "imports", "symbols", "max_id", "$ion_shared_symbol_table"];

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    symbols: Vec<String>,
}

fn ion_err(msg: impl Into<String>) -> AppError {
    AppError::driver(format!("Ion decode error: {}", msg.into()))
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0, symbols: SYSTEM_SYMBOLS.iter().map(|s| (*s).to_string()).collect() }
    }

    fn byte(&mut self) -> AppResult<u8> {
        let b = *self.buf.get(self.pos).ok_or_else(|| ion_err("unexpected end of data"))?;
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> AppResult<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|e| *e <= self.buf.len()).ok_or_else(|| ion_err("length past end of data"))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn var_uint(&mut self) -> AppResult<u64> {
        let mut v: u64 = 0;
        for _ in 0..10 {
            let b = self.byte()?;
            v = (v << 7) | u64::from(b & 0x7F);
            if b & 0x80 != 0 {
                return Ok(v);
            }
        }
        Err(ion_err("VarUInt too long"))
    }

    fn var_int(&mut self) -> AppResult<i64> {
        let first = self.byte()?;
        let neg = first & 0x40 != 0;
        let mut v: i64 = i64::from(first & 0x3F);
        if first & 0x80 == 0 {
            for _ in 0..9 {
                let b = self.byte()?;
                v = (v << 7) | i64::from(b & 0x7F);
                if b & 0x80 != 0 {
                    break;
                }
            }
        }
        Ok(if neg { -v } else { v })
    }

    fn symbol_name(&self, sid: u64) -> String {
        self.symbols.get(sid as usize).cloned().unwrap_or_else(|| format!("${sid}"))
    }

    // WHAT:  Reads the next top-level value; `None` when a symbol table was consumed.
    fn read_top(&mut self) -> AppResult<Option<Ion>> {
        let (annotations, value) = self.read_annotated()?;
        let Some(value) = value else { return Ok(None) };
        if annotations.first() == Some(&3) {
            if let Ion::Struct(fields) = &value {
                self.apply_symbol_table(fields);
                return Ok(None);
            }
        }
        Ok(Some(value))
    }

    fn apply_symbol_table(&mut self, fields: &[(String, Ion)]) {
        let imports = fields.iter().find(|(k, _)| k == "imports").map(|(_, v)| v);
        match imports {
            Some(Ion::Symbol(s)) if s == "$ion_symbol_table" => {}
            Some(Ion::List(items)) => {
                self.symbols.truncate(SYSTEM_SYMBOLS.len());
                for item in items {
                    if let Ion::Struct(f) = item {
                        let max_id = f.iter().find(|(k, _)| k == "max_id").and_then(|(_, v)| if let Ion::Int(i) = v { Some(*i) } else { None }).unwrap_or(0);
                        for _ in 0..max_id.clamp(0, 100_000) {
                            let n = self.symbols.len();
                            self.symbols.push(format!("${n}"));
                        }
                    }
                }
            }
            _ => self.symbols.truncate(SYSTEM_SYMBOLS.len()),
        }
        if let Some((_, Ion::List(syms))) = fields.iter().find(|(k, _)| k == "symbols") {
            for s in syms {
                let n = self.symbols.len();
                self.symbols.push(match s {
                    Ion::String(t) => t.clone(),
                    _ => format!("${n}"),
                });
            }
        }
    }

    // WHAT:  Reads one value; unwraps annotation wrappers (returning the sids) and
    //        skips NOP padding. `None` value only when the stream ends on padding.
    fn read_annotated(&mut self) -> AppResult<(Vec<u64>, Option<Ion>)> {
        loop {
            if self.pos >= self.buf.len() {
                return Ok((vec![], None));
            }
            let td = self.byte()?;
            let t = td >> 4;
            let l = td & 0x0F;
            if t == 0 && l != 15 {
                let len = if l == 14 { self.var_uint()? as usize } else { l as usize };
                self.take(len)?;
                continue;
            }
            if t == 14 {
                if l == 15 {
                    return Ok((vec![], Some(Ion::Null("annotation"))));
                }
                let len = if l == 14 { self.var_uint()? as usize } else { l as usize };
                let end = self.pos + len;
                let alen = self.var_uint()? as usize;
                let aend = self.pos + alen;
                let mut sids = Vec::new();
                while self.pos < aend {
                    sids.push(self.var_uint()?);
                }
                let value = self.read_value()?;
                self.pos = end;
                return Ok((sids, Some(value)));
            }
            return Ok((vec![], Some(self.read_body(t, l)?)));
        }
    }

    fn read_value(&mut self) -> AppResult<Ion> {
        let (_, v) = self.read_annotated()?;
        v.ok_or_else(|| ion_err("missing value"))
    }

    fn read_body(&mut self, t: u8, l: u8) -> AppResult<Ion> {
        if l == 15 {
            return Ok(Ion::Null(type_label(t)));
        }
        if t == 1 {
            return Ok(Ion::Bool(l == 1));
        }
        let len = if l == 14 || (t == 13 && l == 1) { self.var_uint()? as usize } else { l as usize };
        let body = self.take(len)?;
        match t {
            2 | 3 => {
                let mag = uint_of(body)?;
                Ok(Ion::Int(if t == 3 { -mag } else { mag }))
            }
            4 => Ok(Ion::Float(match body.len() {
                0 => 0.0,
                4 => f64::from(f32::from_be_bytes([body[0], body[1], body[2], body[3]])),
                8 => f64::from_be_bytes([body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7]]),
                _ => return Err(ion_err("bad float length")),
            })),
            5 => decode_decimal(body),
            6 => decode_timestamp(body),
            7 => Ok(Ion::Symbol(self.symbol_name(uint_of(body)? as u64))),
            8 => Ok(Ion::String(String::from_utf8_lossy(body).into_owned())),
            9 => Ok(Ion::Clob(body.to_vec())),
            10 => Ok(Ion::Blob(body.to_vec())),
            11 | 12 => {
                let mut sub = Reader { buf: body, pos: 0, symbols: std::mem::take(&mut self.symbols) };
                let mut items = Vec::new();
                while sub.pos < sub.buf.len() {
                    let (_, v) = sub.read_annotated()?;
                    if let Some(v) = v {
                        items.push(v);
                    }
                }
                self.symbols = sub.symbols;
                Ok(if t == 11 { Ion::List(items) } else { Ion::Sexp(items) })
            }
            13 => {
                let mut sub = Reader { buf: body, pos: 0, symbols: std::mem::take(&mut self.symbols) };
                let mut fields = Vec::new();
                while sub.pos < sub.buf.len() {
                    let sid = sub.var_uint()?;
                    let (_, v) = sub.read_annotated()?;
                    if let Some(v) = v {
                        fields.push((sub.symbol_name(sid), v));
                    }
                }
                self.symbols = sub.symbols;
                Ok(Ion::Struct(fields))
            }
            _ => Err(ion_err(format!("unknown type {t}"))),
        }
    }
}

fn type_label(t: u8) -> &'static str {
    match t {
        0 => "null",
        1 => "bool",
        2 | 3 => "int",
        4 => "float",
        5 => "decimal",
        6 => "timestamp",
        7 => "symbol",
        8 => "string",
        9 => "clob",
        10 => "blob",
        11 => "list",
        12 => "sexp",
        13 => "struct",
        _ => "null",
    }
}

fn uint_of(body: &[u8]) -> AppResult<i128> {
    if body.len() > 15 {
        return Err(ion_err("integer wider than 120 bits"));
    }
    Ok(body.iter().fold(0i128, |acc, b| (acc << 8) | i128::from(*b)))
}

// Signed-magnitude Int: sign in the high bit of the first byte.
fn int_of(body: &[u8]) -> AppResult<(bool, i128)> {
    let Some(first) = body.first() else { return Ok((false, 0)) };
    let neg = first & 0x80 != 0;
    let mut bytes = body.to_vec();
    bytes[0] &= 0x7F;
    Ok((neg, uint_of(&bytes)?))
}

fn decode_decimal(body: &[u8]) -> AppResult<Ion> {
    if body.is_empty() {
        return Ok(Ion::Decimal("0".into()));
    }
    let mut r = Reader::new(body);
    let exp = r.var_int()?;
    let (neg, coef) = int_of(&body[r.pos..])?;
    let digits = coef.to_string();
    let text = if exp >= 0 {
        format!("{digits}{}", "0".repeat(exp as usize))
    } else {
        let scale = (-exp) as usize;
        if digits.len() > scale {
            let (i, f) = digits.split_at(digits.len() - scale);
            format!("{i}.{f}")
        } else {
            format!("0.{}{digits}", "0".repeat(scale - digits.len()))
        }
    };
    Ok(Ion::Decimal(if neg { format!("-{text}") } else { text }))
}

fn decode_timestamp(body: &[u8]) -> AppResult<Ion> {
    let mut r = Reader::new(body);
    let unknown_offset = body.first() == Some(&0xC0);
    let offset = r.var_int()?;
    let year = r.var_uint()?;
    let mut parts = Vec::new();
    while r.pos < body.len() && parts.len() < 5 {
        parts.push(r.var_uint()?);
    }
    let frac = if r.pos < body.len() {
        let exp = r.var_int()?;
        let (_, coef) = int_of(&body[r.pos..])?;
        if exp < 0 {
            let digits = coef.to_string();
            let scale = (-exp) as usize;
            Some(format!("{}{digits}", "0".repeat(scale.saturating_sub(digits.len()))))
        } else {
            None
        }
    } else {
        None
    };
    let mut s = format!("{year:04}");
    if let Some(m) = parts.first() {
        s.push_str(&format!("-{m:02}"));
    }
    if let Some(d) = parts.get(1) {
        s.push_str(&format!("-{d:02}"));
    }
    if parts.len() >= 4 {
        s.push_str(&format!("T{:02}:{:02}", parts[2], parts[3]));
        if let Some(sec) = parts.get(4) {
            s.push_str(&format!(":{sec:02}"));
            if let Some(f) = frac {
                s.push_str(&format!(".{f}"));
            }
        }
        if unknown_offset {
            s.push_str("-00:00");
        } else if offset == 0 {
            s.push('Z');
        } else {
            s.push_str(&format!("{}{:02}:{:02}", if offset < 0 { '-' } else { '+' }, offset.abs() / 60, offset.abs() % 60));
        }
    } else if parts.len() < 2 {
        s.push('T');
    }
    Ok(Ion::Timestamp(s))
}

// WHAT:  Decodes a whole Ion binary datagram (BVM + symbol tables + values).
pub fn decode_datagram(bytes: &[u8]) -> AppResult<Vec<Ion>> {
    let mut r = Reader::new(bytes);
    let mut out = Vec::new();
    while r.pos < r.buf.len() {
        if r.buf[r.pos..].starts_with(&[0xE0, 0x01, 0x00, 0xEA]) {
            r.pos += 4;
            r.symbols = SYSTEM_SYMBOLS.iter().map(|s| (*s).to_string()).collect();
            continue;
        }
        if let Some(v) = r.read_top()? {
            out.push(v);
        }
    }
    Ok(out)
}

pub fn ion_to_json(v: &Ion) -> serde_json::Value {
    match v {
        Ion::Null(_) => serde_json::Value::Null,
        Ion::Bool(b) => serde_json::Value::Bool(*b),
        Ion::Int(i) => i64::try_from(*i).map(|x| serde_json::Value::Number(x.into())).unwrap_or_else(|_| serde_json::Value::String(i.to_string())),
        Ion::Float(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null),
        Ion::Decimal(d) | Ion::Timestamp(d) | Ion::Symbol(d) | Ion::String(d) => serde_json::Value::String(d.clone()),
        Ion::Clob(b) => serde_json::Value::String(String::from_utf8_lossy(b).into_owned()),
        Ion::Blob(b) => serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Ion::List(items) | Ion::Sexp(items) => serde_json::Value::Array(items.iter().map(ion_to_json).collect()),
        Ion::Struct(fields) => serde_json::Value::Object(fields.iter().map(|(k, v)| (k.clone(), ion_to_json(v))).collect()),
    }
}

pub fn ion_to_value(v: &Ion) -> Value {
    match v {
        Ion::Null(_) => Value::Null,
        Ion::Bool(b) => Value::Bool(*b),
        Ion::Int(i) => i64::try_from(*i).map(Value::Int).unwrap_or_else(|_| Value::Decimal(i.to_string())),
        Ion::Float(f) => Value::Float(*f),
        Ion::Decimal(d) => Value::Decimal(d.clone()),
        Ion::Timestamp(t) => Value::DateTime(t.clone()),
        Ion::Symbol(s) | Ion::String(s) => Value::Text(s.clone()),
        Ion::Clob(b) => Value::Text(String::from_utf8_lossy(b).into_owned()),
        Ion::Blob(b) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(b)),
        Ion::List(_) | Ion::Sexp(_) | Ion::Struct(_) => Value::Json(ion_to_json(v)),
    }
}

pub fn ion_type_name(v: &Ion) -> &'static str {
    match v {
        Ion::Null(t) => t,
        Ion::Bool(_) => "bool",
        Ion::Int(_) => "int",
        Ion::Float(_) => "float",
        Ion::Decimal(_) => "decimal",
        Ion::Timestamp(_) => "timestamp",
        Ion::Symbol(_) => "symbol",
        Ion::String(_) => "string",
        Ion::Clob(_) => "clob",
        Ion::Blob(_) => "blob",
        Ion::List(_) => "list",
        Ion::Sexp(_) => "sexp",
        Ion::Struct(_) => "struct",
    }
}

// WHAT:  Result values (one Ion value each; structs become rows) → grid.
pub fn values_to_result(values: &[Ion], pinned: &[&str]) -> ResultSet {
    let mut names: Vec<String> = pinned.iter().map(|p| (*p).to_string()).collect();
    let mut types: Vec<&'static str> = vec!["null"; names.len()];
    let all_structs = values.iter().all(|v| matches!(v, Ion::Struct(_)));
    if values.is_empty() && names.is_empty() {
        names.push("value".into());
        types.push("null");
    } else if !all_structs {
        names = vec!["value".into()];
        types = vec![values.first().map(ion_type_name).unwrap_or("null")];
    } else {
        for v in values {
            if let Ion::Struct(fields) = v {
                for (k, fv) in fields {
                    match names.iter().position(|n| n == k) {
                        Some(i) => {
                            if types[i] == "null" && !matches!(fv, Ion::Null(_)) {
                                types[i] = ion_type_name(fv);
                            }
                        }
                        None => {
                            names.push(k.clone());
                            types.push(ion_type_name(fv));
                        }
                    }
                }
            }
        }
    }
    let rows = values
        .iter()
        .map(|v| match v {
            Ion::Struct(fields) if all_structs => names.iter().map(|n| fields.iter().find(|(k, _)| k == n).map(|(_, fv)| ion_to_value(fv)).unwrap_or(Value::Null)).collect(),
            other => vec![ion_to_value(other)],
        })
        .collect();
    let columns = names.into_iter().zip(types).map(|(name, t)| ColumnMeta { name, type_name: t.to_string() }).collect();
    ResultSet { columns, rows, truncated: false }
}

// ---------------------------------------------------------------------------
// PartiQL builders
// ---------------------------------------------------------------------------

fn literal(raw: &str) -> String {
    let t = raw.trim();
    if t.parse::<f64>().is_ok() || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        t.to_lowercase()
    } else {
        quote_literal(t)
    }
}

pub fn where_clause(filters: &[FilterRule]) -> String {
    if filters.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| {
            let col = if f.column == ID { "metadata.id".to_string() } else { format!("data.{}", quote_ident(&f.column)) };
            let v = f.value.trim();
            match f.op {
                FilterOp::Eq => format!("{col} = {}", literal(v)),
                FilterOp::Ne => format!("{col} <> {}", literal(v)),
                FilterOp::Gt => format!("{col} > {}", literal(v)),
                FilterOp::Gte => format!("{col} >= {}", literal(v)),
                FilterOp::Lt => format!("{col} < {}", literal(v)),
                FilterOp::Lte => format!("{col} <= {}", literal(v)),
                FilterOp::Contains => format!("{col} LIKE {}", quote_literal(&format!("%{v}%"))),
                FilterOp::StartsWith => format!("{col} LIKE {}", quote_literal(&format!("{v}%"))),
                FilterOp::EndsWith => format!("{col} LIKE {}", quote_literal(&format!("%{v}"))),
                FilterOp::In => format!("{col} IN ({})", v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(literal).collect::<Vec<_>>().join(", ")),
                FilterOp::IsNull => format!("{col} IS NULL"),
                FilterOp::IsNotNull => format!("{col} IS NOT NULL"),
            }
        })
        .collect();
    format!(" WHERE {}", parts.join(" AND "))
}

// WHAT:  `_committed_data` view exposes metadata.id alongside the document.
fn committed_view(table: &str) -> String {
    quote_ident(&format!("_ql_committed_{table}"))
}

fn select_sql(table: &str, filters: &[FilterRule]) -> String {
    format!("SELECT metadata.id AS {}, data FROM {}{}", quote_ident(ID), committed_view(table), where_clause(filters))
}

// WHAT:  `{ _id, data: {…} }` → `{ _id, …data }` so document fields become columns.
fn flatten_committed(values: Vec<Ion>) -> Vec<Ion> {
    values
        .into_iter()
        .map(|v| match v {
            Ion::Struct(fields) => {
                let mut out = Vec::new();
                for (k, fv) in fields {
                    match (k.as_str(), fv) {
                        ("data", Ion::Struct(inner)) => out.extend(inner),
                        (k, fv) => out.push((k.to_string(), fv)),
                    }
                }
                Ion::Struct(out)
            }
            other => other,
        })
        .collect()
}

pub fn is_write(stmt: &str) -> bool {
    let head = stmt.split_whitespace().next().unwrap_or_default().to_uppercase();
    matches!(head.as_str(), "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "DROP" | "UNDROP" | "FROM")
}

impl QldbIntegration {
    async fn send(&self, body: serde_json::Value) -> AppResult<serde_json::Value> {
        let bytes = serde_json::to_vec(&body).map_err(|e| AppError::internal(e.to_string()))?;
        let signed = sign_post(
            &self.creds,
            &SignRequest {
                service: "qldb",
                host: &self.host,
                path: "/",
                amz_target: Some(TARGET),
                content_type: CONTENT_TYPE,
                body: &bytes,
                now: chrono::Utc::now(),
            },
        )?;
        let mut req = self.http.request(Method::POST, "/").body(bytes);
        for (k, v) in &signed.headers {
            if k != "host" {
                req = req.header(k.as_str(), v.as_str());
            }
        }
        let resp = self.http.send(req).await?;
        resp.json().await.map_err(|e| AppError::driver(format!("Malformed QLDB response: {e}")))
    }

    async fn session_token(&self) -> AppResult<String> {
        let mut guard = self.session.lock().await;
        if let Some(t) = guard.as_ref() {
            return Ok(t.clone());
        }
        let resp = self.send(serde_json::json!({"StartSession": {"LedgerName": self.ledger}})).await?;
        let token = resp.pointer("/StartSession/SessionToken").and_then(|t| t.as_str()).ok_or_else(|| AppError::driver("QLDB did not return a session token."))?.to_string();
        *guard = Some(token.clone());
        Ok(token)
    }

    async fn command(&self, token: &str, cmd: serde_json::Value) -> AppResult<serde_json::Value> {
        let mut body = cmd;
        body["SessionToken"] = serde_json::Value::String(token.to_string());
        match self.send(body).await {
            Err(e) if e.message().contains("InvalidSession") => {
                *self.session.lock().await = None;
                Err(AppError::not_connected("QLDB session expired; run the statement again."))
            }
            other => other,
        }
    }

    async fn abort(&self, token: &str) {
        let _ = self.command(token, serde_json::json!({"AbortTransaction": {}})).await;
    }

    // WHAT:  Runs statements in one transaction. Read-only → abort afterwards;
    //        writes → commit with the hash-chain digest.
    async fn transaction(&self, statements: &[&str], max_values: usize, commit: bool) -> AppResult<Vec<Vec<Ion>>> {
        let token = self.session_token().await?;
        let start = self.command(&token, serde_json::json!({"StartTransaction": {}})).await?;
        let txn = start.pointer("/StartTransaction/TransactionId").and_then(|t| t.as_str()).ok_or_else(|| AppError::driver("QLDB did not return a transaction id."))?.to_string();
        let mut results = Vec::new();
        for stmt in statements {
            match self.run_statement(&token, &txn, stmt, max_values).await {
                Ok(vals) => results.push(vals),
                Err(e) => {
                    self.abort(&token).await;
                    return Err(e);
                }
            }
        }
        if commit {
            let digest = base64::engine::general_purpose::STANDARD.encode(commit_digest(&txn, statements));
            let resp = self.command(&token, serde_json::json!({"CommitTransaction": {"TransactionId": txn, "CommitDigest": digest}})).await;
            if let Err(e) = resp {
                self.abort(&token).await;
                return Err(AppError::driver(format!("QLDB commit failed (transaction rolled back): {}", e.message())));
            }
        } else {
            self.abort(&token).await;
        }
        Ok(results)
    }

    async fn run_statement(&self, token: &str, txn: &str, stmt: &str, max_values: usize) -> AppResult<Vec<Ion>> {
        let resp = self.command(token, serde_json::json!({"ExecuteStatement": {"TransactionId": txn, "Statement": stmt}})).await?;
        let mut page = resp.pointer("/ExecuteStatement/FirstPage").cloned().unwrap_or(serde_json::json!({}));
        let mut values = Vec::new();
        loop {
            for holder in page.get("Values").and_then(|v| v.as_array()).into_iter().flatten() {
                if let Some(b64) = holder.get("IonBinary").and_then(|b| b.as_str()) {
                    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).map_err(|e| AppError::driver(format!("Ion payload is not base64: {e}")))?;
                    values.extend(decode_datagram(&bytes)?);
                } else if let Some(text) = holder.get("IonText").and_then(|t| t.as_str()) {
                    values.push(Ion::String(text.to_string()));
                }
            }
            match page.get("NextPageToken").and_then(|t| t.as_str()) {
                Some(next) if values.len() < max_values => {
                    let fetched = self.command(token, serde_json::json!({"FetchPage": {"TransactionId": txn, "NextPageToken": next}})).await?;
                    page = fetched.pointer("/FetchPage/Page").cloned().unwrap_or(serde_json::json!({}));
                }
                _ => break,
            }
        }
        values.truncate(max_values.max(1));
        Ok(values)
    }

    async fn select(&self, sql: &str, max_values: usize) -> AppResult<Vec<Ion>> {
        Ok(self.transaction(&[sql], max_values, false).await?.into_iter().next().unwrap_or_default())
    }

    async fn table_names(&self) -> AppResult<Vec<String>> {
        let vals = self.select("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'", 1_000).await?;
        Ok(vals
            .iter()
            .filter_map(|v| match v {
                Ion::Struct(f) => f.iter().find(|(k, _)| k == "name").and_then(|(_, n)| match n {
                    Ion::String(s) | Ion::Symbol(s) => Some(s.clone()),
                    _ => None,
                }),
                _ => None,
            })
            .collect())
    }
}

#[async_trait]
impl Integration for QldbIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: true, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true }
    }

    async fn ping(&self) -> AppResult<()> {
        self.session_token().await.map(|_| ())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some(format!("Amazon QLDB ({})", self.creds.region)))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.ledger.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.ledger.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let tables = self.table_names().await?.into_iter().map(|name| TableInfo { schema: Some(self.ledger.clone()), name, kind: TableKind::Table, row_estimate: None }).collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.ledger.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let vals = flatten_committed(self.select(&select_sql(&table.name, &[]), SAMPLE).await?);
        let rs = values_to_result(&vals, &[ID]);
        Ok(rs
            .columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| ColumnInfo { primary_key: c.name == ID, nullable: c.name != ID, name: c.name, data_type: c.type_name, ordinal: i as u32 + 1 })
            .collect())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT COUNT(*) AS n FROM {}{}", committed_view(&table.name), where_clause(filters));
        let vals = self.select(&sql, 1).await?;
        Ok(match vals.first() {
            Some(Ion::Struct(f)) => f.first().map(|(_, v)| ion_to_value(v)).map(|v| match v {
                Value::Int(i) => i,
                Value::Decimal(d) => d.parse().unwrap_or(0),
                _ => 0,
            }).unwrap_or(0),
            Some(Ion::Int(i)) => i64::try_from(*i).unwrap_or(0),
            _ => 0,
        })
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let want = (query.offset as usize + query.limit as usize).min(SCAN_CAP);
        let vals = flatten_committed(self.select(&select_sql(&table.name, &query.filters), want).await?);
        let mut rs = values_to_result(&vals, &[ID]);
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        rs.rows = local::page(&names, rs.rows, &PageQuery { sort: query.sort.clone(), filters: vec![], offset: query.offset, limit: query.limit });
        rs.truncated = vals.len() >= SCAN_CAP;
        Ok(rs)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let statements: Vec<String> = crate::guard::destructive::split_statements(sql).into_iter().map(|s| s.trim().trim_end_matches(';').trim().to_string()).filter(|s| !s.is_empty()).collect();
        if statements.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        let writes = statements.iter().any(|s| is_write(s));
        if writes && self.read_only {
            return Err(AppError::read_only("This connection is read-only; INSERT/UPDATE/DELETE/CREATE/DROP are blocked."));
        }
        let refs: Vec<&str> = statements.iter().map(String::as_str).collect();
        let results = self.transaction(&refs, max_rows, writes).await?;
        Ok(statements
            .iter()
            .zip(results)
            .map(|(stmt, vals)| {
                if is_write(stmt) {
                    StatementResult::Affected { rows_affected: vals.len() as u64 }
                } else {
                    let truncated = vals.len() >= max_rows;
                    let mut rs = values_to_result(&vals, &[]);
                    rs.truncated = truncated;
                    StatementResult::Rows { result: rs }
                }
            })
            .collect())
    }

    async fn close(&self) {
        let token = self.session.lock().await.take();
        if let Some(t) = token {
            let _ = self.send(serde_json::json!({"SessionToken": t, "EndSession": {}})).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BVM: [u8; 4] = [0xE0, 0x01, 0x00, 0xEA];

    fn datagram(body: &[u8]) -> Vec<u8> {
        let mut v = BVM.to_vec();
        v.extend_from_slice(body);
        v
    }

    fn one(body: &[u8]) -> Ion {
        decode_datagram(&datagram(body)).unwrap_or_default().into_iter().next().unwrap_or(Ion::Null("missing"))
    }

    #[test]
    fn sha256_known_answer() {
        assert_eq!(hex::encode(sha256(b"abc")), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assert_eq!(hex::encode(sha256(b"")), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        let long = "a".repeat(1000);
        assert_eq!(hex::encode(sha256(long.as_bytes())), "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3");
    }

    #[test]
    fn scalars_decode() {
        assert_eq!(one(&[0x0F]), Ion::Null("null"));
        assert_eq!(one(&[0x1F]), Ion::Null("bool"));
        assert_eq!(one(&[0x11]), Ion::Bool(true));
        assert_eq!(one(&[0x10]), Ion::Bool(false));
        assert_eq!(one(&[0x20]), Ion::Int(0));
        assert_eq!(one(&[0x21, 0x05]), Ion::Int(5));
        assert_eq!(one(&[0x22, 0x01, 0x00]), Ion::Int(256));
        assert_eq!(one(&[0x31, 0x03]), Ion::Int(-3));
        assert_eq!(one(&[0x40]), Ion::Float(0.0));
        assert_eq!(one(&[0x48, 0x3F, 0xF0, 0, 0, 0, 0, 0, 0]), Ion::Float(1.0));
        assert_eq!(one(&[0x44, 0x3F, 0x80, 0, 0]), Ion::Float(1.0));
        assert_eq!(one(&[0x83, b'a', b'b', b'c']), Ion::String("abc".into()));
        assert_eq!(one(&[0x80]), Ion::String(String::new()));
        assert_eq!(one(&[0xA2, 0x01, 0x02]), Ion::Blob(vec![1, 2]));
        assert_eq!(one(&[0x92, b'h', b'i']), Ion::Clob(b"hi".to_vec()));
        assert_eq!(one(&[0x71, 0x04]), Ion::Symbol("name".into()));
        assert_eq!(one(&[0x71, 0x63]), Ion::Symbol("$99".into()));
    }

    #[test]
    fn decimals_and_timestamps() {
        assert_eq!(one(&[0x52, 0xC2, 0x7D]), Ion::Decimal("1.25".into()));
        assert_eq!(one(&[0x52, 0x82, 0x07]), Ion::Decimal("700".into()));
        assert_eq!(one(&[0x52, 0xC3, 0x05]), Ion::Decimal("0.005".into()));
        assert_eq!(one(&[0x52, 0xC1, 0x85]), Ion::Decimal("-0.5".into()));
        assert_eq!(one(&[0x50]), Ion::Decimal("0".into()));
        // 2024-01-15T10:30:00Z
        assert_eq!(one(&[0x68, 0x80, 0x0F, 0xE8, 0x81, 0x8F, 0x8A, 0x9E, 0x80]), Ion::Timestamp("2024-01-15T10:30:00Z".into()));
        // 2024-01-15T10:30:00.123Z (fraction exponent -3, coefficient 123)
        assert_eq!(one(&[0x6A, 0x80, 0x0F, 0xE8, 0x81, 0x8F, 0x8A, 0x9E, 0x80, 0xC3, 0x7B]), Ion::Timestamp("2024-01-15T10:30:00.123Z".into()));
        // 2024T (year only)
        assert_eq!(one(&[0x63, 0x80, 0x0F, 0xE8]), Ion::Timestamp("2024T".into()));
        // 2024-01-15 with unknown offset
        assert_eq!(one(&[0x65, 0xC0, 0x0F, 0xE8, 0x81, 0x8F]), Ion::Timestamp("2024-01-15".into()));
        // +05:30 offset (330 minutes): VarInt 330 = 0x02 0xCA
        assert_eq!(one(&[0x69, 0x02, 0xCA, 0x0F, 0xE8, 0x81, 0x8F, 0x8A, 0x9E, 0x80]), Ion::Timestamp("2024-01-15T10:30:00+05:30".into()));
    }

    #[test]
    fn containers_decode() {
        assert_eq!(one(&[0xB4, 0x21, 0x01, 0x81, b'a']), Ion::List(vec![Ion::Int(1), Ion::String("a".into())]));
        assert_eq!(one(&[0xB0]), Ion::List(vec![]));
        assert_eq!(one(&[0xC2, 0x21, 0x02]), Ion::Sexp(vec![Ion::Int(2)]));
        // struct { name(4): "x" } using the system symbol `name`
        assert_eq!(one(&[0xD3, 0x84, 0x81, b'x']), Ion::Struct(vec![("name".into(), Ion::String("x".into()))]));
        // struct with L=14 length prefix
        assert_eq!(one(&[0xDE, 0x83, 0x84, 0x81, b'x']), Ion::Struct(vec![("name".into(), Ion::String("x".into()))]));
        // NOP padding then value
        assert_eq!(one(&[0x01, 0x00, 0x21, 0x07]), Ion::Int(7));
    }

    #[test]
    fn local_symbol_table_resolves_field_names() {
        // $ion_symbol_table::{ symbols: ["name", "age"] }
        let mut body = vec![0xEE, 0x8E, 0x81, 0x83, 0xDB, 0x87, 0xB9, 0x84, b'n', b'a', b'm', b'e', 0x83, b'a', b'g', b'e'];
        // { $10: "Bob", $11: 42 }
        body.extend_from_slice(&[0xD8, 0x8A, 0x83, b'B', b'o', b'b', 0x8B, 0x21, 0x2A]);
        // symbol $11
        body.extend_from_slice(&[0x71, 0x0B]);
        let vals = decode_datagram(&datagram(&body)).unwrap_or_default();
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], Ion::Struct(vec![("name".into(), Ion::String("Bob".into())), ("age".into(), Ion::Int(42))]));
        assert_eq!(vals[1], Ion::Symbol("age".into()));
        // A second BVM resets the table.
        let mut two = datagram(&body);
        two.extend_from_slice(&datagram(&[0x71, 0x0A]));
        let vals = decode_datagram(&two).unwrap_or_default();
        assert_eq!(vals[2], Ion::Symbol("$10".into()));
    }

    #[test]
    fn appended_symbol_table() {
        // first table: struct { symbols: ["a"] } = D4 87 B2 81 'a' (5 bytes) → wrapper body 81 83 + 5 = 7
        let first = [0xEE, 0x87, 0x81, 0x83, 0xD4, 0x87, 0xB2, 0x81, b'a'];
        // second: struct { imports: $ion_symbol_table (71 03), symbols: ["b"] } = D7 86 71 03 87 B2 81 'b' (8 bytes) → wrapper 81 83 + 8 = 10
        let second = [0xEE, 0x8A, 0x81, 0x83, 0xD7, 0x86, 0x71, 0x03, 0x87, 0xB2, 0x81, b'b'];
        let mut body = first.to_vec();
        body.extend_from_slice(&second);
        body.extend_from_slice(&[0x71, 0x0A, 0x71, 0x0B]);
        let vals = decode_datagram(&datagram(&body)).unwrap_or_default();
        assert_eq!(vals, vec![Ion::Symbol("a".into()), Ion::Symbol("b".into())]);
    }

    #[test]
    fn annotated_values_unwrap() {
        // foo::5 where foo is $10 via a symbol table would need a table; use system symbol `name`::5
        assert_eq!(one(&[0xE4, 0x81, 0x84, 0x21, 0x05]), Ion::Int(5));
    }

    #[test]
    fn values_become_rows() {
        let vals = vec![
            Ion::Struct(vec![("_id".into(), Ion::String("k1".into())), ("n".into(), Ion::Int(1))]),
            Ion::Struct(vec![("_id".into(), Ion::String("k2".into())), ("tags".into(), Ion::List(vec![Ion::Symbol("x".into())]))]),
        ];
        let rs = values_to_result(&vals, &["_id"]);
        assert_eq!(rs.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["_id", "n", "tags"]);
        assert_eq!(rs.columns[1].type_name, "int");
        assert_eq!(rs.rows[1][1], Value::Null);
        assert_eq!(rs.rows[1][2], Value::Json(serde_json::json!(["x"])));
        let scalar = values_to_result(&[Ion::Int(3)], &[]);
        assert_eq!(scalar.columns[0].name, "value");
        assert_eq!(scalar.rows[0][0], Value::Int(3));
    }

    #[test]
    fn hash_chain_is_deterministic_and_order_independent_in_dot() {
        let a = ion_hash_string("abc");
        let b = ion_hash_string("xyz");
        assert_eq!(qldb_dot(&a, &b), qldb_dot(&b, &a));
        assert_ne!(commit_digest("txn1", &["SELECT 1"]), commit_digest("txn1", &["SELECT 2"]));
        assert_eq!(commit_digest("txn1", &["SELECT 1"]), commit_digest("txn1", &["SELECT 1"]));
        // Ion-hash serialization of the string "a" is 0B 80 61 0E; escape bytes get prefixed.
        assert_eq!(ion_hash_string("a"), sha256(&[0x0B, 0x80, 0x61, 0x0E]));
        assert_eq!(ion_hash_string("\u{0B}"), sha256(&[0x0B, 0x80, 0x0C, 0x0B, 0x0E]));
    }

    #[test]
    fn partiql_builders() {
        let w = where_clause(&[
            FilterRule { column: "age".into(), op: FilterOp::Gte, value: "18".into() },
            FilterRule { column: "name".into(), op: FilterOp::Contains, value: "o'b".into() },
            FilterRule { column: "_id".into(), op: FilterOp::Eq, value: "abc".into() },
        ]);
        assert_eq!(w, " WHERE data.\"age\" >= 18 AND data.\"name\" LIKE '%o''b%' AND metadata.id = 'abc'");
        assert_eq!(select_sql("People", &[]), "SELECT metadata.id AS \"_id\", data FROM \"_ql_committed_People\"");
        let flat = flatten_committed(vec![Ion::Struct(vec![("_id".into(), Ion::String("k".into())), ("data".into(), Ion::Struct(vec![("n".into(), Ion::Int(1))]))])]);
        assert_eq!(flat, vec![Ion::Struct(vec![("_id".into(), Ion::String("k".into())), ("n".into(), Ion::Int(1))])]);
        assert!(is_write("INSERT INTO t VALUE {}"));
        assert!(is_write("FROM t WHERE x = 1 DELETE"));
        assert!(!is_write("SELECT * FROM t"));
    }
}
