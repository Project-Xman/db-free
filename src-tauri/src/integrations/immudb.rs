// SOT: immudb-integration, immudb-rest-api, immudb-sql, immudb-kv, immudb-login

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, local, HttpClient};
use crate::integrations::sql::{order_clause, validate_columns, where_clause};
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    SslMode, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  immudb adapter over its REST gateway (immugw / the embedded HTTP API,
//        port 3323, base path `/api`).
// WHY:   immudb is both a SQL database and an immutable key-value store. SQL
//        tables map 1:1 to the grid; the KV side is exposed as a `kv` schema
//        with one `keys` table (key, value, tx) fed by a bounded prefix scan.
// HOW:   POST /api/login {user, password} (base64) → Bearer token, then
//        POST /api/db/use {databaseName} → per-database token. Reads go to
//        POST /api/db/sqlquery, writes to POST /api/db/sqlexec (refused when
//        read-only). Tables via `SELECT * FROM TABLES()` (immudb ≥ 1.3) with a
//        fallback to GET /api/db/tables; columns via `SELECT * FROM COLUMNS('t')`
//        or POST /api/db/tables/{t}. Typed cells (`{n}`, `{s}`, `{b}`, `{bs}`,
//        `{ts}`, `{f}`, `{null}`) decode to model values. `execute` also takes
//        shorthand KV commands: `GET key`, `SET key value`, `HISTORY key`,
//        `VERIFIED GET key`, `SCAN prefix`.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs (trait)
// ============================================================================

const DEFAULT_PORT: u16 = 3323;
const DEFAULT_DATABASE: &str = "defaultdb";
const DEFAULT_USER: &str = "immudb";
const KV_SCHEMA: &str = "kv";
const KV_TABLE: &str = "keys";
const MAX_KV_ROWS: usize = 5_000;

pub struct ImmudbIntegration {
    engine: Engine,
    http: HttpClient,
    database: String,
    read_only: bool,
}

fn b64(raw: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(raw)
}

fn unb64(raw: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(raw).unwrap_or_else(|_| raw.as_bytes().to_vec())
}

fn unb64_text(raw: &str) -> Value {
    let bytes = unb64(raw);
    match String::from_utf8(bytes) {
        Ok(s) if !s.chars().any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t')) => Value::Text(s),
        _ => Value::Bytes(raw.to_string()),
    }
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty()).unwrap_or(DEFAULT_USER);
    let password = conn.secret.as_deref().unwrap_or("");
    let base = base_url(conn, Some(DEFAULT_PORT), false);
    let insecure = s.ssl_mode == SslMode::Require;
    let anon = HttpClient::new(&base, crate::integrations::http::Auth::None, insecure)?;
    let login: Json = anon
        .post_json("/api/login", &json!({ "user": b64(user.as_bytes()), "password": b64(password.as_bytes()) }))
        .await
        .map_err(|e| match e {
            AppError::Driver { message } if message.contains("invalid user") || message.contains("password") => AppError::not_connected(message),
            other => other,
        })?;
    let token = login
        .get("token")
        .and_then(Json::as_str)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| AppError::not_connected("immudb login did not return a token."))?
        .to_string();
    let session = HttpClient::new(&base, crate::integrations::http::Auth::Bearer(token), insecure)?;
    // Selecting the database returns a database-scoped token on immudb 1.x.
    let use_db: Json = session.post_json("/api/db/use", &json!({ "databaseName": database })).await?;
    let http = match use_db.get("token").and_then(Json::as_str).filter(|t| !t.is_empty()) {
        Some(t) => HttpClient::new(&base, crate::integrations::http::Auth::Bearer(t.to_string()), insecure)?,
        None => session,
    };
    Ok(Arc::new(ImmudbIntegration { engine: s.engine, http, database, read_only: s.read_only }))
}

// ---------------------------------------------------------------------------
// SQL result decoding
// ---------------------------------------------------------------------------

// WHAT:  immudb names columns `(db.table.col)` or `(table.col)`; keep the last segment.
// WHAT:  immudb labels result columns "(database.table.column)"; the grid wants
//        just "column". Expressions like COUNT(*) are left alone.
fn short_column_name(raw: &str) -> String {
    let trimmed = raw.trim();
    let Some(inner) = trimmed.strip_prefix('(').and_then(|r| r.strip_suffix(')')) else {
        return trimmed.to_string();
    };
    // Only a plain dotted path is unwrapped; anything with nested parens is an expression.
    if inner.contains('(') || inner.contains(')') {
        return trimmed.to_string();
    }
    inner.rsplit('.').next().unwrap_or(inner).to_string()
}

fn type_name(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

// WHAT:  A typed cell `{n|s|b|bs|ts|f|null: …}` → model value.
fn decode_cell(cell: &Json) -> Value {
    let Some(obj) = cell.as_object() else {
        return match cell {
            Json::Null => Value::Null,
            Json::Bool(b) => Value::Bool(*b),
            Json::Number(n) => n.as_i64().map(Value::Int).unwrap_or_else(|| Value::Float(n.as_f64().unwrap_or(0.0))),
            Json::String(s) => Value::Text(s.clone()),
            other => Value::Json(other.clone()),
        };
    };
    if obj.contains_key("null") {
        return Value::Null;
    }
    if let Some(n) = obj.get("n") {
        return match n {
            Json::String(s) => s.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Decimal(s.clone())),
            Json::Number(x) => x.as_i64().map(Value::Int).unwrap_or_else(|| Value::Float(x.as_f64().unwrap_or(0.0))),
            other => Value::Text(other.to_string()),
        };
    }
    if let Some(s) = obj.get("s") {
        return Value::Text(s.as_str().map(str::to_string).unwrap_or_else(|| s.to_string()));
    }
    if let Some(b) = obj.get("b") {
        return b.as_bool().map(Value::Bool).unwrap_or(Value::Null);
    }
    if let Some(bs) = obj.get("bs").and_then(Json::as_str) {
        return Value::Bytes(bs.to_string());
    }
    if let Some(f) = obj.get("f") {
        return match f {
            Json::Number(x) => Value::Float(x.as_f64().unwrap_or(0.0)),
            Json::String(s) => s.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::Text(s.clone())),
            other => Value::Text(other.to_string()),
        };
    }
    if let Some(ts) = obj.get("ts") {
        let micros = match ts {
            Json::String(s) => s.parse::<i64>().ok(),
            Json::Number(x) => x.as_i64(),
            _ => None,
        };
        return match micros.and_then(chrono::DateTime::from_timestamp_micros) {
            Some(dt) => Value::DateTime(dt.to_rfc3339()),
            None => Value::Text(ts.to_string()),
        };
    }
    Value::Json(cell.clone())
}

// WHAT:  `{columns:[{name,type}], rows:[{columns:[…], values:[…]}]}` → grid.
fn query_to_result_set(resp: &Json, max_rows: usize) -> ResultSet {
    let mut columns: Vec<ColumnMeta> = resp
        .get("columns")
        .and_then(Json::as_array)
        .map(|a| {
            a.iter()
                .map(|c| ColumnMeta {
                    name: short_column_name(c.get("name").and_then(Json::as_str).unwrap_or("?")),
                    type_name: type_name(c.get("type").and_then(Json::as_str).unwrap_or("")),
                })
                .collect()
        })
        .unwrap_or_default();
    let rows_json = resp.get("rows").and_then(Json::as_array).cloned().unwrap_or_default();
    if columns.is_empty() {
        if let Some(first) = rows_json.first() {
            columns = first
                .get("columns")
                .and_then(Json::as_array)
                .map(|a| a.iter().filter_map(Json::as_str).map(|n| ColumnMeta { name: short_column_name(n), type_name: String::new() }).collect())
                .unwrap_or_default();
        }
    }
    let truncated = rows_json.len() > max_rows;
    let rows = rows_json
        .iter()
        .take(max_rows)
        .map(|r| {
            let values = r.get("values").and_then(Json::as_array).cloned().unwrap_or_default();
            let mut row: Vec<Value> = values.iter().map(decode_cell).collect();
            row.resize(columns.len(), Value::Null);
            row
        })
        .collect();
    ResultSet { columns, rows, truncated }
}

fn first_word(sql: &str) -> String {
    sql.trim_start().split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("").to_ascii_uppercase()
}

fn is_read_sql(sql: &str) -> bool {
    matches!(first_word(sql).as_str(), "SELECT" | "WITH" | "SHOW" | "EXPLAIN")
}

// WHAT:  Splits a script on `;` outside quotes.
fn split_sql(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    for ch in text.chars() {
        match ch {
            '\'' => {
                in_str = !in_str;
                cur.push(ch);
            }
            ';' if !in_str => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

// ---------------------------------------------------------------------------
// KV shorthand
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum KvCommand {
    Get(String),
    VerifiedGet(String),
    Set(String, String),
    History(String),
    Scan(String),
}

// WHAT:  `GET k`, `VERIFIED GET k`, `SET k v`, `HISTORY k`, `SCAN prefix` (quotes optional).
fn parse_kv(text: &str) -> Option<KvCommand> {
    let t = text.trim();
    let unquote = |s: &str| s.trim().trim_matches(|c| c == '\'' || c == '"').to_string();
    let mut parts = t.splitn(2, char::is_whitespace);
    let verb = parts.next()?.to_ascii_uppercase();
    let rest = parts.next().unwrap_or("").trim();
    match verb.as_str() {
        "GET" if !rest.is_empty() => Some(KvCommand::Get(unquote(rest))),
        "HISTORY" if !rest.is_empty() => Some(KvCommand::History(unquote(rest))),
        "SCAN" => Some(KvCommand::Scan(unquote(rest))),
        "VERIFIED" => {
            let rest_up = rest.to_ascii_uppercase();
            let key = rest_up.strip_prefix("GET").map(|_| rest[3..].trim())?;
            (!key.is_empty()).then(|| KvCommand::VerifiedGet(unquote(key)))
        }
        "SET" => {
            let (k, v) = rest.split_once(char::is_whitespace)?;
            Some(KvCommand::Set(unquote(k), unquote(v)))
        }
        _ => None,
    }
}

fn kv_columns() -> Vec<ColumnInfo> {
    [("key", "string", true), ("value", "string", false), ("tx", "integer", false)]
        .iter()
        .enumerate()
        .map(|(i, (n, t, pk))| ColumnInfo { name: (*n).into(), data_type: (*t).into(), nullable: !pk, primary_key: *pk, ordinal: i as u32 + 1 })
        .collect()
}

fn kv_entry_row(e: &Json) -> Vec<Value> {
    let key = e.get("key").and_then(Json::as_str).map(unb64_text).unwrap_or(Value::Null);
    let value = e.get("value").and_then(Json::as_str).map(unb64_text).unwrap_or(Value::Null);
    let tx = e.get("tx").map(|t| match t {
        Json::String(s) => s.parse::<i64>().map(Value::Int).unwrap_or(Value::Text(s.clone())),
        Json::Number(n) => n.as_i64().map(Value::Int).unwrap_or(Value::Null),
        _ => Value::Null,
    });
    vec![key, value, tx.unwrap_or(Value::Null)]
}

fn kv_result_set(entries: &[Json], truncated: bool) -> ResultSet {
    ResultSet {
        columns: kv_columns().into_iter().map(|c| ColumnMeta { name: c.name, type_name: c.data_type }).collect(),
        rows: entries.iter().map(kv_entry_row).collect(),
        truncated,
    }
}

fn is_kv_table(table: &TableRef) -> bool {
    table.schema.as_deref() == Some(KV_SCHEMA) && table.name == KV_TABLE
}

impl ImmudbIntegration {
    async fn query(&self, sql: &str) -> AppResult<Json> {
        self.http.post_json("/api/db/sqlquery", &json!({ "sql": sql, "params": [] })).await
    }

    async fn exec(&self, sql: &str) -> AppResult<Json> {
        self.http.post_json("/api/db/sqlexec", &json!({ "sql": sql, "params": [], "noWait": false })).await
    }

    async fn scan(&self, prefix: &str, limit: usize) -> AppResult<Vec<Json>> {
        let body = json!({ "prefix": b64(prefix.as_bytes()), "limit": limit, "desc": false });
        let v: Json = self.http.post_json("/api/db/scan", &body).await?;
        Ok(v.get("entries").and_then(Json::as_array).cloned().unwrap_or_default())
    }

    async fn table_names(&self) -> AppResult<Vec<String>> {
        if let Ok(v) = self.query("SELECT * FROM TABLES()").await {
            let rs = query_to_result_set(&v, 10_000);
            let idx = rs.columns.iter().position(|c| c.name.eq_ignore_ascii_case("name")).unwrap_or(0);
            let names: Vec<String> = rs.rows.iter().filter_map(|r| match r.get(idx) {
                Some(Value::Text(s)) => Some(s.clone()),
                _ => None,
            }).collect();
            if !names.is_empty() {
                return Ok(names);
            }
        }
        let v: Json = self.http.get_json("/api/db/tables").await?;
        let rs = query_to_result_set(&v, 10_000);
        Ok(rs.rows.iter().filter_map(|r| match r.first() {
            Some(Value::Text(s)) => Some(s.clone()),
            _ => None,
        }).collect())
    }

    async fn sql_columns(&self, table: &str) -> AppResult<Vec<ColumnInfo>> {
        let rs = match self.query(&format!("SELECT * FROM COLUMNS({})", crate::integrations::sql::quote_literal(table))).await {
            Ok(v) => query_to_result_set(&v, 10_000),
            Err(_) => {
                let v: Json = self.http.post_json(&format!("/api/db/tables/{table}"), &json!({})).await?;
                query_to_result_set(&v, 10_000)
            }
        };
        let idx = |name: &str| rs.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name));
        let (i_name, i_type, i_null, i_pk) = (idx("name").or_else(|| idx("column")), idx("type"), idx("nullable"), idx("primary_key").or_else(|| idx("primary key")));
        let text = |row: &Vec<Value>, i: Option<usize>| -> String {
            match i.and_then(|i| row.get(i)) {
                Some(Value::Text(s)) => s.clone(),
                Some(Value::Bool(b)) => b.to_string(),
                Some(other) => format!("{other:?}"),
                None => String::new(),
            }
        };
        let flag = |row: &Vec<Value>, i: Option<usize>| matches!(i.and_then(|i| row.get(i)), Some(Value::Bool(true))) || text(row, i).eq_ignore_ascii_case("true");
        let cols: Vec<ColumnInfo> = rs
            .rows
            .iter()
            .enumerate()
            .filter_map(|(n, row)| {
                let name = text(row, i_name.or(Some(0)));
                if name.is_empty() {
                    return None;
                }
                Some(ColumnInfo {
                    name,
                    data_type: text(row, i_type).to_ascii_lowercase(),
                    nullable: i_null.map(|_| flag(row, i_null)).unwrap_or(true),
                    primary_key: flag(row, i_pk),
                    ordinal: n as u32 + 1,
                })
            })
            .collect();
        if cols.is_empty() {
            return Err(AppError::not_found(format!("Table `{table}` has no columns or does not exist.")));
        }
        Ok(cols)
    }

    fn table_ident(table: &TableRef) -> String {
        quote_ident(&table.name)
    }
}

#[async_trait]
impl Integration for ImmudbIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { transactions: false, views: false, exact_estimate: true, ..Capabilities::SQL }
    }

    async fn ping(&self) -> AppResult<()> {
        self.query("SELECT 1").await.map_err(|e| match e {
            AppError::NotConnected { .. } => e,
            other => AppError::not_connected(format!("immudb did not answer a trivial query: {}", other.message())),
        })?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        for path in ["/api/serverinfo", "/api/health"] {
            if let Ok(v) = self.http.get_json::<Json>(path).await {
                if let Some(ver) = v.get("version").and_then(Json::as_str) {
                    return Ok(Some(format!("immudb {ver}")));
                }
            }
        }
        Ok(Some("immudb".into()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let v: Json = match self.http.get_json("/api/db/list").await {
            Ok(v) => v,
            Err(_) => match self.http.post_json("/api/db/databaselist/v2", &json!({})).await {
                Ok(v) => v,
                Err(_) => return Ok(vec![self.database.clone()]),
            },
        };
        let mut names: Vec<String> = v
            .get("databases")
            .and_then(Json::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|d| d.get("databaseName").or_else(|| d.get("name")).and_then(Json::as_str).map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if names.is_empty() {
            names.push(self.database.clone());
        }
        names.sort();
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let names = self.table_names().await?;
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let row_estimate = self
                .query(&format!("SELECT COUNT(*) FROM {}", quote_ident(&name)))
                .await
                .ok()
                .map(|v| query_to_result_set(&v, 1))
                .and_then(|rs| rs.rows.first().and_then(|r| r.first()).and_then(|v| match v {
                    Value::Int(i) => Some(*i),
                    _ => None,
                }));
            tables.push(TableInfo { schema: Some(self.database.clone()), name, kind: TableKind::Table, row_estimate });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog {
            schemas: vec![
                SchemaInfo { name: self.database.clone(), tables },
                SchemaInfo {
                    name: KV_SCHEMA.into(),
                    tables: vec![TableInfo { schema: Some(KV_SCHEMA.into()), name: KV_TABLE.into(), kind: TableKind::View, row_estimate: None }],
                },
            ],
        })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        if is_kv_table(table) {
            return Ok(kv_columns());
        }
        self.sql_columns(&table.name).await
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        if is_kv_table(table) {
            return Ok(None);
        }
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        if is_kv_table(table) {
            let entries = self.scan("", MAX_KV_ROWS).await?;
            let names: Vec<String> = kv_columns().into_iter().map(|c| c.name).collect();
            let rows: Vec<Vec<Value>> = entries.iter().map(kv_entry_row).collect();
            return Ok(local::apply_filters(&names, rows, filters).len() as i64);
        }
        let sql = format!("SELECT COUNT(*) FROM {}{}", Self::table_ident(table), where_clause(self.engine, filters));
        let v = self.query(&sql).await?;
        let rs = query_to_result_set(&v, 1);
        Ok(match rs.rows.first().and_then(|r| r.first()) {
            Some(Value::Int(i)) => *i,
            _ => 0,
        })
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        if is_kv_table(table) {
            let prefix = query
                .filters
                .iter()
                .find(|f| f.column == "key" && matches!(f.op, crate::model::FilterOp::StartsWith))
                .map(|f| f.value.trim().to_string())
                .unwrap_or_default();
            let entries = self.scan(&prefix, MAX_KV_ROWS).await?;
            let truncated = entries.len() >= MAX_KV_ROWS;
            let names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
            let rows: Vec<Vec<Value>> = entries.iter().map(kv_entry_row).collect();
            let rows = local::page(&names, rows, query);
            let mut rs = kv_result_set(&[], truncated);
            rs.rows = rows;
            return Ok(rs);
        }
        let sql = format!(
            "SELECT * FROM {}{}{} LIMIT {} OFFSET {}",
            Self::table_ident(table),
            where_clause(self.engine, &query.filters),
            order_clause(self.engine, &query.sort),
            query.limit,
            query.offset
        );
        let v = self.query(&sql).await?;
        Ok(query_to_result_set(&v, query.limit as usize))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let max_rows = max_rows.max(1);
        let text = sql.trim();
        if text.is_empty() {
            return Err(AppError::invalid_input("Empty statement."));
        }
        if let Some(cmd) = parse_kv(text) {
            return self.run_kv(cmd, max_rows).await.map(|r| vec![r]);
        }
        let mut out = Vec::new();
        for stmt in split_sql(text) {
            if is_read_sql(&stmt) {
                let v = self.query(&stmt).await?;
                out.push(StatementResult::Rows { result: query_to_result_set(&v, max_rows) });
            } else {
                if self.read_only {
                    return Err(AppError::read_only(format!("This connection is read-only; `{}` is blocked.", first_word(&stmt))));
                }
                let v = self.exec(&stmt).await?;
                let affected = v
                    .get("txs")
                    .and_then(Json::as_array)
                    .map(|txs| {
                        txs.iter()
                            .map(|t| {
                                t.get("updatedRows")
                                    .and_then(|u| u.as_u64().or_else(|| u.as_str().and_then(|s| s.parse().ok())))
                                    .unwrap_or_else(|| {
                                        t.get("lastInsertedPKs").and_then(Json::as_object).map(|m| m.len() as u64).unwrap_or(0)
                                    })
                            })
                            .sum()
                    })
                    .unwrap_or(0);
                out.push(StatementResult::Affected { rows_affected: affected });
            }
        }
        Ok(out)
    }

    async fn close(&self) {}
}

impl ImmudbIntegration {
    async fn run_kv(&self, cmd: KvCommand, max_rows: usize) -> AppResult<StatementResult> {
        match cmd {
            KvCommand::Get(key) => {
                let v: Json = self.http.post_json("/api/db/get", &json!({ "key": b64(key.as_bytes()) })).await?;
                Ok(StatementResult::Rows { result: kv_result_set(&[v], false) })
            }
            KvCommand::VerifiedGet(key) => {
                let body = json!({ "keyRequest": { "key": b64(key.as_bytes()) } });
                let v: Json = self.http.post_json("/api/db/verified/get", &body).await?;
                let entry = v.get("entry").cloned().unwrap_or(v.clone());
                let mut rs = kv_result_set(&[entry], false);
                rs.columns.push(ColumnMeta { name: "verified".into(), type_name: "boolean".into() });
                for row in &mut rs.rows {
                    row.push(Value::Bool(v.get("verifiableTx").is_some()));
                }
                Ok(StatementResult::Rows { result: rs })
            }
            KvCommand::Set(key, value) => {
                if self.read_only {
                    return Err(AppError::read_only("This connection is read-only; SET is blocked."));
                }
                let body = json!({ "KVs": [{ "key": b64(key.as_bytes()), "value": b64(value.as_bytes()) }] });
                let _: Json = self.http.post_json("/api/db/set", &body).await?;
                Ok(StatementResult::Affected { rows_affected: 1 })
            }
            KvCommand::History(key) => {
                let body = json!({ "key": b64(key.as_bytes()), "limit": max_rows.min(1_000), "offset": 0, "desc": true });
                let v: Json = self.http.post_json("/api/db/history", &body).await?;
                let entries = v.get("entries").and_then(Json::as_array).cloned().unwrap_or_default();
                Ok(StatementResult::Rows { result: kv_result_set(&entries, false) })
            }
            KvCommand::Scan(prefix) => {
                let entries = self.scan(&prefix, max_rows.min(MAX_KV_ROWS)).await?;
                let truncated = entries.len() >= max_rows.min(MAX_KV_ROWS);
                Ok(StatementResult::Rows { result: kv_result_set(&entries, truncated) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cells_decode_by_type() {
        assert_eq!(decode_cell(&json!({"n": "42"})), Value::Int(42));
        assert_eq!(decode_cell(&json!({"s": "hi"})), Value::Text("hi".into()));
        assert_eq!(decode_cell(&json!({"b": true})), Value::Bool(true));
        assert_eq!(decode_cell(&json!({"null": "NULL_VALUE"})), Value::Null);
        assert_eq!(decode_cell(&json!({"bs": "AQI="})), Value::Bytes("AQI=".into()));
        assert_eq!(decode_cell(&json!({"f": 1.5})), Value::Float(1.5));
        assert!(matches!(decode_cell(&json!({"ts": "1700000000000000"})), Value::DateTime(_)));
        assert_eq!(short_column_name("(defaultdb.t.id)"), "id");
        assert_eq!(short_column_name("(t.name)"), "name");
        assert_eq!(short_column_name("COUNT(*)"), "COUNT(*)");
    }

    #[test]
    fn query_response_becomes_grid() {
        let resp = json!({
            "columns": [{"name": "(defaultdb.t.id)", "type": "INTEGER"}, {"name": "(defaultdb.t.name)", "type": "VARCHAR"}],
            "rows": [
                {"columns": ["(defaultdb.t.id)", "(defaultdb.t.name)"], "values": [{"n": "1"}, {"s": "a"}]},
                {"columns": ["(defaultdb.t.id)", "(defaultdb.t.name)"], "values": [{"n": "2"}, {"null": "NULL_VALUE"}]}
            ]
        });
        let rs = query_to_result_set(&resp, 10);
        assert_eq!(rs.columns[0].name, "id");
        assert_eq!(rs.columns[1].type_name, "varchar");
        assert_eq!(rs.rows[0][0], Value::Int(1));
        assert_eq!(rs.rows[1][1], Value::Null);
        assert!(query_to_result_set(&resp, 1).truncated);
    }

    #[test]
    fn kv_commands_parse() {
        assert_eq!(parse_kv("GET foo"), Some(KvCommand::Get("foo".into())));
        assert_eq!(parse_kv("get 'my key'"), Some(KvCommand::Get("my key".into())));
        assert_eq!(parse_kv("SET k some value"), Some(KvCommand::Set("k".into(), "some value".into())));
        assert_eq!(parse_kv("VERIFIED GET k"), Some(KvCommand::VerifiedGet("k".into())));
        assert_eq!(parse_kv("history k"), Some(KvCommand::History("k".into())));
        assert_eq!(parse_kv("SCAN"), Some(KvCommand::Scan(String::new())));
        assert_eq!(parse_kv("SELECT * FROM t"), None);
        assert_eq!(parse_kv("SET onlykey"), None);
    }

    #[test]
    fn sql_split_and_classification() {
        let parts = split_sql("CREATE TABLE t(id INTEGER, PRIMARY KEY id); INSERT INTO t(id) VALUES (1); SELECT 'a;b'");
        assert_eq!(parts.len(), 3);
        assert!(is_read_sql("  select 1"));
        assert!(!is_read_sql("UPSERT INTO t(id) VALUES (1)"));
        let row = kv_entry_row(&json!({"key": b64(b"k"), "value": b64(b"v"), "tx": "7"}));
        assert_eq!(row, vec![Value::Text("k".into()), Value::Text("v".into()), Value::Int(7)]);
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        use crate::model::{ConnectionSummary, Environment, FilterOp, SortRule};
        let Ok(url) = std::env::var("DBFREE_TEST_IMMUDB_URL") else { return };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Immudb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: std::env::var("DBFREE_TEST_IMMUDB_DB").ok(),
                username: std::env::var("DBFREE_TEST_IMMUDB_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_IMMUDB_PASSWORD").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        db.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        let _ = db.execute("CREATE TABLE IF NOT EXISTS dbfree_smoke(id INTEGER, name VARCHAR, PRIMARY KEY id)", 10).await.unwrap_or_else(|e| panic!("create: {e}"));
        db.execute("UPSERT INTO dbfree_smoke(id, name) VALUES (1, 'a'); UPSERT INTO dbfree_smoke(id, name) VALUES (2, 'b')", 10)
            .await
            .unwrap_or_else(|e| panic!("upsert: {e}"));
        let cat = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == "dbfree_smoke"), "{cat:?}");
        let t = TableRef { schema: None, name: "dbfree_smoke".into() };
        let cols = db.columns(&t).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "id" && c.primary_key), "{cols:?}");
        let page = db
            .fetch_page(
                &t,
                &PageQuery {
                    sort: vec![SortRule { column: "id".into(), desc: true }],
                    filters: vec![FilterRule { column: "name".into(), op: FilterOp::Eq, value: "b".into() }],
                    offset: 0,
                    limit: 10,
                },
            )
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0][0], Value::Int(2));
        assert_eq!(db.count(&t, &[]).await.unwrap_or_default(), 2);
        db.execute("SET dbfree:k hello", 10).await.unwrap_or_else(|e| panic!("set: {e}"));
        match &db.execute("GET dbfree:k", 10).await.unwrap_or_else(|e| panic!("get: {e}"))[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows[0][1], Value::Text("hello".into())),
            _ => panic!("rows"),
        }
        let kv = TableRef { schema: Some(KV_SCHEMA.into()), name: KV_TABLE.into() };
        let page = db.fetch_page(&kv, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 }).await.unwrap_or_else(|e| panic!("kv page: {e}"));
        assert!(!page.rows.is_empty());
    }
}
