// SOT: mongodb-integration, mongodb-adapter, document-mapping, bson-value-decoding, mongo-command-console, mongo-object-explorer, mongo-server-stats

use crate::error::{AppError, AppResult};
use crate::integrations::http::objects_to_result_set;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats,
    SortRule, SslMode, Stat, StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use futures::TryStreamExt;
use mongodb::bson::{doc, oid::ObjectId, Bson, Document};
use mongodb::options::{ClientOptions, Tls, TlsOptions};
use mongodb::{Client, Collection, Database};
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// WHAT:  MongoDB adapter. A "table" is a collection of the session's database,
//        a "row" is a document flattened to its top-level keys, and `execute`
//        runs raw command documents (the `db.runCommand` surface).
// WHY:   Document stores have no fixed columns; the grid still needs a stable
//        header, so columns are the union of top-level keys across a sample of
//        the collection (`_id` first, marked primary key). Pages align their
//        cells to that same union and append any keys the sample missed.
// HOW:   Filters translate to a `$and` document (`$regex` for text matching,
//        `$in` for lists, `null` / `$ne null` for null checks). `execute` accepts
//        one JSON command document per statement (statements separated by a
//        blank line), plus two shorthands: `find <collection> {filter}` and
//        `count <collection> {filter}`. Only the cursor's first batch is
//        returned (no getMore), capped at `max_rows`. A command may carry
//        `"$db": "<name>"` to run against another database (killOp and the
//        replSet*/listShards family need `admin`; explorer actions target the
//        object's own database); the key is stripped before the driver sees it.
// WHERE: src-tauri/src/integrations/mod.rs (Integration trait), src-tauri/src/model/value.rs
// ============================================================================

impl From<mongodb::error::Error> for AppError {
    fn from(err: mongodb::error::Error) -> Self {
        AppError::driver(err)
    }
}

const SAMPLE_SIZE: i64 = 100;
const DEFAULT_DATABASE: &str = "test";
const ID_FIELD: &str = "_id";

pub struct MongoIntegration {
    client: Client,
    database: String,
}

// WHAT:  Percent-encodes user info so `@`, `:` and `/` in credentials survive the URI.
fn encode_userinfo(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        let keep = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~');
        if keep {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("localhost");
    let port = s.port.unwrap_or(27017);
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let username = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    let userinfo = match (username, conn.secret.as_deref()) {
        (Some(user), Some(secret)) => format!("{}:{}@", encode_userinfo(user), encode_userinfo(secret)),
        (Some(user), None) => format!("{}@", encode_userinfo(user)),
        (None, _) => String::new(),
    };
    // Credentials normally live in `admin`; a database in the path would otherwise be used.
    let query = if username.is_some() { "?authSource=admin" } else { "" };
    let uri = format!("mongodb://{userinfo}{host}:{port}/{database}{query}");
    let mut options = ClientOptions::parse(&uri).await?;
    options.app_name = Some("db-free".to_string());
    options.server_selection_timeout = Some(Duration::from_secs(8));
    options.connect_timeout = Some(Duration::from_secs(8));
    if matches!(s.ssl_mode, SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull) {
        options.tls = Some(Tls::Enabled(TlsOptions::default()));
    }
    let client = Client::with_options(options)?;
    Ok(Arc::new(MongoIntegration { client, database }))
}

// ---------------------------------------------------------------------------
// BSON → model::Value
// ---------------------------------------------------------------------------

fn bson_type_name(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Boolean(_) => "bool",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "javascript",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Timestamp(_) => "timestamp",
        Bson::Binary(_) => "binData",
        Bson::ObjectId(_) => "objectId",
        Bson::DateTime(_) => "date",
        Bson::Symbol(_) => "symbol",
        Bson::Decimal128(_) => "decimal",
        Bson::Undefined => "undefined",
        Bson::MaxKey => "maxKey",
        Bson::MinKey => "minKey",
        Bson::DbPointer(_) => "dbPointer",
    }
}

// WHAT:  Relaxed-JSON view of a BSON value for the inspector (no `$`-wrapped
//        extended JSON: ObjectIds and dates read as plain strings).
fn bson_to_json(value: &Bson) -> serde_json::Value {
    match value {
        Bson::Double(v) => serde_json::Number::from_f64(*v).map(serde_json::Value::Number).unwrap_or(serde_json::Value::Null),
        Bson::String(v) | Bson::Symbol(v) | Bson::JavaScriptCode(v) => serde_json::Value::String(v.clone()),
        Bson::Array(items) => serde_json::Value::Array(items.iter().map(bson_to_json).collect()),
        Bson::Document(document) => serde_json::Value::Object(
            document.iter().map(|(key, inner)| (key.clone(), bson_to_json(inner))).collect(),
        ),
        Bson::Boolean(v) => serde_json::Value::Bool(*v),
        Bson::Null | Bson::Undefined => serde_json::Value::Null,
        Bson::Int32(v) => serde_json::Value::Number((*v).into()),
        Bson::Int64(v) => serde_json::Value::Number((*v).into()),
        Bson::ObjectId(id) => serde_json::Value::String(id.to_hex()),
        Bson::DateTime(dt) => serde_json::Value::String(datetime_text(*dt)),
        Bson::Decimal128(d) => serde_json::Value::String(d.to_string()),
        Bson::Binary(bin) => serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(&bin.bytes)),
        Bson::Timestamp(ts) => serde_json::Value::String(format!("{}:{}", ts.time, ts.increment)),
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

fn datetime_text(dt: mongodb::bson::DateTime) -> String {
    dt.try_to_rfc3339_string().unwrap_or_else(|_| dt.timestamp_millis().to_string())
}

fn bson_to_value(value: &Bson) -> Value {
    match value {
        Bson::Null | Bson::Undefined => Value::Null,
        Bson::Boolean(v) => Value::Bool(*v),
        Bson::Int32(v) => Value::Int(i64::from(*v)),
        Bson::Int64(v) => Value::Int(*v),
        Bson::Double(v) => Value::Float(*v),
        Bson::String(v) | Bson::Symbol(v) | Bson::JavaScriptCode(v) => Value::Text(v.clone()),
        Bson::ObjectId(id) => Value::Text(id.to_hex()),
        Bson::DateTime(dt) => Value::DateTime(datetime_text(*dt)),
        Bson::Decimal128(d) => Value::Decimal(d.to_string()),
        Bson::Binary(bin) => Value::Bytes(base64::engine::general_purpose::STANDARD.encode(&bin.bytes)),
        Bson::Document(_) | Bson::Array(_) => Value::Json(bson_to_json(value)),
        Bson::Timestamp(ts) => Value::Text(format!("{}:{}", ts.time, ts.increment)),
        other => Value::Text(format!("{other:?}")),
    }
}

// ---------------------------------------------------------------------------
// Column inference
// ---------------------------------------------------------------------------

// WHAT:  Union of top-level keys across `docs`, `_id` first, first-seen order.
//        The type is taken from the first non-null value seen for that key.
fn union_columns(docs: &[Document]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = Vec::new();
    let mut types: Vec<Option<&'static str>> = Vec::new();
    let mut push = |name: &str, value: &Bson| {
        let index = match names.iter().position(|n| n == name) {
            Some(i) => i,
            None => {
                names.push(name.to_string());
                types.push(None);
                names.len() - 1
            }
        };
        if types[index].is_none() && !matches!(value, Bson::Null | Bson::Undefined) {
            types[index] = Some(bson_type_name(value));
        }
    };
    for doc in docs {
        if let Some(id) = doc.get(ID_FIELD) {
            push(ID_FIELD, id);
        }
    }
    for doc in docs {
        for (key, value) in doc.iter() {
            push(key, value);
        }
    }
    names
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, ty))| ColumnInfo {
            primary_key: name == ID_FIELD,
            name,
            data_type: ty.unwrap_or("null").to_string(),
            nullable: true,
            ordinal: u32::try_from(i + 1).unwrap_or(u32::MAX),
        })
        .collect()
}

fn rows_for(columns: &[ColumnInfo], docs: &[Document]) -> Vec<Vec<Value>> {
    docs.iter()
        .map(|doc| columns.iter().map(|c| doc.get(&c.name).map(bson_to_value).unwrap_or(Value::Null)).collect())
        .collect()
}

fn metas_for(columns: &[ColumnInfo]) -> Vec<ColumnMeta> {
    columns.iter().map(|c| ColumnMeta { name: c.name.clone(), type_name: c.data_type.clone() }).collect()
}

// ---------------------------------------------------------------------------
// Filter / sort translation
// ---------------------------------------------------------------------------

// WHAT:  Parses a filter value the way a person types it: numbers, booleans,
//        ObjectIds (for `_id`), else a string.
fn lenient_value(column: &str, raw: &str) -> Bson {
    let trimmed = raw.trim();
    if column == ID_FIELD {
        if let Ok(id) = ObjectId::parse_str(trimmed) {
            return Bson::ObjectId(id);
        }
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Bson::Boolean(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Bson::Boolean(false);
    }
    if trimmed.eq_ignore_ascii_case("null") {
        return Bson::Null;
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return Bson::Int64(i);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return Bson::Double(f);
    }
    Bson::String(trimmed.to_string())
}

fn escape_regex(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 4);
    for ch in raw.chars() {
        if "\\^$.|?*+()[]{}/".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

fn regex_predicate(pattern: String) -> Document {
    doc! { "$regex": pattern, "$options": "i" }
}

fn predicate(rule: &FilterRule) -> Document {
    let column = rule.column.as_str();
    let value = rule.value.trim();
    let body: Bson = match rule.op {
        FilterOp::Eq => lenient_value(column, value),
        FilterOp::Ne => Bson::Document(doc! { "$ne": lenient_value(column, value) }),
        FilterOp::Gt => Bson::Document(doc! { "$gt": lenient_value(column, value) }),
        FilterOp::Gte => Bson::Document(doc! { "$gte": lenient_value(column, value) }),
        FilterOp::Lt => Bson::Document(doc! { "$lt": lenient_value(column, value) }),
        FilterOp::Lte => Bson::Document(doc! { "$lte": lenient_value(column, value) }),
        FilterOp::Contains => Bson::Document(regex_predicate(escape_regex(value))),
        FilterOp::StartsWith => Bson::Document(regex_predicate(format!("^{}", escape_regex(value)))),
        FilterOp::EndsWith => Bson::Document(regex_predicate(format!("{}$", escape_regex(value)))),
        FilterOp::In => {
            let items: Vec<Bson> = value
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(|v| lenient_value(column, v))
                .collect();
            Bson::Document(doc! { "$in": items })
        }
        FilterOp::IsNull => Bson::Null,
        FilterOp::IsNotNull => Bson::Document(doc! { "$ne": Bson::Null }),
    };
    doc! { column: body }
}

fn filter_document(filters: &[FilterRule]) -> Document {
    match filters {
        [] => Document::new(),
        [single] => predicate(single),
        many => doc! { "$and": many.iter().map(|f| Bson::Document(predicate(f))).collect::<Vec<Bson>>() },
    }
}

fn sort_document(sort: &[SortRule]) -> Document {
    let mut out = Document::new();
    for rule in sort {
        out.insert(rule.column.clone(), Bson::Int32(if rule.desc { -1 } else { 1 }));
    }
    if out.is_empty() {
        out.insert(ID_FIELD, Bson::Int32(1));
    }
    out
}

// ---------------------------------------------------------------------------
// Command console
// ---------------------------------------------------------------------------

// WHAT:  Splits console input into statements on blank lines.
fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.trim().is_empty() {
                out.push(std::mem::take(&mut current));
            }
            current.clear();
        } else {
            current.push_str(line);
            current.push('\n');
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

fn json_to_document(raw: &str) -> AppResult<Document> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| AppError::invalid_input(format!("Command is not valid JSON: {e}")))?;
    if !value.is_object() {
        return Err(AppError::invalid_input("A command must be a JSON object, e.g. {\"find\": \"users\", \"limit\": 20}."));
    }
    mongodb::bson::serialize_to_document(&value)
        .map_err(|e| AppError::invalid_input(format!("Command could not be converted to BSON: {e}")))
}

// WHAT:  One statement → one command document. Accepts raw JSON or the
//        shorthands `find <collection> {filter}` / `count <collection> {filter}`.
fn parse_command(statement: &str, max_rows: usize) -> AppResult<Document> {
    let text = statement.trim();
    if text.starts_with('{') {
        return json_to_document(text);
    }
    let mut parts = text.splitn(3, char::is_whitespace);
    let verb = parts.next().unwrap_or_default().to_ascii_lowercase();
    let collection = parts.next().map(str::trim).unwrap_or_default();
    let rest = parts.next().map(str::trim).unwrap_or_default();
    if collection.is_empty() || !matches!(verb.as_str(), "find" | "count") {
        return Err(AppError::invalid_input(
            "Enter a JSON command document, e.g. {\"find\": \"users\", \"limit\": 20}, or `find <collection> {filter}` / `count <collection> {filter}`.",
        ));
    }
    let filter = if rest.is_empty() { Document::new() } else { json_to_document(rest)? };
    let limit = i64::try_from(max_rows).unwrap_or(i64::MAX);
    Ok(match verb.as_str() {
        "find" => doc! { "find": collection, "filter": filter, "limit": limit },
        _ => doc! { "count": collection, "query": filter },
    })
}

// WHAT:  Pulls the optional `"$db"` routing key out of a command document.
fn split_target(mut command: Document) -> (Option<String>, Document) {
    let target = command.remove("$db").and_then(|b| b.as_str().map(str::to_string)).filter(|d| !d.is_empty());
    (target, command)
}

fn reply_to_result(reply: Document, max_rows: usize) -> StatementResult {
    let batch: Option<Vec<Document>> = reply
        .get_document("cursor")
        .ok()
        .and_then(|cursor| cursor.get_array("firstBatch").ok())
        .map(|items| items.iter().filter_map(Bson::as_document).cloned().collect());
    match batch {
        Some(docs) => {
            let truncated = docs.len() > max_rows;
            let docs: Vec<Document> = docs.into_iter().take(max_rows).collect();
            let columns = union_columns(&docs);
            StatementResult::Rows {
                result: ResultSet { rows: rows_for(&columns, &docs), columns: metas_for(&columns), truncated },
            }
        }
        None => {
            let single = [reply];
            let columns = union_columns(&single);
            StatementResult::Rows {
                result: ResultSet { rows: rows_for(&columns, &single), columns: metas_for(&columns), truncated: false },
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl MongoIntegration {
    fn db(&self) -> Database {
        self.client.database(&self.database)
    }

    fn collection(&self, table: &TableRef) -> Collection<Document> {
        self.db().collection::<Document>(&table.name)
    }

    async fn sample(&self, table: &TableRef) -> AppResult<Vec<Document>> {
        let cursor = self
            .collection(table)
            .find(Document::new())
            .sort(doc! { ID_FIELD: 1 })
            .limit(SAMPLE_SIZE)
            .await?;
        Ok(cursor.try_collect::<Vec<Document>>().await?)
    }
}

// ---------------------------------------------------------------------------
// Object explorer / administration
// ---------------------------------------------------------------------------
//
// WHAT:  Every listing is one server command whose reply is turned into JSON
//        (`bson_to_json`) and mapped by a pure function, so the mapping is
//        unit-tested offline against captured reply shapes.
// WHY:   `listDatabases`, `listCollections`, `listIndexes`, `usersInfo`,
//        `rolesInfo`, `$currentOp`, `replSetGetStatus`, `listShards`,
//        `getParameter`, `system.profile`, `collStats`, `serverStatus` and
//        `dbStats` are all plain commands; no typed driver helpers are needed.
// HOW:   Index references carry `db.collection` as parent (Mongo's namespace
//        notation, database names cannot contain a dot) so a detail request
//        knows both halves. Actions embed `"$db"` when they must run outside
//        the session database. Features that are not enabled (no replica set,
//        not a mongos, profiling off) list as empty instead of failing.

type Json = serde_json::Value;

const SYSTEM_DATABASES: [&str; 3] = ["admin", "local", "config"];
const OBJECT_CAP: usize = 2_000;
const SLOW_QUERY_CAP: i64 = 200;
const MIB: f64 = 1_048_576.0;
// Server error codes that mean "feature not enabled here" rather than "failed".
const CODE_UNAUTHORIZED: i32 = 13;
const CODE_NAMESPACE_NOT_FOUND: i32 = 26;
const CODE_COMMAND_NOT_FOUND: i32 = 59;
const CODE_NO_REPLICATION: i32 = 76;
const CODE_NOT_SUPPORTED_ON_VIEW: i32 = 166;

fn doc_json(doc: Document) -> Json {
    bson_to_json(&Bson::Document(doc))
}

fn command_code(err: &mongodb::error::Error) -> Option<i32> {
    match err.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => Some(c.code),
        _ => None,
    }
}

fn jstr<'a>(v: &'a Json, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Json::as_str)
}

fn jbool(v: &Json, key: &str) -> bool {
    v.get(key).and_then(Json::as_bool).unwrap_or(false)
}

fn number_of(v: &Json) -> Option<f64> {
    match v {
        Json::Number(n) => n.as_f64(),
        Json::String(s) => s.parse().ok(),
        Json::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn jnum(v: &Json, key: &str) -> Option<f64> {
    v.get(key).and_then(number_of)
}

fn pnum(v: &Json, pointer: &str) -> Option<f64> {
    v.pointer(pointer).and_then(number_of)
}

fn items<'a>(v: &'a Json, key: &str) -> impl Iterator<Item = &'a Json> {
    v.get(key).and_then(Json::as_array).into_iter().flatten()
}

fn cursor_batch(reply: &Json) -> Vec<Json> {
    reply.pointer("/cursor/firstBatch").and_then(Json::as_array).cloned().unwrap_or_default()
}

fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

// WHAT:  One-line rendering for detail text, cut at 120 characters.
fn compact(v: &Json) -> String {
    let text = match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    };
    if text.chars().count() > 120 {
        format!("{}…", text.chars().take(119).collect::<String>())
    } else {
        text
    }
}

fn json_kind(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "bool",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

fn bytes_text(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", value as u64)
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn duration_text(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3_600, (s % 3_600) / 60);
    if d > 0 {
        format!("{d}d {h}h {m}m")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else {
        format!("{m}m {}s", s % 60)
    }
}

fn parse_time(text: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(text).ok()
}

// WHAT:  Numeric-aware name order (op ids, otherwise plain text), capped.
fn sorted(mut list: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    list.sort_by(|a, b| {
        let (x, y) = (&a.reference.name, &b.reference.name);
        match (x.parse::<f64>(), y.parse::<f64>()) {
            (Ok(p), Ok(q)) => p.partial_cmp(&q).unwrap_or(std::cmp::Ordering::Equal),
            _ => x.cmp(y),
        }
    });
    list.truncate(OBJECT_CAP);
    list
}

// WHAT:  `db.collection` parent → (db, Some(collection)); a bare database → (db, None).
fn split_namespace<'a>(parent: Option<&'a str>, default_db: &'a str) -> (&'a str, Option<&'a str>) {
    match parent.map(str::trim).filter(|p| !p.is_empty()) {
        Some(p) => match p.split_once('.') {
            Some((db, coll)) if !db.is_empty() && !coll.is_empty() => (db, Some(coll)),
            _ => (p, None),
        },
        None => (default_db, None),
    }
}

// WHAT:  A command document as console text; `db` adds the `$db` routing key
//        when the action must run outside the session database.
fn command_text(mut command: Json, db: Option<&str>) -> String {
    if let (Some(d), Some(obj)) = (db, command.as_object_mut()) {
        obj.insert("$db".into(), Json::String(d.to_string()));
    }
    command.to_string()
}

fn rows_from(objects: &[Json], id_first: Option<&str>) -> Option<ResultSet> {
    if objects.is_empty() {
        None
    } else {
        Some(objects_to_result_set(objects, id_first, OBJECT_CAP))
    }
}

// ---- listings ---------------------------------------------------------------

fn database_summaries(reply: &Json) -> Vec<ObjectSummary> {
    let list = items(reply, "databases")
        .filter_map(|d| {
            let name = jstr(d, "name")?;
            let mut s = ObjectSummary::new(ObjectKind::Database, name, None);
            if let Some(size) = jnum(d, "sizeOnDisk") {
                s = s.with_detail(bytes_text(size));
            }
            if SYSTEM_DATABASES.contains(&name) {
                s = s.with_badge("system");
            } else if jbool(d, "empty") {
                s = s.with_badge("empty");
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

// WHAT:  `listCollections` → collections (with capped / timeseries badges) or views.
fn collection_summaries(reply: &Json, db: &str, views: bool) -> Vec<ObjectSummary> {
    let list = cursor_batch(reply)
        .iter()
        .filter_map(|c| {
            let name = jstr(c, "name")?;
            if name.starts_with("system.") {
                return None;
            }
            let kind = jstr(c, "type").unwrap_or("collection");
            if (kind == "view") != views {
                return None;
            }
            let options = c.get("options").cloned().unwrap_or(Json::Null);
            let object_kind = if views { ObjectKind::View } else { ObjectKind::Collection };
            let mut s = ObjectSummary::new(object_kind, name, Some(db.to_string()));
            if views {
                s = s.with_badge("view");
                if let Some(on) = jstr(&options, "viewOn") {
                    s = s.with_detail(format!("on {on}"));
                }
            } else if kind == "timeseries" || options.get("timeseries").is_some() {
                s = s.with_badge("timeseries");
                if let Some(field) = options.pointer("/timeseries/timeField").and_then(Json::as_str) {
                    s = s.with_detail(format!("timeField {field}"));
                }
            } else if jbool(&options, "capped") {
                s = s.with_badge("capped");
                if let Some(size) = jnum(&options, "size") {
                    s = s.with_detail(bytes_text(size));
                }
            }
            if options.get("validator").is_some() && s.detail.is_none() {
                s = s.with_detail("validator");
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

fn index_key_text(spec: &Json) -> String {
    spec.get("key")
        .and_then(Json::as_object)
        .map(|k| k.iter().map(|(field, dir)| format!("{field}: {}", compact(dir))).collect::<Vec<_>>().join(", "))
        .unwrap_or_default()
}

fn index_badge(spec: &Json) -> Option<&'static str> {
    let key = spec.get("key").and_then(Json::as_object);
    let has = |needle: &str| key.is_some_and(|k| k.values().any(|v| v.as_str() == Some(needle)));
    if spec.get("expireAfterSeconds").is_some() {
        Some("ttl")
    } else if jbool(spec, "unique") {
        Some("unique")
    } else if spec.get("textIndexVersion").is_some() || has("text") {
        Some("text")
    } else if has("2dsphere") {
        Some("2dsphere")
    } else if has("2d") {
        Some("2d")
    } else if has("hashed") {
        Some("hashed")
    } else if key.is_some_and(|k| k.keys().any(|f| f.contains("$**"))) {
        Some("wildcard")
    } else if jbool(spec, "hidden") {
        Some("hidden")
    } else if jbool(spec, "sparse") {
        Some("sparse")
    } else {
        None
    }
}

// WHAT:  `listIndexes` → one row per index; `namespace` is `db.collection`.
fn index_summaries(reply: &Json, namespace: &str) -> Vec<ObjectSummary> {
    cursor_batch(reply)
        .iter()
        .filter_map(|spec| {
            let name = jstr(spec, "name")?;
            let mut s = ObjectSummary::new(ObjectKind::Index, name, Some(namespace.to_string())).with_detail(index_key_text(spec));
            if let Some(b) = index_badge(spec) {
                s = s.with_badge(b);
            }
            Some(s)
        })
        .collect()
}

fn role_text(role: &Json) -> Option<String> {
    Some(format!("{}@{}", jstr(role, "role")?, jstr(role, "db")?))
}

fn user_summaries(reply: &Json) -> Vec<ObjectSummary> {
    let list = items(reply, "users")
        .filter_map(|u| {
            let name = jstr(u, "user")?;
            let db = jstr(u, "db").unwrap_or("admin");
            let roles: Vec<String> = items(u, "roles").filter_map(role_text).collect();
            let mut s = ObjectSummary::new(ObjectKind::User, name, Some(db.to_string())).with_badge(db);
            if !roles.is_empty() {
                s = s.with_detail(roles.join(", "));
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

fn role_summaries(reply: &Json) -> Vec<ObjectSummary> {
    let list = items(reply, "roles")
        .filter_map(|r| {
            let name = jstr(r, "role")?;
            let db = jstr(r, "db").unwrap_or("admin");
            let privileges = r.get("privileges").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
            let inherits: Vec<String> = items(r, "roles").filter_map(role_text).collect();
            let mut detail = format!("{privileges} privileges");
            if !inherits.is_empty() {
                detail.push_str(&format!(" · inherits {}", inherits.join(", ")));
            }
            let badge = if jbool(r, "isBuiltin") { "builtin" } else { db };
            Some(ObjectSummary::new(ObjectKind::Role, name, Some(db.to_string())).with_detail(detail).with_badge(badge))
        })
        .collect();
    sorted(list)
}

fn op_id_text(op: &Json) -> String {
    match op.get("opid") {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

// WHAT:  `$currentOp` documents → sessions (name = opid).
fn session_summaries(ops: &[Json]) -> Vec<ObjectSummary> {
    let list = ops
        .iter()
        .filter_map(|op| {
            let name = op_id_text(op);
            if name.is_empty() {
                return None;
            }
            let mut parts: Vec<String> = Vec::new();
            if let Some(o) = jstr(op, "op") {
                parts.push(o.to_string());
            }
            if let Some(ns) = jstr(op, "ns").filter(|n| !n.is_empty()) {
                parts.push(ns.to_string());
            }
            if let Some(secs) = jnum(op, "secs_running") {
                parts.push(format!("{secs}s"));
            }
            if let Some(app) = jstr(op, "appName").filter(|a| !a.is_empty()) {
                parts.push(app.to_string());
            }
            let badge = if jbool(op, "active") { "active" } else { "idle" };
            Some(ObjectSummary::new(ObjectKind::Session, name, None).with_detail(parts.join(" · ")).with_badge(badge))
        })
        .collect();
    sorted(list)
}

fn member_lag_seconds(member: &Json, primary_optime: Option<chrono::DateTime<chrono::FixedOffset>>) -> Option<i64> {
    let mine = jstr(member, "optimeDate").and_then(parse_time)?;
    Some((primary_optime? - mine).num_seconds().max(0))
}

// WHAT:  `replSetGetStatus.members` → replicas with state badge and optime / lag.
fn replica_summaries(reply: &Json) -> Vec<ObjectSummary> {
    let set = jstr(reply, "set").map(str::to_string);
    let members: Vec<&Json> = items(reply, "members").collect();
    let primary_optime = members
        .iter()
        .find(|m| jstr(m, "stateStr") == Some("PRIMARY"))
        .and_then(|m| jstr(m, "optimeDate"))
        .and_then(parse_time);
    let list = members
        .iter()
        .filter_map(|m| {
            let name = jstr(m, "name")?;
            let state = jstr(m, "stateStr").unwrap_or("UNKNOWN").to_lowercase();
            let mut parts: Vec<String> = Vec::new();
            if let Some(t) = jstr(m, "optimeDate") {
                parts.push(format!("optime {t}"));
            }
            if let Some(lag) = member_lag_seconds(m, primary_optime) {
                parts.push(format!("lag {lag}s"));
            }
            if jnum(m, "health") == Some(0.0) {
                parts.push("unhealthy".into());
            }
            if jbool(m, "self") {
                parts.push("self".into());
            }
            Some(ObjectSummary::new(ObjectKind::Replica, name, set.clone()).with_detail(parts.join(" · ")).with_badge(state))
        })
        .collect();
    sorted(list)
}

fn shard_summaries(reply: &Json) -> Vec<ObjectSummary> {
    let list = items(reply, "shards")
        .filter_map(|s| {
            let name = jstr(s, "_id")?;
            let mut o = ObjectSummary::new(ObjectKind::Shard, name, None);
            if let Some(host) = jstr(s, "host") {
                o = o.with_detail(host);
            }
            o = o.with_badge(if jbool(s, "draining") { "draining" } else { "active" });
            Some(o)
        })
        .collect();
    sorted(list)
}

// WHAT:  `getParameter: "*"` → one setting per key (reply envelope stripped).
fn setting_summaries(reply: &Json) -> Vec<ObjectSummary> {
    let list = reply
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(k, _)| !matches!(k.as_str(), "ok" | "$clusterTime" | "operationTime"))
        .map(|(k, v)| ObjectSummary::new(ObjectKind::Setting, k, None).with_detail(compact(v)).with_badge(json_kind(v)))
        .collect();
    sorted(list)
}

fn slow_query_name(entry: &Json) -> String {
    format!("{} {} @ {}", jstr(entry, "op").unwrap_or("op"), jstr(entry, "ns").unwrap_or("?"), jstr(entry, "ts").unwrap_or(""))
}

// WHAT:  `system.profile` entries (already millis-desc from the server).
fn slow_query_summaries(entries: &[Json], db: &str) -> Vec<ObjectSummary> {
    entries
        .iter()
        .take(OBJECT_CAP)
        .map(|e| {
            let mut parts: Vec<String> = Vec::new();
            if let Some(ms) = jnum(e, "millis") {
                parts.push(format!("{ms} ms"));
            }
            if let Some(plan) = jstr(e, "planSummary") {
                parts.push(plan.to_string());
            }
            if let Some(n) = jnum(e, "docsExamined") {
                parts.push(format!("{n} examined"));
            }
            let mut s = ObjectSummary::new(ObjectKind::SlowQuery, slow_query_name(e), Some(db.to_string())).with_detail(parts.join(" · "));
            if let Some(op) = jstr(e, "op") {
                s = s.with_badge(op);
            }
            s
        })
        .collect()
}

// ---- details ----------------------------------------------------------------

// WHAT:  `dbStats` → database sheet; children are its collections.
fn database_detail(reference: &ObjectRef, stats: &Json, collections: Vec<ObjectSummary>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(stats), CodeLanguage::Json);
    for (label, key, bytes) in [
        ("Collections", "collections", false),
        ("Views", "views", false),
        ("Objects", "objects", false),
        ("Data size", "dataSize", true),
        ("Storage size", "storageSize", true),
        ("Indexes", "indexes", false),
        ("Index size", "indexSize", true),
        ("Total size", "totalSize", true),
    ] {
        if let Some(n) = jnum(stats, key) {
            d = d.property(label, if bytes { bytes_text(n) } else { crate::model::objects::format_number(n) });
        }
    }
    d.children = collections;
    d.action(ObjectAction::destructive("drop", "Drop database", command_text(serde_json::json!({"dropDatabase": 1}), Some(&reference.name))))
}

// WHAT:  A `collStats` reply reduced to the figures worth showing.
fn coll_stats_summary(stats: &Json) -> Json {
    let mut out = serde_json::Map::new();
    for key in ["ns", "count", "size", "avgObjSize", "storageSize", "nindexes", "totalIndexSize", "capped", "max", "maxSize", "freeStorageSize"] {
        if let Some(v) = stats.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }
    Json::Object(out)
}

// WHAT:  Collection / view sheet: options + stats as JSON, index children,
//        validate / compact / drop actions routed to the object's database.
fn collection_detail(
    reference: &ObjectRef,
    spec: Option<&Json>,
    stats: Option<&Json>,
    columns: Vec<ColumnInfo>,
    indexes: Vec<ObjectSummary>,
    target_db: Option<&str>,
) -> ObjectDetail {
    let options = spec.and_then(|s| s.get("options")).cloned().unwrap_or_else(|| Json::Object(Default::default()));
    let kind = spec.and_then(|s| jstr(s, "type")).unwrap_or(if reference.kind == ObjectKind::View { "view" } else { "collection" });
    let mut definition = serde_json::json!({ "name": reference.name, "type": kind, "options": options });
    if let Some(s) = stats {
        definition["stats"] = coll_stats_summary(s);
    }
    if let Some(uuid) = spec.and_then(|s| s.pointer("/info/uuid")) {
        definition["uuid"] = uuid.clone();
    }
    let mut d = ObjectDetail::empty(reference).definition(pretty(&definition), CodeLanguage::Json).property("Type", kind);
    if let Some(s) = stats {
        for (label, key, bytes) in [
            ("Documents", "count", false),
            ("Data size", "size", true),
            ("Storage size", "storageSize", true),
            ("Average object size", "avgObjSize", true),
            ("Indexes", "nindexes", false),
            ("Total index size", "totalIndexSize", true),
        ] {
            if let Some(n) = jnum(s, key) {
                d = d.property(label, if bytes { bytes_text(n) } else { crate::model::objects::format_number(n) });
            }
        }
    }
    if let Some(on) = jstr(&options, "viewOn") {
        d = d.property("View on", on);
    }
    if jbool(&options, "capped") {
        d = d.property("Capped", "yes");
        if let Some(max) = jnum(&options, "max") {
            d = d.property("Max documents", crate::model::objects::format_number(max));
        }
    }
    if let Some(ts) = options.get("timeseries") {
        d = d.property("Time series", compact(ts));
    }
    if options.get("validator").is_some() {
        d = d.property("Validation level", jstr(&options, "validationLevel").unwrap_or("strict"));
        d = d.property("Validation action", jstr(&options, "validationAction").unwrap_or("error"));
    }
    d.columns = columns;
    d.children = indexes;
    let name = reference.name.as_str();
    if reference.kind == ObjectKind::Collection {
        d = d
            .action(ObjectAction::new("validate", "Validate", command_text(serde_json::json!({"validate": name}), target_db)))
            .action(ObjectAction::destructive("compact", "Compact", command_text(serde_json::json!({"compact": name}), target_db)));
    }
    d.action(ObjectAction::destructive("drop", "Drop", command_text(serde_json::json!({"drop": name}), target_db)))
}

fn index_detail(reference: &ObjectRef, spec: &Json, collection: &str, target_db: Option<&str>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(spec), CodeLanguage::Json).property("Collection", collection).property("Key", index_key_text(spec));
    if let Some(b) = index_badge(spec) {
        d = d.property("Type", b);
    }
    for (label, key) in [("Unique", "unique"), ("Sparse", "sparse"), ("Hidden", "hidden")] {
        if jbool(spec, key) {
            d = d.property(label, "yes");
        }
    }
    if let Some(ttl) = jnum(spec, "expireAfterSeconds") {
        d = d.property("Expire after", format!("{ttl} s"));
    }
    if let Some(p) = spec.get("partialFilterExpression") {
        d = d.property("Partial filter", compact(p));
    }
    if let Some(c) = spec.get("collation") {
        d = d.property("Collation", compact(c));
    }
    let key_rows: Vec<Json> = spec
        .get("key")
        .and_then(Json::as_object)
        .into_iter()
        .flatten()
        .map(|(field, dir)| serde_json::json!({"field": field, "direction": dir}))
        .collect();
    d.rows = rows_from(&key_rows, Some("field"));
    if reference.name != "_id_" {
        let statement = command_text(serde_json::json!({"dropIndexes": collection, "index": reference.name}), target_db);
        d = d.action(ObjectAction::destructive("drop", "Drop index", statement));
    }
    d
}

fn user_detail(reference: &ObjectRef, user: &Json, target_db: Option<&str>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(user), CodeLanguage::Json);
    if let Some(db) = jstr(user, "db") {
        d = d.property("Database", db);
    }
    let mechanisms: Vec<&str> = items(user, "mechanisms").filter_map(Json::as_str).collect();
    if !mechanisms.is_empty() {
        d = d.property("Mechanisms", mechanisms.join(", "));
    }
    if let Some(data) = user.get("customData") {
        d = d.property("Custom data", compact(data));
    }
    let roles: Vec<Json> = items(user, "roles").cloned().collect();
    d.rows = rows_from(&roles, Some("role"));
    d.action(ObjectAction::destructive("drop", "Drop user", command_text(serde_json::json!({"dropUser": reference.name}), target_db)))
}

fn role_detail(reference: &ObjectRef, role: &Json, target_db: Option<&str>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(role), CodeLanguage::Json);
    if let Some(db) = jstr(role, "db") {
        d = d.property("Database", db);
    }
    d = d.property("Built-in", if jbool(role, "isBuiltin") { "yes" } else { "no" });
    let inherits: Vec<String> = items(role, "roles").filter_map(role_text).collect();
    if !inherits.is_empty() {
        d = d.property("Inherits", inherits.join(", "));
    }
    let privileges: Vec<Json> = items(role, "privileges")
        .map(|p| serde_json::json!({"resource": compact(p.get("resource").unwrap_or(&Json::Null)), "actions": compact(p.get("actions").unwrap_or(&Json::Null))}))
        .collect();
    d.rows = rows_from(&privileges, Some("resource"));
    if !jbool(role, "isBuiltin") {
        d = d.action(ObjectAction::destructive("drop", "Drop role", command_text(serde_json::json!({"dropRole": reference.name}), target_db)));
    }
    d
}

fn session_detail(reference: &ObjectRef, op: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(op), CodeLanguage::Json);
    for (label, key) in [("Operation", "op"), ("Namespace", "ns"), ("Client", "client"), ("Application", "appName"), ("Description", "desc"), ("Plan", "planSummary")] {
        if let Some(v) = jstr(op, key).filter(|v| !v.is_empty()) {
            d = d.property(label, v);
        }
    }
    d = d.property("Active", if jbool(op, "active") { "yes" } else { "no" });
    if let Some(secs) = jnum(op, "secs_running") {
        d = d.property("Running", format!("{secs} s"));
    }
    if jbool(op, "waitingForLock") {
        d = d.property("Waiting for lock", "yes");
    }
    if let Some(users) = op.get("effectiveUsers").filter(|u| !u.as_array().is_some_and(Vec::is_empty)) {
        d = d.property("Users", compact(users));
    }
    let opid = op.get("opid").cloned().unwrap_or(Json::String(reference.name.clone()));
    d.action(ObjectAction::destructive("kill", "Kill operation", command_text(serde_json::json!({"killOp": 1, "op": opid}), Some("admin"))))
}

fn replica_detail(reference: &ObjectRef, member: &Json, primary_optime: Option<chrono::DateTime<chrono::FixedOffset>>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(member), CodeLanguage::Json);
    if let Some(state) = jstr(member, "stateStr") {
        d = d.property("State", state);
    }
    d = d.property("Health", if jnum(member, "health") == Some(0.0) { "down" } else { "up" });
    if let Some(up) = jnum(member, "uptime") {
        d = d.property("Uptime", duration_text(up));
    }
    if let Some(t) = jstr(member, "optimeDate") {
        d = d.property("Optime", t);
    }
    if let Some(lag) = member_lag_seconds(member, primary_optime) {
        d = d.property("Replication lag", format!("{lag} s"));
    }
    for (label, key) in [("Sync source", "syncSourceHost"), ("Last heartbeat", "lastHeartbeat"), ("Elected", "electionDate")] {
        if let Some(v) = jstr(member, key).filter(|v| !v.is_empty()) {
            d = d.property(label, v);
        }
    }
    if let Some(ping) = jnum(member, "pingMs") {
        d = d.property("Ping", format!("{ping} ms"));
    }
    d
}

fn shard_detail(reference: &ObjectRef, shard: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(shard), CodeLanguage::Json);
    if let Some(host) = jstr(shard, "host") {
        d = d.property("Host", host);
    }
    if let Some(state) = jnum(shard, "state") {
        d = d.property("State", format!("{state}"));
    }
    if jbool(shard, "draining") {
        d = d.property("Draining", "yes");
    }
    let tags: Vec<&str> = items(shard, "tags").filter_map(Json::as_str).collect();
    if !tags.is_empty() {
        d = d.property("Tags", tags.join(", "));
    }
    d
}

fn setting_detail(reference: &ObjectRef, value: &Json) -> ObjectDetail {
    ObjectDetail::empty(reference).definition(pretty(value), CodeLanguage::Json).property("Type", json_kind(value)).property("Value", compact(value))
}

fn slow_query_detail(reference: &ObjectRef, entry: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(entry), CodeLanguage::Json);
    for (label, key) in [("Operation", "op"), ("Namespace", "ns"), ("Plan", "planSummary"), ("Client", "client"), ("Application", "appName"), ("Timestamp", "ts")] {
        if let Some(v) = jstr(entry, key).filter(|v| !v.is_empty()) {
            d = d.property(label, v);
        }
    }
    for (label, key) in [("Duration (ms)", "millis"), ("Documents examined", "docsExamined"), ("Keys examined", "keysExamined"), ("Returned", "nreturned")] {
        if let Some(n) = jnum(entry, key) {
            d = d.property(label, crate::model::objects::format_number(n));
        }
    }
    d
}

// ---- server stats -------------------------------------------------------------

fn number_stats(source: &Json, specs: &[(&str, &str, Option<&str>)]) -> Vec<Stat> {
    specs
        .iter()
        .filter_map(|(label, pointer, unit)| {
            let value = pnum(source, pointer)?;
            let value = if *unit == Some("MB") && !pointer.starts_with("/mem/") { value / MIB } else { value };
            Some(Stat::number(label, (value * 100.0).round() / 100.0, *unit))
        })
        .collect()
}

fn push_group(groups: &mut Vec<StatGroup>, title: &str, stats: Vec<Stat>) {
    if !stats.is_empty() {
        groups.push(StatGroup { title: title.to_string(), stats });
    }
}

// WHAT:  `serverStatus` + `dbStats` → the Stats tab groups.
fn server_stat_groups(status: &Json, db_stats: &Json, database: &str) -> Vec<StatGroup> {
    let mut groups = Vec::new();
    let mut server = Vec::new();
    if let Some(v) = jstr(status, "version") {
        server.push(Stat::text("Version", v));
    }
    if let Some(h) = jstr(status, "host") {
        server.push(Stat::text("Host", h));
    }
    if let Some(p) = jstr(status, "process") {
        server.push(Stat::text("Process", p));
    }
    if let Some(u) = pnum(status, "/uptime") {
        server.push(Stat::text("Uptime", duration_text(u)));
    }
    if let Some(e) = status.pointer("/storageEngine/name").and_then(Json::as_str) {
        server.push(Stat::text("Storage engine", e));
    }
    push_group(&mut groups, "Server", server);
    push_group(
        &mut groups,
        "Connections",
        number_stats(
            status,
            &[
                ("Current", "/connections/current", None),
                ("Available", "/connections/available", None),
                ("Active", "/connections/active", None),
                ("Total created", "/connections/totalCreated", None),
            ],
        ),
    );
    push_group(&mut groups, "Memory", number_stats(status, &[("Resident", "/mem/resident", Some("MB")), ("Virtual", "/mem/virtual", Some("MB"))]));
    push_group(
        &mut groups,
        "Throughput",
        number_stats(
            status,
            &[
                ("Inserts", "/opcounters/insert", None),
                ("Queries", "/opcounters/query", None),
                ("Updates", "/opcounters/update", None),
                ("Deletes", "/opcounters/delete", None),
                ("Getmores", "/opcounters/getmore", None),
                ("Commands", "/opcounters/command", None),
            ],
        ),
    );
    push_group(
        &mut groups,
        "Network",
        number_stats(status, &[("Bytes in", "/network/bytesIn", Some("MB")), ("Bytes out", "/network/bytesOut", Some("MB")), ("Requests", "/network/numRequests", None)]),
    );
    push_group(
        &mut groups,
        "Cache",
        number_stats(
            status,
            &[
                ("In cache", "/wiredTiger/cache/bytes currently in the cache", Some("MB")),
                ("Maximum", "/wiredTiger/cache/maximum bytes configured", Some("MB")),
                ("Dirty", "/wiredTiger/cache/tracked dirty bytes in the cache", Some("MB")),
                ("Pages read", "/wiredTiger/cache/pages read into cache", None),
                ("Pages written", "/wiredTiger/cache/pages written from cache", None),
            ],
        ),
    );
    let mut replication = Vec::new();
    if let Some(set) = status.pointer("/repl/setName").and_then(Json::as_str) {
        replication.push(Stat::text("Replica set", set));
        if let Some(primary) = status.pointer("/repl/primary").and_then(Json::as_str) {
            replication.push(Stat::text("Primary", primary));
        }
        if let Some(me) = status.pointer("/repl/me").and_then(Json::as_str) {
            replication.push(Stat::text("This member", me));
        }
        if let Some(hosts) = status.pointer("/repl/hosts").and_then(Json::as_array) {
            replication.push(Stat::number("Members", hosts.len() as f64, None));
        }
    }
    push_group(&mut groups, "Replication", replication);
    push_group(
        &mut groups,
        &format!("Database {database}"),
        number_stats(
            db_stats,
            &[
                ("Collections", "/collections", None),
                ("Objects", "/objects", None),
                ("Data size", "/dataSize", Some("MB")),
                ("Storage size", "/storageSize", Some("MB")),
                ("Indexes", "/indexes", None),
                ("Index size", "/indexSize", Some("MB")),
            ],
        ),
    );
    groups
}

impl MongoIntegration {
    async fn command(&self, database: &str, command: Document) -> AppResult<Json> {
        Ok(doc_json(self.client.database(database).run_command(command).await?))
    }

    // WHAT:  Same, but `None` when the server answers with one of `codes`
    //        (feature not enabled here rather than a failure).
    async fn optional_command(&self, database: &str, command: Document, codes: &[i32]) -> AppResult<Option<Json>> {
        match self.client.database(database).run_command(command).await {
            Ok(doc) => Ok(Some(doc_json(doc))),
            Err(e) if command_code(&e).is_some_and(|c| codes.contains(&c)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn target_db<'a>(&self, db: &'a str) -> Option<&'a str> {
        (db != self.database).then_some(db)
    }

    async fn list_collections(&self, db: &str, name: Option<&str>) -> AppResult<Json> {
        let mut cmd = doc! { "listCollections": 1 };
        if let Some(n) = name {
            cmd.insert("filter", doc! { "name": n });
        }
        self.command(db, cmd).await
    }

    async fn collection_spec(&self, db: &str, name: &str) -> AppResult<Option<Json>> {
        Ok(cursor_batch(&self.list_collections(db, Some(name)).await?).into_iter().next())
    }

    async fn indexes_of(&self, db: &str, collection: &str) -> AppResult<Vec<ObjectSummary>> {
        let reply = self.optional_command(db, doc! { "listIndexes": collection }, &[CODE_NAMESPACE_NOT_FOUND, CODE_NOT_SUPPORTED_ON_VIEW]).await?;
        Ok(reply.map(|r| index_summaries(&r, &format!("{db}.{collection}"))).unwrap_or_default())
    }

    async fn index_spec(&self, db: &str, collection: &str, name: &str) -> AppResult<Json> {
        let reply = self.command(db, doc! { "listIndexes": collection }).await?;
        cursor_batch(&reply)
            .into_iter()
            .find(|spec| jstr(spec, "name") == Some(name))
            .ok_or_else(|| AppError::not_found(format!("Index {name} not found on {db}.{collection}.")))
    }

    // WHAT:  Active operations via `$currentOp`, falling back to the legacy
    //        `currentOp` command on servers that reject the aggregation stage.
    async fn current_ops(&self) -> AppResult<Vec<Json>> {
        let pipeline = doc! { "aggregate": 1, "pipeline": [ { "$currentOp": { "allUsers": true, "idleConnections": false } } ], "cursor": {} };
        match self.command("admin", pipeline).await {
            Ok(reply) => Ok(cursor_batch(&reply)),
            Err(_) => {
                let legacy = self.command("admin", doc! { "currentOp": 1 }).await?;
                Ok(items(&legacy, "inprog").cloned().collect())
            }
        }
    }

    async fn all_users(&self) -> AppResult<Json> {
        match self.optional_command("admin", doc! { "usersInfo": { "forAllDBs": true } }, &[CODE_UNAUTHORIZED]).await? {
            Some(reply) => Ok(reply),
            None => self.command(&self.database, doc! { "usersInfo": 1 }).await,
        }
    }

    async fn custom_roles(&self) -> AppResult<Vec<ObjectSummary>> {
        let mut all = role_summaries(&self.command("admin", doc! { "rolesInfo": 1, "showBuiltinRoles": false }).await?);
        if self.database != "admin" {
            all.extend(role_summaries(&self.command(&self.database, doc! { "rolesInfo": 1, "showBuiltinRoles": false }).await?));
        }
        Ok(sorted(all))
    }

    async fn profile_entries(&self) -> AppResult<Vec<Json>> {
        let cmd = doc! { "find": "system.profile", "sort": { "millis": -1 }, "limit": SLOW_QUERY_CAP };
        let reply = self.optional_command(&self.database, cmd, &[CODE_NAMESPACE_NOT_FOUND]).await?;
        Ok(reply.map(|r| cursor_batch(&r)).unwrap_or_default())
    }

    async fn replica_status(&self) -> AppResult<Option<Json>> {
        self.optional_command("admin", doc! { "replSetGetStatus": 1 }, &[CODE_NO_REPLICATION, CODE_COMMAND_NOT_FOUND]).await
    }

    async fn collection_object(&self, reference: &ObjectRef, db: &str) -> AppResult<ObjectDetail> {
        let spec = self.collection_spec(db, &reference.name).await?;
        let is_view = spec.as_ref().and_then(|s| jstr(s, "type")) == Some("view") || reference.kind == ObjectKind::View;
        let (stats, indexes, columns) = if is_view {
            (None, Vec::new(), Vec::new())
        } else {
            let stats = self.optional_command(db, doc! { "collStats": reference.name.as_str() }, &[CODE_NAMESPACE_NOT_FOUND, CODE_NOT_SUPPORTED_ON_VIEW]).await?;
            let indexes = self.indexes_of(db, &reference.name).await?;
            let cursor = self.client.database(db).collection::<Document>(&reference.name).find(Document::new()).limit(SAMPLE_SIZE).await?;
            let docs = cursor.try_collect::<Vec<Document>>().await?;
            (stats, indexes, union_columns(&docs))
        };
        Ok(collection_detail(reference, spec.as_ref(), stats.as_ref(), columns, indexes, self.target_db(db)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { sql: false, namespaces: true, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: false },
        object_kinds: vec![K::Database, K::Collection, K::View, K::Index, K::User, K::Role, K::Session, K::Replica, K::Shard, K::Setting, K::SlowQuery],
        tools: vec![T::Stats, T::PipelineBuilder],
    }
}

#[async_trait]
impl Integration for MongoIntegration {
    fn engine(&self) -> Engine {
        Engine::Mongodb
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        self.db().run_command(doc! { "ping": 1 }).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let info = self.client.database("admin").run_command(doc! { "buildInfo": 1 }).await?;
        Ok(info.get_str("version").ok().map(|v| format!("MongoDB {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let all = self.client.list_database_names().await?;
        let user: Vec<String> = all
            .iter()
            .filter(|name| !matches!(name.as_str(), "admin" | "local" | "config"))
            .cloned()
            .collect();
        let mut names = if user.is_empty() { all } else { user };
        if !names.iter().any(|n| n == &self.database) {
            names.push(self.database.clone());
        }
        names.sort();
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let db = self.db();
        let mut names = db.list_collection_names().await?;
        names.sort();
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let estimate = db.collection::<Document>(&name).estimated_document_count().await.ok();
            tables.push(TableInfo {
                schema: Some(self.database.clone()),
                name,
                kind: TableKind::Table,
                row_estimate: estimate.and_then(|n| i64::try_from(n).ok()),
            });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let docs = self.sample(table).await?;
        let mut columns = union_columns(&docs);
        if columns.is_empty() {
            // An empty collection still has an `_id`; give the grid a header.
            columns.push(ColumnInfo { name: ID_FIELD.to_string(), data_type: "objectId".to_string(), nullable: false, primary_key: true, ordinal: 1 });
        }
        Ok(columns)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let n = self.collection(table).estimated_document_count().await?;
        Ok(i64::try_from(n).ok())
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let n = self.collection(table).count_documents(filter_document(filters)).await?;
        Ok(i64::try_from(n).unwrap_or(i64::MAX))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cursor = self
            .collection(table)
            .find(filter_document(&query.filters))
            .sort(sort_document(&query.sort))
            .skip(query.offset)
            .limit(i64::from(query.limit))
            .await?;
        let docs = cursor.try_collect::<Vec<Document>>().await?;
        // Align cells to the sampled header, then append keys the sample missed.
        let mut columns = self.columns(table).await?;
        for extra in union_columns(&docs) {
            if !columns.iter().any(|c| c.name == extra.name) {
                columns.push(extra);
            }
        }
        Ok(ResultSet { rows: rows_for(&columns, &docs), columns: metas_for(&columns), truncated: false })
    }

    async fn execute(&self, text: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let statements = split_statements(text);
        if statements.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        let db = self.db();
        let mut out = Vec::with_capacity(statements.len());
        for statement in statements {
            let (target, command) = split_target(parse_command(&statement, max_rows)?);
            let reply = match &target {
                Some(name) => self.client.database(name).run_command(command).await?,
                None => db.run_command(command).await?,
            };
            out.push(reply_to_result(reply, max_rows));
        }
        Ok(out)
    }

    async fn close(&self) {
        self.client.clone().shutdown().await;
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let (db, owner) = split_namespace(parent, &self.database);
        match kind {
            ObjectKind::Database => Ok(database_summaries(&self.command("admin", doc! { "listDatabases": 1 }).await?)),
            ObjectKind::Collection | ObjectKind::View => {
                Ok(collection_summaries(&self.list_collections(db, None).await?, db, kind == ObjectKind::View))
            }
            ObjectKind::Index => match owner {
                Some(collection) => Ok(sorted(self.indexes_of(db, collection).await?)),
                None => {
                    let mut all = Vec::new();
                    for collection in collection_summaries(&self.list_collections(db, None).await?, db, false) {
                        all.extend(self.indexes_of(db, &collection.reference.name).await?);
                        if all.len() >= OBJECT_CAP {
                            break;
                        }
                    }
                    Ok(sorted(all))
                }
            },
            ObjectKind::User => Ok(user_summaries(&self.all_users().await?)),
            ObjectKind::Role => self.custom_roles().await,
            ObjectKind::Session => Ok(session_summaries(&self.current_ops().await?)),
            ObjectKind::Replica => Ok(self.replica_status().await?.map(|r| replica_summaries(&r)).unwrap_or_default()),
            ObjectKind::Shard => {
                let reply = self.optional_command("admin", doc! { "listShards": 1 }, &[CODE_COMMAND_NOT_FOUND]).await?;
                Ok(reply.map(|r| shard_summaries(&r)).unwrap_or_default())
            }
            ObjectKind::Setting => Ok(setting_summaries(&self.command("admin", doc! { "getParameter": "*" }).await?)),
            ObjectKind::SlowQuery => Ok(slow_query_summaries(&self.profile_entries().await?, &self.database)),
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let (db, owner) = split_namespace(reference.parent.as_deref(), &self.database);
        let name = reference.name.as_str();
        match reference.kind {
            ObjectKind::Database => {
                let stats = self.command(name, doc! { "dbStats": 1 }).await?;
                let collections = collection_summaries(&self.list_collections(name, None).await?, name, false);
                Ok(database_detail(reference, &stats, collections))
            }
            ObjectKind::Collection | ObjectKind::View => self.collection_object(reference, db).await,
            ObjectKind::Index => {
                let collection = owner.ok_or_else(|| AppError::invalid_input("An index reference needs its `db.collection` parent."))?;
                let spec = self.index_spec(db, collection, name).await?;
                Ok(index_detail(reference, &spec, collection, self.target_db(db)))
            }
            ObjectKind::User => {
                let reply = self.command(db, doc! { "usersInfo": { "user": name, "db": db }, "showPrivileges": true }).await?;
                let user = items(&reply, "users").next().ok_or_else(|| AppError::not_found(format!("User {name}@{db} not found.")))?;
                Ok(user_detail(reference, user, self.target_db(db)))
            }
            ObjectKind::Role => {
                let reply = self.command(db, doc! { "rolesInfo": { "role": name, "db": db }, "showPrivileges": true }).await?;
                let role = items(&reply, "roles").next().ok_or_else(|| AppError::not_found(format!("Role {name}@{db} not found.")))?;
                Ok(role_detail(reference, role, self.target_db(db)))
            }
            ObjectKind::Session => {
                let ops = self.current_ops().await?;
                let op = ops.iter().find(|op| op_id_text(op) == name).ok_or_else(|| AppError::not_found(format!("Operation {name} has finished.")))?;
                Ok(session_detail(reference, op))
            }
            ObjectKind::Replica => {
                let status = self.replica_status().await?.ok_or_else(|| AppError::not_found("This server is not part of a replica set."))?;
                let primary_optime = items(&status, "members")
                    .find(|m| jstr(m, "stateStr") == Some("PRIMARY"))
                    .and_then(|m| jstr(m, "optimeDate"))
                    .and_then(parse_time);
                let member = items(&status, "members").find(|m| jstr(m, "name") == Some(name)).ok_or_else(|| AppError::not_found(format!("Member {name} not found.")))?;
                Ok(replica_detail(reference, member, primary_optime))
            }
            ObjectKind::Shard => {
                let reply = self.command("admin", doc! { "listShards": 1 }).await?;
                let shard = items(&reply, "shards").find(|s| jstr(s, "_id") == Some(name)).ok_or_else(|| AppError::not_found(format!("Shard {name} not found.")))?;
                Ok(shard_detail(reference, shard))
            }
            ObjectKind::Setting => {
                let reply = self.command("admin", doc! { "getParameter": 1, name: 1 }).await?;
                let value = reply.get(name).cloned().unwrap_or(Json::Null);
                Ok(setting_detail(reference, &value))
            }
            ObjectKind::SlowQuery => {
                let entries = self.profile_entries().await?;
                let entry = entries.iter().find(|e| slow_query_name(e) == name).ok_or_else(|| AppError::not_found("This profile entry has rotated out of system.profile."))?;
                Ok(slow_query_detail(reference, entry))
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let status = self.command("admin", doc! { "serverStatus": 1, "repl": 1, "metrics": 0, "locks": 0, "tcmalloc": 0 }).await?;
        let db_stats = self.command(&self.database, doc! { "dbStats": 1 }).await.unwrap_or(Json::Null);
        Ok(ServerStats::now(server_stat_groups(&status, &db_stats, &self.database)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment};

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    #[test]
    fn filters_translate_to_bson() {
        let single = filter_document(&[rule("age", FilterOp::Gt, "30")]);
        assert_eq!(single, doc! { "age": { "$gt": 30_i64 } });

        let many = filter_document(&[
            rule("name", FilterOp::Contains, "a.b"),
            rule("tier", FilterOp::In, "gold, basic"),
            rule("note", FilterOp::IsNull, ""),
            rule("active", FilterOp::Eq, "true"),
            rule("_id", FilterOp::Eq, "507f1f77bcf86cd799439011"),
        ]);
        let expected_id = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap_or_default();
        assert_eq!(
            many,
            doc! { "$and": [
                { "name": { "$regex": "a\\.b", "$options": "i" } },
                { "tier": { "$in": ["gold", "basic"] } },
                { "note": Bson::Null },
                { "active": true },
                { "_id": expected_id },
            ] }
        );
        assert_eq!(filter_document(&[rule("x", FilterOp::IsNotNull, "")]), doc! { "x": { "$ne": Bson::Null } });
        assert_eq!(filter_document(&[rule("n", FilterOp::StartsWith, "ab")]), doc! { "n": { "$regex": "^ab", "$options": "i" } });
        assert_eq!(filter_document(&[]), Document::new());
        assert_eq!(sort_document(&[]), doc! { "_id": 1 });
        assert_eq!(sort_document(&[SortRule { column: "age".into(), desc: true }]), doc! { "age": -1 });
    }

    #[test]
    fn bson_values_decode() {
        let id = ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap_or_default();
        assert_eq!(bson_to_value(&Bson::ObjectId(id)), Value::Text("507f1f77bcf86cd799439011".into()));
        assert_eq!(bson_to_value(&Bson::Int32(7)), Value::Int(7));
        assert_eq!(bson_to_value(&Bson::Int64(9)), Value::Int(9));
        assert_eq!(bson_to_value(&Bson::Double(1.5)), Value::Float(1.5));
        assert_eq!(bson_to_value(&Bson::Boolean(true)), Value::Bool(true));
        assert_eq!(bson_to_value(&Bson::Null), Value::Null);
        assert_eq!(bson_to_value(&Bson::String("x".into())), Value::Text("x".into()));
        let nested = Bson::Document(doc! { "a": [1, { "b": Bson::Null }], "when": mongodb::bson::DateTime::from_millis(0) });
        assert_eq!(
            bson_to_value(&nested),
            Value::Json(serde_json::json!({ "a": [1, { "b": null }], "when": "1970-01-01T00:00:00Z" }))
        );
        assert!(matches!(bson_to_value(&Bson::DateTime(mongodb::bson::DateTime::from_millis(0))), Value::DateTime(ref s) if s.starts_with("1970-01-01")));
    }

    #[test]
    fn columns_union_puts_id_first_and_types_from_first_non_null() {
        let docs = vec![
            doc! { "name": "ann", "_id": 1, "meta": Bson::Null },
            doc! { "_id": 2, "age": 30, "meta": { "k": 1 } },
        ];
        let columns = union_columns(&docs);
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_id", "name", "meta", "age"]);
        assert!(columns[0].primary_key);
        assert_eq!(columns[2].data_type, "object");
        let rows = rows_for(&columns, &docs);
        assert_eq!(rows[0][3], Value::Null);
        assert_eq!(rows[1][3], Value::Int(30));
    }

    #[test]
    fn commands_parse_from_json_and_shorthand() {
        let json = parse_command(r#"{"find": "users", "limit": 5}"#, 100).unwrap_or_default();
        assert_eq!(json, doc! { "find": "users", "limit": 5_i64 });
        let short = parse_command(r#"find people {"age": {"$gt": 30}}"#, 50).unwrap_or_default();
        assert_eq!(short, doc! { "find": "people", "filter": { "age": { "$gt": 30_i64 } }, "limit": 50_i64 });
        let count = parse_command("count people", 50).unwrap_or_default();
        assert_eq!(count, doc! { "count": "people", "query": {} });
        assert!(matches!(parse_command("SELECT 1", 10), Err(AppError::InvalidInput { .. })));
        assert!(matches!(parse_command("[1,2]", 10), Err(AppError::InvalidInput { .. })));
        assert_eq!(split_statements("{\"a\":1}\n\n\n{\"b\":2}\n").len(), 2);
    }

    #[test]
    fn replies_become_rows() {
        let reply = doc! { "cursor": { "firstBatch": [ { "_id": 1, "n": "a" }, { "_id": 2, "n": "b" } ], "id": 0_i64 }, "ok": 1.0 };
        match reply_to_result(reply, 1) {
            StatementResult::Rows { result } => {
                assert_eq!(result.rows.len(), 1);
                assert!(result.truncated);
                assert_eq!(result.columns[0].name, "_id");
            }
            other => panic!("expected rows, got {other:?}"),
        }
        match reply_to_result(doc! { "ok": 1.0, "n": 3 }, 10) {
            StatementResult::Rows { result } => {
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["ok", "n"]);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn userinfo_is_percent_encoded() {
        assert_eq!(encode_userinfo("p@ss:w/rd"), "p%40ss%3Aw%2Frd");
        assert_eq!(encode_userinfo("plain-1_2.~"), "plain-1_2.~");
    }

    #[test]
    fn commands_route_on_db_key() {
        let (target, cmd) = split_target(doc! { "killOp": 1, "op": 7, "$db": "admin" });
        assert_eq!(target.as_deref(), Some("admin"));
        assert_eq!(cmd, doc! { "killOp": 1, "op": 7 });
        let (none, cmd) = split_target(doc! { "ping": 1 });
        assert!(none.is_none());
        assert_eq!(cmd, doc! { "ping": 1 });
        assert_eq!(command_text(serde_json::json!({"drop": "c"}), Some("other")), r#"{"drop":"c","$db":"other"}"#);
        assert_eq!(command_text(serde_json::json!({"validate": "c"}), None), r#"{"validate":"c"}"#);
    }

    #[test]
    fn namespaces_split_and_names_sort() {
        assert_eq!(split_namespace(Some("app.users"), "test"), ("app", Some("users")));
        assert_eq!(split_namespace(Some("app"), "test"), ("app", None));
        assert_eq!(split_namespace(None, "test"), ("test", None));
        assert_eq!(split_namespace(Some("  "), "test"), ("test", None));
        let list = sorted(vec![ObjectSummary::new(ObjectKind::Session, "10", None), ObjectSummary::new(ObjectKind::Session, "9", None)]);
        assert_eq!(list[0].reference.name, "9");
        assert_eq!(bytes_text(1536.0), "1.5 KB");
        assert_eq!(bytes_text(12.0), "12 B");
        assert_eq!(duration_text(90_061.0), "1d 1h 1m");
        assert_eq!(duration_text(65.0), "1m 5s");
    }

    #[test]
    fn databases_and_collections_map() {
        let reply = serde_json::json!({"databases": [
            {"name": "shop", "sizeOnDisk": 2048, "empty": false},
            {"name": "admin", "sizeOnDisk": 100, "empty": false},
            {"name": "blank", "sizeOnDisk": 0, "empty": true}
        ], "ok": 1});
        let dbs = database_summaries(&reply);
        let names: Vec<&str> = dbs.iter().map(|d| d.reference.name.as_str()).collect();
        assert_eq!(names, vec!["admin", "blank", "shop"]);
        assert_eq!(dbs[0].badge.as_deref(), Some("system"));
        assert_eq!(dbs[1].badge.as_deref(), Some("empty"));
        assert_eq!(dbs[2].detail.as_deref(), Some("2.0 KB"));

        let listing = serde_json::json!({"cursor": {"firstBatch": [
            {"name": "orders", "type": "collection", "options": {"validator": {"$jsonSchema": {}}}},
            {"name": "logs", "type": "collection", "options": {"capped": true, "size": 1048576}},
            {"name": "metrics", "type": "timeseries", "options": {"timeseries": {"timeField": "t"}}},
            {"name": "recent", "type": "view", "options": {"viewOn": "orders", "pipeline": []}},
            {"name": "system.views", "type": "collection"}
        ]}});
        let colls = collection_summaries(&listing, "shop", false);
        let names: Vec<&str> = colls.iter().map(|c| c.reference.name.as_str()).collect();
        assert_eq!(names, vec!["logs", "metrics", "orders"]);
        assert_eq!(colls[0].badge.as_deref(), Some("capped"));
        assert_eq!(colls[0].detail.as_deref(), Some("1.0 MB"));
        assert_eq!(colls[1].badge.as_deref(), Some("timeseries"));
        assert_eq!(colls[2].detail.as_deref(), Some("validator"));
        assert_eq!(colls[2].reference.parent.as_deref(), Some("shop"));
        let views = collection_summaries(&listing, "shop", true);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].reference.kind, ObjectKind::View);
        assert_eq!(views[0].detail.as_deref(), Some("on orders"));
    }

    #[test]
    fn indexes_map_with_badges() {
        let reply = serde_json::json!({"cursor": {"firstBatch": [
            {"v": 2, "key": {"_id": 1}, "name": "_id_"},
            {"v": 2, "key": {"email": 1}, "name": "email_1", "unique": true},
            {"v": 2, "key": {"_fts": "text", "_ftsx": 1}, "name": "body_text", "textIndexVersion": 3},
            {"v": 2, "key": {"loc": "2dsphere"}, "name": "loc_2dsphere"},
            {"v": 2, "key": {"createdAt": 1}, "name": "ttl", "expireAfterSeconds": 3600},
            {"v": 2, "key": {"a": 1, "b": -1}, "name": "a_1_b_-1", "sparse": true}
        ]}});
        let idx = index_summaries(&reply, "shop.orders");
        let badges: Vec<Option<&str>> = idx.iter().map(|i| i.badge.as_deref()).collect();
        assert_eq!(badges, vec![None, Some("unique"), Some("text"), Some("2dsphere"), Some("ttl"), Some("sparse")]);
        assert_eq!(idx[5].detail.as_deref(), Some("a: 1, b: -1"));
        assert_eq!(idx[0].reference.parent.as_deref(), Some("shop.orders"));

        let r = ObjectRef { kind: ObjectKind::Index, name: "email_1".into(), parent: Some("shop.orders".into()) };
        let d = index_detail(&r, &serde_json::json!({"key": {"email": 1}, "name": "email_1", "unique": true}), "orders", Some("shop"));
        assert_eq!(d.language, CodeLanguage::Json);
        assert!(d.properties.iter().any(|p| p.name == "Unique" && p.value == "yes"));
        assert_eq!(d.rows.as_ref().map(|r| r.rows.len()), Some(1));
        assert_eq!(d.actions[0].statement, r#"{"dropIndexes":"orders","index":"email_1","$db":"shop"}"#);
        assert!(d.actions[0].destructive);
        let id = ObjectRef { kind: ObjectKind::Index, name: "_id_".into(), parent: Some("shop.orders".into()) };
        assert!(index_detail(&id, &serde_json::json!({"key": {"_id": 1}, "name": "_id_"}), "orders", None).actions.is_empty());
    }

    #[test]
    fn collection_detail_has_stats_children_and_actions() {
        let r = ObjectRef { kind: ObjectKind::Collection, name: "orders".into(), parent: Some("shop".into()) };
        let spec = serde_json::json!({"name": "orders", "type": "collection", "options": {"validator": {"a": 1}, "validationLevel": "moderate"}, "info": {"uuid": "abc"}});
        let stats = serde_json::json!({"ns": "shop.orders", "count": 1500, "size": 4096, "storageSize": 8192, "nindexes": 2, "totalIndexSize": 1024, "wiredTiger": {"huge": true}});
        let children = vec![ObjectSummary::new(ObjectKind::Index, "_id_", Some("shop.orders".into()))];
        let d = collection_detail(&r, Some(&spec), Some(&stats), vec![], children, None);
        let def: serde_json::Value = serde_json::from_str(d.definition.as_deref().unwrap_or("{}")).unwrap_or_default();
        assert_eq!(def["stats"]["count"], 1500);
        assert!(def["stats"].get("wiredTiger").is_none(), "collStats is summarised");
        assert_eq!(def["uuid"], "abc");
        assert!(d.properties.iter().any(|p| p.name == "Documents" && p.value == "1,500"));
        assert!(d.properties.iter().any(|p| p.name == "Validation level" && p.value == "moderate"));
        assert_eq!(d.children.len(), 1);
        let ids: Vec<&str> = d.actions.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(ids, vec!["validate", "compact", "drop"]);
        assert!(!d.actions[0].destructive && d.actions[1].destructive && d.actions[2].destructive);
        assert_eq!(d.actions[2].statement, r#"{"drop":"orders"}"#);

        let v = ObjectRef { kind: ObjectKind::View, name: "recent".into(), parent: Some("shop".into()) };
        let view_spec = serde_json::json!({"name": "recent", "type": "view", "options": {"viewOn": "orders", "pipeline": []}});
        let d = collection_detail(&v, Some(&view_spec), None, vec![], vec![], Some("shop"));
        assert_eq!(d.actions.len(), 1);
        assert_eq!(d.actions[0].statement, r#"{"drop":"recent","$db":"shop"}"#);
        assert!(d.properties.iter().any(|p| p.name == "View on" && p.value == "orders"));
    }

    #[test]
    fn users_roles_sessions_map() {
        let users = serde_json::json!({"users": [
            {"_id": "admin.root", "user": "root", "db": "admin", "roles": [{"role": "root", "db": "admin"}], "mechanisms": ["SCRAM-SHA-256"]},
            {"_id": "shop.app", "user": "app", "db": "shop", "roles": []}
        ]});
        let u = user_summaries(&users);
        assert_eq!(u[0].reference.name, "app");
        assert!(u[0].detail.is_none());
        assert_eq!(u[1].detail.as_deref(), Some("root@admin"));
        assert_eq!(u[1].badge.as_deref(), Some("admin"));
        let ur = ObjectRef { kind: ObjectKind::User, name: "root".into(), parent: Some("admin".into()) };
        let ud = user_detail(&ur, &users["users"][0], Some("admin"));
        assert_eq!(ud.rows.as_ref().map(|r| r.rows.len()), Some(1));
        assert_eq!(ud.actions[0].statement, r#"{"dropUser":"root","$db":"admin"}"#);

        let roles = serde_json::json!({"roles": [
            {"role": "reader", "db": "shop", "isBuiltin": false, "roles": [{"role": "read", "db": "shop"}], "privileges": [{"resource": {"db": "shop", "collection": ""}, "actions": ["find"]}]}
        ]});
        let r = role_summaries(&roles);
        assert_eq!(r[0].detail.as_deref(), Some("1 privileges · inherits read@shop"));
        let rr = ObjectRef { kind: ObjectKind::Role, name: "reader".into(), parent: Some("shop".into()) };
        let rd = role_detail(&rr, &roles["roles"][0], None);
        assert_eq!(rd.rows.as_ref().map(|r| r.columns.len()), Some(2));
        assert_eq!(rd.actions[0].id, "drop");

        let ops = vec![
            serde_json::json!({"opid": 12, "active": true, "op": "query", "ns": "shop.orders", "secs_running": 3, "appName": "db-free"}),
            serde_json::json!({"opid": "shard1:5", "active": false, "op": "none"}),
            serde_json::json!({"desc": "no opid"}),
        ];
        let s = session_summaries(&ops);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].reference.name, "12");
        assert_eq!(s[0].detail.as_deref(), Some("query · shop.orders · 3s · db-free"));
        assert_eq!(s[0].badge.as_deref(), Some("active"));
        assert_eq!(s[1].badge.as_deref(), Some("idle"));
        let sr = ObjectRef { kind: ObjectKind::Session, name: "12".into(), parent: None };
        let sd = session_detail(&sr, &ops[0]);
        assert_eq!(sd.actions[0].statement, r#"{"killOp":1,"op":12,"$db":"admin"}"#);
        let sr2 = ObjectRef { kind: ObjectKind::Session, name: "shard1:5".into(), parent: None };
        assert_eq!(session_detail(&sr2, &ops[1]).actions[0].statement, r#"{"killOp":1,"op":"shard1:5","$db":"admin"}"#);
    }

    #[test]
    fn replicas_shards_settings_profile_map() {
        let status = serde_json::json!({"set": "rs0", "members": [
            {"name": "a:27017", "stateStr": "PRIMARY", "health": 1.0, "optimeDate": "2024-01-01T00:00:10Z", "self": true, "uptime": 100},
            {"name": "b:27017", "stateStr": "SECONDARY", "health": 1.0, "optimeDate": "2024-01-01T00:00:07Z", "syncSourceHost": "a:27017"},
            {"name": "c:27017", "stateStr": "ARBITER", "health": 0.0}
        ]});
        let r = replica_summaries(&status);
        assert_eq!(r[0].badge.as_deref(), Some("primary"));
        assert_eq!(r[0].detail.as_deref(), Some("optime 2024-01-01T00:00:10Z · lag 0s · self"));
        assert_eq!(r[1].detail.as_deref(), Some("optime 2024-01-01T00:00:07Z · lag 3s"));
        assert_eq!(r[2].detail.as_deref(), Some("unhealthy"));
        assert_eq!(r[0].reference.parent.as_deref(), Some("rs0"));
        let rr = ObjectRef { kind: ObjectKind::Replica, name: "b:27017".into(), parent: Some("rs0".into()) };
        let rd = replica_detail(&rr, &status["members"][1], parse_time("2024-01-01T00:00:10Z"));
        assert!(rd.properties.iter().any(|p| p.name == "Replication lag" && p.value == "3 s"));
        assert!(rd.actions.is_empty());

        let shards = shard_summaries(&serde_json::json!({"shards": [{"_id": "s1", "host": "s1/a:1,b:2", "state": 1, "draining": true}]}));
        assert_eq!(shards[0].badge.as_deref(), Some("draining"));
        assert_eq!(shards[0].detail.as_deref(), Some("s1/a:1,b:2"));

        let params = setting_summaries(&serde_json::json!({"quiet": false, "logLevel": 0, "wiredTigerEngineRuntimeConfig": "", "ok": 1, "$clusterTime": {}}));
        let names: Vec<&str> = params.iter().map(|p| p.reference.name.as_str()).collect();
        assert_eq!(names, vec!["logLevel", "quiet", "wiredTigerEngineRuntimeConfig"]);
        assert_eq!(params[1].detail.as_deref(), Some("false"));
        assert_eq!(params[1].badge.as_deref(), Some("bool"));

        let entries = vec![serde_json::json!({"op": "query", "ns": "shop.orders", "ts": "2024-01-01T00:00:00Z", "millis": 250, "planSummary": "COLLSCAN", "docsExamined": 9000})];
        let slow = slow_query_summaries(&entries, "shop");
        assert_eq!(slow[0].reference.name, "query shop.orders @ 2024-01-01T00:00:00Z");
        assert_eq!(slow[0].detail.as_deref(), Some("250 ms · COLLSCAN · 9000 examined"));
        assert_eq!(slow[0].badge.as_deref(), Some("query"));
        let sr = ObjectRef { kind: ObjectKind::SlowQuery, name: slow_query_name(&entries[0]), parent: Some("shop".into()) };
        assert!(slow_query_detail(&sr, &entries[0]).properties.iter().any(|p| p.name == "Duration (ms)" && p.value == "250"));
    }

    #[test]
    fn server_stats_group_figures() {
        let status = serde_json::json!({
            "version": "7.0.5", "host": "box", "process": "mongod", "uptime": 3700,
            "storageEngine": {"name": "wiredTiger"},
            "connections": {"current": 5, "available": 995, "totalCreated": 40, "active": 2},
            "mem": {"resident": 120, "virtual": 1500},
            "opcounters": {"insert": 1, "query": 2, "update": 3, "delete": 4, "getmore": 5, "command": 6},
            "network": {"bytesIn": 2097152, "bytesOut": 1048576, "numRequests": 77},
            "wiredTiger": {"cache": {"bytes currently in the cache": 1048576, "maximum bytes configured": 4194304}},
            "repl": {"setName": "rs0", "primary": "a:27017", "hosts": ["a:27017", "b:27017"]}
        });
        let db_stats = serde_json::json!({"collections": 3, "objects": 42, "dataSize": 1048576, "storageSize": 2097152, "indexes": 4, "indexSize": 524288});
        let groups = server_stat_groups(&status, &db_stats, "shop");
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Connections", "Memory", "Throughput", "Network", "Cache", "Replication", "Database shop"]);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Uptime").map(|s| s.value), Some("1h 1m".into()));
        assert_eq!(find("Connections", "Current").and_then(|s| s.numeric), Some(5.0));
        assert_eq!(find("Memory", "Resident").map(|s| (s.value, s.unit)), Some(("120".into(), Some("MB".into()))));
        assert_eq!(find("Network", "Bytes in").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Cache", "Maximum").and_then(|s| s.numeric), Some(4.0));
        assert_eq!(find("Replication", "Members").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Database shop", "Index size").and_then(|s| s.numeric), Some(0.5));
        assert!(server_stat_groups(&serde_json::json!({}), &serde_json::Value::Null, "x").is_empty());
    }

    fn resolved(url: &str) -> ResolvedConnection {
        // DB_FREE_MONGO_URL=mongodb://host:port — host/port are parsed out of it.
        let hostport = url.trim_start_matches("mongodb://").split('/').next().unwrap_or_default();
        let (host, port) = hostport.split_once(':').unwrap_or((hostport, "27017"));
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Mongodb,
            environment: Environment::Local,
            read_only: false,
            host: Some(host.to_string()),
            port: port.parse().ok(),
            database: Some("dbfree_test".into()),
            username: None,
            password: None,
            file_path: None,
            ssl_mode: SslMode::Disable,
        };
        ResolvedConnection { summary: ConnectionSummary::draft(&input, false), secret: None }
    }

    // WHAT:  Live round trip. Skipped unless DB_FREE_MONGO_URL is set
    //        (e.g. mongodb://127.0.0.1:27018 from a `mongo:7` container).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DB_FREE_MONGO_URL") else {
            return;
        };
        let mongo = connect(&resolved(&url)).await.unwrap_or_else(|e| panic!("connect: {e}"));
        mongo.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert!(mongo.server_version().await.unwrap_or_default().is_some_and(|v| v.starts_with("MongoDB")));

        // Seed through a plain client; the adapter only exposes the trait surface.
        let seed = Client::with_uri_str(&url).await.unwrap_or_else(|e| panic!("seed client: {e}"));
        let people = seed.database("dbfree_test").collection::<Document>("people");
        people.drop().await.unwrap_or_else(|e| panic!("drop: {e}"));
        people
            .insert_many([
                doc! { "name": "Ann", "age": 31, "tags": ["a", "b"], "address": { "city": "Oslo" }, "note": Bson::Null },
                doc! { "name": "Bob", "age": 25, "active": true },
                doc! { "name": "Cara", "age": 40, "balance": 12.5 },
            ])
            .await
            .unwrap_or_else(|e| panic!("insert: {e}"));

        let databases = mongo.databases().await.unwrap_or_else(|e| panic!("databases: {e}"));
        assert!(databases.iter().any(|d| d == "dbfree_test"), "{databases:?}");

        let catalog = mongo.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let people_info = catalog.schemas[0].tables.iter().find(|t| t.name == "people").unwrap_or_else(|| panic!("people missing: {catalog:?}"));
        assert_eq!(people_info.row_estimate, Some(3));

        let table = TableRef { schema: Some("dbfree_test".into()), name: "people".into() };
        let columns = mongo.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names[0], "_id");
        assert!(names.contains(&"tags") && names.contains(&"address") && names.contains(&"balance"), "{names:?}");
        assert!(columns[0].primary_key);

        let query = PageQuery {
            sort: vec![SortRule { column: "age".into(), desc: true }],
            filters: vec![rule("name", FilterOp::Contains, "a")],
            offset: 0,
            limit: 10,
        };
        let page = mongo.fetch_page(&table, &query).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2, "Ann and Cara contain 'a' (case-insensitive)");
        let name_index = page.columns.iter().position(|c| c.name == "name").unwrap_or_default();
        assert_eq!(page.rows[0][name_index], Value::Text("Cara".into()), "sorted by age desc");
        let tags_index = page.columns.iter().position(|c| c.name == "tags").unwrap_or_default();
        assert!(matches!(page.rows[1][tags_index], Value::Json(_)));
        assert_eq!(mongo.count(&table, &query.filters).await.unwrap_or_default(), 2);
        assert_eq!(mongo.row_estimate(&table).await.unwrap_or_default(), Some(3));

        let out = mongo.execute(r#"{"find": "people", "limit": 2}"#, 100).await.unwrap_or_else(|e| panic!("execute: {e}"));
        match out.first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows.len(), 2),
            other => panic!("expected rows, got {other:?}"),
        }
        let out = mongo.execute(r#"find people {"age": {"$gt": 30}}"#, 100).await.unwrap_or_else(|e| panic!("shorthand: {e}"));
        match out.first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows.len(), 2),
            other => panic!("expected rows, got {other:?}"),
        }
        let out = mongo.execute("count people", 100).await.unwrap_or_else(|e| panic!("count: {e}"));
        match out.first() {
            Some(StatementResult::Rows { result }) => {
                let n = result.columns.iter().position(|c| c.name == "n").unwrap_or_default();
                assert_eq!(result.rows[0][n], Value::Int(3));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        assert!(mongo.execute("SELECT 1", 10).await.is_err());

        people.drop().await.unwrap_or_else(|e| panic!("drop: {e}"));
        mongo.close().await;
    }
}
