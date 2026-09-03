// SOT: mongodb-integration, mongodb-adapter, document-mapping, bson-value-decoding, mongo-command-console

use crate::error::{AppError, AppResult};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, SortRule, SslMode, StatementResult, TableInfo, TableKind, TableRef,
    Value,
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
//        returned (no getMore), capped at `max_rows`.
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

#[async_trait]
impl Integration for MongoIntegration {
    fn engine(&self) -> Engine {
        Engine::Mongodb
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: true, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: false }
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
            let command = parse_command(&statement, max_rows)?;
            let reply = db.run_command(command).await?;
            out.push(reply_to_result(reply, max_rows));
        }
        Ok(out)
    }

    async fn close(&self) {
        self.client.clone().shutdown().await;
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
