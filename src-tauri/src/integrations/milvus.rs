// SOT: milvus-integration, milvus-rest-api-v2, vector-collections, milvus-filter-expr, milvus-command-console, zilliz-cloud

use crate::error::{AppError, AppResult};
use crate::integrations::http::{self, json_result, json_to_value, json_type_name, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Milvus adapter over the RESTful API v2 (`/v2/vectordb/...`). Works
//        unchanged against Zilliz Cloud (host is the cluster URL, secret is
//        the API key). A "table" is a collection; columns come from the
//        collection schema (`collections/describe`).
// WHY:   Milvus has a typed schema and a boolean filter expression language,
//        so filters are translated to `field == "x" && n >= 3` expressions and
//        pushed down; `count(*)` is exact. Sort has no server-side equivalent
//        for scalar queries, so it is applied client-side over the page.
// HOW:   Auth is `Bearer user:password` (or `Bearer <token>` when only a
//        secret is set). The `database` field selects the Milvus database
//        (`dbName`), default `default`. `execute` takes JSON envelopes
//        `{"collection": …, "search"|"query"|"insert"|"upsert"|"delete": {…}}`,
//        a raw `{"path": "/v2/vectordb/…", "body": {…}}` passthrough, plus
//        `COLLECTIONS`, `DESCRIBE <c>` and `QUERY <c> [filter]` shorthands.
// WHERE: src-tauri/src/integrations/http.rs, integrations/mod.rs
// ============================================================================

const DEFAULT_PORT: u16 = 19530;
const DEFAULT_DATABASE: &str = "default";
const PAGE_CAP: u64 = 16_384;
const API: &str = "/v2/vectordb";

pub struct MilvusIntegration {
    engine: Engine,
    http: HttpClient,
    database: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let user = s.username.as_deref().map(str::trim).filter(|u| !u.is_empty());
    let secret = conn.secret.as_deref().map(str::trim).filter(|p| !p.is_empty());
    let auth = match (user, secret) {
        (Some(u), Some(p)) => Auth::Bearer(format!("{u}:{p}")),
        (None, Some(p)) => Auth::Bearer(p.to_string()),
        (Some(u), None) => Auth::Bearer(format!("{u}:")),
        (None, None) => Auth::None,
    };
    let is_url = s.host.as_deref().map(|h| h.starts_with("https://")).unwrap_or(false);
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), is_url, auth)?;
    let database = s.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).unwrap_or(DEFAULT_DATABASE).to_string();
    let integration = MilvusIntegration { engine: s.engine, http, database, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

fn quote_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn literal(text: &str) -> String {
    let t = text.trim();
    if t.parse::<i64>().is_ok() || t.parse::<f64>().is_ok() || t == "true" || t == "false" {
        t.to_string()
    } else {
        quote_string(t)
    }
}

fn valid_ident(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        && !name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true)
}

// WHAT:  Filter rules → Milvus boolean expression. Text ops use `like`;
//        `In` becomes `field in [...]`; null checks map to `is null`.
fn filter_expr(filters: &[FilterRule]) -> AppResult<String> {
    let mut parts = Vec::with_capacity(filters.len());
    for f in filters {
        if !valid_ident(&f.column) {
            return Err(AppError::invalid_input(format!("Unsupported field name for filtering: {}", f.column)));
        }
        let col = &f.column;
        let expr = match f.op {
            FilterOp::Eq => format!("{col} == {}", literal(&f.value)),
            FilterOp::Ne => format!("{col} != {}", literal(&f.value)),
            FilterOp::Gt => format!("{col} > {}", literal(&f.value)),
            FilterOp::Gte => format!("{col} >= {}", literal(&f.value)),
            FilterOp::Lt => format!("{col} < {}", literal(&f.value)),
            FilterOp::Lte => format!("{col} <= {}", literal(&f.value)),
            FilterOp::Contains => format!("{col} like {}", quote_string(&format!("%{}%", f.value.trim()))),
            FilterOp::StartsWith => format!("{col} like {}", quote_string(&format!("{}%", f.value.trim()))),
            FilterOp::EndsWith => format!("{col} like {}", quote_string(&format!("%{}", f.value.trim()))),
            FilterOp::In => {
                let items: Vec<String> = f.value.split(',').map(literal).collect();
                format!("{col} in [{}]", items.join(", "))
            }
            FilterOp::IsNull => format!("{col} is null"),
            FilterOp::IsNotNull => format!("{col} is not null"),
        };
        parts.push(format!("({expr})"));
    }
    Ok(parts.join(" && "))
}

fn columns_from_describe(desc: &Json) -> Vec<ColumnInfo> {
    let fields = desc.get("fields").and_then(Json::as_array).cloned().unwrap_or_default();
    let mut cols: Vec<ColumnInfo> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let name = f.get("name").and_then(Json::as_str).unwrap_or("field").to_string();
            let mut data_type = f.get("type").and_then(Json::as_str).unwrap_or("unknown").to_string();
            if let Some(dim) = f.get("params").and_then(Json::as_array).and_then(|ps| {
                ps.iter().find(|p| p.get("key").and_then(Json::as_str) == Some("dim")).and_then(|p| p.get("value"))
            }) {
                let dim = dim.as_str().map(str::to_string).unwrap_or_else(|| dim.to_string());
                data_type = format!("{data_type}({dim})");
            }
            let primary_key = f.get("primaryKey").and_then(Json::as_bool).unwrap_or(false);
            let nullable = f.get("nullable").and_then(Json::as_bool).unwrap_or(!primary_key);
            ColumnInfo { name, data_type, nullable, primary_key, ordinal: i as u32 }
        })
        .collect();
    if desc.get("enableDynamicField").and_then(Json::as_bool).unwrap_or(false) {
        cols.push(ColumnInfo {
            name: "$meta".into(),
            data_type: "JSON".into(),
            nullable: true,
            primary_key: false,
            ordinal: cols.len() as u32,
        });
    }
    cols
}

fn unwrap_data(resp: Json) -> AppResult<Json> {
    let code = resp.get("code").and_then(Json::as_i64).unwrap_or(0);
    if code != 0 {
        let msg = resp.get("message").and_then(Json::as_str).unwrap_or("Milvus error").to_string();
        return Err(if code == 1800 || msg.to_lowercase().contains("auth") {
            AppError::not_connected(format!("Milvus ({code}): {msg}"))
        } else {
            AppError::driver(format!("Milvus ({code}): {msg}"))
        });
    }
    Ok(resp.get("data").cloned().unwrap_or(Json::Null))
}

fn rows_aligned(docs: &[Json], columns: &[ColumnInfo]) -> ResultSet {
    let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut types: Vec<String> = columns.iter().map(|c| c.data_type.clone()).collect();
    for obj in docs.iter().filter_map(Json::as_object) {
        for (k, v) in obj {
            if !names.iter().any(|n| n == k) {
                names.push(k.clone());
                types.push(json_type_name(v).to_string());
            }
        }
    }
    let rows = docs
        .iter()
        .map(|d| {
            let obj = d.as_object();
            names.iter().map(|n| obj.and_then(|o| o.get(n)).map(json_to_value).unwrap_or(Value::Null)).collect()
        })
        .collect();
    ResultSet {
        columns: names.into_iter().zip(types).map(|(name, type_name)| ColumnMeta { name, type_name }).collect(),
        rows,
        truncated: false,
    }
}

#[derive(Debug, PartialEq)]
enum Command {
    Collections,
    Describe(String),
    Query { collection: String, body: Json },
    Search { collection: String, body: Json },
    Insert { collection: String, body: Json },
    Upsert { collection: String, body: Json },
    Delete { collection: String, body: Json },
    Raw { path: String, body: Json },
}

impl Command {
    fn is_mutation(&self) -> bool {
        match self {
            Command::Insert { .. } | Command::Upsert { .. } | Command::Delete { .. } => true,
            Command::Raw { path, .. } => {
                let p = path.to_ascii_lowercase();
                ["/insert", "/upsert", "/delete", "/create", "/drop", "/alter", "/load", "/release", "/rename", "/flush", "/compact", "/grant", "/revoke"]
                    .iter()
                    .any(|m| p.ends_with(m))
            }
            _ => false,
        }
    }
}

fn parse_command(text: &str) -> AppResult<Command> {
    let text = text.trim();
    if text.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if text.starts_with('{') {
        let value: Json = serde_json::from_str(text).map_err(|e| AppError::invalid_input(format!("Invalid JSON: {e}")))?;
        let obj = value.as_object().ok_or_else(|| AppError::invalid_input("Expected a JSON object."))?;
        if let Some(path) = obj.get("path").and_then(Json::as_str) {
            return Ok(Command::Raw { path: path.to_string(), body: obj.get("body").cloned().unwrap_or_else(|| json!({})) });
        }
        let collection = obj
            .get("collection")
            .and_then(Json::as_str)
            .ok_or_else(|| AppError::invalid_input("Missing \"collection\" (or \"path\" for a raw request)."))?
            .to_string();
        let body = |k: &str| obj.get(k).cloned();
        if let Some(body) = body("search") {
            return Ok(Command::Search { collection, body });
        }
        if let Some(body) = body("query") {
            return Ok(Command::Query { collection, body });
        }
        if let Some(body) = body("insert") {
            return Ok(Command::Insert { collection, body });
        }
        if let Some(body) = body("upsert") {
            return Ok(Command::Upsert { collection, body });
        }
        if let Some(body) = body("delete") {
            return Ok(Command::Delete { collection, body });
        }
        return Ok(Command::Describe(collection));
    }
    let mut words = text.splitn(3, char::is_whitespace).map(str::trim);
    let verb = words.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "COLLECTIONS" => Ok(Command::Collections),
        "DESCRIBE" | "INFO" => {
            let c = words.next().filter(|c| !c.is_empty()).ok_or_else(|| AppError::invalid_input("Usage: DESCRIBE <collection>"))?;
            Ok(Command::Describe(c.to_string()))
        }
        "QUERY" => {
            let c = words.next().filter(|c| !c.is_empty()).ok_or_else(|| AppError::invalid_input("Usage: QUERY <collection> [filter]"))?;
            let filter = words.next().unwrap_or("").to_string();
            Ok(Command::Query { collection: c.to_string(), body: json!({"filter": filter, "outputFields": ["*"]}) })
        }
        _ => Err(AppError::invalid_input(
            "Unknown command. Use COLLECTIONS, DESCRIBE <c>, QUERY <c> [filter], or JSON like {\"collection\": \"c\", \"search\": {...}}.",
        )),
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

impl MilvusIntegration {
    async fn call(&self, path: &str, mut body: Json) -> AppResult<Json> {
        if let Some(obj) = body.as_object_mut() {
            obj.entry("dbName").or_insert_with(|| Json::String(self.database.clone()));
        }
        let full = if path.starts_with("/v2/") { path.to_string() } else { format!("{API}/{}", path.trim_start_matches('/')) };
        let resp: Json = self.http.post_json(&full, &body).await?;
        unwrap_data(resp)
    }

    async fn describe(&self, collection: &str) -> AppResult<Json> {
        self.call("collections/describe", json!({"collectionName": collection})).await
    }

    async fn query(&self, collection: &str, filter: &str, limit: u64, offset: u64, output: Json) -> AppResult<Vec<Json>> {
        let body = json!({
            "collectionName": collection,
            "filter": filter,
            "limit": limit,
            "offset": offset,
            "outputFields": output,
        });
        let data = self.call("entities/query", body).await?;
        Ok(data.as_array().cloned().unwrap_or_default())
    }

    async fn run(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        if self.read_only && cmd.is_mutation() {
            return Err(AppError::invalid_input("This connection is read-only; write operations are refused."));
        }
        let cap = |body: &mut Json| {
            if body.get("limit").is_none() {
                body["limit"] = json!(max_rows.min(PAGE_CAP as usize));
            }
        };
        match cmd {
            Command::Collections => {
                let data = self.call("collections/list", json!({})).await?;
                let docs: Vec<Json> = data.as_array().cloned().unwrap_or_default().into_iter().map(|n| json!({"name": n})).collect();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, Some("name"), max_rows) })
            }
            Command::Describe(c) => Ok(StatementResult::Rows { result: json_result(self.describe(&c).await?) }),
            Command::Query { collection, mut body } => {
                cap(&mut body);
                body["collectionName"] = json!(collection);
                if body.get("outputFields").is_none() {
                    body["outputFields"] = json!(["*"]);
                }
                let data = self.call("entities/query", body).await?;
                let docs = data.as_array().cloned().unwrap_or_default();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, None, max_rows) })
            }
            Command::Search { collection, mut body } => {
                cap(&mut body);
                body["collectionName"] = json!(collection);
                if body.get("outputFields").is_none() {
                    body["outputFields"] = json!(["*"]);
                }
                let data = self.call("entities/search", body).await?;
                let docs = data.as_array().cloned().unwrap_or_default();
                Ok(StatementResult::Rows { result: objects_to_result_set(&docs, None, max_rows) })
            }
            Command::Insert { collection, mut body } => {
                body["collectionName"] = json!(collection);
                let data = self.call("entities/insert", body).await?;
                let n = data.get("insertCount").and_then(Json::as_u64).unwrap_or(0);
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Upsert { collection, mut body } => {
                body["collectionName"] = json!(collection);
                let data = self.call("entities/upsert", body).await?;
                let n = data.get("upsertCount").and_then(Json::as_u64).unwrap_or(0);
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Delete { collection, mut body } => {
                body["collectionName"] = json!(collection);
                let data = self.call("entities/delete", body).await?;
                let n = data.get("deleteCount").and_then(Json::as_u64).unwrap_or(0);
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Raw { path, body } => {
                let data = self.call(&path, body).await?;
                Ok(StatementResult::Rows { result: json_result(data) })
            }
        }
    }
}

#[async_trait]
impl Integration for MilvusIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: false,
            namespaces: true,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.call("collections/list", json!({})).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        // v2 REST has no version endpoint; the legacy v1 `/v1/vector/collections` and
        // the metrics endpoint are not guaranteed either. Report the API level.
        Ok(Some(format!("Milvus (REST v2, db {})", self.database)))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        match self.call("databases/list", json!({})).await {
            Ok(data) => {
                let mut names: Vec<String> = data.as_array().map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();
                if names.is_empty() {
                    names.push(self.database.clone());
                }
                Ok(names)
            }
            Err(_) => Ok(vec![self.database.clone()]),
        }
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let data = self.call("collections/list", json!({})).await?;
        let mut names: Vec<String> = data.as_array().map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();
        names.sort();
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let row_estimate = self.row_estimate(&TableRef { schema: None, name: name.clone() }).await.unwrap_or(None);
            tables.push(TableInfo { schema: Some(self.database.clone()), name, kind: TableKind::Table, row_estimate });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let desc = self.describe(&table.name).await?;
        let cols = columns_from_describe(&desc);
        if cols.is_empty() {
            return Err(AppError::driver(format!("Collection {} has no fields.", table.name)));
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let filter = filter_expr(filters)?;
        let body = json!({"collectionName": table.name, "filter": filter, "outputFields": ["count(*)"]});
        let data = self.call("entities/query", body).await?;
        let n = data
            .as_array()
            .and_then(|a| a.first())
            .and_then(|row| row.get("count(*)"))
            .and_then(Json::as_i64)
            .unwrap_or(0);
        Ok(n)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let filter = filter_expr(&query.filters)?;
        let columns = self.columns(table).await?;
        let limit = u64::from(query.limit).clamp(1, PAGE_CAP);
        let docs = self.query(&table.name, &filter, limit, query.offset, json!(["*"])).await?;
        let rs = rows_aligned(&docs, &columns);
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        let mut rows = rs.rows;
        http::local::apply_sort(&names, &mut rows, &query.sort);
        Ok(ResultSet { columns: rs.columns, rows, truncated: false })
    }

    async fn execute(&self, text: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let cmd = parse_command(text)?;
        Ok(vec![self.run(cmd, max_rows).await?])
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    #[test]
    fn builds_filter_expressions() {
        let rules = vec![
            FilterRule { column: "city".into(), op: FilterOp::Eq, value: "Ber\"lin".into() },
            FilterRule { column: "age".into(), op: FilterOp::Gte, value: "18".into() },
            FilterRule { column: "name".into(), op: FilterOp::Contains, value: "an".into() },
            FilterRule { column: "tag".into(), op: FilterOp::In, value: "a, 2".into() },
            FilterRule { column: "x".into(), op: FilterOp::IsNull, value: String::new() },
        ];
        let expr = filter_expr(&rules).unwrap_or_default();
        assert_eq!(expr, r#"(city == "Ber\"lin") && (age >= 18) && (name like "%an%") && (tag in ["a", 2]) && (x is null)"#);
        assert!(filter_expr(&[FilterRule { column: "bad name".into(), op: FilterOp::Eq, value: "1".into() }]).is_err());
        assert!(filter_expr(&[]).unwrap_or_default().is_empty());
    }

    #[test]
    fn describe_to_columns() {
        let desc = json!({
            "fields": [
                {"name": "id", "type": "Int64", "primaryKey": true},
                {"name": "vec", "type": "FloatVector", "params": [{"key": "dim", "value": "128"}]},
                {"name": "title", "type": "VarChar", "nullable": true}
            ],
            "enableDynamicField": true
        });
        let cols = columns_from_describe(&desc);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "vec", "title", "$meta"]);
        assert!(cols[0].primary_key && !cols[0].nullable);
        assert_eq!(cols[1].data_type, "FloatVector(128)");
    }

    #[test]
    fn unwraps_data_and_errors() {
        assert_eq!(unwrap_data(json!({"code": 0, "data": [1]})).ok(), Some(json!([1])));
        assert!(unwrap_data(json!({"code": 1100, "message": "collection not found"})).is_err());
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse_command("collections").ok(), Some(Command::Collections));
        assert_eq!(parse_command("describe books").ok(), Some(Command::Describe("books".into())));
        assert_eq!(
            parse_command("QUERY books id > 3").ok(),
            Some(Command::Query { collection: "books".into(), body: json!({"filter": "id > 3", "outputFields": ["*"]}) })
        );
        let search = parse_command(r#"{"collection":"books","search":{"data":[[0.1]],"annsField":"vec","limit":2}}"#).ok();
        assert!(matches!(search, Some(Command::Search { .. })));
        let raw = parse_command(r#"{"path":"/v2/vectordb/collections/list"}"#).ok();
        assert_eq!(raw, Some(Command::Raw { path: "/v2/vectordb/collections/list".into(), body: json!({}) }));
        assert!(Command::Raw { path: "/v2/vectordb/entities/delete".into(), body: json!({}) }.is_mutation());
        assert!(!Command::Raw { path: "/v2/vectordb/collections/list".into(), body: json!({}) }.is_mutation());
        assert!(parse_command("SELECT 1").is_err());
    }

    #[test]
    fn aligns_rows_to_schema() {
        let cols = columns_from_describe(&json!({"fields": [{"name": "id", "type": "Int64", "primaryKey": true}, {"name": "t", "type": "VarChar"}]}));
        let rs = rows_aligned(&[json!({"id": 1, "t": "a", "extra": true})], &cols);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "t", "extra"]);
        assert_eq!(rs.rows[0][2], Value::Bool(true));
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_MILVUS_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Milvus,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: None,
                username: std::env::var("DBFREE_TEST_MILVUS_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: false,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_MILVUS_SECRET").ok(),
        };
        let m = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let _ = m.execute(r#"{"path":"collections/drop","body":{"collectionName":"dbfree_test"}}"#, 10).await;
        m.execute(r#"{"path":"collections/create","body":{"collectionName":"dbfree_test","dimension":2}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        m.execute(r#"{"collection":"dbfree_test","upsert":{"data":[{"id":1,"vector":[0.1,0.9],"city":"Berlin"},{"id":2,"vector":[0.9,0.1],"city":"Paris"}]}}"#, 10)
            .await
            .unwrap_or_else(|e| panic!("upsert: {e}"));
        let table = TableRef { schema: None, name: "dbfree_test".into() };
        let cols = m.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.primary_key));
        let filters = vec![FilterRule { column: "city".into(), op: FilterOp::Eq, value: "Paris".into() }];
        let page = m
            .fetch_page(&table, &PageQuery { sort: vec![], filters, offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1);
        let _ = m.execute(r#"{"path":"collections/drop","body":{"collectionName":"dbfree_test"}}"#, 10).await;
    }
}
