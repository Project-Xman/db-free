// SOT: meilisearch-integration, meilisearch-rest-api, meilisearch-filter-syntax, meilisearch-console

use crate::error::{AppError, AppResult};
use crate::integrations::http::{json_result, local, objects_to_result_set, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet,
    SchemaCatalog, SchemaInfo, StatementResult, TableInfo, TableKind, TableRef,
};
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Meilisearch adapter (REST, port 7700, `Authorization: Bearer <key>`).
//        An index is a table, a document a row; columns are the union of keys
//        in a 50-document sample with the index's primaryKey marked.
// WHY:   Meilisearch has no schema and no general query language; the
//        documents endpoint pages with offset/limit and accepts its own filter
//        expressions, so the grid maps almost 1:1. Text matching (Contains…)
//        and sorting are not available on that endpoint and run client-side.
// HOW:   `execute` takes JSON `{"index": "uid", "q": "…", …search params}` →
//        POST /indexes/{uid}/search; `{"index", "documents": [...]}` →
//        add-or-replace; `{"method","path","body"}` → raw passthrough; and
//        the shorthands `SEARCH <index> <text>` and `INDEXES`.
// WHERE: src-tauri/src/integrations/mod.rs (trait), integrations/http.rs (client)
// ============================================================================

const DEFAULT_PORT: u16 = 7700;
const SAMPLE_SIZE: usize = 50;
const LOCAL_CAP: u64 = 5_000;
const SCHEMA: &str = "indexes";

pub struct MeilisearchIntegration {
    http: HttpClient,
    read_only: bool,
    default_index: Option<String>,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = match conn.secret.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(key) => Auth::Bearer(key.to_string()),
        None => Auth::None,
    };
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), false, auth)?;
    let default_index = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let integration = MeilisearchIntegration { http, read_only: conn.summary.read_only, default_index };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Filter translation
// ---------------------------------------------------------------------------

fn filter_literal(raw: &str) -> String {
    let t = raw.trim();
    if t.parse::<f64>().is_ok() || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        return t.to_string();
    }
    format!("'{}'", t.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn quote_attr(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-') {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\\\""))
    }
}

/// Server-side expression for one rule, or None when it must run locally.
fn filter_expr(rule: &FilterRule) -> Option<String> {
    let attr = quote_attr(&rule.column);
    let v = rule.value.trim();
    Some(match rule.op {
        FilterOp::Eq => format!("{attr} = {}", filter_literal(v)),
        FilterOp::Ne => format!("{attr} != {}", filter_literal(v)),
        FilterOp::Gt => format!("{attr} > {}", filter_literal(v)),
        FilterOp::Gte => format!("{attr} >= {}", filter_literal(v)),
        FilterOp::Lt => format!("{attr} < {}", filter_literal(v)),
        FilterOp::Lte => format!("{attr} <= {}", filter_literal(v)),
        FilterOp::In => {
            let items: Vec<String> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(filter_literal).collect();
            format!("{attr} IN [{}]", items.join(", "))
        }
        FilterOp::IsNull => format!("{attr} IS NULL"),
        FilterOp::IsNotNull => format!("{attr} EXISTS AND {attr} IS NOT NULL"),
        FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith => return None,
    })
}

// WHAT:  Splits the rules into (server filter string, rules to apply locally).
fn split_filters(filters: &[FilterRule]) -> (Option<String>, Vec<FilterRule>) {
    let mut server = Vec::new();
    let mut local_rules = Vec::new();
    for rule in filters {
        match filter_expr(rule) {
            Some(expr) => server.push(expr),
            None => local_rules.push(rule.clone()),
        }
    }
    let server = if server.is_empty() { None } else { Some(server.join(" AND ")) };
    (server, local_rules)
}

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

fn columns_from_docs(docs: &[Json], primary_key: Option<&str>) -> Vec<ColumnInfo> {
    let set = objects_to_result_set(docs, primary_key, docs.len());
    set.columns
        .into_iter()
        .enumerate()
        .map(|(i, c)| ColumnInfo {
            primary_key: primary_key == Some(c.name.as_str()),
            name: c.name,
            data_type: c.type_name,
            nullable: true,
            ordinal: u32::try_from(i + 1).unwrap_or(u32::MAX),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Console commands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Command {
    Search { index: String, params: Json },
    AddDocuments { index: String, documents: Vec<Json>, primary_key: Option<String> },
    Delete { index: String, ids: Vec<Json> },
    Rest { method: String, path: String, body: Option<Json> },
    Indexes,
}

fn parse_command(text: &str, default_index: Option<&str>) -> AppResult<Command> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if trimmed.starts_with('{') {
        let mut body: Json = serde_json::from_str(trimmed).map_err(|e| AppError::invalid_input(format!("Command is not valid JSON: {e}")))?;
        let obj = body.as_object_mut().ok_or_else(|| AppError::invalid_input("Command must be a JSON object."))?;
        if let Some(method) = obj.get("method").and_then(Json::as_str).map(str::to_ascii_uppercase) {
            let path = obj.get("path").and_then(Json::as_str).ok_or_else(|| AppError::invalid_input("Passthrough needs \"path\"."))?.to_string();
            let body = obj.get("body").cloned().filter(|b| !b.is_null());
            return Ok(Command::Rest { method, path, body });
        }
        let index = obj
            .remove("index")
            .and_then(|v| v.as_str().map(str::to_string))
            .or_else(|| default_index.map(str::to_string))
            .ok_or_else(|| AppError::invalid_input("Add an \"index\" key (or set the connection's index)."))?;
        if let Some(docs) = obj.remove("documents") {
            let documents = docs.as_array().cloned().ok_or_else(|| AppError::invalid_input("\"documents\" must be an array of objects."))?;
            let primary_key = obj.remove("primaryKey").and_then(|v| v.as_str().map(str::to_string));
            return Ok(Command::AddDocuments { index, documents, primary_key });
        }
        if let Some(ids) = obj.remove("delete") {
            let ids = ids.as_array().cloned().ok_or_else(|| AppError::invalid_input("\"delete\" must be an array of document ids."))?;
            return Ok(Command::Delete { index, ids });
        }
        return Ok(Command::Search { index, params: Json::Object(std::mem::take(obj)) });
    }
    let mut parts = trimmed.splitn(3, char::is_whitespace);
    let verb = parts.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "INDEXES" | "INDICES" => Ok(Command::Indexes),
        "SEARCH" => {
            let index = parts.next().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| AppError::invalid_input("Usage: SEARCH <index> <query text>"))?;
            let q = parts.next().map(str::trim).unwrap_or_default();
            Ok(Command::Search { index: index.to_string(), params: json!({"q": q}) })
        }
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" => {
            let path = parts.next().map(str::trim).filter(|s| !s.is_empty()).ok_or_else(|| AppError::invalid_input("Expected a path, e.g. `GET /stats`."))?;
            let body = match parts.next().map(str::trim).filter(|s| !s.is_empty()) {
                Some(raw) => Some(serde_json::from_str::<Json>(raw).map_err(|e| AppError::invalid_input(format!("Body is not valid JSON: {e}")))?),
                None => None,
            };
            let path = if path.starts_with('/') { path.to_string() } else { format!("/{path}") };
            Ok(Command::Rest { method: verb, path, body })
        }
        _ => Err(AppError::invalid_input("Enter JSON ({\"index\": \"movies\", \"q\": \"…\"}), `SEARCH <index> <text>`, `INDEXES`, or `GET /path`.")),
    }
}

fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.trim().is_empty() {
                out.push(std::mem::take(&mut current));
            }
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

fn search_result(body: &Json, max_rows: usize, primary_key: Option<&str>) -> ResultSet {
    match body.get("hits").and_then(Json::as_array) {
        Some(hits) if !hits.is_empty() => objects_to_result_set(hits, primary_key, max_rows),
        Some(_) => ResultSet { columns: vec![ColumnMeta { name: "hits".into(), type_name: "array".into() }], rows: vec![], truncated: false },
        None => json_result(body.clone()),
    }
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

impl MeilisearchIntegration {
    async fn primary_key(&self, index: &str) -> AppResult<Option<String>> {
        let info: Json = self.http.get_json(&format!("/indexes/{index}")).await?;
        Ok(info.get("primaryKey").and_then(Json::as_str).map(str::to_string))
    }

    async fn stats_count(&self, index: &str) -> AppResult<i64> {
        let stats: Json = self.http.get_json(&format!("/indexes/{index}/stats")).await?;
        Ok(stats.get("numberOfDocuments").and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_documents(&self, index: &str, offset: u64, limit: u64, filter: Option<&str>) -> AppResult<(Vec<Json>, i64)> {
        let mut body = json!({"offset": offset, "limit": limit});
        if let Some(f) = filter {
            body["filter"] = Json::String(f.to_string());
        }
        let out: Json = self.http.post_json(&format!("/indexes/{index}/documents/fetch"), &body).await?;
        let docs = out.get("results").and_then(Json::as_array).cloned().unwrap_or_default();
        let total = out.get("total").and_then(Json::as_i64).unwrap_or(docs.len() as i64);
        Ok((docs, total))
    }

    fn refuse_if_read_only(&self, what: &str) -> AppResult<()> {
        if self.read_only {
            return Err(AppError::invalid_input(format!("{what} is refused: this connection is read-only.")));
        }
        Ok(())
    }

    async fn run_command(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        match cmd {
            Command::Indexes => {
                let out: Json = self.http.get_json("/indexes?limit=1000").await?;
                let list = out.get("results").cloned().unwrap_or(Json::Array(vec![]));
                Ok(StatementResult::Rows { result: json_result(list) })
            }
            Command::Search { index, mut params } => {
                if params.get("limit").is_none() {
                    params["limit"] = Json::from(max_rows.min(1000));
                }
                let pk = self.primary_key(&index).await.unwrap_or(None);
                let out: Json = self.http.post_json(&format!("/indexes/{index}/search"), &params).await?;
                Ok(StatementResult::Rows { result: search_result(&out, max_rows, pk.as_deref()) })
            }
            Command::AddDocuments { index, documents, primary_key } => {
                self.refuse_if_read_only("Adding documents")?;
                let n = documents.len() as u64;
                let path = match primary_key {
                    Some(pk) => format!("/indexes/{index}/documents?primaryKey={pk}"),
                    None => format!("/indexes/{index}/documents"),
                };
                let _: Json = self.http.post_json(&path, &Json::Array(documents)).await?;
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Delete { index, ids } => {
                self.refuse_if_read_only("Deleting documents")?;
                let n = ids.len() as u64;
                let _: Json = self.http.post_json(&format!("/indexes/{index}/documents/delete-batch"), &Json::Array(ids)).await?;
                Ok(StatementResult::Affected { rows_affected: n })
            }
            Command::Rest { method, path, body } => {
                if method != "GET" && !(method == "POST" && path.contains("/search")) && !(method == "POST" && path.ends_with("/documents/fetch")) {
                    self.refuse_if_read_only(&format!("{method} {path}"))?;
                }
                let m = Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Unsupported verb {method}.")))?;
                let mut req = self.http.request(m, &path);
                if let Some(b) = body {
                    req = req.json(&b);
                }
                let resp = self.http.send(req).await?;
                let text = resp.text().await.map_err(|e| AppError::driver(e.to_string()))?;
                let parsed: Json = serde_json::from_str(&text).unwrap_or(Json::String(text));
                if let Some(hits) = parsed.get("hits").filter(|h| h.is_array()) {
                    return Ok(StatementResult::Rows { result: objects_to_result_set(hits.as_array().map(Vec::as_slice).unwrap_or_default(), None, max_rows) });
                }
                if let Some(results) = parsed.get("results").and_then(Json::as_array).filter(|r| !r.is_empty() && r.iter().all(Json::is_object)) {
                    return Ok(StatementResult::Rows { result: objects_to_result_set(results, None, max_rows) });
                }
                Ok(StatementResult::Rows { result: json_result(parsed) })
            }
        }
    }
}

#[async_trait]
impl Integration for MeilisearchIntegration {
    fn engine(&self) -> Engine {
        Engine::Meilisearch
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: false, fixed_columns: false, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true }
    }

    async fn ping(&self) -> AppResult<()> {
        let _: Json = self.http.get_json("/version").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v: Json = self.http.get_json("/version").await?;
        Ok(v.get("pkgVersion").and_then(Json::as_str).map(|s| format!("Meilisearch {s}")))
    }

    fn current_database(&self) -> Option<String> {
        Some(SCHEMA.into())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![SCHEMA.into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let out: Json = self.http.get_json("/indexes?limit=1000").await?;
        let mut tables = Vec::new();
        for idx in out.get("results").and_then(Json::as_array).into_iter().flatten() {
            let Some(uid) = idx.get("uid").and_then(Json::as_str) else { continue };
            let row_estimate = self.stats_count(uid).await.ok();
            tables.push(TableInfo { schema: Some(SCHEMA.into()), name: uid.to_string(), kind: TableKind::Table, row_estimate });
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: SCHEMA.into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let pk = self.primary_key(&table.name).await?;
        let (docs, _) = self.fetch_documents(&table.name, 0, SAMPLE_SIZE as u64, None).await?;
        let mut cols = columns_from_docs(&docs, pk.as_deref());
        if cols.is_empty() {
            cols.push(ColumnInfo { name: pk.unwrap_or_else(|| "id".into()), data_type: "string".into(), nullable: false, primary_key: true, ordinal: 1 });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.stats_count(&table.name).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (server, local_rules) = split_filters(filters);
        if local_rules.is_empty() {
            if server.is_none() {
                return self.stats_count(&table.name).await;
            }
            let (_, total) = self.fetch_documents(&table.name, 0, 1, server.as_deref()).await?;
            return Ok(total);
        }
        let (docs, _) = self.fetch_documents(&table.name, 0, LOCAL_CAP, server.as_deref()).await?;
        let set = objects_to_result_set(&docs, None, docs.len());
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        Ok(local::apply_filters(&names, set.rows, &local_rules).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let pk = self.primary_key(&table.name).await?;
        let (server, local_rules) = split_filters(&query.filters);
        let needs_local = !local_rules.is_empty() || !query.sort.is_empty();
        if !needs_local {
            let (docs, _) = self.fetch_documents(&table.name, query.offset, u64::from(query.limit), server.as_deref()).await?;
            return Ok(objects_to_result_set(&docs, pk.as_deref(), query.limit as usize));
        }
        // Sorting needs the whole (capped) set; text filters shrink it afterwards.
        let (docs, _) = self.fetch_documents(&table.name, 0, LOCAL_CAP, server.as_deref()).await?;
        let mut set = objects_to_result_set(&docs, pk.as_deref(), docs.len());
        let names: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery { sort: query.sort.clone(), filters: local_rules, offset: query.offset, limit: query.limit };
        set.rows = local::page(&names, set.rows, &local_query);
        set.truncated = false;
        Ok(set)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut results = Vec::new();
        for stmt in split_statements(sql) {
            let cmd = parse_command(&stmt, self.default_index.as_deref())?;
            results.push(self.run_command(cmd, max_rows).await?);
        }
        Ok(results)
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Value;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode, SortRule};

    #[test]
    fn filter_expressions() {
        let f = |op, v: &str| FilterRule { column: "genre".into(), op, value: v.into() };
        assert_eq!(filter_expr(&f(FilterOp::Eq, "sci-fi")).as_deref(), Some("genre = 'sci-fi'"));
        assert_eq!(filter_expr(&f(FilterOp::Eq, "it's")).as_deref(), Some("genre = 'it\\'s'"));
        assert_eq!(filter_expr(&f(FilterOp::Gt, "3")).as_deref(), Some("genre > 3"));
        assert_eq!(filter_expr(&f(FilterOp::In, "a, 2,b")).as_deref(), Some("genre IN ['a', 2, 'b']"));
        assert_eq!(filter_expr(&f(FilterOp::IsNull, "")).as_deref(), Some("genre IS NULL"));
        assert!(filter_expr(&f(FilterOp::Contains, "x")).is_none());
        let (server, local_rules) = split_filters(&[f(FilterOp::Eq, "a"), f(FilterOp::Contains, "b"), f(FilterOp::Lte, "5")]);
        assert_eq!(server.as_deref(), Some("genre = 'a' AND genre <= 5"));
        assert_eq!(local_rules.len(), 1);
        assert_eq!(quote_attr("weird name"), "\"weird name\"");
    }

    #[test]
    fn columns_mark_primary_key() {
        let docs = vec![json!({"id": 1, "title": "A"}), json!({"id": 2, "year": 1999})];
        let cols = columns_from_docs(&docs, Some("id"));
        assert_eq!(cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id", "title", "year"]);
        assert!(cols[0].primary_key);
        assert!(!cols[1].primary_key);
        assert_eq!(cols[2].data_type, "integer");
    }

    #[test]
    fn command_parsing() {
        assert_eq!(parse_command("INDEXES", None).ok(), Some(Command::Indexes));
        assert_eq!(parse_command("search movies star wars", None).ok(), Some(Command::Search { index: "movies".into(), params: json!({"q": "star wars"}) }));
        assert_eq!(
            parse_command("{\"index\":\"movies\",\"q\":\"x\",\"limit\":5}", None).ok(),
            Some(Command::Search { index: "movies".into(), params: json!({"q": "x", "limit": 5}) })
        );
        assert_eq!(parse_command("{\"q\":\"x\"}", Some("dflt")).ok(), Some(Command::Search { index: "dflt".into(), params: json!({"q": "x"}) }));
        assert!(parse_command("{\"q\":\"x\"}", None).is_err());
        match parse_command("{\"index\":\"m\",\"documents\":[{\"id\":1}],\"primaryKey\":\"id\"}", None) {
            Ok(Command::AddDocuments { index, documents, primary_key }) => {
                assert_eq!(index, "m");
                assert_eq!(documents.len(), 1);
                assert_eq!(primary_key.as_deref(), Some("id"));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(parse_command("{\"index\":\"m\",\"delete\":[1,2]}", None).ok(), Some(Command::Delete { index: "m".into(), ids: vec![json!(1), json!(2)] }));
        assert_eq!(
            parse_command("{\"method\":\"get\",\"path\":\"/stats\"}", None).ok(),
            Some(Command::Rest { method: "GET".into(), path: "/stats".into(), body: None })
        );
        assert_eq!(parse_command("GET stats", None).ok(), Some(Command::Rest { method: "GET".into(), path: "/stats".into(), body: None }));
        assert_eq!(
            parse_command("POST /indexes/m/search {\"q\":\"a\"}", None).ok(),
            Some(Command::Rest { method: "POST".into(), path: "/indexes/m/search".into(), body: Some(json!({"q": "a"})) })
        );
        assert!(parse_command("SELECT 1", None).is_err());
        assert_eq!(split_statements("INDEXES\n\nSEARCH m x\n").len(), 2);
    }

    #[test]
    fn search_hits_become_rows() {
        let body = json!({"hits": [{"id": 1, "t": "a"}], "estimatedTotalHits": 1});
        let set = search_result(&body, 10, Some("id"));
        assert_eq!(set.columns[0].name, "id");
        assert_eq!(set.rows.len(), 1);
        assert_eq!(search_result(&json!({"hits": []}), 10, None).rows.len(), 0);
        assert_eq!(search_result(&json!({"taskUid": 1}), 10, None).columns[0].name, "result");
    }

    // WHAT:  Live round trip. Skipped unless DBFREE_TEST_MEILISEARCH_URL is set.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_MEILISEARCH_URL") else {
            return;
        };
        let input = ConnectionInput {
            name: "live".into(),
            engine: Engine::Meilisearch,
            environment: Environment::Local,
            read_only: false,
            host: Some(url),
            port: None,
            database: None,
            username: None,
            password: None,
            file_path: None,
            ssl_mode: SslMode::Prefer,
        };
        let secret = std::env::var("DBFREE_TEST_MEILISEARCH_KEY").ok();
        let resolved = ResolvedConnection { summary: ConnectionSummary::draft(&input, secret.is_some()), secret };
        let m = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        assert!(m.server_version().await.unwrap_or_default().unwrap_or_default().starts_with("Meilisearch"));
        m.execute("{\"index\":\"dbfree_t\",\"documents\":[{\"id\":1,\"name\":\"alpha\",\"n\":1},{\"id\":2,\"name\":\"beta\",\"n\":2},{\"id\":3,\"name\":\"gamma\",\"n\":3}],\"primaryKey\":\"id\"}", 10)
            .await
            .unwrap_or_else(|e| panic!("add: {e}"));
        // Make n filterable and wait for tasks.
        m.execute("PATCH /indexes/dbfree_t/settings {\"filterableAttributes\":[\"n\",\"name\"]}", 10).await.unwrap_or_else(|e| panic!("settings: {e}"));
        for _ in 0..50 {
            let tasks: Json = m_http(&m).get_json("/tasks?statuses=enqueued,processing").await.unwrap_or(Json::Null);
            if tasks.get("results").and_then(Json::as_array).map(Vec::is_empty).unwrap_or(true) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let table = TableRef { schema: Some(SCHEMA.into()), name: "dbfree_t".into() };
        let cat = m.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == "dbfree_t"));
        let cols = m.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols[0].primary_key && cols[0].name == "id", "{cols:?}");
        let q = PageQuery {
            sort: vec![SortRule { column: "n".into(), desc: true }],
            filters: vec![FilterRule { column: "n".into(), op: FilterOp::Gte, value: "2".into() }, FilterRule { column: "name".into(), op: FilterOp::Contains, value: "a".into() }],
            offset: 0,
            limit: 10,
        };
        let page = m.fetch_page(&table, &q).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2, "{page:?}");
        assert_eq!(page.rows[0][0], Value::Int(3));
        assert_eq!(m.count(&table, &q.filters).await.unwrap_or_default(), 2);
        let out = m.execute("SEARCH dbfree_t beta", 10).await.unwrap_or_else(|e| panic!("search: {e}"));
        match &out[0] {
            StatementResult::Rows { result } => assert_eq!(result.rows.len(), 1),
            other => panic!("unexpected {other:?}"),
        }
        m.execute("DELETE /indexes/dbfree_t", 10).await.ok();
    }

    #[cfg(test)]
    fn m_http(m: &Arc<dyn Integration>) -> HttpClient {
        let url = std::env::var("DBFREE_TEST_MEILISEARCH_URL").unwrap_or_default();
        let key = std::env::var("DBFREE_TEST_MEILISEARCH_KEY").ok();
        let _ = m;
        HttpClient::new(url, key.map(Auth::Bearer).unwrap_or(Auth::None), false).unwrap_or_else(|e| panic!("{e}"))
    }
}
