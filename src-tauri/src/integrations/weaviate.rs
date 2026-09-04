// SOT: weaviate-integration, weaviate-rest-api, graphql, vector-classes, weaviate-aggregate, weaviate-command-console

use crate::error::{AppError, AppResult};
use crate::integrations::http::{self, json_result, json_to_value, json_type_name, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  Weaviate adapter over the REST + GraphQL APIs. A "table" is a class
//        from `/v1/schema`; columns are the class properties plus `_id` (the
//        object uuid, primary key) and `_additional` (creation time, vector…).
// WHY:   The object listing endpoint (`GET /v1/objects`) supports offset
//        paging and sorting but no filtering, and GraphQL `Get` supports
//        `where` but not offset paging beyond `QUERY_MAXIMUM_RESULTS`. We use
//        the REST listing for browsing (server-side sort where the sort column
//        is a property, offset/limit) and fall back to a bounded window +
//        `http::local` when filters are present. Counts use GraphQL
//        `Aggregate { Class { meta { count } } }` with a translated `where`.
// HOW:   Auth is `Bearer <secret>` (Weaviate API key). `execute` accepts a
//        GraphQL document (`{ Get { … } }`), a JSON `{"query": "…"}` body,
//        or a raw `{"path","method","body"}` passthrough; mutations are refused
//        on read-only connections.
// WHERE: src-tauri/src/integrations/http.rs, integrations/mod.rs
// ============================================================================

const DEFAULT_PORT: u16 = 8080;
const ID_COLUMN: &str = "_id";
const ADDITIONAL_COLUMN: &str = "_additional";
const WINDOW_CAP: u64 = 5_000;

pub struct WeaviateIntegration {
    engine: Engine,
    http: HttpClient,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let auth = match conn.secret.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(key) => Auth::Bearer(key.to_string()),
        None => Auth::None,
    };
    let is_url = conn.summary.host.as_deref().map(|h| h.starts_with("https://")).unwrap_or(false);
    let http = HttpClient::from_connection(conn, Some(DEFAULT_PORT), is_url, auth)?;
    let integration = WeaviateIntegration { engine: conn.summary.engine, http, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn gql_string(s: &str) -> String {
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

// WHAT:  One filter rule → GraphQL `where` operand text, or None when the rule
//        needs client-side evaluation (`_additional`, IsNotNull, In on text…).
fn where_operand(rule: &FilterRule) -> Option<String> {
    if !valid_name(&rule.column) || rule.column == ADDITIONAL_COLUMN {
        return None;
    }
    let path = if rule.column == ID_COLUMN { "id".to_string() } else { rule.column.clone() };
    let v = rule.value.trim();
    let is_int = v.parse::<i64>().is_ok();
    let is_num = v.parse::<f64>().is_ok();
    let is_bool = v == "true" || v == "false";
    let typed = |op: &str| {
        let value = if is_int {
            format!("valueInt: {v}")
        } else if is_num {
            format!("valueNumber: {v}")
        } else if is_bool {
            format!("valueBoolean: {v}")
        } else {
            format!("valueText: {}", gql_string(v))
        };
        Some(format!("{{ path: [{}], operator: {op}, {value} }}", gql_string(&path)))
    };
    match rule.op {
        FilterOp::Eq => typed("Equal"),
        FilterOp::Ne => typed("NotEqual"),
        FilterOp::Gt if is_num => typed("GreaterThan"),
        FilterOp::Gte if is_num => typed("GreaterThanEqual"),
        FilterOp::Lt if is_num => typed("LessThan"),
        FilterOp::Lte if is_num => typed("LessThanEqual"),
        FilterOp::Contains if !is_num && !is_bool => {
            Some(format!("{{ path: [{}], operator: Like, valueText: {} }}", gql_string(&path), gql_string(&format!("*{v}*"))))
        }
        FilterOp::StartsWith if !is_num && !is_bool => {
            Some(format!("{{ path: [{}], operator: Like, valueText: {} }}", gql_string(&path), gql_string(&format!("{v}*"))))
        }
        FilterOp::EndsWith if !is_num && !is_bool => {
            Some(format!("{{ path: [{}], operator: Like, valueText: {} }}", gql_string(&path), gql_string(&format!("*{v}"))))
        }
        FilterOp::IsNull => Some(format!("{{ path: [{}], operator: IsNull, valueBoolean: true }}", gql_string(&path))),
        FilterOp::IsNotNull => Some(format!("{{ path: [{}], operator: IsNull, valueBoolean: false }}", gql_string(&path))),
        FilterOp::In => {
            let ops: Vec<String> = v
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .filter_map(|item| where_operand(&FilterRule { column: rule.column.clone(), op: FilterOp::Eq, value: item.to_string() }))
                .collect();
            if ops.is_empty() {
                None
            } else {
                Some(format!("{{ operator: Or, operands: [{}] }}", ops.join(", ")))
            }
        }
        _ => None,
    }
}

// WHAT:  (GraphQL `where: {…}` clause, rules left for client-side filtering).
fn split_filters(filters: &[FilterRule]) -> (Option<String>, Vec<FilterRule>) {
    let mut server = Vec::new();
    let mut local = Vec::new();
    for rule in filters {
        match where_operand(rule) {
            Some(op) => server.push(op),
            None => local.push(rule.clone()),
        }
    }
    let clause = match server.len() {
        0 => None,
        1 => server.pop(),
        _ => Some(format!("{{ operator: And, operands: [{}] }}", server.join(", "))),
    };
    (clause, local)
}

fn columns_from_class(class: &Json) -> Vec<ColumnInfo> {
    let mut cols = vec![ColumnInfo { name: ID_COLUMN.into(), data_type: "uuid".into(), nullable: false, primary_key: true, ordinal: 0 }];
    for prop in class.get("properties").and_then(Json::as_array).into_iter().flatten() {
        let name = prop.get("name").and_then(Json::as_str).unwrap_or("property").to_string();
        let data_type = prop
            .get("dataType")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_str).collect::<Vec<_>>().join("|"))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "text".into());
        cols.push(ColumnInfo { name, data_type, nullable: true, primary_key: false, ordinal: cols.len() as u32 });
    }
    cols.push(ColumnInfo {
        name: ADDITIONAL_COLUMN.into(),
        data_type: "json".into(),
        nullable: true,
        primary_key: false,
        ordinal: cols.len() as u32,
    });
    cols
}

// WHAT:  REST object → flat row object: `_id`, properties, `_additional`.
fn flatten_object(obj: &Json) -> Json {
    let mut out = serde_json::Map::new();
    out.insert(ID_COLUMN.into(), obj.get("id").cloned().unwrap_or(Json::Null));
    if let Some(props) = obj.get("properties").and_then(Json::as_object) {
        for (k, v) in props {
            out.insert(k.clone(), v.clone());
        }
    }
    let mut additional = serde_json::Map::new();
    for key in ["creationTimeUnix", "lastUpdateTimeUnix", "vector", "vectors", "tenant", "additional"] {
        if let Some(v) = obj.get(key).filter(|v| !v.is_null()) {
            additional.insert(key.to_string(), v.clone());
        }
    }
    out.insert(ADDITIONAL_COLUMN.into(), if additional.is_empty() { Json::Null } else { Json::Object(additional) });
    Json::Object(out)
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
    GraphQl(String),
    Raw { method: String, path: String, body: Option<Json> },
}

impl Command {
    fn is_mutation(&self) -> bool {
        match self {
            Command::GraphQl(q) => q.trim_start().to_ascii_lowercase().starts_with("mutation"),
            Command::Raw { method, .. } => !matches!(method.as_str(), "GET" | "HEAD"),
        }
    }
}

fn parse_command(text: &str) -> AppResult<Command> {
    let text = text.trim();
    if text.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if text.starts_with('{') {
        if let Ok(value) = serde_json::from_str::<Json>(text) {
            if let Some(obj) = value.as_object() {
                if let Some(q) = obj.get("query").and_then(Json::as_str) {
                    return Ok(Command::GraphQl(q.to_string()));
                }
                if let Some(path) = obj.get("path").and_then(Json::as_str) {
                    let method = obj.get("method").and_then(Json::as_str).unwrap_or("GET").to_ascii_uppercase();
                    return Ok(Command::Raw { method, path: path.to_string(), body: obj.get("body").cloned() });
                }
                return Err(AppError::invalid_input("JSON must contain \"query\" (GraphQL) or \"path\" (raw REST request)."));
            }
        }
        // Not JSON: a bare GraphQL document `{ Get { … } }`.
        return Ok(Command::GraphQl(text.to_string()));
    }
    let lower = text.to_ascii_lowercase();
    if lower.starts_with("get") || lower.starts_with("aggregate") || lower.starts_with("explore") {
        return Ok(Command::GraphQl(format!("{{ {text} }}")));
    }
    if lower.starts_with("query") || lower.starts_with("mutation") {
        return Ok(Command::GraphQl(text.to_string()));
    }
    let mut words = text.split_whitespace();
    let verb = words.next().unwrap_or_default().to_ascii_uppercase();
    match verb.as_str() {
        "CLASSES" | "SCHEMA" => Ok(Command::Raw { method: "GET".into(), path: "/v1/schema".into(), body: None }),
        "OBJECTS" => {
            let class = words.next().ok_or_else(|| AppError::invalid_input("Usage: OBJECTS <class> [n]"))?;
            let n = words.next().map(|n| n.parse::<u64>().ok()).unwrap_or(Some(25)).unwrap_or(25);
            Ok(Command::Raw { method: "GET".into(), path: format!("/v1/objects?class={class}&limit={n}"), body: None })
        }
        _ => Err(AppError::invalid_input(
            "Unknown command. Use a GraphQL document ({ Get { Class { prop } } }), SCHEMA, OBJECTS <class> [n], or JSON {\"path\": ..}.",
        )),
    }
}

// WHAT:  GraphQL `{"data": {"Get": {"Class": [..]}}}` → rows of the first list found.
fn graphql_rows(data: &Json) -> Option<Vec<Json>> {
    match data {
        Json::Array(items) => Some(items.clone()),
        Json::Object(map) => map.values().find_map(graphql_rows),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

impl WeaviateIntegration {
    async fn graphql(&self, query: &str) -> AppResult<Json> {
        let resp: Json = self.http.post_json("/v1/graphql", &json!({"query": query})).await?;
        if let Some(errors) = resp.get("errors").and_then(Json::as_array).filter(|e| !e.is_empty()) {
            let msg = errors.iter().filter_map(|e| e.get("message").and_then(Json::as_str)).collect::<Vec<_>>().join("; ");
            return Err(AppError::driver(format!("GraphQL: {msg}")));
        }
        Ok(resp.get("data").cloned().unwrap_or(Json::Null))
    }

    async fn class_schema(&self, class: &str) -> AppResult<Json> {
        if !valid_name(class) {
            return Err(AppError::invalid_input(format!("Invalid class name: {class:?}")));
        }
        self.http.get_json(&format!("/v1/schema/{class}")).await
    }

    async fn list_objects(&self, class: &str, limit: u64, offset: u64, sort: &[(String, bool)]) -> AppResult<Vec<Json>> {
        if !valid_name(class) {
            return Err(AppError::invalid_input(format!("Invalid class name: {class:?}")));
        }
        let mut path = format!("/v1/objects?class={class}&limit={limit}&offset={offset}");
        if !sort.is_empty() {
            let cols: Vec<&str> = sort.iter().map(|(c, _)| c.as_str()).collect();
            let orders: Vec<&str> = sort.iter().map(|(_, d)| if *d { "desc" } else { "asc" }).collect();
            path.push_str(&format!("&sort={}&order={}", cols.join(","), orders.join(",")));
        }
        let resp: Json = self.http.get_json(&path).await?;
        Ok(resp.get("objects").and_then(Json::as_array).cloned().unwrap_or_default())
    }

    async fn aggregate_count(&self, class: &str, where_clause: Option<&str>) -> AppResult<i64> {
        if !valid_name(class) {
            return Err(AppError::invalid_input(format!("Invalid class name: {class:?}")));
        }
        let args = where_clause.map(|w| format!("(where: {w})")).unwrap_or_default();
        let query = format!("{{ Aggregate {{ {class}{args} {{ meta {{ count }} }} }} }}");
        let data = self.graphql(&query).await?;
        Ok(data
            .pointer(&format!("/Aggregate/{class}/0/meta/count"))
            .and_then(Json::as_i64)
            .unwrap_or(0))
    }

    // WHAT:  GraphQL `Get` with `where`, returning flattened rows (used when filters exist).
    async fn get_filtered(&self, class: &str, props: &[String], where_clause: &str, limit: u64) -> AppResult<Vec<Json>> {
        let fields = props.iter().filter(|p| valid_name(p)).cloned().collect::<Vec<_>>().join(" ");
        let query = format!(
            "{{ Get {{ {class}(where: {where_clause}, limit: {limit}) {{ {fields} _additional {{ id creationTimeUnix lastUpdateTimeUnix }} }} }} }}"
        );
        let data = self.graphql(&query).await?;
        let items = data.pointer(&format!("/Get/{class}")).and_then(Json::as_array).cloned().unwrap_or_default();
        Ok(items
            .iter()
            .map(|item| {
                let mut out = serde_json::Map::new();
                let additional = item.get("_additional").cloned().unwrap_or(Json::Null);
                out.insert(ID_COLUMN.into(), additional.get("id").cloned().unwrap_or(Json::Null));
                if let Some(obj) = item.as_object() {
                    for (k, v) in obj.iter().filter(|(k, _)| k.as_str() != "_additional") {
                        out.insert(k.clone(), v.clone());
                    }
                }
                out.insert(ADDITIONAL_COLUMN.into(), additional);
                Json::Object(out)
            })
            .collect())
    }

    async fn run(&self, cmd: Command, max_rows: usize) -> AppResult<StatementResult> {
        if self.read_only && cmd.is_mutation() {
            return Err(AppError::invalid_input("This connection is read-only; write operations are refused."));
        }
        match cmd {
            Command::GraphQl(query) => {
                let data = self.graphql(&query).await?;
                match graphql_rows(&data) {
                    Some(items) if items.iter().all(Json::is_object) && !items.is_empty() => {
                        let truncated = items.len() > max_rows;
                        let mut rs = http::objects_to_result_set(&items[..items.len().min(max_rows)], None, max_rows);
                        rs.truncated = truncated;
                        Ok(StatementResult::Rows { result: rs })
                    }
                    _ => Ok(StatementResult::Rows { result: json_result(data) }),
                }
            }
            Command::Raw { method, path, body } => {
                let m = reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| AppError::invalid_input(format!("Bad method {method}")))?;
                let mut req = self.http.request(m, &path);
                if let Some(b) = body {
                    req = req.json(&b);
                }
                let resp = self.http.send(req).await?;
                let text = resp.text().await.map_err(|e| AppError::driver(e.to_string()))?;
                let value: Json = serde_json::from_str(&text).unwrap_or(Json::String(text));
                if let Some(objects) = value.get("objects").and_then(Json::as_array) {
                    let flat: Vec<Json> = objects.iter().map(flatten_object).collect();
                    return Ok(StatementResult::Rows { result: http::objects_to_result_set(&flat, Some(ID_COLUMN), max_rows) });
                }
                if let Some(classes) = value.get("classes").and_then(Json::as_array) {
                    let docs: Vec<Json> = classes
                        .iter()
                        .map(|c| {
                            json!({
                                "class": c.get("class").cloned().unwrap_or(Json::Null),
                                "properties": c.get("properties").and_then(Json::as_array).map(|ps| ps.iter().filter_map(|p| p.get("name").and_then(Json::as_str)).collect::<Vec<_>>().join(", ")).unwrap_or_default(),
                                "vectorizer": c.get("vectorizer").cloned().unwrap_or(Json::Null),
                            })
                        })
                        .collect();
                    return Ok(StatementResult::Rows { result: http::objects_to_result_set(&docs, Some("class"), max_rows) });
                }
                Ok(StatementResult::Rows { result: json_result(value) })
            }
        }
    }
}

#[async_trait]
impl Integration for WeaviateIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: false,
            namespaces: false,
            fixed_columns: false,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.http.get_text("/v1/.well-known/ready").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let meta: Json = self.http.get_json("/v1/meta").await?;
        Ok(meta.get("version").and_then(Json::as_str).map(|v| format!("Weaviate {v}")))
    }

    fn current_database(&self) -> Option<String> {
        Some("default".into())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec!["default".into()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let schema: Json = self.http.get_json("/v1/schema").await?;
        let mut tables = Vec::new();
        for class in schema.get("classes").and_then(Json::as_array).into_iter().flatten() {
            let Some(name) = class.get("class").and_then(Json::as_str) else {
                continue;
            };
            let row_estimate = self.aggregate_count(name, None).await.ok();
            tables.push(TableInfo { schema: Some("classes".into()), name: name.to_string(), kind: TableKind::Table, row_estimate });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: "classes".into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let class = self.class_schema(&table.name).await?;
        Ok(columns_from_class(&class))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.aggregate_count(&table.name, None).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let (clause, local) = split_filters(filters);
        if local.is_empty() {
            return self.aggregate_count(&table.name, clause.as_deref()).await;
        }
        let columns = self.columns(table).await?;
        let rs = self.window(table, &columns, clause.as_deref(), WINDOW_CAP).await?;
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        Ok(http::local::apply_filters(&names, rs.rows, &local).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let columns = self.columns(table).await?;
        let (clause, local) = split_filters(&query.filters);
        let sortable = query
            .sort
            .iter()
            .all(|s| s.column != ID_COLUMN && s.column != ADDITIONAL_COLUMN && valid_name(&s.column));
        if query.filters.is_empty() && sortable {
            let sort: Vec<(String, bool)> = query.sort.iter().map(|s| (s.column.clone(), s.desc)).collect();
            let objects = self.list_objects(&table.name, u64::from(query.limit).max(1), query.offset, &sort).await?;
            let flat: Vec<Json> = objects.iter().map(flatten_object).collect();
            return Ok(rows_aligned(&flat, &columns));
        }
        let window = (query.offset + u64::from(query.limit)).clamp(1, WINDOW_CAP);
        let rs = self.window(table, &columns, clause.as_deref(), window).await?;
        let names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();
        let local_query = PageQuery { sort: query.sort.clone(), filters: local, offset: query.offset, limit: query.limit };
        let rows = http::local::page(&names, rs.rows, &local_query);
        Ok(ResultSet { columns: rs.columns, rows, truncated: false })
    }

    async fn execute(&self, text: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let cmd = parse_command(text)?;
        Ok(vec![self.run(cmd, max_rows).await?])
    }

    async fn close(&self) {}
}

impl WeaviateIntegration {
    // WHAT:  A bounded, unsorted window of rows: GraphQL `Get` when a `where`
    //        clause exists, otherwise the REST listing.
    async fn window(&self, table: &TableRef, columns: &[ColumnInfo], clause: Option<&str>, limit: u64) -> AppResult<ResultSet> {
        let docs = match clause {
            Some(w) => {
                let props: Vec<String> = columns
                    .iter()
                    .map(|c| c.name.clone())
                    .filter(|n| n != ID_COLUMN && n != ADDITIONAL_COLUMN)
                    .collect();
                self.get_filtered(&table.name, &props, w, limit).await?
            }
            None => self.list_objects(&table.name, limit, 0, &[]).await?.iter().map(flatten_object).collect(),
        };
        Ok(rows_aligned(&docs, columns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    #[test]
    fn where_clause_translation() {
        let rules = vec![
            FilterRule { column: "title".into(), op: FilterOp::Eq, value: "Dune".into() },
            FilterRule { column: "year".into(), op: FilterOp::Gte, value: "1965".into() },
            FilterRule { column: "title".into(), op: FilterOp::Contains, value: "un".into() },
            FilterRule { column: "_additional".into(), op: FilterOp::Eq, value: "x".into() },
        ];
        let (clause, local) = split_filters(&rules);
        let clause = clause.unwrap_or_default();
        assert!(clause.starts_with("{ operator: And, operands: ["));
        assert!(clause.contains(r#"{ path: ["title"], operator: Equal, valueText: "Dune" }"#));
        assert!(clause.contains(r#"{ path: ["year"], operator: GreaterThanEqual, valueInt: 1965 }"#));
        assert!(clause.contains(r#"operator: Like, valueText: "*un*""#));
        assert_eq!(local.len(), 1);
        let single = split_filters(&rules[..1]).0.unwrap_or_default();
        assert_eq!(single, r#"{ path: ["title"], operator: Equal, valueText: "Dune" }"#);
        let (id, _) = split_filters(&[FilterRule { column: "_id".into(), op: FilterOp::Eq, value: "abc".into() }]);
        assert_eq!(id.unwrap_or_default(), r#"{ path: ["id"], operator: Equal, valueText: "abc" }"#);
        let (any, _) = split_filters(&[FilterRule { column: "n".into(), op: FilterOp::In, value: "1,2".into() }]);
        assert!(any.unwrap_or_default().starts_with("{ operator: Or"));
    }

    #[test]
    fn class_to_columns_and_rows() {
        let class = json!({"class": "Book", "properties": [{"name": "title", "dataType": ["text"]}, {"name": "year", "dataType": ["int"]}]});
        let cols = columns_from_class(&class);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_id", "title", "year", "_additional"]);
        assert!(cols[0].primary_key);
        let objects = [json!({"id": "u1", "properties": {"title": "Dune", "extra": 1}, "creationTimeUnix": 5})];
        let flat: Vec<Json> = objects.iter().map(flatten_object).collect();
        let rs = rows_aligned(&flat, &cols);
        let names: Vec<&str> = rs.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_id", "title", "year", "_additional", "extra"]);
        assert_eq!(rs.rows[0][0], Value::Text("u1".into()));
        assert_eq!(rs.rows[0][2], Value::Null);
        assert_eq!(rs.rows[0][3], Value::Json(json!({"creationTimeUnix": 5})));
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse_command("{ Get { Book { title } } }").ok(), Some(Command::GraphQl("{ Get { Book { title } } }".into())));
        assert_eq!(parse_command("Get { Book { title } }").ok(), Some(Command::GraphQl("{ Get { Book { title } } }".into())));
        assert_eq!(parse_command(r#"{"query": "{ Get { Book { title } } }"}"#).ok(), Some(Command::GraphQl("{ Get { Book { title } } }".into())));
        assert_eq!(parse_command("schema").ok(), Some(Command::Raw { method: "GET".into(), path: "/v1/schema".into(), body: None }));
        assert_eq!(parse_command("objects Book 5").ok(), Some(Command::Raw { method: "GET".into(), path: "/v1/objects?class=Book&limit=5".into(), body: None }));
        let raw = parse_command(r#"{"path":"/v1/objects","method":"post","body":{"class":"Book"}}"#).ok();
        assert_eq!(raw, Some(Command::Raw { method: "POST".into(), path: "/v1/objects".into(), body: Some(json!({"class": "Book"})) }));
        assert!(raw.map(|c| c.is_mutation()).unwrap_or(false));
        assert!(Command::GraphQl("mutation { x }".into()).is_mutation());
        assert!(parse_command("DROP TABLE x").is_err());
    }

    #[test]
    fn graphql_rows_finds_first_list() {
        let data = json!({"Get": {"Book": [{"title": "Dune"}]}});
        assert_eq!(graphql_rows(&data).map(|r| r.len()), Some(1));
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_WEAVIATE_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Weaviate,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: None,
                username: None,
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: false,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_WEAVIATE_KEY").ok(),
        };
        let w = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = w.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("Weaviate"), "{version}");
        let _ = w.execute(r#"{"path":"/v1/schema/DbfreeTest","method":"DELETE"}"#, 10).await;
        w.execute(
            r#"{"path":"/v1/schema","method":"POST","body":{"class":"DbfreeTest","vectorizer":"none","properties":[{"name":"title","dataType":["text"]},{"name":"year","dataType":["int"]}]}}"#,
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("create class: {e}"));
        for (t, y) in [("Dune", 1965), ("Neuromancer", 1984)] {
            w.execute(&format!(r#"{{"path":"/v1/objects","method":"POST","body":{{"class":"DbfreeTest","properties":{{"title":"{t}","year":{y}}},"vector":[0.1,0.2]}}}}"#), 10)
                .await
                .unwrap_or_else(|e| panic!("insert: {e}"));
        }
        let table = TableRef { schema: Some("classes".into()), name: "DbfreeTest".into() };
        let cols = w.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "title"));
        assert_eq!(w.count(&table, &[]).await.unwrap_or_default(), 2);
        let filters = vec![FilterRule { column: "year".into(), op: FilterOp::Gt, value: "1970".into() }];
        assert_eq!(w.count(&table, &filters).await.unwrap_or_default(), 1);
        let page = w
            .fetch_page(&table, &PageQuery { sort: vec![], filters, offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 1);
        let page = w
            .fetch_page(&table, &PageQuery { sort: vec![crate::model::SortRule { column: "year".into(), desc: true }], filters: vec![], offset: 0, limit: 1 })
            .await
            .unwrap_or_else(|e| panic!("sorted page: {e}"));
        assert_eq!(page.rows[0][2], Value::Int(1984));
        let res = w.execute("{ Get { DbfreeTest { title } } }", 10).await.unwrap_or_else(|e| panic!("gql: {e}"));
        assert!(matches!(&res[0], StatementResult::Rows { result } if result.rows.len() == 2));
        let _ = w.execute(r#"{"path":"/v1/schema/DbfreeTest","method":"DELETE"}"#, 10).await;
    }
}
