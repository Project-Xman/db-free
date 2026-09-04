// SOT: sparql-integration, rdf-triple-store, sparql-protocol, jena-graphdb-stardog-blazegraph-virtuoso, sparql-results-json

use crate::error::{AppError, AppResult};
use crate::integrations::http::{Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SortRule, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use std::sync::Arc;

// ============================================================================
// WHAT:  One adapter for every SPARQL 1.1 endpoint the app lists: Apache Jena
//        Fuseki, GraphDB, Stardog, Blazegraph and Virtuoso. The engine only
//        changes the URL layout (query / update paths) and the auth style.
// WHY:   The SPARQL Protocol + SPARQL Results JSON are identical across stores.
// HOW:   Schema `graphs` lists named graphs (plus `default`) as triple tables
//        (subject, predicate, object …); schema `classes` lists `rdf:type`
//        classes as views whose columns are the class's most common predicates.
//        `execute` sends SELECT/ASK/CONSTRUCT/DESCRIBE to the query endpoint
//        and INSERT/DELETE/LOAD/CLEAR/CREATE/DROP to the update endpoint.
// WHERE: src-tauri/src/integrations/http.rs, src-tauri/src/model/connection.rs
// ============================================================================

const GRAPHS: &str = "graphs";
const CLASSES: &str = "classes";
const DEFAULT_GRAPH: &str = "default";
const CLASS_PREDICATES: usize = 20;
const TRIPLE_COLUMNS: [&str; 6] = ["subject", "predicate", "object", "object_type", "datatype", "lang"];

pub struct SparqlIntegration {
    engine: Engine,
    http: HttpClient,
    query_path: String,
    update_path: String,
    dataset: String,
    default_graph_uri: Option<String>,
    read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    pub query: String,
    pub update: String,
}

// WHAT:  Per-engine endpoint layout. A host that already ends in /sparql or
//        /query is used verbatim (update = same path with `update` swapped in).
pub fn endpoints_for(engine: Engine, host: &str, dataset: &str) -> Endpoints {
    let trimmed = host.trim_end_matches('/');
    if trimmed.ends_with("/sparql") || trimmed.ends_with("/query") {
        let update = if trimmed.ends_with("/query") { format!("{}/update", trimmed.trim_end_matches("/query")) } else { format!("{}/update", trimmed.trim_end_matches("/sparql")) };
        return Endpoints { query: trimmed.to_string(), update };
    }
    match engine {
        Engine::Graphdb => Endpoints { query: format!("/repositories/{dataset}"), update: format!("/repositories/{dataset}/statements") },
        Engine::Stardog => Endpoints { query: format!("/{dataset}/query"), update: format!("/{dataset}/update") },
        Engine::Blazegraph => Endpoints { query: format!("/blazegraph/namespace/{dataset}/sparql"), update: format!("/blazegraph/namespace/{dataset}/sparql") },
        Engine::Virtuoso => Endpoints { query: "/sparql".into(), update: "/sparql".into() },
        _ => Endpoints { query: format!("/{dataset}/sparql"), update: format!("/{dataset}/update") },
    }
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let engine = s.engine;
    let auth = HttpClient::auth_from_connection(conn);
    let auth = match (&auth, engine) {
        // Stardog and GraphDB prefer basic auth even with only a password given.
        (Auth::Bearer(p), Engine::Stardog) => Auth::Basic { user: "admin".into(), password: p.clone() },
        _ => auth,
    };
    let default_port = match engine {
        Engine::Graphdb => 7200,
        Engine::Stardog => 5820,
        Engine::Blazegraph => 9999,
        Engine::Virtuoso => 8890,
        _ => 3030,
    };
    let http = HttpClient::from_connection(conn, Some(default_port), false, auth)?;
    let dataset = s.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).unwrap_or(match engine {
        Engine::Blazegraph => "kb",
        Engine::Virtuoso => "",
        _ => "ds",
    });
    let host = s.host.as_deref().unwrap_or_default();
    let eps = endpoints_for(engine, host, dataset);
    let default_graph_uri = if engine == Engine::Virtuoso && !dataset.is_empty() { Some(dataset.to_string()) } else { None };
    let mut integration = SparqlIntegration {
        engine,
        http,
        query_path: eps.query,
        update_path: eps.update,
        dataset: if dataset.is_empty() { "sparql".into() } else { dataset.to_string() },
        default_graph_uri,
        read_only: s.read_only,
    };
    if let Err(e) = integration.ping().await {
        if engine == Engine::Blazegraph && !host.contains("/bigdata") {
            integration.query_path = integration.query_path.replace("/blazegraph/", "/bigdata/");
            integration.update_path = integration.update_path.replace("/blazegraph/", "/bigdata/");
            integration.ping().await?;
        } else {
            return Err(e);
        }
    }
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// SPARQL text builders
// ---------------------------------------------------------------------------

pub fn sparql_string(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub fn iri(raw: &str) -> String {
    format!("<{}>", raw.trim().trim_matches(|c| c == '<' || c == '>').replace(['<', '>', '"', ' '], ""))
}

fn var_name(column: &str) -> String {
    let cleaned: String = column.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    if cleaned.is_empty() { "_".into() } else { cleaned }
}

// WHAT:  One grid filter → a FILTER expression over ?var.
pub fn filter_clause(rule: &FilterRule) -> String {
    let v = format!("?{}", var_name(&rule.column));
    let raw = rule.value.trim();
    let is_num = raw.parse::<f64>().is_ok();
    let literal = |s: &str| if s.parse::<f64>().is_ok() { s.to_string() } else { sparql_string(s) };
    let lhs = if is_num { format!("xsd:decimal(str({v}))") } else { format!("str({v})") };
    match rule.op {
        FilterOp::Eq => format!("FILTER({lhs} = {})", literal(raw)),
        FilterOp::Ne => format!("FILTER({lhs} != {})", literal(raw)),
        FilterOp::Gt => format!("FILTER({lhs} > {})", literal(raw)),
        FilterOp::Gte => format!("FILTER({lhs} >= {})", literal(raw)),
        FilterOp::Lt => format!("FILTER({lhs} < {})", literal(raw)),
        FilterOp::Lte => format!("FILTER({lhs} <= {})", literal(raw)),
        FilterOp::Contains => format!("FILTER(CONTAINS(LCASE(STR({v})), LCASE({})))", sparql_string(raw)),
        FilterOp::StartsWith => format!("FILTER(STRSTARTS(LCASE(STR({v})), LCASE({})))", sparql_string(raw)),
        FilterOp::EndsWith => format!("FILTER(STRENDS(LCASE(STR({v})), LCASE({})))", sparql_string(raw)),
        FilterOp::In => {
            let items: Vec<String> = raw.split(',').map(str::trim).filter(|s| !s.is_empty()).map(sparql_string).collect();
            format!("FILTER(str({v}) IN ({}))", items.join(", "))
        }
        FilterOp::IsNull => format!("FILTER(!BOUND({v}))"),
        FilterOp::IsNotNull => format!("FILTER(BOUND({v}))"),
    }
}

fn order_clause(sort: &[SortRule], default: &str) -> String {
    if sort.is_empty() {
        return format!(" ORDER BY {default}");
    }
    let parts: Vec<String> = sort.iter().map(|s| if s.desc { format!("DESC(?{})", var_name(&s.column)) } else { format!("?{}", var_name(&s.column)) }).collect();
    format!(" ORDER BY {}", parts.join(" "))
}

fn graph_pattern(graph: &str, inner: &str) -> String {
    if graph == DEFAULT_GRAPH { inner.to_string() } else { format!("GRAPH {} {{ {inner} }}", iri(graph)) }
}

// WHAT:  Triple pattern with derived columns so filters on object_type / datatype / lang work.
fn triple_body(graph: &str, filters: &[FilterRule]) -> String {
    let binds = "BIND(IF(isIRI(?object), \"uri\", IF(isBlank(?object), \"bnode\", \"literal\")) AS ?object_type) BIND(DATATYPE(?object) AS ?datatype) BIND(LANG(?object) AS ?lang)";
    let filters: Vec<String> = filters.iter().map(filter_clause).collect();
    format!("{} {binds} {}", graph_pattern(graph, "?subject ?predicate ?object ."), filters.join(" "))
}

pub fn triples_query(graph: &str, query: &PageQuery) -> String {
    format!(
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?subject ?predicate ?object ?object_type ?datatype ?lang WHERE {{ {} }}{} LIMIT {} OFFSET {}",
        triple_body(graph, &query.filters),
        order_clause(&query.sort, "?subject ?predicate"),
        query.limit,
        query.offset
    )
}

pub fn triples_count(graph: &str, filters: &[FilterRule]) -> String {
    format!("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT (COUNT(*) AS ?n) WHERE {{ {} }}", triple_body(graph, filters))
}

fn class_body(class: &str, predicates: &[String], filters: &[FilterRule]) -> String {
    let optionals: Vec<String> = predicates.iter().enumerate().map(|(i, p)| format!("OPTIONAL {{ ?subject {} ?p{i} }}", iri(p))).collect();
    let filters: Vec<String> = filters.iter().map(filter_clause).collect();
    format!("?subject a {} . {} {}", iri(class), optionals.join(" "), filters.join(" "))
}

pub fn class_query(class: &str, predicates: &[String], query: &PageQuery) -> String {
    let vars: Vec<String> = (0..predicates.len()).map(|i| format!("?p{i}")).collect();
    format!(
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?subject {} WHERE {{ {} }}{} LIMIT {} OFFSET {}",
        vars.join(" "),
        class_body(class, predicates, &query.filters),
        order_clause(&query.sort, "?subject"),
        query.limit,
        query.offset
    )
}

pub fn class_count(class: &str, predicates: &[String], filters: &[FilterRule]) -> String {
    format!("PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT (COUNT(DISTINCT ?subject) AS ?n) WHERE {{ {} }}", class_body(class, predicates, filters))
}

// WHAT:  Class tables name their columns after the predicate's local name; the grid
//        sends those names back in sort/filter rules, so map them to ?pN here.
fn local_name(iri: &str) -> String {
    let tail = iri.rsplit(['#', '/']).next().unwrap_or(iri);
    if tail.is_empty() { iri.to_string() } else { tail.to_string() }
}

fn rename_rules(query: &PageQuery, columns: &[String]) -> PageQuery {
    let map = |c: &str| columns.iter().position(|n| n == c).map(|i| if i == 0 { "subject".to_string() } else { format!("p{}", i - 1) }).unwrap_or_else(|| c.to_string());
    PageQuery {
        sort: query.sort.iter().map(|s| SortRule { column: map(&s.column), desc: s.desc }).collect(),
        filters: query.filters.iter().map(|f| FilterRule { column: map(&f.column), op: f.op, value: f.value.clone() }).collect(),
        offset: query.offset,
        limit: query.limit,
    }
}

// ---------------------------------------------------------------------------
// SPARQL Results JSON → ResultSet
// ---------------------------------------------------------------------------

pub fn term_to_value(term: &serde_json::Value) -> Value {
    let Some(obj) = term.as_object() else { return Value::Null };
    let value = obj.get("value").and_then(|v| v.as_str()).unwrap_or_default();
    match obj.get("type").and_then(|t| t.as_str()) {
        Some("uri") => Value::Text(value.to_string()),
        Some("bnode") => Value::Text(format!("_:{value}")),
        _ => {
            let dt = obj.get("datatype").and_then(|d| d.as_str()).unwrap_or_default();
            let local = dt.rsplit('#').next().unwrap_or_default();
            match local {
                "integer" | "int" | "long" | "short" | "byte" | "nonNegativeInteger" | "positiveInteger" | "negativeInteger" | "nonPositiveInteger" | "unsignedInt" | "unsignedLong" => {
                    value.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Decimal(value.to_string()))
                }
                "decimal" => Value::Decimal(value.to_string()),
                "double" | "float" => value.parse::<f64>().map(Value::Float).unwrap_or_else(|_| Value::Text(value.to_string())),
                "boolean" => Value::Bool(value == "true" || value == "1"),
                "dateTime" | "date" | "dateTimeStamp" => Value::DateTime(value.to_string()),
                _ => Value::Text(value.to_string()),
            }
        }
    }
}

pub fn results_to_set(json: &serde_json::Value, max_rows: usize) -> AppResult<ResultSet> {
    if let Some(b) = json.get("boolean").and_then(|b| b.as_bool()) {
        return Ok(ResultSet { columns: vec![ColumnMeta { name: "result".into(), type_name: "boolean".into() }], rows: vec![vec![Value::Bool(b)]], truncated: false });
    }
    let vars: Vec<String> = json
        .get("head")
        .and_then(|h| h.get("vars"))
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    let bindings = json.get("results").and_then(|r| r.get("bindings")).and_then(|b| b.as_array()).cloned().unwrap_or_default();
    let truncated = bindings.len() > max_rows;
    let rows: Vec<Vec<Value>> = bindings.iter().take(max_rows).map(|b| vars.iter().map(|v| b.get(v).map(term_to_value).unwrap_or(Value::Null)).collect()).collect();
    let columns = vars
        .iter()
        .map(|v| {
            let type_name = bindings
                .iter()
                .find_map(|b| b.get(v))
                .map(|t| match term_to_value(t) {
                    Value::Int(_) => "integer",
                    Value::Float(_) => "double",
                    Value::Decimal(_) => "decimal",
                    Value::Bool(_) => "boolean",
                    Value::DateTime(_) => "dateTime",
                    _ => "string",
                })
                .unwrap_or("string");
            ColumnMeta { name: v.clone(), type_name: type_name.into() }
        })
        .collect();
    Ok(ResultSet { columns, rows, truncated })
}

pub fn query_kind(text: &str) -> &'static str {
    let mut upper = String::new();
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        upper.push_str(&t.to_uppercase());
        upper.push(' ');
    }
    // Skip prefixes / base.
    let mut rest = upper.trim_start();
    loop {
        if let Some(r) = rest.strip_prefix("PREFIX") {
            rest = r.split_once('>').map(|(_, t)| t).unwrap_or("").trim_start();
        } else if let Some(r) = rest.strip_prefix("BASE") {
            rest = r.split_once('>').map(|(_, t)| t).unwrap_or("").trim_start();
        } else {
            break;
        }
    }
    let head = rest.split_whitespace().next().unwrap_or_default();
    match head {
        "SELECT" => "select",
        "ASK" => "ask",
        "CONSTRUCT" | "DESCRIBE" => "graph",
        "INSERT" | "DELETE" | "LOAD" | "CLEAR" | "CREATE" | "DROP" | "COPY" | "MOVE" | "ADD" | "WITH" => "update",
        _ => "select",
    }
}

impl SparqlIntegration {
    fn query_url(&self) -> String {
        match &self.default_graph_uri {
            Some(g) => format!("{}?default-graph-uri={}", self.query_path, urlencode(g)),
            None => self.query_path.clone(),
        }
    }

    async fn select(&self, query: &str) -> AppResult<serde_json::Value> {
        let text = self.http.post_raw(&self.query_url(), "application/sparql-query", query.to_string(), Some("application/sparql-results+json")).await?;
        serde_json::from_str(&text).map_err(|e| AppError::driver(format!("SPARQL endpoint did not return results JSON: {e}: {}", text.chars().take(200).collect::<String>())))
    }

    async fn scalar(&self, query: &str) -> AppResult<i64> {
        let json = self.select(query).await?;
        let rs = results_to_set(&json, 1)?;
        Ok(match rs.rows.first().and_then(|r| r.first()) {
            Some(Value::Int(i)) => *i,
            Some(Value::Decimal(d)) | Some(Value::Text(d)) => d.parse().unwrap_or(0),
            Some(Value::Float(f)) => *f as i64,
            _ => 0,
        })
    }

    async fn class_predicates(&self, class: &str) -> AppResult<Vec<String>> {
        let q = format!("SELECT DISTINCT ?p WHERE {{ ?s a {} ; ?p ?o }} LIMIT {CLASS_PREDICATES}", iri(class));
        let json = self.select(&q).await?;
        let rs = results_to_set(&json, CLASS_PREDICATES)?;
        Ok(rs.rows.into_iter().filter_map(|r| match r.into_iter().next() { Some(Value::Text(t)) => Some(t), _ => None }).collect())
    }

    fn class_columns(predicates: &[String]) -> Vec<String> {
        let mut names = vec!["subject".to_string()];
        for p in predicates {
            let mut n = local_name(p);
            let mut k = 2;
            while names.contains(&n) {
                n = format!("{}_{k}", local_name(p));
                k += 1;
            }
            names.push(n);
        }
        names
    }
}

fn urlencode(raw: &str) -> String {
    let mut out = String::new();
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[async_trait]
impl Integration for SparqlIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: false, exact_estimate: true }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.select("ASK { }").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some(match self.engine {
            Engine::ApacheJena => "Apache Jena Fuseki".into(),
            Engine::Graphdb => "GraphDB".into(),
            Engine::Stardog => "Stardog".into(),
            Engine::Blazegraph => "Blazegraph".into(),
            Engine::Virtuoso => "Virtuoso".into(),
            _ => "SPARQL 1.1".into(),
        }))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.dataset.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.dataset.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut graphs = vec![TableInfo { schema: Some(GRAPHS.into()), name: DEFAULT_GRAPH.into(), kind: TableKind::Table, row_estimate: None }];
        if let Ok(json) = self.select("SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 500").await {
            for row in results_to_set(&json, 500)?.rows {
                if let Some(Value::Text(g)) = row.into_iter().next() {
                    graphs.push(TableInfo { schema: Some(GRAPHS.into()), name: g, kind: TableKind::Table, row_estimate: None });
                }
            }
        }
        let mut classes = Vec::new();
        if let Ok(json) = self.select("SELECT DISTINCT ?type (COUNT(?s) AS ?n) WHERE { ?s a ?type } GROUP BY ?type ORDER BY DESC(?n) LIMIT 500").await {
            for row in results_to_set(&json, 500)?.rows {
                let mut it = row.into_iter();
                if let Some(Value::Text(t)) = it.next() {
                    let n = match it.next() { Some(Value::Int(n)) => Some(n), _ => None };
                    classes.push(TableInfo { schema: Some(CLASSES.into()), name: t, kind: TableKind::View, row_estimate: n });
                }
            }
        }
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: GRAPHS.into(), tables: graphs }, SchemaInfo { name: CLASSES.into(), tables: classes }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let col = |i: usize, name: String, pk: bool| ColumnInfo { name, data_type: "string".into(), nullable: !pk, primary_key: pk, ordinal: i as u32 + 1 };
        if table.schema.as_deref() == Some(CLASSES) {
            let preds = self.class_predicates(&table.name).await?;
            let names = Self::class_columns(&preds);
            return Ok(names.into_iter().enumerate().map(|(i, n)| col(i, n, i == 0)).collect());
        }
        Ok(TRIPLE_COLUMNS.iter().enumerate().map(|(i, n)| col(i, (*n).to_string(), i < 2)).collect())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        if table.schema.as_deref() == Some(CLASSES) {
            let preds = self.class_predicates(&table.name).await?;
            let names = Self::class_columns(&preds);
            let q = rename_rules(&PageQuery { sort: vec![], filters: filters.to_vec(), offset: 0, limit: 0 }, &names);
            return self.scalar(&class_count(&table.name, &preds, &q.filters)).await;
        }
        self.scalar(&triples_count(&table.name, filters)).await
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        if table.schema.as_deref() == Some(CLASSES) {
            let preds = self.class_predicates(&table.name).await?;
            let names = Self::class_columns(&preds);
            let q = rename_rules(query, &names);
            let json = self.select(&class_query(&table.name, &preds, &q)).await?;
            let mut rs = results_to_set(&json, query.limit as usize)?;
            for (c, n) in rs.columns.iter_mut().zip(names) {
                c.name = n;
            }
            return Ok(rs);
        }
        let json = self.select(&triples_query(&table.name, query)).await?;
        results_to_set(&json, query.limit as usize)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let text = sql.trim();
        if text.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        match query_kind(text) {
            "update" => {
                if self.read_only {
                    return Err(AppError::read_only("This connection is read-only; SPARQL UPDATE is blocked."));
                }
                self.http.post_raw(&self.update_path, "application/sparql-update", text.to_string(), Some("*/*")).await?;
                Ok(vec![StatementResult::Affected { rows_affected: 0 }])
            }
            "graph" => {
                let turtle = self.http.post_raw(&self.query_url(), "application/sparql-query", text.to_string(), Some("text/turtle")).await?;
                let lines: Vec<Vec<Value>> = turtle.lines().filter(|l| !l.trim().is_empty()).take(max_rows).map(|l| vec![Value::Text(l.to_string())]).collect();
                Ok(vec![StatementResult::Rows { result: ResultSet { columns: vec![ColumnMeta { name: "turtle".into(), type_name: "string".into() }], rows: lines, truncated: false } }])
            }
            _ => {
                let json = self.select(text).await?;
                Ok(vec![StatementResult::Rows { result: results_to_set(&json, max_rows)? }])
            }
        }
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    #[test]
    fn endpoints_per_engine() {
        assert_eq!(endpoints_for(Engine::ApacheJena, "localhost", "ds").query, "/ds/sparql");
        assert_eq!(endpoints_for(Engine::ApacheJena, "localhost", "ds").update, "/ds/update");
        assert_eq!(endpoints_for(Engine::Graphdb, "localhost", "repo").update, "/repositories/repo/statements");
        assert_eq!(endpoints_for(Engine::Stardog, "localhost", "db").query, "/db/query");
        assert_eq!(endpoints_for(Engine::Blazegraph, "localhost", "kb").query, "/blazegraph/namespace/kb/sparql");
        assert_eq!(endpoints_for(Engine::Virtuoso, "localhost", "").query, "/sparql");
        let e = endpoints_for(Engine::Stardog, "https://x.io/thing/query", "db");
        assert_eq!(e.query, "https://x.io/thing/query");
        assert_eq!(e.update, "https://x.io/thing/update");
    }

    #[test]
    fn builders_render() {
        let q = PageQuery {
            sort: vec![SortRule { column: "object".into(), desc: true }],
            filters: vec![FilterRule { column: "predicate".into(), op: FilterOp::Contains, value: "na\"me".into() }],
            offset: 10,
            limit: 5,
        };
        let s = triples_query("http://g", &q);
        assert!(s.contains("GRAPH <http://g> { ?subject ?predicate ?object . }"));
        assert!(s.contains("FILTER(CONTAINS(LCASE(STR(?predicate)), LCASE(\"na\\\"me\")))"));
        assert!(s.ends_with("ORDER BY DESC(?object) LIMIT 5 OFFSET 10"));
        let d = triples_query(DEFAULT_GRAPH, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 1 });
        assert!(!d.contains("GRAPH"));
        assert!(d.contains("ORDER BY ?subject ?predicate"));
        assert_eq!(filter_clause(&FilterRule { column: "age".into(), op: FilterOp::Gte, value: "5".into() }), "FILTER(xsd:decimal(str(?age)) >= 5)");
        assert_eq!(filter_clause(&FilterRule { column: "x".into(), op: FilterOp::IsNull, value: String::new() }), "FILTER(!BOUND(?x))");
        let c = class_query("http://ex/Person", &["http://ex/name".into(), "http://xmlns.com/foaf/0.1/age".into()], &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 2 });
        assert!(c.contains("SELECT ?subject ?p0 ?p1 WHERE { ?subject a <http://ex/Person> . OPTIONAL { ?subject <http://ex/name> ?p0 } OPTIONAL { ?subject <http://xmlns.com/foaf/0.1/age> ?p1 }"));
        assert_eq!(SparqlIntegration::class_columns(&["http://ex/name".into(), "http://other/name".into()]), vec!["subject", "name", "name_2"]);
    }

    #[test]
    fn rename_rules_maps_class_columns() {
        let names = vec!["subject".to_string(), "name".to_string(), "age".to_string()];
        let q = PageQuery { sort: vec![SortRule { column: "age".into(), desc: false }], filters: vec![FilterRule { column: "name".into(), op: FilterOp::Eq, value: "x".into() }], offset: 0, limit: 1 };
        let r = rename_rules(&q, &names);
        assert_eq!(r.sort[0].column, "p1");
        assert_eq!(r.filters[0].column, "p0");
    }

    #[test]
    fn decodes_results_json() {
        let json = serde_json::json!({
            "head": {"vars": ["s", "n", "b", "d"]},
            "results": {"bindings": [
                {"s": {"type": "uri", "value": "http://a"}, "n": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#integer", "value": "3"},
                 "b": {"type": "bnode", "value": "b0"}, "d": {"type": "literal", "datatype": "http://www.w3.org/2001/XMLSchema#double", "value": "1.5"}},
                {"s": {"type": "literal", "xml:lang": "en", "value": "hi"}}
            ]}
        });
        let rs = results_to_set(&json, 10).unwrap_or_else(|_| ResultSet { columns: vec![], rows: vec![], truncated: false });
        assert_eq!(rs.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["s", "n", "b", "d"]);
        assert_eq!(rs.columns[1].type_name, "integer");
        assert_eq!(rs.rows[0][1], Value::Int(3));
        assert_eq!(rs.rows[0][2], Value::Text("_:b0".into()));
        assert_eq!(rs.rows[0][3], Value::Float(1.5));
        assert_eq!(rs.rows[1][1], Value::Null);
        let ask = results_to_set(&serde_json::json!({"head": {}, "boolean": true}), 1).unwrap_or_else(|_| ResultSet { columns: vec![], rows: vec![], truncated: false });
        assert_eq!(ask.rows[0][0], Value::Bool(true));
    }

    #[test]
    fn classifies_queries() {
        assert_eq!(query_kind("PREFIX ex: <http://ex/>\nSELECT * WHERE { ?s ?p ?o }"), "select");
        assert_eq!(query_kind("# comment\nASK { ?s ?p ?o }"), "ask");
        assert_eq!(query_kind("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), "graph");
        assert_eq!(query_kind("PREFIX ex: <http://ex/> INSERT DATA { ex:a ex:b ex:c }"), "update");
        assert_eq!(query_kind("WITH <g> DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }"), "update");
        assert_eq!(query_kind("drop all"), "update");
    }

    #[test]
    fn iri_strips_brackets() {
        assert_eq!(iri("<http://x/y>"), "<http://x/y>");
        assert_eq!(iri("http://x/y"), "<http://x/y>");
        assert_eq!(local_name("http://xmlns.com/foaf/0.1/name"), "name");
        assert_eq!(local_name("http://ex#Age"), "Age");
    }

    // Runs only when DBFREE_TEST_SPARQL_URL is set:
    // `docker run --rm -d -p 3030:3030 -e ADMIN_PASSWORD=pw stain/jena-fuseki`
    // (create the dataset first, or set _DATASET to an existing one).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_SPARQL_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::ApacheJena,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: Some(std::env::var("DBFREE_TEST_SPARQL_DATASET").unwrap_or_else(|_| "ds".into())),
                username: std::env::var("DBFREE_TEST_SPARQL_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_SPARQL_PASSWORD").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        db.execute(
            "INSERT DATA { <http://ex/a> <http://ex/name> \"Ada\" ; a <http://ex/Person> . <http://ex/b> <http://ex/name> \"Bob\" ; a <http://ex/Person> . }",
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("insert: {e}"));

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(!catalog.schemas.is_empty(), "{catalog:?}");
        let table = TableRef { schema: Some("graphs".into()), name: "default".into() };
        let cols = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "subject"), "{cols:?}");
        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 20 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert!(page.rows.len() >= 4, "{page:?}");
        assert!(db.count(&table, &[]).await.unwrap_or_default() >= 4);
        let rows = db
            .execute("SELECT ?s WHERE { ?s a <http://ex/Person> }", 10)
            .await
            .unwrap_or_else(|e| panic!("select: {e}"));
        match rows.first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows.len(), 2, "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        let ask = db.execute("ASK { <http://ex/a> ?p ?o }", 10).await.unwrap_or_else(|e| panic!("ask: {e}"));
        match ask.first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows[0][0], Value::Bool(true), "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        let _ = db.execute("DELETE WHERE { ?s ?p ?o }", 10).await;
        db.close().await;
    }

}
