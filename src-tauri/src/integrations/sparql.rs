// SOT: sparql-integration, rdf-triple-store, sparql-protocol, jena-graphdb-stardog-blazegraph-virtuoso, sparql-results-json, sparql-object-explorer, sparql-server-stats, rdf-prefixes

use crate::error::{AppError, AppResult};
use crate::integrations::http::{Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::objects::format_number;
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, SortRule, Stat,
    StatGroup, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
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

// ---------------------------------------------------------------------------
// Object explorer / administration
//
// WHAT:  Datasets (the endpoint's repositories, when it has an admin API),
//        named graphs with their triple counts, and the prefixes actually used
//        by the data.
// WHY:   SPARQL 1.1 has no catalog: only the graph list is standard. Everything
//        else is per-product (`/$/datasets` on Fuseki, `/rest/repositories` on
//        GraphDB, `/admin/databases` on Stardog), so each is tried and the
//        configured dataset is the honest fallback.
// HOW:   Prefixes are a bundled well-known list merged with the namespaces seen
//        in a sample of the data — never invented. Actions are SPARQL Update
//        (`CLEAR GRAPH`, `DROP GRAPH`), which `query_kind` classifies as update
//        and the read-only lock already refuses.
// ---------------------------------------------------------------------------

const LIST_CAP: usize = 2_000;
const GRAPH_LIST_CAP: usize = 500;
const SAMPLE_TRIPLES: usize = 50;
const PREFIX_SAMPLE: usize = 1_000;

// WHAT:  Prefixes every RDF tool ships with; shown even when the data does not
//        use them yet, so the list is a usable reference rather than a guess.
const COMMON_PREFIXES: [(&str, &str); 14] = [
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("foaf", "http://xmlns.com/foaf/0.1/"),
    ("dc", "http://purl.org/dc/elements/1.1/"),
    ("dcterms", "http://purl.org/dc/terms/"),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("schema", "http://schema.org/"),
    ("sh", "http://www.w3.org/ns/shacl#"),
    ("prov", "http://www.w3.org/ns/prov#"),
    ("void", "http://rdfs.org/ns/void#"),
    ("geo", "http://www.w3.org/2003/01/geo/wgs84_pos#"),
    ("sd", "http://www.w3.org/ns/sparql-service-description#"),
];

fn jstr(row: &serde_json::Value, key: &str) -> Option<String> {
    row.get(key).filter(|v| !v.is_null()).map(|v| match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    })
    .filter(|s| !s.is_empty())
}

fn finish(mut out: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    out.truncate(LIST_CAP);
    out
}

// ---- datasets ----------------------------------------------------------------

// WHAT:  Fuseki `/$/datasets` → one Dataset per `ds.name`, with its services.
fn fuseki_datasets(body: &serde_json::Value) -> Vec<ObjectSummary> {
    let items = body.get("datasets").and_then(|d| d.as_array()).cloned().unwrap_or_default();
    finish(
        items
            .iter()
            .filter_map(|d| {
                let name = jstr(d, "ds.name")?.trim_start_matches('/').to_string();
                let services: Vec<String> = d
                    .get("ds.services")
                    .and_then(|s| s.as_array())
                    .map(|a| a.iter().filter_map(|s| jstr(s, "srv.type")).collect())
                    .unwrap_or_default();
                let state = d.get("ds.state").and_then(|s| s.as_bool()).unwrap_or(true);
                let mut s = ObjectSummary::new(ObjectKind::Dataset, name, None).with_badge(if state { "active" } else { "offline" });
                if !services.is_empty() {
                    s = s.with_detail(services.join(", "));
                }
                Some(s)
            })
            .collect(),
    )
}

// WHAT:  GraphDB `/rest/repositories` → one Dataset per repository id.
fn graphdb_repositories(body: &serde_json::Value) -> Vec<ObjectSummary> {
    let items = body.as_array().cloned().unwrap_or_default();
    finish(
        items
            .iter()
            .filter_map(|r| {
                let id = jstr(r, "id")?;
                let mut parts = Vec::new();
                if let Some(t) = jstr(r, "title") {
                    parts.push(t);
                }
                if let Some(l) = jstr(r, "location").filter(|l| !l.is_empty()) {
                    parts.push(l);
                }
                let writable = r.get("writable").and_then(|w| w.as_bool()).unwrap_or(true);
                let mut s = ObjectSummary::new(ObjectKind::Dataset, id, None).with_badge(if writable { "read-write" } else { "read-only" });
                if !parts.is_empty() {
                    s = s.with_detail(parts.join(" · "));
                }
                Some(s)
            })
            .collect(),
    )
}

// WHAT:  Stardog `/admin/databases` → `{"databases": ["db", …]}`.
fn stardog_databases(body: &serde_json::Value) -> Vec<ObjectSummary> {
    let items = body.get("databases").and_then(|d| d.as_array()).cloned().unwrap_or_default();
    finish(
        items
            .iter()
            .filter_map(|d| d.as_str())
            .map(|name| ObjectSummary::new(ObjectKind::Dataset, name, None).with_badge("database"))
            .collect(),
    )
}

fn dataset_detail(reference: &ObjectRef, entry: Option<&serde_json::Value>, triples: Option<i64>, graphs: Option<usize>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference);
    if let Some(e) = entry {
        d = d.definition(serde_json::to_string_pretty(e).unwrap_or_default(), CodeLanguage::Json);
        if let Some(services) = e.get("ds.services").and_then(|s| s.as_array()) {
            d.rows = Some(ResultSet {
                columns: ["service", "description", "endpoints"].iter().map(|n| ColumnMeta { name: (*n).into(), type_name: "string".into() }).collect(),
                rows: services
                    .iter()
                    .map(|s| {
                        let endpoints = s.get("srv.endpoints").and_then(|e| e.as_array()).map(|a| a.iter().filter_map(|e| e.as_str()).collect::<Vec<_>>().join(", ")).unwrap_or_default();
                        vec![
                            Value::Text(jstr(s, "srv.type").unwrap_or_default()),
                            Value::Text(jstr(s, "srv.description").unwrap_or_default()),
                            Value::Text(endpoints),
                        ]
                    })
                    .collect(),
                truncated: false,
            });
        }
    }
    if let Some(t) = triples {
        d = d.property("triples", format_number(t as f64));
    }
    if let Some(g) = graphs {
        d = d.property("named graphs", g.to_string());
    }
    // Deleting a dataset is an admin HTTP call, not SPARQL, and every action
    // must run through `execute`, so only read-only queries are offered here.
    d.action(ObjectAction::new("count", "Count triples", "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"))
        .action(ObjectAction::new("graphs", "List graphs", "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY DESC(?n)"))
}

// ---- graphs ---------------------------------------------------------------------

fn graph_summaries(counts: &[(String, i64)]) -> Vec<ObjectSummary> {
    finish(
        counts
            .iter()
            .map(|(name, n)| {
                let mut s = ObjectSummary::new(ObjectKind::Graph, name.clone(), None).with_detail(format!("{} triples", format_number(*n as f64)));
                if name == DEFAULT_GRAPH {
                    s = s.with_badge("default");
                } else {
                    s = s.with_badge("named");
                }
                s
            })
            .collect(),
    )
}

fn graph_sample_query(graph: &str) -> String {
    let body = if graph == DEFAULT_GRAPH { "?subject ?predicate ?object .".to_string() } else { format!("GRAPH {} {{ ?subject ?predicate ?object . }}", iri(graph)) };
    format!("SELECT ?subject ?predicate ?object WHERE {{ {body} }} LIMIT {SAMPLE_TRIPLES}")
}

fn graph_detail(reference: &ObjectRef, count: Option<i64>, sample: Option<ResultSet>) -> ObjectDetail {
    let name = reference.name.as_str();
    let default = name == DEFAULT_GRAPH;
    let mut d = ObjectDetail::empty(reference).definition(graph_sample_query(name), CodeLanguage::Text);
    if let Some(c) = count {
        d = d.property("triples", format_number(c as f64));
    }
    d = d.property("graph", if default { "the endpoint's default graph".into() } else { iri(name) });
    d.rows = sample;
    let target = if default { "DEFAULT".to_string() } else { format!("GRAPH {}", iri(name)) };
    d.action(ObjectAction::new("sample", "Sample 50", graph_sample_query(name)))
        .action(ObjectAction::destructive("clear", "Clear graph", format!("CLEAR {target}")))
        .action(ObjectAction::destructive("drop", "Drop graph", format!("DROP {target}")))
}

// ---- prefixes ---------------------------------------------------------------------

// WHAT:  The namespace part of an IRI: everything up to and including the last
//        `#` or `/` (the split RDF tools use for QNames).
fn namespace_of(term: &str) -> Option<String> {
    let cut = term.rfind('#').map(|i| i + 1).or_else(|| term.rfind('/').map(|i| i + 1))?;
    let ns = &term[..cut];
    (cut < term.len() && ns.starts_with("http")).then(|| ns.to_string())
}

fn well_known(namespace: &str) -> Option<&'static str> {
    COMMON_PREFIXES.iter().find(|(_, ns)| *ns == namespace).map(|(p, _)| *p)
}

// WHAT:  Well-known prefixes ∪ the namespaces the sampled data actually uses.
fn prefix_summaries(uses: &BTreeMap<String, i64>) -> Vec<ObjectSummary> {
    let mut all: BTreeMap<String, i64> = uses.clone();
    for (_, ns) in COMMON_PREFIXES {
        all.entry(ns.to_string()).or_insert(0);
    }
    finish(
        all.into_iter()
            .map(|(namespace, count)| {
                let label = well_known(&namespace);
                let name = label.map(str::to_string).unwrap_or_else(|| namespace.clone());
                let detail = if count > 0 { format!("{namespace} · {} use(s) in the sample", format_number(count as f64)) } else { namespace.clone() };
                let badge = match (label.is_some(), count > 0) {
                    (true, true) => "well-known",
                    (true, false) => "unused",
                    (false, _) => "in data",
                };
                ObjectSummary::new(ObjectKind::Prefix, name, None).with_detail(detail).with_badge(badge)
            })
            .collect(),
    )
}

fn prefix_namespace(reference: &ObjectRef) -> String {
    COMMON_PREFIXES
        .iter()
        .find(|(p, _)| *p == reference.name)
        .map(|(_, ns)| (*ns).to_string())
        .unwrap_or_else(|| reference.name.clone())
}

fn prefix_detail(reference: &ObjectRef, count: Option<i64>, sample: Option<ResultSet>) -> ObjectDetail {
    let namespace = prefix_namespace(reference);
    let label = well_known(&namespace).unwrap_or("ns");
    let mut d = ObjectDetail::empty(reference)
        .definition(format!("PREFIX {label}: {}", iri(&namespace)), CodeLanguage::Text)
        .property("namespace", namespace.clone());
    if let Some(c) = count {
        d = d.property("uses in sample", format_number(c as f64));
    }
    d = d.property("source", if well_known(&namespace).is_some() { "well-known prefix" } else { "seen in the data" });
    d.rows = sample;
    d.action(ObjectAction::new(
        "sample",
        "Sample statements",
        format!("SELECT ?s ?p ?o WHERE {{ ?s ?p ?o FILTER(STRSTARTS(STR(?p), {})) }} LIMIT 25", sparql_string(&namespace)),
    ))
}

// ---- stats ----------------------------------------------------------------------------

// WHAT:  Fuseki `/$/stats` → the counters it keeps per dataset.
fn fuseki_stats(body: &serde_json::Value, dataset: &str) -> Vec<Stat> {
    let Some(datasets) = body.get("datasets").and_then(|d| d.as_object()) else { return Vec::new() };
    let entry = datasets
        .iter()
        .find(|(k, _)| k.trim_start_matches('/') == dataset)
        .or_else(|| datasets.iter().next())
        .map(|(_, v)| v);
    let Some(entry) = entry.and_then(|e| e.as_object()) else { return Vec::new() };
    let mut out = Vec::new();
    for (key, label) in [("Requests", "Requests"), ("RequestsGood", "Successful"), ("RequestsBad", "Failed")] {
        if let Some(n) = entry.get(key).and_then(|v| v.as_f64()) {
            out.push(Stat::number(label, n, None));
        }
    }
    out
}

// WHAT:  A size endpoint's body: a bare number (GraphDB, Stardog) or JSON.
fn size_from_body(body: &str) -> Option<i64> {
    let t = body.trim();
    if let Ok(n) = t.parse::<i64>() {
        return Some(n);
    }
    let v: serde_json::Value = serde_json::from_str(t).ok()?;
    ["total", "size", "count", "inferred", "explicit"].iter().find_map(|k| v.get(*k).and_then(|n| n.as_i64()))
}

// WHAT:  The figures the Data group is built from; counted with SPARQL except
//        `size`, which the store reports through its own admin API.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct DataCounts {
    triples: Option<i64>,
    graphs: usize,
    classes: Option<i64>,
    predicates: Option<i64>,
    size: Option<i64>,
}

fn stat_groups(engine: Engine, dataset: &str, counts: &DataCounts, requests: Vec<Stat>) -> Vec<StatGroup> {
    let DataCounts { triples, graphs, classes, predicates, size } = *counts;
    let product = match engine {
        Engine::ApacheJena => "Apache Jena Fuseki",
        Engine::Graphdb => "GraphDB",
        Engine::Stardog => "Stardog",
        Engine::Blazegraph => "Blazegraph",
        Engine::Virtuoso => "Virtuoso",
        _ => "SPARQL 1.1",
    };
    let mut groups = vec![StatGroup { title: "Server".into(), stats: vec![Stat::text("Endpoint", product), Stat::text("Dataset", dataset)] }];
    let mut data = Vec::new();
    if let Some(t) = triples {
        data.push(Stat::number("Triples", t as f64, None));
    }
    data.push(Stat::number("Named graphs", graphs as f64, None));
    if let Some(c) = classes {
        data.push(Stat::number("Classes", c as f64, None));
    }
    if let Some(p) = predicates {
        data.push(Stat::number("Predicates", p as f64, None));
    }
    if let Some(s) = size {
        data.push(Stat::number("Statements (server)", s as f64, None).with_hint("reported by the store"));
    }
    groups.push(StatGroup { title: "Data".into(), stats: data });
    if !requests.is_empty() {
        groups.push(StatGroup { title: "Throughput".into(), stats: requests });
    }
    groups
}

impl SparqlIntegration {
    async fn admin_json(&self, path: &str) -> Option<serde_json::Value> {
        self.http.get_json::<serde_json::Value>(path).await.ok()
    }

    // WHAT:  The store's own dataset list when it has one; the configured
    //        dataset otherwise (an endpoint URL is often all we are given).
    async fn dataset_objects(&self) -> Vec<ObjectSummary> {
        let listed = match self.engine {
            Engine::ApacheJena => self.admin_json("/$/datasets").await.map(|b| fuseki_datasets(&b)),
            Engine::Graphdb => self.admin_json("/rest/repositories").await.map(|b| graphdb_repositories(&b)),
            Engine::Stardog => self.admin_json("/admin/databases").await.map(|b| stardog_databases(&b)),
            _ => None,
        };
        match listed.filter(|l| !l.is_empty()) {
            Some(mut list) => {
                for entry in &mut list {
                    if entry.reference.name == self.dataset {
                        entry.badge = Some("current".into());
                    }
                }
                list
            }
            None => vec![ObjectSummary::new(ObjectKind::Dataset, self.dataset.clone(), None).with_badge("current").with_detail(self.query_path.clone())],
        }
    }

    async fn dataset_entry(&self, name: &str) -> Option<serde_json::Value> {
        match self.engine {
            Engine::ApacheJena => {
                let body = self.admin_json("/$/datasets").await?;
                body.get("datasets")?
                    .as_array()?
                    .iter()
                    .find(|d| jstr(d, "ds.name").map(|n| n.trim_start_matches('/').to_string()).as_deref() == Some(name))
                    .cloned()
            }
            Engine::Graphdb => {
                let body = self.admin_json("/rest/repositories").await?;
                body.as_array()?.iter().find(|r| jstr(r, "id").as_deref() == Some(name)).cloned()
            }
            _ => None,
        }
    }

    // WHAT:  Named graphs with their triple counts, plus the default graph.
    async fn graph_counts(&self) -> AppResult<Vec<(String, i64)>> {
        let mut out = Vec::new();
        if let Ok(n) = self.scalar("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }").await {
            out.push((DEFAULT_GRAPH.to_string(), n));
        }
        let q = format!("SELECT ?g (COUNT(*) AS ?n) WHERE {{ GRAPH ?g {{ ?s ?p ?o }} }} GROUP BY ?g ORDER BY DESC(?n) LIMIT {GRAPH_LIST_CAP}");
        if let Ok(json) = self.select(&q).await {
            for row in results_to_set(&json, GRAPH_LIST_CAP)?.rows {
                let mut it = row.into_iter();
                let name = match it.next() {
                    Some(Value::Text(g)) => g,
                    _ => continue,
                };
                let count = match it.next() {
                    Some(Value::Int(n)) => n,
                    Some(Value::Decimal(d)) | Some(Value::Text(d)) => d.parse().unwrap_or(0),
                    _ => 0,
                };
                out.push((name, count));
            }
        }
        Ok(out)
    }

    async fn graph_count(&self, graph: &str) -> Option<i64> {
        let q = if graph == DEFAULT_GRAPH {
            "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }".to_string()
        } else {
            format!("SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH {} {{ ?s ?p ?o }} }}", iri(graph))
        };
        self.scalar(&q).await.ok()
    }

    async fn sample(&self, query: &str, limit: usize) -> Option<ResultSet> {
        let json = self.select(query).await.ok()?;
        results_to_set(&json, limit).ok()
    }

    // WHAT:  Namespaces used by the sampled predicates and types, with counts.
    async fn namespace_uses(&self) -> BTreeMap<String, i64> {
        let mut out: BTreeMap<String, i64> = BTreeMap::new();
        let q = format!("SELECT ?p (COUNT(*) AS ?n) WHERE {{ ?s ?p ?o }} GROUP BY ?p ORDER BY DESC(?n) LIMIT {PREFIX_SAMPLE}");
        let Ok(json) = self.select(&q).await else { return out };
        let Ok(rs) = results_to_set(&json, PREFIX_SAMPLE) else { return out };
        for row in rs.rows {
            let mut it = row.into_iter();
            let Some(Value::Text(term)) = it.next() else { continue };
            let count = match it.next() {
                Some(Value::Int(n)) => n,
                Some(Value::Decimal(d)) | Some(Value::Text(d)) => d.parse().unwrap_or(1),
                _ => 1,
            };
            if let Some(ns) = namespace_of(&term) {
                *out.entry(ns).or_insert(0) += count;
            }
        }
        out
    }

    async fn store_size(&self) -> Option<i64> {
        let path = match self.engine {
            Engine::Graphdb => format!("/rest/repositories/{}/size", self.dataset),
            Engine::Stardog => format!("/admin/databases/{}/size", self.dataset),
            Engine::Blazegraph => return None,
            _ => return None,
        };
        size_from_body(&self.http.get_text(&path).await.ok()?)
    }

    async fn explorer_objects(&self, kind: ObjectKind, _parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Dataset => Ok(self.dataset_objects().await),
            ObjectKind::Graph => Ok(graph_summaries(&self.graph_counts().await?)),
            ObjectKind::Prefix => Ok(prefix_summaries(&self.namespace_uses().await)),
            _ => Ok(Vec::new()),
        }
    }

    async fn explorer_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        match reference.kind {
            ObjectKind::Dataset => {
                let entry = self.dataset_entry(name).await;
                let (triples, graphs) = if name == self.dataset {
                    (self.scalar("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }").await.ok(), self.graph_counts().await.ok().map(|g| g.len().saturating_sub(1)))
                } else {
                    (None, None)
                };
                Ok(dataset_detail(reference, entry.as_ref(), triples, graphs))
            }
            ObjectKind::Graph => {
                let count = self.graph_count(name).await;
                let sample = self.sample(&graph_sample_query(name), SAMPLE_TRIPLES).await;
                Ok(graph_detail(reference, count, sample))
            }
            ObjectKind::Prefix => {
                let namespace = prefix_namespace(reference);
                let count = self.namespace_uses().await.get(&namespace).copied();
                let q = format!("SELECT ?s ?p ?o WHERE {{ ?s ?p ?o FILTER(STRSTARTS(STR(?p), {})) }} LIMIT 25", sparql_string(&namespace));
                let sample = self.sample(&q, 25).await;
                Ok(prefix_detail(reference, count, sample))
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn explorer_stats(&self) -> AppResult<ServerStats> {
        let counts = DataCounts {
            triples: self.scalar("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }").await.ok(),
            graphs: self.graph_counts().await.map(|g| g.len().saturating_sub(1)).unwrap_or(0),
            classes: self.scalar("SELECT (COUNT(DISTINCT ?t) AS ?n) WHERE { ?s a ?t }").await.ok(),
            predicates: self.scalar("SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { ?s ?p ?o }").await.ok(),
            size: self.store_size().await,
        };
        let requests = match self.engine {
            Engine::ApacheJena => self.admin_json("/$/stats").await.map(|b| fuseki_stats(&b, &self.dataset)).unwrap_or_default(),
            _ => Vec::new(),
        };
        Ok(ServerStats::now(stat_groups(self.engine, &self.dataset, &counts, requests)))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { sql: false, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: false, exact_estimate: true },
        object_kinds: vec![K::Dataset, K::Graph, K::Prefix],
        tools: vec![T::Stats, T::GraphView],
    }
}

#[async_trait]
impl Integration for SparqlIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
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

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        self.explorer_objects(kind, parent).await
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        self.explorer_detail(reference).await
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        self.explorer_stats().await
    }
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

    #[test]
    fn admin_listings_become_datasets() {
        let fuseki = serde_json::json!({"datasets": [
            {"ds.name": "/ds", "ds.state": true, "ds.services": [{"srv.type": "query", "srv.description": "SPARQL Query", "srv.endpoints": ["sparql", "query"]}, {"srv.type": "gsp-rw"}]},
            {"ds.name": "/offline", "ds.state": false, "ds.services": []}
        ]});
        let d = fuseki_datasets(&fuseki);
        assert_eq!(d.iter().map(|x| x.reference.name.as_str()).collect::<Vec<_>>(), vec!["ds", "offline"]);
        assert_eq!(d[0].detail.as_deref(), Some("query, gsp-rw"));
        assert_eq!(d[0].badge.as_deref(), Some("active"));
        assert_eq!(d[1].badge.as_deref(), Some("offline"));
        assert!(d[1].detail.is_none());

        let gdb = graphdb_repositories(&serde_json::json!([{"id": "repo", "title": "Test repo", "writable": false, "location": ""}]));
        assert_eq!(gdb[0].detail.as_deref(), Some("Test repo"));
        assert_eq!(gdb[0].badge.as_deref(), Some("read-only"));
        let star = stardog_databases(&serde_json::json!({"databases": ["a", "b"]}));
        assert_eq!(star.len(), 2);
        assert_eq!(star[1].badge.as_deref(), Some("database"));

        let entry = fuseki.get("datasets").and_then(|d| d.as_array()).and_then(|a| a.first()).cloned().unwrap_or_default();
        let detail = dataset_detail(&d[0].reference, Some(&entry), Some(42), Some(2));
        assert_eq!(detail.language, CodeLanguage::Json);
        assert_eq!(detail.rows.as_ref().map(|r| r.rows.len()), Some(2));
        assert_eq!(detail.rows.as_ref().map(|r| r.rows[0][2].clone()), Some(Value::Text("sparql, query".into())));
        assert!(detail.properties.iter().any(|p| p.name == "triples" && p.value == "42"));
        assert!(detail.actions.iter().all(|a| !a.destructive));
        assert!(detail.actions.iter().all(|a| query_kind(&a.statement) == "select"));
    }

    #[test]
    fn graphs_carry_counts_and_update_actions() {
        let counts = vec![("default".to_string(), 120), ("http://ex/g1".to_string(), 1234)];
        let s = graph_summaries(&counts);
        assert_eq!(s[0].reference.name, "default");
        assert_eq!(s[0].badge.as_deref(), Some("default"));
        assert_eq!(s[1].detail.as_deref(), Some("1,234 triples"));
        assert_eq!(s[1].badge.as_deref(), Some("named"));

        let d = graph_detail(&s[1].reference, Some(1234), None);
        assert!(d.definition.as_deref().is_some_and(|q| q.contains("GRAPH <http://ex/g1>")));
        assert_eq!(d.actions.len(), 3);
        assert_eq!(d.actions[1].statement, "CLEAR GRAPH <http://ex/g1>");
        assert_eq!(d.actions[2].statement, "DROP GRAPH <http://ex/g1>");
        assert!(d.actions[1].destructive && d.actions[2].destructive && !d.actions[0].destructive);
        assert_eq!(query_kind(&d.actions[1].statement), "update");
        assert_eq!(query_kind(&d.actions[2].statement), "update");
        assert_eq!(query_kind(&d.actions[0].statement), "select");
        let def = graph_detail(&s[0].reference, None, None);
        assert_eq!(def.actions[1].statement, "CLEAR DEFAULT");
        assert!(!graph_sample_query("default").contains("GRAPH"));
    }

    #[test]
    fn prefixes_merge_well_known_and_observed() {
        let mut uses = BTreeMap::new();
        uses.insert("http://xmlns.com/foaf/0.1/".to_string(), 300);
        uses.insert("http://example.org/vocab/".to_string(), 12);
        let s = prefix_summaries(&uses);
        let names: Vec<&str> = s.iter().map(|p| p.reference.name.as_str()).collect();
        assert!(names.contains(&"foaf") && names.contains(&"rdf") && names.contains(&"http://example.org/vocab/"));
        assert_eq!(s.len(), COMMON_PREFIXES.len() + 1);
        let foaf = s.iter().find(|p| p.reference.name == "foaf").cloned().unwrap_or_else(|| ObjectSummary::new(ObjectKind::Prefix, "x", None));
        assert_eq!(foaf.badge.as_deref(), Some("well-known"));
        assert_eq!(foaf.detail.as_deref(), Some("http://xmlns.com/foaf/0.1/ · 300 use(s) in the sample"));
        let rdf = s.iter().find(|p| p.reference.name == "rdf").cloned().unwrap_or_else(|| ObjectSummary::new(ObjectKind::Prefix, "x", None));
        assert_eq!(rdf.badge.as_deref(), Some("unused"));
        let custom = s.iter().find(|p| p.reference.name == "http://example.org/vocab/").cloned().unwrap_or_else(|| ObjectSummary::new(ObjectKind::Prefix, "x", None));
        assert_eq!(custom.badge.as_deref(), Some("in data"));

        assert_eq!(namespace_of("http://xmlns.com/foaf/0.1/name").as_deref(), Some("http://xmlns.com/foaf/0.1/"));
        assert_eq!(namespace_of("http://www.w3.org/2001/XMLSchema#int").as_deref(), Some("http://www.w3.org/2001/XMLSchema#"));
        assert!(namespace_of("mailto:x").is_none());
        assert!(namespace_of("http://ex/").is_none());
        assert_eq!(prefix_namespace(&foaf.reference), "http://xmlns.com/foaf/0.1/");
        assert_eq!(prefix_namespace(&custom.reference), "http://example.org/vocab/");
        let d = prefix_detail(&foaf.reference, Some(300), None);
        assert_eq!(d.definition.as_deref(), Some("PREFIX foaf: <http://xmlns.com/foaf/0.1/>"));
        assert!(d.properties.iter().any(|p| p.name == "source" && p.value == "well-known prefix"));
        assert_eq!(query_kind(&d.actions[0].statement), "select");
        assert!(prefix_detail(&custom.reference, None, None).properties.iter().any(|p| p.value == "seen in the data"));
    }

    #[test]
    fn stats_groups_and_size_parsing() {
        let requests = fuseki_stats(&serde_json::json!({"datasets": {"/ds": {"Requests": 10, "RequestsGood": 9, "RequestsBad": 1}}}), "ds");
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].numeric, Some(10.0));
        assert!(fuseki_stats(&serde_json::json!({}), "ds").is_empty());
        let counts = DataCounts { triples: Some(1234), graphs: 2, classes: Some(5), predicates: Some(30), size: Some(1234) };
        let groups = stat_groups(Engine::ApacheJena, "ds", &counts, requests);
        assert_eq!(groups.iter().map(|g| g.title.as_str()).collect::<Vec<_>>(), vec!["Server", "Data", "Throughput"]);
        assert_eq!(groups[0].stats[0].value, "Apache Jena Fuseki");
        assert_eq!(groups[1].stats[0].value, "1,234");
        assert!(groups[1].stats.iter().any(|s| s.label == "Predicates" && s.numeric == Some(30.0)));
        assert!(groups[1].stats.iter().any(|s| s.label == "Statements (server)" && s.hint.is_some()));
        let bare = stat_groups(Engine::Virtuoso, "sparql", &DataCounts::default(), vec![]);
        assert_eq!(bare.len(), 2);
        assert_eq!(bare[0].stats[0].value, "Virtuoso");
        assert_eq!(bare[1].stats.len(), 1);
        assert_eq!(size_from_body(" 4321 "), Some(4321));
        assert_eq!(size_from_body("{\"total\": 99}"), Some(99));
        assert_eq!(size_from_body("<html>"), None);
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
