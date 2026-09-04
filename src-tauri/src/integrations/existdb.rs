// SOT: existdb-integration, xquery, exist-rest-api, exist-xml-tokenizer

use crate::error::{AppError, AppResult};
use crate::integrations::http::{local, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use std::sync::Arc;

// ============================================================================
// WHAT:  eXist-db adapter over its REST interface (port 8080, `/exist/rest`,
//        Basic auth). `database` = collection path (default `/db`). Schema =
//        the collection, tables = its sub-collections (View) and resources
//        (Table). Rows are the listing (name, created, last-modified, mime-type).
//        `execute` runs XQuery via `?_query=` and returns each result item
//        serialised as one row.
// WHY:   eXist answers with `<exist:result>` XML; a ~100 line tokenizer that
//        understands start / end / self-closing tags is all that is needed.
// HOW:   Writes (xmldb:store, xmldb:remove, update insert/delete/replace …)
//        are refused when the connection is read-only.
// WHERE: src-tauri/src/integrations/http.rs, src-tauri/src/integrations/basex.rs
// ============================================================================

const COLUMNS: [(&str, &str); 5] = [("name", "string"), ("kind", "string"), ("created", "dateTime"), ("last-modified", "dateTime"), ("mime-type", "string")];

pub struct ExistIntegration {
    engine: Engine,
    http: HttpClient,
    base: String,
    collection: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let mut auth = HttpClient::auth_from_connection(conn);
    if let crate::integrations::http::Auth::Bearer(p) = &auth {
        auth = crate::integrations::http::Auth::Basic { user: "admin".into(), password: p.clone() };
    }
    let http = HttpClient::from_connection(conn, Some(8080), false, auth)?;
    let host = conn.summary.host.as_deref().unwrap_or_default();
    let base = if host.trim_end_matches('/').ends_with("/rest") { String::new() } else { "/exist/rest".to_string() };
    let collection = normalise_collection(conn.summary.database.as_deref().unwrap_or("/db"));
    let integration = ExistIntegration { engine: conn.summary.engine, http, base, collection, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

pub fn normalise_collection(raw: &str) -> String {
    let t = raw.trim().trim_end_matches('/');
    if t.is_empty() {
        return "/db".into();
    }
    if t.starts_with('/') { t.to_string() } else { format!("/{t}") }
}

// ---------------------------------------------------------------------------
// Minimal XML tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Start { name: String, attrs: Vec<(String, String)>, self_closing: bool },
    End(String),
    Text(String),
}

pub fn xml_unescape(raw: &str) -> String {
    raw.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

fn parse_attrs(raw: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = raw.trim();
    while let Some(eq) = rest.find('=') {
        let key = rest[..eq].trim().to_string();
        let after = rest[eq + 1..].trim_start();
        let Some(quote) = after.chars().next() else { break };
        if quote != '"' && quote != '\'' {
            break;
        }
        let Some(close) = after[1..].find(quote) else { break };
        out.push((key, xml_unescape(&after[1..1 + close])));
        rest = after[1 + close + 1..].trim_start();
    }
    out
}

pub fn tokenize(xml: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut rest = xml;
    while !rest.is_empty() {
        let Some(lt) = rest.find('<') else {
            let t = xml_unescape(rest);
            if !t.trim().is_empty() {
                out.push(Token::Text(t));
            }
            break;
        };
        if lt > 0 {
            let t = xml_unescape(&rest[..lt]);
            if !t.trim().is_empty() {
                out.push(Token::Text(t));
            }
        }
        let after_lt = &rest[lt + 1..];
        if let Some(r) = after_lt.strip_prefix("![CDATA[") {
            let end = r.find("]]>").unwrap_or(r.len());
            out.push(Token::Text(r[..end].to_string()));
            rest = r.get(end + 3..).unwrap_or("");
            continue;
        }
        if after_lt.starts_with('?') || after_lt.starts_with('!') {
            let end = after_lt.find('>').map(|i| i + 1).unwrap_or(after_lt.len());
            rest = &after_lt[end..];
            continue;
        }
        let Some(gt) = find_tag_end(after_lt) else { break };
        let tag = after_lt[..gt].trim();
        rest = &after_lt[gt + 1..];
        if let Some(name) = tag.strip_prefix('/') {
            out.push(Token::End(name.trim().to_string()));
            continue;
        }
        let self_closing = tag.ends_with('/');
        let tag = tag.trim_end_matches('/').trim();
        let (name, attrs) = tag.split_once(char::is_whitespace).unwrap_or((tag, ""));
        out.push(Token::Start { name: name.to_string(), attrs: parse_attrs(attrs), self_closing });
    }
    out
}

// WHAT:  Index of the `>` closing this tag, skipping quoted attribute values.
fn find_tag_end(s: &str) -> Option<usize> {
    let mut quote: Option<char> = None;
    for (i, c) in s.char_indices() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), _) => {}
            (None, '"') | (None, '\'') => quote = Some(c),
            (None, '>') => return Some(i),
            _ => {}
        }
    }
    None
}

fn local(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

fn attr<'a>(attrs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    attrs.iter().find(|(k, _)| local(k) == name).map(|(_, v)| v.as_str())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    pub is_collection: bool,
    pub created: Option<String>,
    pub modified: Option<String>,
    pub mime: Option<String>,
}

// WHAT:  Children of the (outermost) <exist:collection> in a REST listing.
pub fn parse_listing(xml: &str) -> Vec<Entry> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    for tok in tokenize(xml) {
        match tok {
            Token::Start { name, attrs, self_closing } => {
                let l = local(&name);
                if l == "collection" {
                    if depth == 1 {
                        out.push(Entry { name: attr(&attrs, "name").unwrap_or_default().to_string(), is_collection: true, created: attr(&attrs, "created").map(str::to_string), modified: None, mime: None });
                    }
                    if !self_closing {
                        depth += 1;
                    }
                } else if l == "resource" && depth == 1 {
                    out.push(Entry {
                        name: attr(&attrs, "name").unwrap_or_default().to_string(),
                        is_collection: false,
                        created: attr(&attrs, "created").map(str::to_string),
                        modified: attr(&attrs, "last-modified").map(str::to_string),
                        mime: attr(&attrs, "mime-type").map(str::to_string),
                    });
                }
            }
            Token::End(name) if local(&name) == "collection" => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    out
}

// WHAT:  Serialised direct children of the root <exist:result> element, one per item.
pub fn parse_results(xml: &str) -> Vec<String> {
    let tokens = tokenize(xml);
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();
    let mut in_root = false;
    for tok in tokens {
        match &tok {
            Token::Start { name, attrs, self_closing } => {
                if !in_root {
                    in_root = true;
                    continue;
                }
                let attrs_s: String = attrs.iter().filter(|(k, _)| k != "xmlns:exist").map(|(k, v)| format!(" {k}=\"{}\"", v.replace('"', "&quot;"))).collect();
                current.push_str(&format!("<{name}{attrs_s}{}>", if *self_closing { "/" } else { "" }));
                if !*self_closing {
                    depth += 1;
                } else if depth == 0 {
                    out.push(std::mem::take(&mut current));
                }
            }
            Token::End(name) => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
                current.push_str(&format!("</{name}>"));
                if depth == 0 {
                    out.push(std::mem::take(&mut current));
                }
            }
            Token::Text(t) => {
                if depth == 0 {
                    for line in t.lines().map(str::trim).filter(|l| !l.is_empty()) {
                        out.push(line.to_string());
                    }
                } else {
                    current.push_str(&t.replace('&', "&amp;").replace('<', "&lt;"));
                }
            }
        }
    }
    out
}

pub fn is_write_xquery(text: &str) -> bool {
    let lower = text.to_lowercase();
    ["xmldb:store", "xmldb:remove", "xmldb:create-collection", "xmldb:move", "xmldb:rename", "xmldb:copy", "update insert", "update delete", "update replace", "update value", "update rename"].iter().any(|k| lower.contains(k))
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

fn path_encode(path: &str) -> String {
    path.split('/').map(urlencode).collect::<Vec<_>>().join("/")
}

impl ExistIntegration {
    fn rest(&self, collection: &str) -> String {
        format!("{}{}", self.base, path_encode(collection))
    }

    fn collection_for(&self, table: &TableRef) -> String {
        table.schema.clone().map(|s| normalise_collection(&s)).unwrap_or_else(|| self.collection.clone())
    }

    async fn listing(&self, collection: &str) -> AppResult<Vec<Entry>> {
        let xml = self.http.get_text(&self.rest(collection)).await?;
        Ok(parse_listing(&xml))
    }

    fn rows(entries: &[Entry]) -> Vec<Vec<Value>> {
        entries
            .iter()
            .map(|e| {
                vec![
                    Value::Text(e.name.clone()),
                    Value::Text(if e.is_collection { "collection" } else { "resource" }.into()),
                    e.created.clone().map(Value::DateTime).unwrap_or(Value::Null),
                    e.modified.clone().map(Value::DateTime).unwrap_or(Value::Null),
                    e.mime.clone().map(Value::Text).unwrap_or(Value::Null),
                ]
            })
            .collect()
    }

    fn column_names() -> Vec<String> {
        COLUMNS.iter().map(|(n, _)| (*n).to_string()).collect()
    }

    async fn xquery(&self, collection: &str, query: &str, max: usize) -> AppResult<String> {
        let path = format!("{}?_query={}&_howmany={}&_wrap=yes&_indent=no", self.rest(collection), urlencode(query), max.max(1));
        self.http.get_text(&path).await
    }
}

#[async_trait]
impl Integration for ExistIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: true, transactions: false, exact_estimate: true }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.http.get_text(&self.rest(&self.collection)).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let xml = self.xquery(&self.collection, "system:get-version()", 1).await.unwrap_or_default();
        let v = parse_results(&xml).into_iter().next().unwrap_or_default();
        Ok(Some(if v.is_empty() { "eXist-db".into() } else { format!("eXist-db {v}") }))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.collection.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let mut out = vec![self.collection.clone()];
        for e in self.listing(&self.collection).await.unwrap_or_default() {
            if e.is_collection {
                out.push(format!("{}/{}", self.collection.trim_end_matches('/'), e.name));
            }
        }
        Ok(out)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let entries = self.listing(&self.collection).await?;
        let tables = entries
            .iter()
            .map(|e| TableInfo { schema: Some(self.collection.clone()), name: e.name.clone(), kind: if e.is_collection { TableKind::View } else { TableKind::Table }, row_estimate: None })
            .collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.collection.clone(), tables }] })
    }

    async fn columns(&self, _table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(COLUMNS
            .iter()
            .enumerate()
            .map(|(i, (n, t))| ColumnInfo { name: (*n).into(), data_type: (*t).into(), nullable: i > 0, primary_key: i == 0, ordinal: i as u32 + 1 })
            .collect())
    }

    // WHAT:  A sub-collection table lists its own children; a resource table is one row.
    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let entries = self.entries_for(table).await?;
        Ok(local::apply_filters(&Self::column_names(), Self::rows(&entries), filters).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let entries = self.entries_for(table).await?;
        let rows = local::page(&Self::column_names(), Self::rows(&entries), query);
        Ok(ResultSet { columns: COLUMNS.iter().map(|(n, t)| ColumnMeta { name: (*n).into(), type_name: (*t).into() }).collect(), rows, truncated: false })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let text = sql.trim();
        if text.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        if self.read_only && is_write_xquery(text) {
            return Err(AppError::read_only("This connection is read-only; xmldb:store / update expressions are blocked."));
        }
        if let Some(path) = text.strip_prefix("GET ").or_else(|| text.strip_prefix("get ")) {
            let full = if path.starts_with('/') { path.trim().to_string() } else { format!("{}/{}", self.collection.trim_end_matches('/'), path.trim()) };
            let body = self.http.get_text(&self.rest(&full)).await?;
            return Ok(vec![StatementResult::Rows { result: ResultSet { columns: vec![ColumnMeta { name: "content".into(), type_name: "xml".into() }], rows: vec![vec![Value::Text(body)]], truncated: false } }]);
        }
        let xml = self.xquery(&self.collection, text, max_rows).await?;
        let items = parse_results(&xml);
        let truncated = items.len() > max_rows;
        let rows = items.into_iter().take(max_rows).map(|i| vec![Value::Text(i)]).collect();
        Ok(vec![StatementResult::Rows { result: ResultSet { columns: vec![ColumnMeta { name: "result".into(), type_name: "string".into() }], rows, truncated } }])
    }

    async fn close(&self) {}
}

impl ExistIntegration {
    async fn entries_for(&self, table: &TableRef) -> AppResult<Vec<Entry>> {
        let coll = self.collection_for(table);
        let entries = self.listing(&coll).await?;
        match entries.iter().find(|e| e.name == table.name) {
            Some(e) if e.is_collection => self.listing(&format!("{}/{}", coll.trim_end_matches('/'), table.name)).await,
            Some(e) => Ok(vec![e.clone()]),
            None => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

    const LISTING: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<exist:result xmlns:exist="http://exist.sourceforge.net/NS/exist" exist:hits="0" exist:start="1" exist:count="0">
  <exist:collection name="/db/apps" created="2024-01-01T00:00:00Z" owner="SYSTEM" group="dba" permissions="rwxr-xr-x">
    <exist:collection name="dashboard" created="2024-01-02T00:00:00Z"/>
    <exist:collection name="nested" created="2024-01-02T00:00:00Z"><exist:resource name="inner.xml" mime-type="application/xml"/></exist:collection>
    <exist:resource name="doc.xml" created="2024-01-03T00:00:00Z" last-modified="2024-01-04T00:00:00Z" mime-type="application/xml" owner="admin" group="dba" permissions="rw-r--r--"/>
  </exist:collection>
</exist:result>"#;

    #[test]
    fn tokenizer_handles_all_tag_kinds() {
        let toks = tokenize(r#"<a x="1>2"><b/>hi &amp; bye</a>"#);
        assert_eq!(toks[0], Token::Start { name: "a".into(), attrs: vec![("x".into(), "1>2".into())], self_closing: false });
        assert_eq!(toks[1], Token::Start { name: "b".into(), attrs: vec![], self_closing: true });
        assert_eq!(toks[2], Token::Text("hi & bye".into()));
        assert_eq!(toks[3], Token::End("a".into()));
    }

    #[test]
    fn parses_collection_listing() {
        let entries = parse_listing(LISTING);
        assert_eq!(entries.len(), 3);
        assert!(entries[0].is_collection);
        assert_eq!(entries[0].name, "dashboard");
        assert_eq!(entries[1].name, "nested");
        assert_eq!(entries[2].name, "doc.xml");
        assert_eq!(entries[2].modified.as_deref(), Some("2024-01-04T00:00:00Z"));
        assert_eq!(entries[2].mime.as_deref(), Some("application/xml"));
    }

    #[test]
    fn parses_query_results() {
        let xml = r#"<exist:result xmlns:exist="http://exist.sourceforge.net/NS/exist" exist:hits="3" exist:start="1" exist:count="3"><item id="1"><name>a</name></item><item id="2"/>
        <exist:value exist:type="xs:integer">42</exist:value></exist:result>"#;
        let items = parse_results(xml);
        assert_eq!(items, vec!["<item id=\"1\"><name>a</name></item>", "<item id=\"2\"/>", "<exist:value exist:type=\"xs:integer\">42</exist:value>"]);
        let plain = parse_results(r#"<exist:result xmlns:exist="x">hello
world</exist:result>"#);
        assert_eq!(plain, vec!["hello", "world"]);
    }

    #[test]
    fn write_detection_and_paths() {
        assert!(is_write_xquery("xmldb:store('/db', 'a.xml', <a/>)"));
        assert!(is_write_xquery("update insert <b/> into /a"));
        assert!(!is_write_xquery("//a[@id='update insert']".replace("update insert", "x").as_str()));
        assert_eq!(normalise_collection("db/apps/"), "/db/apps");
        assert_eq!(normalise_collection(""), "/db");
        assert_eq!(path_encode("/db/my apps"), "/db/my%20apps");
    }

    // Runs only when DBFREE_TEST_EXISTDB_URL is set:
    // `docker run --rm -d -p 8080:8080 existdb/existdb:latest`.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_EXISTDB_URL") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Existdb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: Some("/db".into()),
                username: Some(std::env::var("DBFREE_TEST_EXISTDB_USER").unwrap_or_else(|_| "admin".into())),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: Some(std::env::var("DBFREE_TEST_EXISTDB_PASSWORD").unwrap_or_default()),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default();
        assert!(version.is_some(), "no version reported");
        let cat = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(!cat.schemas.is_empty(), "{cat:?}");
        // /db always has the `system` child collection on a fresh server.
        assert!(
            cat.schemas.iter().any(|s| s.tables.iter().any(|t| t.name.contains("system"))),
            "{:?}",
            cat.schemas.iter().flat_map(|s| s.tables.iter().map(|t| t.name.clone())).take(20).collect::<Vec<_>>()
        );
        let table = cat
            .schemas
            .first()
            .and_then(|s| s.tables.first())
            .map(|t| TableRef { schema: t.schema.clone(), name: t.name.clone() })
            .unwrap_or_else(|| panic!("no table in catalog"));
        let cols = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "name" && c.primary_key), "{cols:?}");
        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert!(!page.columns.is_empty(), "{page:?}");
        // XQuery through the REST endpoint.
        let out = db
            .execute("for $i in 1 to 3 return <n>{$i}</n>", 10)
            .await
            .unwrap_or_else(|e| panic!("xquery: {e}"));
        match out.first() {
            Some(StatementResult::Rows { result }) => assert!(!result.rows.is_empty(), "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        db.close().await;
    }

}
