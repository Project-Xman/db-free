// SOT: basex-integration, xquery, basex-rest-api, basex-xml-listing

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
// WHAT:  BaseX adapter over its REST API (port 8984, `/rest`, Basic auth).
//        Schema = database, table = resource (document) inside it. Rows are
//        the resource listing (path, content-type, size); `execute` runs XQuery
//        via POST /rest with a <query> envelope.
// WHY:   BaseX REST answers in a tiny XML dialect, so a ~60 line attribute
//        scanner is enough — no XML crate.
// HOW:   Listing pages are client-side (`http::local`). Commands (CREATE DB,
//        DROP DB, ADD, DELETE, db:create/db:add …) are refused when read-only.
// WHERE: src-tauri/src/integrations/http.rs, src-tauri/src/integrations/existdb.rs
// ============================================================================

const LIST_CAP: usize = 5_000;
const COLUMNS: [(&str, &str); 3] = [("path", "string"), ("content-type", "string"), ("size", "integer")];

pub struct BasexIntegration {
    engine: Engine,
    http: HttpClient,
    database: Option<String>,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let mut auth = HttpClient::auth_from_connection(conn);
    if let crate::integrations::http::Auth::Bearer(p) = &auth {
        auth = crate::integrations::http::Auth::Basic { user: "admin".into(), password: p.clone() };
    }
    let http = HttpClient::from_connection(conn, Some(8984), false, auth)?;
    let database = conn.summary.database.as_deref().map(str::trim).filter(|d| !d.is_empty()).map(str::to_string);
    let integration = BasexIntegration { engine: conn.summary.engine, http, database, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Minimal XML helpers (elements + attributes; enough for BaseX REST listings)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlElement {
    pub name: String,
    pub attrs: Vec<(String, String)>,
    pub text: String,
}

impl XmlElement {
    pub fn attr(&self, name: &str) -> Option<&str> {
        self.attrs.iter().find(|(k, _)| k == name || k.rsplit(':').next() == Some(name)).map(|(_, v)| v.as_str())
    }
}

pub fn xml_unescape(raw: &str) -> String {
    raw.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

pub fn xml_escape(raw: &str) -> String {
    raw.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

// WHAT:  Every element whose local name matches `local`, with attributes and
//        direct text content (children are flattened into the text).
pub fn elements_named(xml: &str, local: &str) -> Vec<XmlElement> {
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find('<') {
        let tag_rest = &rest[start + 1..];
        if tag_rest.starts_with('?') || tag_rest.starts_with('!') || tag_rest.starts_with('/') {
            rest = &rest[start + 1..];
            continue;
        }
        let Some(end) = tag_rest.find('>') else { break };
        let tag = &tag_rest[..end];
        let self_closing = tag.ends_with('/');
        let tag = tag.trim_end_matches('/').trim();
        let (name, attr_str) = tag.split_once(char::is_whitespace).unwrap_or((tag, ""));
        let after = &tag_rest[end + 1..];
        if name.rsplit(':').next() == Some(local) {
            let text = if self_closing {
                String::new()
            } else {
                let close = format!("</{name}>");
                let body = after.find(&close).map(|i| &after[..i]).unwrap_or("");
                strip_tags(body)
            };
            out.push(XmlElement { name: name.to_string(), attrs: parse_attrs(attr_str), text });
        }
        rest = after;
    }
    out
}

fn strip_tags(body: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in body.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            c if !in_tag => out.push(c),
            _ => {}
        }
    }
    xml_unescape(out.trim())
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

pub fn is_write_xquery(text: &str) -> bool {
    let upper = text.to_uppercase();
    let head = upper.split_whitespace().next().unwrap_or_default();
    matches!(head, "CREATE" | "DROP" | "ADD" | "DELETE" | "REPLACE" | "STORE" | "RENAME" | "OPTIMIZE" | "COPY" | "ALTER" | "RESTORE")
        || ["DB:CREATE", "DB:ADD", "DB:DELETE", "DB:DROP", "DB:REPLACE", "DB:STORE", "DB:RENAME", "DB:PUT", "DB:OPTIMIZE", "DB:ALTER", "DB:COPY", "DB:RESTORE", "UPDATE:", "INSERT NODE", "DELETE NODE", "REPLACE NODE", "RENAME NODE"]
            .iter()
            .any(|k| upper.contains(k))
}

// WHAT:  BaseX serialises sequence items separated by newlines; split unless it
//        looks like a single XML document.
pub fn split_results(text: &str) -> Vec<String> {
    let t = text.trim();
    if t.is_empty() {
        return vec![];
    }
    if t.starts_with('<') && t.matches("\n<").count() <= 1 && !t.starts_with("<?") {
        return vec![t.to_string()];
    }
    t.lines().map(|l| l.trim_end().to_string()).filter(|l| !l.is_empty()).collect()
}

fn encode(raw: &str) -> String {
    let mut out = String::new();
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'/') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl BasexIntegration {
    async fn list_databases(&self) -> AppResult<Vec<XmlElement>> {
        let xml = self.http.get_text("/rest").await?;
        Ok(elements_named(&xml, "database"))
    }

    async fn list_resources(&self, db: &str) -> AppResult<Vec<XmlElement>> {
        let xml = self.http.get_text(&format!("/rest/{}", encode(db))).await?;
        Ok(elements_named(&xml, "resource"))
    }

    fn db_for(&self, table: &TableRef) -> AppResult<String> {
        table.schema.clone().or_else(|| self.database.clone()).ok_or_else(|| AppError::invalid_input("Select a BaseX database first."))
    }

    fn resource_rows(resources: &[XmlElement]) -> Vec<Vec<Value>> {
        resources
            .iter()
            .map(|r| {
                vec![
                    Value::Text(r.text.clone()),
                    Value::Text(r.attr("content-type").or_else(|| r.attr("type")).unwrap_or_default().to_string()),
                    r.attr("size").and_then(|s| s.parse::<i64>().ok()).map(Value::Int).unwrap_or(Value::Null),
                ]
            })
            .collect()
    }

    async fn xquery(&self, query: &str) -> AppResult<String> {
        let body = format!("<query xmlns=\"http://basex.org/rest\"><text><![CDATA[{}]]></text></query>", query.replace("]]>", "]]]]><![CDATA[>"));
        self.http.post_raw("/rest", "application/xml", body, Some("*/*")).await
    }
}

#[async_trait]
impl Integration for BasexIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: false, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.http.get_text("/rest").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v = self.xquery("db:system()/generalinformation/version/string()").await.unwrap_or_default();
        let v = v.trim();
        Ok(Some(if v.is_empty() { "BaseX".into() } else { format!("BaseX {v}") }))
    }

    fn current_database(&self) -> Option<String> {
        self.database.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(self.list_databases().await?.into_iter().map(|d| d.text).filter(|n| !n.is_empty()).collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let dbs = match &self.database {
            Some(d) => vec![d.clone()],
            None => self.databases().await?,
        };
        let mut schemas = Vec::new();
        for db in dbs {
            let resources = self.list_resources(&db).await.unwrap_or_default();
            let tables = resources
                .iter()
                .take(LIST_CAP)
                .map(|r| TableInfo { schema: Some(db.clone()), name: r.text.clone(), kind: TableKind::Table, row_estimate: r.attr("size").and_then(|s| s.parse().ok()) })
                .collect();
            schemas.push(SchemaInfo { name: db, tables });
        }
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, _table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(COLUMNS
            .iter()
            .enumerate()
            .map(|(i, (n, t))| ColumnInfo { name: (*n).into(), data_type: (*t).into(), nullable: i > 0, primary_key: i == 0, ordinal: i as u32 + 1 })
            .collect())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let db = self.db_for(table)?;
        let resources = self.list_resources(&db).await?;
        let rows = Self::resource_rows(&resources.into_iter().filter(|r| r.text == table.name || r.text.starts_with(&format!("{}/", table.name))).collect::<Vec<_>>());
        let names: Vec<String> = COLUMNS.iter().map(|(n, _)| (*n).to_string()).collect();
        Ok(local::apply_filters(&names, rows, filters).len() as i64)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let db = self.db_for(table)?;
        let resources = self.list_resources(&db).await?;
        let matching: Vec<XmlElement> = resources.into_iter().filter(|r| r.text == table.name || r.text.starts_with(&format!("{}/", table.name))).collect();
        let names: Vec<String> = COLUMNS.iter().map(|(n, _)| (*n).to_string()).collect();
        let rows = local::page(&names, Self::resource_rows(&matching), query);
        Ok(ResultSet { columns: COLUMNS.iter().map(|(n, t)| ColumnMeta { name: (*n).into(), type_name: (*t).into() }).collect(), rows, truncated: false })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let text = sql.trim();
        if text.is_empty() {
            return Err(AppError::invalid_input("Nothing to run."));
        }
        if self.read_only && is_write_xquery(text) {
            return Err(AppError::read_only("This connection is read-only; database commands and updating expressions are blocked."));
        }
        let mut upper_head = text.split_whitespace().next().unwrap_or_default().to_uppercase();
        upper_head.truncate(6);
        if upper_head == "OPEN" || upper_head == "GET" {
            // `GET <path>` reads one resource of the current database.
            let path = text.split_whitespace().nth(1).ok_or_else(|| AppError::invalid_input("GET needs a resource path."))?;
            let db = self.database.clone().ok_or_else(|| AppError::invalid_input("Select a BaseX database first."))?;
            let body = self.http.get_text(&format!("/rest/{}/{}", encode(&db), encode(path))).await?;
            return Ok(vec![StatementResult::Rows { result: ResultSet { columns: vec![ColumnMeta { name: "content".into(), type_name: "xml".into() }], rows: vec![vec![Value::Text(body)]], truncated: false } }]);
        }
        let query = match &self.database {
            Some(db) if !text.to_lowercase().contains("db:") && !text.to_lowercase().contains("doc(") && !text.to_lowercase().contains("collection(") => {
                {
                let quoted = format!("\"{}\"", db.replace('"', "&quot;"));
                format!("declare context item := db:get({quoted});\n{text}")
            }
            }
            _ => text.to_string(),
        };
        let out = match self.xquery(&query).await {
            Ok(o) => o,
            Err(_) if query != text => self.xquery(text).await?,
            Err(e) => return Err(e),
        };
        let items = split_results(&out);
        let truncated = items.len() > max_rows;
        let rows = items.into_iter().take(max_rows).map(|l| vec![Value::Text(l)]).collect();
        Ok(vec![StatementResult::Rows { result: ResultSet { columns: vec![ColumnMeta { name: "result".into(), type_name: "string".into() }], rows, truncated } }])
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_database_listing() {
        let xml = r#"<?xml version="1.0"?><databases xmlns="http://basex.org/rest"><database resources="2" size="12345">factbook</database><database resources="0" size="0">empty &amp; co</database></databases>"#;
        let dbs = elements_named(xml, "database");
        assert_eq!(dbs.len(), 2);
        assert_eq!(dbs[0].text, "factbook");
        assert_eq!(dbs[0].attr("resources"), Some("2"));
        assert_eq!(dbs[1].text, "empty & co");
    }

    #[test]
    fn parses_resource_listing() {
        let xml = r#"<rest:database name="factbook" resources="2" xmlns:rest="http://basex.org/rest"><rest:resource type="xml" content-type="application/xml" size="1256">factbook.xml</rest:resource><rest:resource type="raw" content-type="image/png" size="10">img/x.png</rest:resource></rest:database>"#;
        let res = elements_named(xml, "resource");
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].text, "factbook.xml");
        assert_eq!(res[1].attr("content-type"), Some("image/png"));
        let rows = BasexIntegration::resource_rows(&res);
        assert_eq!(rows[1][2], Value::Int(10));
    }

    #[test]
    fn splits_results_and_detects_writes() {
        assert_eq!(split_results("1\n2\n3"), vec!["1", "2", "3"]);
        assert_eq!(split_results("<a>\n  <b/>\n  <c/>\n</a>").len(), 1);
        assert!(split_results("").is_empty());
        assert!(is_write_xquery("CREATE DB test"));
        assert!(is_write_xquery("db:add('x', <a/>, 'a.xml')"));
        assert!(is_write_xquery("insert node <x/> into /a"));
        assert!(!is_write_xquery("for $x in //item return $x"));
    }

    #[test]
    fn self_closing_and_attrs() {
        let els = elements_named(r#"<r><item a="1" b='x y'/><item a="2">t</item></r>"#, "item");
        assert_eq!(els.len(), 2);
        assert_eq!(els[0].attr("b"), Some("x y"));
        assert_eq!(els[1].text, "t");
    }
}
