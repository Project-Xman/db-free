// SOT: basex-integration, xquery, basex-rest-api, basex-xml-listing, basex-object-explorer, basex-server-stats, basex-info-parsing

use crate::error::{AppError, AppResult};
use crate::integrations::http::{local, HttpClient};
use crate::integrations::prometheus::{human_bytes, truncate};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat, StatGroup,
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

// ---------------------------------------------------------------------------
// Object explorer / stats
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;
const DOC_CAP: usize = 1_048_576;
/// The index kinds `INFO INDEX` reports, in BaseX's own order.
const INDEX_KINDS: [(&str, &str); 5] = [("text", "textindex"), ("attribute", "attrindex"), ("token", "tokenindex"), ("fulltext", "ftindex"), ("path", "pathindex")];

// WHAT:  `INFO`-family output is `Key: Value` lines, sometimes indented under a
//        heading and sometimes prefixed with `- `. Both spellings collapse here.
pub fn parse_info(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|line| {
            let l = line.trim().trim_start_matches("- ").trim();
            let (k, v) = l.split_once(':')?;
            let (k, v) = (k.trim(), v.trim());
            (!k.is_empty() && !v.is_empty() && !k.contains(' ') || k.contains(' ') && !v.is_empty()).then(|| (k.to_string(), v.to_string()))
        })
        .collect()
}

pub fn info_value(pairs: &[(String, String)], key: &str) -> Option<String> {
    pairs.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v.clone())
}

// WHAT:  `INFO INDEX` splits into blank-line-separated blocks, each headed by
//        the index name ("Text Index", "Attribute Index"…) and followed by its
//        facts. Returns (kind, detail, raw block) per block recognised.
pub fn parse_info_index(text: &str) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for block in text.split("\n\n") {
        let block = block.trim();
        if block.is_empty() {
            continue;
        }
        let mut lines = block.lines();
        let Some(head) = lines.next().map(str::trim) else { continue };
        let lower = head.to_ascii_lowercase();
        let Some((kind, _)) = INDEX_KINDS.iter().find(|(name, _)| lower.starts_with(name)) else { continue };
        let facts = parse_info(block);
        let entries = info_value(&facts, "Entries");
        let size = info_value(&facts, "Size");
        let detail = match (entries, size) {
            (Some(e), Some(s)) => format!("{e} entries · {s}"),
            (Some(e), None) => format!("{e} entries"),
            (None, Some(s)) => s,
            (None, None) => head.to_string(),
        };
        out.push(((*kind).to_string(), detail, block.to_string()));
    }
    out
}

// WHAT:  `SHOW USERS` prints a two-column table under a dashed rule.
pub fn parse_users(text: &str) -> Vec<(String, String)> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('-') && !l.to_ascii_lowercase().starts_with("username"))
        .filter_map(|l| {
            let mut parts = l.split_whitespace();
            let name = parts.next()?;
            // A trailing summary line ("2 users") is not a user row.
            if name.parse::<u64>().is_ok() {
                return None;
            }
            Some((name.to_string(), parts.next().unwrap_or_default().to_string()))
        })
        .collect()
}

fn size_detail(size: Option<&str>) -> Option<String> {
    size.and_then(|s| s.parse::<f64>().ok()).map(human_bytes)
}

impl BasexIntegration {
    /// `GET /rest/{db}?command=…` — a database command with the db as context.
    async fn command(&self, db: Option<&str>, command: &str) -> AppResult<String> {
        let path = match db {
            Some(d) => format!("/rest/{}?command={}", encode(d), encode(command)),
            None => format!("/rest?command={}", encode(command)),
        };
        self.http.get_text(&path).await
    }

    async fn db_names(&self) -> AppResult<Vec<String>> {
        Ok(self.list_databases().await?.into_iter().map(|d| d.text).filter(|n| !n.is_empty()).collect())
    }

    fn scoped_dbs(&self, parent: Option<&str>) -> Option<Vec<String>> {
        parent.map(|p| vec![p.to_string()]).or_else(|| self.database.clone().map(|d| vec![d]))
    }

    // WHAT:  Index list for one database: `INFO INDEX` first (its own wording
    //        and entry counts), falling back to the `db:info` flags when the
    //        command is unavailable to this user.
    async fn indexes_of(&self, db: &str) -> Vec<ObjectSummary> {
        if let Ok(text) = self.command(Some(db), "INFO INDEX").await {
            let parsed = parse_info_index(&text);
            if !parsed.is_empty() {
                return parsed
                    .into_iter()
                    .map(|(kind, detail, _)| ObjectSummary::new(ObjectKind::Index, kind, Some(db.to_string())).with_detail(detail).with_badge("built"))
                    .collect();
            }
        }
        let quoted = format!("\"{}\"", db.replace('"', "&quot;"));
        let xml = self.xquery(&format!("db:info({quoted})")).await.unwrap_or_default();
        INDEX_KINDS
            .iter()
            .filter_map(|(kind, element)| {
                let on = elements_named(&xml, element).first().map(|e| e.text.trim().eq_ignore_ascii_case("true"))?;
                Some(
                    ObjectSummary::new(ObjectKind::Index, *kind, Some(db.to_string()))
                        .with_detail(if on { "available" } else { "not built" })
                        .with_badge(if on { "built" } else { "off" }),
                )
            })
            .collect()
    }

    async fn list_objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let mut out = match kind {
            ObjectKind::Database => self
                .list_databases()
                .await?
                .iter()
                .filter(|d| !d.text.is_empty())
                .map(|d| {
                    let resources = d.attr("resources").unwrap_or("0");
                    let detail = match size_detail(d.attr("size")) {
                        Some(size) => format!("{resources} resources · {size}"),
                        None => format!("{resources} resources"),
                    };
                    let mut s = ObjectSummary::new(ObjectKind::Database, d.text.clone(), None).with_detail(detail);
                    if let Some(modified) = d.attr("modified-date").or_else(|| d.attr("modified")) {
                        s = s.with_badge(truncate(modified, 10));
                    }
                    s
                })
                .collect(),
            ObjectKind::Document => {
                let dbs = match self.scoped_dbs(parent) {
                    Some(d) => d,
                    None => self.db_names().await?,
                };
                let mut out = Vec::new();
                for db in dbs {
                    for r in self.list_resources(&db).await.unwrap_or_default() {
                        let mut s = ObjectSummary::new(ObjectKind::Document, r.text.clone(), Some(db.clone()));
                        if let Some(size) = size_detail(r.attr("size")) {
                            s = s.with_detail(size);
                        }
                        let kind = r.attr("type").or_else(|| r.attr("raw").map(|raw| if raw == "true" { "raw" } else { "xml" })).unwrap_or("xml");
                        s = s.with_badge(kind);
                        out.push(s);
                    }
                }
                out
            }
            ObjectKind::Index => {
                let dbs = match self.scoped_dbs(parent) {
                    Some(d) => d,
                    None => self.db_names().await?,
                };
                let mut out = Vec::new();
                for db in dbs {
                    out.extend(self.indexes_of(&db).await);
                }
                out
            }
            ObjectKind::User => {
                let text = self.command(None, "SHOW USERS").await?;
                parse_users(&text)
                    .into_iter()
                    .map(|(name, permission)| {
                        let s = ObjectSummary::new(ObjectKind::User, name, None);
                        if permission.is_empty() {
                            s
                        } else {
                            s.with_badge(permission.to_lowercase())
                        }
                    })
                    .collect()
            }
            _ => Vec::new(),
        };
        out.sort_by(|a, b| a.reference.parent.cmp(&b.reference.parent).then_with(|| a.reference.name.cmp(&b.reference.name)));
        out.truncate(OBJECT_CAP);
        Ok(out)
    }

    async fn database_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let db = &reference.name;
        let info = self.command(Some(db), "INFO DB").await.unwrap_or_default();
        let pairs = parse_info(&info);
        let resources = self.list_resources(db).await.unwrap_or_default();
        let mut detail = ObjectDetail::empty(reference)
            .definition(if info.trim().is_empty() { format!("db:info(\"{db}\")") } else { info }, CodeLanguage::Text)
            .property("resources", resources.len().to_string());
        for key in ["Size", "Nodes", "Timestamp", "Path", "Input Path", "Up-to-date"] {
            if let Some(v) = info_value(&pairs, key) {
                detail = detail.property(&key.to_lowercase(), v);
            }
        }
        let mut children: Vec<ObjectSummary> = resources
            .iter()
            .map(|r| {
                let mut s = ObjectSummary::new(ObjectKind::Document, r.text.clone(), Some(db.clone()));
                if let Some(size) = size_detail(r.attr("size")) {
                    s = s.with_detail(size);
                }
                s.with_badge(r.attr("type").unwrap_or("xml"))
            })
            .collect();
        children.extend(self.indexes_of(db).await);
        children.truncate(OBJECT_CAP);
        detail.children = children;
        detail = detail
            .action(ObjectAction::new("info", "Database info", "INFO DB".to_string()))
            .action(ObjectAction::destructive("optimize", "Optimize", format!("db:optimize(\"{db}\")")))
            .action(ObjectAction::destructive("drop", "Drop database", format!("DROP DB {db}")));
        Ok(detail)
    }

    async fn document_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let db = reference.parent.clone().or_else(|| self.database.clone()).ok_or_else(|| AppError::invalid_input("A document needs its database."))?;
        let resources = self.list_resources(&db).await.unwrap_or_default();
        let meta = resources.iter().find(|r| r.text == reference.name);
        let body = self.http.get_text(&format!("/rest/{}/{}", encode(&db), encode(&reference.name))).await.unwrap_or_default();
        let truncated = body.len() > DOC_CAP;
        let mut text: String = body.chars().take(DOC_CAP).collect();
        if truncated {
            text.push_str("\n<!-- truncated: the document is larger than 1 MB -->");
        }
        let content_type = meta.and_then(|m| m.attr("content-type").or_else(|| m.attr("type"))).unwrap_or("application/xml").to_string();
        let language = if content_type.contains("xml") || reference.name.ends_with(".xml") { CodeLanguage::Xml } else { CodeLanguage::Text };
        let mut detail = ObjectDetail::empty(reference).definition(text, language).property("database", db.clone()).property("content-type", content_type);
        if let Some(size) = meta.and_then(|m| m.attr("size")) {
            detail = detail.property("size", size_detail(Some(size)).unwrap_or_else(|| size.to_string()));
        }
        if let Some(modified) = meta.and_then(|m| m.attr("modified-date").or_else(|| m.attr("modified"))) {
            detail = detail.property("modified", modified.to_string());
        }
        if truncated {
            detail = detail.property("truncated", "shown: first 1 MB");
        }
        let path = reference.name.replace('"', "&quot;");
        detail = detail
            .action(ObjectAction::new("open", "Open document", format!("GET {}", reference.name)))
            .action(ObjectAction::destructive("delete", "Delete document", format!("db:delete(\"{db}\", \"{path}\")")));
        Ok(detail)
    }

    async fn index_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let db = reference.parent.clone().or_else(|| self.database.clone()).ok_or_else(|| AppError::invalid_input("An index needs its database."))?;
        let raw = self.command(Some(&db), "INFO INDEX").await.unwrap_or_default();
        let block = parse_info_index(&raw).into_iter().find(|(kind, _, _)| *kind == reference.name);
        let (detail_text, body) = match block {
            Some((_, d, b)) => (d, b),
            None => (String::new(), raw.clone()),
        };
        let mut detail = ObjectDetail::empty(reference).definition(body.clone(), CodeLanguage::Text).property("database", db);
        if !detail_text.is_empty() {
            detail = detail.property("summary", detail_text);
        }
        for (k, v) in parse_info(&body) {
            detail = detail.property(&k.to_lowercase(), v);
        }
        Ok(detail)
    }

    async fn user_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let text = self.command(None, "SHOW USERS").await.unwrap_or_default();
        let permission = parse_users(&text)
            .into_iter()
            .find(|(name, _)| *name == reference.name)
            .map(|(_, p)| p)
            .ok_or_else(|| AppError::not_found(format!("User `{}` not found.", reference.name)))?;
        Ok(ObjectDetail::empty(reference).definition(text, CodeLanguage::Text).property("permission", permission))
    }

    async fn detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Database => self.database_detail(reference).await,
            ObjectKind::Document => self.document_detail(reference).await,
            ObjectKind::Index => self.index_detail(reference).await,
            ObjectKind::User => self.user_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn stats(&self) -> AppResult<ServerStats> {
        let info = self.command(None, "INFO").await.unwrap_or_default();
        let pairs = parse_info(&info);
        let databases = self.list_databases().await.unwrap_or_default();
        if info.trim().is_empty() && databases.is_empty() {
            return Err(AppError::driver("BaseX answered neither INFO nor the database listing."));
        }
        let number = |key: &str| info_value(&pairs, key).and_then(|v| v.trim_end_matches(|c: char| !c.is_ascii_digit()).parse::<f64>().ok());

        let mut server = Vec::new();
        for (label, key) in [("Version", "Version"), ("Java", "Java Version"), ("OS", "Operating System"), ("Path", "Database Path")] {
            if let Some(v) = info_value(&pairs, key) {
                server.push(Stat::text(label, truncate(&v, 60)));
            }
        }
        if server.is_empty() {
            server.push(Stat::text("Server", "BaseX"));
        }
        if let Some(db) = &self.database {
            server.push(Stat::text("Database", db.clone()));
        }

        let total_size: f64 = databases.iter().filter_map(|d| d.attr("size")).filter_map(|s| s.parse::<f64>().ok()).sum();
        let total_resources: f64 = databases.iter().filter_map(|d| d.attr("resources")).filter_map(|s| s.parse::<f64>().ok()).sum();
        let mut storage = vec![Stat::number("Databases", databases.len() as f64, None), Stat::number("Resources", total_resources, None)];
        if total_size > 0.0 {
            storage.push(Stat::number("Total size", crate::integrations::prometheus::mib(total_size), Some("MB")).with_hint(human_bytes(total_size)));
        }
        if let Some(largest) = databases.iter().max_by_key(|d| d.attr("size").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0)) {
            if !largest.text.is_empty() {
                storage.push(Stat::text("Largest database", largest.text.clone()));
            }
        }

        let mut memory = Vec::new();
        for (label, key) in [("Used memory", "Used Memory"), ("Max memory", "Maximum Memory")] {
            if let Some(v) = number(key) {
                // BaseX prints these in MB already ("123 MB").
                memory.push(Stat::number(label, v, Some("MB")));
            }
        }

        let groups = [("Server", server), ("Storage", storage), ("Memory", memory)]
            .into_iter()
            .filter(|(_, stats)| !stats.is_empty())
            .map(|(title, stats)| StatGroup { title: title.to_string(), stats })
            .collect();
        Ok(ServerStats::now(groups))
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, sql: false, namespaces: true, fixed_columns: true, paging: true, row_estimate: true, views: false, transactions: false, exact_estimate: true },
        object_kinds: vec![K::Database, K::Document, K::Index, K::User],
        tools: vec![T::Stats, T::XmlViewer],
    }
}

#[async_trait]
impl Integration for BasexIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
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

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        self.list_objects(kind, parent).await
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        self.detail(reference).await
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        self.stats().await
    }
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
    fn info_output_parses() {
        let info = "General Information\n\nVersion: 10.7\nDatabase Path: /srv/basex/data\nUsed Memory: 123 MB\nMaximum Memory: 4096 MB\nJava Version: 17.0.9\n";
        let pairs = parse_info(info);
        assert_eq!(info_value(&pairs, "Version").as_deref(), Some("10.7"));
        assert_eq!(info_value(&pairs, "used memory").as_deref(), Some("123 MB"));
        assert_eq!(info_value(&pairs, "absent"), None);

        let index = "Text Index\n- Entries: 1234\n- Size: 12 KB\n\nAttribute Index\n- Entries: 56\n\nToken Index\n- not available\n\nSomething Else\n- Entries: 1\n";
        let parsed = parse_info_index(index);
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].0, "text");
        assert_eq!(parsed[0].1, "1234 entries · 12 KB");
        assert_eq!(parsed[1].0, "attribute");
        assert_eq!(parsed[1].1, "56 entries");
        // A block with no facts still reports itself.
        assert_eq!(parsed[2].0, "token");
        assert!(parse_info_index("").is_empty());
    }

    #[test]
    fn user_listing_parses() {
        let text = "Username  Permission\n------------------\nadmin     ADMIN\nreader    READ\n\n2 users\n";
        let users = parse_users(text);
        assert_eq!(users, vec![("admin".to_string(), "ADMIN".to_string()), ("reader".to_string(), "READ".to_string())]);
        assert!(parse_users("Username  Permission\n---\n").is_empty());
        assert_eq!(size_detail(Some("2048")).as_deref(), Some("2.0 KB"));
        assert_eq!(size_detail(Some("not a number")), None);
        assert_eq!(size_detail(None), None);
    }

    #[test]
    fn self_closing_and_attrs() {
        let els = elements_named(r#"<r><item a="1" b='x y'/><item a="2">t</item></r>"#, "item");
        assert_eq!(els.len(), 2);
        assert_eq!(els[0].attr("b"), Some("x y"));
        assert_eq!(els[1].text, "t");
    }
}
