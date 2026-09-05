// SOT: s3-integration, s3-rest-api, object-storage, s3-bucket-browser, s3-list-objects

use crate::error::{AppError, AppResult};
use crate::integrations::aws_sigv4::{sign, uri_encode, AwsCredentials, SignRequest};
use crate::integrations::existdb::{tokenize, Token};
use crate::integrations::http::{local, Auth, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat, StatGroup,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use std::sync::Arc;

// ============================================================================
// WHAT:  S3-compatible object storage as a browsable engine: buckets are the
//        tables, objects are the rows.
// WHY:   An object store is the one place a database workbench routinely needs
//        and never has — exports land there, backups live there, and data lakes
//        are just prefixes. Listing is a read-only, paginated, keyed view, which
//        is exactly the shape the grid already draws.
// HOW:   `host` = region, `username` = access key id, secret = secret access key
//        (`SECRET|SESSION_TOKEN` for temporary credentials) and `database` = an
//        optional endpoint override — the same convention DynamoDB uses. With an
//        override the adapter addresses path-style (`{endpoint}/{bucket}/{key}`,
//        what MinIO and R2 want); against real AWS it uses virtual-host style
//        (`{bucket}.s3.{region}.amazonaws.com`) because that is what new buckets
//        require. Every request is SigV4-signed for service "s3", which needs the
//        generalised signer: GET and DELETE with a canonical query string, not
//        just the JSON POST DynamoDB uses.
//        Responses are XML, parsed with the tokenizer existdb already owns
//        rather than adding an XML crate.
//        `execute` speaks a small command language (BUCKETS / LIST / GET / HEAD
//        / DELETE) since S3 has no query language; writes are refused here when
//        the connection is read-only, because the SQL guard cannot parse it.
// WHERE: src-tauri/src/integrations/aws_sigv4.rs (signing),
//        src-tauri/src/integrations/existdb.rs (tokenize)
// ============================================================================

const MAX_KEYS: u32 = 1_000;
const MAX_PAGE_ROWS: u32 = 5_000;
/// Columns every listing row carries, in grid order.
const COLUMNS: [(&str, &str); 5] = [
    ("key", "text"),
    ("size", "bigint"),
    ("last_modified", "timestamp"),
    ("storage_class", "text"),
    ("etag", "text"),
];

pub struct S3Integration {
    engine: Engine,
    http: HttpClient,
    creds: AwsCredentials,
    /// None against real AWS (virtual-host addressing), Some for MinIO / R2.
    endpoint: Option<String>,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let creds = AwsCredentials::from_connection(conn)?;
    let endpoint = conn
        .summary
        .database
        .as_deref()
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| e.trim_end_matches('/').to_string());
    let http = HttpClient::new(endpoint.clone().unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", creds.region)), Auth::None, false)?;
    let integration = S3Integration { engine: conn.summary.engine, http, creds, endpoint, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// WHAT:  The text of every `<tag>` directly inside each `<record>` element.
// WHY:   S3's list responses are flat repeated records (`Bucket`, `Contents`,
//        `CommonPrefixes`), so one pass collecting field text per record covers
//        all of them without a DOM.
fn records(xml: &str, record: &str) -> Vec<Vec<(String, String)>> {
    let mut out = Vec::new();
    let mut current: Option<Vec<(String, String)>> = None;
    let mut field: Option<String> = None;
    for token in tokenize(xml) {
        match token {
            Token::Start { name, self_closing, .. } => {
                if name == record {
                    current = Some(Vec::new());
                } else if current.is_some() && !self_closing {
                    field = Some(name);
                }
            }
            Token::Text(text) => {
                if let (Some(rec), Some(key)) = (current.as_mut(), field.as_ref()) {
                    rec.push((key.clone(), text));
                }
            }
            Token::End(name) => {
                if name == record {
                    if let Some(rec) = current.take() {
                        out.push(rec);
                    }
                } else if field.as_deref() == Some(name.as_str()) {
                    field = None;
                }
            }
        }
    }
    out
}

/// The text of the first top-level `<tag>` in the document.
fn first_value(xml: &str, tag: &str) -> Option<String> {
    let mut want = false;
    for token in tokenize(xml) {
        match token {
            Token::Start { ref name, .. } if name == tag => want = true,
            Token::Text(text) if want => return Some(text),
            Token::End(ref name) if name == tag => want = false,
            _ => {}
        }
    }
    None
}

fn field<'a>(rec: &'a [(String, String)], key: &str) -> Option<&'a str> {
    rec.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

fn columns() -> Vec<ColumnMeta> {
    COLUMNS.iter().map(|(name, ty)| ColumnMeta { name: (*name).into(), type_name: (*ty).into() }).collect()
}

fn column_names() -> Vec<String> {
    COLUMNS.iter().map(|(name, _)| (*name).to_string()).collect()
}

// WHAT:  One `<Contents>` record as a grid row.
fn object_row(rec: &[(String, String)]) -> Vec<Value> {
    vec![
        Value::Text(field(rec, "Key").unwrap_or_default().to_string()),
        field(rec, "Size").and_then(|s| s.parse::<i64>().ok()).map(Value::Int).unwrap_or(Value::Null),
        field(rec, "LastModified").map(|s| Value::DateTime(s.to_string())).unwrap_or(Value::Null),
        Value::Text(field(rec, "StorageClass").unwrap_or("STANDARD").to_string()),
        Value::Text(field(rec, "ETag").unwrap_or_default().trim_matches('"').to_string()),
    ]
}

pub fn human_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

impl S3Integration {
    // WHAT:  (absolute url, host header) for one bucket + key.
    // WHY:   The signature covers the host, so the address and the signed host
    //        must be derived together or every request 403s.
    fn address(&self, bucket: Option<&str>, key: Option<&str>) -> (String, String, String) {
        let encoded_key = key.map(|k| uri_encode(k, false)).unwrap_or_default();
        match &self.endpoint {
            // Path-style: MinIO, Cloudflare R2 and every S3-compatible server.
            Some(endpoint) => {
                let host = endpoint.split("://").nth(1).unwrap_or(endpoint).trim_end_matches('/').to_string();
                let path = match (bucket, key) {
                    (Some(b), Some(_)) => format!("/{b}/{encoded_key}"),
                    (Some(b), None) => format!("/{b}"),
                    _ => "/".to_string(),
                };
                (format!("{endpoint}{path}"), host, path)
            }
            // Virtual-host style: what AWS requires for buckets made since 2020.
            None => {
                let region = &self.creds.region;
                let host = match bucket {
                    Some(b) => format!("{b}.s3.{region}.amazonaws.com"),
                    None => format!("s3.{region}.amazonaws.com"),
                };
                let path = if key.is_some() { format!("/{encoded_key}") } else { "/".to_string() };
                (format!("https://{host}{path}"), host, path)
            }
        }
    }

    // WHAT:  Signs and sends one S3 request, returning the body text.
    // HOW:   `query` pairs are sorted and encoded here so the canonical query
    //        string handed to the signer is exactly what goes on the wire.
    async fn call(&self, method: Method, bucket: Option<&str>, key: Option<&str>, query: &[(&str, String)]) -> AppResult<String> {
        let (url, host, path) = self.address(bucket, key);
        let mut pairs: Vec<(String, String)> = query.iter().map(|(k, v)| (uri_encode(k, true), uri_encode(v, true))).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_query: String = pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");

        let signed = sign(
            &self.creds,
            &SignRequest {
                service: "s3",
                method: method.as_str(),
                host: &host,
                path: &path,
                query: &canonical_query,
                amz_target: None,
                // S3 GET/DELETE carry no body, so content-type is not signed.
                content_type: None,
                body: b"",
                now: chrono::Utc::now(),
            },
        )?;

        let full = if canonical_query.is_empty() { url } else { format!("{url}?{canonical_query}") };
        let mut req = self.http.request(method, &full);
        for (name, value) in signed.headers {
            // reqwest sets Host itself from the URL.
            if name != "host" {
                req = req.header(name, value);
            }
        }
        let resp = self.http.send(req).await?;
        resp.text().await.map_err(|e| AppError::internal(format!("S3 response was not readable: {e}")))
    }

    async fn buckets(&self) -> AppResult<Vec<(String, String)>> {
        let body = self.call(Method::GET, None, None, &[]).await?;
        Ok(records(&body, "Bucket")
            .iter()
            .filter_map(|r| field(r, "Name").map(|n| (n.to_string(), field(r, "CreationDate").unwrap_or_default().to_string())))
            .collect())
    }

    // WHAT:  Lists up to `wanted` keys, following continuation tokens.
    // WHY:   S3 pages by cursor while the grid pages by offset, so the window the
    //        grid asked for is assembled here and sliced client-side.
    async fn list(&self, bucket: &str, prefix: Option<&str>, delimiter: Option<&str>, wanted: u32) -> AppResult<ListPage> {
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let remaining = wanted.saturating_sub(rows.len() as u32).min(MAX_KEYS);
            if remaining == 0 {
                break;
            }
            let mut query: Vec<(&str, String)> = vec![("list-type", "2".into()), ("max-keys", remaining.to_string())];
            if let Some(p) = prefix.filter(|p| !p.is_empty()) {
                query.push(("prefix", p.to_string()));
            }
            if let Some(d) = delimiter {
                query.push(("delimiter", d.to_string()));
            }
            if let Some(t) = &token {
                query.push(("continuation-token", t.clone()));
            }
            let body = self.call(Method::GET, Some(bucket), None, &query).await?;
            rows.extend(records(&body, "Contents").iter().map(|r| object_row(r)));
            prefixes.extend(records(&body, "CommonPrefixes").iter().filter_map(|r| field(r, "Prefix").map(str::to_string)));
            token = first_value(&body, "NextContinuationToken");
            if token.is_none() {
                break;
            }
        }
        Ok(ListPage { rows, prefixes, truncated: token.is_some() })
    }

    // WHAT:  A StartsWith filter on `key` is the S3 `prefix` parameter.
    // WHY:   Pushing it to the server turns a full-bucket scan into a targeted
    //        listing — the difference between usable and not on a large bucket.
    fn prefix_from(filters: &[FilterRule]) -> Option<String> {
        filters
            .iter()
            .find(|f| f.column == "key" && f.op == FilterOp::StartsWith)
            .map(|f| f.value.trim().to_string())
            .filter(|p| !p.is_empty())
    }
}

struct ListPage {
    rows: Vec<Vec<Value>>,
    prefixes: Vec<String>,
    truncated: bool,
}

pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true,
            sql: false,
            namespaces: false,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: false,
        },
        object_kinds: vec![K::Bucket, K::Prefix],
        tools: vec![T::Stats],
    }
}

#[async_trait]
impl Integration for S3Integration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        self.buckets().await.map(|_| ())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some(match &self.endpoint {
            Some(e) => format!("S3-compatible ({e})"),
            None => format!("Amazon S3 ({})", self.creds.region),
        }))
    }

    fn current_database(&self) -> Option<String> {
        self.endpoint.clone()
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(self.endpoint.clone().into_iter().collect())
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut tables: Vec<TableInfo> = self
            .buckets()
            .await?
            .into_iter()
            .map(|(name, _)| TableInfo { schema: None, name, kind: TableKind::Table, row_estimate: None })
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: "buckets".into(), tables }] })
    }

    async fn columns(&self, _table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        Ok(COLUMNS
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| ColumnInfo {
                name: (*name).into(),
                data_type: (*ty).into(),
                nullable: i != 0,
                primary_key: i == 0,
                ordinal: i as u32,
            })
            .collect())
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let prefix = Self::prefix_from(&query.filters);
        let wanted = query
            .offset
            .saturating_add(u64::from(query.limit))
            .min(u64::from(MAX_PAGE_ROWS))
            .try_into()
            .unwrap_or(MAX_PAGE_ROWS);
        let page = self.list(&table.name, prefix.as_deref(), None, wanted).await?;
        let names = column_names();
        // The prefix already ran server-side; the rest of the filters, the sort
        // and the offset window are applied here.
        let rows = local::page(&names, page.rows, query);
        Ok(ResultSet { columns: columns(), rows, truncated: page.truncated })
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        Ok(Some(self.count(table, &[]).await?))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let prefix = Self::prefix_from(filters);
        let page = self.list(&table.name, prefix.as_deref(), None, MAX_PAGE_ROWS).await?;
        let names = column_names();
        Ok(local::apply_filters(&names, page.rows, filters).len() as i64)
    }

    // WHAT:  S3 has no query language, so `execute` takes a handful of commands.
    async fn execute(&self, command: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let trimmed = command.trim();
        let mut parts = trimmed.split_whitespace();
        let verb = parts.next().unwrap_or_default().to_ascii_uppercase();
        let bucket = parts.next().unwrap_or_default().to_string();
        let rest: Vec<&str> = parts.collect();
        let argument = rest.join(" ");

        if self.read_only && matches!(verb.as_str(), "DELETE" | "PUT") {
            return Err(AppError::read_only("This connection is read-only: S3 writes are blocked."));
        }

        let set = match verb.as_str() {
            "BUCKETS" => {
                let buckets = self.buckets().await?;
                ResultSet {
                    columns: vec![
                        ColumnMeta { name: "bucket".into(), type_name: "text".into() },
                        ColumnMeta { name: "created".into(), type_name: "timestamp".into() },
                    ],
                    rows: buckets.into_iter().take(max_rows).map(|(n, c)| vec![Value::Text(n), Value::DateTime(c)]).collect(),
                    truncated: false,
                }
            }
            "LIST" if !bucket.is_empty() => {
                let prefix = (!argument.is_empty()).then_some(argument.as_str());
                let page = self.list(&bucket, prefix, None, max_rows.min(MAX_PAGE_ROWS as usize) as u32).await?;
                ResultSet { columns: columns(), rows: page.rows, truncated: page.truncated }
            }
            "HEAD" | "GET" if !bucket.is_empty() && !argument.is_empty() => {
                let method = if verb == "HEAD" { Method::HEAD } else { Method::GET };
                let body = self.call(method, Some(&bucket), Some(&argument), &[]).await?;
                ResultSet {
                    columns: vec![ColumnMeta { name: "body".into(), type_name: "text".into() }],
                    rows: vec![vec![Value::Text(body)]],
                    truncated: false,
                }
            }
            "DELETE" if !bucket.is_empty() && !argument.is_empty() => {
                self.call(Method::DELETE, Some(&bucket), Some(&argument), &[]).await?;
                return Ok(vec![StatementResult::Affected { rows_affected: 1 }]);
            }
            _ => {
                return Err(AppError::invalid_input(
                    "Enter `BUCKETS`, `LIST <bucket> [prefix]`, `GET <bucket> <key>`, `HEAD <bucket> <key>` or `DELETE <bucket> <key>`.",
                ))
            }
        };
        Ok(vec![StatementResult::Rows { result: set }])
    }

    async fn close(&self) {}

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        match kind {
            ObjectKind::Bucket => Ok(self
                .buckets()
                .await?
                .into_iter()
                .map(|(name, created)| ObjectSummary::new(ObjectKind::Bucket, &name, None).with_detail(created).with_badge("bucket"))
                .collect()),
            // A "folder" in S3 is a common prefix under a delimiter.
            ObjectKind::Prefix => {
                let Some(bucket) = parent else { return Ok(Vec::new()) };
                let page = self.list(bucket, None, Some("/"), MAX_KEYS).await?;
                Ok(page
                    .prefixes
                    .into_iter()
                    .map(|p| ObjectSummary::new(ObjectKind::Prefix, p.trim_end_matches('/'), Some(bucket.to_string())).with_badge("prefix"))
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let mut detail = ObjectDetail::empty(reference);
        match reference.kind {
            ObjectKind::Bucket => {
                let page = self.list(&reference.name, None, Some("/"), MAX_KEYS).await?;
                let bytes: f64 = page
                    .rows
                    .iter()
                    .filter_map(|r| match r.get(1) {
                        Some(Value::Int(n)) => Some(*n as f64),
                        _ => None,
                    })
                    .sum();
                detail = detail
                    .property("Objects", format!("{}{}", page.rows.len(), if page.truncated { "+" } else { "" }))
                    .property("Size", human_bytes(bytes))
                    .property("Prefixes", page.prefixes.len().to_string());
                detail.rows = Some(ResultSet { columns: columns(), rows: page.rows, truncated: page.truncated });
                detail = detail.definition(format!("LIST {}", reference.name), CodeLanguage::Text);
            }
            ObjectKind::Prefix => {
                let Some(bucket) = reference.parent.as_deref() else { return Ok(detail) };
                let prefix = format!("{}/", reference.name);
                let page = self.list(bucket, Some(&prefix), Some("/"), MAX_KEYS).await?;
                detail = detail.property("Objects", page.rows.len().to_string());
                detail.rows = Some(ResultSet { columns: columns(), rows: page.rows, truncated: page.truncated });
                detail = detail.definition(format!("LIST {bucket} {prefix}"), CodeLanguage::Text);
            }
            _ => {}
        }
        Ok(detail)
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let buckets = self.buckets().await?;
        Ok(ServerStats::now(vec![StatGroup {
            title: "Storage".into(),
            stats: vec![
                Stat::text("Region", self.creds.region.clone()),
                Stat::text("Endpoint", self.endpoint.clone().unwrap_or_else(|| "AWS".into())),
                Stat::number("Buckets", buckets.len() as f64, None),
            ],
        }]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode};

    const LIST_BUCKETS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult><Owner><ID>x</ID></Owner><Buckets>
<Bucket><Name>logs</Name><CreationDate>2026-01-02T03:04:05.000Z</CreationDate></Bucket>
<Bucket><Name>backups</Name><CreationDate>2026-02-03T04:05:06.000Z</CreationDate></Bucket>
</Buckets></ListAllMyBucketsResult>"#;

    const LIST_OBJECTS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult><Name>logs</Name><KeyCount>2</KeyCount><IsTruncated>true</IsTruncated>
<NextContinuationToken>tok123</NextContinuationToken>
<Contents><Key>a/app.log</Key><LastModified>2026-03-04T05:06:07.000Z</LastModified><ETag>&quot;abc&quot;</ETag><Size>2048</Size><StorageClass>STANDARD</StorageClass></Contents>
<Contents><Key>b/app.log</Key><LastModified>2026-03-05T05:06:07.000Z</LastModified><ETag>&quot;def&quot;</ETag><Size>10</Size><StorageClass>GLACIER</StorageClass></Contents>
<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>b/</Prefix></CommonPrefixes>
</ListBucketResult>"#;

    fn integration(endpoint: Option<&str>) -> S3Integration {
        S3Integration {
            engine: Engine::S3,
            http: HttpClient::new("http://127.0.0.1:1", Auth::None, false).unwrap_or_else(|_| panic!("client")),
            creds: AwsCredentials {
                region: "us-east-1".into(),
                access_key: "AK".into(),
                secret_key: "SK".into(),
                session_token: None,
            },
            endpoint: endpoint.map(str::to_string),
            read_only: false,
        }
    }

    #[test]
    fn buckets_parse_from_xml() {
        let recs = records(LIST_BUCKETS, "Bucket");
        assert_eq!(recs.len(), 2);
        assert_eq!(field(&recs[0], "Name"), Some("logs"));
        assert_eq!(field(&recs[1], "Name"), Some("backups"));
    }

    #[test]
    fn objects_become_rows_with_typed_cells() {
        let recs = records(LIST_OBJECTS, "Contents");
        assert_eq!(recs.len(), 2);
        let row = object_row(&recs[0]);
        assert_eq!(row[0], Value::Text("a/app.log".into()));
        assert_eq!(row[1], Value::Int(2048));
        assert_eq!(row[2], Value::DateTime("2026-03-04T05:06:07.000Z".into()));
        assert_eq!(row[3], Value::Text("STANDARD".into()));
        // The quotes S3 wraps every ETag in are noise in a grid cell.
        assert_eq!(row[4], Value::Text("abc".into()));
    }

    #[test]
    fn common_prefixes_and_continuation_are_read() {
        let prefixes: Vec<String> = records(LIST_OBJECTS, "CommonPrefixes").iter().filter_map(|r| field(r, "Prefix").map(str::to_string)).collect();
        assert_eq!(prefixes, vec!["a/", "b/"]);
        assert_eq!(first_value(LIST_OBJECTS, "NextContinuationToken").as_deref(), Some("tok123"));
        assert_eq!(first_value(LIST_OBJECTS, "NoSuchTag"), None);
    }

    #[test]
    fn path_style_is_used_for_a_custom_endpoint() {
        let s3 = integration(Some("http://127.0.0.1:59000"));
        let (url, host, path) = s3.address(Some("logs"), Some("a/app.log"));
        assert_eq!(url, "http://127.0.0.1:59000/logs/a/app.log");
        assert_eq!(host, "127.0.0.1:59000");
        assert_eq!(path, "/logs/a/app.log");
    }

    #[test]
    fn virtual_host_style_is_used_for_aws() {
        let s3 = integration(None);
        let (url, host, path) = s3.address(Some("logs"), Some("a/app.log"));
        assert_eq!(url, "https://logs.s3.us-east-1.amazonaws.com/a/app.log");
        assert_eq!(host, "logs.s3.us-east-1.amazonaws.com");
        assert_eq!(path, "/a/app.log");
        // Listing buckets has no bucket in the host.
        let (_, root_host, root_path) = s3.address(None, None);
        assert_eq!(root_host, "s3.us-east-1.amazonaws.com");
        assert_eq!(root_path, "/");
    }

    #[test]
    fn keys_are_encoded_but_keep_their_separators() {
        let s3 = integration(Some("http://minio:9000"));
        let (url, _, path) = s3.address(Some("b"), Some("my folder/a+b.txt"));
        assert!(url.ends_with("/b/my%20folder/a%2Bb.txt"), "{url}");
        assert_eq!(path, "/b/my%20folder/a%2Bb.txt");
    }

    #[test]
    fn startswith_on_key_becomes_the_server_side_prefix() {
        let rule = |op: FilterOp, col: &str, val: &str| FilterRule { column: col.into(), op, value: val.into() };
        assert_eq!(S3Integration::prefix_from(&[rule(FilterOp::StartsWith, "key", "logs/")]).as_deref(), Some("logs/"));
        // Only a StartsWith on `key` can be pushed down; the rest stay local.
        assert_eq!(S3Integration::prefix_from(&[rule(FilterOp::Contains, "key", "logs/")]), None);
        assert_eq!(S3Integration::prefix_from(&[rule(FilterOp::StartsWith, "etag", "a")]), None);
    }

    #[test]
    fn sizes_read_as_human_units() {
        assert_eq!(human_bytes(512.0), "512 B");
        assert_eq!(human_bytes(2048.0), "2.0 KB");
    }

    // WHAT:  Live round trip against MinIO (or any S3-compatible server).
    // HOW:   DBFREE_TEST_S3_ENDPOINT / _KEY / _SECRET (+ optional _REGION, _BUCKET).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(endpoint) = std::env::var("DBFREE_TEST_S3_ENDPOINT") else {
            return;
        };
        let input = ConnectionInput {
            name: "live-s3".into(),
            engine: Engine::Minio,
            environment: Environment::Local,
            read_only: false,
            host: Some(std::env::var("DBFREE_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into())),
            port: None,
            database: Some(endpoint),
            username: Some(std::env::var("DBFREE_TEST_S3_KEY").unwrap_or_else(|_| "minioadmin".into())),
            password: None,
            file_path: None,
            ssl_mode: SslMode::Disable,
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary::draft(&input, true),
            secret: Some(std::env::var("DBFREE_TEST_S3_SECRET").unwrap_or_else(|_| "minioadmin".into())),
        };
        let s3 = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        s3.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert!(s3.server_version().await.unwrap_or_default().is_some());

        let catalog = s3.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let buckets = catalog.schemas.first().map(|s| s.tables.clone()).unwrap_or_default();
        assert!(!buckets.is_empty(), "the test server should have at least one bucket");

        let wanted = std::env::var("DBFREE_TEST_S3_BUCKET").ok();
        let bucket = wanted
            .and_then(|w| buckets.iter().find(|b| b.name == w).cloned())
            .or_else(|| buckets.first().cloned())
            .unwrap_or_else(|| panic!("no bucket"));
        let table = TableRef { schema: None, name: bucket.name.clone() };

        let cols = s3.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(cols.first().map(|c| c.name.as_str()), Some("key"));
        assert!(cols.first().is_some_and(|c| c.primary_key));

        let query = PageQuery { sort: Vec::new(), filters: Vec::new(), offset: 0, limit: 5 };
        let page = s3.fetch_page(&table, &query).await.unwrap_or_else(|e| panic!("page: {e}"));
        assert!(page.rows.len() <= 5);
        assert_eq!(page.columns.len(), COLUMNS.len());
        let total = s3.count(&table, &[]).await.unwrap_or_else(|e| panic!("count: {e}"));
        assert!(total >= page.rows.len() as i64);

        let listed = s3.objects(ObjectKind::Bucket, None).await.unwrap_or_else(|e| panic!("objects: {e}"));
        assert!(listed.iter().any(|b| b.reference.name == bucket.name));
        let detail = s3
            .object_detail(&ObjectRef { kind: ObjectKind::Bucket, name: bucket.name.clone(), parent: None })
            .await
            .unwrap_or_else(|e| panic!("detail: {e}"));
        assert!(detail.properties.iter().any(|p| p.name == "Objects"));

        match s3.execute("BUCKETS", 50).await.unwrap_or_else(|e| panic!("execute: {e}")).first() {
            Some(StatementResult::Rows { result }) => assert!(!result.rows.is_empty()),
            other => panic!("expected rows, got {other:?}"),
        }
        let stats = s3.server_stats().await.unwrap_or_else(|e| panic!("stats: {e}"));
        assert!(!stats.groups.is_empty());
        s3.close().await;
    }
}
