// SOT: orientdb-integration, orientdb-rest-api, orient-sql, orient-gremlin

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, json_to_value, json_type_name, objects_to_result_set, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, SortRule, SslMode, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::{json, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  OrientDB adapter over the REST API (port 2480).
// WHY:   OrientDB classes carry declared properties but records may hold extra
//        fields, so columns = `@rid` (pk) + `@class` + declared properties ∪
//        keys sampled from the first 50 records.
// HOW:   Basic auth (root/secret); `database` defaults to `demodb`. Catalog:
//        GET /database/{db} → classes (system classes and `_`-prefixed ones
//        skipped) with `records` as the row estimate. Paging / counting use
//        OrientDB SQL through POST /command/{db}/sql with backtick-quoted
//        identifiers and escaped string literals (`SKIP n LIMIT m`). `execute`
//        runs SQL (or Gremlin when the text starts with `g.`) via the command
//        endpoint; result arrays become grids, `{count: n}` becomes Affected.
//        INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/TRUNCATE are refused read-only.
// WHERE: src-tauri/src/integrations/http.rs (client), integrations/mod.rs (trait)
// ============================================================================

const DEFAULT_PORT: u16 = 2480;
const DEFAULT_DATABASE: &str = "demodb";
const SAMPLE_SIZE: u32 = 50;
const MAX_PAGE_ROWS: u32 = 5_000;
const SYSTEM_CLASSES: [&str; 9] = ["OIdentity", "ORole", "OUser", "OFunction", "OSchedule", "OSequence", "OTriggered", "ORestricted", "OSecurityPolicy"];
const WRITE_KEYWORDS: [&str; 9] = ["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE", "MOVE", "GRANT"];

pub struct OrientIntegration {
    engine: Engine,
    http: HttpClient,
    database: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let base = base_url(conn, Some(DEFAULT_PORT), false);
    let insecure = s.ssl_mode == SslMode::Require;
    let http = HttpClient::new(base, HttpClient::auth_from_connection(conn), insecure)?;
    let integration = OrientIntegration { engine: s.engine, http, database, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn pct(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

// WHAT:  Backtick-quoted identifier. `@rid` / `@class` are metadata fields and stay bare.
fn ident(raw: &str) -> String {
    if raw.starts_with('@') && raw[1..].chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return raw.to_string();
    }
    format!("`{}`", raw.replace('`', ""))
}

fn quote_str(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('\'');
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn is_rid(raw: &str) -> bool {
    let Some(rest) = raw.strip_prefix('#') else { return false };
    match rest.split_once(':') {
        Some((a, b)) => !a.is_empty() && !b.is_empty() && a.chars().all(|c| c.is_ascii_digit() || c == '-') && b.chars().all(|c| c.is_ascii_digit()),
        None => false,
    }
}

fn literal(column: &str, raw: &str) -> String {
    let t = raw.trim();
    if column == "@rid" && is_rid(t) {
        return t.to_string();
    }
    if t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        return t.to_ascii_lowercase();
    }
    if t.eq_ignore_ascii_case("null") {
        return "null".into();
    }
    if t.parse::<i64>().is_ok() || t.parse::<f64>().is_ok() {
        return t.to_string();
    }
    quote_str(t)
}

fn where_clause(filters: &[FilterRule]) -> String {
    if filters.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| {
            let col = ident(&f.column);
            let v = f.value.trim();
            match f.op {
                FilterOp::Eq => format!("{col} = {}", literal(&f.column, v)),
                FilterOp::Ne => format!("{col} <> {}", literal(&f.column, v)),
                FilterOp::Gt => format!("{col} > {}", literal(&f.column, v)),
                FilterOp::Gte => format!("{col} >= {}", literal(&f.column, v)),
                FilterOp::Lt => format!("{col} < {}", literal(&f.column, v)),
                FilterOp::Lte => format!("{col} <= {}", literal(&f.column, v)),
                FilterOp::Contains => format!("{col}.toString().toLowerCase() LIKE {}", quote_str(&format!("%{}%", v.to_lowercase()))),
                FilterOp::StartsWith => format!("{col}.toString().toLowerCase() LIKE {}", quote_str(&format!("{}%", v.to_lowercase()))),
                FilterOp::EndsWith => format!("{col}.toString().toLowerCase() LIKE {}", quote_str(&format!("%{}", v.to_lowercase()))),
                FilterOp::In => {
                    let items: Vec<String> = v.split(',').map(str::trim).filter(|x| !x.is_empty()).map(|x| literal(&f.column, x)).collect();
                    if items.is_empty() {
                        "false".into()
                    } else {
                        format!("{col} IN [{}]", items.join(", "))
                    }
                }
                FilterOp::IsNull => format!("{col} IS NULL"),
                FilterOp::IsNotNull => format!("{col} IS NOT NULL"),
            }
        })
        .collect();
    format!(" WHERE {}", parts.join(" AND "))
}

fn order_clause(sort: &[SortRule]) -> String {
    if sort.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = sort.iter().map(|s| format!("{} {}", ident(&s.column), if s.desc { "DESC" } else { "ASC" })).collect();
    format!(" ORDER BY {}", parts.join(", "))
}

fn first_word(text: &str) -> String {
    text.trim_start().split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("").to_ascii_uppercase()
}

fn is_write_sql(text: &str) -> bool {
    WRITE_KEYWORDS.contains(&first_word(text).as_str())
}

fn is_gremlin(text: &str) -> bool {
    text.trim_start().starts_with("g.")
}

fn is_system_class(name: &str) -> bool {
    name.starts_with('_') || SYSTEM_CLASSES.contains(&name)
}

// WHAT:  Strips OrientDB's per-record metadata (`@type`, `@version`, `@fieldTypes`)
//        so the grid shows only `@rid`, `@class` and the user fields.
fn clean_record(rec: &Json) -> Json {
    match rec.as_object() {
        Some(o) => Json::Object(o.iter().filter(|(k, _)| !matches!(k.as_str(), "@type" | "@version" | "@fieldTypes")).map(|(k, v)| (k.clone(), v.clone())).collect()),
        None => rec.clone(),
    }
}

// WHAT:  Declared properties ∪ sampled keys; `@rid` first (pk), `@class` second.
fn union_columns(declared: &[(String, String)], docs: &[Json]) -> Vec<ColumnInfo> {
    let mut names: Vec<String> = vec!["@rid".into(), "@class".into()];
    let mut types: Vec<Option<String>> = vec![Some("rid".into()), Some("string".into())];
    let mut push = |name: &str, ty: Option<String>| {
        let idx = match names.iter().position(|n| n == name) {
            Some(i) => i,
            None => {
                names.push(name.to_string());
                types.push(None);
                names.len() - 1
            }
        };
        if types[idx].is_none() && ty.is_some() {
            types[idx] = ty;
        }
    };
    for (name, ty) in declared {
        push(name, Some(ty.clone()));
    }
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for (k, v) in obj {
                if matches!(k.as_str(), "@type" | "@version" | "@fieldTypes") {
                    continue;
                }
                push(k, (!v.is_null()).then(|| json_type_name(v).to_string()));
            }
        }
    }
    names
        .into_iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, ty))| ColumnInfo {
            primary_key: name == "@rid",
            nullable: name != "@rid",
            data_type: ty.unwrap_or_else(|| "null".into()),
            name,
            ordinal: u32::try_from(i + 1).unwrap_or(u32::MAX),
        })
        .collect()
}

fn docs_to_result_set(columns: &[ColumnInfo], docs: &[Json], truncated: bool) -> ResultSet {
    let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut types: Vec<String> = columns.iter().map(|c| c.data_type.clone()).collect();
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for (k, v) in obj {
                if matches!(k.as_str(), "@type" | "@version" | "@fieldTypes") {
                    continue;
                }
                if !names.iter().any(|n| n == k) {
                    names.push(k.clone());
                    types.push(json_type_name(v).to_string());
                }
            }
        }
    }
    let rows = docs
        .iter()
        .map(|doc| {
            let obj = doc.as_object();
            names.iter().map(|n| obj.and_then(|o| o.get(n)).map(json_to_value).unwrap_or(Value::Null)).collect()
        })
        .collect();
    ResultSet { columns: names.into_iter().zip(types).map(|(name, type_name)| ColumnMeta { name, type_name }).collect(), rows, truncated }
}

// WHAT:  A command response → StatementResult. `{result:[{count:n}]}` = Affected,
//        object arrays = grid (@rid pinned), scalars = one `value` column.
fn command_to_result(body: &Json, max_rows: usize) -> StatementResult {
    let items: Vec<Json> = body.get("result").and_then(Json::as_array).cloned().unwrap_or_else(|| match body {
        Json::Array(a) => a.clone(),
        other => vec![other.clone()],
    });
    if items.len() == 1 {
        if let Some(obj) = items[0].as_object() {
            if obj.len() <= 3 && obj.get("count").is_some() && obj.keys().all(|k| k == "count" || k.starts_with('@')) {
                let n = obj.get("count").and_then(Json::as_u64).unwrap_or(0);
                return StatementResult::Affected { rows_affected: n };
            }
        }
    }
    if items.is_empty() {
        return StatementResult::Rows { result: ResultSet { columns: vec![], rows: vec![], truncated: false } };
    }
    if items.iter().all(Json::is_object) {
        let cleaned: Vec<Json> = items.iter().map(clean_record).collect();
        let id = cleaned.iter().any(|d| d.get("@rid").is_some()).then_some("@rid");
        return StatementResult::Rows { result: objects_to_result_set(&cleaned, id, max_rows) };
    }
    let truncated = items.len() > max_rows;
    let type_name = items.iter().find(|v| !v.is_null()).map(json_type_name).unwrap_or("json").to_string();
    StatementResult::Rows {
        result: ResultSet {
            columns: vec![ColumnMeta { name: "value".into(), type_name }],
            rows: items.iter().take(max_rows).map(|v| vec![json_to_value(v)]).collect(),
            truncated,
        },
    }
}

impl OrientIntegration {
    async fn command(&self, language: &str, text: &str) -> AppResult<Json> {
        let path = format!("/command/{}/{language}", pct(&self.database));
        self.http.post_json(&path, &json!({ "command": text })).await
    }

    async fn sql_rows(&self, sql: &str) -> AppResult<Vec<Json>> {
        let body = self.command("sql", sql).await?;
        Ok(body.get("result").and_then(Json::as_array).cloned().unwrap_or_default())
    }

    async fn database_info(&self) -> AppResult<Json> {
        self.http.get_json(&format!("/database/{}", pct(&self.database))).await
    }

    fn class_entries(info: &Json) -> Vec<Json> {
        info.get("classes").and_then(Json::as_array).cloned().unwrap_or_default()
    }
}

#[async_trait]
impl Integration for OrientIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { sql: true, namespaces: false, exact_estimate: true, ..Capabilities::DOCUMENT }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.database_info().await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v: Json = match self.http.get_json("/server").await {
            Ok(v) => v,
            Err(_) => return Ok(Some("OrientDB".into())),
        };
        let version = v
            .get("version")
            .and_then(Json::as_str)
            .map(str::to_string)
            .or_else(|| {
                v.get("properties")
                    .and_then(Json::as_array)
                    .and_then(|props| props.iter().find(|p| p.get("name").and_then(Json::as_str) == Some("server.version")))
                    .and_then(|p| p.get("value").and_then(Json::as_str))
                    .map(str::to_string)
            });
        Ok(Some(version.map(|s| format!("OrientDB {s}")).unwrap_or_else(|| "OrientDB".into())))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let v: Json = match self.http.get_json("/listDatabases").await {
            Ok(v) => v,
            Err(_) => return Ok(vec![self.database.clone()]),
        };
        let mut names: Vec<String> = v
            .get("databases")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        if names.is_empty() {
            names.push(self.database.clone());
        }
        names.sort();
        Ok(names)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let info = self.database_info().await?;
        let mut tables: Vec<TableInfo> = Self::class_entries(&info)
            .iter()
            .filter_map(|c| {
                let name = c.get("name").and_then(Json::as_str)?;
                if is_system_class(name) {
                    return None;
                }
                let abstract_class = c.get("abstract").and_then(Json::as_bool).unwrap_or(false);
                Some(TableInfo {
                    schema: None,
                    name: name.to_string(),
                    kind: if abstract_class { TableKind::View } else { TableKind::Table },
                    row_estimate: c.get("records").and_then(Json::as_i64),
                })
            })
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let info = self.database_info().await?;
        let declared: Vec<(String, String)> = Self::class_entries(&info)
            .iter()
            .find(|c| c.get("name").and_then(Json::as_str) == Some(table.name.as_str()))
            .and_then(|c| c.get("properties").and_then(Json::as_array))
            .map(|props| {
                props
                    .iter()
                    .filter_map(|p| {
                        let name = p.get("name").and_then(Json::as_str)?;
                        let ty = p.get("type").and_then(Json::as_str).unwrap_or("ANY");
                        Some((name.to_string(), ty.to_ascii_lowercase()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let sample = self.sql_rows(&format!("SELECT FROM {} LIMIT {SAMPLE_SIZE}", ident(&table.name))).await?;
        Ok(union_columns(&declared, &sample))
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let info = self.database_info().await?;
        Ok(Self::class_entries(&info)
            .iter()
            .find(|c| c.get("name").and_then(Json::as_str) == Some(table.name.as_str()))
            .and_then(|c| c.get("records").and_then(Json::as_i64)))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let sql = format!("SELECT count(*) AS n FROM {}{}", ident(&table.name), where_clause(filters));
        let rows = self.sql_rows(&sql).await?;
        Ok(rows.first().and_then(|r| r.get("n")).and_then(Json::as_i64).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let limit = query.limit.min(MAX_PAGE_ROWS);
        let sql = format!(
            "SELECT FROM {}{}{} SKIP {} LIMIT {limit}",
            ident(&table.name),
            where_clause(&query.filters),
            order_clause(&query.sort),
            query.offset
        );
        let rows = self.sql_rows(&sql).await?;
        let cleaned: Vec<Json> = rows.iter().map(clean_record).collect();
        Ok(docs_to_result_set(&cols, &cleaned, false))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let text = sql.trim();
        if text.is_empty() {
            return Err(AppError::invalid_input("Empty command."));
        }
        let language = if is_gremlin(text) { "gremlin" } else { "sql" };
        if self.read_only && language == "sql" && is_write_sql(text) {
            return Err(AppError::read_only(format!("This connection is read-only; `{}` is blocked.", first_word(text))));
        }
        if self.read_only && language == "gremlin" && (text.contains(".addV(") || text.contains(".addE(") || text.contains(".drop(") || text.contains(".property(")) {
            return Err(AppError::read_only("This connection is read-only; mutating Gremlin steps are blocked."));
        }
        let body = self.command(language, text).await?;
        if body.get("result").is_none() && !body.is_array() {
            return Ok(vec![StatementResult::Rows { result: json_result(body) }]);
        }
        Ok(vec![command_to_result(&body, max_rows.max(1))])
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    #[test]
    fn where_and_order_render() {
        let w = where_clause(&[
            rule("age", FilterOp::Gte, "5"),
            rule("name", FilterOp::Contains, "O'B"),
            rule("tier", FilterOp::In, "gold, 2"),
            rule("@rid", FilterOp::Eq, "#12:0"),
            rule("note", FilterOp::IsNull, ""),
        ]);
        assert_eq!(
            w,
            " WHERE `age` >= 5 AND `name`.toString().toLowerCase() LIKE '%o\\'b%' AND `tier` IN ['gold', 2] AND @rid = #12:0 AND `note` IS NULL"
        );
        assert_eq!(order_clause(&[SortRule { column: "a".into(), desc: true }]), " ORDER BY `a` DESC");
        assert_eq!(literal("@rid", "not a rid"), "'not a rid'");
        assert_eq!(ident("we`ird"), "`weird`");
    }

    #[test]
    fn write_and_gremlin_detection() {
        assert!(is_write_sql("insert into V set a = 1"));
        assert!(is_write_sql(" DROP CLASS X"));
        assert!(!is_write_sql("SELECT FROM V"));
        assert!(is_gremlin("g.V().limit(5)"));
        assert!(!is_gremlin("SELECT g.V FROM x"));
        assert!(is_system_class("OUser"));
        assert!(is_system_class("_studio"));
        assert!(!is_system_class("Person"));
    }

    #[test]
    fn command_responses_map() {
        let rows = command_to_result(&json!({"result": [{"@rid": "#1:0", "@type": "d", "@version": 1, "name": "a"}]}), 10);
        match rows {
            StatementResult::Rows { result } => {
                let names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
                assert_eq!(names, vec!["@rid", "name"]);
            }
            _ => panic!("rows"),
        }
        let aff = command_to_result(&json!({"result": [{"@type": "d", "count": 3, "@version": 0}]}), 10);
        assert!(matches!(aff, StatementResult::Affected { rows_affected: 3 }));
        let scalar = command_to_result(&json!({"result": [1, 2]}), 10);
        assert!(matches!(scalar, StatementResult::Rows { .. }));
    }

    #[test]
    fn columns_union_declared_and_sampled() {
        let cols = union_columns(&[("name".into(), "string".into())], &[json!({"@rid": "#1:0", "@class": "P", "@version": 2, "age": 3})]);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["@rid", "@class", "name", "age"]);
        assert!(cols[0].primary_key);
        assert_eq!(cols[3].data_type, "integer");
    }

    #[tokio::test]
    async fn live_round_trip_when_configured() {
        use crate::model::{ConnectionSummary, Environment};
        let Ok(url) = std::env::var("DBFREE_TEST_ORIENTDB_URL") else { return };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Orientdb,
                environment: Environment::Local,
                read_only: false,
                host: Some(url),
                port: None,
                database: std::env::var("DBFREE_TEST_ORIENTDB_DB").ok(),
                username: std::env::var("DBFREE_TEST_ORIENTDB_USER").ok(),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: std::env::var("DBFREE_TEST_ORIENTDB_PASSWORD").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let version = db.server_version().await.unwrap_or_default().unwrap_or_default();
        assert!(version.starts_with("OrientDB"), "{version}");
        db.execute("CREATE CLASS DbfreeSmoke", 10).await.unwrap_or_else(|e| panic!("create class: {e}"));
        db.execute("INSERT INTO DbfreeSmoke SET n = 1", 10).await.unwrap_or_else(|e| panic!("insert: {e}"));
        db.execute("INSERT INTO DbfreeSmoke SET n = 2", 10).await.unwrap_or_else(|e| panic!("insert: {e}"));
        let cat = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(cat.schemas[0].tables.iter().any(|t| t.name == "DbfreeSmoke"));
        let t = TableRef { schema: None, name: "DbfreeSmoke".into() };
        let cols = db.columns(&t).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.iter().any(|c| c.name == "n"));
        let page = db
            .fetch_page(&t, &PageQuery { sort: vec![SortRule { column: "n".into(), desc: true }], filters: vec![rule("n", FilterOp::Gte, "1")], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2);
        assert_eq!(db.count(&t, &[rule("n", FilterOp::Eq, "2")]).await.unwrap_or_default(), 1);
        db.execute("DROP CLASS DbfreeSmoke UNSAFE", 10).await.unwrap_or_else(|e| panic!("drop: {e}"));
    }
}
