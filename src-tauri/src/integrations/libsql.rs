// SOT: libsql-integration, turso-adapter, libsql-http-pipeline, sqlite-over-http, libsql-object-explorer, libsql-pragma-settings

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, quote_literal, validate_columns, where_clause};
use crate::integrations::{quote_ident, Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, ObjectAction, ObjectDetail, ObjectKind,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, StatementResult,
    TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Serialize)]
struct PipelineRequest {
    requests: Vec<PipelineItem>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PipelineItem {
    Execute { stmt: StatementPayload },
    Close,
}

#[derive(Debug, Serialize)]
struct StatementPayload {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct PipelineResponse {
    results: Vec<PipelineResultItem>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PipelineResultItem {
    Ok { response: PipelineExecuteResponse },
    Error { error: PipelineError },
}

#[derive(Debug, Deserialize)]
struct PipelineError {
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PipelineExecuteResponse {
    Execute { result: LibsqlQueryResult },
    Close,
}

#[derive(Debug, Deserialize)]
struct LibsqlQueryResult {
    cols: Vec<LibsqlCol>,
    rows: Vec<Vec<LibsqlValue>>,
    affected_row_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct LibsqlCol {
    name: String,
    decltype: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LibsqlValue {
    Null,
    Integer { value: serde_json::Value },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}

pub struct LibsqlIntegration {
    client: Client,
    pipeline_url: String,
    auth_token: Option<String>,
    database_name: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let mut host = s.host.as_deref().map(str::trim).unwrap_or("localhost").to_string();

    // Support libsql://, https://, or raw host
    if let Some(stripped) = host.strip_prefix("libsql://") {
        host = format!("https://{stripped}");
    } else if !host.starts_with("http://") && !host.starts_with("https://") {
        host = format!("https://{host}");
    }

    // Ensure pipeline endpoint
    let pipeline_url = if host.ends_with("/v2/pipeline") {
        host.clone()
    } else {
        format!("{}/v2/pipeline", host.trim_end_matches('/'))
    };

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| AppError::driver(e.to_string()))?;

    let db_name = s.database.clone().unwrap_or_else(|| "default".into());

    let integration = LibsqlIntegration {
        client,
        pipeline_url,
        auth_token: conn.secret.clone(),
        database_name: db_name,
    };

    integration.ping().await?;
    Ok(Arc::new(integration))
}

impl LibsqlIntegration {
    async fn run_sql(&self, sql: &str) -> AppResult<LibsqlQueryResult> {
        let payload = PipelineRequest {
            requests: vec![
                PipelineItem::Execute {
                    stmt: StatementPayload { sql: sql.to_string() },
                },
                PipelineItem::Close,
            ],
        };

        let mut req = self.client.post(&self.pipeline_url).json(&payload);
        if let Some(token) = &self.auth_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        let resp = req.send().await.map_err(|e| AppError::driver(format!("Turso/LibSQL request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::driver(format!("Turso/LibSQL error ({status}): {body}")));
        }

        let pipeline_resp: PipelineResponse = resp
            .json()
            .await
            .map_err(|e| AppError::driver(format!("Failed to parse Turso/LibSQL response: {e}")))?;

        for item in pipeline_resp.results {
            match item {
                PipelineResultItem::Ok {
                    response: PipelineExecuteResponse::Execute { result },
                } => return Ok(result),
                PipelineResultItem::Error { error } => {
                    return Err(AppError::driver(format!("Turso/LibSQL query error: {}", error.message)));
                }
                _ => {}
            }
        }

        Ok(LibsqlQueryResult {
            cols: Vec::new(),
            rows: Vec::new(),
            affected_row_count: Some(0),
        })
    }

    // WHAT:  One catalog query as name → value maps.
    async fn catalog_rows(&self, sql: &str) -> AppResult<Vec<CatalogRow>> {
        let res = self.run_sql(sql).await?;
        let names: Vec<String> = res.cols.iter().map(|c| c.name.clone()).collect();
        Ok(res
            .rows
            .into_iter()
            .map(|row| names.iter().cloned().zip(row.into_iter().map(convert_libsql_value)).collect())
            .collect())
    }

    // WHAT:  Catalog rows, or none when the server rejects the query (PRAGMA support varies).
    async fn optional_rows(&self, sql: &str) -> Vec<CatalogRow> {
        self.catalog_rows(sql).await.unwrap_or_default()
    }

    async fn master_rows(&self, kind: &str, table: Option<&str>) -> AppResult<Vec<CatalogRow>> {
        self.catalog_rows(&master_query(kind, table)).await
    }

    // WHAT:  One named object from sqlite_master, or a not-found error.
    async fn find_master(&self, kind: &str, reference: &ObjectRef) -> AppResult<CatalogRow> {
        let sql = format!(
            "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type = '{kind}' AND name = {} LIMIT 1",
            quote_literal(&reference.name)
        );
        self.catalog_rows(&sql).await?.into_iter().next().ok_or_else(|| not_found(reference))
    }

    async fn settings(&self) -> Vec<CatalogRow> {
        let rows = self.optional_rows(&settings_query()).await;
        if !rows.is_empty() {
            return rows;
        }
        // Older libSQL builds have no pragma_*() functions: fall back to one call each.
        let mut out = Vec::new();
        for (name, _) in SETTINGS {
            if let Some(row) = self.optional_rows(&format!("PRAGMA {name}")).await.into_iter().next() {
                if let Some(value) = row.values().next().cloned() {
                    out.push(CatalogRow::from([("name".to_string(), Value::Text((*name).to_string())), ("value".to_string(), value)]));
                }
            }
        }
        out
    }

    async fn table_detail(&self, reference: &ObjectRef, row: &CatalogRow) -> AppResult<ObjectDetail> {
        let name = cell_text(row, "name").unwrap_or_else(|| reference.name.clone());
        let target = quote_ident(&name);
        let sql = cell_text(row, "sql");
        let mut detail = ObjectDetail::empty(reference);
        if let Some(text) = sql.clone() {
            detail = detail.definition(text, CodeLanguage::Sql);
        }
        detail.columns = columns_from_table_info(&self.optional_rows(&format!("PRAGMA table_info({target})")).await);
        if let Some(module) = sql.as_deref().filter(|s| is_virtual(Some(s))).and_then(virtual_module) {
            detail = detail.property("Module", module);
        }
        if let Ok(rows) = self.catalog_rows(&format!("SELECT count(*) AS \"count\" FROM {target}")).await {
            if let Some(count) = rows.first().and_then(|r| cell_i64(r, "count")) {
                detail = detail.property("Rows", crate::model::objects::format_number(count as f64));
            }
        }
        let column_count = detail.columns.len();
        detail = detail.property("Columns", column_count.to_string());
        let fks = self.optional_rows(&format!("PRAGMA foreign_key_list({target})")).await;
        if !fks.is_empty() {
            detail.rows = Some(to_result_set(&fks, &["id", "seq", "table", "from", "to", "on_update", "on_delete"]));
        }
        let indexes = self.master_rows("index", Some(name.as_str())).await.unwrap_or_default();
        let triggers = self.master_rows("trigger", Some(name.as_str())).await.unwrap_or_default();
        detail.children = indexes
            .iter()
            .filter_map(|r| master_summary(ObjectKind::Index, r))
            .chain(triggers.iter().filter_map(|r| master_summary(ObjectKind::Trigger, r)))
            .collect();
        Ok(detail
            .action(ObjectAction::new("analyze", "Analyze", format!("ANALYZE {target};")))
            .action(ObjectAction::new("reindex", "Reindex", format!("REINDEX {target};")))
            .action(ObjectAction::destructive("truncate", "Delete all rows", format!("DELETE FROM {target};")))
            .action(ObjectAction::destructive("drop", "Drop table", format!("DROP TABLE {target};"))))
    }

    async fn view_detail(&self, reference: &ObjectRef, row: &CatalogRow) -> AppResult<ObjectDetail> {
        let name = cell_text(row, "name").unwrap_or_else(|| reference.name.clone());
        let target = quote_ident(&name);
        let mut detail = ObjectDetail::empty(reference);
        if let Some(text) = cell_text(row, "sql") {
            detail = detail.definition(text, CodeLanguage::Sql);
        }
        detail.columns = columns_from_table_info(&self.optional_rows(&format!("PRAGMA table_info({target})")).await);
        Ok(detail.action(ObjectAction::destructive("drop", "Drop view", format!("DROP VIEW {target};"))))
    }

    async fn index_detail(&self, reference: &ObjectRef, row: &CatalogRow) -> AppResult<ObjectDetail> {
        let name = cell_text(row, "name").unwrap_or_else(|| reference.name.clone());
        let table = cell_text(row, "tbl_name").unwrap_or_default();
        let sql = cell_text(row, "sql");
        let target = quote_ident(&name);
        let mut detail = ObjectDetail::empty(reference).property("Table", table.clone());
        detail = match &sql {
            Some(text) => detail.definition(text.clone(), CodeLanguage::Sql),
            None => detail.definition("-- automatic index created for a PRIMARY KEY or UNIQUE constraint", CodeLanguage::Sql),
        };
        detail = detail.property("Unique", (index_badge(sql.as_deref()) == "unique").to_string());
        let columns = self.optional_rows(&format!("PRAGMA index_info({target})")).await;
        if !columns.is_empty() {
            detail.rows = Some(to_result_set(&columns, &["seqno", "cid", "name"]));
        }
        detail = detail.action(ObjectAction::new("reindex", "Reindex", format!("REINDEX {target};")));
        if sql.is_some() {
            detail = detail.action(ObjectAction::destructive("drop", "Drop index", format!("DROP INDEX {target};")));
        }
        Ok(detail)
    }

    fn trigger_detail(&self, reference: &ObjectRef, row: &CatalogRow) -> ObjectDetail {
        let name = cell_text(row, "name").unwrap_or_else(|| reference.name.clone());
        let sql = cell_text(row, "sql");
        let (timing, event) = sql.as_deref().map(trigger_facts).unwrap_or((None, None));
        let mut detail = ObjectDetail::empty(reference).property("Table", cell_text(row, "tbl_name").unwrap_or_default());
        if let Some(text) = sql {
            detail = detail.definition(text, CodeLanguage::Sql);
        }
        if let Some(t) = timing {
            detail = detail.property("Timing", t);
        }
        if let Some(e) = event {
            detail = detail.property("Event", e);
        }
        detail.action(ObjectAction::destructive("drop", "Drop trigger", format!("DROP TRIGGER {};", quote_ident(&name))))
    }

    async fn setting_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let (name, options) = SETTINGS.iter().find(|(n, _)| *n == reference.name).ok_or_else(|| not_found(reference))?;
        let value = self
            .settings()
            .await
            .iter()
            .find(|row| cell_text(row, "name").as_deref() == Some(*name))
            .and_then(|row| cell_text(row, "value"))
            .ok_or_else(|| not_found(reference))?;
        let mut detail = ObjectDetail::empty(reference)
            .definition(pragma_statement(name, &value), CodeLanguage::Sql)
            .property("Value", value.clone());
        for option in *options {
            if !option.eq_ignore_ascii_case(&value) {
                detail = detail.action(ObjectAction::destructive(
                    &format!("set-{option}"),
                    &format!("Set {name} = {option}"),
                    pragma_statement(name, option),
                ));
            }
        }
        Ok(detail)
    }
}

fn convert_libsql_value(val: LibsqlValue) -> Value {
    match val {
        LibsqlValue::Null => Value::Null,
        LibsqlValue::Integer { value } => {
            if let Some(i) = value.as_i64() {
                Value::Int(i)
            } else if let Some(s) = value.as_str() {
                s.parse::<i64>().map(Value::Int).unwrap_or_else(|_| Value::Text(s.to_string()))
            } else {
                Value::Int(0)
            }
        }
        LibsqlValue::Float { value } => Value::Float(value),
        LibsqlValue::Text { value } => Value::Text(value),
        LibsqlValue::Blob { base64 } => Value::Bytes(base64),
    }
}

// ============================================================================
// OBJECT EXPLORER
//
// WHAT:  Tables, views, indexes, triggers and the PRAGMA settings of a Turso /
//        libSQL database, read over the same HTTP pipeline as every query.
// WHY:   libSQL is SQLite: sqlite_master plus the PRAGMA table-valued functions
//        are the whole catalog, so each kind is one round trip.
// HOW:   Every response is normalised to name → value maps (`CatalogRow`), so
//        the builders below are pure and testable without a server. PRAGMA
//        support varies between libSQL releases: each PRAGMA read is allowed to
//        fail and the explorer returns what worked.
// WHERE: src-tauri/src/model/objects.rs, src/features/objects/ObjectTab.tsx
// ============================================================================

const OBJECT_CAP: usize = 2000;

// WHAT:  One catalog row keyed by column name; PRAGMA column order varies.
type CatalogRow = std::collections::BTreeMap<String, Value>;

fn cell_text(row: &CatalogRow, name: &str) -> Option<String> {
    match row.get(name) {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(b.to_string()),
        Some(Value::Int(i)) => Some(i.to_string()),
        Some(Value::Float(f)) => Some(f.to_string()),
        Some(Value::Json(j)) => Some(j.to_string()),
        Some(Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s)) => Some(s.clone()),
    }
}

fn cell_i64(row: &CatalogRow, name: &str) -> Option<i64> {
    match row.get(name) {
        Some(Value::Int(i)) => Some(*i),
        Some(Value::Float(f)) => Some(*f as i64),
        other => other.and_then(|_| cell_text(row, name)).and_then(|t| t.parse().ok()),
    }
}

// WHAT:  sqlite_master listing for one object type, optionally one owning table.
// HOW:   `kind` is one of our own literals; the table name is single-quote escaped.
fn master_query(kind: &str, table: Option<&str>) -> String {
    let owner = table.map(|t| format!(" AND tbl_name = {}", quote_literal(t))).unwrap_or_default();
    format!(
        "SELECT type, name, tbl_name, sql FROM sqlite_master \
         WHERE type = '{kind}' AND name NOT LIKE 'sqlite_%'{owner} ORDER BY name LIMIT {OBJECT_CAP}"
    )
}

fn is_virtual(sql: Option<&str>) -> bool {
    sql.is_some_and(|s| s.trim_start().to_ascii_uppercase().starts_with("CREATE VIRTUAL TABLE"))
}

// WHAT:  `CREATE VIRTUAL TABLE t USING fts5(...)` → "fts5".
fn virtual_module(sql: &str) -> Option<String> {
    let pos = sql.to_ascii_uppercase().find(" USING ")?;
    let module: String = sql[pos + 7..].trim_start().chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
    Some(module).filter(|m| !m.is_empty())
}

fn index_badge(sql: Option<&str>) -> &'static str {
    match sql {
        None => "auto",
        Some(text) if text.to_ascii_uppercase().contains("UNIQUE") => "unique",
        Some(_) => "index",
    }
}

// WHAT:  (timing, event) of a trigger from its CREATE statement head.
fn trigger_facts(sql: &str) -> (Option<&'static str>, Option<&'static str>) {
    let upper = sql.to_ascii_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();
    let end = tokens.iter().position(|t| *t == "BEGIN").unwrap_or(tokens.len());
    let head = &tokens[..end];
    let timing = if head.contains(&"INSTEAD") {
        Some("INSTEAD OF")
    } else if head.contains(&"BEFORE") {
        Some("BEFORE")
    } else if head.contains(&"AFTER") {
        Some("AFTER")
    } else {
        None
    };
    let event = head.iter().find_map(|t| match *t {
        "INSERT" => Some("INSERT"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        _ => None,
    });
    (timing, event)
}

// WHAT:  One sqlite_master row → the summary its kind deserves.
fn master_summary(kind: ObjectKind, row: &CatalogRow) -> Option<ObjectSummary> {
    let name = cell_text(row, "name")?;
    let owner = cell_text(row, "tbl_name").unwrap_or_else(|| name.clone());
    let sql = cell_text(row, "sql");
    match kind {
        ObjectKind::Table => {
            let mut summary = ObjectSummary::new(ObjectKind::Table, name, Some("main".into()));
            if let Some(module) = sql.as_deref().filter(|s| is_virtual(Some(s))).and_then(virtual_module) {
                summary = summary.with_badge(module).with_detail("virtual table");
            }
            Some(summary)
        }
        ObjectKind::View => Some(ObjectSummary::new(ObjectKind::View, name, Some("main".into()))),
        ObjectKind::Index => Some(
            ObjectSummary::new(ObjectKind::Index, name, Some(owner.clone()))
                .with_detail(format!("on {owner}"))
                .with_badge(index_badge(sql.as_deref())),
        ),
        ObjectKind::Trigger => {
            let (timing, event) = sql.as_deref().map(trigger_facts).unwrap_or((None, None));
            let mut summary = ObjectSummary::new(ObjectKind::Trigger, name, Some(owner.clone()))
                .with_detail(format!("{} on {owner}", timing.unwrap_or("").to_ascii_lowercase()).trim().to_string());
            if let Some(e) = event {
                summary = summary.with_badge(e.to_ascii_lowercase());
            }
            Some(summary)
        }
        _ => None,
    }
}

fn columns_from_table_info(rows: &[CatalogRow]) -> Vec<ColumnInfo> {
    rows.iter()
        .enumerate()
        .filter_map(|(i, row)| {
            let name = cell_text(row, "name")?;
            Some(ColumnInfo {
                name,
                data_type: cell_text(row, "type").unwrap_or_default().to_ascii_lowercase(),
                nullable: cell_i64(row, "notnull").unwrap_or(0) == 0,
                primary_key: cell_i64(row, "pk").unwrap_or(0) > 0,
                ordinal: u32::try_from(cell_i64(row, "cid").unwrap_or(i as i64)).unwrap_or_default(),
            })
        })
        .collect()
}

// WHAT:  Catalog rows as a grid payload for the detail tab's Data view.
fn to_result_set(rows: &[CatalogRow], columns: &[&str]) -> ResultSet {
    ResultSet {
        columns: columns.iter().map(|c| ColumnMeta { name: (*c).to_string(), type_name: "text".into() }).collect(),
        rows: rows
            .iter()
            .map(|row| columns.iter().map(|c| row.get(*c).cloned().unwrap_or(Value::Null)).collect())
            .collect(),
        truncated: false,
    }
}

// WHAT:  The PRAGMAs the Setting kind exposes, with the values worth offering.
const SETTINGS: &[(&str, &[&str])] = &[
    ("journal_mode", &["delete", "truncate", "persist", "memory", "wal"]),
    ("synchronous", &["off", "normal", "full", "extra"]),
    ("foreign_keys", &["on", "off"]),
    ("page_size", &[]),
    ("cache_size", &[]),
    ("auto_vacuum", &["none", "full", "incremental"]),
    ("temp_store", &["default", "file", "memory"]),
    ("user_version", &[]),
    ("application_id", &[]),
    ("encoding", &[]),
    ("wal_autocheckpoint", &[]),
    ("busy_timeout", &[]),
    ("page_count", &[]),
    ("freelist_count", &[]),
];

// WHAT:  One query reading every PRAGMA through its table-valued function.
// WHY:   A round trip per PRAGMA would be a dozen HTTP calls for one sidebar list.
fn settings_query() -> String {
    let parts: Vec<String> = SETTINGS
        .iter()
        .map(|(name, _)| format!("SELECT '{name}' AS name, CAST((SELECT * FROM pragma_{name}()) AS TEXT) AS value"))
        .collect();
    parts.join(" UNION ALL ")
}

fn pragma_statement(name: &str, value: &str) -> String {
    let bare = !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if bare {
        format!("PRAGMA {name} = {value};")
    } else {
        format!("PRAGMA {name} = {};", quote_literal(value))
    }
}

fn not_found(reference: &ObjectRef) -> AppError {
    AppError::not_found(format!("{:?} \"{}\" was not found.", reference.kind, reference.name))
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities {
            sql: true,
            namespaces: false,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: true,
            transactions: true,
            exact_estimate: true,
        },
        object_kinds: vec![K::Table, K::View, K::Index, K::Trigger, K::Setting],
        tools: vec![T::Erd],
    }
}

#[async_trait]
impl Integration for LibsqlIntegration {
    fn engine(&self) -> Engine {
        Engine::Libsql
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.run_sql("SELECT 1").await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let res = self.run_sql("SELECT sqlite_version()").await?;
        if let Some(row) = res.rows.into_iter().next() {
            if let Some(val) = row.into_iter().next() {
                if let Value::Text(s) = convert_libsql_value(val) {
                    return Ok(Some(format!("libSQL (SQLite {s})")));
                }
            }
        }
        Ok(Some("libSQL".into()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database_name.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.database_name.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let sql = "SELECT name, type FROM sqlite_master \
                   WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' \
                   ORDER BY name";
        let res = self.run_sql(sql).await?;
        let mut tables = Vec::new();
        for row in res.rows {
            let mut iter = row.into_iter();
            let name = match iter.next() {
                Some(LibsqlValue::Text { value }) => value,
                _ => continue,
            };
            let kind = match iter.next() {
                Some(LibsqlValue::Text { value }) if value.eq_ignore_ascii_case("view") => TableKind::View,
                _ => TableKind::Table,
            };
            tables.push(TableInfo { schema: None, name, kind, row_estimate: None });
        }

        Ok(SchemaCatalog {
            schemas: vec![SchemaInfo {
                name: "main".into(),
                tables,
            }],
        })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let sql = format!("PRAGMA table_info({})", quote_ident(&table.name));
        let res = self.run_sql(&sql).await?;
        let mut cols = Vec::new();

        for row in res.rows {
            let mut iter = row.into_iter();
            let _cid = iter.next();
            let name = match iter.next() {
                Some(LibsqlValue::Text { value }) => value,
                _ => continue,
            };
            let type_name = match iter.next() {
                Some(LibsqlValue::Text { value }) => value,
                _ => String::from("TEXT"),
            };
            let notnull = match iter.next() {
                Some(LibsqlValue::Integer { value }) => value.as_i64().unwrap_or(0) != 0,
                _ => false,
            };
            let _dflt_value = iter.next();
            let pk = match iter.next() {
                Some(LibsqlValue::Integer { value }) => value.as_i64().unwrap_or(0) > 0,
                _ => false,
            };

            let ordinal = cols.len() as u32;
            cols.push(ColumnInfo {
                name,
                data_type: type_name.to_ascii_lowercase(),
                nullable: !notnull,
                primary_key: pk,
                ordinal,
            });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let target = quote_ident(&table.name);
        let where_str = where_clause(Engine::Libsql, filters);
        let sql = format!("SELECT COUNT(*) FROM {target}{where_str}");
        let res = self.run_sql(&sql).await?;

        if let Some(row) = res.rows.into_iter().next() {
            if let Some(val) = row.into_iter().next() {
                if let Value::Int(c) = convert_libsql_value(val) {
                    return Ok(c);
                }
            }
        }
        Ok(0)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;

        let target = quote_ident(&table.name);
        let where_str = where_clause(Engine::Libsql, &query.filters);
        let order_str = order_clause(Engine::Libsql, &query.sort);

        let sql = format!(
            "SELECT * FROM {target}{where_str}{order_str} LIMIT {} OFFSET {}",
            query.limit, query.offset
        );
        let res = self.run_sql(&sql).await?;

        let columns = res
            .cols
            .into_iter()
            .map(|c| ColumnMeta {
                name: c.name,
                type_name: c.decltype.unwrap_or_else(|| "text".into()).to_lowercase(),
            })
            .collect();

        let mut rows: Vec<Vec<Value>> = res
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(convert_libsql_value).collect())
            .collect();

        let max_rows = query.limit as usize;
        let truncated = rows.len() > max_rows;
        if truncated {
            rows.truncate(max_rows);
        }

        Ok(ResultSet { columns, rows, truncated })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let stmts = split_statements(sql);
        let mut results = Vec::new();

        for stmt in stmts {
            let res = self.run_sql(&stmt).await?;
            if res.cols.is_empty() {
                results.push(StatementResult::Affected {
                    rows_affected: res.affected_row_count.unwrap_or(0),
                });
            } else {
                let columns = res
                    .cols
                    .into_iter()
                    .map(|c| ColumnMeta {
                        name: c.name,
                        type_name: c.decltype.unwrap_or_else(|| "text".into()).to_lowercase(),
                    })
                    .collect();

                let truncated = res.rows.len() > max_rows;
                let rows = res
                    .rows
                    .into_iter()
                    .take(max_rows)
                    .map(|row| row.into_iter().map(convert_libsql_value).collect())
                    .collect();

                results.push(StatementResult::Rows {
                    result: ResultSet { columns, rows, truncated },
                });
            }
        }

        Ok(results)
    }

    async fn close(&self) {}

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        let cat = self.catalog().await?;
        let mut fks = Vec::new();

        for schema in cat.schemas {
            for table in schema.tables {
                let sql = format!("PRAGMA foreign_key_list({})", quote_ident(&table.name));
                let res = match self.run_sql(&sql).await {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                for row in res.rows {
                    let mut iter = row.into_iter();
                    let id = match iter.next() {
                        Some(LibsqlValue::Integer { value }) => value.to_string(),
                        _ => "0".into(),
                    };
                    let _seq = iter.next();
                    let to_table = match iter.next() {
                        Some(LibsqlValue::Text { value }) => value,
                        _ => continue,
                    };
                    let from_col = match iter.next() {
                        Some(LibsqlValue::Text { value }) => value,
                        _ => continue,
                    };
                    let to_col = match iter.next() {
                        Some(LibsqlValue::Text { value }) => value,
                        _ => continue,
                    };

                    fks.push(ForeignKey {
                        name: format!("fk_{}_{}_{id}", table.name, from_col),
                        from_schema: None,
                        from_table: table.name.clone(),
                        from_columns: vec![from_col],
                        to_schema: None,
                        to_table,
                        to_columns: vec![to_col],
                    });
                }
            }
        }

        Ok(fks)
    }

    async fn ddl(&self, table: &TableRef) -> AppResult<Option<String>> {
        let sql = format!(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{}'",
            table.name.replace('\'', "''")
        );
        let res = self.run_sql(&sql).await?;
        if let Some(row) = res.rows.into_iter().next() {
            if let Some(val) = row.into_iter().next() {
                if let Value::Text(ddl_sql) = convert_libsql_value(val) {
                    return Ok(Some(ddl_sql));
                }
            }
        }
        Ok(None)
    }

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        // `parent` is "main" (the only database) for scoped kinds and the owning
        // table when the explorer drills into a table's indexes or triggers.
        let owner = parent.filter(|p| *p != "main");
        let rows = match kind {
            ObjectKind::Table => self.master_rows("table", None).await?,
            ObjectKind::View => self.master_rows("view", None).await?,
            ObjectKind::Index => self.master_rows("index", owner).await?,
            ObjectKind::Trigger => self.master_rows("trigger", owner).await?,
            ObjectKind::Setting => {
                let mut out: Vec<ObjectSummary> = Vec::new();
                for row in self.settings().await {
                    if let Some(name) = cell_text(&row, "name") {
                        let writable = !matches!(name.as_str(), "encoding" | "page_count" | "freelist_count");
                        out.push(
                            ObjectSummary::new(ObjectKind::Setting, name, None)
                                .with_detail(cell_text(&row, "value").unwrap_or_default())
                                .with_badge(if writable { "writable" } else { "read-only" }),
                        );
                    }
                }
                return Ok(out);
            }
            _ => Vec::new(),
        };
        let mut out: Vec<ObjectSummary> = rows.iter().filter_map(|row| master_summary(kind, row)).collect();
        out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        out.truncate(OBJECT_CAP);
        Ok(out)
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Table => {
                let row = self.find_master("table", reference).await?;
                self.table_detail(reference, &row).await
            }
            ObjectKind::View => {
                let row = self.find_master("view", reference).await?;
                self.view_detail(reference, &row).await
            }
            ObjectKind::Index => {
                let row = self.find_master("index", reference).await?;
                self.index_detail(reference, &row).await
            }
            ObjectKind::Trigger => {
                let row = self.find_master("trigger", reference).await?;
                Ok(self.trigger_detail(reference, &row))
            }
            ObjectKind::Setting => self.setting_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn libsql_value_conversion() {
        assert_eq!(convert_libsql_value(LibsqlValue::Null), Value::Null);
        assert_eq!(
            convert_libsql_value(LibsqlValue::Text { value: "hello".into() }),
            Value::Text("hello".into())
        );
        assert_eq!(
            convert_libsql_value(LibsqlValue::Integer { value: serde_json::json!(42) }),
            Value::Int(42)
        );
    }

    fn row(pairs: &[(&str, Value)]) -> CatalogRow {
        pairs.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect()
    }

    fn text(value: &str) -> Value {
        Value::Text(value.to_string())
    }

    #[test]
    fn catalog_queries_are_escaped() {
        assert_eq!(
            master_query("table", None),
            "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 2000"
        );
        assert!(master_query("index", Some("o'rders")).contains("AND tbl_name = 'o''rders'"));
        assert!(settings_query().starts_with("SELECT 'journal_mode' AS name, CAST((SELECT * FROM pragma_journal_mode()) AS TEXT) AS value"));
        assert_eq!(settings_query().matches(" UNION ALL ").count(), SETTINGS.len() - 1);
        assert_eq!(pragma_statement("journal_mode", "wal"), "PRAGMA journal_mode = wal;");
        assert_eq!(pragma_statement("cache_size", "-2000"), "PRAGMA cache_size = -2000;");
        assert_eq!(pragma_statement("x", "it's"), "PRAGMA x = 'it''s';");
    }

    #[test]
    fn master_rows_become_summaries() {
        let table = row(&[("name", text("users")), ("tbl_name", text("users")), ("sql", text("CREATE TABLE users (id)"))]);
        let summary = master_summary(ObjectKind::Table, &table).unwrap_or_else(|| panic!("table summary"));
        assert_eq!(summary.reference.parent.as_deref(), Some("main"));
        assert_eq!(summary.badge, None);

        let virtual_table = row(&[("name", text("docs")), ("tbl_name", text("docs")), ("sql", text("CREATE VIRTUAL TABLE docs USING fts5(a, b)"))]);
        let summary = master_summary(ObjectKind::Table, &virtual_table).unwrap_or_else(|| panic!("virtual summary"));
        assert_eq!(summary.badge.as_deref(), Some("fts5"));
        assert_eq!(summary.detail.as_deref(), Some("virtual table"));

        let unique = row(&[("name", text("users_email")), ("tbl_name", text("users")), ("sql", text("CREATE UNIQUE INDEX users_email ON users(email)"))]);
        let summary = master_summary(ObjectKind::Index, &unique).unwrap_or_else(|| panic!("index summary"));
        assert_eq!(summary.reference.parent.as_deref(), Some("users"));
        assert_eq!(summary.badge.as_deref(), Some("unique"));
        assert_eq!(summary.detail.as_deref(), Some("on users"));

        let auto = row(&[("name", text("sqlite_autoindex_users_1")), ("tbl_name", text("users")), ("sql", Value::Null)]);
        assert_eq!(master_summary(ObjectKind::Index, &auto).and_then(|s| s.badge).as_deref(), Some("auto"));

        let trigger = row(&[
            ("name", text("audit")),
            ("tbl_name", text("orders")),
            ("sql", text("CREATE TRIGGER audit AFTER INSERT ON orders BEGIN SELECT 1; END")),
        ]);
        let summary = master_summary(ObjectKind::Trigger, &trigger).unwrap_or_else(|| panic!("trigger summary"));
        assert_eq!(summary.badge.as_deref(), Some("insert"));
        assert_eq!(summary.detail.as_deref(), Some("after on orders"));
        assert!(master_summary(ObjectKind::Session, &trigger).is_none());
    }

    #[test]
    fn pragma_rows_become_columns_and_grids() {
        let rows = vec![
            row(&[("cid", Value::Int(0)), ("name", text("id")), ("type", text("INTEGER")), ("notnull", Value::Int(1)), ("pk", Value::Int(1))]),
            row(&[("cid", Value::Int(1)), ("name", text("email")), ("type", text("TEXT")), ("notnull", Value::Int(0)), ("pk", Value::Int(0))]),
        ];
        let columns = columns_from_table_info(&rows);
        assert_eq!(columns.len(), 2);
        assert!(columns[0].primary_key && !columns[0].nullable && columns[0].data_type == "integer");
        assert!(!columns[1].primary_key && columns[1].nullable);
        assert_eq!(columns[1].ordinal, 1);

        let grid = to_result_set(&rows, &["cid", "name", "missing"]);
        assert_eq!(grid.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["cid", "name", "missing"]);
        assert_eq!(grid.rows[0][1], text("id"));
        assert_eq!(grid.rows[0][2], Value::Null);
        assert!(!grid.truncated);

        let settings = row(&[("name", text("journal_mode")), ("value", text("wal"))]);
        assert_eq!(cell_text(&settings, "value").as_deref(), Some("wal"));
        assert_eq!(cell_text(&settings, "nope"), None);
        assert_eq!(cell_i64(&row(&[("n", text("42"))]), "n"), Some(42));
        assert_eq!(cell_i64(&row(&[("n", Value::Int(7))]), "n"), Some(7));
        assert_eq!(cell_i64(&row(&[("n", Value::Null)]), "n"), None);
        assert!(is_virtual(Some("CREATE VIRTUAL TABLE t USING fts5(a)")));
        assert!(!is_virtual(None));
        assert_eq!(virtual_module("CREATE VIRTUAL TABLE t USING rtree(id, x, y)").as_deref(), Some("rtree"));
        assert_eq!(virtual_module("CREATE TABLE t (a)"), None);
        assert_eq!(trigger_facts("CREATE TRIGGER t INSTEAD OF DELETE ON v BEGIN SELECT 1; END"), (Some("INSTEAD OF"), Some("DELETE")));
    }
}
