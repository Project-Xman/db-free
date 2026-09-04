// SOT: val-town-integration, val-town-adapter, val-town-api, sqlite-over-http, val-town-object-explorer

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
struct ValTownPayload {
    statement: String,
    params: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ValTownResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

pub struct ValTownIntegration {
    client: Client,
    endpoint: String,
    api_token: Option<String>,
    database_name: String,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let endpoint = s
        .host
        .as_deref()
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(|h| {
            if h.starts_with("http://") || h.starts_with("https://") {
                if h.ends_with("/v1/sqlite/execute") {
                    h.to_string()
                } else {
                    format!("{}/v1/sqlite/execute", h.trim_end_matches('/'))
                }
            } else {
                format!("https://{}/v1/sqlite/execute", h.trim_end_matches('/'))
            }
        })
        .unwrap_or_else(|| "https://api.val.town/v1/sqlite/execute".to_string());

    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| AppError::driver(e.to_string()))?;

    let database_name = s.database.clone().unwrap_or_else(|| "val_town".into());

    let integration = ValTownIntegration {
        client,
        endpoint,
        api_token: conn.secret.clone(),
        database_name,
    };

    integration.ping().await?;
    Ok(Arc::new(integration))
}

impl ValTownIntegration {
    async fn run_sql(&self, sql: &str) -> AppResult<ValTownResponse> {
        let payload = ValTownPayload {
            statement: sql.to_string(),
            params: Vec::new(),
        };

        let mut req = self.client.post(&self.endpoint).json(&payload);
        if let Some(token) = &self.api_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }

        let resp = req.send().await.map_err(|e| AppError::driver(format!("Val Town request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::driver(format!("Val Town error ({status}): {body}")));
        }

        let val_resp: ValTownResponse = resp
            .json()
            .await
            .map_err(|e| AppError::driver(format!("Failed to parse Val Town response: {e}")))?;

        Ok(val_resp)
    }

    // WHAT:  One catalog query as name → value maps.
    async fn catalog_rows(&self, sql: &str) -> AppResult<Vec<CatalogRow>> {
        let res = self.run_sql(sql).await?;
        Ok(res
            .rows
            .into_iter()
            .map(|row| res.columns.iter().cloned().zip(row.iter().map(json_to_value)).collect())
            .collect())
    }

    // WHAT:  Catalog rows, or none when the endpoint rejects the query (PRAGMA support varies).
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
        let sql = cell_text(row, "sql");
        let target = quote_ident(&name);
        let mut detail = ObjectDetail::empty(reference).property("Table", cell_text(row, "tbl_name").unwrap_or_default());
        detail = match &sql {
            Some(text) => detail.definition(text.clone(), CodeLanguage::Sql),
            None => detail.definition("-- automatic index created for a PRIMARY KEY or UNIQUE constraint", CodeLanguage::Sql),
        };
        detail = detail.property("Unique", (index_badge(sql.as_deref()) == "unique").to_string());
        let columns = self.optional_rows(&format!("PRAGMA index_info({target})")).await;
        if !columns.is_empty() {
            detail.rows = Some(to_result_set(&columns, &["seqno", "cid", "name"]));
        }
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
}

fn json_to_value(val: &serde_json::Value) -> Value {
    match val {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(val.clone()),
    }
}

fn val_town_to_result_set(resp: ValTownResponse, max_rows: usize) -> ResultSet {
    let columns = resp
        .columns
        .into_iter()
        .map(|name| ColumnMeta {
            name,
            type_name: "text".into(),
        })
        .collect();

    let total = resp.rows.len();
    let truncated = total > max_rows;
    let rows = resp
        .rows
        .into_iter()
        .take(max_rows)
        .map(|r| r.iter().map(json_to_value).collect())
        .collect();

    ResultSet {
        columns,
        rows,
        truncated,
    }
}

// ============================================================================
// OBJECT EXPLORER
//
// WHAT:  Tables, views, indexes and triggers of a Val Town SQLite database,
//        read through the same /v1/sqlite/execute endpoint as every query.
// WHY:   Val Town is SQLite behind an HTTP API: sqlite_master and the PRAGMA
//        statements are the whole catalog.
// HOW:   Responses are normalised to name → value maps (`CatalogRow`) so the
//        builders below are pure and testable offline. Val Town's PRAGMA
//        support is not documented as complete: every PRAGMA read may fail and
//        the explorer returns what worked.
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
            transactions: false,
            exact_estimate: true,
        },
        object_kinds: vec![K::Table, K::View, K::Index, K::Trigger],
        tools: vec![T::Erd],
    }
}

#[async_trait]
impl Integration for ValTownIntegration {
    fn engine(&self) -> Engine {
        Engine::ValTown
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
                if let Some(s) = val.as_str() {
                    return Ok(Some(format!("Val Town (SQLite {s})")));
                }
            }
        }
        Ok(Some("Val Town".into()))
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
            let name = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                Some(n) => n,
                _ => continue,
            };
            let kind = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                Some(k) if k.eq_ignore_ascii_case("view") => TableKind::View,
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
            let name = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                Some(n) => n,
                _ => continue,
            };
            let type_name = iter
                .next()
                .and_then(|v| v.as_str().map(ToString::to_string))
                .unwrap_or_else(|| "TEXT".into());
            let notnull = iter.next().and_then(|v| v.as_i64()).unwrap_or(0) != 0;
            let _dflt_value = iter.next().and_then(|v| v.as_str().map(ToString::to_string));
            let pk = iter.next().and_then(|v| v.as_i64()).unwrap_or(0) > 0;

            cols.push(ColumnInfo {
                name,
                data_type: type_name,
                nullable: !notnull,
                primary_key: pk,
                ordinal: cols.len() as u32,
            });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let target = quote_ident(&table.name);
        let where_str = where_clause(Engine::ValTown, filters);
        let sql = format!("SELECT COUNT(*) FROM {target}{where_str}");
        let res = self.run_sql(&sql).await?;

        if let Some(row) = res.rows.into_iter().next() {
            if let Some(val) = row.into_iter().next() {
                if let Some(c) = val.as_i64() {
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
        let where_str = where_clause(Engine::ValTown, &query.filters);
        let order_str = order_clause(Engine::ValTown, &query.sort);

        let sql = format!(
            "SELECT * FROM {target}{where_str}{order_str} LIMIT {} OFFSET {}",
            query.limit, query.offset
        );
        let res = self.run_sql(&sql).await?;
        Ok(val_town_to_result_set(res, query.limit as usize))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let stmts = split_statements(sql);
        let mut results = Vec::new();

        for stmt in stmts {
            let res = self.run_sql(&stmt).await?;
            if res.columns.is_empty() {
                results.push(StatementResult::Affected { rows_affected: 0 });
            } else {
                results.push(StatementResult::Rows { result: val_town_to_result_set(res, max_rows) });
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
                    let id = match iter.next().and_then(|v| v.as_i64()) {
                        Some(i) => i.to_string(),
                        _ => "0".into(),
                    };
                    let _seq = iter.next();
                    let to_table = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                        Some(t) => t,
                        _ => continue,
                    };
                    let from_col = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                        Some(f) => f,
                        _ => continue,
                    };
                    let to_col = match iter.next().and_then(|v| v.as_str().map(ToString::to_string)) {
                        Some(t) => t,
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
                if let Some(s) = val.as_str() {
                    return Ok(Some(s.to_string()));
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
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn val_town_value_conversion() {
        assert_eq!(json_to_value(&serde_json::Value::Null), Value::Null);
        assert_eq!(json_to_value(&serde_json::json!(42)), Value::Int(42));
        assert_eq!(
            json_to_value(&serde_json::json!("val_town")),
            Value::Text("val_town".into())
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
            master_query("view", None),
            "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type = 'view' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 2000"
        );
        assert!(master_query("trigger", Some("o'rders")).contains("AND tbl_name = 'o''rders'"));
    }

    #[test]
    fn master_rows_become_summaries() {
        let table = row(&[("name", text("users")), ("tbl_name", text("users")), ("sql", text("CREATE TABLE users (id)"))]);
        let summary = master_summary(ObjectKind::Table, &table).unwrap_or_else(|| panic!("table summary"));
        assert_eq!(summary.reference.parent.as_deref(), Some("main"));

        let virtual_table = row(&[("name", text("docs")), ("tbl_name", text("docs")), ("sql", text("CREATE VIRTUAL TABLE docs USING fts5(a)"))]);
        assert_eq!(master_summary(ObjectKind::Table, &virtual_table).and_then(|s| s.badge).as_deref(), Some("fts5"));

        let index = row(&[("name", text("i")), ("tbl_name", text("users")), ("sql", text("CREATE UNIQUE INDEX i ON users(a)"))]);
        let summary = master_summary(ObjectKind::Index, &index).unwrap_or_else(|| panic!("index summary"));
        assert_eq!(summary.reference.parent.as_deref(), Some("users"));
        assert_eq!(summary.badge.as_deref(), Some("unique"));

        let auto = row(&[("name", text("sqlite_autoindex_users_1")), ("tbl_name", text("users")), ("sql", Value::Null)]);
        assert_eq!(master_summary(ObjectKind::Index, &auto).and_then(|s| s.badge).as_deref(), Some("auto"));

        let trigger = row(&[("name", text("t")), ("tbl_name", text("orders")), ("sql", text("CREATE TRIGGER t BEFORE UPDATE ON orders BEGIN SELECT 1; END"))]);
        let summary = master_summary(ObjectKind::Trigger, &trigger).unwrap_or_else(|| panic!("trigger summary"));
        assert_eq!(summary.badge.as_deref(), Some("update"));
        assert_eq!(summary.detail.as_deref(), Some("before on orders"));
        assert!(master_summary(ObjectKind::Setting, &trigger).is_none());
    }

    #[test]
    fn pragma_rows_become_columns_and_grids() {
        let rows = vec![
            row(&[("cid", Value::Int(0)), ("name", text("id")), ("type", text("INTEGER")), ("notnull", Value::Int(1)), ("pk", Value::Int(1))]),
            row(&[("cid", Value::Int(1)), ("name", text("email")), ("type", text("TEXT")), ("notnull", Value::Int(0)), ("pk", Value::Int(0))]),
        ];
        let columns = columns_from_table_info(&rows);
        assert_eq!(columns.len(), 2);
        assert!(columns[0].primary_key && !columns[0].nullable);
        assert_eq!(columns[1].data_type, "text");

        let grid = to_result_set(&rows, &["name", "missing"]);
        assert_eq!(grid.rows[0][0], text("id"));
        assert_eq!(grid.rows[1][1], Value::Null);
        assert_eq!(cell_i64(&row(&[("n", text("9"))]), "n"), Some(9));
        assert_eq!(cell_text(&row(&[("n", Value::Null)]), "n"), None);
        assert_eq!(virtual_module("CREATE VIRTUAL TABLE t USING rtree(id)").as_deref(), Some("rtree"));
        assert!(!is_virtual(Some("CREATE TABLE t (a)")));
        assert_eq!(trigger_facts("CREATE TRIGGER t AFTER DELETE ON o BEGIN SELECT 1; END"), (Some("AFTER"), Some("DELETE")));
    }
}
