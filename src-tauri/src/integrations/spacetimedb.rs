// SOT: spacetimedb-integration, spacetimedb-http-api, sats-json, spacetimedb-sql

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, local, Auth, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat, StatGroup,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::Value as Json;
use std::sync::Arc;

// ============================================================================
// WHAT:  SpacetimeDB adapter, over the module host's HTTP API.
// WHY:   SpacetimeDB is a relational store whose schema is owned by a WASM
//        module rather than by DDL, so the workbench reads and queries it but
//        never creates tables: `CREATE TABLE` is not part of the language.
// HOW:   `host` is the base URL of the module host, `database` the module name
//        or identity, and the secret an optional Spacetime identity token sent
//        as `Authorization: Bearer`. Two endpoints do the work:
//          POST /v1/database/{db}/sql     raw SQL body, `;`-separated
//              → [{ schema: ProductType, rows: [[cell, …], …] }, …], one entry
//                per statement.
//          GET  /v1/database/{db}/schema?version=9
//              → RawModuleDef: `tables[]` (name, product_type_ref, table_type,
//                table_access) and `typespace.types[]` resolving each table's
//                column names and types.
//        Rows are SATS-JSON ProductValues: positional arrays matching the
//        statement's schema, so columns come from the schema and cells are
//        mapped by position. SpacetimeDB's SQL subset has no OFFSET, so paging
//        is client-side through `http::local::page`.
// WHERE: src-tauri/src/integrations/http.rs (client, local paging),
//        https://spacetimedb.com/docs/http/database
// ============================================================================

const MAX_PAGE_ROWS: u32 = 5_000;
const SCHEMA_VERSION: u32 = 9;

// WHAT:  Statements SpacetimeDB's SQL subset does not accept, plus the writes a
//        read-only connection must not send.
// WHY:   The guard cannot parse this dialect, so the adapter refuses writes
//        itself — the rule every non-SQL adapter in this crate follows.
const WRITE_KEYWORDS: [&str; 3] = ["INSERT", "UPDATE", "DELETE"];

pub struct SpacetimeIntegration {
    engine: Engine,
    http: HttpClient,
    database: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let database = conn
        .summary
        .database
        .clone()
        .filter(|d| !d.trim().is_empty())
        .ok_or_else(|| AppError::invalid_input("SpacetimeDB needs the module name or identity in the database field."))?;
    // The token is optional: an anonymous caller may still read public tables.
    let auth = match conn.secret.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        Some(token) => Auth::Bearer(token.to_string()),
        None => Auth::None,
    };
    let http = HttpClient::new(base_url(conn, Some(3000), false), auth, false)?;
    let integration = SpacetimeIntegration { engine: conn.summary.engine, http, database, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// WHAT:  True when the statement would modify data.
// WHY:   Word-based so a column called `updated_at` does not look like an UPDATE.
fn is_write(sql: &str) -> bool {
    sql.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|w| WRITE_KEYWORDS.iter().any(|k| w.eq_ignore_ascii_case(k)))
}

// WHAT:  The column names of one statement's `schema` ProductType.
// HOW:   Each element is `{ name: {some: "id"} | {none: []}, algebraic_type: … }`.
//        An unnamed element keeps its position as the header, which is what the
//        grid needs to stay aligned with the row arrays.
fn schema_columns(schema: &Json) -> Vec<ColumnMeta> {
    schema
        .get("elements")
        .and_then(Json::as_array)
        .map(|els| {
            els.iter()
                .enumerate()
                .map(|(i, el)| ColumnMeta {
                    name: el
                        .pointer("/name/some")
                        .and_then(Json::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("column{i}")),
                    type_name: algebraic_type_name(el.get("algebraic_type")),
                })
                .collect()
        })
        .unwrap_or_default()
}

// WHAT:  A readable name for a SATS AlgebraicType.
// HOW:   The encoding is a single-key tagged union — `{"String": []}`,
//        `{"Ref": 3}`, `{"Sum": …}`. The tag alone is the useful label; a Ref
//        keeps its index so a user can tell two references apart.
fn algebraic_type_name(ty: Option<&Json>) -> String {
    let Some(obj) = ty.and_then(Json::as_object) else { return "unknown".into() };
    let Some((tag, body)) = obj.iter().next() else { return "unknown".into() };
    match tag.as_str() {
        "Ref" => body.as_u64().map(|n| format!("ref({n})")).unwrap_or_else(|| "ref".into()),
        // An Option is encoded as a Sum of `some` / `none`.
        "Sum" if is_option_sum(body) => format!("{}?", option_inner(body)),
        other => other.to_ascii_lowercase(),
    }
}

fn is_option_sum(sum: &Json) -> bool {
    let names: Vec<&str> = sum
        .get("variants")
        .and_then(Json::as_array)
        .map(|vs| vs.iter().filter_map(|v| v.pointer("/name/some").and_then(Json::as_str)).collect())
        .unwrap_or_default();
    names == ["some", "none"]
}

fn option_inner(sum: &Json) -> String {
    sum.get("variants")
        .and_then(Json::as_array)
        .and_then(|vs| vs.first())
        .map(|v| algebraic_type_name(v.get("algebraic_type")))
        .unwrap_or_else(|| "unknown".into())
}

// WHAT:  One SATS-JSON cell as the workbench's own value type.
// WHY:   SATS encodes 64-bit and larger integers as strings to survive JSON, so
//        a numeric string is kept as Decimal rather than forced through f64
//        where it would lose precision.
fn sats_value(cell: &Json) -> Value {
    match cell {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => n.as_i64().map(Value::Int).unwrap_or_else(|| n.as_f64().map(Value::Float).unwrap_or(Value::Null)),
        Json::String(s) => {
            if s.len() > 1 && s.chars().all(|c| c.is_ascii_digit() || c == '-') {
                Value::Decimal(s.clone())
            } else {
                Value::Text(s.clone())
            }
        }
        other => Value::Json(other.clone()),
    }
}

// WHAT:  Turns one `{schema, rows}` entry into a ResultSet.
fn entry_to_result_set(entry: &Json, max_rows: usize) -> ResultSet {
    let columns = entry.get("schema").map(schema_columns).unwrap_or_default();
    let all = entry.get("rows").and_then(Json::as_array).cloned().unwrap_or_default();
    let truncated = all.len() > max_rows;
    let rows = all
        .iter()
        .take(max_rows)
        .map(|row| match row {
            // A ProductValue is positional; anything else is a single cell.
            Json::Array(cells) => cells.iter().map(sats_value).collect(),
            other => vec![sats_value(other)],
        })
        .collect();
    ResultSet { columns, rows, truncated }
}

impl SpacetimeIntegration {
    fn sql_path(&self) -> String {
        format!("/v1/database/{}/sql", self.database)
    }

    // WHAT:  Runs SQL and returns one entry per statement.
    async fn run(&self, sql: &str) -> AppResult<Vec<Json>> {
        let body = self.http.post_raw(&self.sql_path(), "text/plain", sql.to_string(), Some("application/json")).await?;
        let parsed: Json = serde_json::from_str(&body).map_err(|e| AppError::internal(format!("SpacetimeDB returned invalid JSON: {e}")))?;
        match parsed {
            Json::Array(entries) => Ok(entries),
            other => Ok(vec![other]),
        }
    }

    async fn module_def(&self) -> AppResult<Json> {
        self.http.get_json(&format!("/v1/database/{}/schema?version={SCHEMA_VERSION}", self.database)).await
    }

    // WHAT:  Table name → its columns, resolved through the typespace.
    // HOW:   A table points at `product_type_ref`, an index into
    //        `typespace.types`, whose Product elements are the columns.
    fn table_columns(module: &Json, table: &Json) -> Vec<ColumnInfo> {
        let Some(idx) = table.get("product_type_ref").and_then(Json::as_u64) else { return Vec::new() };
        let Some(product) = module.pointer(&format!("/typespace/types/{idx}/Product")) else { return Vec::new() };
        let primary: Vec<u64> = table
            .get("primary_key")
            .and_then(Json::as_array)
            .map(|a| a.iter().filter_map(Json::as_u64).collect())
            .unwrap_or_default();
        product
            .get("elements")
            .and_then(Json::as_array)
            .map(|els| {
                els.iter()
                    .enumerate()
                    .map(|(i, el)| ColumnInfo {
                        name: el
                            .pointer("/name/some")
                            .and_then(Json::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| format!("column{i}")),
                        data_type: algebraic_type_name(el.get("algebraic_type")),
                        nullable: matches!(el.get("algebraic_type").and_then(Json::as_object).and_then(|o| o.keys().next()).map(String::as_str), Some("Sum")),
                        primary_key: primary.contains(&(i as u64)),
                        ordinal: i as u32,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn tables_of(module: &Json) -> Vec<&Json> {
        module.get("tables").and_then(Json::as_array).map(|a| a.iter().collect()).unwrap_or_default()
    }

    fn table_named<'a>(module: &'a Json, name: &str) -> Option<&'a Json> {
        Self::tables_of(module).into_iter().find(|t| t.get("name").and_then(Json::as_str) == Some(name))
    }

    // WHAT:  `Private` tables are only readable with a token that owns them.
    fn access_badge(table: &Json) -> Option<String> {
        table.get("table_access").and_then(Json::as_object).and_then(|o| o.keys().next()).map(|k| k.to_ascii_lowercase())
    }
}

pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities {
            sql: true,
            // One flat table namespace per module: no schemas or databases.
            namespaces: false,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: false,
            transactions: false,
            exact_estimate: true,
        },
        object_kinds: vec![K::Table, K::Index, K::Constraint, K::Sequence, K::Procedure],
        tools: vec![T::Stats, T::Erd],
    }
}

#[async_trait]
impl Integration for SpacetimeIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        // /v1/ping is unauthenticated and answers for the host as a whole.
        if self.http.get_text("/v1/ping").await.is_ok() {
            return Ok(());
        }
        // Older hosts have no /v1/ping; asking for the module schema proves both
        // that the host is up and that the module name resolves.
        self.module_def().await.map(|_| ())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        // The module def carries no host version; report the module instead so
        // the status bar still says what it is connected to.
        Ok(Some(format!("SpacetimeDB module {}", self.database)))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        // A connection addresses exactly one module; the host has no listing
        // endpoint that an unprivileged token may call.
        Ok(vec![self.database.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let module = self.module_def().await?;
        let mut tables: Vec<TableInfo> = Self::tables_of(&module)
            .iter()
            .filter_map(|t| t.get("name").and_then(Json::as_str))
            .map(|name| TableInfo { schema: None, name: name.to_string(), kind: TableKind::Table, row_estimate: None })
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.database.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let module = self.module_def().await?;
        let found = Self::table_named(&module, &table.name)
            .ok_or_else(|| AppError::invalid_input(format!("SpacetimeDB module has no table `{}`.", table.name)))?;
        Ok(Self::table_columns(&module, found))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let columns = self.columns(table).await?;
        validate_columns(&columns, &query.sort, &query.filters)?;

        // SpacetimeDB's SQL subset has no OFFSET, so the window is taken here.
        // The LIMIT still bounds what crosses the wire.
        let wanted = query.offset.saturating_add(u64::from(query.limit)).min(u64::from(MAX_PAGE_ROWS)).max(1);
        let entries = self.run(&format!("SELECT * FROM {} LIMIT {wanted}", table.name)).await?;
        let set = match entries.first() {
            Some(entry) => entry_to_result_set(entry, MAX_PAGE_ROWS as usize),
            None => return Ok(ResultSet { columns: Vec::new(), rows: Vec::new(), truncated: false }),
        };

        let headers: Vec<String> = set.columns.iter().map(|c| c.name.clone()).collect();
        let hit_cap = set.rows.len() as u32 >= MAX_PAGE_ROWS;
        let rows = local::page(&headers, set.rows, query);
        Ok(ResultSet { columns: set.columns, rows, truncated: hit_cap })
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        // COUNT(*) is exact and cheap here: the whole module lives in memory.
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        // Filters are applied client-side, so an unfiltered count can come from
        // the server while a filtered one has to count the fetched window.
        if filters.is_empty() {
            // SpacetimeDB rejects an unaliased aggregate: "Aggregate expressions
            // must have column aliases".
            let entries = self.run(&format!("SELECT COUNT(*) AS count FROM {}", table.name)).await?;
            if let Some(set) = entries.first().map(|e| entry_to_result_set(e, 1)) {
                if let Some(cell) = set.rows.first().and_then(|r| r.first()) {
                    return Ok(match cell {
                        Value::Int(n) => *n,
                        Value::Decimal(s) | Value::Text(s) => s.parse().unwrap_or(0),
                        _ => 0,
                    });
                }
            }
            return Ok(0);
        }
        let query = PageQuery { sort: Vec::new(), filters: filters.to_vec(), offset: 0, limit: MAX_PAGE_ROWS };
        Ok(self.fetch_page(table, &query).await?.rows.len() as i64)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        if self.read_only && is_write(sql) {
            return Err(AppError::read_only("This connection is read-only: SpacetimeDB writes are blocked."));
        }
        let entries = self.run(sql).await?;
        Ok(entries
            .iter()
            .map(|entry| {
                // A statement that returns nothing still reports its shape, so the
                // editor can say "0 rows" rather than showing an empty grid.
                StatementResult::Rows { result: entry_to_result_set(entry, max_rows) }
            })
            .collect())
    }

    async fn close(&self) {}

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let module = self.module_def().await?;
        let mut out = Vec::new();
        match kind {
            ObjectKind::Table => {
                for t in Self::tables_of(&module) {
                    let Some(name) = t.get("name").and_then(Json::as_str) else { continue };
                    let cols = Self::table_columns(&module, t).len();
                    let mut s = ObjectSummary::new(ObjectKind::Table, name, None).with_detail(format!("{cols} columns"));
                    if let Some(badge) = Self::access_badge(t) {
                        s = s.with_badge(badge);
                    }
                    out.push(s);
                }
            }
            ObjectKind::Index | ObjectKind::Constraint | ObjectKind::Sequence => {
                let key = match kind {
                    ObjectKind::Index => "indexes",
                    ObjectKind::Constraint => "constraints",
                    _ => "sequences",
                };
                for t in Self::tables_of(&module) {
                    let Some(table_name) = t.get("name").and_then(Json::as_str) else { continue };
                    if parent.is_some_and(|p| p != table_name) {
                        continue;
                    }
                    for item in t.get(key).and_then(Json::as_array).into_iter().flatten() {
                        let name = item
                            .get("name")
                            .and_then(|n| n.pointer("/some").and_then(Json::as_str).or_else(|| n.as_str()))
                            .unwrap_or("(unnamed)");
                        out.push(ObjectSummary::new(kind, name, Some(table_name.to_string())));
                    }
                }
            }
            // Reducers are the module's callable procedures.
            ObjectKind::Procedure => {
                for r in module.get("reducers").and_then(Json::as_array).into_iter().flatten() {
                    let Some(name) = r.get("name").and_then(Json::as_str) else { continue };
                    let arity = r.pointer("/params/elements").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
                    let lifecycle = r.pointer("/lifecycle/some").and_then(Json::as_object).and_then(|o| o.keys().next()).cloned();
                    let mut s = ObjectSummary::new(ObjectKind::Procedure, name, None).with_detail(format!("{arity} argument(s)"));
                    if let Some(l) = lifecycle {
                        s = s.with_badge(l.to_ascii_lowercase());
                    }
                    out.push(s);
                }
            }
            _ => {}
        }
        out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        Ok(out)
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let module = self.module_def().await?;
        let mut detail = ObjectDetail::empty(reference);
        match reference.kind {
            ObjectKind::Table => {
                let Some(table) = Self::table_named(&module, &reference.name) else { return Ok(detail) };
                detail = detail.definition(serde_json::to_string_pretty(table).unwrap_or_default(), CodeLanguage::Json);
                detail.columns = Self::table_columns(&module, table);
                let column_count = detail.columns.len();
                if let Some(access) = Self::access_badge(table) {
                    detail = detail.property("Access", access);
                }
                if let Some(ty) = table.get("table_type").and_then(Json::as_object).and_then(|o| o.keys().next()) {
                    detail = detail.property("Type", ty.to_ascii_lowercase());
                }
                detail = detail.property("Columns", column_count.to_string());
                // Reading is the only action: schema changes live in module code.
                detail = detail.action(ObjectAction::new(
                    "select",
                    "Select rows",
                    format!("SELECT * FROM {} LIMIT 100", reference.name),
                ));
            }
            ObjectKind::Procedure => {
                let found = module
                    .get("reducers")
                    .and_then(Json::as_array)
                    .into_iter()
                    .flatten()
                    .find(|r| r.get("name").and_then(Json::as_str) == Some(reference.name.as_str()));
                if let Some(r) = found {
                    detail = detail.definition(serde_json::to_string_pretty(r).unwrap_or_default(), CodeLanguage::Json);
                    for (i, el) in r.pointer("/params/elements").and_then(Json::as_array).into_iter().flatten().enumerate() {
                        let name = el.pointer("/name/some").and_then(Json::as_str).map(str::to_string).unwrap_or_else(|| format!("arg{i}"));
                        detail = detail.property(&name, algebraic_type_name(el.get("algebraic_type")));
                    }
                }
            }
            _ => {}
        }
        Ok(detail)
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let module = self.module_def().await?;
        let tables = Self::tables_of(&module);
        let reducers = module.get("reducers").and_then(Json::as_array).map(Vec::len).unwrap_or(0);
        let columns: usize = tables.iter().map(|t| Self::table_columns(&module, t).len()).sum();
        Ok(ServerStats::now(vec![StatGroup {
            title: "Module".into(),
            stats: vec![
                Stat::text("Name", self.database.clone()),
                Stat::number("Tables", tables.len() as f64, None),
                Stat::number("Columns", columns as f64, None),
                Stat::number("Reducers", reducers as f64, None),
            ],
        }]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionInput, ConnectionSummary, Environment, SslMode};

    fn schema_json() -> Json {
        serde_json::json!({
            "elements": [
                {"name": {"some": "id"}, "algebraic_type": {"U64": []}},
                {"name": {"some": "name"}, "algebraic_type": {"String": []}},
                {"name": {"none": []}, "algebraic_type": {"Ref": 2}}
            ]
        })
    }

    #[test]
    fn schema_elements_become_columns() {
        let cols = schema_columns(&schema_json());
        assert_eq!(cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["id", "name", "column2"]);
        assert_eq!(cols.iter().map(|c| c.type_name.as_str()).collect::<Vec<_>>(), vec!["u64", "string", "ref(2)"]);
    }

    #[test]
    fn option_types_read_as_nullable() {
        let opt = serde_json::json!({"Sum": {"variants": [
            {"name": {"some": "some"}, "algebraic_type": {"String": []}},
            {"name": {"some": "none"}, "algebraic_type": {"Product": {"elements": []}}}
        ]}});
        assert_eq!(algebraic_type_name(Some(&opt)), "string?");
    }

    #[test]
    fn rows_are_positional_product_values() {
        let entry = serde_json::json!({"schema": schema_json(), "rows": [[1, "ann", 7], [2, "bob", 8]]});
        let set = entry_to_result_set(&entry, 10);
        assert_eq!(set.columns.len(), 3);
        assert_eq!(set.rows.len(), 2);
        assert_eq!(set.rows[0][0], Value::Int(1));
        assert_eq!(set.rows[0][1], Value::Text("ann".into()));
        assert!(!set.truncated);
    }

    #[test]
    fn row_cap_marks_truncation() {
        let entry = serde_json::json!({"schema": schema_json(), "rows": [[1, "a", 0], [2, "b", 0], [3, "c", 0]]});
        let set = entry_to_result_set(&entry, 2);
        assert_eq!(set.rows.len(), 2);
        assert!(set.truncated);
    }

    #[test]
    fn wide_integers_keep_their_digits() {
        // SATS sends u64/u128 as strings; f64 would silently round them.
        assert_eq!(sats_value(&serde_json::json!("18446744073709551615")), Value::Decimal("18446744073709551615".into()));
        assert_eq!(sats_value(&serde_json::json!("ann")), Value::Text("ann".into()));
    }

    #[test]
    fn write_detection_is_word_based() {
        assert!(is_write("INSERT INTO person (name) VALUES ('a')"));
        assert!(is_write("delete from person"));
        assert!(!is_write("SELECT updated_at, deleted_flag FROM person"));
    }

    #[test]
    fn columns_resolve_through_the_typespace() {
        let module = serde_json::json!({
            "typespace": {"types": [{"Product": {"elements": [
                {"name": {"some": "id"}, "algebraic_type": {"U32": []}},
                {"name": {"some": "nick"}, "algebraic_type": {"String": []}}
            ]}}]},
            "tables": [{"name": "person", "product_type_ref": 0, "primary_key": [0], "table_access": {"Public": []}}]
        });
        let table = SpacetimeIntegration::table_named(&module, "person").unwrap_or_else(|| panic!("person"));
        let cols = SpacetimeIntegration::table_columns(&module, table);
        assert_eq!(cols.len(), 2);
        assert!(cols[0].primary_key);
        assert!(!cols[1].primary_key);
        assert_eq!(cols[1].data_type, "string");
        assert_eq!(SpacetimeIntegration::access_badge(table).as_deref(), Some("public"));
    }

    // WHAT:  Live round trip against a real module host.
    // HOW:   DBFREE_TEST_SPACETIMEDB_URL / _DB (+ optional _TOKEN).
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(url) = std::env::var("DBFREE_TEST_SPACETIMEDB_URL") else {
            return;
        };
        let input = ConnectionInput {
            name: "live-spacetime".into(),
            engine: Engine::Spacetimedb,
            environment: Environment::Local,
            read_only: false,
            host: Some(url),
            port: None,
            database: Some(std::env::var("DBFREE_TEST_SPACETIMEDB_DB").unwrap_or_else(|_| "quickstart".into())),
            username: None,
            password: None,
            file_path: None,
            ssl_mode: SslMode::Disable,
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary::draft(&input, false),
            secret: std::env::var("DBFREE_TEST_SPACETIMEDB_TOKEN").ok(),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        db.ping().await.unwrap_or_else(|e| panic!("ping: {e}"));
        assert!(db.server_version().await.unwrap_or_default().is_some());

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        let tables = catalog.schemas.first().map(|s| s.tables.clone()).unwrap_or_default();
        assert!(!tables.is_empty(), "the module should expose at least one table");

        if let Some(first) = tables.first() {
            let table_ref = TableRef { schema: None, name: first.name.clone() };
            let cols = db.columns(&table_ref).await.unwrap_or_else(|e| panic!("columns: {e}"));
            assert!(!cols.is_empty());
            let query = PageQuery { sort: Vec::new(), filters: Vec::new(), offset: 0, limit: 5 };
            let page = db.fetch_page(&table_ref, &query).await.unwrap_or_else(|e| panic!("page: {e}"));
            assert!(page.rows.len() <= 5);
            let _ = db.count(&table_ref, &[]).await.unwrap_or_else(|e| panic!("count: {e}"));
            let objects = db.objects(ObjectKind::Table, None).await.unwrap_or_else(|e| panic!("objects: {e}"));
            assert!(!objects.is_empty());
            let detail = db
                .object_detail(&ObjectRef { kind: ObjectKind::Table, name: first.name.clone(), parent: None })
                .await
                .unwrap_or_else(|e| panic!("detail: {e}"));
            assert!(!detail.columns.is_empty());
        }
        let stats = db.server_stats().await.unwrap_or_else(|e| panic!("stats: {e}"));
        assert!(!stats.groups.is_empty());
        db.close().await;
    }
}
