// SOT: objectdb-integration, objectdb-rest, jpql, objectdb-gateway, objectdb-object-explorer

use crate::error::{AppError, AppResult};
use crate::integrations::http::{base_url, json_result, json_to_value, json_type_name, objects_to_result_set, HttpClient};
use crate::integrations::sql::validate_columns;
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind,
    ObjectRef, ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, SortRule, SslMode,
    StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use serde_json::{json, Map, Value as Json};
use std::sync::Arc;

// ============================================================================
// WHAT:  ObjectDB adapter, through the db-free JPQL gateway contract.
// WHY:   ObjectDB has no public HTTP API (its server speaks a proprietary
//        binary protocol only the JPA/JDO client implements), so DB Free talks
//        to a small gateway the user hosts next to the database. The contract
//        is three endpoints (see docs/objectdb-gateway.md):
//          GET  {base}/entities                → ["Customer", …]
//          GET  {base}/entities/{name}/fields  → [{name, type, id?, nullable?}, …]
//          POST {base}/query {jpql, max, first, params} → [row, …] | {rows, truncated} | {affected}
// HOW:   `host` = gateway base URL, `database` = optional path prefix. Pages
//        are `SELECT e FROM Entity e WHERE … ORDER BY …` with named parameters
//        for filter values; counts are `SELECT COUNT(e)`. `execute` is JPQL
//        passthrough (`{"jpql","params"}` JSON accepted); UPDATE / DELETE are
//        refused when the connection is read-only. A 404 on `/entities` is
//        reported as "gateway missing" with a pointer to the doc.
// WHERE: docs/objectdb-gateway.md, src-tauri/src/integrations/http.rs (client)
// ============================================================================

const DEFAULT_PORT: u16 = 8090;
const MAX_PAGE_ROWS: u32 = 5_000;
const GATEWAY_HINT: &str = "ObjectDB requires the db-free JPQL gateway; see docs/objectdb-gateway.md";

pub struct ObjectDbIntegration {
    engine: Engine,
    http: HttpClient,
    prefix: String,
    label: String,
    read_only: bool,
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let prefix = normalise_prefix(s.database.as_deref());
    let base = base_url(conn, Some(DEFAULT_PORT), false);
    let insecure = s.ssl_mode == SslMode::Require;
    let http = HttpClient::new(base, HttpClient::auth_from_connection(conn), insecure)?;
    let label = if prefix.is_empty() { "objectdb".to_string() } else { prefix.trim_matches('/').to_string() };
    let integration = ObjectDbIntegration { engine: s.engine, http, prefix, label, read_only: s.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// WHAT:  `crm` / `/crm/` → `/crm`; empty → ``.
fn normalise_prefix(raw: Option<&str>) -> String {
    let t = raw.map(str::trim).unwrap_or("").trim_matches('/');
    if t.is_empty() {
        String::new()
    } else {
        format!("/{t}")
    }
}

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

// WHAT:  Only Java-identifier-like names reach JPQL (no quoting mechanism exists).
fn check_ident(raw: &str) -> AppResult<&str> {
    let ok = !raw.is_empty()
        && raw.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_' || c == '$')
        && raw.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '$' || c == '.');
    if ok {
        Ok(raw)
    } else {
        Err(AppError::invalid_input(format!("`{raw}` is not a valid JPQL identifier.")))
    }
}

fn lenient_value(raw: &str) -> Json {
    let t = raw.trim();
    if t.eq_ignore_ascii_case("true") {
        return Json::Bool(true);
    }
    if t.eq_ignore_ascii_case("false") {
        return Json::Bool(false);
    }
    if let Ok(i) = t.parse::<i64>() {
        return json!(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return json!(f);
    }
    Json::String(t.to_string())
}

// WHAT:  Filters → JPQL WHERE with named params `:p0`, `:p1`, …
fn where_clause(filters: &[FilterRule], params: &mut Map<String, Json>) -> AppResult<String> {
    if filters.is_empty() {
        return Ok(String::new());
    }
    let mut parts = Vec::new();
    for (i, f) in filters.iter().enumerate() {
        let col = format!("e.{}", check_ident(&f.column)?);
        let p = format!("p{i}");
        let v = f.value.trim();
        let expr = match f.op {
            FilterOp::Eq | FilterOp::Ne | FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => {
                params.insert(p.clone(), lenient_value(v));
                let op = match f.op {
                    FilterOp::Eq => "=",
                    FilterOp::Ne => "<>",
                    FilterOp::Gt => ">",
                    FilterOp::Gte => ">=",
                    FilterOp::Lt => "<",
                    _ => "<=",
                };
                format!("{col} {op} :{p}")
            }
            FilterOp::Contains => {
                params.insert(p.clone(), Json::String(format!("%{}%", v.to_lowercase())));
                format!("LOWER({col}) LIKE :{p}")
            }
            FilterOp::StartsWith => {
                params.insert(p.clone(), Json::String(format!("{}%", v.to_lowercase())));
                format!("LOWER({col}) LIKE :{p}")
            }
            FilterOp::EndsWith => {
                params.insert(p.clone(), Json::String(format!("%{}", v.to_lowercase())));
                format!("LOWER({col}) LIKE :{p}")
            }
            FilterOp::In => {
                let items: Vec<Json> = v.split(',').map(str::trim).filter(|x| !x.is_empty()).map(lenient_value).collect();
                if items.is_empty() {
                    "1 = 0".to_string()
                } else {
                    params.insert(p.clone(), Json::Array(items));
                    format!("{col} IN :{p}")
                }
            }
            FilterOp::IsNull => format!("{col} IS NULL"),
            FilterOp::IsNotNull => format!("{col} IS NOT NULL"),
        };
        parts.push(expr);
    }
    Ok(format!(" WHERE {}", parts.join(" AND ")))
}

fn order_clause(sort: &[SortRule]) -> AppResult<String> {
    if sort.is_empty() {
        return Ok(String::new());
    }
    let mut parts = Vec::new();
    for s in sort {
        parts.push(format!("e.{} {}", check_ident(&s.column)?, if s.desc { "DESC" } else { "ASC" }));
    }
    Ok(format!(" ORDER BY {}", parts.join(", ")))
}

// WHAT:  Gateway field list → ColumnInfo (objects or plain strings; first = key when unmarked).
fn parse_fields(v: &Json) -> Vec<ColumnInfo> {
    let items = v.as_array().cloned().unwrap_or_default();
    let any_marked = items.iter().any(|f| f.get("id").and_then(Json::as_bool).unwrap_or(false));
    items
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            let (name, ty, id, nullable) = match f {
                Json::String(s) => (s.clone(), "object".to_string(), false, true),
                Json::Object(o) => (
                    o.get("name").and_then(Json::as_str)?.to_string(),
                    o.get("type").and_then(Json::as_str).unwrap_or("object").to_string(),
                    o.get("id").and_then(Json::as_bool).unwrap_or(false),
                    o.get("nullable").and_then(Json::as_bool).unwrap_or(true),
                ),
                _ => return None,
            };
            let primary_key = if any_marked { id } else { i == 0 };
            Some(ColumnInfo { name, data_type: ty, nullable: nullable && !primary_key, primary_key, ordinal: i as u32 + 1 })
        })
        .collect()
}

fn parse_entities(v: &Json) -> Vec<String> {
    let mut names: Vec<String> = v
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|e| match e {
                    Json::String(s) => Some(s.clone()),
                    Json::Object(o) => o.get("name").and_then(Json::as_str).map(str::to_string),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

// WHAT:  Gateway query response → (rows, truncated, affected).
fn unwrap_query_response(v: &Json) -> (Vec<Json>, bool, Option<u64>) {
    match v {
        Json::Array(a) => (a.clone(), false, None),
        Json::Object(o) => {
            if let Some(n) = o.get("affected").and_then(Json::as_u64) {
                return (Vec::new(), false, Some(n));
            }
            let rows = o.get("rows").or_else(|| o.get("result")).and_then(Json::as_array).cloned().unwrap_or_default();
            (rows, o.get("truncated").and_then(Json::as_bool).unwrap_or(false), None)
        }
        other => (vec![other.clone()], false, None),
    }
}

// WHAT:  Rows → grid: objects union keys, arrays become c0..cn, scalars a `value` column.
fn rows_to_result_set(rows: &[Json], id_first: Option<&str>, max_rows: usize) -> ResultSet {
    if rows.is_empty() {
        return ResultSet { columns: vec![], rows: vec![], truncated: false };
    }
    if rows.iter().all(Json::is_object) {
        return objects_to_result_set(rows, id_first, max_rows);
    }
    if rows.iter().all(Json::is_array) {
        let width = rows.iter().filter_map(Json::as_array).map(Vec::len).max().unwrap_or(0);
        let columns = (0..width)
            .map(|i| ColumnMeta {
                name: format!("c{i}"),
                type_name: rows.iter().filter_map(|r| r.get(i)).find(|v| !v.is_null()).map(json_type_name).unwrap_or("json").into(),
            })
            .collect();
        let truncated = rows.len() > max_rows;
        let grid = rows
            .iter()
            .take(max_rows)
            .map(|r| (0..width).map(|i| r.get(i).map(json_to_value).unwrap_or(Value::Null)).collect())
            .collect();
        return ResultSet { columns, rows: grid, truncated };
    }
    let truncated = rows.len() > max_rows;
    let type_name = rows.iter().find(|v| !v.is_null()).map(json_type_name).unwrap_or("json").to_string();
    ResultSet {
        columns: vec![ColumnMeta { name: "value".into(), type_name }],
        rows: rows.iter().take(max_rows).map(|v| vec![json_to_value(v)]).collect(),
        truncated,
    }
}

fn align_to_columns(columns: &[ColumnInfo], rows: &[Json], truncated: bool) -> ResultSet {
    if !rows.iter().all(Json::is_object) {
        let pk = columns.iter().find(|c| c.primary_key).map(|c| c.name.as_str());
        let mut rs = rows_to_result_set(rows, pk, usize::MAX);
        rs.truncated = truncated;
        return rs;
    }
    let mut names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut types: Vec<String> = columns.iter().map(|c| c.data_type.clone()).collect();
    for r in rows {
        if let Some(o) = r.as_object() {
            for (k, v) in o {
                if !names.iter().any(|n| n == k) {
                    names.push(k.clone());
                    types.push(json_type_name(v).into());
                }
            }
        }
    }
    let grid = rows
        .iter()
        .map(|r| {
            let o = r.as_object();
            names.iter().map(|n| o.and_then(|o| o.get(n)).map(json_to_value).unwrap_or(Value::Null)).collect()
        })
        .collect();
    ResultSet { columns: names.into_iter().zip(types).map(|(name, type_name)| ColumnMeta { name, type_name }).collect(), rows: grid, truncated }
}

fn first_word(text: &str) -> String {
    text.trim_start().split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("").to_ascii_uppercase()
}

fn is_write_jpql(text: &str) -> bool {
    matches!(first_word(text).as_str(), "UPDATE" | "DELETE" | "INSERT")
}

fn parse_execute_input(text: &str) -> AppResult<(String, Map<String, Json>)> {
    let t = text.trim();
    if t.is_empty() {
        return Err(AppError::invalid_input("Empty JPQL query."));
    }
    if t.starts_with('{') {
        let v: Json = serde_json::from_str(t).map_err(|e| AppError::invalid_input(format!("Invalid JSON body: {e}")))?;
        let jpql = v.get("jpql").or_else(|| v.get("query")).and_then(Json::as_str).ok_or_else(|| AppError::invalid_input("JSON body needs a \"jpql\" string."))?;
        let params = v.get("params").and_then(Json::as_object).cloned().unwrap_or_default();
        return Ok((jpql.to_string(), params));
    }
    Ok((t.to_string(), Map::new()))
}

impl ObjectDbIntegration {
    fn path(&self, rest: &str) -> String {
        format!("{}/{}", self.prefix, rest.trim_start_matches('/'))
    }

    fn gateway_error(err: AppError) -> AppError {
        match err {
            AppError::NotFound { message } => AppError::not_connected(format!("{GATEWAY_HINT} ({message})")),
            other => other,
        }
    }

    async fn entities(&self) -> AppResult<Vec<String>> {
        let v: Json = self.http.get_json(&self.path("entities")).await.map_err(Self::gateway_error)?;
        Ok(parse_entities(&v))
    }

    async fn run(&self, jpql: &str, params: Map<String, Json>, max: usize, first: u64) -> AppResult<(Vec<Json>, bool, Option<u64>)> {
        let body = json!({ "jpql": jpql, "max": max, "first": first, "params": params });
        let v: Json = self.http.post_json(&self.path("query"), &body).await.map_err(Self::gateway_error)?;
        Ok(unwrap_query_response(&v))
    }
}

// ---------------------------------------------------------------------------
// Object explorer
//
// WHAT:  ObjectDB's only first-class object is the persistent class, so the
//        explorer lists entities and shows their fields. There is no catalog of
//        indexes, users or settings behind the gateway contract, and nothing is
//        invented here: only what `/entities` and JPQL can answer.
// ---------------------------------------------------------------------------

const OBJECT_CAP: usize = 2_000;

// WHAT:  Field list → a grid (name, type, id, nullable) for the detail tab.
pub(crate) fn field_rows(columns: &[ColumnInfo]) -> ResultSet {
    ResultSet {
        columns: [("name", "string"), ("type", "string"), ("id", "boolean"), ("nullable", "boolean")]
            .iter()
            .map(|(n, t)| ColumnMeta { name: (*n).to_string(), type_name: (*t).to_string() })
            .collect(),
        rows: columns
            .iter()
            .map(|c| vec![Value::Text(c.name.clone()), Value::Text(c.data_type.clone()), Value::Bool(c.primary_key), Value::Bool(c.nullable)])
            .collect(),
        truncated: false,
    }
}

impl ObjectDbIntegration {
    async fn instance_count(&self, entity: &str) -> Option<i64> {
        self.count(&TableRef { schema: None, name: entity.to_string() }, &[]).await.ok()
    }

    async fn list_objects(&self, kind: ObjectKind) -> AppResult<Vec<ObjectSummary>> {
        if kind != ObjectKind::Class {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for name in self.entities().await?.into_iter().take(OBJECT_CAP) {
            let mut s = ObjectSummary::new(ObjectKind::Class, name.as_str(), None);
            if let Some(n) = self.instance_count(&name).await {
                s = s.with_detail(format!("{n} instances"));
            }
            // A dotted entity name is a fully-qualified class; the badge keeps
            // the package visible once the tree shows only the simple name.
            if let Some((package, _)) = name.rsplit_once('.') {
                s = s.with_badge(package.to_string());
            }
            out.push(s);
        }
        out.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        Ok(out)
    }

    async fn class_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let entity = check_ident(&reference.name)?;
        let cols = self.columns(&TableRef { schema: None, name: reference.name.clone() }).await?;
        let mut detail = ObjectDetail::empty(reference)
            .definition(format!("SELECT e FROM {entity} e"), CodeLanguage::Sql)
            .property("fields", cols.len().to_string());
        if let Some(id) = cols.iter().find(|c| c.primary_key) {
            detail = detail.property("id field", format!("{} ({})", id.name, id.data_type));
        }
        if let Some(n) = self.instance_count(&reference.name).await {
            detail = detail.property("instances", n.to_string());
        }
        detail.rows = Some(field_rows(&cols));
        detail.columns = cols;
        detail = detail
            .action(ObjectAction::new("preview", "Preview instances", format!("SELECT e FROM {entity} e")))
            .action(ObjectAction::new("count", "Count instances", format!("SELECT COUNT(e) FROM {entity} e")))
            .action(ObjectAction::destructive("delete_all", "Delete all instances", format!("DELETE FROM {entity} e")));
        Ok(detail)
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { describes_fields: true, namespaces: false, fixed_columns: true, exact_estimate: true, views: false, ..Capabilities::DOCUMENT },
        object_kinds: vec![K::Class],
        tools: vec![],
    }
}

#[async_trait]
impl Integration for ObjectDbIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
    }

    async fn ping(&self) -> AppResult<()> {
        self.entities().await.map(|_| ())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let v: Json = match self.http.get_json(&self.path("")).await {
            Ok(v) => v,
            Err(_) => return Ok(Some("ObjectDB (JPQL gateway)".into())),
        };
        Ok(Some(
            v.get("version")
                .and_then(Json::as_str)
                .map(|s| format!("ObjectDB {s}"))
                .unwrap_or_else(|| "ObjectDB (JPQL gateway)".into()),
        ))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.label.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.label.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let tables = self
            .entities()
            .await?
            .into_iter()
            .map(|name| TableInfo { schema: None, name, kind: TableKind::Table, row_estimate: None })
            .collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: self.label.clone(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        check_ident(&table.name)?;
        let v: Json = self.http.get_json(&self.path(&format!("entities/{}/fields", pct(&table.name)))).await.map_err(Self::gateway_error)?;
        let cols = parse_fields(&v);
        if cols.is_empty() {
            return Err(AppError::not_found(format!("Entity `{}` reports no fields.", table.name)));
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        self.count(table, &[]).await.map(Some)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let entity = check_ident(&table.name)?;
        let mut params = Map::new();
        let jpql = format!("SELECT COUNT(e) FROM {entity} e{}", where_clause(filters, &mut params)?);
        let (rows, _, _) = self.run(&jpql, params, 1, 0).await?;
        Ok(rows.first().and_then(|r| r.as_i64().or_else(|| r.as_array().and_then(|a| a.first()).and_then(Json::as_i64))).unwrap_or(0))
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;
        let entity = check_ident(&table.name)?;
        let mut params = Map::new();
        let jpql = format!("SELECT e FROM {entity} e{}{}", where_clause(&query.filters, &mut params)?, order_clause(&query.sort)?);
        let limit = query.limit.min(MAX_PAGE_ROWS) as usize;
        let (rows, truncated, _) = self.run(&jpql, params, limit, query.offset).await?;
        Ok(align_to_columns(&cols, &rows, truncated))
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let (jpql, params) = parse_execute_input(sql)?;
        if self.read_only && is_write_jpql(&jpql) {
            return Err(AppError::read_only(format!("This connection is read-only; `{}` JPQL is blocked.", first_word(&jpql))));
        }
        let max = max_rows.max(1);
        let (rows, truncated, affected) = self.run(&jpql, params, max, 0).await?;
        if let Some(n) = affected {
            return Ok(vec![StatementResult::Affected { rows_affected: n }]);
        }
        if rows.is_empty() && is_write_jpql(&jpql) {
            return Ok(vec![StatementResult::Affected { rows_affected: 0 }]);
        }
        let mut rs = if rows.iter().all(Json::is_object) || rows.iter().all(Json::is_array) || rows.iter().all(|r| !r.is_object() && !r.is_array()) {
            rows_to_result_set(&rows, rows.iter().any(|r| r.get("id").is_some()).then_some("id"), max)
        } else {
            json_result(Json::Array(rows))
        };
        rs.truncated = rs.truncated || truncated;
        Ok(vec![StatementResult::Rows { result: rs }])
    }

    async fn close(&self) {}

    async fn objects(&self, kind: ObjectKind, _parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        self.list_objects(kind).await
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        match reference.kind {
            ObjectKind::Class => self.class_detail(reference).await,
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(column: &str, op: FilterOp, value: &str) -> FilterRule {
        FilterRule { column: column.into(), op, value: value.into() }
    }

    #[test]
    fn prefix_and_identifiers() {
        assert_eq!(normalise_prefix(None), "");
        assert_eq!(normalise_prefix(Some(" /crm/ ")), "/crm");
        assert!(check_ident("Customer").is_ok());
        assert!(check_ident("com.acme.Customer").is_ok());
        assert!(check_ident("Cust omer").is_err());
        assert!(check_ident("1abc").is_err());
    }

    #[test]
    fn where_and_order_use_named_params() {
        let mut p = Map::new();
        let w = where_clause(
            &[rule("age", FilterOp::Gte, "5"), rule("name", FilterOp::Contains, "Ac"), rule("tier", FilterOp::In, "gold, 2"), rule("x", FilterOp::IsNull, "")],
            &mut p,
        )
        .unwrap();
        assert_eq!(w, " WHERE e.age >= :p0 AND LOWER(e.name) LIKE :p1 AND e.tier IN :p2 AND e.x IS NULL");
        assert_eq!(p["p0"], json!(5));
        assert_eq!(p["p1"], json!("%ac%"));
        assert_eq!(p["p2"], json!(["gold", 2]));
        assert_eq!(order_clause(&[SortRule { column: "id".into(), desc: true }]).unwrap(), " ORDER BY e.id DESC");
        assert!(where_clause(&[rule("bad name", FilterOp::Eq, "1")], &mut Map::new()).is_err());
    }

    #[test]
    fn fields_and_entities_parse() {
        let cols = parse_fields(&json!([{"name": "id", "type": "long", "id": true}, {"name": "name", "type": "String"}]));
        assert_eq!(cols.len(), 2);
        assert!(cols[0].primary_key && !cols[1].primary_key);
        let plain = parse_fields(&json!(["id", "name"]));
        assert!(plain[0].primary_key);
        assert_eq!(parse_entities(&json!([{"name": "B"}, "A"])), vec!["A", "B"]);
    }

    #[test]
    fn responses_map_to_grids() {
        let (rows, truncated, affected) = unwrap_query_response(&json!({"rows": [1, 2], "truncated": true}));
        assert_eq!(rows.len(), 2);
        assert!(truncated);
        assert_eq!(affected, None);
        assert_eq!(unwrap_query_response(&json!({"affected": 3})).2, Some(3));
        let rs = rows_to_result_set(&[json!([1, "a"]), json!([2, "b"])], None, 10);
        assert_eq!(rs.columns[0].name, "c0");
        assert_eq!(rs.rows[1][1], Value::Text("b".into()));
        let rs = rows_to_result_set(&[json!(7)], None, 10);
        assert_eq!(rs.columns[0].name, "value");
        let cols = parse_fields(&json!([{"name": "id", "id": true}, {"name": "name"}]));
        let rs = align_to_columns(&cols, &[json!({"id": 1, "name": "x", "extra": true})], false);
        assert_eq!(rs.columns.len(), 3);
        assert!(is_write_jpql("update Customer c set c.x = 1"));
        assert!(!is_write_jpql("SELECT c FROM Customer c"));
        let (q, p) = parse_execute_input(r#"{"jpql": "SELECT c FROM C c WHERE c.id = :i", "params": {"i": 1}}"#).unwrap();
        assert!(q.starts_with("SELECT"));
        assert_eq!(p["i"], json!(1));
    }

    #[test]
    fn fields_become_a_detail_grid() {
        let cols = parse_fields(&json!([{"name": "id", "type": "long", "id": true}, {"name": "name", "type": "String"}]));
        let rs = field_rows(&cols);
        assert_eq!(rs.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["name", "type", "id", "nullable"]);
        assert_eq!(rs.rows[0], vec![Value::Text("id".into()), Value::Text("long".into()), Value::Bool(true), Value::Bool(false)]);
        assert_eq!(rs.rows[1][2], Value::Bool(false));
        assert!(field_rows(&[]).rows.is_empty());
    }
}
