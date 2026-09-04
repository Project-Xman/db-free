// SOT: dynamodb-integration, aws-sigv4, partiql, dynamodb-attribute-value

use crate::error::{AppError, AppResult};
use crate::integrations::aws_sigv4::{sign_post, AwsCredentials, SignRequest};
use crate::integrations::http::{json_result, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog,
    SchemaInfo, StatementResult, TableInfo, TableKind, TableRef, Value,
};
use async_trait::async_trait;
use reqwest::Method;
use std::sync::Arc;

// ============================================================================
// WHAT:  Amazon DynamoDB adapter over the JSON 1.0 API, signed with SigV4.
//        Region in `host`, optional endpoint override in `database`
//        (DynamoDB Local: http://localhost:8000). Every call is a POST to `/`
//        with `x-amz-target: DynamoDB_20120810.<Op>`.
// WHY:   No AWS SDK: the signing helper is ~100 lines and the API is JSON.
// HOW:   Tables are scanned with a FilterExpression built from the grid's
//        filters; sort and offset are client-side over the scanned window.
//        `execute` runs PartiQL (`ExecuteStatement`), a raw
//        `{"Operation": "Scan", "Params": {...}}` passthrough, or `TABLES` /
//        `DESCRIBE <table>`.
// WHERE: src-tauri/src/integrations/aws_sigv4.rs, src-tauri/src/integrations/http.rs
// ============================================================================

const CONTENT_TYPE: &str = "application/x-amz-json-1.0";
const SCAN_CAP: usize = 2_000;
const SAMPLE: usize = 50;

pub struct DynamoIntegration {
    engine: Engine,
    http: HttpClient,
    creds: AwsCredentials,
    endpoint: String,
    host: String,
    read_only: bool,
}

pub fn endpoint_for(region: &str, override_url: Option<&str>) -> String {
    match override_url.map(str::trim).filter(|u| !u.is_empty()) {
        Some(u) if u.starts_with("http://") || u.starts_with("https://") => u.trim_end_matches('/').to_string(),
        Some(u) => format!("http://{}", u.trim_end_matches('/')),
        None => format!("https://dynamodb.{region}.amazonaws.com"),
    }
}

fn host_of(url: &str) -> String {
    url.trim_start_matches("https://").trim_start_matches("http://").split('/').next().unwrap_or_default().to_string()
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let creds = AwsCredentials::from_connection(conn)?;
    let endpoint = endpoint_for(&creds.region, conn.summary.database.as_deref());
    let host = host_of(&endpoint);
    let http = HttpClient::new(endpoint.clone(), crate::integrations::http::Auth::None, false)?;
    let integration = DynamoIntegration { engine: conn.summary.engine, http, creds, endpoint, host, read_only: conn.summary.read_only };
    integration.ping().await?;
    Ok(Arc::new(integration))
}

// ---------------------------------------------------------------------------
// AttributeValue → model::Value
// ---------------------------------------------------------------------------

fn number_json(n: &str) -> serde_json::Value {
    if let Ok(i) = n.parse::<i64>() {
        return serde_json::Value::Number(i.into());
    }
    n.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(serde_json::Value::Number).unwrap_or_else(|| serde_json::Value::String(n.to_string()))
}

// WHAT:  Plain JSON view of an AttributeValue (nested M / L unwrapped).
pub fn attribute_to_json(av: &serde_json::Value) -> serde_json::Value {
    let Some(obj) = av.as_object() else { return av.clone() };
    let Some((kind, inner)) = obj.iter().next() else { return serde_json::Value::Null };
    match kind.as_str() {
        "S" | "B" => inner.clone(),
        "N" => inner.as_str().map(number_json).unwrap_or(inner.clone()),
        "BOOL" => inner.clone(),
        "NULL" => serde_json::Value::Null,
        "M" => serde_json::Value::Object(
            inner.as_object().map(|m| m.iter().map(|(k, v)| (k.clone(), attribute_to_json(v))).collect()).unwrap_or_default(),
        ),
        "L" => serde_json::Value::Array(inner.as_array().map(|l| l.iter().map(attribute_to_json).collect()).unwrap_or_default()),
        "SS" | "BS" => inner.clone(),
        "NS" => serde_json::Value::Array(inner.as_array().map(|l| l.iter().map(|n| n.as_str().map(number_json).unwrap_or(n.clone())).collect()).unwrap_or_default()),
        _ => av.clone(),
    }
}

pub fn attribute_to_value(av: &serde_json::Value) -> Value {
    let Some(obj) = av.as_object() else { return Value::Null };
    let Some((kind, inner)) = obj.iter().next() else { return Value::Null };
    match kind.as_str() {
        "S" => Value::Text(inner.as_str().unwrap_or_default().to_string()),
        "N" => {
            let n = inner.as_str().unwrap_or_default();
            if let Ok(i) = n.parse::<i64>() {
                Value::Int(i)
            } else if let Ok(f) = n.parse::<f64>() {
                Value::Float(f)
            } else {
                Value::Decimal(n.to_string())
            }
        }
        "BOOL" => Value::Bool(inner.as_bool().unwrap_or(false)),
        "NULL" => Value::Null,
        "B" => Value::Bytes(inner.as_str().unwrap_or_default().to_string()),
        "M" | "L" | "SS" | "NS" | "BS" => Value::Json(attribute_to_json(av)),
        _ => Value::Json(av.clone()),
    }
}

pub fn attribute_type_name(av: &serde_json::Value) -> &'static str {
    match av.as_object().and_then(|o| o.keys().next()).map(String::as_str) {
        Some("S") => "string",
        Some("N") => "number",
        Some("BOOL") => "boolean",
        Some("NULL") => "null",
        Some("B") => "binary",
        Some("M") => "map",
        Some("L") => "list",
        Some("SS") => "string_set",
        Some("NS") => "number_set",
        Some("BS") => "binary_set",
        _ => "json",
    }
}

fn key_type_name(t: &str) -> &'static str {
    match t {
        "S" => "string",
        "N" => "number",
        "B" => "binary",
        _ => "json",
    }
}

// WHAT:  A grid filter value as an AttributeValue (numbers → N, bools → BOOL, else S).
fn lenient_attribute(raw: &str) -> serde_json::Value {
    let t = raw.trim();
    if t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
        return serde_json::json!({"BOOL": t.eq_ignore_ascii_case("true")});
    }
    if t.parse::<f64>().is_ok() {
        return serde_json::json!({"N": t});
    }
    serde_json::json!({"S": t})
}

#[derive(Debug, Default, PartialEq)]
pub struct FilterExpr {
    pub expression: String,
    pub names: serde_json::Map<String, serde_json::Value>,
    pub values: serde_json::Map<String, serde_json::Value>,
}

// WHAT:  Grid filters → FilterExpression + ExpressionAttributeNames/Values.
pub fn filter_expression(filters: &[FilterRule]) -> FilterExpr {
    let mut out = FilterExpr::default();
    let mut parts = Vec::new();
    for (i, f) in filters.iter().enumerate() {
        let name = format!("#f{i}");
        out.names.insert(name.clone(), serde_json::Value::String(f.column.clone()));
        let val = format!(":v{i}");
        let mut set_val = |v: serde_json::Value| out.values.insert(val.clone(), v);
        let part = match f.op {
            FilterOp::Eq => { set_val(lenient_attribute(&f.value)); format!("{name} = {val}") }
            FilterOp::Ne => { set_val(lenient_attribute(&f.value)); format!("{name} <> {val}") }
            FilterOp::Gt => { set_val(lenient_attribute(&f.value)); format!("{name} > {val}") }
            FilterOp::Gte => { set_val(lenient_attribute(&f.value)); format!("{name} >= {val}") }
            FilterOp::Lt => { set_val(lenient_attribute(&f.value)); format!("{name} < {val}") }
            FilterOp::Lte => { set_val(lenient_attribute(&f.value)); format!("{name} <= {val}") }
            FilterOp::Contains => { set_val(serde_json::json!({"S": f.value.trim()})); format!("contains({name}, {val})") }
            FilterOp::StartsWith => { set_val(serde_json::json!({"S": f.value.trim()})); format!("begins_with({name}, {val})") }
            FilterOp::EndsWith => { set_val(serde_json::json!({"S": f.value.trim()})); format!("contains({name}, {val})") }
            FilterOp::In => {
                let items: Vec<String> = f
                    .value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .enumerate()
                    .map(|(j, s)| {
                        let k = format!(":v{i}_{j}");
                        out.values.insert(k.clone(), lenient_attribute(s));
                        k
                    })
                    .collect();
                format!("{name} IN ({})", items.join(", "))
            }
            FilterOp::IsNull => format!("(attribute_not_exists({name}) OR attribute_type({name}, :null{i}))"),
            FilterOp::IsNotNull => format!("(attribute_exists({name}) AND NOT attribute_type({name}, :null{i}))"),
        };
        if matches!(f.op, FilterOp::IsNull | FilterOp::IsNotNull) {
            out.values.insert(format!(":null{i}"), serde_json::json!({"S": "NULL"}));
        }
        parts.push(part);
    }
    out.expression = parts.join(" AND ");
    out
}

fn apply_filter(body: &mut serde_json::Value, filters: &[FilterRule]) {
    if filters.is_empty() {
        return;
    }
    let fe = filter_expression(filters);
    body["FilterExpression"] = serde_json::Value::String(fe.expression);
    body["ExpressionAttributeNames"] = serde_json::Value::Object(fe.names);
    if !fe.values.is_empty() {
        body["ExpressionAttributeValues"] = serde_json::Value::Object(fe.values);
    }
}

fn items_to_result(columns: &[String], types: &[String], items: &[serde_json::Value]) -> ResultSet {
    let rows = items
        .iter()
        .map(|it| columns.iter().map(|c| it.get(c).map(attribute_to_value).unwrap_or(Value::Null)).collect())
        .collect();
    let columns = columns.iter().zip(types).map(|(n, t)| ColumnMeta { name: n.clone(), type_name: t.clone() }).collect();
    ResultSet { columns, rows, truncated: false }
}

// WHAT:  Union of keys over items (pinned first), type from first non-null value.
fn union_columns(pinned: &[(String, String)], items: &[serde_json::Value]) -> (Vec<String>, Vec<String>) {
    let mut names: Vec<String> = pinned.iter().map(|(n, _)| n.clone()).collect();
    let mut types: Vec<String> = pinned.iter().map(|(_, t)| t.clone()).collect();
    for it in items {
        for (k, v) in it.as_object().into_iter().flatten() {
            if let Some(i) = names.iter().position(|n| n == k) {
                if types[i] == "null" {
                    types[i] = attribute_type_name(v).to_string();
                }
            } else {
                names.push(k.clone());
                types.push(attribute_type_name(v).to_string());
            }
        }
    }
    (names, types)
}

fn is_write_partiql(stmt: &str) -> bool {
    let head = stmt.split_whitespace().next().unwrap_or_default().to_uppercase();
    matches!(head.as_str(), "INSERT" | "UPDATE" | "DELETE" | "UPSERT" | "REPLACE")
}

fn is_write_op(op: &str) -> bool {
    !matches!(op, "Scan" | "Query" | "GetItem" | "BatchGetItem" | "DescribeTable" | "ListTables" | "DescribeTimeToLive" | "DescribeLimits" | "ExecuteStatement" | "TransactGetItems")
}

#[derive(Debug)]
enum Command {
    Tables,
    Describe(String),
    Op { op: String, params: serde_json::Value },
    Partiql(String),
}

fn parse_command(raw: &str) -> AppResult<Command> {
    let text = raw.trim().trim_end_matches(';').trim();
    if text.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }
    if text.starts_with('{') {
        let v: serde_json::Value = serde_json::from_str(text).map_err(|e| AppError::invalid_input(format!("Command is not valid JSON: {e}")))?;
        let op = v.get("Operation").or_else(|| v.get("operation")).and_then(|o| o.as_str()).ok_or_else(|| AppError::invalid_input("JSON commands need an \"Operation\" (e.g. \"Scan\") and \"Params\"."))?;
        let params = v.get("Params").or_else(|| v.get("params")).cloned().unwrap_or(serde_json::json!({}));
        return Ok(Command::Op { op: op.to_string(), params });
    }
    let mut words = text.split_whitespace();
    let head = words.next().unwrap_or_default().to_uppercase();
    match head.as_str() {
        "TABLES" => Ok(Command::Tables),
        "DESCRIBE" | "DESC" => {
            let t = words.next().ok_or_else(|| AppError::invalid_input("DESCRIBE needs a table name."))?;
            Ok(Command::Describe(t.trim_matches('"').to_string()))
        }
        _ => Ok(Command::Partiql(text.to_string())),
    }
}

impl DynamoIntegration {
    async fn call(&self, op: &str, body: &serde_json::Value) -> AppResult<serde_json::Value> {
        let bytes = serde_json::to_vec(body).map_err(|e| AppError::internal(e.to_string()))?;
        let target = format!("DynamoDB_20120810.{op}");
        let signed = sign_post(
            &self.creds,
            &SignRequest {
                service: "dynamodb",
                host: &self.host,
                path: "/",
                amz_target: Some(&target),
                content_type: CONTENT_TYPE,
                body: &bytes,
                now: chrono::Utc::now(),
            },
        )?;
        let mut req = self.http.request(Method::POST, &format!("{}/", self.endpoint)).body(bytes);
        for (k, v) in &signed.headers {
            if k != "host" {
                req = req.header(k.as_str(), v.as_str());
            }
        }
        let resp = self.http.send(req).await?;
        resp.json().await.map_err(|e| AppError::driver(format!("Malformed DynamoDB response: {e}")))
    }

    async fn describe(&self, table: &str) -> AppResult<serde_json::Value> {
        let r = self.call("DescribeTable", &serde_json::json!({"TableName": table})).await?;
        Ok(r.get("Table").cloned().unwrap_or(serde_json::Value::Null))
    }

    // WHAT:  Key schema → (name, type) pairs, partition key first.
    fn key_columns(desc: &serde_json::Value) -> Vec<(String, String)> {
        let attr_type = |name: &str| {
            desc.get("AttributeDefinitions")
                .and_then(|a| a.as_array())
                .into_iter()
                .flatten()
                .find(|d| d.get("AttributeName").and_then(|n| n.as_str()) == Some(name))
                .and_then(|d| d.get("AttributeType").and_then(|t| t.as_str()))
                .map(key_type_name)
                .unwrap_or("json")
                .to_string()
        };
        let mut keys: Vec<(String, String, bool)> = desc
            .get("KeySchema")
            .and_then(|k| k.as_array())
            .into_iter()
            .flatten()
            .filter_map(|k| {
                let name = k.get("AttributeName")?.as_str()?.to_string();
                let hash = k.get("KeyType").and_then(|t| t.as_str()) == Some("HASH");
                Some((name.clone(), attr_type(&name), hash))
            })
            .collect();
        keys.sort_by_key(|(_, _, hash)| !hash);
        keys.into_iter().map(|(n, t, _)| (n, t)).collect()
    }

    // WHAT:  Scans until `want` items are collected (or the table ends / cap hit).
    async fn scan(&self, table: &str, filters: &[FilterRule], want: usize) -> AppResult<Vec<serde_json::Value>> {
        let mut items = Vec::new();
        let mut start_key: Option<serde_json::Value> = None;
        let want = want.min(SCAN_CAP);
        while items.len() < want {
            let mut body = serde_json::json!({"TableName": table, "Limit": (want - items.len()).clamp(1, 1000)});
            apply_filter(&mut body, filters);
            if let Some(k) = &start_key {
                body["ExclusiveStartKey"] = k.clone();
            }
            let resp = self.call("Scan", &body).await?;
            if let Some(list) = resp.get("Items").and_then(|i| i.as_array()) {
                items.extend(list.iter().cloned());
            }
            match resp.get("LastEvaluatedKey").filter(|k| !k.is_null()) {
                Some(k) => start_key = Some(k.clone()),
                None => break,
            }
        }
        items.truncate(want);
        Ok(items)
    }

    async fn table_columns(&self, table: &str) -> AppResult<(Vec<(String, String)>, Vec<String>, Vec<String>)> {
        let desc = self.describe(table).await?;
        let keys = Self::key_columns(&desc);
        let sample = self.scan(table, &[], SAMPLE).await?;
        let (names, types) = union_columns(&keys, &sample);
        Ok((keys, names, types))
    }
}

#[async_trait]
impl Integration for DynamoIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { namespaces: false, ..Capabilities::DOCUMENT }
    }

    async fn ping(&self) -> AppResult<()> {
        let _ = self.call("ListTables", &serde_json::json!({"Limit": 1})).await?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        Ok(Some(if self.endpoint.contains("amazonaws.com") { format!("DynamoDB ({})", self.creds.region) } else { format!("DynamoDB ({})", self.endpoint) }))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.creds.region.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        Ok(vec![self.creds.region.clone()])
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut names = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let mut body = serde_json::json!({"Limit": 100});
            if let Some(s) = &start {
                body["ExclusiveStartTableName"] = serde_json::Value::String(s.clone());
            }
            let resp = self.call("ListTables", &body).await?;
            for t in resp.get("TableNames").and_then(|t| t.as_array()).into_iter().flatten() {
                if let Some(n) = t.as_str() {
                    names.push(n.to_string());
                }
            }
            match resp.get("LastEvaluatedTableName").and_then(|s| s.as_str()) {
                Some(s) if names.len() < 5_000 => start = Some(s.to_string()),
                _ => break,
            }
        }
        let tables = names.into_iter().map(|name| TableInfo { schema: Some("tables".into()), name, kind: TableKind::Table, row_estimate: None }).collect();
        Ok(SchemaCatalog { schemas: vec![SchemaInfo { name: "tables".into(), tables }] })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let (keys, names, types) = self.table_columns(&table.name).await?;
        Ok(names
            .into_iter()
            .zip(types)
            .enumerate()
            .map(|(i, (name, data_type))| ColumnInfo { primary_key: keys.iter().any(|(k, _)| k == &name), nullable: !keys.iter().any(|(k, _)| k == &name), name, data_type, ordinal: i as u32 + 1 })
            .collect())
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let desc = self.describe(&table.name).await?;
        Ok(desc.get("ItemCount").and_then(|c| c.as_i64()))
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        if filters.is_empty() {
            return Ok(self.row_estimate(table).await?.unwrap_or(0));
        }
        let mut total = 0i64;
        let mut scanned = 0i64;
        let mut start_key: Option<serde_json::Value> = None;
        loop {
            let mut body = serde_json::json!({"TableName": table.name, "Select": "COUNT"});
            apply_filter(&mut body, filters);
            if let Some(k) = &start_key {
                body["ExclusiveStartKey"] = k.clone();
            }
            let resp = self.call("Scan", &body).await?;
            total += resp.get("Count").and_then(|c| c.as_i64()).unwrap_or(0);
            scanned += resp.get("ScannedCount").and_then(|c| c.as_i64()).unwrap_or(0);
            match resp.get("LastEvaluatedKey").filter(|k| !k.is_null()) {
                Some(k) if scanned < 100_000 => start_key = Some(k.clone()),
                _ => break,
            }
        }
        Ok(total)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let (_, names, types) = self.table_columns(&table.name).await?;
        let want = query.offset as usize + query.limit as usize;
        let items = self.scan(&table.name, &query.filters, want).await?;
        let (names, types) = union_columns(&names.iter().cloned().zip(types).collect::<Vec<_>>(), &items);
        let mut rs = items_to_result(&names, &types, &items);
        let local_query = PageQuery { sort: query.sort.clone(), filters: vec![], offset: query.offset, limit: query.limit };
        rs.rows = crate::integrations::http::local::page(&names, rs.rows, &local_query);
        rs.truncated = items.len() >= SCAN_CAP;
        Ok(rs)
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let mut out = Vec::new();
        for stmt in crate::guard::destructive::split_statements(sql) {
            let result = match parse_command(&stmt)? {
                Command::Tables => {
                    let cat = self.catalog().await?;
                    let items: Vec<serde_json::Value> = cat.schemas.into_iter().flat_map(|s| s.tables).map(|t| serde_json::json!({"table": t.name})).collect();
                    StatementResult::Rows { result: json_result(serde_json::Value::Array(items)) }
                }
                Command::Describe(t) => StatementResult::Rows { result: json_result(self.describe(&t).await?) },
                Command::Op { op, params } => {
                    if self.read_only && is_write_op(&op) {
                        return Err(AppError::read_only(format!("This connection is read-only; {op} is blocked.")));
                    }
                    let resp = self.call(&op, &params).await?;
                    match resp.get("Items").and_then(|i| i.as_array()) {
                        Some(items) => {
                            let (names, types) = union_columns(&[], items);
                            let mut rs = items_to_result(&names, &types, items);
                            rs.truncated = rs.rows.len() > max_rows;
                            rs.rows.truncate(max_rows);
                            StatementResult::Rows { result: rs }
                        }
                        None if is_write_op(&op) => StatementResult::Affected { rows_affected: 1 },
                        None => StatementResult::Rows { result: json_result(resp) },
                    }
                }
                Command::Partiql(statement) => {
                    if self.read_only && is_write_partiql(&statement) {
                        return Err(AppError::read_only("This connection is read-only; INSERT/UPDATE/DELETE are blocked."));
                    }
                    let mut items = Vec::new();
                    let mut next: Option<String> = None;
                    loop {
                        let mut body = serde_json::json!({"Statement": statement, "Limit": max_rows.clamp(1, 1000)});
                        if let Some(t) = &next {
                            body["NextToken"] = serde_json::Value::String(t.clone());
                        }
                        let resp = self.call("ExecuteStatement", &body).await?;
                        if let Some(list) = resp.get("Items").and_then(|i| i.as_array()) {
                            items.extend(list.iter().cloned());
                        }
                        match resp.get("NextToken").and_then(|t| t.as_str()) {
                            Some(t) if items.len() < max_rows => next = Some(t.to_string()),
                            _ => break,
                        }
                    }
                    if is_write_partiql(&statement) {
                        StatementResult::Affected { rows_affected: items.len().max(1) as u64 }
                    } else {
                        let truncated = items.len() > max_rows;
                        items.truncate(max_rows);
                        let (names, types) = union_columns(&[], &items);
                        let mut rs = items_to_result(&names, &types, &items);
                        rs.truncated = truncated;
                        StatementResult::Rows { result: rs }
                    }
                }
            };
            out.push(result);
        }
        Ok(out)
    }

    async fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_attribute_values() {
        assert_eq!(attribute_to_value(&serde_json::json!({"S": "hi"})), Value::Text("hi".into()));
        assert_eq!(attribute_to_value(&serde_json::json!({"N": "42"})), Value::Int(42));
        assert_eq!(attribute_to_value(&serde_json::json!({"N": "1.5"})), Value::Float(1.5));
        assert_eq!(attribute_to_value(&serde_json::json!({"BOOL": true})), Value::Bool(true));
        assert_eq!(attribute_to_value(&serde_json::json!({"NULL": true})), Value::Null);
        assert_eq!(attribute_to_value(&serde_json::json!({"B": "AQID"})), Value::Bytes("AQID".into()));
        assert_eq!(
            attribute_to_value(&serde_json::json!({"M": {"a": {"N": "1"}, "l": {"L": [{"S": "x"}, {"BOOL": false}]}}})),
            Value::Json(serde_json::json!({"a": 1, "l": ["x", false]}))
        );
        assert_eq!(attribute_to_value(&serde_json::json!({"SS": ["a", "b"]})), Value::Json(serde_json::json!(["a", "b"])));
        assert_eq!(attribute_to_value(&serde_json::json!({"NS": ["1", "2.5"]})), Value::Json(serde_json::json!([1, 2.5])));
        assert_eq!(attribute_type_name(&serde_json::json!({"M": {}})), "map");
    }

    #[test]
    fn filter_expression_shapes() {
        let fe = filter_expression(&[
            FilterRule { column: "age".into(), op: FilterOp::Gte, value: "18".into() },
            FilterRule { column: "name".into(), op: FilterOp::StartsWith, value: "ab".into() },
            FilterRule { column: "tag".into(), op: FilterOp::In, value: "a,b".into() },
            FilterRule { column: "x".into(), op: FilterOp::IsNull, value: String::new() },
        ]);
        assert_eq!(fe.expression, "#f0 >= :v0 AND begins_with(#f1, :v1) AND #f2 IN (:v2_0, :v2_1) AND (attribute_not_exists(#f3) OR attribute_type(#f3, :null3))");
        assert_eq!(fe.names["#f0"], "age");
        assert_eq!(fe.values[":v0"], serde_json::json!({"N": "18"}));
        assert_eq!(fe.values[":v1"], serde_json::json!({"S": "ab"}));
        assert_eq!(fe.values[":v2_1"], serde_json::json!({"S": "b"}));
    }

    #[test]
    fn endpoint_and_commands() {
        assert_eq!(endpoint_for("us-east-1", None), "https://dynamodb.us-east-1.amazonaws.com");
        assert_eq!(endpoint_for("us-east-1", Some("http://localhost:8000/")), "http://localhost:8000");
        assert_eq!(endpoint_for("us-east-1", Some("localhost:8000")), "http://localhost:8000");
        assert_eq!(host_of("http://localhost:8000"), "localhost:8000");
        assert!(matches!(parse_command("TABLES"), Ok(Command::Tables)));
        assert!(matches!(parse_command("describe \"Music\""), Ok(Command::Describe(t)) if t == "Music"));
        assert!(matches!(parse_command(r#"{"Operation":"Scan","Params":{"TableName":"t"}}"#), Ok(Command::Op { op, .. }) if op == "Scan"));
        assert!(matches!(parse_command("SELECT * FROM t;"), Ok(Command::Partiql(s)) if s == "SELECT * FROM t"));
        assert!(is_write_partiql(" insert into t value {}"));
        assert!(!is_write_partiql("SELECT 1"));
        assert!(is_write_op("PutItem"));
        assert!(!is_write_op("Query"));
    }

    #[test]
    fn key_columns_partition_first() {
        let desc = serde_json::json!({
            "KeySchema": [{"AttributeName": "sk", "KeyType": "RANGE"}, {"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"}]
        });
        let keys = DynamoIntegration::key_columns(&desc);
        assert_eq!(keys, vec![("pk".to_string(), "string".to_string()), ("sk".to_string(), "number".to_string())]);
        let (names, types) = union_columns(&keys, &[serde_json::json!({"pk": {"S": "a"}, "extra": {"BOOL": true}})]);
        assert_eq!(names, vec!["pk", "sk", "extra"]);
        assert_eq!(types[2], "boolean");
    }
}
