// SOT: dynamodb-integration, aws-sigv4, partiql, dynamodb-attribute-value, dynamodb-object-explorer, dynamodb-server-stats

use crate::error::{AppError, AppResult};
use crate::integrations::aws_sigv4::{sign_post, AwsCredentials, SignRequest};
use crate::integrations::http::{json_result, objects_to_result_set, HttpClient};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    CodeLanguage, ColumnInfo, ColumnMeta, Engine, FilterOp, FilterRule, ObjectAction, ObjectDetail, ObjectKind, ObjectRef,
    ObjectSummary, PageQuery, ResolvedConnection, ResultSet, SchemaCatalog, SchemaInfo, ServerStats, Stat, StatGroup,
    StatementResult, TableInfo, TableKind, TableRef, Value,
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
                method: "POST",
                host: &self.host,
                path: "/",
                query: "",
                amz_target: Some(&target),
                content_type: Some(CONTENT_TYPE),
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

// ---------------------------------------------------------------------------
// Object explorer / administration
// ---------------------------------------------------------------------------
//
// WHAT:  Tables (`ListTables` + `DescribeTable`), their secondary indexes
//        (GSI / LSI), the tables whose stream is enabled, and backups
//        (`ListBackups`). Stats are the totals over every table plus the
//        account limits from `DescribeLimits`.
// WHY:   DynamoDB has one flat namespace per region: no schemas, users or
//        sessions to list, so the four kinds above are the whole catalog.
// HOW:   `DescribeTable` is the only source for indexes and streams, so the
//        listings describe each table once and map the reply with pure
//        functions. Index and backup references carry their table as parent.
//        `ListBackups` and `DescribeLimits` need extra IAM permissions; both
//        degrade to empty rather than failing the listing.

type Json = serde_json::Value;

const OBJECT_CAP: usize = 2_000;
const TABLE_PAGE: usize = 100;

fn jstr<'a>(v: &'a Json, key: &str) -> Option<&'a str> {
    v.get(key).and_then(Json::as_str)
}

fn jnum(v: &Json, key: &str) -> Option<f64> {
    v.get(key).and_then(Json::as_f64)
}

fn pnum(v: &Json, pointer: &str) -> Option<f64> {
    v.pointer(pointer).and_then(Json::as_f64)
}

fn items<'a>(v: &'a Json, key: &str) -> impl Iterator<Item = &'a Json> {
    v.get(key).and_then(Json::as_array).into_iter().flatten()
}

fn pretty(v: &Json) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

fn sorted(mut list: Vec<ObjectSummary>) -> Vec<ObjectSummary> {
    list.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
    list.truncate(OBJECT_CAP);
    list
}

fn bytes_text(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", value as u64)
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn epoch_text(secs: f64) -> String {
    chrono::DateTime::from_timestamp(secs as i64, 0).map(|t| t.to_rfc3339()).unwrap_or_else(|| format!("{secs}"))
}

// WHAT:  Permission-denied and unsupported-operation replies mean "nothing to
//        list here", not a failure (DynamoDB Local serves no backups or limits).
fn tolerated<T: Default>(result: AppResult<T>) -> AppResult<T> {
    match &result {
        Err(e) => {
            let m = e.message();
            let denied = ["AccessDenied", "UnrecognizedClient", "not authorized", "UnknownOperation", "InvalidAction"].iter().any(|n| m.contains(n));
            if denied || matches!(e, AppError::NotConnected { .. } | AppError::NotFound { .. }) {
                Ok(T::default())
            } else {
                result
            }
        }
        _ => result,
    }
}

// WHAT:  PROVISIONED shows its configured capacity; on-demand says so.
fn billing_text(desc: &Json) -> String {
    match desc.pointer("/BillingModeSummary/BillingMode").and_then(Json::as_str) {
        Some("PAY_PER_REQUEST") => "on-demand".to_string(),
        _ => {
            let read = pnum(desc, "/ProvisionedThroughput/ReadCapacityUnits").unwrap_or(0.0);
            let write = pnum(desc, "/ProvisionedThroughput/WriteCapacityUnits").unwrap_or(0.0);
            format!("{read} RCU / {write} WCU")
        }
    }
}

fn key_schema_text(container: &Json) -> String {
    items(container, "KeySchema")
        .filter_map(|k| {
            let name = jstr(k, "AttributeName")?;
            Some(match jstr(k, "KeyType") {
                Some("HASH") => format!("{name} (partition)"),
                Some("RANGE") => format!("{name} (sort)"),
                _ => name.to_string(),
            })
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn projection_text(index: &Json) -> String {
    match index.pointer("/Projection/ProjectionType").and_then(Json::as_str) {
        Some("INCLUDE") => {
            let cols: Vec<&str> = index.pointer("/Projection/NonKeyAttributes").and_then(Json::as_array).into_iter().flatten().filter_map(Json::as_str).collect();
            format!("INCLUDE ({})", cols.join(", "))
        }
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

// ---- listings ---------------------------------------------------------------

// WHAT:  `DescribeTable` payloads → table rows.
fn table_summaries(descriptions: &[Json]) -> Vec<ObjectSummary> {
    let list = descriptions
        .iter()
        .filter_map(|desc| {
            let name = jstr(desc, "TableName")?;
            let mut parts: Vec<String> = Vec::new();
            if let Some(n) = jnum(desc, "ItemCount") {
                parts.push(format!("{} items", crate::model::objects::format_number(n)));
            }
            if let Some(size) = jnum(desc, "TableSizeBytes") {
                parts.push(bytes_text(size));
            }
            parts.push(billing_text(desc));
            let mut s = ObjectSummary::new(ObjectKind::Table, name, None).with_detail(parts.join(" · "));
            if let Some(status) = jstr(desc, "TableStatus") {
                s = s.with_badge(status.to_lowercase());
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

// WHAT:  GSIs and LSIs of one described table; parent = the table.
fn index_summaries(desc: &Json) -> Vec<ObjectSummary> {
    let Some(table) = jstr(desc, "TableName") else { return Vec::new() };
    let mut list = Vec::new();
    // Global and local indexes are different objects in DynamoDB, so they stay
    // grouped (GSI first) and are sorted by name inside each group rather than
    // interleaved by one flat sort.
    for (key, badge) in [("GlobalSecondaryIndexes", "gsi"), ("LocalSecondaryIndexes", "lsi")] {
        let mut group = Vec::new();
        for index in items(desc, key) {
            let Some(name) = jstr(index, "IndexName") else { continue };
            let mut parts = vec![key_schema_text(index)];
            let projection = projection_text(index);
            if !projection.is_empty() {
                parts.push(projection);
            }
            if let Some(n) = jnum(index, "ItemCount") {
                parts.push(format!("{} items", crate::model::objects::format_number(n)));
            }
            let status = jstr(index, "IndexStatus").filter(|s| *s != "ACTIVE");
            let mut s = ObjectSummary::new(ObjectKind::Index, name, Some(table.to_string())).with_detail(parts.join(" · "));
            s = match status {
                Some(state) => s.with_badge(state.to_lowercase()),
                None => s.with_badge(badge),
            };
            group.push(s);
        }
        group.sort_by(|a, b| a.reference.name.cmp(&b.reference.name));
        list.extend(group);
    }
    list.truncate(OBJECT_CAP);
    list
}

// WHAT:  Tables whose `StreamSpecification` is enabled; name = the table.
fn stream_summaries(descriptions: &[Json]) -> Vec<ObjectSummary> {
    let list = descriptions
        .iter()
        .filter(|d| d.pointer("/StreamSpecification/StreamEnabled").and_then(Json::as_bool) == Some(true))
        .filter_map(|desc| {
            let name = jstr(desc, "TableName")?;
            let mut s = ObjectSummary::new(ObjectKind::Stream, name, None);
            if let Some(arn) = jstr(desc, "LatestStreamArn") {
                s = s.with_detail(arn);
            }
            if let Some(view) = desc.pointer("/StreamSpecification/StreamViewType").and_then(Json::as_str) {
                s = s.with_badge(view.to_lowercase().replace('_', " "));
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

// WHAT:  `ListBackups` → backups; parent = the table they belong to.
fn backup_summaries(reply: &Json, table: Option<&str>) -> Vec<ObjectSummary> {
    let list = items(reply, "BackupSummaries")
        .filter_map(|b| {
            let name = jstr(b, "BackupName")?;
            let owner = jstr(b, "TableName")?;
            if table.is_some_and(|t| t != owner) {
                return None;
            }
            let mut parts: Vec<String> = Vec::new();
            if let Some(t) = jstr(b, "BackupCreationDateTime").map(str::to_string).or_else(|| jnum(b, "BackupCreationDateTime").map(epoch_text)) {
                parts.push(t);
            }
            if let Some(size) = jnum(b, "BackupSizeBytes") {
                parts.push(bytes_text(size));
            }
            if let Some(kind) = jstr(b, "BackupType") {
                parts.push(kind.to_lowercase());
            }
            let mut s = ObjectSummary::new(ObjectKind::Backup, name, Some(owner.to_string())).with_detail(parts.join(" · "));
            if let Some(status) = jstr(b, "BackupStatus") {
                s = s.with_badge(status.to_lowercase());
            }
            Some(s)
        })
        .collect();
    sorted(list)
}

// ---- details ----------------------------------------------------------------

// WHAT:  Table sheet: DescribeTable as the definition, attribute definitions as
//        columns, key schema as rows, indexes (and backups) as children.
fn table_detail(reference: &ObjectRef, desc: &Json, backups: Vec<ObjectSummary>) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(desc), CodeLanguage::Json);
    for (label, key) in [("Status", "TableStatus"), ("ARN", "TableArn"), ("Id", "TableId")] {
        if let Some(v) = jstr(desc, key) {
            d = d.property(label, v);
        }
    }
    if let Some(n) = jnum(desc, "ItemCount") {
        d = d.property("Items", crate::model::objects::format_number(n));
    }
    if let Some(size) = jnum(desc, "TableSizeBytes") {
        d = d.property("Size", bytes_text(size));
    }
    d = d.property("Billing", billing_text(desc)).property("Key schema", key_schema_text(desc));
    if let Some(created) = jnum(desc, "CreationDateTime") {
        d = d.property("Created", epoch_text(created));
    }
    if let Some(class) = desc.pointer("/TableClassSummary/TableClass").and_then(Json::as_str) {
        d = d.property("Table class", class);
    }
    if desc.pointer("/StreamSpecification/StreamEnabled").and_then(Json::as_bool) == Some(true) {
        d = d.property("Stream", desc.pointer("/StreamSpecification/StreamViewType").and_then(Json::as_str).unwrap_or("enabled"));
    }
    if let Some(sse) = desc.pointer("/SSEDescription/Status").and_then(Json::as_str) {
        d = d.property("Encryption", sse);
    }
    d.columns = items(desc, "AttributeDefinitions")
        .enumerate()
        .filter_map(|(i, a)| {
            let name = jstr(a, "AttributeName")?.to_string();
            let key = items(desc, "KeySchema").any(|k| jstr(k, "AttributeName") == Some(name.as_str()));
            Some(ColumnInfo {
                data_type: key_type_name(jstr(a, "AttributeType").unwrap_or_default()).to_string(),
                nullable: !key,
                primary_key: key,
                name,
                ordinal: i as u32 + 1,
            })
        })
        .collect();
    let key_rows: Vec<Json> = items(desc, "KeySchema")
        .map(|k| {
            let name = jstr(k, "AttributeName").unwrap_or_default();
            let attr_type = items(desc, "AttributeDefinitions").find(|a| jstr(a, "AttributeName") == Some(name)).and_then(|a| jstr(a, "AttributeType")).unwrap_or_default();
            serde_json::json!({"attribute": name, "keyType": jstr(k, "KeyType").unwrap_or_default(), "type": key_type_name(attr_type)})
        })
        .collect();
    if !key_rows.is_empty() {
        d.rows = Some(objects_to_result_set(&key_rows, Some("attribute"), OBJECT_CAP));
    }
    d.children = index_summaries(desc).into_iter().chain(backups).collect();
    d
}

fn index_detail(reference: &ObjectRef, desc: &Json, index: &Json, kind: &str) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference)
        .definition(pretty(index), CodeLanguage::Json)
        .property("Type", kind)
        .property("Table", jstr(desc, "TableName").unwrap_or_default())
        .property("Key schema", key_schema_text(index))
        .property("Projection", projection_text(index));
    for (label, key) in [("Status", "IndexStatus"), ("ARN", "IndexArn")] {
        if let Some(v) = jstr(index, key) {
            d = d.property(label, v);
        }
    }
    if let Some(n) = jnum(index, "ItemCount") {
        d = d.property("Items", crate::model::objects::format_number(n));
    }
    if let Some(size) = jnum(index, "IndexSizeBytes") {
        d = d.property("Size", bytes_text(size));
    }
    if index.get("ProvisionedThroughput").is_some() {
        let read = pnum(index, "/ProvisionedThroughput/ReadCapacityUnits").unwrap_or(0.0);
        let write = pnum(index, "/ProvisionedThroughput/WriteCapacityUnits").unwrap_or(0.0);
        d = d.property("Throughput", format!("{read} RCU / {write} WCU"));
    }
    let key_rows: Vec<Json> = items(index, "KeySchema")
        .map(|k| serde_json::json!({"attribute": jstr(k, "AttributeName").unwrap_or_default(), "keyType": jstr(k, "KeyType").unwrap_or_default()}))
        .collect();
    if !key_rows.is_empty() {
        d.rows = Some(objects_to_result_set(&key_rows, Some("attribute"), OBJECT_CAP));
    }
    // Only a GSI can be dropped on its own; an LSI lives and dies with its table.
    if kind == "gsi" {
        let statement = serde_json::json!({
            "Operation": "UpdateTable",
            "Params": {"TableName": jstr(desc, "TableName").unwrap_or_default(), "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": reference.name}}]}
        });
        d = d.action(ObjectAction::destructive("delete", "Delete index", statement.to_string()));
    }
    d
}

fn stream_detail(reference: &ObjectRef, desc: &Json) -> ObjectDetail {
    let definition = serde_json::json!({
        "StreamSpecification": desc.get("StreamSpecification").cloned().unwrap_or(Json::Null),
        "LatestStreamArn": desc.get("LatestStreamArn").cloned().unwrap_or(Json::Null),
        "LatestStreamLabel": desc.get("LatestStreamLabel").cloned().unwrap_or(Json::Null)
    });
    let mut d = ObjectDetail::empty(reference).definition(pretty(&definition), CodeLanguage::Json).property("Table", jstr(desc, "TableName").unwrap_or_default());
    if let Some(view) = desc.pointer("/StreamSpecification/StreamViewType").and_then(Json::as_str) {
        d = d.property("View type", view);
    }
    for (label, key) in [("Stream ARN", "LatestStreamArn"), ("Stream label", "LatestStreamLabel")] {
        if let Some(v) = jstr(desc, key) {
            d = d.property(label, v);
        }
    }
    let statement = serde_json::json!({"Operation": "UpdateTable", "Params": {"TableName": jstr(desc, "TableName").unwrap_or_default(), "StreamSpecification": {"StreamEnabled": false}}});
    d.action(ObjectAction::destructive("disable", "Disable stream", statement.to_string()))
}

fn backup_detail(reference: &ObjectRef, backup: &Json) -> ObjectDetail {
    let mut d = ObjectDetail::empty(reference).definition(pretty(backup), CodeLanguage::Json);
    let details = backup.get("BackupDetails").unwrap_or(backup);
    let table = backup.pointer("/SourceTableDetails/TableName").and_then(Json::as_str).or_else(|| jstr(backup, "TableName")).unwrap_or_default();
    d = d.property("Table", table);
    for (label, key) in [("Status", "BackupStatus"), ("Type", "BackupType"), ("ARN", "BackupArn")] {
        if let Some(v) = jstr(details, key) {
            d = d.property(label, v);
        }
    }
    if let Some(size) = jnum(details, "BackupSizeBytes") {
        d = d.property("Size", bytes_text(size));
    }
    if let Some(created) = jstr(details, "BackupCreationDateTime").map(str::to_string).or_else(|| jnum(details, "BackupCreationDateTime").map(epoch_text)) {
        d = d.property("Created", created);
    }
    if let Some(arn) = jstr(details, "BackupArn") {
        let statement = serde_json::json!({"Operation": "DeleteBackup", "Params": {"BackupArn": arn}});
        d = d.action(ObjectAction::destructive("delete", "Delete backup", statement.to_string()));
    }
    d
}

// ---- server stats -------------------------------------------------------------

fn push_group(groups: &mut Vec<StatGroup>, title: &str, stats: Vec<Stat>) {
    if !stats.is_empty() {
        groups.push(StatGroup { title: title.to_string(), stats });
    }
}

// WHAT:  Totals over the described tables plus the account limits.
fn server_stat_groups(region: &str, endpoint: &str, descriptions: &[Json], limits: &Json) -> Vec<StatGroup> {
    let mut groups = Vec::new();
    let mut server = vec![Stat::text("Region", region), Stat::text("Endpoint", endpoint), Stat::number("Tables", descriptions.len() as f64, None)];
    if !descriptions.is_empty() {
        let active = descriptions.iter().filter(|d| jstr(d, "TableStatus") == Some("ACTIVE")).count();
        server.push(Stat::number("Active tables", active as f64, None));
    }
    push_group(&mut groups, "Server", server);

    let mut storage = Vec::new();
    if !descriptions.is_empty() {
        let items_total: f64 = descriptions.iter().filter_map(|d| jnum(d, "ItemCount")).sum();
        let bytes_total: f64 = descriptions.iter().filter_map(|d| jnum(d, "TableSizeBytes")).sum();
        let indexes: usize = descriptions.iter().map(|d| items(d, "GlobalSecondaryIndexes").count() + items(d, "LocalSecondaryIndexes").count()).sum();
        let streams = descriptions.iter().filter(|d| d.pointer("/StreamSpecification/StreamEnabled").and_then(Json::as_bool) == Some(true)).count();
        storage.push(Stat::number("Items", items_total, None));
        storage.push(Stat::number("Size", (bytes_total / 1_048_576.0 * 100.0).round() / 100.0, Some("MB")).with_hint(bytes_text(bytes_total)));
        storage.push(Stat::number("Secondary indexes", indexes as f64, None));
        storage.push(Stat::number("Streams enabled", streams as f64, None));
    }
    push_group(&mut groups, "Storage", storage);

    let mut capacity = Vec::new();
    if !descriptions.is_empty() {
        let on_demand = descriptions.iter().filter(|d| d.pointer("/BillingModeSummary/BillingMode").and_then(Json::as_str) == Some("PAY_PER_REQUEST")).count();
        let read: f64 = descriptions.iter().filter_map(|d| pnum(d, "/ProvisionedThroughput/ReadCapacityUnits")).sum();
        let write: f64 = descriptions.iter().filter_map(|d| pnum(d, "/ProvisionedThroughput/WriteCapacityUnits")).sum();
        capacity.push(Stat::number("On-demand tables", on_demand as f64, None));
        capacity.push(Stat::number("Provisioned read", read, Some("RCU")));
        capacity.push(Stat::number("Provisioned write", write, Some("WCU")));
    }
    for (label, key, unit) in [
        ("Account read limit", "AccountMaxReadCapacityUnits", "RCU"),
        ("Account write limit", "AccountMaxWriteCapacityUnits", "WCU"),
        ("Table read limit", "TableMaxReadCapacityUnits", "RCU"),
        ("Table write limit", "TableMaxWriteCapacityUnits", "WCU"),
    ] {
        if let Some(n) = jnum(limits, key) {
            capacity.push(Stat::number(label, n, Some(unit)));
        }
    }
    push_group(&mut groups, "Capacity", capacity);
    groups
}

impl DynamoIntegration {
    // WHAT:  Every table name in the region (paged); shared by catalog + explorer.
    async fn table_names(&self) -> AppResult<Vec<String>> {
        let mut names = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let mut body = serde_json::json!({"Limit": TABLE_PAGE});
            if let Some(s) = &start {
                body["ExclusiveStartTableName"] = Json::String(s.clone());
            }
            let resp = self.call("ListTables", &body).await?;
            for t in items(&resp, "TableNames") {
                if let Some(n) = t.as_str() {
                    names.push(n.to_string());
                }
            }
            match jstr(&resp, "LastEvaluatedTableName") {
                Some(s) if names.len() < 5_000 => start = Some(s.to_string()),
                _ => break,
            }
        }
        Ok(names)
    }

    // WHAT:  `DescribeTable` for every table, capped at the listing cap.
    async fn describe_all(&self) -> AppResult<Vec<Json>> {
        let names = self.table_names().await?;
        let mut out = Vec::with_capacity(names.len().min(OBJECT_CAP));
        for name in names.into_iter().take(OBJECT_CAP) {
            out.push(self.describe(&name).await?);
        }
        Ok(out)
    }

    async fn backups(&self, table: Option<&str>) -> AppResult<Json> {
        let mut body = serde_json::json!({"Limit": TABLE_PAGE});
        if let Some(t) = table {
            body["TableName"] = Json::String(t.to_string());
        }
        tolerated(self.call("ListBackups", &body).await)
    }

    // WHAT:  One index of a described table, with the family it belongs to.
    fn find_index<'a>(desc: &'a Json, name: &str) -> Option<(&'a Json, &'static str)> {
        for (key, kind) in [("GlobalSecondaryIndexes", "gsi"), ("LocalSecondaryIndexes", "lsi")] {
            if let Some(index) = items(desc, key).find(|i| jstr(i, "IndexName") == Some(name)) {
                return Some((index, kind));
            }
        }
        None
    }
}

// WHAT:  What this family offers the object explorer and the tool tabs.
// WHY:   Declared here, next to the adapter that must answer `objects()` for
//        every kind listed; rendered by the capability matrix for every engine.
// WHERE: src-tauri/src/integrations/mod.rs (FamilyProfile), src/lib/objects.ts
pub fn profile() -> crate::integrations::FamilyProfile {
    use crate::model::{ObjectKind as K, Tool as T};
    crate::integrations::FamilyProfile {
        capabilities: Capabilities { namespaces: false, ..Capabilities::DOCUMENT },
        object_kinds: vec![K::Table, K::Index, K::Stream, K::Backup],
        tools: vec![T::Stats],
    }
}

#[async_trait]
impl Integration for DynamoIntegration {
    fn engine(&self) -> Engine {
        self.engine
    }

    fn capabilities(&self) -> Capabilities {
        profile().capabilities
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
        let names = self.table_names().await?;
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

    async fn objects(&self, kind: ObjectKind, parent: Option<&str>) -> AppResult<Vec<ObjectSummary>> {
        let owner = parent.map(str::trim).filter(|p| !p.is_empty() && *p != "tables");
        match kind {
            ObjectKind::Table => Ok(table_summaries(&self.describe_all().await?)),
            ObjectKind::Index => match owner {
                Some(table) => Ok(index_summaries(&self.describe(table).await?)),
                None => {
                    let mut all = Vec::new();
                    for desc in self.describe_all().await? {
                        all.extend(index_summaries(&desc));
                        if all.len() >= OBJECT_CAP {
                            break;
                        }
                    }
                    Ok(sorted(all))
                }
            },
            ObjectKind::Stream => match owner {
                Some(table) => Ok(stream_summaries(std::slice::from_ref(&self.describe(table).await?))),
                None => Ok(stream_summaries(&self.describe_all().await?)),
            },
            ObjectKind::Backup => Ok(backup_summaries(&self.backups(owner).await?, owner)),
            _ => Ok(Vec::new()),
        }
    }

    async fn object_detail(&self, reference: &ObjectRef) -> AppResult<ObjectDetail> {
        let name = reference.name.as_str();
        let owner = reference.parent.as_deref().map(str::trim).filter(|p| !p.is_empty() && *p != "tables");
        match reference.kind {
            ObjectKind::Table => {
                let desc = self.describe(name).await?;
                let backups = backup_summaries(&self.backups(Some(name)).await?, Some(name));
                Ok(table_detail(reference, &desc, backups))
            }
            ObjectKind::Index => {
                let table = owner.ok_or_else(|| AppError::invalid_input("An index reference needs its table as parent."))?;
                let desc = self.describe(table).await?;
                let (index, kind) = Self::find_index(&desc, name).ok_or_else(|| AppError::not_found(format!("Index {name} not found on {table}.")))?;
                Ok(index_detail(reference, &desc, index, kind))
            }
            ObjectKind::Stream => {
                let desc = self.describe(owner.unwrap_or(name)).await?;
                Ok(stream_detail(reference, &desc))
            }
            ObjectKind::Backup => {
                let listing = self.backups(owner).await?;
                let summary = items(&listing, "BackupSummaries")
                    .find(|b| jstr(b, "BackupName") == Some(name))
                    .ok_or_else(|| AppError::not_found(format!("Backup {name} not found.")))?;
                // The summary carries the ARN; DescribeBackup adds the source table details.
                let described = match jstr(summary, "BackupArn") {
                    Some(arn) => tolerated(self.call("DescribeBackup", &serde_json::json!({"BackupArn": arn})).await)?.get("BackupDescription").cloned(),
                    None => None,
                };
                Ok(backup_detail(reference, described.as_ref().unwrap_or(summary)))
            }
            _ => Ok(ObjectDetail::empty(reference)),
        }
    }

    async fn server_stats(&self) -> AppResult<ServerStats> {
        let descriptions = self.describe_all().await?;
        let limits = tolerated(self.call("DescribeLimits", &serde_json::json!({})).await)?;
        Ok(ServerStats::now(server_stat_groups(&self.creds.region, &self.endpoint, &descriptions, &limits)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConnectionSummary, Environment, SslMode};

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

    // WHAT:  A DescribeTable payload with both index families and a stream.
    fn described() -> serde_json::Value {
        serde_json::json!({
            "TableName": "Orders",
            "TableStatus": "ACTIVE",
            "TableArn": "arn:aws:dynamodb:us-east-1:1:table/Orders",
            "ItemCount": 1500,
            "TableSizeBytes": 2_097_152,
            "CreationDateTime": 1_700_000_000.0,
            "BillingModeSummary": {"BillingMode": "PAY_PER_REQUEST"},
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"}, {"AttributeName": "userId", "AttributeType": "S"}],
            "GlobalSecondaryIndexes": [{
                "IndexName": "byUser", "IndexStatus": "ACTIVE", "ItemCount": 1500, "IndexSizeBytes": 1024,
                "KeySchema": [{"AttributeName": "userId", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["total", "status"]}
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": "bySk", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
            "LatestStreamArn": "arn:aws:dynamodb:us-east-1:1:table/Orders/stream/2024"
        })
    }

    #[test]
    fn tables_map_with_status_and_billing() {
        let provisioned = serde_json::json!({
            "TableName": "Legacy", "TableStatus": "CREATING", "ItemCount": 3, "TableSizeBytes": 512,
            "ProvisionedThroughput": {"ReadCapacityUnits": 5.0, "WriteCapacityUnits": 1.0}
        });
        let tables = table_summaries(&[described(), provisioned.clone()]);
        assert_eq!(tables[0].reference.name, "Legacy");
        assert_eq!(tables[0].badge.as_deref(), Some("creating"));
        assert_eq!(tables[0].detail.as_deref(), Some("3 items · 512 B · 5 RCU / 1 WCU"));
        assert_eq!(tables[1].badge.as_deref(), Some("active"));
        assert_eq!(tables[1].detail.as_deref(), Some("1,500 items · 2.0 MB · on-demand"));
        assert!(tables[1].reference.parent.is_none(), "DynamoDB has one flat namespace");

        let r = ObjectRef { kind: ObjectKind::Table, name: "Orders".into(), parent: None };
        let d = table_detail(&r, &described(), vec![ObjectSummary::new(ObjectKind::Backup, "b1", Some("Orders".into()))]);
        assert_eq!(d.language, CodeLanguage::Json);
        assert!(d.properties.iter().any(|p| p.name == "Items" && p.value == "1,500"));
        assert!(d.properties.iter().any(|p| p.name == "Size" && p.value == "2.0 MB"));
        assert!(d.properties.iter().any(|p| p.name == "Key schema" && p.value == "pk (partition), sk (sort)"));
        assert!(d.properties.iter().any(|p| p.name == "Stream" && p.value == "NEW_AND_OLD_IMAGES"));
        assert!(d.properties.iter().any(|p| p.name == "Created" && p.value.starts_with("2023-11-14")));
        // Attribute definitions become columns; only the key attributes are primary.
        let cols: Vec<(&str, bool)> = d.columns.iter().map(|c| (c.name.as_str(), c.primary_key)).collect();
        assert_eq!(cols, vec![("pk", true), ("sk", true), ("userId", false)]);
        assert_eq!(d.columns[1].data_type, "number");
        assert_eq!(d.rows.as_ref().map(|r| r.rows.len()), Some(2), "key schema is the tabular payload");
        // Children are the two indexes plus the backup passed in.
        assert_eq!(d.children.len(), 3);
        assert!(d.actions.is_empty(), "dropping a table is not offered from the sheet");
    }

    #[test]
    fn indexes_streams_and_backups_map() {
        let desc = described();
        let idx = index_summaries(&desc);
        assert_eq!(idx.len(), 2);
        assert_eq!(idx[0].reference.name, "byUser");
        assert_eq!(idx[0].badge.as_deref(), Some("gsi"));
        assert_eq!(idx[0].reference.parent.as_deref(), Some("Orders"));
        assert_eq!(idx[0].detail.as_deref(), Some("userId (partition) · INCLUDE (total, status) · 1,500 items"));
        assert_eq!(idx[1].badge.as_deref(), Some("lsi"));
        assert_eq!(idx[1].detail.as_deref(), Some("pk (partition), sk (sort) · ALL"));

        let (gsi, kind) = DynamoIntegration::find_index(&desc, "byUser").unwrap_or((&serde_json::Value::Null, "?"));
        assert_eq!(kind, "gsi");
        let r = ObjectRef { kind: ObjectKind::Index, name: "byUser".into(), parent: Some("Orders".into()) };
        let d = index_detail(&r, &desc, gsi, kind);
        assert!(d.properties.iter().any(|p| p.name == "Projection" && p.value == "INCLUDE (total, status)"));
        assert_eq!(d.actions.len(), 1);
        assert!(d.actions[0].destructive);
        assert_eq!(
            d.actions[0].statement,
            r#"{"Operation":"UpdateTable","Params":{"TableName":"Orders","GlobalSecondaryIndexUpdates":[{"Delete":{"IndexName":"byUser"}}]}}"#
        );
        let (lsi, kind) = DynamoIntegration::find_index(&desc, "bySk").unwrap_or((&serde_json::Value::Null, "?"));
        assert!(index_detail(&r, &desc, lsi, kind).actions.is_empty(), "an LSI cannot be dropped on its own");
        assert!(DynamoIntegration::find_index(&desc, "nope").is_none());

        let no_stream = serde_json::json!({"TableName": "Plain", "StreamSpecification": {"StreamEnabled": false}});
        let streams = stream_summaries(&[desc.clone(), no_stream]);
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].reference.name, "Orders");
        assert_eq!(streams[0].badge.as_deref(), Some("new and old images"));
        let sr = ObjectRef { kind: ObjectKind::Stream, name: "Orders".into(), parent: None };
        let sd = stream_detail(&sr, &desc);
        assert!(sd.properties.iter().any(|p| p.name == "View type" && p.value == "NEW_AND_OLD_IMAGES"));
        assert_eq!(sd.actions[0].statement, r#"{"Operation":"UpdateTable","Params":{"TableName":"Orders","StreamSpecification":{"StreamEnabled":false}}}"#);

        let listing = serde_json::json!({"BackupSummaries": [
            {"BackupName": "nightly", "TableName": "Orders", "BackupStatus": "AVAILABLE", "BackupType": "USER", "BackupSizeBytes": 4096, "BackupCreationDateTime": 1_700_000_000.0, "BackupArn": "arn:backup/1"},
            {"BackupName": "other", "TableName": "Elsewhere", "BackupStatus": "AVAILABLE", "BackupArn": "arn:backup/2"}
        ]});
        let all = backup_summaries(&listing, None);
        assert_eq!(all.len(), 2);
        let scoped = backup_summaries(&listing, Some("Orders"));
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].reference.parent.as_deref(), Some("Orders"));
        assert_eq!(scoped[0].badge.as_deref(), Some("available"));
        assert!(scoped[0].detail.as_deref().unwrap_or_default().contains("4.0 KB"));
        let br = ObjectRef { kind: ObjectKind::Backup, name: "nightly".into(), parent: Some("Orders".into()) };
        let bd = backup_detail(&br, &listing["BackupSummaries"][0]);
        assert!(bd.properties.iter().any(|p| p.name == "Table" && p.value == "Orders"));
        assert_eq!(bd.actions[0].statement, r#"{"Operation":"DeleteBackup","Params":{"BackupArn":"arn:backup/1"}}"#);
    }

    #[test]
    fn server_stats_total_over_tables() {
        let provisioned = serde_json::json!({
            "TableName": "Legacy", "TableStatus": "ACTIVE", "ItemCount": 500, "TableSizeBytes": 1_048_576,
            "ProvisionedThroughput": {"ReadCapacityUnits": 5.0, "WriteCapacityUnits": 2.0}
        });
        let limits = serde_json::json!({"AccountMaxReadCapacityUnits": 40000.0, "AccountMaxWriteCapacityUnits": 40000.0, "TableMaxReadCapacityUnits": 10000.0});
        let groups = server_stat_groups("us-east-1", "https://dynamodb.us-east-1.amazonaws.com", &[described(), provisioned], &limits);
        let titles: Vec<&str> = groups.iter().map(|g| g.title.as_str()).collect();
        assert_eq!(titles, vec!["Server", "Storage", "Capacity"]);
        let find = |group: &str, label: &str| groups.iter().find(|g| g.title == group).and_then(|g| g.stats.iter().find(|s| s.label == label).cloned());
        assert_eq!(find("Server", "Region").map(|s| s.value), Some("us-east-1".into()));
        assert_eq!(find("Server", "Tables").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Items").and_then(|s| s.numeric), Some(2000.0));
        assert_eq!(find("Storage", "Size").and_then(|s| s.numeric), Some(3.0));
        assert_eq!(find("Storage", "Size").and_then(|s| s.hint), Some("3.0 MB".into()));
        assert_eq!(find("Storage", "Secondary indexes").and_then(|s| s.numeric), Some(2.0));
        assert_eq!(find("Storage", "Streams enabled").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Capacity", "On-demand tables").and_then(|s| s.numeric), Some(1.0));
        assert_eq!(find("Capacity", "Provisioned read").and_then(|s| s.numeric), Some(5.0));
        assert_eq!(find("Capacity", "Account read limit").map(|s| s.unit), Some(Some("RCU".into())));
        assert!(find("Capacity", "Table write limit").is_none(), "absent limits are skipped");
        // An empty region still reports where it is pointed.
        let empty = server_stat_groups("eu-west-1", "http://localhost:8000", &[], &serde_json::json!({}));
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].title, "Server");
    }

    #[test]
    fn denied_optional_calls_degrade_to_empty() {
        assert!(matches!(tolerated::<serde_json::Value>(Err(AppError::driver("AccessDeniedException: user is not authorized"))), Ok(v) if v.is_null()));
        assert!(matches!(tolerated::<serde_json::Value>(Err(AppError::driver("UnknownOperationException"))), Ok(v) if v.is_null()));
        assert!(matches!(tolerated::<serde_json::Value>(Err(AppError::not_connected("403"))), Ok(v) if v.is_null()));
        assert!(tolerated::<serde_json::Value>(Err(AppError::driver("ProvisionedThroughputExceeded"))).is_err(), "real failures still surface");
        assert!(tolerated::<serde_json::Value>(Err(AppError::timeout("slow"))).is_err());
        assert_eq!(bytes_text(1536.0), "1.5 KB");
        assert_eq!(bytes_text(42.0), "42 B");
        assert!(epoch_text(0.0).starts_with("1970-01-01"));
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

    // Runs only when DBFREE_TEST_DYNAMODB_ENDPOINT is set:
    // `docker run --rm -d -p 8000:8000 amazon/dynamodb-local`.
    #[tokio::test]
    async fn live_round_trip_when_configured() {
        let Ok(endpoint) = std::env::var("DBFREE_TEST_DYNAMODB_ENDPOINT") else {
            return;
        };
        let resolved = ResolvedConnection {
            summary: ConnectionSummary {
                id: "t".into(),
                name: "t".into(),
                engine: Engine::Dynamodb,
                environment: Environment::Local,
                read_only: false,
                host: Some(std::env::var("DBFREE_TEST_DYNAMODB_REGION").unwrap_or_else(|_| "us-east-1".into())),
                port: None,
                database: Some(endpoint),
                username: Some(std::env::var("DBFREE_TEST_DYNAMODB_KEY").unwrap_or_else(|_| "dummy".into())),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: Some(std::env::var("DBFREE_TEST_DYNAMODB_SECRET").unwrap_or_else(|_| "dummy".into())),
        };
        let db = connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
        let _ = db
            .execute(r#"{"Operation":"DeleteTable","Params":{"TableName":"dbfree_test"}}"#, 10)
            .await;
        db.execute(
            r#"{"Operation":"CreateTable","Params":{"TableName":"dbfree_test","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}}"#,
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("create: {e}"));
        db.execute(
            r#"{"Operation":"PutItem","Params":{"TableName":"dbfree_test","Item":{"pk":{"S":"a"},"city":{"S":"Berlin"},"n":{"N":"1"}}}}"#,
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("put a: {e}"));
        db.execute(
            r#"{"Operation":"PutItem","Params":{"TableName":"dbfree_test","Item":{"pk":{"S":"b"},"city":{"S":"Paris"},"n":{"N":"2"}}}}"#,
            10,
        )
        .await
        .unwrap_or_else(|e| panic!("put b: {e}"));

        let catalog = db.catalog().await.unwrap_or_else(|e| panic!("catalog: {e}"));
        assert!(catalog.schemas.iter().any(|s| s.tables.iter().any(|t| t.name == "dbfree_test")), "{catalog:?}");
        let table = TableRef { schema: None, name: "dbfree_test".into() };
        let cols = db.columns(&table).await.unwrap_or_else(|e| panic!("columns: {e}"));
        assert!(cols.first().map(|c| c.name == "pk" && c.primary_key).unwrap_or(false), "{cols:?}");
        assert!(cols.iter().any(|c| c.name == "city"), "{cols:?}");
        let page = db
            .fetch_page(&table, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 })
            .await
            .unwrap_or_else(|e| panic!("page: {e}"));
        assert_eq!(page.rows.len(), 2, "{page:?}");
        let filters = vec![FilterRule { column: "city".into(), op: FilterOp::Eq, value: "Paris".into() }];
        assert_eq!(db.count(&table, &filters).await.unwrap_or_default(), 1);
        let rows = db
            .execute("SELECT * FROM \"dbfree_test\" WHERE pk = 'a'", 10)
            .await
            .unwrap_or_else(|e| panic!("partiql: {e}"));
        match rows.first() {
            Some(StatementResult::Rows { result }) => assert_eq!(result.rows.len(), 1, "{result:?}"),
            other => panic!("expected rows, got {other:?}"),
        }
        let _ = db.execute(r#"{"Operation":"DeleteTable","Params":{"TableName":"dbfree_test"}}"#, 10).await;
        db.close().await;
    }

}
