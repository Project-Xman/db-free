// SOT: mssql-integration, mssql-adapter, tiberius-driver, mssql-catalog-queries

use crate::error::{AppError, AppResult};
use crate::guard::destructive::split_statements;
use crate::integrations::sql::{order_clause, validate_columns, where_clause};
use crate::integrations::{Capabilities, Integration};
use crate::model::{
    ColumnInfo, ColumnMeta, Engine, FilterRule, ForeignKey, PageQuery, ResolvedConnection,
    ResultSet, SchemaCatalog, SchemaInfo, SslMode, StatementResult, TableInfo, TableKind, TableRef,
    Value,
};
use async_trait::async_trait;
use base64::Engine as _;
use std::sync::Arc;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

impl From<tiberius::error::Error> for AppError {
    fn from(err: tiberius::error::Error) -> Self {
        AppError::driver(err.to_string())
    }
}

const DEFAULT_PORT: u16 = 1433;
const DEFAULT_DATABASE: &str = "master";
const DEFAULT_USER: &str = "sa";

pub struct MssqlIntegration {
    client: Arc<Mutex<Client<Compat<TcpStream>>>>,
    database: String,
}

pub fn quote_ident(raw: &str) -> String {
    format!("[{}]", raw.replace(']', "]]"))
}

pub fn qualified_name(table: &TableRef) -> String {
    match &table.schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
        None => quote_ident(&table.name),
    }
}

pub async fn connect(conn: &ResolvedConnection) -> AppResult<Arc<dyn Integration>> {
    let s = &conn.summary;
    let host = s.host.as_deref().map(str::trim).filter(|h| !h.is_empty()).unwrap_or("localhost");
    let port = s.port.unwrap_or(DEFAULT_PORT);
    let database = s
        .database
        .as_deref()
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string();
    let user = s
        .username
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .unwrap_or(DEFAULT_USER);
    let password = conn.secret.as_deref().unwrap_or_default();

    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(&database);
    config.authentication(AuthMethod::sql_server(user, password));

    match s.ssl_mode {
        SslMode::Disable => config.encryption(EncryptionLevel::NotSupported),
        SslMode::Prefer => {
            config.encryption(EncryptionLevel::Off);
            config.trust_cert();
        }
        SslMode::Require => {
            config.encryption(EncryptionLevel::Required);
            config.trust_cert();
        }
        SslMode::VerifyCa | SslMode::VerifyFull => {
            config.encryption(EncryptionLevel::Required);
        }
    }

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|e| AppError::driver(format!("Could not reach SQL Server at {host}:{port}: {e}")))?;
    let _ = tcp.set_nodelay(true);

    let client = Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| AppError::driver(format!("SQL Server connection failed: {e}")))?;

    Ok(Arc::new(MssqlIntegration {
        client: Arc::new(Mutex::new(client)),
        database,
    }))
}

fn decode_column_data(data: &ColumnData<'_>) -> Value {
    match data {
        ColumnData::U8(opt) => opt.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I16(opt) => opt.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I32(opt) => opt.map(|v| Value::Int(v as i64)).unwrap_or(Value::Null),
        ColumnData::I64(opt) => opt.map(Value::Int).unwrap_or(Value::Null),
        ColumnData::F32(opt) => opt.map(|v| Value::Float(v as f64)).unwrap_or(Value::Null),
        ColumnData::F64(opt) => opt.map(Value::Float).unwrap_or(Value::Null),
        ColumnData::Bit(opt) => opt.map(Value::Bool).unwrap_or(Value::Null),
        ColumnData::String(opt) => opt.as_deref().map(|s| Value::Text(s.to_string())).unwrap_or(Value::Null),
        ColumnData::Guid(opt) => opt.map(|g| Value::Text(g.to_string())).unwrap_or(Value::Null),
        ColumnData::Binary(opt) => opt
            .as_deref()
            .map(|b| Value::Bytes(base64::engine::general_purpose::STANDARD.encode(b)))
            .unwrap_or(Value::Null),
        ColumnData::Numeric(opt) => opt.map(|n| Value::Text(n.to_string())).unwrap_or(Value::Null),
        ColumnData::Xml(opt) => opt.as_deref().map(|s| Value::Text(s.to_string())).unwrap_or(Value::Null),
        ColumnData::DateTime(opt) => opt.map(|dt| Value::DateTime(format!("{dt:?}"))).unwrap_or(Value::Null),
        ColumnData::SmallDateTime(opt) => opt.map(|dt| Value::DateTime(format!("{dt:?}"))).unwrap_or(Value::Null),
        ColumnData::Time(opt) => opt.map(|t| Value::DateTime(format!("{t:?}"))).unwrap_or(Value::Null),
        ColumnData::Date(opt) => opt.map(|d| Value::DateTime(format!("{d:?}"))).unwrap_or(Value::Null),
        ColumnData::DateTime2(opt) => opt.map(|dt| Value::DateTime(format!("{dt:?}"))).unwrap_or(Value::Null),
        ColumnData::DateTimeOffset(opt) => opt.map(|dto| Value::DateTime(format!("{dto:?}"))).unwrap_or(Value::Null),
    }
}

#[async_trait]
impl Integration for MssqlIntegration {
    fn engine(&self) -> Engine {
        Engine::Mssql
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: true,
            namespaces: true,
            fixed_columns: true,
            paging: true,
            row_estimate: true,
            views: true,
            transactions: true,
            exact_estimate: false,
        }
    }

    async fn ping(&self) -> AppResult<()> {
        let mut client = self.client.lock().await;
        client.simple_query("SELECT 1").await.map_err(AppError::from)?;
        Ok(())
    }

    async fn server_version(&self) -> AppResult<Option<String>> {
        let mut client = self.client.lock().await;
        let stream = client
            .simple_query("SELECT @@VERSION")
            .await
            .map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;
        if let Some(row) = rows.into_iter().next() {
            if let Some(ColumnData::String(Some(v))) = row.into_iter().next() {
                return Ok(Some(v.to_string()));
            }
        }
        Ok(Some("Microsoft SQL Server".into()))
    }

    fn current_database(&self) -> Option<String> {
        Some(self.database.clone())
    }

    async fn databases(&self) -> AppResult<Vec<String>> {
        let mut client = self.client.lock().await;
        let stream = client
            .simple_query("SELECT name FROM sys.databases WHERE state = 0 ORDER BY name")
            .await
            .map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;
        let mut dbs = Vec::new();
        for row in rows {
            if let Some(ColumnData::String(Some(name))) = row.into_iter().next() {
                dbs.push(name.to_string());
            }
        }
        Ok(dbs)
    }

    async fn catalog(&self) -> AppResult<SchemaCatalog> {
        let mut client = self.client.lock().await;
        let sql = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                   FROM INFORMATION_SCHEMA.TABLES \
                   WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') \
                   ORDER BY TABLE_SCHEMA, TABLE_NAME";
        let stream = client.simple_query(sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;

        let mut schemas_map: std::collections::BTreeMap<String, Vec<TableInfo>> =
            std::collections::BTreeMap::new();
        for row in rows {
            let mut iter = row.into_iter();
            let schema = match iter.next() {
                Some(ColumnData::String(Some(s))) => s.to_string(),
                _ => continue,
            };
            let name = match iter.next() {
                Some(ColumnData::String(Some(n))) => n.to_string(),
                _ => continue,
            };
            let kind = match iter.next() {
                Some(ColumnData::String(Some(k))) if k.eq_ignore_ascii_case("VIEW") => {
                    TableKind::View
                }
                _ => TableKind::Table,
            };
            let schema_clone = schema.clone();
            schemas_map
                .entry(schema)
                .or_default()
                .push(TableInfo { schema: Some(schema_clone), name, kind, row_estimate: None });
        }

        let schemas = schemas_map
            .into_iter()
            .map(|(name, tables)| SchemaInfo { name, tables })
            .collect();
        Ok(SchemaCatalog { schemas })
    }

    async fn columns(&self, table: &TableRef) -> AppResult<Vec<ColumnInfo>> {
        let schema = table.schema.as_deref().unwrap_or("dbo");
        let name = &table.name;

        let sql = format!(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.ORDINAL_POSITION, \
             CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk \
             FROM INFORMATION_SCHEMA.COLUMNS c \
             LEFT JOIN ( \
                 SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME \
                 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
                 JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku \
                   ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME \
                  AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA \
                 WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
             ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME \
             WHERE c.TABLE_SCHEMA = '{}' AND c.TABLE_NAME = '{}' \
             ORDER BY c.ORDINAL_POSITION",
            schema.replace('\'', "''"),
            name.replace('\'', "''")
        );

        let mut client = self.client.lock().await;
        let stream = client.simple_query(&sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;

        let mut cols = Vec::new();
        for row in rows {
            let mut iter = row.into_iter();
            let col_name = match iter.next() {
                Some(ColumnData::String(Some(n))) => n.to_string(),
                _ => continue,
            };
            let data_type = match iter.next() {
                Some(ColumnData::String(Some(t))) => t.to_string(),
                _ => String::from("unknown"),
            };
            let nullable = match iter.next() {
                Some(ColumnData::String(Some(is_null))) => is_null.eq_ignore_ascii_case("YES"),
                _ => false,
            };
            let ordinal = match iter.next() {
                Some(ColumnData::I32(Some(p))) => u32::try_from(p).unwrap_or(0),
                _ => cols.len() as u32,
            };
            let is_pk = match iter.next() {
                Some(ColumnData::I32(Some(p))) => p == 1,
                _ => false,
            };

            cols.push(ColumnInfo {
                name: col_name,
                data_type,
                nullable,
                primary_key: is_pk,
                ordinal,
            });
        }
        Ok(cols)
    }

    async fn row_estimate(&self, table: &TableRef) -> AppResult<Option<i64>> {
        let schema = table.schema.as_deref().unwrap_or("dbo");
        let name = &table.name;
        let sql = format!(
            "SELECT SUM(p.rows) \
             FROM sys.partitions p \
             JOIN sys.tables t ON p.object_id = t.object_id \
             JOIN sys.schemas s ON t.schema_id = s.schema_id \
             WHERE s.name = '{}' AND t.name = '{}' AND p.index_id IN (0, 1)",
            schema.replace('\'', "''"),
            name.replace('\'', "''")
        );

        let mut client = self.client.lock().await;
        let stream = client.simple_query(&sql).await.map_err(AppError::from)?;
        let row = stream.into_row().await.map_err(AppError::from)?;
        if let Some(r) = row {
            if let Some(ColumnData::I64(Some(count))) = r.into_iter().next() {
                return Ok(Some(count));
            }
        }
        Ok(None)
    }

    async fn count(&self, table: &TableRef, filters: &[FilterRule]) -> AppResult<i64> {
        let target = qualified_name(table);
        let where_str = where_clause(Engine::Mssql, filters);
        let sql = format!("SELECT COUNT(*) FROM {target}{where_str}");

        let mut client = self.client.lock().await;
        let stream = client.simple_query(&sql).await.map_err(AppError::from)?;
        let row = stream.into_row().await.map_err(AppError::from)?;
        if let Some(r) = row {
            if let Some(ColumnData::I32(Some(c))) = r.into_iter().next() {
                return Ok(c as i64);
            }
        }
        Ok(0)
    }

    async fn fetch_page(&self, table: &TableRef, query: &PageQuery) -> AppResult<ResultSet> {
        let cols = self.columns(table).await?;
        validate_columns(&cols, &query.sort, &query.filters)?;

        let target = qualified_name(table);
        let where_str = where_clause(Engine::Mssql, &query.filters);
        let mut order_str = order_clause(Engine::Mssql, &query.sort);
        if order_str.is_empty() {
            // MSSQL OFFSET-FETCH requires an ORDER BY clause.
            let pk = cols.iter().find(|c| c.primary_key).map(|c| c.name.as_str());
            let fallback = pk.unwrap_or_else(|| cols.first().map(|c| c.name.as_str()).unwrap_or("1"));
            order_str = format!(" ORDER BY {}", quote_ident(fallback));
        }

        let sql = format!(
            "SELECT * FROM {target}{where_str}{order_str} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            query.offset, query.limit
        );

        let mut client = self.client.lock().await;
        let stream = client.simple_query(&sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;

        let mut result_columns = Vec::new();
        let mut result_rows = Vec::new();

        if let Some(first_row) = rows.first() {
            for col in first_row.columns() {
                result_columns.push(ColumnMeta {
                    name: col.name().to_string(),
                    type_name: format!("{:?}", col.column_type()).to_lowercase(),
                });
            }
        }

        for row in rows {
            let cells = row.cells().map(|(_, data)| decode_column_data(data)).collect();
            result_rows.push(cells);
        }

        let max_rows = query.limit as usize;
        let truncated = result_rows.len() > max_rows;
        if truncated {
            result_rows.truncate(max_rows);
        }

        Ok(ResultSet {
            columns: result_columns,
            rows: result_rows,
            truncated,
        })
    }

    async fn execute(&self, sql: &str, max_rows: usize) -> AppResult<Vec<StatementResult>> {
        let stmts = split_statements(sql);
        let mut results = Vec::new();
        let mut client = self.client.lock().await;

        for stmt in stmts {
            let stream = match client.simple_query(&stmt).await {
                Ok(s) => s,
                Err(e) => return Err(AppError::driver(e.to_string())),
            };

            let rows = match stream.into_first_result().await {
                Ok(r) => r,
                Err(_) => {
                    results.push(StatementResult::Affected { rows_affected: 0 });
                    continue;
                }
            };

            if rows.is_empty() {
                results.push(StatementResult::Affected { rows_affected: 0 });
                continue;
            }

            let mut cols = Vec::new();
            if let Some(first) = rows.first() {
                for col in first.columns() {
                    cols.push(ColumnMeta {
                        name: col.name().to_string(),
                        type_name: format!("{:?}", col.column_type()).to_lowercase(),
                    });
                }
            }

            let mut out_rows = Vec::new();
            let truncated = rows.len() > max_rows;
            for r in rows.into_iter().take(max_rows) {
                out_rows.push(r.cells().map(|(_, data)| decode_column_data(data)).collect());
            }

            results.push(StatementResult::Rows {
                result: ResultSet {
                    columns: cols,
                    rows: out_rows,
                    truncated,
                },
            });
        }

        Ok(results)
    }

    async fn close(&self) {}

    async fn foreign_keys(&self) -> AppResult<Vec<ForeignKey>> {
        let sql = "SELECT \
                   fk.name AS constraint_name, \
                   OBJECT_SCHEMA_NAME(fk.parent_object_id) AS from_schema, \
                   OBJECT_NAME(fk.parent_object_id) AS from_table, \
                   c1.name AS from_column, \
                   OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS to_schema, \
                   OBJECT_NAME(fk.referenced_object_id) AS to_table, \
                   c2.name AS to_column \
                   FROM sys.foreign_keys fk \
                   INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id \
                   INNER JOIN sys.columns c1 ON fkc.parent_object_id = c1.object_id AND fkc.parent_column_id = c1.column_id \
                   INNER JOIN sys.columns c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id";

        let mut client = self.client.lock().await;
        let stream = client.simple_query(sql).await.map_err(AppError::from)?;
        let rows = stream.into_first_result().await.map_err(AppError::from)?;

        let mut fks = Vec::new();
        for row in rows {
            let mut iter = row.into_iter();
            let name = match iter.next() {
                Some(ColumnData::String(Some(n))) => n.to_string(),
                _ => continue,
            };
            let from_schema = match iter.next() {
                Some(ColumnData::String(Some(s))) => s.to_string(),
                _ => continue,
            };
            let from_table = match iter.next() {
                Some(ColumnData::String(Some(t))) => t.to_string(),
                _ => continue,
            };
            let from_column = match iter.next() {
                Some(ColumnData::String(Some(c))) => c.to_string(),
                _ => continue,
            };
            let to_schema = match iter.next() {
                Some(ColumnData::String(Some(s))) => s.to_string(),
                _ => continue,
            };
            let to_table = match iter.next() {
                Some(ColumnData::String(Some(t))) => t.to_string(),
                _ => continue,
            };
            let to_column = match iter.next() {
                Some(ColumnData::String(Some(c))) => c.to_string(),
                _ => continue,
            };

            fks.push(ForeignKey {
                name,
                from_schema: Some(from_schema),
                from_table,
                from_columns: vec![from_column],
                to_schema: Some(to_schema),
                to_table,
                to_columns: vec![to_column],
            });
        }
        Ok(fks)
    }

    async fn ddl(&self, _table: &TableRef) -> AppResult<Option<String>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mssql_ident_quoting() {
        assert_eq!(quote_ident("users"), "[users]");
        assert_eq!(quote_ident("us]ers"), "[us]]ers]");
        let table = TableRef {
            schema: Some("sales".into()),
            name: "orders".into(),
        };
        assert_eq!(qualified_name(&table), "[sales].[orders]");
    }
}
