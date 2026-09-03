// SOT: changes-service, staged-change-sql, value-literal, commit-script, review-mode-sql

use crate::error::{AppError, AppResult};
use crate::integrations::sql::quote_literal;
use crate::integrations::{qualified_name_for, quote_ident_for};
use crate::model::{CellValue, ChangePreview, Engine, Family, StagedChange, TableRef, Value};
use base64::Engine as _;

// WHAT:  Renders a cell Value as an SQL literal for the given engine.
// WHY:   The Pending Changes panel shows the exact SQL that will run; the UI never
//        builds SQL itself.
// HOW:   Text-like values go through quote_literal ('' escaping); binary becomes
//        the engine's hex form; non-finite floats become NULL.
pub fn literal(engine: Engine, value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => match engine.family() {
            Family::Sqlite
            | Family::Libsql
            | Family::ValTown
            | Family::CloudflareD1
            | Family::Mssql
            | Family::Oracle => {
                if *b { "1" } else { "0" }.to_string()
            }
            _ => {
                if *b { "TRUE" } else { "FALSE" }.to_string()
            }
        },
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_finite() {
                f.to_string()
            } else {
                "NULL".to_string()
            }
        }
        Value::Decimal(s) => {
            if s.parse::<f64>().is_ok() {
                s.clone()
            } else {
                quote_literal(s)
            }
        }
        Value::Text(s) | Value::DateTime(s) | Value::Unsupported(s) => quote_literal(s),
        Value::Json(j) => quote_literal(&j.to_string()),
        Value::Bytes(b64) => {
            let bytes = base64::engine::general_purpose::STANDARD.decode(b64).unwrap_or_default();
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            match engine.family() {
                Family::Postgres => format!("'\\x{hex}'::bytea"),
                Family::Mysql
                | Family::Sqlite
                | Family::Duckdb
                | Family::Libsql
                | Family::ValTown
                | Family::CloudflareD1
                | Family::Snowflake => format!("X'{hex}'"),
                Family::Mssql | Family::Cassandra => format!("0x{hex}"),
                Family::Clickhouse => format!("unhex('{hex}')"),
                Family::Oracle => format!("HEXTORAW('{hex}')"),
                Family::Bigquery => format!("FROM_HEX('{hex}')"),
                _ => quote_literal(b64),
            }
        }
    }
}

fn key_predicate(engine: Engine, key: &[CellValue]) -> AppResult<String> {
    if key.is_empty() {
        return Err(AppError::invalid_input("This table has no primary key, so rows cannot be addressed safely for editing."));
    }
    Ok(key
        .iter()
        .map(|k| match k.value {
            Value::Null => format!("{} IS NULL", quote_ident_for(engine, &k.column)),
            _ => format!("{} = {}", quote_ident_for(engine, &k.column), literal(engine, &k.value)),
        })
        .collect::<Vec<_>>()
        .join(" AND "))
}

fn table_name(engine: Engine, table: &TableRef) -> String {
    qualified_name_for(engine, table)
}

pub fn statement(engine: Engine, change: &StagedChange) -> AppResult<String> {
    Ok(match change {
        StagedChange::Update { table, key, column, new, .. } => format!(
            "UPDATE {} SET {} = {} WHERE {}",
            table_name(engine, table),
            quote_ident_for(engine, column),
            literal(engine, new),
            key_predicate(engine, key)?
        ),
        StagedChange::Insert { table, values, .. } => {
            if values.is_empty() {
                return Err(AppError::invalid_input("An insert needs at least one value."));
            }
            format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name(engine, table),
                values.iter().map(|v| quote_ident_for(engine, &v.column)).collect::<Vec<_>>().join(", "),
                values.iter().map(|v| literal(engine, &v.value)).collect::<Vec<_>>().join(", ")
            )
        }
        StagedChange::Delete { table, key, .. } => {
            format!("DELETE FROM {} WHERE {}", table_name(engine, table), key_predicate(engine, key)?)
        }
    })
}

// WHAT:  Builds the whole commit script. Transactional engines get BEGIN/COMMIT so
//        a failing statement rolls everything back.
pub fn preview(engine: Engine, changes: &[StagedChange]) -> AppResult<ChangePreview> {
    if !engine_supports_sql_edits(engine) {
        return Err(AppError::invalid_input("Inline editing is not available for this engine yet."));
    }
    let statements = changes.iter().map(|c| statement(engine, c)).collect::<AppResult<Vec<_>>>()?;
    let transactional = !matches!(engine, Engine::Clickhouse | Engine::CloudflareD1 | Engine::ValTown);
    let mut script = String::new();
    if transactional {
        if engine == Engine::Mssql {
            script.push_str("BEGIN TRANSACTION;\n");
        } else {
            script.push_str("BEGIN;\n");
        }
    }
    for s in &statements {
        script.push_str(s);
        script.push_str(";\n");
    }
    if transactional {
        if engine == Engine::Mssql {
            script.push_str("COMMIT TRANSACTION;");
        } else {
            script.push_str("COMMIT;");
        }
    }
    Ok(ChangePreview { statements, script })
}

pub fn engine_supports_sql_edits(engine: Engine) -> bool {
    !matches!(engine, Engine::Redis | Engine::Mongodb)
}

// WHAT:  Multi-row INSERT batches for imports.
pub fn insert_batches(engine: Engine, table: &TableRef, columns: &[String], rows: &[Vec<Value>], batch: usize) -> Vec<String> {
    let cols = columns.iter().map(|c| quote_ident_for(engine, c)).collect::<Vec<_>>().join(", ");
    rows.chunks(batch.max(1))
        .map(|chunk| {
            let values = chunk
                .iter()
                .map(|r| format!("({})", r.iter().map(|v| literal(engine, v)).collect::<Vec<_>>().join(", ")))
                .collect::<Vec<_>>()
                .join(",\n");
            format!("INSERT INTO {} ({}) VALUES\n{}", table_name(engine, table), cols, values)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> TableRef {
        TableRef { schema: Some("public".into()), name: "users".into() }
    }

    #[test]
    fn literals_per_engine() {
        assert_eq!(literal(Engine::Postgres, &Value::Text("it's".into())), "'it''s'");
        assert_eq!(literal(Engine::Sqlite, &Value::Bool(true)), "1");
        assert_eq!(literal(Engine::Postgres, &Value::Bool(false)), "FALSE");
        assert_eq!(literal(Engine::Postgres, &Value::Bytes("aGk=".into())), "'\\x6869'::bytea");
        assert_eq!(literal(Engine::Mysql, &Value::Bytes("aGk=".into())), "X'6869'");
        assert_eq!(literal(Engine::Postgres, &Value::Float(f64::NAN)), "NULL");
        assert_eq!(literal(Engine::Postgres, &Value::Json(serde_json::json!({"a": 1}))), "'{\"a\":1}'");
    }

    #[test]
    fn update_insert_delete_scripts() {
        let key = vec![CellValue { column: "id".into(), value: Value::Int(7) }];
        let changes = vec![
            StagedChange::Update { id: "1".into(), table: t(), key: key.clone(), column: "name".into(), old: Value::Text("a".into()), new: Value::Text("b".into()) },
            StagedChange::Insert { id: "2".into(), table: t(), values: vec![CellValue { column: "name".into(), value: Value::Text("c".into()) }, CellValue { column: "n".into(), value: Value::Null }] },
            StagedChange::Delete { id: "3".into(), table: t(), key },
        ];
        let p = preview(Engine::Postgres, &changes).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(p.statements.first().map(String::as_str), Some("UPDATE \"public\".\"users\" SET \"name\" = 'b' WHERE \"id\" = 7"));
        assert_eq!(p.statements.get(1).map(String::as_str), Some("INSERT INTO \"public\".\"users\" (\"name\", \"n\") VALUES ('c', NULL)"));
        assert_eq!(p.statements.get(2).map(String::as_str), Some("DELETE FROM \"public\".\"users\" WHERE \"id\" = 7"));
        assert!(p.script.starts_with("BEGIN;\n") && p.script.ends_with("COMMIT;"));
        let ch = preview(Engine::Clickhouse, &changes).unwrap_or_else(|e| panic!("{e}"));
        assert!(!ch.script.contains("BEGIN"));
        let my = preview(Engine::Mysql, &changes).unwrap_or_else(|e| panic!("{e}"));
        assert!(my.statements.first().is_some_and(|s| s.contains("`public`.`users`")));
    }

    #[test]
    fn missing_key_is_rejected() {
        let err = preview(Engine::Sqlite, &[StagedChange::Delete { id: "x".into(), table: t(), key: vec![] }]).err();
        assert!(matches!(err, Some(AppError::InvalidInput { .. })));
        assert!(preview(Engine::Redis, &[]).is_err());
    }

    #[test]
    fn insert_batches_split() {
        let rows: Vec<Vec<Value>> = (0..5).map(|i| vec![Value::Int(i), Value::Text(format!("n{i}"))]).collect();
        let batches = insert_batches(Engine::Sqlite, &TableRef { schema: None, name: "t".into() }, &["id".into(), "name".into()], &rows, 2);
        assert_eq!(batches.len(), 3);
        assert!(batches.first().is_some_and(|b| b.starts_with("INSERT INTO \"t\" (\"id\", \"name\") VALUES\n(0, 'n0'),\n(1, 'n1')")));
    }
}
