// SOT: transfer-service, export-tables, import-file, csv-io, json-io, sql-dump

use crate::error::{AppError, AppResult};
use crate::guard::SessionCtx;
use crate::model::{
    ExportReport, ExportedFile, PageQuery, SortRule, TableRef, TransferFormat, Value,
};
use crate::services::changes;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

const PAGE: u32 = 1_000;
const INSERT_BATCH: usize = 100;

// WHAT:  Streams every selected table into one file per table (CSV / JSON / SQL).
// WHY:   PRD §7 Phase 3 import/export; files are written page by page so a
//        million-row table never sits in memory.
// HOW:   Rows are ordered by primary key for stable paging; `max_rows` caps a
//        runaway export. SQL output = optional DDL + batched INSERTs.
// WHERE: src/features/transfer/ExportImportTab.tsx (caller via commands::transfer)
pub async fn export_tables(
    ctx: &SessionCtx,
    tables: &[TableRef],
    format: TransferFormat,
    include_schema: bool,
    directory: &Path,
    max_rows: u64,
) -> AppResult<ExportReport> {
    let started = Instant::now();
    if tables.is_empty() {
        return Err(AppError::invalid_input("Select at least one table to export."));
    }
    std::fs::create_dir_all(directory).map_err(AppError::internal)?;
    let engine = ctx.connection.engine;
    let mut files = Vec::with_capacity(tables.len());
    for table in tables {
        let columns = ctx.integration.columns(table).await?;
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let sort: Vec<SortRule> = columns.iter().filter(|c| c.primary_key).map(|c| SortRule { column: c.name.clone(), desc: false }).collect();
        let path = file_path(directory, table, format);
        let mut file = std::fs::File::create(&path).map_err(AppError::internal)?;
        let mut writer = RowWriter::start(&mut file, format, &names)?;
        if format == TransferFormat::Sql && include_schema {
            if let Some(ddl) = ctx.integration.ddl(table).await? {
                writeln!(file, "{ddl};\n").map_err(AppError::internal)?;
            }
        }
        let mut offset: u64 = 0;
        let mut written: u64 = 0;
        loop {
            let query = PageQuery { sort: sort.clone(), filters: Vec::new(), offset, limit: PAGE };
            let page = ctx.integration.fetch_page(table, &query).await?;
            let count = page.rows.len() as u64;
            writer.rows(&mut file, engine, table, &names, &page.rows)?;
            written += count;
            offset += count;
            if count < u64::from(PAGE) || written >= max_rows {
                break;
            }
        }
        writer.finish(&mut file)?;
        files.push(ExportedFile { table: table.clone(), path: path.to_string_lossy().into_owned(), rows: written });
    }
    Ok(ExportReport { files, elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX) })
}

fn file_path(directory: &Path, table: &TableRef, format: TransferFormat) -> PathBuf {
    let base = match &table.schema {
        Some(schema) => format!("{schema}.{}", table.name),
        None => table.name.clone(),
    };
    let safe: String = base.chars().map(|c| if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' { c } else { '_' }).collect();
    directory.join(format!("{safe}.{}", format.extension()))
}

enum RowWriter {
    Csv,
    Json { first: bool },
    Sql,
}

impl RowWriter {
    fn start(file: &mut std::fs::File, format: TransferFormat, columns: &[String]) -> AppResult<RowWriter> {
        match format {
            TransferFormat::Csv => {
                let mut w = csv::Writer::from_writer(&mut *file);
                w.write_record(columns).map_err(AppError::internal)?;
                w.flush().map_err(AppError::internal)?;
                Ok(RowWriter::Csv)
            }
            TransferFormat::Json => {
                file.write_all(b"[\n").map_err(AppError::internal)?;
                Ok(RowWriter::Json { first: true })
            }
            TransferFormat::Sql => Ok(RowWriter::Sql),
        }
    }

    fn rows(&mut self, file: &mut std::fs::File, engine: crate::model::Engine, table: &TableRef, columns: &[String], rows: &[Vec<Value>]) -> AppResult<()> {
        match self {
            RowWriter::Csv => {
                let mut w = csv::Writer::from_writer(&mut *file);
                for row in rows {
                    w.write_record(row.iter().map(csv_cell)).map_err(AppError::internal)?;
                }
                w.flush().map_err(AppError::internal)
            }
            RowWriter::Json { first } => {
                for row in rows {
                    let object: serde_json::Map<String, serde_json::Value> =
                        columns.iter().cloned().zip(row.iter().map(json_cell)).collect();
                    let line = serde_json::to_string(&object).map_err(AppError::internal)?;
                    if *first {
                        *first = false;
                    } else {
                        file.write_all(b",\n").map_err(AppError::internal)?;
                    }
                    file.write_all(line.as_bytes()).map_err(AppError::internal)?;
                }
                Ok(())
            }
            RowWriter::Sql => {
                for stmt in changes::insert_batches(engine, table, columns, rows, INSERT_BATCH) {
                    writeln!(file, "{stmt};").map_err(AppError::internal)?;
                }
                Ok(())
            }
        }
    }

    fn finish(self, file: &mut std::fs::File) -> AppResult<()> {
        if let RowWriter::Json { .. } = self {
            file.write_all(b"\n]\n").map_err(AppError::internal)?;
        }
        file.flush().map_err(AppError::internal)
    }
}

fn csv_cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => s.clone(),
        Value::Json(j) => j.to_string(),
    }
}

pub fn json_cell(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::from(*i),
        Value::Float(f) => serde_json::Value::from(*f),
        Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => serde_json::Value::String(s.clone()),
        Value::Json(j) => j.clone(),
    }
}

// WHAT:  Parses a CSV or JSON file into columns + typed rows for import.
// HOW:   CSV cells are text (empty = NULL); JSON arrays of objects keep their types.
pub fn parse_file(path: &Path, format: TransferFormat) -> AppResult<(Vec<String>, Vec<Vec<Value>>)> {
    match format {
        TransferFormat::Csv => {
            let mut reader = csv::ReaderBuilder::new().flexible(true).from_path(path).map_err(AppError::invalid_input_from)?;
            let columns: Vec<String> = reader.headers().map_err(AppError::invalid_input_from)?.iter().map(String::from).collect();
            let mut rows = Vec::new();
            for record in reader.records() {
                let record = record.map_err(AppError::invalid_input_from)?;
                rows.push(
                    (0..columns.len())
                        .map(|i| match record.get(i) {
                            Some("") | None => Value::Null,
                            Some(text) => Value::Text(text.to_string()),
                        })
                        .collect(),
                );
            }
            Ok((columns, rows))
        }
        TransferFormat::Json => {
            let raw = std::fs::read_to_string(path).map_err(AppError::invalid_input_from)?;
            let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(AppError::invalid_input_from)?;
            let items = parsed.as_array().ok_or_else(|| AppError::invalid_input("JSON import expects an array of objects."))?;
            let mut columns: Vec<String> = Vec::new();
            for item in items {
                if let Some(obj) = item.as_object() {
                    for key in obj.keys() {
                        if !columns.contains(key) {
                            columns.push(key.clone());
                        }
                    }
                }
            }
            let rows = items
                .iter()
                .map(|item| columns.iter().map(|c| item.get(c).map(value_from_json).unwrap_or(Value::Null)).collect())
                .collect();
            Ok((columns, rows))
        }
        TransferFormat::Sql => Err(AppError::invalid_input("Run .sql files from the query tab instead.")),
    }
}

fn value_from_json(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => n.as_i64().map(Value::Int).or_else(|| n.as_f64().map(Value::Float)).unwrap_or(Value::Null),
        serde_json::Value::String(s) => Value::Text(s.clone()),
        other => Value::Json(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_csv_and_json() {
        let dir = std::env::temp_dir().join(format!("db-free-transfer-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        let csv_path = dir.join("a.csv");
        std::fs::write(&csv_path, "id,name\n1,ann\n2,\n").unwrap_or_else(|e| panic!("{e}"));
        let (cols, rows) = parse_file(&csv_path, TransferFormat::Csv).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(cols, vec!["id", "name"]);
        assert_eq!(rows.get(1).and_then(|r| r.get(1)), Some(&Value::Null));
        let json_path = dir.join("a.json");
        std::fs::write(&json_path, r#"[{"id":1,"name":"ann"},{"id":2,"tags":["x"]}]"#).unwrap_or_else(|e| panic!("{e}"));
        let (cols, rows) = parse_file(&json_path, TransferFormat::Json).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(cols, vec!["id", "name", "tags"]);
        assert_eq!(rows.first().and_then(|r| r.first()), Some(&Value::Int(1)));
        assert!(matches!(rows.get(1).and_then(|r| r.get(2)), Some(Value::Json(_))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_names_are_safe() {
        let p = file_path(Path::new("/tmp/x"), &TableRef { schema: Some("pub lic".into()), name: "us/ers".into() }, TransferFormat::Csv);
        assert_eq!(p.file_name().and_then(|n| n.to_str()), Some("pub_lic.us_ers.csv"));
    }
}
