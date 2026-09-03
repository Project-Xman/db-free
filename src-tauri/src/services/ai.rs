// SOT: ai-service, byok-llm, natural-language-to-sql, query-explainer, anthropic-messages-api

use crate::error::{AppError, AppResult};
use crate::guard::SessionCtx;
use crate::model::{AiProvider, AiReply, AiSettings, Engine, PlanReport, StatementResult, Value};
use std::time::Duration;

// WHAT:  Bring-your-own-key assistant: schema-aware NL→SQL and plan explanations.
// WHY:   PRD §4.5. Keys never leave the store unsealed except for the request.
// HOW:   One HTTP client; provider shapes: Anthropic Messages API (with default
//        server-side refusal fallbacks), OpenAI-compatible chat completions
// WHERE: src-tauri/src/services/settings.rs (settings + sealed key)
const MAX_SCHEMA_TABLES: usize = 60;

pub struct AiRequest<'a> {
    pub settings: &'a AiSettings,
    pub api_key: Option<&'a str>,
}

fn engine_label(engine: Engine) -> &'static str {
    match engine {
        Engine::Postgres => "PostgreSQL",
        Engine::Mysql => "MySQL",
        Engine::Mariadb => "MariaDB",
        Engine::Mssql => "SQL Server",
        Engine::Sqlite => "SQLite",
        Engine::Clickhouse => "ClickHouse",
        Engine::Redis => "Redis",
        Engine::Mongodb => "MongoDB",
        Engine::Libsql => "LibSQL / Turso",
        Engine::ValTown => "Val Town",
        Engine::CloudflareD1 => "Cloudflare D1",
        Engine::Supabase => "Supabase",
        Engine::Planetscale => "PlanetScale",
        Engine::Neon => "Neon",
    }
}

fn engine_guidelines(engine: Engine) -> &'static str {
    match engine {
        Engine::Postgres | Engine::Supabase | Engine::Neon => {
            "PostgreSQL Dialect Rules:\n\
             - Use standard PostgreSQL SQL syntax (double-quoted identifiers for mixed-case/reserved names, single quotes for strings).\n\
             - Standard schema is usually 'public'; qualify table names if non-default schema.\n\
             - Use ILIKE for case-insensitive text matching, ::type for casting, and NOW() for current timestamps.\n\
             - Supports CTEs (WITH), window functions, JSONB operators (->, ->>), and RETURNING clauses."
        }
        Engine::Mysql | Engine::Planetscale => {
            "MySQL Dialect Rules:\n\
             - Use standard MySQL SQL syntax with backticks (`table`, `column`) for identifiers when necessary.\n\
             - Use standard MySQL functions: CONCAT(), DATE_SUB(), NOW(), IFNULL(), COALESCE().\n\
             - Use LIMIT [offset,] count or LIMIT count OFFSET offset for pagination."
        }
        Engine::Mariadb => {
            "MariaDB Dialect Rules:\n\
             - Use standard MariaDB SQL syntax with backticks (`table`, `column`) for identifiers when necessary.\n\
             - Use standard MariaDB/MySQL functions: CONCAT(), NOW(), IFNULL(), COALESCE().\n\
             - Use LIMIT count OFFSET offset for pagination."
        }
        Engine::Mssql => {
            "SQL Server (T-SQL) Dialect Rules:\n\
             - Use standard T-SQL syntax with square brackets ([schema].[table], [column]) for identifiers when necessary.\n\
             - Use TOP (n) or OFFSET x ROWS FETCH NEXT y ROWS ONLY for pagination (requires ORDER BY).\n\
             - Use T-SQL functions: GETDATE(), ISNULL(), COALESCE(), LEN(), SUBSTRING(), CHARINDEX().\n\
             - Standard schema is usually 'dbo'."
        }
        Engine::Sqlite | Engine::Libsql | Engine::ValTown | Engine::CloudflareD1 => {
            "SQLite Dialect Rules:\n\
             - Use standard SQLite SQL syntax. SQLite uses dynamic typing.\n\
             - Use SQLite date/time functions: datetime('now'), strftime(), coalesce(), instr().\n\
             - Avoid FULL OUTER JOIN or RIGHT JOIN (unsupported in older versions); use LEFT JOIN or UNION ALL if needed.\n\
             - Use LIMIT count OFFSET offset for pagination."
        }
        Engine::Clickhouse => {
            "ClickHouse Dialect Rules:\n\
             - Use ClickHouse SQL syntax. Tables may require FINAL modifier if using ReplacingMergeTree.\n\
             - Use ClickHouse functions: formatDateTime(), toDate(), toDateTime(), and specialized array/tuple functions.\n\
             - Identifiers are case-sensitive. Use backticks or double quotes if needed."
        }
        Engine::Redis => {
            "Redis Command Rules:\n\
             - Redis is a key-value store, NOT a relational SQL database. Never output SQL statements.\n\
             - Output valid single or multi-line Redis CLI commands (e.g. GET key, SET key val, HGETALL key, LRANGE key 0 -1, SCAN 0 MATCH pattern COUNT 100, ZREVRANGEBYSCORE key +inf -inf WITHSCORES LIMIT 0 10)."
        }
        Engine::Mongodb => {
            "MongoDB Command Rules:\n\
             - MongoDB is a document store, NOT a SQL database. Never output SQL statements.\n\
             - Output either a MongoDB shell command (e.g. db.collection.find({...}).sort({...}).limit(10)) or a valid JSON query / aggregation pipeline document."
        }
    }
}

pub async fn schema_context(
    ctx: &SessionCtx,
    prompt: &str,
    current_table: Option<&str>,
) -> AppResult<String> {
    let catalog = ctx.integration.catalog().await?;
    let foreign_keys = ctx.integration.foreign_keys().await.unwrap_or_default();
    let server_ver = ctx.integration.server_version().await.unwrap_or(None);

    let mut out = String::new();
    let current_db = ctx.integration.current_database().or_else(|| ctx.connection.database.clone());
    if let Some(db) = current_db {
        out.push_str(&format!("Database: {db}\n"));
    }
    out.push_str(&format!("Engine: {}", engine_label(ctx.connection.engine)));
    if let Some(v) = server_ver {
        out.push_str(&format!(" (Server Version: {v})"));
    }
    out.push('\n');

    if catalog.schemas.len() > 1 {
        if let Some(first) = catalog.schemas.first() {
            out.push_str(&format!("Default Schema: {}\n", first.name));
        }
        let schema_names: Vec<&str> = catalog.schemas.iter().map(|s| s.name.as_str()).collect();
        out.push_str(&format!("Available Schemas: {}\n", schema_names.join(", ")));
    }

    // Collect all tables
    let mut all_tables = Vec::new();
    for schema in &catalog.schemas {
        for table in &schema.tables {
            all_tables.push(table.clone());
        }
    }

    let prompt_lower = prompt.to_lowercase();
    let curr_tbl_lower = current_table.map(|t| t.to_lowercase());

    // Sort/prioritize tables:
    // High priority: matches current_table, or table name appears in prompt
    all_tables.sort_by(|a, b| {
        let a_match = curr_tbl_lower.as_deref() == Some(&a.name.to_lowercase())
            || prompt_lower.contains(&a.name.to_lowercase());
        let b_match = curr_tbl_lower.as_deref() == Some(&b.name.to_lowercase())
            || prompt_lower.contains(&b.name.to_lowercase());
        b_match.cmp(&a_match)
    });

    let total_tables = all_tables.len();
    let tables_to_include = all_tables.into_iter().take(MAX_SCHEMA_TABLES).collect::<Vec<_>>();
    let omitted = total_tables.saturating_sub(MAX_SCHEMA_TABLES);

    out.push_str("Schema Tables:\n");
    for table in &tables_to_include {
        let table_ref = crate::model::TableRef {
            schema: table.schema.clone(),
            name: table.name.clone(),
        };
        let cols = ctx.integration.columns(&table_ref).await.unwrap_or_default();
        let col_text: Vec<String> = cols
            .iter()
            .map(|c| {
                let mut s = format!("{} {}", c.name, c.data_type);
                if c.primary_key {
                    s.push_str(" PK");
                }
                if !c.nullable && !c.primary_key {
                    s.push_str(" NOT NULL");
                }
                s
            })
            .collect();

        let full_name = match &table.schema {
            Some(s) if !s.is_empty() => format!("{s}.{}", table.name),
            _ => table.name.clone(),
        };

        let kind_str = match table.kind {
            crate::model::TableKind::Table => "",
            crate::model::TableKind::View => " [VIEW]",
        };

        let row_est = match table.row_estimate {
            Some(rows) if rows >= 0 => format!(" (~{rows} rows)"),
            _ => String::new(),
        };

        out.push_str(&format!("- {full_name}{kind_str}{row_est}: ({})\n", col_text.join(", ")));
    }

    if omitted > 0 {
        out.push_str(&format!("-- ({omitted} more tables omitted)\n"));
    }

    if !foreign_keys.is_empty() {
        out.push_str("\nForeign Keys & Relationships:\n");
        for (fk_count, fk) in foreign_keys.iter().enumerate() {
            if fk_count >= 50 {
                out.push_str("-- (more foreign keys omitted)\n");
                break;
            }
            let from_tbl = match &fk.from_schema {
                Some(s) if !s.is_empty() => format!("{s}.{}", fk.from_table),
                _ => fk.from_table.clone(),
            };
            let to_tbl = match &fk.to_schema {
                Some(s) if !s.is_empty() => format!("{s}.{}", fk.to_table),
                _ => fk.to_table.clone(),
            };
            out.push_str(&format!(
                "- {from_tbl}({}) -> {to_tbl}({})\n",
                fk.from_columns.join(", "),
                fk.to_columns.join(", ")
            ));
        }
    }

    // If an active table is specified, provide its exact DDL if available
    if let Some(target) = current_table {
        let matched = tables_to_include.iter().find(|t| t.name.eq_ignore_ascii_case(target));
        if let Some(tbl) = matched {
            let t_ref = crate::model::TableRef {
                schema: tbl.schema.clone(),
                name: tbl.name.clone(),
            };
            if let Ok(Some(ddl)) = ctx.integration.ddl(&t_ref).await {
                out.push_str(&format!("\nActive Table DDL ({target}):\n```sql\n{}\n```\n", ddl.trim()));
            }
        }
    }

    Ok(out)
}

pub async fn generate(
    ctx: &SessionCtx,
    req: &AiRequest<'_>,
    prompt: &str,
    current_query: Option<&str>,
    current_table: Option<&str>,
    error_context: Option<&str>,
    conversation_history: Option<&[crate::commands::ai::ChatMessage]>,
) -> AppResult<AiReply> {
    let schema = schema_context(ctx, prompt, current_table).await?;
    let engine = ctx.connection.engine;
    let system = format!(
        "You are an expert {} database conversational assistant and data architect inside the DB Free native database workbench.\n\
         Your goal is to converse with the user, answer questions about their database, explain schema relationships, and generate production-grade, highly efficient, and syntactically valid queries.\n\n\
         {}\n\n\
         Operating Rules:\n\
         1. When generating or modifying an executable query or command, enclose it in a single fenced code block (e.g. ```sql for SQL, ```redis for Redis, ```javascript or ```json for MongoDB).\n\
         2. Accompany the code block with a clear, concise explanation of the logic, filters, joins, or trade-offs.\n\
         3. Strict Schema Adherence: Only reference tables and columns that exist in the database context below, unless asked to CREATE or ALTER tables.\n\
         4. Safety & Destruction Warning: If the request requires a destructive statement (DROP, TRUNCATE, DELETE or UPDATE without a restrictive WHERE clause), clearly highlight a warning.\n\
         5. If the user asks a schema or database question that does not require running a query, answer directly and helpfully in markdown.\n\
         6. Maintain conversational context across follow-up questions.\n\n\
         Database Context:\n\
         {}",
        engine_label(engine),
        engine_guidelines(engine),
        schema
    );

    let mut user_message = String::new();
    if let Some(tbl) = current_table {
        let trimmed = tbl.trim();
        if !trimmed.is_empty() {
            user_message.push_str(&format!("Active Table: {trimmed}\n"));
        }
    }
    if let Some(query) = current_query {
        let trimmed = query.trim();
        if !trimmed.is_empty() {
            user_message.push_str(&format!("Current Editor Query:\n```sql\n{trimmed}\n```\n\n"));
        }
    }
    if let Some(err) = error_context {
        let trimmed = err.trim();
        if !trimmed.is_empty() {
            user_message.push_str(&format!("Previous Execution Error:\n{trimmed}\n\n"));
        }
    }
    user_message.push_str(&format!("User Request:\n{}", prompt.trim()));

    let raw = complete(req, &system, &user_message, conversation_history).await?;
    let cleaned = strip_think_blocks(&raw);
    let sql = extract_fence(&cleaned);
    Ok(AiReply { sql, text: cleaned, model: req.settings.model.clone() })
}

pub async fn explain(ctx: &SessionCtx, req: &AiRequest<'_>, sql: &str, max_rows: usize) -> AppResult<PlanReport> {
    let engine = ctx.connection.engine;
    let explain_sql = match engine {
        Engine::Postgres | Engine::Supabase | Engine::Neon => format!("EXPLAIN (FORMAT TEXT) {sql}"),
        Engine::Sqlite | Engine::Libsql | Engine::ValTown | Engine::CloudflareD1 => format!("EXPLAIN QUERY PLAN {sql}"),
        Engine::Mysql | Engine::Mariadb | Engine::Planetscale | Engine::Clickhouse => format!("EXPLAIN {sql}"),
        Engine::Mssql => format!("SET SHOWPLAN_TEXT ON;\n{sql};\nSET SHOWPLAN_TEXT OFF;"),
        Engine::Redis | Engine::Mongodb => return Err(AppError::invalid_input("Execution plans are only available for SQL engines.")),
    };
    let results = ctx.integration.execute(&explain_sql, max_rows).await?;
    let mut plan = String::new();
    for r in results {
        if let StatementResult::Rows { result } = r {
            for row in result.rows {
                let line: Vec<String> = row.iter().map(cell_text).collect();
                plan.push_str(&line.join("  "));
                plan.push('\n');
            }
        }
    }
    let explanation = if req.settings.provider == AiProvider::None {
        None
    } else {
        let schema = schema_context(ctx, sql, None).await.unwrap_or_default();
        let system = format!(
            "You are an expert {} query tuning and performance specialist. Explain execution plans to developers in plain language.\n\
             Point out sequential scans, unindexed filters, missing indexes, costly nested loops, hash joins, or heavy sorts.\n\
             Suggest concrete, runnable improvements (e.g. exact CREATE INDEX statements referencing real columns from the schema).\n\
             Be concise; use short bullet points.\n\n\
             Database Schema:\n{}",
            engine_label(engine),
            schema
        );
        let prompt = format!("Query:\n{sql}\n\nExecution Plan:\n{plan}");
        let raw = complete(req, &system, &prompt, None).await?;
        Some(strip_think_blocks(&raw))
    };
    Ok(PlanReport { plan, explanation })
}

fn cell_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(s) | Value::Text(s) | Value::Bytes(s) | Value::DateTime(s) | Value::Unsupported(s) => s.clone(),
        Value::Json(j) => j.to_string(),
    }
}

pub fn strip_think_blocks(text: &str) -> String {
    let mut result = String::new();
    let mut remaining = text;
    while let Some(start) = remaining.find("<think>") {
        result.push_str(&remaining[..start]);
        if let Some(end) = remaining[start..].find("</think>") {
            remaining = &remaining[start + end + "</think>".len()..];
        } else {
            remaining = "";
            break;
        }
    }
    result.push_str(remaining);
    result.trim().to_string()
}

pub fn extract_fence(text: &str) -> Option<String> {
    // 1. Search for fenced code blocks
    let mut start_idx = 0;
    while let Some(open_rel) = text[start_idx..].find("```") {
        let fence_start = start_idx + open_rel;
        let after_fence = &text[fence_start + 3..];
        let body_start = after_fence.find('\n').map(|i| i + 1).unwrap_or(0);
        let body = &after_fence[body_start..];
        if let Some(end) = body.find("```") {
            let code = body[..end].trim();
            if !code.is_empty() {
                return Some(code.to_string());
            }
            start_idx = fence_start + 3 + body_start + end + 3;
        } else {
            break;
        }
    }

    // 2. Fallback heuristic for raw statements if model omitted code fences
    let trimmed = text.trim();
    let upper = trimmed.to_uppercase();
    let sql_starters = [
        "SELECT ", "WITH ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "ALTER ", "DROP ",
        "EXPLAIN ", "SHOW ", "DESCRIBE ", "SET ", "GET ", "HGET", "SCAN ", "KEYS ", "db.",
    ];
    for starter in sql_starters {
        if upper.starts_with(starter) {
            let code = if let Some(cutoff) = trimmed.find("\n\n") {
                trimmed[..cutoff].trim()
            } else {
                trimmed
            };
            if !code.is_empty() {
                return Some(code.to_string());
            }
        }
    }

    None
}

async fn complete(
    req: &AiRequest<'_>,
    system: &str,
    user: &str,
    conversation_history: Option<&[crate::commands::ai::ChatMessage]>,
) -> AppResult<String> {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(120)).build().map_err(AppError::internal)?;
    let model = req.settings.model.trim();
    if model.is_empty() {
        return Err(AppError::invalid_input("Set an AI model in Settings → AI."));
    }
    match req.settings.provider {
        AiProvider::None => Err(AppError::invalid_input("Choose an AI provider in Settings → AI first.")),
        AiProvider::Anthropic => {
            let key = req.api_key.ok_or_else(|| AppError::invalid_input("Add your Anthropic API key in Settings → AI."))?;
            let base = req.settings.base_url.clone().unwrap_or_else(|| "https://api.anthropic.com".to_string());
            let mut messages_json = Vec::new();
            if let Some(history) = conversation_history {
                for msg in history {
                    messages_json.push(serde_json::json!({
                        "role": if msg.role == "assistant" { "assistant" } else { "user" },
                        "content": msg.content
                    }));
                }
            }
            messages_json.push(serde_json::json!({
                "role": "user",
                "content": user
            }));
            let body = serde_json::json!({
                "model": model,
                "max_tokens": 4000,
                "system": system,
                "messages": messages_json,
                "fallbacks": "default"
            });
            let response = client
                .post(format!("{}/v1/messages", base.trim_end_matches('/')))
                .header("x-api-key", key)
                .header("anthropic-version", "2023-06-01")
                .header("anthropic-beta", "server-side-fallback-2026-07-01")
                .json(&body)
                .send()
                .await?;
            let status = response.status();
            let json: serde_json::Value = response.json().await?;
            if !status.is_success() {
                let msg = json.pointer("/error/message").and_then(|m| m.as_str()).unwrap_or("request failed");
                return Err(AppError::driver(format!("Anthropic API {status}: {msg}")));
            }
            if json.get("stop_reason").and_then(|s| s.as_str()) == Some("refusal") {
                let why = json.pointer("/stop_details/explanation").and_then(|e| e.as_str()).unwrap_or("the request was declined");
                return Err(AppError::driver(format!("The model declined: {why}")));
            }
            let text = json
                .get("content")
                .and_then(|c| c.as_array())
                .map(|blocks| {
                    blocks
                        .iter()
                        .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
                        .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            Ok(text)
        }
        AiProvider::Openai | AiProvider::Openrouter => {
            let key = req.api_key.ok_or_else(|| AppError::invalid_input("Add your API key in Settings → AI."))?;
            let default_base = if req.settings.provider == AiProvider::Openai { "https://api.openai.com/v1" } else { "https://openrouter.ai/api/v1" };
            let base = req.settings.base_url.clone().unwrap_or_else(|| default_base.to_string());
            let mut messages_json = vec![
                serde_json::json!({"role": "system", "content": system})
            ];
            if let Some(history) = conversation_history {
                for msg in history {
                    messages_json.push(serde_json::json!({
                        "role": if msg.role == "assistant" { "assistant" } else { "user" },
                        "content": msg.content
                    }));
                }
            }
            messages_json.push(serde_json::json!({
                "role": "user",
                "content": user
            }));
            let body = serde_json::json!({
                "model": model,
                "messages": messages_json
            });
            let response = client
                .post(format!("{}/chat/completions", base.trim_end_matches('/')))
                .bearer_auth(key)
                .json(&body)
                .send()
                .await?;
            let status = response.status();
            let json: serde_json::Value = response.json().await?;
            if !status.is_success() {
                let msg = json.pointer("/error/message").and_then(|m| m.as_str()).unwrap_or("request failed");
                return Err(AppError::driver(format!("API {status}: {msg}")));
            }
            Ok(json.pointer("/choices/0/message/content").and_then(|c| c.as_str()).unwrap_or_default().to_string())
        }
        AiProvider::Ollama => {
            let base = req.settings.base_url.clone().unwrap_or_else(|| "http://127.0.0.1:11434".to_string());
            let mut messages_json = vec![
                serde_json::json!({"role": "system", "content": system})
            ];
            if let Some(history) = conversation_history {
                for msg in history {
                    messages_json.push(serde_json::json!({
                        "role": if msg.role == "assistant" { "assistant" } else { "user" },
                        "content": msg.content
                    }));
                }
            }
            messages_json.push(serde_json::json!({
                "role": "user",
                "content": user
            }));
            let body = serde_json::json!({
                "model": model,
                "stream": false,
                "messages": messages_json
            });
            let response = client.post(format!("{}/api/chat", base.trim_end_matches('/'))).json(&body).send().await?;
            let status = response.status();
            let json: serde_json::Value = response.json().await?;
            if !status.is_success() {
                return Err(AppError::driver(format!("Ollama {status}: {}", json.get("error").and_then(|e| e.as_str()).unwrap_or("request failed"))));
            }
            Ok(json.pointer("/message/content").and_then(|c| c.as_str()).unwrap_or_default().to_string())
        }
    }
}

// `From<reqwest::Error>` for AppError lives in integrations/clickhouse.rs (one impl per crate).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_extraction() {
        assert_eq!(extract_fence("Here:\n```sql\nSELECT 1;\n```\nDone"), Some("SELECT 1;".into()));
        assert_eq!(extract_fence("```\nGET k\n```"), Some("GET k".into()));
        assert_eq!(extract_fence("```redis\nHGETALL user:1\n```"), Some("HGETALL user:1".into()));
        assert_eq!(extract_fence("```json\n{\"find\": \"users\"}\n```"), Some("{\"find\": \"users\"}".into()));
    }

    #[test]
    fn raw_sql_fallback() {
        assert_eq!(
            extract_fence("SELECT * FROM users WHERE active = true;\n\nThis selects all active users."),
            Some("SELECT * FROM users WHERE active = true;".into())
        );
        assert_eq!(extract_fence("no code here, just a helpful answer"), None);
    }

    #[test]
    fn think_blocks_removal() {
        let text = "<think>\nLet me analyze the tables...\n</think>\nHere is the query:\n```sql\nSELECT * FROM orders;\n```";
        assert_eq!(
            strip_think_blocks(text),
            "Here is the query:\n```sql\nSELECT * FROM orders;\n```"
        );
        let unclosed = "<think>still thinking";
        assert_eq!(strip_think_blocks(unclosed), "");
    }
}
