// SOT: ai-service, byok-llm, natural-language-to-sql, query-explainer, anthropic-messages-api

use crate::error::{AppError, AppResult};
use crate::guard::SessionCtx;
use crate::model::{AiProvider, AiReply, AiSettings, Engine, PlanReport, StatementResult, Value};
use std::time::Duration;

// WHAT:  Bring-your-own-key assistant: schema-aware NL→SQL and plan explanations.
// WHY:   PRD §4.5. Keys never leave the store unsealed except for the request.
// HOW:   One HTTP client; provider shapes: Anthropic Messages API (with default
//        server-side refusal fallbacks), OpenAI-compatible chat completions
//        (OpenAI, OpenRouter), Ollama /api/chat. SQL is read from a ```sql fence.
// WHERE: src-tauri/src/services/settings.rs (settings + sealed key)
const MAX_SCHEMA_TABLES: usize = 60;

pub struct AiRequest<'a> {
    pub settings: &'a AiSettings,
    pub api_key: Option<&'a str>,
}

pub async fn schema_context(ctx: &SessionCtx) -> AppResult<String> {
    let catalog = ctx.integration.catalog().await?;
    let mut out = String::new();
    let mut n = 0;
    for schema in &catalog.schemas {
        for table in &schema.tables {
            if n >= MAX_SCHEMA_TABLES {
                out.push_str("-- (more tables omitted)\n");
                return Ok(out);
            }
            let table_ref = crate::model::TableRef { schema: table.schema.clone(), name: table.name.clone() };
            let cols = ctx.integration.columns(&table_ref).await.unwrap_or_default();
            let col_text: Vec<String> = cols
                .iter()
                .map(|c| format!("{} {}{}", c.name, c.data_type, if c.primary_key { " PK" } else { "" }))
                .collect();
            let name = match &table.schema {
                Some(s) => format!("{s}.{}", table.name),
                None => table.name.clone(),
            };
            out.push_str(&format!("{name}({})\n", col_text.join(", ")));
            n += 1;
        }
    }
    Ok(out)
}

pub async fn generate(ctx: &SessionCtx, req: &AiRequest<'_>, prompt: &str) -> AppResult<AiReply> {
    let schema = schema_context(ctx).await?;
    let engine = ctx.connection.engine;
    let system = format!(
        "You are an expert {} assistant inside a database workbench. Answer with exactly one statement in a ```sql fenced block \
         (for Redis use a command line, for MongoDB a JSON command document), then one short sentence explaining it. \
         Only use tables and columns from this schema:\n{}",
        engine_label(engine),
        schema
    );
    let text = complete(req, &system, prompt).await?;
    Ok(AiReply { sql: extract_fence(&text), text, model: req.settings.model.clone() })
}

pub async fn explain(ctx: &SessionCtx, req: &AiRequest<'_>, sql: &str, max_rows: usize) -> AppResult<PlanReport> {
    let engine = ctx.connection.engine;
    let explain_sql = match engine {
        Engine::Postgres => format!("EXPLAIN (FORMAT TEXT) {sql}"),
        Engine::Sqlite => format!("EXPLAIN QUERY PLAN {sql}"),
        Engine::Mysql | Engine::Mariadb | Engine::Clickhouse => format!("EXPLAIN {sql}"),
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
        let system = format!(
            "You explain {} execution plans to developers in plain language. Point out sequential scans, missing indexes, \
             expensive joins and sorts, and suggest concrete improvements. Be concise; use short bullet points.",
            engine_label(engine)
        );
        let prompt = format!("Query:\n{sql}\n\nPlan:\n{plan}");
        Some(complete(req, &system, &prompt).await?)
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

fn engine_label(engine: Engine) -> &'static str {
    match engine {
        Engine::Postgres => "PostgreSQL",
        Engine::Mysql => "MySQL",
        Engine::Mariadb => "MariaDB",
        Engine::Sqlite => "SQLite",
        Engine::Clickhouse => "ClickHouse",
        Engine::Redis => "Redis",
        Engine::Mongodb => "MongoDB",
    }
}

pub fn extract_fence(text: &str) -> Option<String> {
    let start = text.find("```")?;
    let after = &text[start + 3..];
    let body_start = after.find('\n').map(|i| i + 1).unwrap_or(0);
    let body = &after[body_start..];
    let end = body.find("```")?;
    let sql = body[..end].trim();
    if sql.is_empty() {
        None
    } else {
        Some(sql.to_string())
    }
}

async fn complete(req: &AiRequest<'_>, system: &str, user: &str) -> AppResult<String> {
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
            let body = serde_json::json!({
                "model": model,
                "max_tokens": 4000,
                "system": system,
                "messages": [{"role": "user", "content": user}],
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
            let body = serde_json::json!({
                "model": model,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]
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
            let body = serde_json::json!({
                "model": model,
                "stream": false,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]
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
        assert_eq!(extract_fence("no fence"), None);
    }
}
