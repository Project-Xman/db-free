// SOT: the-block, guard, request-pipeline, read-only-lock, destructive-guard, query-history-log, page-bounds

use crate::error::{AppError, AppResult};
use crate::model::{ConnectionSummary, HistoryOrigin, HistoryStatus, QueryOutcome};
use crate::state::AppState;
use crate::store::history::NewHistoryEntry;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub mod destructive;

use destructive::{classify, StatementKind};

// ============================================================================
// THE BLOCK
//
// WHAT:  Every Tauri command passes through one of the three entry points below.
// WHY:   Concerns that apply to every request live here once — timing, connection
//        resolution, session lookup, read-only enforcement, destructive-statement
//        confirmation, timeouts, bounds and the history log — so no feature has to
//        restate them and none can quietly drop one.
// HOW:   Steps are numbered and ordered cheapest-rejection-first. Add a concern by
//        inserting a step; never by creating a second guard.
// WHERE: scripts/guardrail.py fails the build if a command skips `guard::`.
//
//   #  step                         rejects when
//   1  request setup                — starts the timer
//   2  connection resolution        connection id unknown
//   3  session lookup               connection not connected
//   4  statement classification     — labels each statement Read/Write/Destructive
//   5  read-only gate               connection is read-only and a Write/Destructive exists
//   6  destructive gate             Destructive present and caller did not confirm
//   7  bounds                       page limit / row cap outside range (clamped, not rejected)
//   8  timeout                      handler exceeds the request's deadline
//   9  history log                  — records SQL on success and error
//  10  timing enrichment            — elapsed_ms attached to the outcome
// ============================================================================

pub const MAX_PAGE_LIMIT: u32 = 1_000;
pub const MAX_RESULT_ROWS: u32 = 50_000;
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

pub struct SessionCtx {
    pub connection: ConnectionSummary,
    pub integration: Arc<dyn crate::integrations::Integration>,
    pub started: Instant,
}

impl SessionCtx {
    pub fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

// WHAT:  Store-only requests (connections CRUD, buffers, history reads).
pub async fn local<T>(op: &'static str, fut: impl Future<Output = AppResult<T>>) -> AppResult<T> {
    let started = Instant::now(); // 1
    let result = fut.await;
    log::debug!("{op} finished in {:?} ok={}", started.elapsed(), result.is_ok());
    result
}

// WHAT:  Requests against a live database session that run no user-authored SQL.
pub async fn session<T, F, Fut>(state: &AppState, connection_id: &str, handler: F) -> AppResult<T>
where
    F: FnOnce(SessionCtx) -> Fut,
    Fut: Future<Output = AppResult<T>>,
{
    let ctx = resolve(state, connection_id).await?; // 1–3
    tokio::time::timeout(DEFAULT_TIMEOUT, handler(ctx)) // 8
        .await
        .map_err(|_| AppError::timeout("The request timed out."))?
}

pub struct StatementRequest<'a> {
    pub connection_id: &'a str,
    pub sql: &'a str,
    pub confirm_destructive: bool,
}

// WHAT:  Requests that execute user-authored SQL. Adds steps 4–6 and 9–10.
pub async fn statement<F, Fut>(
    state: &AppState,
    req: StatementRequest<'_>,
    handler: F,
) -> AppResult<QueryOutcome>
where
    F: FnOnce(SessionCtx) -> Fut,
    Fut: Future<Output = AppResult<QueryOutcome>>,
{
    let ctx = resolve(state, req.connection_id).await?; // 1–3
    let trimmed = req.sql.trim();
    if trimmed.is_empty() {
        return Err(AppError::invalid_input("Nothing to run."));
    }

    // 4 — classification
    let statements = classify(trimmed);

    // 5 — read-only gate
    if ctx.connection.read_only {
        if let Some(offender) = statements.iter().find(|s| s.kind != StatementKind::Read) {
            return Err(AppError::read_only(format!(
                "This connection is read-only. Blocked: {}",
                preview(&offender.text)
            )));
        }
    }

    // 6 — destructive gate
    let destructive: Vec<String> = statements
        .iter()
        .filter(|s| s.kind == StatementKind::Destructive)
        .map(|s| match &s.reason {
            Some(reason) => format!("{} — {}", preview(&s.text), reason),
            None => preview(&s.text),
        })
        .collect();
    if !destructive.is_empty() && !req.confirm_destructive {
        return Err(AppError::DestructiveConfirmationRequired {
            message: "This script contains destructive statements.".to_string(),
            statements: destructive,
        });
    }

    // 8 — timeout
    let started = ctx.started;
    let connection_id = ctx.connection.id.clone();
    let result = tokio::time::timeout(DEFAULT_TIMEOUT, handler(ctx))
        .await
        .map_err(|_| AppError::timeout("The query timed out."))
        .and_then(|inner| inner);

    // 9 — history log (success and error alike)
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let (status, error, row_count) = match &result {
        Ok(outcome) => (HistoryStatus::Ok, None, Some(outcome.total_rows())),
        Err(err) => (HistoryStatus::Error, Some(err.message().to_string()), None),
    };
    let logged = state.with_store(|store| {
        store.insert_history(&NewHistoryEntry {
            connection_id: &connection_id,
            sql: trimmed,
            status,
            origin: HistoryOrigin::User,
            error: error.as_deref(),
            elapsed_ms,
            row_count,
        })
    });
    if let Err(err) = logged {
        log::warn!("history log failed: {err}");
    }

    // 10 — timing enrichment
    result.map(|mut outcome| {
        outcome.elapsed_ms = elapsed_ms;
        outcome
    })
}

// 7 — bounds. Clamping (not rejecting) keeps the grid usable if a caller over-asks.
pub fn clamp_page_limit(limit: u32) -> u32 {
    limit.clamp(1, MAX_PAGE_LIMIT)
}

pub fn clamp_result_rows(max_rows: Option<u32>) -> usize {
    max_rows.unwrap_or(1_000).clamp(1, MAX_RESULT_ROWS) as usize
}

async fn resolve(state: &AppState, connection_id: &str) -> AppResult<SessionCtx> {
    let started = Instant::now(); // 1
    let connection = state.with_store(|store| store.get_connection(connection_id))?; // 2
    let integration = state
        .session(connection_id)
        .await
        .ok_or_else(|| AppError::not_connected(format!("Not connected to \"{}\".", connection.name)))?; // 3
    Ok(SessionCtx { connection, integration, started })
}

fn preview(sql: &str) -> String {
    let flat: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() > 80 {
        let head: String = flat.chars().take(77).collect();
        format!("{head}...")
    } else {
        flat
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::keyring::MemoryKeyProvider;
    use crate::model::{ConnectionInput, Engine, Environment, SslMode};
    use crate::store::Store;

    async fn state_with_sqlite(read_only: bool) -> (AppState, String) {
        let dir = std::env::temp_dir().join(format!("db-free-guard-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{e}"));
        let path = dir.join("g.db").to_string_lossy().into_owned();
        // A zero-byte file is a valid empty SQLite database; read-only open needs it to exist.
        std::fs::File::create(&path).unwrap_or_else(|e| panic!("{e}"));
        let store = Store::open_in_memory().unwrap_or_else(|e| panic!("{e}"));
        let input = ConnectionInput {
            name: "guarded".into(),
            engine: Engine::Sqlite,
            environment: Environment::Production,
            read_only,
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
            file_path: Some(path),
            ssl_mode: SslMode::Disable,
        };
        let summary = store.insert_connection(&input, None).unwrap_or_else(|e| panic!("{e}"));
        let state = AppState::new(store, Box::new(MemoryKeyProvider::default()));
        let resolved = crate::model::ResolvedConnection { summary: summary.clone(), secret: None };
        let integration = crate::integrations::connect(&resolved).await.unwrap_or_else(|e| panic!("{e}"));
        state.insert_session(summary.id.clone(), integration).await;
        (state, summary.id)
    }

    async fn run(state: &AppState, id: &str, sql: &str, confirm: bool) -> AppResult<QueryOutcome> {
        statement(
            state,
            StatementRequest { connection_id: id, sql, confirm_destructive: confirm },
            |ctx| async move {
                let statements = ctx.integration.execute(sql, 10).await?;
                Ok(QueryOutcome { statements, elapsed_ms: 0 })
            },
        )
        .await
    }

    #[tokio::test]
    async fn unknown_connection_is_not_found() {
        let (state, _) = state_with_sqlite(false).await;
        let err = session(&state, "nope", |_| async { Ok(()) }).await.err();
        assert!(matches!(err, Some(AppError::NotFound { .. })));
    }

    #[tokio::test]
    async fn read_only_blocks_writes_and_logs_history() {
        let (state, id) = state_with_sqlite(true).await;
        assert!(run(&state, &id, "SELECT 1", false).await.is_ok());
        let err = run(&state, &id, "CREATE TABLE t (a int)", false).await.err();
        assert!(matches!(err, Some(AppError::ReadOnly { .. })));
        let history = state.with_store(|s| s.list_history(Some(&id), None, 10)).unwrap_or_default();
        assert_eq!(history.len(), 1, "read-only rejections happen before execution and are not logged");
    }

    #[tokio::test]
    async fn destructive_requires_confirmation() {
        let (state, id) = state_with_sqlite(false).await;
        run(&state, &id, "CREATE TABLE t (a int)", false).await.unwrap_or_else(|e| panic!("{e}"));
        let err = run(&state, &id, "DELETE FROM t", false).await.err();
        assert!(matches!(err, Some(AppError::DestructiveConfirmationRequired { ref statements, .. }) if statements.len() == 1));
        assert!(run(&state, &id, "DELETE FROM t", true).await.is_ok());
    }

    #[tokio::test]
    async fn errors_are_logged_with_status_error() {
        let (state, id) = state_with_sqlite(false).await;
        assert!(run(&state, &id, "SELECT * FROM missing_table", false).await.is_err());
        let history = state.with_store(|s| s.list_history(Some(&id), None, 10)).unwrap_or_default();
        assert_eq!(history.first().map(|h| h.status), Some(HistoryStatus::Error));
    }

    #[test]
    fn bounds_clamp() {
        assert_eq!(clamp_page_limit(0), 1);
        assert_eq!(clamp_page_limit(5_000), MAX_PAGE_LIMIT);
        assert_eq!(clamp_result_rows(None), 1_000);
        assert_eq!(clamp_result_rows(Some(0)), 1);
    }
}
