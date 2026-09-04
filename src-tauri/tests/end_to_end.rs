// SOT: end-to-end-engine-tests, full-stack-connection-flow

// An integration test *is* the place to panic: a failed assertion is the
// result. The crate-wide clippy denies apply here because tests/ is its own
// crate rather than a `#[cfg(test)]` module.
#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

//! End-to-end tests that drive an engine the way the app does: store a
//! connection, resolve it, connect through `integrations::connect`, then go
//! through the guard and the services the `#[tauri::command]`s call.
//!
//! Unit tests inside each adapter prove request/response shaping; these prove
//! the whole path a user's click takes — save → connect → browse → query —
//! including the read-only lock and the destructive-statement confirmation.
//!
//! Engines that need a server are gated on `DBFREE_TEST_<ENGINE>_URL` (see
//! scripts/live-tests.sh); SQLite and DuckDB run everywhere because they are
//! file-based, so this suite always has real coverage.

use db_free_lib::adapters::keyring::MemoryKeyProvider;
use db_free_lib::error::AppError;
use db_free_lib::guard;
use db_free_lib::model::{ConnectionInput, Engine, Environment, FilterOp, FilterRule, PageQuery, SslMode, StatementResult, TableRef};
use db_free_lib::services;
use db_free_lib::state::AppState;
use db_free_lib::store::Store;

struct Fixture {
    state: AppState,
    id: String,
    _dir: Option<std::path::PathBuf>,
}

// WHAT:  Saves the connection through the store and opens a session, exactly
//        like `commands::connections::connect` does.
async fn open(input: ConnectionInput, dir: Option<std::path::PathBuf>) -> Fixture {
    let store = Store::open_in_memory().unwrap_or_else(|e| panic!("store: {e}"));
    let secret = input.password.clone();
    let summary = store.insert_connection(&input, None).unwrap_or_else(|e| panic!("insert: {e}"));
    let state = AppState::new(store, Box::new(MemoryKeyProvider::default()));
    let resolved = db_free_lib::model::ResolvedConnection { summary: summary.clone(), secret };
    let integration = db_free_lib::integrations::connect(&resolved).await.unwrap_or_else(|e| panic!("connect: {e}"));
    state.insert_session(summary.id.clone(), integration).await;
    Fixture { state, id: summary.id, _dir: dir }
}

fn file_input(name: &str, engine: Engine, path: String, read_only: bool) -> ConnectionInput {
    ConnectionInput {
        name: name.into(),
        engine,
        environment: Environment::Local,
        read_only,
        host: None,
        port: None,
        database: None,
        username: None,
        password: None,
        file_path: Some(path),
        ssl_mode: SslMode::Disable,
    }
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("db-free-e2e-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("mkdir: {e}"));
    dir
}

async fn run_sql(f: &Fixture, sql: &str, confirm: bool) -> Result<Vec<StatementResult>, AppError> {
    guard::statement(
        &f.state,
        guard::StatementRequest { connection_id: &f.id, sql, confirm_destructive: confirm },
        |ctx| async move {
            let statements = ctx.integration.execute(sql, 1_000).await?;
            Ok(db_free_lib::model::QueryOutcome { statements, elapsed_ms: 0 })
        },
    )
    .await
    .map(|outcome| outcome.statements)
}

// WHAT:  The whole user journey against a real engine: create a table, insert,
//        browse it through the paging service, filter, and run a query.
async fn exercise(f: &Fixture, create: &str, insert: &str, table: TableRef) {
    run_sql(f, create, true).await.unwrap_or_else(|e| panic!("create: {e}"));
    run_sql(f, insert, true).await.unwrap_or_else(|e| panic!("insert: {e}"));

    // Catalog: the sidebar's tables panel.
    let catalog = guard::session(&f.state, &f.id, |ctx| async move { services::schema::catalog(&ctx).await })
        .await
        .unwrap_or_else(|e| panic!("catalog: {e}"));
    assert!(
        catalog.schemas.iter().any(|s| s.tables.iter().any(|t| t.name == table.name)),
        "table missing from catalog: {:?}",
        catalog.schemas.iter().flat_map(|s| s.tables.iter().map(|t| t.name.clone())).collect::<Vec<_>>()
    );

    // Page 1: the grid.
    let t = table.clone();
    let page = guard::session(&f.state, &f.id, |ctx| async move {
        services::data::table_page(&ctx, &t, &PageQuery { sort: vec![], filters: vec![], offset: 0, limit: 10 }).await
    })
    .await
    .unwrap_or_else(|e| panic!("page: {e}"));
    assert_eq!(page.rows.len(), 3, "{page:?}");
    assert!(page.columns.iter().any(|c| c.name == "name"), "{:?}", page.columns);

    // Filtered page: the filter builder.
    let t = table.clone();
    let filtered = guard::session(&f.state, &f.id, |ctx| async move {
        services::data::table_page(
            &ctx,
            &t,
            &PageQuery {
                sort: vec![],
                filters: vec![FilterRule { column: "name".into(), op: FilterOp::StartsWith, value: "a".into() }],
                offset: 0,
                limit: 10,
            },
        )
        .await
    })
    .await
    .unwrap_or_else(|e| panic!("filtered page: {e}"));
    assert_eq!(filtered.rows.len(), 2, "{filtered:?}");
    assert_eq!(filtered.total, Some(2));
    assert!(filtered.total_exact);

    // Query tab.
    let out = run_sql(f, &format!("SELECT name FROM {} ORDER BY name", table.name), false)
        .await
        .unwrap_or_else(|e| panic!("select: {e}"));
    match out.first() {
        Some(StatementResult::Rows { result }) => assert_eq!(result.rows.len(), 3, "{result:?}"),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[tokio::test]
async fn sqlite_full_journey_through_guard_and_services() {
    let dir = temp_dir("sqlite");
    let path = dir.join("app.db").to_string_lossy().into_owned();
    std::fs::File::create(&path).unwrap_or_else(|e| panic!("create file: {e}"));
    let f = open(file_input("sqlite", Engine::Sqlite, path, false), Some(dir)).await;
    exercise(
        &f,
        "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO people (id, name) VALUES (1, 'ada'), (2, 'alan'), (3, 'grace')",
        TableRef { schema: None, name: "people".into() },
    )
    .await;
}

#[tokio::test]
async fn duckdb_full_journey_through_guard_and_services() {
    let dir = temp_dir("duckdb");
    let path = dir.join("analytics.duckdb").to_string_lossy().into_owned();
    let f = open(file_input("duckdb", Engine::Duckdb, path, false), Some(dir)).await;
    exercise(
        &f,
        "CREATE TABLE people (id INTEGER PRIMARY KEY, name VARCHAR)",
        "INSERT INTO people (id, name) VALUES (1, 'ada'), (2, 'alan'), (3, 'grace')",
        TableRef { schema: Some("main".into()), name: "people".into() },
    )
    .await;
}

// WHAT:  The read-only lock must stop a write before it reaches the engine,
//        whichever engine it is.
#[tokio::test]
async fn read_only_lock_blocks_writes_on_every_file_engine() {
    for (engine, file, ddl) in [
        (Engine::Sqlite, "ro.db", "CREATE TABLE t (id INTEGER)"),
        (Engine::Duckdb, "ro.duckdb", "CREATE TABLE t (id INTEGER)"),
    ] {
        let dir = temp_dir("ro");
        let path = dir.join(file).to_string_lossy().into_owned();
        // Seed the database first: a read-only connection cannot create it.
        {
            let seed = open(file_input("seed", engine, path.clone(), false), None).await;
            run_sql(&seed, ddl, true).await.unwrap_or_else(|e| panic!("seed {engine:?}: {e}"));
        }
        let f = open(file_input("locked", engine, path, true), Some(dir)).await;
        let err = run_sql(&f, "INSERT INTO t (id) VALUES (1)", true).await.err();
        assert!(matches!(err, Some(AppError::ReadOnly { .. })), "{engine:?} allowed a write: {err:?}");
        // Reads still work.
        run_sql(&f, "SELECT * FROM t", false).await.unwrap_or_else(|e| panic!("read on {engine:?}: {e}"));
    }
}

// WHAT:  A destructive statement needs an explicit confirmation first.
#[tokio::test]
async fn destructive_statements_require_confirmation() {
    let dir = temp_dir("destructive");
    let path = dir.join("d.db").to_string_lossy().into_owned();
    std::fs::File::create(&path).unwrap_or_else(|e| panic!("create file: {e}"));
    let f = open(file_input("d", Engine::Sqlite, path, false), Some(dir)).await;
    run_sql(&f, "CREATE TABLE t (id INTEGER)", true).await.unwrap_or_else(|e| panic!("create: {e}"));

    let err = run_sql(&f, "DROP TABLE t", false).await.err();
    match err {
        Some(AppError::DestructiveConfirmationRequired { statements, .. }) => {
            assert_eq!(statements.len(), 1, "{statements:?}");
        }
        other => panic!("expected a confirmation prompt, got {other:?}"),
    }
    // Confirmed, it runs.
    run_sql(&f, "DROP TABLE t", true).await.unwrap_or_else(|e| panic!("confirmed drop: {e}"));
}

// WHAT:  Every engine in the registry must be reachable from the UI: a label,
//        a category, a form kind and a dispatch arm.
#[test]
fn every_engine_is_wired_end_to_end() {
    for engine in Engine::ALL {
        assert!(!engine.label().is_empty(), "{engine:?} has no label");
        assert!(!engine.as_str().is_empty(), "{engine:?} has no wire name");
        assert_eq!(Engine::parse(engine.as_str()), Some(engine), "{engine:?} does not round-trip");
        // `family()` and `kind()` are total matches; calling them proves every
        // variant is handled rather than falling through a wildcard.
        let _ = engine.family();
        let _ = engine.kind();
        let _ = engine.form();
        if engine.is_file_based() {
            assert_eq!(engine.default_port(), None, "{engine:?} is file based but has a port");
        }
    }
    assert_eq!(Engine::ALL.len(), 73, "Engine::ALL is out of step with the enum");
}
