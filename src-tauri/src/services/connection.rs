// SOT: connection-service, connection-lifecycle, secret-sealing, session-open-close

use crate::adapters::crypto;
use crate::guard::SessionCtx;
use crate::integrations::{self, SessionInfo};
use crate::error::{AppError, AppResult};
use crate::model::{ConnectionInput, ConnectionSummary, ResolvedConnection};
use crate::state::AppState;
use crate::store::connections::SecretChange;

pub fn list(state: &AppState) -> AppResult<Vec<ConnectionSummary>> {
    state.with_store(|store| store.list_connections())
}

// WHAT:  Creates or updates a connection, sealing the password before it is stored.
// WHY:   Passwords never touch disk in the clear (PRD 4.1).
// HOW:   Blank password on update = keep the existing secret.
// WHERE: src-tauri/src/adapters/crypto.rs
pub fn save(state: &AppState, id: Option<&str>, input: &ConnectionInput) -> AppResult<ConnectionSummary> {
    let sealed = match input.password.as_deref().filter(|p| !p.is_empty()) {
        Some(password) => Some(crypto::seal(state.master_key()?, password.as_bytes())?),
        None => None,
    };
    let stored = input.without_password();
    state.with_store(|store| match id {
        Some(id) => {
            let change = match sealed {
                Some(bytes) => SecretChange::Set(bytes),
                None => SecretChange::Keep,
            };
            store.update_connection(id, &stored, change)
        }
        None => store.insert_connection(&stored, sealed),
    })
}

pub async fn delete(state: &AppState, id: &str) -> AppResult<()> {
    state.remove_session(id).await;
    state.with_store(|store| store.delete_connection(id))
}

fn resolve(state: &AppState, id: &str) -> AppResult<ResolvedConnection> {
    let record = state.with_store(|store| store.get_connection_record(id))?;
    let secret = match record.secret_ciphertext {
        Some(blob) => {
            let bytes = crypto::open(state.master_key()?, &blob)?;
            Some(String::from_utf8(bytes).map_err(|_| AppError::crypto("stored secret is not UTF-8"))?)
        }
        None => None,
    };
    Ok(ResolvedConnection { summary: record.summary, secret })
}

// WHAT:  Opens an adapter session for a saved connection and registers it.
// HOW:   `database` overrides the saved default for this session only, which is
//        how the sidebar's database switcher works (reconnect, replace session).
pub async fn connect(state: &AppState, id: &str, database: Option<&str>) -> AppResult<ConnectionSummary> {
    let mut resolved = resolve(state, id)?;
    if let Some(db) = database.map(str::trim).filter(|d| !d.is_empty()) {
        resolved.summary.database = Some(db.to_string());
    }
    let integration = integrations::connect(&resolved).await?;
    integration.ping().await?;
    state.insert_session(id.to_string(), integration).await;
    Ok(resolved.summary)
}

pub async fn disconnect(state: &AppState, id: &str) -> AppResult<()> {
    state.remove_session(id).await;
    Ok(())
}

// WHAT:  "Test connection" for unsaved input. Reuses the stored secret when the
//        form left the password blank on an existing connection.
pub async fn test(state: &AppState, existing_id: Option<&str>, input: &ConnectionInput) -> AppResult<()> {
    let secret = match input.password.as_deref().filter(|p| !p.is_empty()) {
        Some(password) => Some(password.to_string()),
        None => match existing_id {
            Some(id) => resolve(state, id)?.secret,
            None => None,
        },
    };
    let resolved = ResolvedConnection {
        summary: ConnectionSummary::draft(input, secret.is_some()),
        secret,
    };
    let integration = integrations::connect(&resolved).await?;
    let ping = integration.ping().await;
    integration.close().await;
    ping
}

pub async fn active_sessions(state: &AppState) -> Vec<String> {
    state.session_ids().await
}

pub async fn describe(ctx: &SessionCtx) -> AppResult<SessionInfo> {
    integrations::describe(ctx.integration.as_ref()).await
}

// WHAT:  Engine of a saved connection without opening a session (SQL generation
//        needs the dialect before the guard resolves anything).
pub fn engine_of(state: &AppState, id: &str) -> AppResult<crate::model::Engine> {
    Ok(state.with_store(|store| store.get_connection(id))?.engine)
}
