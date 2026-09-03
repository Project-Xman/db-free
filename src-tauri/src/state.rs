// SOT: app-state, sessions-registry, shared-handles, master-key-cache

use crate::adapters::crypto::MasterKey;
use crate::adapters::keyring::KeyProvider;
use crate::integrations::Integration;
use crate::error::{AppError, AppResult};
use crate::store::Store;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::RwLock;

// WHAT:  Everything a command can reach: the local store, live integration sessions,
//        and the master key provider.
// WHY:   One managed struct so the block (guard) resolves connections and
//        sessions from a single place.
// HOW:   Store ops are sub-millisecond and never held across an await.
//        Sessions are keyed by connection id; one integration per connection.
// WHERE: src-tauri/src/guard/mod.rs (the consumer), src-tauri/src/lib.rs (setup)
pub struct AppState {
    store: Mutex<Store>,
    sessions: RwLock<HashMap<String, Arc<dyn Integration>>>,
    keys: Box<dyn KeyProvider>,
    master_key: OnceLock<MasterKey>,
}

impl AppState {
    pub fn new(store: Store, keys: Box<dyn KeyProvider>) -> AppState {
        AppState {
            store: Mutex::new(store),
            sessions: RwLock::new(HashMap::new()),
            keys,
            master_key: OnceLock::new(),
        }
    }

    pub fn with_store<T>(&self, f: impl FnOnce(&Store) -> AppResult<T>) -> AppResult<T> {
        let guard = self
            .store
            .lock()
            .map_err(|_| AppError::internal("local store lock poisoned"))?;
        f(&guard)
    }

    pub fn master_key(&self) -> AppResult<&MasterKey> {
        if let Some(key) = self.master_key.get() {
            return Ok(key);
        }
        let key = self.keys.load_or_create()?;
        Ok(self.master_key.get_or_init(|| key))
    }

    pub async fn session(&self, connection_id: &str) -> Option<Arc<dyn Integration>> {
        self.sessions.read().await.get(connection_id).cloned()
    }

    pub async fn session_ids(&self) -> Vec<String> {
        self.sessions.read().await.keys().cloned().collect()
    }

    pub async fn insert_session(&self, connection_id: String, integration: Arc<dyn Integration>) {
        let previous = self.sessions.write().await.insert(connection_id, integration);
        if let Some(old) = previous {
            old.close().await;
        }
    }

    pub async fn remove_session(&self, connection_id: &str) -> Option<Arc<dyn Integration>> {
        let removed = self.sessions.write().await.remove(connection_id);
        if let Some(integration) = &removed {
            integration.close().await;
        }
        removed
    }
}
