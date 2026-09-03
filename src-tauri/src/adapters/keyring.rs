// SOT: master-key-provider, os-keychain, keyring-adapter, key-provider-trait

use crate::adapters::crypto::MasterKey;
use crate::error::{AppError, AppResult};
use base64::Engine as _;

// WHAT:  Where the master key comes from.
// WHY:   A trait so tests use an in-memory key and never touch the real keychain.
// WHERE: src-tauri/src/state.rs (cached once per process)
pub trait KeyProvider: Send + Sync {
    fn load_or_create(&self) -> AppResult<MasterKey>;
}

// WHAT:  macOS Keychain / Windows Credential Manager / Linux Secret Service.
pub struct OsKeyring {
    service: String,
    account: String,
}

impl OsKeyring {
    pub fn new(service: impl Into<String>, account: impl Into<String>) -> OsKeyring {
        OsKeyring { service: service.into(), account: account.into() }
    }
}

impl Default for OsKeyring {
    fn default() -> Self {
        OsKeyring::new("app.dbfree.desktop", "master-key")
    }
}

impl KeyProvider for OsKeyring {
    fn load_or_create(&self) -> AppResult<MasterKey> {
        let entry = keyring::Entry::new(&self.service, &self.account).map_err(AppError::keyring)?;
        match entry.get_password() {
            Ok(encoded) => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(encoded.trim())
                    .map_err(|_| AppError::keyring("stored master key is not valid base64"))?;
                MasterKey::from_bytes(&bytes)
            }
            Err(keyring::Error::NoEntry) => {
                let key = MasterKey::generate();
                let encoded = base64::engine::general_purpose::STANDARD.encode(key.as_bytes());
                entry.set_password(&encoded).map_err(AppError::keyring)?;
                Ok(key)
            }
            Err(err) => Err(AppError::keyring(err)),
        }
    }
}

// WHAT:  Test double. Generates once, hands back the same key for the process.
#[derive(Default)]
pub struct MemoryKeyProvider {
    key: std::sync::Mutex<Option<MasterKey>>,
}

impl KeyProvider for MemoryKeyProvider {
    fn load_or_create(&self) -> AppResult<MasterKey> {
        let mut slot = self
            .key
            .lock()
            .map_err(|_| AppError::internal("memory key provider poisoned"))?;
        Ok(slot.get_or_insert_with(MasterKey::generate).clone())
    }
}
