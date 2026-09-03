// SOT: settings-service, app-settings-persistence, ai-key-sealing

use crate::adapters::crypto;
use crate::error::{AppError, AppResult};
use crate::model::AppSettings;
use crate::state::AppState;
use base64::Engine as _;

const APP_KEY: &str = "app";
const AI_KEY: &str = "ai_key";

// WHAT:  Loads settings, tolerating missing/unknown fields (serde defaults).
pub fn get(state: &AppState) -> AppResult<AppSettings> {
    state.with_store(|store| {
        let mut settings = match store.get_setting(APP_KEY)? {
            Some(raw) => serde_json::from_str::<AppSettings>(&raw).unwrap_or_default(),
            None => AppSettings::default(),
        };
        settings.ai.has_api_key = store.get_setting(AI_KEY)?.is_some();
        Ok(settings)
    })
}

// WHAT:  Persists settings; the AI key is sealed with the master key like passwords.
// HOW:   `api_key`: Some(non-empty) replaces, Some("") clears, None keeps.
pub fn save(state: &AppState, settings: &AppSettings, api_key: Option<&str>) -> AppResult<AppSettings> {
    let sealed = match api_key {
        Some(key) if !key.trim().is_empty() => Some(Some(crypto::seal(state.master_key()?, key.trim().as_bytes())?)),
        Some(_) => Some(None),
        None => None,
    };
    let mut stored = settings.clone();
    stored.ai.has_api_key = false;
    let raw = serde_json::to_string(&stored).map_err(AppError::internal)?;
    state.with_store(|store| {
        store.set_setting(APP_KEY, &raw)?;
        match sealed {
            Some(Some(bytes)) => store.set_setting(AI_KEY, &base64::engine::general_purpose::STANDARD.encode(bytes))?,
            Some(None) => store.delete_setting(AI_KEY)?,
            None => {}
        }
        Ok(())
    })?;
    get(state)
}

pub fn ai_api_key(state: &AppState) -> AppResult<Option<String>> {
    let sealed = state.with_store(|store| store.get_setting(AI_KEY))?;
    match sealed {
        Some(encoded) => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| AppError::crypto("stored AI key is corrupt"))?;
            let plain = crypto::open(state.master_key()?, &bytes)?;
            Ok(Some(String::from_utf8(plain).map_err(|_| AppError::crypto("stored AI key is not UTF-8"))?))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::keyring::MemoryKeyProvider;
    use crate::model::ExecutionMode;
    use crate::store::Store;

    #[test]
    fn settings_and_key_round_trip() {
        let state = AppState::new(Store::open_in_memory().unwrap_or_else(|e| panic!("{e}")), Box::new(MemoryKeyProvider::default()));
        let defaults = get(&state).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(defaults.execution_mode, ExecutionMode::Review);
        assert!(!defaults.ai.has_api_key);
        let mut next = defaults.clone();
        next.execution_mode = ExecutionMode::Direct;
        let saved = save(&state, &next, Some("sk-test")).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(saved.execution_mode, ExecutionMode::Direct);
        assert!(saved.ai.has_api_key);
        assert_eq!(ai_api_key(&state).unwrap_or_default(), Some("sk-test".into()));
        let kept = save(&state, &next, None).unwrap_or_else(|e| panic!("{e}"));
        assert!(kept.ai.has_api_key);
        let cleared = save(&state, &next, Some("")).unwrap_or_else(|e| panic!("{e}"));
        assert!(!cleared.ai.has_api_key);
    }
}
