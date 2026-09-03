// SOT: app-error, error-kinds, ipc-error-shape, error-mapping

use serde::Serialize;
use std::fmt;
use ts_rs::TS;

// WHAT:  The single error type every layer returns and the UI receives.
// WHY:   One shape means the frontend can switch on `kind` exhaustively; a new
//        variant here is a compile error in TS after `pnpm bindings`.
// HOW:   Vendor errors (sqlx, rusqlite, keyring, aes-gcm) are mapped into it
//        inside their own adapter layer — never leaked past it.
// WHERE: src/lib/bindings/AppError.ts (generated), src/lib/ipc.ts (consumer)
#[derive(Debug, Clone, Serialize, PartialEq, Eq, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum AppError {
    NotFound { message: String },
    NotConnected { message: String },
    ReadOnly { message: String },
    DestructiveConfirmationRequired { message: String, statements: Vec<String> },
    InvalidInput { message: String },
    Timeout { message: String },
    Driver { message: String },
    Store { message: String },
    Crypto { message: String },
    Keyring { message: String },
    Internal { message: String },
}

impl AppError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound { message: message.into() }
    }
    pub fn not_connected(message: impl Into<String>) -> Self {
        Self::NotConnected { message: message.into() }
    }
    pub fn read_only(message: impl Into<String>) -> Self {
        Self::ReadOnly { message: message.into() }
    }
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput { message: message.into() }
    }
    pub fn invalid_input_from(err: impl fmt::Display) -> Self {
        Self::InvalidInput { message: err.to_string() }
    }
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout { message: message.into() }
    }
    pub fn driver(message: impl fmt::Display) -> Self {
        Self::Driver { message: message.to_string() }
    }
    pub fn store(message: impl fmt::Display) -> Self {
        Self::Store { message: message.to_string() }
    }
    pub fn crypto(message: impl fmt::Display) -> Self {
        Self::Crypto { message: message.to_string() }
    }
    pub fn keyring(message: impl fmt::Display) -> Self {
        Self::Keyring { message: message.to_string() }
    }
    pub fn internal(message: impl fmt::Display) -> Self {
        Self::Internal { message: message.to_string() }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::NotFound { message }
            | Self::NotConnected { message }
            | Self::ReadOnly { message }
            | Self::DestructiveConfirmationRequired { message, .. }
            | Self::InvalidInput { message }
            | Self::Timeout { message }
            | Self::Driver { message }
            | Self::Store { message }
            | Self::Crypto { message }
            | Self::Keyring { message }
            | Self::Internal { message } => message,
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for AppError {}

pub type AppResult<T> = Result<T, AppError>;
