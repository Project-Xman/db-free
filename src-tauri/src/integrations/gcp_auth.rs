// SOT: gcp-auth, google-service-account-jwt, oauth-access-token

use crate::error::{AppError, AppResult};
use crate::integrations::http::{Auth, HttpClient};
use crate::model::ResolvedConnection;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::{Duration, Instant};

// ============================================================================
// GOOGLE CLOUD AUTH
//
// WHAT:  Turns a service-account JSON key (pasted into the password field) into
//        a short-lived OAuth2 access token via the RS256 JWT bearer grant.
//        A raw `ya29.…` access token pasted instead is used as-is.
// WHY:   Firestore and BigQuery both need `Authorization: Bearer <token>`; the
//        official SDK would drag in gRPC. Tokens are cached and refreshed 60 s
//        before expiry.
// HOW:   https://developers.google.com/identity/protocols/oauth2/service-account
// WHERE: src-tauri/src/integrations/firestore.rs, src-tauri/src/integrations/bigquery.rs
// ============================================================================

#[derive(Debug, Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    token_uri: Option<String>,
}

#[derive(Debug, Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: i64,
    exp: i64,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Debug)]
enum Source {
    Static(String),
    ServiceAccount { key: Box<ServiceAccount>, scope: String },
}

#[derive(Debug)]
pub struct GcpAuth {
    source: Source,
    cached: Mutex<Option<(String, Instant)>>,
    /// Project id from the key file when the connection did not name one.
    pub project_hint: Option<String>,
}

impl GcpAuth {
    /// `scope` e.g. "https://www.googleapis.com/auth/datastore" or ".../bigquery".
    pub fn from_connection(conn: &ResolvedConnection, scope: &str) -> AppResult<GcpAuth> {
        let secret = conn.secret.as_deref().map(str::trim).unwrap_or_default();
        if secret.is_empty() {
            return Ok(GcpAuth { source: Source::Static(String::new()), cached: Mutex::new(None), project_hint: None });
        }
        if secret.starts_with('{') {
            let key: ServiceAccount =
                serde_json::from_str(secret).map_err(|e| AppError::invalid_input(format!("Service-account JSON is not valid: {e}")))?;
            let project_hint = key.project_id.clone();
            return Ok(GcpAuth { source: Source::ServiceAccount { key: Box::new(key), scope: scope.to_string() }, cached: Mutex::new(None), project_hint });
        }
        Ok(GcpAuth { source: Source::Static(secret.to_string()), cached: Mutex::new(None), project_hint: None })
    }

    pub fn is_anonymous(&self) -> bool {
        matches!(&self.source, Source::Static(t) if t.is_empty())
    }

    pub async fn token(&self) -> AppResult<String> {
        match &self.source {
            Source::Static(t) => Ok(t.clone()),
            Source::ServiceAccount { key, scope } => {
                if let Ok(guard) = self.cached.lock() {
                    if let Some((tok, until)) = guard.as_ref() {
                        if Instant::now() + Duration::from_secs(60) < *until {
                            return Ok(tok.clone());
                        }
                    }
                }
                let (tok, ttl) = exchange(key, scope).await?;
                if let Ok(mut guard) = self.cached.lock() {
                    *guard = Some((tok.clone(), Instant::now() + Duration::from_secs(ttl)));
                }
                Ok(tok)
            }
        }
    }

    pub async fn bearer(&self) -> AppResult<Auth> {
        let tok = self.token().await?;
        Ok(if tok.is_empty() { Auth::None } else { Auth::Bearer(tok) })
    }
}

async fn exchange(key: &ServiceAccount, scope: &str) -> AppResult<(String, u64)> {
    let token_uri = key.token_uri.clone().unwrap_or_else(|| "https://oauth2.googleapis.com/token".into());
    let now = chrono::Utc::now().timestamp();
    let claims = Claims { iss: &key.client_email, scope, aud: &token_uri, iat: now, exp: now + 3600 };
    let enc = EncodingKey::from_rsa_pem(key.private_key.as_bytes()).map_err(|e| AppError::invalid_input(format!("Service-account private key is not a valid RSA PEM: {e}")))?;
    let assertion = jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &enc).map_err(|e| AppError::crypto(e.to_string()))?;

    let client = HttpClient::new(token_uri.clone(), Auth::None, false)?;
    let form = format!("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={assertion}");
    let body = client.post_raw(&token_uri, "application/x-www-form-urlencoded", form, Some("application/json")).await?;
    let resp: TokenResponse = serde_json::from_str(&body).map_err(|e| AppError::driver(format!("Token response was not JSON: {e}")))?;
    Ok((resp.access_token, resp.expires_in.unwrap_or(3600)))
}
