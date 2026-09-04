// SOT: aws-sigv4, aws-request-signing, aws-credentials

use crate::error::{AppError, AppResult};
use crate::model::ResolvedConnection;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

// ============================================================================
// AWS SIGNATURE V4
//
// WHAT:  Signs requests to AWS services (DynamoDB, QLDB, S3) with the
//        connection's access key / secret. No AWS SDK: the services this app
//        talks to are plain HTTPS, and the SDK would add ~150 crates.
// WHY:   Keeping the signing in one audited module means the secret key is
//        only ever read here and never logged.
// HOW:   https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
// WHERE: src-tauri/src/integrations/{dynamodb,qldb,s3}.rs
// ============================================================================

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl AwsCredentials {
    /// host = region, username = access key id, secret = secret access key.
    /// A `secret` of the form `SECRET|SESSION_TOKEN` carries a temporary session token.
    pub fn from_connection(conn: &ResolvedConnection) -> AppResult<AwsCredentials> {
        let s = &conn.summary;
        let region = s
            .host
            .as_deref()
            .map(str::trim)
            .filter(|r| !r.is_empty())
            .ok_or_else(|| AppError::invalid_input("AWS region is required (e.g. us-east-1)."))?
            .to_string();
        let access_key = s
            .username
            .as_deref()
            .map(str::trim)
            .filter(|k| !k.is_empty())
            .ok_or_else(|| AppError::invalid_input("AWS access key ID is required."))?
            .to_string();
        let raw_secret = conn
            .secret
            .as_deref()
            .map(str::trim)
            .filter(|k| !k.is_empty())
            .ok_or_else(|| AppError::invalid_input("AWS secret access key is required."))?;
        let (secret_key, session_token) = match raw_secret.split_once('|') {
            Some((k, t)) if !t.trim().is_empty() => (k.trim().to_string(), Some(t.trim().to_string())),
            _ => (raw_secret.to_string(), None),
        };
        Ok(AwsCredentials { region, access_key, secret_key, session_token })
    }
}

fn hmac(key: &[u8], data: &[u8]) -> AppResult<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|e| AppError::crypto(e.to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

#[derive(Debug, Clone)]
pub struct SignedHeaders {
    pub headers: Vec<(String, String)>,
}

// WHAT:  Percent-encodes one path segment or query value the way SigV4 wants.
// WHY:   S3 keys carry spaces, `+` and unicode; the canonical request must use
//        the same encoding the server derives or every signature mismatches.
//        Unreserved characters are A-Z a-z 0-9 - _ . ~ and nothing else.
pub fn uri_encode(raw: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.as_bytes() {
        let c = *byte as char;
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~') {
            out.push(c);
        } else if c == '/' && !encode_slash {
            out.push('/');
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

// WHAT:  One request to sign. `query` is the canonical query string — keys
//        sorted, both sides already `uri_encode`d — or "" when there is none.
//        `content_type` is None for bodyless verbs so it is not signed.
// HOW:   Returns every header the request must carry (host, x-amz-date,
//        x-amz-target, content-type, x-amz-security-token, authorization).
pub struct SignRequest<'a> {
    pub service: &'a str,
    pub method: &'a str,
    pub host: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub amz_target: Option<&'a str>,
    pub content_type: Option<&'a str>,
    pub body: &'a [u8],
    pub now: chrono::DateTime<chrono::Utc>,
}

/// Signs a POST with a JSON body — the shape DynamoDB and QLDB use.
pub fn sign_post(creds: &AwsCredentials, req: &SignRequest<'_>) -> AppResult<SignedHeaders> {
    sign(creds, req)
}

pub fn sign(creds: &AwsCredentials, req: &SignRequest<'_>) -> AppResult<SignedHeaders> {
    let SignRequest { service, method, host, path, query, amz_target, content_type, body, now } = *req;
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let payload_hash = sha256_hex(body);

    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), host.into()),
        ("x-amz-content-sha256".into(), payload_hash.clone()),
        ("x-amz-date".into(), amz_date.clone()),
    ];
    if let Some(ct) = content_type {
        headers.push(("content-type".into(), ct.into()));
    }
    if let Some(t) = amz_target {
        headers.push(("x-amz-target".into(), t.into()));
    }
    if let Some(tok) = &creds.session_token {
        headers.push(("x-amz-security-token".into(), tok.clone()));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{}\n", v.trim())).collect();
    let signed_headers: String = headers.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let canonical_request = format!("{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    let scope = format!("{date_stamp}/{}/{service}/aws4_request", creds.region);
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(canonical_request.as_bytes()));

    let k_date = hmac(format!("AWS4{}", creds.secret_key).as_bytes(), date_stamp.as_bytes())?;
    let k_region = hmac(&k_date, creds.region.as_bytes())?;
    let k_service = hmac(&k_region, service.as_bytes())?;
    let k_signing = hmac(&k_service, b"aws4_request")?;
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes())?);

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key
    );
    headers.push(("authorization".into(), authorization));
    Ok(SignedHeaders { headers })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Known-answer test derived from the AWS "signing examples" (GET-less POST variant):
    // the derived signing key for the documented example credentials must match.
    #[test]
    fn signing_key_matches_aws_example() {
        let creds = AwsCredentials {
            region: "us-east-1".into(),
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let k_date = hmac(b"AWS4wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", b"20150830").unwrap_or_default();
        let k_region = hmac(&k_date, b"us-east-1").unwrap_or_default();
        let k_service = hmac(&k_region, b"iam").unwrap_or_default();
        let k_signing = hmac(&k_service, b"aws4_request").unwrap_or_default();
        assert_eq!(hex::encode(k_signing), "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9");
        let now = chrono::DateTime::parse_from_rfc3339("2015-08-30T12:36:00Z").map(|d| d.with_timezone(&chrono::Utc));
        let signed = sign_post(
            &creds,
            &SignRequest {
                service: "dynamodb",
                method: "POST",
                host: "dynamodb.us-east-1.amazonaws.com",
                path: "/",
                query: "",
                amz_target: Some("DynamoDB_20120810.ListTables"),
                content_type: Some("application/x-amz-json-1.0"),
                body: b"{}",
                now: now.unwrap_or_default(),
            },
        );
        let signed = signed.unwrap_or(SignedHeaders { headers: vec![] });
        let auth = signed.headers.iter().find(|(k, _)| k == "authorization").map(|(_, v)| v.clone()).unwrap_or_default();
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-target, Signature="));
        assert_eq!(auth.len(), auth.find("Signature=").unwrap_or(0) + "Signature=".len() + 64);
    }

    #[test]
    fn session_token_splits_from_secret() {
        use crate::model::{ConnectionSummary, Engine, Environment, SslMode};
        let conn = ResolvedConnection {
            summary: ConnectionSummary {
                id: "x".into(),
                name: "x".into(),
                engine: Engine::Dynamodb,
                environment: Environment::Local,
                read_only: false,
                host: Some("eu-west-1".into()),
                port: None,
                database: None,
                username: Some("AKIA".into()),
                file_path: None,
                ssl_mode: SslMode::Prefer,
                has_secret: true,
                created_at: String::new(),
                updated_at: String::new(),
            },
            secret: Some("SECRET|TOKEN".into()),
        };
        let c = AwsCredentials::from_connection(&conn).unwrap_or_else(|_| AwsCredentials { region: String::new(), access_key: String::new(), secret_key: String::new(), session_token: None });
        assert_eq!(c.secret_key, "SECRET");
        assert_eq!(c.session_token.as_deref(), Some("TOKEN"));
    }

    #[test]
    fn uri_encoding_follows_the_sigv4_rules() {
        // Unreserved characters pass through; everything else is %XX upper-case.
        assert_eq!(uri_encode("a-z_0.9~", true), "a-z_0.9~");
        assert_eq!(uri_encode("my key+1", true), "my%20key%2B1");
        // A path keeps its separators, an object key used as a value does not.
        assert_eq!(uri_encode("logs/2026/app.log", false), "logs/2026/app.log");
        assert_eq!(uri_encode("logs/2026/app.log", true), "logs%2F2026%2Fapp.log");
    }
}
