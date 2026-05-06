use hmac::{Hmac, Mac};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::OnceLock;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

static JWT_SECRET: OnceLock<String> = OnceLock::new();

/// Initialise the JWT secret. Called at startup if JWT_SECRET env var is set.
/// Auth endpoints will return errors if this was never called.
pub fn init(secret: String) {
    let _ = JWT_SECRET.set(secret);
}

pub fn is_configured() -> bool {
    JWT_SECRET.get().is_some()
}

/// Hash an email address with HMAC-SHA256 keyed on the JWT secret.
/// Emails are lowercased and trimmed before hashing for consistency.
/// The result is a 64-character hex string safe to store in place of the raw address.
pub fn hash_email(email: &str) -> String {
    let key = JWT_SECRET.get().map(|s| s.as_bytes()).unwrap_or(b"");
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(email.to_lowercase().trim().as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn secret() -> Result<&'static str, jsonwebtoken::errors::Error> {
    JWT_SECRET.get().map(|s| s.as_str()).ok_or_else(|| {
        jsonwebtoken::errors::Error::from(jsonwebtoken::errors::ErrorKind::InvalidKeyFormat)
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: Uuid,  // user id
    pub exp: usize, // expiry (epoch seconds)
    pub iat: usize, // issued at
}

/// Create a signed JWT for the given user. Expires in 7 days.
pub fn create_token(user_id: Uuid) -> jsonwebtoken::errors::Result<String> {
    let now = chrono::Utc::now().timestamp() as usize;
    let claims = Claims {
        sub: user_id,
        exp: now + 7 * 24 * 3600,
        iat: now,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret()?.as_bytes()),
    )
}

/// Verify and decode a JWT, returning the claims.
pub fn verify_token(token: &str) -> jsonwebtoken::errors::Result<Claims> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret()?.as_bytes()),
        &Validation::default(),
    )?;
    Ok(data.claims)
}
