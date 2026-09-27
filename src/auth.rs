pub use crate::types::auth::*;
use hmac::{Hmac, Mac};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::OnceLock;
use uuid::Uuid;

include!("types/auth_internal.rs");

/// Initialise the JWT secret. Called at startup if JWT_SECRET env var is set.
/// Auth endpoints will return errors if this was never called.
pub fn init(secret: String) {
    let _ = JWT_SECRET.set(secret);
}

/// Hash an email address with HMAC-SHA256 keyed on the JWT secret.
/// Emails are lowercased and trimmed before hashing for consistency.
/// The result is a 64-character hex string safe to store in place of the raw address.
pub fn hashEmail(email: &str) -> String {
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

/// Create a signed JWT for the given user. Expires in 7 days.
pub fn createToken(user_id: Uuid) -> jsonwebtoken::errors::Result<String> {
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
pub fn verifyToken(token: &str) -> jsonwebtoken::errors::Result<Claims> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret()?.as_bytes()),
        &Validation::default(),
    )?;
    Ok(data.claims)
}

/// One-time startup backfill: hash any plaintext emails still in the database.
/// Detection is simple — plaintext emails contain '@', hashes never do.
/// Runs at every startup but is effectively a no-op once all rows are hashed.
pub(crate) async fn backfillEmailHashes(pool: &sqlx::PgPool) {
    #[derive(sqlx::FromRow)]
    struct EmailRow {
        id: uuid::Uuid,
        email: String,
    }
    #[derive(sqlx::FromRow)]
    struct IdentityEmailRow {
        id: i32,
        email: String,
    }

    // Hash users.email
    let users: Vec<EmailRow> = match sqlx::query_as(
        "SELECT id, email FROM users WHERE email IS NOT NULL AND email LIKE '%@%'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!("⚠️  backfill_email_hashes: could not query users: {}", e);
            return;
        }
    };

    for row in &users {
        let hashed = crate::auth::hashEmail(&row.email);
        if let Err(e) = sqlx::query("UPDATE users SET email = $1 WHERE id = $2")
            .bind(&hashed)
            .bind(row.id)
            .execute(pool)
            .await
        {
            tracing::warn!(
                "⚠️  backfill_email_hashes: failed to update user {}: {}",
                row.id,
                e
            );
        }
    }

    // Hash user_identities.email
    let identities: Vec<IdentityEmailRow> = match sqlx::query_as(
        "SELECT id, email FROM user_identities WHERE email IS NOT NULL AND email LIKE '%@%'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                "⚠️  backfill_email_hashes: could not query user_identities: {}",
                e
            );
            return;
        }
    };

    for row in &identities {
        let hashed = crate::auth::hashEmail(&row.email);
        if let Err(e) = sqlx::query("UPDATE user_identities SET email = $1 WHERE id = $2")
            .bind(&hashed)
            .bind(row.id)
            .execute(pool)
            .await
        {
            tracing::warn!(
                "⚠️  backfill_email_hashes: failed to update identity {}: {}",
                row.id,
                e
            );
        }
    }

    let total = users.len() + identities.len();
    if total > 0 {
        tracing::warn!(
            "✅ backfill_email_hashes: hashed {} plaintext email(s)",
            total
        );
    }
}

impl LinkedAccountResponse {
    pub fn fromLinked(
        la: LinkedAccount,
        trainer_name: Option<String>,
        representative_uma_id: Option<i32>,
    ) -> Self {
        Self {
            id: la.id,
            account_id: la.account_id,
            verification_status: la.verification_status,
            verification_token: la.verification_token,
            verified_at: la.verified_at,
            trainer_name,
            representative_uma_id,
        }
    }
}
