use crate::models::common::{naive_datetime_as_utc, option_naive_datetime_as_utc};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

// ── Database row types ──────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: Uuid,
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub created_at: NaiveDateTime,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct UserIdentity {
    pub id: i32,
    pub user_id: Uuid,
    pub provider: String,
    pub provider_user_id: String,
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub created_at: NaiveDateTime,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct LinkedAccount {
    pub id: i32,
    pub user_id: Uuid,
    pub account_id: String,
    pub verification_token: Option<String>,
    pub verification_status: String,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub verified_at: Option<NaiveDateTime>,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub created_at: NaiveDateTime,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub updated_at: NaiveDateTime,
}

// ── Request types ───────────────────────────────────────────────

fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    use std::fmt;

    struct StringOrNumber;
    impl<'de> Visitor<'de> for StringOrNumber {
        type Value = String;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a string or number")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<String, E> {
            Ok(v.to_owned())
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<String, E> {
            Ok(v.to_string())
        }
    }
    deserializer.deserialize_any(StringOrNumber)
}

#[derive(Debug, Deserialize)]
pub struct LinkAccountRequest {
    #[serde(deserialize_with = "deserialize_string_or_number")]
    pub account_id: String,
}

#[derive(Debug, Deserialize)]
pub struct VerifyAccountRequest {
    #[serde(deserialize_with = "deserialize_string_or_number")]
    pub account_id: String,
}

// ── Response types ──────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub providers: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct IdentityResponse {
    pub provider: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct LinkedAccountResponse {
    pub id: i32,
    pub account_id: String,
    pub verification_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_token: Option<String>,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub verified_at: Option<NaiveDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trainer_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub representative_uma_id: Option<i32>,
}

impl LinkedAccountResponse {
    pub fn from_linked(
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

#[derive(Debug, Serialize)]
pub struct LinkResponse {
    pub verification_token: String,
    pub account_id: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct VerifyResponse {
    pub status: String,
    pub account_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ── API Key types ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct CreateApiKeyResponse {
    pub id: Uuid,
    pub name: String,
    pub key: String, // only returned on creation
    pub key_prefix: String,
}

#[derive(Debug, Serialize, FromRow)]
pub struct ApiKeyResponse {
    pub id: Uuid,
    pub name: String,
    pub key_prefix: String,
    pub revoked: bool,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub last_used: Option<NaiveDateTime>,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub created_at: NaiveDateTime,
}
