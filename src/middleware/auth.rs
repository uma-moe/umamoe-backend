use async_trait::async_trait;
use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::AppState;

/// Authenticated user extracted from a valid JWT `Authorization: Bearer <token>`
/// header **or** a valid `X-API-Key` header (resolved to the key's owner).
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: Uuid,
}

/// Rejection returned when the JWT is missing or invalid.
pub struct AuthRejection {
    message: String,
    status: StatusCode,
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        let body = Json(json!({
            "error": self.message,
            "status": self.status.as_u16()
        }));
        (self.status, body).into_response()
    }
}

#[async_trait]
impl FromRequestParts<AppState> for AuthenticatedUser {
    type Rejection = AuthRejection;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        // 1) Try JWT from Authorization header
        if let Some(header) = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
        {
            if let Some(token) = header.strip_prefix("Bearer ") {
                if let Ok(claims) = crate::auth::verify_token(token) {
                    return Ok(AuthenticatedUser {
                        user_id: claims.sub,
                    });
                }
            }
        }

        // 2) Try API key from X-API-Key header
        if let Some(raw_key) = parts
            .headers
            .get("X-API-Key")
            .and_then(|v| v.to_str().ok())
        {
            let mut hasher = Sha256::new();
            hasher.update(raw_key.as_bytes());
            let key_hash = format!("{:x}", hasher.finalize());

            let result = sqlx::query_scalar::<_, Uuid>(
                "SELECT user_id FROM api_keys WHERE key_hash = $1 AND revoked = FALSE",
            )
            .bind(&key_hash)
            .fetch_optional(&state.db)
            .await
            .map_err(|_| AuthRejection {
                message: "Internal error resolving API key".into(),
                status: StatusCode::INTERNAL_SERVER_ERROR,
            })?;

            if let Some(user_id) = result {
                return Ok(AuthenticatedUser { user_id });
            }
        }

        Err(AuthRejection {
            message: "Missing or invalid authentication (provide Authorization: Bearer <jwt> or X-API-Key header)".into(),
            status: StatusCode::UNAUTHORIZED,
        })
    }
}
