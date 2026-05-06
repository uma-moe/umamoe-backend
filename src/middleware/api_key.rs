use axum::{
    extract::{ConnectInfo, Request, State},
    middleware::Next,
    response::Response,
};
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use tracing::{info, warn};
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ApiKeyRecord {
    pub id: Uuid,
    pub user_id: Uuid,
    pub name: String,
}

pub fn hash_api_key(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub async fn resolve_api_key(
    db: &sqlx::PgPool,
    raw_key: &str,
) -> Result<Option<ApiKeyRecord>, sqlx::Error> {
    let key_hash = hash_api_key(raw_key);

    sqlx::query_as::<_, ApiKeyRecord>(
        "SELECT id, user_id, name FROM api_keys WHERE key_hash = $1 AND revoked = FALSE",
    )
    .bind(&key_hash)
    .fetch_optional(db)
    .await
}

/// Middleware layer that optionally resolves an `X-API-Key` header.
///
/// * If the header is present and valid → updates `last_used`, bumps counters,
///   logs the request.
/// * If the header is absent or the key is unknown/revoked → the request
///   proceeds normally (no enforcement yet).
pub async fn api_key_tracking_middleware(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    request: Request,
    next: Next,
) -> Response {
    let ip = connect_info.map(|ci| ci.0.ip().to_string());
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    // Try to extract the API key from the header
    let api_key_raw = request
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Continue processing the request immediately — tracking is fire-and-forget
    let response = next.run(request).await;

    // After the response is produced, log usage asynchronously
    if let Some(raw_key) = api_key_raw {
        let db = state.db.clone();
        let endpoint = normalize_endpoint(&method.to_string(), &path);

        tokio::spawn(async move {
            let row = resolve_api_key(&db, &raw_key).await;

            match row {
                Ok(Some(key)) => {
                    info!(
                        "🔑 API key '{}' (user {}) → {} (ip: {})",
                        key.name,
                        key.user_id,
                        endpoint,
                        ip.as_deref().unwrap_or("unknown"),
                    );

                    // Bump counters (best-effort)
                    let _ = sqlx::query(
                        "UPDATE api_keys SET last_used = NOW(), total_requests = total_requests + 1 WHERE id = $1",
                    )
                    .bind(key.id)
                    .execute(&db)
                    .await;

                    let _ = sqlx::query(
                        r#"
                        INSERT INTO api_key_usage (api_key_id, endpoint, date, requests)
                        VALUES ($1, $2, CURRENT_DATE, 1)
                        ON CONFLICT (api_key_id, endpoint, date)
                        DO UPDATE SET requests = api_key_usage.requests + 1
                        "#,
                    )
                    .bind(key.id)
                    .bind(&endpoint)
                    .execute(&db)
                    .await;
                }
                Ok(None) => {
                    warn!(
                        "⚠️ Unknown or revoked API key used (prefix: {}) from ip {}",
                        &raw_key[..raw_key.len().min(14)],
                        ip.as_deref().unwrap_or("unknown"),
                    );
                }
                Err(e) => {
                    warn!("⚠️ Failed to look up API key: {}", e);
                }
            }
        });
    }

    response
}

/// Collapse path parameters into placeholders so endpoint grouping is useful.
/// `/api/v4/circles/123` → `GET /api/v4/circles/:id`
fn normalize_endpoint(method: &str, path: &str) -> String {
    let segments: Vec<&str> = path.split('/').collect();
    let normalized: Vec<&str> = segments
        .iter()
        .enumerate()
        .map(|(i, seg)| {
            // If this segment looks like a numeric ID or UUID, replace it
            if i > 0 && (is_numeric(seg) || is_uuid(seg)) {
                ":id"
            } else {
                seg
            }
        })
        .collect();
    format!("{} {}", method, normalized.join("/"))
}

fn is_numeric(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
}

fn is_uuid(s: &str) -> bool {
    s.len() == 36 && s.chars().all(|c| c.is_ascii_hexdigit() || c == '-')
}
