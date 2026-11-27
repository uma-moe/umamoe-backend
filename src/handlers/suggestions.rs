//! Forwards site suggestions to a private Discord channel.
//!
//! Clients post here (gated by the browser-proof middleware and a per-IP rate
//! limit) instead of calling Discord themselves, so the webhook URL never ships
//! in the frontend bundle.

use axum::{
    extract::{ConnectInfo, State},
    http::{header::RETRY_AFTER, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::{net::SocketAddr, time::Duration};

use crate::{
    middleware::turnstile::{check_rate_limit, extract_client_ip},
    AppState,
};

/// Server-only secret: the Discord webhook that receives suggestions.
const WEBHOOK_URL_ENV: &str = "DISCORD_SUGGESTION_WEBHOOK_URL";
/// Discord rejects embed descriptions longer than 2000 characters.
const MESSAGE_MAX: usize = 2000;
/// Free-text metadata is capped so a client cannot pad the Discord embed.
const METADATA_MAX: usize = 512;
/// Per-IP submission cap, tighter than the middleware's global write limit.
const RATE_LIMIT_MAX: u32 = 3;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Category {
    Bug,
    Feature,
    Other,
}

impl Category {
    fn label(&self) -> &'static str {
        match self {
            Category::Bug => "🐞 Bug",
            Category::Feature => "💡 Feature",
            Category::Other => "💬 Other",
        }
    }

    fn color(&self) -> i64 {
        match self {
            Category::Bug => 0xef4444,
            Category::Feature => 0x3b82f6,
            Category::Other => 0x9ca3af,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SuggestionPayload {
    category: Category,
    message: String,
    #[serde(default)]
    route: Option<String>,
    #[serde(default)]
    version: Option<String>,
}

fn json_error(status: StatusCode, error: &str) -> Response {
    (
        status,
        Json(json!({
            "error": error,
            "status": status.as_u16()
        })),
    )
        .into_response()
}

fn rate_limited(retry_after: u64) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(RETRY_AFTER, retry_after.to_string())],
        Json(json!({
            "error": "rate_limited",
            "status": 429
        })),
    )
        .into_response()
}

fn clip(value: &str) -> String {
    value.trim().chars().take(METADATA_MAX).collect()
}

async fn forward_to_discord(
    state: &AppState,
    webhook_url: &str,
    payload: SuggestionPayload,
    user_agent: Option<&str>,
) -> Result<(), reqwest::Error> {
    let mut fields = Vec::new();
    if let Some(route) = payload.route.as_deref().map(clip) {
        if !route.is_empty() {
            fields.push(json!({ "name": "Route", "value": route, "inline": true }));
        }
    }
    if let Some(version) = payload.version.as_deref().map(clip) {
        if !version.is_empty() {
            fields.push(json!({ "name": "Version", "value": version, "inline": true }));
        }
    }
    if let Some(user_agent) = user_agent.map(clip) {
        if !user_agent.is_empty() {
            fields.push(json!({ "name": "User agent", "value": user_agent }));
        }
    }

    let body = json!({
        "embeds": [{
            "title": payload.category.label(),
            "description": payload.message,
            "color": payload.category.color(),
            "fields": fields,
            "timestamp": chrono::Utc::now().to_rfc3339()
        }]
    });

    state
        // Reuse the shared client so the outbound call inherits its connection pool and timeout.
        .search_client
        .post(webhook_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

pub async fn submit_suggestion(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(payload): Json<SuggestionPayload>,
) -> Response {
    let Ok(webhook_url) = std::env::var(WEBHOOK_URL_ENV) else {
        tracing::error!(
            "{} is not configured; suggestions cannot be delivered",
            WEBHOOK_URL_ENV
        );
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "suggestions_not_configured",
        );
    };

    let message = payload.message.trim().to_string();
    if message.is_empty() || message.chars().count() > MESSAGE_MAX {
        return json_error(StatusCode::BAD_REQUEST, "invalid_message");
    }

    // The turnstile middleware already enforces a global per-IP write limit; this
    // tighter per-route limit keeps one visitor from flooding the channel.
    let ip = extract_client_ip(&headers, Some(addr));
    if let Some(retry_after) = check_rate_limit(
        format!("suggestions:{ip}"),
        RATE_LIMIT_MAX,
        RATE_LIMIT_WINDOW,
    ) {
        return rate_limited(retry_after);
    }

    match forward_to_discord(
        &state,
        &webhook_url,
        payload,
        headers
            .get("user-agent")
            .and_then(|value| value.to_str().ok()),
    )
    .await
    {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(error) => {
            tracing::warn!("Failed to deliver suggestion to Discord: {error}");
            json_error(StatusCode::BAD_GATEWAY, "delivery_failed")
        }
    }
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/api/suggestions", post(submit_suggestion))
}
