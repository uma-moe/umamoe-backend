use axum::{
    extract::{Path, Query, State},
    response::{Json, Redirect},
    routing::{delete, get, post},
    Router,
};
use oauth2::{
    basic::BasicClient, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    EndpointNotSet, EndpointSet, RedirectUrl, Scope, TokenResponse, TokenUrl,
};
use rand::Rng;
use serde::Deserialize;
use serde_json::json;
use tracing::info;
use uuid::Uuid;

use crate::errors::AppError;
use crate::middleware::auth::AuthenticatedUser;
use crate::models::auth::{
    ApiKeyResponse, BookmarkEntry, CreateApiKeyRequest, CreateApiKeyResponse, IdentityResponse,
    LinkAccountRequest, LinkResponse, LinkedAccountResponse, UserResponse, VerifyAccountRequest,
    VerifyResponse,
};
use crate::models::{Inheritance, SupportCard, UnifiedAccountRecord};
use crate::AppState;

// ── Types ───────────────────────────────────────────────────────

/// OAuth client with auth URL and token URL configured (type-state).
type OAuthClient =
    BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;

/// Simple error type for the OAuth HTTP client adapter.
#[derive(Debug)]
struct OAuthHttpError(String);

impl std::fmt::Display for OAuthHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for OAuthHttpError {}

// ── Router ──────────────────────────────────────────────────────

pub fn public_router() -> Router<AppState> {
    Router::new()
        .route(
            "/browser-proof",
            post(crate::middleware::turnstile::exchange_browser_proof),
        )
        .route("/login/:provider", get(login))
        .route("/callback/:provider", get(callback))
}

pub fn authenticated_router() -> Router<AppState> {
    Router::new()
        .route("/me", get(get_me))
        .route("/identities", get(list_identities))
        .route("/connect/:provider", get(connect_provider))
        .route("/connect/callback/:provider", get(connect_callback))
        .route("/disconnect/:provider", delete(disconnect_provider))
        .route("/accounts", get(list_accounts))
        .route("/link", post(link_account))
        .route("/verify", post(verify_account))
        .route("/link/:account_id", delete(unlink_account))
        .route("/api-keys", get(list_api_keys).post(create_api_key))
        .route("/api-keys/:key_id", delete(revoke_api_key))
        .route("/bookmarks", get(list_bookmarks))
        .route("/bookmarks/bulk-delete", post(bulk_remove_bookmarks))
        .route(
            "/bookmarks/:account_id",
            post(add_bookmark).delete(remove_bookmark),
        )
}

// ── OAuth helpers ───────────────────────────────────────────────

struct ProviderConfig {
    auth_url: &'static str,
    token_url: &'static str,
    scopes: Vec<&'static str>,
    userinfo_url: &'static str,
}

fn provider_config(provider: &str) -> Result<ProviderConfig, AppError> {
    match provider {
        "google" => Ok(ProviderConfig {
            auth_url: "https://accounts.google.com/o/oauth2/v2/auth",
            token_url: "https://oauth2.googleapis.com/token",
            scopes: vec!["openid", "profile", "email"],
            userinfo_url: "https://www.googleapis.com/oauth2/v3/userinfo",
        }),
        "discord" => Ok(ProviderConfig {
            auth_url: "https://discord.com/api/oauth2/authorize",
            token_url: "https://discord.com/api/oauth2/token",
            scopes: vec!["identify", "email"],
            userinfo_url: "https://discord.com/api/v10/users/@me",
        }),
        "apple" => Ok(ProviderConfig {
            auth_url: "https://appleid.apple.com/auth/authorize",
            token_url: "https://appleid.apple.com/auth/token",
            scopes: vec!["name", "email"],
            userinfo_url: "", // Apple uses ID token, not a userinfo endpoint
        }),
        _ => Err(AppError::BadRequest(format!(
            "Unknown provider: {}. Supported: google, discord, apple",
            provider
        ))),
    }
}

fn build_oauth_client(
    provider: &str,
    config: &ProviderConfig,
    state: &AppState,
) -> Result<OAuthClient, AppError> {
    let env_prefix = provider.to_uppercase();
    let client_id = std::env::var(format!("{}_CLIENT_ID", env_prefix))
        .map_err(|_| AppError::BadRequest(format!("{}_CLIENT_ID not configured", env_prefix)))?;
    let client_secret = std::env::var(format!("{}_CLIENT_SECRET", env_prefix)).map_err(|_| {
        AppError::BadRequest(format!("{}_CLIENT_SECRET not configured", env_prefix))
    })?;

    let redirect_url = format!(
        "{}/api/auth/callback/{}",
        state.oauth_redirect_base, provider
    );

    let client = BasicClient::new(ClientId::new(client_id))
        .set_client_secret(ClientSecret::new(client_secret))
        .set_auth_uri(
            AuthUrl::new(config.auth_url.to_owned())
                .map_err(|e| AppError::BadRequest(format!("Invalid auth URL: {}", e)))?,
        )
        .set_token_uri(
            TokenUrl::new(config.token_url.to_owned())
                .map_err(|e| AppError::BadRequest(format!("Invalid token URL: {}", e)))?,
        )
        .set_redirect_uri(
            RedirectUrl::new(redirect_url)
                .map_err(|e| AppError::BadRequest(format!("Invalid redirect URL: {}", e)))?,
        );

    Ok(client)
}

/// Parsed userinfo from any provider.
struct ProviderUserInfo {
    provider_user_id: String,
    display_name: Option<String>,
    email: Option<String>,
    avatar_url: Option<String>,
}

/// Fetch user profile from the provider's userinfo endpoint.
async fn fetch_userinfo(
    provider: &str,
    access_token: &str,
    http_client: &reqwest::Client,
    config: &ProviderConfig,
) -> Result<ProviderUserInfo, AppError> {
    match provider {
        "google" => {
            let resp: serde_json::Value = http_client
                .get(config.userinfo_url)
                .bearer_auth(access_token)
                .send()
                .await
                .map_err(|e| {
                    AppError::BadRequest(format!("Google userinfo request failed: {}", e))
                })?
                .json()
                .await
                .map_err(|e| {
                    AppError::BadRequest(format!("Google userinfo parse failed: {}", e))
                })?;

            Ok(ProviderUserInfo {
                provider_user_id: resp["sub"].as_str().unwrap_or_default().to_owned(),
                display_name: resp["name"].as_str().map(String::from),
                email: resp["email"].as_str().map(String::from),
                avatar_url: resp["picture"].as_str().map(String::from),
            })
        }
        "discord" => {
            let resp: serde_json::Value = http_client
                .get(config.userinfo_url)
                .bearer_auth(access_token)
                .send()
                .await
                .map_err(|e| {
                    AppError::BadRequest(format!("Discord userinfo request failed: {}", e))
                })?
                .json()
                .await
                .map_err(|e| {
                    AppError::BadRequest(format!("Discord userinfo parse failed: {}", e))
                })?;

            let discord_id = resp["id"].as_str().unwrap_or_default().to_owned();
            let avatar_hash = resp["avatar"].as_str();
            let avatar_url = avatar_hash.map(|h| {
                format!(
                    "https://cdn.discordapp.com/avatars/{}/{}.png",
                    discord_id, h
                )
            });

            Ok(ProviderUserInfo {
                provider_user_id: discord_id,
                display_name: resp["global_name"]
                    .as_str()
                    .or(resp["username"].as_str())
                    .map(String::from),
                email: resp["email"].as_str().map(String::from),
                avatar_url,
            })
        }
        "apple" => {
            // Apple Sign-In requires decoding the id_token JWT and special client_secret
            // generation. This needs a custom token response type. For now, return an error
            // directing users to use Google or Discord.
            Err(AppError::BadRequest(
                "Apple Sign-In is not yet supported. Please use Google or Discord.".into(),
            ))
        }
        _ => Err(AppError::BadRequest("Unsupported provider".into())),
    }
}

/// Generate a 60-character random verification token.
/// Tokens are checked against the game's profanity filter to ensure
/// users can actually save them in their profile description.
fn generate_verification_token() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no 0/O/1/I to avoid confusion
    let mut rng = rand::thread_rng();

    loop {
        let token: String = (0..60)
            .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
            .collect();

        if !token_hits_profanity_filter(&token) {
            return token;
        }
    }
}

/// Check if a token would be flagged by the game's profanity filter.
/// The game normalizes text to lowercase alphanumeric and does substring
/// matching. "High" words flag anywhere; "low" words flag only when
/// adjacent to a space — since the token is surrounded by spaces in a
/// profile description, we check start/end for those.
fn token_hits_profanity_filter(token: &str) -> bool {
    const BLOCKED_ANYWHERE: &[&str] = &[
        "cuck", "puta", "ubre", "naga", "69", "terf", "hate", "anal", "buta", "anus", "twat",
        "tata", "tard", "smut", "suck", "phuq", "muff", "cbt", "gay", "gei", "jcb", "jew", "pud",
        "baka", "damn", "debu", "gash", "jerk", "bum", "xx", "jj", "3p", "sex", "cul", "c8",
        // include words with chars outside our charset for future-proofing
        "spik", "binge", "sodo", "chie", "piss", "roa", "ago", "follo", "k1ll", "rading", "arei",
        "pedo", "injun", "assi", "unti", "shota", "puto", "gringo", "gaiji", "chinc", "insin",
        "ombo", "nabo", "kuso", "kick", "keto", "jot", "omg", "omu", "poa", "boob", "bomb", "dick",
        "cock", "isis", "aho", "ifica", "tits", "inko", "impo", "etti",
    ];
    const BLOCKED_BOUNDARY: &[&str] = &[
        "jap", "ntr", "sag", "butt", "fuk", "ass", "sm", "abo", "kill", "tit", "shine", "ho", "hit",
    ];

    let lower = token.to_ascii_lowercase();

    BLOCKED_ANYWHERE.iter().any(|w| lower.contains(w))
        || BLOCKED_BOUNDARY
            .iter()
            .any(|w| lower.starts_with(w) || lower.ends_with(w))
}

// ── Helpers ─────────────────────────────────────────────────────

/// Resolve the frontend base URL for post-auth redirects.
///
/// If `origin` is supplied and its host matches one of the entries in
/// `ALLOWED_ORIGINS` (the same list used by CORS), we redirect there.
/// Otherwise we fall back to the `FRONTEND_URL` env var (default: `https://uma.moe`).
///
/// This prevents open-redirect attacks while still letting beta/staging
/// deployments pass `?origin=https://beta.uma.moe`.
fn resolve_frontend_url(origin: Option<&str>) -> String {
    let default = std::env::var("FRONTEND_URL").unwrap_or_else(|_| "https://uma.moe".to_string());

    let Some(raw) = origin else {
        return default;
    };

    // Parse the candidate origin so we can compare hosts safely.
    let Ok(candidate) = raw.parse::<axum::http::Uri>() else {
        return default;
    };
    let candidate_host = match candidate.host() {
        Some(h) => h.to_ascii_lowercase(),
        None => return default,
    };

    let allowed = std::env::var("ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "https://uma.moe,https://www.uma.moe".to_string());

    let is_allowed = allowed.split(',').any(|entry| {
        let trimmed = entry.trim();
        if let Ok(u) = trimmed.parse::<axum::http::Uri>() {
            u.host().map(|h| h.to_ascii_lowercase()).as_deref() == Some(candidate_host.as_str())
        } else {
            false
        }
    });

    if is_allowed {
        raw.trim_end_matches('/').to_string()
    } else {
        default
    }
}

// ── Handlers ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct LoginParams {
    pub origin: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    pub code: String,
    pub state: String,
}

/// GET /api/auth/login/:provider — redirect user to SSO provider
async fn login(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Query(params): Query<LoginParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    let config = provider_config(&provider)?;
    let client = build_oauth_client(&provider, &config, &state)?;

    let mut auth_request = client.authorize_url(CsrfToken::new_random);
    for scope in &config.scopes {
        auth_request = auth_request.add_scope(Scope::new(scope.to_string()));
    }
    let (auth_url, csrf_state) = auth_request.url();

    // Store CSRF state in cache (5 min TTL).
    // Value format: "<provider>" or "<provider>|<origin>" when an origin is given.
    let cache_val = match params.origin.as_deref() {
        Some(origin) => format!("{}|{}", provider, origin),
        None => provider.clone(),
    };
    let _ = crate::cache::set(
        &format!("oauth_state:{}", csrf_state.secret()),
        &cache_val,
        std::time::Duration::from_secs(300),
    );

    info!("🔑 OAuth login initiated for provider: {}", provider);

    Ok(Json(json!({
        "url": auth_url.to_string(),
        "state": csrf_state.secret().clone()
    })))
}

/// GET /api/auth/callback/:provider — handle OAuth callback
async fn callback(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Query(params): Query<CallbackParams>,
) -> Result<Redirect, AppError> {
    // Validate CSRF state
    let cached_val: Option<String> = crate::cache::get(&format!("oauth_state:{}", params.state));
    let (cached_provider, login_origin) = match cached_val.as_deref() {
        Some(v) => {
            if let Some((p, o)) = v.split_once('|') {
                (p.to_string(), Some(o.to_string()))
            } else {
                (v.to_string(), None)
            }
        }
        None => {
            return Err(AppError::BadRequest(
                "Invalid or expired OAuth state".into(),
            ));
        }
    };
    if cached_provider != provider {
        return Err(AppError::BadRequest(
            "Invalid or expired OAuth state".into(),
        ));
    }
    // Invalidate used state
    crate::cache::invalidate(&format!("oauth_state:{}", params.state));

    let config = provider_config(&provider)?;
    let client = build_oauth_client(&provider, &config, &state)?;

    // Exchange code for token
    let reqwest_client = state.search_client.clone();
    let http_client = |req: oauth2::HttpRequest| {
        let client = reqwest_client.clone();
        async move {
            let resp = client
                .request(req.method().clone(), req.uri().to_string())
                .headers(req.headers().clone())
                .body(req.into_body())
                .send()
                .await
                .map_err(|e| OAuthHttpError(format!("Request failed: {}", e)))?;
            let status = resp.status();
            let headers = resp.headers().clone();
            let body = resp
                .bytes()
                .await
                .map_err(|e| OAuthHttpError(format!("Body read failed: {}", e)))?;
            let mut http_resp = axum::http::Response::new(body.to_vec());
            *http_resp.status_mut() = status;
            *http_resp.headers_mut() = headers;
            Ok::<_, OAuthHttpError>(http_resp)
        }
    };

    let token_response = client
        .exchange_code(AuthorizationCode::new(params.code))
        .request_async(&http_client)
        .await
        .map_err(|e| AppError::BadRequest(format!("Token exchange failed: {}", e)))?;

    let access_token = token_response.access_token().secret().to_owned();

    // Fetch user profile from provider
    let userinfo = fetch_userinfo(&provider, &access_token, &state.search_client, &config).await?;

    if userinfo.provider_user_id.is_empty() {
        return Err(AppError::BadRequest(
            "Provider did not return a user ID".into(),
        ));
    }

    // Upsert: look up existing identity or create new user + identity
    let existing_user_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT user_id FROM user_identities WHERE provider = $1 AND provider_user_id = $2",
    )
    .bind(&provider)
    .bind(&userinfo.provider_user_id)
    .fetch_optional(&state.db)
    .await?;

    let user_id = if let Some(uid) = existing_user_id {
        // Update identity profile info
        sqlx::query(
            r#"
            UPDATE user_identities SET
                display_name = COALESCE($1, display_name),
                email = COALESCE($2, email),
                avatar_url = COALESCE($3, avatar_url),
                updated_at = NOW()
            WHERE provider = $4 AND provider_user_id = $5
            "#,
        )
        .bind(&userinfo.display_name)
        .bind(userinfo.email.as_deref().map(crate::auth::hash_email))
        .bind(&userinfo.avatar_url)
        .bind(&provider)
        .bind(&userinfo.provider_user_id)
        .execute(&state.db)
        .await?;

        // Also update the user's own profile with the latest info
        sqlx::query(
            r#"
            UPDATE users SET
                display_name = COALESCE($1, display_name),
                avatar_url = COALESCE($2, avatar_url),
                updated_at = NOW()
            WHERE id = $3
            "#,
        )
        .bind(&userinfo.display_name)
        .bind(&userinfo.avatar_url)
        .bind(uid)
        .execute(&state.db)
        .await?;

        uid
    } else {
        // Check if a user with the same email already exists (auto-merge by email)
        let existing_by_email = if let Some(ref email) = userinfo.email {
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM users WHERE email = $1")
                .bind(crate::auth::hash_email(email))
                .fetch_optional(&state.db)
                .await?
        } else {
            None
        };

        if let Some(existing_uid) = existing_by_email {
            // Link this new SSO identity to the existing user
            sqlx::query(
                r#"
                INSERT INTO user_identities (user_id, provider, provider_user_id, display_name, email, avatar_url)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(existing_uid)
            .bind(&provider)
            .bind(&userinfo.provider_user_id)
            .bind(&userinfo.display_name)
            .bind(userinfo.email.as_deref().map(crate::auth::hash_email))
            .bind(&userinfo.avatar_url)
            .execute(&state.db)
            .await?;

            info!(
                "🔗 Auto-linked {} identity to existing user {} by email",
                provider, existing_uid
            );

            existing_uid
        } else {
            // Create new user + identity in a transaction
            let mut tx = state.db.begin().await?;

            let new_user_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                INSERT INTO users (display_name, email, avatar_url)
                VALUES ($1, $2, $3)
                RETURNING id
                "#,
            )
            .bind(&userinfo.display_name)
            .bind(userinfo.email.as_deref().map(crate::auth::hash_email))
            .bind(&userinfo.avatar_url)
            .fetch_one(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO user_identities (user_id, provider, provider_user_id, display_name, email, avatar_url)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(new_user_id)
            .bind(&provider)
            .bind(&userinfo.provider_user_id)
            .bind(&userinfo.display_name)
            .bind(userinfo.email.as_deref().map(crate::auth::hash_email))
            .bind(&userinfo.avatar_url)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            new_user_id
        }
    };

    // Issue JWT
    let token = crate::auth::create_token(user_id)
        .map_err(|e| AppError::BadRequest(format!("Failed to create token: {}", e)))?;

    info!(
        "🔑 User authenticated via {}: {} ({})",
        provider,
        user_id,
        userinfo.display_name.as_deref().unwrap_or("unknown")
    );

    // Redirect to frontend with token
    let frontend_url = resolve_frontend_url(login_origin.as_deref());
    let redirect_url = format!(
        "{}/auth/callback?token={}",
        frontend_url,
        urlencoding::encode(&token)
    );

    Ok(Redirect::temporary(&redirect_url))
}

/// GET /api/auth/me — return current authenticated user
async fn get_me(
    user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<UserResponse>, AppError> {
    let row = sqlx::query_as::<_, crate::models::auth::User>("SELECT * FROM users WHERE id = $1")
        .bind(user.user_id)
        .fetch_optional(&state.db)
        .await?
        .ok_or_else(|| AppError::NotFound("User not found".into()))?;

    let providers = sqlx::query_scalar::<_, String>(
        "SELECT provider FROM user_identities WHERE user_id = $1 ORDER BY created_at",
    )
    .bind(user.user_id)
    .fetch_all(&state.db)
    .await?;

    Ok(Json(UserResponse {
        id: row.id,
        display_name: row.display_name,
        avatar_url: row.avatar_url,
        providers,
    }))
}

/// GET /api/auth/accounts — list linked accounts for current user
async fn list_accounts(
    user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<LinkedAccountResponse>>, AppError> {
    // Rotate verification tokens for pending accounts
    let pending_ids = sqlx::query_scalar::<_, i32>(
        "SELECT id FROM linked_accounts WHERE user_id = $1 AND verification_status = 'pending'",
    )
    .bind(user.user_id)
    .fetch_all(&state.db)
    .await?;

    for id in &pending_ids {
        let new_token = generate_verification_token();
        sqlx::query(
            "UPDATE linked_accounts SET verification_token = $1, updated_at = NOW() WHERE id = $2",
        )
        .bind(&new_token)
        .bind(id)
        .execute(&state.db)
        .await?;
    }

    let rows = sqlx::query_as::<_, crate::models::auth::LinkedAccount>(
        "SELECT * FROM linked_accounts WHERE user_id = $1 ORDER BY created_at",
    )
    .bind(user.user_id)
    .fetch_all(&state.db)
    .await?;

    // Enrich with trainer name and representative uma from DB
    let mut responses = Vec::with_capacity(rows.len());
    for la in rows {
        #[derive(sqlx::FromRow)]
        struct TrainerExtra {
            name: String,
            main_parent_id: Option<i32>,
        }
        let extra = sqlx::query_as::<_, TrainerExtra>(
            r#"
            SELECT t.name, i.main_parent_id
            FROM trainer t
            LEFT JOIN inheritance i ON i.account_id = t.account_id
            WHERE t.account_id = $1
            "#,
        )
        .bind(&la.account_id)
        .fetch_optional(&state.db)
        .await?;

        let (trainer_name, representative_uma_id) = match extra {
            Some(e) => (Some(e.name), e.main_parent_id),
            None => (None, None),
        };
        responses.push(LinkedAccountResponse::from_linked(
            la,
            trainer_name,
            representative_uma_id,
        ));
    }

    Ok(Json(responses))
}

/// POST /api/auth/link — start linking a trainer account
async fn link_account(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<LinkAccountRequest>,
) -> Result<Json<LinkResponse>, AppError> {
    let account_id = payload.account_id.trim().to_string();

    if account_id.is_empty() {
        return Err(AppError::BadRequest("account_id is required".into()));
    }

    // Check if another user already has this account verified
    let existing = sqlx::query_scalar::<_, Uuid>(
        "SELECT user_id FROM linked_accounts WHERE account_id = $1 AND verification_status = 'verified' AND user_id != $2",
    )
    .bind(&account_id)
    .bind(user.user_id)
    .fetch_optional(&state.db)
    .await?;

    if existing.is_some() {
        return Err(AppError::BadRequest(
            "This account is already verified by another user".into(),
        ));
    }

    let token = generate_verification_token();

    // Upsert linked account (if user re-links same account, reset verification)
    sqlx::query(
        r#"
        INSERT INTO linked_accounts (user_id, account_id, verification_token, verification_status)
        VALUES ($1, $2, $3, 'pending')
        ON CONFLICT (user_id, account_id)
        DO UPDATE SET
            verification_token = $3,
            verification_status = 'pending',
            verified_at = NULL,
            updated_at = NOW()
        "#,
    )
    .bind(user.user_id)
    .bind(&account_id)
    .bind(&token)
    .execute(&state.db)
    .await?;

    info!(
        "🔗 User {} started linking account {}",
        user.user_id, account_id
    );

    Ok(Json(LinkResponse {
        verification_token: token,
        account_id,
        status: "pending".into(),
    }))
}

/// POST /api/auth/verify — trigger bot verification and poll for result
async fn verify_account(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<VerifyAccountRequest>,
) -> Result<Json<VerifyResponse>, AppError> {
    let account_id = payload.account_id.trim().to_string();

    // Cooldown: 30 seconds between verify attempts per account
    let cooldown_key = format!("verify_cooldown:{}:{}", user.user_id, account_id);
    if crate::cache::get::<bool>(&cooldown_key).is_some() {
        return Err(AppError::BadRequest(
            "Please wait 30 seconds before trying again.".into(),
        ));
    }
    let _ = crate::cache::set(&cooldown_key, &true, std::time::Duration::from_secs(30));

    // Look up the pending linked account
    let linked = sqlx::query_as::<_, crate::models::auth::LinkedAccount>(
        "SELECT * FROM linked_accounts WHERE user_id = $1 AND account_id = $2 AND verification_status = 'pending'",
    )
    .bind(user.user_id)
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or_else(|| {
        AppError::NotFound("No pending linked account found. Please link the account first.".into())
    })?;

    let verification_token = linked.verification_token.clone().unwrap_or_default();

    // Create a verification task for bots to pick up.
    // Store account_id + linked_account_id to pin the exact row,
    // and the current verification_token so the bot can match it.
    sqlx::query(
        r#"
        INSERT INTO tasks (task_type, task_data, priority, status, created_at)
        VALUES ('verify/account', $1, -1, 'pending', NOW())
        "#,
    )
    .bind(json!({
        "account_id": &account_id,
        "linked_account_id": linked.id,
        "verification_token": &verification_token,
    }))
    .execute(&state.db)
    .await?;

    info!(
        "🔍 Verification task created for user {} account {}",
        user.user_id, account_id
    );

    // Poll for up to 60 seconds (30 checks × 2s interval)
    for _ in 0..30 {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let status: Option<(String,)> =
            sqlx::query_as("SELECT verification_status FROM linked_accounts WHERE id = $1")
                .bind(linked.id)
                .fetch_optional(&state.db)
                .await?;

        match status {
            Some((ref s,)) if s == "verified" => {
                info!(
                    "✅ Account {} verified for user {}",
                    account_id, user.user_id
                );
                return Ok(Json(VerifyResponse {
                    status: "verified".into(),
                    account_id,
                    message: None,
                }));
            }
            Some((ref s,)) if s == "failed" => {
                // Rotate the token so the user gets a fresh one on retry
                let new_token = generate_verification_token();
                sqlx::query(
                    "UPDATE linked_accounts SET verification_token = $1, verification_status = 'pending', updated_at = NOW() WHERE id = $2",
                )
                .bind(&new_token)
                .bind(linked.id)
                .execute(&state.db)
                .await?;

                return Ok(Json(VerifyResponse {
                    status: "failed".into(),
                    account_id,
                    message: Some(
                        "Token not found in profile description. A new token has been generated — please update your profile and try again."
                            .into(),
                    ),
                }));
            }
            _ => continue,
        }
    }

    // Timeout
    Ok(Json(VerifyResponse {
        status: "timeout".into(),
        account_id,
        message: Some(
            "Verification timed out. Please ensure the token is in your profile description and try again."
                .into(),
        ),
    }))
}

/// DELETE /api/auth/link/:account_id — unlink an account
async fn unlink_account(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Path(account_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = sqlx::query("DELETE FROM linked_accounts WHERE user_id = $1 AND account_id = $2")
        .bind(user.user_id)
        .bind(&account_id)
        .execute(&state.db)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Linked account not found".into()));
    }

    info!("🔓 User {} unlinked account {}", user.user_id, account_id);

    Ok(Json(
        json!({ "status": "unlinked", "account_id": account_id }),
    ))
}

// ── Multi-SSO: connect / disconnect / list identities ───────────

/// GET /api/auth/identities — list all connected SSO providers
async fn list_identities(
    user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<IdentityResponse>>, AppError> {
    let rows = sqlx::query_as::<_, crate::models::auth::UserIdentity>(
        "SELECT * FROM user_identities WHERE user_id = $1 ORDER BY created_at",
    )
    .bind(user.user_id)
    .fetch_all(&state.db)
    .await?;

    Ok(Json(
        rows.into_iter()
            .map(|i| IdentityResponse {
                provider: i.provider,
                display_name: i.display_name,
                avatar_url: i.avatar_url,
            })
            .collect(),
    ))
}

/// GET /api/auth/connect/:provider — redirect authenticated user to SSO to link a new provider
async fn connect_provider(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Query(params): Query<LoginParams>,
) -> Result<Json<serde_json::Value>, AppError> {
    // Check if already connected
    let already = sqlx::query_scalar::<_, i32>(
        "SELECT id FROM user_identities WHERE user_id = $1 AND provider = $2",
    )
    .bind(user.user_id)
    .bind(&provider)
    .fetch_optional(&state.db)
    .await?;

    if already.is_some() {
        return Err(AppError::BadRequest(format!(
            "You already have {} connected",
            provider
        )));
    }

    let config = provider_config(&provider)?;
    let client = build_oauth_client(&provider, &config, &state)?;

    // Use a different redirect URL for connect callbacks
    let redirect_url = format!(
        "{}/api/auth/connect/callback/{}",
        state.oauth_redirect_base, provider
    );
    let client = client.set_redirect_uri(
        RedirectUrl::new(redirect_url)
            .map_err(|e| AppError::BadRequest(format!("Invalid redirect URL: {}", e)))?,
    );

    let mut auth_request = client.authorize_url(CsrfToken::new_random);
    for scope in &config.scopes {
        auth_request = auth_request.add_scope(Scope::new(scope.to_string()));
    }
    let (auth_url, csrf_state) = auth_request.url();

    // Store CSRF state with user_id so we know who to link.
    // Format: "<user_id>:<provider>" or "<user_id>:<provider>|<origin>"
    let cache_val = match params.origin.as_deref() {
        Some(origin) => format!("{}:{}|{}", user.user_id, provider, origin),
        None => format!("{}:{}", user.user_id, provider),
    };
    let _ = crate::cache::set(
        &format!("oauth_connect:{}", csrf_state.secret()),
        &cache_val,
        std::time::Duration::from_secs(300),
    );

    info!(
        "🔗 User {} starting SSO connect for {}",
        user.user_id, provider
    );

    Ok(Json(json!({
        "url": auth_url.to_string(),
        "state": csrf_state.secret().clone()
    })))
}

/// GET /api/auth/connect/callback/:provider — handle SSO callback for linking
async fn connect_callback(
    State(state): State<AppState>,
    Path(provider): Path<String>,
    Query(params): Query<CallbackParams>,
) -> Result<Redirect, AppError> {
    // Look up cached connect state
    // Format: "<user_id>:<provider>" or "<user_id>:<provider>|<origin>"
    let cached: Option<String> = crate::cache::get(&format!("oauth_connect:{}", params.state));
    let raw =
        cached.ok_or_else(|| AppError::BadRequest("Invalid or expired connect state".into()))?;
    let (user_id_str, rest) = raw
        .split_once(':')
        .ok_or_else(|| AppError::BadRequest("Invalid or expired connect state".into()))?;
    let (cached_provider, connect_origin) = match rest.split_once('|') {
        Some((p, o)) => (p.to_string(), Some(o.to_string())),
        None => (rest.to_string(), None),
    };

    if cached_provider != provider {
        return Err(AppError::BadRequest("Provider mismatch".into()));
    }

    let user_id: Uuid = user_id_str
        .parse()
        .map_err(|_| AppError::BadRequest("Invalid user ID in state".into()))?;

    crate::cache::invalidate(&format!("oauth_connect:{}", params.state));

    let config = provider_config(&provider)?;

    // Build client with connect redirect URL
    let redirect_url = format!(
        "{}/api/auth/connect/callback/{}",
        state.oauth_redirect_base, provider
    );
    let env_prefix = provider.to_uppercase();
    let client_id = std::env::var(format!("{}_CLIENT_ID", env_prefix))
        .map_err(|_| AppError::BadRequest(format!("{}_CLIENT_ID not configured", env_prefix)))?;
    let client_secret = std::env::var(format!("{}_CLIENT_SECRET", env_prefix)).map_err(|_| {
        AppError::BadRequest(format!("{}_CLIENT_SECRET not configured", env_prefix))
    })?;

    let client = BasicClient::new(ClientId::new(client_id))
        .set_client_secret(ClientSecret::new(client_secret))
        .set_auth_uri(
            AuthUrl::new(config.auth_url.to_owned())
                .map_err(|e| AppError::BadRequest(format!("Invalid auth URL: {}", e)))?,
        )
        .set_token_uri(
            TokenUrl::new(config.token_url.to_owned())
                .map_err(|e| AppError::BadRequest(format!("Invalid token URL: {}", e)))?,
        )
        .set_redirect_uri(
            RedirectUrl::new(redirect_url)
                .map_err(|e| AppError::BadRequest(format!("Invalid redirect URL: {}", e)))?,
        );

    // Exchange code
    let reqwest_client = state.search_client.clone();
    let http_client = |req: oauth2::HttpRequest| {
        let client = reqwest_client.clone();
        async move {
            let resp = client
                .request(req.method().clone(), req.uri().to_string())
                .headers(req.headers().clone())
                .body(req.into_body())
                .send()
                .await
                .map_err(|e| OAuthHttpError(format!("Request failed: {}", e)))?;
            let status = resp.status();
            let headers = resp.headers().clone();
            let body = resp
                .bytes()
                .await
                .map_err(|e| OAuthHttpError(format!("Body read failed: {}", e)))?;
            let mut http_resp = axum::http::Response::new(body.to_vec());
            *http_resp.status_mut() = status;
            *http_resp.headers_mut() = headers;
            Ok::<_, OAuthHttpError>(http_resp)
        }
    };

    let token_response = client
        .exchange_code(AuthorizationCode::new(params.code))
        .request_async(&http_client)
        .await
        .map_err(|e| AppError::BadRequest(format!("Token exchange failed: {}", e)))?;

    let access_token = token_response.access_token().secret().to_owned();
    let userinfo = fetch_userinfo(&provider, &access_token, &state.search_client, &config).await?;

    if userinfo.provider_user_id.is_empty() {
        return Err(AppError::BadRequest(
            "Provider did not return a user ID".into(),
        ));
    }

    // Check if this SSO identity is already linked to another user
    let existing_owner = sqlx::query_scalar::<_, Uuid>(
        "SELECT user_id FROM user_identities WHERE provider = $1 AND provider_user_id = $2",
    )
    .bind(&provider)
    .bind(&userinfo.provider_user_id)
    .fetch_optional(&state.db)
    .await?;

    if let Some(owner) = existing_owner {
        if owner != user_id {
            let frontend_url = resolve_frontend_url(connect_origin.as_deref());
            let redirect_url = format!(
                "{}/auth/connect?error={}",
                frontend_url,
                urlencoding::encode("This SSO account is already linked to another user")
            );
            return Ok(Redirect::temporary(&redirect_url));
        }
        // Already linked to this user — just redirect success
    } else {
        // Link the new identity
        sqlx::query(
            r#"
            INSERT INTO user_identities (user_id, provider, provider_user_id, display_name, email, avatar_url)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(user_id)
        .bind(&provider)
        .bind(&userinfo.provider_user_id)
        .bind(&userinfo.display_name)
        .bind(&userinfo.email)
        .bind(&userinfo.avatar_url)
        .execute(&state.db)
        .await?;

        info!("🔗 User {} connected {} identity", user_id, provider);
    }

    let frontend_url = resolve_frontend_url(connect_origin.as_deref());
    let redirect_url = format!(
        "{}/auth/connect?success={}",
        frontend_url,
        urlencoding::encode(&provider)
    );
    Ok(Redirect::temporary(&redirect_url))
}

/// DELETE /api/auth/disconnect/:provider — remove an SSO identity (must keep at least one)
async fn disconnect_provider(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Path(provider): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    // Must keep at least one identity
    let count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM user_identities WHERE user_id = $1")
            .bind(user.user_id)
            .fetch_one(&state.db)
            .await?;

    if count <= 1 {
        return Err(AppError::BadRequest(
            "Cannot disconnect your only login method".into(),
        ));
    }

    let result = sqlx::query("DELETE FROM user_identities WHERE user_id = $1 AND provider = $2")
        .bind(user.user_id)
        .bind(&provider)
        .execute(&state.db)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "No {} identity found",
            provider
        )));
    }

    info!("🔓 User {} disconnected {}", user.user_id, provider);

    Ok(Json(
        json!({ "status": "disconnected", "provider": provider }),
    ))
}

// ── API Key management ──────────────────────────────────────────

/// Generate a random API key: "uma_k_" + 48 random alphanumeric chars
fn generate_api_key() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    let random: String = (0..48)
        .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
        .collect();
    format!("uma_k_{}", random)
}

/// SHA-256 hash of a key, returned as hex string
fn hash_api_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// POST /api/auth/api-keys — create a new API key
async fn create_api_key(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CreateApiKeyRequest>,
) -> Result<Json<CreateApiKeyResponse>, AppError> {
    let name = payload.name.trim().to_string();
    if name.is_empty() || name.len() > 255 {
        return Err(AppError::BadRequest("name must be 1-255 characters".into()));
    }

    // Limit keys per user
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM api_keys WHERE user_id = $1 AND revoked = FALSE",
    )
    .bind(user.user_id)
    .fetch_one(&state.db)
    .await?;

    if count >= 10 {
        return Err(AppError::BadRequest(
            "Maximum 10 active API keys per user".into(),
        ));
    }

    let raw_key = generate_api_key();
    let key_hash = hash_api_key(&raw_key);
    let key_prefix = raw_key[..14].to_string(); // "uma_k_" + first 8 random chars

    let id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO api_keys (user_id, name, key_hash, key_prefix)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        "#,
    )
    .bind(user.user_id)
    .bind(&name)
    .bind(&key_hash)
    .bind(&key_prefix)
    .fetch_one(&state.db)
    .await?;

    info!("🔑 User {} created API key {} ({})", user.user_id, id, name);

    Ok(Json(CreateApiKeyResponse {
        id,
        name,
        key: raw_key, // only time the full key is shown
        key_prefix,
    }))
}

/// GET /api/auth/api-keys — list all API keys for current user
async fn list_api_keys(
    user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<ApiKeyResponse>>, AppError> {
    let rows = sqlx::query_as::<_, ApiKeyResponse>(
        "SELECT id, name, key_prefix, revoked, last_used, created_at FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC",
    )
    .bind(user.user_id)
    .fetch_all(&state.db)
    .await?;

    Ok(Json(rows))
}

/// DELETE /api/auth/api-keys/:key_id — revoke an API key
async fn revoke_api_key(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Path(key_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = sqlx::query("UPDATE api_keys SET revoked = TRUE WHERE id = $1 AND user_id = $2")
        .bind(key_id)
        .bind(user.user_id)
        .execute(&state.db)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("API key not found".into()));
    }

    info!("🔑 User {} revoked API key {}", user.user_id, key_id);

    Ok(Json(json!({ "status": "revoked", "id": key_id })))
}

// ── Bookmarks ───────────────────────────────────────────────────

/// GET /api/auth/bookmarks — list bookmarked inheritances for the current user.
///
/// Bookmarks are keyed by `account_id` (stable per trainer), not by
/// `inheritance_id` (regenerated on every 7-day refresh). Each record
/// carries an `is_stale` flag that is true when the trainer's inheritance
/// has changed since the user bookmarked it — cheap to compute because
/// `inheritance.content_hash` is a STORED generated column.
async fn list_bookmarks(
    user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<UnifiedAccountRecord>>, AppError> {
    let rows = sqlx::query(
        r#"
        SELECT
            i.account_id,
            t.name AS trainer_name,
            t.follower_num,
            t.last_updated,
            i.inheritance_id,
            i.main_parent_id,
            i.parent_left_id,
            i.parent_right_id,
            i.parent_rank,
            i.parent_rarity,
            i.blue_sparks,
            i.pink_sparks,
            i.green_sparks,
            i.white_sparks,
            i.win_count,
            i.white_count,
            i.main_blue_factors,
            i.main_pink_factors,
            i.main_green_factors,
            i.main_white_factors,
            i.main_white_count,
            i.left_blue_factors,
            i.left_pink_factors,
            i.left_green_factors,
            i.left_white_factors,
            i.left_white_count,
            i.right_blue_factors,
            i.right_pink_factors,
            i.right_green_factors,
            i.right_white_factors,
            i.right_white_count,
            i.main_win_saddles,
            i.left_win_saddles,
            i.right_win_saddles,
            i.race_results,
            i.blue_stars_sum,
            i.pink_stars_sum,
            i.green_stars_sum,
            i.white_stars_sum,
            (COALESCE(i.base_affinity, 0) + COALESCE(i.race_affinity, 0)) AS affinity_score,
            sc.support_card_id,
            sc.limit_break_count,
            sc.experience,
            (ub.bookmarked_hash IS DISTINCT FROM i.content_hash) AS is_stale
        FROM user_bookmarks ub
        JOIN inheritance i ON i.account_id = ub.account_id
        JOIN trainer t ON t.account_id = i.account_id
        LEFT JOIN support_card sc ON sc.account_id = i.account_id
        WHERE ub.user_id = $1
        ORDER BY ub.created_at DESC
        "#,
    )
    .bind(user.user_id)
    .fetch_all(&state.db)
    .await?;

    let records = rows
        .into_iter()
        .map(|row| {
            use sqlx::Row;
            let account_id: String = row.get("account_id");
            let support_card = row
                .try_get::<Option<i32>, _>("support_card_id")
                .ok()
                .flatten()
                .map(|_| SupportCard {
                    account_id: account_id.clone(),
                    support_card_id: row.get("support_card_id"),
                    limit_break_count: row.get("limit_break_count"),
                    experience: row.get("experience"),
                });
            let inheritance = Some(Inheritance {
                inheritance_id: row.get("inheritance_id"),
                account_id: account_id.clone(),
                main_parent_id: row.get("main_parent_id"),
                parent_left_id: row.get("parent_left_id"),
                parent_right_id: row.get("parent_right_id"),
                parent_rank: row.get("parent_rank"),
                parent_rarity: row.get("parent_rarity"),
                blue_sparks: row.get("blue_sparks"),
                pink_sparks: row.get("pink_sparks"),
                green_sparks: row.get("green_sparks"),
                white_sparks: row.get("white_sparks"),
                win_count: row.get("win_count"),
                white_count: row.get("white_count"),
                main_blue_factors: row.get("main_blue_factors"),
                main_pink_factors: row.get("main_pink_factors"),
                main_green_factors: row.get("main_green_factors"),
                main_white_factors: row.get("main_white_factors"),
                main_white_count: row.get("main_white_count"),
                left_blue_factors: row.get("left_blue_factors"),
                left_pink_factors: row.get("left_pink_factors"),
                left_green_factors: row.get("left_green_factors"),
                left_white_factors: row.get("left_white_factors"),
                left_white_count: row.get("left_white_count"),
                right_blue_factors: row.get("right_blue_factors"),
                right_pink_factors: row.get("right_pink_factors"),
                right_green_factors: row.get("right_green_factors"),
                right_white_factors: row.get("right_white_factors"),
                right_white_count: row.get("right_white_count"),
                main_win_saddles: row.get("main_win_saddles"),
                left_win_saddles: row.get("left_win_saddles"),
                right_win_saddles: row.get("right_win_saddles"),
                race_results: row.get("race_results"),
                blue_stars_sum: row.get("blue_stars_sum"),
                pink_stars_sum: row.get("pink_stars_sum"),
                green_stars_sum: row.get("green_stars_sum"),
                white_stars_sum: row.get("white_stars_sum"),
                affinity_score: row.try_get("affinity_score").ok(),
            });
            UnifiedAccountRecord {
                account_id,
                trainer_name: row.get("trainer_name"),
                follower_num: row.get("follower_num"),
                last_updated: row.get("last_updated"),
                inheritance,
                support_card,
                is_stale: Some(row.get::<bool, _>("is_stale")),
            }
        })
        .collect();

    Ok(Json(records))
}

/// POST /api/auth/bookmarks/:account_id — add a bookmark.
///
/// Snapshots the current `content_hash` so we can detect when the inheritance
/// later changes. Idempotent: re-bookmarking refreshes the snapshot.
async fn add_bookmark(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Path(account_id): Path<String>,
) -> Result<Json<BookmarkEntry>, AppError> {
    let count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM user_bookmarks WHERE user_id = $1")
            .bind(user.user_id)
            .fetch_one(&state.db)
            .await?;

    if count >= 500 {
        return Err(AppError::BadRequest(
            "Maximum 500 bookmarks per user".into(),
        ));
    }

    // Verify an inheritance row exists for this account, and grab its hash
    // (may be NULL for rows that predate the content_hash trigger).
    let hash_row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT content_hash FROM inheritance WHERE account_id = $1")
            .bind(&account_id)
            .fetch_optional(&state.db)
            .await?;

    let Some((current_hash,)) = hash_row else {
        return Err(AppError::NotFound(
            "No inheritance found for this account".into(),
        ));
    };

    let row = sqlx::query_as::<_, BookmarkEntry>(
        r#"
        INSERT INTO user_bookmarks (user_id, account_id, bookmarked_hash)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id, account_id)
        DO UPDATE SET bookmarked_hash = EXCLUDED.bookmarked_hash
        RETURNING id, account_id, bookmarked_hash, created_at
        "#,
    )
    .bind(user.user_id)
    .bind(&account_id)
    .bind(&current_hash)
    .fetch_one(&state.db)
    .await?;

    Ok(Json(row))
}

/// DELETE /api/auth/bookmarks/:account_id — remove a bookmark
async fn remove_bookmark(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Path(account_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = sqlx::query("DELETE FROM user_bookmarks WHERE user_id = $1 AND account_id = $2")
        .bind(user.user_id)
        .bind(&account_id)
        .execute(&state.db)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Bookmark not found".into()));
    }

    Ok(Json(
        json!({ "status": "removed", "account_id": account_id }),
    ))
}

#[derive(Debug, Deserialize)]
pub struct BulkRemoveBookmarksRequest {
    /// Specific account_ids to remove. Ignored when `all` is true.
    #[serde(default)]
    pub account_ids: Vec<String>,
    /// When true, remove every bookmark for the user.
    #[serde(default)]
    pub all: bool,
}

/// POST /api/auth/bookmarks/bulk-delete — remove many bookmarks at once.
///
/// Body: `{ "account_ids": ["123", "456"] }` to remove specific bookmarks,
/// or `{ "all": true }` to clear every bookmark for the user.
async fn bulk_remove_bookmarks(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<BulkRemoveBookmarksRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    if !payload.all && payload.account_ids.is_empty() {
        return Err(AppError::BadRequest(
            "Provide `account_ids` or set `all: true`".into(),
        ));
    }

    let result = if payload.all {
        sqlx::query("DELETE FROM user_bookmarks WHERE user_id = $1")
            .bind(user.user_id)
            .execute(&state.db)
            .await?
    } else {
        sqlx::query("DELETE FROM user_bookmarks WHERE user_id = $1 AND account_id = ANY($2)")
            .bind(user.user_id)
            .bind(&payload.account_ids)
            .execute(&state.db)
            .await?
    };

    info!(
        "\u{1f5d1}\u{fe0f} User {} bulk-removed {} bookmarks",
        user.user_id,
        result.rows_affected()
    );

    Ok(Json(json!({
        "status": "removed",
        "removed_count": result.rows_affected(),
    })))
}
