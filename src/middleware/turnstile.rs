use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{
        header::{AUTHORIZATION, RETRY_AFTER, SET_COOKIE},
        HeaderMap, HeaderValue, Method, Request, StatusCode,
    },
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use dashmap::DashMap;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    sync::OnceLock,
    time::{Duration, Instant},
};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{redis_store::RedisStore, AppState};

const TURNSTILE_VERIFY_URL: &str = "https://challenges.cloudflare.com/turnstile/v0/siteverify";
const BROWSER_PROOF_COOKIE: &str = "uma_browser_proof";
const BROWSER_WARMUP_COOKIE: &str = "uma_browser_warmup";
const BROWSER_PROOF_HEADER: &str = "X-Browser-Proof";
const BROWSER_PROOF_TTL_HEADER: &str = "X-Browser-Proof-TTL";
const BROWSER_PROOF_SOURCE_HEADER: &str = "X-Browser-Proof-Source";
const BROWSER_PROOF_AUDIENCE: &str = "uma-api";
const BROWSER_PROOF_TYPE: &str = "browser_proof";
const BROWSER_PROOF_SOURCE_TURNSTILE: &str = "turnstile";
const BROWSER_PROOF_SOURCE_WARMUP: &str = "warmup";
const DEFAULT_TURNSTILE_ACTION: &str = "api_request";

static RATE_LIMITS: OnceLock<DashMap<String, RateWindow>> = OnceLock::new();

#[derive(Debug, Clone, Copy)]
struct RateWindow {
    count: u32,
    reset_at: Instant,
}

#[derive(Debug, Clone)]
struct IssuedBrowserProof {
    token: String,
    ttl_seconds: usize,
    subject: String,
    source: &'static str,
    warmup_marker: Option<String>,
}

#[derive(Debug)]
struct BrowserAuthorization {
    credential: &'static str,
    subject: Option<String>,
    proof_source: Option<String>,
    issued_proof: Option<IssuedBrowserProof>,
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct TurnstileVerifyRequest {
    secret: String,
    response: String,
    remoteip: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TurnstileVerifyResponse {
    success: bool,
    #[serde(rename = "error-codes")]
    error_codes: Option<Vec<String>>,
    hostname: Option<String>,
    action: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct InternalCredentialVerificationRequest {
    method: Option<String>,
    path: Option<String>,
    origin: Option<String>,
    referer: Option<String>,
    host: Option<String>,
    record_usage: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct InternalBrowserProofRequest {
    origin: Option<String>,
    referer: Option<String>,
    host: Option<String>,
    client_ip: Option<String>,
    user_agent: Option<String>,
    warmup_marker: Option<String>,
}

#[derive(Debug, Serialize)]
struct InternalCredentialVerificationResponse {
    valid: bool,
    credential: &'static str,
    message: &'static str,
    usage_recorded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<Uuid>,
    context: InternalVerificationContext,
    api_key: Option<InternalApiKeyVerification>,
    browser_proof: Option<InternalBrowserProofVerification>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct InternalVerificationContext {
    method: String,
    path: String,
    endpoint: String,
    origin: Option<String>,
    referer: Option<String>,
    host: Option<String>,
    client_ip: String,
    allowed_browser_context: bool,
    context_host: Option<String>,
}

#[derive(Debug, Serialize)]
struct InternalApiKeyVerification {
    id: Uuid,
    user_id: Uuid,
    name: String,
    usage_recorded: bool,
}

#[derive(Debug, Serialize)]
struct InternalBrowserProofVerification {
    subject: String,
    user_id: Option<Uuid>,
    issued_at: usize,
    expires_at: usize,
    action: String,
    host: String,
    source: String,
    context_matches_proof: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserProofClaims {
    typ: String,
    jti: String,
    sub: String,
    uid: Option<Uuid>,
    iat: usize,
    exp: usize,
    aud: String,
    action: String,
    host: String,
    #[serde(default = "default_browser_proof_source")]
    source: String,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedBrowserProof {
    proof_id: String,
    subject: String,
    issued_at: usize,
}

impl VerifiedBrowserProof {
    pub(crate) fn proof_id(&self) -> &str {
        &self.proof_id
    }

    pub(crate) fn subject(&self) -> &str {
        &self.subject
    }

    pub(crate) fn issued_at(&self) -> usize {
        self.issued_at
    }
}

#[derive(Debug)]
enum TurnstileError {
    MissingSecret,
    Request(String),
}

#[derive(Debug)]
enum BrowserProofError {
    Invalid(String),
    Store(String),
}

pub async fn api_protection_middleware(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    method: Method,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();

    if should_skip_api_protection(&method, &path) {
        return next.run(request).await;
    }

    let dry_run = api_protection_dry_run();
    if api_protection_bypassed() && !dry_run {
        return next.run(request).await;
    }

    let client_ip = extract_client_ip(&headers, connect_info.map(|ci| ci.0));

    match authorize_browser_request(&state, &headers, &method, &path, &client_ip).await {
        Ok(authorization) => {
            if dry_run {
                log_api_protection_dry_run_allow(
                    &headers,
                    &method,
                    &path,
                    &client_ip,
                    &authorization,
                );
            }

            let mut response = next.run(request).await;
            if let Some(proof) = authorization.issued_proof.as_ref() {
                if let Err(e) = attach_browser_proof(&mut response, proof) {
                    warn!(
                        "Failed to attach browser proof headers for ip {} on {}: {}",
                        client_ip, path, e
                    );
                }
            }

            response
        }
        Err(response) => {
            if dry_run {
                log_api_protection_dry_run_reject(
                    &headers,
                    &method,
                    &path,
                    &client_ip,
                    response.status(),
                );
                return next.run(request).await;
            }

            response
        }
    }
}

async fn authorize_browser_request(
    state: &AppState,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    client_ip: &str,
) -> Result<BrowserAuthorization, Response> {
    if let Some(raw_key) = header_str(&headers, "X-API-Key") {
        if raw_key.trim().is_empty() {
            return Err(json_error(StatusCode::UNAUTHORIZED, "invalid_api_key"));
        }

        match crate::middleware::api_key::resolve_api_key(&state.db, raw_key).await {
            Ok(Some(key)) => {
                let limit = env_u32("API_KEY_REQUESTS_PER_MINUTE", 600);
                if let Some(retry_after) = check_rate_limit(
                    format!("api-key:{}", key.id),
                    limit,
                    Duration::from_secs(60),
                ) {
                    warn!("API key {} rate limited on {}", key.id, path);
                    return Err(rate_limited(retry_after));
                }

                return Ok(BrowserAuthorization {
                    credential: "api_key",
                    subject: Some(key.id.to_string()),
                    proof_source: None,
                    issued_proof: None,
                });
            }
            Ok(None) => {
                warn!("Invalid API key rejected from ip {} on {}", client_ip, path);
                return Err(json_error(StatusCode::UNAUTHORIZED, "invalid_api_key"));
            }
            Err(e) => {
                error!("API key lookup failed: {}", e);
                return Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "api_key_lookup_failed",
                ));
            }
        }
    }

    if let Some(proof) = extract_browser_proof(&headers) {
        match verify_browser_proof(proof, state.redis_store.as_ref()).await {
            Ok(claims) => {
                if claims.source == BROWSER_PROOF_SOURCE_WARMUP
                    && *method != Method::GET
                    && *method != Method::HEAD
                {
                    warn!(
                        "Warmup browser proof rejected for write request from ip {} on {}",
                        client_ip, path
                    );
                    return Err(json_error(StatusCode::FORBIDDEN, "browser_proof_required"));
                }

                let limit = browser_rate_limit(&method);
                if let Some(retry_after) = check_rate_limit(
                    format!("browser-proof:{}", claims.sub),
                    limit,
                    Duration::from_secs(60),
                ) {
                    warn!(
                        "Browser proof subject {} rate limited on {}",
                        claims.sub, path
                    );
                    return Err(rate_limited(retry_after));
                }

                return Ok(BrowserAuthorization {
                    credential: "browser_proof",
                    subject: Some(claims.sub),
                    proof_source: Some(claims.source),
                    issued_proof: None,
                });
            }
            Err(BrowserProofError::Invalid(e)) => {
                warn!(
                    "Invalid browser proof from ip {} on {}: {}",
                    client_ip, path, e
                );
            }
            Err(BrowserProofError::Store(e)) => {
                error!("Browser proof store unavailable: {}", e);
                return Err(json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "browser_proof_unavailable",
                ));
            }
        }
    }

    if let Some(turnstile_token) = extract_turnstile_token(&headers) {
        match validate_turnstile_token(turnstile_token, &headers, Some(client_ip.to_string())).await
        {
            Ok(true) => {
                let limit = browser_rate_limit(&method);
                if let Some(retry_after) = check_rate_limit(
                    format!("turnstile-ip:{}", client_ip),
                    limit,
                    Duration::from_secs(60),
                ) {
                    warn!(
                        "Turnstile browser lane rate limited for ip {} on {}",
                        client_ip, path
                    );
                    return Err(rate_limited(retry_after));
                }

                let issued_proof = match issue_browser_proof(
                    &headers,
                    state.redis_store.as_ref(),
                    BROWSER_PROOF_SOURCE_TURNSTILE,
                    None,
                )
                .await
                {
                    Ok(proof) => proof,
                    Err(e) => {
                        error!(
                            "Failed to issue browser proof after valid Turnstile token from ip {} on {}: {}",
                            client_ip, path, e
                        );
                        return Err(json_error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "browser_proof_unavailable",
                        ));
                    }
                };

                return Ok(BrowserAuthorization {
                    credential: "turnstile",
                    subject: Some(issued_proof.subject.clone()),
                    proof_source: Some(issued_proof.source.to_string()),
                    issued_proof: Some(issued_proof),
                });
            }
            Ok(false) => {
                warn!("Turnstile token rejected from ip {} on {}", client_ip, path);
                return Err(json_error(StatusCode::FORBIDDEN, "turnstile_invalid"));
            }
            Err(TurnstileError::MissingSecret) => {
                error!("TURNSTILE_SECRET_KEY is not set");
                return Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "turnstile_not_configured",
                ));
            }
            Err(TurnstileError::Request(e)) => {
                error!("Turnstile verification error: {}", e);
                return Err(json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "turnstile_unavailable",
                ));
            }
        }
    }

    // Let the first browser page load bootstrap its proof on a safe read.
    if can_bootstrap_browser_read(&method, &headers) {
        let limit = env_u32("API_BROWSER_BOOTSTRAP_READS_PER_MINUTE", 1);
        if let Some(retry_after) = check_rate_limit(
            format!("browser-bootstrap:{}", client_ip),
            limit,
            Duration::from_secs(60),
        ) {
            warn!(
                "Browser bootstrap lane rate limited for ip {} on {}",
                client_ip, path
            );
            return Err(rate_limited(retry_after));
        }

        let warmup_marker =
            match reserve_warmup_bootstrap(&headers, state.redis_store.as_ref(), client_ip, None)
                .await
            {
                Ok(marker) => marker,
                Err(response) => return Err(response),
            };

        let issued_proof = issue_warmup_marker(warmup_marker);

        return Ok(BrowserAuthorization {
            credential: "warmup_bootstrap",
            subject: Some(issued_proof.subject.clone()),
            proof_source: Some(issued_proof.source.to_string()),
            issued_proof: Some(issued_proof),
        });
    }

    warn!("Browser proof required for ip {} on {}", client_ip, path);
    Err(json_error(StatusCode::FORBIDDEN, "browser_proof_required"))
}

fn log_api_protection_dry_run_allow(
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    client_ip: &str,
    authorization: &BrowserAuthorization,
) {
    info!(
        "API protection dry-run would allow request method={} path={} ip={} credential={} subject={:?} proof_source={:?} issued_proof={} origin={:?} referer={:?} host={:?} has_bearer={} has_api_credential={} has_browser_proof={} has_turnstile_token={}",
        method,
        path,
        client_ip,
        authorization.credential,
        authorization.subject,
        authorization.proof_source,
        authorization.issued_proof.is_some(),
        header_str(headers, "Origin"),
        header_str(headers, "Referer"),
        header_str(headers, "Host"),
        bearer_token(headers).is_some(),
        extract_api_token(headers).is_some(),
        extract_browser_proof(headers).is_some(),
        extract_turnstile_token(headers).is_some()
    );
}

fn log_api_protection_dry_run_reject(
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    client_ip: &str,
    status: StatusCode,
) {
    warn!(
        "API protection dry-run would reject request status={} method={} path={} ip={} origin={:?} referer={:?} host={:?} has_bearer={} has_api_credential={} has_browser_proof={} has_turnstile_token={}",
        status.as_u16(),
        method,
        path,
        client_ip,
        header_str(headers, "Origin"),
        header_str(headers, "Referer"),
        header_str(headers, "Host"),
        bearer_token(headers).is_some(),
        extract_api_token(headers).is_some(),
        extract_browser_proof(headers).is_some(),
        extract_turnstile_token(headers).is_some()
    );
}

pub async fn exchange_browser_proof(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
) -> Response {
    if api_protection_bypassed() && !api_protection_dry_run() {
        return StatusCode::NO_CONTENT.into_response();
    }

    let client_ip = extract_client_ip(&headers, connect_info.map(|ci| ci.0));
    let exchange_limit = env_u32("BROWSER_PROOF_EXCHANGE_REQUESTS_PER_MINUTE", 10);
    if let Some(retry_after) = check_rate_limit(
        format!("proof-exchange-ip:{}", client_ip),
        exchange_limit,
        Duration::from_secs(60),
    ) {
        warn!("Browser proof exchange rate limited for ip {}", client_ip);
        return rate_limited(retry_after);
    }

    let Some(turnstile_token) = extract_turnstile_token(&headers) else {
        return json_error(StatusCode::FORBIDDEN, "turnstile_required");
    };

    match validate_turnstile_token(turnstile_token, &headers, Some(client_ip.clone())).await {
        Ok(true) => {}
        Ok(false) => {
            warn!(
                "Browser proof exchange rejected invalid Turnstile token from ip {}",
                client_ip
            );
            return json_error(StatusCode::FORBIDDEN, "turnstile_invalid");
        }
        Err(TurnstileError::MissingSecret) => {
            error!("TURNSTILE_SECRET_KEY is not set");
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "turnstile_not_configured",
            );
        }
        Err(TurnstileError::Request(e)) => {
            error!("Turnstile verification error during proof exchange: {}", e);
            return json_error(StatusCode::SERVICE_UNAVAILABLE, "turnstile_unavailable");
        }
    }

    let proof = match issue_browser_proof(
        &headers,
        state.redis_store.as_ref(),
        BROWSER_PROOF_SOURCE_TURNSTILE,
        None,
    )
    .await
    {
        Ok(proof) => proof,
        Err(e) => {
            error!("Failed to create browser proof: {}", e);
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "browser_proof_unavailable",
            );
        }
    };

    info!(
        "Issued browser proof for {} from ip {}",
        proof.subject(),
        client_ip
    );

    let mut response = StatusCode::NO_CONTENT.into_response();
    match attach_browser_proof(&mut response, &proof) {
        Ok(()) => response,
        Err(e) => {
            error!("Failed to attach browser proof to response: {}", e);
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "browser_proof_unavailable",
            )
        }
    }
}

pub async fn issue_internal_browser_proof(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    payload: Option<Json<InternalBrowserProofRequest>>,
) -> Response {
    let client_ip = extract_client_ip(&headers, connect_info.map(|ci| ci.0));
    let payload = payload.map(|Json(payload)| payload).unwrap_or_default();
    let browser_client_ip = payload
        .client_ip
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(client_ip.as_str())
        .to_string();
    let headers = match internal_browser_context_headers(headers, &payload) {
        Ok(headers) => headers,
        Err(error) => return json_error(StatusCode::BAD_REQUEST, error),
    };
    let limit = env_u32("BROWSER_PROOF_INTERNAL_REQUESTS_PER_MINUTE", 12000);
    if let Some(retry_after) = check_rate_limit(
        format!("proof-internal-ip:{}", browser_client_ip),
        limit,
        Duration::from_secs(60),
    ) {
        warn!(
            "Internal browser proof issuer rate limited for browser ip {} via service ip {}",
            browser_client_ip, client_ip
        );
        return rate_limited(retry_after);
    }

    if !has_allowed_browser_context(&headers) {
        warn!(
            "Internal browser proof issuer rejected request without allowed origin/referer from ip {}",
            client_ip
        );
        return json_error(StatusCode::FORBIDDEN, "browser_context_required");
    }

    let warmup_marker = match reserve_warmup_bootstrap(
        &headers,
        state.redis_store.as_ref(),
        &browser_client_ip,
        payload.warmup_marker.as_deref(),
    )
    .await
    {
        Ok(marker) => marker,
        Err(response) => return response,
    };

    let proof = issue_warmup_marker(warmup_marker);

    info!(
        "Issued internal browser warmup marker for {} from browser ip {} via service ip {}",
        proof.subject(),
        browser_client_ip,
        client_ip
    );

    let mut response = StatusCode::NO_CONTENT.into_response();
    match attach_browser_proof(&mut response, &proof) {
        Ok(()) => response,
        Err(e) => {
            error!("Failed to attach internal browser proof to response: {}", e);
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "browser_proof_unavailable",
            )
        }
    }
}

fn internal_browser_context_headers(
    mut headers: HeaderMap,
    payload: &InternalBrowserProofRequest,
) -> Result<HeaderMap, &'static str> {
    if let Some(origin) = payload
        .origin
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let value = HeaderValue::from_str(origin.trim()).map_err(|_| "invalid_origin")?;
        headers.insert("Origin", value);
    }

    if let Some(referer) = payload
        .referer
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let value = HeaderValue::from_str(referer.trim()).map_err(|_| "invalid_referer")?;
        headers.insert("Referer", value);
    }

    if let Some(host) = payload
        .host
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let value = HeaderValue::from_str(host.trim()).map_err(|_| "invalid_host")?;
        headers.insert("Host", value);
    }

    if let Some(user_agent) = payload
        .user_agent
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let value = HeaderValue::from_str(user_agent.trim()).map_err(|_| "invalid_user_agent")?;
        headers.insert("User-Agent", value);
    }

    Ok(headers)
}

pub async fn verify_internal_credential(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    payload: Option<Json<InternalCredentialVerificationRequest>>,
) -> Response {
    let client_ip = extract_client_ip(&headers, connect_info.map(|ci| ci.0));
    let payload = payload.map(|Json(payload)| payload).unwrap_or_default();
    let context = internal_verification_context(&headers, &payload, client_ip);
    let should_record_usage = payload.record_usage.unwrap_or(true);

    if let Some(token) = bearer_token(&headers) {
        match crate::auth::verify_token(token) {
            Ok(claims) => {
                return (
                    StatusCode::OK,
                    Json(InternalCredentialVerificationResponse {
                        valid: true,
                        credential: "bearer",
                        message: "valid_bearer_token",
                        usage_recorded: false,
                        user_id: Some(claims.sub),
                        context,
                        api_key: None,
                        browser_proof: None,
                        error: None,
                    }),
                )
                    .into_response();
            }
            Err(_) => {
                return internal_verification_error(
                    StatusCode::UNAUTHORIZED,
                    "bearer",
                    context,
                    "invalid_bearer_token",
                );
            }
        }
    }

    if let Some(raw_key) = extract_api_token(&headers) {
        if raw_key.trim().is_empty() {
            return internal_verification_error(
                StatusCode::UNAUTHORIZED,
                "api_key",
                context,
                "invalid_api_key",
            );
        }

        match crate::middleware::api_key::resolve_api_key(&state.db, raw_key).await {
            Ok(Some(key)) => {
                let mut usage_recorded = false;
                if should_record_usage && !state.user_writes_disabled {
                    match crate::middleware::api_key::record_api_key_usage(
                        &state.db,
                        &key,
                        &context.endpoint,
                    )
                    .await
                    {
                        Ok(()) => usage_recorded = true,
                        Err(error) => {
                            warn!(
                                "Internal credential verifier failed to record API key usage: {}",
                                error
                            );
                        }
                    }
                }

                return (
                    StatusCode::OK,
                    Json(InternalCredentialVerificationResponse {
                        valid: true,
                        credential: "api_key",
                        message: if usage_recorded {
                            "valid_api_key_usage_recorded"
                        } else {
                            "valid_api_key"
                        },
                        usage_recorded,
                        user_id: Some(key.user_id),
                        context,
                        api_key: Some(InternalApiKeyVerification {
                            id: key.id,
                            user_id: key.user_id,
                            name: key.name,
                            usage_recorded,
                        }),
                        browser_proof: None,
                        error: None,
                    }),
                )
                    .into_response();
            }
            Ok(None) => {
                warn!(
                    "Internal credential verifier rejected invalid API key from ip {}",
                    context.client_ip
                );
                return internal_verification_error(
                    StatusCode::UNAUTHORIZED,
                    "api_key",
                    context,
                    "invalid_api_key",
                );
            }
            Err(error) => {
                error!(
                    "Internal credential verifier API key lookup failed: {}",
                    error
                );
                return internal_verification_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "api_key",
                    context,
                    "api_key_lookup_failed",
                );
            }
        }
    }

    if let Some(proof) = extract_browser_proof(&headers) {
        match verify_browser_proof(proof, state.redis_store.as_ref()).await {
            Ok(claims) => {
                if claims.source == BROWSER_PROOF_SOURCE_WARMUP
                    && context.method != "GET"
                    && context.method != "HEAD"
                {
                    warn!(
                        "Internal credential verifier rejected warmup browser proof for {} {} from ip {}",
                        context.method, context.path, context.client_ip
                    );
                    return internal_verification_error(
                        StatusCode::FORBIDDEN,
                        "browser_proof",
                        context,
                        "browser_proof_required",
                    );
                }

                let context_matches_proof = context
                    .context_host
                    .as_ref()
                    .map(|host| browser_proof_context_matches(host, &claims.host));
                if context_matches_proof == Some(false) {
                    warn!(
                        "Internal credential verifier accepted browser proof with context mismatch: proof host {}, context {:?}, ip {}",
                        claims.host, context.context_host, context.client_ip
                    );
                }

                return (
                    StatusCode::OK,
                    Json(InternalCredentialVerificationResponse {
                        valid: true,
                        credential: "browser_proof",
                        message: "valid_browser_proof",
                        usage_recorded: false,
                        user_id: claims.uid,
                        context,
                        api_key: None,
                        browser_proof: Some(InternalBrowserProofVerification {
                            subject: claims.sub,
                            user_id: claims.uid,
                            issued_at: claims.iat,
                            expires_at: claims.exp,
                            action: claims.action,
                            host: claims.host,
                            source: claims.source,
                            context_matches_proof,
                        }),
                        error: None,
                    }),
                )
                    .into_response();
            }
            Err(BrowserProofError::Invalid(error)) => {
                warn!(
                    "Internal credential verifier rejected invalid browser proof from ip {}: {}",
                    context.client_ip, error
                );
                return internal_verification_error(
                    StatusCode::UNAUTHORIZED,
                    "browser_proof",
                    context,
                    "invalid_browser_proof",
                );
            }
            Err(BrowserProofError::Store(error)) => {
                error!(
                    "Internal credential verifier browser proof store unavailable: {}",
                    error
                );
                return internal_verification_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "browser_proof",
                    context,
                    "browser_proof_unavailable",
                );
            }
        }
    }

    internal_verification_error(
        StatusCode::BAD_REQUEST,
        "none",
        context,
        "credential_required",
    )
}

async fn validate_turnstile_token(
    token: &str,
    headers: &HeaderMap,
    client_ip: Option<String>,
) -> Result<bool, TurnstileError> {
    if accepts_local_turnstile_dev_token(token, headers) {
        info!(
            "Accepted local Turnstile dev token for origin {:?} host {:?}",
            header_str(headers, "Origin"),
            header_str(headers, "Host")
        );
        return Ok(true);
    }

    let secret_key = std::env::var("TURNSTILE_SECRET_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(TurnstileError::MissingSecret)?;

    let verify_response = siteverify(token, client_ip, &secret_key).await?;

    if !verify_response.success {
        if let Some(error_codes) = verify_response.error_codes {
            warn!(
                "Turnstile verification failed with errors: {:?}",
                error_codes
            );
        }
        return Ok(false);
    }

    let Some(hostname) = verify_response.hostname.as_deref() else {
        warn!("Turnstile verification succeeded without hostname");
        return Ok(false);
    };

    if !allowed_turnstile_host(hostname) {
        warn!("Turnstile hostname '{}' is not allowed", hostname);
        return Ok(false);
    }

    let expected_action = expected_turnstile_action();
    if verify_response.action.as_deref() != Some(expected_action.as_str()) {
        warn!(
            "Turnstile action mismatch: expected '{}', got {:?}",
            expected_action, verify_response.action
        );
        return Ok(false);
    }

    if let Some(origin) = header_str(headers, "Origin") {
        if !allowed_request_origin(origin) {
            warn!(
                "Request origin '{}' is not allowed for Turnstile-protected API",
                origin
            );
            return Ok(false);
        }
    }

    Ok(true)
}

fn accepts_local_turnstile_dev_token(token: &str, headers: &HeaderMap) -> bool {
    if !is_development() {
        return false;
    }

    let Some(expected_token) = env_string("TURNSTILE_DEV_TOKEN") else {
        return false;
    };

    if token.trim() != expected_token {
        return false;
    }

    if let Some(origin) = header_str(headers, "Origin") {
        if !allowed_request_origin(origin) {
            warn!(
                "Local Turnstile dev token rejected for disallowed origin '{}'",
                origin
            );
            return false;
        }
    }

    true
}

async fn siteverify(
    token: &str,
    client_ip: Option<String>,
    secret_key: &str,
) -> Result<TurnstileVerifyResponse, TurnstileError> {
    let client = reqwest::Client::new();
    let verify_request = TurnstileVerifyRequest {
        secret: secret_key.to_string(),
        response: token.to_string(),
        remoteip: client_ip,
    };

    let response = client
        .post(TURNSTILE_VERIFY_URL)
        .form(&verify_request)
        .send()
        .await
        .map_err(|e| TurnstileError::Request(e.to_string()))?;

    if !response.status().is_success() {
        return Err(TurnstileError::Request(format!(
            "Turnstile API returned status {}",
            response.status()
        )));
    }

    response
        .json()
        .await
        .map_err(|e| TurnstileError::Request(e.to_string()))
}

async fn issue_browser_proof(
    headers: &HeaderMap,
    store: Option<&RedisStore>,
    source: &'static str,
    warmup_marker: Option<String>,
) -> Result<IssuedBrowserProof, String> {
    let store = store.ok_or_else(|| "browser proof store is not configured".to_string())?;
    let user_id = bearer_token(headers).and_then(|token| {
        crate::auth::verify_token(token)
            .ok()
            .map(|claims| claims.sub)
    });
    let subject = user_id
        .map(|id| format!("user:{}", id))
        .unwrap_or_else(|| format!("anon:{}", Uuid::new_v4()));

    let host = proof_host(headers);
    let action = expected_turnstile_action();
    let ttl_seconds = browser_proof_ttl_seconds(source);
    let (token, claims) =
        create_browser_proof(&subject, user_id, &host, &action, ttl_seconds, source, true)?;

    store_browser_proof(store, &token, &claims, ttl_seconds).await?;

    Ok(IssuedBrowserProof {
        token,
        ttl_seconds,
        subject,
        source,
        warmup_marker,
    })
}

fn issue_warmup_marker(warmup_marker: String) -> IssuedBrowserProof {
    IssuedBrowserProof {
        token: String::new(),
        ttl_seconds: browser_proof_ttl_seconds(BROWSER_PROOF_SOURCE_WARMUP),
        subject: format!("warmup:{}", warmup_marker),
        source: BROWSER_PROOF_SOURCE_WARMUP,
        warmup_marker: Some(warmup_marker),
    }
}

fn attach_browser_proof(response: &mut Response, proof: &IssuedBrowserProof) -> Result<(), String> {
    let source_value = HeaderValue::from_str(proof.source).map_err(|e| e.to_string())?;

    let headers = response.headers_mut();
    headers.insert(BROWSER_PROOF_SOURCE_HEADER, source_value);

    if proof.source == BROWSER_PROOF_SOURCE_TURNSTILE {
        let ttl_value =
            HeaderValue::from_str(&proof.ttl_seconds.to_string()).map_err(|e| e.to_string())?;
        let cookie = browser_proof_cookie(&proof.token);
        let cookie_value = HeaderValue::from_str(&cookie).map_err(|e| e.to_string())?;
        let proof_value = HeaderValue::from_str(&proof.token).map_err(|e| e.to_string())?;
        headers.insert(BROWSER_PROOF_TTL_HEADER, ttl_value);
        headers.append(SET_COOKIE, cookie_value);
        headers.insert(BROWSER_PROOF_HEADER, proof_value);
        let clear_marker = clear_warmup_marker_cookie();
        let clear_marker_value = HeaderValue::from_str(&clear_marker).map_err(|e| e.to_string())?;
        headers.append(SET_COOKIE, clear_marker_value);
    } else if let Some(marker) = proof.warmup_marker.as_deref() {
        let cookie = warmup_marker_cookie(marker);
        let cookie_value = HeaderValue::from_str(&cookie).map_err(|e| e.to_string())?;
        headers.append(SET_COOKIE, cookie_value);
    }

    Ok(())
}

fn create_browser_proof(
    subject: &str,
    user_id: Option<Uuid>,
    host: &str,
    action: &str,
    ttl_seconds: usize,
    source: &str,
    allow_opaque: bool,
) -> Result<(String, BrowserProofClaims), String> {
    let now = chrono::Utc::now().timestamp() as usize;
    let claims = BrowserProofClaims {
        typ: BROWSER_PROOF_TYPE.to_string(),
        jti: Uuid::new_v4().to_string(),
        sub: subject.to_string(),
        uid: user_id,
        iat: now,
        exp: now + ttl_seconds,
        aud: BROWSER_PROOF_AUDIENCE.to_string(),
        action: action.to_string(),
        host: host.to_ascii_lowercase(),
        source: source.to_string(),
    };

    let token = if let Some(secret) = proof_secret() {
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .map_err(|error| error.to_string())?
    } else if allow_opaque {
        format!("uma_bp_{}", Uuid::new_v4())
    } else {
        return Err("browser proof signing secret is not configured".to_string());
    };

    Ok((token, claims))
}

async fn store_browser_proof(
    store: &RedisStore,
    token: &str,
    claims: &BrowserProofClaims,
    ttl_seconds: usize,
) -> Result<(), String> {
    let key = store.hashed_key("browser-proof", token);
    let payload = serde_json::to_string(claims).map_err(|error| error.to_string())?;
    store
        .set_string_ex(&key, &payload, ttl_seconds as u64)
        .await
}

async fn verify_browser_proof(
    token: &str,
    store: Option<&RedisStore>,
) -> Result<BrowserProofClaims, BrowserProofError> {
    if let Some(store) = store {
        let key = store.hashed_key("browser-proof", token);
        let Some(payload) = store
            .get_string(&key)
            .await
            .map_err(BrowserProofError::Store)?
        else {
            return Err(BrowserProofError::Invalid(
                "proof is not present in shared store".to_string(),
            ));
        };

        let claims = serde_json::from_str::<BrowserProofClaims>(&payload)
            .map_err(|error| BrowserProofError::Invalid(error.to_string()))?;
        validate_browser_proof_claims(claims).map_err(BrowserProofError::Invalid)
    } else {
        verify_signed_browser_proof(token).map_err(BrowserProofError::Invalid)
    }
}

pub(crate) async fn require_turnstile_browser_proof(
    headers: &HeaderMap,
    store: Option<&RedisStore>,
) -> Result<VerifiedBrowserProof, &'static str> {
    let Some(proof) = extract_browser_proof(headers) else {
        return Err("browser_proof_required");
    };

    let claims = match verify_browser_proof(proof, store).await {
        Ok(claims) => claims,
        Err(BrowserProofError::Invalid(_)) => return Err("invalid_browser_proof"),
        Err(BrowserProofError::Store(_)) => return Err("browser_proof_unavailable"),
    };

    if claims.source != BROWSER_PROOF_SOURCE_TURNSTILE {
        return Err("browser_proof_required");
    }

    Ok(VerifiedBrowserProof {
        proof_id: claims.jti,
        subject: claims.sub,
        issued_at: claims.iat,
    })
}

fn verify_signed_browser_proof(token: &str) -> Result<BrowserProofClaims, String> {
    let secret = proof_secret()
        .ok_or_else(|| "browser proof signing secret is not configured".to_string())?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_audience(&[BROWSER_PROOF_AUDIENCE]);

    let data = decode::<BrowserProofClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|e| e.to_string())?;

    validate_browser_proof_claims(data.claims)
}

fn validate_browser_proof_claims(claims: BrowserProofClaims) -> Result<BrowserProofClaims, String> {
    if claims.typ != BROWSER_PROOF_TYPE {
        return Err("wrong proof type".to_string());
    }
    if claims.aud != BROWSER_PROOF_AUDIENCE {
        return Err("wrong proof audience".to_string());
    }
    if claims.action != expected_turnstile_action() {
        return Err("wrong proof action".to_string());
    }
    if !allowed_turnstile_host(&claims.host) {
        return Err("wrong proof host".to_string());
    }
    if claims.source != BROWSER_PROOF_SOURCE_TURNSTILE
        && claims.source != BROWSER_PROOF_SOURCE_WARMUP
    {
        return Err("wrong proof source".to_string());
    }
    let now = chrono::Utc::now().timestamp() as usize;
    if claims.exp <= now {
        return Err("expired proof".to_string());
    }

    Ok(claims)
}

fn default_browser_proof_source() -> String {
    BROWSER_PROOF_SOURCE_TURNSTILE.to_string()
}

fn extract_turnstile_token(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, "X-Turnstile-Token")
        .or_else(|| header_str(headers, "CF-Turnstile-Token"))
        .filter(|value| !value.trim().is_empty())
}

fn extract_browser_proof(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, "X-Browser-Proof")
        .filter(|value| !value.trim().is_empty())
        .or_else(|| cookie_value(headers, BROWSER_PROOF_COOKIE))
}

fn extract_api_token(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, "X-API-Key")
        .or_else(|| header_str(headers, "X-API-Token"))
        .or_else(|| header_str(headers, "X-API-Tokens"))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, AUTHORIZATION.as_str())?.strip_prefix("Bearer ")
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let cookie = header_str(headers, "Cookie")?;
    cookie.split(';').find_map(|part| {
        let (cookie_name, value) = part.trim().split_once('=')?;
        (cookie_name == name).then_some(value)
    })
}

async fn reserve_warmup_bootstrap(
    headers: &HeaderMap,
    store: Option<&RedisStore>,
    client_ip: &str,
    marker_from_payload: Option<&str>,
) -> Result<String, Response> {
    let store = store
        .ok_or_else(|| json_error(StatusCode::SERVICE_UNAVAILABLE, "browser_proof_unavailable"))?;
    let ttl_seconds = warmup_lock_ttl_seconds();
    let max_warmups = warmup_burst_limit();
    let existing_marker = marker_from_payload
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            cookie_value(headers, BROWSER_WARMUP_COOKIE)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        });

    let marker = existing_marker
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let marker_key = store.hashed_key("browser-warmup-marker", &marker);
    let marker_count = increment_warmup_counter(store, &marker_key, ttl_seconds).await?;
    if marker_count > max_warmups {
        warn!(
            "Browser warmup bootstrap rejected because marker exceeded burst count {} for ip {} host {}",
            marker_count,
            client_ip,
            proof_host(headers)
        );
        return Err(rate_limited(ttl_seconds as u64));
    }

    let fingerprint = warmup_fingerprint(headers, client_ip);
    let fingerprint_key = store.hashed_key("browser-warmup-fingerprint", &fingerprint);
    let fingerprint_count = increment_warmup_counter(store, &fingerprint_key, ttl_seconds).await?;
    if fingerprint_count > max_warmups {
        warn!(
            "Browser warmup bootstrap rejected because fingerprint exceeded burst count {} for ip {} host {}",
            fingerprint_count,
            client_ip,
            proof_host(headers)
        );
        return Err(rate_limited(ttl_seconds as u64));
    }

    Ok(marker)
}

async fn increment_warmup_counter(
    store: &RedisStore,
    key: &str,
    ttl_seconds: usize,
) -> Result<u64, Response> {
    store
        .increment_with_expiry(key, ttl_seconds as u64)
        .await
        .map_err(|error| {
            error!("Browser warmup counter update failed: {}", error);
            json_error(StatusCode::SERVICE_UNAVAILABLE, "browser_proof_unavailable")
        })
}

fn warmup_fingerprint(headers: &HeaderMap, client_ip: &str) -> String {
    let host = proof_host(headers);
    let user_agent = header_str(headers, "User-Agent").unwrap_or("<none>");
    let material = format!("{}|{}|{}", host, client_ip.trim(), user_agent.trim());
    hex::encode(Sha256::digest(material.as_bytes()))
}

fn browser_proof_cookie(token: &str) -> String {
    let ttl = browser_proof_ttl_seconds(BROWSER_PROOF_SOURCE_TURNSTILE);
    let secure = std::env::var("BROWSER_PROOF_COOKIE_SECURE")
        .map(|value| value != "false" && value != "0")
        .unwrap_or_else(|_| !is_development());
    let secure_attr = if secure { "; Secure" } else { "" };
    let domain_attr = std::env::var("BROWSER_PROOF_COOKIE_DOMAIN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("; Domain={}", value.trim()))
        .unwrap_or_default();

    format!(
        "{}={}; Max-Age={}; Path=/{}{}; HttpOnly; SameSite=Lax",
        BROWSER_PROOF_COOKIE, token, ttl, domain_attr, secure_attr
    )
}

fn warmup_marker_cookie(marker: &str) -> String {
    let ttl = warmup_lock_ttl_seconds();
    let secure = std::env::var("BROWSER_PROOF_COOKIE_SECURE")
        .map(|value| value != "false" && value != "0")
        .unwrap_or_else(|_| !is_development());
    let secure_attr = if secure { "; Secure" } else { "" };
    let domain_attr = std::env::var("BROWSER_PROOF_COOKIE_DOMAIN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("; Domain={}", value.trim()))
        .unwrap_or_default();

    format!(
        "{}={}; Max-Age={}; Path=/{}{}; HttpOnly; SameSite=Lax",
        BROWSER_WARMUP_COOKIE, marker, ttl, domain_attr, secure_attr
    )
}

fn clear_warmup_marker_cookie() -> String {
    let secure = std::env::var("BROWSER_PROOF_COOKIE_SECURE")
        .map(|value| value != "false" && value != "0")
        .unwrap_or_else(|_| !is_development());
    let secure_attr = if secure { "; Secure" } else { "" };
    let domain_attr = std::env::var("BROWSER_PROOF_COOKIE_DOMAIN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("; Domain={}", value.trim()))
        .unwrap_or_default();

    format!(
        "{}=; Max-Age=0; Path=/{}{}; HttpOnly; SameSite=Lax",
        BROWSER_WARMUP_COOKIE, domain_attr, secure_attr
    )
}

fn proof_host(headers: &HeaderMap) -> String {
    if let Some(host) = header_uri_host(headers, "Origin") {
        return host;
    }

    if let Some(host) = header_uri_host(headers, "Referer") {
        return host;
    }

    header_str(headers, "Host")
        .and_then(|host| host.split(':').next())
        .filter(|host| !host.is_empty())
        .unwrap_or("uma.moe")
        .to_ascii_lowercase()
}

fn header_uri_host(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = header_str(headers, name)?;
    let uri = value.parse::<axum::http::Uri>().ok()?;
    uri.host().map(|host| host.to_ascii_lowercase())
}

fn can_bootstrap_browser_read(method: &Method, headers: &HeaderMap) -> bool {
    if *method != Method::GET && *method != Method::HEAD {
        return false;
    }

    has_allowed_browser_context(headers)
}

fn has_allowed_browser_context(headers: &HeaderMap) -> bool {
    if let Some(origin) = header_str(headers, "Origin") {
        return allowed_request_origin(origin);
    }

    header_str(headers, "Referer")
        .map(allowed_request_referer)
        .unwrap_or(false)
}

fn should_skip_api_protection(method: &Method, path: &str) -> bool {
    if *method == Method::OPTIONS || !(path.starts_with("/api/") || path.starts_with("/ingest/")) {
        return true;
    }

    matches!(path, "/api/health" | "/api/ver" | "/api/ver/history")
        || path.starts_with("/api/docs")
        || path == "/api/auth/browser-proof"
        || path.starts_with("/api/auth/login/")
        || path.starts_with("/api/auth/callback/")
        || path.starts_with("/api/auth/connect/callback/")
}

fn api_protection_bypassed() -> bool {
    env_bool("API_PROTECTION_BYPASS") || env_bool("TURNSTILE_BYPASS")
}

fn api_protection_dry_run() -> bool {
    env_bool("API_PROTECTION_DRY_RUN") || env_bool("TURNSTILE_DRY_RUN")
}

fn browser_rate_limit(method: &Method) -> u32 {
    if *method == Method::GET || *method == Method::HEAD {
        env_u32("API_BROWSER_READS_PER_MINUTE", 120)
    } else {
        env_u32("API_BROWSER_WRITES_PER_MINUTE", 30)
    }
}

fn check_rate_limit(key: String, limit: u32, window: Duration) -> Option<u64> {
    if limit == 0 {
        return None;
    }

    let limits = RATE_LIMITS.get_or_init(DashMap::new);
    let now = Instant::now();

    if limits.len() > 10_000 {
        limits.retain(|_, value| value.reset_at > now);
    }

    if let Some(mut entry) = limits.get_mut(&key) {
        if now >= entry.reset_at {
            entry.count = 1;
            entry.reset_at = now + window;
            return None;
        }

        if entry.count >= limit {
            return Some(
                entry
                    .reset_at
                    .saturating_duration_since(now)
                    .as_secs()
                    .max(1),
            );
        }

        entry.count += 1;
        return None;
    }

    limits.insert(
        key,
        RateWindow {
            count: 1,
            reset_at: now + window,
        },
    );
    None
}

fn allowed_turnstile_host(hostname: &str) -> bool {
    let hostname = hostname.trim().to_ascii_lowercase();
    if hostname.is_empty() {
        return false;
    }

    allowed_hosts()
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(&hostname))
}

fn allowed_request_origin(origin: &str) -> bool {
    if let Ok(uri) = origin.parse::<axum::http::Uri>() {
        if let Some(host) = uri.host() {
            return allowed_turnstile_host(host);
        }
    }

    false
}

fn allowed_request_referer(referer: &str) -> bool {
    if let Ok(uri) = referer.parse::<axum::http::Uri>() {
        if let Some(host) = uri.host() {
            return allowed_turnstile_host(host);
        }
    }

    false
}

fn browser_proof_context_matches(context_host: &str, proof_host: &str) -> bool {
    let context_host = normalize_hostname_for_match(context_host);
    let proof_host = normalize_hostname_for_match(proof_host);
    if context_host.is_empty() || proof_host.is_empty() {
        return false;
    }

    if context_host == proof_host {
        return true;
    }

    let context_site = strip_www_prefix(&context_host);
    let proof_site = strip_www_prefix(&proof_host);
    if context_site == proof_site {
        return true;
    }

    allowed_turnstile_host(proof_site)
        && context_site
            .strip_suffix(proof_site)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn normalize_hostname_for_match(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn strip_www_prefix(host: &str) -> &str {
    host.strip_prefix("www.").unwrap_or(host)
}

fn internal_verification_context(
    headers: &HeaderMap,
    payload: &InternalCredentialVerificationRequest,
    client_ip: String,
) -> InternalVerificationContext {
    let method = payload
        .method
        .as_deref()
        .or_else(|| header_str(headers, "X-Original-Method"))
        .or_else(|| header_str(headers, "X-Forwarded-Method"))
        .unwrap_or("UNKNOWN")
        .trim()
        .to_ascii_uppercase();
    let path = payload
        .path
        .as_deref()
        .or_else(|| header_str(headers, "X-Original-Path"))
        .or_else(|| header_str(headers, "X-Original-Uri"))
        .or_else(|| header_str(headers, "X-Forwarded-Uri"))
        .map(normalize_context_path)
        .unwrap_or_else(|| "/internal/unknown".to_string());
    let origin = payload
        .origin
        .clone()
        .or_else(|| header_str(headers, "Origin").map(ToOwned::to_owned));
    let referer = payload
        .referer
        .clone()
        .or_else(|| header_str(headers, "Referer").map(ToOwned::to_owned));
    let host = payload
        .host
        .clone()
        .or_else(|| header_str(headers, "X-Original-Host").map(ToOwned::to_owned));
    let allowed_browser_context = origin
        .as_deref()
        .map(allowed_request_origin)
        .or_else(|| referer.as_deref().map(allowed_request_referer))
        .unwrap_or(false);
    let context_host = origin
        .as_deref()
        .and_then(browser_context_uri_host)
        .or_else(|| referer.as_deref().and_then(browser_context_uri_host))
        .or_else(|| host.as_deref().and_then(browser_context_header_host));
    let endpoint = crate::middleware::api_key::normalize_endpoint(&method, &path);

    InternalVerificationContext {
        method,
        path,
        endpoint,
        origin,
        referer,
        host,
        client_ip,
        allowed_browser_context,
        context_host,
    }
}

fn normalize_context_path(path: &str) -> String {
    let path = path.trim().split('?').next().unwrap_or(path).trim();
    if path.is_empty() {
        "/internal/unknown".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

fn uri_host(value: &str) -> Option<String> {
    let uri = value.parse::<axum::http::Uri>().ok()?;
    uri.host().map(|host| host.to_ascii_lowercase())
}

fn browser_context_uri_host(value: &str) -> Option<String> {
    let host = uri_host(value)?;
    is_browser_context_host(&host).then_some(host)
}

fn browser_context_header_host(value: &str) -> Option<String> {
    let host = header_host(value)?;
    is_browser_context_host(&host).then_some(host)
}

fn header_host(value: &str) -> Option<String> {
    let value = value.trim();
    let host = if let Some(bracketed) = value.strip_prefix('[') {
        bracketed.split(']').next()?
    } else {
        value.split(':').next()?
    }
    .trim();

    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

fn is_browser_context_host(host: &str) -> bool {
    let host = strip_www_prefix(&normalize_hostname_for_match(host)).to_string();
    allowed_hosts().iter().any(|allowed| {
        let allowed = strip_www_prefix(allowed);
        host == allowed || host.ends_with(&format!(".{}", allowed))
    })
}

fn internal_verification_error(
    status: StatusCode,
    credential: &'static str,
    context: InternalVerificationContext,
    error: &'static str,
) -> Response {
    (
        status,
        Json(InternalCredentialVerificationResponse {
            valid: false,
            credential,
            message: error,
            usage_recorded: false,
            user_id: None,
            context,
            api_key: None,
            browser_proof: None,
            error: Some(error.to_string()),
        }),
    )
        .into_response()
}

fn allowed_hosts() -> Vec<String> {
    std::env::var("TURNSTILE_ALLOWED_HOSTS")
        .unwrap_or_else(|_| {
            if is_development() {
                "uma.moe,www.uma.moe,beta.uma.moe,honse.moe,www.honse.moe,localhost,127.0.0.1"
                    .to_string()
            } else {
                "uma.moe,www.uma.moe,beta.uma.moe,honse.moe,www.honse.moe".to_string()
            }
        })
        .split(',')
        .map(|host| host.trim().to_ascii_lowercase())
        .filter(|host| !host.is_empty())
        .collect()
}

fn expected_turnstile_action() -> String {
    std::env::var("TURNSTILE_ACTION").unwrap_or_else(|_| DEFAULT_TURNSTILE_ACTION.to_string())
}

fn extract_client_ip(headers: &HeaderMap, addr: Option<SocketAddr>) -> String {
    if let Some(cf_ip) = header_str(headers, "CF-Connecting-IP") {
        return cf_ip.to_string();
    }

    if let Some(forwarded_for) = header_str(headers, "X-Forwarded-For") {
        if let Some(first_ip) = forwarded_for.split(',').next() {
            return first_ip.trim().to_string();
        }
    }

    if let Some(real_ip) = header_str(headers, "X-Real-IP") {
        return real_ip.to_string();
    }

    if let Some(forwarded) = header_str(headers, "Forwarded") {
        for pair in forwarded.split(';') {
            if let Some((key, value)) = pair.split_once('=') {
                if key.trim().eq_ignore_ascii_case("for") {
                    return value.trim().trim_matches('"').to_string();
                }
            }
        }
    }

    addr.map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn proof_secret() -> Option<String> {
    std::env::var("BROWSER_PROOF_SECRET")
        .or_else(|_| std::env::var("JWT_SECRET"))
        .ok()
        .filter(|secret| !secret.trim().is_empty())
        .or_else(|| {
            if is_development() {
                Some("dev-insecure-browser-proof-secret-change-me".to_string())
            } else {
                warn!("BROWSER_PROOF_SECRET or JWT_SECRET must be set to issue browser proofs");
                None
            }
        })
}

impl IssuedBrowserProof {
    fn subject(&self) -> &str {
        &self.subject
    }
}

fn browser_proof_ttl_seconds(source: &str) -> usize {
    if source == BROWSER_PROOF_SOURCE_WARMUP {
        env_usize("BROWSER_PROOF_WARMUP_TTL_SECONDS", 30)
    } else {
        env_usize("BROWSER_PROOF_TTL_SECONDS", 300)
    }
}

fn warmup_lock_ttl_seconds() -> usize {
    env_usize("BROWSER_PROOF_WARMUP_LOCK_SECONDS", 120)
}

fn warmup_burst_limit() -> u64 {
    env_u64("BROWSER_PROOF_WARMUP_BURST", 4)
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn is_development() -> bool {
    env_bool("DEBUG_MODE")
}

fn json_error(status: StatusCode, error: &'static str) -> Response {
    (
        status,
        Json(ErrorBody {
            error,
            status: status.as_u16(),
            message: error_message(error),
        }),
    )
        .into_response()
}

fn error_message(error: &'static str) -> Option<&'static str> {
    match error {
        "browser_proof_required" => Some(
            "This endpoint requires a browser proof. Browser clients should wait for the Turnstile/browser-proof exchange and retry. Bots, scripts, and integrations should use an API key instead; API keys can be generated from your Uma account at any time.",
        ),
        _ => None,
    }
}

fn rate_limited(retry_after: u64) -> Response {
    let mut response = json_error(StatusCode::TOO_MANY_REQUESTS, "rate_limited");
    if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}
