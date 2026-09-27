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

include!("../types/middleware/turnstile.rs");

impl VerifiedBrowserProof {
    pub(crate) fn proofId(&self) -> &str {
        &self.proof_id
    }

    pub(crate) fn subject(&self) -> &str {
        &self.subject
    }

    pub(crate) fn issuedAt(&self) -> usize {
        self.issued_at
    }
}

pub async fn apiProtectionMiddleware(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    method: Method,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();

    if shouldSkipApiProtection(&method, &path) {
        return next.run(request).await;
    }

    let dry_run = apiProtectionDryRun();
    if apiProtectionBypassed() && !dry_run {
        return next.run(request).await;
    }

    let client_ip = extractClientIp(&headers, connect_info.map(|ci| ci.0));

    match authorizeBrowserRequest(&state, &headers, &method, &path, &client_ip).await {
        Ok(authorization) => {
            if dry_run {
                logApiProtectionDryRunAllow(&headers, &method, &path, &client_ip, &authorization);
            }

            let mut response = next.run(request).await;
            if let Some(proof) = authorization.issued_proof.as_ref() {
                if let Err(e) = attachBrowserProof(&mut response, proof) {
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
                logApiProtectionDryRunReject(
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

async fn authorizeBrowserRequest(
    state: &AppState,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    client_ip: &str,
) -> Result<BrowserAuthorization, Response> {
    if let Some(raw_key) = headerStr(&headers, "X-API-Key") {
        if raw_key.trim().is_empty() {
            return Err(jsonError(StatusCode::UNAUTHORIZED, "invalid_api_key"));
        }

        match crate::middleware::api_key::resolveApiKey(&state.db, raw_key).await {
            Ok(Some(key)) => {
                let limit = envU32("API_KEY_REQUESTS_PER_MINUTE", 600);
                if let Some(retry_after) = checkRateLimit(
                    format!("api-key:{}", key.id),
                    limit,
                    Duration::from_secs(60),
                ) {
                    warn!("API key {} rate limited on {}", key.id, path);
                    return Err(rateLimited(retry_after));
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
                return Err(jsonError(StatusCode::UNAUTHORIZED, "invalid_api_key"));
            }
            Err(e) => {
                error!("API key lookup failed: {}", e);
                return Err(jsonError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "api_key_lookup_failed",
                ));
            }
        }
    }

    if let Some(proof) = extractBrowserProof(&headers) {
        match verifyBrowserProof(proof, state.redis_store.as_ref()).await {
            Ok(claims) => {
                if claims.source == BROWSER_PROOF_SOURCE_WARMUP
                    && *method != Method::GET
                    && *method != Method::HEAD
                {
                    warn!(
                        "Warmup browser proof rejected for write request from ip {} on {}",
                        client_ip, path
                    );
                    return Err(jsonError(StatusCode::FORBIDDEN, "browser_proof_required"));
                }

                let limit = browserRateLimit(&method);
                if let Some(retry_after) = checkRateLimit(
                    format!("browser-proof:{}", claims.sub),
                    limit,
                    Duration::from_secs(60),
                ) {
                    warn!(
                        "Browser proof subject {} rate limited on {}",
                        claims.sub, path
                    );
                    return Err(rateLimited(retry_after));
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
                return Err(jsonError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "browser_proof_unavailable",
                ));
            }
        }
    }

    if let Some(turnstile_token) = extractTurnstileToken(&headers) {
        match validateTurnstileToken(turnstile_token, &headers, Some(client_ip.to_string())).await {
            Ok(true) => {
                let limit = browserRateLimit(&method);
                if let Some(retry_after) = checkRateLimit(
                    format!("turnstile-ip:{}", client_ip),
                    limit,
                    Duration::from_secs(60),
                ) {
                    warn!(
                        "Turnstile browser lane rate limited for ip {} on {}",
                        client_ip, path
                    );
                    return Err(rateLimited(retry_after));
                }

                let issued_proof = match issueBrowserProof(
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
                        return Err(jsonError(
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
                return Err(jsonError(StatusCode::FORBIDDEN, "turnstile_invalid"));
            }
            Err(TurnstileError::MissingSecret) => {
                error!("TURNSTILE_SECRET_KEY is not set");
                return Err(jsonError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "turnstile_not_configured",
                ));
            }
            Err(TurnstileError::Request(e)) => {
                error!("Turnstile verification error: {}", e);
                return Err(jsonError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "turnstile_unavailable",
                ));
            }
        }
    }

    // Let the first browser page load bootstrap its proof on a safe read.
    if canBootstrapBrowserRead(&method, &headers) {
        let limit = envU32("API_BROWSER_BOOTSTRAP_READS_PER_MINUTE", 1);
        if let Some(retry_after) = checkRateLimit(
            format!("browser-bootstrap:{}", client_ip),
            limit,
            Duration::from_secs(60),
        ) {
            warn!(
                "Browser bootstrap lane rate limited for ip {} on {}",
                client_ip, path
            );
            return Err(rateLimited(retry_after));
        }

        let warmup_marker =
            match reserveWarmupBootstrap(&headers, state.redis_store.as_ref(), client_ip, None)
                .await
            {
                Ok(marker) => marker,
                Err(response) => return Err(response),
            };

        let issued_proof = issueWarmupMarker(warmup_marker);

        return Ok(BrowserAuthorization {
            credential: "warmup_bootstrap",
            subject: Some(issued_proof.subject.clone()),
            proof_source: Some(issued_proof.source.to_string()),
            issued_proof: Some(issued_proof),
        });
    }

    warn!("Browser proof required for ip {} on {}", client_ip, path);
    Err(jsonError(StatusCode::FORBIDDEN, "browser_proof_required"))
}

fn logApiProtectionDryRunAllow(
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
        headerStr(headers, "Origin"),
        headerStr(headers, "Referer"),
        headerStr(headers, "Host"),
        bearerToken(headers).is_some(),
        extractApiToken(headers).is_some(),
        extractBrowserProof(headers).is_some(),
        extractTurnstileToken(headers).is_some()
    );
}

fn logApiProtectionDryRunReject(
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
        headerStr(headers, "Origin"),
        headerStr(headers, "Referer"),
        headerStr(headers, "Host"),
        bearerToken(headers).is_some(),
        extractApiToken(headers).is_some(),
        extractBrowserProof(headers).is_some(),
        extractTurnstileToken(headers).is_some()
    );
}

pub async fn exchangeBrowserProof(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
) -> Response {
    if apiProtectionBypassed() && !apiProtectionDryRun() {
        return StatusCode::NO_CONTENT.into_response();
    }

    let client_ip = extractClientIp(&headers, connect_info.map(|ci| ci.0));
    let exchange_limit = envU32("BROWSER_PROOF_EXCHANGE_REQUESTS_PER_MINUTE", 10);
    if let Some(retry_after) = checkRateLimit(
        format!("proof-exchange-ip:{}", client_ip),
        exchange_limit,
        Duration::from_secs(60),
    ) {
        warn!("Browser proof exchange rate limited for ip {}", client_ip);
        return rateLimited(retry_after);
    }

    let Some(turnstile_token) = extractTurnstileToken(&headers) else {
        return jsonError(StatusCode::FORBIDDEN, "turnstile_required");
    };

    match validateTurnstileToken(turnstile_token, &headers, Some(client_ip.clone())).await {
        Ok(true) => {}
        Ok(false) => {
            warn!(
                "Browser proof exchange rejected invalid Turnstile token from ip {}",
                client_ip
            );
            return jsonError(StatusCode::FORBIDDEN, "turnstile_invalid");
        }
        Err(TurnstileError::MissingSecret) => {
            error!("TURNSTILE_SECRET_KEY is not set");
            return jsonError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "turnstile_not_configured",
            );
        }
        Err(TurnstileError::Request(e)) => {
            error!("Turnstile verification error during proof exchange: {}", e);
            return jsonError(StatusCode::SERVICE_UNAVAILABLE, "turnstile_unavailable");
        }
    }

    let proof = match issueBrowserProof(
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
            return jsonError(
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
    match attachBrowserProof(&mut response, &proof) {
        Ok(()) => response,
        Err(e) => {
            error!("Failed to attach browser proof to response: {}", e);
            jsonError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "browser_proof_unavailable",
            )
        }
    }
}

pub async fn issueInternalBrowserProof(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    payload: Option<Json<InternalBrowserProofRequest>>,
) -> Response {
    let client_ip = extractClientIp(&headers, connect_info.map(|ci| ci.0));
    let payload = payload.map(|Json(payload)| payload).unwrap_or_default();
    let browser_client_ip = payload
        .client_ip
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(client_ip.as_str())
        .to_string();
    let headers = match internalBrowserContextHeaders(headers, &payload) {
        Ok(headers) => headers,
        Err(error) => return jsonError(StatusCode::BAD_REQUEST, error),
    };
    let limit = envU32("BROWSER_PROOF_INTERNAL_REQUESTS_PER_MINUTE", 12000);
    if let Some(retry_after) = checkRateLimit(
        format!("proof-internal-ip:{}", browser_client_ip),
        limit,
        Duration::from_secs(60),
    ) {
        warn!(
            "Internal browser proof issuer rate limited for browser ip {} via service ip {}",
            browser_client_ip, client_ip
        );
        return rateLimited(retry_after);
    }

    if !hasAllowedBrowserContext(&headers) {
        warn!(
            "Internal browser proof issuer rejected request without allowed origin/referer from ip {}",
            client_ip
        );
        return jsonError(StatusCode::FORBIDDEN, "browser_context_required");
    }

    let warmup_marker = match reserveWarmupBootstrap(
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

    let proof = issueWarmupMarker(warmup_marker);

    info!(
        "Issued internal browser warmup marker for {} from browser ip {} via service ip {}",
        proof.subject(),
        browser_client_ip,
        client_ip
    );

    let mut response = StatusCode::NO_CONTENT.into_response();
    match attachBrowserProof(&mut response, &proof) {
        Ok(()) => response,
        Err(e) => {
            error!("Failed to attach internal browser proof to response: {}", e);
            jsonError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "browser_proof_unavailable",
            )
        }
    }
}

fn internalBrowserContextHeaders(
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

pub async fn verifyInternalCredential(
    State(state): State<AppState>,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    payload: Option<Json<InternalCredentialVerificationRequest>>,
) -> Response {
    let client_ip = extractClientIp(&headers, connect_info.map(|ci| ci.0));
    let payload = payload.map(|Json(payload)| payload).unwrap_or_default();
    let context = internalVerificationContext(&headers, &payload, client_ip);
    let should_record_usage = payload.record_usage.unwrap_or(true);

    if let Some(token) = bearerToken(&headers) {
        match crate::auth::verifyToken(token) {
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
                return internalVerificationError(
                    StatusCode::UNAUTHORIZED,
                    "bearer",
                    context,
                    "invalid_bearer_token",
                );
            }
        }
    }

    if let Some(raw_key) = extractApiToken(&headers) {
        if raw_key.trim().is_empty() {
            return internalVerificationError(
                StatusCode::UNAUTHORIZED,
                "api_key",
                context,
                "invalid_api_key",
            );
        }

        match crate::middleware::api_key::resolveApiKey(&state.db, raw_key).await {
            Ok(Some(key)) => {
                let mut usage_recorded = false;
                if should_record_usage && !state.user_writes_disabled {
                    match crate::middleware::api_key::recordApiKeyUsage(
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
                return internalVerificationError(
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
                return internalVerificationError(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "api_key",
                    context,
                    "api_key_lookup_failed",
                );
            }
        }
    }

    if let Some(proof) = extractBrowserProof(&headers) {
        match verifyBrowserProof(proof, state.redis_store.as_ref()).await {
            Ok(claims) => {
                if claims.source == BROWSER_PROOF_SOURCE_WARMUP
                    && context.method != "GET"
                    && context.method != "HEAD"
                {
                    warn!(
                        "Internal credential verifier rejected warmup browser proof for {} {} from ip {}",
                        context.method, context.path, context.client_ip
                    );
                    return internalVerificationError(
                        StatusCode::FORBIDDEN,
                        "browser_proof",
                        context,
                        "browser_proof_required",
                    );
                }

                let context_matches_proof = context
                    .context_host
                    .as_ref()
                    .map(|host| browserProofContextMatches(host, &claims.host));
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
                return internalVerificationError(
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
                return internalVerificationError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "browser_proof",
                    context,
                    "browser_proof_unavailable",
                );
            }
        }
    }

    internalVerificationError(
        StatusCode::BAD_REQUEST,
        "none",
        context,
        "credential_required",
    )
}

async fn validateTurnstileToken(
    token: &str,
    headers: &HeaderMap,
    client_ip: Option<String>,
) -> Result<bool, TurnstileError> {
    if acceptsLocalTurnstileDevToken(token, headers) {
        info!(
            "Accepted local Turnstile dev token for origin {:?} host {:?}",
            headerStr(headers, "Origin"),
            headerStr(headers, "Host")
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

    if !allowedTurnstileHost(hostname) {
        warn!("Turnstile hostname '{}' is not allowed", hostname);
        return Ok(false);
    }

    let expected_action = expectedTurnstileAction();
    if verify_response.action.as_deref() != Some(expected_action.as_str()) {
        warn!(
            "Turnstile action mismatch: expected '{}', got {:?}",
            expected_action, verify_response.action
        );
        return Ok(false);
    }

    if let Some(origin) = headerStr(headers, "Origin") {
        if !allowedRequestOrigin(origin) {
            warn!(
                "Request origin '{}' is not allowed for Turnstile-protected API",
                origin
            );
            return Ok(false);
        }
    }

    Ok(true)
}

fn acceptsLocalTurnstileDevToken(token: &str, headers: &HeaderMap) -> bool {
    if !isDevelopment() {
        return false;
    }

    let Some(expected_token) = envString("TURNSTILE_DEV_TOKEN") else {
        return false;
    };

    if token.trim() != expected_token {
        return false;
    }

    if let Some(origin) = headerStr(headers, "Origin") {
        if !allowedRequestOrigin(origin) {
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

async fn issueBrowserProof(
    headers: &HeaderMap,
    store: Option<&RedisStore>,
    source: &'static str,
    warmup_marker: Option<String>,
) -> Result<IssuedBrowserProof, String> {
    let store = store.ok_or_else(|| "browser proof store is not configured".to_string())?;
    let user_id = bearerToken(headers).and_then(|token| {
        crate::auth::verifyToken(token)
            .ok()
            .map(|claims| claims.sub)
    });
    let subject = user_id
        .map(|id| format!("user:{}", id))
        .unwrap_or_else(|| format!("anon:{}", Uuid::new_v4()));

    let host = proofHost(headers);
    let action = expectedTurnstileAction();
    let ttl_seconds = browserProofTtlSeconds(source);
    let (token, claims) =
        createBrowserProof(&subject, user_id, &host, &action, ttl_seconds, source, true)?;

    storeBrowserProof(store, &token, &claims, ttl_seconds).await?;

    Ok(IssuedBrowserProof {
        token,
        ttl_seconds,
        subject,
        source,
        warmup_marker,
    })
}

fn issueWarmupMarker(warmup_marker: String) -> IssuedBrowserProof {
    IssuedBrowserProof {
        token: String::new(),
        ttl_seconds: browserProofTtlSeconds(BROWSER_PROOF_SOURCE_WARMUP),
        subject: format!("warmup:{}", warmup_marker),
        source: BROWSER_PROOF_SOURCE_WARMUP,
        warmup_marker: Some(warmup_marker),
    }
}

fn attachBrowserProof(response: &mut Response, proof: &IssuedBrowserProof) -> Result<(), String> {
    let source_value = HeaderValue::from_str(proof.source).map_err(|e| e.to_string())?;

    let headers = response.headers_mut();
    headers.insert(BROWSER_PROOF_SOURCE_HEADER, source_value);

    if proof.source == BROWSER_PROOF_SOURCE_TURNSTILE {
        let ttl_value =
            HeaderValue::from_str(&proof.ttl_seconds.to_string()).map_err(|e| e.to_string())?;
        let cookie = browserProofCookie(&proof.token);
        let cookieValue = HeaderValue::from_str(&cookie).map_err(|e| e.to_string())?;
        let proof_value = HeaderValue::from_str(&proof.token).map_err(|e| e.to_string())?;
        headers.insert(BROWSER_PROOF_TTL_HEADER, ttl_value);
        headers.append(SET_COOKIE, cookieValue);
        headers.insert(BROWSER_PROOF_HEADER, proof_value);
        let clear_marker = clearWarmupMarkerCookie();
        let clear_marker_value = HeaderValue::from_str(&clear_marker).map_err(|e| e.to_string())?;
        headers.append(SET_COOKIE, clear_marker_value);
    } else if let Some(marker) = proof.warmup_marker.as_deref() {
        let cookie = warmupMarkerCookie(marker);
        let cookieValue = HeaderValue::from_str(&cookie).map_err(|e| e.to_string())?;
        headers.append(SET_COOKIE, cookieValue);
    }

    Ok(())
}

fn createBrowserProof(
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

    let token = if let Some(secret) = proofSecret() {
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

async fn storeBrowserProof(
    store: &RedisStore,
    token: &str,
    claims: &BrowserProofClaims,
    ttl_seconds: usize,
) -> Result<(), String> {
    let key = store.hashedKey("browser-proof", token);
    let payload = serde_json::to_string(claims).map_err(|error| error.to_string())?;
    store.setStringEx(&key, &payload, ttl_seconds as u64).await
}

async fn verifyBrowserProof(
    token: &str,
    store: Option<&RedisStore>,
) -> Result<BrowserProofClaims, BrowserProofError> {
    if let Some(store) = store {
        let key = store.hashedKey("browser-proof", token);
        let Some(payload) = store
            .getString(&key)
            .await
            .map_err(BrowserProofError::Store)?
        else {
            return Err(BrowserProofError::Invalid(
                "proof is not present in shared store".to_string(),
            ));
        };

        let claims = serde_json::from_str::<BrowserProofClaims>(&payload)
            .map_err(|error| BrowserProofError::Invalid(error.to_string()))?;
        validateBrowserProofClaims(claims).map_err(BrowserProofError::Invalid)
    } else {
        verifySignedBrowserProof(token).map_err(BrowserProofError::Invalid)
    }
}

pub(crate) async fn requireTurnstileBrowserProof(
    headers: &HeaderMap,
    store: Option<&RedisStore>,
) -> Result<VerifiedBrowserProof, &'static str> {
    let Some(proof) = extractBrowserProof(headers) else {
        return Err("browser_proof_required");
    };

    let claims = match verifyBrowserProof(proof, store).await {
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

fn verifySignedBrowserProof(token: &str) -> Result<BrowserProofClaims, String> {
    let secret = proofSecret()
        .ok_or_else(|| "browser proof signing secret is not configured".to_string())?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_audience(&[BROWSER_PROOF_AUDIENCE]);

    let data = decode::<BrowserProofClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|e| e.to_string())?;

    validateBrowserProofClaims(data.claims)
}

fn validateBrowserProofClaims(claims: BrowserProofClaims) -> Result<BrowserProofClaims, String> {
    if claims.typ != BROWSER_PROOF_TYPE {
        return Err("wrong proof type".to_string());
    }
    if claims.aud != BROWSER_PROOF_AUDIENCE {
        return Err("wrong proof audience".to_string());
    }
    if claims.action != expectedTurnstileAction() {
        return Err("wrong proof action".to_string());
    }
    if !allowedTurnstileHost(&claims.host) {
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

fn defaultBrowserProofSource() -> String {
    BROWSER_PROOF_SOURCE_TURNSTILE.to_string()
}

fn extractTurnstileToken(headers: &HeaderMap) -> Option<&str> {
    headerStr(headers, "X-Turnstile-Token")
        .or_else(|| headerStr(headers, "CF-Turnstile-Token"))
        .filter(|value| !value.trim().is_empty())
}

fn extractBrowserProof(headers: &HeaderMap) -> Option<&str> {
    headerStr(headers, "X-Browser-Proof")
        .filter(|value| !value.trim().is_empty())
        .or_else(|| cookieValue(headers, BROWSER_PROOF_COOKIE))
}

fn extractApiToken(headers: &HeaderMap) -> Option<&str> {
    headerStr(headers, "X-API-Key")
        .or_else(|| headerStr(headers, "X-API-Token"))
        .or_else(|| headerStr(headers, "X-API-Tokens"))
}

fn bearerToken(headers: &HeaderMap) -> Option<&str> {
    headerStr(headers, AUTHORIZATION.as_str())?.strip_prefix("Bearer ")
}

fn headerStr<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn cookieValue<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let cookie = headerStr(headers, "Cookie")?;
    cookie.split(';').find_map(|part| {
        let (cookie_name, value) = part.trim().split_once('=')?;
        (cookie_name == name).then_some(value)
    })
}

async fn reserveWarmupBootstrap(
    headers: &HeaderMap,
    store: Option<&RedisStore>,
    client_ip: &str,
    marker_from_payload: Option<&str>,
) -> Result<String, Response> {
    let store = store
        .ok_or_else(|| jsonError(StatusCode::SERVICE_UNAVAILABLE, "browser_proof_unavailable"))?;
    let ttl_seconds = warmupLockTtlSeconds();
    let max_warmups = warmupBurstLimit();
    let existing_marker = marker_from_payload
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            cookieValue(headers, BROWSER_WARMUP_COOKIE)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        });

    let marker = existing_marker
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let marker_key = store.hashedKey("browser-warmup-marker", &marker);
    let marker_count = incrementWarmupCounter(store, &marker_key, ttl_seconds).await?;
    if marker_count > max_warmups {
        warn!(
            "Browser warmup bootstrap rejected because marker exceeded burst count {} for ip {} host {}",
            marker_count,
            client_ip,
            proofHost(headers)
        );
        return Err(rateLimited(ttl_seconds as u64));
    }

    let fingerprint = warmupFingerprint(headers, client_ip);
    let fingerprint_key = store.hashedKey("browser-warmup-fingerprint", &fingerprint);
    let fingerprint_count = incrementWarmupCounter(store, &fingerprint_key, ttl_seconds).await?;
    if fingerprint_count > max_warmups {
        warn!(
            "Browser warmup bootstrap rejected because fingerprint exceeded burst count {} for ip {} host {}",
            fingerprint_count,
            client_ip,
            proofHost(headers)
        );
        return Err(rateLimited(ttl_seconds as u64));
    }

    Ok(marker)
}

async fn incrementWarmupCounter(
    store: &RedisStore,
    key: &str,
    ttl_seconds: usize,
) -> Result<u64, Response> {
    store
        .incrementWithExpiry(key, ttl_seconds as u64)
        .await
        .map_err(|error| {
            error!("Browser warmup counter update failed: {}", error);
            jsonError(StatusCode::SERVICE_UNAVAILABLE, "browser_proof_unavailable")
        })
}

fn warmupFingerprint(headers: &HeaderMap, client_ip: &str) -> String {
    let host = proofHost(headers);
    let user_agent = headerStr(headers, "User-Agent").unwrap_or("<none>");
    let material = format!("{}|{}|{}", host, client_ip.trim(), user_agent.trim());
    hex::encode(Sha256::digest(material.as_bytes()))
}

fn browserProofCookie(token: &str) -> String {
    let ttl = browserProofTtlSeconds(BROWSER_PROOF_SOURCE_TURNSTILE);
    let secure = std::env::var("BROWSER_PROOF_COOKIE_SECURE")
        .map(|value| value != "false" && value != "0")
        .unwrap_or_else(|_| !isDevelopment());
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

fn warmupMarkerCookie(marker: &str) -> String {
    let ttl = warmupLockTtlSeconds();
    let secure = std::env::var("BROWSER_PROOF_COOKIE_SECURE")
        .map(|value| value != "false" && value != "0")
        .unwrap_or_else(|_| !isDevelopment());
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

fn clearWarmupMarkerCookie() -> String {
    let secure = std::env::var("BROWSER_PROOF_COOKIE_SECURE")
        .map(|value| value != "false" && value != "0")
        .unwrap_or_else(|_| !isDevelopment());
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

fn proofHost(headers: &HeaderMap) -> String {
    if let Some(host) = headerUriHost(headers, "Origin") {
        return host;
    }

    if let Some(host) = headerUriHost(headers, "Referer") {
        return host;
    }

    headerStr(headers, "Host")
        .and_then(|host| host.split(':').next())
        .filter(|host| !host.is_empty())
        .unwrap_or("uma.moe")
        .to_ascii_lowercase()
}

fn headerUriHost(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = headerStr(headers, name)?;
    let uri = value.parse::<axum::http::Uri>().ok()?;
    uri.host().map(|host| host.to_ascii_lowercase())
}

fn canBootstrapBrowserRead(method: &Method, headers: &HeaderMap) -> bool {
    if *method != Method::GET && *method != Method::HEAD {
        return false;
    }

    hasAllowedBrowserContext(headers)
}

fn hasAllowedBrowserContext(headers: &HeaderMap) -> bool {
    if let Some(origin) = headerStr(headers, "Origin") {
        return allowedRequestOrigin(origin);
    }

    headerStr(headers, "Referer")
        .map(allowedRequestReferer)
        .unwrap_or(false)
}

fn shouldSkipApiProtection(method: &Method, path: &str) -> bool {
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

fn apiProtectionBypassed() -> bool {
    envBool("API_PROTECTION_BYPASS") || envBool("TURNSTILE_BYPASS")
}

fn apiProtectionDryRun() -> bool {
    envBool("API_PROTECTION_DRY_RUN") || envBool("TURNSTILE_DRY_RUN")
}

fn browserRateLimit(method: &Method) -> u32 {
    if *method == Method::GET || *method == Method::HEAD {
        envU32("API_BROWSER_READS_PER_MINUTE", 120)
    } else {
        envU32("API_BROWSER_WRITES_PER_MINUTE", 30)
    }
}

fn checkRateLimit(key: String, limit: u32, window: Duration) -> Option<u64> {
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

fn allowedTurnstileHost(hostname: &str) -> bool {
    let hostname = hostname.trim().to_ascii_lowercase();
    if hostname.is_empty() {
        return false;
    }

    allowedHosts()
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(&hostname))
}

fn allowedRequestOrigin(origin: &str) -> bool {
    if let Ok(uri) = origin.parse::<axum::http::Uri>() {
        if let Some(host) = uri.host() {
            return allowedTurnstileHost(host);
        }
    }

    false
}

fn allowedRequestReferer(referer: &str) -> bool {
    if let Ok(uri) = referer.parse::<axum::http::Uri>() {
        if let Some(host) = uri.host() {
            return allowedTurnstileHost(host);
        }
    }

    false
}

fn browserProofContextMatches(context_host: &str, proofHost: &str) -> bool {
    let context_host = normalizeHostnameForMatch(context_host);
    let proofHost = normalizeHostnameForMatch(proofHost);
    if context_host.is_empty() || proofHost.is_empty() {
        return false;
    }

    if context_host == proofHost {
        return true;
    }

    let context_site = stripWwwPrefix(&context_host);
    let proof_site = stripWwwPrefix(&proofHost);
    if context_site == proof_site {
        return true;
    }

    allowedTurnstileHost(proof_site)
        && context_site
            .strip_suffix(proof_site)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn normalizeHostnameForMatch(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn stripWwwPrefix(host: &str) -> &str {
    host.strip_prefix("www.").unwrap_or(host)
}

fn internalVerificationContext(
    headers: &HeaderMap,
    payload: &InternalCredentialVerificationRequest,
    client_ip: String,
) -> InternalVerificationContext {
    let method = payload
        .method
        .as_deref()
        .or_else(|| headerStr(headers, "X-Original-Method"))
        .or_else(|| headerStr(headers, "X-Forwarded-Method"))
        .unwrap_or("UNKNOWN")
        .trim()
        .to_ascii_uppercase();
    let path = payload
        .path
        .as_deref()
        .or_else(|| headerStr(headers, "X-Original-Path"))
        .or_else(|| headerStr(headers, "X-Original-Uri"))
        .or_else(|| headerStr(headers, "X-Forwarded-Uri"))
        .map(normalizeContextPath)
        .unwrap_or_else(|| "/internal/unknown".to_string());
    let origin = payload
        .origin
        .clone()
        .or_else(|| headerStr(headers, "Origin").map(ToOwned::to_owned));
    let referer = payload
        .referer
        .clone()
        .or_else(|| headerStr(headers, "Referer").map(ToOwned::to_owned));
    let host = payload
        .host
        .clone()
        .or_else(|| headerStr(headers, "X-Original-Host").map(ToOwned::to_owned));
    let allowed_browser_context = origin
        .as_deref()
        .map(allowedRequestOrigin)
        .or_else(|| referer.as_deref().map(allowedRequestReferer))
        .unwrap_or(false);
    let context_host = origin
        .as_deref()
        .and_then(browserContextUriHost)
        .or_else(|| referer.as_deref().and_then(browserContextUriHost))
        .or_else(|| host.as_deref().and_then(browserContextHeaderHost));
    let endpoint = crate::middleware::api_key::normalizeEndpoint(&method, &path);

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

fn normalizeContextPath(path: &str) -> String {
    let path = path.trim().split('?').next().unwrap_or(path).trim();
    if path.is_empty() {
        "/internal/unknown".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

fn uriHost(value: &str) -> Option<String> {
    let uri = value.parse::<axum::http::Uri>().ok()?;
    uri.host().map(|host| host.to_ascii_lowercase())
}

fn browserContextUriHost(value: &str) -> Option<String> {
    let host = uriHost(value)?;
    isBrowserContextHost(&host).then_some(host)
}

fn browserContextHeaderHost(value: &str) -> Option<String> {
    let host = headerHost(value)?;
    isBrowserContextHost(&host).then_some(host)
}

fn headerHost(value: &str) -> Option<String> {
    let value = value.trim();
    let host = if let Some(bracketed) = value.strip_prefix('[') {
        bracketed.split(']').next()?
    } else {
        value.split(':').next()?
    }
    .trim();

    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

fn isBrowserContextHost(host: &str) -> bool {
    let host = stripWwwPrefix(&normalizeHostnameForMatch(host)).to_string();
    allowedHosts().iter().any(|allowed| {
        let allowed = stripWwwPrefix(allowed);
        host == allowed || host.ends_with(&format!(".{}", allowed))
    })
}

fn internalVerificationError(
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

fn allowedHosts() -> Vec<String> {
    std::env::var("TURNSTILE_ALLOWED_HOSTS")
        .unwrap_or_else(|_| {
            if isDevelopment() {
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

fn expectedTurnstileAction() -> String {
    std::env::var("TURNSTILE_ACTION").unwrap_or_else(|_| DEFAULT_TURNSTILE_ACTION.to_string())
}

fn extractClientIp(headers: &HeaderMap, addr: Option<SocketAddr>) -> String {
    if let Some(cf_ip) = headerStr(headers, "CF-Connecting-IP") {
        return cf_ip.to_string();
    }

    if let Some(forwarded_for) = headerStr(headers, "X-Forwarded-For") {
        if let Some(first_ip) = forwarded_for.split(',').next() {
            return first_ip.trim().to_string();
        }
    }

    if let Some(real_ip) = headerStr(headers, "X-Real-IP") {
        return real_ip.to_string();
    }

    if let Some(forwarded) = headerStr(headers, "Forwarded") {
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

fn proofSecret() -> Option<String> {
    std::env::var("BROWSER_PROOF_SECRET")
        .or_else(|_| std::env::var("JWT_SECRET"))
        .ok()
        .filter(|secret| !secret.trim().is_empty())
        .or_else(|| {
            if isDevelopment() {
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

fn browserProofTtlSeconds(source: &str) -> usize {
    if source == BROWSER_PROOF_SOURCE_WARMUP {
        envUsize("BROWSER_PROOF_WARMUP_TTL_SECONDS", 30)
    } else {
        envUsize("BROWSER_PROOF_TTL_SECONDS", 300)
    }
}

fn warmupLockTtlSeconds() -> usize {
    envUsize("BROWSER_PROOF_WARMUP_LOCK_SECONDS", 120)
}

fn warmupBurstLimit() -> u64 {
    envU64("BROWSER_PROOF_WARMUP_BURST", 4)
}

fn envBool(name: &str) -> bool {
    std::env::var(name)
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false)
}

fn envU32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn envUsize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn envU64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn envString(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn isDevelopment() -> bool {
    envBool("DEBUG_MODE")
}

fn jsonError(status: StatusCode, error: &'static str) -> Response {
    (
        status,
        Json(ErrorBody {
            error,
            status: status.as_u16(),
            message: errorMessage(error),
        }),
    )
        .into_response()
}

fn errorMessage(error: &'static str) -> Option<&'static str> {
    match error {
        "browser_proof_required" => Some(
            "This endpoint requires a browser proof. Browser clients should wait for the Turnstile/browser-proof exchange and retry. Bots, scripts, and integrations should use an API key instead; API keys can be generated from your Uma account at any time.",
        ),
        _ => None,
    }
}

fn rateLimited(retry_after: u64) -> Response {
    let mut response = jsonError(StatusCode::TOO_MANY_REQUESTS, "rate_limited");
    if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}
