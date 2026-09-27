use crate::handlers::{
    affinity, auth as auth_handlers, borrow, carat_planner, circles, docs, partner, profile,
    rankings, search, shame, sharing, stats, tasks, version,
};
use crate::{handlers, middleware, AppState};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use std::net::{IpAddr, SocketAddr};
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info, warn};

fn corsLayer() -> CorsLayer {
    // Configure CORS - more permissive for development, strict for production
    let isDevelopment = std::env::var("DEBUG_MODE").unwrap_or_default() == "true";

    if isDevelopment {
        info!("🔓 Development mode: Using permissive CORS with credentials");
        let dev_origins: Vec<_> = std::env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "http://localhost:4200,http://localhost:3000".to_string())
            .split(',')
            .filter_map(|o| o.trim().parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(dev_origins)
            .allow_credentials(true)
    } else {
        let allowed_origins = std::env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "https://honse.moe,https://www.honse.moe,https://uma.moe,https://www.uma.moe,http://honse.moe,http://www.honse.moe,http://uma.moe,http://www.uma.moe".to_string());

        info!("🔍 Raw ALLOWED_ORIGINS: {}", allowed_origins);

        let origins: Result<Vec<_>, _> = allowed_origins
            .split(',')
            .map(|origin| {
                let trimmed = origin.trim();
                info!("  📍 Parsing origin: '{}'", trimmed);
                trimmed.parse()
            })
            .collect();

        match origins {
            Ok(parsed_origins) => {
                info!("🔒 Production mode: CORS configured for origins: {}", allowed_origins);
                for origin in &parsed_origins {
                    info!("  - Allowed origin: {:?}", origin);
                }
                CorsLayer::new()
                    .allow_origin(parsed_origins)
                    .allow_credentials(true)
            },
            Err(e) => {
                warn!("⚠️ Failed to parse ALLOWED_ORIGINS, using defaults: {}", e);
                let default_origins = vec![
                    "https://honse.moe".parse().unwrap(),
                    "https://www.honse.moe".parse().unwrap(),
                    "https://uma.moe".parse().unwrap(),
                    "https://www.uma.moe".parse().unwrap(),
                    "http://honse.moe".parse().unwrap(),
                    "http://www.honse.moe".parse().unwrap(),
                    "http://uma.moe".parse().unwrap(),
                    "http://www.uma.moe".parse().unwrap(),
                ];
                info!("🔒 Using fallback origins with {} entries", default_origins.len());
                for origin in &default_origins {
                    info!("  - Fallback origin: {:?}", origin);
                }
                CorsLayer::new()
                    .allow_origin(default_origins)
                    .allow_credentials(true)
            }
        }
    }
    .allow_methods([
        axum::http::Method::GET,
        axum::http::Method::POST,
        axum::http::Method::PUT,
        axum::http::Method::DELETE,
        axum::http::Method::OPTIONS,
    ])
    .allow_headers([
        axum::http::header::CONTENT_TYPE,
        axum::http::header::AUTHORIZATION,
        axum::http::header::ACCEPT,
        axum::http::header::USER_AGENT,
        axum::http::header::REFERER,
        axum::http::header::ORIGIN,
        "CF-Turnstile-Token".parse().unwrap(),
        "X-Turnstile-Token".parse().unwrap(),
        "X-Browser-Proof".parse().unwrap(),
        "X-Browser-Proof-Source".parse().unwrap(),
        "X-API-Key".parse().unwrap(),
    ])
    .expose_headers([
        "X-Browser-Proof".parse().unwrap(),
        "X-Browser-Proof-TTL".parse().unwrap(),
        "X-Browser-Proof-Source".parse().unwrap(),
    ])
}

pub(crate) fn router(state: AppState) -> Router {
    let cors = corsLayer();
    // Build the application with proper routing and middleware
    // Public-read endpoints still require an API key or browser proof to prevent scraping.
    let public_routes = publicReadApiRoutes()
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::user_writes::userWriteGuardMiddleware,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::api_key::apiKeyTrackingMiddleware,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::turnstile::apiProtectionMiddleware,
                ))
                .layer(cors.clone()),
        )
        .with_state(state.clone());

    // Open endpoints: no API key / browser proof required.
    // Keep the user-write guard so read-only deployments still block daily counter writes.
    let open_routes = openApiRoutes()
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::user_writes::userWriteGuardMiddleware,
                ))
                .layer(cors.clone()),
        )
        .with_state(state.clone());

    // Protected endpoints (Turnstile + restricted CORS)
    let protected_routes = protectedApiRoutes()
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::user_writes::userWriteGuardMiddleware,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::api_key::apiKeyTrackingMiddleware,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::turnstile::apiProtectionMiddleware,
                ))
                .layer(cors.clone()),
        )
        .with_state(state.clone());

    // Simulator routes enforce API-key grants and record usage themselves, exactly once.
    let simulator_routes = handlers::simulator::routes()
        .layer(cors.clone())
        .with_state(state.clone());

    let version_routes = versionApiRoutes()
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(cors),
        )
        .with_state(state);

    // Merge public and protected routes
    version_routes
        .merge(simulator_routes)
        .merge(open_routes)
        .merge(public_routes)
        .merge(protected_routes)
}

fn publicReadApiRoutes() -> Router<AppState> {
    Router::new()
        .nest("/api/v4/circles", circles::router())
        .nest("/api/v4/rankings", rankings::router())
        .nest("/api/v4/user/profile", profile::router())
        .nest("/api/v4/shame", shame::router())
        .nest("/api/carat-planner", carat_planner::publicRouter())
}

fn openApiRoutes() -> Router<AppState> {
    Router::new().nest("/api/stats", stats::publicRouter())
}

fn protectedApiRoutes() -> Router<AppState> {
    Router::new()
        .nest("/api/docs", docs::router())
        .nest("/api/borrow", borrow::router())
        .nest("/api/v3/borrow", borrow::router())
        .nest("/api/stats", stats::protectedRouter())
        .nest("/api/tasks", tasks::router())
        .nest("/api/v3/tasks", tasks::router())
        .nest("/api/v4/partner", partner::router())
        .nest("/api/v3", search::router())
        .nest("/api/v4/affinity", affinity::router())
        .nest("/api/auth", auth_handlers::publicRouter())
        .nest("/api/auth", auth_handlers::authenticatedRouter())
        .nest("/api/carat-planner", carat_planner::authenticatedRouter())
        .nest("/", sharing::router())
}

fn versionApiRoutes() -> Router<AppState> {
    Router::new()
        .route("/api/health", get(healthCheck))
        .route("/api/ver", get(version::getVersion))
        .route("/api/ver/history", get(version::getVersionHistory))
}

async fn healthCheck() -> Result<Json<serde_json::Value>, StatusCode> {
    Ok(Json(serde_json::json!({
        "status": "healthy",
        "service": "honsemoe-backend",
        "timestamp": chrono::Utc::now(),
        "version": "1.0.0",
        "endpoints": {
            "search": "/api/v3/search",
            "stats": "/api/stats",
            "tasks": "/api/tasks",
            "circles": "/api/v4/circles",
            "rankings": "/api/v4/rankings",
            "health": "/api/health"
        }
    })))
}

pub(crate) async fn spawnInternalApiServer(
    state: AppState,
    host: String,
    port: u16,
) -> anyhow::Result<()> {
    let data_routes = Router::new()
        .merge(openApiRoutes())
        .merge(publicReadApiRoutes())
        .merge(protectedApiRoutes())
        .layer(
            ServiceBuilder::new().layer(axum::middleware::from_fn_with_state(
                state.clone(),
                middleware::user_writes::userWriteGuardMiddleware,
            )),
        )
        .with_state(state.clone());

    let version_routes = versionApiRoutes().with_state(state.clone());

    let internal_auth_routes = Router::new()
        .route(
            "/api/auth/browser-proof/internal",
            post(middleware::turnstile::issueInternalBrowserProof),
        )
        .route(
            "/api/auth/verify/internal",
            post(middleware::turnstile::verifyInternalCredential),
        )
        .with_state(state);

    let app = version_routes
        .merge(data_routes)
        .merge(internal_auth_routes)
        .layer(axum::middleware::from_fn(internalNetworkOnlyMiddleware))
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
    info!("🔒 Internal API listening on http://{}:{}", host, port);

    tokio::spawn(async move {
        if let Err(error) = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        {
            error!("Internal API stopped: {}", error);
        }
    });

    Ok(())
}

async fn internalNetworkOnlyMiddleware(
    connect_info: Option<axum::extract::ConnectInfo<SocketAddr>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let Some(addr) = connect_info.map(|ci| ci.0) else {
        return internalForbidden("missing_connect_info");
    };

    if !isInternalClientIp(addr.ip()) {
        warn!(
            "Rejected internal API request from non-private address {}",
            addr.ip()
        );
        return internalForbidden("internal_network_required");
    }

    next.run(request).await
}

fn internalForbidden(error: &'static str) -> axum::response::Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "valid": false,
            "error": error,
            "status": StatusCode::FORBIDDEN.as_u16()
        })),
    )
        .into_response()
}

fn isInternalClientIp(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            ip.is_loopback() || ip.is_private() || (octets[0] == 169 && octets[1] == 254)
        }
        IpAddr::V6(ip) => {
            let first_segment = ip.segments()[0];
            ip.is_loopback()
                || (first_segment & 0xfe00) == 0xfc00
                || (first_segment & 0xffc0) == 0xfe80
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, extract::ConnectInfo, http::Request};
    use tower::ServiceExt;

    #[tokio::test]
    async fn routingPreservesHealthAndDisabledSimulator() {
        let state = AppState {
            db: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused:unused@127.0.0.1/unused")
                .unwrap(),
            search_client: reqwest::Client::new(),
            search_url: String::new(),
            oauth_redirect_base: String::new(),
            task_notifier: crate::notify::TaskNotifier::new(),
            redis_store: None,
            user_writes_disabled: true,
            simulator: None,
        };
        let app = router(state);
        for (method, path, expected) in [
            ("GET", "/api/health", StatusCode::OK),
            ("POST", "/api/sim/replay", StatusCode::SERVICE_UNAVAILABLE),
            ("GET", "/api/v4/shame/hall", StatusCode::FORBIDDEN),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{method} {path}");
        }
    }

    #[tokio::test]
    async fn internalRoutesRequirePrivateConnectionAddress() {
        let app = Router::new()
            .route("/", get(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn(internalNetworkOnlyMiddleware));
        for (address, expected) in [
            (None, StatusCode::FORBIDDEN),
            (Some("203.0.113.1:1234"), StatusCode::FORBIDDEN),
            (Some("[2001:db8::1]:1234"), StatusCode::FORBIDDEN),
            (Some("127.0.0.1:1234"), StatusCode::OK),
            (Some("192.168.1.2:1234"), StatusCode::OK),
            (Some("[::1]:1234"), StatusCode::OK),
            (Some("[fd00::1]:1234"), StatusCode::OK),
        ] {
            let mut request = Request::new(Body::empty());
            if let Some(address) = address {
                request
                    .extensions_mut()
                    .insert(ConnectInfo(address.parse::<SocketAddr>().unwrap()));
            }
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                expected,
                "{address:?}"
            );
        }
    }
}
