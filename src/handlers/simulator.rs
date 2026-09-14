//! Authenticate once, then forward bounded requests to the private simulator.
#![allow(non_snake_case)]

pub use crate::models::simulator::Simulator;
use crate::{middleware::api_key, AppState};
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, FromRequest, Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

impl Simulator {
    /// Unconfigured backends keep serving their existing routes; simulator requests fail closed.
    pub fn fromEnv() -> anyhow::Result<Option<Self>> {
        let url = std::env::var("SIMULATOR_URL").unwrap_or_default();
        let key = std::env::var("SIMULATOR_BACKEND_KEY").unwrap_or_default();
        let grants = std::env::var("SIMULATOR_ALLOWED_KEYS").unwrap_or_default();
        if url.is_empty() && key.is_empty() && grants.is_empty() {
            return Ok(None);
        }

        let environment = std::env::var("APP_ENV").unwrap_or_default();
        Ok(Some(Self::new(
            simulatorUrl(&url, &environment),
            &key,
            &grants,
        )?))
    }

    pub(crate) fn new(url: &str, key: &str, grants: &str) -> anyhow::Result<Self> {
        let baseUrl = reqwest::Url::parse(url)?;
        anyhow::ensure!(
            matches!(baseUrl.scheme(), "http" | "https")
                && baseUrl.host_str().is_some()
                && baseUrl.username().is_empty()
                && baseUrl.password().is_none()
                && baseUrl.path() == "/"
                && baseUrl.query().is_none()
                && baseUrl.fragment().is_none(),
            "SIMULATOR_URL must be an http(s) origin without credentials, path, query or fragment"
        );
        anyhow::ensure!(
            (32..=512).contains(&key.len()) && key.bytes().all(|b| b.is_ascii_graphic()),
            "SIMULATOR_BACKEND_KEY must contain 32..=512 visible ASCII characters"
        );
        anyhow::ensure!(
            grants.len() <= 1024 * 1024,
            "SIMULATOR_ALLOWED_KEYS exceeds 1 MiB"
        );
        let mut allowedKeys = std::collections::HashSet::new();
        for grant in grants.split_ascii_whitespace() {
            anyhow::ensure!(
                validApiKey(grant),
                "SIMULATOR_ALLOWED_KEYS contains an invalid API key"
            );
            allowedKeys.insert(Sha256::digest(grant.as_bytes()).into());
        }

        let mut backendKey = HeaderValue::from_str(key)?;
        backendKey.set_sensitive(true);
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            client,
            baseUrl,
            backendKey,
            allowedKeys,
        })
    }

    fn allowedKey<'a>(&self, headers: &'a HeaderMap) -> Result<&'a str, Response> {
        let mut keys = headers.get_all("x-api-key").iter();
        let key = keys
            .next()
            .and_then(|v| v.to_str().ok())
            .filter(|key| validApiKey(key));
        if keys.next().is_some() || key.is_none() {
            return Err(error(
                StatusCode::UNAUTHORIZED,
                "exactly one valid X-API-Key is required",
            ));
        }

        let key = key.unwrap();
        if !self
            .allowedKeys
            .contains(&<[u8; 32]>::from(Sha256::digest(key.as_bytes())))
        {
            return Err(error(
                StatusCode::FORBIDDEN,
                "API key is not granted simulator access",
            ));
        }

        Ok(key)
    }

    async fn forward(&self, path: &str, contentType: Option<HeaderValue>, body: Bytes) -> Response {
        let mut url = self.baseUrl.clone();
        url.set_path(path);
        // Build fresh headers so callers cannot supply backend credentials or forward cookies.
        let mut request = self
            .client
            .post(url)
            .header("x-simulator-key", self.backendKey.clone())
            .body(body);
        if let Some(contentType) = contentType {
            request = request.header(header::CONTENT_TYPE, contentType);
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!("Simulator request failed: {err}");
                return error(
                    if err.is_timeout() {
                        StatusCode::GATEWAY_TIMEOUT
                    } else {
                        StatusCode::BAD_GATEWAY
                    },
                    "simulator unavailable",
                );
            }
        };
        let status = response.status();
        if status.is_redirection()
            || matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
        {
            return error(
                StatusCode::BAD_GATEWAY,
                "simulator rejected backend connection",
            );
        }

        let mut headers = HeaderMap::new();
        for name in [
            header::CONTENT_TYPE,
            header::CONTENT_ENCODING,
            header::RETRY_AFTER,
        ] {
            if let Some(value) = response.headers().get(&name) {
                headers.insert(name, value.clone());
            }
        }

        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        let stream = futures::stream::try_unfold(response, |mut response| async {
            Ok::<_, reqwest::Error>(response.chunk().await?.map(|chunk| (chunk, response)))
        });
        (status, headers, Body::from_stream(stream)).into_response()
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/sim/replay", post(proxy))
        .route("/api/sim/monte-carlo", post(proxy))
        .route("/api/sim/optimize", post(proxy))
        .route(
            "/api/sim/races/resimulate",
            post(proxy).layer(DefaultBodyLimit::max(8 * 1024 * 1024)),
        )
        .layer(DefaultBodyLimit::max(256 * 1024))
}

async fn proxy(State(state): State<AppState>, request: Request) -> Response {
    let Some(simulator) = state.simulator.as_ref() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "simulator is not configured",
        );
    };
    let rawKey = match simulator.allowedKey(request.headers()) {
        Ok(key) => key,
        Err(response) => return response,
    };
    let key = match api_key::resolve_api_key(&state.db, rawKey).await {
        Ok(Some(key)) => key,
        Ok(None) => return error(StatusCode::UNAUTHORIZED, "invalid or revoked API key"),
        Err(err) => {
            tracing::warn!("Simulator API key lookup failed: {err}");
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication unavailable",
            );
        }
    };
    let path = request.uri().path().to_owned();
    let contentType = request.headers().get(header::CONTENT_TYPE).cloned();
    let body = match Bytes::from_request(request, &state).await {
        Ok(body) => body,
        Err(rejection) => return rejection.into_response(),
    };
    if !state.user_writes_disabled {
        let endpoint = api_key::normalize_endpoint("POST", &path);
        if let Err(err) = api_key::record_api_key_usage(&state.db, &key, &endpoint).await {
            tracing::warn!("Simulator usage recording failed: {err}");
        }
    }

    simulator
        .forward(path.strip_prefix("/api").unwrap(), contentType, body)
        .await
}

fn simulatorUrl<'a>(configured: &'a str, environment: &str) -> &'a str {
    match (configured, environment) {
        ("", "beta") => "http://192.168.100.1:3109",
        ("", _) => "http://192.168.100.1:3009",
        _ => configured,
    }
}

fn validApiKey(key: &str) -> bool {
    (16..=512).contains(&key.len()) && key.bytes().all(|byte| byte.is_ascii_graphic())
}

fn error(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"error": message})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tower::ServiceExt;

    const BACKEND_KEY: &str = "test-backend-key-123456789012345678901234567890";
    const API_KEY: &str = "test-user-api-key-1234567890";

    #[test]
    fn urlDefaultsFollowDeploymentAndAllowOverrides() {
        for (configured, environment, expected) in [
            ("", "production", "http://192.168.100.1:3009/"),
            ("", "beta", "http://192.168.100.1:3109/"),
            ("", "", "http://192.168.100.1:3009/"),
            ("http://127.0.0.1:9000", "beta", "http://127.0.0.1:9000/"),
        ] {
            let simulator =
                Simulator::new(simulatorUrl(configured, environment), BACKEND_KEY, API_KEY)
                    .unwrap();
            assert_eq!(simulator.baseUrl.as_str(), expected);
        }
    }

    #[test]
    fn grantsAndConfigurationFailClosed() {
        let simulator = Simulator::new("http://127.0.0.1:3009", BACKEND_KEY, API_KEY).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-simulator-key", HeaderValue::from_static(BACKEND_KEY));
        assert_eq!(
            simulator.allowedKey(&headers).unwrap_err().status(),
            StatusCode::UNAUTHORIZED
        );
        headers.insert(
            "x-api-key",
            HeaderValue::from_static("ungranted-user-key-123456"),
        );
        assert_eq!(
            simulator.allowedKey(&headers).unwrap_err().status(),
            StatusCode::FORBIDDEN
        );
        headers.insert("x-api-key", HeaderValue::from_static(API_KEY));
        assert_eq!(simulator.allowedKey(&headers).unwrap(), API_KEY);
        headers.append("x-api-key", HeaderValue::from_static(API_KEY));
        assert_eq!(
            simulator.allowedKey(&headers).unwrap_err().status(),
            StatusCode::UNAUTHORIZED
        );
        for url in [
            "file:///tmp/private",
            "http://user:password@localhost",
            "http://localhost/sim",
            "http://localhost/?query",
            "http://localhost/#fragment",
        ] {
            assert!(Simulator::new(url, BACKEND_KEY, API_KEY).is_err());
        }

        assert!(Simulator::new("http://localhost", "short", API_KEY).is_err());
        assert!(Simulator::new("http://localhost", BACKEND_KEY, "short").is_err());
        assert!(Simulator::new("http://localhost", BACKEND_KEY, "")
            .unwrap()
            .allowedKeys
            .is_empty());
    }

    #[tokio::test]
    async fn forwardingPreservesBodyStatusAndRejectsRedirects() {
        let app = Router::new()
            .route(
                "/sim/replay",
                post(|headers: HeaderMap, body: Bytes| async move {
                    assert_eq!(headers["x-simulator-key"], BACKEND_KEY);
                    assert!(
                        !headers.contains_key("x-api-key")
                            && !headers.contains_key("cookie")
                            && !headers.contains_key("authorization")
                    );
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        [
                            (header::CONTENT_TYPE, "application/octet-stream"),
                            (header::RETRY_AFTER, "7"),
                            (header::SET_COOKIE, "must-not-forward=true"),
                        ],
                        body,
                    )
                }),
            )
            .route(
                "/sim/optimize",
                post(|| async {
                    (
                        StatusCode::TEMPORARY_REDIRECT,
                        [(header::LOCATION, "/sim/replay")],
                    )
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let simulator = Simulator::new(&format!("http://{address}"), BACKEND_KEY, API_KEY).unwrap();
        let body =
            Bytes::from_static(b"{ \"untouched\": [1.000, 1e-20], \"unicode\": \"\\u1234\" }");
        let response = simulator
            .forward(
                "/sim/replay",
                Some(HeaderValue::from_static("application/json")),
                body.clone(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "7");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert!(!response.headers().contains_key(header::SET_COOKIE));
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap(),
            body
        );
        assert_eq!(
            simulator
                .forward("/sim/optimize", None, Bytes::new())
                .await
                .status(),
            StatusCode::BAD_GATEWAY
        );
        task.abort();
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL database named simulator_forwarding_test via SIMULATOR_TEST_DATABASE_URL"]
    async fn authenticatedForwardingRecordsUsageOnce() {
        let url = std::env::var("SIMULATOR_TEST_DATABASE_URL").unwrap();
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str(), Some("127.0.0.1"));
        assert_eq!(parsed.path(), "/simulator_forwarding_test");
        let db = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        sqlx::raw_sql("CREATE TABLE api_keys (id uuid PRIMARY KEY, user_id uuid NOT NULL, name text NOT NULL, key_hash text NOT NULL, revoked boolean NOT NULL DEFAULT false, last_used timestamptz, total_requests bigint NOT NULL DEFAULT 0); CREATE TABLE api_key_usage (api_key_id uuid, endpoint text, date date, requests bigint, PRIMARY KEY (api_key_id, endpoint, date));").execute(&db).await.unwrap();
        let keyId = uuid::Uuid::new_v4();
        sqlx::query(
            "INSERT INTO api_keys (id, user_id, name, key_hash) VALUES ($1, $2, 'test', $3)",
        )
        .bind(keyId)
        .bind(uuid::Uuid::new_v4())
        .bind(api_key::hash_api_key(API_KEY))
        .execute(&db)
        .await
        .unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let upstream = Router::new()
            .fallback(post(move |headers: HeaderMap, body: Bytes| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(headers["x-simulator-key"], BACKEND_KEY);
                    assert!(
                        !headers.contains_key("x-api-key")
                            && !headers.contains_key("cookie")
                            && !headers.contains_key("authorization")
                    );
                    (StatusCode::OK, body)
                }
            }))
            .layer(DefaultBodyLimit::max(8 * 1024 * 1024));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        });
        let simulator = Simulator::new(&format!("http://{address}"), BACKEND_KEY, API_KEY).unwrap();
        let state = AppState {
            db: db.clone(),
            search_client: reqwest::Client::new(),
            search_url: String::new(),
            oauth_redirect_base: String::new(),
            task_notifier: crate::notify::TaskNotifier::new(),
            redis_store: None,
            user_writes_disabled: false,
            simulator: Some(Arc::new(simulator)),
        };
        let app = routes().with_state(state);
        let request = || {
            Request::post("/api/sim/replay")
                .header("x-api-key", API_KEY)
                .header("content-type", "application/json")
                .header("x-simulator-key", "client-forged-service-key")
                .header("authorization", "Bearer must-not-forward")
                .header("cookie", "must-not-forward=true")
                .body(Body::from("{ \"exact\": 1.00 }"))
                .unwrap()
        };
        let response = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap(),
            "{ \"exact\": 1.00 }"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT total_requests FROM api_keys WHERE id = $1")
                .bind(keyId)
                .fetch_one(&db)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT requests FROM api_key_usage WHERE endpoint = 'POST /api/sim/replay'"
            )
            .fetch_one(&db)
            .await
            .unwrap(),
            1
        );

        for (path, size, expected) in [
            (
                "/api/sim/replay",
                256 * 1024 + 1,
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
            ("/api/sim/races/resimulate", 300_000, StatusCode::OK),
            (
                "/api/sim/races/resimulate",
                8 * 1024 * 1024 + 1,
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post(path)
                        .header("x-api-key", API_KEY)
                        .body(Body::from(vec![b'x'; size]))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{path} ({size} bytes)");
        }

        assert_eq!(
            app.clone()
                .oneshot(
                    Request::post("/api/sim/unknown")
                        .body(Body::empty())
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        sqlx::query("UPDATE api_keys SET revoked = true WHERE id = $1")
            .bind(keyId)
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(
            app.oneshot(request()).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "rejected requests must not reach the simulator"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT total_requests FROM api_keys WHERE id = $1")
                .bind(keyId)
                .fetch_one(&db)
                .await
                .unwrap(),
            2
        );
        task.abort();
        db.close().await;
    }
}
