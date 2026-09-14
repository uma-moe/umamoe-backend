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
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

impl Simulator {
    /// Use the private deployment defaults; user grants are checked on every request.
    pub fn fromEnv() -> anyhow::Result<Self> {
        let url = std::env::var("SIMULATOR_URL").unwrap_or_default();
        let file = std::env::var("SIMULATOR_ALLOWED_KEYS_FILE").unwrap_or_default();

        let environment = std::env::var("APP_ENV").unwrap_or_default();
        Self::new(
            simulatorUrl(&url, &environment),
            if file.is_empty() {
                "/config/simulator/allowed-keys.txt"
            } else {
                &file
            },
        )
    }

    pub(crate) fn new(
        url: &str,
        allowedKeysFile: impl Into<std::path::PathBuf>,
    ) -> anyhow::Result<Self> {
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
        let allowedKeysFile = allowedKeysFile.into();
        anyhow::ensure!(
            !allowedKeysFile.as_os_str().is_empty(),
            "allowed-key file path is empty"
        );

        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            client,
            baseUrl,
            allowedKeysFile,
        })
    }

    async fn allowedKey<'a>(&self, headers: &'a HeaderMap) -> Result<&'a str, Response> {
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
        let granted = match self.keyIsGranted(key).await {
            Ok(granted) => granted,
            Err(err) => {
                tracing::warn!("Simulator allowed-key file unavailable: {err}");
                return Err(error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "simulator access list unavailable",
                ));
            }
        };
        if !granted {
            return Err(error(
                StatusCode::FORBIDDEN,
                "API key is not granted simulator access",
            ));
        }

        Ok(key)
    }

    async fn keyIsGranted(&self, key: &str) -> anyhow::Result<bool> {
        // ponytail: scan at most 1 MiB per request; cache by file version if lists grow.
        let file = tokio::fs::File::open(&self.allowedKeysFile).await?;
        let mut contents = String::new();
        file.take(1024 * 1024 + 1)
            .read_to_string(&mut contents)
            .await?;
        anyhow::ensure!(
            contents.len() <= 1024 * 1024,
            "allowed-key file exceeds 1 MiB"
        );
        let requested = Sha256::digest(key.as_bytes());
        let mut granted = false;
        for grant in contents
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            anyhow::ensure!(
                validApiKey(grant),
                "allowed-key file contains an invalid API key"
            );
            granted |= Sha256::digest(grant.as_bytes()) == requested;
        }

        Ok(granted)
    }

    async fn forward(&self, path: &str, headers: &HeaderMap, body: Bytes) -> Response {
        let mut url = self.baseUrl.clone();
        url.set_path(path);
        // Only forward representation headers; user credentials stay in the backend.
        let mut request = self.client.post(url).body(body);
        for name in [header::CONTENT_TYPE, header::ACCEPT] {
            for value in headers.get_all(&name) {
                request = request.header(&name, value);
            }
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
        if status.is_redirection() {
            return error(StatusCode::BAD_GATEWAY, "simulator returned a redirect");
        }

        let mut headers = HeaderMap::new();
        for name in [
            header::CONTENT_TYPE,
            header::CONTENT_ENCODING,
            header::RETRY_AFTER,
            header::HeaderName::from_static("server-timing"),
        ] {
            if let Some(value) = response.headers().get(&name) {
                headers.insert(name, value.clone());
            }
        }

        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .eq_ignore_ascii_case("text/event-stream")
            })
        {
            // Set this here: the internal NGINX hop consumes upstream X-Accel headers.
            headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
            headers.insert(header::VARY, HeaderValue::from_static("Accept"));
        }
        let stream = futures::stream::try_unfold(response, |mut response| async {
            Ok::<_, reqwest::Error>(response.chunk().await?.map(|chunk| (chunk, response)))
        });
        (status, headers, Body::from_stream(stream)).into_response()
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/sim/replay", post(proxy))
        .route("/api/sim/stamina", post(proxy))
        .route("/api/sim/monte-carlo", post(proxy))
        .route("/api/sim/optimize", post(proxy))
        .route(
            "/api/sim/races/resimulate",
            post(proxy).layer(DefaultBodyLimit::max(8 * 1024 * 1024)),
        )
        .layer(DefaultBodyLimit::max(256 * 1024))
}

async fn proxy(State(state): State<AppState>, request: Request) -> Response {
    let started = Instant::now();
    let Some(simulator) = state.simulator.as_ref() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "simulator is not configured",
        );
    };
    let rawKey = match simulator.allowedKey(request.headers()).await {
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
    let authMs = started.elapsed().as_secs_f64() * 1000.0;
    let path = request.uri().path().to_owned();
    let requestHeaders = request.headers().clone();
    let body = match Bytes::from_request(request, &state).await {
        Ok(body) => body,
        Err(rejection) => return rejection.into_response(),
    };
    // Record once alongside the upstream request, keeping both operations awaited.
    let usage = async {
        let started = Instant::now();
        if !state.user_writes_disabled {
            let endpoint = api_key::normalize_endpoint("POST", &path);
            if let Err(err) = api_key::record_api_key_usage(&state.db, &key, &endpoint).await {
                tracing::warn!("Simulator usage recording failed: {err}");
            }
        }

        started.elapsed().as_secs_f64() * 1000.0
    };
    let forward = async {
        let started = Instant::now();
        let response = simulator
            .forward(path.strip_prefix("/api").unwrap(), &requestHeaders, body)
            .await;
        (response, started.elapsed().as_secs_f64() * 1000.0)
    };
    let ((mut response, upstreamMs), usageMs) = tokio::join!(forward, usage);
    let backendMs = started.elapsed().as_secs_f64() * 1000.0;
    response.headers_mut().append("server-timing", format!(
        "auth;dur={authMs:.2}, usage;dur={usageMs:.2}, upstream;dur={upstreamMs:.2}, backend;dur={backendMs:.2}"
    ).parse().expect("numeric timing header"));
    response
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
                Simulator::new(simulatorUrl(configured, environment), "unused-keys.txt").unwrap();
            assert_eq!(simulator.baseUrl.as_str(), expected);
        }
    }

    #[tokio::test]
    async fn grantsReloadWithoutRestartAndFailClosed() {
        let root = std::env::temp_dir().join(format!("simulator-keys-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir(&root).await.unwrap();
        let file = root.join("allowed-keys.txt");
        let simulator = Simulator::new("http://127.0.0.1:3009", &file).unwrap();
        let mut headers = HeaderMap::new();
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::UNAUTHORIZED
        );
        headers.insert(
            "x-api-key",
            HeaderValue::from_static("ungranted-user-key-123456"),
        );
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        tokio::fs::write(&file, format!("# simulator access\r\n\r\n{API_KEY}\r\n"))
            .await
            .unwrap();
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::FORBIDDEN
        );
        headers.insert("x-api-key", HeaderValue::from_static(API_KEY));
        assert_eq!(simulator.allowedKey(&headers).await.unwrap(), API_KEY);

        let replacement = root.join("replacement.txt");
        tokio::fs::write(&replacement, "different-user-api-key-1234567890\n")
            .await
            .unwrap();
        tokio::fs::rename(&replacement, &file).await.unwrap();
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::FORBIDDEN
        );
        tokio::fs::write(&file, "").await.unwrap();
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::FORBIDDEN
        );
        tokio::fs::write(&file, format!("{API_KEY}\nshort\n"))
            .await
            .unwrap();
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        tokio::fs::write(&file, vec![b' '; 1024 * 1024 + 1])
            .await
            .unwrap();
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        tokio::fs::remove_file(&file).await.unwrap();
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        headers.append("x-api-key", HeaderValue::from_static(API_KEY));
        assert_eq!(
            simulator.allowedKey(&headers).await.unwrap_err().status(),
            StatusCode::UNAUTHORIZED
        );
        for url in [
            "file:///tmp/private",
            "http://user:password@localhost",
            "http://localhost/sim",
            "http://localhost/?query",
            "http://localhost/#fragment",
        ] {
            assert!(Simulator::new(url, &file).is_err());
        }

        assert!(Simulator::new("http://localhost", "").is_err());
        tokio::fs::remove_dir(&root).await.unwrap();
    }

    #[tokio::test]
    async fn forwardingPreservesBodyStatusAndRejectsRedirects() {
        let app = Router::new()
            .route(
                "/sim/stamina",
                post(|headers: HeaderMap, body: Bytes| async move {
                    assert!(
                        !headers.contains_key("x-simulator-key")
                            && !headers.contains_key("x-api-key")
                            && !headers.contains_key("cookie")
                            && !headers.contains_key("authorization")
                    );
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        [
                            (header::CONTENT_TYPE, "application/octet-stream"),
                            (header::RETRY_AFTER, "7"),
                            (
                                header::HeaderName::from_static("server-timing"),
                                "queue;dur=12.00, compute;dur=34.00",
                            ),
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
                        [(header::LOCATION, "/sim/stamina")],
                    )
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let simulator = Simulator::new(&format!("http://{address}"), "unused-keys.txt").unwrap();
        let body =
            Bytes::from_static(b"{ \"untouched\": [1.000, 1e-20], \"unicode\": \"\\u1234\" }");
        let response = simulator
            .forward(
                "/sim/stamina",
                &HeaderMap::from_iter([(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )]),
                body.clone(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "7");
        assert_eq!(
            response.headers()["server-timing"],
            "queue;dur=12.00, compute;dur=34.00"
        );
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
                .forward("/sim/optimize", &HeaderMap::new(), Bytes::new())
                .await
                .status(),
            StatusCode::BAD_GATEWAY
        );
        task.abort();
    }

    #[tokio::test]
    async fn forwardingStreamsEventsBeforeCompletionWithoutForwardingCredentials() {
        use futures::StreamExt;
        let finish = Arc::new(tokio::sync::Notify::new());
        let gate = finish.clone();
        let app = Router::new().route(
            "/sim/monte-carlo",
            post(move |headers: HeaderMap| {
                let gate = gate.clone();
                async move {
                    assert!(headers
                        .get_all(header::ACCEPT)
                        .iter()
                        .any(|value| value == "text/event-stream"));
                    for name in ["x-api-key", "authorization", "cookie", "x-simulator-key"] {
                        assert!(!headers.contains_key(name));
                    }
                    let events = futures::stream::once(async {
                        Ok::<_, std::convert::Infallible>(Bytes::from_static(
                            b"event: queued\ndata: {\"waited_ms\":0}\n\n",
                        ))
                    })
                    .chain(futures::stream::once(async move {
                        gate.notified().await;
                        Ok::<_, std::convert::Infallible>(Bytes::from_static(
                            b"event: result\ndata: {\"runs\":100}\n\n",
                        ))
                    }));
                    // No X-Accel header upstream: internal NGINX consumes it in production.
                    (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        Body::from_stream(events),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let simulator = Simulator::new(&format!("http://{address}"), "unused-keys.txt").unwrap();
        let mut headers = HeaderMap::from_iter([
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (header::ACCEPT, HeaderValue::from_static("application/json")),
            (
                header::COOKIE,
                HeaderValue::from_static("must-not-forward=true"),
            ),
            (
                header::AUTHORIZATION,
                HeaderValue::from_static("Bearer must-not-forward"),
            ),
        ]);
        headers.append(
            header::ACCEPT,
            HeaderValue::from_static("text/event-stream"),
        );
        headers.insert("x-api-key", HeaderValue::from_static(API_KEY));
        headers.insert(
            "x-simulator-key",
            HeaderValue::from_static("must-not-forward"),
        );
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            simulator.forward("/sim/monte-carlo", &headers, Bytes::from_static(b"{}")),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        assert_eq!(response.headers()["x-accel-buffering"], "no");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first, "event: queued\ndata: {\"waited_ms\":0}\n\n");
        assert!(tokio::time::timeout(Duration::from_millis(20), body.next())
            .await
            .is_err());
        finish.notify_one();
        let last = tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(last, "event: result\ndata: {\"runs\":100}\n\n");
        assert!(body.next().await.is_none());
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
                    assert!(
                        !headers.contains_key("x-simulator-key")
                            && !headers.contains_key("x-api-key")
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
        let file =
            std::env::temp_dir().join(format!("simulator-keys-{}.txt", uuid::Uuid::new_v4()));
        tokio::fs::write(&file, API_KEY).await.unwrap();
        let simulator = Simulator::new(&format!("http://{address}"), &file).unwrap();
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
        tokio::fs::remove_file(&file).await.unwrap();
    }
}
