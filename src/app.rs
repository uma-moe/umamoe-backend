use crate::config::envFlag;
use crate::notify::TaskNotifier;
use crate::{auth, database, handlers, notify, redis_store, AppState};
use anyhow::Context;
use std::net::SocketAddr;
use tracing::{info, warn, Level};
use tracing_subscriber::EnvFilter;
fn defaultSearchServiceUrl() -> String {
    if std::path::Path::new("/.dockerenv").exists() {
        "http://umamoe-search:3202".to_string()
    } else {
        "http://127.0.0.1:3002".to_string()
    }
}

pub(crate) async fn run() -> anyhow::Result<()> {
    // Initialize tracing - production uses WARN/ERROR only, development uses INFO
    let isDevelopment = std::env::var("DEBUG_MODE").unwrap_or_default() == "true";

    if isDevelopment {
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_env_filter(EnvFilter::new(
                "honsemoe_backend_v2=info,honsemoe_backend=info,sqlx=info,info",
            ))
            .init();
        info!("🔧 Development mode: INFO logging enabled with SQL query logging");
    } else {
        tracing_subscriber::fmt()
            .with_max_level(Level::WARN)
            .with_env_filter(EnvFilter::new(
                "honsemoe_backend_v2=warn,honsemoe_backend=warn,sqlx=warn,warn",
            ))
            .init();
    }

    // Load environment variables
    dotenvy::dotenv().ok();
    let simulator = Some(std::sync::Arc::new(
        handlers::simulator::Simulator::fromEnv()?,
    ));

    // Database connection
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;

    let pool = database::createPool(&database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;
    let require_writable_database = std::env::var("REQUIRE_WRITABLE_DATABASE")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);
    database::logDatabaseWriteState(&pool, require_writable_database).await?;

    let user_writes_disabled = envFlag("USER_WRITES_DISABLED") || envFlag("BETA_READ_ONLY");
    if user_writes_disabled {
        warn!(
            "🔒 User writes are disabled: auth mutations, task creation, bookmarks, profile settings, daily stats, and request usage writes will be rejected or skipped"
        );
    }

    let skip_migrations = envFlag("SKIP_MIGRATIONS");
    database::migrations::run(&pool, skip_migrations).await?;

    // Initialise JWT secret (optional — auth endpoints won't work without it)
    match std::env::var("JWT_SECRET") {
        Ok(secret) => {
            auth::init(secret);
            info!("🔑 JWT authentication configured");
        }
        Err(_) if isDevelopment => {
            auth::init("dev-insecure-secret-change-me".to_string());
            warn!("⚠️ JWT_SECRET not set — using insecure default for development");
        }
        Err(_) => {
            warn!("⚠️ JWT_SECRET not set — auth endpoints will be unavailable");
        }
    }

    // Hash any remaining plaintext emails (one-time backfill, idempotent)
    if user_writes_disabled {
        warn!("🔒 Skipping email hash backfill because user writes are disabled");
    } else {
        auth::backfillEmailHashes(&pool).await;
    }

    let search_url =
        std::env::var("SEARCH_SERVICE_URL").unwrap_or_else(|_| defaultSearchServiceUrl());
    info!("Search service URL: {}", search_url);

    let search_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("failed to build reqwest client")?;

    let oauth_redirect_base = std::env::var("OAUTH_REDIRECT_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3001".to_string());
    info!("OAuth redirect base URL: {}", oauth_redirect_base);

    let redis_store = match redis_store::RedisStore::fromEnv() {
        Ok(Some(store)) => {
            info!("Redis token/cache store configured");
            Some(store)
        }
        Ok(None) => {
            warn!("REDIS_URL not set; browser proofs will use local JWT fallback only");
            None
        }
        Err(error) => {
            warn!("Redis token/cache store disabled: {}", error);
            None
        }
    };

    // Postgres LISTEN/NOTIFY dispatcher used by the partner-lookup SSE
    // endpoint. The trigger that emits these notifications is created in the
    // `add_partner_inheritance` migration.
    let task_notifier = TaskNotifier::new();
    notify::spawnListener(database_url.clone(), task_notifier.clone());

    let state = AppState {
        db: pool.clone(),
        search_client,
        search_url,
        oauth_redirect_base,
        task_notifier,
        redis_store,
        user_writes_disabled,
        simulator,
    };

    database::maintenance::spawnJobs(&pool, user_writes_disabled, skip_migrations);

    let internal_api_host = std::env::var("INTERNAL_API_HOST")
        .or_else(|_| std::env::var("BROWSER_PROOF_INTERNAL_HOST"))
        .unwrap_or_else(|_| "0.0.0.0".to_string());
    let internal_api_port = std::env::var("INTERNAL_API_PORT")
        .or_else(|_| std::env::var("BROWSER_PROOF_INTERNAL_PORT"))
        .unwrap_or_else(|_| "3201".to_string())
        .parse::<u16>()
        .context("INTERNAL_API_PORT must be a valid number")?;
    crate::http::spawnInternalApiServer(state.clone(), internal_api_host, internal_api_port)
        .await?;

    let app = crate::http::router(state);

    // Server configuration
    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse::<u16>()
        .context("PORT must be a valid number")?;

    info!("🚀 Server starting on http://{}:{}", host, port);

    // Start the server using Axum 0.7 syntax
    let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}
