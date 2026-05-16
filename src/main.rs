use axum::{http::StatusCode, response::Json, routing::get, Router};
use sqlx::PgPool;
use std::net::SocketAddr;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info, warn, Level};
use tracing_subscriber::EnvFilter;

mod auth;
mod cache;
mod database;
mod errors;
mod handlers;
mod middleware;
mod models;
mod notify;
mod redis_store;

use handlers::{
    affinity, auth as auth_handlers, circles, docs, partner, profile, rankings, search, sharing,
    stats, tasks, version,
};
use notify::TaskNotifier;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub search_client: reqwest::Client,
    pub search_url: String,
    pub oauth_redirect_base: String,
    pub task_notifier: TaskNotifier,
    pub redis_store: Option<redis_store::RedisStore>,
}

fn default_search_service_url() -> String {
    if std::path::Path::new("/.dockerenv").exists() {
        "http://search:3002".to_string()
    } else {
        "http://127.0.0.1:3002".to_string()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing - production uses WARN/ERROR only, development uses INFO
    let is_development = std::env::var("DEBUG_MODE").unwrap_or_default() == "true";

    if is_development {
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_env_filter(EnvFilter::new("honsemoe_backend=info,sqlx=info,info"))
            .init();
        info!("🔧 Development mode: INFO logging enabled with SQL query logging");
    } else {
        tracing_subscriber::fmt()
            .with_max_level(Level::WARN)
            .with_env_filter(EnvFilter::new("honsemoe_backend=warn,sqlx=warn,warn"))
            .init();
    }

    // Load environment variables
    dotenvy::dotenv().ok();

    // Database connection
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = database::create_pool(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");

    // Run migrations with better error handling (can be disabled via env var)
    let skip_migrations = std::env::var("SKIP_MIGRATIONS")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);
    let ignore_migration_version_mismatch = std::env::var("IGNORE_MIGRATION_VERSION_MISMATCH")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    if skip_migrations {
        warn!("⚠️ Skipping migrations due to SKIP_MIGRATIONS=true");
    } else {
        warn!("🔄 Running database migrations...");

        let migrator = sqlx::migrate!("./migrations");

        // Determine which migrations are already applied so we can log only the new ones.
        let applied: std::collections::HashSet<i64> =
            sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success = true")
                .fetch_all(&pool)
                .await
                .unwrap_or_default()
                .into_iter()
                .collect();

        let pending: Vec<_> = migrator
            .migrations
            .iter()
            .filter(|m| !applied.contains(&m.version))
            .collect();

        if pending.is_empty() {
            warn!("✅ All migrations already applied, nothing to do");
        } else {
            warn!("📋 {} migration(s) pending:", pending.len());
            for m in &pending {
                warn!("   ▶ {} — {}", m.version, m.description);
            }

            match migrator.run(&pool).await {
                Ok(_) => warn!("✅ Migrations completed successfully"),
                Err(sqlx::migrate::MigrateError::VersionMismatch(version)) => {
                    if ignore_migration_version_mismatch {
                        warn!(
                            "⚠️  Ignoring migration version mismatch for version {}",
                            version
                        );
                        warn!(
                            "Database migration history differs from this checkout, but startup will continue"
                        );
                    } else {
                        error!("⚠️  Migration version mismatch: {}", version);
                        error!("Database has different migration state than expected");
                        error!("Set IGNORE_MIGRATION_VERSION_MISMATCH=true to continue startup when this is intentional");
                        error!("Consider resetting migrations: DROP TABLE _sqlx_migrations;");
                        return Err(anyhow::anyhow!("Migration version mismatch"));
                    }
                }
                Err(e) => {
                    error!("❌ Failed to run migrations: {}", e);
                    error!("Migration error details: {:?}", e);
                    return Err(anyhow::anyhow!("Migration failed: {}", e));
                }
            }
        }
    }

    // Initialise JWT secret (optional — auth endpoints won't work without it)
    match std::env::var("JWT_SECRET") {
        Ok(secret) => {
            auth::init(secret);
            info!("🔑 JWT authentication configured");
        }
        Err(_) if is_development => {
            auth::init("dev-insecure-secret-change-me".to_string());
            warn!("⚠️ JWT_SECRET not set — using insecure default for development");
        }
        Err(_) => {
            warn!("⚠️ JWT_SECRET not set — auth endpoints will be unavailable");
        }
    }

    // Hash any remaining plaintext emails (one-time backfill, idempotent)
    backfill_email_hashes(&pool).await;

    let search_url =
        std::env::var("SEARCH_SERVICE_URL").unwrap_or_else(|_| default_search_service_url());
    info!("Search service URL: {}", search_url);

    let search_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client");

    let oauth_redirect_base = std::env::var("OAUTH_REDIRECT_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3001".to_string());

    let redis_store = match redis_store::RedisStore::from_env() {
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
    notify::spawn_listener(database_url.clone(), task_notifier.clone());

    let state = AppState {
        db: pool.clone(),
        search_client,
        search_url,
        oauth_redirect_base,
        task_notifier,
        redis_store,
    };

    // Start background task to refresh materialized views every hour
    tokio::spawn(refresh_stats_task(pool.clone()));

    // Start background task to refresh circle live ranks every 5 minutes
    tokio::spawn(refresh_circle_ranks_task(pool.clone()));

    // Start background task to refresh user fan rankings every hour
    tokio::spawn(refresh_user_rankings_task(pool.clone()));

    // Start background task to ensure daily stats entries exist
    tokio::spawn(ensure_daily_stats_task(pool.clone()));

    // Start background task to clear stale live_points/live_rank every hour
    tokio::spawn(clear_stale_live_task(pool.clone()));

    // Start background task to clean up expired cache entries every 10 minutes
    tokio::spawn(cache_cleanup_task());

    let internal_proof_host =
        std::env::var("BROWSER_PROOF_INTERNAL_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let internal_proof_port = std::env::var("BROWSER_PROOF_INTERNAL_PORT")
        .unwrap_or_else(|_| "3005".to_string())
        .parse::<u16>()
        .expect("BROWSER_PROOF_INTERNAL_PORT must be a valid number");
    spawn_internal_browser_proof_server(state.clone(), internal_proof_host, internal_proof_port)
        .await?;

    // Configure CORS - more permissive for development, strict for production
    let is_development = std::env::var("DEBUG_MODE").unwrap_or_default() == "true";

    let cors = if is_development {
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
        "X-API-Key".parse().unwrap(),
    ])
    .expose_headers([
        "X-Browser-Proof".parse().unwrap(),
        "X-Browser-Proof-TTL".parse().unwrap(),
    ]);

    // Build the application with proper routing and middleware
    // Public-read endpoints still require an API key or browser proof to prevent scraping.
    let public_routes = Router::new()
        .nest("/api/v4/circles", circles::router())
        .nest("/api/v4/rankings", rankings::router())
        .nest("/api/v4/user/profile", profile::router())
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::api_key::api_key_tracking_middleware,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::turnstile::api_protection_middleware,
                ))
                .layer(cors.clone()),
        )
        .with_state(state.clone());

    // Protected endpoints (Turnstile + restricted CORS)
    let protected_routes = Router::new()
        .route("/api/health", get(health_check))
        .route("/api/ver", get(version::get_version))
        .route("/api/ver/history", get(version::get_version_history))
        .nest("/api/docs", docs::router())
        .nest("/api/stats", stats::router())
        .nest("/api/tasks", tasks::router())
        .nest("/api/v3/tasks", tasks::router())
        .nest("/api/v4/partner", partner::router())
        .nest("/api/v3", search::router())
        .nest("/api/v4/affinity", affinity::router())
        .nest("/api/auth", auth_handlers::public_router())
        .nest("/api/auth", auth_handlers::authenticated_router())
        .nest("/", sharing::router())
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::api_key::api_key_tracking_middleware,
                ))
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    middleware::turnstile::api_protection_middleware,
                ))
                .layer(cors),
        )
        .with_state(state);

    // Merge public and protected routes
    let app = public_routes.merge(protected_routes);

    // Server configuration
    let host = std::env::var("HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse::<u16>()
        .expect("PORT must be a valid number");

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

async fn health_check() -> Result<Json<serde_json::Value>, StatusCode> {
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

async fn spawn_internal_browser_proof_server(
    state: AppState,
    host: String,
    port: u16,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route(
            "/api/auth/browser-proof/internal",
            axum::routing::post(middleware::turnstile::issue_internal_browser_proof),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
    info!(
        "🔒 Internal browser proof issuer listening on http://{}:{}",
        host, port
    );

    tokio::spawn(async move {
        if let Err(error) = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        {
            error!("Internal browser proof issuer stopped: {}", error);
        }
    });

    Ok(())
}

/// One-time startup backfill: hash any plaintext emails still in the database.
/// Detection is simple — plaintext emails contain '@', hashes never do.
/// Runs at every startup but is effectively a no-op once all rows are hashed.
async fn backfill_email_hashes(pool: &PgPool) {
    #[derive(sqlx::FromRow)]
    struct EmailRow {
        id: uuid::Uuid,
        email: String,
    }
    #[derive(sqlx::FromRow)]
    struct IdentityEmailRow {
        id: i32,
        email: String,
    }

    // Hash users.email
    let users: Vec<EmailRow> = match sqlx::query_as(
        "SELECT id, email FROM users WHERE email IS NOT NULL AND email LIKE '%@%'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            warn!("⚠️  backfill_email_hashes: could not query users: {}", e);
            return;
        }
    };

    for row in &users {
        let hashed = crate::auth::hash_email(&row.email);
        if let Err(e) = sqlx::query("UPDATE users SET email = $1 WHERE id = $2")
            .bind(&hashed)
            .bind(row.id)
            .execute(pool)
            .await
        {
            warn!(
                "⚠️  backfill_email_hashes: failed to update user {}: {}",
                row.id, e
            );
        }
    }

    // Hash user_identities.email
    let identities: Vec<IdentityEmailRow> = match sqlx::query_as(
        "SELECT id, email FROM user_identities WHERE email IS NOT NULL AND email LIKE '%@%'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            warn!(
                "⚠️  backfill_email_hashes: could not query user_identities: {}",
                e
            );
            return;
        }
    };

    for row in &identities {
        let hashed = crate::auth::hash_email(&row.email);
        if let Err(e) = sqlx::query("UPDATE user_identities SET email = $1 WHERE id = $2")
            .bind(&hashed)
            .bind(row.id)
            .execute(pool)
            .await
        {
            warn!(
                "⚠️  backfill_email_hashes: failed to update identity {}: {}",
                row.id, e
            );
        }
    }

    let total = users.len() + identities.len();
    if total > 0 {
        warn!(
            "✅ backfill_email_hashes: hashed {} plaintext email(s)",
            total
        );
    }
}

// Background task to refresh materialized views periodically
// Background task that nulls out live_points / live_rank for circles
// whose last_updated is older than 4 hours (stale live data).
async fn clear_stale_live_task(pool: PgPool) {
    info!("🔄 Starting stale live-data cleanup task (runs every hour)");
    tokio::time::sleep(tokio::time::Duration::from_secs(45)).await;
    loop {
        match sqlx::query(
            "UPDATE circles \
             SET live_points = NULL, live_rank = NULL \
             WHERE (live_points IS NOT NULL OR live_rank IS NOT NULL) \
               AND (last_live_update IS NULL OR last_live_update < \
                   (date_trunc('day', NOW() AT TIME ZONE 'Asia/Tokyo') AT TIME ZONE 'Asia/Tokyo')::timestamp)"
        )
        .execute(&pool)
        .await
        {
            Ok(res) => {
                if res.rows_affected() > 0 {
                    info!("🧹 Cleared stale live data from {} circles", res.rows_affected());
                }
            }
            Err(e) => error!("Failed to clear stale live data: {}", e),
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

async fn refresh_stats_task(pool: PgPool) {
    info!("🔄 Starting stats refresh background task (runs every hour)");

    // Wait before first run to let the server start
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    loop {
        refresh_mat_view(&pool, "stats_counts").await;
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

// Background task to refresh circle live ranks materialized view
async fn refresh_circle_ranks_task(pool: PgPool) {
    info!("🔄 Starting circle ranks refresh background task (runs every 5 minutes)");

    // Wait before first run to let the server start
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    loop {
        refresh_mat_view(&pool, "circle_live_ranks").await;
        tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
    }
}

// Background task to refresh user fan ranking materialized views
// Architecture: archive table for past months + current mat view for last 2 months
// - Hourly: archive completed months, refresh current + alltime (fast, seconds)
// - Daily: refresh gains (expensive, reads raw data)
async fn refresh_user_rankings_task(pool: PgPool) {
    use sqlx::Row;

    // Wait for startup
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

    let mut gains_tick: u32 = 23; // Start at 23 so gains refresh triggers on first iteration

    info!("🔄 Starting user fan rankings refresh task (current+alltime: hourly, gains: daily)");
    info!("🔄 Forcing initial recalculation of all ranking materialized views");

    loop {
        gains_tick += 1;

        // Step 1: Archive any completed months not yet in the archive table
        match sqlx::query(
            "SELECT DISTINCT cm.year, cm.month \
             FROM circle_member_fans_monthly cm \
             WHERE make_date(cm.year, cm.month, 1) < date_trunc('month', \
                   (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date \
                   - interval '1 month') \
             AND NOT EXISTS ( \
                 SELECT 1 FROM user_fan_rankings_monthly_archive a \
                 WHERE a.year = cm.year AND a.month = cm.month \
                 LIMIT 1 \
             )",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => {
                for row in &rows {
                    let year: i32 = row.get("year");
                    let month: i32 = row.get("month");
                    info!("📦 Archiving fan rankings for {}-{:02}", year, month);
                    match sqlx::query("SELECT archive_fan_rankings_month($1, $2)")
                        .bind(year)
                        .bind(month)
                        .fetch_optional(&pool)
                        .await
                    {
                        Ok(_) => info!("✅ Archived fan rankings for {}-{:02}", year, month),
                        Err(e) => warn!("⚠️ Failed to archive {}-{:02}: {}", year, month, e),
                    }
                }
            }
            Err(e) => warn!("⚠️ Failed to check archive status: {}", e),
        }

        // Step 1b: Archive circle ranks for completed months
        match sqlx::query(
            "SELECT DISTINCT cmf.year, cmf.month \
             FROM circle_member_fans_monthly cmf \
             WHERE make_date(cmf.year, cmf.month, 1) < date_trunc('month', \
                   (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date) \
             AND NOT EXISTS ( \
                 SELECT 1 FROM circle_ranks_monthly_archive a \
                 WHERE a.year = cmf.year AND a.month = cmf.month \
                 LIMIT 1 \
             )",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => {
                for row in &rows {
                    let year: i32 = row.get("year");
                    let month: i32 = row.get("month");
                    info!("📦 Archiving circle ranks for {}-{:02}", year, month);
                    match sqlx::query(
                        "INSERT INTO circle_ranks_monthly_archive \
                             (circle_id, year, month, rank, total_points, member_count, circle_name) \
                         SELECT circle_id, $1, $2, \
                                RANK() OVER (ORDER BY total_points DESC)::INT, \
                                total_points, member_count, circle_name \
                         FROM ( \
                             SELECT cmf.circle_id, \
                                    SUM(cmf.daily_fans[array_length(cmf.daily_fans, 1)]) AS total_points, \
                                    COUNT(DISTINCT cmf.viewer_id)::INT AS member_count, \
                                    MAX(c.name) AS circle_name \
                             FROM circle_member_fans_monthly cmf \
                             LEFT JOIN circles c ON c.circle_id = cmf.circle_id \
                             WHERE cmf.year = $1 AND cmf.month = $2 \
                             GROUP BY cmf.circle_id \
                         ) sub \
                         ON CONFLICT DO NOTHING"
                    )
                    .bind(year)
                    .bind(month)
                    .execute(&pool)
                    .await
                    {
                        Ok(_) => info!("✅ Archived circle ranks for {}-{:02}", year, month),
                        Err(e) => warn!("⚠️ Failed to archive circle ranks {}-{:02}: {}", year, month, e),
                    }
                }
            }
            Err(e) => warn!("⚠️ Failed to check circle rank archive status: {}", e),
        }

        // Step 2: Refresh current-month mat view (fast — only last 2 months of data)
        refresh_mat_view(&pool, "user_fan_rankings_monthly_current").await;

        // Step 3: Refresh alltime (reads from pre-aggregated monthly data — fast)
        refresh_mat_view(&pool, "user_fan_rankings_alltime").await;

        // Step 4: Refresh gains once per day (expensive — reads raw daily_fans arrays)
        if gains_tick >= 24 {
            gains_tick = 0;
            refresh_mat_view(&pool, "user_fan_rankings_gains").await;
        }

        // Wait 1 hour AFTER completion, not from start
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

/// Refresh a materialized view, falling back to non-concurrent if needed
async fn refresh_mat_view(pool: &PgPool, view_name: &str) {
    let start = std::time::Instant::now();
    let sql_concurrent = format!("REFRESH MATERIALIZED VIEW CONCURRENTLY {}", view_name);
    let sql_full = format!("REFRESH MATERIALIZED VIEW {}", view_name);

    match sqlx::query(&sql_concurrent).execute(pool).await {
        Ok(_) => info!(
            "✅ {} refreshed in {:.1}s",
            view_name,
            start.elapsed().as_secs_f64()
        ),
        Err(_) => match sqlx::query(&sql_full).execute(pool).await {
            Ok(_) => info!(
                "✅ {} refreshed (non-concurrent) in {:.1}s",
                view_name,
                start.elapsed().as_secs_f64()
            ),
            Err(e) => warn!("⚠️ Failed to refresh {}: {}", view_name, e),
        },
    }
}

// Background task to clean up expired cache entries
async fn cache_cleanup_task() {
    info!("🧹 Starting cache cleanup background task (runs every 10 minutes)");

    // Wait before first run
    tokio::time::sleep(tokio::time::Duration::from_secs(120)).await;

    loop {
        // Clean up expired entries
        cache::cleanup_expired();

        // Log cache stats
        let stats = cache::stats();
        info!(
            "📊 Cache stats: {} entries, {:.2} MB total, {} expired",
            stats.entry_count,
            stats.total_size_bytes as f64 / 1_048_576.0,
            stats.expired_count
        );

        tokio::time::sleep(tokio::time::Duration::from_secs(600)).await;
    }
}

// Background task to ensure daily_stats entries exist for each day
// This prevents gaps in the daily stats data
async fn ensure_daily_stats_task(pool: PgPool) {
    // Wait before first run
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    info!("📅 Starting daily stats ensure background task (runs every hour)");

    loop {
        // Ensure today's entry exists (insert with 0 if not present)
        let result = sqlx::query(
            r#"
            INSERT INTO daily_stats (date, total_visitors, unique_visitors, visitor_count, created_at, updated_at)
            VALUES (CURRENT_DATE, 0, 0, 0, NOW(), NOW())
            ON CONFLICT (date) DO NOTHING
            "#,
        )
        .execute(&pool)
        .await;

        match result {
            Ok(r) => {
                if r.rows_affected() > 0 {
                    info!("📅 Created daily_stats entry for today");
                }
            }
            Err(e) => warn!("⚠️ Failed to ensure daily_stats entry: {}", e),
        }

        // Also backfill any missing days in the last 7 days
        let backfill_result = sqlx::query(
            r#"
            INSERT INTO daily_stats (date, total_visitors, unique_visitors, visitor_count, created_at, updated_at)
            SELECT 
                d::date,
                0,
                0, 
                0,
                NOW(),
                NOW()
            FROM generate_series(
                CURRENT_DATE - INTERVAL '7 days', 
                CURRENT_DATE, 
                '1 day'::interval
            ) AS d
            WHERE NOT EXISTS (
                SELECT 1 FROM daily_stats WHERE date = d::date
            )
            ON CONFLICT (date) DO NOTHING
            "#,
        )
        .execute(&pool)
        .await;

        match backfill_result {
            Ok(r) => {
                if r.rows_affected() > 0 {
                    info!(
                        "📅 Backfilled {} missing daily_stats entries",
                        r.rows_affected()
                    );
                }
            }
            Err(e) => warn!("⚠️ Failed to backfill daily_stats: {}", e),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}
