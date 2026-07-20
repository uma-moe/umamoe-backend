use axum::{
    extract::{ConnectInfo, Path, State},
    response::Json,
    routing::{get, post},
    Router,
};
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::net::SocketAddr;
use std::time::Duration;

use crate::errors::AppError;
use crate::models::{
    DailyVisitRequest, DataFreshness, FriendlistReportResponse, StatsResponse, TodayActivity,
};
use crate::AppState;

const STATS_CACHE_KEY: &str = "stats:main:v2";
const STATS_CACHE_TTL: Duration = Duration::from_secs(60 * 60);

pub fn public_router() -> Router<AppState> {
    Router::new()
        .route("/daily-visit", post(track_daily_visit))
        .route("/", get(get_stats))
}

pub fn protected_router() -> Router<AppState> {
    Router::new().route("/friendlist/:id", post(report_friendlist_full))
}

// New efficient daily visit tracking (only increments counter once per day per user)
pub async fn track_daily_visit(
    State(state): State<AppState>,
    Json(_payload): Json<DailyVisitRequest>,
) -> Result<Json<Value>, AppError> {
    // Use server's current date instead of trusting client-provided date
    // This prevents incorrect stats from clients with wrong system clocks
    let target_date = chrono::Utc::now().date_naive();

    // Call the database function to increment the daily counter
    let result = sqlx::query_scalar::<_, i32>("SELECT increment_daily_visitor_count($1)")
        .bind(target_date)
        .fetch_one(&state.db)
        .await;

    match result {
        Ok(count) => Ok(Json(json!({
            "success": true,
            "daily_count": count
        }))),
        Err(e) => {
            eprintln!("Database error in track_daily_visit: {}", e);
            // Gracefully handle database errors
            Ok(Json(json!({
                "success": true,
                "daily_count": 1
            })))
        }
    }
}

pub async fn get_stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, AppError> {
    if let Some(cached) = crate::cache::get::<StatsResponse>(STATS_CACHE_KEY) {
        return Ok(Json(cached));
    }

    let response = load_stats(&state.db).await?;
    cache_stats(&response);

    Ok(Json(response))
}

async fn load_stats(pool: &PgPool) -> Result<StatsResponse, AppError> {
    // Read the hourly aggregate instead of scanning trainer/tasks on every
    // cold-cache request. New blue/green containers otherwise cause a cache
    // stampede where many identical full-table counts pin the entire DB pool.
    let row = sqlx::query(
        r#"
        SELECT
            COALESCE(tasks_24h, 0)::bigint AS tasks_24h,
            COALESCE(accounts_24h, 0)::bigint AS accounts_24h,
            COALESCE(trainer_count, 0)::bigint AS trainer_ids_tracked,
            COALESCE(umas_tracked, 0)::bigint AS umas_tracked
        FROM stats_counts
        LIMIT 1
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(StatsResponse {
        today: TodayActivity {
            tasks_24h: row.get::<i64, _>("tasks_24h"),
        },
        freshness: DataFreshness::with_totals(
            row.get::<i64, _>("accounts_24h"),
            row.get::<i64, _>("trainer_ids_tracked"),
            row.get::<i64, _>("umas_tracked"),
        ),
    })
}

fn cache_stats(response: &StatsResponse) {
    if let Err(error) = crate::cache::set(STATS_CACHE_KEY, response, STATS_CACHE_TTL) {
        tracing::warn!("Failed to cache stats response: {}", error);
    }
}

/// Rebuild the process-local response cache after the hourly database refresh.
pub(crate) async fn refresh_cache(pool: &PgPool) -> Result<(), AppError> {
    let response = load_stats(pool).await?;
    cache_stats(&response);
    Ok(())
}

pub async fn report_friendlist_full(
    State(_state): State<AppState>,
    Path(_record_id): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Result<Json<FriendlistReportResponse>, AppError> {
    let _ip_address = addr.ip();

    // For now, just return success without database operations
    Ok(Json(FriendlistReportResponse {
        success: true,
        message: "Report submitted successfully".to_string(),
    }))
}
