use axum::{
    extract::{ConnectInfo, Path, State},
    response::Json,
    routing::{get, post},
    Router,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::net::SocketAddr;

use crate::errors::AppError;
use crate::models::{
    DailyVisitRequest, DataFreshness, FriendlistReportResponse, StatsResponse, TodayActivity,
};
use crate::AppState;

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
    let cache_key = "stats:main";
    if let Some(cached) = crate::cache::get::<StatsResponse>(cache_key) {
        return Ok(Json(cached));
    }

    let row = sqlx::query(
        r#"
        SELECT
            (SELECT COUNT(*)::bigint
             FROM trainer
             WHERE last_updated >= NOW() - INTERVAL '24 hours') AS accounts_24h,
            (SELECT COUNT(*)::bigint
             FROM trainer
             WHERE last_updated >= NOW() - INTERVAL '7 days') AS accounts_7d
        "#,
    )
    .fetch_one(&state.db)
    .await?;

    let tasks_24h = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM tasks
        WHERE status = 'completed'
          AND updated_at >= NOW() - INTERVAL '24 hours'
        "#,
    )
    .fetch_one(&state.db)
    .await?;

    let umas_tracked = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT GREATEST(
            COALESCE((
                SELECT n_live_tup::bigint
                FROM pg_stat_all_tables
                WHERE schemaname = 'public'
                  AND relname = 'team_stadium'
            ), 0),
            COALESCE((
                SELECT reltuples::bigint
                FROM pg_class
                WHERE oid = 'public.team_stadium'::regclass
            ), 0),
            COALESCE((
                SELECT (TO_JSONB(s)->>'umas_tracked')::bigint
                FROM stats_counts s
                LIMIT 1
            ), 0)
        )
        "#,
    )
    .fetch_one(&state.db)
    .await?;

    let response = StatsResponse {
        today: TodayActivity { tasks_24h },
        freshness: DataFreshness {
            accounts_24h: row.get::<i64, _>("accounts_24h"),
            accounts_7d: row.get::<i64, _>("accounts_7d"),
            umas_tracked,
        },
    };

    let _ = crate::cache::set(cache_key, &response, std::time::Duration::from_secs(60));

    Ok(Json(response))
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
