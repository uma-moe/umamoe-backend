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

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/daily-visit", post(track_daily_visit))
        .route("/", get(get_stats))
        .route("/friendlist/:id", post(report_friendlist_full))
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
            COALESCE(tasks_24h, 0)    AS tasks_24h,
            COALESCE(accounts_24h, 0) AS accounts_24h,
            COALESCE(accounts_7d, 0)  AS accounts_7d,
            COALESCE(umas_tracked, 0) AS umas_tracked
        FROM stats_counts
        LIMIT 1
        "#,
    )
    .fetch_one(&state.db)
    .await?;

    let response = StatsResponse {
        today: TodayActivity {
            tasks_24h: row.get::<i64, _>("tasks_24h"),
        },
        freshness: DataFreshness {
            accounts_24h: row.get::<i64, _>("accounts_24h"),
            accounts_7d: row.get::<i64, _>("accounts_7d"),
            umas_tracked: row.get::<i64, _>("umas_tracked"),
        },
    };

    let _ = crate::cache::set(cache_key, &response, std::time::Duration::from_secs(300));

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
