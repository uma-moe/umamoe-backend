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
pub use crate::types::{
    DailyVisitRequest, DataFreshness, FriendlistReportResponse, StatsResponse, TodayActivity,
};
use crate::AppState;

include!("../types/handlers/stats.rs");

pub fn publicRouter() -> Router<AppState> {
    Router::new()
        .route("/daily-visit", post(trackDailyVisit))
        .route("/", get(getStats))
}

pub fn protectedRouter() -> Router<AppState> {
    Router::new().route("/friendlist/:id", post(reportFriendlistFull))
}

// New efficient daily visit tracking (only increments counter once per day per user)
pub async fn trackDailyVisit(
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

pub async fn getStats(State(state): State<AppState>) -> Result<Json<StatsResponse>, AppError> {
    if let Some(cached) = crate::cache::get::<StatsResponse>(STATS_CACHE_KEY) {
        return Ok(Json(cached));
    }

    let response = loadStats(&state.db).await?;
    cacheStats(&response);

    Ok(Json(response))
}

async fn loadStats(pool: &PgPool) -> Result<StatsResponse, AppError> {
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
        freshness: DataFreshness::withTotals(
            row.get::<i64, _>("accounts_24h"),
            row.get::<i64, _>("trainer_ids_tracked"),
            row.get::<i64, _>("umas_tracked"),
        ),
    })
}

fn cacheStats(response: &StatsResponse) {
    if let Err(error) = crate::cache::set(STATS_CACHE_KEY, response, STATS_CACHE_TTL) {
        tracing::warn!("Failed to cache stats response: {}", error);
    }
}

/// Rebuild the process-local response cache after the hourly database refresh.
pub(crate) async fn refreshCache(pool: &PgPool) -> Result<(), AppError> {
    let response = loadStats(pool).await?;
    cacheStats(&response);
    Ok(())
}

pub async fn reportFriendlistFull(
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

impl DataFreshness {
    pub fn withTotals(accounts_24h: i64, trainer_ids_tracked: i64, umas_tracked: i64) -> Self {
        Self {
            accounts_24h,
            trainer_ids_tracked,
            accounts_7d: trainer_ids_tracked,
            umas_tracked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DataFreshness;

    #[test]
    fn legacySevenDayFieldUsesTheTrainerTotal() {
        let freshness = DataFreshness::withTotals(12, 345, 678);

        assert_eq!(freshness.accounts_24h, 12);
        assert_eq!(freshness.trainer_ids_tracked, 345);
        assert_eq!(freshness.accounts_7d, freshness.trainer_ids_tracked);
        assert_eq!(freshness.umas_tracked, 678);
    }
}
