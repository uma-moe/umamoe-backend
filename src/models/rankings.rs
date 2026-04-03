use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// A user's monthly fan ranking with gain and averages
#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
pub struct UserFanRankingMonthly {
    pub viewer_id: i64,
    pub trainer_name: Option<String>,
    pub year: i32,
    pub month: i32,
    /// Cumulative fan count at end of month
    pub total_fans: i64,
    /// Fans gained this month (last - first cumulative)
    pub monthly_gain: i64,
    pub active_days: i32,
    /// Average daily fan gain (monthly_gain / active_days)
    pub avg_daily: Option<f64>,
    /// Average daily gain over last 3 days of month
    pub avg_3d: Option<f64>,
    /// Average daily gain over last 7 days of month
    pub avg_7d: Option<f64>,
    /// Average daily gain over the full month (monthly_gain / days_in_month)
    pub avg_monthly: Option<f64>,
    pub rank: i32,
    /// Circle the user was in at end of month
    pub circle_id: Option<i64>,
    pub circle_name: Option<String>,
    /// Cumulative fans at start of next month (daily_fans[1] of M+1)
    pub next_month_start: Option<i64>,
}

/// A user's all-time fan ranking with gain and averages
#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
pub struct UserFanRankingAlltime {
    pub viewer_id: i64,
    pub trainer_name: Option<String>,
    /// Latest (current) cumulative fan count
    pub total_fans: i64,
    /// All-time gain (latest - earliest cumulative)
    pub total_gain: i64,
    pub active_days: i32,
    /// Average daily fan gain (total_gain / active_days)
    pub avg_day: Option<f64>,
    /// Total fan gain over last 7 days
    pub avg_week: Option<f64>,
    /// Average monthly gain (total_gain / months_active)
    pub avg_month: Option<f64>,
    /// Legacy rank (same as rank_total_gain)
    pub rank: i32,
    pub rank_total_fans: i32,
    pub rank_total_gain: i32,
    pub rank_avg_day: i32,
    pub rank_avg_week: i32,
    pub rank_avg_month: i32,
    /// User's current circle (most recent month)
    pub circle_id: Option<i64>,
    pub circle_name: Option<String>,
}

/// A user's rolling gain totals with ranks
#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
pub struct UserFanRankingGains {
    pub viewer_id: i64,
    pub trainer_name: Option<String>,
    pub gain_3d: i64,
    pub gain_7d: i64,
    pub gain_30d: i64,
    pub rank_3d: i32,
    pub rank_7d: i32,
    pub rank_30d: i32,
    /// User's current circle (most recent data point)
    pub circle_id: Option<i64>,
    pub circle_name: Option<String>,
}

/// Response for monthly rankings endpoint
#[derive(Debug, Serialize)]
pub struct MonthlyRankingsResponse {
    pub rankings: Vec<UserFanRankingMonthly>,
    pub total: i64,
    pub page: i64,
    pub limit: i64,
    pub total_pages: i64,
    pub year: i32,
    pub month: i32,
}

/// Response for all-time rankings endpoint
#[derive(Debug, Serialize)]
pub struct AlltimeRankingsResponse {
    pub rankings: Vec<UserFanRankingAlltime>,
    pub total: i64,
    pub page: i64,
    pub limit: i64,
    pub total_pages: i64,
}

/// Response for gains rankings endpoint
#[derive(Debug, Serialize)]
pub struct GainsRankingsResponse {
    pub rankings: Vec<UserFanRankingGains>,
    pub total: i64,
    pub page: i64,
    pub limit: i64,
    pub total_pages: i64,
    pub sort_by: String,
}
