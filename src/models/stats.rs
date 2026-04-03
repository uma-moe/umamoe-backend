use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Deserialize)]
pub struct DailyVisitRequest {
    /// Client-provided date (ignored - server uses its own date for reliability)
    #[serde(default)]
    #[allow(dead_code)]
    pub date: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatsResponse {
    pub today: TodayActivity,
    pub freshness: DataFreshness,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TodayActivity {
    pub tasks_24h: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataFreshness {
    /// Accounts updated in the last 24 hours
    pub accounts_24h: i64,
    /// Accounts updated in the last 7 days
    pub accounts_7d: i64,
    /// Total TT umas stored
    pub umas_tracked: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FriendlistReportResponse {
    pub success: bool,
    pub message: String,
}
