use serde::{Deserialize, Serialize};

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
    /// Total trainer IDs stored
    pub trainer_ids_tracked: i64,
    /// Deprecated compatibility alias for `trainer_ids_tracked`
    pub accounts_7d: i64,
    /// Total TT umas stored
    pub umas_tracked: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FriendlistReportResponse {
    pub success: bool,
    pub message: String,
}
