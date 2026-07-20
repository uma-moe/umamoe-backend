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

impl DataFreshness {
    pub fn with_totals(accounts_24h: i64, trainer_ids_tracked: i64, umas_tracked: i64) -> Self {
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
    fn legacy_seven_day_field_uses_the_trainer_total() {
        let freshness = DataFreshness::with_totals(12, 345, 678);

        assert_eq!(freshness.accounts_24h, 12);
        assert_eq!(freshness.trainer_ids_tracked, 345);
        assert_eq!(freshness.accounts_7d, freshness.trainer_ids_tracked);
        assert_eq!(freshness.umas_tracked, 678);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FriendlistReportResponse {
    pub success: bool,
    pub message: String,
}
