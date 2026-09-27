// Included by the owning module to preserve private field visibility.

#[derive(Debug, Deserialize)]
pub struct MonthlyRankingsParams {
    /// Month (1-12), defaults to current month in JST
    pub month: Option<i32>,
    /// Year, defaults to current year in JST
    pub year: Option<i32>,
    /// Page number (0-indexed)
    #[serde(default)]
    pub page: Option<i64>,
    /// Results per page (default 100, max 100)
    #[serde(default)]
    pub limit: Option<i64>,
    /// Search by viewer ID (exact), trainer name, or circle name (partial, case-insensitive)
    pub query: Option<String>,
    /// Sort by: monthly_gain (default), total_fans, active_days, avg_daily, avg_3d, avg_7d, avg_monthly
    pub sort_by: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AlltimeRankingsParams {
    /// Page number (0-indexed)
    #[serde(default)]
    pub page: Option<i64>,
    /// Results per page (default 100, max 100)
    #[serde(default)]
    pub limit: Option<i64>,
    /// Search by viewer ID (exact), trainer name, or circle name (partial, case-insensitive)
    pub query: Option<String>,
    /// Sort by: total_gain (default), total_fans, avg_day, avg_week, avg_month
    pub sort_by: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GainsRankingsParams {
    /// Page number (0-indexed)
    #[serde(default)]
    pub page: Option<i64>,
    /// Results per page (default 100, max 100)
    #[serde(default)]
    pub limit: Option<i64>,
    /// Sort/rank by: gain_3d, gain_7d, gain_30d (default: gain_30d)
    pub sort_by: Option<String>,
    /// Search by viewer ID (exact), trainer name, or circle name (partial, case-insensitive)
    pub query: Option<String>,
}
