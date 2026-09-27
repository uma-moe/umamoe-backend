// Included by the owning module to preserve private field visibility.

const CIRCLE_TEXT_SEARCH_DB_CONCURRENCY: usize = 2;

static CIRCLE_TEXT_SEARCH_DB_SLOTS: Semaphore =
    Semaphore::const_new(CIRCLE_TEXT_SEARCH_DB_CONCURRENCY);

#[derive(Debug, Deserialize)]
pub struct CircleQueryParams {
    /// Query by viewer ID - will find their circle
    pub viewer_id: Option<i64>,
    /// Query by circle ID directly
    pub circle_id: Option<i64>,
    /// Filter members by month (1-12)
    pub month: Option<i32>,
    /// Filter members by year
    pub year: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct CircleListParams {
    /// Page number (0-indexed)
    #[serde(default)]
    pub page: Option<i64>,
    /// Results per page
    #[serde(default)]
    pub limit: Option<i64>,
    /// Search by circle name (partial match)
    pub name: Option<String>,
    /// Minimum member count
    pub min_members: Option<i32>,
    /// Minimum monthly rank (lower is better)
    pub max_rank: Option<i32>,
    /// Sort by field (name, member_count, monthly_rank, monthly_point)
    pub sort_by: Option<String>,
    /// Sort direction (asc, desc)
    pub sort_dir: Option<String>,
    /// General search query (circle ID/name, leader ID/name, member ID/name)
    pub query: Option<String>,
    /// Historical ranking year; must be supplied together with month
    pub year: Option<i32>,
    /// Historical ranking month; must be supplied together with year
    pub month: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct CircleResponse {
    pub circle: Circle,
    pub members: Vec<CircleMemberFansMonthly>,
    pub club_rank: Option<i32>,
    pub fans_to_next_tier: Option<i64>,
    pub fans_to_lower_tier: Option<i64>,
    pub yesterday_fans_to_next_tier: Option<i64>,
    pub yesterday_fans_to_lower_tier: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct CircleWithRank {
    #[serde(flatten)]
    pub circle: Circle,
    pub club_rank: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct CircleListResponse {
    pub circles: Vec<CircleWithRank>,
    pub total: i64,
    pub page: i64,
    pub limit: i64,
    pub total_pages: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct HistoricalCircleMonth {
    circle_name: Option<String>,
    monthly_rank: Option<i32>,
    monthly_point: Option<i64>,
    member_count: Option<i32>,
    last_month_rank: Option<i32>,
    last_month_point: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct RankThreshold {
    pub rank_index: i32,
    pub name: String,
    pub ranking_from: Option<i32>,
    pub ranking_to: Option<i32>,
    pub current_min_fans: Option<i64>,
    pub current_fans_per_day: Option<i64>,
    pub yesterday_min_fans: Option<i64>,
    pub yesterday_fans_per_day: Option<i64>,
    pub daily_fans_delta: Option<i64>,
    pub last_month_min_fans: Option<i64>,
    pub last_month_fans_per_day: Option<i64>,
    pub current_vs_last_month_delta: Option<i64>,
}

struct MonthProgress {
    elapsed_days: i64,
    yesterday_elapsed_days: i64,
    previous_month_days: i64,
}

#[derive(Debug, Serialize)]
pub struct RankThresholdsResponse {
    pub thresholds: Vec<RankThreshold>,
}
