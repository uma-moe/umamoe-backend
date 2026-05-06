use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;

use crate::{
    errors::AppError,
    models::{
        AlltimeRankingsResponse, GainsRankingsResponse, MonthlyRankingsResponse,
        UserFanRankingAlltime, UserFanRankingGains, UserFanRankingMonthly,
    },
    AppState,
};

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

/// Create the rankings router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/monthly", get(get_monthly_rankings))
        .route("/alltime", get(get_alltime_rankings))
        .route("/gains", get(get_gains_rankings))
}

/// GET /api/v4/rankings/monthly - Fan rankings for a specific month
///
/// All data comes from the `user_fan_rankings_monthly` materialized view
/// which contains every month. Refreshed hourly.
pub async fn get_monthly_rankings(
    Query(params): Query<MonthlyRankingsParams>,
    State(state): State<AppState>,
) -> Result<Json<MonthlyRankingsResponse>, AppError> {
    use chrono::{Datelike, Duration, FixedOffset, NaiveDate, Utc};

    let page = params.page.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(100);
    let offset = page * limit;

    let jst = FixedOffset::east_opt(9 * 3600).unwrap();
    let now_jst = Utc::now().with_timezone(&jst) - Duration::days(1);
    let target_year = params.year.unwrap_or(now_jst.year());
    let target_month = params.month.unwrap_or(now_jst.month() as i32);
    let sort_by = params.sort_by.as_deref().unwrap_or("monthly_gain");

    // Route to the correct underlying table to avoid slow UNION ALL view.
    // The current matview covers the last 2 months (current + previous in JST).
    // Everything older is in the archive table.
    let current_month_start = NaiveDate::from_ymd_opt(now_jst.year(), now_jst.month(), 1).unwrap();
    let prev_month_start = current_month_start - chrono::Months::new(1);
    let target_date =
        NaiveDate::from_ymd_opt(target_year, target_month as u32, 1).unwrap_or(current_month_start);

    let table_name = if target_date >= prev_month_start {
        "user_fan_rankings_monthly_current"
    } else {
        "user_fan_rankings_monthly_archive"
    };

    let order_by = match sort_by {
        "total_fans" => "total_fans DESC",
        "active_days" => "active_days DESC",
        "avg_daily" => "avg_daily DESC NULLS LAST",
        "avg_3d" => "avg_3d DESC NULLS LAST",
        "avg_7d" => "avg_7d DESC NULLS LAST",
        "avg_monthly" => "avg_monthly DESC NULLS LAST",
        _ => "rank ASC",
    };

    // Build search condition
    let (extra_condition, search_id, search_pattern) = parse_search_query(&params.query, "$3");

    let where_clause = format!(
        "year = $1 AND month = $2{}",
        if extra_condition.is_empty() {
            String::new()
        } else {
            format!(" AND {}", extra_condition)
        }
    );

    // Count — cached per search query (not per page) since it's the slow part
    let count_cache_key = format!(
        "rank:monthly:count:{}:{}:{}",
        target_year,
        target_month,
        params.query.as_deref().unwrap_or("_none_")
    );
    let total: i64 = if let Some(cached) = crate::cache::get::<i64>(&count_cache_key) {
        cached
    } else {
        let count_sql = format!("SELECT COUNT(*) FROM {} WHERE {}", table_name, where_clause);
        let count = bind_search(
            sqlx::query_scalar(&count_sql)
                .bind(target_year)
                .bind(target_month),
            &search_id,
            &search_pattern,
        )
        .fetch_one(&state.db)
        .await?;
        let _ = crate::cache::set(
            &count_cache_key,
            &count,
            std::time::Duration::from_secs(300),
        );
        count
    };

    // Data
    let (lp, op) = if search_id.is_some() || search_pattern.is_some() {
        ("$4", "$5")
    } else {
        ("$3", "$4")
    };
    let data_sql = format!(
        "SELECT viewer_id, trainer_name, year, month, total_fans, monthly_gain, active_days, \
         avg_daily, avg_3d, avg_7d, avg_monthly, rank, circle_id, circle_name, next_month_start \
         FROM {} WHERE {} ORDER BY {} LIMIT {} OFFSET {}",
        table_name, where_clause, order_by, lp, op
    );
    let rankings: Vec<UserFanRankingMonthly> = bind_search_and_page(
        sqlx::query_as(&data_sql)
            .bind(target_year)
            .bind(target_month),
        &search_id,
        &search_pattern,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    let total_pages = if limit > 0 {
        ((total as f64) / (limit as f64)).ceil() as i64
    } else {
        0
    };

    let response = MonthlyRankingsResponse {
        rankings,
        total,
        page,
        limit,
        total_pages,
        year: target_year,
        month: target_month,
    };

    Ok(Json(response))
}

/// GET /api/v4/rankings/alltime - All-time fan rankings across all months
///
/// Reads from the `user_fan_rankings_alltime` materialized view. Refreshed hourly.
pub async fn get_alltime_rankings(
    Query(params): Query<AlltimeRankingsParams>,
    State(state): State<AppState>,
) -> Result<Json<AlltimeRankingsResponse>, AppError> {
    let page = params.page.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(100);
    let offset = page * limit;
    let sort_by_str = params.sort_by.as_deref().unwrap_or("total_gain");

    let order_by = match sort_by_str {
        "total_fans" => "rank_total_fans ASC",
        "avg_day" => "rank_avg_day ASC",
        "avg_week" => "rank_avg_week ASC",
        "avg_month" => "rank_avg_month ASC",
        _ => "rank_total_gain ASC",
    };

    let (extra_condition, search_id, search_pattern) = parse_search_query(&params.query, "$1");

    let where_clause = if extra_condition.is_empty() {
        "TRUE".to_string()
    } else {
        extra_condition
    };

    // Count — cached per search query (not per page)
    let count_cache_key = format!(
        "rank:alltime:count:{}",
        params.query.as_deref().unwrap_or("_none_")
    );
    let total: i64 = if let Some(cached) = crate::cache::get::<i64>(&count_cache_key) {
        cached
    } else {
        let count_sql = format!(
            "SELECT COUNT(*) FROM user_fan_rankings_alltime WHERE {}",
            where_clause
        );
        let count = bind_search(sqlx::query_scalar(&count_sql), &search_id, &search_pattern)
            .fetch_one(&state.db)
            .await?;
        let _ = crate::cache::set(
            &count_cache_key,
            &count,
            std::time::Duration::from_secs(300),
        );
        count
    };

    // Data
    let (lp, op) = if search_id.is_some() || search_pattern.is_some() {
        ("$2", "$3")
    } else {
        ("$1", "$2")
    };
    let data_sql = format!(
        "SELECT viewer_id, trainer_name, total_fans, total_gain, active_days, \
         avg_day, avg_week, avg_month, rank, \
         rank_total_fans, rank_total_gain, rank_avg_day, rank_avg_week, rank_avg_month, \
         circle_id, circle_name \
         FROM user_fan_rankings_alltime WHERE {} ORDER BY {} LIMIT {} OFFSET {}",
        where_clause, order_by, lp, op
    );
    let rankings: Vec<UserFanRankingAlltime> = bind_search_and_page(
        sqlx::query_as(&data_sql),
        &search_id,
        &search_pattern,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    let total_pages = if limit > 0 {
        ((total as f64) / (limit as f64)).ceil() as i64
    } else {
        0
    };

    let response = AlltimeRankingsResponse {
        rankings,
        total,
        page,
        limit,
        total_pages,
    };

    Ok(Json(response))
}

/// GET /api/v4/rankings/gains - Rolling 3d/7d/30d fan gain rankings
///
/// Reads from the `user_fan_rankings_gains` materialized view. Refreshed hourly.
pub async fn get_gains_rankings(
    Query(params): Query<GainsRankingsParams>,
    State(state): State<AppState>,
) -> Result<Json<GainsRankingsResponse>, AppError> {
    let page = params.page.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(100);
    let offset = page * limit;

    let sort_by = params.sort_by.as_deref().unwrap_or("gain_30d");
    let rank_column = match sort_by {
        "gain_3d" => "rank_3d",
        "gain_7d" => "rank_7d",
        _ => "rank_30d",
    };

    let (extra_condition, search_id, search_pattern) = parse_search_query(&params.query, "$1");

    let where_clause = if extra_condition.is_empty() {
        "TRUE".to_string()
    } else {
        extra_condition
    };

    // Count — cached per search query (not per page)
    let count_cache_key = format!(
        "rank:gains:count:{}",
        params.query.as_deref().unwrap_or("_none_")
    );
    let total: i64 = if let Some(cached) = crate::cache::get::<i64>(&count_cache_key) {
        cached
    } else {
        let count_sql = format!(
            "SELECT COUNT(*) FROM user_fan_rankings_gains WHERE {}",
            where_clause
        );
        let count = bind_search(sqlx::query_scalar(&count_sql), &search_id, &search_pattern)
            .fetch_one(&state.db)
            .await?;
        let _ = crate::cache::set(
            &count_cache_key,
            &count,
            std::time::Duration::from_secs(300),
        );
        count
    };

    // Data
    let (lp, op) = if search_id.is_some() || search_pattern.is_some() {
        ("$2", "$3")
    } else {
        ("$1", "$2")
    };
    let data_sql = format!(
        "SELECT viewer_id, trainer_name, gain_3d, gain_7d, gain_30d, rank_3d, rank_7d, rank_30d, circle_id, circle_name \
         FROM user_fan_rankings_gains WHERE {} ORDER BY {} ASC LIMIT {} OFFSET {}",
        where_clause, rank_column, lp, op
    );
    let rankings: Vec<UserFanRankingGains> = bind_search_and_page(
        sqlx::query_as(&data_sql),
        &search_id,
        &search_pattern,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    let total_pages = if limit > 0 {
        ((total as f64) / (limit as f64)).ceil() as i64
    } else {
        0
    };

    let response = GainsRankingsResponse {
        rankings,
        total,
        page,
        limit,
        total_pages,
        sort_by: sort_by.to_string(),
    };

    Ok(Json(response))
}

// ── helpers ──────────────────────────────────────────────────────────

/// Parse an optional search query into a SQL condition fragment + typed value.
/// Matches viewer_id (exact), trainer_name (ILIKE), or circle_name (ILIKE).
/// Reuses the same bind param for both ILIKE checks via OR.
fn parse_search_query(
    query: &Option<String>,
    param: &str,
) -> (String, Option<i64>, Option<String>) {
    if let Some(q) = query {
        let q = q.trim();
        if !q.is_empty() {
            if let Ok(id) = q.parse::<i64>() {
                return (format!("viewer_id = {}", param), Some(id), None);
            } else if q.len() >= 2 {
                let pat = format!("%{}%", q.replace('\'', "''"));
                return (
                    format!(
                        "(trainer_name ILIKE {} OR circle_name ILIKE {})",
                        param, param
                    ),
                    None,
                    Some(pat),
                );
            }
        }
    }
    (String::new(), None, None)
}

/// Bind the search parameter (if any) to a query that returns a scalar.
fn bind_search<'q>(
    query: sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
    search_id: &Option<i64>,
    search_pattern: &Option<String>,
) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments> {
    if let Some(id) = search_id {
        query.bind(*id)
    } else if let Some(pat) = search_pattern {
        query.bind(pat.clone())
    } else {
        query
    }
}

/// Bind search param + LIMIT/OFFSET to a query_as.
fn bind_search_and_page<'q, T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin>(
    query: sqlx::query::QueryAs<'q, sqlx::Postgres, T, sqlx::postgres::PgArguments>,
    search_id: &Option<i64>,
    search_pattern: &Option<String>,
    limit: i64,
    offset: i64,
) -> sqlx::query::QueryAs<'q, sqlx::Postgres, T, sqlx::postgres::PgArguments> {
    let q = if let Some(id) = search_id {
        query.bind(*id)
    } else if let Some(pat) = search_pattern {
        query.bind(pat.clone())
    } else {
        query
    };
    q.bind(limit).bind(offset)
}
