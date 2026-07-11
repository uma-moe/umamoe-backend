use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use chrono::{Datelike, Duration, FixedOffset, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::{
    errors::AppError,
    models::{Circle, CircleMemberFansMonthly},
    AppState,
};

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

/// Create the circles router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(get_circle))
        .route("/list", get(list_circles))
        .route("/rank-thresholds", get(get_rank_thresholds))
}

fn tallying_sql() -> &'static str {
    "(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') >= (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '19 hours') AND (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') < (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day')"
}

fn post_tally_display_sql() -> &'static str {
    "(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') >= (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day') AND (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') < (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')"
}

fn row_has_new_last_month_sql(alias: &str) -> String {
    format!(
        "{}.last_updated >= ((date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day') AT TIME ZONE 'Asia/Tokyo')::timestamp AND NOT COALESCE({}.archived, false)",
        alias, alias
    )
}

fn archived_sql(alias: &str) -> String {
    format!("COALESCE({}.archived, false)", alias)
}

fn valid_live_sql(alias: &str) -> String {
    format!("{}.live_rank > 0 AND {}.live_points > 0", alias, alias)
}

fn has_live_points_sql(alias: &str) -> String {
    format!("{}.live_points > 0", alias)
}

fn positive_rank_sql(expr: &str) -> String {
    format!("CASE WHEN {expr} > 0 THEN {expr} ELSE NULL END")
}

fn disbanded_name_sql(alias: &str) -> String {
    format!(
        "CASE WHEN COALESCE({}.archived, false) AND {}.name NOT LIKE '% ( DISBANDED )' THEN {}.name || ' ( DISBANDED )' ELSE {}.name END",
        alias, alias, alias, alias
    )
}

fn effective_points_sql(alias: &str) -> String {
    format!(
        "CASE \
            WHEN {} AND {} THEN {}.monthly_point \
            WHEN {} AND {} THEN {}.live_points \
            WHEN {} THEN {}.monthly_point \
            WHEN {} AND {} THEN COALESCE({}.last_month_point, {}.monthly_point) \
            WHEN {} THEN {}.monthly_point \
            WHEN {} THEN COALESCE(GREATEST({}.live_points, {}.monthly_point), {}.live_points, {}.monthly_point) \
            ELSE {}.monthly_point \
        END",
        tallying_sql(),
        archived_sql(alias),
        alias,
        tallying_sql(),
        has_live_points_sql(alias),
        alias,
        tallying_sql(),
        alias,
        post_tally_display_sql(),
        row_has_new_last_month_sql(alias),
        alias,
        alias,
        post_tally_display_sql(),
        alias,
        has_live_points_sql(alias),
        alias,
        alias,
        alias,
        alias,
        alias,
    )
}

fn rank_fallback_sql(alias: &str) -> String {
    let monthly_rank = positive_rank_sql(&format!("{}.monthly_rank", alias));
    let live_rank = positive_rank_sql(&format!("{}.live_rank", alias));
    let last_month_rank = positive_rank_sql(&format!("{}.last_month_rank", alias));
    let display_last_month_rank = format!("COALESCE({last_month_rank}, {monthly_rank})");

    format!(
        "CASE \
            WHEN {} AND {} THEN {} \
            WHEN {} AND {} THEN {} \
            WHEN {} THEN {} \
            WHEN {} AND {} THEN {} \
            WHEN {} THEN {} \
            WHEN {} THEN {} \
            ELSE {} \
        END",
        tallying_sql(),
        archived_sql(alias),
        monthly_rank,
        tallying_sql(),
        valid_live_sql(alias),
        live_rank,
        tallying_sql(),
        monthly_rank,
        post_tally_display_sql(),
        row_has_new_last_month_sql(alias),
        display_last_month_rank,
        post_tally_display_sql(),
        monthly_rank,
        valid_live_sql(alias),
        live_rank,
        monthly_rank,
    )
}

fn display_monthly_point_sql(alias: &str) -> String {
    format!(
        "CASE WHEN {} AND {} THEN COALESCE({}.last_month_point, {}.monthly_point) ELSE {}.monthly_point END",
        post_tally_display_sql(),
        row_has_new_last_month_sql(alias),
        alias,
        alias,
        alias,
    )
}

fn display_yesterday_points_sql(alias: &str) -> String {
    format!(
        "CASE WHEN {} AND {} THEN COALESCE({}.last_month_point, {}.yesterday_points) ELSE {}.yesterday_points END",
        post_tally_display_sql(),
        row_has_new_last_month_sql(alias),
        alias,
        alias,
        alias,
    )
}

fn display_yesterday_rank_sql(alias: &str) -> String {
    let yesterday_rank = positive_rank_sql(&format!("{}.yesterday_rank", alias));
    let last_month_rank = positive_rank_sql(&format!("{}.last_month_rank", alias));

    format!(
        "CASE WHEN {} AND {} THEN COALESCE({}, {}) ELSE {} END",
        post_tally_display_sql(),
        row_has_new_last_month_sql(alias),
        last_month_rank,
        yesterday_rank,
        yesterday_rank,
    )
}

fn display_yesterday_rank_expr_sql(alias: &str, live_yesterday_rank_expr: &str) -> String {
    let live_yesterday_rank = positive_rank_sql(&format!("{}::int", live_yesterday_rank_expr));

    format!(
        "COALESCE({}, {})",
        live_yesterday_rank,
        display_yesterday_rank_sql(alias)
    )
}

fn display_live_points_sql(alias: &str) -> String {
    format!(
        "CASE WHEN {} OR ({} AND {}) OR {}.live_points <= 0 THEN NULL ELSE {}.live_points END",
        post_tally_display_sql(),
        tallying_sql(),
        archived_sql(alias),
        alias,
        alias,
    )
}

fn display_live_rank_expr_sql(alias: &str, live_rank_expr: &str) -> String {
    format!(
        "CASE WHEN {} OR ({} AND {}) THEN NULL ELSE {} END",
        post_tally_display_sql(),
        tallying_sql(),
        archived_sql(alias),
        live_rank_expr,
    )
}

fn display_last_live_update_sql(alias: &str) -> String {
    format!(
        "CASE WHEN {} OR ({} AND {}) THEN NULL ELSE {}.last_live_update END",
        post_tally_display_sql(),
        tallying_sql(),
        archived_sql(alias),
        alias,
    )
}

fn effective_circle_points(circle: &Circle) -> i64 {
    let jst_offset = FixedOffset::east_opt(9 * 3600).unwrap();
    let now_jst = Utc::now().with_timezone(&jst_offset);
    let month_start_jst = NaiveDate::from_ymd_opt(now_jst.year(), now_jst.month(), 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let tally_start_jst = month_start_jst + Duration::hours(19);
    let game_month_start_jst = month_start_jst + Duration::days(1);
    let display_end_jst = month_start_jst + Duration::days(2);
    let game_month_start_utc = game_month_start_jst - Duration::hours(9);
    let now_jst_naive = now_jst.naive_local();
    let has_live_points = circle.live_points.unwrap_or(0) > 0;
    let archived = circle.archived.unwrap_or(false);

    if now_jst_naive >= tally_start_jst && now_jst_naive < game_month_start_jst {
        if archived {
            circle.monthly_point.unwrap_or(0)
        } else if has_live_points {
            circle.live_points.unwrap_or(0)
        } else {
            circle.monthly_point.unwrap_or(0)
        }
    } else if now_jst_naive >= game_month_start_jst && now_jst_naive < display_end_jst {
        if !archived
            && circle
                .last_updated
                .is_some_and(|updated| updated >= game_month_start_utc)
        {
            circle
                .last_month_point
                .or(circle.monthly_point)
                .unwrap_or(0)
        } else {
            circle.monthly_point.unwrap_or(0)
        }
    } else if has_live_points {
        circle
            .live_points
            .unwrap_or(0)
            .max(circle.monthly_point.unwrap_or(0))
    } else {
        circle.monthly_point.unwrap_or(0)
    }
}

fn resolve_circle_month(
    year: Option<i32>,
    month: Option<i32>,
) -> Result<Option<(i32, i32, bool)>, AppError> {
    if year.is_none() && month.is_none() {
        return Ok(None);
    }

    let jst = FixedOffset::east_opt(9 * 3600).expect("valid JST offset");
    let current_month = Utc::now()
        .with_timezone(&jst)
        .date_naive()
        .with_day(1)
        .expect("first day exists");
    let target_year = year.unwrap_or(current_month.year());
    let target_month = month.unwrap_or(current_month.month() as i32);
    let target_date = NaiveDate::from_ymd_opt(target_year, target_month as u32, 1)
        .ok_or_else(|| AppError::BadRequest("invalid historical year/month".into()))?;
    if target_date > current_month {
        return Err(AppError::BadRequest(
            "circle month cannot be in the future".into(),
        ));
    }

    Ok(Some((
        target_year,
        target_month,
        target_date < current_month,
    )))
}

async fn apply_historical_circle_month(
    pool: &PgPool,
    circle: &mut Circle,
    year: i32,
    month: i32,
) -> Result<(), AppError> {
    let target_date = NaiveDate::from_ymd_opt(year, month as u32, 1)
        .ok_or_else(|| AppError::BadRequest("invalid historical year/month".into()))?;
    let previous_date = target_date - chrono::Months::new(1);
    let historical = sqlx::query_as::<_, HistoricalCircleMonth>(
        r#"
        SELECT
            selected.circle_name,
            selected.rank AS monthly_rank,
            selected.total_points AS monthly_point,
            selected.member_count,
            previous.rank AS last_month_rank,
            previous.total_points AS last_month_point
        FROM (SELECT 1) request
        LEFT JOIN circle_ranks_monthly_archive selected
          ON selected.circle_id = $1
         AND selected.year = $2
         AND selected.month = $3
        LEFT JOIN circle_ranks_monthly_archive previous
          ON previous.circle_id = $1
         AND previous.year = $4
         AND previous.month = $5
        "#,
    )
    .bind(circle.circle_id)
    .bind(year)
    .bind(month)
    .bind(previous_date.year())
    .bind(previous_date.month() as i32)
    .fetch_one(pool)
    .await?;

    if let Some(name) = historical.circle_name {
        circle.name = name;
    }
    circle.member_count = historical.member_count;
    circle.monthly_rank = historical.monthly_rank;
    circle.monthly_point = historical.monthly_point;
    circle.last_month_rank = historical.last_month_rank;
    circle.last_month_point = historical.last_month_point;
    circle.yesterday_updated = None;
    circle.yesterday_points = None;
    circle.yesterday_rank = None;
    circle.live_points = None;
    circle.live_rank = None;
    circle.last_live_update = None;

    Ok(())
}

/// GET /api/circles - Get circle information and member fan counts
///
/// Parameters:
/// - viewer_id: Get circle for a specific viewer (will add to tasks if not found)
/// - circle_id: Get circle by ID directly
///
/// Returns circle info with all member fan count data
pub async fn get_circle(
    Query(params): Query<CircleQueryParams>,
    State(state): State<AppState>,
) -> Result<Json<CircleResponse>, AppError> {
    // Validate that at least one parameter is provided
    if params.viewer_id.is_none() && params.circle_id.is_none() {
        return Err(AppError::BadRequest(
            "Either viewer_id or circle_id must be provided".to_string(),
        ));
    }

    let requested_month = resolve_circle_month(params.year, params.month)?;
    let historical_month = requested_month.filter(|(_, _, historical)| *historical);

    let mut circle = if let Some(viewer_id) = params.viewer_id {
        // Query by viewer_id - first check if viewer exists in circle_member_fans_monthly
        let member_record = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT circle_id 
            FROM circle_member_fans_monthly 
            WHERE viewer_id = $1 
            LIMIT 1
            "#,
        )
        .bind(viewer_id)
        .fetch_optional(&state.db)
        .await?;

        match member_record {
            Some(circle_id) => {
                // Viewer found, get their circle
                fetch_circle_by_id(&state.db, circle_id).await?
            }
            None => {
                // Viewer not found - add to tasks for later fetching
                add_viewer_to_tasks(&state.db, viewer_id).await?;

                return Err(AppError::NotFound(format!(
                    "Viewer {} not found in any circle. Added to task queue for fetching.",
                    viewer_id
                )));
            }
        }
    } else if let Some(circle_id) = params.circle_id {
        // Query by circle_id directly
        fetch_circle_by_id(&state.db, circle_id).await?
    } else {
        unreachable!("Already validated at least one param exists");
    };

    if let Some((year, month, _)) = historical_month {
        apply_historical_circle_month(&state.db, &mut circle, year, month).await?;
    }

    // Get all members and their fan counts for this circle
    let (member_year, member_month) = requested_month
        .map(|(year, month, _)| (Some(year), Some(month)))
        .unwrap_or((None, None));
    let members =
        fetch_circle_members(&state.db, circle.circle_id, member_year, member_month).await?;

    let points = effective_circle_points(&circle);
    let club_rank = Some(compute_club_rank(circle.monthly_rank, Some(points)));
    let rank = circle.monthly_rank;

    let fans_to_next_tier = if let Some(boundary) = next_tier_boundary(rank, points) {
        let boundary_points = if let Some((year, month, _)) = historical_month {
            fetch_historical_boundary_points(&state.db, year, month, boundary, true).await?
        } else {
            fetch_boundary_points(&state.db, boundary).await?
        };
        match boundary_points {
            Some(bp) => Some((bp - points).max(0)),
            None => Some(0),
        }
    } else {
        Some(0) // Already at SS
    };

    let fans_to_lower_tier = if let Some(boundary) = lower_tier_boundary(rank, points) {
        let boundary_points = if let Some((year, month, _)) = historical_month {
            fetch_historical_boundary_points(&state.db, year, month, boundary, false).await?
        } else {
            fetch_boundary_points(&state.db, boundary).await?
        };
        match boundary_points {
            Some(bp) => Some((points - bp).max(0)),
            None => Some(0),
        }
    } else {
        Some(0) // Already at D
    };

    let (yesterday_fans_to_next_tier, yesterday_fans_to_lower_tier) = if historical_month.is_some()
    {
        // We archive monthly circle ranks, not daily rank snapshots. Never
        // leak current-month "yesterday" gaps into a historical response.
        (None, None)
    } else {
        let y_points = circle.yesterday_points.unwrap_or(0);
        let y_rank = circle.yesterday_rank;
        // Compare yesterday's points against today's tier, so crossing a tier line
        // does not make the displayed threshold appear to jump by an entire bracket.
        let y_tier_gap_rank = historical_tier_gap_rank(rank, y_rank);

        let next = if let Some(boundary) = next_tier_boundary(y_tier_gap_rank, y_points) {
            match fetch_boundary_points_yesterday(&state.db, boundary).await? {
                Some(bp) => Some((bp - y_points).max(0)),
                None => Some(0),
            }
        } else {
            Some(0)
        };
        let lower = if let Some(boundary) = lower_tier_boundary(y_tier_gap_rank, y_points) {
            match fetch_boundary_points_yesterday(&state.db, boundary).await? {
                Some(bp) => Some((y_points - bp).max(0)),
                None => Some(0),
            }
        } else {
            Some(0)
        };
        (next, lower)
    };

    Ok(Json(CircleResponse {
        circle,
        members,
        club_rank,
        fans_to_next_tier,
        fans_to_lower_tier,
        yesterday_fans_to_next_tier,
        yesterday_fans_to_lower_tier,
    }))
}

/// GET /api/circles/list - List all circles with pagination and filtering
///
/// Parameters:
/// - page: Page number (0-indexed, default: 0)
/// - limit: Results per page (default: 100, max: 100)
/// - name: Filter by circle name (partial match, case-insensitive)
/// - min_members: Minimum member count
/// - max_rank: Maximum monthly rank (lower is better, e.g., rank 1 is best)
/// - sort_by: Field to sort by (name, member_count, monthly_rank, monthly_point)
/// - sort_dir: Sort direction (asc, desc)
/// - year/month: Optional completed month; returns the same response shape from the archive
///
/// Returns paginated list of circles
pub async fn list_circles(
    Query(params): Query<CircleListParams>,
    State(state): State<AppState>,
) -> Result<Json<CircleListResponse>, AppError> {
    match (params.year, params.month) {
        (Some(year), Some(month)) => {
            let jst = FixedOffset::east_opt(9 * 3600).expect("valid JST offset");
            let current_month = Utc::now()
                .with_timezone(&jst)
                .date_naive()
                .with_day(1)
                .expect("first day exists");
            let target_month = NaiveDate::from_ymd_opt(year, month as u32, 1)
                .ok_or_else(|| AppError::BadRequest("invalid historical year/month".into()))?;
            if target_month > current_month {
                return Err(AppError::BadRequest(
                    "historical ranking month cannot be in the future".into(),
                ));
            }
            if target_month < current_month {
                return list_historical_circles(&state.db, &params, year, month).await;
            }
        }
        (None, None) => {}
        _ => {
            return Err(AppError::BadRequest(
                "year and month must be supplied together".into(),
            ));
        }
    }

    let page = params.page.unwrap_or(0).max(0);
    let limit = params.limit.unwrap_or(100).clamp(1, 100);
    let offset = page * limit;

    let mut with_parts = Vec::new();

    // If search query is present, add MatchingCircles CTE to optimize search
    let mut join_matching_circles = String::new();

    if let Some(query) = &params.query {
        // Skip very short queries that would match too many results
        let query_trimmed = query.trim();
        if query_trimmed.len() >= 2 {
            let search_pattern = format!("%{}%", query_trimmed.replace("'", "''"));
            let search_exact = query_trimmed.replace("'", "''");
            let is_number = query_trimmed.parse::<i64>().is_ok();

            let mut union_parts = Vec::new();

            // 1. Search by Circle Name
            union_parts.push(format!(
                "SELECT circle_id FROM circles WHERE name ILIKE '{}'",
                search_pattern
            ));

            // 2. Search by Leader Name
            union_parts.push(format!(
            "SELECT c.circle_id FROM circles c JOIN trainer t ON c.leader_viewer_id::text = t.account_id WHERE t.name ILIKE '{}'", 
            search_pattern
        ));

            // 3. Search by Member Name
            union_parts.push(format!(
            r#"
            SELECT cm.circle_id 
            FROM circle_member_fans_monthly cm 
            JOIN trainer tm ON cm.viewer_id::text = tm.account_id 
            WHERE cm.year = extract(year from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int 
              AND cm.month = extract(month from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int 
              AND tm.name ILIKE '{}'
            "#,
            search_pattern
        ));

            if is_number {
                // 4. Search by Circle ID
                union_parts.push(format!(
                    "SELECT circle_id FROM circles WHERE circle_id = {}",
                    search_exact
                ));

                // 5. Search by Leader ID
                union_parts.push(format!(
                    "SELECT circle_id FROM circles WHERE leader_viewer_id = {}",
                    search_exact
                ));

                // 6. Search by Member ID
                union_parts.push(format!(
                r#"
                SELECT circle_id 
                FROM circle_member_fans_monthly 
                WHERE viewer_id = {} 
                  AND year = extract(year from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int 
                  AND month = extract(month from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int
                "#,
                search_exact
            ));
            }

            with_parts.push(format!(
                "MatchingCircles AS ({})",
                union_parts.join(" UNION ")
            ));

            join_matching_circles =
                "INNER JOIN MatchingCircles mc ON c.circle_id = mc.circle_id".to_string();
        } // end of query_trimmed.len() >= 2 check
    }

    let with_clause = if with_parts.is_empty() {
        String::new()
    } else {
        format!("WITH {}", with_parts.join(", "))
    };

    let points_column = effective_points_sql("c");
    let rank_fallback_column = rank_fallback_sql("c");
    let rank_column = format!(
        "COALESCE({}, {})",
        positive_rank_sql("lr.live_rank::int"),
        rank_fallback_column
    );
    let name_column = disbanded_name_sql("c");
    let monthly_point_column = display_monthly_point_sql("c");
    let yesterday_points_column = display_yesterday_points_sql("c");
    let yesterday_rank_column = display_yesterday_rank_expr_sql("c", "lr.live_yesterday_rank");
    let live_points_column = display_live_points_sql("c");
    let live_rank_expr = format!(
        "COALESCE({}, {})",
        positive_rank_sql("lr.live_rank::int"),
        positive_rank_sql("c.live_rank")
    );
    let live_rank_column = display_live_rank_expr_sql("c", &live_rank_expr);
    let last_live_update_column = display_last_live_update_sql("c");
    // Build dynamic query
    let mut count_query = format!(
        "{} SELECT COUNT(*) FROM circles c LEFT JOIN trainer t ON c.leader_viewer_id::text = t.account_id LEFT JOIN circle_live_ranks lr ON lr.circle_id = c.circle_id {} WHERE 1=1",
        with_clause,
        join_matching_circles
    );

    let mut select_query = format!(
        r#"
        {}
        SELECT 
            c.circle_id,
            {} as name,
            c.comment,
            c.leader_viewer_id,
            t.name as leader_name,
            c.member_count,
            c.join_style,
            c.policy,
            c.created_at,
            c.last_updated,
            {} as monthly_rank,
            {} as monthly_point,
            c.last_month_rank,
            c.last_month_point,
            c.archived,
            c.yesterday_updated,
            {} as yesterday_points,
            {} as yesterday_rank,
            {} as live_points,
            {} as live_rank,
            {} as last_live_update
        FROM circles c
        LEFT JOIN trainer t ON c.leader_viewer_id::text = t.account_id
        LEFT JOIN circle_live_ranks lr ON lr.circle_id = c.circle_id
        {}
        WHERE 1=1
        "#,
        with_clause,
        name_column,
        rank_column,
        monthly_point_column,
        yesterday_points_column,
        yesterday_rank_column,
        live_points_column,
        live_rank_column,
        last_live_update_column,
        join_matching_circles
    );

    let mut conditions = Vec::new();

    // Only show circles updated this month to ensure points are current
    // Use JST minus 2 days so the month flips at midnight JST on the 3rd (giving time for data collection)
    conditions.push("c.last_updated >= date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')".to_string());
    conditions.push("(c.archived IS DISTINCT FROM true OR (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp < date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')".to_string());

    // Name filter
    if let Some(name) = &params.name {
        conditions.push(format!("c.name ILIKE '%{}%'", name.replace("'", "''")));
    }

    // General Search Query - handled by CTE now, no extra conditions needed here
    // But we keep the parameter check to avoid unused variable warning if we removed it completely
    // (Actually we used it above to build CTE)

    // Min members filter
    if let Some(min_members) = params.min_members {
        conditions.push(format!("c.member_count >= {}", min_members));
    }

    // Max rank filter (lower rank number is better)
    if let Some(max_rank) = params.max_rank {
        conditions.push(format!("{} <= {}", rank_column, max_rank));
    }

    // Add conditions to queries
    for condition in &conditions {
        count_query.push_str(&format!(" AND {}", condition));
        select_query.push_str(&format!(" AND {}", condition));
    }

    // Get total count
    let total: i64 = sqlx::query_scalar(&count_query)
        .fetch_one(&state.db)
        .await?;

    // Add sorting
    let sort_by = params.sort_by.as_deref().unwrap_or("rank");
    let sort_dir = match params.sort_dir.as_deref() {
        Some(value) if value.eq_ignore_ascii_case("desc") => "DESC",
        _ => "ASC",
    };

    let order_clause = match sort_by {
        "name" => format!(" ORDER BY c.name {}, c.circle_id ASC", sort_dir),
        "member_count" => format!(
            " ORDER BY c.member_count {} NULLS LAST, c.circle_id ASC",
            sort_dir
        ),
        "rank" | "monthly_rank" => {
            format!(" ORDER BY {} ASC NULLS LAST, c.circle_id ASC", rank_column)
        }
        "monthly_point" => format!(
            " ORDER BY {} {} NULLS LAST, c.circle_id ASC",
            points_column, sort_dir
        ),
        _ => format!(" ORDER BY {} ASC NULLS LAST, c.circle_id ASC", rank_column),
    };

    select_query.push_str(&order_clause);
    select_query.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

    // Execute query
    let circles = sqlx::query_as::<_, Circle>(&select_query)
        .fetch_all(&state.db)
        .await?;

    let circles_with_rank: Vec<CircleWithRank> = circles
        .into_iter()
        .map(|circle| {
            let effective_points = effective_circle_points(&circle);
            let club_rank = Some(compute_club_rank(
                circle.monthly_rank,
                Some(effective_points),
            ));
            CircleWithRank { circle, club_rank }
        })
        .collect();

    let total_pages = if limit > 0 {
        ((total as f64) / (limit as f64)).ceil() as i64
    } else {
        0
    };

    Ok(Json(CircleListResponse {
        circles: circles_with_rank,
        total,
        page,
        limit,
        total_pages,
    }))
}

async fn list_historical_circles(
    pool: &PgPool,
    params: &CircleListParams,
    year: i32,
    month: i32,
) -> Result<Json<CircleListResponse>, AppError> {
    let page = params.page.unwrap_or(0).max(0);
    let limit = params.limit.unwrap_or(100).clamp(1, 100);
    let offset = page * limit;
    let name_pattern = params
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("%{value}%"));
    let query = params.query.as_deref().map(str::trim).unwrap_or("");
    let query_id = query.parse::<i64>().ok();
    let query_pattern = if query.len() >= 2 && query_id.is_none() {
        Some(format!("%{query}%"))
    } else {
        None
    };
    let sort_column = match params.sort_by.as_deref() {
        Some("name") => "COALESCE(a.circle_name, c.name)",
        Some("member_count" | "members") => "a.member_count",
        Some("monthly_point" | "fans" | "daily") => "a.total_points",
        _ => "a.rank",
    };
    let sort_direction = if params.sort_dir.as_deref() == Some("desc") {
        "DESC"
    } else {
        "ASC"
    };
    let where_sql = r#"
        a.year = $1 AND a.month = $2
        AND ($3::text IS NULL OR COALESCE(a.circle_name, c.name) ILIKE $3)
        AND ($4::bigint IS NULL OR a.circle_id = $4)
        AND ($5::text IS NULL OR COALESCE(a.circle_name, c.name) ILIKE $5)
        AND ($6::int IS NULL OR a.member_count >= $6)
        AND ($7::int IS NULL OR a.rank <= $7)
    "#;

    let count_sql = format!(
        "SELECT COUNT(*) FROM circle_ranks_monthly_archive a \
         LEFT JOIN circles c ON c.circle_id = a.circle_id WHERE {where_sql}"
    );
    let total = sqlx::query_scalar::<_, i64>(&count_sql)
        .bind(year)
        .bind(month)
        .bind(name_pattern.as_deref())
        .bind(query_id)
        .bind(query_pattern.as_deref())
        .bind(params.min_members)
        .bind(params.max_rank)
        .fetch_one(pool)
        .await?;

    let select_sql = format!(
        r#"
        SELECT
            a.circle_id,
            COALESCE(a.circle_name, c.name, a.circle_id::text) AS name,
            c.comment,
            c.leader_viewer_id,
            t.name AS leader_name,
            a.member_count,
            c.join_style,
            c.policy,
            c.created_at,
            c.last_updated,
            a.rank AS monthly_rank,
            a.total_points AS monthly_point,
            NULL::int AS last_month_rank,
            NULL::bigint AS last_month_point,
            c.archived,
            NULL::timestamp AS yesterday_updated,
            NULL::bigint AS yesterday_points,
            NULL::int AS yesterday_rank,
            NULL::bigint AS live_points,
            NULL::int AS live_rank,
            NULL::timestamp AS last_live_update
        FROM circle_ranks_monthly_archive a
        LEFT JOIN circles c ON c.circle_id = a.circle_id
        LEFT JOIN trainer t ON c.leader_viewer_id::text = t.account_id
        WHERE {where_sql}
        ORDER BY {sort_column} {sort_direction} NULLS LAST, a.circle_id ASC
        LIMIT $8 OFFSET $9
        "#
    );
    let circles = sqlx::query_as::<_, Circle>(&select_sql)
        .bind(year)
        .bind(month)
        .bind(name_pattern.as_deref())
        .bind(query_id)
        .bind(query_pattern.as_deref())
        .bind(params.min_members)
        .bind(params.max_rank)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|circle| CircleWithRank {
            club_rank: Some(compute_club_rank(circle.monthly_rank, circle.monthly_point)),
            circle,
        })
        .collect();
    let total_pages = (total + limit - 1) / limit;

    Ok(Json(CircleListResponse {
        circles,
        total,
        page,
        limit,
        total_pages,
    }))
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

fn month_progress_jst() -> MonthProgress {
    let jst_offset = FixedOffset::east_opt(9 * 3600).unwrap();
    let now_jst = Utc::now().with_timezone(&jst_offset);
    let calendar_day = now_jst.day() as i64;
    let first_day_this_month = NaiveDate::from_ymd_opt(now_jst.year(), now_jst.month(), 1).unwrap();
    let previous_month_last_day = first_day_this_month - Duration::days(1);
    let previous_month_days = previous_month_last_day.day() as i64;
    let elapsed_days = if calendar_day <= 2 {
        previous_month_days
    } else {
        calendar_day - 1
    };

    MonthProgress {
        elapsed_days,
        yesterday_elapsed_days: if calendar_day <= 2 {
            previous_month_days
        } else {
            (elapsed_days - 1).max(1)
        },
        previous_month_days,
    }
}

fn fans_per_day(total_fans: Option<i64>, days: i64) -> Option<i64> {
    let total_fans = total_fans?;
    if days <= 0 || total_fans <= 0 {
        Some(0)
    } else {
        Some(total_fans.saturating_add(days - 1) / days)
    }
}

fn option_delta(current: Option<i64>, previous: Option<i64>) -> Option<i64> {
    Some(current? - previous?)
}

/// GET /api/v4/circles/rank-thresholds - Get the fan requirements for each circle rank tier
pub async fn get_rank_thresholds(
    State(state): State<AppState>,
) -> Result<Json<RankThresholdsResponse>, AppError> {
    let tiers: Vec<(&str, i32, Option<i32>, Option<i32>)> = vec![
        ("SS", 11, Some(1), Some(10)),
        ("S+", 10, Some(11), Some(30)),
        ("S", 9, Some(31), Some(100)),
        ("A+", 8, Some(101), Some(500)),
        ("A", 7, Some(501), Some(1000)),
        ("B+", 6, Some(1001), Some(3000)),
        ("B", 5, Some(3001), Some(5000)),
        ("C+", 4, Some(5001), Some(7000)),
        ("C", 3, Some(7001), Some(10000)),
        ("D+", 2, Some(10001), None),
        ("D", 1, None, None),
    ];

    let month_progress = month_progress_jst();
    let mut thresholds = Vec::new();

    for (name, rank_index, ranking_from, ranking_to) in tiers {
        let (
            current_min_fans,
            yesterday_min_fans,
            last_month_min_fans,
            daily_fans_delta,
            current_vs_last_month_delta,
        ) = if let Some(boundary) = ranking_to {
            let current = fetch_boundary_points(&state.db, boundary).await?;
            let yesterday = fetch_boundary_points_yesterday(&state.db, boundary).await?;
            let last_month = fetch_boundary_points_last_month(&state.db, boundary).await?;
            (
                current,
                yesterday,
                last_month,
                option_delta(current, yesterday),
                option_delta(current, last_month),
            )
        } else {
            (None, None, None, None, None)
        };

        thresholds.push(RankThreshold {
            rank_index,
            name: name.to_string(),
            ranking_from,
            ranking_to,
            current_min_fans,
            current_fans_per_day: fans_per_day(current_min_fans, month_progress.elapsed_days),
            yesterday_min_fans,
            yesterday_fans_per_day: fans_per_day(
                yesterday_min_fans,
                month_progress.yesterday_elapsed_days,
            ),
            daily_fans_delta,
            last_month_min_fans,
            last_month_fans_per_day: fans_per_day(
                last_month_min_fans,
                month_progress.previous_month_days,
            ),
            current_vs_last_month_delta,
        });
    }

    Ok(Json(RankThresholdsResponse { thresholds }))
}

/// Convert a ranking position and monthly points to club rank index (1-11)
/// 1=D, 2=D+, 3=C, 4=C+, 5=B, 6=B+, 7=A, 8=A+, 9=S, 10=S+, 11=SS
fn compute_club_rank(rank: Option<i32>, monthly_point: Option<i64>) -> i32 {
    match rank {
        None | Some(..=0) => match monthly_point {
            None | Some(0) => 1,
            _ => 2,
        },
        Some(r) => match r {
            1..=10 => 11,
            11..=30 => 10,
            31..=100 => 9,
            101..=500 => 8,
            501..=1000 => 7,
            1001..=3000 => 6,
            3001..=5000 => 5,
            5001..=7000 => 4,
            7001..=10000 => 3,
            _ => 2,
        },
    }
}

/// Get the boundary rank for the next tier up (None if already SS)
fn next_tier_boundary(rank: Option<i32>, points: i64) -> Option<i32> {
    // D tier (0 points / unranked) -> next is D+ (rank 10000)
    if points == 0 || rank.is_none() {
        return Some(10000);
    }
    match rank.unwrap() {
        1..=10 => None,
        11..=30 => Some(10),
        31..=100 => Some(30),
        101..=500 => Some(100),
        501..=1000 => Some(500),
        1001..=3000 => Some(1000),
        3001..=5000 => Some(3000),
        5001..=7000 => Some(5000),
        7001..=10000 => Some(7000),
        _ => Some(10000),
    }
}

/// Get the boundary rank for the lower tier (None if already at D)
/// Returns the first rank of the tier below (i.e. the highest-ranked circle in that tier)
fn lower_tier_boundary(rank: Option<i32>, points: i64) -> Option<i32> {
    if points == 0 || rank.is_none() {
        return None; // Already at D
    }
    match rank.unwrap() {
        1..=10 => Some(11),
        11..=30 => Some(31),
        31..=100 => Some(101),
        101..=500 => Some(501),
        501..=1000 => Some(1001),
        1001..=3000 => Some(3001),
        3001..=5000 => Some(5001),
        5001..=7000 => Some(7001),
        7001..=10000 => Some(10001),
        // D+ (10001+) -> lower tier is D (0 points), no boundary to query
        _ => None,
    }
}

fn historical_tier_gap_rank(
    current_rank: Option<i32>,
    historical_rank: Option<i32>,
) -> Option<i32> {
    current_rank
        .filter(|rank| *rank > 0)
        .or_else(|| historical_rank.filter(|rank| *rank > 0))
}

/// Fetch the effective current points of the circle at the given boundary rank
async fn fetch_boundary_points(pool: &PgPool, boundary_rank: i32) -> Result<Option<i64>, AppError> {
    let points_column = effective_points_sql("c");

    let result: Option<Option<i64>> = sqlx::query_scalar(&format!(
        r#"
        SELECT {}
        FROM circles c
        JOIN circle_live_ranks lr ON c.circle_id = lr.circle_id
        WHERE lr.live_rank <= $1
          AND ({}) IS NOT NULL
        ORDER BY lr.live_rank DESC
        LIMIT 1
        "#,
        points_column, points_column
    ))
    .bind(boundary_rank)
    .fetch_optional(pool)
    .await?;

    Ok(result.flatten())
}

/// Fetch the monthly points at a historical tier boundary. For an upper-tier
/// boundary we use the last rank still inside that tier; for a lower-tier
/// boundary we use the first rank in the tier below.
async fn fetch_historical_boundary_points(
    pool: &PgPool,
    year: i32,
    month: i32,
    boundary_rank: i32,
    upper_tier: bool,
) -> Result<Option<i64>, AppError> {
    let (rank_filter, rank_order, points_order) = if upper_tier {
        ("rank <= $3", "rank DESC", "total_points ASC")
    } else {
        ("rank >= $3", "rank ASC", "total_points DESC")
    };
    let sql = format!(
        r#"
        SELECT total_points
        FROM circle_ranks_monthly_archive
        WHERE year = $1
          AND month = $2
          AND {rank_filter}
          AND total_points IS NOT NULL
        ORDER BY {rank_order}, {points_order}
        LIMIT 1
        "#
    );
    let result = sqlx::query_scalar::<_, Option<i64>>(&sql)
        .bind(year)
        .bind(month)
        .bind(boundary_rank)
        .fetch_optional(pool)
        .await?;

    Ok(result.flatten())
}

/// Fetch the yesterday_points of the circle at the given boundary rank (using yesterday's rankings)
async fn fetch_boundary_points_yesterday(
    pool: &PgPool,
    boundary_rank: i32,
) -> Result<Option<i64>, AppError> {
    let points_column = display_yesterday_points_sql("c");

    let result: Option<Option<i64>> = sqlx::query_scalar(&format!(
        r#"
        SELECT {}
        FROM circles c
        JOIN circle_live_ranks lr ON c.circle_id = lr.circle_id
        WHERE lr.live_yesterday_rank <= $1
          AND ({}) IS NOT NULL
        ORDER BY lr.live_yesterday_rank DESC
        LIMIT 1
        "#,
        points_column, points_column
    ))
    .bind(boundary_rank)
    .fetch_optional(pool)
    .await?;

    Ok(result.flatten())
}

/// Fetch the last_month_point of the circle at the given boundary rank.
async fn fetch_boundary_points_last_month(
    pool: &PgPool,
    boundary_rank: i32,
) -> Result<Option<i64>, AppError> {
    let result: Option<Option<i64>> = sqlx::query_scalar(
        r#"
        SELECT c.last_month_point
        FROM circles c
        WHERE c.last_month_rank <= $1
          AND c.last_month_rank > 0
          AND c.last_month_point IS NOT NULL
        ORDER BY c.last_month_rank DESC
        LIMIT 1
        "#,
    )
    .bind(boundary_rank)
    .fetch_optional(pool)
    .await?;

    Ok(result.flatten())
}

/// Fetch circle by ID
async fn fetch_circle_by_id(pool: &PgPool, circle_id: i64) -> Result<Circle, AppError> {
    let rank_fallback_column = rank_fallback_sql("c");
    let rank_column = format!(
        "COALESCE({}, {})",
        positive_rank_sql("lr.live_rank::int"),
        rank_fallback_column
    );
    let name_column = disbanded_name_sql("c");
    let monthly_point_column = display_monthly_point_sql("c");
    let yesterday_points_column = display_yesterday_points_sql("c");
    let yesterday_rank_column = display_yesterday_rank_expr_sql("c", "lr.live_yesterday_rank");
    let live_points_column = display_live_points_sql("c");
    let live_rank_expr = format!(
        "COALESCE({}, {})",
        positive_rank_sql("lr.live_rank::int"),
        positive_rank_sql("c.live_rank")
    );
    let live_rank_column = display_live_rank_expr_sql("c", &live_rank_expr);
    let last_live_update_column = display_last_live_update_sql("c");

    let circle = sqlx::query_as::<_, Circle>(&format!(
        r#"
        SELECT 
            c.circle_id,
            {} as name,
            c.comment,
            c.leader_viewer_id,
            t.name as leader_name,
            c.member_count,
            c.join_style,
            c.policy,
            c.created_at,
            c.last_updated,
            {} as monthly_rank,
            {} as monthly_point,
            c.last_month_rank,
            c.last_month_point,
            c.archived,
            c.yesterday_updated,
            {} as yesterday_points,
            {} as yesterday_rank,
            {} as live_points,
            {} as live_rank,
            {} as last_live_update
        FROM circles c
        LEFT JOIN trainer t ON c.leader_viewer_id::text = t.account_id
        LEFT JOIN circle_live_ranks lr ON lr.circle_id = c.circle_id
        WHERE c.circle_id = $1
        "#,
        name_column,
        rank_column,
        monthly_point_column,
        yesterday_points_column,
        yesterday_rank_column,
        live_points_column,
        live_rank_column,
        last_live_update_column,
    ))
    .bind(circle_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Circle {} not found", circle_id)))?;

    Ok(circle)
}

/// Fetch all members and their fan counts for a circle
async fn fetch_circle_members(
    pool: &PgPool,
    circle_id: i64,
    year: Option<i32>,
    month: Option<i32>,
) -> Result<Vec<CircleMemberFansMonthly>, AppError> {
    use chrono::{Datelike, Duration, FixedOffset, Utc};
    use std::collections::HashMap;

    // Default circle detail members to the new game month starting on the 2nd JST.
    let (target_year, target_month) = if year.is_none() || month.is_none() {
        let jst_offset = FixedOffset::east_opt(9 * 3600).unwrap();
        let now_jst = Utc::now().with_timezone(&jst_offset) - Duration::days(1);
        (
            year.unwrap_or(now_jst.year()),
            month.unwrap_or(now_jst.month() as i32),
        )
    } else {
        (year.unwrap(), month.unwrap())
    };

    #[derive(sqlx::FromRow)]
    struct MemberRecord {
        id: i32,
        circle_id: i64,
        viewer_id: i64,
        trainer_name: Option<String>,
        shame_score: Option<i32>,
        year: i32,
        month: i32,
        daily_fans: Vec<i64>,
        last_updated: Option<chrono::NaiveDateTime>,
        next_month_start: Option<i64>,
    }

    let records = sqlx::query_as::<_, MemberRecord>(
        r#"
        SELECT 
            cm.id,
            cm.circle_id,
            cm.viewer_id,
            t.name as trainer_name,
            s.suspicion_score AS shame_score,
            cm.year,
            cm.month,
            cm.daily_fans,
            cm.last_updated,
            (
                SELECT cm2.daily_fans[1]
                FROM circle_member_fans_monthly cm2
                WHERE cm2.viewer_id = cm.viewer_id
                  AND cm2.year  = CASE WHEN cm.month = 12 THEN cm.year + 1 ELSE cm.year END
                  AND cm2.month = CASE WHEN cm.month = 12 THEN 1 ELSE cm.month + 1 END
                  AND cm2.daily_fans[1] > 0
                LIMIT 1
            ) as next_month_start
        FROM circle_member_fans_monthly cm
        LEFT JOIN trainer t ON cm.viewer_id::text = t.account_id
        LEFT JOIN viewer_suspicion_scores s ON s.viewer_id = cm.viewer_id
        WHERE cm.circle_id = $1 AND cm.year = $2 AND cm.month = $3
        ORDER BY cm.viewer_id
        "#,
    )
    .bind(circle_id)
    .bind(target_year)
    .bind(target_month)
    .fetch_all(pool)
    .await?;

    let mut members: Vec<CircleMemberFansMonthly> = records
        .into_iter()
        .map(|rec| {
            let mut daily_fans = rec.daily_fans;
            daily_fans.resize(32, 0);
            CircleMemberFansMonthly {
                id: rec.id,
                circle_id: rec.circle_id,
                viewer_id: rec.viewer_id,
                trainer_name: rec.trainer_name,
                shame_score: rec.shame_score,
                year: rec.year,
                month: rec.month,
                daily_fans,
                last_updated: rec.last_updated,
                previous_circle_id: None,
                previous_circle_name: None,
                next_month_start: rec.next_month_start,
            }
        })
        .collect();

    // Find members who have leading zeros (joined this circle mid-month)
    let viewer_ids_with_leading_zeros: Vec<i64> = members
        .iter()
        .filter(|m| {
            // Has at least one non-zero value, and the first element is zero
            // (meaning they joined after day 1)
            !m.daily_fans.is_empty() && m.daily_fans[0] == 0 && m.daily_fans.iter().any(|&v| v > 0)
        })
        .map(|m| m.viewer_id)
        .collect();

    if !viewer_ids_with_leading_zeros.is_empty() {
        // Query for records in OTHER circles for these viewers in the same month
        #[derive(sqlx::FromRow)]
        struct PreviousCircleRecord {
            viewer_id: i64,
            circle_id: i64,
            circle_name: String,
            daily_fans: Vec<i64>,
        }

        let previous_records = sqlx::query_as::<_, PreviousCircleRecord>(
            r#"
            SELECT 
                cm.viewer_id,
                cm.circle_id,
                c.name as circle_name,
                cm.daily_fans
            FROM circle_member_fans_monthly cm
            JOIN circles c ON cm.circle_id = c.circle_id
            WHERE cm.viewer_id = ANY($1)
              AND cm.year = $2
              AND cm.month = $3
              AND cm.circle_id != $4
                        "#,
        )
        .bind(&viewer_ids_with_leading_zeros)
        .bind(target_year)
        .bind(target_month)
        .bind(circle_id)
        .fetch_all(pool)
        .await?;

        // Build a map of viewer_id -> Vec<(circle_id, circle_name, daily_fans)>
        // A member could theoretically have been in multiple circles in one month
        let mut prev_map: HashMap<i64, Vec<(i64, String, Vec<i64>)>> = HashMap::new();
        for rec in previous_records {
            prev_map.entry(rec.viewer_id).or_default().push((
                rec.circle_id,
                rec.circle_name,
                rec.daily_fans,
            ));
        }

        // Merge previous circle data into current members
        for member in &mut members {
            if let Some(prev_entries) = prev_map.get(&member.viewer_id) {
                // Find the first non-zero day in current circle
                let first_active_day = member.daily_fans.iter().position(|&v| v > 0);

                if let Some(first_day) = first_active_day {
                    // Track which previous circle contributed the most days
                    let mut best_circle_id: Option<i64> = None;
                    let mut best_circle_name: Option<String> = None;
                    let mut best_days_filled = 0;

                    for (prev_circle_id, prev_circle_name, prev_fans) in prev_entries {
                        let mut days_filled = 0;

                        // Only fill zeros before the first active day in current circle
                        for i in 0..first_day {
                            if let Some(&prev_val) = prev_fans.get(i) {
                                if prev_val > 0 && member.daily_fans[i] == 0 {
                                    member.daily_fans[i] = -prev_val;
                                    days_filled += 1;
                                }
                            }
                        }

                        if days_filled > best_days_filled {
                            best_days_filled = days_filled;
                            best_circle_id = Some(*prev_circle_id);
                            best_circle_name = Some(prev_circle_name.clone());
                        }
                    }

                    member.previous_circle_id = best_circle_id;
                    member.previous_circle_name = best_circle_name;
                }
            }
        }
    }

    Ok(members)
}

/// Add a viewer to the tasks queue for later fetching
async fn add_viewer_to_tasks(pool: &PgPool, viewer_id: i64) -> Result<(), AppError> {
    // Insert into tasks table with viewer_id in task_data
    // account_id is for the worker that processes the task, so we leave it NULL
    sqlx::query(
        r#"
        INSERT INTO tasks (task_type, task_data, status, created_at, updated_at)
        VALUES ('fetch_circle', $1, 'pending', NOW(), NOW())
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(serde_json::json!({ "viewer_id": viewer_id }))
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yesterday_tier_gaps_are_anchored_to_current_rank() {
        let current_rank = Some(501);
        let yesterday_rank = Some(484);
        let yesterday_points = 365_509_264;

        let rank = historical_tier_gap_rank(current_rank, yesterday_rank);

        assert_eq!(rank, current_rank);
        assert_eq!(next_tier_boundary(rank, yesterday_points), Some(500));
        assert_eq!(lower_tier_boundary(rank, yesterday_points), Some(1001));
    }

    #[test]
    fn yesterday_tier_gaps_fall_back_without_current_rank() {
        let rank = historical_tier_gap_rank(None, Some(484));

        assert_eq!(rank, Some(484));
        assert_eq!(next_tier_boundary(rank, 365_509_264), Some(100));
        assert_eq!(lower_tier_boundary(rank, 365_509_264), Some(501));
    }

    #[test]
    fn zero_ranks_are_treated_as_unranked() {
        assert_eq!(compute_club_rank(Some(0), Some(0)), 1);
        assert_eq!(compute_club_rank(Some(0), Some(1)), 2);
        assert_eq!(historical_tier_gap_rank(Some(0), Some(484)), Some(484));
    }

    #[test]
    fn historical_rankings_use_the_same_tiers_as_live_circles() {
        let cases = [
            (Some(1), Some(1), 11),
            (Some(100), Some(1), 9),
            (Some(5_001), Some(1), 4),
            (Some(10_001), Some(1), 2),
            (None, Some(0), 1),
        ];

        for (rank, points, expected_rank) in cases {
            let actual_rank = compute_club_rank(rank, points);
            assert_eq!(actual_rank, expected_rank);
        }
    }

    #[test]
    fn ranking_migration_uses_monthly_summaries_instead_of_raw_arrays() {
        let migration =
            include_str!("../../migrations/20260711000000_optimize_fan_and_circle_rankings.sql");
        let alltime_definition = migration
            .split("CREATE MATERIALIZED VIEW user_fan_rankings_alltime AS")
            .nth(1)
            .expect("all-time ranking view definition")
            .split("CREATE UNIQUE INDEX idx_ufr_alltime_pk")
            .next()
            .expect("all-time ranking view body");

        assert!(alltime_definition.contains("FROM user_fan_rankings_monthly r"));
        assert!(!alltime_definition.contains("unnest"));
        assert!(migration.contains("SUM(r.monthly_gain)::bigint AS total_points"));
        assert!(!migration.contains("daily_fans[array_length"));
    }

    #[test]
    fn displayed_live_points_do_not_require_raw_live_rank() {
        let sql = display_live_points_sql("c");

        assert!(sql.contains("c.live_points <= 0"));
        assert!(!sql.contains("c.live_rank"));
    }

    #[test]
    fn effective_points_use_live_points_without_raw_live_rank() {
        let sql = effective_points_sql("c");

        assert!(sql.contains("c.live_points > 0"));
        assert!(!sql.contains("c.live_rank > 0 AND c.live_points > 0"));
    }
}
