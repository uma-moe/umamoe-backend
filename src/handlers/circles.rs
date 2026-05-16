use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
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

/// Create the circles router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(get_circle))
        .route("/list", get(list_circles))
        .route("/rank-thresholds", get(get_rank_thresholds))
}

fn rollover_active_sql() -> &'static str {
    "(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') < (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')"
}

fn today_start_utc_sql() -> &'static str {
    "(date_trunc('day', NOW() AT TIME ZONE 'Asia/Tokyo') AT TIME ZONE 'Asia/Tokyo')::timestamp"
}

fn valid_live_sql(alias: &str) -> String {
    format!("{}.live_rank > 0 AND {}.live_points > 0", alias, alias)
}

fn effective_points_sql(alias: &str) -> String {
    let valid_live = valid_live_sql(alias);

    format!(
        "CASE \
            WHEN {} AND {} THEN {}.live_points \
            WHEN {} THEN COALESCE({}.last_month_point, {}.monthly_point) \
            WHEN {}.last_live_update >= {} AND {} THEN COALESCE(GREATEST({}.live_points, {}.monthly_point), {}.live_points, {}.monthly_point) \
            ELSE {}.monthly_point \
        END",
        rollover_active_sql(),
        valid_live,
        alias,
        rollover_active_sql(),
        alias,
        alias,
        alias,
        today_start_utc_sql(),
        valid_live,
        alias,
        alias,
        alias,
        alias,
        alias,
    )
}

fn rank_fallback_sql(alias: &str) -> String {
    let valid_live = valid_live_sql(alias);

    format!(
        "CASE \
            WHEN {} AND {} THEN {}.live_rank \
            WHEN {} THEN COALESCE({}.last_month_rank, {}.monthly_rank) \
            ELSE {}.monthly_rank \
        END",
        rollover_active_sql(),
        valid_live,
        alias,
        rollover_active_sql(),
        alias,
        alias,
        alias,
    )
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

    let circle = if let Some(viewer_id) = params.viewer_id {
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

    // Get all members and their fan counts for this circle
    let members =
        fetch_circle_members(&state.db, circle.circle_id, params.year, params.month).await?;

    let points = circle.monthly_point.unwrap_or(0);
    let club_rank = Some(compute_club_rank(circle.monthly_rank, Some(points)));
    let rank = circle.monthly_rank;

    let fans_to_next_tier = if let Some(boundary) = next_tier_boundary(rank, points) {
        match fetch_boundary_points(&state.db, boundary).await? {
            Some(bp) => Some((bp - points).max(0)),
            None => Some(0),
        }
    } else {
        Some(0) // Already at SS
    };

    let fans_to_lower_tier = if let Some(boundary) = lower_tier_boundary(rank, points) {
        match fetch_boundary_points(&state.db, boundary).await? {
            Some(bp) => Some((points - bp).max(0)),
            None => Some(0),
        }
    } else {
        Some(0) // Already at D
    };

    // Yesterday's tier gaps
    let y_points = circle.yesterday_points.unwrap_or(0);
    let y_rank = circle.yesterday_rank;

    let yesterday_fans_to_next_tier = if let Some(boundary) = next_tier_boundary(y_rank, y_points) {
        match fetch_boundary_points_yesterday(&state.db, boundary).await? {
            Some(bp) => Some((bp - y_points).max(0)),
            None => Some(0),
        }
    } else {
        Some(0)
    };

    let yesterday_fans_to_lower_tier = if let Some(boundary) = lower_tier_boundary(y_rank, y_points)
    {
        match fetch_boundary_points_yesterday(&state.db, boundary).await? {
            Some(bp) => Some((y_points - bp).max(0)),
            None => Some(0),
        }
    } else {
        Some(0)
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
///
/// Returns paginated list of circles
pub async fn list_circles(
    Query(params): Query<CircleListParams>,
    State(state): State<AppState>,
) -> Result<Json<CircleListResponse>, AppError> {
    let page = params.page.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(100);
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

    let join_global_ranks = "LEFT JOIN circle_live_ranks lr ON c.circle_id = lr.circle_id";
    let points_column = effective_points_sql("c");
    let rank_column = format!(
        "COALESCE(lr.live_rank::integer, {})",
        rank_fallback_sql("c")
    );
    let yesterday_rank_column = "COALESCE(lr.live_yesterday_rank::integer, c.yesterday_rank)";

    // Build dynamic query
    let mut count_query = format!(
        "{} SELECT COUNT(*) FROM circles c {} LEFT JOIN trainer t ON c.leader_viewer_id::text = t.account_id {} WHERE 1=1",
        with_clause,
        join_global_ranks,
        join_matching_circles
    );

    let mut select_query = format!(
        r#"
        {}
        SELECT 
            c.circle_id,
            c.name,
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
            c.yesterday_points,
            {} as yesterday_rank,
            c.live_points,
            c.live_rank,
            c.last_live_update
        FROM circles c
        {}
        LEFT JOIN trainer t ON c.leader_viewer_id::text = t.account_id
        {}
        WHERE 1=1
        "#,
        with_clause,
        rank_column,
        points_column,
        yesterday_rank_column,
        join_global_ranks,
        join_matching_circles
    );

    let mut conditions = Vec::new();

    // Only show circles updated this month to ensure points are current
    // Use JST minus 2 days so the month flips at midnight JST on the 3rd (giving time for data collection)
    conditions.push("c.last_updated >= date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')".to_string());
    // Exclude archived circles
    conditions.push("(c.archived IS NULL OR c.archived = false)".to_string());

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
    let sort_dir = params.sort_dir.as_deref().unwrap_or("asc");

    let order_clause = match sort_by {
        "name" => format!(
            " ORDER BY c.name {}, c.circle_id ASC",
            sort_dir.to_uppercase()
        ),
        "member_count" => format!(
            " ORDER BY c.member_count {} NULLS LAST, c.circle_id ASC",
            sort_dir.to_uppercase()
        ),
        "rank" | "monthly_rank" => {
            format!(" ORDER BY {} ASC NULLS LAST, c.circle_id ASC", rank_column)
        }
        "monthly_point" => format!(
            " ORDER BY {} {} NULLS LAST, c.circle_id ASC",
            points_column,
            sort_dir.to_uppercase()
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
            let effective_points = circle.monthly_point.unwrap_or(0);
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

#[derive(Debug, Serialize)]
pub struct RankThreshold {
    pub rank_index: i32,
    pub name: String,
    pub ranking_from: Option<i32>,
    pub ranking_to: Option<i32>,
    pub current_min_fans: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct RankThresholdsResponse {
    pub thresholds: Vec<RankThreshold>,
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

    let mut thresholds = Vec::new();

    for (name, rank_index, ranking_from, ranking_to) in tiers {
        let current_min_fans = if let Some(boundary) = ranking_to {
            fetch_boundary_points(&state.db, boundary).await?
        } else {
            None
        };

        thresholds.push(RankThreshold {
            rank_index,
            name: name.to_string(),
            ranking_from,
            ranking_to,
            current_min_fans,
        });
    }

    Ok(Json(RankThresholdsResponse { thresholds }))
}

/// Convert a ranking position and monthly points to club rank index (1-11)
/// 1=D, 2=D+, 3=C, 4=C+, 5=B, 6=B+, 7=A, 8=A+, 9=S, 10=S+, 11=SS
fn compute_club_rank(rank: Option<i32>, monthly_point: Option<i64>) -> i32 {
    // 0 points or no rank = D (index 1)
    match monthly_point {
        None | Some(0) => return 1,
        _ => {}
    }
    match rank {
        None => 1,
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

/// Fetch the effective current points of the circle at the given boundary rank
async fn fetch_boundary_points(pool: &PgPool, boundary_rank: i32) -> Result<Option<i64>, AppError> {
    let points_column = effective_points_sql("c");

    let result: Option<i64> = sqlx::query_scalar(
        &format!(
        r#"
        SELECT {}
        FROM circles c
        JOIN circle_live_ranks lr ON c.circle_id = lr.circle_id
        WHERE lr.live_rank <= $1
          AND (c.live_points IS NOT NULL OR c.last_month_point IS NOT NULL OR c.monthly_point IS NOT NULL)
        ORDER BY lr.live_rank DESC
        LIMIT 1
        "#,
        points_column
        ),
    )
    .bind(boundary_rank)
    .fetch_optional(pool)
    .await?;

    Ok(result)
}

/// Fetch the yesterday_points of the circle at the given boundary rank (using yesterday's rankings)
async fn fetch_boundary_points_yesterday(
    pool: &PgPool,
    boundary_rank: i32,
) -> Result<Option<i64>, AppError> {
    let result: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT c.yesterday_points
        FROM circles c
        JOIN circle_live_ranks lr ON c.circle_id = lr.circle_id
        WHERE lr.live_yesterday_rank <= $1 AND c.yesterday_points IS NOT NULL
        ORDER BY lr.live_yesterday_rank DESC
        LIMIT 1
        "#,
    )
    .bind(boundary_rank)
    .fetch_optional(pool)
    .await?;

    Ok(result)
}

/// Fetch circle by ID
async fn fetch_circle_by_id(pool: &PgPool, circle_id: i64) -> Result<Circle, AppError> {
    let points_column = effective_points_sql("c");
    let rank_column = format!(
        "COALESCE(lr.live_rank::integer, {})",
        rank_fallback_sql("c")
    );

    let circle = sqlx::query_as::<_, Circle>(&format!(
        r#"
        SELECT 
            c.circle_id,
            c.name,
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
            c.yesterday_points,
            COALESCE(lr.live_yesterday_rank::integer, c.yesterday_rank) as yesterday_rank,
            c.live_points,
            c.live_rank,
            c.last_live_update
        FROM circles c
        LEFT JOIN trainer t ON c.leader_viewer_id::text = t.account_id
        LEFT JOIN circle_live_ranks lr ON c.circle_id = lr.circle_id
        WHERE c.circle_id = $1
        "#,
        rank_column, points_column,
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

    // Default to current date in JST minus 1 day if not provided
    // This means the month flips at midnight JST on the 2nd (giving time for data collection)
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
