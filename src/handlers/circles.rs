use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};
use tokio::sync::Semaphore;

use crate::{
    errors::AppError,
    types::{Circle, CircleMemberFansMonthly},
    AppState,
};

include!("../types/handlers/circles.rs");

fn circleSearchSourcesSql(query: &str) -> Option<String> {
    let query = query.trim();
    if query.is_empty() {
        return None;
    }

    if let Ok(query_id) = query.parse::<i64>() {
        return Some(format!(
            r#"
            SELECT circle_id FROM circles WHERE circle_id = {query_id}
            UNION
            SELECT circle_id FROM circles WHERE leader_viewer_id = {query_id}
            UNION
            SELECT circle_id
            FROM circle_member_fans_monthly
            WHERE viewer_id = {query_id}
              AND year = extract(year from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int
              AND month = extract(month from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int
            "#
        ));
    }

    let search_pattern = format!("%{}%", query.replace("'", "''"));
    let circle_visibility = currentCircleVisibilitySql("circle");
    Some(format!(
        r#"
        SELECT circle.circle_id
        FROM circles circle
        WHERE circle.name ILIKE '{search_pattern}'
          AND {circle_visibility}
        UNION
        SELECT circle.circle_id
        FROM circles circle
        JOIN trainer leader ON leader.account_id = circle.leader_viewer_id::text
        WHERE leader.name ILIKE '{search_pattern}'
          AND {circle_visibility}
        UNION
        SELECT rankings.circle_id
        FROM (
            SELECT DISTINCT circle_id
            FROM user_fan_rankings_monthly_current
            WHERE year = extract(year from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int
              AND month = extract(month from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int
              AND trainer_name ILIKE '{search_pattern}'
              AND circle_id IS NOT NULL
        ) rankings
        JOIN circles circle ON circle.circle_id = rankings.circle_id
        WHERE {circle_visibility}
        "#
    ))
}

fn currentCircleVisibilitySql(alias: &str) -> String {
    format!(
        "{alias}.last_updated >= date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days') \
         AND ({alias}.archived IS DISTINCT FROM true OR (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp < date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')"
    )
}

fn sqlCacheKey(prefix: &str, sql: &str) -> String {
    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    format!("{prefix}:{:016x}", hasher.finish())
}

/// Create the circles router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(getCircle))
        .route("/list", get(listCircles))
        .route("/rank-thresholds", get(getRankThresholds))
}

fn isRolloverDisplayWindow(now: DateTime<Utc>) -> bool {
    let jst = FixedOffset::east_opt(9 * 3600).expect("valid JST offset");
    now.with_timezone(&jst).day() == 2
}

fn rolloverDisplaySql() -> &'static str {
    if isRolloverDisplayWindow(Utc::now()) {
        "TRUE"
    } else {
        "FALSE"
    }
}

fn rolloverStartUtc(now: DateTime<Utc>) -> chrono::NaiveDateTime {
    let jst = FixedOffset::east_opt(9 * 3600).expect("valid JST offset");
    let now_jst = now.with_timezone(&jst);
    let rollover_start_jst = NaiveDate::from_ymd_opt(now_jst.year(), now_jst.month(), 1)
        .expect("first day exists")
        .and_hms_opt(0, 0, 0)
        .expect("midnight exists")
        + Duration::days(1);

    rollover_start_jst - Duration::hours(9)
}

fn rowHasNewLastMonthSql(alias: &str) -> String {
    let rolloverStartUtc = rolloverStartUtc(Utc::now()).format("%Y-%m-%d %H:%M:%S");

    format!(
        "{}.last_updated >= TIMESTAMP '{}' AND NOT COALESCE({}.archived, false)",
        alias, rolloverStartUtc, alias
    )
}

fn validLiveSql(alias: &str) -> String {
    format!("{}.live_rank > 0 AND {}.live_points > 0", alias, alias)
}

fn hasLivePointsSql(alias: &str) -> String {
    format!("{}.live_points > 0", alias)
}

fn positiveRankSql(expr: &str) -> String {
    format!("CASE WHEN {expr} > 0 THEN {expr} ELSE NULL END")
}

fn disbandedNameSql(alias: &str) -> String {
    format!(
        "CASE WHEN COALESCE({}.archived, false) AND {}.name NOT LIKE '% ( DISBANDED )' THEN {}.name || ' ( DISBANDED )' ELSE {}.name END",
        alias, alias, alias, alias
    )
}

fn effectivePointsSql(alias: &str) -> String {
    format!(
        "CASE \
            WHEN {} AND {} THEN COALESCE({}.last_month_point, {}.monthly_point) \
            WHEN {} THEN {}.monthly_point \
            WHEN {} THEN COALESCE(GREATEST({}.live_points, {}.monthly_point), {}.live_points, {}.monthly_point) \
            ELSE {}.monthly_point \
        END",
        rolloverDisplaySql(),
        rowHasNewLastMonthSql(alias),
        alias,
        alias,
        rolloverDisplaySql(),
        alias,
        hasLivePointsSql(alias),
        alias,
        alias,
        alias,
        alias,
        alias,
    )
}

fn rankFallbackSql(alias: &str) -> String {
    let monthly_rank = positiveRankSql(&format!("{}.monthly_rank", alias));
    let live_rank = positiveRankSql(&format!("{}.live_rank", alias));
    let last_month_rank = positiveRankSql(&format!("{}.last_month_rank", alias));
    let display_last_month_rank = format!("COALESCE({last_month_rank}, {monthly_rank})");

    format!(
        "CASE \
            WHEN {} AND {} THEN {} \
            WHEN {} THEN {} \
            WHEN {} THEN {} \
            ELSE {} \
        END",
        rolloverDisplaySql(),
        rowHasNewLastMonthSql(alias),
        display_last_month_rank,
        rolloverDisplaySql(),
        monthly_rank,
        validLiveSql(alias),
        live_rank,
        monthly_rank,
    )
}

fn rankColumnSql(alias: &str, live_rank_expr: &str) -> String {
    let rank_fallback = rankFallbackSql(alias);
    let live_rank = positiveRankSql(&format!("{live_rank_expr}::int"));

    format!(
        "CASE WHEN {} THEN {} ELSE COALESCE({}, {}) END",
        rolloverDisplaySql(),
        rank_fallback,
        live_rank,
        rank_fallback,
    )
}

fn displayMonthlyPointSql(alias: &str) -> String {
    format!(
        "CASE WHEN {} AND {} THEN COALESCE({}.last_month_point, {}.monthly_point) ELSE {}.monthly_point END",
        rolloverDisplaySql(),
        rowHasNewLastMonthSql(alias),
        alias,
        alias,
        alias,
    )
}

fn displayYesterdayPointsSql(alias: &str) -> String {
    format!(
        "CASE WHEN {} AND {} THEN COALESCE({}.last_month_point, {}.yesterday_points) ELSE {}.yesterday_points END",
        rolloverDisplaySql(),
        rowHasNewLastMonthSql(alias),
        alias,
        alias,
        alias,
    )
}

fn displayYesterdayRankSql(alias: &str) -> String {
    let yesterday_rank = positiveRankSql(&format!("{}.yesterday_rank", alias));
    let last_month_rank = positiveRankSql(&format!("{}.last_month_rank", alias));

    format!(
        "CASE WHEN {} AND {} THEN COALESCE({}, {}) ELSE {} END",
        rolloverDisplaySql(),
        rowHasNewLastMonthSql(alias),
        last_month_rank,
        yesterday_rank,
        yesterday_rank,
    )
}

fn displayYesterdayRankExprSql(alias: &str, live_yesterday_rank_expr: &str) -> String {
    let live_yesterday_rank = positiveRankSql(&format!("{}::int", live_yesterday_rank_expr));
    let fallback_rank = displayYesterdayRankSql(alias);

    format!(
        "CASE WHEN {} THEN {} ELSE COALESCE({}, {}) END",
        rolloverDisplaySql(),
        fallback_rank,
        live_yesterday_rank,
        fallback_rank,
    )
}

fn displayLivePointsSql(alias: &str) -> String {
    format!(
        "CASE WHEN {} OR {}.live_points <= 0 THEN NULL ELSE {}.live_points END",
        rolloverDisplaySql(),
        alias,
        alias,
    )
}

fn displayLiveRankExprSql(live_rank_expr: &str) -> String {
    format!(
        "CASE WHEN {} THEN NULL ELSE {} END",
        rolloverDisplaySql(),
        live_rank_expr,
    )
}

fn displayLastLiveUpdateSql(alias: &str) -> String {
    format!(
        "CASE WHEN {} THEN NULL ELSE {}.last_live_update END",
        rolloverDisplaySql(),
        alias,
    )
}

fn effectiveCirclePoints(circle: &Circle) -> i64 {
    let jst_offset = FixedOffset::east_opt(9 * 3600).unwrap();
    let now_jst = Utc::now().with_timezone(&jst_offset);
    let month_start_jst = NaiveDate::from_ymd_opt(now_jst.year(), now_jst.month(), 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let game_month_start_jst = month_start_jst + Duration::days(1);
    let display_end_jst = month_start_jst + Duration::days(2);
    let game_month_start_utc = game_month_start_jst - Duration::hours(9);
    let now_jst_naive = now_jst.naive_local();
    let has_live_points = circle.live_points.unwrap_or(0) > 0;
    let archived = circle.archived.unwrap_or(false);

    if now_jst_naive >= game_month_start_jst && now_jst_naive < display_end_jst {
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

fn currentGameMonthStart(now: DateTime<Utc>) -> NaiveDate {
    let jst = FixedOffset::east_opt(9 * 3600).expect("valid JST offset");
    (now.with_timezone(&jst) - Duration::days(1))
        .date_naive()
        .with_day(1)
        .expect("first day exists")
}

fn resolveCircleMonth(
    year: Option<i32>,
    month: Option<i32>,
) -> Result<Option<(i32, i32, bool)>, AppError> {
    if year.is_none() && month.is_none() {
        return Ok(None);
    }

    let current_month = currentGameMonthStart(Utc::now());
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

async fn applyHistoricalCircleMonth(
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
pub async fn getCircle(
    Query(params): Query<CircleQueryParams>,
    State(state): State<AppState>,
) -> Result<Json<CircleResponse>, AppError> {
    // Validate that at least one parameter is provided
    if params.viewer_id.is_none() && params.circle_id.is_none() {
        return Err(AppError::BadRequest(
            "Either viewer_id or circle_id must be provided".to_string(),
        ));
    }

    let requested_month = resolveCircleMonth(params.year, params.month)?;
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
                fetchCircleById(&state.db, circle_id).await?
            }
            None => {
                // Viewer not found - add to tasks for later fetching
                addViewerToTasks(&state.db, viewer_id).await?;

                return Err(AppError::NotFound(format!(
                    "Viewer {} not found in any circle. Added to task queue for fetching.",
                    viewer_id
                )));
            }
        }
    } else if let Some(circle_id) = params.circle_id {
        // Query by circle_id directly
        fetchCircleById(&state.db, circle_id).await?
    } else {
        unreachable!("Already validated at least one param exists");
    };

    if let Some((year, month, _)) = historical_month {
        applyHistoricalCircleMonth(&state.db, &mut circle, year, month).await?;
    }

    // Get all members and their fan counts for this circle
    let (member_year, member_month) = requested_month
        .map(|(year, month, _)| (Some(year), Some(month)))
        .unwrap_or((None, None));
    let members =
        fetchCircleMembers(&state.db, circle.circle_id, member_year, member_month).await?;

    let points = effectiveCirclePoints(&circle);
    let club_rank = Some(computeClubRank(circle.monthly_rank, Some(points)));
    let rank = circle.monthly_rank;

    let fans_to_next_tier = if let Some(boundary) = nextTierBoundary(rank, points) {
        let boundary_points = if let Some((year, month, _)) = historical_month {
            fetchHistoricalBoundaryPoints(&state.db, year, month, boundary, true).await?
        } else {
            fetchBoundaryPoints(&state.db, boundary).await?
        };
        match boundary_points {
            Some(bp) => Some((bp - points).max(0)),
            None => Some(0),
        }
    } else {
        Some(0) // Already at SS
    };

    let fans_to_lower_tier = if let Some(boundary) = lowerTierBoundary(rank, points) {
        let boundary_points = if let Some((year, month, _)) = historical_month {
            fetchHistoricalBoundaryPoints(&state.db, year, month, boundary, false).await?
        } else {
            fetchBoundaryPoints(&state.db, boundary).await?
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
        let y_tier_gap_rank = historicalTierGapRank(rank, y_rank);

        let next = if let Some(boundary) = nextTierBoundary(y_tier_gap_rank, y_points) {
            match fetchBoundaryPointsYesterday(&state.db, boundary).await? {
                Some(bp) => Some((bp - y_points).max(0)),
                None => Some(0),
            }
        } else {
            Some(0)
        };
        let lower = if let Some(boundary) = lowerTierBoundary(y_tier_gap_rank, y_points) {
            match fetchBoundaryPointsYesterday(&state.db, boundary).await? {
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
pub async fn listCircles(
    Query(params): Query<CircleListParams>,
    State(state): State<AppState>,
) -> Result<Json<CircleListResponse>, AppError> {
    match (params.year, params.month) {
        (Some(year), Some(month)) => {
            let current_month = currentGameMonthStart(Utc::now());
            let target_month = NaiveDate::from_ymd_opt(year, month as u32, 1)
                .ok_or_else(|| AppError::BadRequest("invalid historical year/month".into()))?;
            if target_month > current_month {
                return Err(AppError::BadRequest(
                    "historical ranking month cannot be in the future".into(),
                ));
            }
            if target_month < current_month {
                return listHistoricalCircles(&state.db, &params, year, month).await;
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

    let normalized_query = params.query.as_deref().map(str::trim).unwrap_or("");
    let is_text_search = !normalized_query.is_empty() && normalized_query.parse::<i64>().is_err();
    if is_text_search && normalized_query.chars().count() < 3 {
        return Err(AppError::BadRequest(
            "text search queries must contain at least 3 characters".to_string(),
        ));
    }
    let _text_search_slot = if is_text_search {
        Some(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                CIRCLE_TEXT_SEARCH_DB_SLOTS.acquire(),
            )
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable(
                    "Circle search is busy; please retry shortly.".to_string(),
                )
            })?
            .map_err(|_| {
                AppError::ServiceUnavailable("Circle search is unavailable.".to_string())
            })?,
        )
    } else {
        None
    };

    let mut with_parts = Vec::new();

    // If search query is present, add MatchingCircles CTE to optimize search
    let mut join_matching_circles = String::new();

    if let Some(search_sources) = params.query.as_deref().and_then(circleSearchSourcesSql) {
        with_parts.push(format!("MatchingCircles AS ({search_sources})"));
        join_matching_circles =
            "INNER JOIN MatchingCircles mc ON c.circle_id = mc.circle_id".to_string();
    }

    let with_clause = if with_parts.is_empty() {
        String::new()
    } else {
        format!("WITH {}", with_parts.join(", "))
    };

    let points_column = effectivePointsSql("c");
    let rank_column = rankColumnSql("c", "lr.live_rank");
    let name_column = disbandedNameSql("c");
    let monthly_point_column = displayMonthlyPointSql("c");
    let yesterday_points_column = displayYesterdayPointsSql("c");
    let yesterday_rank_column = displayYesterdayRankExprSql("c", "lr.live_yesterday_rank");
    let live_points_column = displayLivePointsSql("c");
    let live_rank_expr = format!(
        "COALESCE({}, {})",
        positiveRankSql("lr.live_rank::int"),
        positiveRankSql("c.live_rank")
    );
    let live_rank_column = displayLiveRankExprSql(&live_rank_expr);
    let last_live_update_column = displayLastLiveUpdateSql("c");
    // Build dynamic query
    let count_rank_join = if params.max_rank.is_some() {
        "LEFT JOIN circle_live_ranks lr ON lr.circle_id = c.circle_id"
    } else {
        ""
    };
    let mut count_query = format!(
        "{} SELECT COUNT(*) FROM circles c {} {} WHERE 1=1",
        with_clause, count_rank_join, join_matching_circles
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
    conditions.push(currentCircleVisibilitySql("c"));

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

    // Counts are identical across pages and change slowly. Keep them separate
    // from the row query so LIMIT/OFFSET can stop early, and cache the result
    // to avoid repeating the full count for pagination probes.
    let count_cache_key = sqlCacheKey("circle:list:count", &count_query);
    let total = if let Some(total) = crate::cache::get::<i64>(&count_cache_key) {
        total
    } else {
        let total = sqlx::query_scalar::<_, i64>(&count_query)
            .fetch_one(&state.db)
            .await?;
        let _ = crate::cache::set(&count_cache_key, &total, std::time::Duration::from_secs(60));
        total
    };

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
            let effective_points = effectiveCirclePoints(&circle);
            let club_rank = Some(computeClubRank(circle.monthly_rank, Some(effective_points)));
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

async fn listHistoricalCircles(
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
            club_rank: Some(computeClubRank(circle.monthly_rank, circle.monthly_point)),
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

fn monthProgressJst() -> MonthProgress {
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

fn fansPerDay(total_fans: Option<i64>, days: i64) -> Option<i64> {
    let total_fans = total_fans?;
    if days <= 0 || total_fans <= 0 {
        Some(0)
    } else {
        Some(total_fans.saturating_add(days - 1) / days)
    }
}

fn optionDelta(current: Option<i64>, previous: Option<i64>) -> Option<i64> {
    Some(current? - previous?)
}

/// GET /api/v4/circles/rank-thresholds - Get the fan requirements for each circle rank tier
pub async fn getRankThresholds(
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

    let month_progress = monthProgressJst();
    let mut thresholds = Vec::new();

    for (name, rank_index, ranking_from, ranking_to) in tiers {
        let (
            current_min_fans,
            yesterday_min_fans,
            last_month_min_fans,
            daily_fans_delta,
            current_vs_last_month_delta,
        ) = if let Some(boundary) = ranking_to {
            let current = fetchBoundaryPoints(&state.db, boundary).await?;
            let yesterday = fetchBoundaryPointsYesterday(&state.db, boundary).await?;
            let last_month = fetchBoundaryPointsLastMonth(&state.db, boundary).await?;
            (
                current,
                yesterday,
                last_month,
                optionDelta(current, yesterday),
                optionDelta(current, last_month),
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
            current_fans_per_day: fansPerDay(current_min_fans, month_progress.elapsed_days),
            yesterday_min_fans,
            yesterday_fans_per_day: fansPerDay(
                yesterday_min_fans,
                month_progress.yesterday_elapsed_days,
            ),
            daily_fans_delta,
            last_month_min_fans,
            last_month_fans_per_day: fansPerDay(
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
fn computeClubRank(rank: Option<i32>, monthly_point: Option<i64>) -> i32 {
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
fn nextTierBoundary(rank: Option<i32>, points: i64) -> Option<i32> {
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
fn lowerTierBoundary(rank: Option<i32>, points: i64) -> Option<i32> {
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

fn historicalTierGapRank(current_rank: Option<i32>, historical_rank: Option<i32>) -> Option<i32> {
    current_rank
        .filter(|rank| *rank > 0)
        .or_else(|| historical_rank.filter(|rank| *rank > 0))
}

/// Fetch the effective current points of the circle at the given boundary rank
async fn fetchBoundaryPoints(pool: &PgPool, boundary_rank: i32) -> Result<Option<i64>, AppError> {
    let cache_key = format!(
        "circle:boundary:current:{}:{boundary_rank}",
        Utc::now().date_naive()
    );
    if let Some(points) = crate::cache::get::<Option<i64>>(&cache_key) {
        return Ok(points);
    }

    let points_column = effectivePointsSql("c");
    let rank_column = rankColumnSql("c", "lr.live_rank");

    let result: Option<Option<i64>> = sqlx::query_scalar(&format!(
        r#"
        SELECT {}
        FROM circles c
        JOIN circle_live_ranks lr ON c.circle_id = lr.circle_id
        WHERE ({}) <= $1
          AND ({}) IS NOT NULL
        ORDER BY ({}) DESC
        LIMIT 1
        "#,
        points_column, rank_column, points_column, rank_column
    ))
    .bind(boundary_rank)
    .fetch_optional(pool)
    .await?;

    let points = result.flatten();
    let _ = crate::cache::set(&cache_key, &points, std::time::Duration::from_secs(300));
    Ok(points)
}

/// Fetch the monthly points at a historical tier boundary. For an upper-tier
/// boundary we use the last rank still inside that tier; for a lower-tier
/// boundary we use the first rank in the tier below.
async fn fetchHistoricalBoundaryPoints(
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
async fn fetchBoundaryPointsYesterday(
    pool: &PgPool,
    boundary_rank: i32,
) -> Result<Option<i64>, AppError> {
    let cache_key = format!(
        "circle:boundary:yesterday:{}:{boundary_rank}",
        Utc::now().date_naive()
    );
    if let Some(points) = crate::cache::get::<Option<i64>>(&cache_key) {
        return Ok(points);
    }

    let points_column = displayYesterdayPointsSql("c");
    let rank_column = displayYesterdayRankExprSql("c", "lr.live_yesterday_rank");

    let result: Option<Option<i64>> = sqlx::query_scalar(&format!(
        r#"
        SELECT {}
        FROM circles c
        JOIN circle_live_ranks lr ON c.circle_id = lr.circle_id
        WHERE ({}) <= $1
          AND ({}) IS NOT NULL
        ORDER BY ({}) DESC
        LIMIT 1
        "#,
        points_column, rank_column, points_column, rank_column
    ))
    .bind(boundary_rank)
    .fetch_optional(pool)
    .await?;

    let points = result.flatten();
    let _ = crate::cache::set(&cache_key, &points, std::time::Duration::from_secs(300));
    Ok(points)
}

/// Fetch the last_month_point of the circle at the given boundary rank.
async fn fetchBoundaryPointsLastMonth(
    pool: &PgPool,
    boundary_rank: i32,
) -> Result<Option<i64>, AppError> {
    let cache_key = format!(
        "circle:boundary:last-month:{}:{boundary_rank}",
        Utc::now().date_naive()
    );
    if let Some(points) = crate::cache::get::<Option<i64>>(&cache_key) {
        return Ok(points);
    }

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

    let points = result.flatten();
    let _ = crate::cache::set(&cache_key, &points, std::time::Duration::from_secs(300));
    Ok(points)
}

/// Fetch circle by ID
async fn fetchCircleById(pool: &PgPool, circle_id: i64) -> Result<Circle, AppError> {
    let rank_column = rankColumnSql("c", "lr.live_rank");
    let name_column = disbandedNameSql("c");
    let monthly_point_column = displayMonthlyPointSql("c");
    let yesterday_points_column = displayYesterdayPointsSql("c");
    let yesterday_rank_column = displayYesterdayRankExprSql("c", "lr.live_yesterday_rank");
    let live_points_column = displayLivePointsSql("c");
    let live_rank_expr = format!(
        "COALESCE({}, {})",
        positiveRankSql("lr.live_rank::int"),
        positiveRankSql("c.live_rank")
    );
    let live_rank_column = displayLiveRankExprSql(&live_rank_expr);
    let last_live_update_column = displayLastLiveUpdateSql("c");

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
async fn fetchCircleMembers(
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
async fn addViewerToTasks(pool: &PgPool, viewer_id: i64) -> Result<(), AppError> {
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
    fn textSearchReusesExistingIndexedRankings() {
        let sql = circleSearchSourcesSql("Sta").expect("search SQL");

        assert!(sql.contains("FROM user_fan_rankings_monthly_current"));
        assert!(sql.contains("ILIKE '%Sta%'"));
        assert!(!sql.contains("circle_member_fans_monthly"));
        assert!(!sql.contains("circle_search_documents"));
    }

    #[test]
    fn numericSearchKeepsDirectIndexableLookups() {
        let sql = circleSearchSourcesSql(" 123 ").expect("search SQL");

        assert!(sql.contains("circle_id = 123"));
        assert!(sql.contains("leader_viewer_id = 123"));
        assert!(sql.contains("viewer_id = 123"));
        assert!(!sql.contains("user_fan_rankings_monthly_current"));
    }

    #[test]
    fn yesterdayTierGapsAreAnchoredToCurrentRank() {
        let current_rank = Some(501);
        let yesterday_rank = Some(484);
        let yesterday_points = 365_509_264;

        let rank = historicalTierGapRank(current_rank, yesterday_rank);

        assert_eq!(rank, current_rank);
        assert_eq!(nextTierBoundary(rank, yesterday_points), Some(500));
        assert_eq!(lowerTierBoundary(rank, yesterday_points), Some(1001));
    }

    #[test]
    fn yesterdayTierGapsFallBackWithoutCurrentRank() {
        let rank = historicalTierGapRank(None, Some(484));

        assert_eq!(rank, Some(484));
        assert_eq!(nextTierBoundary(rank, 365_509_264), Some(100));
        assert_eq!(lowerTierBoundary(rank, 365_509_264), Some(501));
    }

    #[test]
    fn zeroRanksAreTreatedAsUnranked() {
        assert_eq!(computeClubRank(Some(0), Some(0)), 1);
        assert_eq!(computeClubRank(Some(0), Some(1)), 2);
        assert_eq!(historicalTierGapRank(Some(0), Some(484)), Some(484));
    }

    #[test]
    fn historicalRankingsUseTheSameTiersAsLiveCircles() {
        let cases = [
            (Some(1), Some(1), 11),
            (Some(100), Some(1), 9),
            (Some(5_001), Some(1), 4),
            (Some(10_001), Some(1), 2),
            (None, Some(0), 1),
        ];

        for (rank, points, expected_rank) in cases {
            let actual_rank = computeClubRank(rank, points);
            assert_eq!(actual_rank, expected_rank);
        }
    }

    #[test]
    fn rankingMigrationUsesMonthlySummariesInsteadOfRawArrays() {
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
    fn displayedLivePointsDoNotRequireRawLiveRank() {
        let sql = displayLivePointsSql("c");

        assert!(sql.contains("c.live_points <= 0"));
        assert!(!sql.contains("c.live_rank"));
    }

    #[test]
    fn effectivePointsUseLivePointsWithoutRawLiveRank() {
        let sql = effectivePointsSql("c");

        assert!(sql.contains("c.live_points > 0"));
        assert!(!sql.contains("c.live_rank > 0 AND c.live_points > 0"));
    }

    #[test]
    fn circleRolloverIsLimitedToSecondThroughThirdJst() {
        let august_1 = DateTime::parse_from_rfc3339("2026-08-01T14:59:59Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let august_2_start = DateTime::parse_from_rfc3339("2026-08-01T15:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let august_3_start = DateTime::parse_from_rfc3339("2026-08-02T15:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);

        assert!(!isRolloverDisplayWindow(august_1));
        assert!(isRolloverDisplayWindow(august_2_start));
        assert!(!isRolloverDisplayWindow(august_3_start));
        assert_eq!(
            currentGameMonthStart(august_1),
            NaiveDate::from_ymd_opt(2026, 7, 1).expect("valid date")
        );
        assert_eq!(
            currentGameMonthStart(august_2_start),
            NaiveDate::from_ymd_opt(2026, 8, 1).expect("valid date")
        );
        assert_eq!(rolloverStartUtc(august_2_start), august_2_start.naive_utc());

        let migration =
            include_str!("../../migrations/20260801000000_use_api_clock_for_circle_rollover.sql");
        assert!(!migration.contains("rollover_start_jst"));
        assert!(!migration.contains("last_month_rank"));
        assert!(!migration.contains("last_month_point"));
    }
}
