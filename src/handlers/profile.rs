use axum::{
    extract::{Path, State},
    response::Json,
    routing::{get, put},
    Router,
};
use sqlx::FromRow;

use crate::errors::AppError;
use crate::middleware::auth::AuthenticatedUser;
use crate::models::profile::{
    CircleHistoryEntry, CircleInfo, FanHistory, ProfileResponse, ProfileVisibility,
    TeamStadiumMember, TrainerInfo, VeteranCharacter,
};
use crate::models::{
    Inheritance, SupportCard, UserFanRankingAlltime, UserFanRankingGains, UserFanRankingMonthly,
};
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/:account_id", get(get_profile))
        .route(
            "/:account_id/visibility",
            get(get_visibility).put(update_visibility),
        )
        .route(
            "/:account_id/veterans/:trained_chara_id/pin",
            put(pin_veteran).delete(unpin_veteran),
        )
}

/// GET /api/v4/user/profile/:account_id — single call returning everything for a user profile
async fn get_profile(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    auth_user: Option<AuthenticatedUser>,
) -> Result<Json<ProfileResponse>, AppError> {
    let is_owner = check_is_owner(&state.db, auth_user.as_ref(), &account_id).await?;
    let visibility = get_privacy_settings(&state.db, &account_id).await?;

    if visibility.profile_hidden && !is_owner {
        return Err(AppError::Forbidden("This profile is hidden".into()));
    }
    // 1) Trainer info
    let trainer = sqlx::query_as::<_, TrainerInfo>(
        r#"SELECT account_id, name, follower_num, best_team_class, team_class,
               team_evaluation_point, leader_chara_dress_id, rank_score,
               release_num_info, trophy_num_info, team_stadium_user,
               own_follow_num, enable_circle_scout, comment
        FROM trainer WHERE account_id = $1"#,
    )
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Trainer not found".into()))?;

    let viewer_id: i64 = account_id
        .parse()
        .map_err(|_| AppError::BadRequest("Invalid account_id".into()))?;

    // 2) Current circle — find latest circle_member_fans_monthly entry
    #[derive(FromRow)]
    struct CurrentCircleRow {
        circle_id: i64,
    }
    let current_circle_row = sqlx::query_as::<_, CurrentCircleRow>(
        r#"
        SELECT circle_id
        FROM circle_member_fans_monthly
        WHERE viewer_id = $1
        ORDER BY year DESC, month DESC
        LIMIT 1
        "#,
    )
    .bind(viewer_id)
    .fetch_optional(&state.db)
    .await?;

    let circle = if let Some(row) = current_circle_row {
        #[derive(FromRow)]
        struct CircleRow {
            circle_id: i64,
            name: String,
            member_count: Option<i32>,
            monthly_rank: Option<i32>,
            monthly_point: Option<i64>,
            last_month_rank: Option<i32>,
            last_month_point: Option<i64>,
            live_points: Option<i64>,
            live_rank: Option<i32>,
        }
        sqlx::query_as::<_, CircleRow>(
            r#"
            SELECT circle_id, name, member_count,
                   monthly_rank, monthly_point,
                   last_month_rank, last_month_point,
                   live_points, live_rank
            FROM circles
            WHERE circle_id = $1
            "#,
        )
        .bind(row.circle_id)
        .fetch_optional(&state.db)
        .await?
        .map(|c| CircleInfo {
            circle_id: c.circle_id,
            name: c.name,
            member_count: c.member_count,
            monthly_rank: c.monthly_rank,
            monthly_point: c.monthly_point,
            last_month_rank: c.last_month_rank,
            last_month_point: c.last_month_point,
            live_points: c.live_points,
            live_rank: c.live_rank,
        })
    } else {
        None
    };

    // 3) Circle membership history — which circle the user was in each month, with rank
    let circle_history = sqlx::query_as::<_, CircleHistoryEntry>(
        r#"
        SELECT DISTINCT ON (cmf.year, cmf.month)
               cmf.year, cmf.month, cmf.circle_id,
               c.name AS circle_name,
               cra.rank AS circle_rank,
               cra.total_points AS circle_points
        FROM circle_member_fans_monthly cmf
        LEFT JOIN circles c ON c.circle_id = cmf.circle_id
        LEFT JOIN circle_ranks_monthly_archive cra
            ON cra.circle_id = cmf.circle_id
            AND cra.year = cmf.year
            AND cra.month = cmf.month
        WHERE cmf.viewer_id = $1
        ORDER BY cmf.year DESC, cmf.month DESC
        "#,
    )
    .bind(viewer_id)
    .fetch_all(&state.db)
    .await?;

    // 4) Fan history — monthly rankings (graceful fallback if views have old schema)
    let monthly = sqlx::query_as::<_, UserFanRankingMonthly>(
        r#"
        SELECT viewer_id, trainer_name, year, month, total_fans, monthly_gain,
               active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
               circle_id, circle_name, next_month_start
        FROM user_fan_rankings_monthly
        WHERE viewer_id = $1
        ORDER BY year DESC, month DESC
        "#,
    )
    .bind(viewer_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    // 5) Rolling gains
    let rolling = sqlx::query_as::<_, UserFanRankingGains>(
        r#"SELECT viewer_id, trainer_name, gain_3d, gain_7d, gain_30d,
                rank_3d, rank_7d, rank_30d, circle_id, circle_name
         FROM user_fan_rankings_gains WHERE viewer_id = $1"#,
    )
    .bind(viewer_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    // 6) All-time ranking
    let alltime = sqlx::query_as::<_, UserFanRankingAlltime>(
        r#"SELECT viewer_id, trainer_name, total_fans, total_gain, active_days,
                avg_day, avg_week, avg_month, rank,
                rank_total_fans, rank_total_gain, rank_avg_day, rank_avg_week, rank_avg_month,
                circle_id, circle_name
         FROM user_fan_rankings_alltime WHERE viewer_id = $1"#,
    )
    .bind(viewer_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    // 7) Inheritance
    let inheritance = sqlx::query_as::<_, Inheritance>(
        "SELECT * FROM inheritance WHERE account_id = $1",
    )
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await?;

    // 8) Support card
    let support_card = sqlx::query_as::<_, SupportCard>(
        "SELECT * FROM support_card WHERE account_id = $1",
    )
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await?;

    // 9) Team stadium members
    let team_stadium = sqlx::query_as::<_, TeamStadiumMember>(
        r#"SELECT id, trainer_id, distance_type, member_id, trained_chara_id,
               running_style, card_id::int4, speed, power, stamina, wiz, guts, fans,
               rank_score::int8, skills, creation_time::text, scenario_id, factors, support_cards,
               proper_ground_turf, proper_ground_dirt,
               proper_running_style_nige, proper_running_style_senko,
               proper_running_style_sashi, proper_running_style_oikomi,
               proper_distance_short, proper_distance_mile,
               proper_distance_middle, proper_distance_long,
               rarity, talent_level, team_rating
        FROM team_stadium WHERE trainer_id = $1
        ORDER BY distance_type, member_id"#,
    )
    .bind(&account_id)
    .fetch_all(&state.db)
    .await?;

    // 10) Veteran characters with pinned status
    let veterans = sqlx::query_as::<_, VeteranCharacter>(
        r#"
        SELECT vc.account_id, vc.trained_chara_id, vc.card_id, vc.scenario_id, vc.route_id,
               vc.rarity, vc.succession_trained_chara_id_1, vc.succession_trained_chara_id_2,
               vc.succession_num, vc.speed, vc.stamina, vc.power, vc.wiz, vc.guts, vc.fans,
               vc.rank_score, vc.rank, vc.chara_grade, vc.talent_level, vc.running_style,
               vc.race_cloth_id, vc.nickname_id, vc.wins,
               vc.proper_ground_turf, vc.proper_ground_dirt,
               vc.proper_running_style_nige, vc.proper_running_style_senko,
               vc.proper_running_style_sashi, vc.proper_running_style_oikomi,
               vc.proper_distance_short, vc.proper_distance_mile,
               vc.proper_distance_middle, vc.proper_distance_long,
               vc.skill_array, vc.support_card_list, vc.factor_info_array, vc.win_saddle_id_array,
               vc.succession_chara_array,
               vc.register_time, vc.create_time, vc.ingested_at, vc.updated_at,
               (vp.trained_chara_id IS NOT NULL) AS is_pinned
        FROM veteran_characters vc
        LEFT JOIN veteran_pins vp
            ON vp.account_id = vc.account_id AND vp.trained_chara_id = vc.trained_chara_id
        WHERE vc.account_id = $1
        ORDER BY is_pinned DESC, vc.rank_score DESC NULLS LAST
        "#,
    )
    .bind(&account_id)
    .fetch_all(&state.db)
    .await?;

    // Apply per-section visibility for non-owners
    let hidden = &visibility.hidden_sections;
    let circle = if !is_owner && hidden.iter().any(|s| s == "circle") {
        None
    } else {
        circle
    };
    let circle_history = if !is_owner && hidden.iter().any(|s| s == "circle") {
        vec![]
    } else {
        circle_history
    };
    let (monthly, rolling, alltime) = if !is_owner && hidden.iter().any(|s| s == "fan_history") {
        (vec![], None, None)
    } else {
        (monthly, rolling, alltime)
    };
    let inheritance = if !is_owner && hidden.iter().any(|s| s == "inheritance") {
        None
    } else {
        inheritance
    };
    let support_card = if !is_owner && hidden.iter().any(|s| s == "support_card") {
        None
    } else {
        support_card
    };
    let team_stadium = if !is_owner && hidden.iter().any(|s| s == "team_stadium") {
        vec![]
    } else {
        team_stadium
    };
    let veterans = if !is_owner && hidden.iter().any(|s| s == "veterans") {
        vec![]
    } else {
        veterans
    };

    Ok(Json(ProfileResponse {
        trainer,
        circle,
        circle_history,
        fan_history: FanHistory {
            monthly,
            rolling,
            alltime,
        },
        inheritance,
        support_card,
        team_stadium,
        veterans,
    }))
}

/// GET /api/v4/user/profile/:account_id/visibility
async fn get_visibility(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
) -> Result<Json<ProfileVisibility>, AppError> {
    let vis = get_privacy_settings(&state.db, &account_id).await?;
    Ok(Json(vis))
}

/// PUT /api/v4/user/profile/:account_id/visibility
async fn update_visibility(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    auth_user: AuthenticatedUser,
    Json(body): Json<ProfileVisibility>,
) -> Result<Json<ProfileVisibility>, AppError> {
    let is_owner = check_is_owner(&state.db, Some(&auth_user), &account_id).await?;
    if !is_owner {
        return Err(AppError::Forbidden(
            "You don't have permission to edit this profile".into(),
        ));
    }

    sqlx::query(
        r#"INSERT INTO user_privacy_settings (account_id, profile_hidden, hidden_sections)
           VALUES ($1, $2, $3)
           ON CONFLICT (account_id) DO UPDATE SET
               profile_hidden = EXCLUDED.profile_hidden,
               hidden_sections = EXCLUDED.hidden_sections,
               updated_at = NOW()"#,
    )
    .bind(&account_id)
    .bind(body.profile_hidden)
    .bind(&body.hidden_sections)
    .execute(&state.db)
    .await?;

    Ok(Json(body))
}

/// PUT /api/v4/user/profile/:account_id/veterans/:trained_chara_id/pin
async fn pin_veteran(
    State(state): State<AppState>,
    Path((account_id, trained_chara_id)): Path<(String, i64)>,
    auth_user: AuthenticatedUser,
) -> Result<Json<serde_json::Value>, AppError> {
    let is_owner = check_is_owner(&state.db, Some(&auth_user), &account_id).await?;
    if !is_owner {
        return Err(AppError::Forbidden(
            "You don't have permission to pin veterans on this profile".into(),
        ));
    }

    sqlx::query(
        r#"INSERT INTO veteran_pins (account_id, trained_chara_id)
           VALUES ($1, $2)
           ON CONFLICT DO NOTHING"#,
    )
    .bind(&account_id)
    .bind(trained_chara_id)
    .execute(&state.db)
    .await?;

    Ok(Json(serde_json::json!({ "pinned": true })))
}

/// DELETE /api/v4/user/profile/:account_id/veterans/:trained_chara_id/pin
async fn unpin_veteran(
    State(state): State<AppState>,
    Path((account_id, trained_chara_id)): Path<(String, i64)>,
    auth_user: AuthenticatedUser,
) -> Result<Json<serde_json::Value>, AppError> {
    let is_owner = check_is_owner(&state.db, Some(&auth_user), &account_id).await?;
    if !is_owner {
        return Err(AppError::Forbidden(
            "You don't have permission to unpin veterans on this profile".into(),
        ));
    }

    sqlx::query("DELETE FROM veteran_pins WHERE account_id = $1 AND trained_chara_id = $2")
        .bind(&account_id)
        .bind(trained_chara_id)
        .execute(&state.db)
        .await?;

    Ok(Json(serde_json::json!({ "pinned": false })))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

async fn get_privacy_settings(
    db: &sqlx::PgPool,
    account_id: &str,
) -> Result<ProfileVisibility, AppError> {
    let row = sqlx::query_as::<_, ProfileVisibility>(
        "SELECT profile_hidden, hidden_sections FROM user_privacy_settings WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await?;

    Ok(row.unwrap_or(ProfileVisibility {
        profile_hidden: false,
        hidden_sections: vec![],
    }))
}

async fn check_is_owner(
    db: &sqlx::PgPool,
    auth_user: Option<&AuthenticatedUser>,
    account_id: &str,
) -> Result<bool, AppError> {
    let Some(user) = auth_user else {
        return Ok(false);
    };
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM linked_accounts \
         WHERE user_id = $1 AND account_id = $2 AND verification_status = 'verified')",
    )
    .bind(user.user_id)
    .bind(account_id)
    .fetch_one(db)
    .await?;
    Ok(exists)
}
