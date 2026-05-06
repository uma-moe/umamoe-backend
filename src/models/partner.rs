use crate::models::common::naive_datetime_as_utc;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use validator::{Validate, ValidationError};

fn validate_partner_lookup_id(value: &str) -> Result<(), ValidationError> {
    let trimmed = value.trim();
    let is_valid = !trimmed.is_empty()
        && trimmed.chars().all(|c| c.is_ascii_digit())
        && (trimmed.len() == 9 || trimmed.len() == 12);

    if is_valid {
        Ok(())
    } else {
        let mut err = ValidationError::new("partner_id_format");
        err.message = Some(
            "partner_id must be exactly 9 digits (practice partner) or 12 digits (trainer)".into(),
        );
        Err(err)
    }
}

/// Persistent storage row for a partner inheritance lookup result.
/// One row per (user_id, account_id). Re-lookups upsert the row and bump
/// `updated_at`.
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct PartnerInheritance {
    pub id: i32,
    #[serde(skip_serializing)]
    pub user_id: Uuid,
    pub account_id: String,

    pub main_parent_id: i32,
    pub parent_left_id: i32,
    pub parent_right_id: i32,
    pub parent_rank: i32,
    pub parent_rarity: i32,

    pub blue_sparks: Vec<i32>,
    pub pink_sparks: Vec<i32>,
    pub green_sparks: Vec<i32>,
    pub white_sparks: Vec<i32>,

    pub win_count: i32,
    pub white_count: i32,

    pub main_blue_factors: i32,
    pub main_pink_factors: i32,
    pub main_green_factors: i32,
    pub main_white_factors: Vec<i32>,
    pub main_white_count: i32,

    pub left_blue_factors: i32,
    pub left_pink_factors: i32,
    pub left_green_factors: i32,
    pub left_white_factors: Vec<i32>,
    pub left_white_count: i32,

    pub right_blue_factors: i32,
    pub right_pink_factors: i32,
    pub right_green_factors: i32,
    pub right_white_factors: Vec<i32>,
    pub right_white_count: i32,

    pub main_win_saddles: Vec<i32>,
    pub left_win_saddles: Vec<i32>,
    pub right_win_saddles: Vec<i32>,
    pub race_results: Vec<i32>,

    pub blue_stars_sum: i32,
    pub pink_stars_sum: i32,
    pub green_stars_sum: i32,
    pub white_stars_sum: i32,

    pub affinity_score: Option<i32>,

    pub trainer_name: Option<String>,

    pub label: Option<String>,

    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub created_at: NaiveDateTime,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub updated_at: NaiveDateTime,
}

/// One entry in the anonymous-migration batch.
/// Contains the full inheritance payload (already fetched by the bot previously
/// and cached in the client's localStorage). We upsert directly without
/// scheduling a new bot task.
#[derive(Debug, Deserialize)]
pub struct AnonMigrateEntry {
    pub account_id: String,
    pub main_parent_id: i32,
    pub parent_left_id: i32,
    pub parent_right_id: i32,
    pub parent_rank: i32,
    pub parent_rarity: i32,
    pub blue_sparks: Vec<i32>,
    pub pink_sparks: Vec<i32>,
    pub green_sparks: Vec<i32>,
    pub white_sparks: Vec<i32>,
    pub win_count: i32,
    pub white_count: i32,
    pub main_blue_factors: i32,
    pub main_pink_factors: i32,
    pub main_green_factors: i32,
    pub main_white_factors: Vec<i32>,
    pub main_white_count: i32,
    pub left_blue_factors: i32,
    pub left_pink_factors: i32,
    pub left_green_factors: i32,
    pub left_white_factors: Vec<i32>,
    pub left_white_count: i32,
    pub right_blue_factors: i32,
    pub right_pink_factors: i32,
    pub right_green_factors: i32,
    pub right_white_factors: Vec<i32>,
    pub right_white_count: i32,
    pub main_win_saddles: Vec<i32>,
    pub left_win_saddles: Vec<i32>,
    pub right_win_saddles: Vec<i32>,
    pub race_results: Vec<i32>,
    pub blue_stars_sum: i32,
    pub pink_stars_sum: i32,
    pub green_stars_sum: i32,
    pub white_stars_sum: i32,
    pub affinity_score: Option<i32>,
    pub trainer_name: Option<String>,
    pub label: Option<String>,
}

/// Request body for `POST /api/v4/partner/lookup`.
/// `partner_id` must be numeric and either:
/// - 9 digits (practice partner id), or
/// - 12 digits (trainer/account id).
/// The bot is
/// responsible for resolving either to a full inheritance dump.
#[derive(Debug, Deserialize, Validate)]
pub struct PartnerLookupRequest {
    #[validate(custom(function = "validate_partner_lookup_id"))]
    pub partner_id: String,
    /// Optional human-readable label the user can attach so they can recognise
    /// the entry later in their saved list.
    #[validate(length(max = 64))]
    pub label: Option<String>,
}

/// Slim result returned when a trainer ID is found directly in our DB.
/// Contains trainer info and their latest inheritance — no support card.
#[derive(Debug, Serialize)]
pub struct PartnerDirectResult {
    pub account_id: String,
    pub trainer_name: String,
    pub follower_num: Option<i32>,
    #[serde(serialize_with = "crate::models::common::option_naive_datetime_as_utc::serialize")]
    pub last_updated: Option<chrono::NaiveDateTime>,
    pub inheritance: Option<crate::models::Inheritance>,
}

/// Returned immediately when a lookup task is queued. Frontend opens
/// `GET /api/v4/partner/lookup/{task_id}/stream` for progress + final result.
///
/// When `result` is present the data was served directly from the DB and
/// no bot task was created — `task_id` will be `None`.
#[derive(Debug, Serialize)]
pub struct PartnerLookupResponse {
    /// `None` when the result was served directly from the DB.
    pub task_id: Option<i32>,
    pub status: String,
    /// True when the request was made by an authenticated user and the result
    /// will be persisted in `partner_inheritance` for future fast access.
    pub will_persist: bool,
    /// Populated immediately when the trainer ID already exists in our DB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<PartnerDirectResult>,
}
