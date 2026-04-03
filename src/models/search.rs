use crate::models::common::{deserialize_vec_string_from_query, deserialize_vec_i32_from_query, option_naive_datetime_as_utc};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse<T> {
    pub items: Vec<T>,
    pub total: String,
    pub page: i64,
    pub limit: i64,
    pub total_pages: i64,
}

// V3 Search API models
#[allow(dead_code)] // Some fields are parsed for API compatibility but not yet used
#[derive(Debug, Deserialize)]
pub struct UnifiedSearchParams {
    #[serde(default)]
    pub page: Option<i64>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub search_type: Option<String>, // "inheritance", "support_cards", or "all" (default)

    // Inheritance filtering
    #[serde(default, deserialize_with = "deserialize_vec_i32_from_query")]
    pub main_parent_id: Vec<i32>,
    #[serde(default, deserialize_with = "deserialize_vec_i32_from_query")]
    pub exclude_main_parent_id: Vec<i32>, // Excludes main parent IDs
    #[serde(default, deserialize_with = "deserialize_vec_i32_from_query")]
    pub parent_id: Vec<i32>, // Matches against both left and right parent positions
    #[serde(default, deserialize_with = "deserialize_vec_i32_from_query")]
    pub parent_left_id: Vec<i32>, // Matches left parent only
    #[serde(default, deserialize_with = "deserialize_vec_i32_from_query")]
    pub parent_right_id: Vec<i32>, // Matches right parent only
    #[serde(default, deserialize_with = "deserialize_vec_i32_from_query")]
    pub exclude_parent_id: Vec<i32>, // Excludes from both left and right parent positions
    #[serde(default)]
    pub parent_rank: Option<i32>,
    #[serde(default)]
    pub parent_rarity: Option<i32>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub blue_sparks: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub pink_sparks: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub green_sparks: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub white_sparks: Vec<String>,
    // 9-star spark filtering (searches across all stat types)
    #[serde(default)]
    pub blue_sparks_9star: Option<bool>,
    #[serde(default)]
    pub pink_sparks_9star: Option<bool>,
    #[serde(default)]
    pub green_sparks_9star: Option<bool>,
    // Main parent spark filtering
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub main_parent_blue_sparks: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub main_parent_pink_sparks: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub main_parent_green_sparks: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub main_parent_white_sparks: Vec<String>,
    #[serde(default)]
    pub min_win_count: Option<i32>,
    #[serde(default)]
    pub min_white_count: Option<i32>,

    // Star sum filtering
    #[serde(default)]
    pub min_blue_stars_sum: Option<i32>,
    #[serde(default)]
    pub max_blue_stars_sum: Option<i32>,
    #[serde(default)]
    pub min_pink_stars_sum: Option<i32>,
    #[serde(default)]
    pub max_pink_stars_sum: Option<i32>,
    #[serde(default)]
    pub min_green_stars_sum: Option<i32>,
    #[serde(default)]
    pub max_green_stars_sum: Option<i32>,
    #[serde(default)]
    pub min_white_stars_sum: Option<i32>,
    #[serde(default)]
    pub max_white_stars_sum: Option<i32>,

    // Main inherit filtering
    #[serde(default)]
    pub min_main_blue_factors: Option<i32>,
    #[serde(default)]
    pub min_main_pink_factors: Option<i32>,
    #[serde(default)]
    pub min_main_green_factors: Option<i32>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub main_white_factors: Vec<String>,
    #[serde(default)]
    pub min_main_white_count: Option<i32>,

    // Optional white skill scoring (soft preference, not hard filter)
    // These take skill TYPE IDs only (factor_id), not encoded values
    // Scoring: COUNT(DISTINCT types) * 100 + SUM(levels) - prioritizes more matches over higher levels
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub optional_white_sparks: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_vec_string_from_query")]
    pub optional_main_white_factors: Vec<String>,

    // Support card filtering
    #[serde(default)]
    pub support_card_id: Option<i32>,
    #[serde(default)]
    pub min_limit_break: Option<i32>,
    #[serde(default)]
    pub max_limit_break: Option<i32>,
    #[serde(default)]
    pub min_experience: Option<i32>,

    // Common filtering
    #[serde(default)]
    pub trainer_id: Option<String>, // Direct trainer ID lookup
    #[serde(default)]
    pub trainer_name: Option<String>, // Trainer name search
    #[serde(default)]
    pub max_follower_num: Option<i32>,
    #[serde(default)]
    pub sort_by: Option<String>,
    #[serde(default)]
    pub sort_order: Option<String>,

    // Affinity calculation
    #[serde(default)]
    pub player_chara_id: Option<i32>, // Character ID for affinity score calculation (p0)
    #[serde(default)]
    pub player_chara_id_2: Option<i32>, // Second character ID for dual-parent training (p2)

    // Win saddle filtering
    #[serde(default, deserialize_with = "deserialize_vec_i32_from_query")]
    pub main_win_saddle: Vec<i32>,

    // Desired main character filter
    #[serde(default)]
    pub desired_main_chara_id: Option<i32>, // Filter inheritances where main parent is this character (p0 parent)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnifiedAccountRecord {
    pub account_id: String,
    pub trainer_name: String,
    pub follower_num: Option<i32>,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub last_updated: Option<NaiveDateTime>,
    pub inheritance: Option<super::inheritance::Inheritance>,
    pub support_card: Option<super::support_cards::SupportCard>, // Single best support card, not array
}
