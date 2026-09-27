use serde::{Deserialize, Serialize};
use sqlx::FromRow;

pub const INHERITANCE_SELECT_COLUMNS: &str = r#"
    inheritance_id,
    account_id,
    main_parent_id,
    parent_left_id,
    parent_right_id,
    parent_rank,
    parent_rarity,
    blue_sparks,
    pink_sparks,
    green_sparks,
    white_sparks,
    win_count,
    white_count,
    main_blue_factors,
    main_pink_factors,
    main_green_factors,
    main_white_factors,
    main_white_count,
    left_blue_factors,
    left_pink_factors,
    left_green_factors,
    left_white_factors,
    left_white_count,
    right_blue_factors,
    right_pink_factors,
    right_green_factors,
    right_white_factors,
    right_white_count,
    main_win_saddles,
    left_win_saddles,
    right_win_saddles,
    race_results,
    blue_stars_sum,
    pink_stars_sum,
    green_stars_sum,
    white_stars_sum
"#;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Inheritance {
    pub inheritance_id: i32,
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
    #[sqlx(default)]
    #[serde(default)]
    pub left_blue_factors: i32,
    #[sqlx(default)]
    #[serde(default)]
    pub left_pink_factors: i32,
    #[sqlx(default)]
    #[serde(default)]
    pub left_green_factors: i32,
    #[sqlx(default)]
    #[serde(default)]
    pub left_white_factors: Vec<i32>,
    #[sqlx(default)]
    #[serde(default)]
    pub left_white_count: i32,

    #[sqlx(default)]
    #[serde(default)]
    pub right_blue_factors: i32,
    #[sqlx(default)]
    #[serde(default)]
    pub right_pink_factors: i32,
    #[sqlx(default)]
    #[serde(default)]
    pub right_green_factors: i32,
    #[sqlx(default)]
    #[serde(default)]
    pub right_white_factors: Vec<i32>,
    #[sqlx(default)]
    #[serde(default)]
    pub right_white_count: i32,

    #[sqlx(default)]
    #[serde(default)]
    pub main_win_saddles: Vec<i32>,
    #[sqlx(default)]
    #[serde(default)]
    pub left_win_saddles: Vec<i32>,
    #[sqlx(default)]
    #[serde(default)]
    pub right_win_saddles: Vec<i32>,
    #[sqlx(default)]
    #[serde(default)]
    pub race_results: Vec<i32>,

    #[sqlx(default)]
    pub blue_stars_sum: i32,
    #[sqlx(default)]
    pub pink_stars_sum: i32,
    #[sqlx(default)]
    pub green_stars_sum: i32,
    #[sqlx(default)]
    pub white_stars_sum: i32,
    #[sqlx(default)]
    pub affinity_score: Option<i32>,
}
