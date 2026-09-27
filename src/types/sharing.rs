use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct InheritanceShareData {
    pub account_id: String,
    pub trainer_name: String,
    pub character_name: String,
    pub parent_left_name: String,
    pub parent_right_name: String,
    pub parent_rank: i32,
    pub parent_rarity: i32,
    pub win_count: i32,
    pub white_count: i32,
    pub blue_factors_summary: String,
    pub pink_factors_summary: String,
    pub green_factors_summary: String,
    pub white_factors_summary: String,
    pub main_factors_summary: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SupportCardShareData {
    pub account_id: String,
    pub trainer_name: String,
    pub card_name: String,
    pub card_rarity: String,
    pub limit_break_count: Option<i32>,
    pub experience: i32,
    pub card_type: String,
}

#[derive(Debug, Deserialize)]
pub struct SharePathParams {
    pub share_type: String,
    pub account_id: String,
}
