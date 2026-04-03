use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct ProfileVisibility {
    pub profile_hidden: bool,
    pub hidden_sections: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ProfileResponse {
    pub trainer: TrainerInfo,
    pub circle: Option<CircleInfo>,
    pub circle_history: Vec<CircleHistoryEntry>,
    pub fan_history: FanHistory,
    pub inheritance: Option<super::inheritance::Inheritance>,
    pub support_card: Option<super::support_cards::SupportCard>,
    pub team_stadium: Vec<TeamStadiumMember>,
    pub veterans: Vec<VeteranCharacter>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct VeteranCharacter {
    pub account_id: String,
    pub trained_chara_id: i64,
    pub card_id: Option<i32>,
    pub scenario_id: Option<i32>,
    pub route_id: Option<i32>,
    pub rarity: Option<i32>,
    pub succession_trained_chara_id_1: Option<i64>,
    pub succession_trained_chara_id_2: Option<i64>,
    pub succession_num: Option<i32>,
    pub speed: Option<i32>,
    pub stamina: Option<i32>,
    pub power: Option<i32>,
    pub wiz: Option<i32>,
    pub guts: Option<i32>,
    pub fans: Option<i32>,
    pub rank_score: Option<i64>,
    pub rank: Option<i32>,
    pub chara_grade: Option<i32>,
    pub talent_level: Option<i32>,
    pub running_style: Option<i32>,
    pub race_cloth_id: Option<i32>,
    pub nickname_id: Option<i32>,
    pub wins: Option<i32>,
    pub proper_ground_turf: Option<i32>,
    pub proper_ground_dirt: Option<i32>,
    pub proper_running_style_nige: Option<i32>,
    pub proper_running_style_senko: Option<i32>,
    pub proper_running_style_sashi: Option<i32>,
    pub proper_running_style_oikomi: Option<i32>,
    pub proper_distance_short: Option<i32>,
    pub proper_distance_mile: Option<i32>,
    pub proper_distance_middle: Option<i32>,
    pub proper_distance_long: Option<i32>,
    pub skill_array: serde_json::Value,
    pub support_card_list: serde_json::Value,
    pub factor_info_array: serde_json::Value,
    pub win_saddle_id_array: serde_json::Value,
    pub succession_chara_array: serde_json::Value,
    pub register_time: Option<chrono::DateTime<chrono::Utc>>,
    pub create_time: Option<chrono::DateTime<chrono::Utc>>,
    pub ingested_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub is_pinned: bool,
}

#[derive(Debug, Serialize, FromRow)]
pub struct TrainerInfo {
    pub account_id: String,
    pub name: String,
    pub follower_num: Option<i32>,
    pub best_team_class: Option<i32>,
    pub team_class: Option<i32>,
    pub team_evaluation_point: Option<i64>,
    pub leader_chara_dress_id: Option<i32>,
    pub rank_score: Option<i64>,
    pub release_num_info: Option<serde_json::Value>,
    pub trophy_num_info: Option<serde_json::Value>,
    pub team_stadium_user: Option<serde_json::Value>,
    pub own_follow_num: Option<i32>,
    pub enable_circle_scout: Option<i32>,
    pub comment: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CircleInfo {
    pub circle_id: i64,
    pub name: String,
    pub member_count: Option<i32>,
    pub monthly_rank: Option<i32>,
    pub monthly_point: Option<i64>,
    pub last_month_rank: Option<i32>,
    pub last_month_point: Option<i64>,
    pub live_points: Option<i64>,
    pub live_rank: Option<i32>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct CircleHistoryEntry {
    pub year: i32,
    pub month: i32,
    pub circle_id: i64,
    pub circle_name: Option<String>,
    pub circle_rank: Option<i32>,
    pub circle_points: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct FanHistory {
    pub monthly: Vec<super::rankings::UserFanRankingMonthly>,
    pub rolling: Option<super::rankings::UserFanRankingGains>,
    pub alltime: Option<super::rankings::UserFanRankingAlltime>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct TeamStadiumMember {
    pub id: i32,
    pub trainer_id: String,
    pub distance_type: Option<i32>,
    pub member_id: Option<i32>,
    pub trained_chara_id: Option<i32>,
    pub running_style: Option<i32>,
    pub card_id: Option<i32>,
    pub speed: Option<i32>,
    pub power: Option<i32>,
    pub stamina: Option<i32>,
    pub wiz: Option<i32>,
    pub guts: Option<i32>,
    pub fans: Option<i32>,
    pub rank_score: Option<i64>,
    pub skills: Option<Vec<i32>>,
    pub creation_time: Option<String>,
    pub scenario_id: Option<i32>,
    pub factors: Option<Vec<i32>>,
    pub support_cards: Option<Vec<i32>>,
    pub proper_ground_turf: Option<i32>,
    pub proper_ground_dirt: Option<i32>,
    pub proper_running_style_nige: Option<i32>,
    pub proper_running_style_senko: Option<i32>,
    pub proper_running_style_sashi: Option<i32>,
    pub proper_running_style_oikomi: Option<i32>,
    pub proper_distance_short: Option<i32>,
    pub proper_distance_mile: Option<i32>,
    pub proper_distance_middle: Option<i32>,
    pub proper_distance_long: Option<i32>,
    pub rarity: Option<i32>,
    pub talent_level: Option<i32>,
    pub team_rating: Option<i32>,
}
