use serde::{Deserialize, Serialize};
use sqlx::FromRow;

pub const SUPPORT_CARD_SELECT_COLUMNS: &str =
    "account_id, support_card_id, limit_break_count, experience";

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct SupportCard {
    pub account_id: String,
    pub support_card_id: i32,
    pub limit_break_count: Option<i32>,
    pub experience: i32,
}
