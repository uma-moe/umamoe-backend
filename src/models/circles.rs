use crate::models::common::option_naive_datetime_as_utc;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
pub struct Circle {
    pub circle_id: i64,
    pub name: String,
    pub comment: Option<String>,
    pub leader_viewer_id: Option<i64>,
    pub leader_name: Option<String>,
    pub member_count: Option<i32>,
    pub join_style: Option<i32>,
    pub policy: Option<i32>,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub created_at: Option<NaiveDateTime>,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub last_updated: Option<NaiveDateTime>,
    pub monthly_rank: Option<i32>,
    pub monthly_point: Option<i64>,
    pub last_month_rank: Option<i32>,
    pub last_month_point: Option<i64>,
    pub archived: Option<bool>,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub yesterday_updated: Option<NaiveDateTime>,
    pub yesterday_points: Option<i64>,
    pub yesterday_rank: Option<i32>,
    /// Live fan count for top-100 circles, updated every 5 minutes.
    /// `None` for circles outside the live-tracking window.
    pub live_points: Option<i64>,
    /// Live rank for top-100 circles, updated every 5 minutes.
    /// `None` for circles outside the live-tracking window.
    pub live_rank: Option<i32>,
    /// Timestamp of the last live_points update. Used to determine whether live data is fresh
    /// (within 1 day). `None` if live tracking has never run for this circle.
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub last_live_update: Option<NaiveDateTime>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CircleMemberFansMonthly {
    pub id: i32,
    pub circle_id: i64,
    pub viewer_id: i64,
    pub trainer_name: Option<String>,
    pub year: i32,
    pub month: i32,
    pub daily_fans: Vec<i64>,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub last_updated: Option<NaiveDateTime>,
    /// Circle ID the member was in before joining this circle (if they switched mid-month)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_circle_id: Option<i64>,
    /// Name of the previous circle
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_circle_name: Option<String>,
    /// Cumulative fans at start of next month (daily_fans[1] of M+1), for computing last day's gain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_month_start: Option<i64>,
}
