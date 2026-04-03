use crate::models::common::{naive_datetime_as_utc, option_naive_datetime_as_utc};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use validator::Validate;

// Task-related models for background job processing
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Task {
    pub id: i32,
    pub task_type: String,
    pub task_data: serde_json::Value,
    pub priority: i32,
    pub status: String,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub created_at: NaiveDateTime,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub updated_at: Option<NaiveDateTime>,
    pub worker_id: Option<String>,
    pub error_message: Option<String>,
    pub account_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct CreateTaskRequest {
    pub task_type: String,
    pub task_data: serde_json::Value,
    #[validate(range(min = 0, max = 10))]
    pub priority: Option<i32>,
    pub account_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrainerSubmissionRequest {
    pub trainer_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskResponse {
    pub id: i32,
    pub task_type: String,
    pub task_data: serde_json::Value,
    pub priority: i32,
    pub status: String,
    pub account_id: Option<String>,
    #[serde(serialize_with = "naive_datetime_as_utc::serialize")]
    pub created_at: NaiveDateTime,
    #[serde(serialize_with = "option_naive_datetime_as_utc::serialize")]
    pub updated_at: Option<NaiveDateTime>,
}
