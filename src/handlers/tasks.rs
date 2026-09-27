use crate::tasks::insertOrGetActiveTask;
use axum::{
    extract::{Path, State},
    response::Json,
    routing::post,
    Router,
};
use serde_json::json;
use validator::Validate;

use crate::errors::AppError;
pub use crate::types::{CreateTaskRequest, TaskResponse, TrainerSubmissionRequest};
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/submit", post(submitTrainerId))
        .route("/task", post(createTask))
        .route(
            "/report-unavailable/:trainer_id",
            post(reportTrainerUnavailable),
        )
}

/// Submit a trainer ID for friend search task
async fn submitTrainerId(
    State(state): State<AppState>,
    Json(payload): Json<TrainerSubmissionRequest>,
) -> Result<Json<TaskResponse>, AppError> {
    // Validate trainer ID format (9-12 digits)
    let trainer_id = payload.trainer_id.trim();
    if trainer_id.is_empty()
        || !trainer_id.chars().all(|c| c.is_ascii_digit())
        || trainer_id.len() < 9
        || trainer_id.len() > 12
    {
        return Err(AppError::BadRequest(
            "Invalid trainer ID format. Must be 9-12 digits.".to_string(),
        ));
    }

    let task_data = json!({
        "id": trainer_id,
        "action": "search"
    });

    let (task, _) = insertOrGetActiveTask(&state.db, "friend/search", &task_data, 1, None)
        .await
        .map_err(|e| {
            tracing::error!("Failed to insert or find active task: {}", e);
            AppError::DatabaseError("Failed to create task".to_string())
        })?;

    Ok(Json(TaskResponse {
        id: task.id,
        task_type: task.task_type,
        task_data: task.task_data,
        priority: task.priority,
        status: task.status,
        account_id: task.account_id,
        created_at: task.created_at,
        updated_at: task.updated_at,
    }))
}

/// Generic task creation endpoint
async fn createTask(
    State(state): State<AppState>,
    Json(payload): Json<CreateTaskRequest>,
) -> Result<Json<TaskResponse>, AppError> {
    payload
        .validate()
        .map_err(|e| AppError::BadRequest(format!("Validation error: {}", e)))?;

    let priority = payload.priority.unwrap_or(0);

    let (task, _) = insertOrGetActiveTask(
        &state.db,
        &payload.task_type,
        &payload.task_data,
        priority,
        payload.account_id.as_deref(),
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to insert or find active task: {}", e);
        AppError::DatabaseError("Failed to create task".to_string())
    })?;

    Ok(Json(TaskResponse {
        id: task.id,
        task_type: task.task_type,
        task_data: task.task_data,
        priority: task.priority,
        status: task.status,
        account_id: task.account_id,
        created_at: task.created_at,
        updated_at: task.updated_at,
    }))
}

/// Report a trainer as unavailable (friend list full) - triggers immediate update
async fn reportTrainerUnavailable(
    State(state): State<AppState>,
    Path(trainer_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let trainer_id = trainer_id.trim();
    if trainer_id.is_empty()
        || !trainer_id.chars().all(|c| c.is_ascii_digit())
        || trainer_id.len() < 9
        || trainer_id.len() > 12
    {
        return Err(AppError::BadRequest(
            "Invalid trainer ID format".to_string(),
        ));
    }

    let task_data = json!({
        "id": trainer_id
    });

    let (_, task_created) = insertOrGetActiveTask(&state.db, "friend/search", &task_data, 0, None)
        .await
        .map_err(|e| {
            tracing::error!("Failed to insert or find active task: {}", e);
            AppError::DatabaseError("Failed to create task".to_string())
        })?;

    Ok(Json(json!({
        "success": true,
        "task_created": task_created,
        "message": "Trainer scheduled for immediate update"
    })))
}

#[cfg(test)]
mod tests {
    #[test]
    fn everyPublicTaskCreationPathUsesActiveTaskIdempotency() {
        let source = include_str!("tasks.rs");
        let helper = include_str!("../tasks.rs");

        assert!(helper.contains("ON CONFLICT (task_type, task_data)"));
        assert!(helper.contains("WHERE status IN ('pending', 'processing')"));
        assert!(helper.contains("DO NOTHING"));
        assert!(helper.contains("isTaskSequenceCollision(&error)"));
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production handler source should precede its tests");
        assert_eq!(
            production.matches("insertOrGetActiveTask(").count(),
            3,
            "all three task creation endpoints must use the idempotent path"
        );
    }
}
