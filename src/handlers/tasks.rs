use axum::{
    extract::{Path, State},
    response::Json,
    routing::post,
    Router,
};
use serde_json::json;
use sqlx::PgPool;
use validator::Validate;

use crate::errors::AppError;
use crate::models::{CreateTaskRequest, TaskResponse, TrainerSubmissionRequest};
use crate::AppState;

/// Reset the tasks id sequence if it falls behind (e.g. after manual inserts).
/// Called once on the first sequence-collision retry so subsequent inserts succeed.
async fn fix_task_sequence(pool: &PgPool) {
    let _ = sqlx::query(
        "SELECT setval(pg_get_serial_sequence('tasks','id'), COALESCE((SELECT MAX(id) FROM tasks),0)+1, false)"
    )
    .execute(pool)
    .await;
}

fn is_task_sequence_collision(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database_error| database_error.constraint())
        == Some("tasks_pkey")
}

/// Insert a task, or return the already-active equivalent task. The workers'
/// partial unique index deliberately makes active task creation idempotent;
/// surfacing that conflict as HTTP 500 causes callers to retry work which is
/// already queued and amplifies load during controller recovery.
async fn insert_or_get_active_task(
    pool: &PgPool,
    task_type: &str,
    task_data: &serde_json::Value,
    priority: i32,
    account_id: Option<&str>,
) -> Result<(crate::models::Task, bool), sqlx::Error> {
    let mut sequence_fixed = false;
    for _ in 0..3 {
        let inserted = sqlx::query_as::<_, crate::models::Task>(
            r#"
            INSERT INTO tasks (task_type, task_data, priority, status, created_at, account_id)
            VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP, $4)
            ON CONFLICT (task_type, task_data)
                WHERE status IN ('pending', 'processing')
            DO NOTHING
            RETURNING id, task_type, task_data, priority, status, created_at, updated_at,
                      worker_id, error_message, account_id
            "#,
        )
        .bind(task_type)
        .bind(task_data)
        .bind(priority)
        .bind(account_id)
        .fetch_optional(pool)
        .await;

        match inserted {
            Ok(Some(task)) => return Ok((task, true)),
            Ok(None) => {
                if let Some(task) = sqlx::query_as::<_, crate::models::Task>(
                    r#"
                    SELECT id, task_type, task_data, priority, status, created_at, updated_at,
                           worker_id, error_message, account_id
                    FROM tasks
                    WHERE task_type = $1
                      AND task_data = $2
                      AND status IN ('pending', 'processing')
                    ORDER BY id ASC
                    LIMIT 1
                    "#,
                )
                .bind(task_type)
                .bind(task_data)
                .fetch_optional(pool)
                .await?
                {
                    return Ok((task, false));
                }
                // The conflicting row may have completed between INSERT and
                // SELECT. Retry so the request still leaves active work behind.
            }
            Err(error) if !sequence_fixed && is_task_sequence_collision(&error) => {
                fix_task_sequence(pool).await;
                sequence_fixed = true;
            }
            Err(error) => return Err(error),
        }
    }
    Err(sqlx::Error::RowNotFound)
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/submit", post(submit_trainer_id))
        .route("/task", post(create_task))
        .route(
            "/report-unavailable/:trainer_id",
            post(report_trainer_unavailable),
        )
}

/// Submit a trainer ID for friend search task
async fn submit_trainer_id(
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

    let (task, _) = insert_or_get_active_task(&state.db, "friend/search", &task_data, 1, None)
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
async fn create_task(
    State(state): State<AppState>,
    Json(payload): Json<CreateTaskRequest>,
) -> Result<Json<TaskResponse>, AppError> {
    payload
        .validate()
        .map_err(|e| AppError::BadRequest(format!("Validation error: {}", e)))?;

    let priority = payload.priority.unwrap_or(0);

    let (task, _) = insert_or_get_active_task(
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
async fn report_trainer_unavailable(
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

    let (_, task_created) =
        insert_or_get_active_task(&state.db, "friend/search", &task_data, 0, None)
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
    fn every_public_task_creation_path_uses_active_task_idempotency() {
        let source = include_str!("tasks.rs");
        let helper = source
            .split("async fn insert_or_get_active_task")
            .nth(1)
            .and_then(|tail| tail.split("pub fn router").next())
            .expect("task insertion helper should be defined before the router");

        assert!(helper.contains("ON CONFLICT (task_type, task_data)"));
        assert!(helper.contains("WHERE status IN ('pending', 'processing')"));
        assert!(helper.contains("DO NOTHING"));
        assert!(helper.contains("is_task_sequence_collision(&error)"));
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production handler source should precede its tests");
        assert_eq!(
            production.matches("insert_or_get_active_task(").count(),
            4,
            "the helper plus all three task creation endpoints must use the idempotent path"
        );
    }
}
