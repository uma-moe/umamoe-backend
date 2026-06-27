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

    let task = {
        let q = || {
            sqlx::query_as::<_, crate::models::Task>(
                r#"
                INSERT INTO tasks (task_type, task_data, priority, status, created_at, account_id)
                VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP, $4)
                RETURNING id, task_type, task_data, priority, status, created_at, updated_at, worker_id, error_message, account_id
                "#,
            )
            .bind("friend/search")
            .bind(&task_data)
            .bind(1i32)
            .bind(None::<String>)
        };
        match q().fetch_one(&state.db).await {
            Ok(t) => t,
            Err(_) => {
                fix_task_sequence(&state.db).await;
                q().fetch_one(&state.db).await.map_err(|e| {
                    tracing::error!("Failed to insert task: {}", e);
                    AppError::DatabaseError("Failed to create task".to_string())
                })?
            }
        }
    };

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

    let task = {
        let q = || {
            sqlx::query_as::<_, crate::models::Task>(
                r#"
                INSERT INTO tasks (task_type, task_data, priority, status, created_at, account_id)
                VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP, $4)
                RETURNING id, task_type, task_data, priority, status, created_at, updated_at, worker_id, error_message, account_id
                "#,
            )
            .bind(&payload.task_type)
            .bind(&payload.task_data)
            .bind(priority)
            .bind(&payload.account_id)
        };
        match q().fetch_one(&state.db).await {
            Ok(t) => t,
            Err(_) => {
                fix_task_sequence(&state.db).await;
                q().fetch_one(&state.db).await.map_err(|e| {
                    tracing::error!("Failed to insert task: {}", e);
                    AppError::DatabaseError("Failed to create task".to_string())
                })?
            }
        }
    };

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

    let q = || {
        sqlx::query(
            r#"
            INSERT INTO tasks (task_type, task_data, priority, status, created_at, account_id)
            VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP, $4)
            "#,
        )
        .bind("friend/search")
        .bind(&task_data)
        .bind(0i32)
        .bind(None::<String>)
    };
    match q().execute(&state.db).await {
        Ok(_) => {}
        Err(_) => {
            fix_task_sequence(&state.db).await;
            q().execute(&state.db).await.map_err(|e| {
                tracing::error!("Failed to create task: {}", e);
                AppError::DatabaseError("Failed to create task".to_string())
            })?;
        }
    }

    Ok(Json(json!({
        "success": true,
        "task_created": true,
        "message": "Trainer scheduled for immediate update"
    })))
}
