use axum::{
    extract::{Path, State},
    response::Json,
    routing::{get, post},
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
        .route("/track-copy/:trainer_id", post(track_trainer_copy))
        .route("/trainer/:trainer_id/status", get(get_trainer_status))
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

    // Create task data
    let task_data = json!({
        "id": trainer_id,
        "action": "search"
    });

    // Insert task into database (retry once if the id sequence is out of sync)
    let task = {
        let q = || {
            sqlx::query_as::<_, crate::models::Task>(
                r#"
                INSERT INTO tasks (task_type, task_data, priority, status, created_at, account_id)
                VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP, $4)
                RETURNING id, task_type, task_data, priority, status, created_at, updated_at, worker_id, error_message, account_id
                "#
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
    // Validate the request
    payload
        .validate()
        .map_err(|e| AppError::BadRequest(format!("Validation error: {}", e)))?;

    let priority = payload.priority.unwrap_or(0);

    // Insert task into database (retry once if the id sequence is out of sync)
    let task = {
        let q = || {
            sqlx::query_as::<_, crate::models::Task>(
                r#"
                INSERT INTO tasks (task_type, task_data, priority, status, created_at, account_id)
                VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP, $4)
                RETURNING id, task_type, task_data, priority, status, created_at, updated_at, worker_id, error_message, account_id
                "#
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
    // Validate trainer ID
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

    // Create task data for friend search (force update)
    let task_data = json!({
        "id": trainer_id
    });

    // Create high-priority friend search task (retry once if the id sequence is out of sync)
    let q = || {
        sqlx::query(
            r#"
            INSERT INTO tasks (task_type, task_data, priority, status, created_at, account_id)
            VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP, $4)
            "#,
        )
        .bind("friend/search")
        .bind(&task_data)
        .bind(0i32) // High priority for forced updates
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

/// Track when a trainer ID is copied (for automatic re-checking)
async fn track_trainer_copy(
    State(state): State<AppState>,
    Path(trainer_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    // Validate trainer ID
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

    // Increment copy count
    let copy_count = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO trainer_copies (trainer_id, copy_count, last_copied)
        VALUES ($1, 1, CURRENT_TIMESTAMP)
        ON CONFLICT (trainer_id) 
        DO UPDATE SET 
            copy_count = trainer_copies.copy_count + 1,
            last_copied = CURRENT_TIMESTAMP
        RETURNING copy_count
        "#,
    )
    .bind(trainer_id)
    .fetch_one(&state.db)
    .await
    .map_err(|e| {
        tracing::error!("Failed to update copy count: {}", e);
        AppError::DatabaseError("Failed to track copy".to_string())
    })?;

    // If copy count reaches threshold (e.g., 10), create a re-check task
    if copy_count >= 10 && copy_count % 10 == 0 {
        // Every 10 copies
        // Check if trainer was previously marked as unavailable
        let was_unavailable = sqlx::query_scalar::<_, bool>(
            "SELECT follower_num > 1000 FROM trainer WHERE account_id = $1",
        )
        .bind(trainer_id)
        .fetch_optional(&state.db)
        .await?
        .unwrap_or(false);

        if was_unavailable {
            let task_data = json!({
                "id": trainer_id,
                "action": "recheck",
                "reason": "high_copy_count",
                "copy_count": copy_count
            });

            // Create medium-priority recheck task
            sqlx::query(
                r#"
                INSERT INTO tasks (task_type, task_data, priority, status, created_at)
                VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP)
                "#,
            )
            .bind("friend/recheck")
            .bind(&task_data)
            .bind(5i32) // Medium priority for rechecks
            .execute(&state.db)
            .await?;
        }
    }

    Ok(Json(json!({
        "success": true,
        "copy_count": copy_count,
        "task_created": copy_count >= 10 && copy_count % 10 == 0
    })))
}

/// Get trainer availability status
async fn get_trainer_status(
    State(state): State<AppState>,
    Path(trainer_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let status = sqlx::query_as::<_, (Option<i32>, Option<String>, Option<i32>)>(
        r#"
        SELECT 
            t.follower_num,
            t.status,
            tc.copy_count
        FROM trainer t
        LEFT JOIN trainer_copies tc ON t.account_id = tc.trainer_id
        WHERE t.account_id = $1
        "#,
    )
    .bind(&trainer_id)
    .fetch_optional(&state.db)
    .await?;

    if let Some((follower_num, status, copy_count)) = status {
        Ok(Json(json!({
            "trainer_id": trainer_id,
            "available": follower_num.unwrap_or(0) <= 1000,
            "follower_num": follower_num,
            "status": status,
            "copy_count": copy_count.unwrap_or(0)
        })))
    } else {
        Ok(Json(json!({
            "trainer_id": trainer_id,
            "available": true,
            "status": "unknown",
            "copy_count": 0
        })))
    }
}
