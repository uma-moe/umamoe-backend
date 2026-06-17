use axum::{
    extract::{ConnectInfo, Path, State},
    http::HeaderMap,
    response::Json,
    routing::{get, post},
    Json as AxumJson, Router,
};
use chrono::{TimeZone, Utc};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::net::SocketAddr;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BorrowInteraction {
    View,
    Copy,
}

impl BorrowInteraction {
    fn from_path(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "view" | "views" => Some(Self::View),
            "copy" | "copied" | "copies" => Some(Self::Copy),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Copy => "copy",
        }
    }

    fn bucket_seconds(self) -> i64 {
        match self {
            Self::View => env_i64("BORROW_VIEW_BUCKET_SECONDS", 30 * 60),
            Self::Copy => env_i64("BORROW_COPY_BUCKET_SECONDS", 10 * 60),
        }
        .max(60)
    }
}

#[derive(Debug, Default, Deserialize)]
struct BorrowInteractionPayload {
    inheritance_id: Option<i64>,
    support_card_id: Option<i32>,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Clone, Copy, Debug)]
struct BorrowContext {
    inheritance_id: i64,
    support_card_id: i32,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

impl BorrowContext {
    fn from_payload(payload: Option<BorrowInteractionPayload>) -> Self {
        let payload = payload.unwrap_or_default();
        Self {
            inheritance_id: payload.inheritance_id.unwrap_or(0).max(0),
            support_card_id: payload.support_card_id.unwrap_or(0).max(0),
            support_card_limit_break: payload.support_card_limit_break.filter(|value| *value >= 0),
            support_card_experience: payload.support_card_experience.filter(|value| *value >= 0),
        }
    }
}

#[derive(Debug)]
struct BorrowCounts {
    view_count: i64,
    copy_count: i64,
    theoretical_copy_count: i32,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/submit", post(submit_trainer_id))
        .route("/task", post(create_task))
        .route(
            "/report-unavailable/:trainer_id",
            post(report_trainer_unavailable),
        )
        .route(
            "/borrow/:trainer_id/:interaction",
            post(track_borrow_interaction),
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

async fn track_borrow_interaction(
    State(state): State<AppState>,
    Path((trainer_id, interaction)): Path<(String, String)>,
    headers: HeaderMap,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    payload: Option<AxumJson<BorrowInteractionPayload>>,
) -> Result<Json<serde_json::Value>, AppError> {
    let Some(action) = BorrowInteraction::from_path(&interaction) else {
        return Err(AppError::BadRequest(
            "Invalid borrow interaction type".to_string(),
        ));
    };

    track_borrow(
        &state,
        &trainer_id,
        BorrowContext::from_payload(payload.map(|AxumJson(payload)| payload)),
        action,
        &headers,
        connect_info,
    )
    .await
}

/// Track when a trainer ID is copied (for automatic re-checking).
/// Kept for older frontend callers; internally it uses the deduped borrow-copy path.
async fn track_trainer_copy(
    State(state): State<AppState>,
    Path(trainer_id): Path<String>,
    headers: HeaderMap,
    connect_info: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<serde_json::Value>, AppError> {
    track_borrow(
        &state,
        &trainer_id,
        BorrowContext::from_payload(None),
        BorrowInteraction::Copy,
        &headers,
        connect_info,
    )
    .await
}

async fn track_borrow(
    state: &AppState,
    trainer_id: &str,
    context: BorrowContext,
    action: BorrowInteraction,
    headers: &HeaderMap,
    connect_info: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<serde_json::Value>, AppError> {
    let trainer_id = validate_trainer_id(trainer_id)?;
    let actor_hash = borrow_actor_hash(headers, connect_info.map(|info| info.0));
    let bucket_start = borrow_bucket_start(action.bucket_seconds());

    let inserted_bucket = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO borrow_interaction_buckets (
            trainer_id, inheritance_id, support_card_id,
            interaction_type, actor_hash, bucket_start, event_count, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 1, NOW(), NOW())
        ON CONFLICT DO NOTHING
        RETURNING 1
        "#,
    )
    .bind(trainer_id)
    .bind(context.inheritance_id)
    .bind(context.support_card_id)
    .bind(action.as_str())
    .bind(&actor_hash)
    .bind(bucket_start)
    .fetch_optional(&state.db)
    .await
    .map_err(|error| {
        tracing::error!("Failed to insert borrow interaction bucket: {}", error);
        AppError::DatabaseError("Failed to track borrow interaction".to_string())
    })?;

    let accepted = inserted_bucket.is_some();
    if !accepted {
        sqlx::query(
            r#"
            UPDATE borrow_interaction_buckets
            SET event_count = event_count + 1, updated_at = NOW()
            WHERE trainer_id = $1
              AND inheritance_id = $2
              AND support_card_id = $3
              AND interaction_type = $4
              AND actor_hash = $5
              AND bucket_start = $6
            "#,
        )
        .bind(trainer_id)
        .bind(context.inheritance_id)
        .bind(context.support_card_id)
        .bind(action.as_str())
        .bind(&actor_hash)
        .bind(bucket_start)
        .execute(&state.db)
        .await
        .map_err(|error| {
            tracing::warn!(
                "Failed to update duplicate borrow interaction bucket: {}",
                error
            );
            AppError::DatabaseError("Failed to track borrow interaction".to_string())
        })?;

        let counts = borrow_counts(&state.db, trainer_id, context).await?;
        let total_count = borrow_total_for_action(action, &counts);
        return Ok(Json(json!({
            "success": true,
            "accepted": false,
            "trainer_id": trainer_id,
            "inheritance_id": context.inheritance_id,
            "support_card_id": context.support_card_id,
            "action": action.as_str(),
            "total_count": total_count,
            "view_count": counts.view_count,
            "copy_count": counts.copy_count,
            "theoretical_copy_count": counts.theoretical_copy_count,
            "task_created": false
        })));
    }

    let counts = increment_borrow_total(&state.db, trainer_id, context, action).await?;
    let task_created = if action == BorrowInteraction::Copy {
        sync_legacy_copy_counter(&state.db, trainer_id).await?;
        maybe_create_full_follower_recheck(&state.db, trainer_id, context, &counts).await?
    } else {
        false
    };
    let counts = if action == BorrowInteraction::Copy {
        borrow_counts(&state.db, trainer_id, context).await?
    } else {
        counts
    };
    let total_count = borrow_total_for_action(action, &counts);

    Ok(Json(json!({
        "success": true,
        "accepted": true,
        "trainer_id": trainer_id,
        "inheritance_id": context.inheritance_id,
        "support_card_id": context.support_card_id,
        "action": action.as_str(),
        "total_count": total_count,
        "view_count": counts.view_count,
        "copy_count": counts.copy_count,
        "theoretical_copy_count": counts.theoretical_copy_count,
        "task_created": task_created
    })))
}

fn borrow_total_for_action(action: BorrowInteraction, counts: &BorrowCounts) -> i64 {
    match action {
        BorrowInteraction::View => counts.view_count,
        BorrowInteraction::Copy => counts.copy_count,
    }
}

async fn increment_borrow_total(
    pool: &PgPool,
    trainer_id: &str,
    context: BorrowContext,
    action: BorrowInteraction,
) -> Result<BorrowCounts, AppError> {
    let sql = match action {
        BorrowInteraction::View => {
            r#"
            INSERT INTO borrow_interaction_totals (
                trainer_id, inheritance_id, support_card_id,
                support_card_limit_break, support_card_experience,
                view_count, last_viewed_at, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, 1, NOW(), NOW(), NOW())
            ON CONFLICT (trainer_id, inheritance_id, support_card_id) DO UPDATE SET
                view_count = borrow_interaction_totals.view_count + 1,
                support_card_limit_break = EXCLUDED.support_card_limit_break,
                support_card_experience = EXCLUDED.support_card_experience,
                last_viewed_at = NOW(),
                updated_at = NOW()
            RETURNING view_count, copy_count, theoretical_copy_count
            "#
        }
        BorrowInteraction::Copy => {
            r#"
            INSERT INTO borrow_interaction_totals (
                trainer_id, inheritance_id, support_card_id,
                support_card_limit_break, support_card_experience,
                copy_count, theoretical_copy_count, last_copied_at, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, 1, 1, NOW(), NOW(), NOW())
            ON CONFLICT (trainer_id, inheritance_id, support_card_id) DO UPDATE SET
                copy_count = borrow_interaction_totals.copy_count + 1,
                theoretical_copy_count = borrow_interaction_totals.theoretical_copy_count + 1,
                support_card_limit_break = EXCLUDED.support_card_limit_break,
                support_card_experience = EXCLUDED.support_card_experience,
                last_copied_at = NOW(),
                updated_at = NOW()
            RETURNING view_count, copy_count, theoretical_copy_count
            "#
        }
    };

    sqlx::query_as::<_, (i64, i64, i32)>(sql)
        .bind(trainer_id)
        .bind(context.inheritance_id)
        .bind(context.support_card_id)
        .bind(context.support_card_limit_break)
        .bind(context.support_card_experience)
        .fetch_one(pool)
        .await
        .map(
            |(view_count, copy_count, theoretical_copy_count)| BorrowCounts {
                view_count,
                copy_count,
                theoretical_copy_count,
            },
        )
        .map_err(|error| {
            tracing::error!("Failed to update borrow totals: {}", error);
            AppError::DatabaseError("Failed to track borrow interaction".to_string())
        })
}

async fn borrow_counts(
    pool: &PgPool,
    trainer_id: &str,
    context: BorrowContext,
) -> Result<BorrowCounts, AppError> {
    sqlx::query_as::<_, (i64, i64, i32)>(
        r#"
        SELECT view_count, copy_count, theoretical_copy_count
        FROM borrow_interaction_totals
        WHERE trainer_id = $1
          AND inheritance_id = $2
          AND support_card_id = $3
        "#,
    )
    .bind(trainer_id)
    .bind(context.inheritance_id)
    .bind(context.support_card_id)
    .fetch_optional(pool)
    .await
    .map(|row| {
        row.map(
            |(view_count, copy_count, theoretical_copy_count)| BorrowCounts {
                view_count,
                copy_count,
                theoretical_copy_count,
            },
        )
        .unwrap_or(BorrowCounts {
            view_count: 0,
            copy_count: 0,
            theoretical_copy_count: 0,
        })
    })
    .map_err(|error| {
        tracing::error!("Failed to read borrow totals: {}", error);
        AppError::DatabaseError("Failed to read borrow totals".to_string())
    })
}

async fn trainer_borrow_counts(
    pool: &PgPool,
    trainer_id: &str,
) -> Result<(i64, i64, i64), AppError> {
    sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
        SELECT
            COALESCE(SUM(view_count), 0)::bigint,
            COALESCE(SUM(copy_count), 0)::bigint,
            COALESCE(SUM(theoretical_copy_count), 0)::bigint
        FROM borrow_interaction_totals
        WHERE trainer_id = $1
        "#,
    )
    .bind(trainer_id)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        tracing::error!("Failed to read aggregate borrow totals: {}", error);
        AppError::DatabaseError("Failed to read borrow totals".to_string())
    })
}

async fn sync_legacy_copy_counter(pool: &PgPool, trainer_id: &str) -> Result<(), AppError> {
    let copy_count = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(SUM(copy_count), 0)::bigint FROM borrow_interaction_totals WHERE trainer_id = $1",
    )
    .bind(trainer_id)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        tracing::error!("Failed to sum combo copy counters: {}", error);
        AppError::DatabaseError("Failed to track copy".to_string())
    })?;
    let copy_count = i32::try_from(copy_count).unwrap_or(i32::MAX);
    sqlx::query(
        r#"
        INSERT INTO trainer_copies (trainer_id, copy_count, last_copied)
        VALUES ($1, $2, NOW())
        ON CONFLICT (trainer_id) DO UPDATE SET
            copy_count = EXCLUDED.copy_count,
            last_copied = NOW()
        "#,
    )
    .bind(trainer_id)
    .bind(copy_count)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| {
        tracing::error!("Failed to sync legacy trainer copy counter: {}", error);
        AppError::DatabaseError("Failed to track copy".to_string())
    })
}

async fn maybe_create_full_follower_recheck(
    pool: &PgPool,
    trainer_id: &str,
    context: BorrowContext,
    counts: &BorrowCounts,
) -> Result<bool, AppError> {
    let follower_num = sqlx::query_scalar::<_, Option<i32>>(
        "SELECT follower_num FROM trainer WHERE account_id = $1",
    )
    .bind(trainer_id)
    .fetch_optional(pool)
    .await?
    .flatten()
    .unwrap_or(0);

    let theoretical_follower_num = follower_num.saturating_add(counts.theoretical_copy_count);
    if theoretical_follower_num < 1000 {
        return Ok(false);
    }

    let task_data = json!({
        "id": trainer_id,
        "action": "recheck",
        "reason": "borrow_theoretical_full_followers",
        "inheritance_id": context.inheritance_id,
        "support_card_id": context.support_card_id,
        "copy_count": counts.copy_count,
        "theoretical_copy_count": counts.theoretical_copy_count,
        "last_known_follower_num": follower_num,
        "theoretical_follower_num": theoretical_follower_num
    });

    let insert = || {
        sqlx::query(
            r#"
            INSERT INTO tasks (task_type, task_data, priority, status, created_at)
            SELECT $1, $2, $3, 'pending', CURRENT_TIMESTAMP
            WHERE NOT EXISTS (
                SELECT 1
                FROM tasks
                WHERE task_type IN ('friend/search', 'friend/recheck')
                  AND status IN ('pending', 'processing')
                  AND (task_data->>'id' = $4 OR task_data->>'friend_viewer_id' = $4)
            )
            "#,
        )
        .bind("friend/recheck")
        .bind(&task_data)
        .bind(5i32)
        .bind(trainer_id)
    };

    let task_created = match insert().execute(pool).await {
        Ok(result) => result.rows_affected() > 0,
        Err(_) => {
            fix_task_sequence(pool).await;
            insert()
                .execute(pool)
                .await
                .map(|result| result.rows_affected() > 0)
                .map_err(|error| {
                    tracing::error!("Failed to create borrow recheck task: {}", error);
                    AppError::DatabaseError("Failed to create recheck task".to_string())
                })?
        }
    };

    reset_theoretical_copy_count(pool, trainer_id, context, follower_num, task_created).await?;
    Ok(task_created)
}

async fn reset_theoretical_copy_count(
    pool: &PgPool,
    trainer_id: &str,
    context: BorrowContext,
    follower_num: i32,
    task_created: bool,
) -> Result<(), AppError> {
    sqlx::query(
        r#"
        UPDATE borrow_interaction_totals
        SET theoretical_copy_count = 0,
            last_known_follower_num = $4,
            last_recheck_at = NOW(),
            last_recheck_task_created_at = CASE WHEN $5 THEN NOW() ELSE last_recheck_task_created_at END,
            updated_at = NOW()
        WHERE trainer_id = $1
          AND inheritance_id = $2
          AND support_card_id = $3
        "#,
    )
    .bind(trainer_id)
    .bind(context.inheritance_id)
    .bind(context.support_card_id)
    .bind(follower_num)
    .bind(task_created)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| {
        tracing::error!("Failed to reset theoretical borrow copy counter: {}", error);
        AppError::DatabaseError("Failed to reset borrow recheck counter".to_string())
    })
}

fn validate_trainer_id(trainer_id: &str) -> Result<&str, AppError> {
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

    Ok(trainer_id)
}

fn borrow_bucket_start(bucket_seconds: i64) -> chrono::DateTime<Utc> {
    let now = Utc::now();
    let bucket_timestamp = now.timestamp().div_euclid(bucket_seconds) * bucket_seconds;
    Utc.timestamp_opt(bucket_timestamp, 0)
        .single()
        .unwrap_or(now)
}

fn borrow_actor_hash(headers: &HeaderMap, addr: Option<SocketAddr>) -> String {
    let material = header_str(headers, "X-API-Key")
        .or_else(|| header_str(headers, "X-API-Token"))
        .map(|value| format!("api:{}", value.trim()))
        .or_else(|| {
            header_str(headers, "Authorization")
                .and_then(|value| value.trim().strip_prefix("Bearer "))
                .map(|value| format!("bearer:{}", value.trim()))
        })
        .unwrap_or_else(|| {
            let client_ip = extract_client_ip(headers, addr);
            let user_agent = header_str(headers, "User-Agent").unwrap_or("<none>").trim();
            format!("browser:{}:{}", client_ip, user_agent)
        });

    hex::encode(Sha256::digest(material.as_bytes()))
}

fn extract_client_ip(headers: &HeaderMap, addr: Option<SocketAddr>) -> String {
    if let Some(cf_ip) = header_str(headers, "CF-Connecting-IP") {
        return cf_ip.to_string();
    }

    if let Some(forwarded_for) = header_str(headers, "X-Forwarded-For") {
        if let Some(first_ip) = forwarded_for.split(',').next() {
            return first_ip.trim().to_string();
        }
    }

    if let Some(real_ip) = header_str(headers, "X-Real-IP") {
        return real_ip.to_string();
    }

    addr.map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

/// Get trainer availability status
async fn get_trainer_status(
    State(state): State<AppState>,
    Path(trainer_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let trainer_id = validate_trainer_id(&trainer_id)?;
    let status = sqlx::query_as::<
        _,
        (
            Option<i32>,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        ),
    >(
        r#"
            SELECT
                t.follower_num,
                t.status,
                bit.view_count,
                GREATEST(COALESCE(bit.copy_count, 0), COALESCE(tc.copy_count::bigint, 0)) AS copy_count,
                bit.theoretical_copy_count
            FROM trainer t
            LEFT JOIN (
                SELECT trainer_id,
                       SUM(view_count)::bigint AS view_count,
                       SUM(copy_count)::bigint AS copy_count,
                       SUM(theoretical_copy_count)::bigint AS theoretical_copy_count
                FROM borrow_interaction_totals
                GROUP BY trainer_id
            ) bit ON t.account_id = bit.trainer_id
            LEFT JOIN trainer_copies tc ON t.account_id = tc.trainer_id
            WHERE t.account_id = $1
            "#,
    )
    .bind(trainer_id)
    .fetch_optional(&state.db)
    .await?;

    if let Some((follower_num, status, view_count, copy_count, theoretical_copy_count)) = status {
        Ok(Json(json!({
            "trainer_id": trainer_id,
            "available": follower_num.map(|followers| followers < 1000).unwrap_or(true),
            "follower_num": follower_num,
            "status": status,
            "view_count": view_count.unwrap_or(0),
            "copy_count": copy_count.unwrap_or(0),
            "theoretical_copy_count": theoretical_copy_count.unwrap_or(0)
        })))
    } else {
        let (view_count, copy_count, theoretical_copy_count) =
            trainer_borrow_counts(&state.db, trainer_id).await?;
        Ok(Json(json!({
            "trainer_id": trainer_id,
            "available": true,
            "status": "unknown",
            "view_count": view_count,
            "copy_count": copy_count,
            "theoretical_copy_count": theoretical_copy_count
        })))
    }
}
