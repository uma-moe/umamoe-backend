use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::Json,
    routing::{get, post},
    Json as AxumJson, Router,
};
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};

use crate::borrow_key;
use crate::errors::AppError;
use crate::middleware::turnstile::require_turnstile_browser_proof;
use crate::AppState;

const MAX_BORROW_VIEW_BATCH_SIZE: usize = 100;

async fn fix_task_sequence(pool: &PgPool) {
    let _ = sqlx::query(
        "SELECT setval(pg_get_serial_sequence('tasks','id'), COALESCE((SELECT MAX(id) FROM tasks),0)+1, false)",
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
}

#[derive(Debug, Default, Deserialize)]
struct BorrowInteractionPayload {
    borrow_key: Option<String>,
    inheritance_id: Option<i64>,
    support_card_id: Option<i32>,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct BorrowViewBatchPayload {
    views: Vec<BorrowViewPayload>,
}

#[derive(Debug, Deserialize)]
struct BorrowViewPayload {
    trainer_id: String,
    borrow_key: Option<String>,
    inheritance_id: Option<i64>,
    support_card_id: Option<i32>,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Debug, Serialize)]
struct BorrowViewBatchRow {
    trainer_id: String,
    borrow_key: String,
    inheritance_id: i64,
    support_card_id: i32,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Clone, Debug)]
struct BorrowContext {
    borrow_key: String,
    inheritance_id: i64,
    support_card_id: i32,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

impl BorrowContext {
    fn from_payload(payload: Option<BorrowInteractionPayload>) -> Self {
        let payload = payload.unwrap_or_default();
        Self::from_parts(
            payload.borrow_key,
            payload.inheritance_id,
            payload.support_card_id,
            payload.support_card_limit_break,
            payload.support_card_experience,
        )
    }

    fn from_parts(
        borrow_key: Option<String>,
        inheritance_id: Option<i64>,
        support_card_id: Option<i32>,
        support_card_limit_break: Option<i32>,
        support_card_experience: Option<i32>,
    ) -> Self {
        let inheritance_id = inheritance_id.unwrap_or(0).max(0);
        let support_card_id = support_card_id.unwrap_or(0).max(0);
        Self {
            borrow_key: borrow_key::normalize_borrow_key(
                borrow_key.as_deref(),
                inheritance_id,
                support_card_id,
            ),
            inheritance_id,
            support_card_id,
            support_card_limit_break: support_card_limit_break.filter(|value| *value >= 0),
            support_card_experience: support_card_experience.filter(|value| *value >= 0),
        }
    }
}

#[derive(Debug)]
struct BorrowCounts {
    view_count: i64,
    copy_count: i64,
    theoretical_copy_count: i32,
}

#[derive(Clone, Debug)]
struct BorrowActor {
    hash: String,
    bucket_start: chrono::DateTime<Utc>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/views", post(track_borrow_view_batch))
        .route("/track-copy/:trainer_id", post(track_trainer_copy))
        .route("/trainer/:trainer_id/status", get(get_trainer_status))
        .route("/:trainer_id/:interaction", post(track_borrow_interaction))
}

async fn track_borrow_view_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumJson(payload): AxumJson<BorrowViewBatchPayload>,
) -> Result<Json<serde_json::Value>, AppError> {
    if payload.views.len() > MAX_BORROW_VIEW_BATCH_SIZE {
        return Err(AppError::BadRequest(format!(
            "Borrow view batch is too large. Maximum is {} views.",
            MAX_BORROW_VIEW_BATCH_SIZE
        )));
    }

    if payload.views.is_empty() {
        return Ok(Json(json!({
            "success": true,
            "submitted_count": 0,
            "received_count": 0,
            "accepted_count": 0,
            "duplicate_count": 0
        })));
    }

    let mut rows = Vec::with_capacity(payload.views.len());
    for view in payload.views {
        let trainer_id = validate_trainer_id(&view.trainer_id)?.to_string();
        let context = BorrowContext::from_parts(
            view.borrow_key,
            view.inheritance_id,
            view.support_card_id,
            view.support_card_limit_break,
            view.support_card_experience,
        );
        rows.push(BorrowViewBatchRow {
            trainer_id,
            borrow_key: context.borrow_key,
            inheritance_id: context.inheritance_id,
            support_card_id: context.support_card_id,
            support_card_limit_break: context.support_card_limit_break,
            support_card_experience: context.support_card_experience,
        });
    }

    let submitted_count = rows.len();
    let rows_json = serde_json::to_value(&rows).map_err(|error| {
        tracing::error!("Failed to encode borrow view batch: {}", error);
        AppError::DatabaseError("Failed to track borrow views".to_string())
    })?;
    let actor = require_borrow_browser_actor(&state, &headers).await?;

    let (received_count, accepted_count, duplicate_count) = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
            WITH incoming_raw AS (
                SELECT *
                FROM jsonb_to_recordset($1::jsonb) AS item(
                    trainer_id TEXT,
                    borrow_key TEXT,
                    inheritance_id BIGINT,
                    support_card_id INTEGER,
                    support_card_limit_break INTEGER,
                    support_card_experience INTEGER
                )
            ),
            incoming AS (
                SELECT DISTINCT ON (trainer_id, borrow_key)
                    trainer_id,
                    borrow_key,
                    inheritance_id,
                    support_card_id,
                    support_card_limit_break,
                    support_card_experience
                FROM incoming_raw
                ORDER BY trainer_id, borrow_key
            ),
            updated_existing AS (
                UPDATE borrow_interaction_buckets_v2 bucket
                SET event_count = bucket.event_count + 1,
                    updated_at = NOW()
                FROM incoming
                WHERE bucket.trainer_id = incoming.trainer_id
                  AND bucket.borrow_key = incoming.borrow_key
                  AND bucket.interaction_type = 'view'
                  AND bucket.actor_hash = $2
                  AND bucket.bucket_start = $3
                RETURNING bucket.trainer_id
            ),
            inserted AS (
                INSERT INTO borrow_interaction_buckets_v2 (
                    trainer_id, borrow_key, inheritance_id, support_card_id,
                    interaction_type, actor_hash, bucket_start, event_count, created_at, updated_at
                )
                SELECT trainer_id, borrow_key, inheritance_id, support_card_id, 'view', $2, $3, 1, NOW(), NOW()
                FROM incoming
                ON CONFLICT DO NOTHING
                RETURNING trainer_id, borrow_key
            ),
            updated_totals AS (
                INSERT INTO borrow_interaction_totals_v2 (
                    trainer_id, borrow_key, inheritance_id, support_card_id,
                    support_card_limit_break, support_card_experience,
                    view_count, last_viewed_at, created_at, updated_at
                )
                SELECT
                    incoming.trainer_id,
                    incoming.borrow_key,
                    incoming.inheritance_id,
                    incoming.support_card_id,
                    incoming.support_card_limit_break,
                    incoming.support_card_experience,
                    1,
                    NOW(),
                    NOW(),
                    NOW()
                FROM inserted
                JOIN incoming USING (trainer_id, borrow_key)
                ON CONFLICT (trainer_id, borrow_key) DO UPDATE SET
                    view_count = borrow_interaction_totals_v2.view_count + 1,
                    inheritance_id = EXCLUDED.inheritance_id,
                    support_card_id = EXCLUDED.support_card_id,
                    support_card_limit_break = EXCLUDED.support_card_limit_break,
                    support_card_experience = EXCLUDED.support_card_experience,
                    last_viewed_at = NOW(),
                    updated_at = NOW()
                RETURNING trainer_id, borrow_key
            ),
            updated_trends AS (
                INSERT INTO borrow_interaction_trends (
                    trend_date, trainer_id, borrow_key, inheritance_id, support_card_id,
                    support_card_limit_break, support_card_experience,
                    view_count, created_at, updated_at
                )
                SELECT
                    CURRENT_DATE,
                    incoming.trainer_id,
                    incoming.borrow_key,
                    incoming.inheritance_id,
                    incoming.support_card_id,
                    incoming.support_card_limit_break,
                    incoming.support_card_experience,
                    1,
                    NOW(),
                    NOW()
                FROM updated_totals
                JOIN incoming USING (trainer_id, borrow_key)
                ON CONFLICT (trend_date, trainer_id, borrow_key) DO UPDATE SET
                    view_count = borrow_interaction_trends.view_count + 1,
                    inheritance_id = EXCLUDED.inheritance_id,
                    support_card_id = EXCLUDED.support_card_id,
                    support_card_limit_break = EXCLUDED.support_card_limit_break,
                    support_card_experience = EXCLUDED.support_card_experience,
                    updated_at = NOW()
                RETURNING trainer_id
            )
            SELECT
                (SELECT COUNT(*) FROM incoming)::bigint AS received_count,
                (SELECT COUNT(*) FROM updated_trends)::bigint AS accepted_count,
                (SELECT COUNT(*) FROM updated_existing)::bigint AS duplicate_count
            "#,
    )
    .bind(rows_json)
    .bind(&actor.hash)
    .bind(actor.bucket_start)
    .fetch_one(&state.db)
    .await
    .map_err(|error| {
        tracing::error!("Failed to track borrow view batch: {}", error);
        AppError::DatabaseError("Failed to track borrow views".to_string())
    })?;

    Ok(Json(json!({
        "success": true,
        "submitted_count": submitted_count,
        "received_count": received_count,
        "accepted_count": accepted_count,
        "duplicate_count": duplicate_count
    })))
}

async fn track_borrow_interaction(
    State(state): State<AppState>,
    Path((trainer_id, interaction)): Path<(String, String)>,
    headers: HeaderMap,
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
    )
    .await
}

async fn track_trainer_copy(
    State(state): State<AppState>,
    Path(trainer_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    track_borrow(
        &state,
        &trainer_id,
        BorrowContext::from_payload(None),
        BorrowInteraction::Copy,
        &headers,
    )
    .await
}

async fn track_borrow(
    state: &AppState,
    trainer_id: &str,
    context: BorrowContext,
    action: BorrowInteraction,
    headers: &HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let trainer_id = validate_trainer_id(trainer_id)?;
    let actor = require_borrow_browser_actor(state, headers).await?;

    let inserted_bucket = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO borrow_interaction_buckets_v2 (
            trainer_id, borrow_key, inheritance_id, support_card_id,
            interaction_type, actor_hash, bucket_start, event_count, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 1, NOW(), NOW())
        ON CONFLICT DO NOTHING
        RETURNING 1
        "#,
    )
    .bind(trainer_id)
    .bind(&context.borrow_key)
    .bind(context.inheritance_id)
    .bind(context.support_card_id)
    .bind(action.as_str())
    .bind(&actor.hash)
    .bind(actor.bucket_start)
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
            UPDATE borrow_interaction_buckets_v2
            SET event_count = event_count + 1, updated_at = NOW()
            WHERE trainer_id = $1
              AND borrow_key = $2
              AND interaction_type = $3
              AND actor_hash = $4
              AND bucket_start = $5
            "#,
        )
        .bind(trainer_id)
        .bind(&context.borrow_key)
        .bind(action.as_str())
        .bind(&actor.hash)
        .bind(actor.bucket_start)
        .execute(&state.db)
        .await
        .map_err(|error| {
            tracing::warn!(
                "Failed to update duplicate borrow interaction bucket: {}",
                error
            );
            AppError::DatabaseError("Failed to track borrow interaction".to_string())
        })?;

        let counts = borrow_counts(&state.db, trainer_id, &context).await?;
        let total_count = borrow_total_for_action(action, &counts);
        return Ok(Json(json!({
            "success": true,
            "accepted": false,
            "trainer_id": trainer_id,
            "borrow_key": &context.borrow_key,
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

    let counts = increment_borrow_total(&state.db, trainer_id, &context, action).await?;
    let task_created = if action == BorrowInteraction::Copy {
        sync_legacy_copy_counter(&state.db, trainer_id).await?;
        maybe_create_full_follower_recheck(&state.db, trainer_id, &context, &counts).await?
    } else {
        false
    };
    let counts = if action == BorrowInteraction::Copy {
        borrow_counts(&state.db, trainer_id, &context).await?
    } else {
        counts
    };
    let total_count = borrow_total_for_action(action, &counts);

    Ok(Json(json!({
        "success": true,
        "accepted": true,
        "trainer_id": trainer_id,
        "borrow_key": &context.borrow_key,
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
    context: &BorrowContext,
    action: BorrowInteraction,
) -> Result<BorrowCounts, AppError> {
    let sql = match action {
        BorrowInteraction::View => {
            r#"
            INSERT INTO borrow_interaction_totals_v2 (
                trainer_id, borrow_key, inheritance_id, support_card_id,
                support_card_limit_break, support_card_experience,
                view_count, last_viewed_at, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 1, NOW(), NOW(), NOW())
            ON CONFLICT (trainer_id, borrow_key) DO UPDATE SET
                view_count = borrow_interaction_totals_v2.view_count + 1,
                inheritance_id = EXCLUDED.inheritance_id,
                support_card_id = EXCLUDED.support_card_id,
                support_card_limit_break = EXCLUDED.support_card_limit_break,
                support_card_experience = EXCLUDED.support_card_experience,
                last_viewed_at = NOW(),
                updated_at = NOW()
            RETURNING view_count, copy_count, theoretical_copy_count
            "#
        }
        BorrowInteraction::Copy => {
            r#"
            INSERT INTO borrow_interaction_totals_v2 (
                trainer_id, borrow_key, inheritance_id, support_card_id,
                support_card_limit_break, support_card_experience,
                copy_count, theoretical_copy_count, last_copied_at, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 1, 1, NOW(), NOW(), NOW())
            ON CONFLICT (trainer_id, borrow_key) DO UPDATE SET
                copy_count = borrow_interaction_totals_v2.copy_count + 1,
                theoretical_copy_count = borrow_interaction_totals_v2.theoretical_copy_count + 1,
                inheritance_id = EXCLUDED.inheritance_id,
                support_card_id = EXCLUDED.support_card_id,
                support_card_limit_break = EXCLUDED.support_card_limit_break,
                support_card_experience = EXCLUDED.support_card_experience,
                last_copied_at = NOW(),
                updated_at = NOW()
            RETURNING view_count, copy_count, theoretical_copy_count
            "#
        }
    };

    let mut tx = pool.begin().await.map_err(|error| {
        tracing::error!("Failed to start borrow total transaction: {}", error);
        AppError::DatabaseError("Failed to track borrow interaction".to_string())
    })?;

    let counts = sqlx::query_as::<_, (i64, i64, i32)>(sql)
        .bind(trainer_id)
        .bind(&context.borrow_key)
        .bind(context.inheritance_id)
        .bind(context.support_card_id)
        .bind(context.support_card_limit_break)
        .bind(context.support_card_experience)
        .fetch_one(&mut *tx)
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
        })?;

    increment_borrow_trend(&mut tx, trainer_id, context, action).await?;

    tx.commit().await.map_err(|error| {
        tracing::error!("Failed to commit borrow total transaction: {}", error);
        AppError::DatabaseError("Failed to track borrow interaction".to_string())
    })?;

    Ok(counts)
}

async fn increment_borrow_trend(
    tx: &mut Transaction<'_, Postgres>,
    trainer_id: &str,
    context: &BorrowContext,
    action: BorrowInteraction,
) -> Result<(), AppError> {
    let (view_increment, copy_increment) = match action {
        BorrowInteraction::View => (1i64, 0i64),
        BorrowInteraction::Copy => (0i64, 1i64),
    };

    sqlx::query(
        r#"
        INSERT INTO borrow_interaction_trends (
            trend_date, trainer_id, borrow_key, inheritance_id, support_card_id,
            support_card_limit_break, support_card_experience,
            view_count, copy_count, created_at, updated_at
        )
        VALUES (CURRENT_DATE, $1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
        ON CONFLICT (trend_date, trainer_id, borrow_key) DO UPDATE SET
            view_count = borrow_interaction_trends.view_count + EXCLUDED.view_count,
            copy_count = borrow_interaction_trends.copy_count + EXCLUDED.copy_count,
            inheritance_id = EXCLUDED.inheritance_id,
            support_card_id = EXCLUDED.support_card_id,
            support_card_limit_break = EXCLUDED.support_card_limit_break,
            support_card_experience = EXCLUDED.support_card_experience,
            updated_at = NOW()
        "#,
    )
    .bind(trainer_id)
    .bind(&context.borrow_key)
    .bind(context.inheritance_id)
    .bind(context.support_card_id)
    .bind(context.support_card_limit_break)
    .bind(context.support_card_experience)
    .bind(view_increment)
    .bind(copy_increment)
    .execute(&mut **tx)
    .await
    .map(|_| ())
    .map_err(|error| {
        tracing::error!("Failed to update borrow trend bucket: {}", error);
        AppError::DatabaseError("Failed to track borrow interaction".to_string())
    })
}

async fn borrow_counts(
    pool: &PgPool,
    trainer_id: &str,
    context: &BorrowContext,
) -> Result<BorrowCounts, AppError> {
    sqlx::query_as::<_, (i64, i64, i32)>(
        r#"
        SELECT view_count, copy_count, theoretical_copy_count
        FROM borrow_interaction_totals_v2
        WHERE trainer_id = $1
          AND borrow_key = $2
        "#,
    )
    .bind(trainer_id)
    .bind(&context.borrow_key)
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
        FROM borrow_interaction_totals_v2
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
        "SELECT COALESCE(SUM(copy_count), 0)::bigint FROM borrow_interaction_totals_v2 WHERE trainer_id = $1",
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
    context: &BorrowContext,
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
        "action": "search",
        "reason": "borrow_theoretical_full_followers",
        "borrow_key": &context.borrow_key,
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
                WHERE task_type = 'friend/search'
                  AND status IN ('pending', 'processing')
                  AND (task_data->>'id' = $4 OR task_data->>'friend_viewer_id' = $4)
            )
            "#,
        )
        .bind("friend/search")
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
    context: &BorrowContext,
    follower_num: i32,
    task_created: bool,
) -> Result<(), AppError> {
    sqlx::query(
        r#"
        UPDATE borrow_interaction_totals_v2
        SET theoretical_copy_count = 0,
            last_known_follower_num = $3,
            last_recheck_at = NOW(),
            last_recheck_task_created_at = CASE WHEN $4 THEN NOW() ELSE last_recheck_task_created_at END,
            updated_at = NOW()
        WHERE trainer_id = $1
          AND borrow_key = $2
        "#,
    )
    .bind(trainer_id)
    .bind(&context.borrow_key)
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

async fn require_borrow_browser_actor(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<BorrowActor, AppError> {
    if has_api_key_header(headers) {
        return Err(AppError::Forbidden("api_key_not_allowed".to_string()));
    }

    let proof = require_turnstile_browser_proof(headers, state.redis_store.as_ref())
        .await
        .map_err(|error| match error {
            "browser_proof_unavailable" => {
                AppError::DatabaseError("browser_proof_unavailable".to_string())
            }
            other => AppError::Forbidden(other.to_string()),
        })?;
    let material = format!("browser-proof:{}:{}", proof.subject(), proof.proof_id());
    let issued_at = i64::try_from(proof.issued_at()).unwrap_or(i64::MAX);
    let bucket_start = Utc
        .timestamp_opt(issued_at, 0)
        .single()
        .unwrap_or_else(Utc::now);

    Ok(BorrowActor {
        hash: hex::encode(Sha256::digest(material.as_bytes())),
        bucket_start,
    })
}

fn has_api_key_header(headers: &HeaderMap) -> bool {
    ["X-API-Key", "X-API-Token", "X-API-Tokens"]
        .iter()
        .any(|name| {
            header_str(headers, name)
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
        })
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

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
                FROM borrow_interaction_totals_v2
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
