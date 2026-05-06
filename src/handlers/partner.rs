//! Partner inheritance lookup endpoints.
//!
//! Flow:
//!   1. Frontend POSTs to `/api/v4/partner/lookup` with a `partner_id`
//!      (either a 9-digit practice partner ID or a 12-digit trainer/account
//!      ID).
//!   2. Backend creates a `practice_race/get_partner_info` task. If the user
//!      is logged in we attach the JWT user UUID so the bot can persist the
//!      result into `partner_inheritance`.
//!   3. Frontend opens an SSE connection at
//!      `/api/v4/partner/lookup/{task_id}/stream` and waits for completion.
//!   4. Bot worker fetches data from the game API, writes the inheritance
//!      row, and updates the task to `completed`. The Postgres trigger fires
//!      a NOTIFY → backend's listener fans out → SSE delivers the result.
//!
//! Logged-in users additionally have `GET /saved` to list previously fetched
//! partners ordered by `updated_at DESC` and `DELETE /saved/{account_id}` to
//! remove an entry.

use std::convert::Infallible;
use std::time::Duration;

use axum::{
    extract::{Path, State},
    response::{
        sse::{Event, KeepAlive, Sse},
        Json,
    },
    routing::{delete, get, post},
    Router,
};
use serde_json::json;
use tokio_stream::wrappers::ReceiverStream;
use validator::Validate;

use crate::errors::AppError;
use crate::middleware::auth::{AuthenticatedUser, OptionalUser};
use crate::models::{
    AnonMigrateEntry, Inheritance, PartnerDirectResult, PartnerInheritance, PartnerLookupRequest,
    PartnerLookupResponse,
};
use crate::AppState;

const TASK_TYPE: &str = "practice_race/get_partner_info";

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/lookup", post(create_lookup))
        .route("/lookup/:task_id/stream", get(stream_lookup))
        .route("/saved", get(list_saved))
        .route("/saved/:account_id", delete(delete_saved))
        .route("/saved/migrate", post(migrate_anon))
}

/// Reset the tasks id sequence after a manual-insert collision.
async fn fix_task_sequence(pool: &sqlx::PgPool) {
    let _ = sqlx::query(
        "SELECT setval(pg_get_serial_sequence('tasks','id'), COALESCE((SELECT MAX(id) FROM tasks),0)+1, false)"
    )
    .execute(pool)
    .await;
}

/// Queue a partner lookup task. Returns a task id that the client uses to
/// open an SSE stream for the result.
async fn create_lookup(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
    Json(payload): Json<PartnerLookupRequest>,
) -> Result<Json<PartnerLookupResponse>, AppError> {
    payload
        .validate()
        .map_err(|e| AppError::BadRequest(format!("Validation error: {e}")))?;

    let partner_id = payload.partner_id.trim().to_string();

    let lookup_kind = match partner_id.len() {
        9 => "practice_partner",
        12 => "trainer",
        _ => {
            return Err(AppError::BadRequest(
                "partner_id must be exactly 9 digits (practice partner) or 12 digits (trainer)"
                    .into(),
            ));
        }
    };

    let user_id_str = user.as_ref().map(|u| u.user_id.to_string());
    let will_persist = user_id_str.is_some();

    // For trainer IDs (12 digits) check our DB first — no need to queue a
    // bot task if we already have fresh inheritance data.
    if lookup_kind == "trainer" {
        #[derive(sqlx::FromRow)]
        struct TrainerRow {
            trainer_name: String,
            follower_num: Option<i32>,
            last_updated: Option<chrono::NaiveDateTime>,
        }

        let trainer_row = sqlx::query_as::<_, TrainerRow>(
            "SELECT name AS trainer_name, follower_num, last_updated FROM trainer WHERE account_id = $1",
        )
        .bind(&partner_id)
        .fetch_optional(&state.db)
        .await?;

        let inheritance_row = sqlx::query_as::<_, Inheritance>(
            "SELECT * FROM inheritance WHERE account_id = $1 ORDER BY inheritance_id DESC LIMIT 1",
        )
        .bind(&partner_id)
        .fetch_optional(&state.db)
        .await?;

        if trainer_row.is_some() || inheritance_row.is_some() {
            let t = trainer_row;
            let record = PartnerDirectResult {
                account_id: partner_id.clone(),
                trainer_name: t
                    .as_ref()
                    .map(|r| r.trainer_name.clone())
                    .unwrap_or_default(),
                follower_num: t.as_ref().and_then(|r| r.follower_num),
                last_updated: t.as_ref().and_then(|r| r.last_updated),
                inheritance: inheritance_row,
            };
            return Ok(Json(PartnerLookupResponse {
                task_id: None,
                status: "completed".into(),
                will_persist: false,
                result: Some(record),
            }));
        }
    }

    let task_data = json!({
        "partner_id": partner_id,
        "lookup_kind": lookup_kind,
        "user_id": user_id_str,
        "label": payload.label,
    });

    // Insert task with one retry on sequence collision (consistent with other
    // task endpoints).
    let q = || {
        sqlx::query_as::<_, (i32, String)>(
            r#"
            INSERT INTO tasks (task_type, task_data, priority, status, created_at)
            VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP)
            RETURNING id, status
            "#,
        )
        .bind(TASK_TYPE)
        .bind(&task_data)
        .bind(0i32) // user-instructed priority
    };
    let (task_id, status) = match q().fetch_one(&state.db).await {
        Ok(t) => t,
        Err(_) => {
            fix_task_sequence(&state.db).await;
            q().fetch_one(&state.db).await.map_err(|e| {
                tracing::error!("Failed to insert partner lookup task: {e}");
                AppError::DatabaseError("Failed to create lookup task".into())
            })?
        }
    };

    Ok(Json(PartnerLookupResponse {
        task_id: Some(task_id),
        status,
        will_persist,
        result: None,
    }))
}

/// SSE stream that emits a single completion event for the given task id.
/// Anonymous lookups return the raw task result (`task_data.result`); logged-in
/// lookups return the persisted `partner_inheritance` row.
async fn stream_lookup(
    State(state): State<AppState>,
    Path(task_id): Path<i32>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    // Verify task exists and belongs to our partner-lookup type. We don't
    // gate by user — task_id is opaque enough for this purpose, and anon
    // lookups have no user to gate against anyway.
    let existing: Option<(String, String, serde_json::Value)> =
        sqlx::query_as("SELECT task_type, status, task_data FROM tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&state.db)
            .await?;

    let (task_type, current_status, _) =
        existing.ok_or_else(|| AppError::NotFound(format!("Task {task_id} not found")))?;

    if task_type != TASK_TYPE {
        return Err(AppError::BadRequest("Task is not a partner lookup".into()));
    }

    // Helper that wraps a single event in a one-shot ReceiverStream.
    async fn one_shot(evt: Event) -> ReceiverStream<Result<Event, Infallible>> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = tx.send(Ok(evt)).await;
        ReceiverStream::new(rx)
    }

    // If already terminal, emit immediately and close.
    if current_status == "completed" || current_status == "failed" {
        let evt = build_completion_event(&state, task_id, &current_status).await;
        return Ok(Sse::new(one_shot(evt).await).keep_alive(KeepAlive::default()));
    }

    // Subscribe before any await on the DB-state lookup so we don't miss an
    // immediate notification.
    let rx = state.task_notifier.subscribe(task_id).await;

    // Re-check status after subscribing in case the worker completed while
    // we were setting up.
    let status_now: Option<String> = sqlx::query_scalar("SELECT status FROM tasks WHERE id = $1")
        .bind(task_id)
        .fetch_optional(&state.db)
        .await?;

    if let Some(s) = status_now.as_deref() {
        if s == "completed" || s == "failed" {
            let evt = build_completion_event(&state, task_id, s).await;
            return Ok(Sse::new(one_shot(evt).await).keep_alive(KeepAlive::default()));
        }
    }

    let state_clone = state.clone();

    // Build a stream that:
    //   1. Emits a `pending` event right away so the client knows the
    //      connection is live.
    //   2. Awaits the broadcast notification (with a generous timeout).
    //   3. Loads + emits the result, then closes.
    let pending_evt = Event::default()
        .event("pending")
        .data(json!({ "task_id": task_id, "status": "pending" }).to_string());

    // Use a small mpsc channel + a spawned task to drive the stream. This
    // keeps us off `async_stream` (not in our dep tree). Note: `rx` (the
    // broadcast receiver) is moved into the spawned task; `tx`/`out_rx` is
    // the mpsc channel feeding the SSE response.
    let (tx, out_rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(4);

    // Send the pending event up front (best-effort).
    let _ = tx.send(Ok(pending_evt)).await;

    let mut broadcast_rx = rx;
    tokio::spawn(async move {
        // Hard deadline — 2 minutes total. We loop inside handling intermediate
        // `processing` events so the frontend can transition its UI state.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // Timed out — delete the stale task so it doesn't linger in
                // the queue and get retried endlessly.
                let _ = sqlx::query("DELETE FROM tasks WHERE id = $1")
                    .bind(task_id)
                    .execute(&state_clone.db)
                    .await;
                let _ = tx
                    .send(Ok(Event::default().event("timeout").data(
                        json!({ "task_id": task_id, "status": "timeout" }).to_string(),
                    )))
                    .await;
                break;
            }

            let outcome = tokio::time::timeout(remaining, broadcast_rx.recv()).await;
            match outcome {
                Ok(Ok(notification)) => {
                    if notification.status == "processing" {
                        // Intermediate event: bot picked up the task. Emit and
                        // keep the loop going — we still need the final result.
                        let _ = tx
                            .send(Ok(Event::default().event("processing").data(
                                json!({ "task_id": task_id, "status": "processing" }).to_string(),
                            )))
                            .await;
                    } else {
                        // Terminal (completed / failed).
                        let evt = build_completion_event(
                            &state_clone,
                            notification.task_id,
                            &notification.status,
                        )
                        .await;
                        let _ = tx.send(Ok(evt)).await;
                        break;
                    }
                }
                Ok(Err(_)) => {
                    // Sender dropped without firing — fall back to a DB read.
                    let s: Option<String> =
                        sqlx::query_scalar("SELECT status FROM tasks WHERE id = $1")
                            .bind(task_id)
                            .fetch_optional(&state_clone.db)
                            .await
                            .ok()
                            .flatten();
                    let status = s.unwrap_or_else(|| "failed".to_string());
                    let evt = build_completion_event(&state_clone, task_id, &status).await;
                    let _ = tx.send(Ok(evt)).await;
                    break;
                }
                Err(_) => {
                    // Timed out — delete the stale task so it doesn't linger in
                    // the queue and get retried endlessly.
                    let _ = sqlx::query("DELETE FROM tasks WHERE id = $1")
                        .bind(task_id)
                        .execute(&state_clone.db)
                        .await;
                    let _ = tx
                        .send(Ok(Event::default().event("timeout").data(
                            json!({ "task_id": task_id, "status": "timeout" }).to_string(),
                        )))
                        .await;
                    break;
                }
            }
        }
        // Dropping `tx` here ends the SSE stream.
    });

    let stream = ReceiverStream::new(out_rx);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn build_completion_event(state: &AppState, task_id: i32, status: &str) -> Event {
    // Pull task row to get task_data (which the bot may stash a `result`
    // payload into — used for anonymous lookups that have no DB row).
    let row: Option<(serde_json::Value, Option<String>)> =
        sqlx::query_as("SELECT task_data, error_message FROM tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();

    let (task_data, error_message) = row.unwrap_or((serde_json::Value::Null, None));

    // Try to load the persisted partner_inheritance row using the user_id +
    // resulting account_id stashed in task_data.
    let user_id_str = task_data.get("user_id").and_then(|v| v.as_str());
    let account_id = task_data
        .get("result")
        .and_then(|r| r.get("account_id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut payload = json!({
        "task_id": task_id,
        "status": status,
    });

    if let Some(err) = error_message {
        payload["error"] = json!(err);
    }

    if status == "completed" {
        if let (Some(uid_str), Some(acct)) = (user_id_str, account_id.as_deref()) {
            if let Ok(uid) = uuid::Uuid::parse_str(uid_str) {
                let row = sqlx::query_as::<_, PartnerInheritance>(
                    "SELECT * FROM partner_inheritance WHERE user_id = $1 AND account_id = $2",
                )
                .bind(uid)
                .bind(acct)
                .fetch_optional(&state.db)
                .await
                .ok()
                .flatten();
                if let Some(r) = row {
                    payload["inheritance"] =
                        serde_json::to_value(&r).unwrap_or(serde_json::Value::Null);
                }
            }
        }

        // Fall back to whatever the bot stashed in task_data.result for anon
        // (or unpersisted) lookups.
        if payload.get("inheritance").is_none() {
            if let Some(result) = task_data.get("result") {
                payload["inheritance"] = result.clone();
            }
        }
    }

    let event_name = match status {
        "completed" => "completed",
        "failed" => "failed",
        _ => "update",
    };

    Event::default().event(event_name).data(payload.to_string())
}

/// List all partner inheritances saved by the authenticated user.
/// Returns an empty list for unauthenticated users (data is stored locally).
async fn list_saved(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
) -> Result<Json<Vec<PartnerInheritance>>, AppError> {
    let Some(user) = user else {
        return Ok(Json(vec![]));
    };

    let rows = sqlx::query_as::<_, PartnerInheritance>(
        "SELECT * FROM partner_inheritance WHERE user_id = $1 ORDER BY updated_at DESC",
    )
    .bind(user.user_id)
    .fetch_all(&state.db)
    .await?;

    Ok(Json(rows))
}

/// Delete a saved partner entry for the authenticated user.
async fn delete_saved(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(account_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result =
        sqlx::query("DELETE FROM partner_inheritance WHERE user_id = $1 AND account_id = $2")
            .bind(user.user_id)
            .bind(&account_id)
            .execute(&state.db)
            .await?;

    Ok(Json(json!({
        "success": true,
        "deleted": result.rows_affected(),
    })))
}

/// Bulk-upsert anonymous lookups from localStorage into the authenticated
/// user's `partner_inheritance` table. Called once on first login.
///
/// Each entry already contains the full inheritance payload (fetched by the
/// bot previously and cached client-side). We upsert directly without
/// scheduling new bot tasks � the data is already known.
///
/// Entries that conflict on `(user_id, account_id)` are skipped (DO NOTHING)
/// so that newer server-side data is never overwritten by stale local cache.
async fn migrate_anon(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(entries): Json<Vec<AnonMigrateEntry>>,
) -> Result<Json<serde_json::Value>, AppError> {
    if entries.is_empty() {
        return Ok(Json(json!({ "migrated": 0 })));
    }

    // Cap to a sane batch size to prevent abuse.
    const MAX_ENTRIES: usize = 50;
    let entries = entries.into_iter().take(MAX_ENTRIES).collect::<Vec<_>>();

    let mut migrated = 0u32;
    for e in &entries {
        if e.account_id.is_empty() {
            continue;
        }
        // Skip (DO NOTHING) if a row already exists so newer server data wins.
        let rows = sqlx::query(
            r#"
            INSERT INTO partner_inheritance (
                user_id, account_id,
                main_parent_id, parent_left_id, parent_right_id,
                parent_rank, parent_rarity,
                blue_sparks, pink_sparks, green_sparks, white_sparks,
                win_count, white_count,
                main_blue_factors, main_pink_factors, main_green_factors,
                main_white_factors, main_white_count,
                left_blue_factors, left_pink_factors, left_green_factors,
                left_white_factors, left_white_count,
                right_blue_factors, right_pink_factors, right_green_factors,
                right_white_factors, right_white_count,
                main_win_saddles, left_win_saddles, right_win_saddles,
                race_results,
                blue_stars_sum, pink_stars_sum, green_stars_sum, white_stars_sum,
                affinity_score, trainer_name, label,
                created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18,
                $19, $20, $21, $22, $23,
                $24, $25, $26, $27, $28,
                $29, $30, $31, $32,
                $33, $34, $35, $36,
                $37, $38, $39,
                NOW(), NOW()
            )
            ON CONFLICT (user_id, account_id) DO NOTHING
            "#,
        )
        .bind(user.user_id)
        .bind(&e.account_id)
        .bind(e.main_parent_id)
        .bind(e.parent_left_id)
        .bind(e.parent_right_id)
        .bind(e.parent_rank)
        .bind(e.parent_rarity)
        .bind(&e.blue_sparks)
        .bind(&e.pink_sparks)
        .bind(&e.green_sparks)
        .bind(&e.white_sparks)
        .bind(e.win_count)
        .bind(e.white_count)
        .bind(e.main_blue_factors)
        .bind(e.main_pink_factors)
        .bind(e.main_green_factors)
        .bind(&e.main_white_factors)
        .bind(e.main_white_count)
        .bind(e.left_blue_factors)
        .bind(e.left_pink_factors)
        .bind(e.left_green_factors)
        .bind(&e.left_white_factors)
        .bind(e.left_white_count)
        .bind(e.right_blue_factors)
        .bind(e.right_pink_factors)
        .bind(e.right_green_factors)
        .bind(&e.right_white_factors)
        .bind(e.right_white_count)
        .bind(&e.main_win_saddles)
        .bind(&e.left_win_saddles)
        .bind(&e.right_win_saddles)
        .bind(&e.race_results)
        .bind(e.blue_stars_sum)
        .bind(e.pink_stars_sum)
        .bind(e.green_stars_sum)
        .bind(e.white_stars_sum)
        .bind(e.affinity_score)
        .bind(&e.trainer_name)
        .bind(&e.label)
        .execute(&state.db)
        .await?;

        migrated += rows.rows_affected() as u32;
    }

    Ok(Json(json!({ "migrated": migrated })))
}
