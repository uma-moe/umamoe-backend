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
//!   4. Bot worker fetches data from the game API and writes the normalized
//!      result to the temporary task row. Authenticated results are also saved
//!      to `partner_inheritance`; anonymous results are delivered only by SSE.
//!      The Postgres trigger fires a NOTIFY and the backend also reconciles the
//!      task row until it can deliver the terminal result.
//!
//! Logged-in users additionally have `GET /saved` to list previously fetched
//! partners ordered by `updated_at DESC`, `DELETE /saved/id/{id}` to remove one
//! saved runner, and legacy `DELETE /saved/{account_id}` to remove entries for
//! a trainer.

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
use crate::tasks::insertOrGetActiveTask;
pub use crate::types::{
    AnonMigrateEntry, Inheritance, PartnerDirectResult, PartnerInheritance, PartnerLookupRequest,
    PartnerLookupResponse, INHERITANCE_SELECT_COLUMNS,
};
use crate::AppState;

include!("../types/handlers/partner.rs");

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/lookup", post(createLookup))
        .route("/lookup/:task_id/stream", get(streamLookup))
        .route("/saved", get(listSaved))
        .route("/saved/id/:saved_id", delete(deleteSavedById))
        .route("/saved/:account_id", delete(deleteSavedByAccount))
        .route("/saved/migrate", post(migrateAnon))
}

fn completionStatus(status: &str, task_data: &serde_json::Value) -> Option<&'static str> {
    match status {
        "completed" => Some("completed"),
        "failed" => Some("failed"),
        // The worker writes the anonymous result before its terminal status.
        // Treat that committed payload as authoritative so a failed/missed
        // status update or NOTIFY cannot strand the SSE stream. Authenticated
        // lookups must still wait for their saved row to commit before the
        // stream reports completion.
        _ if isAnonymousLookup(task_data) && task_data.get("result").is_some() => Some("completed"),
        _ => None,
    }
}

fn isAnonymousLookup(task_data: &serde_json::Value) -> bool {
    task_data
        .get("user_id")
        .and_then(|value| value.as_str())
        .is_none_or(|user_id| user_id.trim().is_empty())
}

/// Queue a partner lookup task. Returns a task id that the client uses to
/// open an SSE stream for the result.
async fn createLookup(
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

    if payload.require_persistence && !will_persist {
        return Err(AppError::Unauthorized(
            "Your session is missing or expired; sign in again to save this partner lookup".into(),
        ));
    }

    // Anonymous trainer-ID lookups may use a cached direct result. Signed-in
    // lookups must go through the worker so completion means the result was
    // actually written to partner_inheritance and will survive a reload.
    if lookup_kind == "trainer" && !will_persist {
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

        let inheritance_sql = format!(
            "SELECT {INHERITANCE_SELECT_COLUMNS} FROM inheritance WHERE account_id = $1 ORDER BY inheritance_id DESC LIMIT 1"
        );
        let inheritance_row = sqlx::query_as::<_, Inheritance>(&inheritance_sql)
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
                inheritance: inheritance_row.and_then(|row| serde_json::to_value(row).ok()),
            };
            return Ok(Json(PartnerLookupResponse {
                task_id: None,
                status: "completed".into(),
                will_persist: false,
                result: Some(record),
            }));
        }
    }

    if state.user_writes_disabled {
        return Err(AppError::Forbidden(
            "Partner lookups that need a new task are disabled in this read-only environment"
                .into(),
        ));
    }

    let task_data = json!({
        "partner_id": partner_id,
        "lookup_kind": lookup_kind,
        "user_id": user_id_str,
        "label": payload.label,
    });

    let (task, _) = insertOrGetActiveTask(&state.db, TASK_TYPE, &task_data, 0, None)
        .await
        .map_err(|e| {
            tracing::error!("Failed to insert or find active partner lookup task: {e}");
            AppError::DatabaseError("Failed to create lookup task".into())
        })?;

    Ok(Json(PartnerLookupResponse {
        task_id: Some(task.id),
        status: task.status,
        will_persist,
        result: None,
    }))
}

/// SSE stream that emits a single completion event for the given task id.
/// Anonymous lookups return the raw task result (`task_data.result`); logged-in
/// lookups return the persisted `partner_inheritance` row.
async fn streamLookup(
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

    let (task_type, current_status, current_task_data) =
        existing.ok_or_else(|| AppError::NotFound(format!("Task {task_id} not found")))?;

    if task_type != TASK_TYPE {
        return Err(AppError::BadRequest("Task is not a partner lookup".into()));
    }

    // Helper that wraps a single event in a one-shot ReceiverStream.
    async fn oneShot(evt: Event) -> ReceiverStream<Result<Event, Infallible>> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = tx.send(Ok(evt)).await;
        ReceiverStream::new(rx)
    }

    // If already terminal (or the worker committed a result before its final
    // status update), emit immediately and close.
    if let Some(status) = completionStatus(&current_status, &current_task_data) {
        let evt = buildTerminalEvent(&state, task_id, status).await;
        return Ok(Sse::new(oneShot(evt).await).keep_alive(KeepAlive::default()));
    }

    // Subscribe before any await on the DB-state lookup so we don't miss an
    // immediate notification.
    let rx = state.task_notifier.subscribe(task_id).await;

    // Re-check status after subscribing in case the worker completed while
    // we were setting up.
    let state_now: Option<(String, serde_json::Value)> =
        sqlx::query_as("SELECT status, task_data FROM tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&state.db)
            .await?;

    if let Some((status, task_data)) = state_now {
        if let Some(status) = completionStatus(&status, &task_data) {
            let evt = buildTerminalEvent(&state, task_id, status).await;
            return Ok(Sse::new(oneShot(evt).await).keep_alive(KeepAlive::default()));
        }
    }

    let state_clone = state.clone();

    // Build a stream that:
    //   1. Emits a `pending` event right away so the client knows the
    //      connection is live.
    //   2. Awaits broadcast notifications while periodically reconciling the
    //      database state, so a dropped NOTIFY cannot strand the request.
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
        loop {
            tokio::select! {
                // A lookup can legitimately stay processing while it waits in
                // the workers' hot queue. Keep the anonymous task row and SSE
                // result channel alive until the worker reaches a terminal
                // state. Stop the reconciler promptly if the browser leaves.
                _ = tx.closed() => break,
                outcome = broadcast_rx.recv() => match outcome {
                    Ok(notification) => {
                    if notification.status == "processing" {
                        if tx
                            .send(Ok(Event::default().event("processing").data(
                                json!({ "task_id": task_id, "status": "processing" }).to_string(),
                            )))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    } else if matches!(notification.status.as_str(), "completed" | "failed") {
                        let evt = buildTerminalEvent(
                            &state_clone,
                            notification.task_id,
                            &notification.status,
                        )
                        .await;
                        let _ = tx.send(Ok(evt)).await;
                        break;
                    }
                    }
                    Err(_) => {
                        if let Some(status) = reconciledCompletionStatus(&state_clone, task_id).await {
                            let evt = buildTerminalEvent(&state_clone, task_id, &status).await;
                            let _ = tx.send(Ok(evt)).await;
                            break;
                        }
                        // A closed/lagged local channel is recoverable because
                        // the database remains the source of truth.
                        broadcast_rx = state_clone.task_notifier.subscribe(task_id).await;
                    }
                },
                _ = tokio::time::sleep(RECONCILE_INTERVAL) => {
                    if let Some(status) = reconciledCompletionStatus(&state_clone, task_id).await {
                        let evt = buildTerminalEvent(&state_clone, task_id, &status).await;
                        let _ = tx.send(Ok(evt)).await;
                        break;
                    }
                }
            }
        }
        // Dropping `tx` here ends the SSE stream.
    });

    let stream = ReceiverStream::new(out_rx);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn reconciledCompletionStatus(state: &AppState, task_id: i32) -> Option<String> {
    let row: Result<Option<(String, serde_json::Value)>, sqlx::Error> =
        sqlx::query_as("SELECT status, task_data FROM tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&state.db)
            .await;

    match row {
        Ok(Some((status, task_data))) => completionStatus(&status, &task_data).map(str::to_string),
        Ok(None) => None,
        Err(error) => {
            tracing::warn!(task_id, %error, "Failed to reconcile partner lookup task");
            None
        }
    }
}

async fn buildTerminalEvent(state: &AppState, task_id: i32, status: &str) -> Event {
    let built = buildCompletionEvent(state, task_id, status).await;
    if built.anonymous && !state.user_writes_disabled {
        let db = state.db.clone();
        tokio::spawn(async move {
            // Give concurrently connected SSE clients time to read the same
            // result, then remove the anonymous transport row. Anonymous
            // lookups are never promoted to saved partner history.
            tokio::time::sleep(ANONYMOUS_TASK_CLEANUP_DELAY).await;
            let _ = sqlx::query(
                r#"
                DELETE FROM tasks
                WHERE id = $1
                  AND task_type = $2
                  AND task_data->>'user_id' IS NULL
                  AND (status IN ('completed', 'failed') OR task_data ? 'result')
                "#,
            )
            .bind(task_id)
            .bind(TASK_TYPE)
            .execute(&db)
            .await;
        });
    }
    built.event
}

async fn buildCompletionEvent(
    state: &AppState,
    task_id: i32,
    status: &str,
) -> BuiltCompletionEvent {
    // Pull task row to get task_data (which the bot may stash a `result`
    // payload into — used for anonymous lookups that have no DB row).
    let row: Option<(serde_json::Value, Option<String>)> =
        sqlx::query_as("SELECT task_data, error_message FROM tasks WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();

    let anonymous = row
        .as_ref()
        .map(|(task_data, _)| isAnonymousLookup(task_data))
        .unwrap_or(false);
    let (task_data, error_message) = row.unwrap_or((serde_json::Value::Null, None));
    let task_result = task_data.get("result");
    let task_inheritance = task_result
        .and_then(|result| result.get("inheritance"))
        .or(task_result);

    // Try to load the persisted partner_inheritance row using the user_id +
    // resulting account_id stashed in task_data.
    let user_id_str = task_data.get("user_id").and_then(|v| v.as_str());
    let account_id = task_inheritance
        .and_then(|r| r.get("account_id"))
        .or_else(|| task_result.and_then(|r| r.get("account_id")))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let content_hash = task_inheritance
        .and_then(|r| r.get("content_hash"))
        .or_else(|| task_result.and_then(|r| r.get("content_hash")))
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
                let row = if let Some(hash) = content_hash.as_deref() {
                    sqlx::query_as::<_, PartnerInheritance>(
                        "SELECT * FROM partner_inheritance WHERE user_id = $1 AND account_id = $2 AND content_hash = $3",
                    )
                    .bind(uid)
                    .bind(acct)
                    .bind(hash)
                    .fetch_optional(&state.db)
                    .await
                    .ok()
                    .flatten()
                } else {
                    sqlx::query_as::<_, PartnerInheritance>(
                        "SELECT * FROM partner_inheritance WHERE user_id = $1 AND account_id = $2 ORDER BY updated_at DESC, id DESC LIMIT 1",
                    )
                    .bind(uid)
                    .bind(acct)
                    .fetch_optional(&state.db)
                    .await
                    .ok()
                    .flatten()
                };
                if let Some(r) = row {
                    payload["inheritance"] =
                        serde_json::to_value(&r).unwrap_or(serde_json::Value::Null);
                }
            }
        }

        // Fall back to whatever the bot stashed in task_data.result for anon
        // (or unpersisted) lookups.
        if payload.get("inheritance").is_none() {
            if let Some(inheritance) = task_inheritance {
                payload["inheritance"] = inheritance.clone();
            }
        }
    }

    let event_name = match status {
        "completed" => "completed",
        "failed" => "failed",
        _ => "update",
    };

    BuiltCompletionEvent {
        event: Event::default().event(event_name).data(payload.to_string()),
        anonymous,
    }
}

/// List all partner inheritances saved by the authenticated user.
/// Authentication failures are returned to the client rather than being
/// disguised as an empty saved-history list.
async fn listSaved(
    State(state): State<AppState>,
    user: AuthenticatedUser,
) -> Result<Json<Vec<PartnerInheritance>>, AppError> {
    let rows = sqlx::query_as::<_, PartnerInheritance>(
        "SELECT * FROM partner_inheritance WHERE user_id = $1 ORDER BY updated_at DESC",
    )
    .bind(user.user_id)
    .fetch_all(&state.db)
    .await?;

    Ok(Json(rows))
}

/// Delete a single saved partner entry for the authenticated user.
async fn deleteSavedById(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(saved_id): Path<i32>,
) -> Result<Json<serde_json::Value>, AppError> {
    let result = sqlx::query("DELETE FROM partner_inheritance WHERE user_id = $1 AND id = $2")
        .bind(user.user_id)
        .bind(saved_id)
        .execute(&state.db)
        .await?;

    Ok(Json(json!({
        "success": true,
        "deleted": result.rows_affected(),
    })))
}

/// Legacy delete route. Removes every saved partner entry for a trainer.
async fn deleteSavedByAccount(
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
async fn migrateAnon(
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
                main_parent_id, scenario_id, parent_left_id, parent_right_id,
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
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14,
                $15, $16, $17, $18, $19,
                $20, $21, $22, $23, $24,
                $25, $26, $27, $28, $29,
                $30, $31, $32, $33,
                $34, $35, $36, $37,
                $38, $39, $40,
                NOW(), NOW()
            )
            ON CONFLICT (user_id, account_id, content_hash) DO NOTHING
            "#,
        )
        .bind(user.user_id)
        .bind(&e.account_id)
        .bind(e.main_parent_id)
        .bind(e.scenario_id)
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

#[cfg(test)]
mod tests {
    use super::{completionStatus, isAnonymousLookup};
    pub use crate::types::PartnerLookupRequest;

    #[test]
    fn persistenceRequirementIsBackwardsCompatibleAndExplicit() {
        let anonymous: PartnerLookupRequest = serde_json::from_value(serde_json::json!({
            "partner_id": "123456789",
            "label": null
        }))
        .expect("legacy anonymous request should deserialize");
        let signed_in: PartnerLookupRequest = serde_json::from_value(serde_json::json!({
            "partner_id": "123456789",
            "label": null,
            "require_persistence": true
        }))
        .expect("persistence-required request should deserialize");

        assert!(!anonymous.require_persistence);
        assert!(signed_in.require_persistence);
    }

    #[test]
    fn committedResultIsTerminalEvenBeforeStatusUpdate() {
        let task_data = serde_json::json!({
            "partner_id": "123456789",
            "user_id": null,
            "result": { "account_id": "123456789012" }
        });

        assert_eq!(
            completionStatus("processing", &task_data),
            Some("completed")
        );
        assert_eq!(completionStatus("pending", &task_data), Some("completed"));
        assert_eq!(completionStatus("failed", &task_data), Some("failed"));
    }

    #[test]
    fn authenticatedResultWaitsForPersistenceToComplete() {
        let task_data = serde_json::json!({
            "partner_id": "123456789",
            "user_id": "83d8a8f0-a1a1-4d9f-b7a8-c5f650ba27d6",
            "result": { "account_id": "123456789012" }
        });

        assert_eq!(completionStatus("pending", &task_data), None);
        assert_eq!(completionStatus("processing", &task_data), None);
        assert_eq!(completionStatus("completed", &task_data), Some("completed"));
    }

    #[test]
    fn activeTaskWithoutResultIsNotTerminal() {
        let task_data = serde_json::json!({
            "partner_id": "123456789",
            "user_id": null
        });

        assert_eq!(completionStatus("pending", &task_data), None);
        assert_eq!(completionStatus("processing", &task_data), None);
    }

    #[test]
    fn anonymousLookupHasNoUsableUserId() {
        assert!(isAnonymousLookup(&serde_json::json!({ "user_id": null })));
        assert!(isAnonymousLookup(&serde_json::json!({})));
        assert!(!isAnonymousLookup(&serde_json::json!({
            "user_id": "83d8a8f0-a1a1-4d9f-b7a8-c5f650ba27d6"
        })));
    }

    #[test]
    fn partnerLookupTaskCreationIsIdempotent() {
        let source = include_str!("partner.rs");
        let create_block = source
            .split("async fn createLookup(")
            .nth(1)
            .and_then(|tail| tail.split("async fn streamLookup(").next())
            .expect("partner lookup creation block should exist");

        assert!(create_block.contains("insertOrGetActiveTask("));
        assert!(!create_block.contains("fixTaskSequence"));
        assert!(!create_block.contains("INSERT INTO tasks"));
    }

    #[test]
    fn signedInLookupsCannotTakeNonPersistentFallbacks() {
        let source = include_str!("partner.rs");
        let create_block = source
            .split("async fn createLookup(")
            .nth(1)
            .and_then(|tail| tail.split("async fn streamLookup(").next())
            .expect("partner lookup creation block should exist");

        assert!(create_block.contains("payload.require_persistence && !will_persist"));
        assert!(create_block.contains("AppError::Unauthorized"));
        assert!(create_block.contains("lookup_kind == \"trainer\" && !will_persist"));
    }

    #[test]
    fn savedHistoryRequiresAuthentication() {
        let source = include_str!("partner.rs");
        let list_block = source
            .split("async fn listSaved(")
            .nth(1)
            .and_then(|tail| tail.split("async fn deleteSavedById(").next())
            .expect("saved-history block should exist");

        assert!(list_block.contains("user: AuthenticatedUser"));
        assert!(!list_block.contains("OptionalUser"));
        assert!(!list_block.contains("Ok(Json(vec![]))"));
    }

    #[test]
    fn activePartnerStreamHasNoDestructiveTimeout() {
        let source = include_str!("partner.rs");
        let stream_block = source
            .split("async fn streamLookup(")
            .nth(1)
            .and_then(|tail| tail.split("async fn reconciledCompletionStatus(").next())
            .expect("partner lookup stream block should exist");

        assert!(stream_block.contains("tx.closed()"));
        assert!(!stream_block.contains("LOOKUP_TIMEOUT"));
        assert!(!stream_block.contains("build_timeout_event"));
        assert!(!stream_block.contains("DELETE FROM tasks"));
    }
}

pub(crate) fn validatePartnerLookupId(value: &str) -> Result<(), validator::ValidationError> {
    let trimmed = value.trim();
    let is_valid = !trimmed.is_empty()
        && trimmed.chars().all(|c| c.is_ascii_digit())
        && (trimmed.len() == 9 || trimmed.len() == 12);

    if is_valid {
        Ok(())
    } else {
        let mut err = validator::ValidationError::new("partner_id_format");
        err.message = Some(
            "partner_id must be exactly 9 digits (practice partner) or 12 digits (trainer)".into(),
        );
        Err(err)
    }
}
