use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

use crate::{errors::AppError, middleware::auth::AuthenticatedUser, AppState};

const MAX_COLLECTION_BYTES: usize = 1_048_576;
const MAX_SHARE_BYTES: usize = 262_144;
const MAX_PLANS: usize = 50;

pub fn public_router() -> Router<AppState> {
    Router::new().route("/shared/:share_id", get(get_shared_plan))
}

pub fn authenticated_router() -> Router<AppState> {
    Router::new()
        .route("/state", get(get_state).put(put_state))
        .route("/shares", post(upsert_share))
        .route("/shares/:plan_id", delete(delete_share))
}

#[derive(Debug, FromRow)]
struct PlannerStateRow {
    revision: i64,
    collection: Value,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct PlannerStateResponse {
    revision: i64,
    collection: Option<Value>,
    updated_at: Option<DateTime<Utc>>,
}

impl From<PlannerStateRow> for PlannerStateResponse {
    fn from(row: PlannerStateRow) -> Self {
        Self {
            revision: row.revision,
            collection: Some(row.collection),
            updated_at: Some(row.updated_at),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PutPlannerStateRequest {
    base_revision: i64,
    collection: Value,
}

#[derive(Debug, Deserialize)]
struct SharePlanRequest {
    plan: Value,
}

#[derive(Debug, FromRow)]
struct ShareRow {
    share_id: String,
    plan_id: String,
    plan_name: String,
    plan: Value,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ShareResponse {
    share_id: String,
    plan_id: String,
    plan_name: String,
    plan: Value,
    updated_at: DateTime<Utc>,
}

impl From<ShareRow> for ShareResponse {
    fn from(row: ShareRow) -> Self {
        Self {
            share_id: row.share_id,
            plan_id: row.plan_id,
            plan_name: row.plan_name,
            plan: row.plan,
            updated_at: row.updated_at,
        }
    }
}

async fn get_state(
    user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<PlannerStateResponse>, AppError> {
    let row = load_state(&state, user.user_id).await?;
    Ok(Json(row.map(Into::into).unwrap_or(PlannerStateResponse {
        revision: 0,
        collection: None,
        updated_at: None,
    })))
}

async fn put_state(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<PutPlannerStateRequest>,
) -> Result<Response, AppError> {
    if payload.base_revision < 0 {
        return Err(AppError::BadRequest(
            "base_revision cannot be negative".into(),
        ));
    }
    validate_collection(&payload.collection)?;

    let saved = sqlx::query_as::<_, PlannerStateRow>(
        r#"
        INSERT INTO carat_planner_states AS current_state (
            user_id, revision, collection, updated_at
        )
        VALUES ($1, 1, $2, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
            collection = EXCLUDED.collection,
            revision = current_state.revision + 1,
            updated_at = NOW()
        WHERE current_state.revision = $3
        RETURNING revision, collection, updated_at
        "#,
    )
    .bind(user.user_id)
    .bind(payload.collection)
    .bind(payload.base_revision)
    .fetch_optional(&state.db)
    .await?;

    if let Some(row) = saved {
        return Ok((StatusCode::OK, Json(PlannerStateResponse::from(row))).into_response());
    }

    let current = load_state(&state, user.user_id)
        .await?
        .map(PlannerStateResponse::from)
        .unwrap_or(PlannerStateResponse {
            revision: 0,
            collection: None,
            updated_at: None,
        });
    Ok((StatusCode::CONFLICT, Json(current)).into_response())
}

async fn upsert_share(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<SharePlanRequest>,
) -> Result<(StatusCode, Json<ShareResponse>), AppError> {
    let (plan_id, plan_name) = validate_plan(&payload.plan)?;

    for _ in 0..5 {
        let share_id = random_share_id();
        let result = sqlx::query_as::<_, ShareRow>(
            r#"
            INSERT INTO carat_plan_shares (
                share_id, user_id, plan_id, plan_name, plan, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
            ON CONFLICT (user_id, plan_id) DO UPDATE SET
                plan_name = EXCLUDED.plan_name,
                plan = EXCLUDED.plan,
                updated_at = NOW()
            RETURNING share_id, plan_id, plan_name, plan, updated_at
            "#,
        )
        .bind(&share_id)
        .bind(user.user_id)
        .bind(&plan_id)
        .bind(&plan_name)
        .bind(&payload.plan)
        .fetch_one(&state.db)
        .await;

        match result {
            Ok(row) => return Ok((StatusCode::OK, Json(row.into()))),
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => continue,
            Err(error) => return Err(error.into()),
        }
    }

    Err(AppError::ServiceUnavailable(
        "Could not allocate a share link; please retry".into(),
    ))
}

async fn get_shared_plan(
    State(state): State<AppState>,
    Path(share_id): Path<String>,
) -> Result<Json<ShareResponse>, AppError> {
    if !valid_share_id(&share_id) {
        return Err(AppError::NotFound("Shared plan not found".into()));
    }
    let row = sqlx::query_as::<_, ShareRow>(
        r#"
        SELECT share_id, plan_id, plan_name, plan, updated_at
        FROM carat_plan_shares
        WHERE share_id = $1
        "#,
    )
    .bind(share_id)
    .fetch_optional(&state.db)
    .await?
    .ok_or_else(|| AppError::NotFound("Shared plan not found".into()))?;

    Ok(Json(row.into()))
}

async fn delete_share(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Path(plan_id): Path<String>,
) -> Result<StatusCode, AppError> {
    if plan_id.is_empty() || plan_id.len() > 100 {
        return Err(AppError::BadRequest("Invalid plan id".into()));
    }
    sqlx::query("DELETE FROM carat_plan_shares WHERE user_id = $1 AND plan_id = $2")
        .bind(user.user_id)
        .bind(plan_id)
        .execute(&state.db)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn load_state(state: &AppState, user_id: Uuid) -> Result<Option<PlannerStateRow>, AppError> {
    Ok(sqlx::query_as::<_, PlannerStateRow>(
        "SELECT revision, collection, updated_at FROM carat_planner_states WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?)
}

fn validate_collection(collection: &Value) -> Result<(), AppError> {
    let object = collection
        .as_object()
        .ok_or_else(|| AppError::BadRequest("Planner collection must be an object".into()))?;
    let plans = object
        .get("plans")
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::BadRequest("Planner collection must contain plans".into()))?;
    if plans.is_empty() || plans.len() > MAX_PLANS {
        return Err(AppError::BadRequest(format!(
            "Planner collection must contain between 1 and {MAX_PLANS} plans"
        )));
    }
    if object.get("activePlanId").and_then(Value::as_str).is_none() {
        return Err(AppError::BadRequest(
            "Planner collection must contain an activePlanId".into(),
        ));
    }
    validate_json_size(collection, MAX_COLLECTION_BYTES, "Planner collection")
}

fn validate_plan(plan: &Value) -> Result<(String, String), AppError> {
    let object = plan
        .as_object()
        .ok_or_else(|| AppError::BadRequest("Shared plan must be an object".into()))?;
    let plan_id = object
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 100)
        .ok_or_else(|| AppError::BadRequest("Shared plan has an invalid id".into()))?;
    let plan_name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.chars().count() <= 80)
        .ok_or_else(|| AppError::BadRequest("Shared plan has an invalid name".into()))?;
    validate_json_size(plan, MAX_SHARE_BYTES, "Shared plan")?;
    Ok((plan_id.to_string(), plan_name.to_string()))
}

fn validate_json_size(value: &Value, max_bytes: usize, label: &str) -> Result<(), AppError> {
    let size = serde_json::to_vec(value)
        .map_err(|_| AppError::BadRequest(format!("{label} is not valid JSON")))?
        .len();
    if size > max_bytes {
        return Err(AppError::BadRequest(format!(
            "{label} is too large ({size} bytes; maximum {max_bytes})"
        )));
    }
    Ok(())
}

fn random_share_id() -> String {
    let mut bytes = [0_u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn valid_share_id(value: &str) -> bool {
    let legacy_id =
        (8..=12).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_alphanumeric());
    let hex_id = value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    legacy_id || hex_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_compact_planner_collections() {
        let collection = serde_json::json!({
            "version": 1,
            "activePlanId": "plan-1",
            "plans": [{ "id": "plan-1", "name": "My plan" }]
        });
        assert!(validate_collection(&collection).is_ok());
        assert!(validate_collection(&serde_json::json!({ "plans": [] })).is_err());
    }

    #[test]
    fn share_ids_are_short_and_url_safe() {
        let share_id = random_share_id();
        assert_eq!(share_id.len(), 16);
        assert!(share_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(valid_share_id(&share_id));
        assert!(valid_share_id("A1b2C3d4E5"));
        assert!(!valid_share_id("../../secret"));
    }
}
