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
use serde_json::{json, Value};
use sqlx::FromRow;
use uuid::Uuid;

use crate::{errors::AppError, middleware::auth::AuthenticatedUser, AppState};

include!("../types/handlers/carat_planner.rs");

pub fn publicRouter() -> Router<AppState> {
    Router::new().route("/shared/:share_id", get(getSharedPlan))
}

pub fn authenticatedRouter() -> Router<AppState> {
    Router::new()
        .route("/state", get(getState).put(putState))
        .route("/shares", post(upsertShare))
        .route("/shares/:plan_id", delete(deleteShare))
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

impl From<LegacyShareRow> for ShareResponse {
    fn from(row: LegacyShareRow) -> Self {
        Self {
            share_id: row.share_id,
            plan_id: row.plan_id,
            plan_name: row.plan_name,
            plan: Some(row.plan),
            collection: None,
            updated_at: row.updated_at,
        }
    }
}

async fn getState(
    user: AuthenticatedUser,
    State(state): State<AppState>,
) -> Result<Json<PlannerStateResponse>, AppError> {
    let row = loadState(&state, user.user_id).await?;
    Ok(Json(row.map(Into::into).unwrap_or(PlannerStateResponse {
        revision: 0,
        collection: None,
        updated_at: None,
    })))
}

async fn putState(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<PutPlannerStateRequest>,
) -> Result<Response, AppError> {
    if payload.base_revision < 0 {
        return Err(AppError::BadRequest(
            "base_revision cannot be negative".into(),
        ));
    }
    validateCollection(&payload.collection)?;

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
        if let Err(error) =
            promoteLegacyShareReferences(&state, user.user_id, &row.collection).await
        {
            tracing::warn!(
                user_id = %user.user_id,
                error = %error,
                "Planner state saved but legacy share cleanup will need a retry"
            );
        }
        return Ok((StatusCode::OK, Json(PlannerStateResponse::from(row))).into_response());
    }

    let current = loadState(&state, user.user_id)
        .await?
        .map(PlannerStateResponse::from)
        .unwrap_or(PlannerStateResponse {
            revision: 0,
            collection: None,
            updated_at: None,
        });
    Ok((StatusCode::CONFLICT, Json(current)).into_response())
}

async fn upsertShare(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<SharePlanRequest>,
) -> Result<(StatusCode, Json<ShareResponse>), AppError> {
    let snapshot_details = payload.plan.as_ref().map(validatePlan).transpose()?;
    let explicit_plan_id = payload.plan_id.as_deref().map(validatePlanId).transpose()?;
    let explicit_plan_name = payload
        .plan_name
        .as_deref()
        .map(validatePlanName)
        .transpose()?;
    if let (Some(explicit), Some((snapshot_id, _))) =
        (explicit_plan_id.as_deref(), snapshot_details.as_ref())
    {
        if explicit != snapshot_id {
            return Err(AppError::BadRequest(
                "Shared plan id does not match the supplied plan".into(),
            ));
        }
    }
    let is_reference_request = explicit_plan_id.is_some();
    let plan_id = explicit_plan_id
        .or_else(|| snapshot_details.as_ref().map(|(id, _)| id.clone()))
        .ok_or_else(|| AppError::BadRequest("Shared plan must contain a plan_id".into()))?;

    if let Some(referenced) = loadPlanForOwner(&state, user.user_id, &plan_id).await? {
        let share_id = upsertPlanReference(&state, user.user_id, &plan_id).await?;
        removeRedundantLegacySnapshot(&state, user.user_id, &plan_id, &share_id).await?;
        let mut response = referencedPlanResponse(share_id, referenced)?;
        // An old frontend expects `plan` in the POST response. Return its own
        // transient payload without persisting another copy.
        response.plan = payload.plan;
        return Ok((StatusCode::OK, Json(response)));
    }

    if is_reference_request {
        let plan_name = explicit_plan_name
            .or_else(|| snapshot_details.as_ref().map(|(_, name)| name.clone()))
            .ok_or_else(|| AppError::BadRequest("Shared plan must contain a plan_name".into()))?;
        let share_id = upsertPlanReference(&state, user.user_id, &plan_id).await?;
        return Ok((
            StatusCode::OK,
            Json(ShareResponse {
                share_id,
                plan_id,
                plan_name,
                plan: payload.plan,
                collection: None,
                updated_at: Utc::now(),
            }),
        ));
    }

    // Old clients may share a brand-new plan before their account state PUT has
    // completed. Preserve that legacy behavior as a compatibility fallback.
    if let (Some(plan), Some((_, plan_name))) = (payload.plan, snapshot_details) {
        let row = upsertLegacySnapshot(&state, user.user_id, &plan_id, &plan_name, plan).await?;
        return Ok((StatusCode::OK, Json(row.into())));
    }

    Err(AppError::BadRequest(
        "Save this plan to your account before sharing it".into(),
    ))
}

async fn getSharedPlan(
    State(state): State<AppState>,
    Path(share_id): Path<String>,
) -> Result<Json<ShareResponse>, AppError> {
    if !validShareId(&share_id) {
        return Err(AppError::NotFound("Shared plan not found".into()));
    }
    if let Some(row) = sqlx::query_as::<_, StoredPlanRow>(
        r#"
        SELECT reference.plan_id, planner.collection, planner.updated_at
        FROM carat_plan_references AS reference
        JOIN carat_planner_states AS planner ON planner.user_id = reference.user_id
        WHERE reference.share_id = $1
        "#,
    )
    .bind(&share_id)
    .fetch_optional(&state.db)
    .await?
    {
        if let Ok(response) = referencedPlanResponse(share_id.clone(), row) {
            return Ok(Json(response));
        }
    }

    let row = sqlx::query_as::<_, LegacyShareRow>(
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

async fn deleteShare(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Path(plan_id): Path<String>,
) -> Result<StatusCode, AppError> {
    if plan_id.is_empty() || plan_id.len() > 100 {
        return Err(AppError::BadRequest("Invalid plan id".into()));
    }
    sqlx::query("DELETE FROM carat_plan_references WHERE user_id = $1 AND plan_id = $2")
        .bind(user.user_id)
        .bind(&plan_id)
        .execute(&state.db)
        .await?;
    sqlx::query("DELETE FROM carat_plan_shares WHERE user_id = $1 AND plan_id = $2")
        .bind(user.user_id)
        .bind(&plan_id)
        .execute(&state.db)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn loadState(state: &AppState, user_id: Uuid) -> Result<Option<PlannerStateRow>, AppError> {
    Ok(sqlx::query_as::<_, PlannerStateRow>(
        "SELECT revision, collection, updated_at FROM carat_planner_states WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?)
}

async fn loadPlanForOwner(
    state: &AppState,
    user_id: Uuid,
    plan_id: &str,
) -> Result<Option<StoredPlanRow>, AppError> {
    let Some(row) = loadState(state, user_id).await? else {
        return Ok(None);
    };
    if projectPlanCollection(&row.collection, plan_id).is_none() {
        return Ok(None);
    }
    Ok(Some(StoredPlanRow {
        plan_id: plan_id.to_string(),
        collection: row.collection,
        updated_at: row.updated_at,
    }))
}

async fn upsertPlanReference(
    state: &AppState,
    user_id: Uuid,
    plan_id: &str,
) -> Result<String, AppError> {
    if let Some(share_id) = sqlx::query_scalar::<_, String>(
        "SELECT share_id FROM carat_plan_references WHERE user_id = $1 AND plan_id = $2",
    )
    .bind(user_id)
    .bind(plan_id)
    .fetch_optional(&state.db)
    .await?
    {
        return Ok(share_id);
    }

    let legacy_share_id = sqlx::query_scalar::<_, String>(
        "SELECT share_id FROM carat_plan_shares WHERE user_id = $1 AND plan_id = $2",
    )
    .bind(user_id)
    .bind(plan_id)
    .fetch_optional(&state.db)
    .await?;

    for attempt in 0..5 {
        let share_id = if attempt == 0 {
            legacy_share_id.clone().unwrap_or_else(randomShareId)
        } else {
            randomShareId()
        };
        let legacy_collision = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM carat_plan_shares WHERE share_id = $1)",
        )
        .bind(&share_id)
        .fetch_one(&state.db)
        .await?;
        if legacy_collision && legacy_share_id.as_deref() != Some(share_id.as_str()) {
            continue;
        }

        let result = sqlx::query_scalar::<_, String>(
            r#"
            INSERT INTO carat_plan_references (share_id, user_id, plan_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, plan_id) DO UPDATE SET
                plan_id = EXCLUDED.plan_id
            RETURNING share_id
            "#,
        )
        .bind(&share_id)
        .bind(user_id)
        .bind(plan_id)
        .fetch_one(&state.db)
        .await;

        match result {
            Ok(value) => return Ok(value),
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => continue,
            Err(error) => return Err(error.into()),
        }
    }

    Err(AppError::ServiceUnavailable(
        "Could not allocate a share link; please retry".into(),
    ))
}

async fn upsertLegacySnapshot(
    state: &AppState,
    user_id: Uuid,
    plan_id: &str,
    plan_name: &str,
    plan: Value,
) -> Result<LegacyShareRow, AppError> {
    let reference_id = sqlx::query_scalar::<_, String>(
        "SELECT share_id FROM carat_plan_references WHERE user_id = $1 AND plan_id = $2",
    )
    .bind(user_id)
    .bind(plan_id)
    .fetch_optional(&state.db)
    .await?;

    for attempt in 0..5 {
        let share_id = if attempt == 0 {
            reference_id.clone().unwrap_or_else(randomShareId)
        } else {
            randomShareId()
        };
        let reference_collision = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM carat_plan_references WHERE share_id = $1)",
        )
        .bind(&share_id)
        .fetch_one(&state.db)
        .await?;
        if reference_collision && reference_id.as_deref() != Some(share_id.as_str()) {
            continue;
        }

        let result = sqlx::query_as::<_, LegacyShareRow>(
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
        .bind(user_id)
        .bind(plan_id)
        .bind(plan_name)
        .bind(&plan)
        .fetch_one(&state.db)
        .await;

        match result {
            Ok(row) => return Ok(row),
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => continue,
            Err(error) => return Err(error.into()),
        }
    }

    Err(AppError::ServiceUnavailable(
        "Could not allocate a share link; please retry".into(),
    ))
}

async fn removeRedundantLegacySnapshot(
    state: &AppState,
    user_id: Uuid,
    plan_id: &str,
    share_id: &str,
) -> Result<(), AppError> {
    sqlx::query(
        r#"
        DELETE FROM carat_plan_shares
        WHERE user_id = $1 AND plan_id = $2 AND share_id = $3
          AND EXISTS (
              SELECT 1
              FROM carat_plan_references
              WHERE user_id = $1 AND plan_id = $2 AND share_id = $3
          )
        "#,
    )
    .bind(user_id)
    .bind(plan_id)
    .bind(share_id)
    .execute(&state.db)
    .await?;
    Ok(())
}

fn referencedPlanResponse(share_id: String, row: StoredPlanRow) -> Result<ShareResponse, AppError> {
    let (plan_name, collection) = projectPlanCollection(&row.collection, &row.plan_id)
        .ok_or_else(|| AppError::NotFound("Shared plan not found".into()))?;
    Ok(ShareResponse {
        share_id,
        plan_id: row.plan_id,
        plan_name,
        plan: None,
        collection: Some(collection),
        updated_at: row.updated_at,
    })
}

fn projectPlanCollection(collection: &Value, plan_id: &str) -> Option<(String, Value)> {
    let object = collection.as_object()?;
    let version = object.get("version")?.clone();
    let plan = object
        .get("plans")?
        .as_array()?
        .iter()
        .find(|plan| storedPlanId(plan) == Some(plan_id))?
        .clone();
    let name = storedPlanName(&version, &plan)?.to_string();
    Some((
        name,
        json!({
            "version": version,
            "activePlanId": plan_id,
            "plans": [plan],
        }),
    ))
}

fn storedPlanId(plan: &Value) -> Option<&str> {
    match plan {
        Value::Object(object) => object.get("id")?.as_str(),
        Value::Array(values) => values.first()?.as_str(),
        _ => None,
    }
}

fn storedPlanName<'a>(version: &Value, plan: &'a Value) -> Option<&'a str> {
    match plan {
        Value::Object(object) => object.get("name")?.as_str(),
        Value::Array(values) if version.as_i64() == Some(2) => {
            values.get(5)?.as_array()?.get(1)?.as_str()
        }
        Value::Array(values) => values.get(1)?.as_str(),
        _ => None,
    }
}

async fn promoteLegacyShareReferences(
    state: &AppState,
    user_id: Uuid,
    collection: &Value,
) -> Result<(), AppError> {
    let plan_ids: Vec<String> = collection
        .get("plans")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(storedPlanId)
        .map(str::to_string)
        .collect();
    if plan_ids.is_empty() {
        return Ok(());
    }

    let mut transaction = state.db.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO carat_plan_references (share_id, user_id, plan_id)
        SELECT share_id, user_id, plan_id
        FROM carat_plan_shares
        WHERE user_id = $1 AND plan_id = ANY($2)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(user_id)
    .bind(&plan_ids)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        r#"
        DELETE FROM carat_plan_shares AS share
        USING carat_plan_references AS reference
        WHERE share.user_id = $1
          AND share.plan_id = ANY($2)
          AND reference.share_id = share.share_id
          AND reference.user_id = share.user_id
          AND reference.plan_id = share.plan_id
        "#,
    )
    .bind(user_id)
    .bind(&plan_ids)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

fn validateCollection(collection: &Value) -> Result<(), AppError> {
    let object = collection
        .as_object()
        .ok_or_else(|| AppError::BadRequest("Planner collection must be an object".into()))?;
    if object.get("version").and_then(Value::as_i64) != Some(3) {
        return Err(AppError::BadRequest(
            "Planner collection must use sparse storage version 3".into(),
        ));
    }
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
    validateJsonSize(collection, MAX_COLLECTION_BYTES, "Planner collection")
}

fn validatePlan(plan: &Value) -> Result<(String, String), AppError> {
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
    validateJsonSize(plan, MAX_SHARE_BYTES, "Shared plan")?;
    Ok((plan_id.to_string(), plan_name.to_string()))
}

fn validatePlanId(value: &str) -> Result<String, AppError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 100 {
        return Err(AppError::BadRequest(
            "Shared plan has an invalid plan_id".into(),
        ));
    }
    Ok(value.to_string())
}

fn validatePlanName(value: &str) -> Result<String, AppError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 80 {
        return Err(AppError::BadRequest(
            "Shared plan has an invalid plan_name".into(),
        ));
    }
    Ok(value.to_string())
}

fn validateJsonSize(value: &Value, max_bytes: usize, label: &str) -> Result<(), AppError> {
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

fn randomShareId() -> String {
    let mut bytes = [0_u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn validShareId(value: &str) -> bool {
    let legacy_id =
        (8..=12).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_alphanumeric());
    let hex_id = value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    legacy_id || hex_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validatesCompactPlannerCollections() {
        let collection = serde_json::json!({
            "version": 3,
            "activePlanId": "plan-1",
            "plans": [["plan-1", "My plan", 0, 0, 0, [], [], [], [], [], [], [], [], [], []]]
        });
        assert!(validateCollection(&collection).is_ok());
        assert!(validateCollection(&serde_json::json!({
            "version": 1,
            "activePlanId": "plan-1",
            "plans": [{ "id": "plan-1", "name": "Legacy" }]
        }))
        .is_err());
        assert!(validateCollection(&serde_json::json!({ "plans": [] })).is_err());
    }

    #[test]
    fn projectsOnlyTheReferencedSparsePlan() {
        let collection = serde_json::json!({
            "version": 3,
            "activePlanId": "private-plan",
            "plans": [
                ["shared-plan", "Shared", 0, 0, 0, [], [], [], [], [], [], [], [], [], []],
                ["private-plan", "Private", 0, 0, 0, [], [], [], [], [], [], [], [], [], []]
            ]
        });

        let (name, projected) = projectPlanCollection(&collection, "shared-plan").unwrap();
        assert_eq!(name, "Shared");
        assert_eq!(projected["activePlanId"], "shared-plan");
        assert_eq!(projected["plans"].as_array().unwrap().len(), 1);
        assert_eq!(projected["plans"][0][0], "shared-plan");
    }

    #[test]
    fn keepsLegacyCollectionsProjectableDuringMigration() {
        let v1 = serde_json::json!({
            "version": 1,
            "activePlanId": "plan-1",
            "plans": [{ "id": "plan-1", "name": "Legacy full plan" }]
        });
        let v2 = serde_json::json!({
            "version": 2,
            "activePlanId": "plan-2",
            "plans": [["plan-2", "created", "updated", [], [], [2, "Legacy tuple"]]]
        });

        assert_eq!(
            projectPlanCollection(&v1, "plan-1").unwrap().0,
            "Legacy full plan"
        );
        assert_eq!(
            projectPlanCollection(&v2, "plan-2").unwrap().0,
            "Legacy tuple"
        );
    }

    #[test]
    fn shareIdsAreShortAndUrlSafe() {
        let share_id = randomShareId();
        assert_eq!(share_id.len(), 16);
        assert!(share_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(validShareId(&share_id));
        assert!(validShareId("A1b2C3d4E5"));
        assert!(!validShareId("../../secret"));
    }
}
