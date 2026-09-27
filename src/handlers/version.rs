use axum::{extract::State, http::StatusCode, response::Json};
use serde::Serialize;
use sqlx::FromRow;

use crate::AppState;

include!("../types/handlers/version.rs");

/// Handler for GET /api/ver - returns the latest app & resource versions from the database
pub async fn getVersion(
    State(state): State<AppState>,
) -> Result<Json<VersionResponse>, StatusCode> {
    let row: MasterVersion = sqlx::query_as(
        "SELECT app_version, resource_version, updated_at FROM master_versions ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(VersionResponse {
        app_version: row.app_version,
        resource_version: row.resource_version,
        updated_at: row.updated_at.format("%Y-%m-%dT%H:%M:%S").to_string(),
    }))
}

/// Handler for GET /api/ver/history - returns the latest version plus full history
pub async fn getVersionHistory(
    State(state): State<AppState>,
) -> Result<Json<VersionHistoryResponse>, StatusCode> {
    let rows: Vec<MasterVersion> = sqlx::query_as(
        "SELECT app_version, resource_version, updated_at FROM master_versions ORDER BY id DESC",
    )
    .fetch_all(&state.db)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let first = rows.first().ok_or(StatusCode::NOT_FOUND)?;

    let current = VersionResponse {
        app_version: first.app_version.clone(),
        resource_version: first.resource_version.clone(),
        updated_at: first.updated_at.format("%Y-%m-%dT%H:%M:%S").to_string(),
    };

    let history = rows
        .into_iter()
        .skip(1)
        .map(|row| VersionResponse {
            app_version: row.app_version,
            resource_version: row.resource_version,
            updated_at: row.updated_at.format("%Y-%m-%dT%H:%M:%S").to_string(),
        })
        .collect();

    Ok(Json(VersionHistoryResponse { current, history }))
}
