use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::AppState;

/// Game region a version row belongs to. Both regions share `master_versions`, so every
/// query has to name one or it would return whichever region wrote last.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Region {
    #[default]
    Global,
    Jp,
}

impl Region {
    fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Jp => "jp",
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct VersionParams {
    /// Global when omitted, so existing callers keep getting the Global version.
    #[serde(default)]
    pub region: Region,
}

#[derive(Debug, Serialize, FromRow)]
pub struct MasterVersion {
    pub app_version: String,
    pub resource_version: String,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Serialize)]
pub struct VersionResponse {
    pub app_version: String,
    pub resource_version: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct VersionHistoryResponse {
    pub current: VersionResponse,
    pub history: Vec<VersionResponse>,
}

/// Handler for GET /api/ver - returns a region's latest app & resource versions from the database
pub async fn get_version(
    State(state): State<AppState>,
    Query(params): Query<VersionParams>,
) -> Result<Json<VersionResponse>, StatusCode> {
    let row: MasterVersion = sqlx::query_as(
        "SELECT app_version, resource_version, updated_at FROM master_versions WHERE region = $1 ORDER BY id DESC LIMIT 1",
    )
    .bind(params.region.as_str())
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

/// Handler for GET /api/ver/history - returns a region's latest version plus its full history
pub async fn get_version_history(
    State(state): State<AppState>,
    Query(params): Query<VersionParams>,
) -> Result<Json<VersionHistoryResponse>, StatusCode> {
    let rows: Vec<MasterVersion> = sqlx::query_as(
        "SELECT app_version, resource_version, updated_at FROM master_versions WHERE region = $1 ORDER BY id DESC",
    )
    .bind(params.region.as_str())
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Uri;

    fn region_for(uri: &'static str) -> Option<Region> {
        Query::<VersionParams>::try_from_uri(&Uri::from_static(uri))
            .ok()
            .map(|Query(params)| params.region)
    }

    #[test]
    fn version_region_defaults_to_global() {
        assert_eq!(region_for("/api/ver"), Some(Region::Global));
        assert_eq!(region_for("/api/ver?region=global"), Some(Region::Global));
        assert_eq!(region_for("/api/ver/history?region=jp"), Some(Region::Jp));
    }

    #[test]
    fn unknown_version_region_is_rejected() {
        assert_eq!(region_for("/api/ver?region=kr"), None);
        assert_eq!(region_for("/api/ver?region=JP"), None);
    }

    #[test]
    fn region_names_match_the_stored_values() {
        assert_eq!(Region::Global.as_str(), "global");
        assert_eq!(Region::Jp.as_str(), "jp");
    }
}
