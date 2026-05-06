use axum::{
    extract::{Query, State},
    http::header,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use std::collections::HashMap;

use crate::{errors::Result, AppState};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tree", get(affinity_tree))
        .route("/data", get(affinity_data))
}

async fn affinity_tree(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>> {
    let qs: String = params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&");

    let url = format!("{}/affinity/tree?{}", state.search_url, qs);

    let resp = state
        .search_client
        .get(&url)
        .send()
        .await
        .map_err(|e| crate::errors::AppError::BadRequest(format!("search service error: {}", e)))?
        .error_for_status()
        .map_err(|e| crate::errors::AppError::BadRequest(format!("search service error: {}", e)))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| crate::errors::AppError::BadRequest(format!("invalid response: {}", e)))?;

    Ok(Json(resp))
}

async fn affinity_data(State(state): State<AppState>) -> Result<impl IntoResponse> {
    let url = format!("{}/affinity/data", state.search_url);

    let resp = state
        .search_client
        .get(&url)
        .send()
        .await
        .map_err(|e| crate::errors::AppError::BadRequest(format!("search service error: {}", e)))?
        .error_for_status()
        .map_err(|e| crate::errors::AppError::BadRequest(format!("search service error: {}", e)))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| crate::errors::AppError::BadRequest(format!("invalid response: {}", e)))?;

    Ok((
        [(
            header::CACHE_CONTROL,
            "public, max-age=86400, s-maxage=86400, stale-while-revalidate=3600",
        )],
        Json(resp),
    ))
}
