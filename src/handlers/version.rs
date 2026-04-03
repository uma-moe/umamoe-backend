use axum::{http::StatusCode, response::Json};
use serde_json::Value;
use tokio::fs;

/// Handler for GET /api/ver - serves /root/version.json from the server
pub async fn get_version() -> Result<Json<Value>, StatusCode> {
    let content = fs::read_to_string("/root/version.json")
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;

    let json: Value = serde_json::from_str(&content)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json))
}
