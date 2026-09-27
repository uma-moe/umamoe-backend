pub use crate::shame::{HallParams, HallResponse, ViewerReport, ViewerReportParams};
use crate::{errors::AppError, shame, AppState};
use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::get,
    Router,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/hall", get(getHallOfShame))
        .route("/viewer/:viewer_id", get(getViewerReport))
}

async fn getHallOfShame(
    Query(params): Query<HallParams>,
    State(state): State<AppState>,
) -> Result<Json<HallResponse>, AppError> {
    shame::hall(&state.db, params).await.map(Json)
}

async fn getViewerReport(
    Path(viewer_id): Path<i64>,
    Query(params): Query<ViewerReportParams>,
    State(state): State<AppState>,
) -> Result<Json<ViewerReport>, AppError> {
    shame::viewerReport(&state.db, viewer_id, params)
        .await
        .map(Json)
}
