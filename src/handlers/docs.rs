use axum::{response::Html, routing::get, Router};

include!("../types/handlers/docs.rs");

async fn swaggerUi() -> Html<&'static str> {
    Html(SWAGGER_HTML)
}

async fn openapiSpec() -> ([(&'static str, &'static str); 1], &'static str) {
    ([("content-type", "text/yaml")], OPENAPI_YAML)
}

pub fn router() -> Router<crate::AppState> {
    Router::new()
        .route("/", get(swaggerUi))
        .route("/openapi.yaml", get(openapiSpec))
}
