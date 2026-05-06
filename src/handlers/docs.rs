use axum::{response::Html, routing::get, Router};

const SWAGGER_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>uma.moe API Documentation</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
    <style>
        html { box-sizing: border-box; overflow-y: scroll; }
        *, *::before, *::after { box-sizing: inherit; }
        body { margin: 0; background: #fafafa; }
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
        SwaggerUIBundle({
            url: '/api/docs/openapi.yaml',
            dom_id: '#swagger-ui',
            deepLinking: true,
            presets: [
                SwaggerUIBundle.presets.apis,
                SwaggerUIBundle.SwaggerUIStandalonePreset
            ],
            layout: 'BaseLayout'
        });
    </script>
</body>
</html>"#;

const OPENAPI_YAML: &str = include_str!("../../openapi.yaml");

async fn swagger_ui() -> Html<&'static str> {
    Html(SWAGGER_HTML)
}

async fn openapi_spec() -> ([(&'static str, &'static str); 1], &'static str) {
    ([("content-type", "text/yaml")], OPENAPI_YAML)
}

pub fn router() -> Router<crate::AppState> {
    Router::new()
        .route("/", get(swagger_ui))
        .route("/openapi.yaml", get(openapi_spec))
}
