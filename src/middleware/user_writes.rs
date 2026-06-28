use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::AppState;

pub const USER_WRITES_DISABLED_MESSAGE: &str = "This environment is read-only for user writes.";

pub async fn user_write_guard_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if state.user_writes_disabled && is_user_write_request(request.method(), request.uri().path()) {
        let body = Json(json!({
            "error": USER_WRITES_DISABLED_MESSAGE,
            "status": StatusCode::FORBIDDEN.as_u16(),
        }));
        return (StatusCode::FORBIDDEN, body).into_response();
    }

    next.run(request).await
}

fn is_user_write_request(method: &Method, path: &str) -> bool {
    if matches!(method, &Method::GET | &Method::HEAD | &Method::OPTIONS) {
        return is_auth_flow_get(path);
    }

    if method == Method::POST {
        return !is_allowed_read_only_post(path);
    }

    true
}

fn is_auth_flow_get(path: &str) -> bool {
    path.starts_with("/api/auth/login/")
        || path.starts_with("/api/auth/callback/")
        || path.starts_with("/api/auth/connect/")
}

fn is_allowed_read_only_post(path: &str) -> bool {
    matches!(path, "/api/auth/browser-proof" | "/api/v4/partner/lookup")
        || path.starts_with("/api/stats/friendlist/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_auth_flow_gets_that_can_create_sessions_or_identities() {
        assert!(is_user_write_request(
            &Method::GET,
            "/api/auth/login/google"
        ));
        assert!(is_user_write_request(
            &Method::GET,
            "/api/auth/callback/google"
        ));
        assert!(is_user_write_request(
            &Method::GET,
            "/api/auth/connect/google"
        ));
        assert!(is_user_write_request(
            &Method::GET,
            "/api/auth/connect/callback/google"
        ));
        assert!(!is_user_write_request(&Method::GET, "/api/auth/me"));
    }

    #[test]
    fn blocks_mutating_methods_by_default() {
        assert!(is_user_write_request(&Method::POST, "/api/tasks/submit"));
        assert!(is_user_write_request(
            &Method::PUT,
            "/api/v4/user/profile/123/visibility"
        ));
        assert!(is_user_write_request(
            &Method::DELETE,
            "/api/auth/bookmarks/123"
        ));
    }

    #[test]
    fn allows_backend_generated_or_non_db_posts() {
        assert!(!is_user_write_request(
            &Method::POST,
            "/api/auth/browser-proof"
        ));
        assert!(!is_user_write_request(
            &Method::POST,
            "/api/v4/partner/lookup"
        ));
    }
}
