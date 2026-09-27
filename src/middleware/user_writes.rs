use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::AppState;

include!("../types/middleware/user_writes.rs");

pub async fn userWriteGuardMiddleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if state.user_writes_disabled && isUserWriteRequest(request.method(), request.uri().path()) {
        let body = Json(json!({
            "error": USER_WRITES_DISABLED_MESSAGE,
            "status": StatusCode::FORBIDDEN.as_u16(),
        }));
        return (StatusCode::FORBIDDEN, body).into_response();
    }

    next.run(request).await
}

fn isUserWriteRequest(method: &Method, path: &str) -> bool {
    if matches!(method, &Method::GET | &Method::HEAD | &Method::OPTIONS) {
        return isAuthFlowGet(path);
    }

    if method == Method::POST {
        return !isAllowedReadOnlyPost(path);
    }

    true
}

fn isAuthFlowGet(path: &str) -> bool {
    path.starts_with("/api/auth/login/")
        || path.starts_with("/api/auth/callback/")
        || path.starts_with("/api/auth/connect/")
}

fn isAllowedReadOnlyPost(path: &str) -> bool {
    matches!(path, "/api/auth/browser-proof" | "/api/v4/partner/lookup")
        || path.starts_with("/api/stats/friendlist/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocksAuthFlowGetsThatCanCreateSessionsOrIdentities() {
        assert!(isUserWriteRequest(&Method::GET, "/api/auth/login/google"));
        assert!(isUserWriteRequest(
            &Method::GET,
            "/api/auth/callback/google"
        ));
        assert!(isUserWriteRequest(&Method::GET, "/api/auth/connect/google"));
        assert!(isUserWriteRequest(
            &Method::GET,
            "/api/auth/connect/callback/google"
        ));
        assert!(!isUserWriteRequest(&Method::GET, "/api/auth/me"));
    }

    #[test]
    fn blocksMutatingMethodsByDefault() {
        assert!(isUserWriteRequest(&Method::POST, "/api/tasks/submit"));
        assert!(isUserWriteRequest(
            &Method::PUT,
            "/api/v4/user/profile/123/visibility"
        ));
        assert!(isUserWriteRequest(
            &Method::DELETE,
            "/api/auth/bookmarks/123"
        ));
    }

    #[test]
    fn allowsBackendGeneratedOrNonDbPosts() {
        assert!(!isUserWriteRequest(
            &Method::POST,
            "/api/auth/browser-proof"
        ));
        assert!(!isUserWriteRequest(&Method::POST, "/api/v4/partner/lookup"));
    }
}
