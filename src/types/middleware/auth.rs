// Included by the owning module to preserve private field visibility.

/// Authenticated user extracted from a valid JWT `Authorization: Bearer <token>`
/// header **or** a valid `X-API-Key` header (resolved to the key's owner).
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: Uuid,
}

/// Rejection returned when the JWT is missing or invalid.
pub struct AuthRejection {
    message: String,
    status: StatusCode,
}

/// Like [`AuthenticatedUser`] but does not reject when no credentials are
/// supplied. Returns `Some(user)` if a valid JWT or API key is present,
/// `None` otherwise. Useful for endpoints that have anonymous fallback
/// behaviour (e.g. partner lookup without persistence).
#[derive(Debug, Clone)]
pub struct OptionalUser(pub Option<AuthenticatedUser>);
