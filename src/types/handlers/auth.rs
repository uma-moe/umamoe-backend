// Included by the owning module to preserve private field visibility.

/// OAuth client with auth URL and token URL configured (type-state).
type OAuthClient =
    BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;

/// Simple error type for the OAuth HTTP client adapter.
#[derive(Debug)]
struct OAuthHttpError(String);

struct ProviderConfig {
    auth_url: &'static str,
    token_url: &'static str,
    scopes: Vec<&'static str>,
    userinfo_url: &'static str,
}

/// Parsed userinfo from any provider.
struct ProviderUserInfo {
    provider_user_id: String,
    display_name: Option<String>,
    email: Option<String>,
    email_verified: bool,
    avatar_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LoginParams {
    pub origin: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    pub code: String,
    pub state: String,
}

#[derive(Debug, Default, Deserialize)]
struct AddBookmarkRequest {
    borrow_key: Option<String>,
    support_card_id: Option<i32>,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Clone, Debug)]
struct BookmarkHashUpgrade {
    account_id: String,
    previous_hash: Option<String>,
    previous_borrow_key: Option<String>,
    next_hash: String,
    legacy_hash: Option<String>,
    support_card_id: Option<i32>,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct BulkRemoveBookmarksRequest {
    /// Specific account_ids to remove. Ignored when `all` is true.
    #[serde(default)]
    pub account_ids: Vec<String>,
    /// When true, remove every bookmark for the user.
    #[serde(default)]
    pub all: bool,
}
