// Included by the owning module to preserve private field visibility.

const TURNSTILE_VERIFY_URL: &str = "https://challenges.cloudflare.com/turnstile/v0/siteverify";

const BROWSER_PROOF_COOKIE: &str = "uma_browser_proof";

const BROWSER_WARMUP_COOKIE: &str = "uma_browser_warmup";

const BROWSER_PROOF_HEADER: &str = "X-Browser-Proof";

const BROWSER_PROOF_TTL_HEADER: &str = "X-Browser-Proof-TTL";

const BROWSER_PROOF_SOURCE_HEADER: &str = "X-Browser-Proof-Source";

const BROWSER_PROOF_AUDIENCE: &str = "uma-api";

const BROWSER_PROOF_TYPE: &str = "browser_proof";

const BROWSER_PROOF_SOURCE_TURNSTILE: &str = "turnstile";

const BROWSER_PROOF_SOURCE_WARMUP: &str = "warmup";

const DEFAULT_TURNSTILE_ACTION: &str = "api_request";

static RATE_LIMITS: OnceLock<DashMap<String, RateWindow>> = OnceLock::new();

#[derive(Debug, Clone, Copy)]
struct RateWindow {
    count: u32,
    reset_at: Instant,
}

#[derive(Debug, Clone)]
struct IssuedBrowserProof {
    token: String,
    ttl_seconds: usize,
    subject: String,
    source: &'static str,
    warmup_marker: Option<String>,
}

#[derive(Debug)]
struct BrowserAuthorization {
    credential: &'static str,
    subject: Option<String>,
    proof_source: Option<String>,
    issued_proof: Option<IssuedBrowserProof>,
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct TurnstileVerifyRequest {
    secret: String,
    response: String,
    remoteip: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TurnstileVerifyResponse {
    success: bool,
    #[serde(rename = "error-codes")]
    error_codes: Option<Vec<String>>,
    hostname: Option<String>,
    action: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct InternalCredentialVerificationRequest {
    method: Option<String>,
    path: Option<String>,
    origin: Option<String>,
    referer: Option<String>,
    host: Option<String>,
    record_usage: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct InternalBrowserProofRequest {
    origin: Option<String>,
    referer: Option<String>,
    host: Option<String>,
    client_ip: Option<String>,
    user_agent: Option<String>,
    warmup_marker: Option<String>,
}

#[derive(Debug, Serialize)]
struct InternalCredentialVerificationResponse {
    valid: bool,
    credential: &'static str,
    message: &'static str,
    usage_recorded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<Uuid>,
    context: InternalVerificationContext,
    api_key: Option<InternalApiKeyVerification>,
    browser_proof: Option<InternalBrowserProofVerification>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct InternalVerificationContext {
    method: String,
    path: String,
    endpoint: String,
    origin: Option<String>,
    referer: Option<String>,
    host: Option<String>,
    client_ip: String,
    allowed_browser_context: bool,
    context_host: Option<String>,
}

#[derive(Debug, Serialize)]
struct InternalApiKeyVerification {
    id: Uuid,
    user_id: Uuid,
    name: String,
    usage_recorded: bool,
}

#[derive(Debug, Serialize)]
struct InternalBrowserProofVerification {
    subject: String,
    user_id: Option<Uuid>,
    issued_at: usize,
    expires_at: usize,
    action: String,
    host: String,
    source: String,
    context_matches_proof: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserProofClaims {
    typ: String,
    jti: String,
    sub: String,
    uid: Option<Uuid>,
    iat: usize,
    exp: usize,
    aud: String,
    action: String,
    host: String,
    #[serde(default = "defaultBrowserProofSource")]
    source: String,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedBrowserProof {
    proof_id: String,
    subject: String,
    issued_at: usize,
}

#[derive(Debug)]
enum TurnstileError {
    MissingSecret,
    Request(String),
}

#[derive(Debug)]
enum BrowserProofError {
    Invalid(String),
    Store(String),
}
