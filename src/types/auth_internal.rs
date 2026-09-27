// Included by the owning module to preserve private field visibility.

type HmacSha256 = Hmac<Sha256>;

static JWT_SECRET: OnceLock<String> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: Uuid,  // user id
    pub exp: usize, // expiry (epoch seconds)
    pub iat: usize, // issued at
}
