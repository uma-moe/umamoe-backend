use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct RedisStore {
    client: redis::Client,
    prefix: String,
}

impl RedisStore {
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some(url) = std::env::var("REDIS_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };

        let client = redis::Client::open(url).map_err(|error| error.to_string())?;
        let prefix = std::env::var("REDIS_KEY_PREFIX")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "umamoe".to_string());

        Ok(Some(Self { client, prefix }))
    }

    pub fn hashed_key(&self, namespace: &str, value: &str) -> String {
        let digest = Sha256::digest(value.as_bytes());
        format!("{}:{}:{}", self.prefix, namespace, hex::encode(digest))
    }

    pub async fn get_string(&self, key: &str) -> Result<Option<String>, String> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| error.to_string())?;

        redis::cmd("GET")
            .arg(key)
            .query_async(&mut connection)
            .await
            .map_err(|error| error.to_string())
    }

    pub async fn set_string_ex(
        &self,
        key: &str,
        value: &str,
        ttl_seconds: u64,
    ) -> Result<(), String> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| error.to_string())?;

        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("EX")
            .arg(ttl_seconds.max(1))
            .query_async(&mut connection)
            .await
            .map_err(|error| error.to_string())
    }
}
