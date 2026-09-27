use sha2::{Digest, Sha256};

include!("types/redis_store.rs");

impl RedisStore {
    pub fn fromEnv() -> Result<Option<Self>, String> {
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

    pub fn hashedKey(&self, namespace: &str, value: &str) -> String {
        let digest = Sha256::digest(value.as_bytes());
        format!("{}:{}:{}", self.prefix, namespace, hex::encode(digest))
    }

    pub async fn getString(&self, key: &str) -> Result<Option<String>, String> {
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

    pub async fn setStringEx(
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

    pub async fn incrementWithExpiry(&self, key: &str, ttl_seconds: u64) -> Result<u64, String> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| error.to_string())?;

        let count: i64 = redis::cmd("INCR")
            .arg(key)
            .query_async(&mut connection)
            .await
            .map_err(|error| error.to_string())?;

        if count == 1 {
            redis::cmd("EXPIRE")
                .arg(key)
                .arg(ttl_seconds.max(1))
                .query_async::<()>(&mut connection)
                .await
                .map_err(|error| error.to_string())?;
        }

        Ok(count.max(0) as u64)
    }
}
