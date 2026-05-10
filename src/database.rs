use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::str::FromStr;

pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let options = PgConnectOptions::from_str(database_url)?
        .application_name("honsemoe-backend")
        .statement_cache_capacity(500); // Increase statement cache

    PgPoolOptions::new()
        .max_connections(32) // Increase from default 10
        .min_connections(8) // Keep minimum connections ready
        .acquire_timeout(std::time::Duration::from_secs(2))
        .idle_timeout(std::time::Duration::from_secs(10))
        .test_before_acquire(false) // Disable if you trust connection stability
        .connect_with(options)
        .await
}
