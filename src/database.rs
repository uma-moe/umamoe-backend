use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::str::FromStr;
use std::time::Duration;

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

pub async fn create_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let idle_in_transaction_timeout_seconds =
        env_u64("DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS", 300).max(30);
    let idle_in_transaction_timeout = format!("{idle_in_transaction_timeout_seconds}s");
    let options = PgConnectOptions::from_str(database_url)?
        .application_name("honsemoe-backend")
        .statement_cache_capacity(500)
        // Never let an abandoned transaction retain table locks indefinitely.
        // PostgreSQL terminates only the affected session; SQLx replaces it.
        .options([(
            "idle_in_transaction_session_timeout",
            idle_in_transaction_timeout,
        )]);

    let max_connections = env_u32("DATABASE_MAX_CONNECTIONS", 16).max(1);
    let min_connections = env_u32("DATABASE_MIN_CONNECTIONS", 2).min(max_connections);
    let acquire_timeout_seconds = env_u64("DATABASE_ACQUIRE_TIMEOUT_SECONDS", 2).max(1);
    let idle_timeout_seconds = env_u64("DATABASE_IDLE_TIMEOUT_SECONDS", 60).max(10);

    PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(Duration::from_secs(acquire_timeout_seconds))
        .idle_timeout(Duration::from_secs(idle_timeout_seconds))
        .test_before_acquire(false) // Disable if you trust connection stability
        .connect_with(options)
        .await
}
