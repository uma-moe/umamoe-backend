pub(crate) mod maintenance;
pub(crate) mod migrations;

use crate::config::{envU32, envU64};
use tracing::warn;

use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::str::FromStr;
use std::time::Duration;

pub async fn createPool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let idle_in_transaction_timeout_seconds =
        envU64("DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS", 300).max(30);
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

    let max_connections = envU32("DATABASE_MAX_CONNECTIONS", 16).max(1);
    let min_connections = envU32("DATABASE_MIN_CONNECTIONS", 2).min(max_connections);
    let acquire_timeout_seconds = envU64("DATABASE_ACQUIRE_TIMEOUT_SECONDS", 2).max(1);
    let idle_timeout_seconds = envU64("DATABASE_IDLE_TIMEOUT_SECONDS", 60).max(10);

    PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(Duration::from_secs(acquire_timeout_seconds))
        .idle_timeout(Duration::from_secs(idle_timeout_seconds))
        .test_before_acquire(false) // Disable if you trust connection stability
        .connect_with(options)
        .await
}

pub(crate) async fn logDatabaseWriteState(
    pool: &PgPool,
    require_writable: bool,
) -> anyhow::Result<()> {
    let (in_recovery, transaction_read_only, current_user, current_database) =
        sqlx::query_as::<_, (bool, String, String, String)>(
            "SELECT pg_is_in_recovery(), current_setting('transaction_read_only'), current_user, current_database()",
        )
        .fetch_one(pool)
        .await?;

    let is_read_only = in_recovery || transaction_read_only.eq_ignore_ascii_case("on");
    if is_read_only {
        warn!(
            "Database connection is read-only: database={}, user={}, pg_is_in_recovery={}, transaction_read_only={}",
            current_database, current_user, in_recovery, transaction_read_only
        );
        if require_writable {
            anyhow::bail!("REQUIRE_WRITABLE_DATABASE=true but PostgreSQL connection is read-only");
        }
    } else {
        tracing::info!(
            "Database connection is writable: database={}, user={}, pg_is_in_recovery={}, transaction_read_only={}",
            current_database, current_user, in_recovery, transaction_read_only
        );
    }

    Ok(())
}
