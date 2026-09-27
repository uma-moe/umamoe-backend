mod bookmarks;
mod rankings;
mod retention;
mod stats;

use crate::config::{envFlag, envU64};
use crate::{cache, cheat_analysis};
use sqlx::{pool::PoolConnection, PgPool, Postgres};
use tracing::{error, info, warn};

include!("../../types/database/maintenance/mod.rs");

pub(super) async fn acquireMaintenanceConnection(
    pool: &PgPool,
    lock_name: &str,
) -> Option<PoolConnection<Postgres>> {
    let mut conn = match pool.acquire().await {
        Ok(conn) => conn,
        Err(error) => {
            warn!(
                "Skipping maintenance job {} because no DB connection was available: {}",
                lock_name, error
            );
            return None;
        }
    };

    match sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock(hashtext($1)::bigint)")
        .bind(lock_name)
        .fetch_one(&mut *conn)
        .await
    {
        Ok(true) => Some(conn),
        Ok(false) => {
            info!(
                "Skipping maintenance job {} because another backend owns the lock",
                lock_name
            );
            None
        }
        Err(error) => {
            warn!(
                "Skipping maintenance job {} because the advisory lock failed: {}",
                lock_name, error
            );
            None
        }
    }
}

pub(super) async fn releaseMaintenanceLock(conn: &mut PoolConnection<Postgres>, lock_name: &str) {
    if let Err(error) = sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
        .bind(lock_name)
        .execute(&mut **conn)
        .await
    {
        warn!(
            "Failed to release maintenance lock {}: {}",
            lock_name, error
        );
    }
}

pub(super) async fn currentTimeoutSettings(
    conn: &mut PoolConnection<Postgres>,
) -> Result<(String, String), sqlx::Error> {
    sqlx::query_as("SELECT current_setting('statement_timeout'), current_setting('lock_timeout')")
        .fetch_one(&mut **conn)
        .await
}

pub(super) async fn setMaintenanceTimeouts(
    conn: &mut PoolConnection<Postgres>,
    statement_timeout_seconds: u64,
    lock_timeout_seconds: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT set_config('statement_timeout', $1, false), set_config('lock_timeout', $2, false)",
    )
    .bind(format!("{}s", statement_timeout_seconds))
    .bind(format!("{}s", lock_timeout_seconds))
    .execute(&mut **conn)
    .await?;
    Ok(())
}

pub(super) async fn restoreTimeoutSettings(
    conn: &mut PoolConnection<Postgres>,
    previous_statement_timeout: &str,
    previous_lock_timeout: &str,
) {
    if let Err(error) = sqlx::query(
        "SELECT set_config('statement_timeout', $1, false), set_config('lock_timeout', $2, false)",
    )
    .bind(previous_statement_timeout)
    .bind(previous_lock_timeout)
    .execute(&mut **conn)
    .await
    {
        warn!("Failed to restore maintenance DB timeouts: {}", error);
    }
}

fn safeMaterializedViewName(view_name: &str) -> Option<&'static str> {
    match view_name {
        "stats_counts" => Some("stats_counts"),
        "circle_live_ranks" => Some("circle_live_ranks"),
        "user_fan_rankings_monthly_current" => Some("user_fan_rankings_monthly_current"),
        "user_fan_rankings_alltime" => Some("user_fan_rankings_alltime"),
        "user_fan_rankings_gains" => Some("user_fan_rankings_gains"),
        _ => None,
    }
}

// Background task that rebuilds the suspicious-activity analysis aggregates by
// streaming `circle_member_fan_snapshots` through the in-process Rust
// pipeline (see `cheat_analysis::run_full_rebuild`). The DB only stores
// the final aggregate tables; all sessionization / scoring happens here.
// Runs every hour.
async fn refreshCheatAnalysisTask(pool: PgPool) {
    info!("🔄 Starting suspicious-activity analysis refresh background task (runs every hour)");

    // Preload the in-memory /shame snapshot from the currently published
    // aggregate tables so requests are fast immediately after startup.
    match crate::shame::rebuildSnapshot(&pool).await {
        Ok(()) => info!("✅ initial shame snapshot loaded from aggregate tables"),
        Err(e) => warn!("⚠️ Failed to preload shame snapshot: {}", e),
    }

    // Delay first run a bit so startup migrations / other initial work finish first.
    tokio::time::sleep(tokio::time::Duration::from_secs(90)).await;

    loop {
        let Some(mut maintenance_lock) =
            acquireMaintenanceConnection(&pool, HEAVY_DATABASE_MAINTENANCE_LOCK).await
        else {
            warn!("Skipping suspicious-activity rebuild because heavy database maintenance is already running");
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
            continue;
        };

        let start = std::time::Instant::now();
        info!("▶ suspicious-activity analysis rebuild starting");
        match cheat_analysis::runFullRebuild(&pool).await {
            Ok(stats) => {
                info!(
                    "✅ suspicious-activity analysis refreshed: {} snapshots, {} viewers scored, last_snapshot_id={} in {} ms (wall {:.1}s)",
                    stats.snapshots_processed,
                    stats.viewers_scored,
                    stats.last_snapshot_id,
                    stats.duration_ms,
                    start.elapsed().as_secs_f64()
                );
            }
            Err(e) => warn!("⚠️ Failed to refresh suspicious-activity analysis: {}", e),
        }
        releaseMaintenanceLock(&mut maintenance_lock, HEAVY_DATABASE_MAINTENANCE_LOCK).await;

        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

/// Refresh a materialized view without letting every backend container run it at once.
pub(super) async fn refreshMatView(pool: &PgPool, view_name: &str) {
    let Some(view_name) = safeMaterializedViewName(view_name) else {
        warn!(
            "Refusing to refresh unknown materialized view {}",
            view_name
        );
        return;
    };

    let lock_name = HEAVY_DATABASE_MAINTENANCE_LOCK.to_string();
    let Some(mut conn) = acquireMaintenanceConnection(pool, &lock_name).await else {
        return;
    };

    let start = std::time::Instant::now();
    let sql_concurrent = format!("REFRESH MATERIALIZED VIEW CONCURRENTLY {}", view_name);
    let sql_full = format!("REFRESH MATERIALIZED VIEW {}", view_name);
    let previous_timeouts = currentTimeoutSettings(&mut conn).await.ok();
    if let Err(error) = setMaintenanceTimeouts(
        &mut conn,
        envU64("MATVIEW_REFRESH_STATEMENT_TIMEOUT_SECONDS", 300).max(30),
        envU64("MATVIEW_REFRESH_LOCK_TIMEOUT_SECONDS", 3).max(1),
    )
    .await
    {
        warn!(
            "Failed to set maintenance DB timeouts before refreshing {}: {}",
            view_name, error
        );
        releaseMaintenanceLock(&mut conn, &lock_name).await;
        return;
    }

    match sqlx::query(&sql_concurrent).execute(&mut *conn).await {
        Ok(_) => info!(
            "✅ {} refreshed in {:.1}s",
            view_name,
            start.elapsed().as_secs_f64()
        ),
        Err(error) if envFlag("MATVIEW_REFRESH_ALLOW_NONCONCURRENT") => {
            warn!(
                "Concurrent refresh failed for {}, trying non-concurrent refresh: {}",
                view_name, error
            );
            match sqlx::query(&sql_full).execute(&mut *conn).await {
                Ok(_) => info!(
                    "✅ {} refreshed (non-concurrent) in {:.1}s",
                    view_name,
                    start.elapsed().as_secs_f64()
                ),
                Err(e) => warn!("⚠️ Failed to refresh {}: {}", view_name, e),
            }
        }
        Err(error) => warn!("Failed to refresh {}: {}", view_name, error),
    }

    if let Some((statement_timeout, lock_timeout)) = previous_timeouts {
        restoreTimeoutSettings(&mut conn, &statement_timeout, &lock_timeout).await;
    }
    releaseMaintenanceLock(&mut conn, &lock_name).await;
}

pub(crate) fn spawnJobs(pool: &PgPool, user_writes_disabled: bool, skip_migrations: bool) {
    let db_background_jobs_disabled = skip_migrations
        || envFlag("DISABLE_DB_BACKGROUND_JOBS")
        || envFlag("DISABLE_DB_MAINTENANCE_TASKS");
    if user_writes_disabled || db_background_jobs_disabled {
        warn!(
            "Skipping DB-writing background jobs (user_writes_disabled={}, skip_or_disabled={})",
            user_writes_disabled, db_background_jobs_disabled
        );
    } else {
        // Start background task to refresh materialized views every hour
        tokio::spawn(stats::refreshStatsTask(pool.clone()));

        // Start background task to refresh circle live ranks every 5 minutes
        tokio::spawn(rankings::refreshCircleRanksTask(pool.clone()));

        // Start background task to refresh user fan rankings every hour
        tokio::spawn(rankings::refreshUserRankingsTask(pool.clone()));

        // Start background task to ensure daily stats entries exist
        tokio::spawn(stats::ensureDailyStatsTask(pool.clone()));

        // Start background task to clear stale live_points/live_rank every hour
        tokio::spawn(rankings::clearStaleLiveTask(pool.clone()));

        // Keep high-volume operational history and processed fan snapshots bounded.
        tokio::spawn(retention::runRetentionTask(pool.clone()));
    }

    // Start background task to clean up expired cache entries every 10 minutes
    tokio::spawn(cache::cleanupTask());

    if !(user_writes_disabled || db_background_jobs_disabled) {
        // Start background task to clean up short-lived borrow anti-spam buckets
        tokio::spawn(retention::cleanupBorrowInteractionBucketsV2Task(
            pool.clone(),
        ));

        // Start background task to incrementally refresh suspicious-activity analysis
        tokio::spawn(refreshCheatAnalysisTask(pool.clone()));
    }

    let bookmark_hash_backfill_disabled =
        skip_migrations || envFlag("DISABLE_BOOKMARK_HASH_BACKFILL");
    if bookmark_hash_backfill_disabled {
        warn!(
            "Skipping bookmark hash backfill (skip_migrations={}, disabled_by_env={})",
            skip_migrations,
            envFlag("DISABLE_BOOKMARK_HASH_BACKFILL")
        );
    } else {
        // This is a resumable schema-repair job, not a user write. It is safe to run on beta.
        tokio::spawn(bookmarks::backfillBookmarkHashesTask(pool.clone()));
    }
}
