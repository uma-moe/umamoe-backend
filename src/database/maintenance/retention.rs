use crate::config::envU64;
use sqlx::{pool::PoolConnection, PgPool, Postgres};
use tracing::{info, warn};

include!("../../types/database/maintenance/retention.rs");

impl RetentionConfig {
    fn fromEnv() -> Self {
        Self {
            // stats_counts exposes a rolling 24-hour task total. Keep a twelve-hour
            // cushion so an hourly refresh never loses rows on the cutoff boundary.
            completed_task_hours: envU64("COMPLETED_TASK_RETENTION_HOURS", 36).clamp(24, 24 * 30),
            failed_task_days: envU64("FAILED_TASK_RETENTION_DAYS", 14).clamp(1, 365),
            // Raw attempts are diagnostic telemetry. Aggregated health tables are the
            // durable representation; two days is enough for incident investigation.
            task_attempt_hours: envU64("TASK_ATTEMPT_RETENTION_HOURS", 48).clamp(24, 24 * 30),
            // Hard caps keep an old server env file from restoring the original
            // aggressive 25k x 20 catch-up loop after this safety fix deploys.
            batch_size: envU64("DATABASE_RETENTION_BATCH_SIZE", 5_000).clamp(1_000, 5_000) as i64,
            max_batches_per_table: envU64("DATABASE_RETENTION_MAX_BATCHES", 2).clamp(1, 2),
            batch_delay_ms: envU64("DATABASE_RETENTION_BATCH_DELAY_MILLISECONDS", 2_000)
                .clamp(250, 30_000),
            interval_seconds: envU64("DATABASE_RETENTION_INTERVAL_SECONDS", 15 * 60)
                .clamp(60, 24 * 60 * 60),
            start_delay_seconds: envU64("DATABASE_RETENTION_START_DELAY_SECONDS", 5 * 60)
                .clamp(5, 24 * 60 * 60),
            statement_timeout_seconds: envU64("DATABASE_RETENTION_STATEMENT_TIMEOUT_SECONDS", 30)
                .clamp(5, 15 * 60),
            lock_timeout_seconds: envU64("DATABASE_RETENTION_LOCK_TIMEOUT_SECONDS", 2).clamp(1, 60),
        }
    }
}

impl RetentionStats {
    fn total(&self) -> u64 {
        self.completed_tasks + self.failed_tasks + self.task_attempts
    }
}

pub(crate) async fn runRetentionTask(pool: PgPool) {
    let config = RetentionConfig::fromEnv();
    info!(
        "Starting database retention (completed_tasks={}h, failed_tasks={}d, task_attempts={}h, batch={}, max_batches={}, batch_delay={}ms, interval={}s)",
        config.completed_task_hours,
        config.failed_task_days,
        config.task_attempt_hours,
        config.batch_size,
        config.max_batches_per_table,
        config.batch_delay_ms,
        config.interval_seconds,
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(config.start_delay_seconds)).await;

    loop {
        match runRetentionCycle(&pool, config).await {
            Ok(stats) if stats.total() > 0 => info!(
                "Database retention deleted {} rows (completed_tasks={}, failed_tasks={}, task_attempts={})",
                stats.total(),
                stats.completed_tasks,
                stats.failed_tasks,
                stats.task_attempts,
            ),
            Ok(_) => info!("Database retention found no expired rows"),
            Err(error) => warn!("Database retention cycle failed: {}", error),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(config.interval_seconds)).await;
    }
}

async fn runRetentionCycle(
    pool: &PgPool,
    config: RetentionConfig,
) -> Result<RetentionStats, sqlx::Error> {
    let lock_name = super::HEAVY_DATABASE_MAINTENANCE_LOCK;
    let Some(mut conn) = super::acquireMaintenanceConnection(pool, lock_name).await else {
        return Ok(RetentionStats::default());
    };

    let previous_timeouts = super::currentTimeoutSettings(&mut conn).await.ok();
    let result = async {
        super::setMaintenanceTimeouts(
            &mut conn,
            config.statement_timeout_seconds,
            config.lock_timeout_seconds,
        )
        .await?;

        Ok(RetentionStats {
            completed_tasks: deleteCompletedTasks(&mut conn, config).await?,
            failed_tasks: deleteFailedTasks(&mut conn, config).await?,
            task_attempts: deleteTaskAttempts(&mut conn, config).await?,
        })
    }
    .await;

    if let Some((statement_timeout, lock_timeout)) = previous_timeouts {
        super::restoreTimeoutSettings(&mut conn, &statement_timeout, &lock_timeout).await;
    }
    super::releaseMaintenanceLock(&mut conn, lock_name).await;
    result
}

async fn deleteCompletedTasks(
    conn: &mut PoolConnection<Postgres>,
    config: RetentionConfig,
) -> Result<u64, sqlx::Error> {
    deleteInBatches(conn, config, |conn, batch_size| {
        Box::pin(async move {
            sqlx::query(
                r#"
                WITH doomed AS (
                    SELECT id
                    FROM tasks
                    WHERE status = 'completed'
                      AND updated_at < CURRENT_TIMESTAMP
                          - ($1::bigint * interval '1 hour')
                    ORDER BY updated_at, id
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM tasks AS task
                USING doomed
                WHERE task.id = doomed.id
                "#,
            )
            .bind(config.completed_task_hours as i64)
            .bind(batch_size)
            .execute(&mut **conn)
            .await
            .map(|result| result.rows_affected())
        })
    })
    .await
}

async fn deleteFailedTasks(
    conn: &mut PoolConnection<Postgres>,
    config: RetentionConfig,
) -> Result<u64, sqlx::Error> {
    deleteInBatches(conn, config, |conn, batch_size| {
        Box::pin(async move {
            sqlx::query(
                r#"
                WITH doomed AS (
                    SELECT id
                    FROM tasks
                    WHERE status = 'failed'
                      AND COALESCE(updated_at, created_at) < CURRENT_TIMESTAMP
                          - ($1::bigint * interval '1 day')
                    ORDER BY COALESCE(updated_at, created_at), id
                    LIMIT $2
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM tasks AS task
                USING doomed
                WHERE task.id = doomed.id
                "#,
            )
            .bind(config.failed_task_days as i64)
            .bind(batch_size)
            .execute(&mut **conn)
            .await
            .map(|result| result.rows_affected())
        })
    })
    .await
}

async fn deleteTaskAttempts(
    conn: &mut PoolConnection<Postgres>,
    config: RetentionConfig,
) -> Result<u64, sqlx::Error> {
    let table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.task_attempts') IS NOT NULL")
            .fetch_one(&mut **conn)
            .await?;
    if !table_exists {
        return Ok(0);
    }

    // Keep selection and deletion as two bounded statements. On large attempt
    // tables PostgreSQL otherwise prefers a hash join that sequentially scans
    // the entire table for every small DELETE batch. The first statement uses
    // idx_task_attempts_created_at; the second uses the primary key.
    let mut total = 0;
    for batch_index in 0..config.max_batches_per_table {
        let ids: Vec<i64> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM task_attempts
            WHERE created_at < CURRENT_TIMESTAMP
                - ($1::bigint * interval '1 hour')
            ORDER BY created_at, id
            LIMIT $2
            "#,
        )
        .bind(config.task_attempt_hours as i64)
        .bind(config.batch_size)
        .fetch_all(&mut **conn)
        .await?;

        if ids.is_empty() {
            break;
        }

        let selected = ids.len() as u64;
        let deleted = sqlx::query("DELETE FROM task_attempts WHERE id = ANY($1::bigint[])")
            .bind(&ids)
            .execute(&mut **conn)
            .await?
            .rows_affected();
        total += deleted;

        if selected < config.batch_size as u64 {
            break;
        }
        if batch_index + 1 < config.max_batches_per_table {
            tokio::time::sleep(tokio::time::Duration::from_millis(config.batch_delay_ms)).await;
        }
    }
    Ok(total)
}

async fn deleteInBatches<F>(
    conn: &mut PoolConnection<Postgres>,
    config: RetentionConfig,
    mut delete_batch: F,
) -> Result<u64, sqlx::Error>
where
    F: for<'a> FnMut(
        &'a mut PoolConnection<Postgres>,
        i64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<u64, sqlx::Error>> + Send + 'a>,
    >,
{
    let mut total = 0;
    for batch_index in 0..config.max_batches_per_table {
        let deleted = delete_batch(conn, config.batch_size).await?;
        total += deleted;
        if deleted < config.batch_size as u64 {
            break;
        }
        if batch_index + 1 < config.max_batches_per_table {
            tokio::time::sleep(tokio::time::Duration::from_millis(config.batch_delay_ms)).await;
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retentionDefaultsPreserveTheRollingDayMetric() {
        let config = RetentionConfig {
            completed_task_hours: 36,
            failed_task_days: 14,
            task_attempt_hours: 48,
            batch_size: 5_000,
            max_batches_per_table: 2,
            batch_delay_ms: 2_000,
            interval_seconds: 15 * 60,
            start_delay_seconds: 5 * 60,
            statement_timeout_seconds: 30,
            lock_timeout_seconds: 2,
        };

        assert!(config.completed_task_hours >= 24);
        assert!(config.batch_size <= 5_000);
        assert!(config.max_batches_per_table <= 2);
    }
}

pub(super) async fn cleanupBorrowInteractionBucketsV2Task(pool: PgPool) {
    let retention_seconds =
        envU64("BORROW_INTERACTION_BUCKET_RETENTION_SECONDS", 24 * 60 * 60).max(600);
    let interval_seconds = envU64(
        "BORROW_INTERACTION_BUCKET_CLEANUP_INTERVAL_SECONDS",
        60 * 60,
    )
    .max(300);
    let trend_retention_days = envU64("BORROW_INTERACTION_TREND_RETENTION_DAYS", 14).max(7);

    info!(
        "Starting borrow interaction cleanup task (bucket_retention={}s, trend_retention={}d, interval={}s)",
        retention_seconds, trend_retention_days, interval_seconds
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(180)).await;

    loop {
        let result = sqlx::query(
            r#"
            DELETE FROM borrow_interaction_buckets_v2
            WHERE bucket_start < NOW() - ($1::bigint * interval '1 second')
            "#,
        )
        .bind(retention_seconds as i64)
        .execute(&pool)
        .await;

        match result {
            Ok(result) => {
                if result.rows_affected() > 0 {
                    info!(
                        "Cleaned up {} expired borrow interaction bucket rows",
                        result.rows_affected()
                    );
                }
            }
            Err(error) => warn!(
                "Failed to clean up expired borrow interaction buckets: {}",
                error
            ),
        }

        let trend_result = sqlx::query(
            r#"
            DELETE FROM borrow_interaction_trends
            WHERE trend_date < CURRENT_DATE - ($1::integer)
            "#,
        )
        .bind(trend_retention_days as i32)
        .execute(&pool)
        .await;

        match trend_result {
            Ok(result) => {
                if result.rows_affected() > 0 {
                    info!(
                        "Cleaned up {} expired borrow trend rows",
                        result.rows_affected()
                    );
                }
            }
            Err(error) => warn!("Failed to clean up expired borrow trend rows: {}", error),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(interval_seconds)).await;
    }
}
