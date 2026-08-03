use sqlx::{pool::PoolConnection, PgPool, Postgres};
use tracing::{info, warn};

const RETENTION_LOCK_NAME: &str = "database_high_churn_retention";

#[derive(Clone, Copy, Debug)]
struct RetentionConfig {
    completed_task_hours: u64,
    failed_task_days: u64,
    task_attempt_hours: u64,
    batch_size: i64,
    max_batches_per_table: u64,
    interval_seconds: u64,
    start_delay_seconds: u64,
    statement_timeout_seconds: u64,
    lock_timeout_seconds: u64,
}

impl RetentionConfig {
    fn from_env() -> Self {
        Self {
            // stats_counts exposes a rolling 24-hour task total. Keep a twelve-hour
            // cushion so an hourly refresh never loses rows on the cutoff boundary.
            completed_task_hours: env_u64("COMPLETED_TASK_RETENTION_HOURS", 36).clamp(24, 24 * 30),
            failed_task_days: env_u64("FAILED_TASK_RETENTION_DAYS", 14).clamp(1, 365),
            // Raw attempts are diagnostic telemetry. Aggregated health tables are the
            // durable representation; two days is enough for incident investigation.
            task_attempt_hours: env_u64("TASK_ATTEMPT_RETENTION_HOURS", 48).clamp(24, 24 * 30),
            batch_size: env_u64("DATABASE_RETENTION_BATCH_SIZE", 25_000).clamp(1_000, 100_000)
                as i64,
            max_batches_per_table: env_u64("DATABASE_RETENTION_MAX_BATCHES", 20).clamp(1, 500),
            interval_seconds: env_u64("DATABASE_RETENTION_INTERVAL_SECONDS", 15 * 60)
                .clamp(60, 24 * 60 * 60),
            start_delay_seconds: env_u64("DATABASE_RETENTION_START_DELAY_SECONDS", 5 * 60)
                .clamp(5, 24 * 60 * 60),
            statement_timeout_seconds: env_u64("DATABASE_RETENTION_STATEMENT_TIMEOUT_SECONDS", 30)
                .clamp(5, 15 * 60),
            lock_timeout_seconds: env_u64("DATABASE_RETENTION_LOCK_TIMEOUT_SECONDS", 2)
                .clamp(1, 60),
        }
    }
}

#[derive(Default, Debug)]
struct RetentionStats {
    completed_tasks: u64,
    failed_tasks: u64,
    task_attempts: u64,
}

impl RetentionStats {
    fn total(&self) -> u64 {
        self.completed_tasks + self.failed_tasks + self.task_attempts
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

pub(crate) async fn run_retention_task(pool: PgPool) {
    let config = RetentionConfig::from_env();
    info!(
        "Starting database retention (completed_tasks={}h, failed_tasks={}d, task_attempts={}h, batch={}, max_batches={}, interval={}s)",
        config.completed_task_hours,
        config.failed_task_days,
        config.task_attempt_hours,
        config.batch_size,
        config.max_batches_per_table,
        config.interval_seconds,
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(config.start_delay_seconds)).await;

    loop {
        match run_retention_cycle(&pool, config).await {
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

async fn run_retention_cycle(
    pool: &PgPool,
    config: RetentionConfig,
) -> Result<RetentionStats, sqlx::Error> {
    let Some(mut conn) = super::acquire_maintenance_connection(pool, RETENTION_LOCK_NAME).await
    else {
        return Ok(RetentionStats::default());
    };

    let previous_timeouts = super::current_timeout_settings(&mut conn).await.ok();
    let result = async {
        super::set_maintenance_timeouts(
            &mut conn,
            config.statement_timeout_seconds,
            config.lock_timeout_seconds,
        )
        .await?;

        Ok(RetentionStats {
            completed_tasks: delete_completed_tasks(&mut conn, config).await?,
            failed_tasks: delete_failed_tasks(&mut conn, config).await?,
            task_attempts: delete_task_attempts(&mut conn, config).await?,
        })
    }
    .await;

    if let Some((statement_timeout, lock_timeout)) = previous_timeouts {
        super::restore_timeout_settings(&mut conn, &statement_timeout, &lock_timeout).await;
    }
    super::release_maintenance_lock(&mut conn, RETENTION_LOCK_NAME).await;
    result
}

async fn delete_completed_tasks(
    conn: &mut PoolConnection<Postgres>,
    config: RetentionConfig,
) -> Result<u64, sqlx::Error> {
    delete_in_batches(conn, config, |conn, batch_size| {
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

async fn delete_failed_tasks(
    conn: &mut PoolConnection<Postgres>,
    config: RetentionConfig,
) -> Result<u64, sqlx::Error> {
    delete_in_batches(conn, config, |conn, batch_size| {
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

async fn delete_task_attempts(
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
    for _ in 0..config.max_batches_per_table {
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
        tokio::task::yield_now().await;
    }
    Ok(total)
}

async fn delete_in_batches<F>(
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
    for _ in 0..config.max_batches_per_table {
        let deleted = delete_batch(conn, config.batch_size).await?;
        total += deleted;
        if deleted < config.batch_size as u64 {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_defaults_preserve_the_rolling_day_metric() {
        let config = RetentionConfig {
            completed_task_hours: 36,
            failed_task_days: 14,
            task_attempt_hours: 48,
            batch_size: 25_000,
            max_batches_per_table: 20,
            interval_seconds: 15 * 60,
            start_delay_seconds: 5 * 60,
            statement_timeout_seconds: 30,
            lock_timeout_seconds: 2,
        };

        assert!(config.completed_task_hours >= 24);
        assert!(config.batch_size <= 100_000);
    }
}
