use super::*;
use crate::handlers::stats;
pub(super) async fn refreshStatsTask(pool: PgPool) {
    info!("🔄 Starting stats refresh background task (runs every hour)");

    // Wait before first run to let the server start
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    loop {
        refreshMatView(&pool, "stats_counts").await;
        if let Err(error) = stats::refreshCache(&pool).await {
            warn!(
                "Failed to refresh the hourly stats response cache: {}",
                error
            );
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

// Background task to ensure daily_stats entries exist for each day
// This prevents gaps in the daily stats data
pub(super) async fn ensureDailyStatsTask(pool: PgPool) {
    // Wait before first run
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    info!("📅 Starting daily stats ensure background task (runs every hour)");

    loop {
        // Ensure today's entry exists (insert with 0 if not present)
        let result = sqlx::query(
            r#"
            INSERT INTO daily_stats (date, total_visitors, unique_visitors, visitor_count, created_at, updated_at)
            VALUES (CURRENT_DATE, 0, 0, 0, NOW(), NOW())
            ON CONFLICT (date) DO NOTHING
            "#,
        )
        .execute(&pool)
        .await;

        match result {
            Ok(r) => {
                if r.rows_affected() > 0 {
                    info!("📅 Created daily_stats entry for today");
                }
            }
            Err(e) => warn!("⚠️ Failed to ensure daily_stats entry: {}", e),
        }

        // Also backfill any missing days in the last 7 days
        let backfill_result = sqlx::query(
            r#"
            INSERT INTO daily_stats (date, total_visitors, unique_visitors, visitor_count, created_at, updated_at)
            SELECT
                d::date,
                0,
                0,
                0,
                NOW(),
                NOW()
            FROM generate_series(
                CURRENT_DATE - INTERVAL '7 days',
                CURRENT_DATE,
                '1 day'::interval
            ) AS d
            WHERE NOT EXISTS (
                SELECT 1 FROM daily_stats WHERE date = d::date
            )
            ON CONFLICT (date) DO NOTHING
            "#,
        )
        .execute(&pool)
        .await;

        match backfill_result {
            Ok(r) => {
                if r.rows_affected() > 0 {
                    info!(
                        "📅 Backfilled {} missing daily_stats entries",
                        r.rows_affected()
                    );
                }
            }
            Err(e) => warn!("⚠️ Failed to backfill daily_stats: {}", e),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}
