use super::*;
pub(super) async fn clearStaleLiveTask(pool: PgPool) {
    let stale_after_minutes =
        envU64("CIRCLE_LIVE_STALE_AFTER_MINUTES", 90).clamp(15, 24 * 60) as i32;

    info!("🔄 Starting stale live-data cleanup task (runs every hour)");
    info!(
        "Circle live-data stale threshold: {} minutes",
        stale_after_minutes
    );
    tokio::time::sleep(tokio::time::Duration::from_secs(45)).await;
    loop {
        match sqlx::query(
                        r#"
                        UPDATE circles
                        SET live_points = NULL, live_rank = NULL
                        WHERE (live_points IS NOT NULL OR live_rank IS NOT NULL)
                            AND (
                                last_live_update IS NULL
                                OR last_live_update < (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - ($1::int * interval '1 minute')
                                OR (
                                    last_updated IS NOT NULL
                                    AND last_updated > last_live_update + ($1::int * interval '1 minute')
                                )
                            )
                        "#,
        )
        .bind(stale_after_minutes)
        .execute(&pool)
        .await
        {
            Ok(res) => {
                if res.rows_affected() > 0 {
                    info!("🧹 Cleared stale live data from {} circles", res.rows_affected());
                }
            }
            Err(e) => error!("Failed to clear stale live data: {}", e),
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

// Background task to refresh circle live ranks materialized view
pub(super) async fn refreshCircleRanksTask(pool: PgPool) {
    info!("🔄 Starting circle ranks refresh background task (runs every 5 minutes)");

    // Wait before first run to let the server start
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    loop {
        refreshMatView(&pool, "circle_live_ranks").await;
        tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
    }
}

// Background task to refresh user fan ranking materialized views
// Architecture: archive table for past months + current mat view for last 2 months
// - Hourly: archive completed months, refresh current + alltime
// - Daily: refresh gains (expensive, reads raw data)
pub(super) async fn refreshUserRankingsTask(pool: PgPool) {
    use sqlx::Row;

    // Wait for startup
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

    // Avoid rebuilding the expensive daily gains view during every deployment.
    let mut gains_tick: u32 = if envFlag("REFRESH_GAINS_ON_STARTUP") {
        23
    } else {
        0
    };

    info!("🔄 Starting user fan rankings refresh task (current+alltime: hourly, gains: daily)");
    info!("🔄 Recalculating current and all-time ranking materialized views");

    loop {
        gains_tick += 1;

        // Every backend checks archive status hourly. Serialize this work across
        // replicas so the same expensive scan cannot run concurrently.
        if let Some(mut archive_lock) =
            acquireMaintenanceConnection(&pool, RANKING_ARCHIVE_LOCK).await
        {
            // Keep a bad plan or an unexpectedly large backfill from holding a
            // backend forever. Restore the pool connection's original settings
            // before returning it to the pool.
            let previous_archive_timeouts = currentTimeoutSettings(&mut archive_lock).await.ok();
            if let Err(error) = setMaintenanceTimeouts(
                &mut archive_lock,
                envU64("RANKING_ARCHIVE_STATEMENT_TIMEOUT_SECONDS", 300).clamp(30, 600),
                3,
            )
            .await
            {
                warn!("Failed to set ranking archive DB timeouts: {}", error);
            }

            // Step 1: Archive any completed months not yet in the archive table.
            // Generate the small calendar range and probe each month through the
            // (year, month) index instead of DISTINCT-scanning the full history.
            match sqlx::query(
                "WITH cutoff AS ( \
                     SELECT date_trunc('month', \
                                (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date \
                                - interval '1 month')::date AS month_start \
                 ), source_start AS ( \
                     SELECT make_date(year, month, 1) AS month_start \
                     FROM circle_member_fans_monthly \
                     ORDER BY year, month \
                     LIMIT 1 \
                 ), candidate_months AS ( \
                     SELECT extract(year FROM months.month_start)::int AS year, \
                            extract(month FROM months.month_start)::int AS month \
                     FROM source_start \
                     CROSS JOIN cutoff \
                     CROSS JOIN LATERAL generate_series( \
                         source_start.month_start, \
                         cutoff.month_start - interval '1 month', \
                         interval '1 month' \
                     ) AS months(month_start) \
                 ) \
                 SELECT candidate_months.year, candidate_months.month \
                 FROM candidate_months \
                 WHERE NOT EXISTS ( \
                     SELECT 1 FROM user_fan_rankings_monthly_archive a \
                     WHERE a.year = candidate_months.year \
                       AND a.month = candidate_months.month \
                 ) \
                   AND EXISTS ( \
                       SELECT 1 FROM circle_member_fans_monthly cm \
                       WHERE cm.year = candidate_months.year \
                         AND cm.month = candidate_months.month \
                   ) \
                 ORDER BY candidate_months.year, candidate_months.month",
            )
            .fetch_all(&mut *archive_lock)
            .await
            {
                Ok(rows) => {
                    for row in &rows {
                        let year: i32 = row.get("year");
                        let month: i32 = row.get("month");
                        info!("📦 Archiving fan rankings for {}-{:02}", year, month);
                        match sqlx::query("SELECT archive_fan_rankings_month($1, $2)")
                            .bind(year)
                            .bind(month)
                            .fetch_optional(&mut *archive_lock)
                            .await
                        {
                            Ok(_) => info!("✅ Archived fan rankings for {}-{:02}", year, month),
                            Err(e) => warn!("⚠️ Failed to archive {}-{:02}: {}", year, month, e),
                        }
                    }
                }
                Err(e) => warn!("⚠️ Failed to check archive status: {}", e),
            }

            // Step 1b: Archive circle ranks for completed months. This uses the same
            // bounded calendar/index-probe strategy as the fan archive check.
            match sqlx::query(
                "WITH cutoff AS ( \
                     SELECT date_trunc('month', \
                                (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date \
                                - interval '1 day')::date AS month_start, \
                            (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date AS today \
                 ), source_start AS ( \
                     SELECT make_date(year, month, 1) AS month_start \
                     FROM circle_member_fans_monthly \
                     ORDER BY year, month \
                     LIMIT 1 \
                 ), candidate_months AS ( \
                     SELECT extract(year FROM months.month_start)::int AS year, \
                            extract(month FROM months.month_start)::int AS month \
                     FROM source_start \
                     CROSS JOIN cutoff \
                     CROSS JOIN LATERAL generate_series( \
                         source_start.month_start, \
                         cutoff.month_start - interval '1 month', \
                         interval '1 month' \
                     ) AS months(month_start) \
                 ) \
                 SELECT candidate_months.year, candidate_months.month \
                 FROM candidate_months \
                 CROSS JOIN cutoff \
                 WHERE ( \
                     NOT EXISTS ( \
                         SELECT 1 FROM circle_ranks_monthly_archive a \
                         WHERE a.year = candidate_months.year \
                           AND a.month = candidate_months.month \
                     ) \
                     OR ( \
                         candidate_months.year = extract(year FROM cutoff.month_start - interval '1 month')::int \
                         AND candidate_months.month = extract(month FROM cutoff.month_start - interval '1 month')::int \
                         AND extract(day FROM cutoff.today) <= 3 \
                     ) \
                 ) \
                   AND EXISTS ( \
                       SELECT 1 FROM circle_member_fans_monthly cmf \
                       WHERE cmf.year = candidate_months.year \
                         AND cmf.month = candidate_months.month \
                   ) \
                 ORDER BY candidate_months.year, candidate_months.month",
            )
            .fetch_all(&mut *archive_lock)
            .await
            {
                Ok(rows) => {
                    for row in &rows {
                        let year: i32 = row.get("year");
                        let month: i32 = row.get("month");
                        info!("📦 Archiving circle ranks for {}-{:02}", year, month);
                        match sqlx::query("SELECT archive_circle_rankings_month($1, $2)")
                            .bind(year)
                            .bind(month)
                            .fetch_optional(&mut *archive_lock)
                            .await
                        {
                            Ok(_) => info!("✅ Archived circle ranks for {}-{:02}", year, month),
                            Err(e) => warn!(
                                "⚠️ Failed to archive circle ranks {}-{:02}: {}",
                                year, month, e
                            ),
                        }
                    }
                }
                Err(e) => warn!("⚠️ Failed to check circle rank archive status: {}", e),
            }

            if let Some((statement_timeout, lock_timeout)) = previous_archive_timeouts {
                restoreTimeoutSettings(&mut archive_lock, &statement_timeout, &lock_timeout).await;
            }
            releaseMaintenanceLock(&mut archive_lock, RANKING_ARCHIVE_LOCK).await;
        }

        // Step 2: Refresh current-month rankings.
        refreshMatView(&pool, "user_fan_rankings_monthly_current").await;

        // Step 3: Refresh alltime (reads from pre-aggregated monthly data — fast)
        refreshMatView(&pool, "user_fan_rankings_alltime").await;

        // Step 4: Refresh gains once per day (expensive — reads raw daily_fans arrays)
        if gains_tick >= 24 {
            gains_tick = 0;
            refreshMatView(&pool, "user_fan_rankings_gains").await;
        }

        // Wait 1 hour AFTER completion, not from start
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}
