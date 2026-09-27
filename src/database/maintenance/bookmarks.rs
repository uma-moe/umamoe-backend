use super::*;
include!("../../types/database/maintenance/bookmarks.rs");

pub(super) async fn backfillBookmarkHashesTask(pool: PgPool) {
    if envFlag("DISABLE_BOOKMARK_HASH_BACKFILL") {
        warn!("Bookmark content-hash backfill task disabled by env");
        return;
    }

    let start_delay_seconds = envU64("BOOKMARK_HASH_BACKFILL_START_DELAY_SECONDS", 300);
    let interval_seconds = envU64("BOOKMARK_HASH_BACKFILL_INTERVAL_SECONDS", 5).max(1);
    let idle_interval_seconds =
        envU64("BOOKMARK_HASH_BACKFILL_IDLE_INTERVAL_SECONDS", 3600).max(60);
    let queue_batch_size =
        envU64("BOOKMARK_HASH_BACKFILL_QUEUE_BATCH_SIZE", 5000).clamp(100, 50_000) as i64;
    let process_batch_size = envU64("BOOKMARK_HASH_BACKFILL_BATCH_SIZE", 100).clamp(1, 1000) as i64;

    warn!(
        "Starting bookmark hash backfill task (queue_batch={}, process_batch={}, interval={}s)",
        queue_batch_size, process_batch_size, interval_seconds
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(start_delay_seconds)).await;

    loop {
        match runBookmarkHashBackfillBatch(&pool, queue_batch_size, process_batch_size).await {
            Ok(stats) => {
                if stats.queued_accounts > 0
                    || stats.processed_accounts > 0
                    || stats.updated_inheritances > 0
                    || stats.updated_bookmarks > 0
                {
                    warn!(
                        "Bookmark hash backfill batch: queued={}, pending_queued={}, processed_accounts={}, updated_inheritances={}, updated_bookmarks={}",
                        stats.queued_accounts,
                        stats.pending_queued_accounts,
                        stats.processed_accounts,
                        stats.updated_inheritances,
                        stats.updated_bookmarks
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(interval_seconds)).await;
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_secs(idle_interval_seconds))
                        .await;
                }
            }
            Err(error) => {
                warn!("Bookmark hash backfill batch failed: {}", error);
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            }
        }
    }
}

async fn runBookmarkHashBackfillBatch(
    pool: &PgPool,
    queue_batch_size: i64,
    process_batch_size: i64,
) -> anyhow::Result<BookmarkHashBackfillStats> {
    use sqlx::Row;

    let lock_name = "bookmark_content_hash_backfill";
    let Some(mut conn) = acquireMaintenanceConnection(pool, lock_name).await else {
        return Ok(BookmarkHashBackfillStats::default());
    };

    let previous_timeouts = currentTimeoutSettings(&mut conn).await.ok();
    let result = async {
        setMaintenanceTimeouts(
            &mut conn,
            envU64("BOOKMARK_HASH_BACKFILL_STATEMENT_TIMEOUT_SECONDS", 30).max(5),
            envU64("BOOKMARK_HASH_BACKFILL_LOCK_TIMEOUT_SECONDS", 2).max(1),
        )
        .await?;

        let queued_accounts: i64 = sqlx::query_scalar(
            r#"
            WITH queued AS (
                INSERT INTO bookmark_content_hash_backfill_queue (account_id)
                SELECT account_id
                FROM (
                    SELECT DISTINCT ub.account_id
                    FROM user_bookmarks ub
                    LEFT JOIN bookmark_content_hash_backfill_queue q
                        ON q.account_id = ub.account_id
                    WHERE ub.account_id IS NOT NULL
                      AND q.account_id IS NULL
                    ORDER BY ub.account_id
                    LIMIT $1
                ) pending
                ON CONFLICT (account_id) DO NOTHING
                RETURNING 1
            )
            SELECT COUNT(*)::bigint FROM queued
            "#,
        )
        .bind(queue_batch_size)
        .fetch_one(&mut *conn)
        .await?;

        let row = sqlx::query(
            r#"
            WITH batch AS MATERIALIZED (
                SELECT account_id
                FROM bookmark_content_hash_backfill_queue
                WHERE processed_at IS NULL
                ORDER BY queued_at, account_id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ),
            old_matches AS MATERIALIZED (
                SELECT ub.id, ub.account_id
                FROM user_bookmarks ub
                JOIN inheritance i ON i.account_id = ub.account_id
                JOIN batch b ON b.account_id = ub.account_id
                WHERE ub.bookmarked_hash IS NOT DISTINCT FROM i.content_hash
            ),
            updated_inheritance AS (
                UPDATE inheritance i
                SET content_hash = i.content_hash
                FROM batch b
                WHERE i.account_id = b.account_id
                RETURNING i.account_id, i.content_hash
            ),
            updated_bookmarks AS (
                UPDATE user_bookmarks ub
                SET bookmarked_hash = ui.content_hash
                FROM updated_inheritance ui
                JOIN old_matches m ON m.account_id = ui.account_id
                WHERE ub.id = m.id
                RETURNING ub.id
            ),
            marked AS (
                UPDATE bookmark_content_hash_backfill_queue q
                SET
                    processed_at = NOW(),
                    attempts = attempts + 1,
                    last_error = NULL,
                    updated_at = NOW()
                FROM batch b
                WHERE q.account_id = b.account_id
                RETURNING q.account_id
            )
            SELECT
                (SELECT COUNT(*)::bigint FROM marked) AS processed_accounts,
                (SELECT COUNT(*)::bigint FROM updated_inheritance) AS updated_inheritances,
                (SELECT COUNT(*)::bigint FROM updated_bookmarks) AS updated_bookmarks
            "#,
        )
        .bind(process_batch_size)
        .fetch_one(&mut *conn)
        .await?;

        let pending_queued_accounts: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)::bigint
            FROM bookmark_content_hash_backfill_queue
            WHERE processed_at IS NULL
            "#,
        )
        .fetch_one(&mut *conn)
        .await?;

        Ok(BookmarkHashBackfillStats {
            queued_accounts,
            pending_queued_accounts,
            processed_accounts: row.get("processed_accounts"),
            updated_inheritances: row.get("updated_inheritances"),
            updated_bookmarks: row.get("updated_bookmarks"),
        })
    }
    .await;

    if let Some((statement_timeout, lock_timeout)) = previous_timeouts {
        restoreTimeoutSettings(&mut conn, &statement_timeout, &lock_timeout).await;
    }
    releaseMaintenanceLock(&mut conn, lock_name).await;
    result
}
