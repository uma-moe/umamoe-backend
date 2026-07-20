-- Restore all-time trainer and Uma counts while retaining the rolling activity
-- figures. The backend refreshes this materialized cache once per hour.

DROP MATERIALIZED VIEW IF EXISTS stats_counts;

CREATE MATERIALIZED VIEW stats_counts AS
WITH totals AS (
  SELECT
    (SELECT COUNT(*) FROM trainer) AS trainer_count,
    (SELECT COUNT(*) FROM team_stadium) AS umas_tracked
)
SELECT
  1::smallint AS singleton_key,

  -- Rolling 24h activity
  (SELECT COUNT(*) FROM tasks
   WHERE status = 'completed'
     AND updated_at >= NOW() - INTERVAL '24 hours'
  ) AS tasks_24h,

  (SELECT COUNT(*) FROM trainer
   WHERE last_updated >= NOW() - INTERVAL '24 hours'
  ) AS accounts_24h,

  -- All-time totals
  totals.trainer_count,
  -- Keep old backend/frontend instances working during rolling deployments.
  totals.trainer_count AS accounts_7d,
  totals.umas_tracked,

  NOW() AS last_refreshed
FROM totals;

-- Required by REFRESH MATERIALIZED VIEW CONCURRENTLY.
CREATE UNIQUE INDEX idx_stats_counts_singleton ON stats_counts (singleton_key);
