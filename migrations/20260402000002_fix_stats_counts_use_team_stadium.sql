-- Fix stats_counts to count umas from team_stadium instead of veteran_characters.

DROP MATERIALIZED VIEW IF EXISTS stats_counts CASCADE;

CREATE MATERIALIZED VIEW stats_counts AS
SELECT
  -- Rolling 24h activity
  (SELECT COUNT(*) FROM tasks
   WHERE status = 'completed'
     AND updated_at >= NOW() - INTERVAL '24 hours'
  ) AS tasks_24h,

  -- Account freshness (expect ~0 stale since all accounts refresh within 7d)
  (SELECT COUNT(*) FROM trainer
   WHERE last_updated >= NOW() - INTERVAL '24 hours'
  ) AS accounts_24h,

  (SELECT COUNT(*) FROM trainer
   WHERE last_updated >= NOW() - INTERVAL '7 days'
  ) AS accounts_7d,

  -- TT umas stored
  (SELECT COUNT(*) FROM team_stadium) AS umas_tracked,

  NOW() AS last_refreshed;

CREATE UNIQUE INDEX idx_stats_counts_singleton ON stats_counts((1));

REFRESH MATERIALIZED VIEW stats_counts;
