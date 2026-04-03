-- Fix circle_live_ranks: use live_points directly when fresh (<4h), else monthly_point.
-- Drops GREATEST() which caused live_points to always win over monthly_point.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT
    circle_id,
    RANK() OVER (
        ORDER BY CASE
            WHEN last_updated >= NOW() - INTERVAL '4 hours'
            THEN live_points
            ELSE monthly_point
        END DESC NULLS LAST
    ) AS live_rank,
    RANK() OVER (
        ORDER BY yesterday_points DESC NULLS LAST
    ) AS live_yesterday_rank
FROM circles
WHERE (archived IS NULL OR archived = false)
  AND last_updated >= date_trunc('month',
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days');

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
