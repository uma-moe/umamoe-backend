-- Simplify circle_live_ranks: rank by GREATEST(live_points, monthly_point) unconditionally.
-- No freshness check needed — if live_points is present it is always at least as current
-- as monthly_point, so taking the greater value is always correct.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT
    circle_id,
    RANK() OVER (
        ORDER BY GREATEST(COALESCE(live_points, 0), COALESCE(monthly_point, 0)) DESC NULLS LAST
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
