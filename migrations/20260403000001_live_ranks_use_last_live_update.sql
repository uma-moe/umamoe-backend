-- Update circle_live_ranks: use last_live_update to determine freshness (within 1 day).
-- When fresh, use GREATEST(live_points, monthly_point) so the higher value always wins,
-- guarding against the monthly reset window where monthly_point may briefly exceed live_points.
-- When stale, fall back to monthly_point only.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT
    circle_id,
    RANK() OVER (
        ORDER BY CASE
            WHEN last_live_update >= (date_trunc('day', NOW() AT TIME ZONE 'Asia/Tokyo') AT TIME ZONE 'Asia/Tokyo')::timestamp
            THEN GREATEST(COALESCE(live_points, 0), COALESCE(monthly_point, 0))
            ELSE COALESCE(monthly_point, 0)
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
