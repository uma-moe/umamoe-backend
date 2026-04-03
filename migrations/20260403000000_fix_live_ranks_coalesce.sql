-- Fix circle_live_ranks: rank by COALESCE(live_points, monthly_point) unconditionally.
-- The previous 4-hour freshness check caused wrong rankings when circles were scraped
-- at different times (e.g. circle A scraped at 5am stale -> uses monthly_point, while
-- circle B scraped at 11am fresh -> uses live_points, making B appear ranked higher).

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT
    circle_id,
    RANK() OVER (
        ORDER BY COALESCE(live_points, monthly_point) DESC NULLS LAST
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
