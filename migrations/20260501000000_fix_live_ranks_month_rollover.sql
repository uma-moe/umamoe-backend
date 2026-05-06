-- Delay circle_live_ranks month rollover until 00:00 on the 2nd in JST.
-- During the 1st JST, scrape/tally updates may already have written new-month
-- monthly_point values even though live rankings should still reflect the
-- just-finished month. Preserve the highest pre-rollover total during that
-- window, then switch back to the normal fresh-live vs monthly behavior.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
WITH time_bounds AS (
    SELECT
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp AS now_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day')::timestamp AS rollover_start_jst,
        (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') AT TIME ZONE 'Asia/Tokyo')::timestamp AS today_start_utc
)
SELECT
    c.circle_id,
    RANK() OVER (
        ORDER BY CASE
            WHEN tb.now_jst < tb.rollover_start_jst
            THEN GREATEST(
                COALESCE(c.last_month_point, 0),
                COALESCE(c.live_points, 0),
                COALESCE(c.monthly_point, 0)
            )
            WHEN c.last_live_update >= tb.today_start_utc
            THEN GREATEST(COALESCE(c.live_points, 0), COALESCE(c.monthly_point, 0))
            ELSE COALESCE(c.monthly_point, 0)
        END DESC NULLS LAST
    ) AS live_rank,
    RANK() OVER (
        ORDER BY c.yesterday_points DESC NULLS LAST
    ) AS live_yesterday_rank
FROM circles c
CROSS JOIN time_bounds tb
WHERE (c.archived IS NULL OR c.archived = false)
  AND c.last_updated >= date_trunc('month',
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days');

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
