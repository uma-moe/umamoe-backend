-- Rebuild circle_live_ranks to ignore zero live_rank/live_points sentinels.
-- Some rows can have live_rank = 0 and live_points = 0; those are not valid
-- top-100 live rows and must not sort before rank 1 during the rollover tally
-- window.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
WITH time_bounds AS (
    SELECT
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp AS now_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')::timestamp AS tally_window_end_jst,
        (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') AT TIME ZONE 'Asia/Tokyo')::timestamp AS today_start_utc
)
SELECT
    c.circle_id,
    RANK() OVER (
        ORDER BY
            CASE
                WHEN tb.now_jst < tb.tally_window_end_jst
                 AND c.live_rank > 0
                 AND c.live_points > 0
                THEN c.live_rank
                ELSE NULL
            END ASC NULLS LAST,
            CASE
                WHEN tb.now_jst < tb.tally_window_end_jst
                 AND c.live_rank > 0
                 AND c.live_points > 0
                THEN c.live_points
                WHEN tb.now_jst < tb.tally_window_end_jst
                THEN COALESCE(c.last_month_point, c.monthly_point)
                WHEN c.last_live_update >= tb.today_start_utc
                 AND c.live_rank > 0
                 AND c.live_points > 0
                THEN COALESCE(GREATEST(c.live_points, c.monthly_point), c.live_points, c.monthly_point)
                ELSE c.monthly_point
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