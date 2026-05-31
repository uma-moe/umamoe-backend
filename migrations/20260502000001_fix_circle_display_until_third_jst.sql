-- Keep effective circle display ranks on last-month values until 00:00 on the 3rd JST.
-- Raw live_rank/live_points remain available on the circles table/API payload,
-- but circle_live_ranks should not make effective display ranking advance early.

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
    CASE
        WHEN tb.now_jst < tb.tally_window_end_jst
        THEN COALESCE(c.last_month_rank, c.monthly_rank)::bigint
        ELSE RANK() OVER (
            ORDER BY CASE
                WHEN c.last_live_update >= tb.today_start_utc
                 AND c.live_rank > 0
                 AND c.live_points > 0
                THEN COALESCE(GREATEST(c.live_points, c.monthly_point), c.live_points, c.monthly_point)
                ELSE c.monthly_point
            END DESC NULLS LAST
        )
    END AS live_rank,
    CASE
        WHEN tb.now_jst < tb.tally_window_end_jst
        THEN COALESCE(c.last_month_rank, c.yesterday_rank)::bigint
        ELSE RANK() OVER (
            ORDER BY c.yesterday_points DESC NULLS LAST
        )
    END AS live_yesterday_rank
FROM circles c
CROSS JOIN time_bounds tb
WHERE (c.archived IS NULL OR c.archived = false)
  AND c.last_updated >= date_trunc('month',
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days');

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
