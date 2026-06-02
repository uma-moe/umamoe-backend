-- Lock circle ranking output to last-month values during the rollover tally window.
-- From 19:00 JST on the 1st until 00:00 JST on the 3rd, current/live circle
-- data is still in flux, so public ranking consumers should see the completed
-- previous month. Archived circles remain included while the display-month
-- date window still reaches the prior month.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
WITH time_bounds AS (
    SELECT
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp AS now_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '19 hours')::timestamp AS rollover_lock_start_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')::timestamp AS rollover_lock_end_jst
)
SELECT
    c.circle_id,
    CASE
        WHEN tb.now_jst >= tb.rollover_lock_start_jst
         AND tb.now_jst < tb.rollover_lock_end_jst
        THEN COALESCE(c.last_month_rank, c.monthly_rank)::bigint
        ELSE RANK() OVER (
            ORDER BY
                CASE
                    WHEN c.live_rank > 0
                     AND c.live_points > 0
                    THEN c.live_rank
                    ELSE NULL
                END ASC NULLS LAST,
                CASE
                    WHEN c.live_rank > 0
                     AND c.live_points > 0
                    THEN COALESCE(GREATEST(c.live_points, c.monthly_point), c.live_points, c.monthly_point)
                    ELSE c.monthly_point
                END DESC NULLS LAST
        )
    END AS live_rank,
    CASE
        WHEN tb.now_jst >= tb.rollover_lock_start_jst
         AND tb.now_jst < tb.rollover_lock_end_jst
        THEN COALESCE(c.last_month_rank, c.yesterday_rank)::bigint
        ELSE RANK() OVER (
            ORDER BY c.yesterday_points DESC NULLS LAST
        )
    END AS live_yesterday_rank
FROM circles c
CROSS JOIN time_bounds tb
WHERE c.last_updated >= date_trunc('month', tb.now_jst - interval '2 days');

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
