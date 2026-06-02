-- Correct the circle display-rank window after 20260531000002.
-- Top-100 rows use the live leaderboard whenever live_rank/live_points are valid.
-- Non-top-100 rows use last-month rank/points only from 00:00 on the 2nd JST
-- until 00:00 on the 3rd JST. Outside that window they use current monthly
-- rank/points. Archived circles remain included while their last_updated keeps
-- them in the active display-month window.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
WITH time_bounds AS (
    SELECT
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp AS now_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day')::timestamp AS display_start_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')::timestamp AS display_end_jst
)
SELECT
    c.circle_id,
    RANK() OVER (
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
                THEN c.live_points
                WHEN tb.now_jst >= tb.display_start_jst
                 AND tb.now_jst < tb.display_end_jst
                THEN COALESCE(c.last_month_point, c.monthly_point)
                ELSE c.monthly_point
            END DESC NULLS LAST
    ) AS live_rank,
    RANK() OVER (
        ORDER BY c.yesterday_points DESC NULLS LAST
    ) AS live_yesterday_rank
FROM circles c
CROSS JOIN time_bounds tb
WHERE c.last_updated >= date_trunc('month', tb.now_jst - interval '2 days');

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
