-- Apply the corrected circle rollover display rules without rewriting prior migrations.
-- Rollover rules:
--   * June/next-month tallying begins on the 1st JST evening, but display stays
--     on prior-month ranking until 00:00 on the 3rd JST for non-top-100 clubs;
--   * from 00:00 on the 2nd JST, valid top-100 live leaderboard rows display
--     with their live rank/points;
--   * circles archived during the display month remain ranked until the display
--     month rolls forward. The API marks those rows as disbanded.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
WITH time_bounds AS (
    SELECT
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp AS now_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day')::timestamp AS game_month_start_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')::timestamp AS display_month_end_jst,
        (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') AT TIME ZONE 'Asia/Tokyo')::timestamp AS today_start_utc
)
SELECT
    c.circle_id,
    RANK() OVER (
        ORDER BY
            CASE
                WHEN tb.now_jst >= tb.game_month_start_jst
                 AND tb.now_jst < tb.display_month_end_jst
                 AND c.live_rank > 0
                 AND c.live_points > 0
                THEN c.live_rank
                ELSE NULL
            END ASC NULLS LAST,
            CASE
                WHEN tb.now_jst >= tb.game_month_start_jst
                 AND tb.now_jst < tb.display_month_end_jst
                 AND c.live_rank > 0
                 AND c.live_points > 0
                THEN c.live_points
                WHEN tb.now_jst < tb.display_month_end_jst
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
WHERE c.last_updated >= date_trunc('month', tb.now_jst - interval '2 days');

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
