-- Delay circle_live_ranks month rollover until 00:00 on the 2nd in JST.
-- The effective ranking month is based on JST minus 1 day, so the 1st JST
-- still belongs to the previous month. During that window, rank strictly by
-- last_month_point; new-month monthly_point/live_points must not participate.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
WITH time_bounds AS (
    SELECT
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp AS now_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day')::timestamp AS rollover_start_jst,
    date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '1 day')::timestamp AS effective_month_start_jst,
        (date_trunc('day', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') AT TIME ZONE 'Asia/Tokyo')::timestamp AS today_start_utc
)
SELECT
    c.circle_id,
    RANK() OVER (
        ORDER BY CASE
            WHEN tb.now_jst < tb.rollover_start_jst
            THEN COALESCE(c.last_month_point, 0)
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
    AND c.last_updated >= tb.effective_month_start_jst;

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
