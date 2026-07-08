-- Treat stored rank 0 values as unknown instead of better than rank 1.
--
-- Freshly inserted circles can briefly carry rank sentinels of 0. Public list
-- ordering should put those rows after ranked circles, not at the front.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
WITH time_bounds AS (
    SELECT
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::timestamp AS now_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '19 hours')::timestamp AS tally_start_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day')::timestamp AS game_month_start_jst,
        (date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '2 days')::timestamp AS display_end_jst,
        ((date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') + interval '1 day') AT TIME ZONE 'Asia/Tokyo')::timestamp AS game_month_start_utc
)
SELECT
    c.circle_id,
    CASE
        WHEN tb.now_jst >= tb.tally_start_jst
         AND tb.now_jst < tb.game_month_start_jst
         AND COALESCE(c.archived, false)
        THEN NULLIF(c.monthly_rank, 0)::bigint
        WHEN tb.now_jst >= tb.tally_start_jst
         AND tb.now_jst < tb.game_month_start_jst
         AND c.live_rank > 0
         AND c.live_points > 0
        THEN c.live_rank::bigint
        WHEN tb.now_jst >= tb.tally_start_jst
         AND tb.now_jst < tb.game_month_start_jst
        THEN NULLIF(c.monthly_rank, 0)::bigint
        WHEN tb.now_jst >= tb.game_month_start_jst
         AND tb.now_jst < tb.display_end_jst
         AND NOT COALESCE(c.archived, false)
         AND c.last_updated >= tb.game_month_start_utc
        THEN COALESCE(NULLIF(c.last_month_rank, 0), NULLIF(c.monthly_rank, 0))::bigint
        WHEN tb.now_jst >= tb.game_month_start_jst
         AND tb.now_jst < tb.display_end_jst
        THEN NULLIF(c.monthly_rank, 0)::bigint
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
        WHEN tb.now_jst >= tb.tally_start_jst
         AND tb.now_jst < tb.game_month_start_jst
         AND COALESCE(c.archived, false)
        THEN NULLIF(c.monthly_rank, 0)::bigint
        WHEN tb.now_jst >= tb.tally_start_jst
         AND tb.now_jst < tb.game_month_start_jst
         AND c.live_rank > 0
         AND c.live_points > 0
        THEN c.live_rank::bigint
        WHEN tb.now_jst >= tb.tally_start_jst
         AND tb.now_jst < tb.game_month_start_jst
        THEN NULLIF(c.monthly_rank, 0)::bigint
        WHEN tb.now_jst >= tb.game_month_start_jst
         AND tb.now_jst < tb.display_end_jst
         AND NOT COALESCE(c.archived, false)
         AND c.last_updated >= tb.game_month_start_utc
        THEN COALESCE(NULLIF(c.last_month_rank, 0), NULLIF(c.yesterday_rank, 0))::bigint
        WHEN tb.now_jst >= tb.game_month_start_jst
         AND tb.now_jst < tb.display_end_jst
        THEN NULLIF(c.yesterday_rank, 0)::bigint
        ELSE ROW_NUMBER() OVER (
            ORDER BY
                c.yesterday_points DESC NULLS LAST,
                NULLIF(c.yesterday_rank, 0) ASC NULLS LAST,
                NULLIF(c.monthly_rank, 0) ASC NULLS LAST,
                c.circle_id ASC
        )
    END AS live_yesterday_rank
FROM circles c
CROSS JOIN time_bounds tb
WHERE c.last_updated >= date_trunc('month', tb.now_jst - interval '2 days')
    AND (NOT COALESCE(c.archived, false) OR tb.now_jst < tb.display_end_jst);

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
CREATE INDEX idx_circle_live_ranks_yesterday_rank ON circle_live_ranks (live_yesterday_rank);
