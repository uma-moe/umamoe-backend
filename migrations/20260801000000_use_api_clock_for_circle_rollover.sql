-- The application owns the public JST rollover window. Keeping the
-- materialized view time-neutral avoids a PostgreSQL host clock skew from
-- showing last-month values before the 2nd JST.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT
    c.circle_id,
    RANK() OVER (
        ORDER BY
            CASE
                WHEN c.live_points > 0
                THEN COALESCE(GREATEST(c.live_points, c.monthly_point), c.live_points, c.monthly_point)
                ELSE c.monthly_point
            END DESC NULLS LAST,
            CASE
                WHEN c.live_points > 0
                 AND c.live_rank > 0
                THEN c.live_rank
                ELSE NULL
            END ASC NULLS LAST
    ) AS live_rank,
    ROW_NUMBER() OVER (
        ORDER BY
            c.yesterday_points DESC NULLS LAST,
            NULLIF(c.yesterday_rank, 0) ASC NULLS LAST,
            NULLIF(c.monthly_rank, 0) ASC NULLS LAST,
            c.circle_id ASC
    ) AS live_yesterday_rank
FROM circles c
WHERE c.last_updated >= date_trunc(
        'month',
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days'
    )
  AND NOT COALESCE(c.archived, false);

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
CREATE INDEX idx_circle_live_ranks_yesterday_rank ON circle_live_ranks (live_yesterday_rank);
