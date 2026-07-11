-- Keep the expensive raw daily fan expansion in the monthly refresh, where it
-- is bounded to recent months.  All-time rankings can then be derived from the
-- compact monthly summaries instead of expanding every historical array on
-- every hourly refresh.
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;

CREATE MATERIALIZED VIEW user_fan_rankings_alltime AS
WITH ranked_monthly AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY r.viewer_id
            ORDER BY r.year DESC, r.month DESC
        ) AS recency
    FROM user_fan_rankings_monthly r
),
monthly_totals AS (
    SELECT
        r.viewer_id,
        MAX(r.total_fans) FILTER (WHERE r.recency = 1)::bigint AS snapshot_fans,
        SUM(r.monthly_gain)::bigint AS snapshot_gain,
        SUM(r.active_days)::int AS active_days,
        COUNT(*)::int AS months_active,
        MAX(r.avg_7d) FILTER (WHERE r.recency = 1) AS latest_avg_7d,
        MAX(r.circle_id) FILTER (WHERE r.recency = 1) AS circle_id,
        MAX(r.circle_name) FILTER (WHERE r.recency = 1) AS circle_name
    FROM ranked_monthly r
    GROUP BY r.viewer_id
),
current_totals AS (
    SELECT
        m.viewer_id,
        COALESCE(t.name, latest.trainer_name) AS trainer_name,
        GREATEST(m.snapshot_fans, COALESCE(t.fans::bigint, m.snapshot_fans)) AS total_fans,
        m.snapshot_gain
            + GREATEST(COALESCE(t.fans::bigint, m.snapshot_fans) - m.snapshot_fans, 0)
            AS total_gain,
        m.active_days,
        m.months_active,
        CASE
            WHEN m.latest_avg_7d IS NULL THEN NULL
            ELSE (m.latest_avg_7d * 7.0)
                + GREATEST(COALESCE(t.fans::bigint, m.snapshot_fans) - m.snapshot_fans, 0)
        END AS avg_week,
        m.circle_id,
        COALESCE(c.name, m.circle_name) AS circle_name
    FROM monthly_totals m
    JOIN ranked_monthly latest
      ON latest.viewer_id = m.viewer_id
     AND latest.recency = 1
    LEFT JOIN trainer t ON t.account_id = m.viewer_id::text
    LEFT JOIN circles c ON c.circle_id = m.circle_id
),
scored AS (
    SELECT
        c.*,
        c.total_gain::float8 / NULLIF(c.active_days, 0) AS avg_day,
        c.total_gain::float8 / NULLIF(c.months_active, 0) AS avg_month
    FROM current_totals c
)
SELECT
    s.viewer_id,
    s.trainer_name,
    s.total_fans::bigint AS total_fans,
    s.total_gain::bigint AS total_gain,
    s.active_days,
    s.avg_day,
    s.avg_week,
    s.avg_month,
    RANK() OVER (ORDER BY s.total_gain DESC)::int AS rank,
    RANK() OVER (ORDER BY s.total_fans DESC)::int AS rank_total_fans,
    RANK() OVER (ORDER BY s.total_gain DESC)::int AS rank_total_gain,
    RANK() OVER (ORDER BY s.avg_day DESC NULLS LAST)::int AS rank_avg_day,
    RANK() OVER (ORDER BY s.avg_week DESC NULLS LAST)::int AS rank_avg_week,
    RANK() OVER (ORDER BY s.avg_month DESC NULLS LAST)::int AS rank_avg_month,
    s.circle_id,
    s.circle_name
FROM scored s;

CREATE UNIQUE INDEX idx_ufr_alltime_pk
    ON user_fan_rankings_alltime (viewer_id);
CREATE INDEX idx_ufr_alltime_name
    ON user_fan_rankings_alltime USING gin (trainer_name gin_trgm_ops);
CREATE INDEX idx_ufr_alltime_circle_name
    ON user_fan_rankings_alltime USING gin (circle_name gin_trgm_ops);
CREATE INDEX idx_ufr_alltime_rank
    ON user_fan_rankings_alltime (rank);
CREATE INDEX idx_ufr_alltime_rank_total_fans
    ON user_fan_rankings_alltime (rank_total_fans);
CREATE INDEX idx_ufr_alltime_rank_total_gain
    ON user_fan_rankings_alltime (rank_total_gain);
CREATE INDEX idx_ufr_alltime_rank_avg_day
    ON user_fan_rankings_alltime (rank_avg_day);
CREATE INDEX idx_ufr_alltime_rank_avg_week
    ON user_fan_rankings_alltime (rank_avg_week);
CREATE INDEX idx_ufr_alltime_rank_avg_month
    ON user_fan_rankings_alltime (rank_avg_month);
CREATE INDEX idx_ufr_alltime_total_fans
    ON user_fan_rankings_alltime (total_fans DESC);
CREATE INDEX idx_ufr_alltime_total_gain
    ON user_fan_rankings_alltime (total_gain DESC);

CREATE INDEX IF NOT EXISTS idx_circle_ranks_archive_month_rank
    ON circle_ranks_monthly_archive (year, month, rank, circle_id);
CREATE INDEX IF NOT EXISTS idx_circle_ranks_archive_name
    ON circle_ranks_monthly_archive USING gin (circle_name gin_trgm_ops);

-- Build a durable monthly circle leaderboard from the same monthly member
-- gains used by fan_history.  The old implementation summed daily_fans[32],
-- which is normally the unused zero/overflow slot and produced zero-point ties.
CREATE OR REPLACE FUNCTION archive_circle_rankings_month(p_year int, p_month int)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    target_month date := make_date(p_year, p_month, 1);
    previous_jst_month date := (
        date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date)
        - interval '1 month'
    )::date;
BEGIN
    DELETE FROM circle_ranks_monthly_archive
    WHERE year = p_year AND month = p_month;

    INSERT INTO circle_ranks_monthly_archive
        (circle_id, year, month, rank, total_points, member_count, circle_name)
    WITH circle_totals AS (
        SELECT
            r.circle_id,
            SUM(r.monthly_gain)::bigint AS total_points,
            COUNT(*)::int AS member_count,
            MAX(r.circle_name) AS circle_name
        FROM user_fan_rankings_monthly r
        WHERE r.year = p_year
          AND r.month = p_month
          AND r.circle_id IS NOT NULL
        GROUP BY r.circle_id
    ),
    ranked AS (
        SELECT
            t.*,
            RANK() OVER (
                ORDER BY t.total_points DESC
            )::int AS rank
        FROM circle_totals t
    )
    SELECT
        r.circle_id,
        p_year,
        p_month,
        r.rank,
        r.total_points,
        r.member_count,
        COALESCE(r.circle_name, c.name)
    FROM ranked r
    LEFT JOIN circles c ON c.circle_id = r.circle_id;

    -- Immediately after rollover, the game's last-month values are more
    -- authoritative than a reconstruction from incomplete member snapshots.
    IF target_month = previous_jst_month THEN
        INSERT INTO circle_ranks_monthly_archive
            (circle_id, year, month, rank, total_points, member_count, circle_name)
        SELECT
            c.circle_id,
            p_year,
            p_month,
            NULLIF(c.last_month_rank, 0),
            NULLIF(c.last_month_point, 0),
            c.member_count,
            c.name
        FROM circles c
        WHERE NOT COALESCE(c.archived, false)
          AND (c.last_month_rank > 0 OR c.last_month_point > 0)
        ON CONFLICT (circle_id, year, month) DO UPDATE SET
            rank = COALESCE(EXCLUDED.rank, circle_ranks_monthly_archive.rank),
            total_points = COALESCE(
                EXCLUDED.total_points,
                circle_ranks_monthly_archive.total_points
            ),
            member_count = COALESCE(
                EXCLUDED.member_count,
                circle_ranks_monthly_archive.member_count
            ),
            circle_name = COALESCE(
                EXCLUDED.circle_name,
                circle_ranks_monthly_archive.circle_name
            );
    END IF;
END;
$$;

-- Repair every completed month.  This scans the compact monthly summaries,
-- not the raw 32-element fan arrays.
DO $$
DECLARE
    archive_month record;
BEGIN
    FOR archive_month IN
        SELECT DISTINCT r.year, r.month
        FROM user_fan_rankings_monthly r
        WHERE make_date(r.year, r.month, 1) < date_trunc(
            'month',
            (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date
        )
        ORDER BY r.year, r.month
    LOOP
        PERFORM archive_circle_rankings_month(archive_month.year, archive_month.month);
    END LOOP;
END;
$$;

ANALYZE user_fan_rankings_alltime;
ANALYZE circle_ranks_monthly_archive;
