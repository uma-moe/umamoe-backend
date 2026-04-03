-- ============================================================================
-- Add per-criteria rank columns to alltime rankings
-- ============================================================================
-- Currently only has a single `rank` based on total_gain DESC.
-- Add rank_total_gain, rank_total_fans, rank_active_days, rank_avg_daily,
-- rank_avg_3d, rank_avg_7d, rank_avg_monthly so the correct rank is shown
-- regardless of which sort option the user picks.
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;

CREATE MATERIALIZED VIEW user_fan_rankings_alltime AS
WITH all_monthly AS (
    SELECT viewer_id, trainer_name, year, month,
           total_fans, monthly_gain, active_days, avg_3d, avg_7d,
           circle_id, circle_name
    FROM user_fan_rankings_monthly
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY viewer_id
                           ORDER BY year DESC, month DESC) as rn
    FROM all_monthly
),
aggregated AS (
    SELECT
        viewer_id,
        MAX(CASE WHEN rn = 1 THEN trainer_name END) as trainer_name,
        MAX(CASE WHEN rn = 1 THEN total_fans END)::bigint as total_fans,
        SUM(monthly_gain)::bigint as total_gain,
        SUM(active_days)::int as active_days,
        COUNT(*)::int as months_active,
        MAX(CASE WHEN rn = 1 THEN avg_3d END) as avg_3d,
        MAX(CASE WHEN rn = 1 THEN avg_7d END) as avg_7d,
        MAX(CASE WHEN rn = 1 THEN circle_id END) as circle_id,
        MAX(CASE WHEN rn = 1 THEN circle_name END) as circle_name
    FROM ranked
    GROUP BY viewer_id
)
SELECT
    viewer_id,
    trainer_name,
    total_fans,
    total_gain,
    active_days,
    (total_gain::float8 / NULLIF(active_days, 0)) as avg_daily,
    avg_3d,
    avg_7d,
    (total_gain::float8 / NULLIF(months_active, 0)) as avg_monthly,
    RANK() OVER (ORDER BY total_gain DESC)::int as rank,
    RANK() OVER (ORDER BY total_gain DESC)::int as rank_total_gain,
    RANK() OVER (ORDER BY total_fans DESC)::int as rank_total_fans,
    RANK() OVER (ORDER BY active_days DESC)::int as rank_active_days,
    RANK() OVER (ORDER BY (total_gain::float8 / NULLIF(active_days, 0)) DESC NULLS LAST)::int as rank_avg_daily,
    RANK() OVER (ORDER BY avg_3d DESC NULLS LAST)::int as rank_avg_3d,
    RANK() OVER (ORDER BY avg_7d DESC NULLS LAST)::int as rank_avg_7d,
    RANK() OVER (ORDER BY (total_gain::float8 / NULLIF(months_active, 0)) DESC NULLS LAST)::int as rank_avg_monthly,
    circle_id,
    circle_name
FROM aggregated;

-- Indexes
CREATE UNIQUE INDEX idx_ufr_alltime_pk
    ON user_fan_rankings_alltime (viewer_id);

CREATE INDEX idx_ufr_alltime_rank
    ON user_fan_rankings_alltime (rank);

CREATE INDEX idx_ufr_alltime_name
    ON user_fan_rankings_alltime USING gin (trainer_name gin_trgm_ops);

CREATE INDEX idx_ufr_alltime_total_fans
    ON user_fan_rankings_alltime (total_fans DESC);

CREATE INDEX idx_ufr_alltime_total_gain
    ON user_fan_rankings_alltime (total_gain DESC);

CREATE INDEX idx_ufr_alltime_active_days
    ON user_fan_rankings_alltime (active_days DESC);

CREATE INDEX idx_ufr_alltime_rank_total_gain
    ON user_fan_rankings_alltime (rank_total_gain);

CREATE INDEX idx_ufr_alltime_rank_total_fans
    ON user_fan_rankings_alltime (rank_total_fans);

CREATE INDEX idx_ufr_alltime_rank_active_days
    ON user_fan_rankings_alltime (rank_active_days);

CREATE INDEX idx_ufr_alltime_rank_avg_daily
    ON user_fan_rankings_alltime (rank_avg_daily);

CREATE INDEX idx_ufr_alltime_rank_avg_3d
    ON user_fan_rankings_alltime (rank_avg_3d);

CREATE INDEX idx_ufr_alltime_rank_avg_7d
    ON user_fan_rankings_alltime (rank_avg_7d);

CREATE INDEX idx_ufr_alltime_rank_avg_monthly
    ON user_fan_rankings_alltime (rank_avg_monthly);
