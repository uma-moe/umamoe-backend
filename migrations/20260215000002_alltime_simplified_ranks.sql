-- ============================================================================
-- Simplify alltime rankings: 5 sort criteria with per-criteria ranks
-- ============================================================================
-- Rank columns: rank_total_fans, rank_total_gain, rank_avg_day, rank_avg_week, rank_avg_month
-- avg_week = total 7-day gain (not per-day average)
-- avg_day  = total_gain / active_days
-- avg_month = total_gain / months_active
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;

CREATE MATERIALIZED VIEW user_fan_rankings_alltime AS
WITH all_monthly AS (
    SELECT viewer_id, trainer_name, year, month,
           total_fans, monthly_gain, active_days, avg_7d,
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
        -- avg_7d is per-day average over last 7 days; multiply by 7 for weekly total
        MAX(CASE WHEN rn = 1 THEN avg_7d END) as avg_7d_per_day,
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
    (total_gain::float8 / NULLIF(active_days, 0)) as avg_day,
    (avg_7d_per_day * 7.0) as avg_week,
    (total_gain::float8 / NULLIF(months_active, 0)) as avg_month,
    RANK() OVER (ORDER BY total_gain DESC)::int as rank,
    RANK() OVER (ORDER BY total_fans DESC)::int as rank_total_fans,
    RANK() OVER (ORDER BY total_gain DESC)::int as rank_total_gain,
    RANK() OVER (ORDER BY (total_gain::float8 / NULLIF(active_days, 0)) DESC NULLS LAST)::int as rank_avg_day,
    RANK() OVER (ORDER BY (avg_7d_per_day * 7.0) DESC NULLS LAST)::int as rank_avg_week,
    RANK() OVER (ORDER BY (total_gain::float8 / NULLIF(months_active, 0)) DESC NULLS LAST)::int as rank_avg_month,
    circle_id,
    circle_name
FROM aggregated;

-- Indexes
CREATE UNIQUE INDEX idx_ufr_alltime_pk
    ON user_fan_rankings_alltime (viewer_id);

CREATE INDEX idx_ufr_alltime_name
    ON user_fan_rankings_alltime USING gin (trainer_name gin_trgm_ops);

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
