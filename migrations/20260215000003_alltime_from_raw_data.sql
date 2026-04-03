-- ============================================================================
-- Rebuild alltime rankings from raw daily fan data
-- ============================================================================
-- Previously this was derived from user_fan_rankings_monthly (pre-aggregated
-- monthly summaries). That means total_gain was SUM(monthly_gain) which can
-- miss fans gained between months, and active_days was the sum of per-month
-- counts rather than true distinct days.
--
-- This version computes directly from circle_member_fans_monthly.daily_fans[]
-- (the raw cumulative fan arrays), giving accurate all-time numbers:
--   total_fans  = latest cumulative value
--   total_gain  = latest - earliest cumulative value
--   active_days = actual distinct days with data
--   avg_day     = total_gain / active_days
--   avg_week    = total 7-day gain (latest - 7 days ago)
--   avg_month   = total_gain / months_active
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;

CREATE MATERIALIZED VIEW user_fan_rankings_alltime AS
WITH
ref AS (
    SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')
            - interval '1 day')::date as ref_date
),
daily_expanded AS (
    SELECT
        cm.viewer_id,
        cm.circle_id,
        make_date(cm.year, cm.month, 1) + (d.ord::int - 1) as fan_date,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1)
                   + interval '1 month' - interval '1 day'))
),
viewer_daily AS (
    SELECT viewer_id, fan_date,
           MAX(cumulative_fans) as cumulative_fans
    FROM daily_expanded
    GROUP BY viewer_id, fan_date
),
-- Current circle: from the most recent data point
current_circle AS (
    SELECT DISTINCT ON (viewer_id)
        viewer_id, circle_id
    FROM daily_expanded
    ORDER BY viewer_id, fan_date DESC, cumulative_fans DESC
),
viewer_stats AS (
    SELECT
        vd.viewer_id,
        MAX(CASE WHEN vd.fan_date <= r.ref_date
            THEN vd.cumulative_fans END) as latest_fans,
        MIN(vd.cumulative_fans) as earliest_fans,
        COUNT(*)::int as active_days,
        COUNT(DISTINCT date_trunc('month', vd.fan_date))::int as months_active,
        -- 7-day lookback for avg_week
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 7
            THEN vd.cumulative_fans END) as fans_7d_ago
    FROM viewer_daily vd
    CROSS JOIN ref r
    WHERE vd.fan_date <= r.ref_date
    GROUP BY vd.viewer_id
    HAVING COUNT(*) >= 3
)
SELECT
    vs.viewer_id,
    t.name as trainer_name,
    vs.latest_fans::bigint as total_fans,
    (vs.latest_fans - vs.earliest_fans)::bigint as total_gain,
    vs.active_days,
    ((vs.latest_fans - vs.earliest_fans)::float8
        / NULLIF(vs.active_days, 0)) as avg_day,
    CASE WHEN vs.fans_7d_ago IS NOT NULL
        THEN (vs.latest_fans - vs.fans_7d_ago)::float8
        ELSE NULL END as avg_week,
    ((vs.latest_fans - vs.earliest_fans)::float8
        / NULLIF(vs.months_active, 0)) as avg_month,
    -- Legacy rank (= rank_total_gain)
    RANK() OVER (ORDER BY (vs.latest_fans - vs.earliest_fans) DESC)::int as rank,
    RANK() OVER (ORDER BY vs.latest_fans DESC)::int as rank_total_fans,
    RANK() OVER (ORDER BY (vs.latest_fans - vs.earliest_fans) DESC)::int as rank_total_gain,
    RANK() OVER (ORDER BY ((vs.latest_fans - vs.earliest_fans)::float8
        / NULLIF(vs.active_days, 0)) DESC NULLS LAST)::int as rank_avg_day,
    RANK() OVER (ORDER BY (CASE WHEN vs.fans_7d_ago IS NOT NULL
        THEN (vs.latest_fans - vs.fans_7d_ago)::float8
        ELSE NULL END) DESC NULLS LAST)::int as rank_avg_week,
    RANK() OVER (ORDER BY ((vs.latest_fans - vs.earliest_fans)::float8
        / NULLIF(vs.months_active, 0)) DESC NULLS LAST)::int as rank_avg_month,
    cc.circle_id,
    c.name as circle_name
FROM viewer_stats vs
LEFT JOIN current_circle cc ON vs.viewer_id = cc.viewer_id
LEFT JOIN circles c ON cc.circle_id = c.circle_id
LEFT JOIN trainer t ON vs.viewer_id::text = t.account_id
WHERE vs.latest_fans IS NOT NULL;

-- Indexes
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
