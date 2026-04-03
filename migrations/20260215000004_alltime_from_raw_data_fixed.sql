-- ============================================================================
-- Rebuild alltime rankings from raw data with correct first/last logic
-- ============================================================================
-- Previous version used MIN(cumulative_fans) for "earliest" which picks the
-- lowest value across all dates, not the actual first recorded value.
-- This version uses DISTINCT ON to get the real first and last data points.
--
-- All values computed directly from circle_member_fans_monthly.daily_fans[]:
--   total_fans  = cumulative fans on the most recent recorded day
--   total_gain  = latest cumulative - first recorded cumulative
--   active_days = distinct days with fan data
--   avg_day     = total_gain / active_days
--   avg_week    = gain over last 7 days (latest - value 7 days ago)
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
-- Actual first recorded data point per user
viewer_first AS (
    SELECT DISTINCT ON (viewer_id)
        viewer_id,
        cumulative_fans as first_fans
    FROM viewer_daily
    ORDER BY viewer_id, fan_date ASC
),
-- Actual latest data point per user (up to ref_date)
viewer_latest AS (
    SELECT DISTINCT ON (vd.viewer_id)
        vd.viewer_id,
        vd.cumulative_fans as latest_fans
    FROM viewer_daily vd
    CROSS JOIN ref r
    WHERE vd.fan_date <= r.ref_date
    ORDER BY vd.viewer_id, vd.fan_date DESC
),
-- Value from ~7 days ago for avg_week
viewer_7d_ago AS (
    SELECT DISTINCT ON (vd.viewer_id)
        vd.viewer_id,
        vd.cumulative_fans as fans_7d_ago
    FROM viewer_daily vd
    CROSS JOIN ref r
    WHERE vd.fan_date <= r.ref_date - 7
    ORDER BY vd.viewer_id, vd.fan_date DESC
),
-- Aggregate counts
viewer_counts AS (
    SELECT
        vd.viewer_id,
        COUNT(*)::int as active_days,
        COUNT(DISTINCT date_trunc('month', vd.fan_date))::int as months_active
    FROM viewer_daily vd
    CROSS JOIN ref r
    WHERE vd.fan_date <= r.ref_date
    GROUP BY vd.viewer_id
    HAVING COUNT(*) >= 3
),
-- Current circle from most recent raw data
current_circle AS (
    SELECT DISTINCT ON (viewer_id)
        viewer_id, circle_id
    FROM daily_expanded
    ORDER BY viewer_id, fan_date DESC, cumulative_fans DESC
),
-- Combine everything
combined AS (
    SELECT
        vc.viewer_id,
        vl.latest_fans,
        vf.first_fans,
        (vl.latest_fans - vf.first_fans) as total_gain,
        vc.active_days,
        vc.months_active,
        v7.fans_7d_ago,
        cc.circle_id
    FROM viewer_counts vc
    JOIN viewer_latest vl ON vc.viewer_id = vl.viewer_id
    JOIN viewer_first vf ON vc.viewer_id = vf.viewer_id
    LEFT JOIN viewer_7d_ago v7 ON vc.viewer_id = v7.viewer_id
    LEFT JOIN current_circle cc ON vc.viewer_id = cc.viewer_id
)
SELECT
    cb.viewer_id,
    t.name as trainer_name,
    cb.latest_fans::bigint as total_fans,
    cb.total_gain::bigint as total_gain,
    cb.active_days,
    (cb.total_gain::float8 / NULLIF(cb.active_days, 0)) as avg_day,
    CASE WHEN cb.fans_7d_ago IS NOT NULL
        THEN (cb.latest_fans - cb.fans_7d_ago)::float8
        ELSE NULL END as avg_week,
    (cb.total_gain::float8 / NULLIF(cb.months_active, 0)) as avg_month,
    -- Legacy rank (= rank_total_gain)
    RANK() OVER (ORDER BY cb.total_gain DESC)::int as rank,
    RANK() OVER (ORDER BY cb.latest_fans DESC)::int as rank_total_fans,
    RANK() OVER (ORDER BY cb.total_gain DESC)::int as rank_total_gain,
    RANK() OVER (ORDER BY (cb.total_gain::float8
        / NULLIF(cb.active_days, 0)) DESC NULLS LAST)::int as rank_avg_day,
    RANK() OVER (ORDER BY (CASE WHEN cb.fans_7d_ago IS NOT NULL
        THEN (cb.latest_fans - cb.fans_7d_ago)::float8
        ELSE NULL END) DESC NULLS LAST)::int as rank_avg_week,
    RANK() OVER (ORDER BY (cb.total_gain::float8
        / NULLIF(cb.months_active, 0)) DESC NULLS LAST)::int as rank_avg_month,
    cb.circle_id,
    c.name as circle_name
FROM combined cb
LEFT JOIN circles c ON cb.circle_id = c.circle_id
LEFT JOIN trainer t ON cb.viewer_id::text = t.account_id
WHERE cb.latest_fans IS NOT NULL;

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
