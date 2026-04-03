-- ============================================================================
-- Add circle info to rankings + fix active_days calculation
-- ============================================================================
-- Changes:
--   1. Add circle_id (bigint) and circle_name (text) to all ranking views
--      - Monthly: circle the user was in at END of month (last active day)
--      - Alltime: user's CURRENT circle (most recent month)
--      - Gains: user's CURRENT circle (most recent data point)
--   2. Fix active_days to count only days where fans actually INCREASED
--      (positive delta vs previous day, not just non-zero cumulative)
-- ============================================================================

-- Add columns to archive table
ALTER TABLE user_fan_rankings_monthly_archive
    ADD COLUMN IF NOT EXISTS circle_id bigint,
    ADD COLUMN IF NOT EXISTS circle_name text;

-- Drop dependent objects in correct order
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_gains;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;
DROP VIEW IF EXISTS user_fan_rankings_monthly;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_monthly_current;


-- ============================================================================
-- Re-populate archive with corrected active_days + circle info
-- ============================================================================

TRUNCATE user_fan_rankings_monthly_archive;

INSERT INTO user_fan_rankings_monthly_archive
    (viewer_id, trainer_name, year, month, total_fans, monthly_gain,
     active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
     circle_id, circle_name)
WITH daily_expanded AS (
    SELECT
        cm.viewer_id, cm.year, cm.month, cm.circle_id,
        d.ord::int as day_of_month,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1)
                   + interval '1 month' - interval '1 day'))
      -- Only past months (before the 2-month window)
      AND make_date(cm.year, cm.month, 1) < date_trunc('month',
            (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date
            - interval '1 month')
),
viewer_daily AS (
    SELECT viewer_id, year, month, day_of_month,
           MAX(cumulative_fans) as cumulative_fans
    FROM daily_expanded
    GROUP BY viewer_id, year, month, day_of_month
),
viewer_daily_with_delta AS (
    SELECT *,
           (cumulative_fans - COALESCE(
               LAG(cumulative_fans) OVER (
                   PARTITION BY viewer_id, year, month
                   ORDER BY day_of_month
               ), 0
           )) as daily_delta
    FROM viewer_daily
),
viewer_monthly_stats AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as total_fans,
        (MAX(cumulative_fans) - MIN(cumulative_fans))::bigint as monthly_gain,
        COUNT(*) FILTER (WHERE daily_delta > 0)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily_with_delta
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
),
-- Circle the user was in on the last active day of the month
end_of_month_circle AS (
    SELECT DISTINCT ON (viewer_id, year, month)
        viewer_id, year, month, circle_id
    FROM daily_expanded
    ORDER BY viewer_id, year, month, day_of_month DESC, cumulative_fans DESC
),
lookback AS (
    SELECT vd.viewer_id, vd.year, vd.month,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 3
            THEN vd.cumulative_fans END) as fans_3d_ago,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 7
            THEN vd.cumulative_fans END) as fans_7d_ago
    FROM viewer_daily vd
    JOIN viewer_monthly_stats vms
        ON vd.viewer_id = vms.viewer_id
        AND vd.year = vms.year AND vd.month = vms.month
    GROUP BY vd.viewer_id, vd.year, vd.month
)
SELECT
    vms.viewer_id, t.name, vms.year, vms.month,
    vms.total_fans, vms.monthly_gain, vms.active_days,
    (vms.monthly_gain::float8 / NULLIF(vms.active_days, 0)),
    CASE WHEN lb.fans_3d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_3d_ago)::float8 / 3.0)
        ELSE NULL END,
    CASE WHEN lb.fans_7d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_7d_ago)::float8 / 7.0)
        ELSE NULL END,
    (vms.monthly_gain::float8 / extract(day from (
        make_date(vms.year, vms.month, 1) + interval '1 month' - interval '1 day'))),
    RANK() OVER (PARTITION BY vms.year, vms.month
                 ORDER BY vms.monthly_gain DESC)::int,
    eoc.circle_id,
    c.name
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN end_of_month_circle eoc
    ON vms.viewer_id = eoc.viewer_id
    AND vms.year = eoc.year AND vms.month = eoc.month
LEFT JOIN circles c ON eoc.circle_id = c.circle_id
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id;


-- ============================================================================
-- Updated archive function (with circle + fixed active_days)
-- ============================================================================

CREATE OR REPLACE FUNCTION archive_fan_rankings_month(p_year int, p_month int)
RETURNS void
LANGUAGE sql AS $$
INSERT INTO user_fan_rankings_monthly_archive
    (viewer_id, trainer_name, year, month, total_fans, monthly_gain,
     active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
     circle_id, circle_name)
WITH daily_expanded AS (
    SELECT
        cm.viewer_id, cm.year, cm.month, cm.circle_id,
        d.ord::int as day_of_month,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1)
                   + interval '1 month' - interval '1 day'))
      AND cm.year = p_year AND cm.month = p_month
),
viewer_daily AS (
    SELECT viewer_id, year, month, day_of_month,
           MAX(cumulative_fans) as cumulative_fans
    FROM daily_expanded
    GROUP BY viewer_id, year, month, day_of_month
),
viewer_daily_with_delta AS (
    SELECT *,
           (cumulative_fans - COALESCE(
               LAG(cumulative_fans) OVER (
                   PARTITION BY viewer_id, year, month
                   ORDER BY day_of_month
               ), 0
           )) as daily_delta
    FROM viewer_daily
),
viewer_monthly_stats AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as total_fans,
        (MAX(cumulative_fans) - MIN(cumulative_fans))::bigint as monthly_gain,
        COUNT(*) FILTER (WHERE daily_delta > 0)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily_with_delta
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
),
end_of_month_circle AS (
    SELECT DISTINCT ON (viewer_id, year, month)
        viewer_id, year, month, circle_id
    FROM daily_expanded
    ORDER BY viewer_id, year, month, day_of_month DESC, cumulative_fans DESC
),
lookback AS (
    SELECT vd.viewer_id, vd.year, vd.month,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 3
            THEN vd.cumulative_fans END) as fans_3d_ago,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 7
            THEN vd.cumulative_fans END) as fans_7d_ago
    FROM viewer_daily vd
    JOIN viewer_monthly_stats vms
        ON vd.viewer_id = vms.viewer_id
        AND vd.year = vms.year AND vd.month = vms.month
    GROUP BY vd.viewer_id, vd.year, vd.month
)
SELECT
    vms.viewer_id, t.name, vms.year, vms.month,
    vms.total_fans, vms.monthly_gain, vms.active_days,
    (vms.monthly_gain::float8 / NULLIF(vms.active_days, 0)),
    CASE WHEN lb.fans_3d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_3d_ago)::float8 / 3.0)
        ELSE NULL END,
    CASE WHEN lb.fans_7d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_7d_ago)::float8 / 7.0)
        ELSE NULL END,
    (vms.monthly_gain::float8 / extract(day from (
        make_date(vms.year, vms.month, 1) + interval '1 month' - interval '1 day'))),
    RANK() OVER (PARTITION BY vms.year, vms.month
                 ORDER BY vms.monthly_gain DESC)::int,
    eoc.circle_id,
    c.name
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN end_of_month_circle eoc
    ON vms.viewer_id = eoc.viewer_id
    AND vms.year = eoc.year AND vms.month = eoc.month
LEFT JOIN circles c ON eoc.circle_id = c.circle_id
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id
ON CONFLICT (year, month, viewer_id) DO NOTHING;
$$;


-- ============================================================================
-- Recreate current-period matview (last 2 months, with circle + fixed active_days)
-- ============================================================================

CREATE MATERIALIZED VIEW user_fan_rankings_monthly_current AS
WITH daily_expanded AS (
    SELECT
        cm.viewer_id, cm.year, cm.month, cm.circle_id,
        d.ord::int as day_of_month,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1)
                   + interval '1 month' - interval '1 day'))
      AND make_date(cm.year, cm.month, 1) >= date_trunc('month',
            (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date
            - interval '1 month')
),
viewer_daily AS (
    SELECT viewer_id, year, month, day_of_month,
           MAX(cumulative_fans) as cumulative_fans
    FROM daily_expanded
    GROUP BY viewer_id, year, month, day_of_month
),
viewer_daily_with_delta AS (
    SELECT *,
           (cumulative_fans - COALESCE(
               LAG(cumulative_fans) OVER (
                   PARTITION BY viewer_id, year, month
                   ORDER BY day_of_month
               ), 0
           )) as daily_delta
    FROM viewer_daily
),
viewer_monthly_stats AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as total_fans,
        (MAX(cumulative_fans) - MIN(cumulative_fans))::bigint as monthly_gain,
        COUNT(*) FILTER (WHERE daily_delta > 0)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily_with_delta
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
),
end_of_month_circle AS (
    SELECT DISTINCT ON (viewer_id, year, month)
        viewer_id, year, month, circle_id
    FROM daily_expanded
    ORDER BY viewer_id, year, month, day_of_month DESC, cumulative_fans DESC
),
lookback AS (
    SELECT vd.viewer_id, vd.year, vd.month,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 3
            THEN vd.cumulative_fans END) as fans_3d_ago,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 7
            THEN vd.cumulative_fans END) as fans_7d_ago
    FROM viewer_daily vd
    JOIN viewer_monthly_stats vms
        ON vd.viewer_id = vms.viewer_id
        AND vd.year = vms.year AND vd.month = vms.month
    GROUP BY vd.viewer_id, vd.year, vd.month
)
SELECT
    vms.viewer_id,
    t.name as trainer_name,
    vms.year,
    vms.month,
    vms.total_fans,
    vms.monthly_gain,
    vms.active_days,
    (vms.monthly_gain::float8 / NULLIF(vms.active_days, 0)) as avg_daily,
    CASE WHEN lb.fans_3d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_3d_ago)::float8 / 3.0)
        ELSE NULL END as avg_3d,
    CASE WHEN lb.fans_7d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_7d_ago)::float8 / 7.0)
        ELSE NULL END as avg_7d,
    (vms.monthly_gain::float8 / extract(day from (
        make_date(vms.year, vms.month, 1) + interval '1 month' - interval '1 day'
    ))) as avg_monthly,
    RANK() OVER (PARTITION BY vms.year, vms.month
                 ORDER BY vms.monthly_gain DESC)::int as rank,
    eoc.circle_id,
    c.name as circle_name
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN end_of_month_circle eoc
    ON vms.viewer_id = eoc.viewer_id
    AND vms.year = eoc.year AND vms.month = eoc.month
LEFT JOIN circles c ON eoc.circle_id = c.circle_id
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id;

-- Indexes for current matview
CREATE UNIQUE INDEX idx_ufr_current_pk
    ON user_fan_rankings_monthly_current (viewer_id, year, month);

CREATE INDEX idx_ufr_current_rank
    ON user_fan_rankings_monthly_current (year, month, rank);

CREATE INDEX idx_ufr_current_name
    ON user_fan_rankings_monthly_current USING gin (trainer_name gin_trgm_ops);

CREATE INDEX idx_ufr_current_gain
    ON user_fan_rankings_monthly_current (year, month, monthly_gain DESC);

CREATE INDEX idx_ufr_current_total_fans
    ON user_fan_rankings_monthly_current (year, month, total_fans DESC);

CREATE INDEX idx_ufr_current_active_days
    ON user_fan_rankings_monthly_current (year, month, active_days DESC);

CREATE INDEX idx_ufr_current_viewer
    ON user_fan_rankings_monthly_current (viewer_id);


-- ============================================================================
-- Combining VIEW (archive + current, with circle columns)
-- ============================================================================

CREATE VIEW user_fan_rankings_monthly AS
SELECT viewer_id, trainer_name, year, month, total_fans, monthly_gain,
       active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
       circle_id, circle_name
FROM user_fan_rankings_monthly_archive
WHERE make_date(year, month, 1) < date_trunc('month',
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date
        - interval '1 month')
UNION ALL
SELECT viewer_id, trainer_name, year, month, total_fans, monthly_gain,
       active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
       circle_id, circle_name
FROM user_fan_rankings_monthly_current;


-- ============================================================================
-- All-time rankings (current circle from most recent month)
-- ============================================================================

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
    circle_id,
    circle_name
FROM aggregated;

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


-- ============================================================================
-- Rolling gain rankings (current circle from most recent data point)
-- ============================================================================

CREATE MATERIALIZED VIEW user_fan_rankings_gains AS
WITH
ref AS (
    SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')
            - interval '1 day')::date as ref_date
),
daily_expanded AS (
    SELECT
        cm.viewer_id, cm.circle_id,
        make_date(cm.year, cm.month, 1) + (d.ord::int - 1) as fan_date,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1)
                   + interval '1 month' - interval '1 day'))
      AND make_date(cm.year, cm.month, 1) >= (CURRENT_DATE - interval '60 days')
),
viewer_daily AS (
    SELECT viewer_id, fan_date, MAX(cumulative_fans) as cumulative_fans
    FROM daily_expanded
    GROUP BY viewer_id, fan_date
),
qualified AS (
    SELECT viewer_id
    FROM viewer_daily
    GROUP BY viewer_id
    HAVING COUNT(*) >= 3
),
-- Current circle: from the most recent data point
current_circle AS (
    SELECT DISTINCT ON (viewer_id)
        viewer_id, circle_id
    FROM daily_expanded
    ORDER BY viewer_id, fan_date DESC, cumulative_fans DESC
),
lookbacks AS (
    SELECT
        vd.viewer_id,
        MAX(CASE WHEN vd.fan_date <= r.ref_date
            THEN vd.cumulative_fans END) as latest_fans,
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 3
            THEN vd.cumulative_fans END) as fans_3d_ago,
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 7
            THEN vd.cumulative_fans END) as fans_7d_ago,
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 30
            THEN vd.cumulative_fans END) as fans_30d_ago
    FROM viewer_daily vd
    CROSS JOIN ref r
    JOIN qualified q ON q.viewer_id = vd.viewer_id
    WHERE vd.fan_date <= r.ref_date
    GROUP BY vd.viewer_id
)
SELECT
    lb.viewer_id,
    t.name as trainer_name,
    COALESCE(lb.latest_fans - lb.fans_3d_ago, 0)::bigint as gain_3d,
    COALESCE(lb.latest_fans - lb.fans_7d_ago, 0)::bigint as gain_7d,
    COALESCE(lb.latest_fans - lb.fans_30d_ago, 0)::bigint as gain_30d,
    RANK() OVER (ORDER BY COALESCE(lb.latest_fans - lb.fans_3d_ago, 0) DESC)::int as rank_3d,
    RANK() OVER (ORDER BY COALESCE(lb.latest_fans - lb.fans_7d_ago, 0) DESC)::int as rank_7d,
    RANK() OVER (ORDER BY COALESCE(lb.latest_fans - lb.fans_30d_ago, 0) DESC)::int as rank_30d,
    cc.circle_id,
    c.name as circle_name
FROM lookbacks lb
LEFT JOIN current_circle cc ON lb.viewer_id = cc.viewer_id
LEFT JOIN circles c ON cc.circle_id = c.circle_id
LEFT JOIN trainer t ON lb.viewer_id::text = t.account_id
WHERE lb.latest_fans IS NOT NULL
  AND (lb.fans_3d_ago IS NOT NULL OR lb.fans_7d_ago IS NOT NULL
       OR lb.fans_30d_ago IS NOT NULL);

CREATE UNIQUE INDEX idx_ufr_gains_pk
    ON user_fan_rankings_gains (viewer_id);

CREATE INDEX idx_ufr_gains_rank_3d
    ON user_fan_rankings_gains (rank_3d);

CREATE INDEX idx_ufr_gains_rank_7d
    ON user_fan_rankings_gains (rank_7d);

CREATE INDEX idx_ufr_gains_rank_30d
    ON user_fan_rankings_gains (rank_30d);

CREATE INDEX idx_ufr_gains_name
    ON user_fan_rankings_gains USING gin (trainer_name gin_trgm_ops);

CREATE INDEX idx_ufr_gains_gain_3d
    ON user_fan_rankings_gains (gain_3d DESC);

CREATE INDEX idx_ufr_gains_gain_7d
    ON user_fan_rankings_gains (gain_7d DESC);

CREATE INDEX idx_ufr_gains_gain_30d
    ON user_fan_rankings_gains (gain_30d DESC);


-- ============================================================================
-- ANALYZE
-- ============================================================================

ANALYZE user_fan_rankings_monthly_archive;
