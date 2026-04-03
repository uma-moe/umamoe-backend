-- ============================================================================
-- User Fan Rankings Materialized Views
-- ============================================================================
-- IMPORTANT: daily_fans[] stores CUMULATIVE fan totals, not incremental gains.
-- e.g. daily_fans = [2000000000, 2030000000, 2065000000, ...]
-- So the gain for a period = value_at_end - value_at_start.
--
-- Three views:
--   1. user_fan_rankings_monthly  - per month: total fans, gain, averages
--   2. user_fan_rankings_alltime  - all-time:  total fans, gain, averages
--   3. user_fan_rankings_gains    - rolling 3d/7d/30d gain rankings
--
-- All require at least 3 non-zero daily_fans entries to qualify.
-- Refreshed hourly.
-- ============================================================================

-- ============================================================================
-- VIEW 1: Monthly fan rankings (ALL months)
--
-- Columns:
--   total_fans   = cumulative fan count at end of month (MAX value)
--   monthly_gain = fans gained that month (MAX - MIN cumulative)
--   avg_daily    = monthly_gain / active_days
--   avg_3d       = gain over last 3 days of month / 3
--   avg_7d       = gain over last 7 days of month / 7
--   avg_monthly  = monthly_gain / calendar days in month
-- ============================================================================

DROP VIEW IF EXISTS user_fan_rankings_monthly;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_monthly;

CREATE MATERIALIZED VIEW user_fan_rankings_monthly AS
WITH daily_expanded AS (
    SELECT
        cm.viewer_id,
        cm.year,
        cm.month,
        d.ord::int as day_of_month,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1) + interval '1 month' - interval '1 day'))
),
viewer_daily AS (
    -- Deduplicate: if a viewer appears in multiple circles on the same day,
    -- take the MAX (cumulative totals are the same regardless of circle)
    SELECT
        viewer_id, year, month, day_of_month,
        MAX(cumulative_fans) as cumulative_fans
    FROM daily_expanded
    GROUP BY viewer_id, year, month, day_of_month
),
viewer_monthly_stats AS (
    SELECT
        viewer_id, year, month,
        MAX(cumulative_fans)::bigint as total_fans,
        (MAX(cumulative_fans) - MIN(cumulative_fans))::bigint as monthly_gain,
        COUNT(*)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
),
lookback AS (
    SELECT
        vd.viewer_id, vd.year, vd.month,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 3 THEN vd.cumulative_fans END) as fans_3d_ago,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 7 THEN vd.cumulative_fans END) as fans_7d_ago
    FROM viewer_daily vd
    JOIN viewer_monthly_stats vms
        ON vd.viewer_id = vms.viewer_id AND vd.year = vms.year AND vd.month = vms.month
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
        ELSE NULL
    END as avg_3d,
    CASE WHEN lb.fans_7d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_7d_ago)::float8 / 7.0)
        ELSE NULL
    END as avg_7d,
    (vms.monthly_gain::float8 /
        extract(day from (make_date(vms.year, vms.month, 1) + interval '1 month' - interval '1 day'))
    ) as avg_monthly,
    RANK() OVER (PARTITION BY vms.year, vms.month ORDER BY vms.monthly_gain DESC)::int as rank
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_ufr_monthly_pk
ON user_fan_rankings_monthly (viewer_id, year, month);

CREATE INDEX idx_ufr_monthly_rank
ON user_fan_rankings_monthly (year, month, rank);

CREATE INDEX idx_ufr_monthly_name
ON user_fan_rankings_monthly USING gin (trainer_name gin_trgm_ops);

CREATE INDEX idx_ufr_monthly_gain
ON user_fan_rankings_monthly (year, month, monthly_gain DESC);

CREATE INDEX idx_ufr_monthly_total_fans
ON user_fan_rankings_monthly (year, month, total_fans DESC);

CREATE INDEX idx_ufr_monthly_active_days
ON user_fan_rankings_monthly (year, month, active_days DESC);

CREATE INDEX idx_ufr_monthly_viewer
ON user_fan_rankings_monthly (viewer_id);


-- ============================================================================
-- VIEW 2: All-time fan rankings
--
-- Columns:
--   total_fans  = latest (current) cumulative fan count
--   total_gain  = all-time gain (latest - earliest)
--   avg_daily   = total_gain / active_days
--   avg_3d      = (latest - value 3 days ago) / 3
--   avg_7d      = (latest - value 7 days ago) / 7
--   avg_monthly = total_gain / months_active
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;

CREATE MATERIALIZED VIEW user_fan_rankings_alltime AS
WITH daily_expanded AS (
    SELECT
        cm.viewer_id,
        cm.year,
        cm.month,
        make_date(cm.year, cm.month, 1) + (d.ord::int - 1) as fan_date,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1) + interval '1 month' - interval '1 day'))
),
viewer_daily AS (
    SELECT viewer_id, year, month, fan_date, MAX(cumulative_fans) as cumulative_fans
    FROM daily_expanded
    GROUP BY viewer_id, year, month, fan_date
),
viewer_totals AS (
    SELECT
        viewer_id,
        MAX(cumulative_fans)::bigint as total_fans,
        (MAX(cumulative_fans) - MIN(cumulative_fans))::bigint as total_gain,
        COUNT(*)::int as active_days,
        COUNT(DISTINCT (year * 100 + month))::int as months_active
    FROM viewer_daily
    GROUP BY viewer_id
    HAVING COUNT(*) >= 3
),
ref AS (
    SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '1 day')::date as ref_date
),
lookback AS (
    SELECT
        vd.viewer_id,
        MAX(CASE WHEN vd.fan_date <= r.ref_date THEN vd.cumulative_fans END) as latest_fans,
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 3 THEN vd.cumulative_fans END) as fans_3d_ago,
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 7 THEN vd.cumulative_fans END) as fans_7d_ago
    FROM viewer_daily vd
    CROSS JOIN ref r
    JOIN viewer_totals vt ON vd.viewer_id = vt.viewer_id
    GROUP BY vd.viewer_id
)
SELECT
    vt.viewer_id,
    t.name as trainer_name,
    vt.total_fans,
    vt.total_gain,
    vt.active_days,
    (vt.total_gain::float8 / NULLIF(vt.active_days, 0)) as avg_daily,
    CASE WHEN lb.fans_3d_ago IS NOT NULL AND lb.latest_fans IS NOT NULL
        THEN ((lb.latest_fans - lb.fans_3d_ago)::float8 / 3.0)
        ELSE NULL
    END as avg_3d,
    CASE WHEN lb.fans_7d_ago IS NOT NULL AND lb.latest_fans IS NOT NULL
        THEN ((lb.latest_fans - lb.fans_7d_ago)::float8 / 7.0)
        ELSE NULL
    END as avg_7d,
    (vt.total_gain::float8 / NULLIF(vt.months_active, 0)) as avg_monthly,
    RANK() OVER (ORDER BY vt.total_gain DESC)::int as rank
FROM viewer_totals vt
LEFT JOIN lookback lb ON vt.viewer_id = lb.viewer_id
LEFT JOIN trainer t ON vt.viewer_id::text = t.account_id;

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
-- VIEW 3: Rolling gain rankings (3-day, 7-day, 30-day)
-- Gain = cumulative value on ref_date minus value N days earlier
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_gains;

CREATE MATERIALIZED VIEW user_fan_rankings_gains AS
WITH
ref AS (
    SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '1 day')::date as ref_date
),
daily_expanded AS (
    SELECT
        cm.viewer_id,
        make_date(cm.year, cm.month, 1) + (d.ord::int - 1) as fan_date,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1) + interval '1 month' - interval '1 day'))
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
lookbacks AS (
    SELECT
        vd.viewer_id,
        MAX(CASE WHEN vd.fan_date <= r.ref_date THEN vd.cumulative_fans END) as latest_fans,
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 3 THEN vd.cumulative_fans END) as fans_3d_ago,
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 7 THEN vd.cumulative_fans END) as fans_7d_ago,
        MAX(CASE WHEN vd.fan_date <= r.ref_date - 30 THEN vd.cumulative_fans END) as fans_30d_ago
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
    RANK() OVER (ORDER BY COALESCE(lb.latest_fans - lb.fans_30d_ago, 0) DESC)::int as rank_30d
FROM lookbacks lb
LEFT JOIN trainer t ON lb.viewer_id::text = t.account_id
WHERE lb.latest_fans IS NOT NULL
  AND (lb.fans_3d_ago IS NOT NULL OR lb.fans_7d_ago IS NOT NULL OR lb.fans_30d_ago IS NOT NULL);

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

ANALYZE circle_member_fans_monthly;
