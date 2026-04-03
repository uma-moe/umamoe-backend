-- ============================================================================
-- User Fan Rankings — Optimized with Archive/Current Split
-- ============================================================================
-- IMPORTANT: daily_fans[] stores CUMULATIVE fan totals, not incremental gains.
--
-- Architecture:
--   - user_fan_rankings_monthly_archive  TABLE  : completed months (static)
--   - user_fan_rankings_monthly_current  MATVIEW: last 2 months (refreshed hourly)
--   - user_fan_rankings_monthly          VIEW   : UNION ALL of archive + current
--   - user_fan_rankings_alltime          MATVIEW: aggregated from monthly data (fast)
--   - user_fan_rankings_gains            MATVIEW: rolling 3d/7d/30d from raw data
--   - archive_fan_rankings_month()       FUNC   : archives a single month
--
-- Past months never change, so they're computed once and stored in the archive
-- table. Only the current period (last 2 months) is recomputed on refresh.
-- This reduces refresh from ~2 minutes (all history) to seconds.
-- ============================================================================

-- Cleanup any existing objects
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_gains;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_monthly;
DROP VIEW IF EXISTS user_fan_rankings_monthly;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_monthly_current;
DROP TABLE IF EXISTS user_fan_rankings_monthly_archive;
DROP FUNCTION IF EXISTS archive_fan_rankings_month;


-- ============================================================================
-- PART 1: Archive table for completed months
-- ============================================================================

CREATE TABLE user_fan_rankings_monthly_archive (
    viewer_id   bigint  NOT NULL,
    trainer_name text,
    year        int     NOT NULL,
    month       int     NOT NULL,
    total_fans  bigint  NOT NULL,
    monthly_gain bigint NOT NULL,
    active_days int     NOT NULL,
    avg_daily   float8,
    avg_3d      float8,
    avg_7d      float8,
    avg_monthly float8,
    rank        int     NOT NULL,
    PRIMARY KEY (year, month, viewer_id)
);

CREATE INDEX idx_ufr_archive_rank
    ON user_fan_rankings_monthly_archive (year, month, rank);

CREATE INDEX idx_ufr_archive_name
    ON user_fan_rankings_monthly_archive USING gin (trainer_name gin_trgm_ops);

CREATE INDEX idx_ufr_archive_gain
    ON user_fan_rankings_monthly_archive (year, month, monthly_gain DESC);

CREATE INDEX idx_ufr_archive_total_fans
    ON user_fan_rankings_monthly_archive (year, month, total_fans DESC);

CREATE INDEX idx_ufr_archive_active_days
    ON user_fan_rankings_monthly_archive (year, month, active_days DESC);

CREATE INDEX idx_ufr_archive_viewer
    ON user_fan_rankings_monthly_archive (viewer_id);


-- ============================================================================
-- PART 2: Function to archive a single month
-- Called by Rust background task to archive completed months.
-- ============================================================================

CREATE OR REPLACE FUNCTION archive_fan_rankings_month(p_year int, p_month int)
RETURNS void
LANGUAGE sql AS $$
INSERT INTO user_fan_rankings_monthly_archive
    (viewer_id, trainer_name, year, month, total_fans, monthly_gain,
     active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank)
WITH daily_expanded AS (
    SELECT
        cm.viewer_id, cm.year, cm.month,
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
viewer_monthly_stats AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as total_fans,
        (MAX(cumulative_fans) - MIN(cumulative_fans))::bigint as monthly_gain,
        COUNT(*)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
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
                 ORDER BY vms.monthly_gain DESC)::int
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id
ON CONFLICT (year, month, viewer_id) DO NOTHING;
$$;


-- ============================================================================
-- PART 3: Populate archive with all months before the 2-month window
-- (one-time operation during migration)
-- ============================================================================

INSERT INTO user_fan_rankings_monthly_archive
    (viewer_id, trainer_name, year, month, total_fans, monthly_gain,
     active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank)
WITH daily_expanded AS (
    SELECT
        cm.viewer_id, cm.year, cm.month,
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
viewer_monthly_stats AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as total_fans,
        (MAX(cumulative_fans) - MIN(cumulative_fans))::bigint as monthly_gain,
        COUNT(*)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
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
                 ORDER BY vms.monthly_gain DESC)::int
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id;


-- ============================================================================
-- PART 4: Current-period materialized view (last 2 months only)
-- This is the ONLY part that gets refreshed. Fast because it's ~2 months
-- instead of all history.
-- ============================================================================

CREATE MATERIALIZED VIEW user_fan_rankings_monthly_current AS
WITH daily_expanded AS (
    SELECT
        cm.viewer_id, cm.year, cm.month,
        d.ord::int as day_of_month,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1)
                   + interval '1 month' - interval '1 day'))
      -- Only the current 2-month window
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
viewer_monthly_stats AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as total_fans,
        (MAX(cumulative_fans) - MIN(cumulative_fans))::bigint as monthly_gain,
        COUNT(*)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
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
                 ORDER BY vms.monthly_gain DESC)::int as rank
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id;

-- Unique index for CONCURRENTLY refresh
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
-- PART 5: Combining VIEW (transparent to handlers)
-- Handlers query user_fan_rankings_monthly — this just unions archive + current.
-- The date filter ensures no overlap: archive has old months, current has recent.
-- ============================================================================

CREATE VIEW user_fan_rankings_monthly AS
SELECT viewer_id, trainer_name, year, month, total_fans, monthly_gain,
       active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank
FROM user_fan_rankings_monthly_archive
WHERE make_date(year, month, 1) < date_trunc('month',
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date
        - interval '1 month')
UNION ALL
SELECT viewer_id, trainer_name, year, month, total_fans, monthly_gain,
       active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank
FROM user_fan_rankings_monthly_current;


-- ============================================================================
-- PART 6: All-time rankings (from pre-aggregated monthly data — FAST)
-- Instead of scanning raw daily_fans arrays for all time, this aggregates
-- from the monthly view which is already computed.
-- ============================================================================

CREATE MATERIALIZED VIEW user_fan_rankings_alltime AS
WITH all_monthly AS (
    SELECT viewer_id, trainer_name, year, month,
           total_fans, monthly_gain, active_days, avg_3d, avg_7d
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
        MAX(CASE WHEN rn = 1 THEN avg_7d END) as avg_7d
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
    RANK() OVER (ORDER BY total_gain DESC)::int as rank
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
-- PART 7: Rolling gain rankings (3d/7d/30d from raw data, 60-day scope)
-- Refreshed once per day (this is the expensive view).
-- ============================================================================

CREATE MATERIALIZED VIEW user_fan_rankings_gains AS
WITH
ref AS (
    SELECT ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')
            - interval '1 day')::date as ref_date
),
daily_expanded AS (
    SELECT
        cm.viewer_id,
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
    RANK() OVER (ORDER BY COALESCE(lb.latest_fans - lb.fans_30d_ago, 0) DESC)::int as rank_30d
FROM lookbacks lb
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

ANALYZE circle_member_fans_monthly;
ANALYZE user_fan_rankings_monthly_archive;
