-- ============================================================================
-- Add next_month_start to monthly rankings + fix total_fans/monthly_gain
-- ============================================================================
-- The daily_fans[] array for month M may not contain the final day's value.
-- That value appears as daily_fans[1] of month M+1.
--
-- CRITICAL FIX: total_fans and monthly_gain now use next_month_start when
-- available, so the last day's gain is included in the ranking calculations.
--   total_fans  = COALESCE(next_month_start, MAX(cumulative_fans))
--   monthly_gain = total_fans - MIN(cumulative_fans)
-- ============================================================================

-- 1. Add column to archive table
ALTER TABLE user_fan_rankings_monthly_archive
    ADD COLUMN IF NOT EXISTS next_month_start bigint;

-- 2. Drop dependent objects
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;
DROP VIEW IF EXISTS user_fan_rankings_monthly;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_monthly_current;

-- 3. Re-populate archive with corrected total_fans/monthly_gain using next_month_start
TRUNCATE user_fan_rankings_monthly_archive;

INSERT INTO user_fan_rankings_monthly_archive
    (viewer_id, trainer_name, year, month, total_fans, monthly_gain,
     active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
     circle_id, circle_name, next_month_start)
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
viewer_monthly_raw AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as max_fans,
        MIN(cumulative_fans)::bigint as min_fans,
        COUNT(*) FILTER (WHERE daily_delta > 0)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily_with_delta
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
),
next_month_lookup AS (
    SELECT
        vmr.viewer_id, vmr.year, vmr.month,
        (SELECT MAX(cm2.daily_fans[1])
         FROM circle_member_fans_monthly cm2
         WHERE cm2.viewer_id = vmr.viewer_id
           AND cm2.year  = CASE WHEN vmr.month = 12 THEN vmr.year + 1 ELSE vmr.year END
           AND cm2.month = CASE WHEN vmr.month = 12 THEN 1 ELSE vmr.month + 1 END
           AND cm2.daily_fans[1] > 0
        )::bigint as next_month_start
    FROM viewer_monthly_raw vmr
),
viewer_monthly_stats AS (
    SELECT
        vmr.viewer_id, vmr.year, vmr.month,
        COALESCE(nml.next_month_start, vmr.max_fans)::bigint as total_fans,
        (COALESCE(nml.next_month_start, vmr.max_fans) - vmr.min_fans)::bigint as monthly_gain,
        vmr.active_days,
        vmr.last_day,
        nml.next_month_start
    FROM viewer_monthly_raw vmr
    LEFT JOIN next_month_lookup nml
        ON vmr.viewer_id = nml.viewer_id
        AND vmr.year = nml.year AND vmr.month = nml.month
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
    c.name,
    vms.next_month_start
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
-- 4. Recreate current matview (total_fans/monthly_gain use next_month_start)
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
viewer_monthly_raw AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as max_fans,
        MIN(cumulative_fans)::bigint as min_fans,
        COUNT(*) FILTER (WHERE daily_delta > 0)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily_with_delta
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
),
next_month_lookup AS (
    SELECT
        vmr.viewer_id, vmr.year, vmr.month,
        (SELECT MAX(cm2.daily_fans[1])
         FROM circle_member_fans_monthly cm2
         WHERE cm2.viewer_id = vmr.viewer_id
           AND cm2.year  = CASE WHEN vmr.month = 12 THEN vmr.year + 1 ELSE vmr.year END
           AND cm2.month = CASE WHEN vmr.month = 12 THEN 1 ELSE vmr.month + 1 END
           AND cm2.daily_fans[1] > 0
        )::bigint as next_month_start
    FROM viewer_monthly_raw vmr
),
viewer_monthly_stats AS (
    SELECT
        vmr.viewer_id, vmr.year, vmr.month,
        COALESCE(nml.next_month_start, vmr.max_fans)::bigint as total_fans,
        (COALESCE(nml.next_month_start, vmr.max_fans) - vmr.min_fans)::bigint as monthly_gain,
        vmr.active_days,
        vmr.last_day,
        nml.next_month_start
    FROM viewer_monthly_raw vmr
    LEFT JOIN next_month_lookup nml
        ON vmr.viewer_id = nml.viewer_id
        AND vmr.year = nml.year AND vmr.month = nml.month
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
    c.name as circle_name,
    vms.next_month_start
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN end_of_month_circle eoc
    ON vms.viewer_id = eoc.viewer_id
    AND vms.year = eoc.year AND vms.month = eoc.month
LEFT JOIN circles c ON eoc.circle_id = c.circle_id
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id;

-- Indexes
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
-- 5. Recreate combining VIEW
-- ============================================================================

CREATE VIEW user_fan_rankings_monthly AS
SELECT viewer_id, trainer_name, year, month, total_fans, monthly_gain,
       active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
       circle_id, circle_name, next_month_start
FROM user_fan_rankings_monthly_archive
WHERE make_date(year, month, 1) < date_trunc('month',
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date
        - interval '1 month')
UNION ALL
SELECT viewer_id, trainer_name, year, month, total_fans, monthly_gain,
       active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
       circle_id, circle_name, next_month_start
FROM user_fan_rankings_monthly_current;


-- ============================================================================
-- 6. Recreate alltime matview
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
-- 7. Update archive function (total_fans/monthly_gain use next_month_start)
-- ============================================================================

CREATE OR REPLACE FUNCTION archive_fan_rankings_month(p_year int, p_month int)
RETURNS void
LANGUAGE sql AS $$
INSERT INTO user_fan_rankings_monthly_archive
    (viewer_id, trainer_name, year, month, total_fans, monthly_gain,
     active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
     circle_id, circle_name, next_month_start)
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
viewer_monthly_raw AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as max_fans,
        MIN(cumulative_fans)::bigint as min_fans,
        COUNT(*) FILTER (WHERE daily_delta > 0)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily_with_delta
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
),
next_month_lookup AS (
    SELECT
        vmr.viewer_id, vmr.year, vmr.month,
        (SELECT MAX(cm2.daily_fans[1])
         FROM circle_member_fans_monthly cm2
         WHERE cm2.viewer_id = vmr.viewer_id
           AND cm2.year  = CASE WHEN vmr.month = 12 THEN vmr.year + 1 ELSE vmr.year END
           AND cm2.month = CASE WHEN vmr.month = 12 THEN 1 ELSE vmr.month + 1 END
           AND cm2.daily_fans[1] > 0
        )::bigint as next_month_start
    FROM viewer_monthly_raw vmr
),
viewer_monthly_stats AS (
    SELECT
        vmr.viewer_id, vmr.year, vmr.month,
        COALESCE(nml.next_month_start, vmr.max_fans)::bigint as total_fans,
        (COALESCE(nml.next_month_start, vmr.max_fans) - vmr.min_fans)::bigint as monthly_gain,
        vmr.active_days,
        vmr.last_day,
        nml.next_month_start
    FROM viewer_monthly_raw vmr
    LEFT JOIN next_month_lookup nml
        ON vmr.viewer_id = nml.viewer_id
        AND vmr.year = nml.year AND vmr.month = nml.month
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
    c.name,
    vms.next_month_start
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN end_of_month_circle eoc
    ON vms.viewer_id = eoc.viewer_id
    AND vms.year = eoc.year AND vms.month = eoc.month
LEFT JOIN circles c ON eoc.circle_id = c.circle_id
LEFT JOIN next_month_lookup nml
    ON vms.viewer_id = nml.viewer_id
    AND vms.year = nml.year AND vms.month = nml.month
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id
ON CONFLICT (year, month, viewer_id) DO NOTHING;
$$;


-- ============================================================================
-- ANALYZE
-- ============================================================================

ANALYZE user_fan_rankings_monthly_archive;
