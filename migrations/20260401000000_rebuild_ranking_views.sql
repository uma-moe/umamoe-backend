-- ============================================================================
-- Rebuild all ranking views to correct schema
-- ============================================================================
-- Migration 20260211000000 ran AFTER 20260213-20260215 and recreated the views
-- with the old schema (no circle_id, no per-sort ranks, old column names).
-- This migration restores the correct definitions.
-- ============================================================================

-- 1. Ensure archive table has all required columns
ALTER TABLE user_fan_rankings_monthly_archive
    ADD COLUMN IF NOT EXISTS circle_id bigint,
    ADD COLUMN IF NOT EXISTS circle_name text,
    ADD COLUMN IF NOT EXISTS next_month_start bigint;

-- 2. Drop all dependent objects in correct order
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_gains;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_alltime;
DROP VIEW IF EXISTS user_fan_rankings_monthly;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_monthly;
DROP MATERIALIZED VIEW IF EXISTS user_fan_rankings_monthly_current;


-- ============================================================================
-- 3. Re-populate archive with circle + next_month_start + corrected active_days
-- ============================================================================

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
-- 4. Recreate current-period matview
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
-- 6. Recreate alltime matview (from raw data, with per-sort ranks)
-- ============================================================================

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
viewer_first AS (
    SELECT DISTINCT ON (viewer_id)
        viewer_id,
        cumulative_fans as first_fans
    FROM viewer_daily
    ORDER BY viewer_id, fan_date ASC
),
viewer_latest AS (
    SELECT DISTINCT ON (vd.viewer_id)
        vd.viewer_id,
        vd.cumulative_fans as latest_fans
    FROM viewer_daily vd
    CROSS JOIN ref r
    WHERE vd.fan_date <= r.ref_date
    ORDER BY vd.viewer_id, vd.fan_date DESC
),
viewer_7d_ago AS (
    SELECT DISTINCT ON (vd.viewer_id)
        vd.viewer_id,
        vd.cumulative_fans as fans_7d_ago
    FROM viewer_daily vd
    CROSS JOIN ref r
    WHERE vd.fan_date <= r.ref_date - 7
    ORDER BY vd.viewer_id, vd.fan_date DESC
),
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
current_circle AS (
    SELECT DISTINCT ON (viewer_id)
        viewer_id, circle_id
    FROM daily_expanded
    ORDER BY viewer_id, fan_date DESC, cumulative_fans DESC
),
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
CREATE INDEX idx_ufr_alltime_total_fans
    ON user_fan_rankings_alltime (total_fans DESC);
CREATE INDEX idx_ufr_alltime_total_gain
    ON user_fan_rankings_alltime (total_gain DESC);
CREATE INDEX idx_ufr_alltime_active_days
    ON user_fan_rankings_alltime (active_days DESC);


-- ============================================================================
-- 7. Recreate gains matview (with circle_id/circle_name)
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
CREATE INDEX idx_ufr_gains_circle_name
    ON user_fan_rankings_gains USING gin (circle_name gin_trgm_ops);
CREATE INDEX idx_ufr_gains_gain_3d
    ON user_fan_rankings_gains (gain_3d DESC);
CREATE INDEX idx_ufr_gains_gain_7d
    ON user_fan_rankings_gains (gain_7d DESC);
CREATE INDEX idx_ufr_gains_gain_30d
    ON user_fan_rankings_gains (gain_30d DESC);


-- ============================================================================
-- 8. Update archive function
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
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id
ON CONFLICT (year, month, viewer_id) DO NOTHING;
$$;


ANALYZE user_fan_rankings_monthly_archive;
