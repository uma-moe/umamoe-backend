-- Archive table for historical circle ranks per month.
-- Populated at month-end alongside user fan ranking archival.
CREATE TABLE IF NOT EXISTS circle_ranks_monthly_archive (
    circle_id    BIGINT NOT NULL,
    year         INT NOT NULL,
    month        INT NOT NULL,
    rank         INT,
    total_points BIGINT,
    member_count INT,
    circle_name  TEXT,
    PRIMARY KEY (circle_id, year, month)
);

-- Backfill historical circle ranks from circle_member_fans_monthly raw data.
-- For each (circle_id, year, month): sum the last non-zero daily_fans element
-- per member to get total points, count members, rank by total points.
INSERT INTO circle_ranks_monthly_archive (circle_id, year, month, rank, total_points, member_count, circle_name)
SELECT
    circle_id,
    year,
    month,
    RANK() OVER (PARTITION BY year, month ORDER BY total_points DESC)::INT AS rank,
    total_points,
    member_count,
    circle_name
FROM (
    SELECT
        cmf.circle_id,
        cmf.year,
        cmf.month,
        SUM(cmf.daily_fans[array_length(cmf.daily_fans, 1)]) AS total_points,
        COUNT(DISTINCT cmf.viewer_id)::INT AS member_count,
        MAX(c.name) AS circle_name
    FROM circle_member_fans_monthly cmf
    LEFT JOIN circles c ON c.circle_id = cmf.circle_id
    WHERE make_date(cmf.year, cmf.month, 1) < date_trunc('month', CURRENT_DATE)
    GROUP BY cmf.circle_id, cmf.year, cmf.month
) sub
ON CONFLICT DO NOTHING;

-- Also insert current month from circles table (monthly_rank is live)
-- and last month if not already backfilled
INSERT INTO circle_ranks_monthly_archive (circle_id, year, month, rank, total_points, member_count, circle_name)
SELECT circle_id,
       EXTRACT(YEAR FROM NOW() - INTERVAL '1 month')::INT,
       EXTRACT(MONTH FROM NOW() - INTERVAL '1 month')::INT,
       last_month_rank,
       last_month_point,
       member_count,
       name
FROM circles
WHERE last_month_rank IS NOT NULL
ON CONFLICT (circle_id, year, month)
DO UPDATE SET
    rank = EXCLUDED.rank,
    total_points = EXCLUDED.total_points;

