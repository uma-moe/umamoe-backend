-- Migration: Add live_points / live_ranking columns to circles and use them for ranking
-- Date: 2026-03-06
-- Purpose: Top-100 circles have live_points and live_ranking updated every 5 minutes
--          by the data collector.  We want:
--            1. circle_live_ranks to rank by COALESCE(live_points, monthly_point)
--               so the leaderboard is always up-to-date.
--            2. The API to expose live_points so clients can show the 5-min value
--               while still returning yesterday_points for daily-gain display.

-- Add the two new columns (no-op if already present in the database)
ALTER TABLE circles
    ADD COLUMN IF NOT EXISTS live_points  bigint,
    ADD COLUMN IF NOT EXISTS live_ranking integer;

-- Index for fast ORDER BY / COALESCE lookups on live_points
CREATE INDEX IF NOT EXISTS idx_circles_live_points
    ON circles (live_points DESC NULLS LAST)
    WHERE (archived IS NULL OR archived = false);

-- Rebuild the materialized view to rank by live_points where available,
-- falling back to monthly_point for circles outside the top 100.
DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT
    circle_id,
    RANK() OVER (ORDER BY GREATEST(live_points, monthly_point) DESC NULLS LAST) AS live_rank,
    RANK() OVER (ORDER BY yesterday_points DESC NULLS LAST)                     AS live_yesterday_rank
FROM circles
WHERE (archived IS NULL OR archived = false)
  AND last_updated >= date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days');

-- Restore indexes
CREATE UNIQUE INDEX idx_circle_live_ranks_id   ON circle_live_ranks (circle_id);
CREATE INDEX        idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
