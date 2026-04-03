-- Migration: Remove upper bound limit for current month circle updates
-- Date: 2026-03-01
-- Purpose: Because of the upper bound logic, any club updated on the new month 
--          during the delayed rollover period gets completely hidden, 
--          as their last_updated is >= the upper bound (start of new month).

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT 
    circle_id,
    RANK() OVER (ORDER BY monthly_point DESC NULLS LAST) as live_rank,
    RANK() OVER (ORDER BY yesterday_points DESC NULLS LAST) as live_yesterday_rank
FROM circles
WHERE (archived IS NULL OR archived = false)
  AND last_updated >= date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days');

-- Index for fast lookups
CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
