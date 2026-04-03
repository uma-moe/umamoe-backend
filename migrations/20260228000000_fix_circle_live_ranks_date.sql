-- Migration: Fix circle_live_ranks date boundary
-- Date: 2026-02-28
-- Purpose: The monthly reset happens on the 2nd at 0:00 JST, not the 1st.
--          Subtract 1 day before truncating to month, matching the list_circles handler logic.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT 
    circle_id,
    RANK() OVER (ORDER BY monthly_point DESC NULLS LAST) as live_rank,
    RANK() OVER (ORDER BY yesterday_points DESC NULLS LAST) as live_yesterday_rank
FROM circles
WHERE (archived IS NULL OR archived = false)
  AND last_updated >= date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '1 day')
  AND last_updated < date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '1 day') + interval '1 month';

-- Index for fast lookups
CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
