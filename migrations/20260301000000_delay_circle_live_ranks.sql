-- Migration: Delay circle_live_ranks rollover by 1 day
-- Date: 2026-03-01
-- Purpose: The monthly reset happens on the 2nd at 0:00 JST, not the 1st.
--          By changing from - interval '1 day' to - interval '2 days', the new month ranking won't appear
--          until the 3rd at 0 JST, avoiding clubs disappearing during the 1-day transition period.

DROP MATERIALIZED VIEW IF EXISTS circle_live_ranks;

CREATE MATERIALIZED VIEW circle_live_ranks AS
SELECT 
    circle_id,
    RANK() OVER (ORDER BY monthly_point DESC NULLS LAST) as live_rank,
    RANK() OVER (ORDER BY yesterday_points DESC NULLS LAST) as live_yesterday_rank
FROM circles
WHERE (archived IS NULL OR archived = false)
  AND last_updated >= date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')
  AND last_updated < date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days') + interval '1 month';

-- Index for fast lookups
CREATE UNIQUE INDEX idx_circle_live_ranks_id ON circle_live_ranks (circle_id);
CREATE INDEX idx_circle_live_ranks_rank ON circle_live_ranks (live_rank);
