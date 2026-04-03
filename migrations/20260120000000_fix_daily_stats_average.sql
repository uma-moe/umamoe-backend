-- Fix the unique_visitors_7_day calculation to properly average over 7 days
-- even when some days are missing (treats missing days as 0 visitors)

-- ==============================================================================
-- RECREATE MATERIALIZED VIEW WITH CORRECTED AVERAGE
-- ==============================================================================

DROP MATERIALIZED VIEW IF EXISTS stats_counts CASCADE;

CREATE MATERIALIZED VIEW stats_counts AS
SELECT 
  (SELECT COUNT(*) FROM trainer) as trainer_count,
  (SELECT COUNT(*) FROM circles) as circles_count,
  (SELECT COUNT(*) FROM team_stadium) as team_stadium_count,
  (SELECT COUNT(*) FROM inheritance) as inheritance_count,
  (SELECT COUNT(*) FROM support_card) as support_card_count,
  -- Fixed: Always divide by 7 days, treating missing days as 0 visitors
  (SELECT COALESCE(SUM(unique_visitors)::float8, 0) / 7.0
   FROM daily_stats 
   WHERE date >= CURRENT_DATE - INTERVAL '6 days' 
     AND date <= CURRENT_DATE) as unique_visitors_7_day,
  NOW() as last_refreshed;

-- Recreate the unique index for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_stats_counts_singleton ON stats_counts((1));

-- Initial refresh
REFRESH MATERIALIZED VIEW stats_counts;

-- ==============================================================================
-- UPDATE THE INCREMENT FUNCTION TO HANDLE MISSING DAYS BETTER
-- ==============================================================================
-- This function now also ensures entries are created properly

CREATE OR REPLACE FUNCTION increment_daily_visitor_count(target_date DATE)
RETURNS INTEGER AS $$
DECLARE
    new_count INTEGER;
BEGIN
    -- Insert or update the daily_stats for the target date
    INSERT INTO daily_stats (date, total_visitors, unique_visitors, visitor_count, created_at, updated_at)
    VALUES (target_date, 1, 1, 1, NOW(), NOW())
    ON CONFLICT (date) 
    DO UPDATE SET 
        total_visitors = daily_stats.total_visitors + 1,
        unique_visitors = daily_stats.unique_visitors + 1,
        updated_at = NOW()
    RETURNING total_visitors INTO new_count;
    
    RETURN new_count;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- OPTIONAL: BACKFILL MISSING DATES WITH ZERO VALUES
-- ==============================================================================
-- This creates entries for any missing dates in the last 30 days
-- Uncomment and run manually if you want to fill gaps

-- INSERT INTO daily_stats (date, total_visitors, unique_visitors, visitor_count, created_at, updated_at)
-- SELECT 
--     d::date,
--     0,
--     0, 
--     0,
--     NOW(),
--     NOW()
-- FROM generate_series(
--     CURRENT_DATE - INTERVAL '30 days', 
--     CURRENT_DATE, 
--     '1 day'::interval
-- ) AS d
-- WHERE NOT EXISTS (
--     SELECT 1 FROM daily_stats WHERE date = d::date
-- );
