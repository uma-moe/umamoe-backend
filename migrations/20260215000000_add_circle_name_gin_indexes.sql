-- Add GIN trigram indexes on circle_name for ranking tables
-- The ILIKE search on circle_name was doing full scans because only trainer_name had a GIN index

CREATE INDEX IF NOT EXISTS idx_ufr_archive_circle_name_gin
    ON user_fan_rankings_monthly_archive USING gin (circle_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_ufr_current_circle_name_gin
    ON user_fan_rankings_monthly_current USING gin (circle_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_ufr_alltime_circle_name_gin
    ON user_fan_rankings_alltime USING gin (circle_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_ufr_gains_circle_name_gin
    ON user_fan_rankings_gains USING gin (circle_name gin_trgm_ops);
