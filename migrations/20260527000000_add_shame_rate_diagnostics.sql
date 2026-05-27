ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS career_rate_sample_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS career_rate_sample_seconds BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS career_rate_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS avg_careers_per_day DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS high_fan_rate_windows INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS high_fan_rate_total_fan_gain BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS high_fan_rate_total_seconds INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_avg_careers_day_idx
    ON viewer_suspicion_scores (avg_careers_per_day DESC, suspicion_score DESC);