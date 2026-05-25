ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS short_fan_gain_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS short_fan_gain_score_buckets DOUBLE PRECISION[] NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_short_fan_gain_score_idx
    ON viewer_suspicion_scores (short_fan_gain_score DESC, short_high_fan_careers DESC);