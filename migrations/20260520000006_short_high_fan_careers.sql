ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS short_high_fan_careers INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_short_high_fan_idx
    ON viewer_suspicion_scores (short_high_fan_careers DESC, suspicion_score DESC);