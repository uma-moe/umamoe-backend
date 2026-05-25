ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS recent_fan_gain_3d BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS baseline_fan_gain_14d BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recent_fans_per_day DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS baseline_fans_per_day DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fan_gain_spike_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS behavior_change_score DOUBLE PRECISION NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_behavior_change_idx
    ON viewer_suspicion_scores (behavior_change_score DESC, fan_gain_spike_ratio DESC);