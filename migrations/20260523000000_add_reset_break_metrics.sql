ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS reset_recovery_windows INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reset_breaks INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_reset_recovery_seconds INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reset_break_score DOUBLE PRECISION NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_reset_break_score_idx
    ON viewer_suspicion_scores (reset_break_score DESC, reset_breaks DESC);