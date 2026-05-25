ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS trainer_name TEXT,
    ADD COLUMN IF NOT EXISTS circle_id BIGINT,
    ADD COLUMN IF NOT EXISTS circle_name TEXT,
    ADD COLUMN IF NOT EXISTS circle_monthly_rank INTEGER;

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_trainer_name_idx
    ON viewer_suspicion_scores (trainer_name);