ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS avg_career_length_last20_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS shortest_career_seconds INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS shortest_career_count INTEGER NOT NULL DEFAULT 0;
