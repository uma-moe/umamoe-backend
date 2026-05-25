ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS career_length_buckets INTEGER[] NOT NULL DEFAULT '{}',
    DROP COLUMN IF EXISTS shortest_career_seconds,
    DROP COLUMN IF EXISTS shortest_career_count;
