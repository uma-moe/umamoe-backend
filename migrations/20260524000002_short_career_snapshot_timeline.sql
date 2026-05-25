ALTER TABLE viewer_short_career_snapshots
    ADD COLUMN IF NOT EXISTS prior_snapshots JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS next_snapshots JSONB NOT NULL DEFAULT '[]'::jsonb;