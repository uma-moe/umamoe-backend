-- Keep the historical table/API key name, but store top playtime days instead
-- of only the longest individual observed windows. Each row is one high-playtime
-- day and `sessions` contains the observed fan-gain windows for that day.

ALTER TABLE viewer_top_sessions
    ADD COLUMN IF NOT EXISTS day DATE,
    ADD COLUMN IF NOT EXISTS session_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS longest_session_sec INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS distinct_hours SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sessions JSONB NOT NULL DEFAULT '[]'::jsonb;
