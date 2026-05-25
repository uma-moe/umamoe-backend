ALTER TABLE viewer_suspicion_scores
    ADD COLUMN IF NOT EXISTS probe_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS probe_metrics JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_probe_score_idx
    ON viewer_suspicion_scores (probe_score DESC, suspicion_score DESC);

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_probe_metrics_gin_idx
    ON viewer_suspicion_scores USING GIN (probe_metrics jsonb_path_ops);
