CREATE TABLE IF NOT EXISTS viewer_short_career_snapshots (
    viewer_id BIGINT NOT NULL,
    rank SMALLINT NOT NULL,
    total_count INTEGER NOT NULL,
    snapshot_id BIGINT NOT NULL,
    circle_id BIGINT NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    previous_snapshot_id BIGINT NOT NULL,
    previous_snapshot_time TIMESTAMPTZ NOT NULL,
    previous_snapshot_fans BIGINT NOT NULL,
    current_fans BIGINT NOT NULL,
    fan_gain BIGINT NOT NULL,
    snapshot_gap_seconds INTEGER NOT NULL,
    previous_career_snapshot_time TIMESTAMPTZ NOT NULL,
    previous_career_gap_seconds INTEGER NOT NULL,
    career_length_seconds INTEGER NOT NULL,
    fans_per_minute DOUBLE PRECISION NOT NULL,
    short_training_score DOUBLE PRECISION NOT NULL,
    is_high_fan_short BOOLEAN NOT NULL,
    PRIMARY KEY (viewer_id, rank)
);

CREATE INDEX IF NOT EXISTS viewer_short_career_snapshots_viewer_time_idx
    ON viewer_short_career_snapshots (viewer_id, snapshot_time DESC);
