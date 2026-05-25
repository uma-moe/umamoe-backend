-- The previous cheat-analysis migration stored every per-(viewer, snapshot)
-- event in a giant table and did all sessionization / scoring inside a single
-- plpgsql transaction. With ~1.76M snapshots × ~30 members ≈ 50M unnested
-- events that is the wrong tool: the function held one long transaction and
-- nothing became visible to the API until commit. This migration moves the
-- heavy lifting into the Rust process. The DB now only stores final
-- aggregates (daily, heatmap, suspicion scores, top streaks) and a metadata
-- row. The Rust pipeline streams snapshot rows, folds everything in memory,
-- and bulk-writes the aggregate tables inside a single transaction (an
-- atomic swap from the API's perspective).

-- Drop the old plpgsql analysis stack if it exists.
DROP FUNCTION IF EXISTS refresh_cheat_analysis(BOOLEAN);
DROP FUNCTION IF EXISTS rebuild_cheat_aggregates_for(BIGINT[]);
DROP TABLE IF EXISTS viewer_snapshot_events;
DROP TABLE IF EXISTS viewer_snapshot_events_meta;

-- Aggregate tables are recreated fresh — schema is similar but no longer
-- carries fields that only made sense for the in-DB pipeline.
DROP TABLE IF EXISTS viewer_activity_daily;
DROP TABLE IF EXISTS viewer_activity_heatmap;
DROP TABLE IF EXISTS viewer_suspicion_scores;
DROP TABLE IF EXISTS viewer_top_streaks;
DROP TABLE IF EXISTS cheat_analysis_meta;

CREATE TABLE viewer_activity_daily (
    viewer_id           BIGINT  NOT NULL,
    day                 DATE    NOT NULL,             -- Europe/Berlin calendar day
    active_seconds      INTEGER NOT NULL,
    careers             INTEGER NOT NULL,
    fan_gain            BIGINT  NOT NULL,
    sessions            INTEGER NOT NULL,
    longest_session_sec INTEGER NOT NULL,
    longest_online_sec  INTEGER NOT NULL,
    distinct_hours      SMALLINT NOT NULL,
    PRIMARY KEY (viewer_id, day)
);

CREATE INDEX viewer_activity_daily_day_idx ON viewer_activity_daily (day);

CREATE TABLE viewer_activity_heatmap (
    viewer_id       BIGINT   NOT NULL,
    dow             SMALLINT NOT NULL,                -- 0=Sun .. 6=Sat (Berlin)
    hour            SMALLINT NOT NULL,                -- 0..23 (Berlin)
    active_seconds  INTEGER  NOT NULL,
    careers         INTEGER  NOT NULL,
    PRIMARY KEY (viewer_id, dow, hour)
);

CREATE TABLE viewer_suspicion_scores (
    viewer_id                    BIGINT       PRIMARY KEY,
    first_seen                   TIMESTAMPTZ  NOT NULL,
    last_seen                    TIMESTAMPTZ  NOT NULL,
    days_observed                INTEGER      NOT NULL,
    days_active                  INTEGER      NOT NULL,
    total_active_seconds         BIGINT       NOT NULL,
    total_fan_gain               BIGINT       NOT NULL,
    total_careers                INTEGER      NOT NULL,
    careers_per_active_hour      DOUBLE PRECISION NOT NULL,
    avg_career_length_last20_seconds DOUBLE PRECISION NOT NULL,
    career_length_buckets        INTEGER[]    NOT NULL DEFAULT '{}',
    fans_per_active_minute       DOUBLE PRECISION NOT NULL,
    peak_fans_per_minute         DOUBLE PRECISION NOT NULL,
    max_daily_active_seconds     INTEGER      NOT NULL,
    max_daily_careers            INTEGER      NOT NULL,
    max_session_seconds          INTEGER      NOT NULL,
    max_online_streak_seconds    INTEGER      NOT NULL,
    days_over_16h                INTEGER      NOT NULL,
    days_over_20h                INTEGER      NOT NULL,
    distinct_weekly_hour_buckets SMALLINT     NOT NULL,
    flag_no_sleep                BOOLEAN      NOT NULL,
    flag_extreme_session         BOOLEAN      NOT NULL,
    flag_inhuman_career_rate     BOOLEAN      NOT NULL,
    flag_247                     BOOLEAN      NOT NULL,
    flag_marathon                BOOLEAN      NOT NULL,
    suspicion_score              INTEGER      NOT NULL,
    refreshed_at                 TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX viewer_suspicion_scores_score_idx
    ON viewer_suspicion_scores (suspicion_score DESC, max_online_streak_seconds DESC);
CREATE INDEX viewer_suspicion_scores_session_idx
    ON viewer_suspicion_scores (max_session_seconds DESC);
CREATE INDEX viewer_suspicion_scores_online_idx
    ON viewer_suspicion_scores (max_online_streak_seconds DESC);

-- One row per top-N continuous-online streak per viewer (top 10).
CREATE TABLE viewer_top_streaks (
    viewer_id        BIGINT      NOT NULL,
    rank             SMALLINT    NOT NULL,            -- 1 = longest
    started_at       TIMESTAMPTZ NOT NULL,
    ended_at         TIMESTAMPTZ NOT NULL,
    duration_seconds INTEGER     NOT NULL,
    careers          INTEGER     NOT NULL,
    fan_gain         BIGINT      NOT NULL,
    PRIMARY KEY (viewer_id, rank)
);

CREATE TABLE cheat_analysis_meta (
    id                  INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_snapshot_id    BIGINT       NOT NULL DEFAULT 0,
    last_refreshed_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_duration_ms    INTEGER      NOT NULL DEFAULT 0,
    snapshots_processed BIGINT       NOT NULL DEFAULT 0,
    viewers_scored      BIGINT       NOT NULL DEFAULT 0
);

INSERT INTO cheat_analysis_meta (id) VALUES (1)
    ON CONFLICT (id) DO NOTHING;
