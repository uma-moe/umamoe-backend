-- Switch from "continuous-online streak" (a snapshot-gap heuristic) to a
-- login-anchored session model:
--
--   * A session opens at every observed change of `last_login_time` for a
--     viewer (or at the next 15:00 UTC = 00:00 JST forced relog, which the
--     game enforces by kicking the player).
--   * The session ends at the timestamp of the last observed fan gain
--     inside that login window. If no fan gain was ever observed, the
--     session is dropped entirely (the player logged in and did nothing).
--   * `idle_seconds` records the total time inside the session interval
--     that we did NOT see fan gain on the next adjacent snapshot; this lets
--     the UI down-weight long but mostly-idle sessions.
--
-- This collapses the previous "session" and "online streak" concepts into
-- a single notion. The redundant columns / table / indexes are removed.

-- Replace the streaks table with a sessions table that carries
-- idle/active breakdown alongside duration.
DROP TABLE IF EXISTS viewer_top_streaks;

CREATE TABLE viewer_top_sessions (
    viewer_id        BIGINT      NOT NULL,
    rank             SMALLINT    NOT NULL,            -- 1 = longest
    started_at       TIMESTAMPTZ NOT NULL,            -- last_login_time (or JST reset boundary)
    ended_at         TIMESTAMPTZ NOT NULL,            -- last observed fan-gain snapshot
    duration_seconds INTEGER     NOT NULL,            -- ended_at - started_at
    active_seconds   INTEGER     NOT NULL,            -- duration minus observed idle
    idle_seconds     INTEGER     NOT NULL,            -- contiguous gaps with no fan delta
    careers          INTEGER     NOT NULL,
    fan_gain         BIGINT      NOT NULL,
    PRIMARY KEY (viewer_id, rank)
);

-- Score-table cleanup: drop the "online streak" duplicate. The longest
-- session value lives in max_session_seconds going forward.
DROP INDEX IF EXISTS viewer_suspicion_scores_score_idx;
DROP INDEX IF EXISTS viewer_suspicion_scores_online_idx;

ALTER TABLE viewer_suspicion_scores
    DROP COLUMN IF EXISTS max_online_streak_seconds;

CREATE INDEX viewer_suspicion_scores_score_idx
    ON viewer_suspicion_scores (suspicion_score DESC, max_session_seconds DESC);

-- Daily cleanup: longest_online_sec was the per-day streak counterpart and
-- is now redundant with longest_session_sec under the new model.
ALTER TABLE viewer_activity_daily
    DROP COLUMN IF EXISTS longest_online_sec;
