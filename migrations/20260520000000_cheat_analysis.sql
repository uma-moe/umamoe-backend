-- Cheat / botting detection built on circle_member_fan_snapshots.
--
-- Source rows are parallel-array circle snapshots taken ~every 5 minutes for
-- ~70 days × ~100 circles × ~30 members. We unnest them per viewer and use
-- LAG to derive per-viewer events with fan/login deltas, then aggregate to a
-- hall-of-shame score and a per-viewer activity heatmap.
--
-- Detection model (constants tunable in functions below):
--   * Career-finished signal:   fan_delta >= 100_000 (legend race / team trial
--     noise is ~2_000, real careers are 500k–1.5M, so 100k is safely above
--     the noise floor and well below a single career).
--   * Estimated careers per transition: max(1, round(fan_delta / 700_000)).
--   * "User is online right now" marker: fan_delta > 0 AND last_login_time
--     unchanged across the snapshot pair. This is the trick the user
--     described — Cygames only refreshes last_login on login, so monotonic
--     fan growth with a frozen login timestamp means the session never
--     ended.
--   * Active transition: career, login_changed, or any fan increase.
--   * Active seconds attributed to a transition: the snapshot gap capped at
--     900s (15 min), so a long delay between snapshots doesn't inflate.
--   * Session boundary: gap_seconds > 1800 OR transition not active.
--
-- Snapshot service is NOT 100% reliable — gaps of minutes to hours occur.
-- Gap handling rules used everywhere:
--   * Normal gap (<=30min) with activity → cap active_seconds at 15min.
--   * Service gap (>30min) with careers  → attribute ~15min PER estimated
--     career so careers/hour stays meaningful (don't dunk the denominator).
--   * is_online_marker requires gap<=30min — long gaps cannot count as
--     continuous online presence even if fans grew across them.
--   * Session boundary fires on gap>30min so a service outage never
--     "extends" a session.
--   * Heatmap only attributes buckets for snapshots with gap<=30min, so a
--     single end-of-gap snapshot can't paint a wildly inflated hour spike.
--
-- All timestamps are bucketed in Europe/Berlin for the heatmap / per-day
-- views (per user choice).
--
-- The pipeline is incrementally maintained: refresh_cheat_analysis() only
-- reprocesses snapshots above the stored watermark and only rewrites
-- aggregate rows for viewers that received new events.

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS viewer_snapshot_events (
    viewer_id           BIGINT      NOT NULL,
    snapshot_time       TIMESTAMPTZ NOT NULL,
    snapshot_id         BIGINT      NOT NULL,
    fans                BIGINT      NOT NULL,
    last_login_time     TIMESTAMPTZ,
    prev_snapshot_time  TIMESTAMPTZ,
    prev_fans           BIGINT,
    prev_last_login     TIMESTAMPTZ,
    fan_delta           BIGINT      NOT NULL,
    gap_seconds         INTEGER,
    active_seconds      INTEGER     NOT NULL,
    login_changed       BOOLEAN     NOT NULL,
    is_active           BOOLEAN     NOT NULL,
    is_career           BOOLEAN     NOT NULL,
    career_count        INTEGER     NOT NULL,
    is_online_marker    BOOLEAN     NOT NULL,
    PRIMARY KEY (viewer_id, snapshot_time)
);

CREATE INDEX IF NOT EXISTS viewer_snapshot_events_time_idx
    ON viewer_snapshot_events (snapshot_time);

CREATE INDEX IF NOT EXISTS viewer_snapshot_events_career_idx
    ON viewer_snapshot_events (viewer_id, snapshot_time)
    WHERE is_career;

CREATE TABLE IF NOT EXISTS viewer_snapshot_events_meta (
    id                  INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_snapshot_id    BIGINT      NOT NULL DEFAULT 0,
    last_refreshed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO viewer_snapshot_events_meta (id) VALUES (1)
    ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS viewer_activity_daily (
    viewer_id           BIGINT  NOT NULL,
    day                 DATE    NOT NULL,             -- Europe/Berlin calendar day
    active_seconds      INTEGER NOT NULL,
    careers             INTEGER NOT NULL,
    fan_gain            BIGINT  NOT NULL,
    sessions            INTEGER NOT NULL,
    longest_session_sec INTEGER NOT NULL,
    longest_online_sec  INTEGER NOT NULL,             -- longest streak of online_marker
    distinct_hours      SMALLINT NOT NULL,            -- 0..24 of the day
    PRIMARY KEY (viewer_id, day)
);

CREATE INDEX IF NOT EXISTS viewer_activity_daily_day_idx
    ON viewer_activity_daily (day);

CREATE TABLE IF NOT EXISTS viewer_activity_heatmap (
    viewer_id       BIGINT   NOT NULL,
    dow             SMALLINT NOT NULL,                -- 0=Sun .. 6=Sat (Berlin)
    hour            SMALLINT NOT NULL,                -- 0..23 (Berlin)
    active_seconds  INTEGER  NOT NULL,
    careers         INTEGER  NOT NULL,
    PRIMARY KEY (viewer_id, dow, hour)
);

CREATE TABLE IF NOT EXISTS viewer_suspicion_scores (
    viewer_id                   BIGINT       PRIMARY KEY,
    first_seen                  TIMESTAMPTZ  NOT NULL,
    last_seen                   TIMESTAMPTZ  NOT NULL,
    days_observed               INTEGER      NOT NULL,
    days_active                 INTEGER      NOT NULL,
    total_active_seconds        BIGINT       NOT NULL,
    total_fan_gain              BIGINT       NOT NULL,
    total_careers               INTEGER      NOT NULL,
    careers_per_active_hour     DOUBLE PRECISION NOT NULL,
    max_daily_active_seconds    INTEGER      NOT NULL,
    max_daily_careers           INTEGER      NOT NULL,
    max_session_seconds         INTEGER      NOT NULL,
    max_online_streak_seconds   INTEGER      NOT NULL,  -- longest continuous-online run
    days_over_16h               INTEGER      NOT NULL,
    days_over_20h               INTEGER      NOT NULL,
    distinct_weekly_hour_buckets SMALLINT    NOT NULL,  -- 0..168, how saturated their schedule is
    flag_no_sleep               BOOLEAN      NOT NULL,  -- any day with >18h active
    flag_extreme_session        BOOLEAN      NOT NULL,  -- any continuous-online run >8h
    flag_inhuman_career_rate    BOOLEAN      NOT NULL,  -- careers_per_active_hour >6
    flag_247                    BOOLEAN      NOT NULL,  -- >140 of 168 weekly hour buckets active
    flag_marathon               BOOLEAN      NOT NULL,  -- any day with >22h active
    suspicion_score             INTEGER      NOT NULL,  -- count of flags (0..5)
    refreshed_at                TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_score_idx
    ON viewer_suspicion_scores (suspicion_score DESC, max_online_streak_seconds DESC);

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_session_idx
    ON viewer_suspicion_scores (max_session_seconds DESC);

CREATE INDEX IF NOT EXISTS viewer_suspicion_scores_online_idx
    ON viewer_suspicion_scores (max_online_streak_seconds DESC);

-- ---------------------------------------------------------------------------
-- Refresh function
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION refresh_cheat_analysis(p_full BOOLEAN DEFAULT FALSE)
RETURNS TABLE (
    new_events           BIGINT,
    affected_viewers     BIGINT,
    snapshots_processed  BIGINT,
    duration_ms          INTEGER
) LANGUAGE plpgsql AS $$
DECLARE
    v_start          TIMESTAMPTZ := clock_timestamp();
    v_watermark      BIGINT;
    v_max_snapshot   BIGINT;
    v_new_events     BIGINT := 0;
    v_snapshots      BIGINT := 0;
    v_affected       BIGINT := 0;
BEGIN
    IF p_full THEN
        TRUNCATE viewer_snapshot_events;
        TRUNCATE viewer_activity_daily;
        TRUNCATE viewer_activity_heatmap;
        TRUNCATE viewer_suspicion_scores;
        UPDATE viewer_snapshot_events_meta SET last_snapshot_id = 0 WHERE id = 1;
    END IF;

    SELECT last_snapshot_id INTO v_watermark FROM viewer_snapshot_events_meta WHERE id = 1;
    SELECT COALESCE(MAX(id), v_watermark) INTO v_max_snapshot
        FROM circle_member_fan_snapshots WHERE id > v_watermark;

    IF v_max_snapshot <= v_watermark THEN
        RETURN QUERY SELECT 0::BIGINT, 0::BIGINT, 0::BIGINT,
            EXTRACT(MILLISECOND FROM clock_timestamp() - v_start)::INTEGER;
        RETURN;
    END IF;

    -- 1) Unnest new snapshots into per-viewer raw rows.
    CREATE TEMP TABLE _new_raw ON COMMIT DROP AS
    SELECT
        u.viewer_id::BIGINT             AS viewer_id,
        s.snapshot_time                 AS snapshot_time,
        s.id                            AS snapshot_id,
        u.fans::BIGINT                  AS fans,
        u.last_login_time::TIMESTAMPTZ  AS last_login_time
    FROM circle_member_fan_snapshots s
    CROSS JOIN LATERAL unnest(s.viewer_ids, s.fans, s.last_login_times)
        AS u(viewer_id, fans, last_login_time)
    WHERE s.id > v_watermark AND s.id <= v_max_snapshot
      AND u.viewer_id IS NOT NULL;

    SELECT COUNT(*) INTO v_snapshots FROM (
        SELECT DISTINCT snapshot_id FROM _new_raw) sub;

    -- Defensive dedupe in case a viewer appears twice in one snapshot.
    CREATE TEMP TABLE _new_dedup ON COMMIT DROP AS
    SELECT DISTINCT ON (viewer_id, snapshot_time)
        viewer_id, snapshot_time, snapshot_id, fans, last_login_time
    FROM _new_raw
    ORDER BY viewer_id, snapshot_time, snapshot_id;

    CREATE INDEX ON _new_dedup (viewer_id, snapshot_time);

    -- 2) For each affected viewer, fetch the previous event already stored so
    -- LAG can start from the correct seed.
    CREATE TEMP TABLE _affected ON COMMIT DROP AS
    SELECT DISTINCT viewer_id FROM _new_dedup;
    CREATE INDEX ON _affected (viewer_id);

    SELECT COUNT(*) INTO v_affected FROM _affected;

    CREATE TEMP TABLE _seed ON COMMIT DROP AS
    SELECT DISTINCT ON (e.viewer_id)
        e.viewer_id, e.snapshot_time, e.snapshot_id, e.fans, e.last_login_time
    FROM viewer_snapshot_events e
    JOIN _affected a USING (viewer_id)
    ORDER BY e.viewer_id, e.snapshot_time DESC;

    -- 3) Union seed + new, compute deltas via LAG, insert only the new rows.
    WITH combined AS (
        SELECT viewer_id, snapshot_time, snapshot_id, fans, last_login_time,
               FALSE AS is_new
        FROM _seed
        UNION ALL
        SELECT viewer_id, snapshot_time, snapshot_id, fans, last_login_time,
               TRUE AS is_new
        FROM _new_dedup
    ),
    deltas AS (
        SELECT
            viewer_id, snapshot_time, snapshot_id, fans, last_login_time, is_new,
            LAG(snapshot_time)   OVER w AS prev_snapshot_time,
            LAG(fans)            OVER w AS prev_fans,
            LAG(last_login_time) OVER w AS prev_last_login
        FROM combined
        WINDOW w AS (PARTITION BY viewer_id ORDER BY snapshot_time, snapshot_id)
    ),
    classified AS (
        SELECT
            viewer_id, snapshot_time, snapshot_id, fans, last_login_time,
            prev_snapshot_time, prev_fans, prev_last_login, is_new,
            GREATEST(COALESCE(fans - prev_fans, 0), 0)::BIGINT AS fan_delta,
            CASE WHEN prev_snapshot_time IS NULL THEN NULL
                 ELSE EXTRACT(EPOCH FROM snapshot_time - prev_snapshot_time)::INTEGER
            END AS gap_seconds,
            (prev_last_login IS NOT NULL
                AND last_login_time IS DISTINCT FROM prev_last_login) AS login_changed
        FROM deltas
    )
    INSERT INTO viewer_snapshot_events (
        viewer_id, snapshot_time, snapshot_id, fans, last_login_time,
        prev_snapshot_time, prev_fans, prev_last_login,
        fan_delta, gap_seconds, active_seconds,
        login_changed, is_active, is_career, career_count, is_online_marker
    )
    SELECT
        viewer_id, snapshot_time, snapshot_id, fans, last_login_time,
        prev_snapshot_time, prev_fans, prev_last_login,
        fan_delta,
        gap_seconds,
        -- Active-seconds attribution. The snapshot service is not 100%
        -- reliable: gaps of minutes-to-hours occur. We must NOT inflate any
        -- "active time" metric just because a long gap had a career in it,
        -- nor must we tank the careers/hour ratio by attributing many
        -- careers to ~zero active time.
        --
        --   * Normal gap (<=30 min) with activity:  cap at 15 min.
        --   * Service gap (>30 min) with careers:   attribute 15 min PER
        --     estimated career so the rate ratio stays meaningful.
        --   * Service gap (>30 min) login-only:     60 s (brief login).
        --   * Otherwise:                            0.
        CASE
            WHEN prev_snapshot_time IS NULL THEN 0
            WHEN COALESCE(gap_seconds, 0) <= 1800
                 AND (fan_delta > 0 OR login_changed)
                THEN LEAST(COALESCE(gap_seconds, 0), 900)
            WHEN COALESCE(gap_seconds, 0) > 1800 AND fan_delta >= 100000
                THEN GREATEST(1, ((fan_delta + 350000) / 700000)::INT) * 900
            WHEN COALESCE(gap_seconds, 0) > 1800 AND login_changed
                THEN 60
            ELSE 0
        END AS active_seconds,
        login_changed,
        (prev_snapshot_time IS NOT NULL AND (fan_delta > 0 OR login_changed)) AS is_active,
        (fan_delta >= 100000) AS is_career,
        CASE
            WHEN fan_delta >= 100000
                THEN GREATEST(1, ((fan_delta + 350000) / 700000)::INT)
            ELSE 0
        END AS career_count,
        -- Strongest "still online" signal: fans growing while last_login is
        -- frozen AND we have a tight snapshot gap (a multi-hour service
        -- outage means we cannot claim the user was continuously online).
        (prev_snapshot_time IS NOT NULL
            AND fan_delta > 0
            AND NOT login_changed
            AND COALESCE(gap_seconds, 0) <= 1800) AS is_online_marker
    FROM classified
    WHERE is_new
    ON CONFLICT (viewer_id, snapshot_time) DO NOTHING;

    GET DIAGNOSTICS v_new_events = ROW_COUNT;

    -- 4) Rebuild aggregates for affected viewers only.
    PERFORM rebuild_cheat_aggregates_for(ARRAY(SELECT viewer_id FROM _affected));

    UPDATE viewer_snapshot_events_meta
       SET last_snapshot_id = v_max_snapshot,
           last_refreshed_at = NOW()
     WHERE id = 1;

    RETURN QUERY SELECT
        v_new_events,
        v_affected,
        v_snapshots,
        EXTRACT(MILLISECOND FROM clock_timestamp() - v_start)::INTEGER;
END;
$$;

-- ---------------------------------------------------------------------------
-- Aggregate rebuild (scoped to a viewer batch)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION rebuild_cheat_aggregates_for(p_viewer_ids BIGINT[])
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    IF p_viewer_ids IS NULL OR array_length(p_viewer_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    DELETE FROM viewer_activity_daily   WHERE viewer_id = ANY(p_viewer_ids);
    DELETE FROM viewer_activity_heatmap WHERE viewer_id = ANY(p_viewer_ids);
    DELETE FROM viewer_suspicion_scores WHERE viewer_id = ANY(p_viewer_ids);

    -- Per-viewer events with sessions and online-streaks.
    -- Sessions: gap_seconds > 1800 OR is_active = false breaks the session
    -- (we still keep inactive snapshots out of any session by giving them
    -- their own session id).
    -- Online streaks: contiguous run of is_online_marker.
    WITH evs AS (
        SELECT e.*,
               (e.snapshot_time AT TIME ZONE 'Europe/Berlin')::DATE AS day_berlin,
               EXTRACT(DOW  FROM e.snapshot_time AT TIME ZONE 'Europe/Berlin')::SMALLINT AS dow_berlin,
               EXTRACT(HOUR FROM e.snapshot_time AT TIME ZONE 'Europe/Berlin')::SMALLINT AS hour_berlin
        FROM viewer_snapshot_events e
        WHERE e.viewer_id = ANY(p_viewer_ids)
    ),
    session_flags AS (
        SELECT *,
            CASE WHEN NOT is_active
                 THEN 1
                 WHEN COALESCE(gap_seconds, 99999) > 1800
                 THEN 1
                 ELSE 0
            END AS session_break,
            CASE WHEN is_online_marker THEN 0 ELSE 1 END AS online_break
        FROM evs
    ),
    sessioned AS (
        SELECT *,
            SUM(session_break) OVER (PARTITION BY viewer_id ORDER BY snapshot_time, snapshot_id
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id,
            SUM(online_break)  OVER (PARTITION BY viewer_id ORDER BY snapshot_time, snapshot_id
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS online_run_id
        FROM session_flags
    ),
    -- Active sessions: sum of active_seconds within a session_id where the
    -- session is "real" (is_active = true at least once).
    session_sums AS (
        SELECT viewer_id, session_id,
               SUM(active_seconds) FILTER (WHERE is_active) AS sess_seconds,
               MIN(snapshot_time)  AS sess_start,
               MAX(snapshot_time)  AS sess_end,
               BOOL_OR(is_active)  AS has_activity
        FROM sessioned
        GROUP BY viewer_id, session_id
    ),
    online_runs AS (
        SELECT viewer_id, online_run_id,
               SUM(COALESCE(gap_seconds, 0)) FILTER (WHERE is_online_marker) AS run_seconds,
               MIN(snapshot_time) FILTER (WHERE is_online_marker) AS run_start,
               BOOL_OR(is_online_marker) AS has_online
        FROM sessioned
        GROUP BY viewer_id, online_run_id
    ),
    -- Per-(viewer, day) aggregates including session counts.
    daily_base AS (
        SELECT viewer_id, day_berlin AS day,
               SUM(active_seconds)::INTEGER AS active_seconds,
               SUM(career_count)::INTEGER  AS careers,
               SUM(fan_delta)::BIGINT      AS fan_gain,
               COUNT(DISTINCT hour_berlin) FILTER (WHERE is_active)::SMALLINT AS distinct_hours
        FROM sessioned
        GROUP BY viewer_id, day_berlin
    ),
    daily_sessions AS (
        SELECT viewer_id,
               (sess_start AT TIME ZONE 'Europe/Berlin')::DATE AS day,
               COUNT(*)::INTEGER AS sessions,
               MAX(sess_seconds)::INTEGER AS longest_session_sec
        FROM session_sums
        WHERE has_activity
        GROUP BY viewer_id, (sess_start AT TIME ZONE 'Europe/Berlin')::DATE
    ),
    daily_online AS (
        SELECT viewer_id,
               (run_start AT TIME ZONE 'Europe/Berlin')::DATE AS day,
               MAX(run_seconds)::INTEGER AS longest_online_sec
        FROM online_runs
        WHERE has_online
        GROUP BY viewer_id, (run_start AT TIME ZONE 'Europe/Berlin')::DATE
    ),
    daily AS (
        SELECT b.viewer_id, b.day,
               b.active_seconds,
               b.careers,
               b.fan_gain,
               COALESCE(ds.sessions, 0)::INTEGER AS sessions,
               COALESCE(ds.longest_session_sec, 0) AS longest_session_sec,
               COALESCE(dn.longest_online_sec, 0)  AS longest_online_sec,
               COALESCE(b.distinct_hours, 0)::SMALLINT AS distinct_hours
        FROM daily_base b
        LEFT JOIN daily_sessions ds USING (viewer_id, day)
        LEFT JOIN daily_online   dn USING (viewer_id, day)
    ),
    ins_daily AS (
        INSERT INTO viewer_activity_daily (
            viewer_id, day, active_seconds, careers, fan_gain,
            sessions, longest_session_sec, longest_online_sec, distinct_hours)
        SELECT viewer_id, day, active_seconds, careers, fan_gain,
               sessions, longest_session_sec, longest_online_sec, distinct_hours
        FROM daily
        RETURNING viewer_id
    ),
    heatmap AS (
        -- Only attribute heatmap buckets when the snapshot gap was tight
        -- enough that we can confidently say WHEN the activity happened.
        -- Service gaps (>30min) are excluded from per-hour attribution to
        -- avoid painting wildly inflated late-night spikes from a single
        -- end-of-gap snapshot.
        SELECT viewer_id, dow_berlin AS dow, hour_berlin AS hour,
               SUM(LEAST(active_seconds, 900))::INTEGER AS active_seconds,
               SUM(career_count)::INTEGER   AS careers
        FROM sessioned
        WHERE is_active AND COALESCE(gap_seconds, 0) <= 1800
        GROUP BY viewer_id, dow_berlin, hour_berlin
    ),
    ins_heat AS (
        INSERT INTO viewer_activity_heatmap (viewer_id, dow, hour, active_seconds, careers)
        SELECT viewer_id, dow, hour, active_seconds, careers FROM heatmap
        RETURNING viewer_id
    ),
    -- Overall stats per viewer.
    overall AS (
        SELECT viewer_id,
               MIN(snapshot_time) AS first_seen,
               MAX(snapshot_time) AS last_seen,
               SUM(active_seconds)::BIGINT AS total_active_seconds,
               SUM(fan_delta)::BIGINT      AS total_fan_gain,
               SUM(career_count)::INTEGER  AS total_careers
        FROM sessioned
        GROUP BY viewer_id
    ),
    overall_sessions AS (
        SELECT viewer_id,
               COALESCE(MAX(sess_seconds), 0)::INTEGER AS max_session_seconds
        FROM session_sums
        WHERE has_activity
        GROUP BY viewer_id
    ),
    overall_online AS (
        SELECT viewer_id,
               COALESCE(MAX(run_seconds), 0)::INTEGER AS max_online_streak_seconds
        FROM online_runs
        WHERE has_online
        GROUP BY viewer_id
    ),
    overall_daily AS (
        SELECT viewer_id,
               COUNT(*)::INTEGER AS days_observed,
               COUNT(*) FILTER (WHERE active_seconds > 0)::INTEGER AS days_active,
               MAX(active_seconds)::INTEGER AS max_daily_active_seconds,
               MAX(careers)::INTEGER        AS max_daily_careers,
               COUNT(*) FILTER (WHERE active_seconds > 16*3600)::INTEGER AS days_over_16h,
               COUNT(*) FILTER (WHERE active_seconds > 20*3600)::INTEGER AS days_over_20h,
               COUNT(*) FILTER (WHERE active_seconds > 22*3600)::INTEGER AS days_over_22h
        FROM daily
        GROUP BY viewer_id
    ),
    overall_weekly AS (
        SELECT viewer_id,
               COUNT(*)::SMALLINT AS distinct_weekly_hour_buckets
        FROM (SELECT DISTINCT viewer_id, dow, hour FROM heatmap) h
        GROUP BY viewer_id
    )
    INSERT INTO viewer_suspicion_scores (
        viewer_id, first_seen, last_seen,
        days_observed, days_active,
        total_active_seconds, total_fan_gain, total_careers,
        careers_per_active_hour,
        max_daily_active_seconds, max_daily_careers,
        max_session_seconds, max_online_streak_seconds,
        days_over_16h, days_over_20h,
        distinct_weekly_hour_buckets,
        flag_no_sleep, flag_extreme_session, flag_inhuman_career_rate,
        flag_247, flag_marathon, suspicion_score, refreshed_at
    )
    SELECT
        o.viewer_id,
        o.first_seen, o.last_seen,
        COALESCE(od.days_observed, 0),
        COALESCE(od.days_active, 0),
        o.total_active_seconds,
        o.total_fan_gain,
        o.total_careers,
        CASE WHEN o.total_active_seconds >= 3600
             THEN o.total_careers::DOUBLE PRECISION / (o.total_active_seconds / 3600.0)
             ELSE 0
        END AS careers_per_active_hour,
        COALESCE(od.max_daily_active_seconds, 0),
        COALESCE(od.max_daily_careers, 0),
        COALESCE(os.max_session_seconds, 0),
        COALESCE(oo.max_online_streak_seconds, 0),
        COALESCE(od.days_over_16h, 0),
        COALESCE(od.days_over_20h, 0),
        COALESCE(ow.distinct_weekly_hour_buckets, 0),
        -- Flags: each requires a meaningful observation window to fire.
        (COALESCE(od.days_observed, 0) >= 3
            AND COALESCE(od.max_daily_active_seconds, 0) > 18*3600) AS flag_no_sleep,
        (COALESCE(oo.max_online_streak_seconds, 0) > 8*3600) AS flag_extreme_session,
        (o.total_active_seconds >= 6*3600
            AND o.total_careers >= 10
            AND (o.total_careers::DOUBLE PRECISION / (o.total_active_seconds / 3600.0)) > 6.0) AS flag_inhuman_career_rate,
        (COALESCE(ow.distinct_weekly_hour_buckets, 0) > 140
            AND COALESCE(od.days_observed, 0) >= 7) AS flag_247,
        (COALESCE(od.days_over_22h, 0) >= 1) AS flag_marathon,
        (
            (CASE WHEN COALESCE(od.days_observed,0)>=3 AND COALESCE(od.max_daily_active_seconds,0)>18*3600 THEN 1 ELSE 0 END) +
            (CASE WHEN COALESCE(oo.max_online_streak_seconds,0)>8*3600 THEN 1 ELSE 0 END) +
            (CASE WHEN o.total_active_seconds>=6*3600 AND o.total_careers>=10
                  AND (o.total_careers::DOUBLE PRECISION / (o.total_active_seconds / 3600.0))>6.0 THEN 1 ELSE 0 END) +
            (CASE WHEN COALESCE(ow.distinct_weekly_hour_buckets,0)>140 AND COALESCE(od.days_observed,0)>=7 THEN 1 ELSE 0 END) +
            (CASE WHEN COALESCE(od.days_over_22h,0)>=1 THEN 1 ELSE 0 END)
        )::INTEGER AS suspicion_score,
        NOW()
    FROM overall o
    LEFT JOIN overall_sessions os USING (viewer_id)
    LEFT JOIN overall_online   oo USING (viewer_id)
    LEFT JOIN overall_daily    od USING (viewer_id)
    LEFT JOIN overall_weekly   ow USING (viewer_id);
END;
$$;
