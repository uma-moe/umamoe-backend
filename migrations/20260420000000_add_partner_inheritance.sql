-- Partner Lookup feature
-- Stores inheritance data fetched on-demand by the bot for "practice partner" / trainer ID
-- lookups initiated by users. Mirrors the relevant fields of the inheritance table.

CREATE TABLE IF NOT EXISTS partner_inheritance (
    id              SERIAL PRIMARY KEY,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id      TEXT NOT NULL,

    -- Mirror of inheritance fields
    main_parent_id      INTEGER NOT NULL DEFAULT 0,
    parent_left_id      INTEGER NOT NULL DEFAULT 0,
    parent_right_id     INTEGER NOT NULL DEFAULT 0,
    parent_rank         INTEGER NOT NULL DEFAULT 0,
    parent_rarity       INTEGER NOT NULL DEFAULT 0,

    blue_sparks         INTEGER[] NOT NULL DEFAULT '{}',
    pink_sparks         INTEGER[] NOT NULL DEFAULT '{}',
    green_sparks        INTEGER[] NOT NULL DEFAULT '{}',
    white_sparks        INTEGER[] NOT NULL DEFAULT '{}',

    win_count           INTEGER NOT NULL DEFAULT 0,
    white_count         INTEGER NOT NULL DEFAULT 0,

    main_blue_factors   INTEGER NOT NULL DEFAULT 0,
    main_pink_factors   INTEGER NOT NULL DEFAULT 0,
    main_green_factors  INTEGER NOT NULL DEFAULT 0,
    main_white_factors  INTEGER[] NOT NULL DEFAULT '{}',
    main_white_count    INTEGER NOT NULL DEFAULT 0,

    left_blue_factors   INTEGER NOT NULL DEFAULT 0,
    left_pink_factors   INTEGER NOT NULL DEFAULT 0,
    left_green_factors  INTEGER NOT NULL DEFAULT 0,
    left_white_factors  INTEGER[] NOT NULL DEFAULT '{}',
    left_white_count    INTEGER NOT NULL DEFAULT 0,

    right_blue_factors  INTEGER NOT NULL DEFAULT 0,
    right_pink_factors  INTEGER NOT NULL DEFAULT 0,
    right_green_factors INTEGER NOT NULL DEFAULT 0,
    right_white_factors INTEGER[] NOT NULL DEFAULT '{}',
    right_white_count   INTEGER NOT NULL DEFAULT 0,

    main_win_saddles    INTEGER[] NOT NULL DEFAULT '{}',
    left_win_saddles    INTEGER[] NOT NULL DEFAULT '{}',
    right_win_saddles   INTEGER[] NOT NULL DEFAULT '{}',
    race_results        INTEGER[] NOT NULL DEFAULT '{}',

    blue_stars_sum      INTEGER NOT NULL DEFAULT 0,
    pink_stars_sum      INTEGER NOT NULL DEFAULT 0,
    green_stars_sum     INTEGER NOT NULL DEFAULT 0,
    white_stars_sum     INTEGER NOT NULL DEFAULT 0,

    affinity_score      INTEGER,

    -- Optional label the user can attach to this saved entry
    label               TEXT,

    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),

    UNIQUE (user_id, account_id)
);

CREATE INDEX IF NOT EXISTS idx_partner_inheritance_user_id
    ON partner_inheritance (user_id, updated_at DESC);

-- ---------------------------------------------------------------------------
-- Task completion notification
-- A trigger emits a NOTIFY when a task transitions to a terminal status.
-- Backend processes LISTEN for `task_completion` and forward to SSE subscribers
-- so frontend clients see partner lookup results in (near) realtime.
-- Payload format: "<task_id>:<status>"  (status = completed | failed)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION notify_task_completion() RETURNS trigger AS $$
BEGIN
    IF NEW.status IN ('completed', 'failed')
       AND (OLD.status IS DISTINCT FROM NEW.status) THEN
        PERFORM pg_notify(
            'task_completion',
            NEW.id::text || ':' || NEW.status
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_task_completion ON tasks;
CREATE TRIGGER trg_notify_task_completion
    AFTER UPDATE OF status ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION notify_task_completion();
