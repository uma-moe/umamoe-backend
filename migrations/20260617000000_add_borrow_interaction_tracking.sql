-- Track real borrow interest without letting repeated API calls inflate totals.
-- Totals power trending; theoretical_copy_count estimates new follows since the last cap recheck.

CREATE TABLE IF NOT EXISTS trainer_copies (
    trainer_id TEXT PRIMARY KEY,
    copy_count INTEGER NOT NULL DEFAULT 0,
    last_copied TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS borrow_interaction_totals (
    trainer_id TEXT NOT NULL,
    inheritance_id BIGINT NOT NULL DEFAULT 0,
    support_card_id INTEGER NOT NULL DEFAULT 0,
    support_card_limit_break INTEGER,
    support_card_experience INTEGER,
    view_count BIGINT NOT NULL DEFAULT 0,
    copy_count BIGINT NOT NULL DEFAULT 0,
    theoretical_copy_count INTEGER NOT NULL DEFAULT 0,
    last_known_follower_num INTEGER,
    last_viewed_at TIMESTAMPTZ,
    last_copied_at TIMESTAMPTZ,
    last_recheck_at TIMESTAMPTZ,
    last_recheck_task_created_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trainer_id, inheritance_id, support_card_id)
);

CREATE TABLE IF NOT EXISTS borrow_interaction_buckets (
    trainer_id TEXT NOT NULL,
    inheritance_id BIGINT NOT NULL DEFAULT 0,
    support_card_id INTEGER NOT NULL DEFAULT 0,
    interaction_type TEXT NOT NULL CHECK (interaction_type IN ('view', 'copy')),
    actor_hash TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trainer_id, inheritance_id, support_card_id, interaction_type, actor_hash, bucket_start)
);

INSERT INTO borrow_interaction_totals (
    trainer_id, inheritance_id, support_card_id, copy_count, last_copied_at, created_at, updated_at
)
SELECT trainer_id, 0, 0, copy_count::bigint, last_copied, NOW(), NOW()
FROM trainer_copies
ON CONFLICT (trainer_id, inheritance_id, support_card_id) DO UPDATE SET
    copy_count = GREATEST(borrow_interaction_totals.copy_count, EXCLUDED.copy_count),
    last_copied_at = CASE
        WHEN borrow_interaction_totals.last_copied_at IS NULL THEN EXCLUDED.last_copied_at
        WHEN EXCLUDED.last_copied_at IS NULL THEN borrow_interaction_totals.last_copied_at
        ELSE GREATEST(borrow_interaction_totals.last_copied_at, EXCLUDED.last_copied_at)
    END,
    updated_at = NOW();

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_totals_trending
    ON borrow_interaction_totals ((view_count + copy_count * 3) DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_totals_trainer
    ON borrow_interaction_totals (trainer_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_totals_combo
    ON borrow_interaction_totals (inheritance_id, support_card_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_totals_recent_views
    ON borrow_interaction_totals (last_viewed_at DESC)
    WHERE last_viewed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_totals_recent_copies
    ON borrow_interaction_totals (last_copied_at DESC)
    WHERE last_copied_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_buckets_actor_recent
    ON borrow_interaction_buckets (actor_hash, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_buckets_combo_recent
    ON borrow_interaction_buckets (
        trainer_id, inheritance_id, support_card_id, interaction_type, bucket_start DESC
    );
