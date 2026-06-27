-- Stable borrow keys need a different uniqueness model than the original
-- inheritance_id/support_card_id tables. Keep the existing tables untouched for
-- rollback safety and write bk1 data into v2 tables during the beta soak.

CREATE TABLE IF NOT EXISTS borrow_interaction_totals_v2 (
    trainer_id TEXT NOT NULL,
    borrow_key TEXT NOT NULL,
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
    PRIMARY KEY (trainer_id, borrow_key)
);

CREATE TABLE IF NOT EXISTS borrow_interaction_buckets_v2 (
    trainer_id TEXT NOT NULL,
    borrow_key TEXT NOT NULL,
    inheritance_id BIGINT NOT NULL DEFAULT 0,
    support_card_id INTEGER NOT NULL DEFAULT 0,
    interaction_type TEXT NOT NULL CHECK (interaction_type IN ('view', 'copy')),
    actor_hash TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trainer_id, borrow_key, interaction_type, actor_hash, bucket_start)
);

INSERT INTO borrow_interaction_totals_v2 (
    trainer_id,
    borrow_key,
    inheritance_id,
    support_card_id,
    support_card_limit_break,
    support_card_experience,
    view_count,
    copy_count,
    theoretical_copy_count,
    last_known_follower_num,
    last_viewed_at,
    last_copied_at,
    last_recheck_at,
    last_recheck_task_created_at,
    created_at,
    updated_at
)
SELECT
    trainer_id,
    'legacy:' || inheritance_id::text || ':' || support_card_id::text,
    inheritance_id,
    support_card_id,
    support_card_limit_break,
    support_card_experience,
    view_count,
    copy_count,
    theoretical_copy_count,
    last_known_follower_num,
    last_viewed_at,
    last_copied_at,
    last_recheck_at,
    last_recheck_task_created_at,
    created_at,
    updated_at
FROM borrow_interaction_totals
ON CONFLICT (trainer_id, borrow_key) DO UPDATE SET
    inheritance_id = EXCLUDED.inheritance_id,
    support_card_id = EXCLUDED.support_card_id,
    support_card_limit_break = EXCLUDED.support_card_limit_break,
    support_card_experience = EXCLUDED.support_card_experience,
    view_count = GREATEST(borrow_interaction_totals_v2.view_count, EXCLUDED.view_count),
    copy_count = GREATEST(borrow_interaction_totals_v2.copy_count, EXCLUDED.copy_count),
    theoretical_copy_count = GREATEST(
        borrow_interaction_totals_v2.theoretical_copy_count,
        EXCLUDED.theoretical_copy_count
    ),
    last_known_follower_num = COALESCE(
        EXCLUDED.last_known_follower_num,
        borrow_interaction_totals_v2.last_known_follower_num
    ),
    last_viewed_at = GREATEST(
        borrow_interaction_totals_v2.last_viewed_at,
        EXCLUDED.last_viewed_at
    ),
    last_copied_at = GREATEST(
        borrow_interaction_totals_v2.last_copied_at,
        EXCLUDED.last_copied_at
    ),
    last_recheck_at = GREATEST(
        borrow_interaction_totals_v2.last_recheck_at,
        EXCLUDED.last_recheck_at
    ),
    last_recheck_task_created_at = GREATEST(
        borrow_interaction_totals_v2.last_recheck_task_created_at,
        EXCLUDED.last_recheck_task_created_at
    ),
    updated_at = NOW();

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_totals_v2_trending
    ON borrow_interaction_totals_v2 (copy_count DESC, view_count DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_totals_v2_combo
    ON borrow_interaction_totals_v2 (borrow_key, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_buckets_v2_combo_recent
    ON borrow_interaction_buckets_v2 (
        trainer_id, borrow_key, interaction_type, bucket_start DESC
    );

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_buckets_v2_bucket_start
    ON borrow_interaction_buckets_v2 (bucket_start);
