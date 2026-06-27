-- Recent aggregate buckets for "Trending".
-- Lifetime totals live in borrow_interaction_totals_v2 for display; this table
-- keeps a small per-day signal so default sorting can decay instead of
-- permanently rewarding old winners.

CREATE TABLE IF NOT EXISTS borrow_interaction_trends (
    trend_date DATE NOT NULL,
    trainer_id TEXT NOT NULL,
    borrow_key TEXT NOT NULL,
    inheritance_id BIGINT NOT NULL DEFAULT 0,
    support_card_id INTEGER NOT NULL DEFAULT 0,
    support_card_limit_break INTEGER,
    support_card_experience INTEGER,
    view_count BIGINT NOT NULL DEFAULT 0,
    copy_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trend_date, trainer_id, borrow_key)
);

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_trends_recent
    ON borrow_interaction_trends (trend_date DESC, copy_count DESC, view_count DESC);

CREATE INDEX IF NOT EXISTS idx_borrow_interaction_trends_combo
    ON borrow_interaction_trends (trainer_id, borrow_key, trend_date DESC);
