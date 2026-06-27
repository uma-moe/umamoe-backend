-- Seed the recent trend table from existing lifetime totals.
--
-- We cannot reconstruct the true day-by-day interaction history from lifetime
-- counters, so this intentionally writes a weak bootstrap signal to the oldest
-- day still considered by search (CURRENT_DATE - 6). Counts are logarithmically
-- compressed so historic winners do not permanently dominate fresh activity.

INSERT INTO borrow_interaction_trends (
    trend_date,
    trainer_id,
    borrow_key,
    inheritance_id,
    support_card_id,
    support_card_limit_break,
    support_card_experience,
    view_count,
    copy_count,
    created_at,
    updated_at
)
SELECT
    CURRENT_DATE - 6,
    trainer_id,
    borrow_key,
    inheritance_id,
    support_card_id,
    support_card_limit_break,
    support_card_experience,
    CEIL(LN(view_count::numeric + 1))::bigint,
    CEIL(LN(copy_count::numeric + 1))::bigint,
    NOW(),
    NOW()
FROM borrow_interaction_totals_v2
WHERE borrow_key IS NOT NULL
  AND (view_count > 0 OR copy_count > 0)
ON CONFLICT (trend_date, trainer_id, borrow_key) DO UPDATE SET
    view_count = GREATEST(borrow_interaction_trends.view_count, EXCLUDED.view_count),
    copy_count = GREATEST(borrow_interaction_trends.copy_count, EXCLUDED.copy_count),
    inheritance_id = EXCLUDED.inheritance_id,
    support_card_id = EXCLUDED.support_card_id,
    support_card_limit_break = EXCLUDED.support_card_limit_break,
    support_card_experience = EXCLUDED.support_card_experience,
    updated_at = NOW();
