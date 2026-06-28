-- Replay legacy borrow totals into v2 legacy-key rows.
--
-- The initial v2 migration copied the legacy rows once. Some live deployments
-- can still have fresher legacy totals, so keep this idempotent catch-up step
-- before relying on v2/search-index counters for display and trending. The
-- source v1 table is left untouched; readers merge v2 bk1 rows with these v2
-- legacy rows for the same visible borrow entry.

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
    GREATEST(
        LEAST(COALESCE(last_viewed_at, last_copied_at, updated_at)::date, CURRENT_DATE),
        CURRENT_DATE - 6
    ),
    trainer_id,
    'legacy:' || inheritance_id::text || ':' || support_card_id::text,
    inheritance_id,
    support_card_id,
    support_card_limit_break,
    support_card_experience,
    CASE
        WHEN last_viewed_at::date >= CURRENT_DATE - 6 THEN CEIL(LN(view_count::numeric + 1))::bigint
        ELSE 0
    END,
    CASE
        WHEN last_copied_at::date >= CURRENT_DATE - 6 THEN CEIL(LN(copy_count::numeric + 1))::bigint
        ELSE 0
    END,
    NOW(),
    NOW()
FROM borrow_interaction_totals
WHERE COALESCE(last_viewed_at, last_copied_at, updated_at)::date >= CURRENT_DATE - 6
  AND (view_count > 0 OR copy_count > 0)
ON CONFLICT (trend_date, trainer_id, borrow_key) DO UPDATE SET
    view_count = GREATEST(borrow_interaction_trends.view_count, EXCLUDED.view_count),
    copy_count = GREATEST(borrow_interaction_trends.copy_count, EXCLUDED.copy_count),
    inheritance_id = EXCLUDED.inheritance_id,
    support_card_id = EXCLUDED.support_card_id,
    support_card_limit_break = EXCLUDED.support_card_limit_break,
    support_card_experience = EXCLUDED.support_card_experience,
    updated_at = NOW();
