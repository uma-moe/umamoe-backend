-- Carry legacy borrow totals into v2 as trainer-scoped transition rows.
--
-- The original v1 identity used inheritance_id/support_card_id, but those ids
-- are not stable enough to be a historical borrow key. Keep v1 untouched and
-- aggregate all legacy history by trainer_id into one derived v2 row. Readers
-- merge that transition row with the current bk1 row.

WITH legacy_v1 AS (
    SELECT
        trainer_id,
        SUM(view_count)::bigint AS view_count,
        SUM(copy_count)::bigint AS copy_count,
        LEAST(SUM(theoretical_copy_count)::bigint, 2147483647)::int AS theoretical_copy_count,
        MAX(last_known_follower_num) AS last_known_follower_num,
        MAX(last_viewed_at) AS last_viewed_at,
        MAX(last_copied_at) AS last_copied_at,
        MAX(last_recheck_at) AS last_recheck_at,
        MAX(last_recheck_task_created_at) AS last_recheck_task_created_at,
        MIN(created_at) AS created_at,
        MAX(updated_at) AS updated_at
    FROM borrow_interaction_totals
    GROUP BY trainer_id
),
legacy_v2_row_keys AS (
    SELECT
        trainer_id,
        SUM(view_count)::bigint AS view_count,
        SUM(copy_count)::bigint AS copy_count,
        LEAST(SUM(theoretical_copy_count)::bigint, 2147483647)::int AS theoretical_copy_count,
        MAX(last_known_follower_num) AS last_known_follower_num,
        MAX(last_viewed_at) AS last_viewed_at,
        MAX(last_copied_at) AS last_copied_at,
        MAX(last_recheck_at) AS last_recheck_at,
        MAX(last_recheck_task_created_at) AS last_recheck_task_created_at,
        MIN(created_at) AS created_at,
        MAX(updated_at) AS updated_at
    FROM borrow_interaction_totals_v2
    WHERE borrow_key LIKE 'legacy:%'
    GROUP BY trainer_id
),
legacy_sources AS (
    SELECT * FROM legacy_v1
    UNION ALL
    SELECT * FROM legacy_v2_row_keys
),
legacy_totals AS (
    SELECT
        trainer_id,
        MAX(view_count)::bigint AS view_count,
        MAX(copy_count)::bigint AS copy_count,
        MAX(theoretical_copy_count)::int AS theoretical_copy_count,
        MAX(last_known_follower_num) AS last_known_follower_num,
        MAX(last_viewed_at) AS last_viewed_at,
        MAX(last_copied_at) AS last_copied_at,
        MAX(last_recheck_at) AS last_recheck_at,
        MAX(last_recheck_task_created_at) AS last_recheck_task_created_at,
        COALESCE(MIN(created_at), NOW()) AS created_at,
        COALESCE(MAX(updated_at), NOW()) AS updated_at
    FROM legacy_sources
    GROUP BY trainer_id
)
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
    'legacy-trainer:' || trainer_id,
    0,
    0,
    NULL,
    NULL,
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
FROM legacy_totals
WHERE view_count > 0 OR copy_count > 0 OR theoretical_copy_count > 0
ON CONFLICT (trainer_id, borrow_key) DO UPDATE SET
    inheritance_id = 0,
    support_card_id = 0,
    support_card_limit_break = NULL,
    support_card_experience = NULL,
    view_count = GREATEST(borrow_interaction_totals_v2.view_count, EXCLUDED.view_count),
    copy_count = GREATEST(borrow_interaction_totals_v2.copy_count, EXCLUDED.copy_count),
    theoretical_copy_count = GREATEST(
        borrow_interaction_totals_v2.theoretical_copy_count,
        EXCLUDED.theoretical_copy_count
    ),
    last_known_follower_num = COALESCE(GREATEST(
        borrow_interaction_totals_v2.last_known_follower_num,
        EXCLUDED.last_known_follower_num
    ), borrow_interaction_totals_v2.last_known_follower_num, EXCLUDED.last_known_follower_num),
    last_viewed_at = COALESCE(GREATEST(
        borrow_interaction_totals_v2.last_viewed_at,
        EXCLUDED.last_viewed_at
    ), borrow_interaction_totals_v2.last_viewed_at, EXCLUDED.last_viewed_at),
    last_copied_at = COALESCE(GREATEST(
        borrow_interaction_totals_v2.last_copied_at,
        EXCLUDED.last_copied_at
    ), borrow_interaction_totals_v2.last_copied_at, EXCLUDED.last_copied_at),
    last_recheck_at = COALESCE(GREATEST(
        borrow_interaction_totals_v2.last_recheck_at,
        EXCLUDED.last_recheck_at
    ), borrow_interaction_totals_v2.last_recheck_at, EXCLUDED.last_recheck_at),
    last_recheck_task_created_at = COALESCE(GREATEST(
        borrow_interaction_totals_v2.last_recheck_task_created_at,
        EXCLUDED.last_recheck_task_created_at
    ), borrow_interaction_totals_v2.last_recheck_task_created_at, EXCLUDED.last_recheck_task_created_at),
    updated_at = NOW();

WITH legacy_v1 AS (
    SELECT
        trainer_id,
        SUM(view_count)::bigint AS view_count,
        SUM(copy_count)::bigint AS copy_count,
        MAX(last_viewed_at) AS last_viewed_at,
        MAX(last_copied_at) AS last_copied_at,
        MAX(updated_at) AS updated_at
    FROM borrow_interaction_totals
    GROUP BY trainer_id
),
legacy_v2_row_keys AS (
    SELECT
        trainer_id,
        SUM(view_count)::bigint AS view_count,
        SUM(copy_count)::bigint AS copy_count,
        MAX(last_viewed_at) AS last_viewed_at,
        MAX(last_copied_at) AS last_copied_at,
        MAX(updated_at) AS updated_at
    FROM borrow_interaction_totals_v2
    WHERE borrow_key LIKE 'legacy:%'
    GROUP BY trainer_id
),
legacy_sources AS (
    SELECT * FROM legacy_v1
    UNION ALL
    SELECT * FROM legacy_v2_row_keys
),
legacy_totals AS (
    SELECT
        trainer_id,
        MAX(view_count)::bigint AS view_count,
        MAX(copy_count)::bigint AS copy_count,
        MAX(last_viewed_at) AS last_viewed_at,
        MAX(last_copied_at) AS last_copied_at,
        MAX(updated_at) AS updated_at
    FROM legacy_sources
    GROUP BY trainer_id
)
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
    'legacy-trainer:' || trainer_id,
    0,
    0,
    NULL,
    NULL,
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
FROM legacy_totals
WHERE COALESCE(last_viewed_at, last_copied_at, updated_at)::date >= CURRENT_DATE - 6
  AND (view_count > 0 OR copy_count > 0)
ON CONFLICT (trend_date, trainer_id, borrow_key) DO UPDATE SET
    view_count = GREATEST(borrow_interaction_trends.view_count, EXCLUDED.view_count),
    copy_count = GREATEST(borrow_interaction_trends.copy_count, EXCLUDED.copy_count),
    inheritance_id = 0,
    support_card_id = 0,
    support_card_limit_break = NULL,
    support_card_experience = NULL,
    updated_at = NOW();
