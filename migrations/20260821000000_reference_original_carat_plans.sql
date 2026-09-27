CREATE TABLE IF NOT EXISTS carat_plan_references (
    share_id VARCHAR(16) PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan_id VARCHAR(100) NOT NULL,
    UNIQUE (user_id, plan_id)
);

-- Reuse every production share id whose original plan still exists.
INSERT INTO carat_plan_references (share_id, user_id, plan_id)
SELECT share.share_id, share.user_id, share.plan_id
FROM carat_plan_shares AS share
JOIN carat_planner_states AS state ON state.user_id = share.user_id
WHERE EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(state.collection -> 'plans') = 'array'
                THEN state.collection -> 'plans'
            ELSE '[]'::jsonb
        END
    ) AS stored_plan
    WHERE COALESCE(
        CASE WHEN jsonb_typeof(stored_plan) = 'array' THEN stored_plan ->> 0 END,
        CASE WHEN jsonb_typeof(stored_plan) = 'object' THEN stored_plan ->> 'id' END
    ) = share.plan_id
)
ON CONFLICT DO NOTHING;

-- Once the same id points at the original account plan, its copied JSON is
-- redundant. Rows whose original plan no longer exists are deliberately kept
-- so their historical links continue to open.
DELETE FROM carat_plan_shares AS share
WHERE EXISTS (
    SELECT 1
    FROM carat_plan_references AS reference
    WHERE reference.share_id = share.share_id
      AND reference.user_id = share.user_id
      AND reference.plan_id = share.plan_id
);

-- The startup backfill rewrites existing v1/v2 rows atomically after this
-- migration. The unvalidated constraint permits that one-time rewrite while
-- requiring every newly inserted or updated row to use sparse v3.
ALTER TABLE carat_planner_states
    ADD CONSTRAINT carat_planner_states_sparse_v3
    CHECK (collection @> '{"version": 3}'::jsonb) NOT VALID;
