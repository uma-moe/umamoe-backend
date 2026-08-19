CREATE TABLE IF NOT EXISTS carat_planner_states (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    collection JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (jsonb_typeof(collection) = 'object'),
    CHECK (octet_length(collection::text) <= 1048576)
);

CREATE TABLE IF NOT EXISTS carat_plan_shares (
    share_id VARCHAR(12) PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan_id VARCHAR(100) NOT NULL,
    plan_name VARCHAR(80) NOT NULL,
    plan JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, plan_id),
    CHECK (jsonb_typeof(plan) = 'object'),
    CHECK (octet_length(plan::text) <= 262144)
);

CREATE INDEX IF NOT EXISTS idx_carat_plan_shares_user_id
    ON carat_plan_shares (user_id);
