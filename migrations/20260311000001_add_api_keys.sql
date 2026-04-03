-- API keys table: allows users to create API keys for tracking usage.
-- Keys are not enforced yet — just logged when present.
CREATE TABLE IF NOT EXISTS api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        VARCHAR(255) NOT NULL,          -- user-given label, e.g. "My App"
    key_hash    VARCHAR(64) NOT NULL UNIQUE,     -- SHA-256 hash of the raw key
    key_prefix  VARCHAR(8) NOT NULL,             -- first 8 chars for display ("uma_k_Ab...")
    last_used   TIMESTAMP,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    revoked     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_api_keys_user_id ON api_keys (user_id);
CREATE INDEX idx_api_keys_key_hash ON api_keys (key_hash);
