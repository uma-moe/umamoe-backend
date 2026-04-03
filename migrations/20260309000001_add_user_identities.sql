-- Refactor: support multiple SSO providers per user via user_identities table.
-- Move provider info out of users into a separate 1:N mapping table.

-- 1) Create user_identities table
CREATE TABLE user_identities (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider VARCHAR(20) NOT NULL,               -- 'google', 'discord', 'apple', 'twitter'
    provider_user_id VARCHAR(255) NOT NULL,       -- unique ID from SSO provider
    display_name VARCHAR(255),
    email VARCHAR(255),
    avatar_url VARCHAR(512),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (provider, provider_user_id)           -- one SSO identity can only belong to one user
);

-- 2) Migrate existing data from users into user_identities
INSERT INTO user_identities (user_id, provider, provider_user_id, display_name, email, avatar_url, created_at, updated_at)
SELECT id, provider, provider_user_id, display_name, email, avatar_url, created_at, updated_at
FROM users
WHERE provider IS NOT NULL AND provider_user_id IS NOT NULL;

-- 3) Drop the old columns from users (they now live in user_identities)
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_provider_provider_user_id_key;
ALTER TABLE users DROP COLUMN IF EXISTS provider;
ALTER TABLE users DROP COLUMN IF EXISTS provider_user_id;

-- 4) Fast lookup: find user by provider identity
CREATE INDEX idx_user_identities_user_id ON user_identities (user_id);
