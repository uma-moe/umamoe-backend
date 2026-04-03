-- Ensure gen_random_uuid() is available (built-in from PG 13+, extension for older)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Users table: stores SSO identity information (no passwords)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider VARCHAR(20) NOT NULL,              -- 'google', 'discord', 'apple', 'twitter'
    provider_user_id VARCHAR(255) NOT NULL,      -- unique ID from SSO provider
    display_name VARCHAR(255),
    email VARCHAR(255),
    avatar_url VARCHAR(512),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (provider, provider_user_id)
);

-- Linked accounts: 1 user can link multiple in-game trainer accounts
CREATE TABLE IF NOT EXISTS linked_accounts (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id VARCHAR(255) NOT NULL,            -- trainer/account_id from existing system
    verification_token VARCHAR(64),              -- random code user pastes in-game description
    verification_status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- 'pending', 'verified', 'failed'
    verified_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, account_id)
);

-- Fast lookup by account_id (check if already linked by another user)
CREATE INDEX IF NOT EXISTS idx_linked_accounts_account_id ON linked_accounts (account_id);

-- Fast bot lookup: find pending verification by token
CREATE INDEX IF NOT EXISTS idx_linked_accounts_pending_token ON linked_accounts (verification_token)
    WHERE verification_status = 'pending';
