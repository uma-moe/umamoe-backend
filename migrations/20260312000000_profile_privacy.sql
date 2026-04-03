CREATE TABLE IF NOT EXISTS user_privacy_settings (
    account_id     TEXT        PRIMARY KEY,
    profile_hidden BOOLEAN     NOT NULL DEFAULT FALSE,
    hidden_sections TEXT[]     NOT NULL DEFAULT '{}',
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
