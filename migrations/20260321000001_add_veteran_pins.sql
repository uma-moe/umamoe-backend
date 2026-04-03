-- Tracks which veteran characters a user has pinned on their profile.
-- Pinned veterans appear first in the profile veterans list.

CREATE TABLE IF NOT EXISTS veteran_pins (
    account_id          TEXT    NOT NULL,
    trained_chara_id    BIGINT  NOT NULL,
    pinned_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, trained_chara_id)
);

CREATE INDEX IF NOT EXISTS idx_veteran_pins_account_id
    ON veteran_pins (account_id);
