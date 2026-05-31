-- Add stable public UUIDs for sharing/loading individual veteran characters.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE veteran_characters
    ADD COLUMN IF NOT EXISTS id UUID;

UPDATE veteran_characters
SET id = gen_random_uuid()
WHERE id IS NULL;

ALTER TABLE veteran_characters
    ALTER COLUMN id SET DEFAULT gen_random_uuid(),
    ALTER COLUMN id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_veteran_characters_id
    ON veteran_characters (id);
