-- Add succession_chara_array column to veteran_characters.
-- Stores the full succession lineage array from the game export.

ALTER TABLE veteran_characters
    ADD COLUMN IF NOT EXISTS succession_chara_array JSONB NOT NULL DEFAULT '[]';
