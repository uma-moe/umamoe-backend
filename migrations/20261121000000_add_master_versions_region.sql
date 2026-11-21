-- Global and JP versions share master_versions. Rows written before this column are Global.
-- The workers add the same column in their own migration; whichever runs first wins and the
-- other is a no-op.
SET LOCAL lock_timeout = '2s';

ALTER TABLE master_versions
    ADD COLUMN IF NOT EXISTS region TEXT NOT NULL DEFAULT 'global'
        CHECK (region IN ('global', 'jp'));
