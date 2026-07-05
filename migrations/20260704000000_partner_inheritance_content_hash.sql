-- Allow multiple saved practice partners from the same trainer.
--
-- Previously partner_inheritance was keyed by (user_id, account_id), where
-- account_id is the trainer/viewer id. Importing a second runner from the same
-- trainer overwrote the first one. This adds a content hash for the inheritance
-- payload and uses it as part of the saved-row identity.

ALTER TABLE partner_inheritance
    ADD COLUMN IF NOT EXISTS scenario_id INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS content_hash TEXT;

UPDATE partner_inheritance
SET scenario_id = 0
WHERE scenario_id IS NULL;

ALTER TABLE partner_inheritance
    ALTER COLUMN scenario_id SET DEFAULT 0,
    ALTER COLUMN scenario_id SET NOT NULL;

CREATE OR REPLACE FUNCTION partner_inheritance_content_hash()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.content_hash := md5(
        'partner-inheritance-content-v1|' ||
        coalesce(NEW.main_parent_id::text, '')       || '|' ||
        coalesce(NEW.scenario_id::text, '')          || '|' ||
        coalesce(NEW.parent_left_id::text, '')       || '|' ||
        coalesce(NEW.parent_right_id::text, '')      || '|' ||
        coalesce(NEW.parent_rank::text, '')          || '|' ||
        coalesce(NEW.parent_rarity::text, '')        || '|' ||
        coalesce(NEW.blue_sparks::text, '')          || '|' ||
        coalesce(NEW.pink_sparks::text, '')          || '|' ||
        coalesce(NEW.green_sparks::text, '')         || '|' ||
        coalesce(NEW.white_sparks::text, '')         || '|' ||
        coalesce(NEW.win_count::text, '')            || '|' ||
        coalesce(NEW.white_count::text, '')          || '|' ||
        coalesce(NEW.main_blue_factors::text, '')    || '|' ||
        coalesce(NEW.main_pink_factors::text, '')    || '|' ||
        coalesce(NEW.main_green_factors::text, '')   || '|' ||
        coalesce(NEW.main_white_factors::text, '')   || '|' ||
        coalesce(NEW.main_white_count::text, '')     || '|' ||
        coalesce(NEW.left_blue_factors::text, '')    || '|' ||
        coalesce(NEW.left_pink_factors::text, '')    || '|' ||
        coalesce(NEW.left_green_factors::text, '')   || '|' ||
        coalesce(NEW.left_white_factors::text, '')   || '|' ||
        coalesce(NEW.left_white_count::text, '')     || '|' ||
        coalesce(NEW.right_blue_factors::text, '')   || '|' ||
        coalesce(NEW.right_pink_factors::text, '')   || '|' ||
        coalesce(NEW.right_green_factors::text, '')  || '|' ||
        coalesce(NEW.right_white_factors::text, '')  || '|' ||
        coalesce(NEW.right_white_count::text, '')    || '|' ||
        coalesce(NEW.main_win_saddles::text, '')     || '|' ||
        coalesce(NEW.left_win_saddles::text, '')     || '|' ||
        coalesce(NEW.right_win_saddles::text, '')    || '|' ||
        coalesce(NEW.race_results::text, '')
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_partner_inheritance_content_hash ON partner_inheritance;
CREATE TRIGGER trg_partner_inheritance_content_hash
    BEFORE INSERT OR UPDATE ON partner_inheritance
    FOR EACH ROW EXECUTE FUNCTION partner_inheritance_content_hash();

UPDATE partner_inheritance
SET content_hash = content_hash
WHERE content_hash IS NULL;

ALTER TABLE partner_inheritance
    ALTER COLUMN content_hash SET NOT NULL;

WITH ranked AS (
    SELECT
        id,
        row_number() OVER (
            PARTITION BY user_id, account_id, content_hash
            ORDER BY updated_at DESC, id DESC
        ) AS rn
    FROM partner_inheritance
)
DELETE FROM partner_inheritance p
USING ranked r
WHERE p.id = r.id
  AND r.rn > 1;

ALTER TABLE partner_inheritance
    DROP CONSTRAINT IF EXISTS partner_inheritance_user_id_account_id_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'partner_inheritance_user_id_account_id_content_hash_key'
    ) THEN
        ALTER TABLE partner_inheritance
            ADD CONSTRAINT partner_inheritance_user_id_account_id_content_hash_key
            UNIQUE (user_id, account_id, content_hash);
    END IF;
END
$$;
