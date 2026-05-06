-- Bookmarks: switch from inheritance_id (regenerated on every refresh) to
-- account_id (stable per trainer), and add a precomputed content hash so the
-- frontend can show a "changed since you bookmarked" badge.
--
-- The hash is a STORED generated column: Postgres computes it once on INSERT,
-- so reads are a plain TEXT comparison — no per-request hashing.

-- ── 1. Add content_hash to inheritance ──────────────────────────
-- GENERATED ALWAYS AS requires IMMUTABLE functions, but the implicit
-- INTEGER[]→text cast is only STABLE, so we use a trigger instead.
-- The result is identical: Postgres fills the column automatically on
-- every INSERT or UPDATE; the bot's code is unchanged.

ALTER TABLE inheritance
    ADD COLUMN IF NOT EXISTS content_hash TEXT;

CREATE OR REPLACE FUNCTION inheritance_content_hash()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.content_hash := md5(
        coalesce(NEW.main_parent_id::text, '')      || '|' ||
        coalesce(NEW.parent_left_id::text, '')      || '|' ||
        coalesce(NEW.parent_right_id::text, '')     || '|' ||
        coalesce(NEW.parent_rank::text, '')         || '|' ||
        coalesce(NEW.parent_rarity::text, '')       || '|' ||
        coalesce(NEW.blue_sparks::text, '')         || '|' ||
        coalesce(NEW.pink_sparks::text, '')         || '|' ||
        coalesce(NEW.green_sparks::text, '')        || '|' ||
        coalesce(NEW.white_sparks::text, '')        || '|' ||
        coalesce(NEW.win_count::text, '')           || '|' ||
        coalesce(NEW.white_count::text, '')         || '|' ||
        coalesce(NEW.main_blue_factors::text, '')   || '|' ||
        coalesce(NEW.main_pink_factors::text, '')   || '|' ||
        coalesce(NEW.main_green_factors::text, '')  || '|' ||
        coalesce(NEW.main_white_factors::text, '')  || '|' ||
        coalesce(NEW.main_win_saddles::text, '')    || '|' ||
        coalesce(NEW.left_blue_factors::text, '')   || '|' ||
        coalesce(NEW.left_pink_factors::text, '')   || '|' ||
        coalesce(NEW.left_green_factors::text, '')  || '|' ||
        coalesce(NEW.left_white_factors::text, '')  || '|' ||
        coalesce(NEW.left_win_saddles::text, '')    || '|' ||
        coalesce(NEW.right_blue_factors::text, '')  || '|' ||
        coalesce(NEW.right_pink_factors::text, '')  || '|' ||
        coalesce(NEW.right_green_factors::text, '') || '|' ||
        coalesce(NEW.right_white_factors::text, '') || '|' ||
        coalesce(NEW.right_win_saddles::text, '')   || '|' ||
        coalesce(NEW.race_results::text, '')
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_inheritance_content_hash ON inheritance;
CREATE TRIGGER trg_inheritance_content_hash
    BEFORE INSERT OR UPDATE ON inheritance
    FOR EACH ROW EXECUTE FUNCTION inheritance_content_hash();

-- Backfill existing rows (trigger only fires on future writes)
UPDATE inheritance SET content_hash = md5(
    coalesce(main_parent_id::text, '')      || '|' ||
    coalesce(parent_left_id::text, '')      || '|' ||
    coalesce(parent_right_id::text, '')     || '|' ||
    coalesce(parent_rank::text, '')         || '|' ||
    coalesce(parent_rarity::text, '')       || '|' ||
    coalesce(blue_sparks::text, '')         || '|' ||
    coalesce(pink_sparks::text, '')         || '|' ||
    coalesce(green_sparks::text, '')        || '|' ||
    coalesce(white_sparks::text, '')        || '|' ||
    coalesce(win_count::text, '')           || '|' ||
    coalesce(white_count::text, '')         || '|' ||
    coalesce(main_blue_factors::text, '')   || '|' ||
    coalesce(main_pink_factors::text, '')   || '|' ||
    coalesce(main_green_factors::text, '')  || '|' ||
    coalesce(main_white_factors::text, '')  || '|' ||
    coalesce(main_win_saddles::text, '')    || '|' ||
    coalesce(left_blue_factors::text, '')   || '|' ||
    coalesce(left_pink_factors::text, '')   || '|' ||
    coalesce(left_green_factors::text, '')  || '|' ||
    coalesce(left_white_factors::text, '')  || '|' ||
    coalesce(left_win_saddles::text, '')    || '|' ||
    coalesce(right_blue_factors::text, '')  || '|' ||
    coalesce(right_pink_factors::text, '')  || '|' ||
    coalesce(right_green_factors::text, '') || '|' ||
    coalesce(right_white_factors::text, '') || '|' ||
    coalesce(right_win_saddles::text, '')   || '|' ||
    coalesce(race_results::text, '')
)
WHERE content_hash IS NULL;

-- ── 2. Extend user_bookmarks with account_id + snapshot hash ────
ALTER TABLE user_bookmarks
    ADD COLUMN IF NOT EXISTS account_id      VARCHAR(255),
    ADD COLUMN IF NOT EXISTS bookmarked_hash TEXT;

-- Backfill from current inheritance rows (best effort: if the inheritance
-- referenced by an old bookmark no longer exists, that bookmark is lost —
-- which was already happening every 7 days under the old design).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'user_bookmarks'
          AND column_name = 'inheritance_id'
    ) THEN
        UPDATE user_bookmarks ub
        SET account_id      = i.account_id,
            bookmarked_hash = i.content_hash
        FROM inheritance i
        WHERE i.inheritance_id = ub.inheritance_id
          AND ub.account_id IS NULL;
    END IF;
END
$$;

-- Drop bookmarks we couldn't backfill (orphaned inheritance refs).
DELETE FROM user_bookmarks WHERE account_id IS NULL;

ALTER TABLE user_bookmarks
    ALTER COLUMN account_id SET NOT NULL;

-- ── 3. Swap the unique constraint ───────────────────────────────
ALTER TABLE user_bookmarks
    DROP CONSTRAINT IF EXISTS user_bookmarks_user_id_inheritance_id_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'user_bookmarks_user_id_account_id_key'
    ) THEN
        ALTER TABLE user_bookmarks
            ADD CONSTRAINT user_bookmarks_user_id_account_id_key
            UNIQUE (user_id, account_id);
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_user_bookmarks_account_id
    ON user_bookmarks (account_id);

-- ── 4. Drop the now-unused inheritance_id column ────────────────
ALTER TABLE user_bookmarks
    DROP COLUMN IF EXISTS inheritance_id;
