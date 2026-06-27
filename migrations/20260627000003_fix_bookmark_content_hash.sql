-- Include all source inheritance fields in the bookmark staleness hash.
--
-- The original hash omitted the per-parent white skill counts. Because those
-- counts are visible in bookmark records and can change independently from the
-- white factor id arrays, the old comparison could miss real changes.
--
-- This migration is intentionally schema/function-only. Existing rows are
-- recomputed by a Rust background job in small autocommit batches so deploying
-- this cannot rewrite the full inheritance table in one startup transaction.

CREATE OR REPLACE FUNCTION inheritance_content_hash()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.content_hash := md5(
        'inheritance-content-v2|' ||
        coalesce(NEW.main_parent_id::text, '')       || '|' ||
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

DROP TRIGGER IF EXISTS trg_inheritance_content_hash ON inheritance;
CREATE TRIGGER trg_inheritance_content_hash
    BEFORE INSERT OR UPDATE ON inheritance
    FOR EACH ROW EXECUTE FUNCTION inheritance_content_hash();

CREATE TABLE IF NOT EXISTS bookmark_content_hash_backfill_queue (
    account_id VARCHAR(255) PRIMARY KEY,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bookmark_content_hash_backfill_pending
    ON bookmark_content_hash_backfill_queue (queued_at, account_id)
    WHERE processed_at IS NULL;
