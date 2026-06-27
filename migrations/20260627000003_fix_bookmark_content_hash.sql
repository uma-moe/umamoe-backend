-- Include all source inheritance fields in the bookmark staleness hash.
--
-- The original hash omitted the per-parent white skill counts. Because those
-- counts are visible in bookmark records and can change independently from the
-- white factor id arrays, the old comparison could miss real changes.
--
-- Preserve bookmarks that were known to match under the old hash before
-- recalculating content_hash with the fixed definition. Bookmarks already
-- stale under the old definition stay stale.

BEGIN;

CREATE TEMP TABLE _bookmark_hash_matches ON COMMIT DROP AS
SELECT ub.id
FROM user_bookmarks ub
JOIN inheritance i ON i.account_id = ub.account_id
WHERE ub.bookmarked_hash IS NOT DISTINCT FROM i.content_hash;

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

-- Recompute every row with the new trigger definition.
UPDATE inheritance
SET content_hash = content_hash;

-- Carry forward snapshots for bookmarks that matched immediately before the
-- hash definition changed. Existing modified bookmarks keep their old snapshot.
UPDATE user_bookmarks ub
SET bookmarked_hash = i.content_hash
FROM inheritance i, _bookmark_hash_matches m
WHERE m.id = ub.id
  AND i.account_id = ub.account_id;

COMMIT;
