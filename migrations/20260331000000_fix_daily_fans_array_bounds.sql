-- Fix daily_fans arrays that have a 0-based lower bound.
-- PostgreSQL array slicing (e.g. daily_fans[:5] || ...) can produce arrays
-- starting at index 0, which sqlx rejects.
-- array_agg always produces 1-based arrays, so we reconstruct them.

-- 1. Fix existing 0-based rows
UPDATE circle_member_fans_monthly
SET daily_fans = (
    SELECT COALESCE(array_agg(v ORDER BY ordinality), '{}'::bigint[])
    FROM unnest(daily_fans) WITH ORDINALITY AS t(v, ordinality)
)
WHERE array_lower(daily_fans, 1) IS DISTINCT FROM 1;

-- 2. Auto-correct future 0-based arrays on INSERT or UPDATE.
-- When the bot writes to index [0], PostgreSQL creates a [0:N] array with N+1 elements.
-- We strip the bogus [0] element (which is the latest value duplicated) and re-index.
CREATE OR REPLACE FUNCTION fix_daily_fans_array_bounds()
RETURNS TRIGGER AS $$
BEGIN
    IF array_lower(NEW.daily_fans, 1) = 0 THEN
        -- Bot wrote to [0]: strip it (it's a duplicate of the latest value)
        NEW.daily_fans := (
            SELECT array_agg(v ORDER BY ordinality)
            FROM unnest(NEW.daily_fans) WITH ORDINALITY AS t(v, ordinality)
            WHERE ordinality > 1
        );
    ELSIF array_lower(NEW.daily_fans, 1) IS DISTINCT FROM 1 AND NEW.daily_fans != '{}'::bigint[] THEN
        -- Some other non-1-based bound: just re-index
        NEW.daily_fans := (
            SELECT array_agg(v ORDER BY ordinality)
            FROM unnest(NEW.daily_fans) WITH ORDINALITY AS t(v, ordinality)
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_fix_daily_fans_bounds
    BEFORE INSERT OR UPDATE OF daily_fans
    ON circle_member_fans_monthly
    FOR EACH ROW
    EXECUTE FUNCTION fix_daily_fans_array_bounds();
