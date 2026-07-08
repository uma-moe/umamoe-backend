-- Allow broader hourly circle live-rank feeds to reuse circles.live_*.
--
-- The materialized circle_live_ranks view already accepts any positive
-- live_rank/live_points pair. Keep the backing table ready for wider live
-- windows, such as top-10k/top-11k hourly refreshes, without adding parallel
-- live columns.

ALTER TABLE circles
    ADD COLUMN IF NOT EXISTS live_rank integer,
    ADD COLUMN IF NOT EXISTS live_points bigint,
    ADD COLUMN IF NOT EXISTS last_live_update timestamp without time zone;

CREATE INDEX IF NOT EXISTS idx_circles_live_rank
    ON circles (live_rank ASC NULLS LAST)
    WHERE live_rank > 0
      AND live_points > 0
      AND (archived IS NULL OR archived = false);

CREATE INDEX IF NOT EXISTS idx_circles_last_live_update
    ON circles (last_live_update)
    WHERE live_rank IS NOT NULL
       OR live_points IS NOT NULL;

COMMENT ON COLUMN circles.live_rank IS
    'Live rank for circles in the live refresh window. Top-100 rows may update every 5 minutes; broader top-10k/top-11k rows may update hourly.';

COMMENT ON COLUMN circles.live_points IS
    'Live fan count for circles in the live refresh window. Top-100 rows may update every 5 minutes; broader top-10k/top-11k rows may update hourly.';

COMMENT ON COLUMN circles.last_live_update IS
    'Timestamp of the latest circles.live_rank/live_points refresh.';
