-- General circle search starts with a trigram lookup in trainer, then joins
-- matching viewer IDs to the requested game month. The old viewer-only
-- expression index still had to visit every historical month for each match.
-- Keep this transactional because the deployment's SQLx migrator wraps simple
-- migrations in a transaction.
CREATE INDEX IF NOT EXISTS idx_circle_member_fans_viewer_text_month
    ON circle_member_fans_monthly ((viewer_id::text), year, month)
    INCLUDE (circle_id);

ANALYZE circle_member_fans_monthly;
