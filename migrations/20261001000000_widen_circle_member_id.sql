-- Monthly member upserts exhausted the INTEGER sequence, even on conflict updates.
-- Widen both parts together without resetting the sequence or changing existing IDs.
-- This rewrites the table and rebuilds indexes under an exclusive lock. Deploy
-- with the matching i64 backend; the old i32 decoder cannot read BIGINT columns.
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15min';

ALTER TABLE circle_member_fans_monthly ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE circle_member_fans_monthly_id_seq AS BIGINT NO MAXVALUE;
