-- Keep cleanup of ephemeral anti-spam buckets cheap without changing the
-- already-applied borrow tracking migration checksum.
CREATE INDEX IF NOT EXISTS idx_borrow_interaction_buckets_bucket_start
    ON borrow_interaction_buckets (bucket_start);
