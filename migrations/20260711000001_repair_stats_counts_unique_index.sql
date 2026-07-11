-- Required by REFRESH MATERIALIZED VIEW CONCURRENTLY stats_counts.
-- Some existing databases were baselined without this historical index.
DROP INDEX IF EXISTS idx_stats_counts_singleton;
CREATE UNIQUE INDEX idx_stats_counts_singleton ON stats_counts ((1));
