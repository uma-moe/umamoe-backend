-- no-transaction

-- Support live /api/stats freshness counts without scanning trainer.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trainer_last_updated
ON trainer (last_updated);
