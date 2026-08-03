# Database storage runbook

The backend now bounds the fastest-growing operational tables with small,
committed batches. Only the primary writable backend runs the task; beta and
read-only instances skip it, and a PostgreSQL advisory lock prevents two
writable instances from running the same cycle concurrently.

Default retention:

- completed tasks: 36 hours (the public statistic needs the latest 24 hours)
- failed tasks: 14 days
- raw task attempts: 48 hours

Circle fan snapshots are deliberately not deleted. The current cheat-analysis
job performs a full rebuild from all raw snapshots every hour, so its processed
watermark is not a durable rollup and does not make older input disposable.
Snapshots need either explicitly rolling-window scoring or persisted
incremental state before retention can be enabled safely. Monthly daily-fan
rows and published cheat-analysis aggregates are also not deleted.

## Ranking archive maintenance

Ranking archive discovery runs hourly, but only one backend may run it at a
time. It generates the bounded month range and probes `(year, month)` indexes;
it does not scan the full monthly history. The session also uses
`RANKING_ARCHIVE_STATEMENT_TIMEOUT_SECONDS` (300 seconds by default), so an
unexpectedly expensive plan or backfill is canceled and retried on the next
cycle instead of consuming a database connection indefinitely.

If an old deployment left archive scans running, cancel only those statements
after the replacement backend is healthy:

```sql
SELECT pg_cancel_backend(pid)
FROM pg_stat_activity
WHERE application_name = 'honsemoe-backend'
  AND state = 'active'
  AND (
    query ILIKE 'SELECT DISTINCT cm.year, cm.month%'
    OR query ILIKE 'SELECT DISTINCT cmf.year, cmf.month%'
  );
```

## Rollout

1. Deploy the backend migration and retention task.
2. Watch for `Database retention deleted ...` messages. At the defaults, each
   15-minute cycle deletes at most 10,000 rows from each retained table, in paced
   5,000-row transactions. A large existing backlog is intentionally drained over
   several cycles.
3. Run the read-only production report:

   ```sh
   psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 \
     -f scripts/database_storage_diagnostics.sql
   ```

4. After the expired-row counts reach zero, run ordinary vacuum during a
   lower-traffic period:

   ```sql
   VACUUM (ANALYZE) tasks;
   VACUUM (ANALYZE) task_attempts;
   VACUUM (ANALYZE) trainer;
   VACUUM (ANALYZE) team_stadium;
   ```

Ordinary vacuum makes deleted space reusable by the same table and keeps
future growth bounded. It normally does not reduce the filesystem allocation
or the number reported by `pg_total_relation_size`.

## Returning space to the filesystem

If disk headroom requires the relation files to shrink immediately, schedule a
separate maintenance window after retention catches up. `VACUUM FULL` rewrites
the table, takes an `ACCESS EXCLUSIVE` lock, and temporarily needs enough disk
for the replacement table and indexes. Do not put it in an application
migration.

Run one table at a time, smallest/least critical first:

```sql
VACUUM (FULL, ANALYZE) task_attempts;
VACUUM (FULL, ANALYZE) tasks;
```

For a busy system, an online rewrite tool such as `pg_repack` is preferable if
it is installed and tested. It still requires temporary disk capacity.

## Cross-table audit priorities

The local July 3 copy and the July 28 production size report identify these
additional candidates. Run the diagnostic script in production before acting,
because row counts and index-use statistics differ from the local restore.

High-confidence operational cleanup:

- `task_attempts` is raw telemetry and was 98.5% successful attempts locally.
  Retain successful attempts for a much shorter window than failures if the
  operational dashboards have already rolled them up. Daily range partitions
  would make future expiration a partition drop instead of millions of row
  deletes.
- Local `borrow_interaction_buckets_v2` and `borrow_interaction_trends` were
  empty but still occupied 223 MB in old index pages. If production also has
  few live rows, `REINDEX TABLE CONCURRENTLY` returns that space. Date
  partitions prevent recurrence.
- The current backend reads and writes only the v2 borrow tables. The legacy
  `borrow_interaction_totals` and `borrow_interaction_buckets` occupy about
  474 MB in production and can be retired after confirming no old deployment
  or external client still writes them.
- `worker_instances` is ephemeral liveness state. The local copy had 114 rows,
  but production reports a 641 MB relation, which strongly suggests heartbeat
  update bloat or missing stale-instance cleanup. Delete expired state and
  rewrite the table after checking the owning worker service.

Coordinated representation changes:

- Six `team_stadium.parent_factors_*` arrays are written by `honsebot` but are
  not read by any local service. They average about 280 bytes per row: roughly
  7.4 GB in the 28.2-million-row local table. Stop writing/recreating the
  columns before dropping them, then rewrite the table to return the space.
- `trainer.release_num_info`, `trophy_num_info`, and `team_stadium_user` have
  fixed JSON object shapes. Typed integer fields need about 104 bytes versus
  582 bytes of JSONB in the local sample. A typed one-to-one detail table with
  update-on-change semantics would save roughly 3.4 GB across production's
  7.6 million trainers and keep the frequently updated trainer row narrow.
- `circle_member_fan_snapshots.last_login_times` is a `text[]`, and every
  analysis rebuild reparses it into timestamps. A `timestamptz[]` needs about
  258 bytes versus 725 bytes per sampled row, a 1.4 GB saving in the local
  snapshot table and likely more in production.
- Raw monthly fan arrays plus ranking archives currently grow by roughly
  0.8-1.0 GB per month. The API exposes historical daily charts, so completed
  raw months cannot simply be deleted. Keep current/previous months hot and
  move older daily detail to month partitions or compressed per-circle
  archives; the existing ranking archives can continue serving ranked lists.

Do not automatically delete raw circle fan snapshots yet. The analysis job
rebuilds lifetime state from the complete raw stream, so deleting a processed
row changes later results even when it is below the stored watermark.

## Redundant index cleanup

The local restored database contains several byte-for-byte redundant indexes.
Confirm the same definitions and nonzero replacement indexes in production,
then run each concurrent drop as a separate statement, outside a transaction:

```sql
-- Keep idx_trainer_updated, which the worker schema bootstrap owns.
DROP INDEX CONCURRENTLY IF EXISTS idx_trainer_last_updated;

-- Keep idx_trainer_account_follower.
DROP INDEX CONCURRENTLY IF EXISTS idx_trainer_follower_account_composite;
DROP INDEX CONCURRENTLY IF EXISTS idx_trainer_account_id_filtered;

-- Keep idx_circle_member_fans_circle_month, which the worker owns.
DROP INDEX CONCURRENTLY IF EXISTS idx_circle_member_fans_search;

-- The unique account_id constraint covers these plain account indexes.
DROP INDEX CONCURRENTLY IF EXISTS idx_inheritance_account_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_inheritance_trainer_id;

-- Keep idx_inheritance_main_chara_id.
DROP INDEX CONCURRENTLY IF EXISTS idx_inheritance_main_chara;
```

The local copy also has two unique constraints on `inheritance(account_id)`.
Drop the legacy `inheritance_account_id_unique` only if the worker-owned
`inheritance_account_id_key` is present and valid. This takes a brief table
lock and belongs in a low-traffic window:

```sql
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.inheritance'::regclass
          AND conname = 'inheritance_account_id_key'
          AND contype = 'u'
    ) AND EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.inheritance'::regclass
          AND conname = 'inheritance_account_id_unique'
          AND contype = 'u'
    ) THEN
        ALTER TABLE public.inheritance
            DROP CONSTRAINT inheritance_account_id_unique;
    END IF;
END
$$;
```

## `trainer` and `team_stadium`

The July 28 production sample reports 7.62 million live trainer rows, about
5.6 GB of logical row data, a 23 GB heap, and 8.3 GB of indexes. The heap is
4.16 times the sampled logical row size. Only about 9,400 dead tuples remain
after a recent autovacuum, so ordinary vacuum is working but cannot compact the
already fragmented heap. Roughly 480 million updates produced only 5 million
HOT updates; the current 80% fillfactor therefore provides little benefit for
this workload because frequently changed columns are indexed.

Before rewriting `trainer`, the three redundant production indexes in the
previous section can return about 1.7 GB immediately. If write downtime is
acceptable, consider raising the fillfactor during the rewrite and reclaiming
the heap in a maintenance window:

```sql
ALTER TABLE public.trainer SET (fillfactor = 95);
VACUUM (FULL, ANALYZE) public.trainer;
```

`VACUUM FULL` blocks reads and writes that need the table and needs temporary
disk for the replacement relation. For this busy table, prefer a tested
`pg_repack` run when the extension/tooling is available. Either approach
should be performed only after checking free disk space and long-running
transactions. The sampled live data suggests a compact heap on the order of
7 GB rather than 23 GB, but verify the result rather than treating that as a
guarantee.

The local `team_stadium` heap is mostly genuine live data: about 28.2 million
unique rows averaging 630 bytes. The six `parent_factors_*` arrays average
roughly 280 bytes per row and are written by `honsebot` but not read by any
local service. Removing them would save several GB, but is deliberately not
automatic because it discards potentially useful source data and requires a
coordinated worker/schema deployment.

## Affinity indexes

The local `inheritance` table also has roughly seventy
`idx_inheritance_total_affinity_*` indexes, totaling about 1.1 GB in the older
copy. The current search service computes affinity from its in-memory index,
but the backend still has a PostgreSQL fallback when that service is down.
Retire the affinity indexes only after either removing that fallback or
confirming from production logs that its degraded performance is acceptable.
Dropping these indexes would also materially reduce inheritance write
amplification.
