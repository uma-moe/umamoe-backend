\pset pager off
\timing on

\echo 'Database and vacuum configuration'
SELECT version(),
       current_database(),
       pg_size_pretty(pg_database_size(current_database())) AS database_size,
       current_setting('autovacuum') AS autovacuum,
       current_setting('track_counts') AS track_counts,
       current_setting('autovacuum_vacuum_scale_factor') AS default_vacuum_scale_factor,
       current_setting('autovacuum_analyze_scale_factor') AS default_analyze_scale_factor;

SELECT stats_reset
FROM pg_stat_database
WHERE datname = current_database();

\echo 'Largest tables: heap, indexes, TOAST, and total'
SELECT c.relname AS table_name,
       pg_size_pretty(pg_relation_size(c.oid)) AS heap,
       pg_size_pretty(pg_indexes_size(c.oid)) AS indexes,
       pg_size_pretty(
           pg_total_relation_size(c.oid)
           - pg_relation_size(c.oid)
           - pg_indexes_size(c.oid)
       ) AS toast,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total,
       c.reltuples::bigint AS estimated_rows,
       s.n_dead_tup AS estimated_dead_rows,
       s.last_autovacuum,
       s.last_autoanalyze,
       c.reloptions
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables AS s ON s.relid = c.oid
WHERE n.nspname = 'public'
  AND c.relkind IN ('r', 'p')
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 40;

\echo 'Operational history volume and configured retention candidates'
SELECT status,
       count(*) AS rows,
       min(created_at) AS oldest,
       max(COALESCE(updated_at, created_at)) AS newest,
       count(*) FILTER (
           WHERE status = 'completed'
             AND updated_at < CURRENT_TIMESTAMP - interval '36 hours'
       ) AS expired_at_default
FROM tasks
GROUP BY status
ORDER BY rows DESC;

SELECT date_trunc('day', created_at)::date AS day,
       result_class,
       count(*) AS rows,
       pg_size_pretty(sum(pg_column_size(attempt.*))::bigint) AS logical_row_bytes
FROM task_attempts AS attempt
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

SELECT count(*) AS task_attempt_rows_expired_at_48h
FROM task_attempts
WHERE created_at < CURRENT_TIMESTAMP - interval '48 hours';

SELECT result_class,
       count(*) AS rows,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS percent,
       pg_size_pretty(sum(pg_column_size(attempt.*))::bigint) AS logical_row_bytes,
       min(created_at) AS oldest,
       max(created_at) AS newest
FROM task_attempts AS attempt
GROUP BY result_class
ORDER BY rows DESC;

\echo 'Ephemeral worker state and time-bucket retention'
SELECT count(*) AS worker_instances,
       min(started_at) AS oldest_start,
       max(last_heartbeat_at) AS newest_heartbeat,
       count(*) FILTER (
           WHERE last_heartbeat_at < CURRENT_TIMESTAMP - interval '1 day'
       ) AS stale_1d,
       count(*) FILTER (
           WHERE last_heartbeat_at < CURRENT_TIMESTAMP - interval '7 days'
       ) AS stale_7d
FROM worker_instances;

SELECT count(*) AS worker_leases,
       count(*) FILTER (WHERE lease_expires_at < CURRENT_TIMESTAMP) AS expired,
       min(assigned_at) AS oldest_assignment,
       max(updated_at) AS newest_update
FROM worker_account_leases;

SELECT 'borrow_interaction_buckets_v2' AS table_name,
       count(*) AS rows,
       min(bucket_start)::text AS oldest,
       max(bucket_start)::text AS newest
FROM borrow_interaction_buckets_v2
UNION ALL
SELECT 'borrow_interaction_trends',
       count(*),
       min(trend_date)::text,
       max(trend_date)::text
FROM borrow_interaction_trends
UNION ALL
SELECT 'borrow_interaction_buckets_legacy',
       count(*),
       min(bucket_start)::text,
       max(bucket_start)::text
FROM borrow_interaction_buckets;

\echo 'Fan history growth and analysis watermark (watermark alone does not make snapshots disposable)'
SELECT date_trunc('month', snapshot_time)::date AS month,
       count(*) AS rows,
       pg_size_pretty(sum(pg_column_size(snapshot.*))::bigint) AS logical_row_bytes
FROM circle_member_fan_snapshots AS snapshot
GROUP BY 1
ORDER BY 1 DESC;

SELECT meta.last_snapshot_id,
       meta.last_refreshed_at,
       count(*) FILTER (
           WHERE snapshot.id <= meta.last_snapshot_id
             AND snapshot.snapshot_time < CURRENT_TIMESTAMP - interval '90 days'
       ) AS processed_snapshots_older_than_90d
FROM cheat_analysis_meta AS meta
LEFT JOIN circle_member_fan_snapshots AS snapshot ON true
WHERE meta.id = 1
GROUP BY meta.last_snapshot_id, meta.last_refreshed_at;

SELECT year,
       month,
       count(*) AS rows,
       pg_size_pretty(sum(pg_column_size(monthly.*))::bigint) AS logical_row_bytes
FROM circle_member_fans_monthly AS monthly
GROUP BY year, month
ORDER BY year DESC, month DESC;

SELECT raw.year,
       raw.month,
       raw.rows AS raw_monthly_rows,
       ranking.rows AS archived_ranking_rows,
       circle.rows AS archived_circle_rows
FROM (
    SELECT year, month, count(*) AS rows
    FROM circle_member_fans_monthly
    GROUP BY year, month
) AS raw
LEFT JOIN (
    SELECT year, month, count(*) AS rows
    FROM user_fan_rankings_monthly_archive
    GROUP BY year, month
) AS ranking USING (year, month)
LEFT JOIN (
    SELECT year, month, count(*) AS rows
    FROM circle_ranks_monthly_archive
    GROUP BY year, month
) AS circle USING (year, month)
ORDER BY raw.year DESC, raw.month DESC;

\echo 'Wide-row samples (SYSTEM sampling avoids scanning the full tables)'
SELECT count(*) AS sampled_rows,
       round(avg(pg_column_size(stadium.*))) AS avg_full_row_bytes,
       round(avg(pg_column_size(skills))) AS avg_skills_bytes,
       round(avg(pg_column_size(factors))) AS avg_factors_bytes,
       round(avg(pg_column_size(support_cards))) AS avg_support_cards_bytes,
       round(avg(pg_column_size(parent_factors_10))) AS avg_parent_10_bytes,
       round(avg(pg_column_size(parent_factors_20))) AS avg_parent_20_bytes,
       round(avg(pg_column_size(parent_factors_11))) AS avg_parent_11_bytes,
       round(avg(pg_column_size(parent_factors_12))) AS avg_parent_12_bytes,
       round(avg(pg_column_size(parent_factors_21))) AS avg_parent_21_bytes,
       round(avg(pg_column_size(parent_factors_22))) AS avg_parent_22_bytes
FROM team_stadium AS stadium TABLESAMPLE SYSTEM (0.1);

SELECT count(*) AS sampled_rows,
       round(avg(pg_column_size(trainer.*))) AS avg_full_row_bytes,
       round(avg(pg_column_size(release_num_info))
             FILTER (WHERE release_num_info IS NOT NULL)) AS avg_release_bytes,
       round(avg(pg_column_size(trophy_num_info))
             FILTER (WHERE trophy_num_info IS NOT NULL)) AS avg_trophy_bytes,
       round(avg(pg_column_size(team_stadium_user))
             FILTER (WHERE team_stadium_user IS NOT NULL)) AS avg_stadium_user_bytes
FROM trainer AS trainer TABLESAMPLE SYSTEM (0.5);

SELECT count(*) AS sampled_rows,
       round(avg(cardinality(last_login_times))) AS avg_members,
       round(avg(pg_column_size(last_login_times))) AS avg_login_text_array_bytes,
       round(avg(pg_column_size(array_fill(
           CURRENT_TIMESTAMP,
           ARRAY[cardinality(last_login_times)]
       )))) AS estimated_timestamptz_array_bytes
FROM circle_member_fan_snapshots TABLESAMPLE SYSTEM (0.2);

\echo 'Large-table index definitions and usage since the last statistics reset'
SELECT indexes.relname AS table_name,
       indexes.indexrelname AS index_name,
       pg_size_pretty(pg_relation_size(indexes.indexrelid)) AS index_size,
       indexes.idx_scan,
       pg_get_indexdef(indexes.indexrelid) AS definition
FROM pg_stat_user_indexes AS indexes
WHERE indexes.schemaname = 'public'
  AND indexes.relname IN (
      'trainer',
      'team_stadium',
      'tasks',
      'task_attempts',
      'inheritance',
      'support_card',
      'user_fan_rankings_monthly_archive',
      'circle_member_fans_monthly',
      'circle_member_fan_snapshots',
      'worker_instances',
      'worker_account_leases',
      'borrow_interaction_totals',
      'borrow_interaction_buckets',
      'borrow_interaction_totals_v2',
      'borrow_interaction_buckets_v2',
      'borrow_interaction_trends'
  )
ORDER BY indexes.relname, pg_relation_size(indexes.indexrelid) DESC;

\echo 'Byte-for-byte equivalent indexes'
WITH indexes AS (
    SELECT x.indrelid,
           tables.relname AS table_name,
           index_rel.relname AS index_name,
           x.indisunique,
           x.indisprimary,
           access_method.amname,
           x.indnkeyatts,
           x.indnatts,
           x.indkey,
           x.indclass,
           x.indcollation,
           x.indoption,
           pg_get_expr(x.indexprs, x.indrelid) AS expressions,
           pg_get_expr(x.indpred, x.indrelid) AS predicate
    FROM pg_index AS x
    JOIN pg_class AS tables ON tables.oid = x.indrelid
    JOIN pg_class AS index_rel ON index_rel.oid = x.indexrelid
    JOIN pg_namespace AS namespace ON namespace.oid = tables.relnamespace
    JOIN pg_am AS access_method ON access_method.oid = index_rel.relam
    WHERE namespace.nspname = 'public'
)
SELECT table_name,
       string_agg(
           index_name || CASE
               WHEN indisprimary THEN ' [PK]'
               WHEN indisunique THEN ' [UNIQUE]'
               ELSE ''
           END,
           ', ' ORDER BY index_name
       ) AS equivalent_indexes
FROM indexes
GROUP BY indrelid,
         table_name,
         amname,
         indnkeyatts,
         indnatts,
         indkey,
         indclass,
         indcollation,
         indoption,
         expressions,
         predicate
HAVING count(*) > 1
ORDER BY table_name, equivalent_indexes;

\echo 'Sessions that can prevent vacuum from reclaiming dead tuples'
SELECT pid,
       usename,
       application_name,
       state,
       age(clock_timestamp(), xact_start) AS transaction_age,
       wait_event_type,
       wait_event,
       left(query, 160) AS query
FROM pg_stat_activity
WHERE datname = current_database()
  AND xact_start IS NOT NULL
ORDER BY xact_start;

SELECT slot_name,
       slot_type,
       active,
       age(xmin) AS xmin_age,
       age(catalog_xmin) AS catalog_xmin_age,
       pg_size_pretty(
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
       ) AS retained_wal
FROM pg_replication_slots
ORDER BY slot_name;
