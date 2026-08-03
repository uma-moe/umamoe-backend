-- Keep autovacuum responsive on tables whose row counts or update rates make
-- PostgreSQL's default 20% vacuum threshold much too large. These settings do
-- not delete rows or rewrite tables; they apply to subsequent maintenance.

DO $$
DECLARE
    table_name TEXT;
    parameter_index INTEGER;
    parameter_name TEXT;
    parameter_value TEXT;
    parameter_names TEXT[] := ARRAY[
        'autovacuum_vacuum_scale_factor',
        'autovacuum_vacuum_threshold',
        'autovacuum_analyze_scale_factor',
        'autovacuum_analyze_threshold'
    ];
    parameter_values TEXT[] := ARRAY['0.01', '10000', '0.02', '10000'];
    table_options TEXT[];
BEGIN
    -- Insert-triggered vacuum settings were added in PostgreSQL 13. Keep the
    -- migration compatible with the PostgreSQL 12 minimum documented by the
    -- service while enabling them on current servers.
    IF current_setting('server_version_num')::INTEGER >= 130000 THEN
        parameter_names := parameter_names || ARRAY[
            'autovacuum_vacuum_insert_scale_factor',
            'autovacuum_vacuum_insert_threshold'
        ];
        parameter_values := parameter_values || ARRAY['0.02', '10000'];
    END IF;

    FOREACH table_name IN ARRAY ARRAY[
        'tasks',
        'task_attempts',
        'trainer',
        'team_stadium',
        'inheritance',
        'circle_member_fans_monthly',
        'circle_member_fan_snapshots',
        'worker_instances',
        'worker_account_leases',
        'borrow_interaction_buckets_v2',
        'borrow_interaction_trends',
        'viewer_activity_daily',
        'viewer_activity_heatmap',
        'viewer_top_sessions',
        'viewer_short_career_snapshots',
        'viewer_suspicion_scores'
    ]
    LOOP
        IF to_regclass(format('public.%I', table_name)) IS NOT NULL THEN
            SELECT c.reloptions
            INTO table_options
            FROM pg_class AS c
            WHERE c.oid = to_regclass(format('public.%I', table_name));

            FOR parameter_index IN 1..array_length(parameter_names, 1)
            LOOP
                parameter_name := parameter_names[parameter_index];
                parameter_value := parameter_values[parameter_index];

                -- Preserve hand-tuned production settings. Only supply a
                -- per-table default when that parameter is currently absent.
                IF NOT EXISTS (
                    SELECT 1
                    FROM unnest(COALESCE(table_options, ARRAY[]::TEXT[])) AS option(value)
                    WHERE split_part(option.value, '=', 1) = parameter_name
                ) THEN
                    EXECUTE format(
                        'ALTER TABLE public.%I SET (%I = %s)',
                        table_name,
                        parameter_name,
                        parameter_value
                    );
                END IF;
            END LOOP;
        END IF;
    END LOOP;
END
$$;
