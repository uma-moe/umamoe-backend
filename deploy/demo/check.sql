-- Run against a freshly seeded demo, before testing mutations.
\set ON_ERROR_STOP on
DO $$
DECLARE
    relation text;
    actual bigint;
    expected bigint;
BEGIN
    IF current_database() <> 'umamoe_demo' THEN
        RAISE EXCEPTION 'Run this check only against umamoe_demo';
    END IF;
    FOR relation IN SELECT tablename FROM pg_tables
        WHERE schemaname = 'public' AND tablename <> '_sqlx_migrations'
    LOOP
        expected := CASE relation
            WHEN 'demo_seed' THEN 1
            WHEN 'cheat_analysis_meta' THEN 1
            WHEN 'circle_member_fans_monthly' THEN 300
            WHEN 'circle_member_fan_snapshots' THEN 200
            ELSE 100 END;
        EXECUTE format('SELECT count(*) FROM %I', relation) INTO actual;
        IF actual <> expected THEN
            RAISE EXCEPTION '%: expected % rows, found %', relation, expected, actual;
        END IF;
    END LOOP;
    IF (SELECT count(*) FROM trainer JOIN inheritance USING (account_id)
        JOIN support_card USING (account_id) JOIN linked_accounts USING (account_id)
        JOIN users ON users.id = linked_accounts.user_id) <> 100 THEN
        RAISE EXCEPTION 'Demo trainers, borrow records, and user accounts must be linked';
    END IF;
    IF (SELECT count(*) FROM circle_search_documents) <> 100
        OR (SELECT count(*) FROM user_fan_rankings_alltime) <> 100 THEN
        RAISE EXCEPTION 'Demo search and ranking views must be populated';
    END IF;
END $$;
SELECT 'Demo data checks passed' AS result;
