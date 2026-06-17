-- `friend/recheck` was briefly used for borrow-triggered refreshes, but the
-- task worker only understands existing task types. Normalize any rows that
-- were already inserted before the backend was corrected.
DO $$
DECLARE
    task_data_udt TEXT;
BEGIN
    SELECT udt_name
    INTO task_data_udt
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'tasks'
      AND column_name = 'task_data';

    IF task_data_udt = 'json' THEN
        UPDATE tasks
        SET task_type = 'friend/search',
            task_data = jsonb_set(
                COALESCE(task_data::jsonb, '{}'::jsonb),
                '{action}',
                to_jsonb('search'::text),
                true
            )::json,
            updated_at = NOW()
        WHERE task_type = 'friend/recheck';
    ELSE
        UPDATE tasks
        SET task_type = 'friend/search',
            task_data = jsonb_set(
                COALESCE(task_data::jsonb, '{}'::jsonb),
                '{action}',
                to_jsonb('search'::text),
                true
            ),
            updated_at = NOW()
        WHERE task_type = 'friend/recheck';
    END IF;
END $$;
