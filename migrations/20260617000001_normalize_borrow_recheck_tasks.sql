-- `friend/recheck` was briefly used for borrow-triggered refreshes, but the
-- task worker only understands existing task types. Normalize any rows that
-- were already inserted before the backend was corrected.
UPDATE tasks
SET task_type = 'friend/search',
    task_data = jsonb_set(
        COALESCE(task_data, '{}'::jsonb),
        '{action}',
        to_jsonb('search'::text),
        true
    ),
    updated_at = NOW()
WHERE task_type = 'friend/recheck';
