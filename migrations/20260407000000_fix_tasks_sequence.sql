-- Fix tasks_pkey sequence being out of sync with actual data.
-- This resets the sequence to MAX(id) so auto-generated IDs no longer collide.
SELECT setval(
    pg_get_serial_sequence('tasks', 'id'),
    COALESCE((SELECT MAX(id) FROM tasks), 0) + 1,
    false
);
