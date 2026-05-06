-- Extend the task-completion trigger to also fire when a task transitions to
-- `processing` (i.e. a bot worker picks it up). This lets the backend's SSE
-- handler emit a `processing` event to the frontend so it can switch from
-- "Sending request…" to "Fetching inheritance from the game…" only once a
-- real worker has claimed the task.

CREATE OR REPLACE FUNCTION notify_task_completion() RETURNS trigger AS $$
BEGIN
    IF NEW.status IN ('processing', 'completed', 'failed')
       AND (OLD.status IS DISTINCT FROM NEW.status) THEN
        PERFORM pg_notify(
            'task_completion',
            NEW.id::text || ':' || NEW.status
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
