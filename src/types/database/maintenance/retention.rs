// Included by the owning module to preserve private field visibility.

#[derive(Clone, Copy, Debug)]
struct RetentionConfig {
    completed_task_hours: u64,
    failed_task_days: u64,
    task_attempt_hours: u64,
    batch_size: i64,
    max_batches_per_table: u64,
    batch_delay_ms: u64,
    interval_seconds: u64,
    start_delay_seconds: u64,
    statement_timeout_seconds: u64,
    lock_timeout_seconds: u64,
}

#[derive(Default, Debug)]
struct RetentionStats {
    completed_tasks: u64,
    failed_tasks: u64,
    task_attempts: u64,
}
