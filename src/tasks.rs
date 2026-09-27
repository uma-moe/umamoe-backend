pub use crate::types::tasks::*;
use sqlx::PgPool;

/// Reset the tasks id sequence if it falls behind (e.g. after manual inserts).
/// Called once on the first sequence-collision retry so subsequent inserts succeed.
pub(crate) async fn fixTaskSequence(pool: &PgPool) {
    let _ = sqlx::query(
        "SELECT setval(pg_get_serial_sequence('tasks','id'), COALESCE((SELECT MAX(id) FROM tasks),0)+1, false)"
    )
    .execute(pool)
    .await;
}

fn isTaskSequenceCollision(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database_error| database_error.constraint())
        == Some("tasks_pkey")
}

/// Insert a task, or return the already-active equivalent task. The workers'
/// partial unique index deliberately makes active task creation idempotent;
/// surfacing that conflict as HTTP 500 causes callers to retry work which is
/// already queued and amplifies load during controller recovery.
pub(crate) async fn insertOrGetActiveTask(
    pool: &PgPool,
    task_type: &str,
    task_data: &serde_json::Value,
    priority: i32,
    account_id: Option<&str>,
) -> Result<(Task, bool), sqlx::Error> {
    let mut sequence_fixed = false;
    for _ in 0..3 {
        let inserted = sqlx::query_as::<_, Task>(
            r#"
            INSERT INTO tasks (task_type, task_data, priority, status, created_at, account_id)
            VALUES ($1, $2, $3, 'pending', CURRENT_TIMESTAMP, $4)
            ON CONFLICT (task_type, task_data)
                WHERE status IN ('pending', 'processing')
            DO NOTHING
            RETURNING id, task_type, task_data, priority, status, created_at, updated_at,
                      worker_id, error_message, account_id
            "#,
        )
        .bind(task_type)
        .bind(task_data)
        .bind(priority)
        .bind(account_id)
        .fetch_optional(pool)
        .await;

        match inserted {
            Ok(Some(task)) => return Ok((task, true)),
            Ok(None) => {
                if let Some(task) = sqlx::query_as::<_, Task>(
                    r#"
                    SELECT id, task_type, task_data, priority, status, created_at, updated_at,
                           worker_id, error_message, account_id
                    FROM tasks
                    WHERE task_type = $1
                      AND task_data = $2
                      AND status IN ('pending', 'processing')
                    ORDER BY id ASC
                    LIMIT 1
                    "#,
                )
                .bind(task_type)
                .bind(task_data)
                .fetch_optional(pool)
                .await?
                {
                    return Ok((task, false));
                }
                // The conflicting row may have completed between INSERT and
                // SELECT. Retry so the request still leaves active work behind.
            }
            Err(error) if !sequence_fixed && isTaskSequenceCollision(&error) => {
                fixTaskSequence(pool).await;
                sequence_fixed = true;
            }
            Err(error) => return Err(error),
        }
    }
    Err(sqlx::Error::RowNotFound)
}
