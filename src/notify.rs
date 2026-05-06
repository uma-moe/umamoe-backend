//! Postgres LISTEN/NOTIFY dispatcher.
//!
//! Listens on the `task_completion` channel and fans notifications out to any
//! subscribers that are awaiting a particular `task_id` (typically an SSE
//! connection started by `POST /api/v4/partner/lookup`).
//!
//! Payload format (set in the `notify_task_completion` SQL trigger):
//!     "<task_id>:<status>"
//! where `status` is `completed` or `failed`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::{PgListener, PgPoolOptions};
use sqlx::PgPool;
use tokio::sync::{broadcast, Mutex};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct TaskCompletion {
    pub task_id: i32,
    pub status: String,
}

/// Per-task fan-out channels. We hand each subscriber a `broadcast::Receiver`
/// so multiple SSE connections waiting on the same task all get notified
/// (useful in dev when the dialog is opened twice).
#[derive(Clone)]
pub struct TaskNotifier {
    inner: Arc<Mutex<HashMap<i32, broadcast::Sender<TaskCompletion>>>>,
}

impl TaskNotifier {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Subscribe to completion events for `task_id`. Multiple subscribers per
    /// task are fine. The sender is removed once a terminal event has been
    /// dispatched (see `dispatch`).
    pub async fn subscribe(&self, task_id: i32) -> broadcast::Receiver<TaskCompletion> {
        let mut map = self.inner.lock().await;
        let tx = map
            .entry(task_id)
            .or_insert_with(|| broadcast::channel(4).0)
            .clone();
        tx.subscribe()
    }

    async fn dispatch(&self, evt: TaskCompletion) {
        let terminal = evt.status == "completed" || evt.status == "failed";
        let mut map = self.inner.lock().await;
        if terminal {
            // Remove the entry so we stop fanning out after a terminal event.
            if let Some(tx) = map.remove(&evt.task_id) {
                let _ = tx.send(evt);
            }
        } else if let Some(tx) = map.get(&evt.task_id) {
            // Non-terminal (e.g. "processing") — keep the sender in place.
            let _ = tx.send(evt);
        }
    }
}

impl Default for TaskNotifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn a background task that maintains a Postgres LISTEN connection and
/// forwards notifications to the supplied `TaskNotifier`. Reconnects on error.
pub fn spawn_listener(database_url: String, notifier: TaskNotifier) {
    tokio::spawn(async move {
        loop {
            match run_listener(&database_url, &notifier).await {
                Ok(()) => warn!("task_completion listener exited cleanly; reconnecting"),
                Err(e) => error!("task_completion listener error: {e}; reconnecting in 5s"),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn run_listener(database_url: &str, notifier: &TaskNotifier) -> sqlx::Result<()> {
    // Use a small dedicated pool — PgListener takes ownership of one connection
    // and never returns it.
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen("task_completion").await?;
    info!("📡 Listening for task_completion notifications");

    loop {
        let notification = listener.recv().await?;
        let payload = notification.payload();
        if let Some((id_str, status)) = payload.split_once(':') {
            if let Ok(task_id) = id_str.parse::<i32>() {
                notifier
                    .dispatch(TaskCompletion {
                        task_id,
                        status: status.to_string(),
                    })
                    .await;
            } else {
                warn!("Malformed task_completion payload: {payload}");
            }
        } else {
            warn!("Unexpected task_completion payload: {payload}");
        }
    }
}
