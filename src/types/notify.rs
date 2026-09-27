// Included by the owning module to preserve private field visibility.

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
