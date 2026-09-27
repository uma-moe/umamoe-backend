// Included by the owning module to preserve private field visibility.

const TASK_TYPE: &str = "practice_race/get_partner_info";

const RECONCILE_INTERVAL: Duration = Duration::from_secs(2);

const ANONYMOUS_TASK_CLEANUP_DELAY: Duration = Duration::from_secs(30);

struct BuiltCompletionEvent {
    event: Event,
    anonymous: bool,
}
