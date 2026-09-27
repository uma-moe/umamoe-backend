use crate::{handlers, notify::TaskNotifier, redis_store};
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub search_client: reqwest::Client,
    pub search_url: String,
    pub oauth_redirect_base: String,
    pub task_notifier: TaskNotifier,
    pub redis_store: Option<redis_store::RedisStore>,
    pub user_writes_disabled: bool,
    pub simulator: Option<std::sync::Arc<handlers::simulator::Simulator>>,
}
