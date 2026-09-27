// Included by the owning module to preserve private field visibility.

#[derive(Clone)]
pub struct RedisStore {
    client: redis::Client,
    prefix: String,
}
