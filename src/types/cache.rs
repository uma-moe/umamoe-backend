// Included by the owning module to preserve private field visibility.

/// Maximum number of cache entries before eviction kicks in
const MAX_CACHE_ENTRIES: usize = 1000;

/// Global cache storage
static CACHE: OnceLock<DashMap<String, CacheEntry>> = OnceLock::new();

/// Cache entry with expiration and access tracking
#[derive(Clone)]
struct CacheEntry {
    data: String,
    expires_at: Instant,
    last_accessed: Instant,
    #[allow(dead_code)]
    size_bytes: usize,
}

/// Cache statistics
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CacheStats {
    pub entry_count: usize,
    pub total_size_bytes: usize,
    pub expired_count: usize,
}
