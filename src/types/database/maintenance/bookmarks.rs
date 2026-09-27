// Included by the owning module to preserve private field visibility.

#[derive(Debug, Default)]
struct BookmarkHashBackfillStats {
    queued_accounts: i64,
    pending_queued_accounts: i64,
    processed_accounts: i64,
    updated_inheritances: i64,
    updated_bookmarks: i64,
}
