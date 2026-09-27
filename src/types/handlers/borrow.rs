// Included by the owning module to preserve private field visibility.

const MAX_BORROW_VIEW_BATCH_SIZE: usize = 100;

const BORROW_VIEW_DB_CONCURRENCY: usize = 2;

static BORROW_VIEW_DB_SLOTS: Semaphore = Semaphore::const_new(BORROW_VIEW_DB_CONCURRENCY);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BorrowInteraction {
    View,
    Copy,
}

#[derive(Debug, Default, Deserialize)]
struct BorrowInteractionPayload {
    borrow_key: Option<String>,
    inheritance_id: Option<i64>,
    support_card_id: Option<i32>,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct BorrowViewBatchPayload {
    views: Vec<BorrowViewPayload>,
}

#[derive(Debug, Deserialize)]
struct BorrowViewPayload {
    trainer_id: String,
    borrow_key: Option<String>,
    inheritance_id: Option<i64>,
    support_card_id: Option<i32>,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Debug, Serialize)]
struct BorrowViewBatchRow {
    trainer_id: String,
    borrow_key: String,
    inheritance_id: i64,
    support_card_id: i32,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Clone, Debug)]
struct BorrowContext {
    borrow_key: String,
    inheritance_id: i64,
    support_card_id: i32,
    support_card_limit_break: Option<i32>,
    support_card_experience: Option<i32>,
}

#[derive(Debug)]
struct BorrowCounts {
    view_count: i64,
    copy_count: i64,
    theoretical_copy_count: i32,
}

#[derive(Clone, Debug)]
struct BorrowActor {
    hash: String,
    bucket_start: chrono::DateTime<Utc>,
}
