// Included by the owning module to preserve private field visibility.

const MAX_COLLECTION_BYTES: usize = 1_048_576;

const MAX_SHARE_BYTES: usize = 262_144;

const MAX_PLANS: usize = 50;

#[derive(Debug, FromRow)]
struct PlannerStateRow {
    revision: i64,
    collection: Value,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct PlannerStateResponse {
    revision: i64,
    collection: Option<Value>,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct PutPlannerStateRequest {
    base_revision: i64,
    collection: Value,
}

#[derive(Debug, Deserialize)]
struct SharePlanRequest {
    #[serde(default)]
    plan_id: Option<String>,
    #[serde(default)]
    plan_name: Option<String>,
    /// Kept for one rollout window so older frontends and unsynced plans remain
    /// readable. Reference-backed shares never persist this duplicate payload.
    #[serde(default)]
    plan: Option<Value>,
}

#[derive(Debug, FromRow)]
struct LegacyShareRow {
    share_id: String,
    plan_id: String,
    plan_name: String,
    plan: Value,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct StoredPlanRow {
    plan_id: String,
    collection: Value,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ShareResponse {
    share_id: String,
    plan_id: String,
    plan_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    plan: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection: Option<Value>,
    updated_at: DateTime<Utc>,
}
