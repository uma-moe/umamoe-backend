// Included by the owning module to preserve private field visibility.

#[derive(Debug, Serialize, FromRow)]
pub struct MasterVersion {
    pub app_version: String,
    pub resource_version: String,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Serialize)]
pub struct VersionResponse {
    pub app_version: String,
    pub resource_version: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct VersionHistoryResponse {
    pub current: VersionResponse,
    pub history: Vec<VersionResponse>,
}
