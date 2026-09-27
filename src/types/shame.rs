// Included by the owning module to preserve private field visibility.

const FAN_GAIN_RATE_EVIDENCE_LIFETIME_MIN: f64 = 50_000.0;

const FAN_GAIN_RATE_EVIDENCE_PEAK_MIN: f64 = 180_000.0;

const FAN_GAIN_RATE_EVIDENCE_HIGH_LIFETIME: f64 = 80_000.0;

const FAN_GAIN_RATE_EVIDENCE_HIGH_PEAK: f64 = 300_000.0;

#[derive(Debug, Deserialize)]
pub struct HallParams {
    #[serde(default)]
    pub page: Option<i64>,
    #[serde(default)]
    pub limit: Option<i64>,
    /// score (default), behavior_change, short_fan_gain, short_high_fan,
    /// max_session, careers_per_hour, avg_careers_per_day,
    /// avg_career_length, careers, active_time, fans_per_minute, peak_fans_per_minute,
    /// reset_breaks, long_hours, probe_score, career_quantization,
    /// career_regularity, login_regularity, zero_idle, burst_careers,
    /// coactivity
    pub sort_by: Option<String>,
    /// Minimum suspicion_score to include (default: suspicious threshold)
    pub min_score: Option<i32>,
    /// Minimum days observed (default 3 — exclude one-off blips)
    pub min_days: Option<i32>,
    /// Optional search by viewer_id/current circle_id or trainer/current
    /// circle name (case-insensitive partial)
    pub query: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HallEntry {
    pub viewer_id: i64,
    pub trainer_name: Option<String>,
    pub circle_id: Option<i64>,
    pub circle_name: Option<String>,
    pub circle_monthly_rank: Option<i32>,
    pub first_seen: chrono::DateTime<chrono::Utc>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
    pub days_observed: i32,
    pub days_active: i32,
    pub total_active_seconds: i64,
    pub total_fan_gain: i64,
    pub total_careers: i32,
    /// Careers averaged over observed calendar days.
    pub avg_careers_per_day: f64,
    /// Careers per hour from bounded finish-to-finish career-end intervals.
    /// The first observed career end is not counted because it has no prior
    /// end timestamp; intervals over 120 minutes are excluded.
    pub careers_per_active_hour: f64,
    pub career_rate_sample_count: i32,
    pub career_rate_sample_seconds: i64,
    pub career_rate_breakdown: CareerRateBreakdown,
    pub avg_career_length_last20_seconds: f64,
    /// Histogram of estimated career lengths. Index `i` counts careers
    /// whose estimated wall-clock duration fell into `[i*5, (i+1)*5)`
    /// minutes; the last bucket is an overflow for anything longer.
    pub career_length_buckets: Vec<i32>,
    /// Observable careers that were both short (<15 min) and high-fan-rate
    /// (>=90k fans/minute per estimated career).
    pub short_high_fan_careers: i32,
    /// Weighted severity for short high-fan careers. Higher means shorter
    /// careers with larger fan gain; useful for isolating 5-10 min full-fan
    /// runs.
    pub short_fan_gain_score: f64,
    /// Same 5-minute bucket mapping as `career_length_buckets`, but values
    /// are weighted short/high-fan severity rather than counts.
    pub short_fan_gain_score_buckets: Vec<f64>,
    /// Fan-gain distribution across all observable short careers (<15 min),
    /// using estimated fan gain per career.
    pub short_career_avg_fan_gain: f64,
    pub short_career_p50_fan_gain: f64,
    pub short_career_p90_fan_gain: f64,
    pub short_career_p95_fan_gain: f64,
    pub short_career_max_fan_gain: f64,
    /// Recent fan-gain spike signal: latest 3 observed days compared to the
    /// previous 14 observed days.
    pub recent_fan_gain_3d: i64,
    pub baseline_fan_gain_14d: i64,
    pub recent_fans_per_day: f64,
    pub baseline_fans_per_day: f64,
    pub fan_gain_spike_ratio: f64,
    pub behavior_change_score: f64,
    pub fans_per_active_minute: f64,
    pub peak_fans_per_minute: f64,
    pub high_fan_rate_windows: i32,
    pub high_fan_rate_total_fan_gain: i64,
    pub high_fan_rate_total_seconds: i32,
    pub max_daily_active_seconds: i32,
    pub max_daily_careers: i32,
    pub max_session_seconds: i32,
    pub days_over_16h: i32,
    pub days_over_20h: i32,
    pub reset_recovery_windows: i32,
    pub reset_breaks: i32,
    pub max_reset_recovery_seconds: i32,
    pub reset_break_score: f64,
    pub probe_score: f64,
    pub probe_metrics: SuspicionProbeMetrics,
    pub distinct_weekly_hour_buckets: i16,
    pub flag_no_sleep: bool,
    pub flag_extreme_session: bool,
    pub flag_inhuman_career_rate: bool,
    pub flag_247: bool,
    pub flag_marathon: bool,
    pub suspicion_score: i32,
    pub is_suspicious: bool,
    pub evidence: EvidenceSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EvidenceSummary {
    /// Machine-readable verdict for badges / filtering.
    pub verdict: String,
    /// Short human-readable explanation of the strongest signal.
    pub summary: String,
    /// The highest-confidence signal key, if any.
    pub strongest_signal: Option<String>,
    /// Ordered evidence, strongest first.
    pub reasons: Vec<EvidenceReason>,
    /// Important interpretation notes for the UI.
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceReason {
    pub key: String,
    pub label: String,
    /// critical, high, medium, low, info.
    pub severity: String,
    /// strong, medium, contextual.
    pub confidence: String,
    pub message: String,
    pub display_value: String,
    pub caveat: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HallResponse {
    pub entries: Vec<HallEntry>,
    pub total: i64,
    pub page: i64,
    pub limit: i64,
    pub total_pages: i64,
    pub suspicion_score_threshold: i32,
    pub last_refreshed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct ViewerReportParams {
    /// Number of recent days to include in the daily series (default 60)
    pub days: Option<i64>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct DailyPoint {
    pub day: chrono::NaiveDate,
    pub active_seconds: i32,
    pub careers: i32,
    pub fan_gain: i64,
    pub sessions: i32,
    pub longest_session_sec: i32,
    pub distinct_hours: i16,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct HeatmapCell {
    pub dow: i16,
    pub hour: i16,
    pub active_seconds: i32,
    pub careers: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopSessionBreakdown {
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub ended_at: chrono::DateTime<chrono::Utc>,
    pub duration_seconds: i32,
    pub active_seconds: i32,
    pub idle_seconds: i32,
    pub careers: i32,
    pub fan_gain: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopSession {
    pub day: chrono::NaiveDate,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub ended_at: chrono::DateTime<chrono::Utc>,
    pub playtime_seconds: i32,
    pub observed_seconds: i32,
    pub idle_seconds: i32,
    pub careers: i32,
    pub fan_gain: i64,
    pub session_count: i32,
    pub longest_session_sec: i32,
    pub distinct_hours: i16,
    pub sessions: Vec<TopSessionBreakdown>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ShortCareerTimelineSnapshot {
    pub snapshot_id: i64,
    pub circle_id: i64,
    pub snapshot_time: chrono::DateTime<chrono::Utc>,
    pub fans: i64,
    pub last_login: chrono::DateTime<chrono::Utc>,
    pub fan_delta: i64,
    pub gap_seconds: i32,
    pub login_changed: bool,
    pub tight_gap: bool,
    pub career_count: i32,
    pub active_seconds: i32,
    #[serde(default)]
    pub observed_runtime_seconds: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShortCareerSnapshot {
    pub rank: i16,
    pub total_count: i32,
    pub snapshot_id: i64,
    pub circle_id: i64,
    pub snapshot_time: chrono::DateTime<chrono::Utc>,
    pub previous_snapshot_id: i64,
    pub previous_snapshot_time: chrono::DateTime<chrono::Utc>,
    pub previous_snapshot_fans: i64,
    pub current_fans: i64,
    pub fan_gain: i64,
    pub snapshot_gap_seconds: i32,
    pub previous_career_snapshot_time: chrono::DateTime<chrono::Utc>,
    pub previous_career_gap_seconds: i32,
    pub career_length_seconds: i32,
    pub fans_per_minute: f64,
    pub short_training_score: f64,
    pub is_high_fan_short: bool,
    pub prior_snapshots: Vec<ShortCareerTimelineSnapshot>,
    pub next_snapshots: Vec<ShortCareerTimelineSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct ViewerReport {
    pub score: Option<HallEntry>,
    pub daily: Vec<DailyPoint>,
    pub heatmap: Vec<HeatmapCell>,
    pub top_sessions: Vec<TopSession>,
    pub short_career_snapshots: Vec<ShortCareerSnapshot>,
    pub short_career_snapshots_total: i32,
    pub last_refreshed_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub struct ShameSnapshot {
    /// All entries with `evidence` already attached. Stored in a stable
    /// default order; per-request sorts re-order an indices vector to
    /// avoid moving the heavy entries themselves.
    entries: Vec<HallEntry>,
    default_hall_indices: Vec<usize>,
    by_viewer: HashMap<i64, usize>,
    /// Last 365 days of activity per viewer, ordered by `day DESC`.
    daily: HashMap<i64, Vec<DailyPoint>>,
    heatmap: HashMap<i64, Vec<HeatmapCell>>,
    top_sessions: HashMap<i64, Vec<TopSession>>,
    short_career_snapshots: HashMap<i64, Vec<ShortCareerSnapshot>>,
    last_refreshed_at: Option<chrono::DateTime<chrono::Utc>>,
}
