// Included by the owning module to preserve private field visibility.

const CAREER_FAN_THRESHOLD: i64 = 100_000;

const CAREER_AVG_FANS: i64 = 700_000;

/// Cap a single transition's active seconds at 15 min for tight gaps.
const ACTIVE_SECONDS_CAP_PER_GAP: u32 = 900;

/// Anything above this is treated as a snapshot service gap for the
/// purposes of career-length / heatmap attribution (we can't claim time we
/// never observed).
const SESSION_GAP_MAX_SECONDS: i64 = 1800;

/// Short-career evidence needs stricter continuity than broad activity.
/// If a career finish appears after a long collection gap, the finish could
/// have happened anywhere inside that gap, so it cannot safely anchor the
/// next finish-to-finish runtime.
const TRUSTED_CAREER_CHAIN_MAX_GAP_SECONDS: i64 = 10 * 60;

/// Observed sessions should also split after a long no-gain stretch even if
/// snapshots keep arriving on time, otherwise one or two careers per day can
/// smear into all-day windows.
const OBSERVED_SESSION_IDLE_BREAK_SECONDS: i64 = 60 * 60;

const TOP_PLAYTIME_DAYS_PER_VIEWER: usize = 10;

const SHORT_CAREER_SNAPSHOTS_PER_VIEWER: usize = 25;

/// The game force-relogs every player at 00:00 JST = 15:00 UTC. Treat that
/// instant as an implicit session boundary even if we don't see a
/// `last_login_time` change in the snapshot stream right away.
const JST_RESET_HOUR_UTC: u32 = 15;

const LAST_CAREER_LENGTH_WINDOW: usize = 20;

const CAREER_RATE_MAX_SAMPLE_SECONDS: u32 = 120 * 60;

const CAREER_RATE_ROBUST_MIN_SAMPLES: usize = 10;

const CAREER_RATE_ZERO_IQR_LOWER_MULTIPLIER: f64 = 0.5;

const CAREER_RATE_ZERO_IQR_UPPER_MULTIPLIER: f64 = 2.0;

/// Width of each career-length histogram bucket, in seconds.
const CAREER_LENGTH_BUCKET_SECONDS: u32 = 300;

/// Number of buckets. Bucket i covers `[i*5, (i+1)*5)` minutes for
/// i < N-1; the last bucket is an overflow for anything longer.
const CAREER_LENGTH_BUCKETS: usize = 36;

/// Short runs under this duration are noisy alone, but suspicious when they
/// also produce very high fans.
const SHORT_HIGH_FAN_MAX_SECONDS: u32 = 15 * 60;

/// If we see a career finish but did not observe a chained finish-to-finish
/// interval, assume at least this much activity per counted career for rate
/// denominators. Shorter careers are only trusted when directly observed.
const UNOBSERVED_CAREER_ACTIVE_SECONDS_FLOOR: u32 = 10 * 60;

/// Fan gain per minute needed for the short/high-fan signal. This catches
/// the suspicious combination: very little observed career time but a large
/// fan jump during that interval.
const HIGH_FAN_GAIN_PER_SHORT_CAREER_MINUTE: f64 = 90_000.0;

/// Baseline fan gain for a full/high-value career. Weighted short-fan score
/// scales up above this and down below it.
const SHORT_FAN_GAIN_BASE_FANS: f64 = 900_000.0;

/// Max contribution multiplier from very short durations. A 15 min run is
/// 1x, 10 min is 1.5x, 5 min is 3x, and anything shorter caps at 4x.
const SHORT_FAN_GAIN_MAX_DURATION_MULTIPLIER: f64 = 4.0;

/// Sustained lifetime fan gain needs to be well above strong manual play
/// before it deserves a large score contribution.
const SUSTAINED_FAN_RATE_SCORE_BASE_FANS_PER_MINUTE: f64 = 50_000.0;

const SUSTAINED_FAN_RATE_SCORE_MAX: f64 = 6.0;

/// Peak fan-gain spikes are noisier than sustained pace, so require a much
/// higher threshold and cap their contribution lower than before.
const PEAK_FAN_RATE_SCORE_BASE_FANS_PER_MINUTE: f64 = 180_000.0;

const PEAK_FAN_RATE_SCORE_MAX: f64 = 6.0;

const REPEATED_HIGH_FAN_RATE_MIN_WINDOWS: u32 = 2;

const REPEATED_HIGH_FAN_RATE_FULL_WINDOWS: u32 = 4;

/// Broad weekly-hour coverage is mostly context. A long-lived account can
/// eventually touch the whole 7x24 grid, so the score contribution stays low
/// unless paired with real daily volume.
const HEATMAP_COVERAGE_SCORE_MAX: f64 = 4.0;

const HEATMAP_COVERAGE_FULL_VOLUME_ACTIVE_SECONDS_PER_DAY: f64 = 10.0 * 3600.0;

const FLAG_247_MIN_AVG_ACTIVE_SECONDS_PER_DAY: f64 = 8.0 * 3600.0;

/// Long daily productive coverage is the main signal for slower automation:
/// it catches accounts grinding for hours and hours even when each individual
/// career/session pace is not absurd.
const LONG_HOURS_SCORE_MAX: f64 = 28.0;

const LONG_HOURS_MAX_DAY_SCORE_MAX: f64 = 10.0;

const LONG_HOURS_AVG_DAY_SCORE_MAX: f64 = 8.0;

const LONG_HOURS_DAYS_OVER_16H_SCORE_MAX: f64 = 6.0;

const LONG_HOURS_DAYS_OVER_20H_SCORE_MAX: f64 = 8.0;

const SESSION_LENGTH_SCORE_MAX: f64 = 16.0;

const SESSION_LENGTH_SCORE_START_SECONDS: f64 = 4.0 * 3600.0;

const SESSION_LENGTH_SCORE_FULL_SECONDS: f64 = 10.0 * 3600.0;

/// Reset recovery: if an account was producing fans shortly before the daily
/// 00:00 JST reset and then repeatedly takes a long time to produce fans
/// again, that is useful bot-break context.
const RESET_RECOVERY_PRE_RESET_ACTIVE_WINDOW_SECONDS: i64 = 2 * 3600;

const RESET_BREAK_MIN_DELAY_SECONDS: i64 = 45 * 60;

const RESET_BREAK_SCORE_MAX: f64 = 10.0;

/// Experimental pattern probes. These scores are useful evidence, but capped
/// in the composite so they can't dominate direct pace/fan signals.
const PROBE_SCORE_CONTRIBUTION_MAX: f64 = 20.0;

const CAREER_FAN_GAIN_SCORE_MAX: f64 = 8.0;

const CAREER_REGULARITY_SCORE_MAX: f64 = 8.0;

const LOGIN_REGULARITY_SCORE_MAX: f64 = 5.0;

const POST_LOGIN_LATENCY_SCORE_MAX: f64 = 5.0;

const ZERO_IDLE_SCORE_MAX: f64 = 6.0;

const SCHEDULE_SHAPE_SCORE_MAX: f64 = 6.0;

const BURST_CAREER_SCORE_MAX: f64 = 5.0;

const SERVICE_GAP_RESUME_SCORE_MAX: f64 = 3.0;

const CIRCLE_CHURN_SCORE_MAX: f64 = 3.0;

const COACTIVITY_CLUSTER_SCORE_MAX: f64 = 6.0;

const ROLLING_BURST_WINDOW_SECONDS: i64 = 30 * 60;

const FAN_GAIN_MODE_BUCKET_FANS: u32 = 50_000;

const LOGIN_GAP_MODE_BUCKET_SECONDS: u32 = 15 * 60;

const MIN_PATTERN_SAMPLES: usize = 8;

/// Behavior-change signal: compare the latest 3 observed calendar days
/// against the previous 14 observed calendar days.
const RECENT_BEHAVIOR_DAYS: i64 = 3;

const BASELINE_BEHAVIOR_DAYS: i64 = 14;

/// Avoid infinite/absurd ratios for accounts with tiny historical baseline.
const BEHAVIOR_BASELINE_FAN_FLOOR: f64 = 500_000.0;

/// Default suspicious-activity cutoff for accounts considered suspicious.
pub const SUSPICIOUS_SCORE_THRESHOLD: i32 = 60;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SuspicionProbeMetrics {
    pub career_fan_gain_samples: i32,
    pub career_fan_gain_mode_share: f64,
    pub career_fan_gain_cv: f64,
    pub career_fan_gain_score: f64,
    pub career_rhythm_samples: i32,
    pub career_rhythm_cv: f64,
    pub career_length_cv: f64,
    pub career_regularity_score: f64,
    pub login_gap_samples: i32,
    pub login_gap_cv: f64,
    pub login_gap_mode_share: f64,
    pub login_regularity_score: f64,
    pub post_login_latency_samples: i32,
    pub post_login_latency_median_seconds: i32,
    pub post_login_latency_cv: f64,
    pub post_login_latency_score: f64,
    pub max_zero_idle_fan_gain_streak: i32,
    pub max_zero_idle_active_seconds: i32,
    pub zero_idle_score: f64,
    pub weekday_weekend_similarity: f64,
    pub hourly_entropy: f64,
    pub night_active_ratio: f64,
    pub night_active_seconds: i64,
    pub schedule_shape_score: f64,
    pub max_careers_30m: i32,
    pub burst_career_windows: i32,
    pub burst_career_score: f64,
    pub service_gap_resume_events: i32,
    pub service_gap_resume_score: f64,
    pub distinct_circles_seen: i32,
    pub circle_churn_score: f64,
    pub coactivity_cluster_size: i32,
    pub coactivity_cluster_score: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CareerRateBreakdown {
    pub all: CareerRateWindow,
    pub last_30d: CareerRateWindow,
    pub last_7d: CareerRateWindow,
    pub last_3d: CareerRateWindow,
    pub last_20: CareerRateWindow,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CareerRateWindow {
    pub careers_per_hour: f64,
    pub sample_count: i32,
    pub sample_seconds: i64,
}

#[derive(Debug, Clone)]
struct CareerRateSample {
    finished_at: DateTime<Utc>,
    seconds: u32,
}

pub struct RebuildStats {
    pub snapshots_processed: i64,
    pub viewers_scored: i64,
    pub last_snapshot_id: i64,
    pub duration_ms: i64,
}

const VERIFY_RATE_DIAGNOSTIC_COLUMNS_SQL: &str = r#"SELECT COUNT(*)
   FROM pg_attribute
   WHERE attrelid = 'viewer_suspicion_scores'::regclass
     AND attname::text = ANY($1::text[])
     AND attnum > 0
     AND NOT attisdropped"#;

#[derive(Default)]
struct DailyAccum {
    active_seconds: u32,
    careers: u32,
    fan_gain: u64,
    sessions: u32,
    longest_session_sec: u32,
    session_breakdown: Vec<SessionRecord>,
    /// bitmap of UTC-hour buckets touched today (bit 0..23)
    hours_bitmap: u32,
}

/// Flat 7*24 = 168 heatmap buckets. Wrapped so we can implement `Default`
/// (which the stdlib only derives for arrays up to length 32).
struct HeatmapBuckets([u32; 7 * 24]);

/// Per-viewer histogram of estimated career lengths. Index `i` counts
/// careers whose estimated wall-clock duration fell into
/// `[i*5, (i+1)*5)` minutes; the last bucket is an overflow for anything
/// longer than `(N-1)*5` minutes.
struct CareerLengthBuckets([u32; CAREER_LENGTH_BUCKETS]);

/// Per-viewer 5-minute buckets containing weighted short/high-fan severity,
/// not counts. Same index mapping as `CareerLengthBuckets`.
struct CareerLengthScoreBuckets([f64; CAREER_LENGTH_BUCKETS]);

/// One finalized observed session. `started_at` is the start of the tight
/// gap where fan gain first appeared; `ended_at` is the last tight-gap
/// fan-gain snapshot inside that observed window.
#[derive(Debug, Clone, Serialize)]
struct SessionRecord {
    started_at: DateTime<Utc>,
    ended_at: DateTime<Utc>,
    #[serde(rename = "duration_seconds")]
    duration_sec: u32,
    #[serde(rename = "active_seconds")]
    active_sec: u32,
    #[serde(rename = "idle_seconds")]
    idle_sec: u32,
    careers: u32,
    fan_gain: u64,
}

#[derive(Debug, Clone)]
struct ShortCareerSnapshotRecord {
    snapshot_id: i64,
    circle_id: i64,
    snapshot_time: DateTime<Utc>,
    previous_snapshot_id: i64,
    previous_snapshot_time: DateTime<Utc>,
    previous_snapshot_fans: i64,
    current_fans: i64,
    fan_gain: i64,
    snapshot_gap_seconds: u32,
    previous_career_snapshot_time: DateTime<Utc>,
    previous_career_gap_seconds: u32,
    career_length_seconds: u32,
    fans_per_minute: f64,
    short_training_score: f64,
    is_high_fan_short: bool,
    prior_snapshots: Vec<ShortCareerTimelineSnapshot>,
    next_snapshots: Vec<ShortCareerTimelineSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShortCareerTimelineSnapshot {
    snapshot_id: i64,
    circle_id: i64,
    snapshot_time: DateTime<Utc>,
    fans: i64,
    last_login: DateTime<Utc>,
    fan_delta: i64,
    gap_seconds: i32,
    login_changed: bool,
    tight_gap: bool,
    career_count: i32,
    active_seconds: i32,
    #[serde(default)]
    observed_runtime_seconds: i32,
}

struct PendingShortCareerSnapshotRecord {
    record: ShortCareerSnapshotRecord,
}

#[derive(Default)]
struct CareerObservation {
    active_seconds: Option<u32>,
    short_career_snapshot: Option<ShortCareerSnapshotRecord>,
}

/// Running state for the currently-open observed fan-gain session for one
/// viewer. Login timestamps are intentionally excluded from session
/// boundaries; they are too noisy to anchor top-session evidence.
#[derive(Clone)]
struct OpenSession {
    /// Start of the first tight observed fan-gain gap in this session.
    started_at: DateTime<Utc>,
    /// Wall-clock timestamp of the most recent snapshot where this viewer
    /// gained fans, or `None` if no fan growth has been observed yet.
    last_fan_gain_at: Option<DateTime<Utc>>,
    /// Cumulative seconds across adjacent snapshots inside this session
    /// that had no fan growth — i.e. observed idle time. Excludes time
    /// outside the [started_at, last seen] interval.
    idle_seconds_so_far: u32,
    /// Cumulative active seconds using the same conservative attribution
    /// as daily/total aggregates. Long outages contribute nothing and
    /// isolated career finishes use a plausible floor instead of only the
    /// final polling gap.
    active_seconds_so_far: u32,
    careers: u32,
    fan_gain: u64,
}

struct PendingResetRecovery {
    reset_at: DateTime<Utc>,
}

struct ShortCareerContext {
    snapshot_id: i64,
    circle_id: i64,
    snapshot_time: DateTime<Utc>,
    last_login: DateTime<Utc>,
    previous_snapshot_id: i64,
    previous_snapshot_time: DateTime<Utc>,
    previous_snapshot_fans: i64,
    current_fans: i64,
    snapshot_gap_seconds: i64,
    career_count: u32,
    fan_delta: i64,
}

#[derive(Default)]
struct ViewerAccum {
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    latest_circle_id: Option<i64>,
    circle_ids_seen: HashSet<i64>,

    // Rolling per-viewer state (last snapshot observed for this viewer).
    prev_fans: Option<i64>,
    prev_login: Option<DateTime<Utc>>,
    prev_snapshot_time: Option<DateTime<Utc>>,
    prev_snapshot_id: Option<i64>,

    daily: HashMap<NaiveDate, DailyAccum>,
    /// Flat 7*24 = 168 buckets, indexed by `dow * 24 + hour`.
    heatmap_active: HeatmapBuckets,
    heatmap_careers: HeatmapBuckets,

    total_active_seconds: u64,
    total_fan_gain: u64,
    total_careers: u32,

    career_lengths_last20: VecDeque<u32>,
    career_length_buckets: CareerLengthBuckets,
    short_high_fan_careers: u32,
    short_fan_gain_score: f64,
    short_fan_gain_score_buckets: CareerLengthScoreBuckets,
    short_career_fan_gains: Vec<u32>,
    trusted_career_fan_gains: Vec<u32>,
    trusted_career_lengths: Vec<u32>,
    career_finish_intervals: Vec<u32>,
    career_rate_samples: Vec<CareerRateSample>,
    /// Highest fan-gain rate observed across a single tight-gap transition,
    /// expressed as fans per minute. Long service-gap intervals are
    /// excluded so a multi-hour outage doesn't produce a fake peak.
    peak_fans_per_minute: f64,
    high_fan_rate_windows: u32,
    high_fan_rate_total_fan_gain: u64,
    high_fan_rate_total_seconds: u32,

    login_gap_seconds: Vec<u32>,
    pending_login_latency: Option<DateTime<Utc>>,
    post_login_latency_seconds: Vec<u32>,
    current_zero_idle_fan_gain_streak: u32,
    current_zero_idle_active_seconds: u32,
    max_zero_idle_fan_gain_streak: u32,
    max_zero_idle_active_seconds: u32,
    career_window_30m: VecDeque<(DateTime<Utc>, u32)>,
    career_window_30m_sum: u32,
    max_careers_30m: u32,
    burst_career_windows: u32,
    service_gap_resume_events: u32,

    max_session_seconds: u32,

    /// Currently-open observed fan-gain session, if any. `None` until a
    /// tight adjacent snapshot gap actually shows fan growth.
    open_session: Option<OpenSession>,

    reset_recovery_windows: u32,
    reset_breaks: u32,
    max_reset_recovery_seconds: u32,
    pending_reset_recovery: Option<PendingResetRecovery>,

    /// Timestamp of the previous observed career-finish snapshot, used by
    /// `record_career_lengths` to estimate per-career duration. Reset to
    /// `None` whenever an observation gap > `SESSION_GAP_MAX_SECONDS`
    /// breaks the chain.
    last_career_finish_at: Option<DateTime<Utc>>,

    /// True iff every adjacent snapshot transition since
    /// `last_career_finish_at` was tight **and** produced `fan_delta > 0`.
    /// When the next career finish lands and this is still true, we know
    /// the user was continuously playing through that whole interval, so
    /// the inter-finish gap honestly represents the new career's
    /// wall-clock length. If we observed any idle stretch or pipeline
    /// gap in between, we still keep a conservative display estimate,
    /// but we do not use that interval for short/high-fan automation
    /// scoring.
    career_chain_active: bool,

    short_career_snapshot_total: u32,
    short_career_snapshots: VecDeque<ShortCareerSnapshotRecord>,
    recent_short_career_timeline: VecDeque<ShortCareerTimelineSnapshot>,
    pending_short_career_snapshots: VecDeque<PendingShortCareerSnapshotRecord>,

    /// Min-heap (via Reverse) of the top-N longest sessions observed so far.
    top_sessions: BinaryHeap<Reverse<SessionOrd>>,
}

/// Wrapper that orders sessions by `(duration, start_timestamp)` so the
/// `BinaryHeap<Reverse<_>>` retains the longest entries.
struct SessionOrd {
    record: SessionRecord,
}

struct ScheduleShapeMetrics {
    weekday_weekend_similarity: f64,
    hourly_entropy: f64,
    night_active_ratio: f64,
    night_active_seconds: i64,
}

struct HallMetadata {
    trainer_name: Option<String>,
    circle_id: Option<i64>,
    circle_name: Option<String>,
    circle_monthly_rank: Option<i32>,
}

struct FanGainStats {
    avg: f64,
    p50: f64,
    p90: f64,
    p95: f64,
    max: f64,
}

struct BehaviorChangeStats {
    recent_fan_gain_3d: i64,
    baseline_fan_gain_14d: i64,
    recent_fans_per_day: f64,
    baseline_fans_per_day: f64,
    fan_gain_spike_ratio: f64,
    behavior_change_score: f64,
}

struct DailyRow {
    viewer_id: i64,
    day: NaiveDate,
    active_seconds: i32,
    careers: i32,
    fan_gain: i64,
    sessions: i32,
    longest_session_sec: i32,
    distinct_hours: i16,
}

struct HeatmapRow {
    viewer_id: i64,
    dow: i16,
    hour: i16,
    active_seconds: i32,
    careers: i32,
}

struct ScoreRow {
    viewer_id: i64,
    trainer_name: Option<String>,
    circle_id: Option<i64>,
    circle_name: Option<String>,
    circle_monthly_rank: Option<i32>,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    days_observed: i32,
    days_active: i32,
    total_active_seconds: i64,
    total_fan_gain: i64,
    total_careers: i32,
    careers_per_active_hour: f64,
    career_rate_sample_count: i32,
    career_rate_sample_seconds: i64,
    career_rate_breakdown: CareerRateBreakdown,
    avg_careers_per_day: f64,
    avg_career_length_last20_seconds: f64,
    career_length_buckets: Vec<i32>,
    short_high_fan_careers: i32,
    short_fan_gain_score: f64,
    short_fan_gain_score_buckets: Vec<f64>,
    short_career_avg_fan_gain: f64,
    short_career_p50_fan_gain: f64,
    short_career_p90_fan_gain: f64,
    short_career_p95_fan_gain: f64,
    short_career_max_fan_gain: f64,
    recent_fan_gain_3d: i64,
    baseline_fan_gain_14d: i64,
    recent_fans_per_day: f64,
    baseline_fans_per_day: f64,
    fan_gain_spike_ratio: f64,
    behavior_change_score: f64,
    fans_per_active_minute: f64,
    peak_fans_per_minute: f64,
    high_fan_rate_windows: i32,
    high_fan_rate_total_fan_gain: i64,
    high_fan_rate_total_seconds: i32,
    max_daily_active_seconds: i32,
    max_daily_careers: i32,
    max_session_seconds: i32,
    days_over_16h: i32,
    days_over_20h: i32,
    reset_recovery_windows: i32,
    reset_breaks: i32,
    max_reset_recovery_seconds: i32,
    reset_break_score: f64,
    probe_score: f64,
    probe_metrics: SuspicionProbeMetrics,
    coactivity_fingerprint: u64,
    distinct_weekly_hour_buckets: i16,
    flag_no_sleep: bool,
    flag_extreme_session: bool,
    flag_inhuman_career_rate: bool,
    flag_247: bool,
    flag_marathon: bool,
    suspicion_score: i32,
}

struct SessionRow {
    viewer_id: i64,
    rank: i16,
    day: NaiveDate,
    started_at: DateTime<Utc>,
    ended_at: DateTime<Utc>,
    duration_seconds: i32,
    active_seconds: i32,
    idle_seconds: i32,
    careers: i32,
    fan_gain: i64,
    session_count: i32,
    longest_session_sec: i32,
    distinct_hours: i16,
    sessions: Vec<SessionRecord>,
}

struct ShortCareerSnapshotRow {
    viewer_id: i64,
    rank: i16,
    total_count: i32,
    snapshot_id: i64,
    circle_id: i64,
    snapshot_time: DateTime<Utc>,
    previous_snapshot_id: i64,
    previous_snapshot_time: DateTime<Utc>,
    previous_snapshot_fans: i64,
    current_fans: i64,
    fan_gain: i64,
    snapshot_gap_seconds: i32,
    previous_career_snapshot_time: DateTime<Utc>,
    previous_career_gap_seconds: i32,
    career_length_seconds: i32,
    fans_per_minute: f64,
    short_training_score: f64,
    is_high_fan_short: bool,
    prior_snapshots: Vec<ShortCareerTimelineSnapshot>,
    next_snapshots: Vec<ShortCareerTimelineSnapshot>,
}

const CHUNK_ROWS: usize = 10_000;

const PUBLISH_VIEWER_CHUNK: usize = 500;
