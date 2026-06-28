use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use sqlx::types::Json as SqlJson;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::{
    cache,
    cheat_analysis::{CareerRateBreakdown, SuspicionProbeMetrics, SUSPICIOUS_SCORE_THRESHOLD},
    errors::AppError,
    AppState,
};

const FAN_GAIN_RATE_EVIDENCE_LIFETIME_MIN: f64 = 50_000.0;
const FAN_GAIN_RATE_EVIDENCE_PEAK_MIN: f64 = 180_000.0;
const FAN_GAIN_RATE_EVIDENCE_HIGH_LIFETIME: f64 = 80_000.0;
const FAN_GAIN_RATE_EVIDENCE_HIGH_PEAK: f64 = 300_000.0;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/hall", get(get_hall_of_shame))
        .route("/viewer/:viewer_id", get(get_viewer_report))
}

// ---------------------------------------------------------------------------
// Suspicious activity: ranked list
// ---------------------------------------------------------------------------

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

impl<'r> FromRow<'r, PgRow> for HallEntry {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            viewer_id: row.try_get("viewer_id")?,
            trainer_name: row.try_get("trainer_name")?,
            circle_id: row.try_get("circle_id")?,
            circle_name: row.try_get("circle_name")?,
            circle_monthly_rank: row.try_get("circle_monthly_rank")?,
            first_seen: row.try_get("first_seen")?,
            last_seen: row.try_get("last_seen")?,
            days_observed: row.try_get("days_observed")?,
            days_active: row.try_get("days_active")?,
            total_active_seconds: row.try_get("total_active_seconds")?,
            total_fan_gain: row.try_get("total_fan_gain")?,
            total_careers: row.try_get("total_careers")?,
            avg_careers_per_day: row.try_get("avg_careers_per_day")?,
            careers_per_active_hour: row.try_get("careers_per_active_hour")?,
            career_rate_sample_count: row.try_get("career_rate_sample_count")?,
            career_rate_sample_seconds: row.try_get("career_rate_sample_seconds")?,
            career_rate_breakdown: row
                .try_get::<SqlJson<CareerRateBreakdown>, _>("career_rate_breakdown")?
                .0,
            avg_career_length_last20_seconds: row.try_get("avg_career_length_last20_seconds")?,
            career_length_buckets: row.try_get("career_length_buckets")?,
            short_high_fan_careers: row.try_get("short_high_fan_careers")?,
            short_fan_gain_score: row.try_get("short_fan_gain_score")?,
            short_fan_gain_score_buckets: row.try_get("short_fan_gain_score_buckets")?,
            short_career_avg_fan_gain: row.try_get("short_career_avg_fan_gain")?,
            short_career_p50_fan_gain: row.try_get("short_career_p50_fan_gain")?,
            short_career_p90_fan_gain: row.try_get("short_career_p90_fan_gain")?,
            short_career_p95_fan_gain: row.try_get("short_career_p95_fan_gain")?,
            short_career_max_fan_gain: row.try_get("short_career_max_fan_gain")?,
            recent_fan_gain_3d: row.try_get("recent_fan_gain_3d")?,
            baseline_fan_gain_14d: row.try_get("baseline_fan_gain_14d")?,
            recent_fans_per_day: row.try_get("recent_fans_per_day")?,
            baseline_fans_per_day: row.try_get("baseline_fans_per_day")?,
            fan_gain_spike_ratio: row.try_get("fan_gain_spike_ratio")?,
            behavior_change_score: row.try_get("behavior_change_score")?,
            fans_per_active_minute: row.try_get("fans_per_active_minute")?,
            peak_fans_per_minute: row.try_get("peak_fans_per_minute")?,
            high_fan_rate_windows: row.try_get("high_fan_rate_windows")?,
            high_fan_rate_total_fan_gain: row.try_get("high_fan_rate_total_fan_gain")?,
            high_fan_rate_total_seconds: row.try_get("high_fan_rate_total_seconds")?,
            max_daily_active_seconds: row.try_get("max_daily_active_seconds")?,
            max_daily_careers: row.try_get("max_daily_careers")?,
            max_session_seconds: row.try_get("max_session_seconds")?,
            days_over_16h: row.try_get("days_over_16h")?,
            days_over_20h: row.try_get("days_over_20h")?,
            reset_recovery_windows: row.try_get("reset_recovery_windows")?,
            reset_breaks: row.try_get("reset_breaks")?,
            max_reset_recovery_seconds: row.try_get("max_reset_recovery_seconds")?,
            reset_break_score: row.try_get("reset_break_score")?,
            probe_score: row.try_get("probe_score")?,
            probe_metrics: row
                .try_get::<SqlJson<SuspicionProbeMetrics>, _>("probe_metrics")?
                .0,
            distinct_weekly_hour_buckets: row.try_get("distinct_weekly_hour_buckets")?,
            flag_no_sleep: row.try_get("flag_no_sleep")?,
            flag_extreme_session: row.try_get("flag_extreme_session")?,
            flag_inhuman_career_rate: row.try_get("flag_inhuman_career_rate")?,
            flag_247: row.try_get("flag_247")?,
            flag_marathon: row.try_get("flag_marathon")?,
            suspicion_score: row.try_get("suspicion_score")?,
            is_suspicious: row.try_get("is_suspicious")?,
            evidence: EvidenceSummary::default(),
        })
    }
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

impl HallEntry {
    fn career_rate_last20(&self) -> f64 {
        self.career_rate_breakdown.last_20.careers_per_hour
    }

    fn attach_evidence(&mut self) {
        self.evidence = build_evidence_summary(self);
    }

    fn with_evidence(mut self) -> Self {
        self.attach_evidence();
        self
    }

    fn for_hall_list(mut self) -> Self {
        self.careers_per_active_hour = self.career_rate_breakdown.last_20.careers_per_hour;
        self.career_rate_sample_count = self.career_rate_breakdown.last_20.sample_count;
        self.career_rate_sample_seconds = self.career_rate_breakdown.last_20.sample_seconds;
        self.attach_evidence();
        self
    }
}

fn build_evidence_summary(entry: &HallEntry) -> EvidenceSummary {
    let mut reasons = Vec::new();
    let mut caveats = vec![
        "Names and circles can be old. The viewer_id is the account id.".to_string(),
        "These numbers describe the account, not who was playing it.".to_string(),
    ];

    let impossible_short = bucket_count(&entry.career_length_buckets, 0)
        + bucket_count(&entry.career_length_buckets, 1);
    let hard_short = bucket_count(&entry.career_length_buckets, 2);
    let short_total = impossible_short + hard_short;
    let short_ratio = if entry.total_careers > 0 {
        short_total as f64 / entry.total_careers as f64
    } else {
        0.0
    };
    let has_trusted_short_samples = entry.short_career_max_fan_gain > 0.0;
    let probes = &entry.probe_metrics;
    let avg_active_seconds_per_observed_day = if entry.days_observed > 0 {
        entry.total_active_seconds as f64 / entry.days_observed as f64
    } else {
        0.0
    };

    if entry.short_high_fan_careers > 0 || entry.short_fan_gain_score >= 8.0 {
        reasons.push(EvidenceReason {
            key: "short_high_fan_careers".to_string(),
            label: "Very short high-fan trainings".to_string(),
            severity: if entry.short_fan_gain_score >= 35.0 || entry.short_high_fan_careers >= 10 {
                "critical"
            } else {
                "high"
            }
            .to_string(),
            confidence: "strong".to_string(),
            message: format!(
                "{} training finish(es) were under 15 minutes and gained a lot of fans.",
                entry.short_high_fan_careers
            ),
            display_value: format!(
                "score {:.1}, max short gain {}",
                entry.short_fan_gain_score,
                format_fans(entry.short_career_max_fan_gain)
            ),
            caveat: Some("This is much stronger than a busy schedule by itself.".to_string()),
        });
    }

    if short_total > 0 && has_trusted_short_samples {
        let strong_short_gain = entry.short_career_p95_fan_gain >= 700_000.0;
        let high_volume_short = short_total >= 10 && short_ratio >= 0.10;
        reasons.push(EvidenceReason {
            key: "career_length_distribution".to_string(),
            label: "Very short trainings".to_string(),
            severity: if strong_short_gain && impossible_short > 0 {
                "high"
            } else if high_volume_short {
                "medium"
            } else {
                "low"
            }
            .to_string(),
            confidence: if strong_short_gain {
                "strong"
            } else if high_volume_short {
                "medium"
            } else {
                "contextual"
            }
            .to_string(),
            message: format!(
                "{} trusted training sample(s) were under 15 minutes; {} were under 10 minutes.",
                short_total, impossible_short
            ),
            display_value: format!(
                "{:.0}% short, p95 gain {}, avg last 20 {}",
                short_ratio * 100.0,
                format_fans(entry.short_career_p95_fan_gain),
                format_duration(entry.avg_career_length_last20_seconds.round() as i64)
            ),
            caveat: Some(
                "A short training can be an abandoned run. Short plus high fan gain matters more."
                    .to_string(),
            ),
        });
    }

    if entry.behavior_change_score > 0.0 {
        reasons.push(EvidenceReason {
            key: "behavior_change".to_string(),
            label: "Recent jump".to_string(),
            severity: if entry.fan_gain_spike_ratio >= 4.0 { "high" } else { "medium" }
                .to_string(),
            confidence: "medium".to_string(),
            message: "Recent days gained many more fans than the earlier days."
                .to_string(),
            display_value: format!(
                "{:.1}x baseline, recent {}/day",
                entry.fan_gain_spike_ratio,
                format_fans(entry.recent_fans_per_day)
            ),
            caveat: Some(
                "A sudden change can have normal reasons, so compare it with speed and training-time signals."
                    .to_string(),
            ),
        });
    }

    let has_repeated_peak_fan_rate = entry.high_fan_rate_windows >= 2
        && entry.peak_fans_per_minute >= FAN_GAIN_RATE_EVIDENCE_PEAK_MIN;
    let has_sustained_fan_rate =
        entry.fans_per_active_minute >= FAN_GAIN_RATE_EVIDENCE_LIFETIME_MIN;
    if has_repeated_peak_fan_rate || has_sustained_fan_rate {
        let display_value = if entry.high_fan_rate_windows > 0 {
            format!(
                "{} fast windows, {} fans over {}, peak {}/min, active avg {}/min",
                entry.high_fan_rate_windows,
                format_fans(entry.high_fan_rate_total_fan_gain as f64),
                format_duration(entry.high_fan_rate_total_seconds as i64),
                format_fans(entry.peak_fans_per_minute),
                format_fans(entry.fans_per_active_minute)
            )
        } else {
            format!(
                "active avg {}/min, peak {}/min",
                format_fans(entry.fans_per_active_minute),
                format_fans(entry.peak_fans_per_minute)
            )
        };
        reasons.push(EvidenceReason {
            key: "fan_gain_rate".to_string(),
            label: "Fast fan gain".to_string(),
            severity: if (entry.high_fan_rate_windows >= 3
                && entry.peak_fans_per_minute >= FAN_GAIN_RATE_EVIDENCE_HIGH_PEAK)
                || entry.fans_per_active_minute >= FAN_GAIN_RATE_EVIDENCE_HIGH_LIFETIME
            {
                "high"
            } else {
                "medium"
            }
            .to_string(),
            confidence: "medium".to_string(),
            message: if has_repeated_peak_fan_rate {
                "Fans went up unusually fast across multiple trusted snapshot windows.".to_string()
            } else {
                "Fans went up unusually fast across the trusted active-time total.".to_string()
            },
            display_value,
            caveat: Some(
                "This matters most when short-training evidence points the same way.".to_string(),
            ),
        });
    }

    if probes.career_fan_gain_score > 0.0 {
        reasons.push(EvidenceReason {
            key: "career_fan_gain_quantization".to_string(),
            label: "Repeated fan gains".to_string(),
            severity: if probes.career_fan_gain_score >= 6.0 {
                "high"
            } else {
                "medium"
            }
            .to_string(),
            confidence: if probes.career_fan_gain_score >= 7.0 {
                "strong"
            } else {
                "medium"
            }
            .to_string(),
            message: "Many trainings gained almost the same number of fans."
                .to_string(),
            display_value: format!(
                "mode {}, cv {:.2}, {} samples",
                format_percent(probes.career_fan_gain_mode_share),
                probes.career_fan_gain_cv,
                probes.career_fan_gain_samples
            ),
            caveat: Some(
                "Repeated numbers are only a clue. They matter more with short trainings or fast fan gain."
                    .to_string(),
            ),
        });
    }

    if probes.career_regularity_score > 0.0 {
        reasons.push(EvidenceReason {
            key: "career_rhythm_regularity".to_string(),
            label: "Repeated timing".to_string(),
            severity: if probes.career_regularity_score >= 6.0 {
                "high"
            } else {
                "medium"
            }
            .to_string(),
            confidence: "medium".to_string(),
            message: "Training finishes happened at very similar spacing."
                .to_string(),
            display_value: format!(
                "rhythm cv {:.2}, length cv {:.2}, {} samples",
                probes.career_rhythm_cv,
                probes.career_length_cv,
                probes.career_rhythm_samples
            ),
            caveat: Some(
                "Regular timing alone is not enough. It is more useful with high volume or other strong signals."
                    .to_string(),
            ),
        });
    }

    if probes.login_regularity_score + probes.post_login_latency_score >= 2.0 {
        reasons.push(EvidenceReason {
            key: "login_cadence_regularity".to_string(),
            label: "Repeated login timing".to_string(),
            severity: if probes.login_regularity_score + probes.post_login_latency_score >= 7.0 {
                "high"
            } else {
                "medium"
            }
            .to_string(),
            confidence: "contextual".to_string(),
            message: "Login timing, or time from login to first training finish, repeats closely."
                .to_string(),
            display_value: format!(
                "gap cv {:.2}, latency {}, latency cv {:.2}",
                probes.login_gap_cv,
                format_duration(probes.post_login_latency_median_seconds as i64),
                probes.post_login_latency_cv
            ),
            caveat: Some(
                "This is a weak clue because normal routines can also repeat.".to_string(),
            ),
        });
    }

    if probes.zero_idle_score > 0.0 {
        reasons.push(EvidenceReason {
            key: "zero_idle_streak".to_string(),
            label: "No-pause streak".to_string(),
            severity: if probes.zero_idle_score >= 4.5 { "high" } else { "medium" }
                .to_string(),
            confidence: "medium".to_string(),
            message: "Fans kept going up across many snapshots without a pause."
                .to_string(),
            display_value: format!(
                "{} snapshots, {} active",
                probes.max_zero_idle_fan_gain_streak,
                format_duration(probes.max_zero_idle_active_seconds as i64)
            ),
            caveat: Some(
                "This shows steady grinding. It is stronger when speed or short-training evidence agrees."
                    .to_string(),
            ),
        });
    }

    if probes.burst_career_score > 0.0 {
        reasons.push(EvidenceReason {
            key: "burst_careers".to_string(),
            label: "Training burst".to_string(),
            severity: if probes.max_careers_30m >= 5 { "high" } else { "medium" }.to_string(),
            confidence: "medium".to_string(),
            message: "Several trainings finished inside a short time window."
                .to_string(),
            display_value: format!(
                "max {} trainings / 30m, {} burst windows",
                probes.max_careers_30m, probes.burst_career_windows
            ),
            caveat: Some(
                "Snapshot timing can bunch events together, so compare this with the short-training rows."
                    .to_string(),
            ),
        });
    }

    if probes.coactivity_cluster_score > 0.0 {
        reasons.push(EvidenceReason {
            key: "coactivity_cluster".to_string(),
            label: "Similar schedule in circle".to_string(),
            severity: if probes.coactivity_cluster_size >= 6 { "high" } else { "medium" }
                .to_string(),
            confidence: "contextual".to_string(),
            message: "Several accounts in the same circle were active at very similar times."
                .to_string(),
            display_value: format!("{} matched accounts", probes.coactivity_cluster_size),
            caveat: Some(
                "Circle members can naturally play at similar times. Treat this as a lead, not proof."
                    .to_string(),
            ),
        });
    }

    if probes.schedule_shape_score > 0.0 {
        reasons.push(EvidenceReason {
            key: "schedule_shape".to_string(),
            label: "Very even schedule".to_string(),
            severity: if probes.schedule_shape_score >= 4.5 { "medium" } else { "low" }
                .to_string(),
            confidence: "contextual".to_string(),
            message: "The account plays at very even times, including nights or similar weekday/weekend hours."
                .to_string(),
            display_value: format!(
                "similarity {:.2}, entropy {:.2}, night {}",
                probes.weekday_weekend_similarity,
                probes.hourly_entropy,
                format_percent(probes.night_active_ratio)
            ),
            caveat: Some(
                "Schedule clues are background only. They need stronger evidence next to them."
                    .to_string(),
            ),
        });
    }

    if probes.service_gap_resume_score >= 1.5 {
        reasons.push(EvidenceReason {
            key: "post_gap_fan_gain".to_string(),
            label: "Fan gain after data gaps".to_string(),
            severity: "low".to_string(),
            confidence: "contextual".to_string(),
            message: "After data gaps, the next snapshot often already had a training-sized fan increase."
                .to_string(),
            display_value: format!("{} gap event(s)", probes.service_gap_resume_events),
            caveat: Some(
                "We cannot see what happened inside the gap, so this is a weak clue."
                    .to_string(),
            ),
        });
    }

    if probes.circle_churn_score > 0.0 {
        reasons.push(EvidenceReason {
            key: "circle_churn".to_string(),
            label: "Many circle changes".to_string(),
            severity: "low".to_string(),
            confidence: "contextual".to_string(),
            message: "The account appeared in many different circles during the data window."
                .to_string(),
            display_value: format!("{} circles", probes.distinct_circles_seen),
            caveat: Some(
                "Circle changes can happen normally, especially around recruiting or month end."
                    .to_string(),
            ),
        });
    }

    if entry.flag_inhuman_career_rate {
        reasons.push(EvidenceReason {
            key: "career_rate".to_string(),
            label: "Career runtime rate".to_string(),
            severity: "high".to_string(),
            confidence: "medium".to_string(),
            message: "The account finished too many trainings for the observed career runtimes."
                .to_string(),
            display_value: format!(
                "{:.1}/hour from {} runs over {}",
                entry.careers_per_active_hour,
                entry.career_rate_sample_count,
                format_duration(entry.career_rate_sample_seconds)
            ),
            caveat: None,
        });
    }

    if entry.reset_break_score >= 2.0 {
        let break_ratio = if entry.reset_recovery_windows > 0 {
            entry.reset_breaks as f64 / entry.reset_recovery_windows as f64
        } else {
            0.0
        };
        reasons.push(EvidenceReason {
            key: "reset_breaks".to_string(),
            label: "Stops after daily reset".to_string(),
            severity: if entry.reset_breaks >= 5 || break_ratio >= 0.5 {
                "high"
            } else {
                "medium"
            }
            .to_string(),
            confidence: "medium".to_string(),
            message: "The account was active before daily reset, then often took a long time to gain fans again."
                .to_string(),
            display_value: format!(
                "{} / {} reset windows, max recovery {}",
                entry.reset_breaks,
                entry.reset_recovery_windows,
                format_duration(entry.max_reset_recovery_seconds as i64)
            ),
            caveat: Some(
                "This can be normal sleep or stopping for the day. It matters most with long daily activity."
                    .to_string(),
            ),
        });
    }

    if entry.flag_247
        || (entry.distinct_weekly_hour_buckets >= 120
            && avg_active_seconds_per_observed_day >= 4.0 * 3600.0)
    {
        let caveat = "A full heatmap can happen with multi accounting or very heavy play. It is context, not proof.".to_string();
        caveats.push(caveat.clone());
        reasons.push(EvidenceReason {
            key: "heatmap_coverage".to_string(),
            label: "Heatmap coverage".to_string(),
            severity: if entry.flag_247 { "medium" } else { "low" }.to_string(),
            confidence: "contextual".to_string(),
            message: "The account was active across many different hours of the week.".to_string(),
            display_value: format!(
                "{} / 168 weekly hour buckets",
                entry.distinct_weekly_hour_buckets
            ),
            caveat: Some(caveat),
        });
    }

    if entry.flag_no_sleep
        || entry.flag_marathon
        || entry.days_over_16h > 0
        || entry.max_daily_active_seconds >= 14 * 3600
    {
        let caveat = "Long days can happen with multi accounting. Compare this with short-training and fan-speed evidence.".to_string();
        caveats.push(caveat.clone());
        reasons.push(EvidenceReason {
            key: "no_sleep_days".to_string(),
            label: "Long daily coverage".to_string(),
            severity: if entry.flag_marathon || entry.days_over_20h > 0 {
                "high"
            } else if entry.flag_no_sleep || entry.days_over_16h > 0 {
                "medium"
            } else {
                "low"
            }
            .to_string(),
            confidence: "contextual".to_string(),
            message: "The account was active for unusually long days.".to_string(),
            display_value: format!(
                "max day {}, days over 16h {}, days over 20h {}",
                format_duration(entry.max_daily_active_seconds as i64),
                entry.days_over_16h,
                entry.days_over_20h
            ),
            caveat: Some(caveat),
        });
    }

    if entry.flag_extreme_session || entry.max_session_seconds >= 6 * 3600 {
        reasons.push(EvidenceReason {
            key: "long_session".to_string(),
            label: "Long activity window".to_string(),
            severity: if entry.flag_extreme_session {
                "medium"
            } else {
                "low"
            }
            .to_string(),
            confidence: "contextual".to_string(),
            message: "Fan gain continued across one long observed activity window.".to_string(),
            display_value: format_duration(entry.max_session_seconds as i64),
            caveat: Some(
                "This shows the account gained fans across a long stretch. It does not tell us who was playing."
                    .to_string(),
            ),
        });
    }

    if reasons.is_empty() && entry.suspicion_score >= SUSPICIOUS_SCORE_THRESHOLD {
        reasons.push(EvidenceReason {
            key: "composite_score".to_string(),
            label: "Overall score".to_string(),
            severity: "medium".to_string(),
            confidence: "contextual".to_string(),
            message: "The total score is high, but no single reason stands out.".to_string(),
            display_value: format!("{} / 100", entry.suspicion_score),
            caveat: Some("Show the raw metrics next to this score for context.".to_string()),
        });
    }

    reasons.sort_by_key(|reason| evidence_rank(reason));
    caveats.sort();
    caveats.dedup();

    let strongest_signal = reasons.first().map(|reason| reason.key.clone());
    let verdict = classify_verdict(entry, &reasons);
    let summary = summarize_evidence(entry, &reasons, &verdict);

    EvidenceSummary {
        verdict,
        summary,
        strongest_signal,
        reasons,
        caveats,
    }
}

fn bucket_count(buckets: &[i32], index: usize) -> i32 {
    buckets.get(index).copied().unwrap_or_default()
}

fn evidence_rank(reason: &EvidenceReason) -> i32 {
    let severity = match reason.severity.as_str() {
        "critical" => 0,
        "high" => 10,
        "medium" => 20,
        "low" => 30,
        _ => 40,
    };
    let confidence = match reason.confidence.as_str() {
        "strong" => 0,
        "medium" => 1,
        _ => 2,
    };
    severity + confidence
}

fn classify_verdict(entry: &HallEntry, reasons: &[EvidenceReason]) -> String {
    let has_strong_automation = reasons.iter().any(|reason| {
        matches!(
            reason.key.as_str(),
            "short_high_fan_careers"
                | "career_length_distribution"
                | "fan_gain_rate"
                | "career_fan_gain_quantization"
                | "career_rhythm_regularity"
                | "zero_idle_streak"
        ) && reason.confidence == "strong"
    });
    let has_only_contextual = !reasons.is_empty()
        && reasons
            .iter()
            .all(|reason| reason.confidence == "contextual");

    if has_strong_automation {
        "strong_automation_signal".to_string()
    } else if entry.suspicion_score >= 80 {
        "very_high_suspicion".to_string()
    } else if entry.suspicion_score >= SUSPICIOUS_SCORE_THRESHOLD && has_only_contextual {
        "schedule_suspicion".to_string()
    } else if entry.suspicion_score >= SUSPICIOUS_SCORE_THRESHOLD {
        "suspicious".to_string()
    } else {
        "below_threshold".to_string()
    }
}

fn summarize_evidence(entry: &HallEntry, reasons: &[EvidenceReason], verdict: &str) -> String {
    if let Some(strongest) = reasons.first() {
        return format!(
            "{}: {} Score {} / 100.",
            strongest.label, strongest.display_value, entry.suspicion_score
        );
    }

    match verdict {
        "below_threshold" => format!(
            "Below the default suspicious threshold: score {} / 100.",
            entry.suspicion_score
        ),
        _ => format!("Suspicion score {} / 100.", entry.suspicion_score),
    }
}

fn format_duration(seconds: i64) -> String {
    if seconds <= 0 {
        return "0m".to_string();
    }
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    if hours > 0 {
        format!("{}h {}m", hours, minutes)
    } else {
        format!("{}m", minutes.max(1))
    }
}

fn format_fans(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.0}k", value / 1_000.0)
    } else {
        format!("{:.0}", value)
    }
}

fn format_percent(value: f64) -> String {
    format!("{:.0}%", (value * 100.0).clamp(0.0, 999.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cheat_analysis::CareerRateWindow;

    fn ts(value: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn entry_with_defaults() -> HallEntry {
        HallEntry {
            viewer_id: 1,
            trainer_name: None,
            circle_id: None,
            circle_name: None,
            circle_monthly_rank: None,
            first_seen: ts("2026-05-01T00:00:00Z"),
            last_seen: ts("2026-05-20T00:00:00Z"),
            days_observed: 20,
            days_active: 20,
            total_active_seconds: 20 * 3600,
            total_fan_gain: 20_000_000,
            total_careers: 30,
            avg_careers_per_day: 1.5,
            careers_per_active_hour: 1.5,
            career_rate_sample_count: 30,
            career_rate_sample_seconds: 20 * 3600,
            career_rate_breakdown: CareerRateBreakdown::default(),
            avg_career_length_last20_seconds: 3600.0,
            career_length_buckets: vec![0; 36],
            short_high_fan_careers: 0,
            short_fan_gain_score: 0.0,
            short_fan_gain_score_buckets: vec![0.0; 36],
            short_career_avg_fan_gain: 0.0,
            short_career_p50_fan_gain: 0.0,
            short_career_p90_fan_gain: 0.0,
            short_career_p95_fan_gain: 0.0,
            short_career_max_fan_gain: 0.0,
            recent_fan_gain_3d: 0,
            baseline_fan_gain_14d: 0,
            recent_fans_per_day: 0.0,
            baseline_fans_per_day: 0.0,
            fan_gain_spike_ratio: 0.0,
            behavior_change_score: 0.0,
            fans_per_active_minute: 0.0,
            peak_fans_per_minute: 0.0,
            high_fan_rate_windows: 0,
            high_fan_rate_total_fan_gain: 0,
            high_fan_rate_total_seconds: 0,
            max_daily_active_seconds: 3600,
            max_daily_careers: 2,
            max_session_seconds: 3600,
            days_over_16h: 0,
            days_over_20h: 0,
            reset_recovery_windows: 0,
            reset_breaks: 0,
            max_reset_recovery_seconds: 0,
            reset_break_score: 0.0,
            probe_score: 0.0,
            probe_metrics: SuspicionProbeMetrics::default(),
            distinct_weekly_hour_buckets: 20,
            flag_no_sleep: false,
            flag_extreme_session: false,
            flag_inhuman_career_rate: false,
            flag_247: false,
            flag_marathon: false,
            suspicion_score: 20,
            is_suspicious: false,
            evidence: EvidenceSummary::default(),
        }
    }

    #[test]
    fn stale_short_buckets_without_trusted_short_samples_are_not_evidence() {
        let mut entry = entry_with_defaults();
        entry.career_length_buckets[0] = 7;
        entry.career_length_buckets[1] = 8;
        entry.career_length_buckets[2] = 11;

        let summary = build_evidence_summary(&entry);

        assert!(!summary
            .reasons
            .iter()
            .any(|reason| reason.key == "career_length_distribution"));
    }

    #[test]
    fn hall_list_uses_last20_career_rate_fields() {
        let mut entry = entry_with_defaults();
        entry.careers_per_active_hour = 2.0;
        entry.career_rate_sample_count = 100;
        entry.career_rate_sample_seconds = 180_000;
        entry.career_rate_breakdown.last_20 = CareerRateWindow {
            careers_per_hour: 8.5,
            sample_count: 20,
            sample_seconds: 8_470,
        };

        let entry = entry.for_hall_list();

        assert_eq!(entry.careers_per_active_hour, 8.5);
        assert_eq!(entry.career_rate_sample_count, 20);
        assert_eq!(entry.career_rate_sample_seconds, 8_470);
    }

    #[test]
    fn trusted_short_samples_can_still_surface_career_distribution() {
        let mut entry = entry_with_defaults();
        entry.total_careers = 20;
        entry.career_length_buckets[0] = 2;
        entry.career_length_buckets[2] = 4;
        entry.short_career_p95_fan_gain = 850_000.0;
        entry.short_career_max_fan_gain = 900_000.0;

        let summary = build_evidence_summary(&entry);
        let reason = summary
            .reasons
            .iter()
            .find(|reason| reason.key == "career_length_distribution")
            .unwrap();

        assert_eq!(reason.confidence, "strong");
        assert_eq!(reason.severity, "high");
    }

    #[test]
    fn normal_career_finish_rates_do_not_surface_fan_rate_evidence() {
        let mut entry = entry_with_defaults();
        entry.peak_fans_per_minute = 60_000.0;
        entry.fans_per_active_minute = 36_000.0;

        let summary = build_evidence_summary(&entry);

        assert!(!summary
            .reasons
            .iter()
            .any(|reason| reason.key == "fan_gain_rate"));
    }

    #[test]
    fn extreme_attributed_fan_rate_still_surfaces() {
        let mut entry = entry_with_defaults();
        entry.peak_fans_per_minute = 320_000.0;
        entry.fans_per_active_minute = 90_000.0;

        let summary = build_evidence_summary(&entry);
        let reason = summary
            .reasons
            .iter()
            .find(|reason| reason.key == "fan_gain_rate")
            .unwrap();

        assert_eq!(reason.severity, "high");
        assert_eq!(reason.confidence, "medium");
    }

    #[test]
    fn single_peak_fan_rate_does_not_surface_without_repetition() {
        let mut entry = entry_with_defaults();
        entry.peak_fans_per_minute = 320_000.0;
        entry.high_fan_rate_windows = 1;

        let summary = build_evidence_summary(&entry);

        assert!(!summary
            .reasons
            .iter()
            .any(|reason| reason.key == "fan_gain_rate"));
    }

    #[test]
    fn repeated_peak_fan_rate_surfaces_with_window_count() {
        let mut entry = entry_with_defaults();
        entry.peak_fans_per_minute = 320_000.0;
        entry.high_fan_rate_windows = 3;

        let summary = build_evidence_summary(&entry);
        let reason = summary
            .reasons
            .iter()
            .find(|reason| reason.key == "fan_gain_rate")
            .unwrap();

        assert_eq!(reason.severity, "high");
        assert!(reason.display_value.contains("3 fast windows"));
    }

    #[test]
    fn loud_probe_metrics_surface_as_automation_evidence() {
        let mut entry = entry_with_defaults();
        entry.probe_score = 18.0;
        entry.probe_metrics.career_fan_gain_samples = 24;
        entry.probe_metrics.career_fan_gain_mode_share = 0.82;
        entry.probe_metrics.career_fan_gain_cv = 0.08;
        entry.probe_metrics.career_fan_gain_score = 7.5;
        entry.probe_metrics.max_zero_idle_fan_gain_streak = 20;
        entry.probe_metrics.max_zero_idle_active_seconds = 3 * 3600;
        entry.probe_metrics.zero_idle_score = 5.8;
        entry.suspicion_score = 72;

        let summary = build_evidence_summary(&entry);

        assert_eq!(summary.verdict, "strong_automation_signal");
        assert!(summary
            .reasons
            .iter()
            .any(|reason| reason.key == "career_fan_gain_quantization"
                && reason.confidence == "strong"));
        assert!(summary
            .reasons
            .iter()
            .any(|reason| reason.key == "zero_idle_streak"));
    }

    #[test]
    fn arc_like_low_volume_context_does_not_surface_noisy_reasons() {
        let mut entry = entry_with_defaults();
        entry.days_observed = 80;
        entry.days_active = 80;
        entry.total_active_seconds = 432_509;
        entry.total_fan_gain = 260_029_483;
        entry.total_careers = 319;
        entry.max_daily_active_seconds = 12_591;
        entry.max_session_seconds = 10_943;
        entry.reset_recovery_windows = 5;
        entry.reset_breaks = 3;
        entry.max_reset_recovery_seconds = 50_113;
        entry.reset_break_score = 0.0;
        entry.probe_score = 0.0;
        entry.probe_metrics.login_regularity_score = 0.67;
        entry.probe_metrics.service_gap_resume_events = 16;
        entry.probe_metrics.service_gap_resume_score = 0.0;
        entry.distinct_weekly_hour_buckets = 155;
        entry.suspicion_score = 15;

        let summary = build_evidence_summary(&entry);

        for noisy_key in [
            "reset_breaks",
            "login_cadence_regularity",
            "post_gap_fan_gain",
            "heatmap_coverage",
        ] {
            assert!(
                !summary.reasons.iter().any(|reason| reason.key == noisy_key),
                "{noisy_key} should not surface for Arc-like low-volume context"
            );
        }
        assert_eq!(summary.verdict, "below_threshold");
    }
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

async fn get_hall_of_shame(
    Query(params): Query<HallParams>,
    State(state): State<AppState>,
) -> Result<Json<HallResponse>, AppError> {
    let page = params.page.unwrap_or(0).max(0);
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let offset = page * limit;
    let min_score = params.min_score.unwrap_or(SUSPICIOUS_SCORE_THRESHOLD);
    let min_days = params.min_days.unwrap_or(3);
    let sort_by = params.sort_by.as_deref().unwrap_or("");
    let query_filter = params
        .query
        .as_ref()
        .map(|q| q.trim().to_string())
        .filter(|q| !q.is_empty());

    // Fast path: serve directly from the in-memory snapshot published by
    // the suspicious-activity rebuild. The snapshot already
    // contains every HallEntry with evidence attached, so this is just an
    // in-memory filter + sort + slice.
    if let Some(snapshot) = ensure_snapshot(&state.db).await {
        let response = snapshot.hall_page(
            min_score,
            min_days,
            sort_by,
            query_filter.as_deref(),
            page,
            limit,
        );
        return Ok(Json(response));
    }

    // Cold-start fallback: no snapshot yet (first ~90s after process
    // start, before the initial rebuild). Use the legacy SQL path so the
    // endpoint still works.
    crate::cheat_analysis::ensure_rate_diagnostic_columns(&state.db)
        .await
        .map_err(|e| {
            AppError::DatabaseError(format!(
                "failed to ensure suspicious-activity rate columns: {e}"
            ))
        })?;

    let cache_key = format!(
        "shame:hall:p={}:l={}:s={}:ms={}:md={}:q={}",
        page,
        limit,
        sort_by,
        min_score,
        min_days,
        query_filter.as_deref().unwrap_or(""),
    );
    if let Some(cached) = cache::get::<HallResponse>(&cache_key) {
        return Ok(Json(cached));
    }

    let order_by = match params.sort_by.as_deref() {
        Some("longest_session") | Some("online_streak") | Some("max_session") => {
            "s.max_session_seconds DESC"
        }
        Some("careers_per_hour") => {
            "COALESCE(((s.career_rate_breakdown->'last_20'->>'careers_per_hour')::double precision), 0) DESC"
        }
        Some("avg_careers_per_day") => "s.avg_careers_per_day DESC",
        Some("avg_career_length") => "s.avg_career_length_last20_seconds ASC",
        Some("behavior_change") => "s.behavior_change_score DESC, s.fan_gain_spike_ratio DESC",
        Some("short_fan_gain") => "s.short_fan_gain_score DESC, s.short_high_fan_careers DESC",
        Some("short_high_fan") => "s.short_high_fan_careers DESC, s.suspicion_score DESC",
        Some("fans_per_minute") => "s.fans_per_active_minute DESC",
        Some("peak_fans_per_minute") => {
            "s.high_fan_rate_windows DESC, s.peak_fans_per_minute DESC"
        }
        Some("reset_breaks") => "s.reset_break_score DESC, s.reset_breaks DESC",
        Some("long_hours") => {
            "s.days_over_20h DESC, s.days_over_16h DESC, s.max_daily_active_seconds DESC"
        }
        Some("probe_score") => "s.probe_score DESC, s.suspicion_score DESC",
        Some("career_quantization") => {
            "((s.probe_metrics->>'career_fan_gain_score')::double precision) DESC, s.probe_score DESC"
        }
        Some("career_regularity") => {
            "((s.probe_metrics->>'career_regularity_score')::double precision) DESC, s.probe_score DESC"
        }
        Some("login_regularity") => {
            "(((s.probe_metrics->>'login_regularity_score')::double precision) + ((s.probe_metrics->>'post_login_latency_score')::double precision)) DESC, s.probe_score DESC"
        }
        Some("zero_idle") => {
            "((s.probe_metrics->>'zero_idle_score')::double precision) DESC, s.probe_score DESC"
        }
        Some("burst_careers") => {
            "((s.probe_metrics->>'burst_career_score')::double precision) DESC, s.probe_score DESC"
        }
        Some("coactivity") => {
            "((s.probe_metrics->>'coactivity_cluster_score')::double precision) DESC, ((s.probe_metrics->>'coactivity_cluster_size')::int) DESC"
        }
        Some("careers") => "s.total_careers DESC",
        Some("active_time") => "s.total_active_seconds DESC",
        _ => "s.suspicion_score DESC, s.max_session_seconds DESC",
    };

    let mut where_clauses: Vec<String> = vec![
        "s.suspicion_score >= $1".to_string(),
        "s.days_observed >= $2".to_string(),
    ];
    let mut bind_idx = 3;
    if let Some(q) = &query_filter {
        if q.chars().all(|c| c.is_ascii_digit()) {
            where_clauses.push(format!(
                "(s.viewer_id = ${0} OR s.circle_id = ${0})",
                bind_idx
            ));
        } else {
            where_clauses.push(format!(
                "(s.trainer_name ILIKE ${0} OR s.circle_name ILIKE ${0})",
                bind_idx
            ));
        }
        bind_idx += 1;
    }
    let where_sql = where_clauses.join(" AND ");

    let count_sql = format!(
        r#"SELECT COUNT(*)::BIGINT FROM viewer_suspicion_scores s
           WHERE {where_sql}"#
    );
    let list_sql = format!(
        r#"SELECT
              s.viewer_id,
              s.trainer_name,
              s.circle_id,
              s.circle_name,
              s.circle_monthly_rank,
              s.first_seen, s.last_seen,
              s.days_observed, s.days_active,
              s.total_active_seconds, s.total_fan_gain, s.total_careers,
              s.avg_careers_per_day,
              s.careers_per_active_hour,
              s.career_rate_sample_count,
              s.career_rate_sample_seconds,
              s.career_rate_breakdown,
              s.avg_career_length_last20_seconds,
              s.career_length_buckets,
              s.short_high_fan_careers,
              s.short_fan_gain_score,
              s.short_fan_gain_score_buckets,
              s.short_career_avg_fan_gain,
              s.short_career_p50_fan_gain,
              s.short_career_p90_fan_gain,
              s.short_career_p95_fan_gain,
              s.short_career_max_fan_gain,
              s.recent_fan_gain_3d,
              s.baseline_fan_gain_14d,
              s.recent_fans_per_day,
              s.baseline_fans_per_day,
              s.fan_gain_spike_ratio,
              s.behavior_change_score,
              s.fans_per_active_minute,
              s.peak_fans_per_minute,
              s.high_fan_rate_windows,
              s.high_fan_rate_total_fan_gain,
              s.high_fan_rate_total_seconds,
              s.max_daily_active_seconds, s.max_daily_careers,
              s.max_session_seconds,
              s.days_over_16h, s.days_over_20h,
              s.reset_recovery_windows, s.reset_breaks,
              s.max_reset_recovery_seconds, s.reset_break_score,
              s.probe_score, s.probe_metrics,
              s.distinct_weekly_hour_buckets,
              s.flag_no_sleep, s.flag_extreme_session, s.flag_inhuman_career_rate,
                  s.flag_247, s.flag_marathon, s.suspicion_score,
                  (s.suspicion_score >= {SUSPICIOUS_SCORE_THRESHOLD}) AS is_suspicious
           FROM viewer_suspicion_scores s
           WHERE {where_sql}
           ORDER BY {order_by}
           LIMIT ${} OFFSET ${}"#,
        bind_idx,
        bind_idx + 1,
    );

    let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql)
        .bind(min_score)
        .bind(min_days);
    if let Some(q) = &query_filter {
        if q.chars().all(|c| c.is_ascii_digit()) {
            let id: i64 = q.parse().unwrap_or(0);
            count_q = count_q.bind(id);
        } else {
            count_q = count_q.bind(format!("%{}%", q));
        }
    }
    let total = count_q.fetch_one(&state.db).await?;

    let mut list_q = sqlx::query_as::<_, HallEntry>(&list_sql)
        .bind(min_score)
        .bind(min_days);
    if let Some(q) = &query_filter {
        if q.chars().all(|c| c.is_ascii_digit()) {
            let id: i64 = q.parse().unwrap_or(0);
            list_q = list_q.bind(id);
        } else {
            list_q = list_q.bind(format!("%{}%", q));
        }
    }
    let entries = list_q.bind(limit).bind(offset).fetch_all(&state.db).await?;
    let entries: Vec<HallEntry> = entries.into_iter().map(HallEntry::for_hall_list).collect();

    let last_refreshed_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT last_refreshed_at FROM cheat_analysis_meta WHERE id = 1")
            .fetch_optional(&state.db)
            .await?;

    let total_pages = if limit > 0 {
        (total + limit - 1) / limit
    } else {
        0
    };

    let response = HallResponse {
        entries,
        total,
        page,
        limit,
        total_pages,
        suspicion_score_threshold: SUSPICIOUS_SCORE_THRESHOLD,
        last_refreshed_at,
    };
    if let Err(err) = cache::set(&cache_key, &response, Duration::from_secs(60)) {
        warn!("failed to cache shame hall response: {err}");
    }
    Ok(Json(response))
}

// ---------------------------------------------------------------------------
// Per-viewer report: aggregates + daily series + heatmap + top sessions
// ---------------------------------------------------------------------------

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

fn top_session_from_row(row: &PgRow) -> Result<TopSession, sqlx::Error> {
    let active_seconds: i32 = row.try_get("active_seconds")?;
    Ok(TopSession {
        day: row.try_get("day")?,
        started_at: row.try_get("started_at")?,
        ended_at: row.try_get("ended_at")?,
        playtime_seconds: active_seconds,
        observed_seconds: row.try_get("duration_seconds")?,
        idle_seconds: row.try_get("idle_seconds")?,
        careers: row.try_get("careers")?,
        fan_gain: row.try_get("fan_gain")?,
        session_count: row.try_get("session_count")?,
        longest_session_sec: row.try_get("longest_session_sec")?,
        distinct_hours: row.try_get("distinct_hours")?,
        sessions: row
            .try_get::<SqlJson<Vec<TopSessionBreakdown>>, _>("sessions")?
            .0,
    })
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

fn short_career_snapshot_from_row(row: &PgRow) -> Result<ShortCareerSnapshot, sqlx::Error> {
    Ok(ShortCareerSnapshot {
        rank: row.try_get("rank")?,
        total_count: row.try_get("total_count")?,
        snapshot_id: row.try_get("snapshot_id")?,
        circle_id: row.try_get("circle_id")?,
        snapshot_time: row.try_get("snapshot_time")?,
        previous_snapshot_id: row.try_get("previous_snapshot_id")?,
        previous_snapshot_time: row.try_get("previous_snapshot_time")?,
        previous_snapshot_fans: row.try_get("previous_snapshot_fans")?,
        current_fans: row.try_get("current_fans")?,
        fan_gain: row.try_get("fan_gain")?,
        snapshot_gap_seconds: row.try_get("snapshot_gap_seconds")?,
        previous_career_snapshot_time: row.try_get("previous_career_snapshot_time")?,
        previous_career_gap_seconds: row.try_get("previous_career_gap_seconds")?,
        career_length_seconds: row.try_get("career_length_seconds")?,
        fans_per_minute: row.try_get("fans_per_minute")?,
        short_training_score: row.try_get("short_training_score")?,
        is_high_fan_short: row.try_get("is_high_fan_short")?,
        prior_snapshots: row
            .try_get::<SqlJson<Vec<ShortCareerTimelineSnapshot>>, _>("prior_snapshots")?
            .0,
        next_snapshots: row
            .try_get::<SqlJson<Vec<ShortCareerTimelineSnapshot>>, _>("next_snapshots")?
            .0,
    })
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

async fn get_viewer_report(
    Path(viewer_id): Path<i64>,
    Query(params): Query<ViewerReportParams>,
    State(state): State<AppState>,
) -> Result<Json<ViewerReport>, AppError> {
    let days = params.days.unwrap_or(60).clamp(1, 365) as usize;

    // Fast path: serve from the in-memory snapshot.
    if let Some(snapshot) = ensure_snapshot(&state.db).await {
        return Ok(Json(snapshot.viewer_report(viewer_id, days)));
    }

    // Cold-start fallback: legacy SQL path.
    crate::cheat_analysis::ensure_rate_diagnostic_columns(&state.db)
        .await
        .map_err(|e| {
            AppError::DatabaseError(format!(
                "failed to ensure suspicious-activity rate columns: {e}"
            ))
        })?;

    let days_i64 = days as i64;

    let score_sql = format!(
        r#"SELECT
              s.viewer_id,
              s.trainer_name,
              s.circle_id,
              s.circle_name,
              s.circle_monthly_rank,
              s.first_seen, s.last_seen,
              s.days_observed, s.days_active,
              s.total_active_seconds, s.total_fan_gain, s.total_careers,
              s.avg_careers_per_day,
              s.careers_per_active_hour,
              s.career_rate_sample_count,
              s.career_rate_sample_seconds,
              s.career_rate_breakdown,
              s.avg_career_length_last20_seconds,
              s.career_length_buckets,
              s.short_high_fan_careers,
              s.short_fan_gain_score,
              s.short_fan_gain_score_buckets,
              s.short_career_avg_fan_gain,
              s.short_career_p50_fan_gain,
              s.short_career_p90_fan_gain,
              s.short_career_p95_fan_gain,
              s.short_career_max_fan_gain,
              s.recent_fan_gain_3d,
              s.baseline_fan_gain_14d,
              s.recent_fans_per_day,
              s.baseline_fans_per_day,
              s.fan_gain_spike_ratio,
              s.behavior_change_score,
              s.fans_per_active_minute,
              s.peak_fans_per_minute,
              s.high_fan_rate_windows,
              s.high_fan_rate_total_fan_gain,
              s.high_fan_rate_total_seconds,
              s.max_daily_active_seconds, s.max_daily_careers,
              s.max_session_seconds,
              s.days_over_16h, s.days_over_20h,
              s.reset_recovery_windows, s.reset_breaks,
              s.max_reset_recovery_seconds, s.reset_break_score,
              s.probe_score, s.probe_metrics,
              s.distinct_weekly_hour_buckets,
              s.flag_no_sleep, s.flag_extreme_session, s.flag_inhuman_career_rate,
                  s.flag_247, s.flag_marathon, s.suspicion_score,
                  (s.suspicion_score >= {SUSPICIOUS_SCORE_THRESHOLD}) AS is_suspicious
           FROM viewer_suspicion_scores s
           WHERE s.viewer_id = $1"#,
    );
    let score = sqlx::query_as::<_, HallEntry>(&score_sql)
        .bind(viewer_id)
        .fetch_optional(&state.db)
        .await?
        .map(HallEntry::with_evidence);

    let daily = sqlx::query_as::<_, DailyPoint>(
        r#"SELECT day, active_seconds, careers, fan_gain, sessions,
                  longest_session_sec, distinct_hours
           FROM viewer_activity_daily
           WHERE viewer_id = $1
           ORDER BY day DESC
           LIMIT $2"#,
    )
    .bind(viewer_id)
    .bind(days_i64)
    .fetch_all(&state.db)
    .await?;

    let heatmap = sqlx::query_as::<_, HeatmapCell>(
        r#"SELECT dow, hour, active_seconds, careers
           FROM viewer_activity_heatmap
           WHERE viewer_id = $1
           ORDER BY dow, hour"#,
    )
    .bind(viewer_id)
    .fetch_all(&state.db)
    .await?;

    // Top playtime days: precomputed by the Rust pipeline. The response key
    // remains `top_sessions` for API compatibility.
    let top_session_rows = sqlx::query(
        r#"SELECT COALESCE(day, (started_at AT TIME ZONE 'UTC')::date) AS day,
                  started_at, ended_at, duration_seconds, active_seconds,
                  idle_seconds, careers, fan_gain,
                  COALESCE(NULLIF(session_count, 0), 1) AS session_count,
                  COALESCE(NULLIF(longest_session_sec, 0), duration_seconds) AS longest_session_sec,
                  distinct_hours,
                  CASE WHEN sessions = '[]'::jsonb THEN jsonb_build_array(jsonb_build_object(
                      'started_at', started_at,
                      'ended_at', ended_at,
                      'duration_seconds', duration_seconds,
                      'active_seconds', active_seconds,
                      'idle_seconds', idle_seconds,
                      'careers', careers,
                      'fan_gain', fan_gain
                  )) ELSE sessions END AS sessions
           FROM viewer_top_sessions
           WHERE viewer_id = $1
           ORDER BY rank
           LIMIT 10"#,
    )
    .bind(viewer_id)
    .fetch_all(&state.db)
    .await?;
    let top_sessions: Vec<TopSession> = top_session_rows
        .into_iter()
        .map(|row| top_session_from_row(&row))
        .collect::<Result<_, _>>()?;

    let short_career_snapshot_rows = sqlx::query(
        r#"SELECT rank, total_count, snapshot_id, circle_id, snapshot_time,
                  previous_snapshot_id, previous_snapshot_time, previous_snapshot_fans,
                  current_fans, fan_gain, snapshot_gap_seconds,
                  previous_career_snapshot_time, previous_career_gap_seconds,
                  career_length_seconds, fans_per_minute, short_training_score,
                  is_high_fan_short, prior_snapshots, next_snapshots
           FROM viewer_short_career_snapshots
           WHERE viewer_id = $1
           ORDER BY rank
           LIMIT 25"#,
    )
    .bind(viewer_id)
    .fetch_all(&state.db)
    .await?;
    let short_career_snapshots: Vec<ShortCareerSnapshot> = short_career_snapshot_rows
        .into_iter()
        .map(|row| short_career_snapshot_from_row(&row))
        .collect::<Result<_, _>>()?;
    let short_career_snapshots_total = short_career_snapshots
        .first()
        .map(|row| row.total_count)
        .unwrap_or_default();

    let last_refreshed_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT last_refreshed_at FROM cheat_analysis_meta WHERE id = 1")
            .fetch_optional(&state.db)
            .await?;

    Ok(Json(ViewerReport {
        score,
        daily,
        heatmap,
        top_sessions,
        short_career_snapshots,
        short_career_snapshots_total,
        last_refreshed_at,
    }))
}

// ---------------------------------------------------------------------------
// In-memory snapshot
//
// The suspicious-activity pipeline runs every hour and republishes the
// entire `viewer_suspicion_scores` / `viewer_activity_daily` /
// `viewer_activity_heatmap` / `viewer_top_sessions` tables. Per-request
// fetches over those tables (especially the ~40-column score row plus
// COUNT, ORDER BY arbitrary column, and three follow-up queries for the
// viewer report) were the slow part of the `/shame` endpoints.
//
// Instead, after every rebuild we eagerly load all of that data into an
// `Arc<ShameSnapshot>` and answer requests entirely from memory. The
// snapshot also pre-attaches `evidence` to every HallEntry, so paginated
// hall responses are just filter + sort + clone on already-finished data.
// ---------------------------------------------------------------------------

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

fn snapshot_slot() -> &'static RwLock<Option<Arc<ShameSnapshot>>> {
    static SLOT: OnceLock<RwLock<Option<Arc<ShameSnapshot>>>> = OnceLock::new();
    SLOT.get_or_init(|| RwLock::new(None))
}

fn current_snapshot() -> Option<Arc<ShameSnapshot>> {
    snapshot_slot().read().ok().and_then(|g| g.clone())
}

fn install_snapshot(snapshot: ShameSnapshot) {
    if let Ok(mut g) = snapshot_slot().write() {
        *g = Some(Arc::new(snapshot));
    }
}

async fn ensure_snapshot(pool: &PgPool) -> Option<Arc<ShameSnapshot>> {
    if let Some(snapshot) = current_snapshot() {
        return Some(snapshot);
    }

    match rebuild_snapshot(pool).await {
        Ok(()) => current_snapshot(),
        Err(err) => {
            warn!("failed to build shame snapshot on demand: {}", err);
            None
        }
    }
}

/// Rebuild the in-memory snapshot from the freshly published aggregate
/// tables. Called from `cheat_analysis::run_full_rebuild` after commit.
pub async fn rebuild_snapshot(pool: &PgPool) -> anyhow::Result<()> {
    let start = Instant::now();

    crate::cheat_analysis::ensure_rate_diagnostic_columns(pool).await?;

    // These feeds are independent. Fetch them concurrently so the
    // overall rebuild time is bounded by the slowest query (the scores
    // SELECT) instead of the sum.
    let daily_cutoff = chrono::Utc::now().date_naive() - chrono::Duration::days(365);
    let score_sql = format!(
        r#"SELECT
              s.viewer_id, s.trainer_name, s.circle_id, s.circle_name,
              s.circle_monthly_rank,
              s.first_seen, s.last_seen,
              s.days_observed, s.days_active,
              s.total_active_seconds, s.total_fan_gain, s.total_careers,
              s.avg_careers_per_day,
              s.careers_per_active_hour,
              s.career_rate_sample_count,
              s.career_rate_sample_seconds,
              s.career_rate_breakdown,
              s.avg_career_length_last20_seconds,
              s.career_length_buckets,
              s.short_high_fan_careers,
              s.short_fan_gain_score,
              s.short_fan_gain_score_buckets,
              s.short_career_avg_fan_gain,
              s.short_career_p50_fan_gain,
              s.short_career_p90_fan_gain,
              s.short_career_p95_fan_gain,
              s.short_career_max_fan_gain,
              s.recent_fan_gain_3d,
              s.baseline_fan_gain_14d,
              s.recent_fans_per_day,
              s.baseline_fans_per_day,
              s.fan_gain_spike_ratio,
              s.behavior_change_score,
              s.fans_per_active_minute,
              s.peak_fans_per_minute,
              s.high_fan_rate_windows,
              s.high_fan_rate_total_fan_gain,
              s.high_fan_rate_total_seconds,
              s.max_daily_active_seconds, s.max_daily_careers,
              s.max_session_seconds,
              s.days_over_16h, s.days_over_20h,
              s.reset_recovery_windows, s.reset_breaks,
              s.max_reset_recovery_seconds, s.reset_break_score,
              s.probe_score, s.probe_metrics,
              s.distinct_weekly_hour_buckets,
              s.flag_no_sleep, s.flag_extreme_session, s.flag_inhuman_career_rate,
              s.flag_247, s.flag_marathon, s.suspicion_score,
              (s.suspicion_score >= {SUSPICIOUS_SCORE_THRESHOLD}) AS is_suspicious
           FROM viewer_suspicion_scores s
           ORDER BY s.suspicion_score DESC, s.max_session_seconds DESC, s.viewer_id"#,
    );
    let scores_fut = sqlx::query_as::<_, HallEntry>(&score_sql).fetch_all(pool);
    let daily_fut = sqlx::query(
        r#"SELECT viewer_id, day, active_seconds, careers, fan_gain, sessions,
                  longest_session_sec, distinct_hours
           FROM viewer_activity_daily
           WHERE day >= $1
           ORDER BY viewer_id, day DESC"#,
    )
    .bind(daily_cutoff)
    .fetch_all(pool);
    let heatmap_fut = sqlx::query(
        r#"SELECT viewer_id, dow, hour, active_seconds, careers
           FROM viewer_activity_heatmap
           ORDER BY viewer_id, dow, hour"#,
    )
    .fetch_all(pool);
    let sessions_fut = sqlx::query(
        r#"SELECT viewer_id,
                  COALESCE(day, (started_at AT TIME ZONE 'UTC')::date) AS day,
                  started_at, ended_at, duration_seconds, active_seconds,
                  idle_seconds, careers, fan_gain,
                  COALESCE(NULLIF(session_count, 0), 1) AS session_count,
                  COALESCE(NULLIF(longest_session_sec, 0), duration_seconds) AS longest_session_sec,
                  distinct_hours,
                  CASE WHEN sessions = '[]'::jsonb THEN jsonb_build_array(jsonb_build_object(
                      'started_at', started_at,
                      'ended_at', ended_at,
                      'duration_seconds', duration_seconds,
                      'active_seconds', active_seconds,
                      'idle_seconds', idle_seconds,
                      'careers', careers,
                      'fan_gain', fan_gain
                  )) ELSE sessions END AS sessions
           FROM viewer_top_sessions
           ORDER BY viewer_id, rank"#,
    )
    .fetch_all(pool);
    let short_snapshots_fut = sqlx::query(
        r#"SELECT viewer_id, rank, total_count, snapshot_id, circle_id, snapshot_time,
                previous_snapshot_id, previous_snapshot_time, previous_snapshot_fans,
                current_fans, fan_gain, snapshot_gap_seconds,
                previous_career_snapshot_time, previous_career_gap_seconds,
            career_length_seconds, fans_per_minute, short_training_score,
            is_high_fan_short, prior_snapshots, next_snapshots
            FROM viewer_short_career_snapshots
            ORDER BY viewer_id, rank"#,
    )
    .fetch_all(pool);
    let meta_fut = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT last_refreshed_at FROM cheat_analysis_meta WHERE id = 1",
    )
    .fetch_optional(pool);

    let (
        mut entries,
        daily_rows,
        heatmap_rows,
        session_rows,
        short_snapshot_rows,
        last_refreshed_at,
    ) = tokio::try_join!(
        scores_fut,
        daily_fut,
        heatmap_fut,
        sessions_fut,
        short_snapshots_fut,
        meta_fut
    )?;
    for entry in &mut entries {
        entry.attach_evidence();
    }
    let by_viewer: HashMap<i64, usize> = entries
        .iter()
        .enumerate()
        .map(|(i, e)| (e.viewer_id, i))
        .collect();
    let default_hall_indices: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            entry.suspicion_score >= SUSPICIOUS_SCORE_THRESHOLD && entry.days_observed >= 3
        })
        .map(|(i, _)| i)
        .collect();

    let mut daily: HashMap<i64, Vec<DailyPoint>> = HashMap::with_capacity(entries.len());
    for row in daily_rows {
        let viewer_id: i64 = row.try_get("viewer_id")?;
        daily.entry(viewer_id).or_default().push(DailyPoint {
            day: row.try_get("day")?,
            active_seconds: row.try_get("active_seconds")?,
            careers: row.try_get("careers")?,
            fan_gain: row.try_get("fan_gain")?,
            sessions: row.try_get("sessions")?,
            longest_session_sec: row.try_get("longest_session_sec")?,
            distinct_hours: row.try_get("distinct_hours")?,
        });
    }

    let mut heatmap: HashMap<i64, Vec<HeatmapCell>> = HashMap::with_capacity(entries.len());
    for row in heatmap_rows {
        let viewer_id: i64 = row.try_get("viewer_id")?;
        heatmap.entry(viewer_id).or_default().push(HeatmapCell {
            dow: row.try_get("dow")?,
            hour: row.try_get("hour")?,
            active_seconds: row.try_get("active_seconds")?,
            careers: row.try_get("careers")?,
        });
    }

    let mut top_sessions: HashMap<i64, Vec<TopSession>> = HashMap::with_capacity(entries.len());
    for row in session_rows {
        let viewer_id: i64 = row.try_get("viewer_id")?;
        top_sessions
            .entry(viewer_id)
            .or_default()
            .push(top_session_from_row(&row)?);
    }

    let mut short_career_snapshots: HashMap<i64, Vec<ShortCareerSnapshot>> =
        HashMap::with_capacity(entries.len());
    for row in short_snapshot_rows {
        let viewer_id: i64 = row.try_get("viewer_id")?;
        short_career_snapshots
            .entry(viewer_id)
            .or_default()
            .push(short_career_snapshot_from_row(&row)?);
    }

    let entries_count = entries.len();
    install_snapshot(ShameSnapshot {
        entries,
        default_hall_indices,
        by_viewer,
        daily,
        heatmap,
        top_sessions,
        short_career_snapshots,
        last_refreshed_at,
    });

    info!(
        "shame snapshot loaded: {} entries in {} ms",
        entries_count,
        start.elapsed().as_millis()
    );
    Ok(())
}

impl ShameSnapshot {
    fn hall_page(
        &self,
        min_score: i32,
        min_days: i32,
        sort_by: &str,
        query: Option<&str>,
        page: i64,
        limit: i64,
    ) -> HallResponse {
        // Filter
        let query_numeric: Option<i64> = query
            .filter(|q| q.chars().all(|c| c.is_ascii_digit()))
            .and_then(|q| q.parse().ok());
        let query_lower: Option<String> = query
            .filter(|_| query_numeric.is_none())
            .map(|q| q.to_lowercase());

        let mut filtered: Vec<usize> = if sort_by.is_empty()
            && query.is_none()
            && min_score == SUSPICIOUS_SCORE_THRESHOLD
            && min_days == 3
        {
            self.default_hall_indices.clone()
        } else {
            self.entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.suspicion_score >= min_score && e.days_observed >= min_days)
                .filter(|(_, e)| match (&query_numeric, &query_lower) {
                    (Some(id), _) => e.viewer_id == *id || e.circle_id == Some(*id),
                    (_, Some(q)) => {
                        e.trainer_name
                            .as_deref()
                            .map(|n| n.to_lowercase().contains(q))
                            .unwrap_or(false)
                            || e.circle_name
                                .as_deref()
                                .map(|n| n.to_lowercase().contains(q))
                                .unwrap_or(false)
                    }
                    _ => true,
                })
                .map(|(i, _)| i)
                .collect()
        };

        // Sort
        use std::cmp::Ordering;
        let cmp: Box<dyn Fn(&HallEntry, &HallEntry) -> Ordering> = match sort_by {
            "longest_session" | "online_streak" | "max_session" => Box::new(|a, b| {
                b.max_session_seconds
                    .cmp(&a.max_session_seconds)
                    .then(b.suspicion_score.cmp(&a.suspicion_score))
            }),
            "careers_per_hour" => {
                Box::new(|a, b| b.career_rate_last20().total_cmp(&a.career_rate_last20()))
            }
            "avg_careers_per_day" => {
                Box::new(|a, b| b.avg_careers_per_day.total_cmp(&a.avg_careers_per_day))
            }
            "avg_career_length" => Box::new(|a, b| {
                a.avg_career_length_last20_seconds
                    .total_cmp(&b.avg_career_length_last20_seconds)
            }),
            "behavior_change" => Box::new(|a, b| {
                b.behavior_change_score
                    .total_cmp(&a.behavior_change_score)
                    .then(b.fan_gain_spike_ratio.total_cmp(&a.fan_gain_spike_ratio))
            }),
            "short_fan_gain" => Box::new(|a, b| {
                b.short_fan_gain_score
                    .total_cmp(&a.short_fan_gain_score)
                    .then(b.short_high_fan_careers.cmp(&a.short_high_fan_careers))
            }),
            "short_high_fan" => Box::new(|a, b| {
                b.short_high_fan_careers
                    .cmp(&a.short_high_fan_careers)
                    .then(b.suspicion_score.cmp(&a.suspicion_score))
            }),
            "fans_per_minute" => Box::new(|a, b| {
                b.fans_per_active_minute
                    .total_cmp(&a.fans_per_active_minute)
            }),
            "peak_fans_per_minute" => Box::new(|a, b| {
                b.high_fan_rate_windows
                    .cmp(&a.high_fan_rate_windows)
                    .then(b.peak_fans_per_minute.total_cmp(&a.peak_fans_per_minute))
            }),
            "reset_breaks" => Box::new(|a, b| {
                b.reset_break_score
                    .total_cmp(&a.reset_break_score)
                    .then(b.reset_breaks.cmp(&a.reset_breaks))
            }),
            "long_hours" => Box::new(|a, b| {
                b.days_over_20h
                    .cmp(&a.days_over_20h)
                    .then(b.days_over_16h.cmp(&a.days_over_16h))
                    .then(b.max_daily_active_seconds.cmp(&a.max_daily_active_seconds))
            }),
            "probe_score" => Box::new(|a, b| {
                b.probe_score
                    .total_cmp(&a.probe_score)
                    .then(b.suspicion_score.cmp(&a.suspicion_score))
            }),
            "career_quantization" => Box::new(|a, b| {
                b.probe_metrics
                    .career_fan_gain_score
                    .total_cmp(&a.probe_metrics.career_fan_gain_score)
                    .then(b.probe_score.total_cmp(&a.probe_score))
            }),
            "career_regularity" => Box::new(|a, b| {
                b.probe_metrics
                    .career_regularity_score
                    .total_cmp(&a.probe_metrics.career_regularity_score)
                    .then(b.probe_score.total_cmp(&a.probe_score))
            }),
            "login_regularity" => Box::new(|a, b| {
                let a_score = a.probe_metrics.login_regularity_score
                    + a.probe_metrics.post_login_latency_score;
                let b_score = b.probe_metrics.login_regularity_score
                    + b.probe_metrics.post_login_latency_score;
                b_score
                    .total_cmp(&a_score)
                    .then(b.probe_score.total_cmp(&a.probe_score))
            }),
            "zero_idle" => Box::new(|a, b| {
                b.probe_metrics
                    .zero_idle_score
                    .total_cmp(&a.probe_metrics.zero_idle_score)
                    .then(b.probe_score.total_cmp(&a.probe_score))
            }),
            "burst_careers" => Box::new(|a, b| {
                b.probe_metrics
                    .burst_career_score
                    .total_cmp(&a.probe_metrics.burst_career_score)
                    .then(b.probe_score.total_cmp(&a.probe_score))
            }),
            "coactivity" => Box::new(|a, b| {
                b.probe_metrics
                    .coactivity_cluster_score
                    .total_cmp(&a.probe_metrics.coactivity_cluster_score)
                    .then(
                        b.probe_metrics
                            .coactivity_cluster_size
                            .cmp(&a.probe_metrics.coactivity_cluster_size),
                    )
            }),
            "careers" => Box::new(|a, b| b.total_careers.cmp(&a.total_careers)),
            "active_time" => Box::new(|a, b| b.total_active_seconds.cmp(&a.total_active_seconds)),
            _ => Box::new(|a, b| {
                b.suspicion_score
                    .cmp(&a.suspicion_score)
                    .then(b.max_session_seconds.cmp(&a.max_session_seconds))
            }),
        };
        if !(sort_by.is_empty()
            && query.is_none()
            && min_score == SUSPICIOUS_SCORE_THRESHOLD
            && min_days == 3)
        {
            filtered.sort_by(|&i, &j| cmp(&self.entries[i], &self.entries[j]));
        }

        let total = filtered.len() as i64;
        let total_pages = if limit > 0 {
            (total + limit - 1) / limit
        } else {
            0
        };
        let offset = (page * limit).max(0) as usize;
        let end = (offset + limit as usize).min(filtered.len());
        let entries: Vec<HallEntry> = if offset < filtered.len() {
            filtered[offset..end]
                .iter()
                .map(|&i| self.entries[i].clone().for_hall_list())
                .collect()
        } else {
            Vec::new()
        };

        HallResponse {
            entries,
            total,
            page,
            limit,
            total_pages,
            suspicion_score_threshold: SUSPICIOUS_SCORE_THRESHOLD,
            last_refreshed_at: self.last_refreshed_at,
        }
    }

    fn viewer_report(&self, viewer_id: i64, days: usize) -> ViewerReport {
        let score = self
            .by_viewer
            .get(&viewer_id)
            .map(|&i| self.entries[i].clone());
        let daily = self
            .daily
            .get(&viewer_id)
            .map(|v| v.iter().take(days).cloned().collect())
            .unwrap_or_default();
        let heatmap = self.heatmap.get(&viewer_id).cloned().unwrap_or_default();
        let top_sessions = self
            .top_sessions
            .get(&viewer_id)
            .cloned()
            .unwrap_or_default();
        let short_career_snapshots = self
            .short_career_snapshots
            .get(&viewer_id)
            .cloned()
            .unwrap_or_default();
        let short_career_snapshots_total = short_career_snapshots
            .first()
            .map(|row| row.total_count)
            .unwrap_or_default();
        ViewerReport {
            score,
            daily,
            heatmap,
            top_sessions,
            short_career_snapshots,
            short_career_snapshots_total,
            last_refreshed_at: self.last_refreshed_at,
        }
    }
}
