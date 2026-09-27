use super::*;

impl HallEntry {
    pub(super) fn careerRateLast20(&self) -> f64 {
        self.career_rate_breakdown.last_20.careers_per_hour
    }

    pub(super) fn attachEvidence(&mut self) {
        self.evidence = buildEvidenceSummary(self);
    }

    pub(super) fn withEvidence(mut self) -> Self {
        self.attachEvidence();
        self
    }

    pub(super) fn forHallList(mut self) -> Self {
        self.careers_per_active_hour = self.career_rate_breakdown.last_20.careers_per_hour;
        self.career_rate_sample_count = self.career_rate_breakdown.last_20.sample_count;
        self.career_rate_sample_seconds = self.career_rate_breakdown.last_20.sample_seconds;
        self.attachEvidence();
        self
    }
}

fn buildEvidenceSummary(entry: &HallEntry) -> EvidenceSummary {
    let mut reasons = Vec::new();
    let mut caveats = vec![
        "Names and circles can be old. The viewer_id is the account id.".to_string(),
        "These numbers describe the account, not who was playing it.".to_string(),
    ];

    let impossible_short =
        bucketCount(&entry.career_length_buckets, 0) + bucketCount(&entry.career_length_buckets, 1);
    let hard_short = bucketCount(&entry.career_length_buckets, 2);
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
                formatFans(entry.short_career_max_fan_gain)
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
                formatFans(entry.short_career_p95_fan_gain),
                formatDuration(entry.avg_career_length_last20_seconds.round() as i64)
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
                formatFans(entry.recent_fans_per_day)
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
                formatFans(entry.high_fan_rate_total_fan_gain as f64),
                formatDuration(entry.high_fan_rate_total_seconds as i64),
                formatFans(entry.peak_fans_per_minute),
                formatFans(entry.fans_per_active_minute)
            )
        } else {
            format!(
                "active avg {}/min, peak {}/min",
                formatFans(entry.fans_per_active_minute),
                formatFans(entry.peak_fans_per_minute)
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
                formatPercent(probes.career_fan_gain_mode_share),
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
                formatDuration(probes.post_login_latency_median_seconds as i64),
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
                formatDuration(probes.max_zero_idle_active_seconds as i64)
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
                formatPercent(probes.night_active_ratio)
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
                formatDuration(entry.career_rate_sample_seconds)
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
                formatDuration(entry.max_reset_recovery_seconds as i64)
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
                formatDuration(entry.max_daily_active_seconds as i64),
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
            display_value: formatDuration(entry.max_session_seconds as i64),
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

    reasons.sort_by_key(|reason| evidenceRank(reason));
    caveats.sort();
    caveats.dedup();

    let strongest_signal = reasons.first().map(|reason| reason.key.clone());
    let verdict = classifyVerdict(entry, &reasons);
    let summary = summarizeEvidence(entry, &reasons, &verdict);

    EvidenceSummary {
        verdict,
        summary,
        strongest_signal,
        reasons,
        caveats,
    }
}

fn bucketCount(buckets: &[i32], index: usize) -> i32 {
    buckets.get(index).copied().unwrap_or_default()
}

fn evidenceRank(reason: &EvidenceReason) -> i32 {
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

fn classifyVerdict(entry: &HallEntry, reasons: &[EvidenceReason]) -> String {
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

fn summarizeEvidence(entry: &HallEntry, reasons: &[EvidenceReason], verdict: &str) -> String {
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

fn formatDuration(seconds: i64) -> String {
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

fn formatFans(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.0}k", value / 1_000.0)
    } else {
        format!("{:.0}", value)
    }
}

fn formatPercent(value: f64) -> String {
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

    fn entryWithDefaults() -> HallEntry {
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
    fn staleShortBucketsWithoutTrustedShortSamplesAreNotEvidence() {
        let mut entry = entryWithDefaults();
        entry.career_length_buckets[0] = 7;
        entry.career_length_buckets[1] = 8;
        entry.career_length_buckets[2] = 11;

        let summary = buildEvidenceSummary(&entry);

        assert!(!summary
            .reasons
            .iter()
            .any(|reason| reason.key == "career_length_distribution"));
    }

    #[test]
    fn hallListUsesLast20CareerRateFields() {
        let mut entry = entryWithDefaults();
        entry.careers_per_active_hour = 2.0;
        entry.career_rate_sample_count = 100;
        entry.career_rate_sample_seconds = 180_000;
        entry.career_rate_breakdown.last_20 = CareerRateWindow {
            careers_per_hour: 8.5,
            sample_count: 20,
            sample_seconds: 8_470,
        };

        let entry = entry.forHallList();

        assert_eq!(entry.careers_per_active_hour, 8.5);
        assert_eq!(entry.career_rate_sample_count, 20);
        assert_eq!(entry.career_rate_sample_seconds, 8_470);
    }

    #[test]
    fn trustedShortSamplesCanStillSurfaceCareerDistribution() {
        let mut entry = entryWithDefaults();
        entry.total_careers = 20;
        entry.career_length_buckets[0] = 2;
        entry.career_length_buckets[2] = 4;
        entry.short_career_p95_fan_gain = 850_000.0;
        entry.short_career_max_fan_gain = 900_000.0;

        let summary = buildEvidenceSummary(&entry);
        let reason = summary
            .reasons
            .iter()
            .find(|reason| reason.key == "career_length_distribution")
            .unwrap();

        assert_eq!(reason.confidence, "strong");
        assert_eq!(reason.severity, "high");
    }

    #[test]
    fn normalCareerFinishRatesDoNotSurfaceFanRateEvidence() {
        let mut entry = entryWithDefaults();
        entry.peak_fans_per_minute = 60_000.0;
        entry.fans_per_active_minute = 36_000.0;

        let summary = buildEvidenceSummary(&entry);

        assert!(!summary
            .reasons
            .iter()
            .any(|reason| reason.key == "fan_gain_rate"));
    }

    #[test]
    fn extremeAttributedFanRateStillSurfaces() {
        let mut entry = entryWithDefaults();
        entry.peak_fans_per_minute = 320_000.0;
        entry.fans_per_active_minute = 90_000.0;

        let summary = buildEvidenceSummary(&entry);
        let reason = summary
            .reasons
            .iter()
            .find(|reason| reason.key == "fan_gain_rate")
            .unwrap();

        assert_eq!(reason.severity, "high");
        assert_eq!(reason.confidence, "medium");
    }

    #[test]
    fn singlePeakFanRateDoesNotSurfaceWithoutRepetition() {
        let mut entry = entryWithDefaults();
        entry.peak_fans_per_minute = 320_000.0;
        entry.high_fan_rate_windows = 1;

        let summary = buildEvidenceSummary(&entry);

        assert!(!summary
            .reasons
            .iter()
            .any(|reason| reason.key == "fan_gain_rate"));
    }

    #[test]
    fn repeatedPeakFanRateSurfacesWithWindowCount() {
        let mut entry = entryWithDefaults();
        entry.peak_fans_per_minute = 320_000.0;
        entry.high_fan_rate_windows = 3;

        let summary = buildEvidenceSummary(&entry);
        let reason = summary
            .reasons
            .iter()
            .find(|reason| reason.key == "fan_gain_rate")
            .unwrap();

        assert_eq!(reason.severity, "high");
        assert!(reason.display_value.contains("3 fast windows"));
    }

    #[test]
    fn loudProbeMetricsSurfaceAsAutomationEvidence() {
        let mut entry = entryWithDefaults();
        entry.probe_score = 18.0;
        entry.probe_metrics.career_fan_gain_samples = 24;
        entry.probe_metrics.career_fan_gain_mode_share = 0.82;
        entry.probe_metrics.career_fan_gain_cv = 0.08;
        entry.probe_metrics.career_fan_gain_score = 7.5;
        entry.probe_metrics.max_zero_idle_fan_gain_streak = 20;
        entry.probe_metrics.max_zero_idle_active_seconds = 3 * 3600;
        entry.probe_metrics.zero_idle_score = 5.8;
        entry.suspicion_score = 72;

        let summary = buildEvidenceSummary(&entry);

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
    fn arcLikeLowVolumeContextDoesNotSurfaceNoisyReasons() {
        let mut entry = entryWithDefaults();
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

        let summary = buildEvidenceSummary(&entry);

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
