use super::*;

pub(super) fn shortFanGainSeverity(seconds: u32, fan_gain_per_career: f64) -> f64 {
    let safe_seconds = seconds.max(1) as f64;
    let duration_multiplier = (SHORT_HIGH_FAN_MAX_SECONDS as f64 / safe_seconds)
        .clamp(1.0, SHORT_FAN_GAIN_MAX_DURATION_MULTIPLIER);
    let fan_multiplier = (fan_gain_per_career / SHORT_FAN_GAIN_BASE_FANS).clamp(0.25, 3.0);
    duration_multiplier * fan_multiplier
}

pub(super) fn averageActiveSecondsPerObservedDay(
    total_active_seconds: i64,
    days_observed: i32,
) -> f64 {
    if days_observed <= 0 {
        0.0
    } else {
        total_active_seconds as f64 / days_observed as f64
    }
}

pub(super) fn avgCareersPerObservedDay(total_careers: u32, days_observed: i32) -> f64 {
    if days_observed <= 0 {
        0.0
    } else {
        total_careers as f64 / days_observed as f64
    }
}

pub(super) fn careerRateBreakdown(
    samples: &[CareerRateSample],
    reference_at: DateTime<Utc>,
) -> CareerRateBreakdown {
    let bounded: Vec<&CareerRateSample> = samples
        .iter()
        .filter(|sample| sample.seconds <= CAREER_RATE_MAX_SAMPLE_SECONDS)
        .collect();
    let last_20: Vec<&CareerRateSample> = bounded.iter().rev().take(20).copied().collect();

    CareerRateBreakdown {
        all: careerRateWindowRobust(bounded.iter().copied()),
        last_30d: careerRateWindowForDays(&bounded, reference_at, 30),
        last_7d: careerRateWindowForDays(&bounded, reference_at, 7),
        last_3d: careerRateWindowForDays(&bounded, reference_at, 3),
        last_20: careerRateWindowRobust(last_20.iter().copied()),
    }
}

pub(super) fn careerRateWindowForDays(
    samples: &[&CareerRateSample],
    reference_at: DateTime<Utc>,
    days: i64,
) -> CareerRateWindow {
    let cutoff = reference_at - chrono::Duration::days(days);
    careerRateWindowRobust(
        samples
            .iter()
            .copied()
            .filter(|sample| sample.finished_at >= cutoff),
    )
}

pub(super) fn careerRateWindow<'a>(
    samples: impl IntoIterator<Item = &'a CareerRateSample>,
) -> CareerRateWindow {
    let mut sample_count = 0i32;
    let mut sample_seconds = 0i64;
    for sample in samples {
        sample_count = sample_count.saturating_add(1);
        sample_seconds = sample_seconds.saturating_add(sample.seconds as i64);
    }
    let careers_per_hour = if sample_seconds > 0 {
        sample_count as f64 / (sample_seconds as f64 / 3600.0)
    } else {
        0.0
    };
    CareerRateWindow {
        careers_per_hour,
        sample_count,
        sample_seconds,
    }
}

pub(super) fn careerRateWindowRobust<'a>(
    samples: impl IntoIterator<Item = &'a CareerRateSample>,
) -> CareerRateWindow {
    let samples: Vec<&CareerRateSample> = samples.into_iter().collect();
    if samples.len() < CAREER_RATE_ROBUST_MIN_SAMPLES {
        return careerRateWindow(samples.iter().copied());
    }

    let mut sorted_seconds: Vec<u32> = samples.iter().map(|sample| sample.seconds).collect();
    sorted_seconds.sort_unstable();

    let q1 = sorted_seconds[sorted_seconds.len() / 4] as f64;
    let median = sorted_seconds[sorted_seconds.len() / 2] as f64;
    let q3 = sorted_seconds[(sorted_seconds.len() * 3) / 4] as f64;
    let iqr = q3 - q1;
    let (lower, upper) = if iqr > 0.0 {
        ((q1 - 1.5 * iqr).max(1.0), q3 + 1.5 * iqr)
    } else {
        (
            (median * CAREER_RATE_ZERO_IQR_LOWER_MULTIPLIER).max(1.0),
            median * CAREER_RATE_ZERO_IQR_UPPER_MULTIPLIER,
        )
    };

    let mut sample_count = 0i32;
    let mut sample_seconds = 0i64;
    for sample in samples {
        sample_count = sample_count.saturating_add(1);
        sample_seconds = sample_seconds
            .saturating_add((sample.seconds as f64).clamp(lower, upper).round() as i64);
    }

    let careers_per_hour = if sample_seconds > 0 {
        sample_count as f64 / (sample_seconds as f64 / 3600.0)
    } else {
        0.0
    };

    CareerRateWindow {
        careers_per_hour,
        sample_count,
        sample_seconds,
    }
}

pub(super) fn repeatedHighFanRateFactor(windows: u32) -> f64 {
    if windows < REPEATED_HIGH_FAN_RATE_MIN_WINDOWS {
        0.0
    } else {
        (windows as f64 / REPEATED_HIGH_FAN_RATE_FULL_WINDOWS as f64).clamp(0.0, 1.0)
    }
}

pub(super) fn coverageScheduleScore(
    distinct_weekly_hour_buckets: i16,
    avg_active_seconds_per_observed_day: f64,
) -> f64 {
    let saturation = (distinct_weekly_hour_buckets as f64 / 168.0).clamp(0.0, 1.0);
    let volume = (avg_active_seconds_per_observed_day
        / HEATMAP_COVERAGE_FULL_VOLUME_ACTIVE_SECONDS_PER_DAY)
        .clamp(0.0, 1.0);
    saturation * volume * HEATMAP_COVERAGE_SCORE_MAX
}

pub(super) fn longHoursScore(
    max_daily_active_seconds: i32,
    avg_active_seconds_per_observed_day: f64,
    days_over_16h: i32,
    days_over_20h: i32,
    days_observed: i32,
) -> f64 {
    if days_observed <= 0 {
        return 0.0;
    }

    let max_day_score = rangedScore(
        max_daily_active_seconds as f64,
        12.0 * 3600.0,
        20.0 * 3600.0,
        LONG_HOURS_MAX_DAY_SCORE_MAX,
    );
    let avg_day_score = rangedScore(
        avg_active_seconds_per_observed_day,
        8.0 * 3600.0,
        16.0 * 3600.0,
        LONG_HOURS_AVG_DAY_SCORE_MAX,
    );
    let over_16h_score =
        ((days_over_16h.max(0) as f64) / 5.0).clamp(0.0, 1.0) * LONG_HOURS_DAYS_OVER_16H_SCORE_MAX;
    let over_20h_score =
        ((days_over_20h.max(0) as f64) / 2.0).clamp(0.0, 1.0) * LONG_HOURS_DAYS_OVER_20H_SCORE_MAX;

    (max_day_score + avg_day_score + over_16h_score + over_20h_score).min(LONG_HOURS_SCORE_MAX)
}

pub(super) fn resetBreakScore(
    reset_breaks: u32,
    reset_recovery_windows: u32,
    avg_active_seconds_per_observed_day: f64,
    max_daily_active_seconds: i32,
    days_over_16h: i32,
) -> f64 {
    if reset_breaks == 0 || reset_recovery_windows < 3 {
        return 0.0;
    }

    let break_ratio = reset_breaks as f64 / reset_recovery_windows.max(1) as f64;
    let count_score = (reset_breaks as f64 / 3.0).clamp(0.0, 1.0) * 6.0;
    let ratio_score = (break_ratio / 0.5).clamp(0.0, 1.0) * 4.0;
    let volume_context = (rangedScore(
        avg_active_seconds_per_observed_day,
        4.0 * 3600.0,
        10.0 * 3600.0,
        0.55,
    ) + rangedScore(
        max_daily_active_seconds as f64,
        10.0 * 3600.0,
        18.0 * 3600.0,
        0.25,
    ) + rangedScore(days_over_16h as f64, 1.0, 5.0, 0.20))
    .clamp(0.0, 1.0);

    if volume_context <= 0.0 {
        return 0.0;
    }

    (count_score + ratio_score).min(RESET_BREAK_SCORE_MAX) * volume_context
}

pub(super) fn careerFanGainScore(samples: usize, mode_share: f64, cv: f64) -> f64 {
    if samples < MIN_PATTERN_SAMPLES {
        return 0.0;
    }

    let mode_score = rangedScore(mode_share, 0.35, 0.70, 5.0);
    let cv_score = lowValueScore(cv, 0.35, 0.10, 3.0);
    (mode_score + cv_score).min(CAREER_FAN_GAIN_SCORE_MAX)
}

pub(super) fn careerRegularityScore(
    rhythm_samples: usize,
    length_samples: usize,
    rhythm_cv: f64,
    length_cv: f64,
) -> f64 {
    let rhythm_score = if rhythm_samples >= MIN_PATTERN_SAMPLES {
        lowValueScore(rhythm_cv, 0.45, 0.12, 4.0)
    } else {
        0.0
    };
    let length_score = if length_samples >= MIN_PATTERN_SAMPLES {
        lowValueScore(length_cv, 0.40, 0.10, 4.0)
    } else {
        0.0
    };
    (rhythm_score + length_score).min(CAREER_REGULARITY_SCORE_MAX)
}

pub(super) fn loginRegularityScore(samples: usize, cv: f64, mode_share: f64) -> f64 {
    if samples < 12 {
        return 0.0;
    }

    let cv_score = lowValueScore(cv, 0.30, 0.10, 2.5);
    let mode_score = rangedScore(mode_share, 0.55, 0.80, 2.5);
    (cv_score + mode_score).min(LOGIN_REGULARITY_SCORE_MAX)
}

pub(super) fn postLoginLatencyScore(samples: usize, median_seconds: i32, cv: f64) -> f64 {
    if samples < 5 || median_seconds <= 0 || median_seconds > 45 * 60 {
        return 0.0;
    }

    let consistency_score = lowValueScore(cv, 0.55, 0.15, 3.0);
    let speed_score = lowValueScore(median_seconds as f64, 45.0 * 60.0, 12.0 * 60.0, 2.0);
    (consistency_score + speed_score).min(POST_LOGIN_LATENCY_SCORE_MAX)
}

pub(super) fn zeroIdleScore(max_streak: u32, max_active_seconds: u32) -> f64 {
    let streak_score = rangedScore(max_streak as f64, 6.0, 18.0, 3.0);
    let active_score = rangedScore(max_active_seconds as f64, 45.0 * 60.0, 3.0 * 3600.0, 3.0);
    (streak_score + active_score).min(ZERO_IDLE_SCORE_MAX)
}

pub(super) fn scheduleShapeScore(
    weekday_weekend_similarity: f64,
    hourly_entropy: f64,
    night_active_ratio: f64,
    avg_active_seconds_per_observed_day: f64,
    days_observed: i32,
) -> f64 {
    if days_observed < 7 || avg_active_seconds_per_observed_day < 4.0 * 3600.0 {
        return 0.0;
    }

    let similarity_score = rangedScore(weekday_weekend_similarity, 0.86, 0.98, 2.0);
    let entropy_score = rangedScore(hourly_entropy, 0.72, 0.94, 2.0);
    let night_score = rangedScore(night_active_ratio, 0.18, 0.40, 2.0);
    (similarity_score + entropy_score + night_score).min(SCHEDULE_SHAPE_SCORE_MAX)
}

pub(super) fn burstCareerScore(max_careers_30m: u32, burst_windows: u32) -> f64 {
    let peak_score = rangedScore(max_careers_30m as f64, 3.0, 6.0, 3.0);
    let repeat_score = rangedScore(burst_windows as f64, 2.0, 10.0, 2.0);
    (peak_score + repeat_score).min(BURST_CAREER_SCORE_MAX)
}

pub(super) fn serviceGapResumeScore(
    events: u32,
    avg_active_seconds_per_observed_day: f64,
    days_observed: i32,
) -> f64 {
    if days_observed < 14 || avg_active_seconds_per_observed_day < 6.0 * 3600.0 {
        return 0.0;
    }

    let volume_context = rangedScore(
        avg_active_seconds_per_observed_day,
        6.0 * 3600.0,
        12.0 * 3600.0,
        1.0,
    );
    rangedScore(events as f64, 3.0, 12.0, SERVICE_GAP_RESUME_SCORE_MAX) * volume_context
}

pub(super) fn circleChurnScore(distinct_circles_seen: i32, days_observed: i32) -> f64 {
    if days_observed < 7 {
        return 0.0;
    }
    rangedScore(
        distinct_circles_seen as f64,
        4.0,
        12.0,
        CIRCLE_CHURN_SCORE_MAX,
    )
}

pub(super) fn probeMetricsTotal(metrics: &SuspicionProbeMetrics) -> f64 {
    metrics.career_fan_gain_score
        + metrics.career_regularity_score
        + metrics.login_regularity_score
        + metrics.post_login_latency_score
        + metrics.zero_idle_score
        + metrics.schedule_shape_score
        + metrics.burst_career_score
        + metrics.service_gap_resume_score
        + metrics.circle_churn_score
        + metrics.coactivity_cluster_score
}

pub(super) fn lowValueScore(value: f64, start: f64, full: f64, max_score: f64) -> f64 {
    if start <= full || max_score <= 0.0 {
        return 0.0;
    }
    ((start - value) / (start - full)).clamp(0.0, 1.0) * max_score
}

pub(super) fn rangedScore(value: f64, start: f64, full: f64, max_score: f64) -> f64 {
    if full <= start || max_score <= 0.0 {
        return 0.0;
    }

    ((value - start) / (full - start)).clamp(0.0, 1.0) * max_score
}

pub(super) fn is247Schedule(
    distinct_weekly_hour_buckets: i16,
    avg_active_seconds_per_observed_day: f64,
    days_observed: i32,
) -> bool {
    distinct_weekly_hour_buckets > 140
        && days_observed >= 14
        && avg_active_seconds_per_observed_day >= FLAG_247_MIN_AVG_ACTIVE_SECONDS_PER_DAY
}

pub(super) fn normalizedRateScore(value: f64, full_scale: f64, max_score: f64) -> f64 {
    if full_scale <= 0.0 {
        0.0
    } else {
        (value / full_scale).clamp(0.0, 1.0) * max_score
    }
}

pub(super) fn observedFanRatePerMinute(fan_delta: i64, active_seconds: u32) -> f64 {
    if fan_delta <= 0 || active_seconds == 0 {
        return 0.0;
    }

    let denominator_seconds = active_seconds.max(60) as f64;
    fan_delta as f64 * 60.0 / denominator_seconds
}

pub(super) fn coefficientOfVariation(values: &[u32]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().map(|&v| v as f64).sum::<f64>() / values.len() as f64;
    if mean <= 0.0 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|&v| {
            let delta = v as f64 - mean;
            delta * delta
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt() / mean
}

pub(super) fn modeShareRounded(values: &[u32], bucket_size: u32) -> f64 {
    if values.is_empty() || bucket_size == 0 {
        return 0.0;
    }

    let mut buckets: HashMap<u32, u32> = HashMap::new();
    for &value in values {
        let bucket =
            (value.saturating_add(bucket_size / 2) / bucket_size).saturating_mul(bucket_size);
        *buckets.entry(bucket).or_default() += 1;
    }
    let max_count = buckets.values().copied().max().unwrap_or(0);
    max_count as f64 / values.len() as f64
}

pub(super) fn medianU32(values: &[u32]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    percentileSorted(&sorted, 0.50)
}

pub(super) fn scheduleShapeMetrics(
    heatmap_active: &HeatmapBuckets,
    total_active_seconds: i64,
) -> ScheduleShapeMetrics {
    let mut hour_totals = [0u64; 24];
    let mut weekday_hours = [0u64; 24];
    let mut weekend_hours = [0u64; 24];
    let mut night_active_seconds: u64 = 0;

    for dow in 0usize..7 {
        for hour in 0usize..24 {
            let idx = dow * 24 + hour;
            let active = heatmap_active.get(idx) as u64;
            hour_totals[hour] = hour_totals[hour].saturating_add(active);
            if dow == 0 || dow == 6 {
                weekend_hours[hour] = weekend_hours[hour].saturating_add(active);
            } else {
                weekday_hours[hour] = weekday_hours[hour].saturating_add(active);
            }
            if (2..6).contains(&hour) {
                night_active_seconds = night_active_seconds.saturating_add(active);
            }
        }
    }

    let weekday_weekend_similarity = cosineSimilarity(&weekday_hours, &weekend_hours);
    let hourly_entropy = normalizedEntropy(&hour_totals);
    let night_active_ratio = if total_active_seconds > 0 {
        night_active_seconds as f64 / total_active_seconds as f64
    } else {
        0.0
    };

    ScheduleShapeMetrics {
        weekday_weekend_similarity,
        hourly_entropy,
        night_active_ratio,
        night_active_seconds: night_active_seconds.min(i64::MAX as u64) as i64,
    }
}

pub(super) fn cosineSimilarity(a: &[u64; 24], b: &[u64; 24]) -> f64 {
    let mut dot = 0.0;
    let mut a_norm = 0.0;
    let mut b_norm = 0.0;
    for i in 0..24 {
        let av = a[i] as f64;
        let bv = b[i] as f64;
        dot += av * bv;
        a_norm += av * av;
        b_norm += bv * bv;
    }
    if a_norm <= 0.0 || b_norm <= 0.0 {
        0.0
    } else {
        dot / (a_norm.sqrt() * b_norm.sqrt())
    }
}

pub(super) fn normalizedEntropy(values: &[u64; 24]) -> f64 {
    let total: u64 = values.iter().sum();
    if total == 0 {
        return 0.0;
    }
    let entropy = values.iter().fold(0.0, |acc, &value| {
        if value == 0 {
            acc
        } else {
            let p = value as f64 / total as f64;
            acc - p * p.ln()
        }
    });
    (entropy / (24.0f64).ln()).clamp(0.0, 1.0)
}

pub(super) fn coactivityFingerprint(heatmap_active: &HeatmapBuckets) -> u64 {
    let mut fingerprint = 0u64;
    for dow in 0usize..7 {
        for block in 0usize..8 {
            let mut active = 0u32;
            for hour in (block * 3)..(block * 3 + 3) {
                active = active.saturating_add(heatmap_active.get(dow * 24 + hour));
            }
            if active >= 10 * 60 {
                fingerprint |= 1u64 << (dow * 8 + block);
            }
        }
    }
    fingerprint
}

pub(super) fn applyCoactivityClusters(rows: &mut [ScoreRow]) {
    let mut clusters: HashMap<(Option<i64>, u64), u32> = HashMap::new();
    for row in rows.iter() {
        if row.coactivity_fingerprint != 0
            && row.days_observed >= 7
            && row.total_active_seconds >= 20 * 3600
        {
            *clusters
                .entry((row.circle_id, row.coactivity_fingerprint))
                .or_default() += 1;
        }
    }

    for row in rows {
        let cluster_size = clusters
            .get(&(row.circle_id, row.coactivity_fingerprint))
            .copied()
            .unwrap_or(0);
        if cluster_size < 3 {
            continue;
        }

        let previous_contribution = row.probe_score.min(PROBE_SCORE_CONTRIBUTION_MAX);
        let score = rangedScore(cluster_size as f64, 3.0, 8.0, COACTIVITY_CLUSTER_SCORE_MAX);
        row.probe_metrics.coactivity_cluster_size = cluster_size as i32;
        row.probe_metrics.coactivity_cluster_score = score;
        row.probe_score = probeMetricsTotal(&row.probe_metrics);
        let new_contribution = row.probe_score.min(PROBE_SCORE_CONTRIBUTION_MAX);
        let delta = (new_contribution - previous_contribution).round() as i32;
        row.suspicion_score = row.suspicion_score.saturating_add(delta).min(100);
    }
}

pub(super) fn fanGainStats(values: &[u32]) -> FanGainStats {
    if values.is_empty() {
        return FanGainStats {
            avg: 0.0,
            p50: 0.0,
            p90: 0.0,
            p95: 0.0,
            max: 0.0,
        };
    }

    let sum: u64 = values.iter().map(|&value| value as u64).sum();
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    FanGainStats {
        avg: sum as f64 / values.len() as f64,
        p50: percentileSorted(&sorted, 0.50),
        p90: percentileSorted(&sorted, 0.90),
        p95: percentileSorted(&sorted, 0.95),
        max: sorted.last().copied().unwrap_or(0) as f64,
    }
}

pub(super) fn percentileSorted(sorted: &[u32], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let max_idx = sorted.len() - 1;
    let idx = ((max_idx as f64) * percentile).round() as usize;
    sorted[idx.min(max_idx)] as f64
}

pub(super) fn behaviorChangeStats(daily: &HashMap<NaiveDate, DailyAccum>) -> BehaviorChangeStats {
    let Some(latest_day) = daily.keys().max().copied() else {
        return BehaviorChangeStats {
            recent_fan_gain_3d: 0,
            baseline_fan_gain_14d: 0,
            recent_fans_per_day: 0.0,
            baseline_fans_per_day: 0.0,
            fan_gain_spike_ratio: 0.0,
            behavior_change_score: 0.0,
        };
    };

    let mut recent_fan_gain: u64 = 0;
    let mut recent_days: u32 = 0;
    let mut baseline_fan_gain: u64 = 0;
    let mut baseline_days: u32 = 0;

    for (day, bucket) in daily {
        let age_days = latest_day.signed_duration_since(*day).num_days();
        if (0..RECENT_BEHAVIOR_DAYS).contains(&age_days) {
            recent_fan_gain = recent_fan_gain.saturating_add(bucket.fan_gain);
            recent_days += 1;
        } else if (RECENT_BEHAVIOR_DAYS..RECENT_BEHAVIOR_DAYS + BASELINE_BEHAVIOR_DAYS)
            .contains(&age_days)
        {
            baseline_fan_gain = baseline_fan_gain.saturating_add(bucket.fan_gain);
            baseline_days += 1;
        }
    }

    let recent_fans_per_day = if recent_days > 0 {
        recent_fan_gain as f64 / recent_days as f64
    } else {
        0.0
    };
    let baseline_fans_per_day = if baseline_days > 0 {
        baseline_fan_gain as f64 / baseline_days as f64
    } else {
        0.0
    };
    let fan_gain_spike_ratio = if recent_days > 0 && baseline_days >= 3 {
        recent_fans_per_day / baseline_fans_per_day.max(BEHAVIOR_BASELINE_FAN_FLOOR)
    } else {
        0.0
    };
    let behavior_change_score = if fan_gain_spike_ratio >= 1.5 && recent_fans_per_day >= 2_000_000.0
    {
        let ratio_score = ((fan_gain_spike_ratio - 1.5) / 3.5).clamp(0.0, 1.0) * 12.0;
        let volume_score = (recent_fans_per_day / 10_000_000.0).clamp(0.0, 1.0) * 8.0;
        ratio_score + volume_score
    } else {
        0.0
    };

    BehaviorChangeStats {
        recent_fan_gain_3d: recent_fan_gain.min(i64::MAX as u64) as i64,
        baseline_fan_gain_14d: baseline_fan_gain.min(i64::MAX as u64) as i64,
        recent_fans_per_day,
        baseline_fans_per_day,
        fan_gain_spike_ratio,
        behavior_change_score,
    }
}
