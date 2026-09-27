use super::*;

fn snapshotSlot() -> &'static RwLock<Option<Arc<ShameSnapshot>>> {
    static SLOT: OnceLock<RwLock<Option<Arc<ShameSnapshot>>>> = OnceLock::new();
    SLOT.get_or_init(|| RwLock::new(None))
}

fn currentSnapshot() -> Option<Arc<ShameSnapshot>> {
    snapshotSlot().read().ok().and_then(|g| g.clone())
}

fn installSnapshot(snapshot: ShameSnapshot) {
    if let Ok(mut g) = snapshotSlot().write() {
        *g = Some(Arc::new(snapshot));
    }
}

pub(super) async fn ensureSnapshot(pool: &PgPool) -> Option<Arc<ShameSnapshot>> {
    if let Some(snapshot) = currentSnapshot() {
        return Some(snapshot);
    }

    match rebuildSnapshot(pool).await {
        Ok(()) => currentSnapshot(),
        Err(err) => {
            warn!("failed to build shame snapshot on demand: {}", err);
            None
        }
    }
}

/// Rebuild the in-memory snapshot from the freshly published aggregate
/// tables. Called from `cheat_analysis::run_full_rebuild` after commit.
pub async fn rebuildSnapshot(pool: &PgPool) -> anyhow::Result<()> {
    let start = Instant::now();

    crate::cheat_analysis::verifyRateDiagnosticColumns(pool).await?;

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
        entry.attachEvidence();
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
            .push(topSessionFromRow(&row)?);
    }

    let mut short_career_snapshots: HashMap<i64, Vec<ShortCareerSnapshot>> =
        HashMap::with_capacity(entries.len());
    for row in short_snapshot_rows {
        let viewer_id: i64 = row.try_get("viewer_id")?;
        short_career_snapshots
            .entry(viewer_id)
            .or_default()
            .push(shortCareerSnapshotFromRow(&row)?);
    }

    let entries_count = entries.len();
    installSnapshot(ShameSnapshot {
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
    pub(super) fn hallPage(
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
                Box::new(|a, b| b.careerRateLast20().total_cmp(&a.careerRateLast20()))
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
                .map(|&i| self.entries[i].clone().forHallList())
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

    pub(super) fn viewerReport(&self, viewer_id: i64, days: usize) -> ViewerReport {
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
