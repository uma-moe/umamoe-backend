use crate::{
    cache,
    cheat_analysis::{CareerRateBreakdown, SuspicionProbeMetrics, SUSPICIOUS_SCORE_THRESHOLD},
    errors::AppError,
};
use serde::{Deserialize, Serialize};
use sqlx::types::Json as SqlJson;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

mod evidence;
mod snapshot;
use snapshot::ensureSnapshot;
pub use snapshot::rebuildSnapshot;

include!("../types/shame.rs");

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

pub(crate) async fn hall(pool: &PgPool, params: HallParams) -> Result<HallResponse, AppError> {
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
    if let Some(snapshot) = ensureSnapshot(pool).await {
        let response = snapshot.hallPage(
            min_score,
            min_days,
            sort_by,
            query_filter.as_deref(),
            page,
            limit,
        );
        return Ok(response);
    }

    // Cold-start fallback: no snapshot yet (first ~90s after process
    // start, before the initial rebuild). Use the legacy SQL path so the
    // endpoint still works.
    crate::cheat_analysis::verifyRateDiagnosticColumns(pool)
        .await
        .map_err(|e| {
            AppError::DatabaseError(format!(
                "failed to verify suspicious-activity rate columns: {e}"
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
        return Ok(cached);
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
    let total = count_q.fetch_one(pool).await?;

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
    let entries = list_q.bind(limit).bind(offset).fetch_all(pool).await?;
    let entries: Vec<HallEntry> = entries.into_iter().map(HallEntry::forHallList).collect();

    let last_refreshed_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT last_refreshed_at FROM cheat_analysis_meta WHERE id = 1")
            .fetch_optional(pool)
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
    Ok(response)
}

fn topSessionFromRow(row: &PgRow) -> Result<TopSession, sqlx::Error> {
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

fn shortCareerSnapshotFromRow(row: &PgRow) -> Result<ShortCareerSnapshot, sqlx::Error> {
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

pub(crate) async fn viewerReport(
    pool: &PgPool,
    viewer_id: i64,
    params: ViewerReportParams,
) -> Result<ViewerReport, AppError> {
    let days = params.days.unwrap_or(60).clamp(1, 365) as usize;

    // Fast path: serve from the in-memory snapshot.
    if let Some(snapshot) = ensureSnapshot(pool).await {
        return Ok(snapshot.viewerReport(viewer_id, days));
    }

    // Cold-start fallback: legacy SQL path.
    crate::cheat_analysis::verifyRateDiagnosticColumns(pool)
        .await
        .map_err(|e| {
            AppError::DatabaseError(format!(
                "failed to verify suspicious-activity rate columns: {e}"
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
        .fetch_optional(pool)
        .await?
        .map(HallEntry::withEvidence);

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
    .fetch_all(pool)
    .await?;

    let heatmap = sqlx::query_as::<_, HeatmapCell>(
        r#"SELECT dow, hour, active_seconds, careers
           FROM viewer_activity_heatmap
           WHERE viewer_id = $1
           ORDER BY dow, hour"#,
    )
    .bind(viewer_id)
    .fetch_all(pool)
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
    .fetch_all(pool)
    .await?;
    let top_sessions: Vec<TopSession> = top_session_rows
        .into_iter()
        .map(|row| topSessionFromRow(&row))
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
    .fetch_all(pool)
    .await?;
    let short_career_snapshots: Vec<ShortCareerSnapshot> = short_career_snapshot_rows
        .into_iter()
        .map(|row| shortCareerSnapshotFromRow(&row))
        .collect::<Result<_, _>>()?;
    let short_career_snapshots_total = short_career_snapshots
        .first()
        .map(|row| row.total_count)
        .unwrap_or_default();

    let last_refreshed_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT last_refreshed_at FROM cheat_analysis_meta WHERE id = 1")
            .fetch_optional(pool)
            .await?;

    Ok(ViewerReport {
        score,
        daily,
        heatmap,
        top_sessions,
        short_career_snapshots,
        short_career_snapshots_total,
        last_refreshed_at,
    })
}
