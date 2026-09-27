//! Suspicious-activity analysis pipeline.
//!
//! Reads `circle_member_fan_snapshots`, streams every snapshot row in
//! `(snapshot_time, id)` order, unnests the parallel `viewer_ids / fans /
//! last_login_times` arrays, and folds per-viewer state in memory:
//!
//!   * Per-day buckets (UTC): active seconds, careers, fan gain,
//!     observed fan-gain sessions started, longest session, distinct hours
//!     touched.
//!   * Weekly heatmap (dow × hour, UTC).
//!   * Overall counters used for the suspicion score and the 5 boolean
//!     flags (no-sleep, extreme-session, inhuman-career-rate, 24/7,
//!     marathon).
//!   * Top-10 playtime days per viewer, with observed fan-gain windows nested
//!     inside each day (for the `/api/v4/shame/viewer/:id` report).
//!
//! Detection constants:
//!   * Career-finished signal: `fan_delta >= 100_000`.
//!   * Estimated careers per transition: `floor(fan_delta / 700_000)`,
//!     minimum 1 when the threshold is crossed. This is intentionally
//!     conservative because a normal high-fan run can exceed 1M fans.
//!   * Active-seconds attribution uses observed time when we have it and a
//!     conservative floor when a career finish proves activity happened
//!     but the start was not observed. Complete career intervals use the
//!     wall-clock time between consecutive career-finish snapshots in the
//!     same tight observation window. Isolated career finishes credit a
//!     minimum plausible 10 min per counted career instead of only the
//!     final polling gap; otherwise sparse polling makes ordinary careers
//!     look like 5 min completions. Other tight fan/login transitions are
//!     capped at 15 min. Long service gaps contribute zero seconds — we
//!     don't try to back-fill activity across data outages.
//!   * A *session* is an observed fan-gain window. It opens on the first
//!     tight snapshot gap with fan growth, stays open across short idle
//!     gaps, and closes after a long no-gain stretch, a JST reset, a long
//!     observation gap, or end of data. `last_login_time` is deliberately
//!     not used as a session boundary because it can carry polling/refresh
//!     artifacts. Sessions with no tight observed fan gain are dropped.
//!     `active_seconds` uses the same conservative active-time attribution
//!     as the daily/total aggregates, and `idle_seconds` is the session
//!     duration minus that active time.
//!   * Career-length buckets are conservative display estimates for
//!     single-career finishes. Trusted samples come from the gap between
//!     consecutive career-finish snapshots when every adjacent snapshot
//!     since the previous finish had `fan_delta > 0` and a tight gap —
//!     i.e. we actually observed continuous career chaining. Isolated
//!     single-career finishes use the later of the previous career finish
//!     or current `last_login_time`, but are floored to 10 min so sparse
//!     polling plus a fresh login does not manufacture fake 5-10 minute
//!     runs. The short/high-fan automation signal only uses trusted chained
//!     samples.
//!
//! Output is published in viewer batches: each batch deletes and reinserts
//! only that viewer slice inside a short transaction, so the previous report
//! stays readable while the new rebuild gradually replaces it. Bulk INSERTs
//! still use UNNEST for throughput.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::time::Instant;

mod scoring;
mod storage;
use scoring::*;
use storage::*;

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Tunable detection constants
// ---------------------------------------------------------------------------

include!("../types/cheat_analysis.rs");

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub async fn verifyRateDiagnosticColumns(pool: &PgPool) -> anyhow::Result<()> {
    const REQUIRED_COLUMNS: &[&str] = &[
        "career_rate_sample_count",
        "career_rate_sample_seconds",
        "avg_careers_per_day",
        "career_rate_breakdown",
        "high_fan_rate_windows",
        "high_fan_rate_total_fan_gain",
        "high_fan_rate_total_seconds",
    ];
    let present: i64 = sqlx::query_scalar(VERIFY_RATE_DIAGNOSTIC_COLUMNS_SQL)
        .bind(REQUIRED_COLUMNS)
        .fetch_one(pool)
        .await?;
    anyhow::ensure!(
        present == REQUIRED_COLUMNS.len() as i64,
        "viewer_suspicion_scores is missing rate diagnostic columns; run database migrations"
    );

    Ok(())
}

/// Run a full rebuild of the cheat-analysis aggregates.
pub async fn runFullRebuild(pool: &PgPool) -> anyhow::Result<RebuildStats> {
    let start = Instant::now();

    verifyRateDiagnosticColumns(pool).await?;

    let max_snapshot_id: Option<i64> =
        sqlx::query_scalar("SELECT MAX(id) FROM circle_member_fan_snapshots")
            .fetch_one(pool)
            .await?;
    let Some(max_snapshot_id) = max_snapshot_id else {
        info!("suspicious-activity analysis: no snapshots to process");
        return Ok(RebuildStats {
            snapshots_processed: 0,
            viewers_scored: 0,
            last_snapshot_id: 0,
            duration_ms: start.elapsed().as_millis() as i64,
        });
    };

    info!(
        "suspicious-activity analysis: starting full rebuild up to snapshot id {}",
        max_snapshot_id
    );

    // --- Stream snapshots and fold per-viewer state in memory ---------------
    let stream_start = Instant::now();
    let mut snapshots_processed: i64 = 0;
    let mut viewers: HashMap<i64, ViewerAccum> = HashMap::with_capacity(50_000);

    let mut rows = sqlx::query(
        "SELECT id, circle_id, snapshot_time, viewer_ids, fans, \
            ARRAY( \
                SELECT login_text::timestamp AT TIME ZONE 'UTC' \
                FROM unnest(last_login_times) AS login(login_text) \
            ) AS last_login_times \
         FROM circle_member_fan_snapshots \
         WHERE id <= $1 \
         ORDER BY snapshot_time, id",
    )
    .bind(max_snapshot_id)
    .fetch(pool);

    while let Some(row) = rows.try_next().await? {
        let snapshot_id: i64 = row.try_get("id")?;
        let circle_id: i64 = row.try_get("circle_id")?;
        let snapshot_time: DateTime<Utc> = row.try_get("snapshot_time")?;
        let viewer_ids: Vec<i64> = row.try_get("viewer_ids")?;
        let fans: Vec<i64> = row.try_get("fans")?;
        let logins: Vec<DateTime<Utc>> = row.try_get("last_login_times")?;

        let n = viewer_ids.len().min(fans.len()).min(logins.len());
        for i in 0..n {
            let acc = viewers.entry(viewer_ids[i]).or_default();
            acc.processEvent(snapshot_id, circle_id, snapshot_time, fans[i], logins[i]);
        }
        snapshots_processed += 1;

        // Periodic progress log so a multi-minute scan isn't silent.
        if snapshots_processed % 100_000 == 0 {
            info!(
                "suspicious-activity analysis: {} snapshots streamed in {:.1}s ({} viewers held)",
                snapshots_processed,
                stream_start.elapsed().as_secs_f64(),
                viewers.len()
            );
        }
    }

    info!(
        "suspicious-activity analysis: streamed {} snapshot rows ({} viewers) in {:.1}s",
        snapshots_processed,
        viewers.len(),
        stream_start.elapsed().as_secs_f64()
    );

    // Close any sessions/streaks left open at the end of the data.
    for acc in viewers.values_mut() {
        acc.finalize();
    }

    // Materialize output row sets.
    let mut daily_rows: Vec<DailyRow> = Vec::with_capacity(viewers.len() * 30);
    let mut heatmap_rows: Vec<HeatmapRow> = Vec::with_capacity(viewers.len() * 50);
    let mut score_rows: Vec<ScoreRow> = Vec::with_capacity(viewers.len());
    let mut session_rows: Vec<SessionRow> =
        Vec::with_capacity(viewers.len() * TOP_PLAYTIME_DAYS_PER_VIEWER);
    let mut short_career_rows: Vec<ShortCareerSnapshotRow> =
        Vec::with_capacity(viewers.len() * SHORT_CAREER_SNAPSHOTS_PER_VIEWER.min(2));

    for (viewer_id, acc) in &viewers {
        acc.collectInto(
            *viewer_id,
            &mut daily_rows,
            &mut heatmap_rows,
            &mut score_rows,
            &mut session_rows,
            &mut short_career_rows,
        );
    }

    applyHallMetadata(pool, &mut score_rows).await?;
    applyCoactivityClusters(&mut score_rows);
    let viewers_scored = score_rows.len() as i64;

    daily_rows.sort_by_key(|row| (row.viewer_id, row.day));
    heatmap_rows.sort_by_key(|row| (row.viewer_id, row.dow, row.hour));
    score_rows.sort_by_key(|row| row.viewer_id);
    session_rows.sort_by_key(|row| (row.viewer_id, row.rank));
    short_career_rows.sort_by_key(|row| (row.viewer_id, row.rank));

    // --- Rolling publish: keep the last version visible while replacing -----
    // Each viewer batch is deleted/reinserted inside a short transaction.
    // Readers keep seeing the old rows for untouched viewers, and touched
    // viewers switch over at batch commit instead of the whole report being
    // table-locked behind a TRUNCATE.
    let write_start = Instant::now();

    for score_chunk in score_rows.chunks(PUBLISH_VIEWER_CHUNK) {
        let Some(first_score) = score_chunk.first() else {
            continue;
        };
        let Some(last_score) = score_chunk.last() else {
            continue;
        };
        let min_viewer_id = first_score.viewer_id;
        let max_viewer_id = last_score.viewer_id;
        let viewer_ids: Vec<i64> = score_chunk.iter().map(|row| row.viewer_id).collect();

        let (daily_start, daily_end) = viewerRangeBy(
            &daily_rows,
            min_viewer_id,
            max_viewer_id,
            |row: &DailyRow| row.viewer_id,
        );
        let (heatmap_start, heatmap_end) = viewerRangeBy(
            &heatmap_rows,
            min_viewer_id,
            max_viewer_id,
            |row: &HeatmapRow| row.viewer_id,
        );
        let (session_start, session_end) = viewerRangeBy(
            &session_rows,
            min_viewer_id,
            max_viewer_id,
            |row: &SessionRow| row.viewer_id,
        );
        let (short_start, short_end) = viewerRangeBy(
            &short_career_rows,
            min_viewer_id,
            max_viewer_id,
            |row: &ShortCareerSnapshotRow| row.viewer_id,
        );

        let mut tx = pool.begin().await?;
        deleteViewerAggregates(&mut tx, &viewer_ids).await?;
        insertDaily(&mut tx, &daily_rows[daily_start..daily_end]).await?;
        insertHeatmap(&mut tx, &heatmap_rows[heatmap_start..heatmap_end]).await?;
        insertScores(&mut tx, score_chunk).await?;
        insertSessions(&mut tx, &session_rows[session_start..session_end]).await?;
        insertShortCareerSnapshots(&mut tx, &short_career_rows[short_start..short_end]).await?;
        tx.commit().await?;
    }

    let total_duration_ms = start.elapsed().as_millis() as i64;
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE cheat_analysis_meta \
         SET last_snapshot_id = $1, \
             last_refreshed_at = NOW(), \
             last_duration_ms = $2, \
             snapshots_processed = $3, \
             viewers_scored = $4 \
         WHERE id = 1",
    )
    .bind(max_snapshot_id)
    .bind(total_duration_ms as i32)
    .bind(snapshots_processed)
    .bind(viewers_scored)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // Invalidate cached /shame responses so the next request observes the
    // freshly published aggregates instead of stale data.
    crate::cache::clearAll();

    // Reload the in-memory snapshot that the /shame handlers serve from.
    // This is best-effort: a failure here just means the next request
    // falls back to the SQL path until the next rebuild.
    if let Err(err) = crate::shame::rebuildSnapshot(pool).await {
        warn!("shame snapshot rebuild failed: {}", err);
    }

    info!(
        "suspicious-activity analysis: published {} daily / {} heatmap / {} scores / {} sessions / {} short-career snapshots in {:.1}s (total {:.1}s)",
        daily_rows.len(),
        heatmap_rows.len(),
        score_rows.len(),
        session_rows.len(),
        short_career_rows.len(),
        write_start.elapsed().as_secs_f64(),
        start.elapsed().as_secs_f64(),
    );

    Ok(RebuildStats {
        snapshots_processed,
        viewers_scored,
        last_snapshot_id: max_snapshot_id,
        duration_ms: total_duration_ms,
    })
}

// ---------------------------------------------------------------------------
// Per-viewer accumulator
// ---------------------------------------------------------------------------

impl Default for HeatmapBuckets {
    fn default() -> Self {
        HeatmapBuckets([0u32; 7 * 24])
    }
}

impl HeatmapBuckets {
    #[inline]
    fn add(&mut self, idx: usize, value: u32) {
        self.0[idx] = self.0[idx].saturating_add(value);
    }
    #[inline]
    fn get(&self, idx: usize) -> u32 {
        self.0[idx]
    }
}

impl Default for CareerLengthBuckets {
    fn default() -> Self {
        CareerLengthBuckets([0u32; CAREER_LENGTH_BUCKETS])
    }
}

impl CareerLengthBuckets {
    fn add(&mut self, seconds: u32, count: u32) {
        let raw_idx = (seconds / CAREER_LENGTH_BUCKET_SECONDS) as usize;
        let idx = raw_idx.min(CAREER_LENGTH_BUCKETS - 1);
        self.0[idx] = self.0[idx].saturating_add(count);
    }
}

impl Default for CareerLengthScoreBuckets {
    fn default() -> Self {
        CareerLengthScoreBuckets([0.0; CAREER_LENGTH_BUCKETS])
    }
}

impl CareerLengthScoreBuckets {
    fn add(&mut self, seconds: u32, value: f64) {
        let raw_idx = (seconds / CAREER_LENGTH_BUCKET_SECONDS) as usize;
        let idx = raw_idx.min(CAREER_LENGTH_BUCKETS - 1);
        self.0[idx] += value;
    }
}

impl SessionRecord {
    fn key(&self) -> (u32, i64) {
        // Sort first by duration, tiebreak by start so the heap is total-order.
        (self.duration_sec, self.started_at.timestamp())
    }
}

impl PartialEq for SessionOrd {
    fn eq(&self, other: &Self) -> bool {
        self.record.key() == other.record.key()
    }
}
impl Eq for SessionOrd {}
impl PartialOrd for SessionOrd {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SessionOrd {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.record.key().cmp(&other.record.key())
    }
}

impl ViewerAccum {
    fn processEvent(
        &mut self,
        snapshot_id: i64,
        circle_id: i64,
        snapshot_time: DateTime<Utc>,
        fans: i64,
        last_login: DateTime<Utc>,
    ) {
        if self.first_seen.is_none() {
            self.first_seen = Some(snapshot_time);
        }
        self.last_seen = Some(snapshot_time);
        self.latest_circle_id = Some(circle_id);
        self.circle_ids_seen.insert(circle_id);

        let prev_fans = self.prev_fans;
        let prev_login = self.prev_login;
        let prev_time = self.prev_snapshot_time;
        let prev_snapshot_id = self.prev_snapshot_id;

        // Update rolling state for next iteration up front; we only consume
        // the deltas below.
        self.prev_fans = Some(fans);
        self.prev_login = Some(last_login);
        self.prev_snapshot_time = Some(snapshot_time);
        self.prev_snapshot_id = Some(snapshot_id);

        let (Some(prev_fans), Some(prev_login), Some(prev_time)) =
            (prev_fans, prev_login, prev_time)
        else {
            // First sample for this viewer — nothing to delta against.
            return;
        };

        let raw_delta = fans - prev_fans;
        let fan_delta: i64 = raw_delta.max(0);
        let gap_seconds: i64 = (snapshot_time - prev_time).num_seconds();
        if gap_seconds <= 0 {
            // Out-of-order or duplicate within a snapshot; skip but state is
            // still updated above.
            return;
        }
        let tight_gap = gap_seconds <= SESSION_GAP_MAX_SECONDS;
        let login_changed = last_login != prev_login;
        let is_active = fan_delta > 0 || login_changed;
        let is_career = fan_delta >= CAREER_FAN_THRESHOLD;
        let career_count: u32 = if is_career {
            let estimated = (fan_delta / CAREER_AVG_FANS).max(1);
            estimated.try_into().unwrap_or(u32::MAX)
        } else {
            0
        };

        if login_changed {
            let login_gap_seconds = (last_login - prev_login).num_seconds();
            if (60..=14 * 24 * 3600).contains(&login_gap_seconds) {
                self.login_gap_seconds
                    .push(login_gap_seconds.try_into().unwrap_or(u32::MAX));
            }
            self.pending_login_latency = Some(last_login);
        }

        if !tight_gap && fan_delta >= CAREER_FAN_THRESHOLD && gap_seconds <= 6 * 3600 {
            self.service_gap_resume_events = self.service_gap_resume_events.saturating_add(1);
        }

        if !tight_gap {
            self.last_career_finish_at = None;
            self.career_chain_active = false;
        } else if fan_delta == 0 || gap_seconds > TRUSTED_CAREER_CHAIN_MAX_GAP_SECONDS {
            // Observed idle stretch between snapshots — we can no longer
            // claim the user was continuously playing since the previous
            // career finish. A long collection gap has the same problem:
            // the career could have finished anywhere inside the gap.
            self.career_chain_active = false;
        }

        // Active-seconds attribution. Careers only pay active time once we
        // can observe a full interval between consecutive career finishes.
        // The first finish after a gap still gets the final tight snapshot
        // gap or a 10-minute plausible floor as a conservative baseline,
        // and its display length estimate is floored to that same 10 min
        // baseline because the true start time is unknown.
        let career_observation = if career_count > 0 && tight_gap {
            self.recordCareerLengths(ShortCareerContext {
                snapshot_id,
                circle_id,
                snapshot_time,
                last_login,
                previous_snapshot_id: prev_snapshot_id.unwrap_or_default(),
                previous_snapshot_time: prev_time,
                previous_snapshot_fans: prev_fans,
                current_fans: fans,
                snapshot_gap_seconds: gap_seconds,
                career_count,
                fan_delta,
            })
        } else {
            CareerObservation::default()
        };
        let active_seconds: u32 = if career_count > 0 && tight_gap {
            career_observation
                .active_seconds
                .unwrap_or_else(|| unobservedCareerActiveSeconds(career_count, gap_seconds))
        } else if is_active && tight_gap {
            (gap_seconds as u32).min(ACTIVE_SECONDS_CAP_PER_GAP)
        } else {
            0
        };
        let timeline_entry = ShortCareerTimelineSnapshot {
            snapshot_id,
            circle_id,
            snapshot_time,
            fans,
            last_login,
            fan_delta,
            gap_seconds: gap_seconds.try_into().unwrap_or(i32::MAX),
            login_changed,
            tight_gap,
            career_count: career_count as i32,
            active_seconds: active_seconds as i32,
            observed_runtime_seconds: observedTransitionRuntimeSeconds(
                career_count,
                tight_gap,
                gap_seconds,
                active_seconds,
            ) as i32,
        };
        self.extendPendingShortCareerSnapshots(&timeline_entry);
        if let Some(short_career_snapshot) = career_observation.short_career_snapshot {
            self.pending_short_career_snapshots
                .push_back(PendingShortCareerSnapshotRecord {
                    record: short_career_snapshot,
                });
        }
        if self.recent_short_career_timeline.len() == 3 {
            self.recent_short_career_timeline.pop_front();
        }
        self.recent_short_career_timeline.push_back(timeline_entry);

        if tight_gap && fan_delta > 0 && active_seconds > 0 {
            let rate = observedFanRatePerMinute(fan_delta, active_seconds);
            if rate > self.peak_fans_per_minute {
                self.peak_fans_per_minute = rate;
            }
            if rate >= PEAK_FAN_RATE_SCORE_BASE_FANS_PER_MINUTE {
                self.high_fan_rate_windows = self.high_fan_rate_windows.saturating_add(1);
                self.high_fan_rate_total_fan_gain = self
                    .high_fan_rate_total_fan_gain
                    .saturating_add(fan_delta as u64);
                self.high_fan_rate_total_seconds = self
                    .high_fan_rate_total_seconds
                    .saturating_add(active_seconds);
            }
            self.current_zero_idle_fan_gain_streak =
                self.current_zero_idle_fan_gain_streak.saturating_add(1);
            self.current_zero_idle_active_seconds = self
                .current_zero_idle_active_seconds
                .saturating_add(active_seconds);
            if self.current_zero_idle_fan_gain_streak > self.max_zero_idle_fan_gain_streak {
                self.max_zero_idle_fan_gain_streak = self.current_zero_idle_fan_gain_streak;
            }
            if self.current_zero_idle_active_seconds > self.max_zero_idle_active_seconds {
                self.max_zero_idle_active_seconds = self.current_zero_idle_active_seconds;
            }
        } else if !tight_gap || fan_delta == 0 {
            self.current_zero_idle_fan_gain_streak = 0;
            self.current_zero_idle_active_seconds = 0;
        }

        if tight_gap && career_count > 0 {
            if let Some(login_at) = self.pending_login_latency.take() {
                let latency_seconds = (snapshot_time - login_at).num_seconds();
                if (0..=6 * 3600).contains(&latency_seconds) {
                    self.post_login_latency_seconds
                        .push(latency_seconds.try_into().unwrap_or(u32::MAX));
                }
            }

            self.career_window_30m
                .push_back((snapshot_time, career_count));
            self.career_window_30m_sum = self.career_window_30m_sum.saturating_add(career_count);
            while let Some((old_at, old_count)) = self.career_window_30m.front().copied() {
                if (snapshot_time - old_at).num_seconds() <= ROLLING_BURST_WINDOW_SECONDS {
                    break;
                }
                self.career_window_30m.pop_front();
                self.career_window_30m_sum = self.career_window_30m_sum.saturating_sub(old_count);
            }
            if self.career_window_30m_sum > self.max_careers_30m {
                self.max_careers_30m = self.career_window_30m_sum;
            }
            if self.career_window_30m_sum >= 4 {
                self.burst_career_windows = self.burst_career_windows.saturating_add(1);
            }
        }

        // --- Update per-day bucket (snapshot_time end-of-transition day) ---
        //
        // Only credit fan gain / careers when the snapshot gap is tight.
        // A long gap (service outage, snapshot pipeline lag, missed
        // pulls) produces a single delta that may span many hours or
        // even cross day boundaries, so we can't honestly attribute it
        // to any particular day. Crediting fan_delta but not
        // active_seconds in that case produces artifacts like "23M fans
        // in 15 min active" on the daily chart.
        let (day, dow, hour) = utcParts(snapshot_time);
        let day_bucket = self.daily.entry(day).or_default();
        day_bucket.active_seconds = day_bucket.active_seconds.saturating_add(active_seconds);
        if tight_gap {
            day_bucket.careers = day_bucket.careers.saturating_add(career_count);
            day_bucket.fan_gain = day_bucket.fan_gain.saturating_add(fan_delta as u64);
        }
        if active_seconds > 0 {
            day_bucket.hours_bitmap |= 1u32 << hour;
        }

        // --- Heatmap: only attribute when gap is tight ---------------------
        if tight_gap && (active_seconds > 0 || career_count > 0) {
            let idx = (dow as usize) * 24 + (hour as usize);
            self.heatmap_active.add(idx, active_seconds);
            self.heatmap_careers.add(idx, career_count);
        }

        // --- Overall totals -------------------------------------------------
        //
        // Same reasoning as the daily bucket: lifetime fans-per-minute
        // and careers-per-hour rely on total_active_seconds in the
        // denominator, so we must not feed it numerators we couldn't
        // also credit active time for.
        self.total_active_seconds = self
            .total_active_seconds
            .saturating_add(active_seconds as u64);
        if tight_gap {
            self.total_fan_gain = self.total_fan_gain.saturating_add(fan_delta as u64);
            self.total_careers = self.total_careers.saturating_add(career_count);
        }

        // --- Observed session bookkeeping ----------------------------------
        //
        // A session is now an observed fan-gain window, not a login window.
        // It starts on the first tight snapshot gap with fan growth, stays
        // open across short idle gaps, and closes after a long no-gain
        // stretch, a reset, a long observation gap, or end of data. This prevents hidden
        // `last_login_time` refresh cadence from manufacturing top sessions.
        let reset_boundaries = jstResetsBetween(prev_time, snapshot_time);
        let last_reset_boundary = reset_boundaries.last().copied();

        // Long snapshot gaps are observation boundaries. If the current
        // transition has fan gain, deliberately do not seed a new session;
        // it could have happened anywhere inside the unobserved gap.
        if !tight_gap {
            self.pending_reset_recovery = None;
            self.closeSessionAt(prev_time);
        } else {
            let first_reset_boundary = reset_boundaries.first().copied();
            let idle_break_cutoff = self.open_session.as_ref().and_then(|open| {
                open.last_fan_gain_at.and_then(|last_gain_at| {
                    let idle_since_gain = (snapshot_time - last_gain_at).num_seconds();
                    if idle_since_gain >= OBSERVED_SESSION_IDLE_BREAK_SECONDS {
                        Some(
                            last_gain_at
                                + chrono::Duration::seconds(OBSERVED_SESSION_IDLE_BREAK_SECONDS),
                        )
                    } else {
                        None
                    }
                })
            });
            if let Some(cutoff) = idle_break_cutoff {
                if first_reset_boundary.map_or(true, |reset_at| cutoff <= reset_at) {
                    self.closeSessionAt(cutoff);
                }
            }

            for reset_at in reset_boundaries {
                // Only act if our currently-open session was anchored before
                // the boundary. Otherwise the boundary is irrelevant.
                let needs_close = matches!(
                    self.open_session.as_ref(),
                    Some(open) if open.started_at < reset_at
                );
                if needs_close {
                    self.recordResetRecoveryWindow(reset_at, tight_gap);
                    self.closeSessionAt(reset_at);
                }
            }

            if fan_delta > 0 {
                if self.open_session.is_none() {
                    self.open_session = Some(OpenSession {
                        started_at: last_reset_boundary.unwrap_or(prev_time),
                        last_fan_gain_at: None,
                        idle_seconds_so_far: 0,
                        active_seconds_so_far: 0,
                        careers: 0,
                        fan_gain: 0,
                    });
                }

                if let Some(open) = self.open_session.as_mut() {
                    open.last_fan_gain_at = Some(snapshot_time);
                    open.active_seconds_so_far =
                        open.active_seconds_so_far.saturating_add(active_seconds);
                    open.careers = open.careers.saturating_add(career_count);
                    open.fan_gain = open.fan_gain.saturating_add(fan_delta as u64);
                }
            } else if let Some(open) = self.open_session.as_mut() {
                let gap_u32: u32 = gap_seconds.try_into().unwrap_or(u32::MAX);
                open.idle_seconds_so_far = open.idle_seconds_so_far.saturating_add(gap_u32);
            }
        }

        self.resolveResetRecoveryIfNeeded(snapshot_time, fan_delta, tight_gap);
    }

    /// Finalize the currently-open observed fan-gain session at the actual
    /// boundary `cutoff_at`, so tolerated idle before that boundary shows up
    /// in the active-vs-idle split.
    fn closeSessionAt(&mut self, cutoff_at: DateTime<Utc>) {
        let Some(open) = self.open_session.take() else {
            return;
        };
        // Drop sessions where we never observed any fan gain.
        let Some(_last_fan_gain_at) = open.last_fan_gain_at else {
            return;
        };
        let ended_at = cutoff_at;
        let duration = (ended_at - open.started_at).num_seconds();
        if duration <= 0 {
            return;
        }
        let duration_sec: u32 = duration.try_into().unwrap_or(u32::MAX);
        if duration_sec > self.max_session_seconds {
            self.max_session_seconds = duration_sec;
        }
        let active_sec = open.active_seconds_so_far.min(duration_sec);
        let idle_sec = duration_sec.saturating_sub(active_sec);
        let (start_day, _, _) = utcParts(open.started_at);
        let bucket = self.daily.entry(start_day).or_default();
        bucket.sessions = bucket.sessions.saturating_add(1);
        if duration_sec > bucket.longest_session_sec {
            bucket.longest_session_sec = duration_sec;
        }
        let record = SessionRecord {
            started_at: open.started_at,
            ended_at,
            duration_sec,
            active_sec,
            idle_sec,
            careers: open.careers,
            fan_gain: open.fan_gain,
        };
        bucket.session_breakdown.push(record.clone());
        pushTopSession(&mut self.top_sessions, record);
    }

    fn recordResetRecoveryWindow(&mut self, reset_at: DateTime<Utc>, tight_gap: bool) {
        if !tight_gap {
            return;
        }

        let Some(open) = self.open_session.as_ref() else {
            return;
        };
        let Some(last_fan_gain_at) = open.last_fan_gain_at else {
            return;
        };
        let seconds_since_gain = (reset_at - last_fan_gain_at).num_seconds();
        if !(0..=RESET_RECOVERY_PRE_RESET_ACTIVE_WINDOW_SECONDS).contains(&seconds_since_gain) {
            return;
        }

        self.reset_recovery_windows = self.reset_recovery_windows.saturating_add(1);
        self.pending_reset_recovery = Some(PendingResetRecovery { reset_at });
    }

    fn resolveResetRecoveryIfNeeded(
        &mut self,
        snapshot_time: DateTime<Utc>,
        fan_delta: i64,
        tight_gap: bool,
    ) {
        if !tight_gap {
            self.pending_reset_recovery = None;
            return;
        }
        if fan_delta <= 0 {
            return;
        }

        let Some(pending) = self.pending_reset_recovery.take() else {
            return;
        };
        let delay_seconds = (snapshot_time - pending.reset_at).num_seconds();
        if delay_seconds < 0 {
            return;
        }
        let delay_seconds_u32: u32 = delay_seconds.try_into().unwrap_or(u32::MAX);
        if delay_seconds_u32 > self.max_reset_recovery_seconds {
            self.max_reset_recovery_seconds = delay_seconds_u32;
        }
        if delay_seconds >= RESET_BREAK_MIN_DELAY_SECONDS {
            self.reset_breaks = self.reset_breaks.saturating_add(1);
        }
    }

    fn recordCareerLengths(&mut self, ctx: ShortCareerContext) -> CareerObservation {
        let mut observation = CareerObservation::default();
        if let Some(prev_finish_at) = self.last_career_finish_at {
            let gap_seconds = (ctx.snapshot_time - prev_finish_at).num_seconds();
            if (1..=6 * 3600).contains(&gap_seconds) {
                let seconds = gap_seconds.try_into().unwrap_or(u32::MAX);
                self.career_finish_intervals.push(seconds);
                self.career_rate_samples.push(CareerRateSample {
                    finished_at: ctx.snapshot_time,
                    seconds,
                });
            }
        }
        let trusted_chain_seconds = self.last_career_finish_at.and_then(|prev_finish_at| {
            let gap_seconds = (ctx.snapshot_time - prev_finish_at).num_seconds();
            if gap_seconds > 0 && self.career_chain_active {
                Some(gap_seconds as u32)
            } else {
                None
            }
        });

        if let Some(length_seconds) = trusted_chain_seconds {
            observation.active_seconds = Some(length_seconds);
        }

        // Always keep a conservative display estimate for single-career
        // finishes. Only trusted finish-to-finish intervals are allowed to
        // feed short/high-fan automation scoring.
        if ctx.career_count == 1 {
            if let Some(length_seconds) = estimatedSingleCareerLengthSeconds(
                ctx.snapshot_time,
                ctx.last_login,
                self.last_career_finish_at,
                trusted_chain_seconds,
            ) {
                self.career_length_buckets.add(length_seconds, 1);

                if trusted_chain_seconds.is_some() {
                    let fan_gain_sample = ctx.fan_delta.max(0).min(u32::MAX as i64) as u32;
                    self.trusted_career_fan_gains.push(fan_gain_sample);
                    self.trusted_career_lengths.push(length_seconds);
                    let fan_gain_per_career = ctx.fan_delta as f64;
                    let fan_gain_per_career_minute =
                        fan_gain_per_career * 60.0 / length_seconds.max(1) as f64;
                    if length_seconds < SHORT_HIGH_FAN_MAX_SECONDS {
                        self.short_career_fan_gains.push(fan_gain_sample);
                    }
                    let is_high_fan_short = length_seconds < SHORT_HIGH_FAN_MAX_SECONDS
                        && fan_gain_per_career_minute >= HIGH_FAN_GAIN_PER_SHORT_CAREER_MINUTE;
                    let short_training_score = if is_high_fan_short {
                        shortFanGainSeverity(length_seconds, fan_gain_per_career)
                    } else {
                        0.0
                    };
                    if length_seconds < SHORT_HIGH_FAN_MAX_SECONDS {
                        if let Some(previous_career_snapshot_time) = self.last_career_finish_at {
                            observation.short_career_snapshot = Some(ShortCareerSnapshotRecord {
                                snapshot_id: ctx.snapshot_id,
                                circle_id: ctx.circle_id,
                                snapshot_time: ctx.snapshot_time,
                                previous_snapshot_id: ctx.previous_snapshot_id,
                                previous_snapshot_time: ctx.previous_snapshot_time,
                                previous_snapshot_fans: ctx.previous_snapshot_fans,
                                current_fans: ctx.current_fans,
                                fan_gain: ctx.fan_delta,
                                snapshot_gap_seconds: ctx
                                    .snapshot_gap_seconds
                                    .try_into()
                                    .unwrap_or(u32::MAX),
                                previous_career_snapshot_time,
                                previous_career_gap_seconds: length_seconds,
                                career_length_seconds: length_seconds,
                                fans_per_minute: fan_gain_per_career_minute,
                                short_training_score,
                                is_high_fan_short,
                                prior_snapshots: self
                                    .recent_short_career_timeline
                                    .iter()
                                    .cloned()
                                    .collect(),
                                next_snapshots: Vec::new(),
                            });
                        }
                    }
                    if is_high_fan_short {
                        self.short_high_fan_careers = self.short_high_fan_careers.saturating_add(1);
                        self.short_fan_gain_score += short_training_score;
                        self.short_fan_gain_score_buckets
                            .add(length_seconds, short_training_score);
                    }
                }

                if self.career_lengths_last20.len() == LAST_CAREER_LENGTH_WINDOW {
                    self.career_lengths_last20.pop_front();
                }
                self.career_lengths_last20.push_back(length_seconds);
            }
        }
        self.last_career_finish_at = Some(ctx.snapshot_time);
        // A career finish only anchors the next short-career measurement if
        // this finish itself was observed at normal collection cadence.
        self.career_chain_active = ctx.snapshot_gap_seconds <= TRUSTED_CAREER_CHAIN_MAX_GAP_SECONDS;
        observation
    }

    fn extendPendingShortCareerSnapshots(&mut self, timeline: &ShortCareerTimelineSnapshot) {
        let mut completed = 0usize;
        for pending in &mut self.pending_short_career_snapshots {
            if pending.record.next_snapshots.len() < 3 {
                pending.record.next_snapshots.push(timeline.clone());
            }
            if pending.record.next_snapshots.len() >= 3 {
                completed += 1;
            } else {
                break;
            }
        }

        for _ in 0..completed {
            if let Some(pending) = self.pending_short_career_snapshots.pop_front() {
                self.pushShortCareerSnapshot(pending.record);
            }
        }
    }

    fn pushShortCareerSnapshot(&mut self, record: ShortCareerSnapshotRecord) {
        self.short_career_snapshot_total = self.short_career_snapshot_total.saturating_add(1);
        if self.short_career_snapshots.len() == SHORT_CAREER_SNAPSHOTS_PER_VIEWER {
            self.short_career_snapshots.pop_front();
        }
        self.short_career_snapshots.push_back(record);
    }

    fn finalize(&mut self) {
        // End-of-data: close whatever session is open at the last observed
        // snapshot. `last_seen` is the cutoff because we have no evidence
        // of activity after that.
        if let Some(cutoff) = self.last_seen {
            self.closeSessionAt(cutoff);
        }
        while let Some(pending) = self.pending_short_career_snapshots.pop_front() {
            self.pushShortCareerSnapshot(pending.record);
        }
    }

    fn collectInto(
        &self,
        viewer_id: i64,
        daily_out: &mut Vec<DailyRow>,
        heatmap_out: &mut Vec<HeatmapRow>,
        scores_out: &mut Vec<ScoreRow>,
        sessions_out: &mut Vec<SessionRow>,
        short_career_out: &mut Vec<ShortCareerSnapshotRow>,
    ) {
        let (Some(first_seen), Some(last_seen)) = (self.first_seen, self.last_seen) else {
            return;
        };

        // Daily.
        let mut days_observed: i32 = 0;
        let mut days_active: i32 = 0;
        let mut max_daily_active_seconds: i32 = 0;
        let mut max_daily_careers: i32 = 0;
        let mut days_over_16h: i32 = 0;
        let mut days_over_20h: i32 = 0;
        let mut days_over_22h: i32 = 0;

        for (day, bucket) in &self.daily {
            let distinct_hours = bucket.hours_bitmap.count_ones() as i16;
            let active = bucket.active_seconds as i32;
            let careers = bucket.careers as i32;
            daily_out.push(DailyRow {
                viewer_id,
                day: *day,
                active_seconds: active,
                careers,
                fan_gain: bucket.fan_gain as i64,
                sessions: bucket.sessions as i32,
                longest_session_sec: bucket.longest_session_sec as i32,
                distinct_hours,
            });
            days_observed += 1;
            if active > 0 {
                days_active += 1;
            }
            if active > max_daily_active_seconds {
                max_daily_active_seconds = active;
            }
            if careers > max_daily_careers {
                max_daily_careers = careers;
            }
            if active > 16 * 3600 {
                days_over_16h += 1;
            }
            if active > 20 * 3600 {
                days_over_20h += 1;
            }
            if active > 22 * 3600 {
                days_over_22h += 1;
            }
        }

        // Heatmap.
        let mut distinct_weekly_hour_buckets: i16 = 0;
        for dow in 0u8..7 {
            for hour in 0u8..24 {
                let idx = (dow as usize) * 24 + (hour as usize);
                let active = self.heatmap_active.get(idx);
                let careers = self.heatmap_careers.get(idx);
                if active > 0 || careers > 0 {
                    heatmap_out.push(HeatmapRow {
                        viewer_id,
                        dow: dow as i16,
                        hour: hour as i16,
                        active_seconds: active as i32,
                        careers: careers as i32,
                    });
                    if active > 0 {
                        distinct_weekly_hour_buckets += 1;
                    }
                }
            }
        }

        // Top playtime days: rank by observed-window active time, then
        // include the observed fan-gain windows that explain that day.
        let mut playtime_days: Vec<_> = self
            .daily
            .iter()
            .filter(|(_, bucket)| !bucket.session_breakdown.is_empty())
            .collect();
        playtime_days.sort_by(|(day_a, a), (day_b, b)| {
            let active_a: u32 = a.session_breakdown.iter().map(|s| s.active_sec).sum();
            let active_b: u32 = b.session_breakdown.iter().map(|s| s.active_sec).sum();
            let fan_gain_a: u64 = a.session_breakdown.iter().map(|s| s.fan_gain).sum();
            let fan_gain_b: u64 = b.session_breakdown.iter().map(|s| s.fan_gain).sum();
            active_b
                .cmp(&active_a)
                .then(fan_gain_b.cmp(&fan_gain_a))
                .then(day_b.cmp(day_a))
        });
        for (i, (day, bucket)) in playtime_days
            .iter()
            .take(TOP_PLAYTIME_DAYS_PER_VIEWER)
            .enumerate()
        {
            let mut sessions = bucket.session_breakdown.clone();
            sessions.sort_by(|a, b| a.started_at.cmp(&b.started_at));
            let Some(started_at) = sessions.first().map(|s| s.started_at) else {
                continue;
            };
            let Some(ended_at) = sessions.last().map(|s| s.ended_at) else {
                continue;
            };
            let duration_seconds: u32 = sessions.iter().map(|s| s.duration_sec).sum();
            let active_seconds: u32 = sessions.iter().map(|s| s.active_sec).sum();
            let idle_seconds: u32 = sessions.iter().map(|s| s.idle_sec).sum();
            let careers: u32 = sessions.iter().map(|s| s.careers).sum();
            let fan_gain: u64 = sessions.iter().map(|s| s.fan_gain).sum();
            sessions_out.push(SessionRow {
                viewer_id,
                rank: (i + 1) as i16,
                day: **day,
                started_at,
                ended_at,
                duration_seconds: duration_seconds as i32,
                active_seconds: active_seconds as i32,
                idle_seconds: idle_seconds as i32,
                careers: careers as i32,
                fan_gain: fan_gain as i64,
                session_count: sessions.len() as i32,
                longest_session_sec: bucket.longest_session_sec as i32,
                distinct_hours: bucket.hours_bitmap.count_ones() as i16,
                sessions,
            });
        }

        for (i, snapshot) in self.short_career_snapshots.iter().rev().enumerate() {
            short_career_out.push(ShortCareerSnapshotRow {
                viewer_id,
                rank: (i + 1) as i16,
                total_count: self.short_career_snapshot_total as i32,
                snapshot_id: snapshot.snapshot_id,
                circle_id: snapshot.circle_id,
                snapshot_time: snapshot.snapshot_time,
                previous_snapshot_id: snapshot.previous_snapshot_id,
                previous_snapshot_time: snapshot.previous_snapshot_time,
                previous_snapshot_fans: snapshot.previous_snapshot_fans,
                current_fans: snapshot.current_fans,
                fan_gain: snapshot.fan_gain,
                snapshot_gap_seconds: snapshot.snapshot_gap_seconds as i32,
                previous_career_snapshot_time: snapshot.previous_career_snapshot_time,
                previous_career_gap_seconds: snapshot.previous_career_gap_seconds as i32,
                career_length_seconds: snapshot.career_length_seconds as i32,
                fans_per_minute: snapshot.fans_per_minute,
                short_training_score: snapshot.short_training_score,
                is_high_fan_short: snapshot.is_high_fan_short,
                prior_snapshots: snapshot.prior_snapshots.clone(),
                next_snapshots: snapshot.next_snapshots.clone(),
            });
        }

        // Scores + flags.
        let total_active_seconds = self.total_active_seconds as i64;
        let total_careers = self.total_careers as i32;
        let total_fan_gain = self.total_fan_gain as i64;
        let avg_active_seconds_per_observed_day =
            averageActiveSecondsPerObservedDay(total_active_seconds, days_observed);
        let career_rate_breakdown = careerRateBreakdown(&self.career_rate_samples, last_seen);
        let careers_per_active_hour = career_rate_breakdown.all.careers_per_hour;
        let career_rate_sample_count = career_rate_breakdown.all.sample_count;
        let career_rate_sample_seconds = career_rate_breakdown.all.sample_seconds;
        let avg_careers_per_day = avgCareersPerObservedDay(self.total_careers, days_observed);
        let fans_per_active_minute = if total_active_seconds >= 60 {
            (total_fan_gain as f64) / (total_active_seconds as f64 / 60.0)
        } else {
            0.0
        };
        let peak_fans_per_minute = self.peak_fans_per_minute;
        let flag_no_sleep = days_observed >= 3 && max_daily_active_seconds > 18 * 3600;
        let flag_extreme_session = (self.max_session_seconds as i32) > 8 * 3600;
        let flag_inhuman_career_rate =
            career_rate_sample_count >= 10 && careers_per_active_hour > 6.0;
        let flag_247 = is247Schedule(
            distinct_weekly_hour_buckets,
            avg_active_seconds_per_observed_day,
            days_observed,
        );
        let flag_marathon = days_over_22h >= 1;
        let avg_career_length_last20_seconds = if self.career_lengths_last20.is_empty() {
            0.0
        } else {
            let sum: u32 = self.career_lengths_last20.iter().sum();
            sum as f64 / self.career_lengths_last20.len() as f64
        };
        let short_career_fan_gain_stats = fanGainStats(&self.short_career_fan_gains);
        let behaviorChangeStats = behaviorChangeStats(&self.daily);
        let (probe_metrics, coactivity_fingerprint) = self.buildProbeMetrics(
            days_observed,
            avg_active_seconds_per_observed_day,
            total_active_seconds,
        );
        let probe_score = probeMetricsTotal(&probe_metrics);

        // Composite suspicion score in [0, 100]. Broad schedule coverage is
        // only weak context; the schedule-heavy weight now comes from actual
        // long productive days so slower OCR automation is visible without
        // treating a merely full heatmap as suspicious by default.
        let careers_rate_score = (careers_per_active_hour / 12.0).clamp(0.0, 1.0) * 20.0;
        let session_score = rangedScore(
            self.max_session_seconds as f64,
            SESSION_LENGTH_SCORE_START_SECONDS,
            SESSION_LENGTH_SCORE_FULL_SECONDS,
            SESSION_LENGTH_SCORE_MAX,
        );
        let longHoursScore = longHoursScore(
            max_daily_active_seconds,
            avg_active_seconds_per_observed_day,
            days_over_16h,
            days_over_20h,
            days_observed,
        );
        let coverage_score = coverageScheduleScore(
            distinct_weekly_hour_buckets,
            avg_active_seconds_per_observed_day,
        );
        let short_career_ratio = if total_careers > 0 {
            // Bucket widths are 5 min. In practice:
            //   buckets 0..=1 (< 10 min)  -> physically impossible
            //   bucket 2     (10–15 min)  -> very hard, rare
            //   bucket 3     (15–20 min)  -> doable but fast
            // Weight impossible careers fully, hard careers half, and
            // ignore the merely-fast bucket.
            let impossible = self.career_length_buckets.0[0] + self.career_length_buckets.0[1];
            let hard = self.career_length_buckets.0[2];
            (impossible as f64 + 0.5 * hard as f64) / total_careers as f64
        } else {
            0.0
        };
        // Short alone is noisy because users can abandon runs. Keep it as a
        // mild signal and let short+high-fan carry the heavier weight below.
        let short_career_score = short_career_ratio.clamp(0.0, 1.0) * 10.0;
        let short_fan_gain_score_component =
            (self.short_fan_gain_score / 35.0).clamp(0.0, 1.0) * 35.0;
        let behavior_change_score = behaviorChangeStats.behavior_change_score;
        let fans_per_min_score = normalizedRateScore(
            fans_per_active_minute,
            SUSTAINED_FAN_RATE_SCORE_BASE_FANS_PER_MINUTE,
            SUSTAINED_FAN_RATE_SCORE_MAX,
        );
        let peak_fans_per_min_score = normalizedRateScore(
            peak_fans_per_minute,
            PEAK_FAN_RATE_SCORE_BASE_FANS_PER_MINUTE,
            PEAK_FAN_RATE_SCORE_MAX,
        ) * repeatedHighFanRateFactor(self.high_fan_rate_windows);
        let reset_break_score = resetBreakScore(
            self.reset_breaks,
            self.reset_recovery_windows,
            avg_active_seconds_per_observed_day,
            max_daily_active_seconds,
            days_over_16h,
        );
        // Volume: many days of observation with extreme career counts.
        let total_careers_score = ((total_careers as f64) / 500.0).clamp(0.0, 1.0) * 4.0;
        let composite = careers_rate_score
            + session_score
            + longHoursScore
            + coverage_score
            + short_career_score
            + short_fan_gain_score_component
            + behavior_change_score
            + fans_per_min_score
            + peak_fans_per_min_score
            + reset_break_score
            + probe_score.min(PROBE_SCORE_CONTRIBUTION_MAX)
            + total_careers_score;
        let suspicion_score = composite.round().clamp(0.0, 100.0) as i32;

        scores_out.push(ScoreRow {
            viewer_id,
            trainer_name: None,
            circle_id: self.latest_circle_id,
            circle_name: None,
            circle_monthly_rank: None,
            first_seen,
            last_seen,
            days_observed,
            days_active,
            total_active_seconds,
            total_fan_gain,
            total_careers,
            careers_per_active_hour,
            career_rate_sample_count,
            career_rate_sample_seconds,
            career_rate_breakdown,
            avg_careers_per_day,
            avg_career_length_last20_seconds,
            career_length_buckets: self
                .career_length_buckets
                .0
                .iter()
                .map(|&n| n as i32)
                .collect(),
            short_high_fan_careers: self.short_high_fan_careers as i32,
            short_fan_gain_score: self.short_fan_gain_score,
            short_fan_gain_score_buckets: self.short_fan_gain_score_buckets.0.to_vec(),
            short_career_avg_fan_gain: short_career_fan_gain_stats.avg,
            short_career_p50_fan_gain: short_career_fan_gain_stats.p50,
            short_career_p90_fan_gain: short_career_fan_gain_stats.p90,
            short_career_p95_fan_gain: short_career_fan_gain_stats.p95,
            short_career_max_fan_gain: short_career_fan_gain_stats.max,
            recent_fan_gain_3d: behaviorChangeStats.recent_fan_gain_3d,
            baseline_fan_gain_14d: behaviorChangeStats.baseline_fan_gain_14d,
            recent_fans_per_day: behaviorChangeStats.recent_fans_per_day,
            baseline_fans_per_day: behaviorChangeStats.baseline_fans_per_day,
            fan_gain_spike_ratio: behaviorChangeStats.fan_gain_spike_ratio,
            behavior_change_score: behaviorChangeStats.behavior_change_score,
            fans_per_active_minute,
            peak_fans_per_minute,
            high_fan_rate_windows: self.high_fan_rate_windows as i32,
            high_fan_rate_total_fan_gain: self.high_fan_rate_total_fan_gain as i64,
            high_fan_rate_total_seconds: self.high_fan_rate_total_seconds as i32,
            max_daily_active_seconds,
            max_daily_careers,
            max_session_seconds: self.max_session_seconds as i32,
            days_over_16h,
            days_over_20h,
            reset_recovery_windows: self.reset_recovery_windows as i32,
            reset_breaks: self.reset_breaks as i32,
            max_reset_recovery_seconds: self.max_reset_recovery_seconds as i32,
            reset_break_score,
            probe_score,
            probe_metrics,
            coactivity_fingerprint,
            distinct_weekly_hour_buckets,
            flag_no_sleep,
            flag_extreme_session,
            flag_inhuman_career_rate,
            flag_247,
            flag_marathon,
            suspicion_score,
        });
    }

    fn buildProbeMetrics(
        &self,
        days_observed: i32,
        avg_active_seconds_per_observed_day: f64,
        total_active_seconds: i64,
    ) -> (SuspicionProbeMetrics, u64) {
        let career_fan_gain_samples = self.trusted_career_fan_gains.len() as i32;
        let career_fan_gain_mode_share =
            modeShareRounded(&self.trusted_career_fan_gains, FAN_GAIN_MODE_BUCKET_FANS);
        let career_fan_gain_cv = coefficientOfVariation(&self.trusted_career_fan_gains);
        let career_fan_gain_score = careerFanGainScore(
            self.trusted_career_fan_gains.len(),
            career_fan_gain_mode_share,
            career_fan_gain_cv,
        );

        let career_rhythm_samples = self.career_finish_intervals.len() as i32;
        let career_rhythm_cv = coefficientOfVariation(&self.career_finish_intervals);
        let career_length_cv = coefficientOfVariation(&self.trusted_career_lengths);
        let career_regularity_score = careerRegularityScore(
            self.career_finish_intervals.len(),
            self.trusted_career_lengths.len(),
            career_rhythm_cv,
            career_length_cv,
        );

        let login_gap_samples = self.login_gap_seconds.len() as i32;
        let login_gap_cv = coefficientOfVariation(&self.login_gap_seconds);
        let login_gap_mode_share =
            modeShareRounded(&self.login_gap_seconds, LOGIN_GAP_MODE_BUCKET_SECONDS);
        let login_regularity_score = loginRegularityScore(
            self.login_gap_seconds.len(),
            login_gap_cv,
            login_gap_mode_share,
        );

        let post_login_latency_samples = self.post_login_latency_seconds.len() as i32;
        let post_login_latency_median_seconds =
            medianU32(&self.post_login_latency_seconds).round() as i32;
        let post_login_latency_cv = coefficientOfVariation(&self.post_login_latency_seconds);
        let post_login_latency_score = postLoginLatencyScore(
            self.post_login_latency_seconds.len(),
            post_login_latency_median_seconds,
            post_login_latency_cv,
        );

        let zero_idle_score = zeroIdleScore(
            self.max_zero_idle_fan_gain_streak,
            self.max_zero_idle_active_seconds,
        );

        let schedule = scheduleShapeMetrics(&self.heatmap_active, total_active_seconds);
        let schedule_shape_score = scheduleShapeScore(
            schedule.weekday_weekend_similarity,
            schedule.hourly_entropy,
            schedule.night_active_ratio,
            avg_active_seconds_per_observed_day,
            days_observed,
        );

        let burst_career_score = burstCareerScore(self.max_careers_30m, self.burst_career_windows);
        let service_gap_resume_score = serviceGapResumeScore(
            self.service_gap_resume_events,
            avg_active_seconds_per_observed_day,
            days_observed,
        );
        let distinct_circles_seen = self.circle_ids_seen.len() as i32;
        let circle_churn_score = circleChurnScore(distinct_circles_seen, days_observed);
        let coactivity_fingerprint = coactivityFingerprint(&self.heatmap_active);

        (
            SuspicionProbeMetrics {
                career_fan_gain_samples,
                career_fan_gain_mode_share,
                career_fan_gain_cv,
                career_fan_gain_score,
                career_rhythm_samples,
                career_rhythm_cv,
                career_length_cv,
                career_regularity_score,
                login_gap_samples,
                login_gap_cv,
                login_gap_mode_share,
                login_regularity_score,
                post_login_latency_samples,
                post_login_latency_median_seconds,
                post_login_latency_cv,
                post_login_latency_score,
                max_zero_idle_fan_gain_streak: self.max_zero_idle_fan_gain_streak as i32,
                max_zero_idle_active_seconds: self.max_zero_idle_active_seconds as i32,
                zero_idle_score,
                weekday_weekend_similarity: schedule.weekday_weekend_similarity,
                hourly_entropy: schedule.hourly_entropy,
                night_active_ratio: schedule.night_active_ratio,
                night_active_seconds: schedule.night_active_seconds,
                schedule_shape_score,
                max_careers_30m: self.max_careers_30m as i32,
                burst_career_windows: self.burst_career_windows as i32,
                burst_career_score,
                service_gap_resume_events: self.service_gap_resume_events as i32,
                service_gap_resume_score,
                distinct_circles_seen,
                circle_churn_score,
                coactivity_cluster_size: 0,
                coactivity_cluster_score: 0.0,
            },
            coactivity_fingerprint,
        )
    }
}

fn pushTopSession(heap: &mut BinaryHeap<Reverse<SessionOrd>>, record: SessionRecord) {
    let entry = Reverse(SessionOrd { record });
    if heap.len() < TOP_PLAYTIME_DAYS_PER_VIEWER {
        heap.push(entry);
    } else if let Some(min) = heap.peek() {
        if entry.0.record.key() > min.0.record.key() {
            heap.pop();
            heap.push(entry);
        }
    }
}

fn unobservedCareerActiveSeconds(career_count: u32, gap_seconds: i64) -> u32 {
    let final_observed_gap = (gap_seconds as u32).min(ACTIVE_SECONDS_CAP_PER_GAP);
    let plausible_floor =
        UNOBSERVED_CAREER_ACTIVE_SECONDS_FLOOR.saturating_mul(career_count.max(1));
    final_observed_gap.max(plausible_floor)
}

fn observedTransitionRuntimeSeconds(
    career_count: u32,
    tight_gap: bool,
    gap_seconds: i64,
    active_seconds: u32,
) -> u32 {
    if career_count > 0 && tight_gap && gap_seconds > 0 {
        (gap_seconds as u32).min(ACTIVE_SECONDS_CAP_PER_GAP)
    } else {
        active_seconds
    }
}

fn estimatedSingleCareerLengthSeconds(
    snapshot_time: DateTime<Utc>,
    last_login: DateTime<Utc>,
    last_career_finish_at: Option<DateTime<Utc>>,
    trusted_chain_seconds: Option<u32>,
) -> Option<u32> {
    if trusted_chain_seconds.is_some() {
        return trusted_chain_seconds;
    }

    let inferred_start = last_career_finish_at
        .map(|prev_finish_at| prev_finish_at.max(last_login))
        .unwrap_or(last_login);
    let raw_seconds: u32 = (snapshot_time - inferred_start)
        .num_seconds()
        .max(0)
        .try_into()
        .unwrap_or(u32::MAX);
    if raw_seconds == 0 {
        None
    } else {
        Some(raw_seconds.max(UNOBSERVED_CAREER_ACTIVE_SECONDS_FLOOR))
    }
}

/// Yields each 15:00 UTC instant strictly between `from` and `to`. This is
/// the 00:00 JST forced relog the game enforces; we treat it as an
/// implicit session boundary even when the snapshot stream doesn't record
/// a `last_login_time` change at exactly that instant.
fn jstResetsBetween(from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<DateTime<Utc>> {
    if to <= from {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut day = from.date_naive();
    loop {
        if let Some(reset) = day
            .and_hms_opt(JST_RESET_HOUR_UTC, 0, 0)
            .map(|naive| naive.and_utc())
        {
            if reset > from && reset < to {
                out.push(reset);
            }
            if reset >= to {
                break;
            }
        }
        let Some(next) = day.succ_opt() else {
            break;
        };
        day = next;
        // Safety: we'll break once the candidate reset >= to.
        if out.len() > 32 {
            break;
        }
    }
    out
}

fn utcParts(ts: DateTime<Utc>) -> (NaiveDate, u8, u8) {
    (
        ts.date_naive(),
        ts.weekday().num_days_from_sunday() as u8,
        ts.hour() as u8,
    )
}

// ---------------------------------------------------------------------------
// Output row types and bulk inserts
// ---------------------------------------------------------------------------

fn viewerRangeBy<T>(
    rows: &[T],
    min_viewer_id: i64,
    max_viewer_id: i64,
    get_viewer_id: fn(&T) -> i64,
) -> (usize, usize) {
    let start = rows.partition_point(|row| get_viewer_id(row) < min_viewer_id);
    let end = rows.partition_point(|row| get_viewer_id(row) <= max_viewer_id);
    (start, end)
}

// (Box<[u32; 168]> has no built-in Default, so build the array zero-initialized
// then box it once per viewer.)

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    use dotenvy::dotenv;

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[derive(Debug, Clone)]
    struct ViewerProbeEvent {
        id: i64,
        circle_id: i64,
        snapshot_time: DateTime<Utc>,
        fans: i64,
        last_login: DateTime<Utc>,
    }

    #[derive(Debug)]
    struct TransitionDebug {
        snapshot_id: i64,
        circle_id: i64,
        snapshot_time: DateTime<Utc>,
        gap_seconds: i64,
        fan_delta: i64,
        last_login: DateTime<Utc>,
        login_changed: bool,
        tight_gap: bool,
        career_count: u32,
        active_seconds: u32,
        trusted_chain_seconds: Option<u32>,
        display_length_seconds: Option<u32>,
    }

    #[derive(Debug)]
    struct ShortRunDebug {
        snapshot_id: i64,
        circle_id: i64,
        snapshot_time: DateTime<Utc>,
        prev_finish_at: DateTime<Utc>,
        last_login: DateTime<Utc>,
        gap_seconds: i64,
        fan_delta: i64,
        active_seconds: u32,
        length_seconds: u32,
        fan_gain_per_minute: f64,
        qualifies_high_fan: bool,
    }

    #[derive(Debug)]
    struct ClosedSessionDebug {
        record: SessionRecord,
        reason: &'static str,
    }

    #[derive(Default)]
    struct ViewerProbe {
        prev_fans: Option<i64>,
        prev_login: Option<DateTime<Utc>>,
        prev_snapshot_time: Option<DateTime<Utc>>,
        open_session: Option<OpenSession>,
        last_career_finish_at: Option<DateTime<Utc>>,
        career_chain_active: bool,
        transitions: Vec<TransitionDebug>,
        short_runs: Vec<ShortRunDebug>,
        sessions: Vec<ClosedSessionDebug>,
    }

    impl ViewerProbe {
        fn observe(&mut self, event: &ViewerProbeEvent) {
            let prev_fans = self.prev_fans;
            let prev_login = self.prev_login;
            let prev_snapshot_time = self.prev_snapshot_time;

            self.prev_fans = Some(event.fans);
            self.prev_login = Some(event.last_login);
            self.prev_snapshot_time = Some(event.snapshot_time);

            let (Some(prev_fans), Some(prev_login), Some(prev_snapshot_time)) =
                (prev_fans, prev_login, prev_snapshot_time)
            else {
                return;
            };

            let fan_delta = (event.fans - prev_fans).max(0);
            let gap_seconds = (event.snapshot_time - prev_snapshot_time).num_seconds();
            if gap_seconds <= 0 {
                return;
            }

            let tight_gap = gap_seconds <= SESSION_GAP_MAX_SECONDS;
            let login_changed = event.last_login != prev_login;
            let is_active = fan_delta > 0 || login_changed;
            let career_count = if fan_delta >= CAREER_FAN_THRESHOLD {
                ((fan_delta / CAREER_AVG_FANS).max(1)) as u32
            } else {
                0
            };

            if !tight_gap {
                self.last_career_finish_at = None;
                self.career_chain_active = false;
            } else if fan_delta == 0 || gap_seconds > TRUSTED_CAREER_CHAIN_MAX_GAP_SECONDS {
                self.career_chain_active = false;
            }

            let trusted_chain_seconds = if career_count > 0 && tight_gap {
                self.last_career_finish_at.and_then(|prev_finish_at| {
                    let length_seconds = (event.snapshot_time - prev_finish_at).num_seconds();
                    if length_seconds > 0 && self.career_chain_active {
                        Some(length_seconds as u32)
                    } else {
                        None
                    }
                })
            } else {
                None
            };

            let display_length_seconds = if career_count == 1 && tight_gap {
                estimatedSingleCareerLengthSeconds(
                    event.snapshot_time,
                    event.last_login,
                    self.last_career_finish_at,
                    trusted_chain_seconds,
                )
            } else {
                None
            };

            let active_seconds = if career_count > 0 && tight_gap {
                trusted_chain_seconds
                    .unwrap_or_else(|| unobservedCareerActiveSeconds(career_count, gap_seconds))
            } else if is_active && tight_gap {
                (gap_seconds as u32).min(ACTIVE_SECONDS_CAP_PER_GAP)
            } else {
                0
            };

            if let (Some(prev_finish_at), Some(length_seconds)) =
                (self.last_career_finish_at, display_length_seconds)
            {
                let fan_gain_per_minute = fan_delta as f64 * 60.0 / length_seconds.max(1) as f64;
                let qualifies_high_fan = length_seconds < SHORT_HIGH_FAN_MAX_SECONDS
                    && trusted_chain_seconds.is_some()
                    && fan_gain_per_minute >= HIGH_FAN_GAIN_PER_SHORT_CAREER_MINUTE;
                if length_seconds < SHORT_HIGH_FAN_MAX_SECONDS && trusted_chain_seconds.is_some() {
                    self.short_runs.push(ShortRunDebug {
                        snapshot_id: event.id,
                        circle_id: event.circle_id,
                        snapshot_time: event.snapshot_time,
                        prev_finish_at,
                        last_login: event.last_login,
                        gap_seconds,
                        fan_delta,
                        active_seconds,
                        length_seconds,
                        fan_gain_per_minute,
                        qualifies_high_fan,
                    });
                }
            }

            self.transitions.push(TransitionDebug {
                snapshot_id: event.id,
                circle_id: event.circle_id,
                snapshot_time: event.snapshot_time,
                gap_seconds,
                fan_delta,
                last_login: event.last_login,
                login_changed,
                tight_gap,
                career_count,
                active_seconds,
                trusted_chain_seconds,
                display_length_seconds,
            });

            let reset_boundaries = jstResetsBetween(prev_snapshot_time, event.snapshot_time);
            let last_reset_boundary = reset_boundaries.last().copied();

            if !tight_gap {
                self.closeSessionAt(prev_snapshot_time, "observation_gap");
            } else {
                let first_reset_boundary = reset_boundaries.first().copied();
                let idle_break_cutoff = self.open_session.as_ref().and_then(|open| {
                    open.last_fan_gain_at.and_then(|last_fan_gain_at| {
                        let idle_since_gain =
                            (event.snapshot_time - last_fan_gain_at).num_seconds();
                        if idle_since_gain >= OBSERVED_SESSION_IDLE_BREAK_SECONDS {
                            Some(
                                last_fan_gain_at
                                    + chrono::Duration::seconds(
                                        OBSERVED_SESSION_IDLE_BREAK_SECONDS,
                                    ),
                            )
                        } else {
                            None
                        }
                    })
                });
                if let Some(cutoff) = idle_break_cutoff {
                    if first_reset_boundary.map_or(true, |reset_at| cutoff <= reset_at) {
                        self.closeSessionAt(cutoff, "idle_break");
                    }
                }

                for reset_at in reset_boundaries {
                    let needs_close = matches!(
                        self.open_session.as_ref(),
                        Some(open) if open.started_at < reset_at
                    );
                    if needs_close {
                        self.closeSessionAt(reset_at, "jst_reset");
                    }
                }

                if fan_delta > 0 {
                    if self.open_session.is_none() {
                        self.open_session = Some(OpenSession {
                            started_at: last_reset_boundary.unwrap_or(prev_snapshot_time),
                            last_fan_gain_at: None,
                            idle_seconds_so_far: 0,
                            active_seconds_so_far: 0,
                            careers: 0,
                            fan_gain: 0,
                        });
                    }

                    if let Some(open) = self.open_session.as_mut() {
                        open.last_fan_gain_at = Some(event.snapshot_time);
                        open.active_seconds_so_far =
                            open.active_seconds_so_far.saturating_add(active_seconds);
                        open.careers = open.careers.saturating_add(career_count);
                        open.fan_gain = open.fan_gain.saturating_add(fan_delta as u64);
                    }
                } else if let Some(open) = self.open_session.as_mut() {
                    let gap_u32 = gap_seconds as u32;
                    open.idle_seconds_so_far = open.idle_seconds_so_far.saturating_add(gap_u32);
                }
            }

            if career_count > 0 && tight_gap {
                self.last_career_finish_at = Some(event.snapshot_time);
                self.career_chain_active = gap_seconds <= TRUSTED_CAREER_CHAIN_MAX_GAP_SECONDS;
            }
        }

        fn finalize(&mut self, cutoff_at: Option<DateTime<Utc>>) {
            if let Some(cutoff_at) = cutoff_at {
                self.closeSessionAt(cutoff_at, "end_of_data");
            }
        }

        fn closeSessionAt(&mut self, cutoff_at: DateTime<Utc>, reason: &'static str) {
            let Some(open) = self.open_session.take() else {
                return;
            };
            let Some(_last_fan_gain_at) = open.last_fan_gain_at else {
                return;
            };
            let ended_at = cutoff_at;
            let duration = (ended_at - open.started_at).num_seconds();
            if duration <= 0 {
                return;
            }
            let duration_sec = duration as u32;
            let active_sec = open.active_seconds_so_far.min(duration_sec);
            let idle_sec = duration_sec.saturating_sub(active_sec);
            self.sessions.push(ClosedSessionDebug {
                record: SessionRecord {
                    started_at: open.started_at,
                    ended_at,
                    duration_sec,
                    active_sec,
                    idle_sec,
                    careers: open.careers,
                    fan_gain: open.fan_gain,
                },
                reason,
            });
        }
    }

    #[tokio::test]
    #[ignore = "manual DB-backed viewer replay probe"]
    async fn dbProbeViewerReplay() {
        dotenv().ok();

        let viewer_id = env::var("CHEAT_PROBE_VIEWER_ID")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(407_502_161_973);
        let recent_days = env::var("CHEAT_PROBE_RECENT_DAYS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(14);
        let context_radius = env::var("CHEAT_PROBE_CONTEXT_RADIUS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4);

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = PgPool::connect(&database_url).await.unwrap();

        let rows = sqlx::query(
            r#"SELECT s.id,
                      s.circle_id,
                      s.snapshot_time,
                      x.fan,
                      x.last_login
               FROM circle_member_fan_snapshots s
               CROSS JOIN LATERAL unnest(
                   s.viewer_ids,
                   s.fans,
                   ARRAY(
                       SELECT login_text::timestamp AT TIME ZONE 'UTC'
                       FROM unnest(s.last_login_times) AS login(login_text)
                   )
               ) WITH ORDINALITY AS x(viewer_id, fan, last_login, ord)
               WHERE $1 = ANY(s.viewer_ids)
                 AND x.viewer_id = $1
               ORDER BY s.snapshot_time, s.id"#,
        )
        .bind(viewer_id)
        .fetch_all(&pool)
        .await
        .unwrap();

        let events: Vec<ViewerProbeEvent> = rows
            .into_iter()
            .map(|row| ViewerProbeEvent {
                id: row.try_get("id").unwrap(),
                circle_id: row.try_get("circle_id").unwrap(),
                snapshot_time: row.try_get("snapshot_time").unwrap(),
                fans: row.try_get("fan").unwrap(),
                last_login: row.try_get("last_login").unwrap(),
            })
            .collect();

        assert!(
            !events.is_empty(),
            "viewer {viewer_id} has no snapshot history to inspect"
        );

        let mut acc = ViewerAccum::default();
        let mut probe = ViewerProbe::default();
        for event in &events {
            acc.processEvent(
                event.id,
                event.circle_id,
                event.snapshot_time,
                event.fans,
                event.last_login,
            );
            probe.observe(event);
        }
        acc.finalize();
        probe.finalize(events.last().map(|event| event.snapshot_time));

        let mut daily_rows = Vec::new();
        let mut heatmap_rows = Vec::new();
        let mut score_rows = Vec::new();
        let mut session_rows = Vec::new();
        let mut short_career_rows = Vec::new();
        acc.collectInto(
            viewer_id,
            &mut daily_rows,
            &mut heatmap_rows,
            &mut score_rows,
            &mut session_rows,
            &mut short_career_rows,
        );
        let score_row = score_rows.first().unwrap();

        let qualifying_short_runs = probe
            .short_runs
            .iter()
            .filter(|run| run.qualifies_high_fan)
            .count();
        assert_eq!(qualifying_short_runs, acc.short_high_fan_careers as usize);

        println!(
            "viewer={} events={} first_seen={} last_seen={} total_active_hours={:.2} total_careers={} short_high_fan_careers={} peak_fans_per_minute={:.1}",
            viewer_id,
            events.len(),
            acc.first_seen.unwrap(),
            acc.last_seen.unwrap(),
            acc.total_active_seconds as f64 / 3600.0,
            acc.total_careers,
            acc.short_high_fan_careers,
            acc.peak_fans_per_minute,
        );
        println!(
            "career_length_buckets_0_to_60m={:?}",
            &acc.career_length_buckets.0[..12]
        );
        println!(
            "retuned_score={} reset_breaks={}/{} max_reset_recovery={} reset_break_score={:.1} max_daily_active_hours={:.2} days_over_16h={} days_over_20h={} max_session_hours={:.2}",
            score_row.suspicion_score,
            score_row.reset_breaks,
            score_row.reset_recovery_windows,
            score_row.max_reset_recovery_seconds,
            score_row.reset_break_score,
            score_row.max_daily_active_seconds as f64 / 3600.0,
            score_row.days_over_16h,
            score_row.days_over_20h,
            score_row.max_session_seconds as f64 / 3600.0,
        );
        println!(
            "probe_score={:.1} fan_gain_mode={:.0}% fan_gain_cv={:.2} career_rhythm_cv={:.2} login_gap_cv={:.2} post_login_latency={} zero_idle_streak={} max_careers_30m={} schedule_entropy={:.2} coactivity_cluster={}",
            score_row.probe_score,
            score_row.probe_metrics.career_fan_gain_mode_share * 100.0,
            score_row.probe_metrics.career_fan_gain_cv,
            score_row.probe_metrics.career_rhythm_cv,
            score_row.probe_metrics.login_gap_cv,
            score_row.probe_metrics.post_login_latency_median_seconds,
            score_row.probe_metrics.max_zero_idle_fan_gain_streak,
            score_row.probe_metrics.max_careers_30m,
            score_row.probe_metrics.hourly_entropy,
            score_row.probe_metrics.coactivity_cluster_size,
        );
        println!(
            "trusted_short_single_career_samples={} qualifying_high_fan_samples={}",
            probe.short_runs.len(),
            qualifying_short_runs,
        );

        for run in &probe.short_runs {
            println!(
                "short_run snapshot_id={} circle_id={} snapshot_time={} prev_finish_at={} last_login={} gap_seconds={} length_seconds={} active_seconds={} fan_delta={} fan_gain_per_minute={:.1} qualifies_high_fan={}",
                run.snapshot_id,
                run.circle_id,
                run.snapshot_time,
                run.prev_finish_at,
                run.last_login,
                run.gap_seconds,
                run.length_seconds,
                run.active_seconds,
                run.fan_delta,
                run.fan_gain_per_minute,
                run.qualifies_high_fan,
            );

            if let Some(idx) = probe
                .transitions
                .iter()
                .position(|transition| transition.snapshot_id == run.snapshot_id)
            {
                let start = idx.saturating_sub(context_radius);
                let end = (idx + context_radius + 1).min(probe.transitions.len());
                println!(
                    "context_for_short_run snapshot_id={} transitions={}..{}",
                    run.snapshot_id, start, end
                );
                for transition in &probe.transitions[start..end] {
                    println!(
                        "  snapshot_id={} circle_id={} snapshot_time={} gap_seconds={} fan_delta={} login_changed={} tight_gap={} career_count={} active_seconds={} trusted_chain_seconds={:?} display_length_seconds={:?} last_login={}",
                        transition.snapshot_id,
                        transition.circle_id,
                        transition.snapshot_time,
                        transition.gap_seconds,
                        transition.fan_delta,
                        transition.login_changed,
                        transition.tight_gap,
                        transition.career_count,
                        transition.active_seconds,
                        transition.trusted_chain_seconds,
                        transition.display_length_seconds,
                        transition.last_login,
                    );
                }
            }
        }

        let recent_cutoff = acc.last_seen.unwrap() - chrono::Duration::days(recent_days);
        let mut recent_days_rows: Vec<_> = acc.daily.iter().collect();
        recent_days_rows.sort_by_key(|(day, _)| **day);
        println!("recent_daily_rows cutoff={}:", recent_cutoff.date_naive());
        for (day, bucket) in recent_days_rows.into_iter().rev() {
            if *day < recent_cutoff.date_naive() {
                break;
            }
            println!(
                "  day={} active_hours={:.2} careers={} fan_gain={} sessions={} longest_session_hours={:.2} distinct_hours={}",
                day,
                bucket.active_seconds as f64 / 3600.0,
                bucket.careers,
                bucket.fan_gain,
                bucket.sessions,
                bucket.longest_session_sec as f64 / 3600.0,
                bucket.hours_bitmap.count_ones(),
            );
        }

        probe
            .sessions
            .sort_by_key(|session| session.record.ended_at.timestamp());
        println!("recent_sessions cutoff={recent_cutoff}:");
        for session in probe.sessions.iter().rev() {
            if session.record.ended_at < recent_cutoff {
                break;
            }
            println!(
                "  started_at={} ended_at={} duration_hours={:.2} active_hours={:.2} idle_hours={:.2} fan_gain={} careers={} close_reason={}",
                session.record.started_at,
                session.record.ended_at,
                session.record.duration_sec as f64 / 3600.0,
                session.record.active_sec as f64 / 3600.0,
                session.record.idle_sec as f64 / 3600.0,
                session.record.fan_gain,
                session.record.careers,
                session.reason,
            );
        }
    }

    #[tokio::test]
    #[ignore = "manual DB-backed session duration distribution probe"]
    async fn dbSessionDurationDistributionProbe() -> anyhow::Result<()> {
        dotenv().ok();

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = PgPool::connect(&database_url).await?;

        let rebuild = runFullRebuild(&pool).await?;
        println!(
            "rebuilt_cheat_analysis snapshots_processed={} viewers_scored={} last_snapshot_id={} duration_ms={}",
            rebuild.snapshots_processed,
            rebuild.viewers_scored,
            rebuild.last_snapshot_id,
            rebuild.duration_ms,
        );

        let cohort_rows = sqlx::query(
            r#"
            WITH sessions AS (
                SELECT t.viewer_id,
                       t.rank,
                       t.duration_seconds,
                       t.active_seconds,
                       t.idle_seconds,
                       t.careers,
                       s.suspicion_score,
                       CASE
                           WHEN s.suspicion_score >= 60 THEN 'suspicious'
                           WHEN s.suspicion_score >= 30 THEN 'watch'
                           ELSE 'below'
                       END AS cohort
                FROM viewer_top_sessions t
                JOIN viewer_suspicion_scores s USING (viewer_id)
                WHERE t.duration_seconds > 0
            )
            SELECT cohort,
                   COUNT(*)::BIGINT AS sessions,
                   COUNT(DISTINCT viewer_id)::BIGINT AS viewers,
                   AVG(duration_seconds)::DOUBLE PRECISION AS avg_seconds,
                   PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_seconds)::DOUBLE PRECISION AS p50_seconds,
                   PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY duration_seconds)::DOUBLE PRECISION AS p90_seconds,
                   PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_seconds)::DOUBLE PRECISION AS p99_seconds,
                   COUNT(*) FILTER (WHERE duration_seconds BETWEEN 9900 AND 11700)::BIGINT AS near_3h_sessions,
                   COUNT(DISTINCT viewer_id) FILTER (WHERE duration_seconds BETWEEN 9900 AND 11700)::BIGINT AS near_3h_viewers,
                   COUNT(*) FILTER (WHERE rank = 1 AND duration_seconds BETWEEN 9900 AND 11700)::BIGINT AS near_3h_top1_sessions,
                   AVG(active_seconds)::DOUBLE PRECISION AS avg_active_seconds,
                   AVG(idle_seconds)::DOUBLE PRECISION AS avg_idle_seconds,
                   AVG(careers)::DOUBLE PRECISION AS avg_careers
            FROM sessions
            GROUP BY cohort
            ORDER BY CASE cohort WHEN 'suspicious' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END
            "#,
        )
        .fetch_all(&pool)
        .await?;

        println!("session_duration_cohorts near_3h = 2h45m..3h15m");
        for row in cohort_rows {
            let cohort: String = row.try_get("cohort")?;
            let sessions: i64 = row.try_get("sessions")?;
            let viewers: i64 = row.try_get("viewers")?;
            let near_3h_sessions: i64 = row.try_get("near_3h_sessions")?;
            let near_3h_viewers: i64 = row.try_get("near_3h_viewers")?;
            let near_3h_top1_sessions: i64 = row.try_get("near_3h_top1_sessions")?;
            println!(
                "  cohort={cohort} sessions={sessions} viewers={viewers} avg={} p50={} p90={} p99={} near_3h_sessions={} ({:.1}%) near_3h_viewers={} ({:.1}%) near_3h_top1={} avg_active={} avg_idle={} avg_careers={:.1}",
                formatProbeDuration(row.try_get::<f64, _>("avg_seconds")?),
                formatProbeDuration(row.try_get::<f64, _>("p50_seconds")?),
                formatProbeDuration(row.try_get::<f64, _>("p90_seconds")?),
                formatProbeDuration(row.try_get::<f64, _>("p99_seconds")?),
                near_3h_sessions,
                pct(near_3h_sessions, sessions),
                near_3h_viewers,
                pct(near_3h_viewers, viewers),
                near_3h_top1_sessions,
                formatProbeDuration(row.try_get::<f64, _>("avg_active_seconds")?),
                formatProbeDuration(row.try_get::<f64, _>("avg_idle_seconds")?),
                row.try_get::<f64, _>("avg_careers")?,
            );
        }

        let bucket_rows = sqlx::query(
            r#"
            WITH sessions AS (
                SELECT t.viewer_id,
                       t.duration_seconds,
                       s.suspicion_score,
                       CASE
                           WHEN s.suspicion_score >= 60 THEN 'suspicious'
                           WHEN s.suspicion_score >= 30 THEN 'watch'
                           ELSE 'below'
                       END AS cohort
                FROM viewer_top_sessions t
                JOIN viewer_suspicion_scores s USING (viewer_id)
                WHERE t.duration_seconds > 0
            ), buckets AS (
                SELECT cohort,
                       (ROUND(duration_seconds / 300.0) * 5)::INT AS bucket_minutes,
                       COUNT(*)::BIGINT AS sessions,
                       COUNT(DISTINCT viewer_id)::BIGINT AS viewers,
                       AVG(suspicion_score)::DOUBLE PRECISION AS avg_score
                FROM sessions
                GROUP BY cohort, bucket_minutes
            )
            SELECT *
            FROM buckets
            ORDER BY sessions DESC, bucket_minutes
            LIMIT 30
            "#,
        )
        .fetch_all(&pool)
        .await?;

        println!("top_duration_buckets rounded_to_5m");
        for row in bucket_rows {
            println!(
                "  cohort={} bucket={}m sessions={} viewers={} avg_score={:.1}",
                row.try_get::<String, _>("cohort")?,
                row.try_get::<i32, _>("bucket_minutes")?,
                row.try_get::<i64, _>("sessions")?,
                row.try_get::<i64, _>("viewers")?,
                row.try_get::<f64, _>("avg_score")?,
            );
        }

        let repeated_rows = sqlx::query(
            r#"
            WITH per_viewer AS (
                SELECT s.viewer_id,
                       s.suspicion_score,
                       COUNT(*)::INT AS top_session_count,
                       COUNT(*) FILTER (WHERE t.duration_seconds BETWEEN 9900 AND 11700)::INT AS near_3h_count,
                       AVG(t.duration_seconds)::DOUBLE PRECISION AS avg_seconds
                FROM viewer_suspicion_scores s
                JOIN viewer_top_sessions t USING (viewer_id)
                WHERE t.duration_seconds > 0
                GROUP BY s.viewer_id, s.suspicion_score
            )
            SELECT CASE
                       WHEN suspicion_score >= 60 THEN 'suspicious'
                       WHEN suspicion_score >= 30 THEN 'watch'
                       ELSE 'below'
                   END AS cohort,
                   COUNT(*)::BIGINT AS viewers,
                   COUNT(*) FILTER (WHERE near_3h_count >= 3)::BIGINT AS viewers_with_3plus_near_3h,
                   COUNT(*) FILTER (WHERE near_3h_count >= 5)::BIGINT AS viewers_with_5plus_near_3h,
                   AVG(near_3h_count::DOUBLE PRECISION / GREATEST(top_session_count, 1))::DOUBLE PRECISION AS avg_near_3h_share
            FROM per_viewer
            GROUP BY cohort
            ORDER BY MIN(CASE
                WHEN suspicion_score >= 60 THEN 0
                WHEN suspicion_score >= 30 THEN 1
                ELSE 2
            END)
            "#,
        )
        .fetch_all(&pool)
        .await?;

        println!("per_viewer_repeated_near_3h among stored top sessions");
        for row in repeated_rows {
            let viewers: i64 = row.try_get("viewers")?;
            let viewers_with_3plus: i64 = row.try_get("viewers_with_3plus_near_3h")?;
            let viewers_with_5plus: i64 = row.try_get("viewers_with_5plus_near_3h")?;
            println!(
                "  cohort={} viewers={} 3plus={} ({:.1}%) 5plus={} ({:.1}%) avg_near_3h_share={:.1}%",
                row.try_get::<String, _>("cohort")?,
                viewers,
                viewers_with_3plus,
                pct(viewers_with_3plus, viewers),
                viewers_with_5plus,
                pct(viewers_with_5plus, viewers),
                row.try_get::<f64, _>("avg_near_3h_share")? * 100.0,
            );
        }

        Ok(())
    }

    fn formatProbeDuration(seconds: f64) -> String {
        let minutes = (seconds / 60.0).round() as i64;
        format!("{}h{:02}m", minutes / 60, minutes % 60)
    }

    fn pct(part: i64, total: i64) -> f64 {
        if total <= 0 {
            0.0
        } else {
            part as f64 * 100.0 / total as f64
        }
    }

    #[test]
    fn rateDiagnosticSchemaVerificationIsReadOnly() {
        let query = VERIFY_RATE_DIAGNOSTIC_COLUMNS_SQL.to_ascii_uppercase();

        assert!(query.trim_start().starts_with("SELECT"));
        assert!(!query.contains("ALTER TABLE"));
        assert!(!query.contains("CREATE INDEX"));
    }

    #[test]
    fn averageCareerDailyMetricUsesObservedDays() {
        let careers_per_day = avgCareersPerObservedDay(48, 3);

        assert!((careers_per_day - 16.0).abs() < f64::EPSILON);
    }

    #[test]
    fn careerRateSamplesUseFinishToFinishIntervalsUnderTwoHours() {
        let reference_at = ts("2026-05-20T12:00:00Z");
        let samples = vec![
            CareerRateSample {
                finished_at: reference_at - chrono::Duration::days(40),
                seconds: 30 * 60,
            },
            CareerRateSample {
                finished_at: reference_at - chrono::Duration::days(2),
                seconds: 50 * 60,
            },
            CareerRateSample {
                finished_at: reference_at - chrono::Duration::days(1),
                seconds: 20 * 60,
            },
            CareerRateSample {
                finished_at: reference_at,
                seconds: 121 * 60,
            },
        ];

        let breakdown = careerRateBreakdown(&samples, reference_at);

        assert_eq!(breakdown.all.sample_count, 3);
        assert_eq!(breakdown.all.sample_seconds, 100 * 60);
        assert!((breakdown.all.careers_per_hour - 1.8).abs() < 1e-9);
        assert_eq!(breakdown.last_30d.sample_count, 2);
        assert_eq!(breakdown.last_3d.sample_seconds, 70 * 60);
        assert_eq!(breakdown.last_20.sample_count, 3);
    }

    #[test]
    fn careerRateWindowsWinsorizeOutlierIntervals() {
        let reference_at = ts("2026-05-20T12:00:00Z");
        let mut samples: Vec<CareerRateSample> = (0..18)
            .map(|idx| CareerRateSample {
                finished_at: reference_at + chrono::Duration::minutes(idx),
                seconds: 20 * 60,
            })
            .collect();
        samples.push(CareerRateSample {
            finished_at: reference_at + chrono::Duration::minutes(18),
            seconds: 100 * 60,
        });
        samples.push(CareerRateSample {
            finished_at: reference_at + chrono::Duration::minutes(19),
            seconds: 100 * 60,
        });

        let breakdown = careerRateBreakdown(&samples, reference_at);

        assert_eq!(breakdown.all.sample_count, 20);
        assert_eq!(breakdown.all.sample_seconds, 440 * 60);
        assert_eq!(breakdown.last_20.sample_count, 20);
        assert_eq!(breakdown.last_20.sample_seconds, 440 * 60);
        assert_eq!(breakdown.last_30d.sample_seconds, 440 * 60);
        assert_eq!(breakdown.last_7d.sample_seconds, 440 * 60);
        assert_eq!(breakdown.last_3d.sample_seconds, 440 * 60);
        assert!((breakdown.last_20.careers_per_hour - (20.0 / (440.0 / 60.0))).abs() < 1e-9);
    }

    #[test]
    fn careerRateWindowsKeepSmallSamplesRaw() {
        let reference_at = ts("2026-05-20T12:00:00Z");
        let samples = vec![
            CareerRateSample {
                finished_at: reference_at,
                seconds: 20 * 60,
            },
            CareerRateSample {
                finished_at: reference_at + chrono::Duration::minutes(1),
                seconds: 100 * 60,
            },
        ];

        let breakdown = careerRateBreakdown(&samples, reference_at);

        assert_eq!(breakdown.all.sample_count, 2);
        assert_eq!(breakdown.all.sample_seconds, 120 * 60);
        assert!((breakdown.all.careers_per_hour - 1.0).abs() < 1e-9);
        assert_eq!(breakdown.last_30d.sample_seconds, 120 * 60);
        assert_eq!(breakdown.last_7d.sample_seconds, 120 * 60);
        assert_eq!(breakdown.last_3d.sample_seconds, 120 * 60);
        assert_eq!(breakdown.last_20.sample_count, 2);
        assert_eq!(breakdown.last_20.sample_seconds, 120 * 60);
        assert!((breakdown.last_20.careers_per_hour - 1.0).abs() < 1e-9);
    }

    #[test]
    fn observedActivitySessionIgnoresLoginChanges() {
        let mut acc = ViewerAccum::default();
        let t0 = ts("2026-05-20T12:00:00Z");
        let t5 = ts("2026-05-20T12:05:00Z");
        let t10 = ts("2026-05-20T12:10:00Z");
        let t15 = ts("2026-05-20T12:15:00Z");
        let login0 = ts("2026-05-20T11:50:00Z");
        let login1 = ts("2026-05-20T12:09:00Z");

        acc.processEvent(1, 1, t0, 0, login0);
        acc.processEvent(2, 1, t5, 50_000, login0);
        acc.processEvent(3, 1, t10, 50_000, login1);
        acc.processEvent(4, 1, t15, 100_000, login1);
        acc.finalize();

        let mut daily_rows = Vec::new();
        let mut heatmap_rows = Vec::new();
        let mut score_rows = Vec::new();
        let mut session_rows = Vec::new();
        let mut short_career_rows = Vec::new();
        acc.collectInto(
            1,
            &mut daily_rows,
            &mut heatmap_rows,
            &mut score_rows,
            &mut session_rows,
            &mut short_career_rows,
        );

        assert_eq!(session_rows.len(), 1);
        let session = &session_rows[0];
        assert_eq!(session.started_at, t0);
        assert_eq!(session.ended_at, t15);
        assert_eq!(session.duration_seconds, 900);
        assert_eq!(session.active_seconds, 600);
        assert_eq!(session.idle_seconds, 300);
        assert_eq!(session.fan_gain, 100_000);
        assert_eq!(acc.max_session_seconds, 900);
    }

    #[test]
    fn observedActivitySessionBreaksAfterLongIdle() {
        let mut acc = ViewerAccum::default();
        let login = ts("2026-05-20T11:50:00Z");

        acc.processEvent(1, 1, ts("2026-05-20T12:00:00Z"), 0, login);
        acc.processEvent(2, 1, ts("2026-05-20T12:05:00Z"), 50_000, login);

        for (idx, snapshot_time) in [
            "2026-05-20T12:10:00Z",
            "2026-05-20T12:15:00Z",
            "2026-05-20T12:20:00Z",
            "2026-05-20T12:25:00Z",
            "2026-05-20T12:30:00Z",
            "2026-05-20T12:35:00Z",
            "2026-05-20T12:40:00Z",
            "2026-05-20T12:45:00Z",
            "2026-05-20T12:50:00Z",
            "2026-05-20T12:55:00Z",
            "2026-05-20T13:00:00Z",
            "2026-05-20T13:05:00Z",
        ]
        .into_iter()
        .enumerate()
        {
            acc.processEvent(3 + idx as i64, 1, ts(snapshot_time), 50_000, login);
        }

        acc.processEvent(20, 1, ts("2026-05-20T13:10:00Z"), 100_000, login);
        acc.finalize();

        let mut daily_rows = Vec::new();
        let mut heatmap_rows = Vec::new();
        let mut score_rows = Vec::new();
        let mut session_rows = Vec::new();
        let mut short_career_rows = Vec::new();
        acc.collectInto(
            1,
            &mut daily_rows,
            &mut heatmap_rows,
            &mut score_rows,
            &mut session_rows,
            &mut short_career_rows,
        );

        assert_eq!(session_rows.len(), 1);
        assert_eq!(session_rows[0].duration_seconds, 4200);
        assert_eq!(session_rows[0].active_seconds, 600);
        assert_eq!(session_rows[0].idle_seconds, 3600);
        assert_eq!(session_rows[0].session_count, 2);
        assert_eq!(session_rows[0].sessions[0].duration_sec, 3900);
        assert_eq!(
            session_rows[0].sessions[1].started_at,
            ts("2026-05-20T13:05:00Z")
        );
        assert_eq!(
            session_rows[0].sessions[1].ended_at,
            ts("2026-05-20T13:10:00Z")
        );
        assert_eq!(session_rows[0].sessions[1].duration_sec, 300);
    }

    #[test]
    fn longGapCrossingJstResetClosesAtLastObservedSnapshot() {
        let mut acc = ViewerAccum::default();
        let login = ts("2026-04-01T08:00:00Z");

        acc.processEvent(1, 1, ts("2026-04-01T08:32:50Z"), 0, login);
        acc.processEvent(2, 1, ts("2026-04-01T08:47:50Z"), 958_057, login);
        acc.processEvent(3, 1, ts("2026-04-01T15:05:00Z"), 958_057, login);
        acc.finalize();

        let mut daily_rows = Vec::new();
        let mut heatmap_rows = Vec::new();
        let mut score_rows = Vec::new();
        let mut session_rows = Vec::new();
        let mut short_career_rows = Vec::new();
        acc.collectInto(
            1,
            &mut daily_rows,
            &mut heatmap_rows,
            &mut score_rows,
            &mut session_rows,
            &mut short_career_rows,
        );

        assert_eq!(session_rows.len(), 1);
        assert_eq!(session_rows[0].started_at, ts("2026-04-01T08:32:50Z"));
        assert_eq!(session_rows[0].ended_at, ts("2026-04-01T08:47:50Z"));
        assert_eq!(session_rows[0].duration_seconds, 900);
        assert_eq!(session_rows[0].active_seconds, 900);
        assert_eq!(session_rows[0].idle_seconds, 0);
    }

    #[test]
    fn isolatedRecentLoginCareerLengthIsFlooredForDisplay() {
        let mut acc = ViewerAccum::default();
        let first_seen = ts("2026-05-20T12:00:00Z");
        let finish = ts("2026-05-20T12:05:00Z");

        acc.processEvent(1, 1, first_seen, 0, first_seen);
        acc.processEvent(2, 1, finish, 900_000, ts("2026-05-20T12:00:30Z"));

        assert_eq!(acc.career_length_buckets.0[0], 0);
        assert_eq!(acc.career_length_buckets.0[1], 0);
        assert_eq!(acc.career_length_buckets.0[2], 1);
        assert_eq!(acc.career_length_buckets.0[3], 0);
        assert_eq!(
            acc.career_lengths_last20
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![600]
        );
        assert_eq!(acc.short_high_fan_careers, 0);
        assert_eq!(acc.short_fan_gain_score, 0.0);
        assert!(acc.short_career_snapshots.is_empty());
        assert_eq!(acc.peak_fans_per_minute, 90_000.0);
        assert_eq!(acc.total_careers, 1);
        assert_eq!(acc.total_active_seconds, 600);
    }

    #[test]
    fn trustedChainedCareerFinishEntersLengthBuckets() {
        let mut acc = ViewerAccum::default();
        let first_seen = ts("2026-05-20T12:00:00Z");
        let first_finish = ts("2026-05-20T12:05:00Z");
        let second_finish = ts("2026-05-20T12:15:00Z");

        acc.processEvent(1, 1, first_seen, 0, first_seen);
        acc.processEvent(2, 1, first_finish, 900_000, first_seen);
        acc.processEvent(3, 1, second_finish, 1_800_000, first_seen);
        acc.finalize();

        assert_eq!(acc.career_length_buckets.0[2], 2);
        assert_eq!(acc.career_length_buckets.0[3], 0);
        assert_eq!(
            acc.career_lengths_last20
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![600, 600]
        );
        assert_eq!(acc.short_high_fan_careers, 1);
        assert!(acc.short_fan_gain_score > 0.0);
        let short_snapshot = acc.short_career_snapshots.front().unwrap();
        assert_eq!(short_snapshot.snapshot_id, 3);
        assert_eq!(short_snapshot.previous_snapshot_id, 2);
        assert_eq!(short_snapshot.previous_snapshot_fans, 900_000);
        assert_eq!(short_snapshot.current_fans, 1_800_000);
        assert_eq!(short_snapshot.fan_gain, 900_000);
        assert_eq!(short_snapshot.previous_career_gap_seconds, 600);
        assert!(short_snapshot.short_training_score > 0.0);

        let mut daily_rows = Vec::new();
        let mut heatmap_rows = Vec::new();
        let mut score_rows = Vec::new();
        let mut session_rows = Vec::new();
        let mut short_career_rows = Vec::new();
        acc.collectInto(
            1,
            &mut daily_rows,
            &mut heatmap_rows,
            &mut score_rows,
            &mut session_rows,
            &mut short_career_rows,
        );

        assert_eq!(score_rows[0].career_rate_sample_count, 1);
        assert_eq!(score_rows[0].career_rate_sample_seconds, 600);
        assert_eq!(score_rows[0].careers_per_active_hour, 6.0);
    }

    #[test]
    fn gapResumeCareerDoesNotSeedShortCareerEvidence() {
        let mut acc = ViewerAccum::default();
        let login = ts("2026-04-08T02:12:43Z");

        acc.processEvent(1, 1, ts("2026-04-08T03:17:52Z"), 4_988_487_481, login);
        acc.processEvent(2, 1, ts("2026-04-08T03:45:11Z"), 4_989_530_087, login);
        acc.processEvent(3, 1, ts("2026-04-08T03:50:10Z"), 4_990_617_195, login);
        acc.finalize();

        assert_eq!(acc.short_high_fan_careers, 0);
        assert_eq!(acc.short_fan_gain_score, 0.0);
        assert!(acc.short_career_snapshots.is_empty());
    }

    #[test]
    fn shortTrainingSnapshotKeepsThreeBeforeAndAfterContextRows() {
        let mut acc = ViewerAccum::default();
        let login = ts("2026-05-20T11:50:00Z");

        acc.processEvent(1, 1, ts("2026-05-20T12:00:00Z"), 0, login);
        acc.processEvent(2, 1, ts("2026-05-20T12:05:00Z"), 0, login);
        acc.processEvent(3, 1, ts("2026-05-20T12:10:00Z"), 0, login);
        acc.processEvent(4, 1, ts("2026-05-20T12:15:00Z"), 900_000, login);
        acc.processEvent(5, 1, ts("2026-05-20T12:20:00Z"), 1_800_000, login);
        acc.processEvent(6, 1, ts("2026-05-20T12:25:00Z"), 1_800_000, login);
        acc.processEvent(7, 1, ts("2026-05-20T12:30:00Z"), 1_800_000, login);
        acc.processEvent(8, 1, ts("2026-05-20T12:35:00Z"), 1_800_000, login);
        acc.finalize();

        let short_snapshot = acc.short_career_snapshots.front().unwrap();
        assert_eq!(short_snapshot.snapshot_id, 5);
        assert_eq!(short_snapshot.prior_snapshots.len(), 3);
        assert_eq!(short_snapshot.prior_snapshots[0].snapshot_id, 2);
        assert_eq!(short_snapshot.prior_snapshots[1].snapshot_id, 3);
        assert_eq!(short_snapshot.prior_snapshots[2].snapshot_id, 4);
        assert_eq!(short_snapshot.next_snapshots.len(), 3);
        assert_eq!(short_snapshot.next_snapshots[0].snapshot_id, 6);
        assert_eq!(short_snapshot.next_snapshots[1].snapshot_id, 7);
        assert_eq!(short_snapshot.next_snapshots[2].snapshot_id, 8);
    }

    #[test]
    fn broadHeatmapNeedsDailyVolumeToScoreLike247() {
        let example_avg_active_seconds = 420_849.0 / 77.0;

        assert!(!is247Schedule(155, example_avg_active_seconds, 77));
        assert!(coverageScheduleScore(155, example_avg_active_seconds) < 2.0);
        assert_eq!(normalizedRateScore(36_000.0, 50_000.0, 6.0), 4.32);
        assert_eq!(normalizedRateScore(218_000.0, 180_000.0, 6.0), 6.0);
    }

    #[test]
    fn resetBreaksNeedSustainedVolumeContext() {
        let arc_avg_active_seconds = 432_509.0 / 80.0;
        assert_eq!(
            resetBreakScore(3, 5, arc_avg_active_seconds, 12_591, 0),
            0.0
        );

        let long_hour_avg_active_seconds = 863.0 * 3600.0 / 80.0;
        assert!(resetBreakScore(11, 71, long_hour_avg_active_seconds, 17 * 3600, 10) > 6.0);
    }

    #[test]
    fn weakContextProbesStayQuietForArcShape() {
        let arc_avg_active_seconds = 432_509.0 / 80.0;

        assert_eq!(loginRegularityScore(459, 0.4414, 0.4858), 0.0);
        assert_eq!(serviceGapResumeScore(16, arc_avg_active_seconds, 80), 0.0);
    }
}
