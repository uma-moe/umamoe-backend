use super::*;

pub(super) async fn applyHallMetadata(pool: &PgPool, rows: &mut [ScoreRow]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let viewer_ids: Vec<i64> = rows.iter().map(|row| row.viewer_id).collect();
    let circle_ids: Vec<Option<i64>> = rows.iter().map(|row| row.circle_id).collect();
    let metadata_rows = sqlx::query(
        "SELECT ids.viewer_id, \
                t.name AS trainer_name, \
                ids.circle_id, \
                c.name AS circle_name, \
                c.monthly_rank AS circle_monthly_rank \
         FROM UNNEST($1::bigint[], $2::bigint[]) AS ids(viewer_id, circle_id) \
         LEFT JOIN trainer t ON t.account_id::BIGINT = ids.viewer_id \
         LEFT JOIN circles c ON c.circle_id = ids.circle_id",
    )
    .bind(&viewer_ids)
    .bind(&circle_ids)
    .fetch_all(pool)
    .await?;

    let mut metadata: HashMap<i64, HallMetadata> = HashMap::with_capacity(metadata_rows.len());
    for row in metadata_rows {
        let viewer_id: i64 = row.try_get("viewer_id")?;
        metadata.insert(
            viewer_id,
            HallMetadata {
                trainer_name: row.try_get("trainer_name")?,
                circle_id: row.try_get("circle_id")?,
                circle_name: row.try_get("circle_name")?,
                circle_monthly_rank: row.try_get("circle_monthly_rank")?,
            },
        );
    }

    for row in rows {
        if let Some(meta) = metadata.remove(&row.viewer_id) {
            row.trainer_name = meta.trainer_name;
            row.circle_id = meta.circle_id;
            row.circle_name = meta.circle_name;
            row.circle_monthly_rank = meta.circle_monthly_rank;
        }
    }

    Ok(())
}

pub(super) async fn deleteViewerAggregates(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    viewer_ids: &[i64],
) -> anyhow::Result<()> {
    if viewer_ids.is_empty() {
        return Ok(());
    }

    sqlx::query("DELETE FROM viewer_activity_daily WHERE viewer_id = ANY($1)")
        .bind(viewer_ids)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM viewer_activity_heatmap WHERE viewer_id = ANY($1)")
        .bind(viewer_ids)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM viewer_suspicion_scores WHERE viewer_id = ANY($1)")
        .bind(viewer_ids)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM viewer_top_sessions WHERE viewer_id = ANY($1)")
        .bind(viewer_ids)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM viewer_short_career_snapshots WHERE viewer_id = ANY($1)")
        .bind(viewer_ids)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

pub(super) async fn insertDaily(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[DailyRow],
) -> anyhow::Result<()> {
    for chunk in rows.chunks(CHUNK_ROWS) {
        let mut viewer_id: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut day: Vec<NaiveDate> = Vec::with_capacity(chunk.len());
        let mut active_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut careers: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut fan_gain: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut sessions: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut longest_session_sec: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut distinct_hours: Vec<i16> = Vec::with_capacity(chunk.len());
        for r in chunk {
            viewer_id.push(r.viewer_id);
            day.push(r.day);
            active_seconds.push(r.active_seconds);
            careers.push(r.careers);
            fan_gain.push(r.fan_gain);
            sessions.push(r.sessions);
            longest_session_sec.push(r.longest_session_sec);
            distinct_hours.push(r.distinct_hours);
        }
        sqlx::query(
            "INSERT INTO viewer_activity_daily \
             (viewer_id, day, active_seconds, careers, fan_gain, sessions, \
              longest_session_sec, distinct_hours) \
             SELECT * FROM UNNEST($1::bigint[], $2::date[], $3::int[], $4::int[], \
                                  $5::bigint[], $6::int[], $7::int[], $8::smallint[])",
        )
        .bind(&viewer_id)
        .bind(&day)
        .bind(&active_seconds)
        .bind(&careers)
        .bind(&fan_gain)
        .bind(&sessions)
        .bind(&longest_session_sec)
        .bind(&distinct_hours)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn insertHeatmap(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[HeatmapRow],
) -> anyhow::Result<()> {
    for chunk in rows.chunks(CHUNK_ROWS) {
        let mut viewer_id: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut dow: Vec<i16> = Vec::with_capacity(chunk.len());
        let mut hour: Vec<i16> = Vec::with_capacity(chunk.len());
        let mut active_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut careers: Vec<i32> = Vec::with_capacity(chunk.len());
        for r in chunk {
            viewer_id.push(r.viewer_id);
            dow.push(r.dow);
            hour.push(r.hour);
            active_seconds.push(r.active_seconds);
            careers.push(r.careers);
        }
        sqlx::query(
            "INSERT INTO viewer_activity_heatmap \
             (viewer_id, dow, hour, active_seconds, careers) \
             SELECT * FROM UNNEST($1::bigint[], $2::smallint[], $3::smallint[], \
                                  $4::int[], $5::int[])",
        )
        .bind(&viewer_id)
        .bind(&dow)
        .bind(&hour)
        .bind(&active_seconds)
        .bind(&careers)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn insertScores(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[ScoreRow],
) -> anyhow::Result<()> {
    for chunk in rows.chunks(CHUNK_ROWS) {
        let mut viewer_id: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut trainer_name: Vec<Option<String>> = Vec::with_capacity(chunk.len());
        let mut circle_id: Vec<Option<i64>> = Vec::with_capacity(chunk.len());
        let mut circle_name: Vec<Option<String>> = Vec::with_capacity(chunk.len());
        let mut circle_monthly_rank: Vec<Option<i32>> = Vec::with_capacity(chunk.len());
        let mut first_seen: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut last_seen: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut days_observed: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut days_active: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut total_active_seconds: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut total_fan_gain: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut total_careers: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut careers_per_active_hour: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut career_rate_sample_count: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut career_rate_sample_seconds: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut career_rate_breakdown_text: Vec<String> = Vec::with_capacity(chunk.len());
        let mut avg_careers_per_day: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut avg_career_length_last20_seconds: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut career_length_buckets_text: Vec<String> = Vec::with_capacity(chunk.len());
        let mut short_high_fan_careers: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut short_fan_gain_score: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut short_fan_gain_score_buckets_text: Vec<String> = Vec::with_capacity(chunk.len());
        let mut short_career_avg_fan_gain: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut short_career_p50_fan_gain: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut short_career_p90_fan_gain: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut short_career_p95_fan_gain: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut short_career_max_fan_gain: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut recent_fan_gain_3d: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut baseline_fan_gain_14d: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut recent_fans_per_day: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut baseline_fans_per_day: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut fan_gain_spike_ratio: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut behavior_change_score: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut fans_per_active_minute: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut peak_fans_per_minute: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut high_fan_rate_windows: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut high_fan_rate_total_fan_gain: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut high_fan_rate_total_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut max_daily_active_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut max_daily_careers: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut max_session_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut days_over_16h: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut days_over_20h: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut reset_recovery_windows: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut reset_breaks: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut max_reset_recovery_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut reset_break_score: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut probe_score: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut probe_metrics_text: Vec<String> = Vec::with_capacity(chunk.len());
        let mut distinct_weekly_hour_buckets: Vec<i16> = Vec::with_capacity(chunk.len());
        let mut flag_no_sleep: Vec<bool> = Vec::with_capacity(chunk.len());
        let mut flag_extreme_session: Vec<bool> = Vec::with_capacity(chunk.len());
        let mut flag_inhuman_career_rate: Vec<bool> = Vec::with_capacity(chunk.len());
        let mut flag_247: Vec<bool> = Vec::with_capacity(chunk.len());
        let mut flag_marathon: Vec<bool> = Vec::with_capacity(chunk.len());
        let mut suspicion_score: Vec<i32> = Vec::with_capacity(chunk.len());
        for r in chunk {
            viewer_id.push(r.viewer_id);
            trainer_name.push(r.trainer_name.clone());
            circle_id.push(r.circle_id);
            circle_name.push(r.circle_name.clone());
            circle_monthly_rank.push(r.circle_monthly_rank);
            first_seen.push(r.first_seen);
            last_seen.push(r.last_seen);
            days_observed.push(r.days_observed);
            days_active.push(r.days_active);
            total_active_seconds.push(r.total_active_seconds);
            total_fan_gain.push(r.total_fan_gain);
            total_careers.push(r.total_careers);
            careers_per_active_hour.push(r.careers_per_active_hour);
            career_rate_sample_count.push(r.career_rate_sample_count);
            career_rate_sample_seconds.push(r.career_rate_sample_seconds);
            career_rate_breakdown_text.push(serde_json::to_string(&r.career_rate_breakdown)?);
            avg_careers_per_day.push(r.avg_careers_per_day);
            avg_career_length_last20_seconds.push(r.avg_career_length_last20_seconds);
            // Postgres array literal: "{1,2,3,...}". Cast to integer[] in
            // the SELECT below since UNNEST can't emit per-row arrays.
            let mut lit = String::with_capacity(r.career_length_buckets.len() * 3 + 2);
            lit.push('{');
            for (i, n) in r.career_length_buckets.iter().enumerate() {
                if i > 0 {
                    lit.push(',');
                }
                lit.push_str(&n.to_string());
            }
            lit.push('}');
            career_length_buckets_text.push(lit);
            short_high_fan_careers.push(r.short_high_fan_careers);
            short_fan_gain_score.push(r.short_fan_gain_score);
            let mut score_lit = String::with_capacity(r.short_fan_gain_score_buckets.len() * 5 + 2);
            score_lit.push('{');
            for (i, n) in r.short_fan_gain_score_buckets.iter().enumerate() {
                if i > 0 {
                    score_lit.push(',');
                }
                score_lit.push_str(&n.to_string());
            }
            score_lit.push('}');
            short_fan_gain_score_buckets_text.push(score_lit);
            short_career_avg_fan_gain.push(r.short_career_avg_fan_gain);
            short_career_p50_fan_gain.push(r.short_career_p50_fan_gain);
            short_career_p90_fan_gain.push(r.short_career_p90_fan_gain);
            short_career_p95_fan_gain.push(r.short_career_p95_fan_gain);
            short_career_max_fan_gain.push(r.short_career_max_fan_gain);
            recent_fan_gain_3d.push(r.recent_fan_gain_3d);
            baseline_fan_gain_14d.push(r.baseline_fan_gain_14d);
            recent_fans_per_day.push(r.recent_fans_per_day);
            baseline_fans_per_day.push(r.baseline_fans_per_day);
            fan_gain_spike_ratio.push(r.fan_gain_spike_ratio);
            behavior_change_score.push(r.behavior_change_score);
            fans_per_active_minute.push(r.fans_per_active_minute);
            peak_fans_per_minute.push(r.peak_fans_per_minute);
            high_fan_rate_windows.push(r.high_fan_rate_windows);
            high_fan_rate_total_fan_gain.push(r.high_fan_rate_total_fan_gain);
            high_fan_rate_total_seconds.push(r.high_fan_rate_total_seconds);
            max_daily_active_seconds.push(r.max_daily_active_seconds);
            max_daily_careers.push(r.max_daily_careers);
            max_session_seconds.push(r.max_session_seconds);
            days_over_16h.push(r.days_over_16h);
            days_over_20h.push(r.days_over_20h);
            reset_recovery_windows.push(r.reset_recovery_windows);
            reset_breaks.push(r.reset_breaks);
            max_reset_recovery_seconds.push(r.max_reset_recovery_seconds);
            reset_break_score.push(r.reset_break_score);
            probe_score.push(r.probe_score);
            probe_metrics_text.push(serde_json::to_string(&r.probe_metrics)?);
            distinct_weekly_hour_buckets.push(r.distinct_weekly_hour_buckets);
            flag_no_sleep.push(r.flag_no_sleep);
            flag_extreme_session.push(r.flag_extreme_session);
            flag_inhuman_career_rate.push(r.flag_inhuman_career_rate);
            flag_247.push(r.flag_247);
            flag_marathon.push(r.flag_marathon);
            suspicion_score.push(r.suspicion_score);
        }
        sqlx::query(
            "INSERT INTO viewer_suspicion_scores ( \
                viewer_id, trainer_name, circle_id, circle_name, circle_monthly_rank, \
                first_seen, last_seen, days_observed, days_active, \
                total_active_seconds, total_fan_gain, total_careers, careers_per_active_hour, \
                avg_career_length_last20_seconds, career_length_buckets, \
                short_high_fan_careers, short_fan_gain_score, short_fan_gain_score_buckets, \
                short_career_avg_fan_gain, short_career_p50_fan_gain, short_career_p90_fan_gain, \
                short_career_p95_fan_gain, short_career_max_fan_gain, \
                recent_fan_gain_3d, baseline_fan_gain_14d, recent_fans_per_day, \
                baseline_fans_per_day, fan_gain_spike_ratio, behavior_change_score, \
                fans_per_active_minute, peak_fans_per_minute, \
                max_daily_active_seconds, max_daily_careers, max_session_seconds, \
                days_over_16h, days_over_20h, \
                reset_recovery_windows, reset_breaks, max_reset_recovery_seconds, reset_break_score, \
                probe_score, probe_metrics, \
                distinct_weekly_hour_buckets, flag_no_sleep, flag_extreme_session, \
                     flag_inhuman_career_rate, flag_247, flag_marathon, suspicion_score, \
                     avg_careers_per_day, career_rate_sample_count, career_rate_sample_seconds, \
                     high_fan_rate_windows, high_fan_rate_total_fan_gain, \
                     high_fan_rate_total_seconds, career_rate_breakdown, refreshed_at) \
             SELECT viewer_id, trainer_name, circle_id, circle_name, circle_monthly_rank, \
                    first_seen, last_seen, days_observed, days_active, \
                    total_active_seconds, total_fan_gain, total_careers, careers_per_active_hour, \
                    avg_career_length_last20_seconds, career_length_buckets::integer[], \
                    short_high_fan_careers, short_fan_gain_score, short_fan_gain_score_buckets::double precision[], \
                    short_career_avg_fan_gain, short_career_p50_fan_gain, short_career_p90_fan_gain, \
                    short_career_p95_fan_gain, short_career_max_fan_gain, \
                    recent_fan_gain_3d, baseline_fan_gain_14d, recent_fans_per_day, \
                    baseline_fans_per_day, fan_gain_spike_ratio, behavior_change_score, \
                    fans_per_active_minute, peak_fans_per_minute, \
                    max_daily_active_seconds, max_daily_careers, max_session_seconds, \
                    days_over_16h, days_over_20h, \
                    reset_recovery_windows, reset_breaks, max_reset_recovery_seconds, reset_break_score, \
                    probe_score, probe_metrics::jsonb, \
                    distinct_weekly_hour_buckets, flag_no_sleep, flag_extreme_session, \
                          flag_inhuman_career_rate, flag_247, flag_marathon, suspicion_score, \
                          avg_careers_per_day, career_rate_sample_count, career_rate_sample_seconds, \
                          high_fan_rate_windows, high_fan_rate_total_fan_gain, \
                          high_fan_rate_total_seconds, career_rate_breakdown::jsonb, NOW() \
             FROM UNNEST( \
                       $1::bigint[], $2::text[], $3::bigint[], $4::text[], $5::int[], \
                       $6::timestamptz[], $7::timestamptz[], $8::int[], $9::int[], \
                       $10::bigint[], $11::bigint[], $12::int[], $13::double precision[], \
                             $14::double precision[], $15::text[], $16::int[], \
                             $17::double precision[], $18::text[], \
                             $19::double precision[], $20::double precision[], $21::double precision[], \
                             $22::double precision[], $23::double precision[], \
                             $24::bigint[], $25::bigint[], $26::double precision[], \
                             $27::double precision[], $28::double precision[], $29::double precision[], \
                             $30::double precision[], $31::double precision[], \
                             $32::int[], $33::int[], $34::int[], $35::int[], $36::int[], \
                             $37::int[], $38::int[], $39::int[], $40::double precision[], \
                             $41::double precision[], $42::text[], \
                             $43::smallint[], $44::boolean[], $45::boolean[], $46::boolean[], \
                             $47::boolean[], $48::boolean[], $49::int[], \
                             $50::double precision[], $51::int[], $52::bigint[], \
                             $53::int[], $54::bigint[], $55::int[], $56::text[] \
                      ) AS u(viewer_id, trainer_name, circle_id, circle_name, circle_monthly_rank, \
                          first_seen, last_seen, days_observed, days_active, \
                    total_active_seconds, total_fan_gain, total_careers, careers_per_active_hour, \
                          avg_career_length_last20_seconds, career_length_buckets, \
                              short_high_fan_careers, short_fan_gain_score, short_fan_gain_score_buckets, \
                              short_career_avg_fan_gain, short_career_p50_fan_gain, short_career_p90_fan_gain, \
                              short_career_p95_fan_gain, short_career_max_fan_gain, \
                              recent_fan_gain_3d, baseline_fan_gain_14d, recent_fans_per_day, \
                              baseline_fans_per_day, fan_gain_spike_ratio, behavior_change_score, \
                          fans_per_active_minute, peak_fans_per_minute, \
                    max_daily_active_seconds, max_daily_careers, max_session_seconds, \
                    days_over_16h, days_over_20h, \
                    reset_recovery_windows, reset_breaks, max_reset_recovery_seconds, reset_break_score, \
                    probe_score, probe_metrics, \
                    distinct_weekly_hour_buckets, flag_no_sleep, flag_extreme_session, \
                    flag_inhuman_career_rate, flag_247, flag_marathon, suspicion_score, \
                    avg_careers_per_day, career_rate_sample_count, career_rate_sample_seconds, \
                    high_fan_rate_windows, high_fan_rate_total_fan_gain, \
                    high_fan_rate_total_seconds, career_rate_breakdown)",
        )
        .bind(&viewer_id)
        .bind(&trainer_name)
        .bind(&circle_id)
        .bind(&circle_name)
        .bind(&circle_monthly_rank)
        .bind(&first_seen)
        .bind(&last_seen)
        .bind(&days_observed)
        .bind(&days_active)
        .bind(&total_active_seconds)
        .bind(&total_fan_gain)
        .bind(&total_careers)
        .bind(&careers_per_active_hour)
        .bind(&avg_career_length_last20_seconds)
        .bind(&career_length_buckets_text)
        .bind(&short_high_fan_careers)
        .bind(&short_fan_gain_score)
        .bind(&short_fan_gain_score_buckets_text)
        .bind(&short_career_avg_fan_gain)
        .bind(&short_career_p50_fan_gain)
        .bind(&short_career_p90_fan_gain)
        .bind(&short_career_p95_fan_gain)
        .bind(&short_career_max_fan_gain)
        .bind(&recent_fan_gain_3d)
        .bind(&baseline_fan_gain_14d)
        .bind(&recent_fans_per_day)
        .bind(&baseline_fans_per_day)
        .bind(&fan_gain_spike_ratio)
        .bind(&behavior_change_score)
        .bind(&fans_per_active_minute)
        .bind(&peak_fans_per_minute)
        .bind(&max_daily_active_seconds)
        .bind(&max_daily_careers)
        .bind(&max_session_seconds)
        .bind(&days_over_16h)
        .bind(&days_over_20h)
        .bind(&reset_recovery_windows)
        .bind(&reset_breaks)
        .bind(&max_reset_recovery_seconds)
        .bind(&reset_break_score)
        .bind(&probe_score)
        .bind(&probe_metrics_text)
        .bind(&distinct_weekly_hour_buckets)
        .bind(&flag_no_sleep)
        .bind(&flag_extreme_session)
        .bind(&flag_inhuman_career_rate)
        .bind(&flag_247)
        .bind(&flag_marathon)
        .bind(&suspicion_score)
        .bind(&avg_careers_per_day)
        .bind(&career_rate_sample_count)
        .bind(&career_rate_sample_seconds)
        .bind(&high_fan_rate_windows)
        .bind(&high_fan_rate_total_fan_gain)
        .bind(&high_fan_rate_total_seconds)
        .bind(&career_rate_breakdown_text)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn insertSessions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[SessionRow],
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    for chunk in rows.chunks(CHUNK_ROWS) {
        let mut viewer_id: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut rank: Vec<i16> = Vec::with_capacity(chunk.len());
        let mut day: Vec<NaiveDate> = Vec::with_capacity(chunk.len());
        let mut started_at: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut ended_at: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut duration_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut active_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut idle_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut careers: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut fan_gain: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut session_count: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut longest_session_sec: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut distinct_hours: Vec<i16> = Vec::with_capacity(chunk.len());
        let mut sessions_json: Vec<String> = Vec::with_capacity(chunk.len());
        for r in chunk {
            viewer_id.push(r.viewer_id);
            rank.push(r.rank);
            day.push(r.day);
            started_at.push(r.started_at);
            ended_at.push(r.ended_at);
            duration_seconds.push(r.duration_seconds);
            active_seconds.push(r.active_seconds);
            idle_seconds.push(r.idle_seconds);
            careers.push(r.careers);
            fan_gain.push(r.fan_gain);
            session_count.push(r.session_count);
            longest_session_sec.push(r.longest_session_sec);
            distinct_hours.push(r.distinct_hours);
            sessions_json.push(serde_json::to_string(&r.sessions)?);
        }
        sqlx::query(
            "INSERT INTO viewer_top_sessions \
             (viewer_id, rank, day, started_at, ended_at, duration_seconds, active_seconds, \
              idle_seconds, careers, fan_gain, session_count, longest_session_sec, \
              distinct_hours, sessions) \
             SELECT viewer_id, rank, day, started_at, ended_at, duration_seconds, active_seconds, \
                    idle_seconds, careers, fan_gain, session_count, longest_session_sec, \
                    distinct_hours, sessions::jsonb \
             FROM UNNEST($1::bigint[], $2::smallint[], $3::date[], $4::timestamptz[], \
                         $5::timestamptz[], $6::int[], $7::int[], $8::int[], $9::int[], \
                         $10::bigint[], $11::int[], $12::int[], $13::smallint[], $14::text[]) \
                  AS u(viewer_id, rank, day, started_at, ended_at, duration_seconds, active_seconds, \
                       idle_seconds, careers, fan_gain, session_count, longest_session_sec, \
                       distinct_hours, sessions)",
        )
        .bind(&viewer_id)
        .bind(&rank)
        .bind(&day)
        .bind(&started_at)
        .bind(&ended_at)
        .bind(&duration_seconds)
        .bind(&active_seconds)
        .bind(&idle_seconds)
        .bind(&careers)
        .bind(&fan_gain)
        .bind(&session_count)
        .bind(&longest_session_sec)
        .bind(&distinct_hours)
        .bind(&sessions_json)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn insertShortCareerSnapshots(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[ShortCareerSnapshotRow],
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    for chunk in rows.chunks(CHUNK_ROWS) {
        let mut viewer_id: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut rank: Vec<i16> = Vec::with_capacity(chunk.len());
        let mut total_count: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut snapshot_id: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut circle_id: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut snapshot_time: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut previous_snapshot_id: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut previous_snapshot_time: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut previous_snapshot_fans: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut current_fans: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut fan_gain: Vec<i64> = Vec::with_capacity(chunk.len());
        let mut snapshot_gap_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut previous_career_snapshot_time: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut previous_career_gap_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut career_length_seconds: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut fans_per_minute: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut short_training_score: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut is_high_fan_short: Vec<bool> = Vec::with_capacity(chunk.len());
        let mut prior_snapshots_json: Vec<String> = Vec::with_capacity(chunk.len());
        let mut next_snapshots_json: Vec<String> = Vec::with_capacity(chunk.len());

        for row in chunk {
            viewer_id.push(row.viewer_id);
            rank.push(row.rank);
            total_count.push(row.total_count);
            snapshot_id.push(row.snapshot_id);
            circle_id.push(row.circle_id);
            snapshot_time.push(row.snapshot_time);
            previous_snapshot_id.push(row.previous_snapshot_id);
            previous_snapshot_time.push(row.previous_snapshot_time);
            previous_snapshot_fans.push(row.previous_snapshot_fans);
            current_fans.push(row.current_fans);
            fan_gain.push(row.fan_gain);
            snapshot_gap_seconds.push(row.snapshot_gap_seconds);
            previous_career_snapshot_time.push(row.previous_career_snapshot_time);
            previous_career_gap_seconds.push(row.previous_career_gap_seconds);
            career_length_seconds.push(row.career_length_seconds);
            fans_per_minute.push(row.fans_per_minute);
            short_training_score.push(row.short_training_score);
            is_high_fan_short.push(row.is_high_fan_short);
            prior_snapshots_json.push(serde_json::to_string(&row.prior_snapshots)?);
            next_snapshots_json.push(serde_json::to_string(&row.next_snapshots)?);
        }

        sqlx::query(
            "INSERT INTO viewer_short_career_snapshots ( \
                viewer_id, rank, total_count, snapshot_id, circle_id, snapshot_time, \
                previous_snapshot_id, previous_snapshot_time, previous_snapshot_fans, \
                current_fans, fan_gain, snapshot_gap_seconds, previous_career_snapshot_time, \
                previous_career_gap_seconds, career_length_seconds, fans_per_minute, \
                short_training_score, is_high_fan_short, prior_snapshots, next_snapshots) \
             SELECT * FROM UNNEST( \
                $1::bigint[], $2::smallint[], $3::int[], $4::bigint[], $5::bigint[], \
                $6::timestamptz[], $7::bigint[], $8::timestamptz[], $9::bigint[], \
                $10::bigint[], $11::bigint[], $12::int[], $13::timestamptz[], \
                $14::int[], $15::int[], $16::double precision[], $17::double precision[], \
                $18::boolean[], $19::jsonb[], $20::jsonb[])",
        )
        .bind(&viewer_id)
        .bind(&rank)
        .bind(&total_count)
        .bind(&snapshot_id)
        .bind(&circle_id)
        .bind(&snapshot_time)
        .bind(&previous_snapshot_id)
        .bind(&previous_snapshot_time)
        .bind(&previous_snapshot_fans)
        .bind(&current_fans)
        .bind(&fan_gain)
        .bind(&snapshot_gap_seconds)
        .bind(&previous_career_snapshot_time)
        .bind(&previous_career_gap_seconds)
        .bind(&career_length_seconds)
        .bind(&fans_per_minute)
        .bind(&short_training_score)
        .bind(&is_high_fan_short)
        .bind(&prior_snapshots_json)
        .bind(&next_snapshots_json)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}
