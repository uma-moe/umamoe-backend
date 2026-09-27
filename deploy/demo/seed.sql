-- Synthetic, linked records. Run once in a transaction after the real migrations.
CREATE TEMP TABLE demo_rows ON COMMIT DROP AS
SELECT n, (900000000000 + n)::text AS account_id,
       900000000000 + n AS viewer_id, 9000000 + n AS circle_id,
       ('00000000-0000-4000-8000-' || lpad(n::text, 12, '0'))::uuid AS user_id
FROM generate_series(1, 100) AS n;

INSERT INTO circles (circle_id, name, comment, leader_viewer_id, member_count,
    join_style, policy, created_at, monthly_rank, monthly_point, last_month_rank,
    last_month_point, yesterday_points, yesterday_rank, yesterday_updated,
    live_points, live_rank, last_live_update)
SELECT circle_id, 'Demo Circle ' || lpad(n::text, 3, '0'), 'Synthetic local demo circle',
    viewer_id, 1, 1, 1, now() - interval '100 days', n, (101-n)::bigint*10000000,
    n, (101-n)::bigint*9000000, (101-n)::bigint*9500000, n, now(),
    (101-n)::bigint*10000000, n, now() FROM demo_rows;

INSERT INTO trainer (account_id, name, follower_num, circle_id, circle_name,
    circle_membership, fans, best_team_class, team_class, team_evaluation_point,
    leader_chara_dress_id, rank_score, own_follow_num, enable_circle_scout, comment)
SELECT account_id, 'Demo Trainer ' || lpad(n::text, 3, '0'), n*7%950, circle_id,
    'Demo Circle ' || lpad(n::text, 3, '0'), 1, 500000000+n::bigint*1000000,
    6, 6, 120000+n*100, 100101, 20000+n*100, n%100, 1,
    'Synthetic local demo trainer' FROM demo_rows;

INSERT INTO inheritance (account_id, main_parent_id, parent_left_id, parent_right_id,
    parent_rank, parent_rarity, blue_sparks, pink_sparks, green_sparks, white_sparks,
    win_count, white_count, main_blue_factors, main_pink_factors, main_green_factors,
    main_white_factors, main_white_count, left_blue_factors, left_pink_factors,
    right_blue_factors, right_pink_factors, main_win_saddles, left_win_saddles,
    right_win_saddles, blue_stars_sum, pink_stars_sum, green_stars_sum, white_stars_sum,
    base_affinity, race_affinity, affinity_scores, scenario_id)
SELECT account_id, 100101+(n%10)*100, 100201, 100301, 5+n%5, 3+n%3,
    ARRAY[10103,10203,10303], ARRAY[20103], ARRAY[10010103], ARRAY[20010101,20020102],
    20+n%20, 12, 10103, 20103, 10010103, ARRAY[20010101,20020102], 2,
    10203, 20203, 10303, 20303, ARRAY[101,102], ARRAY[101], ARRAY[102],
    9, 3, 3, 3, 60+n%50, 20, array_fill(80+n%20, ARRAY[200]), 1+n%3 FROM demo_rows;

INSERT INTO support_card (account_id, support_card_id, limit_break_count, experience)
SELECT account_id, 30001+n%10, n%5, 10000+n*250 FROM demo_rows;

INSERT INTO team_stadium (trainer_id, distance_type, member_id, trained_chara_id,
    running_style, card_id, speed, power, stamina, wiz, guts, fans, rank_score,
    creation_time, scenario_id, skills, factors, support_cards, rarity, talent_level,
    proper_ground_turf, proper_distance_mile)
SELECT account_id, 1+n%5, 1, 100000+n, 1+n%4, 100101, 900+n, 850+n, 800+n,
    750+n, 700+n, 100000+n*1000, 20000+n*100, now()-interval '2 days', 1,
    ARRAY[200101,200201], ARRAY[10103], ARRAY[30001], 3, 5, 7, 7 FROM demo_rows;

-- Three months let current, historical, and all-time ranking pages work.
INSERT INTO circle_member_fans_monthly (circle_id, viewer_id, year, month, daily_fans)
SELECT circle_id, viewer_id, extract(year FROM month_start)::int,
    extract(month FROM month_start)::int,
    ARRAY(SELECT 100000000+n::bigint*1000000+age*31000000+day*1000000
          FROM generate_series(0, CASE WHEN age=2 THEN extract(day FROM CURRENT_DATE)::int-1
               ELSE extract(day FROM month_start+interval '1 month'-interval '1 day')::int-1 END) AS day)
FROM demo_rows CROSS JOIN generate_series(0,2) AS age
CROSS JOIN LATERAL (SELECT date_trunc('month', CURRENT_DATE)-(2-age)*interval '1 month' AS month_start) AS months;

INSERT INTO circle_member_fan_snapshots (circle_id, snapshot_time, viewer_ids, fans, last_login_times)
SELECT circle_id, now()-(2-sample)*interval '10 minutes', ARRAY[viewer_id],
    ARRAY[500000000+n::bigint*1000000+sample*500000],
    ARRAY[to_char(now()-interval '2 hours', 'YYYY-MM-DD HH24:MI:SS')]
FROM demo_rows CROSS JOIN generate_series(0,1) AS sample;

INSERT INTO daily_stats (date, total_visitors, unique_visitors, inheritance_uploads, support_card_uploads, visitor_count)
SELECT CURRENT_DATE-n+1, 100+n*3, 80+n, n%20, n%10, 100+n*3 FROM demo_rows;
INSERT INTO daily_visitor_counters (date, visitor_count) SELECT date, visitor_count FROM daily_stats;
INSERT INTO friendlist_reports (trainer_id, report_count) SELECT account_id, 1+n%4 FROM demo_rows;
INSERT INTO tasks (task_type, task_data, priority, status, account_id)
SELECT 'friend/search', jsonb_build_object('trainer_id', account_id, 'demo', true), n%10,
    CASE n%4 WHEN 0 THEN 'pending' WHEN 1 THEN 'failed' ELSE 'completed' END, account_id FROM demo_rows;
INSERT INTO master_versions (app_version, resource_version, updated_at)
SELECT 'demo-1.'||n, 'demo-resources-'||n, now()-(100-n)*interval '1 hour' FROM demo_rows;

INSERT INTO users (id, display_name, email)
SELECT user_id, 'Demo Developer '||lpad(n::text,3,'0'), encode(digest('demo-'||n, 'sha256'), 'hex') FROM demo_rows;
INSERT INTO user_identities (user_id, provider, provider_user_id)
SELECT user_id, 'demo', 'synthetic-'||n FROM demo_rows;
INSERT INTO linked_accounts (user_id, account_id, verification_status, verified_at)
SELECT user_id, account_id, 'verified', now() FROM demo_rows;
INSERT INTO user_privacy_settings (account_id) SELECT account_id FROM demo_rows;
INSERT INTO api_keys (id, user_id, name, key_hash, key_prefix, total_requests)
SELECT user_id, user_id, 'Local development key',
    encode(digest('uma_demo_key_'||lpad(n::text,3,'0'), 'sha256'), 'hex'), 'uma_demo', n FROM demo_rows;
INSERT INTO api_key_usage (api_key_id, endpoint, requests)
SELECT user_id, '/api/v3/search', n FROM demo_rows;
INSERT INTO user_bookmarks (user_id, account_id, bookmarked_hash)
SELECT d.user_id, d.account_id, i.content_hash FROM demo_rows d JOIN inheritance i USING (account_id);
INSERT INTO bookmark_content_hash_backfill_queue (account_id, processed_at)
SELECT account_id, now() FROM demo_rows;
INSERT INTO partner_inheritance (user_id, account_id, main_parent_id, parent_left_id,
    parent_right_id, parent_rank, parent_rarity, blue_sparks, pink_sparks, green_sparks,
    white_sparks, win_count, white_count, content_hash, label)
SELECT d.user_id, d.account_id, i.main_parent_id, i.parent_left_id, i.parent_right_id,
    i.parent_rank, i.parent_rarity, i.blue_sparks, i.pink_sparks, i.green_sparks,
    i.white_sparks, i.win_count, i.white_count, i.content_hash, 'Demo partner '||d.n
FROM demo_rows d JOIN inheritance i USING (account_id);

INSERT INTO veteran_characters (account_id, trained_chara_id, card_id, scenario_id,
    rarity, speed, stamina, power, wiz, guts, fans, rank_score, rank, talent_level,
    proper_ground_turf, proper_distance_mile, create_time, register_time)
SELECT account_id, 100000+n, 100101, 1, 3, 900+n, 800+n, 850+n, 750+n, 700+n,
    100000+n*1000, 20000+n*100, 5+n%5, 5, 7, 7, now()-interval '2 days', now() FROM demo_rows;
INSERT INTO veteran_pins (account_id, trained_chara_id) SELECT account_id, trained_chara_id FROM veteran_characters;
INSERT INTO carat_planner_states (user_id, collection)
SELECT user_id, jsonb_build_object('version',3,'activePlanId','demo-'||n,
    'plans',jsonb_build_array(jsonb_build_array('demo-'||n,'Demo Plan '||n,
        (extract(epoch FROM now())*1000)::bigint, (extract(epoch FROM now())*1000)::bigint,
        CURRENT_DATE-date '2020-01-01', jsonb_build_array(30000+n*100), '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
        '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb))) FROM demo_rows;
INSERT INTO carat_plan_references (share_id, user_id, plan_id)
SELECT 'demo'||lpad(n::text,8,'0'), user_id, 'demo-'||n FROM demo_rows;
INSERT INTO carat_plan_shares (share_id, user_id, plan_id, plan_name, plan)
SELECT 'past'||lpad(n::text,8,'0'), user_id, 'past-'||n, 'Demo Archived Plan '||n,
    jsonb_build_object('id','past-'||n,'name','Demo Archived Plan '||n) FROM demo_rows;

INSERT INTO trainer_copies (trainer_id, copy_count) SELECT account_id, n FROM demo_rows;
INSERT INTO borrow_interaction_totals (trainer_id, inheritance_id, support_card_id, view_count, copy_count)
SELECT d.account_id, i.inheritance_id, s.support_card_id, d.n*10, d.n
FROM demo_rows d JOIN inheritance i USING(account_id) JOIN support_card s USING(account_id);
INSERT INTO borrow_interaction_totals_v2 (trainer_id, borrow_key, inheritance_id,
    support_card_id, support_card_limit_break, support_card_experience, view_count, copy_count)
SELECT d.account_id, 'legacy-trainer:'||d.account_id, i.inheritance_id,
    s.support_card_id, s.limit_break_count, s.experience, d.n*10, d.n
FROM demo_rows d JOIN inheritance i USING(account_id) JOIN support_card s USING(account_id);
INSERT INTO borrow_interaction_buckets (trainer_id, interaction_type, actor_hash, bucket_start)
SELECT account_id, 'view', md5(account_id), date_trunc('hour',now()) FROM demo_rows;
INSERT INTO borrow_interaction_buckets_v2 (trainer_id, borrow_key, interaction_type, actor_hash, bucket_start)
SELECT account_id, 'legacy-trainer:'||account_id, 'view', md5(account_id), date_trunc('hour',now()) FROM demo_rows;
INSERT INTO borrow_interaction_trends (trend_date, trainer_id, borrow_key, inheritance_id,
    support_card_id, support_card_limit_break, support_card_experience, view_count, copy_count)
SELECT CURRENT_DATE, trainer_id, borrow_key, inheritance_id, support_card_id,
    support_card_limit_break, support_card_experience, view_count, copy_count FROM borrow_interaction_totals_v2;

INSERT INTO viewer_activity_daily (viewer_id, day, active_seconds, careers, fan_gain, sessions, longest_session_sec, distinct_hours)
SELECT viewer_id, CURRENT_DATE, 3600, 4, 2000000, 2, 1800, 2 FROM demo_rows;
INSERT INTO viewer_activity_heatmap (viewer_id, dow, hour, active_seconds, careers)
SELECT viewer_id, extract(dow FROM CURRENT_DATE), 12, 3600, 4 FROM demo_rows;
INSERT INTO viewer_top_sessions (viewer_id, rank, started_at, ended_at, duration_seconds, active_seconds, idle_seconds, careers, fan_gain)
SELECT viewer_id, 1, now()-interval '2 hours', now()-interval '1 hour', 3600, 3600, 0, 4, 2000000 FROM demo_rows;
INSERT INTO viewer_suspicion_scores (viewer_id, first_seen, last_seen, days_observed,
    days_active, total_active_seconds, total_fan_gain, total_careers, careers_per_active_hour,
    avg_career_length_last20_seconds, fans_per_active_minute, peak_fans_per_minute,
    max_daily_active_seconds, max_daily_careers, max_session_seconds, days_over_16h,
    days_over_20h, distinct_weekly_hour_buckets, flag_no_sleep, flag_extreme_session,
    flag_inhuman_career_rate, flag_247, flag_marathon, suspicion_score,
    trainer_name, circle_id, circle_name, circle_monthly_rank)
SELECT d.viewer_id, now()-interval '30 days', now(), 30, 25, 90000, 50000000, 100, 4,
    900, 33333, 50000, 3600, 4, 3600, 0, 0, 14, false, false, false, false, false,
    d.n%101, t.name, d.circle_id, t.circle_name, d.n FROM demo_rows d JOIN trainer t USING(account_id);
INSERT INTO viewer_short_career_snapshots (viewer_id, rank, total_count, snapshot_id,
    circle_id, snapshot_time, previous_snapshot_id, previous_snapshot_time,
    previous_snapshot_fans, current_fans, fan_gain, snapshot_gap_seconds,
    previous_career_snapshot_time, previous_career_gap_seconds, career_length_seconds,
    fans_per_minute, short_training_score, is_high_fan_short)
SELECT d.viewer_id, 1, 1, curr.id, d.circle_id, curr.snapshot_time, prev.id,
    prev.snapshot_time, prev.fans[1], curr.fans[1], 500000, 600, prev.snapshot_time,
    600, 600, 50000, 0, false FROM demo_rows d
JOIN LATERAL (SELECT * FROM circle_member_fan_snapshots WHERE circle_id=d.circle_id ORDER BY snapshot_time LIMIT 1) prev ON true
JOIN LATERAL (SELECT * FROM circle_member_fan_snapshots WHERE circle_id=d.circle_id ORDER BY snapshot_time DESC LIMIT 1) curr ON true;
UPDATE cheat_analysis_meta SET snapshots_processed=200, viewers_scored=100,
    last_snapshot_id=(SELECT max(id) FROM circle_member_fan_snapshots), last_refreshed_at=now();

SELECT archive_fan_rankings_month(extract(year FROM CURRENT_DATE-interval '2 months')::int,
    extract(month FROM CURRENT_DATE-interval '2 months')::int);
SELECT archive_circle_rankings_month(extract(year FROM CURRENT_DATE-interval '1 month')::int,
    extract(month FROM CURRENT_DATE-interval '1 month')::int);
REFRESH MATERIALIZED VIEW circle_live_ranks;
REFRESH MATERIALIZED VIEW circle_search_documents;
REFRESH MATERIALIZED VIEW user_fan_rankings_monthly_current;
REFRESH MATERIALIZED VIEW user_fan_rankings_alltime;
REFRESH MATERIALIZED VIEW user_fan_rankings_gains;
REFRESH MATERIALIZED VIEW stats_counts;
