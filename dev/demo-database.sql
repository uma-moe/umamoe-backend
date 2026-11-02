-- Demo database for local development. The frontend's local stack (umamoe-frontend
-- compose.local.yaml) loads it by default; any empty Postgres works:
--   psql -v ON_ERROR_STOP=1 -d <empty database> -f dev/demo-database.sql
-- Part 1 is the schema with the sqlx migration history and no rows. Part 2 adds 100 synthetic,
-- linked trainers, circles, users, API keys and planner states, with dates relative to the day
-- the file is loaded. The backend applies newer migrations on top.
-- Demo trainer 900000000001; API keys uma_demo_key_001 to uma_demo_key_100.
--
-- Regenerate part 1 from a database migrated by this repository's backend (tables only the
-- private scraper workers use are left out):
--   pg_dump --schema-only --no-owner --no-acl -d umamoe
--   pg_dump --data-only --table=_sqlx_migrations -d umamoe
-- then append a REFRESH for each materialized view, in dependency order, as at the end of part 1.
-- After a schema change, load the whole file into an empty Postgres to check part 2 still applies.

--
-- PostgreSQL database dump
--

\restrict H21W7otcCOdU6XaXG3OOoA1ebQ43yOteC33aMrgldzonMrvNwyNXBeGTXm4ZkeG

-- Dumped from database version 17.11 (Debian 17.11-1.pgdg12+2)
-- Dumped by pg_dump version 17.11 (Debian 17.11-1.pgdg12+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: btree_gin; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;


--
-- Name: EXTENSION btree_gin; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION btree_gin IS 'support for indexing common datatypes in GIN';


--
-- Name: intarray; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS intarray WITH SCHEMA public;


--
-- Name: EXTENSION intarray; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION intarray IS 'functions, operators, and index support for 1-D arrays of integers';


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- Name: archive_circle_rankings_month(integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.archive_circle_rankings_month(p_year integer, p_month integer) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    target_month date := make_date(p_year, p_month, 1);
    previous_jst_month date := (
        date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date)
        - interval '1 month'
    )::date;
BEGIN
    DELETE FROM circle_ranks_monthly_archive
    WHERE year = p_year AND month = p_month;

    INSERT INTO circle_ranks_monthly_archive
        (circle_id, year, month, rank, total_points, member_count, circle_name)
    WITH circle_totals AS (
        SELECT
            r.circle_id,
            SUM(r.monthly_gain)::bigint AS total_points,
            COUNT(*)::int AS member_count,
            MAX(r.circle_name) AS circle_name
        FROM user_fan_rankings_monthly r
        WHERE r.year = p_year
          AND r.month = p_month
          AND r.circle_id IS NOT NULL
        GROUP BY r.circle_id
    ),
    ranked AS (
        SELECT
            t.*,
            RANK() OVER (
                ORDER BY t.total_points DESC
            )::int AS rank
        FROM circle_totals t
    )
    SELECT
        r.circle_id,
        p_year,
        p_month,
        r.rank,
        r.total_points,
        r.member_count,
        COALESCE(r.circle_name, c.name)
    FROM ranked r
    LEFT JOIN circles c ON c.circle_id = r.circle_id;

    -- Immediately after rollover, the game's last-month values are more
    -- authoritative than a reconstruction from incomplete member snapshots.
    IF target_month = previous_jst_month THEN
        INSERT INTO circle_ranks_monthly_archive
            (circle_id, year, month, rank, total_points, member_count, circle_name)
        SELECT
            c.circle_id,
            p_year,
            p_month,
            NULLIF(c.last_month_rank, 0),
            NULLIF(c.last_month_point, 0),
            c.member_count,
            c.name
        FROM circles c
        WHERE NOT COALESCE(c.archived, false)
          AND (c.last_month_rank > 0 OR c.last_month_point > 0)
        ON CONFLICT (circle_id, year, month) DO UPDATE SET
            rank = COALESCE(EXCLUDED.rank, circle_ranks_monthly_archive.rank),
            total_points = COALESCE(
                EXCLUDED.total_points,
                circle_ranks_monthly_archive.total_points
            ),
            member_count = COALESCE(
                EXCLUDED.member_count,
                circle_ranks_monthly_archive.member_count
            ),
            circle_name = COALESCE(
                EXCLUDED.circle_name,
                circle_ranks_monthly_archive.circle_name
            );
    END IF;
END;
$$;


--
-- Name: archive_fan_rankings_month(integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.archive_fan_rankings_month(p_year integer, p_month integer) RETURNS void
    LANGUAGE sql
    AS $$
INSERT INTO user_fan_rankings_monthly_archive
    (viewer_id, trainer_name, year, month, total_fans, monthly_gain,
     active_days, avg_daily, avg_3d, avg_7d, avg_monthly, rank,
     circle_id, circle_name, next_month_start)
WITH daily_expanded AS (
    SELECT
        cm.viewer_id, cm.year, cm.month, cm.circle_id,
        d.ord::int as day_of_month,
        d.val as cumulative_fans
    FROM circle_member_fans_monthly cm,
    LATERAL unnest(cm.daily_fans) WITH ORDINALITY AS d(val, ord)
    WHERE d.val > 0
      AND d.ord <= extract(day from (make_date(cm.year, cm.month, 1)
                   + interval '1 month' - interval '1 day'))
      AND cm.year = p_year AND cm.month = p_month
),
viewer_daily AS (
    SELECT viewer_id, year, month, day_of_month,
           MAX(cumulative_fans) as cumulative_fans
    FROM daily_expanded
    GROUP BY viewer_id, year, month, day_of_month
),
viewer_daily_with_delta AS (
    SELECT *,
           (cumulative_fans - COALESCE(
               LAG(cumulative_fans) OVER (
                   PARTITION BY viewer_id, year, month
                   ORDER BY day_of_month
               ), 0
           )) as daily_delta
    FROM viewer_daily
),
viewer_monthly_raw AS (
    SELECT viewer_id, year, month,
        MAX(cumulative_fans)::bigint as max_fans,
        MIN(cumulative_fans)::bigint as min_fans,
        COUNT(*) FILTER (WHERE daily_delta > 0)::int as active_days,
        MAX(day_of_month) as last_day
    FROM viewer_daily_with_delta
    GROUP BY viewer_id, year, month
    HAVING COUNT(*) >= 3
),
next_month_lookup AS (
    SELECT
        vmr.viewer_id, vmr.year, vmr.month,
        (SELECT MAX(cm2.daily_fans[1])
         FROM circle_member_fans_monthly cm2
         WHERE cm2.viewer_id = vmr.viewer_id
           AND cm2.year  = CASE WHEN vmr.month = 12 THEN vmr.year + 1 ELSE vmr.year END
           AND cm2.month = CASE WHEN vmr.month = 12 THEN 1 ELSE vmr.month + 1 END
           AND cm2.daily_fans[1] > 0
        )::bigint as next_month_start
    FROM viewer_monthly_raw vmr
),
viewer_monthly_stats AS (
    SELECT
        vmr.viewer_id, vmr.year, vmr.month,
        COALESCE(nml.next_month_start, vmr.max_fans)::bigint as total_fans,
        (COALESCE(nml.next_month_start, vmr.max_fans) - vmr.min_fans)::bigint as monthly_gain,
        vmr.active_days,
        vmr.last_day,
        nml.next_month_start
    FROM viewer_monthly_raw vmr
    LEFT JOIN next_month_lookup nml
        ON vmr.viewer_id = nml.viewer_id
        AND vmr.year = nml.year AND vmr.month = nml.month
),
end_of_month_circle AS (
    SELECT DISTINCT ON (viewer_id, year, month)
        viewer_id, year, month, circle_id
    FROM daily_expanded
    ORDER BY viewer_id, year, month, day_of_month DESC, cumulative_fans DESC
),
lookback AS (
    SELECT vd.viewer_id, vd.year, vd.month,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 3
            THEN vd.cumulative_fans END) as fans_3d_ago,
        MAX(CASE WHEN vd.day_of_month <= vms.last_day - 7
            THEN vd.cumulative_fans END) as fans_7d_ago
    FROM viewer_daily vd
    JOIN viewer_monthly_stats vms
        ON vd.viewer_id = vms.viewer_id
        AND vd.year = vms.year AND vd.month = vms.month
    GROUP BY vd.viewer_id, vd.year, vd.month
)
SELECT
    vms.viewer_id, t.name, vms.year, vms.month,
    vms.total_fans, vms.monthly_gain, vms.active_days,
    (vms.monthly_gain::float8 / NULLIF(vms.active_days, 0)),
    CASE WHEN lb.fans_3d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_3d_ago)::float8 / 3.0)
        ELSE NULL END,
    CASE WHEN lb.fans_7d_ago IS NOT NULL
        THEN ((vms.total_fans - lb.fans_7d_ago)::float8 / 7.0)
        ELSE NULL END,
    (vms.monthly_gain::float8 / extract(day from (
        make_date(vms.year, vms.month, 1) + interval '1 month' - interval '1 day'))),
    RANK() OVER (PARTITION BY vms.year, vms.month
                 ORDER BY vms.monthly_gain DESC)::int,
    eoc.circle_id,
    c.name,
    vms.next_month_start
FROM viewer_monthly_stats vms
LEFT JOIN lookback lb
    ON vms.viewer_id = lb.viewer_id
    AND vms.year = lb.year AND vms.month = lb.month
LEFT JOIN end_of_month_circle eoc
    ON vms.viewer_id = eoc.viewer_id
    AND vms.year = eoc.year AND vms.month = eoc.month
LEFT JOIN circles c ON eoc.circle_id = c.circle_id
LEFT JOIN trainer t ON vms.viewer_id::text = t.account_id
ON CONFLICT (year, month, viewer_id) DO NOTHING;
$$;


--
-- Name: calculate_sparks_score(integer[], integer[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.calculate_sparks_score(sparks integer[], factor_ids integer[]) RETURNS integer
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $$
DECLARE
    score int := 0;
    spark int;
    factor int;
    seen_factors int[] := '{}';
BEGIN
    IF sparks IS NULL OR factor_ids IS NULL OR array_length(factor_ids, 1) IS NULL THEN
        RETURN 0;
    END IF;

    FOREACH spark IN ARRAY sparks LOOP
        factor := spark / 10;
        
        -- Check if factor is in factor_ids
        IF factor = ANY(factor_ids) THEN
            score := score + (spark % 10);
            
            -- Check if we already counted this factor
            IF NOT (factor = ANY(seen_factors)) THEN
                seen_factors := array_append(seen_factors, factor);
                score := score + 100; -- Add 100 for the distinct factor
            END IF;
        END IF;
    END LOOP;
    
    RETURN score;
END;
$$;


--
-- Name: calculate_star_sums(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.calculate_star_sums() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.blue_stars_sum := COALESCE((SELECT SUM(x % 10) FROM unnest(NEW.blue_sparks) AS x), 0);
    NEW.pink_stars_sum := COALESCE((SELECT SUM(x % 10) FROM unnest(NEW.pink_sparks) AS x), 0);
    NEW.green_stars_sum := COALESCE((SELECT SUM(x % 10) FROM unnest(NEW.green_sparks) AS x), 0);
    NEW.white_stars_sum := COALESCE((SELECT SUM(x % 10) FROM unnest(NEW.white_sparks) AS x), 0);
    RETURN NEW;
END;
$$;


--
-- Name: count_array_overlap(integer[], integer[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.count_array_overlap(arr integer[], search_values integer[]) RETURNS integer
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT COUNT(*)::int
    FROM unnest(arr) AS elem
    WHERE elem = ANY(search_values);
$$;


--
-- Name: fix_daily_fans_array_bounds(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fix_daily_fans_array_bounds() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF array_lower(NEW.daily_fans, 1) IS DISTINCT FROM 1 AND NEW.daily_fans != '{}'::bigint[] THEN
        NEW.daily_fans := (
            SELECT array_agg(v ORDER BY ordinality)
            FROM unnest(NEW.daily_fans) WITH ORDINALITY AS t(v, ordinality)
        );
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: increment_daily_inheritance_uploads(date); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.increment_daily_inheritance_uploads(target_date date) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    current_count INTEGER;
BEGIN
    INSERT INTO daily_stats (date, total_visitors, unique_visitors, inheritance_uploads, support_card_uploads)
    VALUES (target_date, 0, 0, 1, 0)
    ON CONFLICT (date) 
    DO UPDATE SET 
        inheritance_uploads = daily_stats.inheritance_uploads + 1,
        updated_at = NOW()
    RETURNING inheritance_uploads INTO current_count;
    
    RETURN current_count;
END;
$$;


--
-- Name: FUNCTION increment_daily_inheritance_uploads(target_date date); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.increment_daily_inheritance_uploads(target_date date) IS 'Atomically increments daily inheritance upload count';


--
-- Name: increment_daily_support_card_uploads(date); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.increment_daily_support_card_uploads(target_date date) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    current_count INTEGER;
BEGIN
    INSERT INTO daily_stats (date, total_visitors, unique_visitors, inheritance_uploads, support_card_uploads)
    VALUES (target_date, 0, 0, 0, 1)
    ON CONFLICT (date) 
    DO UPDATE SET 
        support_card_uploads = daily_stats.support_card_uploads + 1,
        updated_at = NOW()
    RETURNING support_card_uploads INTO current_count;
    
    RETURN current_count;
END;
$$;


--
-- Name: FUNCTION increment_daily_support_card_uploads(target_date date); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.increment_daily_support_card_uploads(target_date date) IS 'Atomically increments daily support card upload count';


--
-- Name: increment_daily_visitor_count(date); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.increment_daily_visitor_count(target_date date) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    new_count INTEGER;
BEGIN
    -- Insert or update the daily_stats for the target date
    INSERT INTO daily_stats (date, total_visitors, unique_visitors, visitor_count, created_at, updated_at)
    VALUES (target_date, 1, 1, 1, NOW(), NOW())
    ON CONFLICT (date) 
    DO UPDATE SET 
        total_visitors = daily_stats.total_visitors + 1,
        unique_visitors = daily_stats.unique_visitors + 1,
        updated_at = NOW()
    RETURNING total_visitors INTO new_count;
    
    RETURN new_count;
END;
$$;


--
-- Name: FUNCTION increment_daily_visitor_count(target_date date); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.increment_daily_visitor_count(target_date date) IS 'Atomically increments daily visitor count, handles concurrent access safely';


--
-- Name: inheritance_content_hash(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.inheritance_content_hash() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.content_hash := md5(
        'inheritance-content-v2|' ||
        coalesce(NEW.main_parent_id::text, '')       || '|' ||
        coalesce(NEW.parent_left_id::text, '')       || '|' ||
        coalesce(NEW.parent_right_id::text, '')      || '|' ||
        coalesce(NEW.parent_rank::text, '')          || '|' ||
        coalesce(NEW.parent_rarity::text, '')        || '|' ||
        coalesce(NEW.blue_sparks::text, '')          || '|' ||
        coalesce(NEW.pink_sparks::text, '')          || '|' ||
        coalesce(NEW.green_sparks::text, '')         || '|' ||
        coalesce(NEW.white_sparks::text, '')         || '|' ||
        coalesce(NEW.win_count::text, '')            || '|' ||
        coalesce(NEW.white_count::text, '')          || '|' ||
        coalesce(NEW.main_blue_factors::text, '')    || '|' ||
        coalesce(NEW.main_pink_factors::text, '')    || '|' ||
        coalesce(NEW.main_green_factors::text, '')   || '|' ||
        coalesce(NEW.main_white_factors::text, '')   || '|' ||
        coalesce(NEW.main_white_count::text, '')     || '|' ||
        coalesce(NEW.left_blue_factors::text, '')    || '|' ||
        coalesce(NEW.left_pink_factors::text, '')    || '|' ||
        coalesce(NEW.left_green_factors::text, '')   || '|' ||
        coalesce(NEW.left_white_factors::text, '')   || '|' ||
        coalesce(NEW.left_white_count::text, '')     || '|' ||
        coalesce(NEW.right_blue_factors::text, '')   || '|' ||
        coalesce(NEW.right_pink_factors::text, '')   || '|' ||
        coalesce(NEW.right_green_factors::text, '')  || '|' ||
        coalesce(NEW.right_white_factors::text, '')  || '|' ||
        coalesce(NEW.right_white_count::text, '')    || '|' ||
        coalesce(NEW.main_win_saddles::text, '')     || '|' ||
        coalesce(NEW.left_win_saddles::text, '')     || '|' ||
        coalesce(NEW.right_win_saddles::text, '')    || '|' ||
        coalesce(NEW.race_results::text, '')
    );
    RETURN NEW;
END;
$$;


--
-- Name: notify_task_completion(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.notify_task_completion() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.status IN ('processing', 'completed', 'failed')
       AND (OLD.status IS DISTINCT FROM NEW.status) THEN
        PERFORM pg_notify(
            'task_completion',
            NEW.id::text || ':' || NEW.status
        );
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: partner_inheritance_content_hash(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.partner_inheritance_content_hash() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.content_hash := md5(
        'partner-inheritance-content-v1|' ||
        coalesce(NEW.main_parent_id::text, '')       || '|' ||
        coalesce(NEW.scenario_id::text, '')          || '|' ||
        coalesce(NEW.parent_left_id::text, '')       || '|' ||
        coalesce(NEW.parent_right_id::text, '')      || '|' ||
        coalesce(NEW.parent_rank::text, '')          || '|' ||
        coalesce(NEW.parent_rarity::text, '')        || '|' ||
        coalesce(NEW.blue_sparks::text, '')          || '|' ||
        coalesce(NEW.pink_sparks::text, '')          || '|' ||
        coalesce(NEW.green_sparks::text, '')         || '|' ||
        coalesce(NEW.white_sparks::text, '')         || '|' ||
        coalesce(NEW.win_count::text, '')            || '|' ||
        coalesce(NEW.white_count::text, '')          || '|' ||
        coalesce(NEW.main_blue_factors::text, '')    || '|' ||
        coalesce(NEW.main_pink_factors::text, '')    || '|' ||
        coalesce(NEW.main_green_factors::text, '')   || '|' ||
        coalesce(NEW.main_white_factors::text, '')   || '|' ||
        coalesce(NEW.main_white_count::text, '')     || '|' ||
        coalesce(NEW.left_blue_factors::text, '')    || '|' ||
        coalesce(NEW.left_pink_factors::text, '')    || '|' ||
        coalesce(NEW.left_green_factors::text, '')   || '|' ||
        coalesce(NEW.left_white_factors::text, '')   || '|' ||
        coalesce(NEW.left_white_count::text, '')     || '|' ||
        coalesce(NEW.right_blue_factors::text, '')   || '|' ||
        coalesce(NEW.right_pink_factors::text, '')   || '|' ||
        coalesce(NEW.right_green_factors::text, '')  || '|' ||
        coalesce(NEW.right_white_factors::text, '')  || '|' ||
        coalesce(NEW.right_white_count::text, '')    || '|' ||
        coalesce(NEW.main_win_saddles::text, '')     || '|' ||
        coalesce(NEW.left_win_saddles::text, '')     || '|' ||
        coalesce(NEW.right_win_saddles::text, '')    || '|' ||
        coalesce(NEW.race_results::text, '')
    );
    RETURN NEW;
END;
$$;


--
-- Name: refresh_circle_live_ranks(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_circle_live_ranks() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY circle_live_ranks;
END;
$$;


--
-- Name: refresh_stats_counts(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_stats_counts() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY stats_counts;
END;
$$;


--
-- Name: update_friendlist_reports_count(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.update_friendlist_reports_count() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- This would update a count field if we had one
    -- For now, we'll just count reports in queries
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _sqlx_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public._sqlx_migrations (
    version bigint NOT NULL,
    description text NOT NULL,
    installed_on timestamp with time zone DEFAULT now() NOT NULL,
    success boolean NOT NULL,
    checksum bytea NOT NULL,
    execution_time bigint NOT NULL
);


--
-- Name: api_key_usage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_key_usage (
    api_key_id uuid NOT NULL,
    endpoint character varying(255) NOT NULL,
    date date DEFAULT CURRENT_DATE NOT NULL,
    requests bigint DEFAULT 1 NOT NULL
);


--
-- Name: api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_keys (
    id uuid DEFAULT public.gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    name character varying(255) NOT NULL,
    key_hash character varying(64) NOT NULL,
    key_prefix character varying(16) NOT NULL,
    last_used timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    revoked boolean DEFAULT false NOT NULL,
    total_requests bigint DEFAULT 0 NOT NULL
);


--
-- Name: blue_factors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.blue_factors (
    record_id uuid NOT NULL,
    factor_type character varying(20) NOT NULL,
    level integer NOT NULL,
    CONSTRAINT blue_factors_factor_type_check CHECK (((factor_type)::text = ANY (ARRAY[('Speed'::character varying)::text, ('Stamina'::character varying)::text, ('Power'::character varying)::text, ('Guts'::character varying)::text, ('Wit'::character varying)::text]))),
    CONSTRAINT blue_factors_level_check CHECK (((level >= 1) AND (level <= 9)))
);


--
-- Name: bookmark_content_hash_backfill_queue; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bookmark_content_hash_backfill_queue (
    account_id character varying(255) NOT NULL,
    queued_at timestamp with time zone DEFAULT now() NOT NULL,
    processed_at timestamp with time zone,
    attempts integer DEFAULT 0 NOT NULL,
    last_error text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: borrow_interaction_buckets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.borrow_interaction_buckets (
    trainer_id text NOT NULL,
    inheritance_id bigint DEFAULT 0 NOT NULL,
    support_card_id integer DEFAULT 0 NOT NULL,
    interaction_type text NOT NULL,
    actor_hash text NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    event_count integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT borrow_interaction_buckets_interaction_type_check CHECK ((interaction_type = ANY (ARRAY['view'::text, 'copy'::text])))
);


--
-- Name: borrow_interaction_buckets_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.borrow_interaction_buckets_v2 (
    trainer_id text NOT NULL,
    borrow_key text NOT NULL,
    inheritance_id bigint DEFAULT 0 NOT NULL,
    support_card_id integer DEFAULT 0 NOT NULL,
    interaction_type text NOT NULL,
    actor_hash text NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    event_count integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT borrow_interaction_buckets_v2_interaction_type_check CHECK ((interaction_type = ANY (ARRAY['view'::text, 'copy'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: borrow_interaction_totals; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.borrow_interaction_totals (
    trainer_id text NOT NULL,
    inheritance_id bigint DEFAULT 0 NOT NULL,
    support_card_id integer DEFAULT 0 NOT NULL,
    support_card_limit_break integer,
    support_card_experience integer,
    view_count bigint DEFAULT 0 NOT NULL,
    copy_count bigint DEFAULT 0 NOT NULL,
    theoretical_copy_count integer DEFAULT 0 NOT NULL,
    last_known_follower_num integer,
    last_viewed_at timestamp with time zone,
    last_copied_at timestamp with time zone,
    last_recheck_at timestamp with time zone,
    last_recheck_task_created_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: borrow_interaction_totals_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.borrow_interaction_totals_v2 (
    trainer_id text NOT NULL,
    borrow_key text NOT NULL,
    inheritance_id bigint DEFAULT 0 NOT NULL,
    support_card_id integer DEFAULT 0 NOT NULL,
    support_card_limit_break integer,
    support_card_experience integer,
    view_count bigint DEFAULT 0 NOT NULL,
    copy_count bigint DEFAULT 0 NOT NULL,
    theoretical_copy_count integer DEFAULT 0 NOT NULL,
    last_known_follower_num integer,
    last_viewed_at timestamp with time zone,
    last_copied_at timestamp with time zone,
    last_recheck_at timestamp with time zone,
    last_recheck_task_created_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: borrow_interaction_trends; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.borrow_interaction_trends (
    trend_date date NOT NULL,
    trainer_id text NOT NULL,
    borrow_key text NOT NULL,
    inheritance_id bigint DEFAULT 0 NOT NULL,
    support_card_id integer DEFAULT 0 NOT NULL,
    support_card_limit_break integer,
    support_card_experience integer,
    view_count bigint DEFAULT 0 NOT NULL,
    copy_count bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: carat_plan_shares; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.carat_plan_shares (
    share_id character varying(16) NOT NULL,
    user_id uuid NOT NULL,
    plan_id character varying(100) NOT NULL,
    plan_name character varying(80) NOT NULL,
    plan jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT carat_plan_shares_plan_check CHECK ((jsonb_typeof(plan) = 'object'::text)),
    CONSTRAINT carat_plan_shares_plan_check1 CHECK ((octet_length((plan)::text) <= 262144))
);


--
-- Name: carat_planner_states; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.carat_planner_states (
    user_id uuid NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    collection jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT carat_planner_states_collection_check CHECK ((jsonb_typeof(collection) = 'object'::text)),
    CONSTRAINT carat_planner_states_collection_check1 CHECK ((octet_length((collection)::text) <= 1048576)),
    CONSTRAINT carat_planner_states_revision_check CHECK ((revision > 0))
);


--
-- Name: cheat_analysis_meta; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cheat_analysis_meta (
    id integer DEFAULT 1 NOT NULL,
    last_snapshot_id bigint DEFAULT 0 NOT NULL,
    last_refreshed_at timestamp with time zone DEFAULT now() NOT NULL,
    last_duration_ms integer DEFAULT 0 NOT NULL,
    snapshots_processed bigint DEFAULT 0 NOT NULL,
    viewers_scored bigint DEFAULT 0 NOT NULL,
    CONSTRAINT cheat_analysis_meta_id_check CHECK ((id = 1))
);


--
-- Name: circles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.circles (
    circle_id bigint NOT NULL,
    name character varying(255) NOT NULL,
    comment text,
    leader_viewer_id bigint,
    member_count integer,
    join_style integer,
    policy integer,
    created_at timestamp without time zone,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    monthly_rank integer,
    monthly_point bigint,
    last_month_rank integer,
    last_month_point bigint,
    archived boolean DEFAULT false,
    yesterday_updated timestamp without time zone,
    yesterday_points bigint,
    yesterday_rank integer,
    live_rank integer,
    live_points bigint,
    live_ranking integer,
    last_live_update timestamp without time zone
);


--
-- Name: COLUMN circles.live_rank; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.circles.live_rank IS 'Live rank for circles in the live refresh window. Top-100 rows may update every 5 minutes; broader top-10k/top-11k rows may update hourly.';


--
-- Name: COLUMN circles.live_points; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.circles.live_points IS 'Live fan count for circles in the live refresh window. Top-100 rows may update every 5 minutes; broader top-10k/top-11k rows may update hourly.';


--
-- Name: COLUMN circles.last_live_update; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.circles.last_live_update IS 'Timestamp of the latest circles.live_rank/live_points refresh.';


--
-- Name: circle_live_ranks; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.circle_live_ranks AS
 SELECT circle_id,
    rank() OVER (ORDER BY
        CASE
            WHEN (live_points > 0) THEN COALESCE(GREATEST(live_points, monthly_point), live_points, monthly_point)
            ELSE monthly_point
        END DESC NULLS LAST,
        CASE
            WHEN ((live_points > 0) AND (live_rank > 0)) THEN live_rank
            ELSE NULL::integer
        END) AS live_rank,
    row_number() OVER (ORDER BY yesterday_points DESC NULLS LAST, NULLIF(yesterday_rank, 0), NULLIF(monthly_rank, 0), circle_id) AS live_yesterday_rank
   FROM public.circles c
  WHERE ((last_updated >= date_trunc('month'::text, ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo'::text) - '2 days'::interval))) AND (NOT COALESCE(archived, false)))
  WITH NO DATA;


--
-- Name: circle_member_fan_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.circle_member_fan_snapshots (
    id bigint NOT NULL,
    circle_id bigint NOT NULL,
    viewer_ids bigint[] NOT NULL,
    fans bigint[] NOT NULL,
    last_login_times text[] NOT NULL,
    snapshot_time timestamp with time zone DEFAULT now() NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: circle_member_fan_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.circle_member_fan_snapshots_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: circle_member_fan_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.circle_member_fan_snapshots_id_seq OWNED BY public.circle_member_fan_snapshots.id;


--
-- Name: circle_member_fans_monthly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.circle_member_fans_monthly (
    id bigint NOT NULL,
    circle_id bigint NOT NULL,
    viewer_id bigint NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    daily_fans bigint[] DEFAULT '{}'::bigint[] NOT NULL,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_login_time text
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: circle_member_fans_monthly_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.circle_member_fans_monthly_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: circle_member_fans_monthly_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.circle_member_fans_monthly_id_seq OWNED BY public.circle_member_fans_monthly.id;


--
-- Name: circle_ranks_monthly_archive; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.circle_ranks_monthly_archive (
    circle_id bigint NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    rank integer,
    total_points bigint,
    member_count integer,
    circle_name text
);


--
-- Name: trainer; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trainer (
    account_id character varying(255) NOT NULL,
    name character varying(255) NOT NULL,
    follower_num integer DEFAULT 0,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    circle_id bigint,
    circle_name character varying(255),
    circle_membership integer,
    fans bigint,
    best_team_class integer,
    team_class integer,
    team_evaluation_point bigint DEFAULT 0,
    leader_chara_dress_id integer,
    rank_score bigint,
    release_num_info jsonb,
    trophy_num_info jsonb,
    team_stadium_user jsonb,
    own_follow_num integer,
    enable_circle_scout integer,
    comment text,
    is_deleted_or_banned boolean DEFAULT false,
    deleted_or_banned_at timestamp without time zone
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: circle_search_documents; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.circle_search_documents AS
 WITH target_month AS (
         SELECT (EXTRACT(year FROM ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo'::text) - '2 days'::interval)))::integer AS year,
            (EXTRACT(month FROM ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo'::text) - '2 days'::interval)))::integer AS month
        ), current_member_names AS (
         SELECT cm.circle_id,
            string_agg(DISTINCT (member.name)::text, ' '::text ORDER BY (member.name)::text) AS member_names
           FROM ((public.circle_member_fans_monthly cm
             CROSS JOIN target_month target)
             JOIN public.trainer member ON (((member.account_id)::text = (cm.viewer_id)::text)))
          WHERE ((cm.year = target.year) AND (cm.month = target.month) AND (member.name IS NOT NULL))
          GROUP BY cm.circle_id
        )
 SELECT circle.circle_id,
    concat_ws(' '::text, circle.name, leader.name, members.member_names) AS search_text
   FROM ((public.circles circle
     LEFT JOIN public.trainer leader ON (((leader.account_id)::text = (circle.leader_viewer_id)::text)))
     LEFT JOIN current_member_names members ON ((members.circle_id = circle.circle_id)))
  WITH NO DATA;


--
-- Name: daily_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.daily_stats (
    id integer NOT NULL,
    date date NOT NULL,
    total_visitors integer DEFAULT 0,
    unique_visitors integer DEFAULT 0,
    inheritance_uploads integer DEFAULT 0,
    support_card_uploads integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    visitor_count integer DEFAULT 0
);


--
-- Name: TABLE daily_stats; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.daily_stats IS 'Efficient daily aggregated stats - one row per day instead of individual visit tracking';


--
-- Name: daily_stats_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.daily_stats_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: daily_stats_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.daily_stats_id_seq OWNED BY public.daily_stats.id;


--
-- Name: daily_visitor_counters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.daily_visitor_counters (
    date date NOT NULL,
    visitor_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: friendlist_reports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.friendlist_reports (
    trainer_id character varying(15) NOT NULL,
    reported_at timestamp with time zone DEFAULT now(),
    report_count integer DEFAULT 1,
    CONSTRAINT trainer_id_format_fl CHECK (((trainer_id)::text ~ '^[0-9 ]+$'::text))
);


--
-- Name: inheritance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.inheritance (
    inheritance_id integer NOT NULL,
    account_id character varying(255) NOT NULL,
    main_parent_id integer DEFAULT 0 NOT NULL,
    parent_left_id integer DEFAULT 0 NOT NULL,
    parent_right_id integer DEFAULT 0 NOT NULL,
    blue_sparks integer[] DEFAULT '{}'::integer[],
    pink_sparks integer[] DEFAULT '{}'::integer[],
    green_sparks integer[] DEFAULT '{}'::integer[],
    white_sparks integer[] DEFAULT '{}'::integer[],
    win_count integer DEFAULT 0,
    white_count integer DEFAULT 0,
    parent_rank integer DEFAULT 0 NOT NULL,
    parent_rarity integer DEFAULT 0 NOT NULL,
    main_blue_factors integer DEFAULT 0,
    main_pink_factors integer DEFAULT 0,
    main_green_factors integer DEFAULT 0,
    main_white_factors integer[] DEFAULT '{}'::integer[],
    main_white_count integer DEFAULT 0,
    main_chara_id integer GENERATED ALWAYS AS ((main_parent_id / 100)) STORED,
    left_chara_id integer GENERATED ALWAYS AS ((parent_left_id / 100)) STORED,
    right_chara_id integer GENERATED ALWAYS AS ((parent_right_id / 100)) STORED,
    affinity_scores integer[],
    base_affinity integer DEFAULT 0,
    race_affinity integer DEFAULT 0,
    blue_stars_sum integer DEFAULT 0,
    pink_stars_sum integer DEFAULT 0,
    green_stars_sum integer DEFAULT 0,
    white_stars_sum integer DEFAULT 0,
    main_win_saddles integer[] DEFAULT '{}'::integer[],
    race_results integer[] DEFAULT '{}'::integer[],
    left_blue_factors integer DEFAULT 0,
    left_pink_factors integer DEFAULT 0,
    left_green_factors integer DEFAULT 0,
    left_white_factors integer[] DEFAULT '{}'::integer[],
    left_white_count integer DEFAULT 0,
    left_win_saddles integer[] DEFAULT '{}'::integer[],
    right_blue_factors integer DEFAULT 0,
    right_pink_factors integer DEFAULT 0,
    right_green_factors integer DEFAULT 0,
    right_white_factors integer[] DEFAULT '{}'::integer[],
    right_white_count integer DEFAULT 0,
    right_win_saddles integer[] DEFAULT '{}'::integer[],
    content_hash text,
    scenario_id integer DEFAULT 0 NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: TABLE inheritance; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.inheritance IS 'Inheritance records combining main parent with left/right sub-legacies. Includes pre-computed affinity scores for instant sorting without JOINs.';


--
-- Name: COLUMN inheritance.main_chara_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.inheritance.main_chara_id IS 'Character ID extracted from main_parent_id (parent_id / 100). Used for affinity JOINs.';


--
-- Name: COLUMN inheritance.left_chara_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.inheritance.left_chara_id IS 'Character ID extracted from parent_left_id (parent_id / 100). Used for affinity JOINs.';


--
-- Name: COLUMN inheritance.right_chara_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.inheritance.right_chara_id IS 'Character ID extracted from parent_right_id (parent_id / 100). Used for affinity JOINs.';


--
-- Name: COLUMN inheritance.affinity_scores; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.inheritance.affinity_scores IS 'Pre-computed affinity scores with all characters as int[]. Index mapping: chara_id 1001 = index 1, 1002 = index 2, etc. Formula: aff2(player,main) + aff3(player,main,left) + aff3(player,main,right). Populated by affinity.py script.';


--
-- Name: COLUMN inheritance.base_affinity; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.inheritance.base_affinity IS 'Base affinity score (main×left×right) for default sorting when no player character is selected. Formula: aff2(main,left) + aff3(main,left,right). Populated by affinity.py script.';


--
-- Name: COLUMN inheritance.race_affinity; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.inheritance.race_affinity IS 'Race affinity bonus from parent combination (left×right). Formula: aff2(left,right). Added to both affinity_scores[N] and base_affinity for final sort score. Populated by affinity.py script.';


--
-- Name: inheritance_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.inheritance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: inheritance_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.inheritance_id_seq OWNED BY public.inheritance.inheritance_id;


--
-- Name: inheritance_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.inheritance_records (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    trainer_id character varying(15) NOT NULL,
    main_character_id integer NOT NULL,
    parent1_id integer NOT NULL,
    parent2_id integer NOT NULL,
    submitted_at timestamp with time zone DEFAULT now(),
    verified boolean DEFAULT false,
    upvotes integer DEFAULT 0,
    downvotes integer DEFAULT 0,
    notes text,
    CONSTRAINT trainer_id_format CHECK (((trainer_id)::text ~ '^[0-9 ]+$'::text))
);


--
-- Name: linked_accounts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.linked_accounts (
    id integer NOT NULL,
    user_id uuid NOT NULL,
    account_id character varying(255) NOT NULL,
    verification_token character varying(64),
    verification_status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    verified_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: linked_accounts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.linked_accounts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: linked_accounts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.linked_accounts_id_seq OWNED BY public.linked_accounts.id;


--
-- Name: master_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.master_versions (
    id integer NOT NULL,
    app_version character varying(50) NOT NULL,
    resource_version character varying(50) NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: master_versions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.master_versions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: master_versions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.master_versions_id_seq OWNED BY public.master_versions.id;


--
-- Name: partner_inheritance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.partner_inheritance (
    id integer NOT NULL,
    user_id uuid NOT NULL,
    account_id text NOT NULL,
    main_parent_id integer DEFAULT 0 NOT NULL,
    parent_left_id integer DEFAULT 0 NOT NULL,
    parent_right_id integer DEFAULT 0 NOT NULL,
    parent_rank integer DEFAULT 0 NOT NULL,
    parent_rarity integer DEFAULT 0 NOT NULL,
    blue_sparks integer[] DEFAULT '{}'::integer[] NOT NULL,
    pink_sparks integer[] DEFAULT '{}'::integer[] NOT NULL,
    green_sparks integer[] DEFAULT '{}'::integer[] NOT NULL,
    white_sparks integer[] DEFAULT '{}'::integer[] NOT NULL,
    win_count integer DEFAULT 0 NOT NULL,
    white_count integer DEFAULT 0 NOT NULL,
    main_blue_factors integer DEFAULT 0 NOT NULL,
    main_pink_factors integer DEFAULT 0 NOT NULL,
    main_green_factors integer DEFAULT 0 NOT NULL,
    main_white_factors integer[] DEFAULT '{}'::integer[] NOT NULL,
    main_white_count integer DEFAULT 0 NOT NULL,
    left_blue_factors integer DEFAULT 0 NOT NULL,
    left_pink_factors integer DEFAULT 0 NOT NULL,
    left_green_factors integer DEFAULT 0 NOT NULL,
    left_white_factors integer[] DEFAULT '{}'::integer[] NOT NULL,
    left_white_count integer DEFAULT 0 NOT NULL,
    right_blue_factors integer DEFAULT 0 NOT NULL,
    right_pink_factors integer DEFAULT 0 NOT NULL,
    right_green_factors integer DEFAULT 0 NOT NULL,
    right_white_factors integer[] DEFAULT '{}'::integer[] NOT NULL,
    right_white_count integer DEFAULT 0 NOT NULL,
    main_win_saddles integer[] DEFAULT '{}'::integer[] NOT NULL,
    left_win_saddles integer[] DEFAULT '{}'::integer[] NOT NULL,
    right_win_saddles integer[] DEFAULT '{}'::integer[] NOT NULL,
    race_results integer[] DEFAULT '{}'::integer[] NOT NULL,
    blue_stars_sum integer DEFAULT 0 NOT NULL,
    pink_stars_sum integer DEFAULT 0 NOT NULL,
    green_stars_sum integer DEFAULT 0 NOT NULL,
    white_stars_sum integer DEFAULT 0 NOT NULL,
    affinity_score integer,
    trainer_name text,
    label text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    scenario_id integer DEFAULT 0 NOT NULL,
    content_hash text NOT NULL
);


--
-- Name: partner_inheritance_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.partner_inheritance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: partner_inheritance_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.partner_inheritance_id_seq OWNED BY public.partner_inheritance.id;


--
-- Name: pink_factors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pink_factors (
    record_id uuid NOT NULL,
    factor_type character varying(20) NOT NULL,
    level integer NOT NULL,
    CONSTRAINT pink_factors_factor_type_check CHECK (((factor_type)::text = ANY (ARRAY[('Turf'::character varying)::text, ('Dirt'::character varying)::text, ('Sprint'::character varying)::text, ('Mile'::character varying)::text, ('Middle'::character varying)::text, ('Long'::character varying)::text, ('Front Runner'::character varying)::text, ('Pace Chaser'::character varying)::text, ('Late Surger'::character varying)::text, ('End'::character varying)::text]))),
    CONSTRAINT pink_factors_level_check CHECK (((level >= 1) AND (level <= 9)))
);


--
-- Name: tasks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tasks (
    id integer NOT NULL,
    task_type character varying(255) NOT NULL,
    task_data jsonb NOT NULL,
    priority integer DEFAULT 0,
    status character varying(50) DEFAULT 'pending'::character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    worker_id character varying(255),
    error_message text,
    account_id character varying(255),
    retry_count integer DEFAULT 0,
    max_retries integer DEFAULT 3
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: team_stadium; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.team_stadium (
    id integer NOT NULL,
    trainer_id character varying(255) NOT NULL,
    distance_type integer NOT NULL,
    member_id integer NOT NULL,
    trained_chara_id integer NOT NULL,
    running_style integer NOT NULL,
    card_id bigint NOT NULL,
    speed integer DEFAULT 0 NOT NULL,
    power integer DEFAULT 0 NOT NULL,
    stamina integer DEFAULT 0 NOT NULL,
    wiz integer DEFAULT 0 NOT NULL,
    guts integer DEFAULT 0 NOT NULL,
    fans integer DEFAULT 0 NOT NULL,
    rank_score integer DEFAULT 0 NOT NULL,
    skills integer[] DEFAULT '{}'::integer[],
    creation_time timestamp without time zone NOT NULL,
    scenario_id integer DEFAULT 0 NOT NULL,
    factors integer[] DEFAULT '{}'::integer[],
    support_cards integer[] DEFAULT '{}'::integer[],
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    proper_ground_turf integer DEFAULT 0,
    proper_ground_dirt integer DEFAULT 0,
    proper_running_style_front integer DEFAULT 0,
    proper_running_style_pace integer DEFAULT 0,
    proper_running_style_late integer DEFAULT 0,
    proper_running_style_end integer DEFAULT 0,
    proper_distance_short integer DEFAULT 0,
    proper_distance_mile integer DEFAULT 0,
    proper_distance_middle integer DEFAULT 0,
    proper_distance_long integer DEFAULT 0,
    rarity integer DEFAULT 0,
    talent_level integer DEFAULT 0,
    proper_running_style_nige integer DEFAULT 0,
    proper_running_style_senko integer DEFAULT 0,
    proper_running_style_sashi integer DEFAULT 0,
    proper_running_style_oikomi integer DEFAULT 0,
    team_rating integer DEFAULT 0,
    parent_factors_10 integer[] DEFAULT '{}'::integer[],
    parent_factors_20 integer[] DEFAULT '{}'::integer[],
    parent_factors_11 integer[] DEFAULT '{}'::integer[],
    parent_factors_12 integer[] DEFAULT '{}'::integer[],
    parent_factors_21 integer[] DEFAULT '{}'::integer[],
    parent_factors_22 integer[] DEFAULT '{}'::integer[]
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: stats_counts; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.stats_counts AS
 WITH totals AS (
         SELECT ( SELECT count(*) AS count
                   FROM public.trainer) AS trainer_count,
            ( SELECT count(*) AS count
                   FROM public.team_stadium) AS umas_tracked
        )
 SELECT (1)::smallint AS singleton_key,
    ( SELECT count(*) AS count
           FROM public.tasks
          WHERE (((tasks.status)::text = 'completed'::text) AND (tasks.updated_at >= (now() - '24:00:00'::interval)))) AS tasks_24h,
    ( SELECT count(*) AS count
           FROM public.trainer
          WHERE (trainer.last_updated >= (now() - '24:00:00'::interval))) AS accounts_24h,
    trainer_count,
    trainer_count AS accounts_7d,
    umas_tracked,
    now() AS last_refreshed
   FROM totals
  WITH NO DATA;


--
-- Name: support_card; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.support_card (
    account_id character varying(255) NOT NULL,
    support_card_id integer NOT NULL,
    limit_break_count integer DEFAULT 0,
    experience integer DEFAULT 0 NOT NULL
);


--
-- Name: support_card_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.support_card_records (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    trainer_id character varying(15) NOT NULL,
    card_id character varying(50) NOT NULL,
    limit_break integer NOT NULL,
    rarity integer NOT NULL,
    card_type integer NOT NULL,
    submitted_at timestamp with time zone DEFAULT now(),
    upvotes integer DEFAULT 0,
    downvotes integer DEFAULT 0,
    CONSTRAINT support_card_records_card_type_check CHECK (((card_type >= 0) AND (card_type <= 6))),
    CONSTRAINT support_card_records_limit_break_check CHECK (((limit_break >= 0) AND (limit_break <= 4))),
    CONSTRAINT support_card_records_rarity_check CHECK (((rarity >= 1) AND (rarity <= 3))),
    CONSTRAINT trainer_id_format CHECK (((trainer_id)::text ~ '^[0-9]+$'::text))
);


--
-- Name: task_attempts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_attempts (
    id bigint NOT NULL,
    task_id bigint,
    worker_id text NOT NULL,
    account_id text,
    proxy_label text,
    steam_proxy_label text,
    endpoint text NOT NULL,
    dialect text NOT NULL,
    result_code integer,
    response_code integer,
    result_class text NOT NULL,
    elapsed_ms bigint NOT NULL,
    request_bytes integer,
    response_bytes integer,
    error_kind text,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: task_attempts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.task_attempts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_attempts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.task_attempts_id_seq OWNED BY public.task_attempts.id;


--
-- Name: tasks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.tasks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tasks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.tasks_id_seq OWNED BY public.tasks.id;


--
-- Name: team_stadium_characters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.team_stadium_characters (
    id integer NOT NULL,
    trainer_id bigint NOT NULL,
    distance_type integer NOT NULL,
    member_id integer NOT NULL,
    trained_chara_id integer NOT NULL,
    running_style integer NOT NULL,
    card_id bigint NOT NULL,
    speed integer DEFAULT 0 NOT NULL,
    power integer DEFAULT 0 NOT NULL,
    stamina integer DEFAULT 0 NOT NULL,
    wiz integer DEFAULT 0 NOT NULL,
    guts integer DEFAULT 0 NOT NULL,
    fans integer DEFAULT 0 NOT NULL,
    rank_score integer DEFAULT 0 NOT NULL,
    skills text,
    creation_time timestamp without time zone NOT NULL,
    scenario_id integer DEFAULT 0 NOT NULL,
    factors text,
    support_cards text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: team_stadium_characters_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.team_stadium_characters_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: team_stadium_characters_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.team_stadium_characters_id_seq OWNED BY public.team_stadium_characters.id;


--
-- Name: team_stadium_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.team_stadium_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: team_stadium_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.team_stadium_id_seq OWNED BY public.team_stadium.id;


--
-- Name: trainer_copies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trainer_copies (
    trainer_id text NOT NULL,
    copy_count integer DEFAULT 0 NOT NULL,
    last_copied timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: unique_skills; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.unique_skills (
    record_id uuid NOT NULL,
    skill_id integer NOT NULL,
    level integer NOT NULL,
    CONSTRAINT unique_skills_level_check CHECK (((level >= 1) AND (level <= 9)))
);


--
-- Name: user_bookmarks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_bookmarks (
    id integer NOT NULL,
    user_id uuid NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    account_id character varying(255) NOT NULL,
    bookmarked_hash text,
    bookmarked_borrow_key text,
    support_card_id integer,
    support_card_limit_break integer,
    support_card_experience integer
);


--
-- Name: user_bookmarks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.user_bookmarks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: user_bookmarks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.user_bookmarks_id_seq OWNED BY public.user_bookmarks.id;


--
-- Name: user_fan_rankings_monthly_archive; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_fan_rankings_monthly_archive (
    viewer_id bigint NOT NULL,
    trainer_name text,
    year integer NOT NULL,
    month integer NOT NULL,
    total_fans bigint NOT NULL,
    monthly_gain bigint NOT NULL,
    active_days integer NOT NULL,
    avg_daily double precision,
    avg_3d double precision,
    avg_7d double precision,
    avg_monthly double precision,
    rank integer NOT NULL,
    circle_id bigint,
    circle_name text,
    next_month_start bigint
);


--
-- Name: user_fan_rankings_monthly_current; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.user_fan_rankings_monthly_current AS
 WITH daily_expanded AS (
         SELECT cm.viewer_id,
            cm.year,
            cm.month,
            cm.circle_id,
            (d.ord)::integer AS day_of_month,
            d.val AS cumulative_fans
           FROM public.circle_member_fans_monthly cm,
            LATERAL unnest(cm.daily_fans) WITH ORDINALITY d(val, ord)
          WHERE ((d.val > 0) AND ((d.ord)::double precision <= date_part('day'::text, ((make_date(cm.year, cm.month, 1) + '1 mon'::interval) - '1 day'::interval))) AND (make_date(cm.year, cm.month, 1) >= date_trunc('month'::text, ((timezone('Asia/Tokyo'::text, CURRENT_TIMESTAMP))::date - '1 mon'::interval))))
        ), viewer_daily AS (
         SELECT daily_expanded.viewer_id,
            daily_expanded.year,
            daily_expanded.month,
            daily_expanded.day_of_month,
            max(daily_expanded.cumulative_fans) AS cumulative_fans
           FROM daily_expanded
          GROUP BY daily_expanded.viewer_id, daily_expanded.year, daily_expanded.month, daily_expanded.day_of_month
        ), viewer_daily_with_delta AS (
         SELECT viewer_daily.viewer_id,
            viewer_daily.year,
            viewer_daily.month,
            viewer_daily.day_of_month,
            viewer_daily.cumulative_fans,
            (viewer_daily.cumulative_fans - COALESCE(lag(viewer_daily.cumulative_fans) OVER (PARTITION BY viewer_daily.viewer_id, viewer_daily.year, viewer_daily.month ORDER BY viewer_daily.day_of_month), (0)::bigint)) AS daily_delta
           FROM viewer_daily
        ), viewer_monthly_raw AS (
         SELECT viewer_daily_with_delta.viewer_id,
            viewer_daily_with_delta.year,
            viewer_daily_with_delta.month,
            max(viewer_daily_with_delta.cumulative_fans) AS max_fans,
            min(viewer_daily_with_delta.cumulative_fans) AS min_fans,
            (count(*) FILTER (WHERE (viewer_daily_with_delta.daily_delta > 0)))::integer AS active_days,
            max(viewer_daily_with_delta.day_of_month) AS last_day
           FROM viewer_daily_with_delta
          GROUP BY viewer_daily_with_delta.viewer_id, viewer_daily_with_delta.year, viewer_daily_with_delta.month
         HAVING (count(*) >= 3)
        ), next_month_lookup AS (
         SELECT vmr.viewer_id,
            vmr.year,
            vmr.month,
            ( SELECT max(cm2.daily_fans[1]) AS max
                   FROM public.circle_member_fans_monthly cm2
                  WHERE ((cm2.viewer_id = vmr.viewer_id) AND (cm2.year =
                        CASE
                            WHEN (vmr.month = 12) THEN (vmr.year + 1)
                            ELSE vmr.year
                        END) AND (cm2.month =
                        CASE
                            WHEN (vmr.month = 12) THEN 1
                            ELSE (vmr.month + 1)
                        END) AND (cm2.daily_fans[1] > 0))) AS next_month_start
           FROM viewer_monthly_raw vmr
        ), viewer_monthly_stats AS (
         SELECT vmr.viewer_id,
            vmr.year,
            vmr.month,
            COALESCE(nml.next_month_start, vmr.max_fans) AS total_fans,
            (COALESCE(nml.next_month_start, vmr.max_fans) - vmr.min_fans) AS monthly_gain,
            vmr.active_days,
            vmr.last_day,
            nml.next_month_start
           FROM (viewer_monthly_raw vmr
             LEFT JOIN next_month_lookup nml ON (((vmr.viewer_id = nml.viewer_id) AND (vmr.year = nml.year) AND (vmr.month = nml.month))))
        ), end_of_month_circle AS (
         SELECT DISTINCT ON (daily_expanded.viewer_id, daily_expanded.year, daily_expanded.month) daily_expanded.viewer_id,
            daily_expanded.year,
            daily_expanded.month,
            daily_expanded.circle_id
           FROM daily_expanded
          ORDER BY daily_expanded.viewer_id, daily_expanded.year, daily_expanded.month, daily_expanded.day_of_month DESC, daily_expanded.cumulative_fans DESC
        ), lookback AS (
         SELECT vd.viewer_id,
            vd.year,
            vd.month,
            max(
                CASE
                    WHEN (vd.day_of_month <= (vms_1.last_day - 3)) THEN vd.cumulative_fans
                    ELSE NULL::bigint
                END) AS fans_3d_ago,
            max(
                CASE
                    WHEN (vd.day_of_month <= (vms_1.last_day - 7)) THEN vd.cumulative_fans
                    ELSE NULL::bigint
                END) AS fans_7d_ago
           FROM (viewer_daily vd
             JOIN viewer_monthly_stats vms_1 ON (((vd.viewer_id = vms_1.viewer_id) AND (vd.year = vms_1.year) AND (vd.month = vms_1.month))))
          GROUP BY vd.viewer_id, vd.year, vd.month
        )
 SELECT vms.viewer_id,
    t.name AS trainer_name,
    vms.year,
    vms.month,
    vms.total_fans,
    vms.monthly_gain,
    vms.active_days,
    ((vms.monthly_gain)::double precision / (NULLIF(vms.active_days, 0))::double precision) AS avg_daily,
        CASE
            WHEN (lb.fans_3d_ago IS NOT NULL) THEN (((vms.total_fans - lb.fans_3d_ago))::double precision / (3.0)::double precision)
            ELSE NULL::double precision
        END AS avg_3d,
        CASE
            WHEN (lb.fans_7d_ago IS NOT NULL) THEN (((vms.total_fans - lb.fans_7d_ago))::double precision / (7.0)::double precision)
            ELSE NULL::double precision
        END AS avg_7d,
    ((vms.monthly_gain)::double precision / date_part('day'::text, ((make_date(vms.year, vms.month, 1) + '1 mon'::interval) - '1 day'::interval))) AS avg_monthly,
    (rank() OVER (PARTITION BY vms.year, vms.month ORDER BY vms.monthly_gain DESC))::integer AS rank,
    eoc.circle_id,
    c.name AS circle_name,
    vms.next_month_start
   FROM ((((viewer_monthly_stats vms
     LEFT JOIN lookback lb ON (((vms.viewer_id = lb.viewer_id) AND (vms.year = lb.year) AND (vms.month = lb.month))))
     LEFT JOIN end_of_month_circle eoc ON (((vms.viewer_id = eoc.viewer_id) AND (vms.year = eoc.year) AND (vms.month = eoc.month))))
     LEFT JOIN public.circles c ON ((eoc.circle_id = c.circle_id)))
     LEFT JOIN public.trainer t ON (((vms.viewer_id)::text = (t.account_id)::text)))
  WITH NO DATA;


--
-- Name: user_fan_rankings_monthly; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.user_fan_rankings_monthly AS
 SELECT user_fan_rankings_monthly_archive.viewer_id,
    user_fan_rankings_monthly_archive.trainer_name,
    user_fan_rankings_monthly_archive.year,
    user_fan_rankings_monthly_archive.month,
    user_fan_rankings_monthly_archive.total_fans,
    user_fan_rankings_monthly_archive.monthly_gain,
    user_fan_rankings_monthly_archive.active_days,
    user_fan_rankings_monthly_archive.avg_daily,
    user_fan_rankings_monthly_archive.avg_3d,
    user_fan_rankings_monthly_archive.avg_7d,
    user_fan_rankings_monthly_archive.avg_monthly,
    user_fan_rankings_monthly_archive.rank,
    user_fan_rankings_monthly_archive.circle_id,
    user_fan_rankings_monthly_archive.circle_name,
    user_fan_rankings_monthly_archive.next_month_start
   FROM public.user_fan_rankings_monthly_archive
  WHERE (make_date(user_fan_rankings_monthly_archive.year, user_fan_rankings_monthly_archive.month, 1) < date_trunc('month'::text, ((timezone('Asia/Tokyo'::text, CURRENT_TIMESTAMP))::date - '1 mon'::interval)))
UNION ALL
 SELECT user_fan_rankings_monthly_current.viewer_id,
    user_fan_rankings_monthly_current.trainer_name,
    user_fan_rankings_monthly_current.year,
    user_fan_rankings_monthly_current.month,
    user_fan_rankings_monthly_current.total_fans,
    user_fan_rankings_monthly_current.monthly_gain,
    user_fan_rankings_monthly_current.active_days,
    user_fan_rankings_monthly_current.avg_daily,
    user_fan_rankings_monthly_current.avg_3d,
    user_fan_rankings_monthly_current.avg_7d,
    user_fan_rankings_monthly_current.avg_monthly,
    user_fan_rankings_monthly_current.rank,
    user_fan_rankings_monthly_current.circle_id,
    user_fan_rankings_monthly_current.circle_name,
    user_fan_rankings_monthly_current.next_month_start
   FROM public.user_fan_rankings_monthly_current;


--
-- Name: user_fan_rankings_alltime; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.user_fan_rankings_alltime AS
 WITH ranked_monthly AS (
         SELECT r.viewer_id,
            r.trainer_name,
            r.year,
            r.month,
            r.total_fans,
            r.monthly_gain,
            r.active_days,
            r.avg_daily,
            r.avg_3d,
            r.avg_7d,
            r.avg_monthly,
            r.rank,
            r.circle_id,
            r.circle_name,
            r.next_month_start,
            row_number() OVER (PARTITION BY r.viewer_id ORDER BY r.year DESC, r.month DESC) AS recency
           FROM public.user_fan_rankings_monthly r
        ), monthly_totals AS (
         SELECT r.viewer_id,
            max(r.total_fans) FILTER (WHERE (r.recency = 1)) AS snapshot_fans,
            (sum(r.monthly_gain))::bigint AS snapshot_gain,
            (sum(r.active_days))::integer AS active_days,
            (count(*))::integer AS months_active,
            max(r.avg_7d) FILTER (WHERE (r.recency = 1)) AS latest_avg_7d,
            max(r.circle_id) FILTER (WHERE (r.recency = 1)) AS circle_id,
            max(r.circle_name) FILTER (WHERE (r.recency = 1)) AS circle_name
           FROM ranked_monthly r
          GROUP BY r.viewer_id
        ), current_totals AS (
         SELECT m.viewer_id,
            COALESCE(t.name, (latest.trainer_name)::character varying) AS trainer_name,
            GREATEST(m.snapshot_fans, COALESCE(t.fans, m.snapshot_fans)) AS total_fans,
            (m.snapshot_gain + GREATEST((COALESCE(t.fans, m.snapshot_fans) - m.snapshot_fans), (0)::bigint)) AS total_gain,
            m.active_days,
            m.months_active,
                CASE
                    WHEN (m.latest_avg_7d IS NULL) THEN NULL::double precision
                    ELSE ((m.latest_avg_7d * (7.0)::double precision) + (GREATEST((COALESCE(t.fans, m.snapshot_fans) - m.snapshot_fans), (0)::bigint))::double precision)
                END AS avg_week,
            m.circle_id,
            COALESCE(c.name, (m.circle_name)::character varying) AS circle_name
           FROM (((monthly_totals m
             JOIN ranked_monthly latest ON (((latest.viewer_id = m.viewer_id) AND (latest.recency = 1))))
             LEFT JOIN public.trainer t ON (((t.account_id)::text = (m.viewer_id)::text)))
             LEFT JOIN public.circles c ON ((c.circle_id = m.circle_id)))
        ), scored AS (
         SELECT c.viewer_id,
            c.trainer_name,
            c.total_fans,
            c.total_gain,
            c.active_days,
            c.months_active,
            c.avg_week,
            c.circle_id,
            c.circle_name,
            ((c.total_gain)::double precision / (NULLIF(c.active_days, 0))::double precision) AS avg_day,
            ((c.total_gain)::double precision / (NULLIF(c.months_active, 0))::double precision) AS avg_month
           FROM current_totals c
        )
 SELECT viewer_id,
    trainer_name,
    total_fans,
    total_gain,
    active_days,
    avg_day,
    avg_week,
    avg_month,
    (rank() OVER (ORDER BY total_gain DESC))::integer AS rank,
    (rank() OVER (ORDER BY total_fans DESC))::integer AS rank_total_fans,
    (rank() OVER (ORDER BY total_gain DESC))::integer AS rank_total_gain,
    (rank() OVER (ORDER BY avg_day DESC NULLS LAST))::integer AS rank_avg_day,
    (rank() OVER (ORDER BY avg_week DESC NULLS LAST))::integer AS rank_avg_week,
    (rank() OVER (ORDER BY avg_month DESC NULLS LAST))::integer AS rank_avg_month,
    circle_id,
    circle_name
   FROM scored s
  WITH NO DATA;


--
-- Name: user_fan_rankings_gains; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.user_fan_rankings_gains AS
 WITH ref AS (
         SELECT ((timezone('Asia/Tokyo'::text, CURRENT_TIMESTAMP) - '1 day'::interval))::date AS ref_date
        ), daily_expanded AS (
         SELECT cm.viewer_id,
            cm.circle_id,
            (make_date(cm.year, cm.month, 1) + ((d.ord)::integer - 1)) AS fan_date,
            d.val AS cumulative_fans
           FROM public.circle_member_fans_monthly cm,
            LATERAL unnest(cm.daily_fans) WITH ORDINALITY d(val, ord)
          WHERE ((d.val > 0) AND ((d.ord)::double precision <= date_part('day'::text, ((make_date(cm.year, cm.month, 1) + '1 mon'::interval) - '1 day'::interval))) AND (make_date(cm.year, cm.month, 1) >= (CURRENT_DATE - '60 days'::interval)))
        ), viewer_daily AS (
         SELECT daily_expanded.viewer_id,
            daily_expanded.fan_date,
            max(daily_expanded.cumulative_fans) AS cumulative_fans
           FROM daily_expanded
          GROUP BY daily_expanded.viewer_id, daily_expanded.fan_date
        ), qualified AS (
         SELECT viewer_daily.viewer_id
           FROM viewer_daily
          GROUP BY viewer_daily.viewer_id
         HAVING (count(*) >= 3)
        ), current_circle AS (
         SELECT DISTINCT ON (daily_expanded.viewer_id) daily_expanded.viewer_id,
            daily_expanded.circle_id
           FROM daily_expanded
          ORDER BY daily_expanded.viewer_id, daily_expanded.fan_date DESC, daily_expanded.cumulative_fans DESC
        ), lookbacks AS (
         SELECT vd.viewer_id,
            max(
                CASE
                    WHEN (vd.fan_date <= r.ref_date) THEN vd.cumulative_fans
                    ELSE NULL::bigint
                END) AS latest_fans,
            max(
                CASE
                    WHEN (vd.fan_date <= (r.ref_date - 3)) THEN vd.cumulative_fans
                    ELSE NULL::bigint
                END) AS fans_3d_ago,
            max(
                CASE
                    WHEN (vd.fan_date <= (r.ref_date - 7)) THEN vd.cumulative_fans
                    ELSE NULL::bigint
                END) AS fans_7d_ago,
            max(
                CASE
                    WHEN (vd.fan_date <= (r.ref_date - 30)) THEN vd.cumulative_fans
                    ELSE NULL::bigint
                END) AS fans_30d_ago
           FROM ((viewer_daily vd
             CROSS JOIN ref r)
             JOIN qualified q ON ((q.viewer_id = vd.viewer_id)))
          WHERE (vd.fan_date <= r.ref_date)
          GROUP BY vd.viewer_id
        )
 SELECT lb.viewer_id,
    t.name AS trainer_name,
    COALESCE((lb.latest_fans - lb.fans_3d_ago), (0)::bigint) AS gain_3d,
    COALESCE((lb.latest_fans - lb.fans_7d_ago), (0)::bigint) AS gain_7d,
    COALESCE((lb.latest_fans - lb.fans_30d_ago), (0)::bigint) AS gain_30d,
    (rank() OVER (ORDER BY COALESCE((lb.latest_fans - lb.fans_3d_ago), (0)::bigint) DESC))::integer AS rank_3d,
    (rank() OVER (ORDER BY COALESCE((lb.latest_fans - lb.fans_7d_ago), (0)::bigint) DESC))::integer AS rank_7d,
    (rank() OVER (ORDER BY COALESCE((lb.latest_fans - lb.fans_30d_ago), (0)::bigint) DESC))::integer AS rank_30d,
    cc.circle_id,
    c.name AS circle_name
   FROM (((lookbacks lb
     LEFT JOIN current_circle cc ON ((lb.viewer_id = cc.viewer_id)))
     LEFT JOIN public.circles c ON ((cc.circle_id = c.circle_id)))
     LEFT JOIN public.trainer t ON (((lb.viewer_id)::text = (t.account_id)::text)))
  WHERE ((lb.latest_fans IS NOT NULL) AND ((lb.fans_3d_ago IS NOT NULL) OR (lb.fans_7d_ago IS NOT NULL) OR (lb.fans_30d_ago IS NOT NULL)))
  WITH NO DATA;


--
-- Name: user_identities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_identities (
    id integer NOT NULL,
    user_id uuid NOT NULL,
    provider character varying(20) NOT NULL,
    provider_user_id character varying(255) NOT NULL,
    display_name character varying(255),
    email character varying(255),
    avatar_url character varying(512),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: user_identities_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.user_identities_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: user_identities_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.user_identities_id_seq OWNED BY public.user_identities.id;


--
-- Name: user_privacy_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_privacy_settings (
    account_id text NOT NULL,
    profile_hidden boolean DEFAULT false NOT NULL,
    hidden_sections text[] DEFAULT '{}'::text[] NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id uuid DEFAULT public.gen_random_uuid() NOT NULL,
    display_name character varying(255),
    email character varying(255),
    avatar_url character varying(512),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: veteran_characters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.veteran_characters (
    account_id text NOT NULL,
    trained_chara_id bigint NOT NULL,
    card_id integer,
    scenario_id integer,
    route_id integer,
    rarity integer,
    succession_trained_chara_id_1 bigint,
    succession_trained_chara_id_2 bigint,
    succession_num integer,
    speed integer,
    stamina integer,
    power integer,
    wiz integer,
    guts integer,
    fans integer,
    rank_score bigint,
    rank integer,
    chara_grade integer,
    talent_level integer,
    running_style integer,
    race_cloth_id integer,
    nickname_id integer,
    wins integer,
    proper_ground_turf integer,
    proper_ground_dirt integer,
    proper_running_style_nige integer,
    proper_running_style_senko integer,
    proper_running_style_sashi integer,
    proper_running_style_oikomi integer,
    proper_distance_short integer,
    proper_distance_mile integer,
    proper_distance_middle integer,
    proper_distance_long integer,
    skill_array jsonb DEFAULT '[]'::jsonb NOT NULL,
    support_card_list jsonb DEFAULT '[]'::jsonb NOT NULL,
    factor_info_array jsonb DEFAULT '[]'::jsonb NOT NULL,
    win_saddle_id_array jsonb DEFAULT '[]'::jsonb NOT NULL,
    register_time timestamp with time zone,
    create_time timestamp with time zone,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    succession_chara_array jsonb DEFAULT '[]'::jsonb NOT NULL,
    id uuid DEFAULT gen_random_uuid() NOT NULL
);


--
-- Name: veteran_pins; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.veteran_pins (
    account_id text NOT NULL,
    trained_chara_id bigint NOT NULL,
    pinned_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: viewer_activity_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.viewer_activity_daily (
    viewer_id bigint NOT NULL,
    day date NOT NULL,
    active_seconds integer NOT NULL,
    careers integer NOT NULL,
    fan_gain bigint NOT NULL,
    sessions integer NOT NULL,
    longest_session_sec integer NOT NULL,
    distinct_hours smallint NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: viewer_activity_heatmap; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.viewer_activity_heatmap (
    viewer_id bigint NOT NULL,
    dow smallint NOT NULL,
    hour smallint NOT NULL,
    active_seconds integer NOT NULL,
    careers integer NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: viewer_short_career_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.viewer_short_career_snapshots (
    viewer_id bigint NOT NULL,
    rank smallint NOT NULL,
    total_count integer NOT NULL,
    snapshot_id bigint NOT NULL,
    circle_id bigint NOT NULL,
    snapshot_time timestamp with time zone NOT NULL,
    previous_snapshot_id bigint NOT NULL,
    previous_snapshot_time timestamp with time zone NOT NULL,
    previous_snapshot_fans bigint NOT NULL,
    current_fans bigint NOT NULL,
    fan_gain bigint NOT NULL,
    snapshot_gap_seconds integer NOT NULL,
    previous_career_snapshot_time timestamp with time zone NOT NULL,
    previous_career_gap_seconds integer NOT NULL,
    career_length_seconds integer NOT NULL,
    fans_per_minute double precision NOT NULL,
    short_training_score double precision NOT NULL,
    is_high_fan_short boolean NOT NULL,
    prior_snapshots jsonb DEFAULT '[]'::jsonb NOT NULL,
    next_snapshots jsonb DEFAULT '[]'::jsonb NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: viewer_suspicion_scores; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.viewer_suspicion_scores (
    viewer_id bigint NOT NULL,
    first_seen timestamp with time zone NOT NULL,
    last_seen timestamp with time zone NOT NULL,
    days_observed integer NOT NULL,
    days_active integer NOT NULL,
    total_active_seconds bigint NOT NULL,
    total_fan_gain bigint NOT NULL,
    total_careers integer NOT NULL,
    careers_per_active_hour double precision NOT NULL,
    avg_career_length_last20_seconds double precision NOT NULL,
    career_length_buckets integer[] DEFAULT '{}'::integer[] NOT NULL,
    fans_per_active_minute double precision NOT NULL,
    peak_fans_per_minute double precision NOT NULL,
    max_daily_active_seconds integer NOT NULL,
    max_daily_careers integer NOT NULL,
    max_session_seconds integer NOT NULL,
    days_over_16h integer NOT NULL,
    days_over_20h integer NOT NULL,
    distinct_weekly_hour_buckets smallint NOT NULL,
    flag_no_sleep boolean NOT NULL,
    flag_extreme_session boolean NOT NULL,
    flag_inhuman_career_rate boolean NOT NULL,
    flag_247 boolean NOT NULL,
    flag_marathon boolean NOT NULL,
    suspicion_score integer NOT NULL,
    refreshed_at timestamp with time zone DEFAULT now() NOT NULL,
    trainer_name text,
    circle_id bigint,
    circle_name text,
    circle_monthly_rank integer,
    short_high_fan_careers integer DEFAULT 0 NOT NULL,
    short_fan_gain_score double precision DEFAULT 0 NOT NULL,
    short_fan_gain_score_buckets double precision[] DEFAULT '{}'::double precision[] NOT NULL,
    short_career_avg_fan_gain double precision DEFAULT 0 NOT NULL,
    short_career_p50_fan_gain double precision DEFAULT 0 NOT NULL,
    short_career_p90_fan_gain double precision DEFAULT 0 NOT NULL,
    short_career_p95_fan_gain double precision DEFAULT 0 NOT NULL,
    short_career_max_fan_gain double precision DEFAULT 0 NOT NULL,
    recent_fan_gain_3d bigint DEFAULT 0 NOT NULL,
    baseline_fan_gain_14d bigint DEFAULT 0 NOT NULL,
    recent_fans_per_day double precision DEFAULT 0 NOT NULL,
    baseline_fans_per_day double precision DEFAULT 0 NOT NULL,
    fan_gain_spike_ratio double precision DEFAULT 0 NOT NULL,
    behavior_change_score double precision DEFAULT 0 NOT NULL,
    reset_recovery_windows integer DEFAULT 0 NOT NULL,
    reset_breaks integer DEFAULT 0 NOT NULL,
    max_reset_recovery_seconds integer DEFAULT 0 NOT NULL,
    reset_break_score double precision DEFAULT 0 NOT NULL,
    probe_score double precision DEFAULT 0 NOT NULL,
    probe_metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    career_rate_sample_count integer DEFAULT 0 NOT NULL,
    career_rate_sample_seconds bigint DEFAULT 0 NOT NULL,
    career_rate_breakdown jsonb DEFAULT '{}'::jsonb NOT NULL,
    avg_careers_per_day double precision DEFAULT 0 NOT NULL,
    high_fan_rate_windows integer DEFAULT 0 NOT NULL,
    high_fan_rate_total_fan_gain bigint DEFAULT 0 NOT NULL,
    high_fan_rate_total_seconds integer DEFAULT 0 NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: viewer_top_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.viewer_top_sessions (
    viewer_id bigint NOT NULL,
    rank smallint NOT NULL,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone NOT NULL,
    duration_seconds integer NOT NULL,
    active_seconds integer NOT NULL,
    idle_seconds integer NOT NULL,
    careers integer NOT NULL,
    fan_gain bigint NOT NULL,
    day date,
    session_count integer DEFAULT 0 NOT NULL,
    longest_session_sec integer DEFAULT 0 NOT NULL,
    distinct_hours smallint DEFAULT 0 NOT NULL,
    sessions jsonb DEFAULT '[]'::jsonb NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_vacuum_threshold='10000', autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000', autovacuum_vacuum_insert_scale_factor='0.02', autovacuum_vacuum_insert_threshold='10000');


--
-- Name: votes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.votes (
    id integer NOT NULL,
    record_id uuid NOT NULL,
    user_ip inet NOT NULL,
    vote_type character varying(10) NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT votes_vote_type_check CHECK (((vote_type)::text = ANY (ARRAY[('up'::character varying)::text, ('down'::character varying)::text])))
);


--
-- Name: votes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.votes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: votes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.votes_id_seq OWNED BY public.votes.id;


--
-- Name: circle_member_fan_snapshots id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.circle_member_fan_snapshots ALTER COLUMN id SET DEFAULT nextval('public.circle_member_fan_snapshots_id_seq'::regclass);


--
-- Name: circle_member_fans_monthly id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.circle_member_fans_monthly ALTER COLUMN id SET DEFAULT nextval('public.circle_member_fans_monthly_id_seq'::regclass);


--
-- Name: daily_stats id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_stats ALTER COLUMN id SET DEFAULT nextval('public.daily_stats_id_seq'::regclass);


--
-- Name: inheritance inheritance_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inheritance ALTER COLUMN inheritance_id SET DEFAULT nextval('public.inheritance_id_seq'::regclass);


--
-- Name: linked_accounts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.linked_accounts ALTER COLUMN id SET DEFAULT nextval('public.linked_accounts_id_seq'::regclass);


--
-- Name: master_versions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.master_versions ALTER COLUMN id SET DEFAULT nextval('public.master_versions_id_seq'::regclass);


--
-- Name: partner_inheritance id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.partner_inheritance ALTER COLUMN id SET DEFAULT nextval('public.partner_inheritance_id_seq'::regclass);


--
-- Name: task_attempts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_attempts ALTER COLUMN id SET DEFAULT nextval('public.task_attempts_id_seq'::regclass);


--
-- Name: tasks id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tasks ALTER COLUMN id SET DEFAULT nextval('public.tasks_id_seq'::regclass);


--
-- Name: team_stadium id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_stadium ALTER COLUMN id SET DEFAULT nextval('public.team_stadium_id_seq'::regclass);


--
-- Name: team_stadium_characters id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_stadium_characters ALTER COLUMN id SET DEFAULT nextval('public.team_stadium_characters_id_seq'::regclass);


--
-- Name: user_bookmarks id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_bookmarks ALTER COLUMN id SET DEFAULT nextval('public.user_bookmarks_id_seq'::regclass);


--
-- Name: user_identities id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_identities ALTER COLUMN id SET DEFAULT nextval('public.user_identities_id_seq'::regclass);


--
-- Name: votes id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.votes ALTER COLUMN id SET DEFAULT nextval('public.votes_id_seq'::regclass);


--
-- Name: _sqlx_migrations _sqlx_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public._sqlx_migrations
    ADD CONSTRAINT _sqlx_migrations_pkey PRIMARY KEY (version);


--
-- Name: api_key_usage api_key_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_usage
    ADD CONSTRAINT api_key_usage_pkey PRIMARY KEY (api_key_id, endpoint, date);


--
-- Name: api_keys api_keys_key_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_key_hash_key UNIQUE (key_hash);


--
-- Name: api_keys api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);


--
-- Name: blue_factors blue_factors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.blue_factors
    ADD CONSTRAINT blue_factors_pkey PRIMARY KEY (record_id, factor_type);


--
-- Name: bookmark_content_hash_backfill_queue bookmark_content_hash_backfill_queue_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bookmark_content_hash_backfill_queue
    ADD CONSTRAINT bookmark_content_hash_backfill_queue_pkey PRIMARY KEY (account_id);


--
-- Name: borrow_interaction_buckets borrow_interaction_buckets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.borrow_interaction_buckets
    ADD CONSTRAINT borrow_interaction_buckets_pkey PRIMARY KEY (trainer_id, inheritance_id, support_card_id, interaction_type, actor_hash, bucket_start);


--
-- Name: borrow_interaction_buckets_v2 borrow_interaction_buckets_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.borrow_interaction_buckets_v2
    ADD CONSTRAINT borrow_interaction_buckets_v2_pkey PRIMARY KEY (trainer_id, borrow_key, interaction_type, actor_hash, bucket_start);


--
-- Name: borrow_interaction_totals borrow_interaction_totals_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.borrow_interaction_totals
    ADD CONSTRAINT borrow_interaction_totals_pkey PRIMARY KEY (trainer_id, inheritance_id, support_card_id);


--
-- Name: borrow_interaction_totals_v2 borrow_interaction_totals_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.borrow_interaction_totals_v2
    ADD CONSTRAINT borrow_interaction_totals_v2_pkey PRIMARY KEY (trainer_id, borrow_key);


--
-- Name: borrow_interaction_trends borrow_interaction_trends_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.borrow_interaction_trends
    ADD CONSTRAINT borrow_interaction_trends_pkey PRIMARY KEY (trend_date, trainer_id, borrow_key);


--
-- Name: carat_plan_shares carat_plan_shares_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.carat_plan_shares
    ADD CONSTRAINT carat_plan_shares_pkey PRIMARY KEY (share_id);


--
-- Name: carat_plan_shares carat_plan_shares_user_id_plan_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.carat_plan_shares
    ADD CONSTRAINT carat_plan_shares_user_id_plan_id_key UNIQUE (user_id, plan_id);


--
-- Name: carat_planner_states carat_planner_states_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.carat_planner_states
    ADD CONSTRAINT carat_planner_states_pkey PRIMARY KEY (user_id);


--
-- Name: cheat_analysis_meta cheat_analysis_meta_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cheat_analysis_meta
    ADD CONSTRAINT cheat_analysis_meta_pkey PRIMARY KEY (id);


--
-- Name: circle_member_fan_snapshots circle_member_fan_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.circle_member_fan_snapshots
    ADD CONSTRAINT circle_member_fan_snapshots_pkey PRIMARY KEY (id);


--
-- Name: circle_member_fans_monthly circle_member_fans_monthly_circle_id_viewer_id_year_month_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.circle_member_fans_monthly
    ADD CONSTRAINT circle_member_fans_monthly_circle_id_viewer_id_year_month_key UNIQUE (circle_id, viewer_id, year, month);


--
-- Name: circle_member_fans_monthly circle_member_fans_monthly_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.circle_member_fans_monthly
    ADD CONSTRAINT circle_member_fans_monthly_pkey PRIMARY KEY (id);


--
-- Name: circle_ranks_monthly_archive circle_ranks_monthly_archive_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.circle_ranks_monthly_archive
    ADD CONSTRAINT circle_ranks_monthly_archive_pkey PRIMARY KEY (circle_id, year, month);


--
-- Name: circles circles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.circles
    ADD CONSTRAINT circles_pkey PRIMARY KEY (circle_id);


--
-- Name: daily_stats daily_stats_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_stats
    ADD CONSTRAINT daily_stats_date_key UNIQUE (date);


--
-- Name: daily_stats daily_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_stats
    ADD CONSTRAINT daily_stats_pkey PRIMARY KEY (id);


--
-- Name: daily_visitor_counters daily_visitor_counters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_visitor_counters
    ADD CONSTRAINT daily_visitor_counters_pkey PRIMARY KEY (date);


--
-- Name: friendlist_reports friendlist_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.friendlist_reports
    ADD CONSTRAINT friendlist_reports_pkey PRIMARY KEY (trainer_id);


--
-- Name: inheritance inheritance_account_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inheritance
    ADD CONSTRAINT inheritance_account_id_key UNIQUE (account_id);


--
-- Name: inheritance inheritance_account_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inheritance
    ADD CONSTRAINT inheritance_account_id_unique UNIQUE (account_id);


--
-- Name: inheritance inheritance_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inheritance
    ADD CONSTRAINT inheritance_pkey PRIMARY KEY (inheritance_id);


--
-- Name: inheritance_records inheritance_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inheritance_records
    ADD CONSTRAINT inheritance_records_pkey PRIMARY KEY (id);


--
-- Name: linked_accounts linked_accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.linked_accounts
    ADD CONSTRAINT linked_accounts_pkey PRIMARY KEY (id);


--
-- Name: linked_accounts linked_accounts_user_id_account_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.linked_accounts
    ADD CONSTRAINT linked_accounts_user_id_account_id_key UNIQUE (user_id, account_id);


--
-- Name: master_versions master_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.master_versions
    ADD CONSTRAINT master_versions_pkey PRIMARY KEY (id);


--
-- Name: partner_inheritance partner_inheritance_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.partner_inheritance
    ADD CONSTRAINT partner_inheritance_pkey PRIMARY KEY (id);


--
-- Name: partner_inheritance partner_inheritance_user_id_account_id_content_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.partner_inheritance
    ADD CONSTRAINT partner_inheritance_user_id_account_id_content_hash_key UNIQUE (user_id, account_id, content_hash);


--
-- Name: pink_factors pink_factors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pink_factors
    ADD CONSTRAINT pink_factors_pkey PRIMARY KEY (record_id, factor_type);


--
-- Name: support_card support_card_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.support_card
    ADD CONSTRAINT support_card_pkey PRIMARY KEY (account_id, support_card_id);


--
-- Name: support_card_records support_card_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.support_card_records
    ADD CONSTRAINT support_card_records_pkey PRIMARY KEY (id);


--
-- Name: task_attempts task_attempts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_attempts
    ADD CONSTRAINT task_attempts_pkey PRIMARY KEY (id);


--
-- Name: tasks tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_pkey PRIMARY KEY (id);


--
-- Name: team_stadium_characters team_stadium_characters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_stadium_characters
    ADD CONSTRAINT team_stadium_characters_pkey PRIMARY KEY (id);


--
-- Name: team_stadium_characters team_stadium_characters_trainer_id_distance_type_member_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_stadium_characters
    ADD CONSTRAINT team_stadium_characters_trainer_id_distance_type_member_id_key UNIQUE (trainer_id, distance_type, member_id);


--
-- Name: team_stadium team_stadium_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_stadium
    ADD CONSTRAINT team_stadium_pkey PRIMARY KEY (id);


--
-- Name: team_stadium team_stadium_trainer_id_distance_type_member_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_stadium
    ADD CONSTRAINT team_stadium_trainer_id_distance_type_member_id_key UNIQUE (trainer_id, distance_type, member_id);


--
-- Name: trainer_copies trainer_copies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trainer_copies
    ADD CONSTRAINT trainer_copies_pkey PRIMARY KEY (trainer_id);


--
-- Name: trainer trainer_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trainer
    ADD CONSTRAINT trainer_pkey PRIMARY KEY (account_id);


--
-- Name: unique_skills unique_skills_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.unique_skills
    ADD CONSTRAINT unique_skills_pkey PRIMARY KEY (record_id, skill_id);


--
-- Name: support_card_records unique_submission; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.support_card_records
    ADD CONSTRAINT unique_submission UNIQUE (trainer_id, card_id, limit_break);


--
-- Name: user_bookmarks user_bookmarks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_bookmarks
    ADD CONSTRAINT user_bookmarks_pkey PRIMARY KEY (id);


--
-- Name: user_bookmarks user_bookmarks_user_id_account_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_bookmarks
    ADD CONSTRAINT user_bookmarks_user_id_account_id_key UNIQUE (user_id, account_id);


--
-- Name: user_fan_rankings_monthly_archive user_fan_rankings_monthly_archive_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_fan_rankings_monthly_archive
    ADD CONSTRAINT user_fan_rankings_monthly_archive_pkey PRIMARY KEY (year, month, viewer_id);


--
-- Name: user_identities user_identities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_identities
    ADD CONSTRAINT user_identities_pkey PRIMARY KEY (id);


--
-- Name: user_identities user_identities_provider_provider_user_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_identities
    ADD CONSTRAINT user_identities_provider_provider_user_id_key UNIQUE (provider, provider_user_id);


--
-- Name: user_privacy_settings user_privacy_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_privacy_settings
    ADD CONSTRAINT user_privacy_settings_pkey PRIMARY KEY (account_id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: veteran_characters veteran_characters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.veteran_characters
    ADD CONSTRAINT veteran_characters_pkey PRIMARY KEY (account_id, trained_chara_id);


--
-- Name: veteran_pins veteran_pins_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.veteran_pins
    ADD CONSTRAINT veteran_pins_pkey PRIMARY KEY (account_id, trained_chara_id);


--
-- Name: viewer_activity_daily viewer_activity_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_activity_daily
    ADD CONSTRAINT viewer_activity_daily_pkey PRIMARY KEY (viewer_id, day);


--
-- Name: viewer_activity_heatmap viewer_activity_heatmap_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_activity_heatmap
    ADD CONSTRAINT viewer_activity_heatmap_pkey PRIMARY KEY (viewer_id, dow, hour);


--
-- Name: viewer_short_career_snapshots viewer_short_career_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_short_career_snapshots
    ADD CONSTRAINT viewer_short_career_snapshots_pkey PRIMARY KEY (viewer_id, rank);


--
-- Name: viewer_suspicion_scores viewer_suspicion_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_suspicion_scores
    ADD CONSTRAINT viewer_suspicion_scores_pkey PRIMARY KEY (viewer_id);


--
-- Name: viewer_top_sessions viewer_top_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.viewer_top_sessions
    ADD CONSTRAINT viewer_top_sessions_pkey PRIMARY KEY (viewer_id, rank);


--
-- Name: votes votes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.votes
    ADD CONSTRAINT votes_pkey PRIMARY KEY (id);


--
-- Name: votes votes_record_id_user_ip_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.votes
    ADD CONSTRAINT votes_record_id_user_ip_key UNIQUE (record_id, user_ip);


--
-- Name: circle_search_documents_circle_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX circle_search_documents_circle_id_idx ON public.circle_search_documents USING btree (circle_id);


--
-- Name: circle_search_documents_text_trgm_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX circle_search_documents_text_trgm_idx ON public.circle_search_documents USING gin (search_text public.gin_trgm_ops);


--
-- Name: idx_api_key_usage_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_api_key_usage_date ON public.api_key_usage USING btree (date);


--
-- Name: idx_api_keys_key_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_api_keys_key_hash ON public.api_keys USING btree (key_hash);


--
-- Name: idx_api_keys_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_api_keys_user_id ON public.api_keys USING btree (user_id);


--
-- Name: idx_blue_factors_composite; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_blue_factors_composite ON public.blue_factors USING btree (factor_type, level, record_id);


--
-- Name: idx_blue_factors_record; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_blue_factors_record ON public.blue_factors USING btree (record_id);


--
-- Name: idx_blue_factors_type_level; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_blue_factors_type_level ON public.blue_factors USING btree (factor_type, level);


--
-- Name: idx_bookmark_content_hash_backfill_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bookmark_content_hash_backfill_pending ON public.bookmark_content_hash_backfill_queue USING btree (queued_at, account_id) WHERE (processed_at IS NULL);


--
-- Name: idx_borrow_interaction_buckets_actor_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_buckets_actor_recent ON public.borrow_interaction_buckets USING btree (actor_hash, bucket_start DESC);


--
-- Name: idx_borrow_interaction_buckets_bucket_start; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_buckets_bucket_start ON public.borrow_interaction_buckets USING btree (bucket_start);


--
-- Name: idx_borrow_interaction_buckets_combo_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_buckets_combo_recent ON public.borrow_interaction_buckets USING btree (trainer_id, inheritance_id, support_card_id, interaction_type, bucket_start DESC);


--
-- Name: idx_borrow_interaction_buckets_v2_bucket_start; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_buckets_v2_bucket_start ON public.borrow_interaction_buckets_v2 USING btree (bucket_start);


--
-- Name: idx_borrow_interaction_buckets_v2_combo_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_buckets_v2_combo_recent ON public.borrow_interaction_buckets_v2 USING btree (trainer_id, borrow_key, interaction_type, bucket_start DESC);


--
-- Name: idx_borrow_interaction_totals_combo; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_totals_combo ON public.borrow_interaction_totals USING btree (inheritance_id, support_card_id, updated_at DESC);


--
-- Name: idx_borrow_interaction_totals_recent_copies; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_totals_recent_copies ON public.borrow_interaction_totals USING btree (last_copied_at DESC) WHERE (last_copied_at IS NOT NULL);


--
-- Name: idx_borrow_interaction_totals_recent_views; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_totals_recent_views ON public.borrow_interaction_totals USING btree (last_viewed_at DESC) WHERE (last_viewed_at IS NOT NULL);


--
-- Name: idx_borrow_interaction_totals_trainer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_totals_trainer ON public.borrow_interaction_totals USING btree (trainer_id, updated_at DESC);


--
-- Name: idx_borrow_interaction_totals_trending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_totals_trending ON public.borrow_interaction_totals USING btree (((view_count + (copy_count * 3))) DESC, updated_at DESC);


--
-- Name: idx_borrow_interaction_totals_v2_combo; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_totals_v2_combo ON public.borrow_interaction_totals_v2 USING btree (borrow_key, updated_at DESC);


--
-- Name: idx_borrow_interaction_totals_v2_trending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_totals_v2_trending ON public.borrow_interaction_totals_v2 USING btree (copy_count DESC, view_count DESC, updated_at DESC);


--
-- Name: idx_borrow_interaction_trends_combo; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_trends_combo ON public.borrow_interaction_trends USING btree (trainer_id, borrow_key, trend_date DESC);


--
-- Name: idx_borrow_interaction_trends_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_borrow_interaction_trends_recent ON public.borrow_interaction_trends USING btree (trend_date DESC, copy_count DESC, view_count DESC);


--
-- Name: idx_carat_plan_shares_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_carat_plan_shares_user_id ON public.carat_plan_shares USING btree (user_id);


--
-- Name: idx_circle_leader; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_leader ON public.circles USING btree (leader_viewer_id);


--
-- Name: idx_circle_live_ranks_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_circle_live_ranks_id ON public.circle_live_ranks USING btree (circle_id);


--
-- Name: idx_circle_live_ranks_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_live_ranks_rank ON public.circle_live_ranks USING btree (live_rank);


--
-- Name: idx_circle_live_ranks_yesterday_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_live_ranks_yesterday_rank ON public.circle_live_ranks USING btree (live_yesterday_rank);


--
-- Name: idx_circle_member_fans_circle_month; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_member_fans_circle_month ON public.circle_member_fans_monthly USING btree (circle_id, year, month);


--
-- Name: idx_circle_member_fans_member_month; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_member_fans_member_month ON public.circle_member_fans_monthly USING btree (viewer_id, year, month);


--
-- Name: idx_circle_member_fans_month; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_member_fans_month ON public.circle_member_fans_monthly USING btree (year, month);


--
-- Name: idx_circle_member_fans_search; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_member_fans_search ON public.circle_member_fans_monthly USING btree (circle_id, year, month);


--
-- Name: idx_circle_member_fans_viewer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_member_fans_viewer ON public.circle_member_fans_monthly USING btree (viewer_id);


--
-- Name: idx_circle_member_fans_viewer_text; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_member_fans_viewer_text ON public.circle_member_fans_monthly USING btree (((viewer_id)::text));


--
-- Name: idx_circle_member_fans_viewer_text_month; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_member_fans_viewer_text_month ON public.circle_member_fans_monthly USING btree (((viewer_id)::text), year, month) INCLUDE (circle_id);


--
-- Name: idx_circle_member_fans_ym_circle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_member_fans_ym_circle ON public.circle_member_fans_monthly USING btree (year, month, circle_id);


--
-- Name: idx_circle_ranks_archive_month_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_ranks_archive_month_rank ON public.circle_ranks_monthly_archive USING btree (year, month, rank, circle_id);


--
-- Name: idx_circle_ranks_archive_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_ranks_archive_name ON public.circle_ranks_monthly_archive USING gin (circle_name public.gin_trgm_ops);


--
-- Name: idx_circle_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circle_updated ON public.circles USING btree (last_updated);


--
-- Name: idx_circles_active_monthly_point; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_active_monthly_point ON public.circles USING btree (monthly_point DESC) WHERE ((archived = false) AND (monthly_point IS NOT NULL));


--
-- Name: idx_circles_last_live_update; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_last_live_update ON public.circles USING btree (last_live_update) WHERE ((live_rank IS NOT NULL) OR (live_points IS NOT NULL));


--
-- Name: idx_circles_leader_viewer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_leader_viewer_id ON public.circles USING btree (leader_viewer_id);


--
-- Name: idx_circles_leader_viewer_text; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_leader_viewer_text ON public.circles USING btree (((leader_viewer_id)::text));


--
-- Name: idx_circles_list_filter; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_list_filter ON public.circles USING btree (last_updated, monthly_rank) WHERE ((archived IS NULL) OR (archived = false));


--
-- Name: idx_circles_live_points; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_live_points ON public.circles USING btree (live_points DESC NULLS LAST) WHERE ((archived IS NULL) OR (archived = false));


--
-- Name: idx_circles_live_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_live_rank ON public.circles USING btree (live_rank) WHERE ((live_rank > 0) AND (live_points > 0) AND ((archived IS NULL) OR (archived = false)));


--
-- Name: idx_circles_name_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_name_trgm ON public.circles USING gin (name public.gin_trgm_ops);


--
-- Name: idx_circles_ranking_monthly; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_ranking_monthly ON public.circles USING btree (last_updated, monthly_point DESC) WHERE ((archived IS NULL) OR (archived = false));


--
-- Name: idx_circles_ranking_yesterday; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_circles_ranking_yesterday ON public.circles USING btree (last_updated, yesterday_points DESC) WHERE ((archived IS NULL) OR (archived = false));


--
-- Name: idx_daily_stats_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_stats_date ON public.daily_stats USING btree (date);


--
-- Name: idx_daily_stats_date_performance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_stats_date_performance ON public.daily_stats USING btree (date DESC);


--
-- Name: idx_daily_visitor_counters_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_visitor_counters_date ON public.daily_visitor_counters USING btree (date);


--
-- Name: idx_fan_snapshots_circle_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_fan_snapshots_circle_time ON public.circle_member_fan_snapshots USING btree (circle_id, snapshot_time DESC);


--
-- Name: idx_fan_snapshots_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_fan_snapshots_time ON public.circle_member_fan_snapshots USING btree (snapshot_time DESC);


--
-- Name: idx_fan_snapshots_viewer_ids_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_fan_snapshots_viewer_ids_gin ON public.circle_member_fan_snapshots USING gin (viewer_ids);


--
-- Name: idx_friendlist_reports_reported_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_friendlist_reports_reported_at ON public.friendlist_reports USING btree (reported_at);


--
-- Name: idx_friendlist_reports_trainer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_friendlist_reports_trainer ON public.friendlist_reports USING btree (trainer_id);


--
-- Name: idx_inheritance_account_default_affinity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_account_default_affinity ON public.inheritance USING btree (account_id, ((base_affinity + race_affinity)) DESC);


--
-- Name: idx_inheritance_account_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_account_id ON public.inheritance USING btree (account_id) WHERE (inheritance_id IS NOT NULL);


--
-- Name: idx_inheritance_account_rarity_win; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_account_rarity_win ON public.inheritance USING btree (account_id, parent_rarity DESC, win_count DESC, white_count DESC);


--
-- Name: idx_inheritance_account_wins; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_account_wins ON public.inheritance USING btree (account_id, win_count DESC, parent_rank DESC);


--
-- Name: idx_inheritance_affinity_default; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_affinity_default ON public.inheritance USING btree (((COALESCE(base_affinity, 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_blue_sparks; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_blue_sparks ON public.inheritance USING gin (blue_sparks);


--
-- Name: idx_inheritance_blue_sparks_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_blue_sparks_gin ON public.inheritance USING gin (blue_sparks public.gin__int_ops);


--
-- Name: idx_inheritance_blue_stars_sum; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_blue_stars_sum ON public.inheritance USING btree (blue_stars_sum);


--
-- Name: idx_inheritance_default_affinity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_default_affinity ON public.inheritance USING btree (((COALESCE(base_affinity, 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_filter_composite; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_filter_composite ON public.inheritance USING btree (parent_rarity DESC, win_count DESC, white_count DESC, main_white_count DESC);


--
-- Name: idx_inheritance_green_sparks; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_green_sparks ON public.inheritance USING gin (green_sparks);


--
-- Name: idx_inheritance_green_sparks_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_green_sparks_gin ON public.inheritance USING gin (green_sparks public.gin__int_ops);


--
-- Name: idx_inheritance_green_stars_sum; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_green_stars_sum ON public.inheritance USING btree (green_stars_sum);


--
-- Name: idx_inheritance_left_chara; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_left_chara ON public.inheritance USING btree (left_chara_id);


--
-- Name: idx_inheritance_main_blue_factors; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_blue_factors ON public.inheritance USING btree (main_blue_factors);


--
-- Name: idx_inheritance_main_chara; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_chara ON public.inheritance USING btree (main_chara_id);


--
-- Name: idx_inheritance_main_chara_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_chara_id ON public.inheritance USING btree (main_chara_id);


--
-- Name: idx_inheritance_main_chara_id_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_chara_id_hash ON public.inheritance USING hash (main_chara_id);


--
-- Name: idx_inheritance_main_character; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_character ON public.inheritance_records USING btree (main_character_id);


--
-- Name: idx_inheritance_main_green_factors; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_green_factors ON public.inheritance USING btree (main_green_factors);


--
-- Name: idx_inheritance_main_parent_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_parent_id ON public.inheritance USING btree (main_parent_id);


--
-- Name: idx_inheritance_main_pink_factors; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_pink_factors ON public.inheritance USING btree (main_pink_factors);


--
-- Name: idx_inheritance_main_white_factors_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_main_white_factors_gin ON public.inheritance USING gin (main_white_factors public.gin__int_ops);


--
-- Name: idx_inheritance_parent_left_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_parent_left_id ON public.inheritance USING btree (parent_left_id);


--
-- Name: idx_inheritance_parent_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_parent_rank ON public.inheritance USING btree (parent_rank);


--
-- Name: idx_inheritance_parent_rarity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_parent_rarity ON public.inheritance USING btree (parent_rarity);


--
-- Name: idx_inheritance_parent_right_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_parent_right_id ON public.inheritance USING btree (parent_right_id);


--
-- Name: idx_inheritance_parents; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_parents ON public.inheritance_records USING btree (parent1_id, parent2_id);


--
-- Name: idx_inheritance_pink_sparks; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_pink_sparks ON public.inheritance USING gin (pink_sparks);


--
-- Name: idx_inheritance_pink_sparks_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_pink_sparks_gin ON public.inheritance USING gin (pink_sparks public.gin__int_ops);


--
-- Name: idx_inheritance_pink_stars_sum; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_pink_stars_sum ON public.inheritance USING btree (pink_stars_sum);


--
-- Name: idx_inheritance_race_affinity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_race_affinity ON public.inheritance USING btree (race_affinity DESC NULLS LAST);


--
-- Name: idx_inheritance_right_chara; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_right_chara ON public.inheritance USING btree (right_chara_id);


--
-- Name: idx_inheritance_search; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_search ON public.inheritance USING btree (account_id, main_parent_id, parent_left_id, parent_right_id);


--
-- Name: idx_inheritance_submitted; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_submitted ON public.inheritance_records USING btree (submitted_at DESC);


--
-- Name: idx_inheritance_total_affinity_1001; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1001 ON public.inheritance USING btree (((COALESCE(affinity_scores[1], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1002; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1002 ON public.inheritance USING btree (((COALESCE(affinity_scores[2], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1003; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1003 ON public.inheritance USING btree (((COALESCE(affinity_scores[3], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1004; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1004 ON public.inheritance USING btree (((COALESCE(affinity_scores[4], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1005; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1005 ON public.inheritance USING btree (((COALESCE(affinity_scores[5], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1006; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1006 ON public.inheritance USING btree (((COALESCE(affinity_scores[6], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1007; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1007 ON public.inheritance USING btree (((COALESCE(affinity_scores[7], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1008; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1008 ON public.inheritance USING btree (((COALESCE(affinity_scores[8], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1009; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1009 ON public.inheritance USING btree (((COALESCE(affinity_scores[9], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1010; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1010 ON public.inheritance USING btree (((COALESCE(affinity_scores[10], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1011; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1011 ON public.inheritance USING btree (((COALESCE(affinity_scores[11], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1012; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1012 ON public.inheritance USING btree (((COALESCE(affinity_scores[12], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1013; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1013 ON public.inheritance USING btree (((COALESCE(affinity_scores[13], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1014; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1014 ON public.inheritance USING btree (((COALESCE(affinity_scores[14], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1015; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1015 ON public.inheritance USING btree (((COALESCE(affinity_scores[15], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1016; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1016 ON public.inheritance USING btree (((COALESCE(affinity_scores[16], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1017; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1017 ON public.inheritance USING btree (((COALESCE(affinity_scores[17], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1018; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1018 ON public.inheritance USING btree (((COALESCE(affinity_scores[18], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1019; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1019 ON public.inheritance USING btree (((COALESCE(affinity_scores[19], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1020; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1020 ON public.inheritance USING btree (((COALESCE(affinity_scores[20], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1021; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1021 ON public.inheritance USING btree (((COALESCE(affinity_scores[21], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1022; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1022 ON public.inheritance USING btree (((COALESCE(affinity_scores[22], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1023; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1023 ON public.inheritance USING btree (((COALESCE(affinity_scores[23], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1024; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1024 ON public.inheritance USING btree (((COALESCE(affinity_scores[24], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1025; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1025 ON public.inheritance USING btree (((COALESCE(affinity_scores[25], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1026; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1026 ON public.inheritance USING btree (((COALESCE(affinity_scores[26], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1027; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1027 ON public.inheritance USING btree (((COALESCE(affinity_scores[27], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1028; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1028 ON public.inheritance USING btree (((COALESCE(affinity_scores[28], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1030; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1030 ON public.inheritance USING btree (((COALESCE(affinity_scores[30], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1032; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1032 ON public.inheritance USING btree (((COALESCE(affinity_scores[32], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1033; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1033 ON public.inheritance USING btree (((COALESCE(affinity_scores[33], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1034; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1034 ON public.inheritance USING btree (((COALESCE(affinity_scores[34], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1035; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1035 ON public.inheritance USING btree (((COALESCE(affinity_scores[35], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1037; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1037 ON public.inheritance USING btree (((COALESCE(affinity_scores[37], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1038; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1038 ON public.inheritance USING btree (((COALESCE(affinity_scores[38], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1039; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1039 ON public.inheritance USING btree (((COALESCE(affinity_scores[39], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1040; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1040 ON public.inheritance USING btree (((COALESCE(affinity_scores[40], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1041; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1041 ON public.inheritance USING btree (((COALESCE(affinity_scores[41], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1044; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1044 ON public.inheritance USING btree (((COALESCE(affinity_scores[44], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1045; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1045 ON public.inheritance USING btree (((COALESCE(affinity_scores[45], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1046; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1046 ON public.inheritance USING btree (((COALESCE(affinity_scores[46], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1048; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1048 ON public.inheritance USING btree (((COALESCE(affinity_scores[48], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1050; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1050 ON public.inheritance USING btree (((COALESCE(affinity_scores[50], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1051; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1051 ON public.inheritance USING btree (((COALESCE(affinity_scores[51], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1052; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1052 ON public.inheritance USING btree (((COALESCE(affinity_scores[52], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1056; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1056 ON public.inheritance USING btree (((COALESCE(affinity_scores[56], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1058; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1058 ON public.inheritance USING btree (((COALESCE(affinity_scores[58], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1059; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1059 ON public.inheritance USING btree (((COALESCE(affinity_scores[59], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1060; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1060 ON public.inheritance USING btree (((COALESCE(affinity_scores[60], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1061; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1061 ON public.inheritance USING btree (((COALESCE(affinity_scores[61], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1068; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1068 ON public.inheritance USING btree (((COALESCE(affinity_scores[68], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1069; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1069 ON public.inheritance USING btree (((COALESCE(affinity_scores[69], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_total_affinity_1071; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_total_affinity_1071 ON public.inheritance USING btree (((COALESCE(affinity_scores[71], 0) + COALESCE(race_affinity, 0))) DESC);


--
-- Name: idx_inheritance_trainer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_trainer ON public.inheritance_records USING btree (trainer_id);


--
-- Name: idx_inheritance_trainer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_trainer_id ON public.inheritance USING btree (account_id);


--
-- Name: idx_inheritance_verified; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_verified ON public.inheritance_records USING btree (verified);


--
-- Name: idx_inheritance_white_count; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_white_count ON public.inheritance USING btree (white_count DESC);


--
-- Name: idx_inheritance_white_sparks; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_white_sparks ON public.inheritance USING gin (white_sparks);


--
-- Name: idx_inheritance_white_sparks_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_white_sparks_gin ON public.inheritance USING gin (white_sparks public.gin__int_ops);


--
-- Name: idx_inheritance_white_stars_sum; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_white_stars_sum ON public.inheritance USING btree (white_stars_sum);


--
-- Name: idx_inheritance_win_count; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_win_count ON public.inheritance USING btree (win_count DESC);


--
-- Name: idx_inheritance_win_count_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_inheritance_win_count_desc ON public.inheritance USING btree (win_count DESC, account_id);


--
-- Name: idx_linked_accounts_account_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_linked_accounts_account_id ON public.linked_accounts USING btree (account_id);


--
-- Name: idx_linked_accounts_pending_token; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_linked_accounts_pending_token ON public.linked_accounts USING btree (verification_token) WHERE ((verification_status)::text = 'pending'::text);


--
-- Name: idx_linked_accounts_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_linked_accounts_status ON public.linked_accounts USING btree (verification_status);


--
-- Name: idx_linked_accounts_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_linked_accounts_user_id ON public.linked_accounts USING btree (user_id);


--
-- Name: idx_partner_inheritance_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_partner_inheritance_user_id ON public.partner_inheritance USING btree (user_id);


--
-- Name: idx_pink_factors_composite; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pink_factors_composite ON public.pink_factors USING btree (factor_type, level, record_id);


--
-- Name: idx_pink_factors_record; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pink_factors_record ON public.pink_factors USING btree (record_id);


--
-- Name: idx_pink_factors_type_level; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pink_factors_type_level ON public.pink_factors USING btree (factor_type, level);


--
-- Name: idx_stats_counts_singleton; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_stats_counts_singleton ON public.stats_counts USING btree (singleton_key);


--
-- Name: idx_support_card_account_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_support_card_account_unique ON public.support_card USING btree (account_id);


--
-- Name: idx_support_card_exp_desc_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_exp_desc_account ON public.support_card USING btree (experience DESC, account_id);


--
-- Name: idx_support_card_experience_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_experience_account ON public.support_card USING btree (experience DESC NULLS LAST, account_id);


--
-- Name: idx_support_card_filter; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_filter ON public.support_card USING btree (account_id, support_card_id, limit_break_count, experience);


--
-- Name: idx_support_card_id_lb_exp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_id_lb_exp ON public.support_card USING btree (support_card_id, limit_break_count, experience DESC, account_id) WHERE (support_card_id IS NOT NULL);


--
-- Name: idx_support_card_records_card_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_records_card_id ON public.support_card_records USING btree (card_id);


--
-- Name: idx_support_card_records_limit_break; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_records_limit_break ON public.support_card_records USING btree (limit_break);


--
-- Name: idx_support_card_records_submitted_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_records_submitted_at ON public.support_card_records USING btree (submitted_at);


--
-- Name: idx_support_card_records_trainer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_records_trainer_id ON public.support_card_records USING btree (trainer_id);


--
-- Name: idx_support_card_records_type_rarity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_records_type_rarity ON public.support_card_records USING btree (card_type, rarity);


--
-- Name: idx_support_card_records_type_rarity_lb; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_records_type_rarity_lb ON public.support_card_records USING btree (card_type, rarity, limit_break);


--
-- Name: idx_support_card_records_upvotes; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_support_card_records_upvotes ON public.support_card_records USING btree (upvotes DESC);


--
-- Name: idx_task_attempts_account_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_attempts_account_recent ON public.task_attempts USING btree (account_id, created_at DESC) WHERE (account_id IS NOT NULL);


--
-- Name: idx_task_attempts_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_attempts_created_at ON public.task_attempts USING btree (created_at DESC);


--
-- Name: idx_task_attempts_endpoint_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_attempts_endpoint_recent ON public.task_attempts USING btree (endpoint, dialect, created_at DESC);


--
-- Name: idx_task_attempts_proxy_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_task_attempts_proxy_recent ON public.task_attempts USING btree (proxy_label, created_at DESC) WHERE (proxy_label IS NOT NULL);


--
-- Name: idx_tasks_account_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_account_status ON public.tasks USING btree (account_id, status, updated_at);


--
-- Name: idx_tasks_active_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_tasks_active_unique ON public.tasks USING btree (task_type, task_data) WHERE ((status)::text = ANY (ARRAY[('pending'::character varying)::text, ('processing'::character varying)::text]));


--
-- Name: idx_tasks_dedup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_dedup ON public.tasks USING btree (task_type, status) WHERE ((status)::text = ANY (ARRAY[('pending'::character varying)::text, ('processing'::character varying)::text]));


--
-- Name: idx_tasks_friend_viewer_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_friend_viewer_id ON public.tasks USING btree (((task_data ->> 'friend_viewer_id'::text))) WHERE (((task_type)::text = 'friend/search'::text) AND ((status)::text = ANY (ARRAY[('pending'::character varying)::text, ('processing'::character varying)::text])));


--
-- Name: idx_tasks_management_claim_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_management_claim_pending ON public.tasks USING btree (priority, created_at, id) WHERE (((status)::text = 'pending'::text) AND (((task_type)::text = ANY (ARRAY[('circle/ranking_top'::character varying)::text, ('circle/ranking_full'::character varying)::text])) OR (COALESCE((task_data ->> 'management_only'::text), 'false'::text) = 'true'::text)));


--
-- Name: idx_tasks_pending_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_pending_claim ON public.tasks USING btree (priority, created_at, id) WHERE ((status)::text = 'pending'::text);


--
-- Name: idx_tasks_pending_priority; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_pending_priority ON public.tasks USING btree (priority, created_at) WHERE ((status)::text = 'pending'::text);


--
-- Name: idx_tasks_pending_priority_created_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_pending_priority_created_id ON public.tasks USING btree (priority, created_at, id) WHERE ((status)::text = 'pending'::text);


--
-- Name: idx_tasks_priority; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_priority ON public.tasks USING btree (priority DESC);


--
-- Name: idx_tasks_processing_stale; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_processing_stale ON public.tasks USING btree (updated_at, task_type) WHERE ((status)::text = 'processing'::text);


--
-- Name: idx_tasks_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_status ON public.tasks USING btree (status);


--
-- Name: idx_tasks_task_data_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_task_data_gin ON public.tasks USING gin (task_data);


--
-- Name: idx_tasks_task_type_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_task_type_status ON public.tasks USING btree (task_type, status, priority, created_at, id);


--
-- Name: idx_tasks_updated_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_updated_status ON public.tasks USING btree (updated_at DESC) WHERE ((status)::text = 'completed'::text);


--
-- Name: idx_tasks_viral_friend_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_tasks_viral_friend_unique ON public.tasks USING btree (task_type, ((task_data ->> 'friend_viewer_id'::text)), status) WHERE (((task_type)::text = 'friend/search'::text) AND ((status)::text = ANY (ARRAY[('pending'::character varying)::text, ('processing'::character varying)::text])));


--
-- Name: idx_tasks_worker_claim_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_tasks_worker_claim_pending ON public.tasks USING btree (priority, created_at, id) WHERE (((status)::text = 'pending'::text) AND (NOT (((task_type)::text = ANY (ARRAY[('circle/ranking_top'::character varying)::text, ('circle/ranking_full'::character varying)::text])) OR (COALESCE((task_data ->> 'management_only'::text), 'false'::text) = 'true'::text))));


--
-- Name: idx_team_stadium_chara; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_chara ON public.team_stadium_characters USING btree (trained_chara_id);


--
-- Name: idx_team_stadium_creation; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_creation ON public.team_stadium_characters USING btree (creation_time);


--
-- Name: idx_team_stadium_distance; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_distance ON public.team_stadium_characters USING btree (distance_type);


--
-- Name: idx_team_stadium_factors_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_factors_gin ON public.team_stadium_characters USING gin (((factors)::jsonb));


--
-- Name: idx_team_stadium_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_rank ON public.team_stadium_characters USING btree (rank_score);


--
-- Name: idx_team_stadium_scenario; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_scenario ON public.team_stadium_characters USING btree (scenario_id);


--
-- Name: idx_team_stadium_skills_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_skills_gin ON public.team_stadium_characters USING gin (((skills)::jsonb));


--
-- Name: idx_team_stadium_stats; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_stats ON public.team_stadium_characters USING btree (speed, power, stamina, wiz, guts);


--
-- Name: idx_team_stadium_style; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_style ON public.team_stadium_characters USING btree (running_style);


--
-- Name: idx_team_stadium_support_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_support_gin ON public.team_stadium_characters USING gin (((support_cards)::jsonb));


--
-- Name: idx_team_stadium_trainer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_stadium_trainer ON public.team_stadium_characters USING btree (trainer_id);


--
-- Name: idx_trainer_account_follower; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_account_follower ON public.trainer USING btree (account_id, follower_num) WHERE ((follower_num IS NULL) OR (follower_num < 1000));


--
-- Name: idx_trainer_account_id_filtered; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_account_id_filtered ON public.trainer USING btree (account_id) WHERE ((follower_num IS NULL) OR (follower_num < 1000));


--
-- Name: idx_trainer_circle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_circle ON public.trainer USING btree (circle_id);


--
-- Name: idx_trainer_deleted_or_banned; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_deleted_or_banned ON public.trainer USING btree (is_deleted_or_banned, deleted_or_banned_at);


--
-- Name: idx_trainer_evaluation; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_evaluation ON public.trainer USING btree (team_evaluation_point DESC);


--
-- Name: idx_trainer_follower_account_composite; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_follower_account_composite ON public.trainer USING btree (account_id, follower_num) WHERE ((follower_num IS NULL) OR (follower_num < 1000));


--
-- Name: idx_trainer_follower_num; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_follower_num ON public.trainer USING btree (follower_num) WHERE ((follower_num IS NULL) OR (follower_num < 1000));


--
-- Name: idx_trainer_last_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_last_updated ON public.trainer USING btree (last_updated);


--
-- Name: idx_trainer_name_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_name_trgm ON public.trainer USING gin (name public.gin_trgm_ops);


--
-- Name: idx_trainer_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainer_updated ON public.trainer USING btree (last_updated);


--
-- Name: idx_trainers_follower_num; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trainers_follower_num ON public.trainer USING btree (follower_num DESC);


--
-- Name: idx_ufr_alltime_circle_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_circle_name ON public.user_fan_rankings_alltime USING gin (circle_name public.gin_trgm_ops);


--
-- Name: idx_ufr_alltime_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_name ON public.user_fan_rankings_alltime USING gin (trainer_name public.gin_trgm_ops);


--
-- Name: idx_ufr_alltime_pk; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_ufr_alltime_pk ON public.user_fan_rankings_alltime USING btree (viewer_id);


--
-- Name: idx_ufr_alltime_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_rank ON public.user_fan_rankings_alltime USING btree (rank);


--
-- Name: idx_ufr_alltime_rank_avg_day; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_rank_avg_day ON public.user_fan_rankings_alltime USING btree (rank_avg_day);


--
-- Name: idx_ufr_alltime_rank_avg_month; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_rank_avg_month ON public.user_fan_rankings_alltime USING btree (rank_avg_month);


--
-- Name: idx_ufr_alltime_rank_avg_week; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_rank_avg_week ON public.user_fan_rankings_alltime USING btree (rank_avg_week);


--
-- Name: idx_ufr_alltime_rank_total_fans; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_rank_total_fans ON public.user_fan_rankings_alltime USING btree (rank_total_fans);


--
-- Name: idx_ufr_alltime_rank_total_gain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_rank_total_gain ON public.user_fan_rankings_alltime USING btree (rank_total_gain);


--
-- Name: idx_ufr_alltime_total_fans; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_total_fans ON public.user_fan_rankings_alltime USING btree (total_fans DESC);


--
-- Name: idx_ufr_alltime_total_gain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_alltime_total_gain ON public.user_fan_rankings_alltime USING btree (total_gain DESC);


--
-- Name: idx_ufr_archive_active_days; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_archive_active_days ON public.user_fan_rankings_monthly_archive USING btree (year, month, active_days DESC);


--
-- Name: idx_ufr_archive_circle_name_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_archive_circle_name_gin ON public.user_fan_rankings_monthly_archive USING gin (circle_name public.gin_trgm_ops);


--
-- Name: idx_ufr_archive_gain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_archive_gain ON public.user_fan_rankings_monthly_archive USING btree (year, month, monthly_gain DESC);


--
-- Name: idx_ufr_archive_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_archive_name ON public.user_fan_rankings_monthly_archive USING gin (trainer_name public.gin_trgm_ops);


--
-- Name: idx_ufr_archive_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_archive_rank ON public.user_fan_rankings_monthly_archive USING btree (year, month, rank);


--
-- Name: idx_ufr_archive_total_fans; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_archive_total_fans ON public.user_fan_rankings_monthly_archive USING btree (year, month, total_fans DESC);


--
-- Name: idx_ufr_archive_viewer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_archive_viewer ON public.user_fan_rankings_monthly_archive USING btree (viewer_id);


--
-- Name: idx_ufr_current_active_days; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_current_active_days ON public.user_fan_rankings_monthly_current USING btree (year, month, active_days DESC);


--
-- Name: idx_ufr_current_gain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_current_gain ON public.user_fan_rankings_monthly_current USING btree (year, month, monthly_gain DESC);


--
-- Name: idx_ufr_current_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_current_name ON public.user_fan_rankings_monthly_current USING gin (trainer_name public.gin_trgm_ops);


--
-- Name: idx_ufr_current_pk; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_ufr_current_pk ON public.user_fan_rankings_monthly_current USING btree (viewer_id, year, month);


--
-- Name: idx_ufr_current_rank; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_current_rank ON public.user_fan_rankings_monthly_current USING btree (year, month, rank);


--
-- Name: idx_ufr_current_total_fans; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_current_total_fans ON public.user_fan_rankings_monthly_current USING btree (year, month, total_fans DESC);


--
-- Name: idx_ufr_current_viewer; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_current_viewer ON public.user_fan_rankings_monthly_current USING btree (viewer_id);


--
-- Name: idx_ufr_gains_circle_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_gains_circle_name ON public.user_fan_rankings_gains USING gin (circle_name public.gin_trgm_ops);


--
-- Name: idx_ufr_gains_gain_30d; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_gains_gain_30d ON public.user_fan_rankings_gains USING btree (gain_30d DESC);


--
-- Name: idx_ufr_gains_gain_3d; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_gains_gain_3d ON public.user_fan_rankings_gains USING btree (gain_3d DESC);


--
-- Name: idx_ufr_gains_gain_7d; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_gains_gain_7d ON public.user_fan_rankings_gains USING btree (gain_7d DESC);


--
-- Name: idx_ufr_gains_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_gains_name ON public.user_fan_rankings_gains USING gin (trainer_name public.gin_trgm_ops);


--
-- Name: idx_ufr_gains_pk; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_ufr_gains_pk ON public.user_fan_rankings_gains USING btree (viewer_id);


--
-- Name: idx_ufr_gains_rank_30d; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_gains_rank_30d ON public.user_fan_rankings_gains USING btree (rank_30d);


--
-- Name: idx_ufr_gains_rank_3d; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_gains_rank_3d ON public.user_fan_rankings_gains USING btree (rank_3d);


--
-- Name: idx_ufr_gains_rank_7d; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ufr_gains_rank_7d ON public.user_fan_rankings_gains USING btree (rank_7d);


--
-- Name: idx_unique_skills_composite; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_unique_skills_composite ON public.unique_skills USING btree (skill_id, level, record_id);


--
-- Name: idx_unique_skills_record; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_unique_skills_record ON public.unique_skills USING btree (record_id);


--
-- Name: idx_unique_skills_skill_level; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_unique_skills_skill_level ON public.unique_skills USING btree (skill_id, level);


--
-- Name: idx_user_bookmarks_account_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_bookmarks_account_id ON public.user_bookmarks USING btree (account_id);


--
-- Name: idx_user_bookmarks_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_bookmarks_user_id ON public.user_bookmarks USING btree (user_id);


--
-- Name: idx_user_identities_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_identities_user_id ON public.user_identities USING btree (user_id);


--
-- Name: idx_veteran_characters_account_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_veteran_characters_account_id ON public.veteran_characters USING btree (account_id);


--
-- Name: idx_veteran_characters_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_veteran_characters_id ON public.veteran_characters USING btree (id);


--
-- Name: idx_veteran_pins_account_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_veteran_pins_account_id ON public.veteran_pins USING btree (account_id);


--
-- Name: idx_votes_record; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_votes_record ON public.votes USING btree (record_id);


--
-- Name: viewer_activity_daily_day_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_activity_daily_day_idx ON public.viewer_activity_daily USING btree (day);


--
-- Name: viewer_short_career_snapshots_viewer_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_short_career_snapshots_viewer_time_idx ON public.viewer_short_career_snapshots USING btree (viewer_id, snapshot_time DESC);


--
-- Name: viewer_suspicion_scores_avg_careers_day_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_avg_careers_day_idx ON public.viewer_suspicion_scores USING btree (avg_careers_per_day DESC, suspicion_score DESC);


--
-- Name: viewer_suspicion_scores_behavior_change_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_behavior_change_idx ON public.viewer_suspicion_scores USING btree (behavior_change_score DESC, fan_gain_spike_ratio DESC);


--
-- Name: viewer_suspicion_scores_probe_metrics_gin_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_probe_metrics_gin_idx ON public.viewer_suspicion_scores USING gin (probe_metrics jsonb_path_ops);


--
-- Name: viewer_suspicion_scores_probe_score_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_probe_score_idx ON public.viewer_suspicion_scores USING btree (probe_score DESC, suspicion_score DESC);


--
-- Name: viewer_suspicion_scores_reset_break_score_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_reset_break_score_idx ON public.viewer_suspicion_scores USING btree (reset_break_score DESC, reset_breaks DESC);


--
-- Name: viewer_suspicion_scores_score_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_score_idx ON public.viewer_suspicion_scores USING btree (suspicion_score DESC, max_session_seconds DESC);


--
-- Name: viewer_suspicion_scores_session_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_session_idx ON public.viewer_suspicion_scores USING btree (max_session_seconds DESC);


--
-- Name: viewer_suspicion_scores_short_fan_gain_score_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_short_fan_gain_score_idx ON public.viewer_suspicion_scores USING btree (short_fan_gain_score DESC, short_high_fan_careers DESC);


--
-- Name: viewer_suspicion_scores_short_high_fan_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_short_high_fan_idx ON public.viewer_suspicion_scores USING btree (short_high_fan_careers DESC, suspicion_score DESC);


--
-- Name: viewer_suspicion_scores_trainer_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX viewer_suspicion_scores_trainer_name_idx ON public.viewer_suspicion_scores USING btree (trainer_name);


--
-- Name: friendlist_reports friendlist_reports_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER friendlist_reports_trigger AFTER INSERT ON public.friendlist_reports FOR EACH ROW EXECUTE FUNCTION public.update_friendlist_reports_count();


--
-- Name: circle_member_fans_monthly trg_fix_daily_fans_bounds; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_fix_daily_fans_bounds BEFORE INSERT OR UPDATE OF daily_fans ON public.circle_member_fans_monthly FOR EACH ROW EXECUTE FUNCTION public.fix_daily_fans_array_bounds();


--
-- Name: inheritance trg_inheritance_content_hash; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_inheritance_content_hash BEFORE INSERT OR UPDATE ON public.inheritance FOR EACH ROW EXECUTE FUNCTION public.inheritance_content_hash();


--
-- Name: tasks trg_notify_task_completion; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_notify_task_completion AFTER UPDATE OF status ON public.tasks FOR EACH ROW EXECUTE FUNCTION public.notify_task_completion();


--
-- Name: partner_inheritance trg_partner_inheritance_content_hash; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_partner_inheritance_content_hash BEFORE INSERT OR UPDATE ON public.partner_inheritance FOR EACH ROW EXECUTE FUNCTION public.partner_inheritance_content_hash();


--
-- Name: api_key_usage api_key_usage_api_key_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_usage
    ADD CONSTRAINT api_key_usage_api_key_id_fkey FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE CASCADE;


--
-- Name: api_keys api_keys_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: blue_factors blue_factors_record_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.blue_factors
    ADD CONSTRAINT blue_factors_record_id_fkey FOREIGN KEY (record_id) REFERENCES public.inheritance_records(id) ON DELETE CASCADE;


--
-- Name: carat_plan_shares carat_plan_shares_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.carat_plan_shares
    ADD CONSTRAINT carat_plan_shares_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: carat_planner_states carat_planner_states_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.carat_planner_states
    ADD CONSTRAINT carat_planner_states_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: inheritance inheritance_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inheritance
    ADD CONSTRAINT inheritance_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.trainer(account_id) ON DELETE CASCADE;


--
-- Name: partner_inheritance partner_inheritance_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.partner_inheritance
    ADD CONSTRAINT partner_inheritance_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: pink_factors pink_factors_record_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pink_factors
    ADD CONSTRAINT pink_factors_record_id_fkey FOREIGN KEY (record_id) REFERENCES public.inheritance_records(id) ON DELETE CASCADE;


--
-- Name: support_card support_card_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.support_card
    ADD CONSTRAINT support_card_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.trainer(account_id);


--
-- Name: unique_skills unique_skills_record_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.unique_skills
    ADD CONSTRAINT unique_skills_record_id_fkey FOREIGN KEY (record_id) REFERENCES public.inheritance_records(id) ON DELETE CASCADE;


--
-- Name: user_bookmarks user_bookmarks_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_bookmarks
    ADD CONSTRAINT user_bookmarks_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: user_identities user_identities_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_identities
    ADD CONSTRAINT user_identities_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: votes votes_record_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.votes
    ADD CONSTRAINT votes_record_id_fkey FOREIGN KEY (record_id) REFERENCES public.inheritance_records(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict H21W7otcCOdU6XaXG3OOoA1ebQ43yOteC33aMrgldzonMrvNwyNXBeGTXm4ZkeG

--
-- PostgreSQL database dump
--

\restrict ZttwIuRO6QBeW8IIghgco6jMCfAkWQrlB8ZmhDwsMdiEolYcAc3XrwHfiMU1Gsw

-- Dumped from database version 17.11 (Debian 17.11-1.pgdg12+2)
-- Dumped by pg_dump version 17.11 (Debian 17.11-1.pgdg12+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: _sqlx_migrations; Type: TABLE DATA; Schema: public; Owner: umamoe
--

COPY public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) FROM stdin;
20251117000002	support card experience index	2025-12-12 03:40:30.002351+00	t	\\xd5783cc6ef3c872fc84a1fb893b16efb34fff0efc8f43412254ae0d386ba20f1c31741e1c9469e60988cee3a43fd5d1e	0
20251117000000	optimize stats	2025-12-12 03:40:19.011834+00	t	\\x43619c8dfb16aa6bc9dff39429a7070dd079bc2c694a2c277ebea95d2b26c5884d095ec606ff935a3e07900f80328b7a	0
20251117000001	support card range index	2025-12-12 03:40:27.563734+00	t	\\x3e6f731579cdba1c73e2803c23fbd775b15bcceaf8ca4f2119d1bdc1eb011cd16a337e8fb39e6e344ddde108cf622203	0
20251117000003	cleanup support card indexes	2025-12-12 03:40:31.197228+00	t	\\x5854c8867e038f9e3dc8fcfb2a26c2f65376ca291b6ff1a9c564b77c4d8e9b40a75596ad71e05abab6a642055d67417a	0
20251201000000	inheritance affinity optimization	2025-12-12 03:45:32.791955+00	t	\\x5f2dd94d03a0f2a067a72303e6ceadaad00108859b4b3a126a5079d4ee8e00da5602636aa3935ed16eaae856ad89204c	0
20251208000000	add search indexes	2025-12-12 03:45:32.794764+00	t	\\x5622d89278dedcb006fa3779543ee8b4bf7012fc4eac5ae963632088331612b29604d1ed146d2b0b546fda34cd237e9e	0
20251209000000	add circle indexes	2025-12-12 03:45:34.163932+00	t	\\xa7529ee109c690919c3bd64637aed64ab4af72540a9df8a6236609bf53bcf5e0cd9b038bfdcde38527b8eb8c86c06ce2	0
20251210000000	optimize slow queries	2025-12-12 03:45:34.545431+00	t	\\x992a6842c5fadcf17572a48c8655b813e9dc8ed539e073d01b9477dbb438af8b9bbde5804bf42907ddeff0cb9b53489d	0
20251211000000	optimize circle joins	2025-12-12 03:45:43.640046+00	t	\\x43a2f285bb3ce66a069ececafb4e18d4cd1c99afb0ae40f826ea978b2d3da9175d1eff78cb789ed3ba05f456f623691f	0
20251212000000	optimize spark scoring	2025-12-12 03:45:46.853706+00	t	\\x29b9254f028249b84b2bb437f7ddc34865e9e3a2ec3af199fcf2af1d403e2160618727d059b08f9575dc063810fae780	0
20251213000000	add star sum columns	2026-01-04 17:55:44.725807+00	t	\\xa29d5b157569ab38e7204965ce907c22343704cd4d66027bdceb5504b24af92152f5017e8d8b82ae87d2b5b1816fc627	0
20260104000000	optimize query performance	2026-01-04 18:46:35.936731+00	t	\\x31a21865f769b00d4b2c80bc82f660535ddb8a885fd9413fd988930e34dc416604a7a39f42834c5e3d5ea86b273d83e7	0
20260104000001	remove star sums trigger	2026-01-04 18:46:45.190347+00	t	\\xc5282390c571a3d2959b4ae254621df87b0d551d2fa86e4c5e945cb0372157ffd2887169d50f4c44effa13c5b57d11ec	0
20260316000000	add veteran characters	2026-03-31 18:20:07.173469+00	t	\\x75b3eb50c5ab5ebea5f375d17020d1ac5cc0c7774dbff2fa702c86128e46d497f3df1c049a0864335e8d529714d8b3f8	0
20260331000000	fix daily fans array bounds	2026-03-31 18:33:33.490582+00	t	\\x84c424266c88bb60911c2bbec06f3d32dcba77b98aa7a36a2a2fdd5b65e362b004e521f8491c66535532c75ffd5a13d7	0
20260401000000	rebuild ranking views	2026-03-31 23:33:34.101598+00	t	\\x125c1c78797b10adf51ef4bc8209a2838f9dffce3d8a0762bb4095279e4da910f01e8eb61346e10bccde0204037c9a28	0
20260402000000	redesign stats counts	2026-04-02 01:02:47.342198+00	t	\\xe933e5a11cbeba53621fff6163133a4ec6f534514bdf4478e2d200d296c3ab3a94bdc577d1f90545b2f4dd843d9b6015	0
20260403000001	live ranks use last live update	2026-04-03 15:11:22.975766+00	t	\\xa10637c8aaf20c8c53bedadd8143165bdba8ad660fd52cbc510026a01a29345abaeace297d0110e128714af30d29aeed	0
20260422000000	bookmark by account id	2026-04-23 00:06:55.953447+00	t	\\xc4529e6b6b2998910355c465b3d97170f84b72a1a0d552fc0a44624cb80d8e6a482ad84911ee815cf68d2c8255b04734	0
20260502000000	fix circle tally window last month	2026-05-31 15:50:48.761665+00	t	\\x158d68f29bd4f44e411428566baa258eef3a15f61b719caace5dd8ccebd7c46479ef302fbda87c97b5a3135f38aa0093	0
20260502000001	fix circle display until third jst	2026-05-31 16:14:53.869577+00	t	\\x7937ae8541fc3daaefd1b921d4cf71a53378872391a06e9847356cc034556548dd7712986d87d7d0a999969c255c5611	0
20260120000000	fix daily stats average	2026-01-20 09:52:45.130976+00	t	\\xd25c01276e0fecff09d25e68689fdabe6893ebd923c8a73973da593bfc256bb174bbeaadc4ab6412c76ac7e724dde4b0	0
20260213000000	add user fan rankings update	2026-02-15 03:57:09.588909+00	t	\\x8cbcf69bad7d63d6ee6150911c96506168b1feb3a2af9919db41c832c1b58cbeaeeab6818246a7d35ac1299250e44ebb	0
20260214000001	add next month start	2026-02-15 04:12:15.668223+00	t	\\xf2b35d1c944269a8237e43a077c8ff6ae4986dd7ddb3617b5882c95033f2c0adbd6fd2eba1865f65c3c8a616ad2df428	0
20260215000001	alltime per sort ranks	2026-02-15 05:16:40.094725+00	t	\\xc6d89e28c458e14d22ea3c3bd5efc981062632ad9c5d85785c261a84511933231ebbab941e1bc37422f5e03475a94281	0
20260215000003	alltime from raw data	2026-02-15 14:48:02.163651+00	t	\\xcde88b3e2d95a59e7329d4cca907cd01fc3484898cbc9e9808cd50345def524c7835d1ce978e340895f6dff749f285d6	0
20260228000000	fix circle live ranks date	2026-02-28 15:45:58.595754+00	t	\\x298bbe70a4186f05b8f9c775785c09c488a6d66e290388a3104834299c94f8a650aa435352d23fe3926b6173820295ae	0
20260301000001	fix circle live ranks upper bound	2026-03-01 11:04:48.270583+00	t	\\x92cd208fc45987c768029c4ffcbf8ebd0f46d5180c346ff6c08a75c3a06636aa2c3950ce2a21305506ca1287ea445e45	0
20260306000001	fix march daily fans gaps	2026-03-06 18:08:50.45882+00	t	\\xb204f8cab18929a8a48e37405f9048f5cdcbaae595e20d83f015caf09232b2aeba3581837f83bf6f683c6f69e6e3e3ea	0
20260309000001	add user identities	2026-03-31 18:19:55.631055+00	t	\\x846dd934b10a08765d6afa88ff746f0112b2a7aa40f316887b79c44a697d1614bf01dcd9d56122d6552684fa6a999e5f	0
20260311000001	add api keys	2026-03-31 18:20:07.06637+00	t	\\xd7dfa8ec5e0d648651eee36f5d1827a34a3bcc8eee453eafa1c818ca14fb051498aef142de5590e3acfadcdf20be681d	0
20260312000000	profile privacy	2026-03-31 18:20:07.140596+00	t	\\x0718719a8062eea6892645379d786c732025df17761f8f86deb746276f6b6ca3d3723ffcd23c03c493ace130597501e0	0
20260316000002	add succession chara array	2026-03-31 18:20:07.216415+00	t	\\xd06a170bc3ff50feefe53801fbdc77f6f655c458c2f4c64a1ac58c12d7f442363e2855b8faa1fdcdd020e2a48389a733	0
20260321000003	hash emails	2026-03-31 18:20:07.275788+00	t	\\xaad4a23cabe30c52b1ce3cc09128c91c2f3d48f9ebed5feeb60e491fe5d4799dae6161d892ed91d359d305bf3d53911a	0
20260331000001	fix daily fans shifted element	2026-03-31 19:54:23.467748+00	t	\\x90e49a7c8aa989e99db68e7fb3afe70b8dd2439b0a57c01fb5042f7595e542cf4614eab98fbe0a838623eb4bef3ed656	0
20260402000001	fix circle live ranks use live points	2026-04-02 01:02:48.394971+00	t	\\x43f5f6f02d55c161e67826ec8a6fcc61ef11f267484eba4fd92fffb699a03afb9e043c95a90d502aef0a12cc768ff02d	0
20260402000003	fix circle live ranks no greatest	2026-04-02 01:27:16.326112+00	t	\\x99dc959d54bf084a8aadb2fb51361a32f08dd1836497da933e79bc686164542d65a6e51330c1b4d32b7792e3b99302f4	0
20260412000000	add master versions	2026-04-21 20:45:59.156295+00	t	\\x59dd577a0ef0af88b24af996e29e81af08d6589a79d8c0a3edb38630463125e762a247cf45d4f0998887730cce099ac4	0
20260420000000	add partner inheritance	2026-04-21 20:45:59.21513+00	t	\\x6a9022eef92009671db76717b2f94cc5cf84048dfe772b4a91cd3fc159994778047cec374cc895d97c88575722fccffd	0
20260421000001	notify on processing	2026-04-21 20:45:59.249045+00	t	\\x6a15ef60f483f071f5d4d729ff5516458d6f619e4fe246bab1dacc745e859d589fb38722b0a794f6ff327ebda71207a9	0
20260501000000	fix live ranks month rollover	2026-05-01 12:08:03.487854+00	t	\\x7bb10e7057cd12f958228adf8a6829cc00ad72ca84a2b65162866b5858afee51d561dfdfc836dab582bee15e4afbada6	0
20260501000002	fix live ranks tally window	2026-05-01 14:18:14.019615+00	t	\\x2d0be867d3d0d6176ec8112cbf33178825aa2ced491d292bd94c2e0c3583373bdd071af13c933c6c5138c29d6d36c436	0
20260502000002	restore circle live ranks tally	2026-05-31 16:36:12.71236+00	t	\\x42b4cf58c00487c69831487aedf788594576a90de34aa2b5e4e4c480eeba4a89e2bcdeca6d736a5124b334e1c98e4ab2	0
20260502000004	fix circle live rank window	2026-05-31 17:05:50.269128+00	t	\\x634b368876724d93cf1d18d68cfe9206a624d52c414476ea4e70761b667fc758f65331988cc4c4b2044700cd3250c393	0
20260502000006	use row aware circle rollover source	2026-06-01 15:57:10.100429+00	t	\\xe2d6f4d8627d8e5c236373d733cc7a9347005d9008a1420f088f7851e05c7666c3ffdf40a8162319bdfe71b3251915bf	0
20260520000001	cheat analysis simplify	2026-06-02 00:38:17.835581+00	t	\\x7bfdcc9cd9355b341fbb90fd420be21312325b37420ad1cac53b6e4ced1cfb6a14111cff692bf42d35a3b7a686e7326f	0
20260520000003	add fan rate metrics	2026-06-02 00:38:17.981117+00	t	\\x57a3b98eb0490cba3649aae08a5108e192deb5511de9ac3f5bfc1c82f59fa7f349079e5bf633245bf36cd64ee746d865	0
20260520000005	precompute shame hall display	2026-06-02 00:38:18.002405+00	t	\\x28299c96f8fe0f9baa8d0d359149343b8b5254aa66b7899a1fac45cb6a7ee23dcaa19bcd4872a93b8fcc396db1132ec6	0
20260520000007	short fan gain score	2026-06-02 00:38:18.052344+00	t	\\xb26cfe32e12d4a23f1496850ca0ae345d9cbba587129d47c3ab8bbe3b770a22f5297913993fe30ac257f8a8888aec186	0
20260521000000	behavior change metrics	2026-06-02 00:38:18.090543+00	t	\\xccccdec5d73551549d7960c901807af47ceb36a742df772f67f71d17201905465451a8acbb99b0723391fc7dbc0aeb7b	0
20260523000000	add reset break metrics	2026-06-02 00:38:18.158989+00	t	\\x48effa97c32e5e3e857256592a1a398d9d75cf97ec73c8c69913db00bff81a06ab5eec476d7af0c7a25e8609a8a4ed19	0
20260524000001	short career snapshots	2026-06-02 00:38:18.210932+00	t	\\x560d71cbef58d39b46c29f89b620d94d4610d1d016c30afb99414397c6312ac8b793c9a757e3df4cf7f9c99bdfd487a6	0
20260524000003	top playtime days	2026-06-02 00:38:18.263698+00	t	\\x6284e04b29c19ce3b5cf59b1abe076380b719675ef3fc7c732e65285317628eed2c24870af695c6eb20c9eca9b041438	0
20260531000000	add veteran uuid	2026-06-02 00:38:18.312321+00	t	\\xa1f45d9ccdfa3f54f3a628e2ff27f64a9c21618f4eb021188b131a4a31ca390bd0686b35fa6129f66b8342807ae8f379	0
20260628000000	resync legacy borrow interaction totals	2026-06-28 20:31:38.640656+00	t	\\x7c879837557d7aef8d7472bc5e7663bc941a57e3f9304a64a12dc8f32cf085d60887a0e8eecbd0f075062f9b7911493a	0
20260211000000	add user fan rankings	2026-03-31 18:07:24.547123+00	t	\\x3b458a5e6262e91d6d006cc47183acd6423ad5e42b3d28c5201f8225329dcbff99d51767836f9788364d0da9cdb49607	0
20260214000000	add circle to rankings	2026-02-15 04:03:46.16075+00	t	\\x73b564f55ac4152a685b17c7bc353b70567faa1ee5afcdcee4fd55e3d7b31583bb005e1bc6bf85861582b775ae9034eb	0
20260215000000	add circle name gin indexes	2026-02-15 05:16:24.518429+00	t	\\x57e818bcfbda0e069540fd968b5a05f1b165b5944a48d3fdf441e38860ca0f3344cfbe7a3ed2ef1eb75290044e0bd411	0
20260215000002	alltime simplified ranks	2026-02-15 14:19:01.744382+00	t	\\x6eab4ed8fa72ca8c2143b6dc6d28c4be5d48fd513d342531d74bd8a76b9077ac59c0513951e5d2ac6e196b7891b3b658	0
20260215000004	alltime from raw data fixed	2026-02-15 15:17:41.798141+00	t	\\x404699904f1384f2079e7ba2ad1ee97cb8f05a9c469f0fd9b86bc86e6175347103db696602fa2c4218c3a658ca3b3cda	0
20260301000000	delay circle live ranks	2026-03-01 10:33:41.079588+00	t	\\xc4d03c720a614d7d4929d30d74b94d6803345fc3b9e490f6c211d58655560b9973af4909eca8c95de7884b014d3f987b	0
20260306000000	use live points for ranking	2026-03-06 17:04:23.670573+00	t	\\xc41220c809454f0c51e9e08529a38c83a410ff320a44edc29dc8e39b355facbde3fbd0ea8e87c7e34b492d16abf099cc	0
20260309000000	add user accounts	2026-03-31 18:19:55.57939+00	t	\\x9d4f586cd4acecf6ce64ab8be714bb5df904a46ec96cf768c2323f273dc46fd21f9e8fdf75ac61209943b460bb6011e4	0
20260311000000	add circle ranks archive	2026-03-31 18:19:55.68056+00	t	\\xc4e94b737125c260c4d81573d3607761656bb2805d6476ca146d1e8fdda3d4286454f9ca418f38af55d0cde45c2b448d	0
20260311000002	add api key usage	2026-03-31 18:20:07.113834+00	t	\\x85d3911cbb15f179c4f6b1557c84071d8a8276952163b5232e22d6b9f873e9431b8f4424b23a22a7876abd8fa456fa4b	0
20260321000001	add veteran pins	2026-03-31 18:20:07.227045+00	t	\\xee7de03f1b8bba0f092fd00958e204d93c762a9934472ad8d049197ab54403c71fbe83533c7a956eb7c00e8d051440fb	0
20260331000002	fix daily fans leading zeros	2026-03-31 19:58:47.241386+00	t	\\xa61dd2e517cab364346af408443893fbdfc7fba053615e1efbeb97ca39adf1f6e9f507c2b3433272bcf0b77fc97b5b95	0
20260402000002	fix stats counts use team stadium	2026-04-02 01:17:20.795503+00	t	\\x5c8d1da2d568cedabb3cb762f059f554250f6c592bae21f3b88cddb6f4d52f4d3f559ef6f06c2c5364dc781c1f7b852a	0
20260403000000	fix live ranks coalesce	2026-04-03 15:11:22.570742+00	t	\\xfd7491dcab78eb598ce9c3f19fb30c6bb9c3858d70486cf692149ade279b588249bce554ac684f802486d1302e50c1fd	0
20260403000002	live ranks greatest unconditional	2026-04-03 16:49:56.633802+00	t	\\x158ccaac41bb758c91942685068937a252705585f8407dcd564440ff87c1924ae477843e29b714dd0fa5d81a458b37e0	0
20260419000000	add user bookmarks	2026-04-21 20:45:59.164288+00	t	\\x2eb8451e6a81ecfb225dd35813879552e920b40f29ad6749b6a640e25c3bc5f739d43bbf23a52564c297d92dd7e3b01a	0
20260421000000	add trainer name to partner inheritance	2026-04-21 20:45:59.240351+00	t	\\x1c29178dfbfa4e9c1c8e3ad130267373808e3a4f8d3223936af4af4aac49f8dd3ffe3b9e4b164cb965ece92722ecd94e	0
20260501000001	fix live ranks month	2026-05-01 13:12:54.171121+00	t	\\x5d2780bee3b4068cf40ae88c74903a34c2f2a9b77bae37d2d082dd617139ad21895485c96cea29d7e5f2f6def24498cc	0
20260501000003	ignore zero live ranks	2026-05-01 14:18:14.770121+00	t	\\xb45f1ae6e90d6d35d4a39723230b70ca2434f6266af25260e63865439d865be0b400fc15c45bc746e07591d5998081ae	0
20260502000003	fix circle rollover display rules	2026-05-31 16:53:51.802906+00	t	\\x6b30cfeace47c2eed7f0f6622ccf1391d7f6f3e7dd485b4cb68221fc9af088d372ee009f243d211da11ac64a6d8f9685	0
20260502000005	lock circle rollover payload	2026-06-01 11:44:12.240291+00	t	\\x0aa022c90efb6a181b6d1a72e6cae4714eb577e9d87abfd5c70b0b6866429675216b8f6e735b310ceff87ee154afa92b	0
20260520000000	cheat analysis	2026-06-02 00:38:17.682935+00	t	\\x2269221e07113da5f6b5825aceebe7b29a09c920510c709b626a4c0480e118ce2b059d4a90b6f9c822d111fb96fc88af	0
20260520000002	add career length metrics	2026-06-02 00:38:17.969256+00	t	\\x92c6f896902c3320afb733e04bb46ed32700587942dbd6245b304c39afb04fe2a364a2fa1032d0a103c104bcb214f715	0
20260520000004	career length buckets	2026-06-02 00:38:17.991659+00	t	\\x3237cf8bc51e494aecc7c2976d29f01106e902ee681517eca1cba6d934bab742932471d62ddf3ae4364cf62b26f37f49	0
20260520000006	short high fan careers	2026-06-02 00:38:18.025723+00	t	\\x189e7b28fc1100747c68694610d034a7fcc0fd7b6ef9430c52166cf93ab6ad6a739b8e9b6f27b3d0bd45bee5ef850a87	0
20260520000008	short career fan gain stats	2026-06-02 00:38:18.074028+00	t	\\x6799630ddd8de6086807472ce673ea9bd49170b28693256f783378af69dd5357d274e4b29424daa739eacb351304c6a0	0
20260522000000	sessions from logins	2026-06-02 00:38:18.121379+00	t	\\x2bfe3f588bd0e00173eda4095041c80d5df7d18edfc5b3c78976dce269bd4c30d25ef55080adf585ff613096ba930c0b	0
20260524000000	add snapshot probe metrics	2026-06-02 00:38:18.184916+00	t	\\x55c907b45a3edde5b0fb50ce9856dd62a48e033c3973d640dfa5f4650f77e37c64d5f9158a704c38fbee679c17fdab00	0
20260524000002	short career snapshot timeline	2026-06-02 00:38:18.242225+00	t	\\x167425de91421d5b9b9b013380e9e10425c11cc6329c05dcaa637807ae438bf907a36a10dfafb928c394c91739302dda	0
20260527000000	add shame rate diagnostics	2026-06-02 00:38:18.285726+00	t	\\xefb7f6ab0e5c279e783936ad2ca7d72ea1c2a64b3bbdc669e42f1fc3ffdfe4d9bff5d30f582663337e1bc6b73d68d41b	0
20260613000000	fix circle yesterday rank ties	2026-06-14 21:39:00.709956+00	t	\\x4bb1e8e28a3eb26ce81f118468c40a2a383ac0b68a17b5b7bdebd35875237fbf0db6d8b66c6529e7b4182a4626f3ae62	0
20260617000000	add borrow interaction tracking	2026-06-17 02:37:25.927054+00	t	\\xedf60d2e5ede612b13b78edc6fc38ca11c8190c054d0fddcbed8bf6270d31f264272ceafcc0a9fa39841b6fd1e36a077	0
20260617000001	normalize borrow recheck tasks	2026-06-17 05:51:59.89464+00	t	\\x7351de916d4a41fa7a4628d281d0c5d9f49950bf21f0ba5450a85dc67bd1b9859709b2c82798294c11248ef1d07723c5	0
20260617000002	add borrow bucket cleanup index	2026-06-17 05:52:00.183542+00	t	\\xe777ea6123d89e66dcd5b517753eef2960d29d40ee6eb072b60d69ae7911bfd535048f7738f82ae61ca3da8426d21e17	0
20260627000000	add borrow interaction keys	2026-06-27 21:51:18.507844+00	t	\\x478b9b61b753f0e20a583431b71c3d17a1fcd80d3d86f3636461198c2a4d275b78695e87df22f63e213cd177c75239e9	0
20260627000001	add borrow interaction trends	2026-06-27 21:51:20.830053+00	t	\\x217b18ad9d02acc659166f9280e5825d697c0b54fc510bb1ae40b499ea3411e28365ad84442b465628817437a935a76a	0
20260627000002	backfill borrow interaction trends	2026-06-27 21:51:20.895856+00	t	\\xd06ab840acabd37f304d3ed884e9c8e4d0e1f604a03c9f3056a49a222b68ab4cb86c6add143c2163429e8aaf6a4e97d9	0
20260627000003	fix bookmark content hash	2026-06-27 23:24:11.662013+00	t	\\xe48a6c6e99b7e0bfe1375504fd59781822b7a9336aabad9beb08a3f58378c7637bb10bfe20341c2fc11d667aad07264d	0
20260627000004	bookmark bk1 snapshot	2026-06-27 23:24:11.712703+00	t	\\x1b1f8db54f2df4fbc1773f129a21d900e531b2bfb0b7d32d700e82a48443c0a230e7ddc9b28b67edd0f1119b82e29acf	0
20260628000001	merge legacy borrow totals by trainer	2026-06-28 21:34:21.660087+00	t	\\xc89c5e4d776dc76f88272570bf55bb49ec55f672d87c2e26c016393d53f641cae498de52d336da61709e61b59d48c597	0
20260704000000	partner inheritance content hash	2026-07-11 00:47:44.863088+00	t	\\xfc1c1c0f6bd725ad9a3bc41708d7e01aabb4ff2f18d99b35375eea78651ae1b1ea2637dca23d6c712ed2533c630c0072	185450600
20260707000000	allow hourly circle live data	2026-07-11 00:47:45.054969+00	t	\\x47e72fc0487abee02a796cb9c76097a1b9ad4c4b489587c6546c7958016374ba2cde6a603a7005f468d7957e8cc46188	96496592
20260708000000	ignore zero circle ranks	2026-07-11 00:47:45.155069+00	t	\\x355d60d827ac4f19ea1c7fcd1265c96ff11a8b3ec4f120c8dc1a368ad0f8b0a4728633df6b7b62dcc565a18a56a4f784	1504061592
20260709000000	rank circle live points first	2026-07-11 00:47:46.690356+00	t	\\xdd454352aedbd71884999d999f72b6a0deddfd1e44af79a2cda15049d3e0cd2777a893955480c0613aa97e2bf48a67a9	1594071212
20260709000001	add trainer last updated index	2026-07-11 00:48:44.765834+00	t	\\x10001066da51f350072e07cc3dfaf0c3d994261fa7d71c2e0cb155b95ca7f6cb90be8eff8275243a579c8b3c129cbeb5	56743001951
20260711000000	optimize fan and circle rankings	2026-07-11 18:05:56.294733+00	t	\\x754508a8fc0924b9be080b16a1a91a3b465f61497d6564ea95ac9429c5a6a2250dc8c1c3e575627741ae3ae125962d04	103275491684
20260711000001	repair stats counts unique index	2026-07-11 23:24:27.2079+00	t	\\xb84a437b3786a13ad43466ea181f6f1534b80a488d89f94df4034f43c5b7859a6b0fdeefb6e275abbca9d9ac4d065dd4	34546208
20260719000000	cache total stats	2026-07-20 20:52:09.697707+00	t	\\xdd598195e5935384b7dded7d4e8c817da6046d779cea5aef9030ff73cd8b2bc8f45e9b05f1d060334eaabb64aad91dc8	6460035360
20260728000000	bound high churn storage	2026-11-01 19:48:47.148553+00	t	\\x505b4b65c676bc81d7ef48608e04011ba754c77cfbdb9f37f5e666e56fb087b721222d16d4d514367e530cbb3c2d9874	14350232
20260731000000	limit circle rollover to second jst	2026-11-01 19:48:47.163197+00	t	\\xa9b77e25ca012032192241798b5bfa348152ef352f7ce5bfe5824e183fcf78425950e0d20637be2308169399a15a7166	14975240
20260801000000	use api clock for circle rollover	2026-11-01 19:48:47.179085+00	t	\\x16a81a23fb53dea706dc6ce02e3ae1d10e6d6808e35a3fec611f5ac552034bcf3fd80ecdcc96252c6207baabc8406c11	4632556
20260803000000	optimize circle member search	2026-11-01 19:48:47.18468+00	t	\\x0892fb34626702cca9ef5fe1f1b6eb45de69265bb5a4363c56da9f2c4a991993a52f2527986f9f4cd1efe36fe9f698db	2742916
20260804000000	add circle search documents	2026-11-01 19:48:47.188342+00	t	\\x5c06755ac1809d4d2fd77f98ed655cb921da9168666b5ad7256986b22d3717f7324dd6fa68cff9eae90e41dee4747256	12521982
20260819000000	add carat planner storage	2026-11-01 19:48:47.201758+00	t	\\x7eb1cbb315b0bbfc2c14a8b31840e6c5d9d80d0d65fecc27665cd935f636be38f242682ba79895988de08337c5874e8c	6520476
20260819010000	expand carat plan share ids	2026-11-01 19:48:47.20911+00	t	\\x6fb27119d31b78bbac461a01c09f0abf7a644b3d05f669e24ab790bba6e36502a211370a69021fef8be9fb83a9ff3e88	2091362
20261001000000	widen circle member id	2026-11-01 19:48:47.212105+00	t	\\x8c6894105a89560a8f596dfdbdeba82532640799d8ed7216bf1e99d345665c10892c6268613765af9a4d031a487f6945	7231054
\.


--
-- PostgreSQL database dump complete
--

\unrestrict ZttwIuRO6QBeW8IIghgco6jMCfAkWQrlB8ZmhDwsMdiEolYcAc3XrwHfiMU1Gsw

-- Schema-only dumps create materialized views WITH NO DATA; populate them (empty) so queries work.
REFRESH MATERIALIZED VIEW public.circle_live_ranks;
REFRESH MATERIALIZED VIEW public.circle_search_documents;
REFRESH MATERIALIZED VIEW public.stats_counts;
REFRESH MATERIALIZED VIEW public.user_fan_rankings_gains;
REFRESH MATERIALIZED VIEW public.user_fan_rankings_monthly_current;
-- Reads user_fan_rankings_monthly_current, so it comes after it.
REFRESH MATERIALIZED VIEW public.user_fan_rankings_alltime;

--
-- Part 2: synthetic rows (no real player data)
--

SELECT pg_catalog.set_config('search_path', 'public', false);
BEGIN;
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
COMMIT;
