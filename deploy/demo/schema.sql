-- Local-only base tables predating this repository's first migration.
-- Later schema changes are applied by the real SQLx migrations.

CREATE TABLE trainer (
    account_id character varying(255) PRIMARY KEY,
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
    status text DEFAULT 'active',
    comment text,
    is_deleted_or_banned boolean NOT NULL DEFAULT false,
    deleted_or_banned_at timestamptz
);

CREATE TABLE circles (
    circle_id bigint PRIMARY KEY,
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
    yesterday_points bigint,
    yesterday_rank integer,
    yesterday_updated timestamp,
    live_points bigint,
    live_rank integer,
    last_live_update timestamp
);

CREATE TABLE circle_member_fans_monthly (
    id SERIAL PRIMARY KEY,
    circle_id bigint NOT NULL,
    viewer_id bigint NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    daily_fans bigint[] DEFAULT '{}'::bigint[] NOT NULL,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (circle_id, viewer_id, year, month)
);

CREATE TABLE inheritance (
    inheritance_id SERIAL PRIMARY KEY,
    account_id character varying(255) NOT NULL CONSTRAINT inheritance_account_id_unique UNIQUE,
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
    left_blue_factors integer NOT NULL DEFAULT 0,
    left_pink_factors integer NOT NULL DEFAULT 0,
    left_green_factors integer NOT NULL DEFAULT 0,
    left_white_factors integer[] NOT NULL DEFAULT '{}',
    left_white_count integer NOT NULL DEFAULT 0,
    right_blue_factors integer NOT NULL DEFAULT 0,
    right_pink_factors integer NOT NULL DEFAULT 0,
    right_green_factors integer NOT NULL DEFAULT 0,
    right_white_factors integer[] NOT NULL DEFAULT '{}',
    right_white_count integer NOT NULL DEFAULT 0,
    main_win_saddles integer[] NOT NULL DEFAULT '{}',
    left_win_saddles integer[] NOT NULL DEFAULT '{}',
    right_win_saddles integer[] NOT NULL DEFAULT '{}',
    race_results integer[] NOT NULL DEFAULT '{}'
);

CREATE TABLE support_card (
    account_id character varying(255) PRIMARY KEY,
    support_card_id integer NOT NULL,
    limit_break_count integer DEFAULT 0,
    experience integer DEFAULT 0 NOT NULL
);

CREATE TABLE team_stadium (
    id SERIAL PRIMARY KEY,
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
    team_rating integer DEFAULT 0
);

CREATE TABLE daily_stats (
    id SERIAL PRIMARY KEY,
    date date NOT NULL UNIQUE,
    total_visitors integer DEFAULT 0,
    unique_visitors integer DEFAULT 0,
    inheritance_uploads integer DEFAULT 0,
    support_card_uploads integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    visitor_count integer DEFAULT 0
);

CREATE TABLE daily_visitor_counters (
    date date PRIMARY KEY,
    visitor_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

CREATE TABLE friendlist_reports (
    trainer_id character varying(15) PRIMARY KEY,
    reported_at timestamp with time zone DEFAULT now(),
    report_count integer DEFAULT 1,
    CONSTRAINT trainer_id_format_fl CHECK (((trainer_id)::text ~ '^[0-9 ]+$'::text))
);

CREATE TABLE tasks (
    id SERIAL PRIMARY KEY,
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
);

CREATE UNIQUE INDEX tasks_active_unique ON tasks (task_type, task_data)
    WHERE status IN ('pending', 'processing');

CREATE TABLE circle_member_fan_snapshots (
    id BIGSERIAL PRIMARY KEY,
    circle_id bigint NOT NULL,
    snapshot_time timestamp NOT NULL,
    viewer_ids bigint[] NOT NULL,
    fans bigint[] NOT NULL,
    last_login_times text[] NOT NULL
);
