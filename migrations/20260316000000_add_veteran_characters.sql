-- Veteran characters ingested from player exports.
-- account_id references linked_accounts.account_id in the main DB (no FK — separate service).
-- JSONB is used for variable-length array fields to keep the schema flat.

CREATE TABLE IF NOT EXISTS veteran_characters (
    -- Identity
    account_id                      TEXT        NOT NULL,
    trained_chara_id                BIGINT      NOT NULL,

    -- Card / scenario
    card_id                         INT,
    scenario_id                     INT,
    route_id                        INT,
    rarity                          INT,
    succession_trained_chara_id_1   BIGINT,
    succession_trained_chara_id_2   BIGINT,
    succession_num                  INT,

    -- Stats
    speed                           INT,
    stamina                         INT,
    power                           INT,
    wiz                             INT,
    guts                            INT,
    fans                            INT,
    rank_score                      BIGINT,
    rank                            INT,

    -- Grade / style
    chara_grade                     INT,
    talent_level                    INT,
    running_style                   INT,
    race_cloth_id                   INT,
    nickname_id                     INT,
    wins                            INT,

    -- Aptitudes
    proper_ground_turf              INT,
    proper_ground_dirt              INT,
    proper_running_style_nige       INT,
    proper_running_style_senko      INT,
    proper_running_style_sashi      INT,
    proper_running_style_oikomi     INT,
    proper_distance_short           INT,
    proper_distance_mile            INT,
    proper_distance_middle          INT,
    proper_distance_long            INT,

    -- Variable-length arrays stored as JSONB
    skill_array                     JSONB       NOT NULL DEFAULT '[]',
    support_card_list               JSONB       NOT NULL DEFAULT '[]',
    factor_info_array               JSONB       NOT NULL DEFAULT '[]',
    win_saddle_id_array             JSONB       NOT NULL DEFAULT '[]',

    -- Timestamps from the game data (stored as UTC)
    register_time                   TIMESTAMPTZ,
    create_time                     TIMESTAMPTZ,

    -- Ingest bookkeeping
    ingested_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (account_id, trained_chara_id)
);

-- Speed up per-account fetches and deletes
CREATE INDEX IF NOT EXISTS idx_veteran_characters_account_id
    ON veteran_characters (account_id);
