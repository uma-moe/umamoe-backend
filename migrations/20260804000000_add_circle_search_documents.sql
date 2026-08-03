-- Resolve general circle text searches against one compact row per circle.
-- The previous request-time query expanded every matching trainer name into
-- the large monthly history table, which made common terms take tens of
-- seconds and held scarce application connections for the duration.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE MATERIALIZED VIEW circle_search_documents AS
WITH target_month AS (
    SELECT
        extract(year from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int AS year,
        extract(month from (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo') - interval '2 days')::int AS month
),
current_member_names AS (
    SELECT
        cm.circle_id,
        string_agg(DISTINCT member.name, ' ' ORDER BY member.name) AS member_names
    FROM circle_member_fans_monthly cm
    CROSS JOIN target_month target
    JOIN trainer member ON member.account_id = cm.viewer_id::text
    WHERE cm.year = target.year
      AND cm.month = target.month
      AND member.name IS NOT NULL
    GROUP BY cm.circle_id
)
SELECT
    circle.circle_id,
    concat_ws(' ', circle.name, leader.name, members.member_names) AS search_text
FROM circles circle
LEFT JOIN trainer leader ON leader.account_id = circle.leader_viewer_id::text
LEFT JOIN current_member_names members ON members.circle_id = circle.circle_id;

-- Required for non-blocking REFRESH MATERIALIZED VIEW CONCURRENTLY.
CREATE UNIQUE INDEX circle_search_documents_circle_id_idx
    ON circle_search_documents (circle_id);

CREATE INDEX circle_search_documents_text_trgm_idx
    ON circle_search_documents USING gin (search_text gin_trgm_ops);

ANALYZE circle_search_documents;
