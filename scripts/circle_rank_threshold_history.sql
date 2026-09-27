-- Reconstruct circle-rank thresholds from cumulative member fan arrays.
-- One row per recorded day and tier; forecast rows extend the current game month.
WITH
params AS (
    SELECT
        14::int AS forecast_lookback_days,
        date_trunc(
            'month',
            (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo')::date - 1
        )::date AS current_game_month
),
tiers (tier, rank_index, boundary_rank) AS (
    VALUES
        ('SS', 11, 10),
        ('S+', 10, 30),
        ('S',   9, 100),
        ('A+',  8, 500),
        ('A',   7, 1000),
        ('B+',  6, 3000),
        ('B',   5, 5000),
        ('C+',  4, 7000),
        ('C',   3, 10000)
),
source_rows AS MATERIALIZED (
    SELECT
        cm.id AS member_month_id,
        cm.circle_id,
        cm.viewer_id,
        make_date(cm.year, cm.month, 1) AS month_start,
        cm.daily_fans,
        array_remove(
            cm.daily_fans[1:extract(day FROM (
                make_date(cm.year, cm.month, 1) + interval '1 month - 1 day'
            ))::int],
            0::bigint
        ) AS nonzero_fans,
        cm.last_updated
    FROM circle_member_fans_monthly cm
),
end_of_month_memberships AS (
    SELECT DISTINCT ON (viewer_id, month_start)
        member_month_id,
        viewer_id,
        month_start
    FROM source_rows
    WHERE cardinality(nonzero_fans) > 0
    ORDER BY
        viewer_id,
        month_start,
        last_updated DESC NULLS LAST,
        cardinality(nonzero_fans) DESC,
        member_month_id DESC
),
month_closers AS (
    -- The next month's first value closes the previous month's last day.
    SELECT
        previous.member_month_id,
        max(next_month.daily_fans[1])::bigint AS closing_fans
    FROM end_of_month_memberships previous
    JOIN source_rows next_month
      ON next_month.viewer_id = previous.viewer_id
     AND next_month.month_start = (previous.month_start + interval '1 month')::date
     AND next_month.daily_fans[1] > 0
    GROUP BY previous.member_month_id
),
latest_source_month AS (
    SELECT max(month_start) AS month_start
    FROM source_rows
),
month_horizons AS (
    SELECT
        source.month_start,
        CASE
            WHEN source.month_start < latest.month_start THEN extract(day FROM (
                source.month_start + interval '1 month - 1 day'
            ))::int
            -- The latest array entry is the still-running day.
            ELSE greatest(max(cardinality(source.nonzero_fans)) - 1, 0)
        END AS observed_through_day
    FROM source_rows source
    CROSS JOIN latest_source_month latest
    GROUP BY source.month_start, latest.month_start
),
circle_days AS (
    SELECT
        source.month_start,
        source.month_start + day.day_of_month - 1 AS recorded_on,
        day.day_of_month,
        source.circle_id,
        sum(greatest(
            CASE
                WHEN closer.closing_fans IS NOT NULL
                 AND day.day_of_month = horizon.observed_through_day
                 AND source.month_start < latest.month_start
                THEN greatest(
                    history.fans[cardinality(history.fans)],
                    closer.closing_fans
                )
                ELSE history.fans[cardinality(history.fans)]
            END - source.nonzero_fans[1],
            0
        ))::bigint
            AS reconstructed_points
    FROM source_rows source
    JOIN month_horizons horizon USING (month_start)
    CROSS JOIN latest_source_month latest
    CROSS JOIN LATERAL generate_series(1, horizon.observed_through_day)
        AS day(day_of_month)
    CROSS JOIN LATERAL (
        SELECT array_remove(source.daily_fans[1:day.day_of_month], 0::bigint) AS fans
    ) history
    LEFT JOIN month_closers closer USING (member_month_id)
    WHERE cardinality(history.fans) > 0
    GROUP BY
        source.month_start,
        day.day_of_month,
        source.circle_id
),
ranked_circle_days AS (
    SELECT
        circle_days.*,
        row_number() OVER (
            PARTITION BY month_start, recorded_on
            ORDER BY reconstructed_points DESC, circle_id
        )::int AS ranking,
        count(*) OVER (PARTITION BY month_start, recorded_on)::int
            AS circles_observed
    FROM circle_days
),
actual AS (
    SELECT
        ranked.month_start,
        ranked.recorded_on,
        ranked.day_of_month,
        tier.tier,
        tier.rank_index,
        tier.boundary_rank,
        ranked.reconstructed_points AS required_fans,
        ranked.circles_observed
    FROM tiers tier
    JOIN ranked_circle_days ranked
      ON ranked.ranking = tier.boundary_rank
),
actual_metrics AS (
    SELECT
        actual.*,
        actual.required_fans - previous_day.required_fans AS required_fans_delta_day,
        actual.required_fans - previous_week.required_fans AS required_fans_delta_7d
    FROM actual
    LEFT JOIN actual previous_day
      ON previous_day.boundary_rank = actual.boundary_rank
     AND previous_day.recorded_on = actual.recorded_on - 1
    LEFT JOIN actual previous_week
      ON previous_week.boundary_rank = actual.boundary_rank
     AND previous_week.recorded_on = actual.recorded_on - 7
),
current_model_rows AS (
    SELECT
        actual.*,
        params.forecast_lookback_days,
        max(day_of_month) OVER (PARTITION BY boundary_rank) AS latest_day
    FROM actual
    CROSS JOIN params
    WHERE actual.month_start = params.current_game_month
),
forecast_models AS (
    SELECT
        month_start,
        tier,
        rank_index,
        boundary_rank,
        max(latest_day)::int AS latest_day,
        max(required_fans) FILTER (WHERE day_of_month = latest_day)::bigint
            AS latest_required_fans,
        max(circles_observed) FILTER (WHERE day_of_month = latest_day)::int
            AS circles_observed,
        regr_slope(required_fans::double precision, day_of_month::double precision)
            FILTER (WHERE day_of_month > latest_day - forecast_lookback_days)
            AS daily_slope
    FROM current_model_rows
    GROUP BY month_start, tier, rank_index, boundary_rank
),
forecast AS (
    -- ponytail: anchored 14-day linear trend; replace only after a seasonal model
    -- proves lower backtest error.
    SELECT
        model.month_start,
        model.month_start + future.day_of_month - 1 AS recorded_on,
        future.day_of_month,
        model.tier,
        model.rank_index,
        model.boundary_rank,
        round(
            model.latest_required_fans
            + greatest(
                coalesce(
                    model.daily_slope,
                    model.latest_required_fans::double precision / model.latest_day
                ),
                0
            ) * (future.day_of_month - model.latest_day)
        )::bigint AS required_fans,
        model.circles_observed
    FROM forecast_models model
    CROSS JOIN LATERAL generate_series(
        model.latest_day + 1,
        extract(day FROM (model.month_start + interval '1 month - 1 day'))::int
    ) AS future(day_of_month)
)
SELECT
    actual.recorded_on,
    date_trunc('week', actual.recorded_on)::date AS week_start,
    actual.month_start,
    actual.day_of_month,
    actual.tier,
    actual.rank_index,
    actual.boundary_rank,
    actual.required_fans,
    ceil(actual.required_fans::numeric / actual.day_of_month)::bigint
        AS required_fans_per_day,
    ceil(actual.required_fans::numeric * 7 / actual.day_of_month)::bigint
        AS required_fans_per_week,
    actual.required_fans_delta_day,
    actual.required_fans_delta_7d,
    actual.circles_observed,
    false AS is_forecast
FROM actual_metrics actual

UNION ALL

SELECT
    forecast.recorded_on,
    date_trunc('week', forecast.recorded_on)::date AS week_start,
    forecast.month_start,
    forecast.day_of_month,
    forecast.tier,
    forecast.rank_index,
    forecast.boundary_rank,
    forecast.required_fans,
    ceil(forecast.required_fans::numeric / forecast.day_of_month)::bigint
        AS required_fans_per_day,
    ceil(forecast.required_fans::numeric * 7 / forecast.day_of_month)::bigint
        AS required_fans_per_week,
    NULL::bigint AS required_fans_delta_day,
    NULL::bigint AS required_fans_delta_7d,
    forecast.circles_observed,
    true AS is_forecast
FROM forecast
ORDER BY recorded_on, boundary_rank;
