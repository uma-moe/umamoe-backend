pub(crate) fn monthly_club_rank_joins(ranking_alias: &str) -> String {
    format!(
        "LEFT JOIN circle_ranks_monthly_archive cra \
            ON cra.circle_id = {alias}.circle_id \
           AND cra.year = {alias}.year \
           AND cra.month = {alias}.month \
         LEFT JOIN circles c ON c.circle_id = {alias}.circle_id \
         LEFT JOIN circle_live_ranks lr ON lr.circle_id = {alias}.circle_id",
        alias = ranking_alias
    )
}

pub(crate) fn monthly_club_rank_selects(ranking_alias: &str) -> (String, String) {
    let rank_expr = "COALESCE(cra.rank, lr.live_rank::int, c.live_rank, c.monthly_rank)";
    let points_expr = "COALESCE(cra.total_points, c.monthly_point, c.live_points, 0)";
    let club_rank_expr = club_rank_index_sql(rank_expr, points_expr);
    let nullable_club_rank_expr = format!(
        "CASE WHEN {alias}.circle_id IS NULL THEN NULL ELSE {club_rank_expr} END",
        alias = ranking_alias
    );
    let club_rank_name_expr = club_rank_name_sql(&nullable_club_rank_expr);

    (nullable_club_rank_expr, club_rank_name_expr)
}

fn club_rank_index_sql(rank_expr: &str, points_expr: &str) -> String {
    format!(
        "CASE \
            WHEN {rank_expr} IS NULL THEN \
                CASE WHEN COALESCE({points_expr}, 0) = 0 THEN 1 ELSE 2 END \
            WHEN {rank_expr} BETWEEN 1 AND 10 THEN 11 \
            WHEN {rank_expr} BETWEEN 11 AND 30 THEN 10 \
            WHEN {rank_expr} BETWEEN 31 AND 100 THEN 9 \
            WHEN {rank_expr} BETWEEN 101 AND 500 THEN 8 \
            WHEN {rank_expr} BETWEEN 501 AND 1000 THEN 7 \
            WHEN {rank_expr} BETWEEN 1001 AND 3000 THEN 6 \
            WHEN {rank_expr} BETWEEN 3001 AND 5000 THEN 5 \
            WHEN {rank_expr} BETWEEN 5001 AND 7000 THEN 4 \
            WHEN {rank_expr} BETWEEN 7001 AND 10000 THEN 3 \
            ELSE 2 \
        END"
    )
}

fn club_rank_name_sql(club_rank_expr: &str) -> String {
    format!(
        "CASE {club_rank_expr} \
            WHEN 11 THEN 'SS' \
            WHEN 10 THEN 'S+' \
            WHEN 9 THEN 'S' \
            WHEN 8 THEN 'A+' \
            WHEN 7 THEN 'A' \
            WHEN 6 THEN 'B+' \
            WHEN 5 THEN 'B' \
            WHEN 4 THEN 'C+' \
            WHEN 3 THEN 'C' \
            WHEN 2 THEN 'D+' \
            WHEN 1 THEN 'D' \
            ELSE NULL \
        END"
    )
}
