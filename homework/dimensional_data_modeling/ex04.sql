-- SELECT *
-- FROM actors_history_scd;

INSERT INTO actors_history_scd
WITH
    --track the changed and unchanged
    with_previous AS (SELECT actorid,
                             actor,
                             current_year,
                             is_active,
                             lag(is_active, 1) over (PARTITION BY actorid ORDER BY current_year)     AS previous_is_active,
                             quality_class,
                             lag(quality_class, 1) over (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class
                      FROM actors
                      WHERE current_year <= 2020),
    --indicate the change
    with_indicators AS (SELECT *,
                               CASE
                                   WHEN is_active <> previous_is_active THEN 1
                                   WHEN quality_class <> previous_quality_class THEN 1
                                   ELSE 0
                                   END AS change_indicator
                        FROM with_previous),
    --calculate the streak (the sum increase when some changes happen so rows have same streak value belongs to one streak)
    with_streaks AS (SELECT *,
                            SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
                     FROM with_indicators)
SELECT actorid,
       actor,
       quality_class,
       is_active,
       MIN(current_year) AS start_date,
       MAX(current_year) AS end_date,
       2020              AS current_year
FROM with_streaks
GROUP BY actorid, actor, quality_class, is_active, streak_identifier
ORDER BY actorid, streak_identifier;

-- SELECT * FROM actors_history_scd;