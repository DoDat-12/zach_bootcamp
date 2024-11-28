--struct type for unnest update change
CREATE TYPE actor_scd_type AS
(
    quality_class quality_class,
    is_active     BOOLEAN,
    start_date    INTEGER,
    end_date      INTEGER
);

WITH
    --history has streak up to current_year (1 actor per row for sure)
    last_year_scd AS (SELECT * FROM actors_history_scd WHERE current_year = 2020 AND end_date = 2020),
    --history has streak end before current_year (just for adding when finishing)
    historical_scd AS (SELECT
                           actorid,
                           actor,
                           quality_class,
                           is_active,
                           start_date,
                           end_date,
                           2021 AS current_year
                       FROM actors_history_scd
                       WHERE current_year = 2020
                         AND end_date < 2020),
    --new data
    this_year_data AS (SELECT * FROM actors WHERE current_year = 2021),
    --unchanged records
    unchanged_records AS (SELECT t.actorid,
                                 t.actor,
                                 t.quality_class,
                                 t.is_active,
                                 l.start_date,
                                 t.current_year AS end_date,
                                 2021           AS current_year
                          FROM this_year_data t
                                   JOIN last_year_scd l
                                        ON t.actorid = l.actorid
                          WHERE t.is_active = l.is_active
                            AND t.quality_class = l.quality_class),
    --changed records (from 1 row to 2 rows - the 2020 and the new 2021 status)
    changed_records AS (SELECT t.actorid,
                               t.actor,
                               unnest(ARRAY [
                                   ROW (
                                       l.quality_class,
                                       l.is_active,
                                       l.start_date,
                                       l.end_date
                                       )::actor_scd_type,
                                   ROW (
                                       t.quality_class,
                                       t.is_active,
                                       t.current_year,
                                       t.current_year
                                       )::actor_scd_type
                                   ]) AS record,
                               2021   AS current_year
                        FROM this_year_data t
                                 JOIN last_year_scd l
                                      ON t.actorid = l.actorid
                        WHERE t.is_active <> l.is_active
                           OR t.quality_class <> l.quality_class),
    --unnest all attribute
    unnested_changed_records AS (SELECT actorid,
                                        actor,
                                        (record::actor_scd_type).quality_class,
                                        (record::actor_scd_type).is_active,
                                        (record::actor_scd_type).start_date,
                                        (record::actor_scd_type).end_date,
                                        current_year
                                 FROM changed_records),
    --new record (new actor that history not have)
    new_records AS (SELECT t.actorid,
                           t.actor,
                           t.quality_class,
                           t.is_active,
                           t.current_year AS start_date,
                           t.current_year AS end_date,
                           t.current_year AS current_year
                    FROM this_year_data t
                             LEFT JOIN last_year_scd l
                                       ON t.actorid = l.actorid
                    WHERE l.actorid IS NULL)
--union all data
SELECT * FROM historical_scd --past scd
UNION ALL
SELECT * FROM unchanged_records --unchanged last year
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;
