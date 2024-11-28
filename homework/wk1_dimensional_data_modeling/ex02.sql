--2 - cumulative table generation query
--min1970 max2021
SELECT DISTINCT (year)
FROM actor_films
ORDER BY year;

--query
INSERT INTO actors
WITH yesterday AS (SELECT * FROM actors WHERE current_year = 1969),
     today AS (SELECT actorid,
                      actor,
                      year,
                      AVG(rating)                                              AS rating,
                      ARRAY_AGG(ROW (film, votes, rating, filmid)::film_stats) AS films
               FROM actor_films
               WHERE year = 1970
               GROUP BY actorid, actor, year)
SELECT COALESCE(t.actorid, y.actorid)       AS actorid,
       COALESCE(t.actor, y.actor)           AS actor,
       COALESCE(t.year, y.current_year + 1) AS current_year,
       CASE
           WHEN t.year IS NOT NULL THEN TRUE
           ELSE FALSE
           END                              AS is_active,
       CASE
           WHEN t.year IS NOT NULL THEN
               CASE
                   WHEN t.rating > 8 THEN 'star'
                   WHEN t.rating > 7 THEN 'good'
                   WHEN t.rating > 6 THEN 'average'
                   ELSE 'bad'
                   END::quality_class
           ELSE y.quality_class
           END                              AS quality_class,
       CASE
           WHEN y.films IS NULL
               THEN t.films
           WHEN t.year IS NOT NULL
               THEN y.films || t.films
           ELSE y.films
           END                              AS films
FROM today t
         FULL OUTER JOIN yesterday y ON t.actorid = y.actorid;