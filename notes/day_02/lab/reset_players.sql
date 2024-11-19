DROP TABLE players;
DROP TYPE scoring_class;
DROP TYPE season_stats;

CREATE TYPE scoring_class AS ENUM (
    'star',
    'good',
    'average',
    'bad'
    );

CREATE TYPE season_stats AS
(
    season INTEGER,
    gp     INTEGER,
    pts    REAL,
    reb    REAL,
    ast    REAL
);

CREATE TABLE players
(
    player_name             TEXT,
    height                  TEXT,
    college                 TEXT,
    country                 TEXT,
    draft_year              TEXT,
    draft_round             TEXT,
    draft_number            TEXT,
    season_stats            season_stats[],
    scoring_class           scoring_class,
    years_since_last_season INTEGER,
    current_season          INTEGER,
    is_active               BOOLEAN,
    PRIMARY KEY (player_name, current_season)
);

INSERT INTO players
WITH years AS (SELECT *
               FROM GENERATE_SERIES(1996, 2022) AS season),
     p AS (SELECT player_name,
                  MIN(season) AS first_season
           FROM player_seasons
           GROUP BY player_name),
     players_and_seasons AS (SELECT *
                             FROM p
                                      JOIN years y
                                           ON p.first_season <= y.season),
     windowed AS (SELECT pas.player_name,
                         pas.season,
                         ARRAY_REMOVE(
                                         ARRAY_AGG(
                                         CASE
                                             WHEN ps.season IS NOT NULL
                                                 THEN ROW (
                                                 ps.season,
                                                 ps.gp,
                                                 ps.pts,
                                                 ps.reb,
                                                 ps.ast
                                                 )::season_stats
                                             END)
                                         OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
                                         NULL
                         ) AS seasons
                  FROM players_and_seasons pas
                           LEFT JOIN player_seasons ps
                                     ON pas.player_name = ps.player_name
                                         AND pas.season = ps.season
                  ORDER BY pas.player_name, pas.season),
     static AS (SELECT player_name,
                       MAX(height)       AS height,
                       MAX(college)      AS college,
                       MAX(country)      AS country,
                       MAX(draft_year)   AS draft_year,
                       MAX(draft_round)  AS draft_round,
                       MAX(draft_number) AS draft_number
                FROM player_seasons
                GROUP BY player_name)
SELECT w.player_name,
       s.height,
       s.college,
       s.country,
       s.draft_year,
       s.draft_round,
       s.draft_number,
       seasons                                                         AS season_stats,
       CASE
           WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
           WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
           WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
           ELSE 'bad'
           END::scoring_class                                          AS scoring_class,
       w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
       w.season,
       (seasons[CARDINALITY(seasons)]::season_stats).season = season   AS is_active
FROM windowed w
         JOIN static s
              ON w.player_name = s.player_name;

SELECT player_name, scoring_class, current_season
FROM players
WHERE player_name = 'Aaron Brooks';