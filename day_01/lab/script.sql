SELECT *
FROM player_seasons
LIMIT 10;

--create struct for the season attribute (change between seasons)
CREATE TYPE season_stats AS
(
    season INTEGER,
    gp     INTEGER,
    pts    REAL,
    reb    REAL,
    ast    REAL
);

--players table for good compression
CREATE TABLE players
(
    player_name    TEXT,
    height         TEXT,
    college        TEXT,
    country        TEXT,
    draft_year     TEXT,
    draft_round    TEXT,
    draft_number   TEXT,
    season_stats   season_stats[], -- season attributes
    current_season INTEGER,        -- for cumulative
    PRIMARY KEY (player_name, current_season)
);

--check - empty table
SELECT *
FROM players;

--first season year - 1996
SELECT MIN(season)
FROM player_seasons;

INSERT INTO players
WITH yesterday AS (SELECT *
                   FROM players
                   WHERE current_season = 2000),
     today AS (SELECT *
               FROM player_seasons
               WHERE season = 2001)

--first yesterday will be null because it has no data yet
-- SELECT *
-- FROM today t
--          FULL OUTER JOIN yesterday y
--                          ON t.player_name = y.player_name;

--coalesce to merge these two into 1 cumulative table
--seed query for cumulating, just take data from today (because yesterday is empty)
SELECT COALESCE(t.player_name, y.player_name)   AS player_name,
       COALESCE(t.height, y.height)             AS height,
       COALESCE(t.college, y.college)           AS college,
       COALESCE(t.country, y.country)           AS country,
       COALESCE(t.draft_year, y.draft_year)     AS draft_year,
       COALESCE(t.draft_round, y.draft_round)   AS draft_round,
       COALESCE(t.draft_number, y.draft_number) AS draft_number, --end of general
       CASE
           WHEN y.season_stats IS NULL -- first run will be null
               THEN ARRAY [ROW (
               t.season,
               t.gp,
               t.pts,
               t.reb,
               t.ast)::season_stats]
           WHEN t.season IS NOT NULL
               THEN --concatenates today to yesterday, create array of two seasons
               y.season_stats || ARRAY [ROW (
                   t.season,
                   t.gp,
                   t.pts,
                   t.reb,
                   t.ast)::season_stats]
           ELSE y.season_stats --player is retired, no data of today
           END
                                                AS season_stats,
       COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
         FULL OUTER JOIN yesterday y
                         ON t.player_name = y.player_name;

--after insert players will have data
SELECT *
FROM players
WHERE current_season = 2001
  AND player_name = 'Michael Jordan';

--easily convert back to flat table (unnest)
--season_stats from array to each element each row
SELECT player_name,
       unnest(season_stats)::season_stats AS season_stats
FROM players
WHERE current_season = 2001
  AND player_name = 'Michael Jordan';

--fully flat, back to old schema
WITH unnested AS (SELECT player_name,
                         unnest(season_stats)::season_stats AS season_stats
                  FROM players
                  WHERE current_season = 2001)
SELECT player_name, (season_stats::season_stats).*
FROM unnested;
--always sort within name
--join with cumulative, after that unnest and everything will be nice and compress

--analytical query on cumulative table design
DROP TABLE players;

CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');

--add 2 columns
CREATE TABLE players
(
    player_name            TEXT,
    height                 TEXT,
    college                TEXT,
    country                TEXT,
    draft_year             TEXT,
    draft_round            TEXT,
    draft_number           TEXT,
    season_stats           season_stats[], -- season attributes
    scoring_class          scoring_class,
    year_since_last_season INTEGER,
    current_season         INTEGER,        -- for cumulative
    PRIMARY KEY (player_name, current_season)
);

SELECT MIN(season)
FROM player_seasons;

INSERT INTO players
WITH yesterday AS (SELECT *
                   FROM players
                   WHERE current_season = 2000),
     today AS (SELECT *
               FROM player_seasons
               WHERE season = 2001)

--first yesterday will be null because it has no data yet
-- SELECT *
-- FROM today t
--          FULL OUTER JOIN yesterday y
--                          ON t.player_name = y.player_name;

--coalesce to merge these two into 1 cumulative table
--seed query for cumulating, just take data from today (because yesterday is empty)
SELECT COALESCE(t.player_name, y.player_name)   AS player_name,
       COALESCE(t.height, y.height)             AS height,
       COALESCE(t.college, y.college)           AS college,
       COALESCE(t.country, y.country)           AS country,
       COALESCE(t.draft_year, y.draft_year)     AS draft_year,
       COALESCE(t.draft_round, y.draft_round)   AS draft_round,
       COALESCE(t.draft_number, y.draft_number) AS draft_number, --end of general
       CASE
           WHEN y.season_stats IS NULL -- first run will be null
               THEN ARRAY [ROW (
               t.season,
               t.gp,
               t.pts,
               t.reb,
               t.ast)::season_stats]
           WHEN t.season IS NOT NULL
               THEN --concatenates today to yesterday, create array of two seasons
               y.season_stats || ARRAY [ROW (
                   t.season,
                   t.gp,
                   t.pts,
                   t.reb,
                   t.ast)::season_stats]
           ELSE y.season_stats --player is retired, no data of today
           END                                  AS season_stats,
       CASE
           WHEN t.season IS NOT NULL THEN
               CASE
                   WHEN t.pts > 20 THEN 'star'
                   WHEN t.pts > 15 THEN 'good'
                   WHEN t.pts > 10 THEN 'average'
                   ELSE 'bad'
                   END::scoring_class
           ELSE y.scoring_class
           END                                  AS scoring_class,
       CASE
           WHEN t.season IS NOT NULL
               THEN 0
           ELSE y.year_since_last_season + 1
           END                                  AS year_since_last_season,
       COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
         FULL OUTER JOIN yesterday y
                         ON t.player_name = y.player_name;

--not even need GROUP BY, no shuffle needed
SELECT player_name,
       (season_stats[cardinality(season_stats)]::season_stats).pts /
       CASE
           WHEN (season_stats[1]::season_stats).pts = 0
               THEN 1
           ELSE (season_stats[1]::season_stats).pts
           END AS improve
FROM players
WHERE current_season = 2001
ORDER BY 2 DESC;
