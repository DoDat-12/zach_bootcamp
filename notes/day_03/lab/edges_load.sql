--edge plays_in
INSERT INTO edges
WITH deduped AS (SELECT *,
                        --row_number for indexing
                        row_number() over (partition by player_id, game_id) AS row_nums
                 FROM game_details)
SELECT player_id             AS subject_identifier,
       'player'::vertex_type AS subject_type,
       game_id               AS object_identifier,
       'game'::vertex_type   AS object_type,
       'plays_in'::edge_type AS edge_type,
       json_build_object(
               'start_position', start_position,
               'pts', pts,
               'team_id', team_id,
               'team_abbreviation', team_abbreviation
       )                     AS properties
FROM deduped
WHERE row_nums = 1;

-- SELECT * FROM game_details;

SELECT v.properties ->> 'player_name' AS player_name,
       MAX(CAST(e.properties ->> 'pts' AS INTEGER))
FROM vertices v
         JOIN edges e
              ON e.subject_identifier = v.identifier
                  ANd e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC;

--edge shares_team and plays_against
--subject and object don't matter
INSERT INTO edges
WITH deduped AS (SELECT *,
                        --row_number for indexing
                        row_number() over (partition by player_id, game_id) AS row_nums
                 FROM game_details),
     filtered AS (SELECT *
                  FROM deduped
                  WHERE row_nums = 1),
     aggregated AS (SELECT f1.player_id        AS subject_player_id,
                           MAX(f1.player_name) AS subject_player_name, --fix same id but diff name
                           f2.player_id        AS object_player_id,
                           MAX(f2.player_name) AS object_player_name,
                           CASE
                               WHEN f1.team_abbreviation = f2.team_abbreviation
                                   THEN 'shares_team'::edge_type
                               ELSE 'plays_against'::edge_type
                               END             AS edge_type,
                           COUNT(1)            AS num_games,
                           SUM(f1.pts)         AS subject_points,
                           SUM(f2.pts)         AS object_points
                    FROM filtered f1
                             JOIN filtered f2
                                  ON f1.game_id = f2.game_id
                                      AND f1.player_name <> f2.player_name
                    WHERE f1.player_name > f2.player_name --remove double edge
                    GROUP BY f1.player_id,
                             f2.player_id,
                             CASE
                                 WHEN f1.team_abbreviation = f2.team_abbreviation
                                     THEN 'shares_team'::edge_type
                                 ELSE 'plays_against'::edge_type
                                 END)
SELECT subject_player_id     AS subject_identifier,
       'player'::vertex_type AS subject_type,
       object_player_id      AS object_identidier,
       'player'::vertex_type AS object_type,
       edge_type             AS edge_type,
       json_build_object(
               'num_games', num_games,
               'subject_points', subject_points,
               'object_points', object_points
       )
FROM aggregated;

--performance of subject against each object
SELECT v.properties ->> 'player_name',
       e.object_identifier,
       CAST(v.properties ->> 'number_of_games' AS REAL) /
       CASE
           WHEN CAST(v.properties ->> 'total_points' AS REAL) = 0 THEN 1
           ELSE CAST(v.properties ->> 'total_points' AS REAL)
           END AS points_per_game,
       e.properties ->> 'subject_points',
       e.properties ->> 'num_games'
FROM vertices v
         JOIN edges e
              ON v.identifier = e.subject_identifier
                  AND v.type = e.subject_type
WHERE e.object_type = 'player';