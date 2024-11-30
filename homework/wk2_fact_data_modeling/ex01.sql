--A query to deduplicate `game_details` from Day 1 so there's no duplicates
WITH dedupped AS (
    SELECT
        *,
        row_number() over (partition by game_id, team_id, player_id) AS row_nums
    FROM game_details
)
SELECT *
FROM dedupped
WHERE row_nums = 1;
