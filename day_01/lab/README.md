# Data Modeling - Cumulative Dimensions, Struct and Array

## `player_seasons` table

- Problem: When joining with another talbe, it will cause the shuffle and ruin compression
- Solution: Create a table that is one row per player and it has an array of all the seasons

- Not changing columns through the seasons
    - player_name
    - height
    - weight
    - college
    - draft_year
    - ...

- Actual attribute of the season
    - season (year)
    - gp (game_play)
    - pts (points)
    - reb
    - ast (assists)
    