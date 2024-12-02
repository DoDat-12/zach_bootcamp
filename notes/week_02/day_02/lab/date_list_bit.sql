WITH users AS (SELECT *
               FROM users_cumulated
               WHERE date = DATE('2023-01-31')),
     series AS (SELECT *
                FROM generate_series('2023-01-02', '2023-01-31', INTERVAL '1 day') --30days
                         as series_date),
     place_holder_ints AS (SELECT
                               --date - DATE(series_date), --to get the position
                               CASE
                                   WHEN dates_active @> ARRAY [DATE(series_date)] --[series_date] là tập con của dates_active hay không
                                       THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT) --2^n turn to bit will change to 1 at that n
                                   ELSE 0
                                   END AS placeholder_int_value,
                               *
                           FROM users
                                    CROSS JOIN series)
--CROSS JOIN: if the series_date is in the dates_active, we create bit 1 at that position
SELECT user_id,
       CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
       bit_count(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
       bit_count(CAST('11111110000000000000000000000000' AS BIT(32)) &
                 CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active,
       bit_count(CAST('10000000000000000000000000000000' AS BIT(32)) &
                 CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id;