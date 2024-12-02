-- SELECT *
-- FROM user_devices_cumulated;
-- datelist_int generation query
WITH users_devices AS (SELECT *
                       FROM user_devices_cumulated
                       WHERE curr_date = DATE('2023-01-31')),
     series AS (SELECT *
                FROM generate_series('2023-01-02', '2023-01-31', INTERVAL '1 day') AS series_date),
     place_holder_ints AS (SELECT CASE
                                      WHEN ud.device_activity_datelist @> ARRAY [DATE(s.series_date)]
                                          THEN CAST(POW(2, 32 - (ud.curr_date - DATE(s.series_date))) AS BIGINT)
                                      ELSE 0 END AS place_int_value,
                                  *
                           FROM users_devices ud
                                    CROSS JOIN series s)
SELECT
    user_id,
    browser_type,
    CAST(CAST(SUM(place_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
FROM place_holder_ints
GROUP BY user_id, browser_type;