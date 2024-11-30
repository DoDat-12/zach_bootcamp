-- A cumulative query to generate device_activity_datelist from events

-- SELECT MIN(event_time), MAX(event_time)
-- FROM events;
-- 0101 to 3101

INSERT INTO user_devices_cumulated
WITH yesterday AS (SELECT *
                   FROM user_devices_cumulated
                   WHERE curr_date = DATE('2023-01-02')),
     today AS (SELECT user_id,
                      d.browser_type,
                      DATE(CAST(e.event_time AS TIMESTAMP)) AS date
               FROM events e
                        INNER JOIN devices d
                                   ON e.device_id = d.device_id
               WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-03')
                 AND user_id IS NOT NULL
               GROUP BY user_id, d.browser_type, DATE(CAST(e.event_time AS TIMESTAMP)))
SELECT COALESCE(y.user_id, t.user_id)                         AS user_id,
       COALESCE(y.browser_type, t.browser_type)               AS browser_type,
       CASE
           WHEN y.device_activity_datelist IS NULL
               THEN ARRAY [t.date]
           WHEN t.date IS NULL
               THEN y.device_activity_datelist
           ELSE ARRAY [t.date] || y.device_activity_datelist
           END                                                AS device_activity_datelist,
       DATE(COALESCE(t.date, y.curr_date + INTERVAL '1 day')) AS curr_date
FROM yesterday y
         FULL OUTER JOIN today t
                         ON y.user_id = t.user_id
                             AND y.browser_type = t.browser_type;

-- SELECT *
-- FROM user_devices_cumulated
-- WHERE curr_date = DATE('2023-01-03');
