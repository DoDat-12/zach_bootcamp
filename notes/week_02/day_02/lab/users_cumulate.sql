SELECT *
FROM events;
--TODO: Cumulate and find all the days that the user's active

SELECT MIN(event_time),
       MAX(event_time)
FROM events;
--2023-01-01 00:06:50.079000
--2023-01-31 23:51:51.685000

CREATE TABLE users_cumulated
(
    user_id      TEXT,
    dates_active DATE[], --the list of dates in the past when the user was active
    date         DATE,   --current date
    PRIMARY KEY (user_id, date)
);
-- DROP TABLE users_cumulated;

--cumulating, need to get the DATE LIST bit in the lecture
INSERT INTO users_cumulated
WITH yesterday AS (SELECT *
                   FROM users_cumulated
                   WHERE date = DATE('2023-01-30')),
     today AS (SELECT CAST(user_id AS TEXT),
                      DATE(CAST(event_time AS TIMESTAMP)) as date_active
               FROM events
               WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
                 AND user_id IS NOT NULL
               GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP)))
SELECT COALESCE(t.user_id, y.user_id)                     AS user_id,
       CASE
           WHEN y.dates_active IS NULL
               THEN ARRAY [t.date_active]
           WHEN t.date_active IS NULL
               THEN y.dates_active
           WHEN y.dates_active IS NOT NULL
               THEN ARRAY [t.date_active] || y.dates_active
           END                                            AS dates_active,
       COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date --latest date
FROM today t
         FULL OUTER JOIN yesterday y
                         ON t.user_id = y.user_id;