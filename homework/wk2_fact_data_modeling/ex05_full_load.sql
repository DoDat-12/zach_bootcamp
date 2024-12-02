--full load hosts_cumulated
DO
$$
    DECLARE
        day_start DATE := DATE('2022-12-31');
        day_end   DATE := DATE('2023-01-30');
        day_var   DATE;
    BEGIN
        day_var := day_start;
        WHILE day_var <= day_end
            LOOP
                INSERT INTO hosts_cumulated
                WITH yesterday AS (SELECT *
                                   FROM hosts_cumulated
                                   WHERE curr_date = day_var),
                     today AS (SELECT host,
                                      DATE(CAST(event_time AS TIMESTAMP)) AS date
                               FROM events
                               WHERE DATE(CAST(event_time AS TIMESTAMP)) = day_var + INTERVAL '1 day'
                               GROUP BY (host, DATE(CAST(event_time AS TIMESTAMP))))
                SELECT COALESCE(y.host, t.host)                               AS host,
                       CASE
                           WHEN y.host_activity_datelist IS NULL
                               THEN ARRAY [t.date]
                           WHEN t.date IS NULL
                               THEN y.host_activity_datelist
                           ELSE
                               y.host_activity_datelist || ARRAY [t.date]
                           END                                                AS host_activity_datelist,
                       DATE(COALESCE(t.date, y.curr_date + INTERVAL '1 day')) AS curr_date
                FROM yesterday y
                         FULL OUTER JOIN today t
                                         ON y.host = t.host;
                day_var := day_var + INTERVAL '1 day';
            END LOOP;
    END
$$;

SELECT *
FROM hosts_cumulated;