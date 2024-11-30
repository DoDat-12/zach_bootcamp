WITH yesterday AS (SELECT *
                   FROM user_devices_cumulated
                   WHERE curr_date = DATE('2023-01-01')),
     today AS (SELECT e.user_id,
                      d.browser_type,
                      DATE(CAST(e.event_time AS timestamp)) AS event_date
               FROM events e
                        INNER JOIN devices d
                                   ON e.device_id = d.device_id
               WHERE DATE(CAST(e.event_time AS timestamp)) = DATE('2023-01-01')
                 AND user_id IS NOT NULL
               GROUP BY e.user_id, d.browser_type, DATE(CAST(e.event_time AS timestamp))),
     merged AS (SELECT COALESCE(y.user_id, t.user_id)                               AS user_id,
                       CASE
                           WHEN y.device_activity_datelist IS NULL
                               THEN jsonb_build_object(t.browser_type, to_jsonb(ARRAY [t.event_date]::DATE[]))
                           WHEN t.browser_type IS NULL
                               THEN y.device_activity_datelist
                           ELSE jsonb_set(y.device_activity_datelist, ARRAY [t.browser_type],
                                          COALESCE(device_activity_datelist -> t.browser_type, '[]'::jsonb) ||
                                          to_jsonb(ARRAY [t.event_date]::DATE[]))
                           END                                                      AS device_activity_datelist,
                       DATE(COALESCE(t.event_date, y.curr_date + INTERVAL '1 day')) AS curr_date
                FROM yesterday y
                         FULL OUTER JOIN today t
                                         ON y.user_id = t.user_id),
    pairs AS (
        SELECT
    )
SELECT *
FROM merged;