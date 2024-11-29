--source, min: 2023-01-01
SELECT *
FROM events;

--create reduced fact table
CREATE TABLE array_metrics
(
    user_id      NUMERIC,
    month_start  DATE,
    metric_name  TEXT, --action
    metric_array REAL[],
    PRIMARY KEY (user_id, month_start, metric_name)
);

INSERT INTO array_metrics
WITH daily_aggregate AS (SELECT user_id,
                                DATE(event_time) AS date,
                                COUNT(1)         AS num_site_hits
                         FROM events
                         WHERE DATE(event_time) = DATE('2023-01-03')
                           AND user_id IS NOT NULL
                         GROUP BY user_id, DATE(event_time)),
     yesterday_aggregate AS (SELECT *
                             FROM array_metrics
                             WHERE month_start = DATE('2023-01-01'))
SELECT COALESCE(da.user_id, ya.user_id)                       AS user_id,
       COALESCE(ya.month_start, date_trunc('month', da.date)) AS month_start,
       'site_hits'                                            AS metric_name,
       --array from first day of the month to the end
       CASE
           WHEN ya.metric_array IS NOT NULL
               THEN ya.metric_array || ARRAY [COALESCE(da.num_site_hits, 0)]
           ELSE
               array_fill(0,
                          ARRAY [COALESCE(date - DATE(date_trunc('month', date)), 0)]) || -- fill data for users appear in the middle of the month
               ARRAY [COALESCE(da.num_site_hits, 0)]
           END                                                AS metric_array
FROM daily_aggregate da
         FULL OUTER JOIN yesterday_aggregate ya ON
    da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
    DO UPDATE SET metric_array = excluded.metric_array;

SELECT *
FROM array_metrics;

-- DROP TABLE  array_metrics;

-- date_trunc('datepart', field)
-- SELECT date_trunc('month', DATE('2024-01-01')) AS month;