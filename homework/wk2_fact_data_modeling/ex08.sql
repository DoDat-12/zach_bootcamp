-- An incremental query that loads host_activity_reduced

INSERT INTO host_activity_reduced
WITH daily_aggregated AS (SELECT DATE(CAST(event_time AS TIMESTAMP)) AS date,
                                 host,
                                 COUNT(1)                            AS hit_array,
                                 COUNT(DISTINCT user_id)             AS unique_visitors,
                                 array_agg(DISTINCT user_id)         AS list_visitors
                          FROM events
                          WHERE DATE(CAST(event_time AS TIMESTAMP)) = '2023-01-01'
                            AND user_id IS NOT NULL
                          GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))),
     yesterday AS (SELECT *
                   FROM host_activity_reduced
                   WHERE MONTH = '2023-01-01')
SELECT COALESCE(y.month, DATE(date_trunc('month', da.date))) AS month,
       COALESCE(da.host, y.host)                             AS host,
       CASE
           WHEN y.hit_array IS NOT NULL
               THEN y.hit_array || ARRAY [COALESCE(da.hit_array, 0)]
           ELSE
               array_fill(0,
                          ARRAY [date - DATE(date_trunc('month', date))]) ||
               ARRAY [COALESCE(da.hit_array, 0)]
           END                                               AS hit_array,
       CASE
           WHEN y.unique_visitors IS NOT NULL
               THEN y.unique_visitors || ARRAY [COALESCE(da.unique_visitors, 0)]
           ELSE
               array_fill(0,
                          ARRAY [date - DATE(date_trunc('month', date))]) ||
               ARRAY [COALESCE(da.unique_visitors, 0)]
           END                                               AS unique_visitors
FROM daily_aggregated da
         FULL OUTER JOIN yesterday y
                         ON da.host = y.host
ON CONFLICT (month, host)
    DO UPDATE SET hit_array       = excluded.hit_array,
                  unique_visitors = excluded.unique_visitors;

-- SELECT * FROM host_activity_reduced;