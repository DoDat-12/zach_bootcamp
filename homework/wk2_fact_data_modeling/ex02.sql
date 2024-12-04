-- A DDL for an user_devices_cumulated table that has

CREATE TABLE user_devices_cumulated
(
    user_id                  NUMERIC,
    browser_type             TEXT,
    device_activity_datelist DATE[],
    curr_date                DATE,
    PRIMARY KEY (user_id, browser_type, curr_date)
);

-- DROP TABLE user_devices_cumulated;
-- device_activity_datelist MAP<STRING, ARRAY[DATE]>
-- Example: 'Chrome': DATE[]