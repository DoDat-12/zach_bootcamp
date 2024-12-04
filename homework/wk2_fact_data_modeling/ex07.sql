-- A monthly, reduced fact table DDL host_activity_reduced
CREATE TABLE host_activity_reduced
(
    month           DATE,
    host            TEXT,
    hit_array       INTEGER[],
    unique_visitors INTEGER[],
    PRIMARY KEY (host, month)
);

DROP TABLE host_activity_reduced;