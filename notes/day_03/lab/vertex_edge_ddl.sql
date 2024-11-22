-- GRAPH SCHEMA

-- DROP TYPE vertex_type CASCADE;
CREATE TYPE vertex_type
AS ENUM ('player', 'team', 'game');

CREATE TABLE vertices
(
    identifier TEXT,
    type       vertex_type, --enumeration
    properties JSON,
    PRIMARY KEY (identifier, type)
);

CREATE TYPE edge_type
AS ENUM (
    'plays_against', --player against player
    'shares_team', --player with player
    'plays_in', -- player in game
    'plays_on' -- player on team
    );

CREATE TABLE edges
(
    subject_identifier TEXT,
    subject_type       vertex_type,
    object_identifier  TEXT,
    object_type        vertex_type,
    edge_type          edge_type,
    properties         JSON,
    PRIMARY KEY (
                 subject_identifier,
                 subject_type,
                 object_identifier,
                 object_type,
                 edge_type
        )
);


