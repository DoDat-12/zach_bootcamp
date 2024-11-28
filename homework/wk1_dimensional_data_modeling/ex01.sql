-- SELECT *
-- FROM actor_films;

--1 - DDL for actors table
--struct for film's information
CREATE TYPE film_stats AS
(
    film   TEXT,
    votes  INTEGER,
    rating REAL,
    filmid TEXT
);

--enum for quality_class
CREATE TYPE quality_class AS ENUM (
    'star',
    'good',
    'average',
    'bad'
    );

--table
CREATE TABLE actors
(
    actorid       TEXT,
    actor         TEXT,
    current_year  INTEGER,
    is_active     BOOLEAN,
    quality_class quality_class,
    films         film_stats[],
    PRIMARY KEY (actorid, current_year)
);
-- DROP TABLE actors;
