---
--- Index creation fails when referencing an object due to search_path issues
---
--- See https://github.com/dimitri/pgcopydb/issues/489

BEGIN;

CREATE SCHEMA """abc""";

CREATE TYPE """abc""".state AS ENUM (
    'PROPOSED',
    'SCHEDULED',
    'STARTED'
);

CREATE TABLE """abc""".job (
    id bigint NOT NULL,
    state """abc""".state DEFAULT 'SCHEDULED'::"""abc""".state NOT NULL,
    date date NOT NULL
);

CREATE INDEX indexname ON """abc""".job USING btree (state) WHERE (state = 'SCHEDULED'::state);

COMMIT;
