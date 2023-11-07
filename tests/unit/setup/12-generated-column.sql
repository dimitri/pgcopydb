--
-- See https://github.com/dimitri/pgcopydb/issues/496
--
-- Migration fails when table has generated columns
--

CREATE TABLE ref_admin_commune (
    id character varying NOT NULL,
    nom character varying(50),
    test text GENERATED ALWAYS AS (upper((nom)::text)) STORED
);