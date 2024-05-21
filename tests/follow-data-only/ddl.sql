---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;

CREATE TABLE table_a (id serial PRIMARY KEY, f1 int4, f2 text);
CREATE TABLE table_b (id serial PRIMARY KEY, f1 int4, f2 text[]);

commit;

begin;

CREATE TABLE IF NOT EXISTS update_test
(
    id bigint primary key,
    name text
);

CREATE TABLE IF NOT EXISTS generated_column_test
(
    id bigint primary key,
    name text,
    greet_hello text generated always as ('Hello ' || name) stored,
    greet_hi  text generated always as ('Hi ' || name) stored,
    -- tests identifier escaping
    time text generated always as ('identifier 1' || 'now') stored,
    email text not null,
    -- tests identifier escaping
    "table" text generated always as ('identifier 2' || name) stored,
    """table""" text generated always as ('identifier 3' || name) stored,
    """hel""lo""" text generated always as ('identifier 4' || name) stored
);

commit;
