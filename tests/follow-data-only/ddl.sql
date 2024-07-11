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

commit;
