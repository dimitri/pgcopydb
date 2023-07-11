---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;

CREATE TABLE table_a (id serial PRIMARY KEY, f1 int4, f2 text);
CREATE TABLE table_b (id serial PRIMARY KEY, f1 int4, f2 text[]);

commit;
