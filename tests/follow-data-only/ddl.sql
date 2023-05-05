---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;
CREATE TABLE table_a ( id serial PRIMARY KEY, some_field int4 );
commit;
