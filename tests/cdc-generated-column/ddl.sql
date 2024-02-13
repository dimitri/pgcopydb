---
--- pgcopydb tests/cdc-generated-column/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;

CREATE TABLE IF NOT EXISTS generated_column_test
(
    id bigint primary key,
    name text,
    greet_hello text generated always as ('Hello ' || name) stored,
    greet_hi  text generated always as ('Hi ' || name) stored,
    email text not null,
    time text generated always as ('quoted identifier' || name) stored
);

commit;
