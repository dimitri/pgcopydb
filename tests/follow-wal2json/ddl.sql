---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;
alter table payment_p2022_01 replica identity full;
alter table payment_p2022_02 replica identity full;
alter table payment_p2022_03 replica identity full;
alter table payment_p2022_04 replica identity full;
alter table payment_p2022_05 replica identity full;
alter table payment_p2022_06 replica identity full;
alter table payment_p2022_07 replica identity full;

--- test_table_with_composite_pk is used to create the test case for the a bug in wall2json problem.
--- For more information, see: https://github.com/dimitri/pgcopydb/issues/750
CREATE TABLE test_table_with_composite_pk (
    id INTEGER,
    name TEXT,
    CONSTRAINT test_pk PRIMARY KEY (id, name)
);
commit;
