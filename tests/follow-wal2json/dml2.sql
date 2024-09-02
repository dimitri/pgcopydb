---
--- pgcopydb test/cdc/dml2.sql
---
--- This file is used to create the test case for the a bug in wall2json problem.
--- For more information, see: https://github.com/dimitri/pgcopydb/issues/750

begin;
insert into test_table_with_composite_pk (id,name) values (1,'postgres');
insert into test_table_with_composite_pk (id,name) values (2,'oracle');
insert into test_table_with_composite_pk (id,name) values (3,'mysql');
insert into test_table_with_composite_pk (id,name) values (4,'couchbase');
insert into test_table_with_composite_pk (id,name) values (5,'redis');
commit;

begin;
update test_table_with_composite_pk set id = id where 1 = 1;
update test_table_with_composite_pk set name = 'REDIS' where id = 5;
commit;
