---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;
alter table payment_p2020_01 replica identity full;
alter table payment_p2020_02 replica identity full;
alter table payment_p2020_03 replica identity full;
alter table payment_p2020_04 replica identity full;
alter table payment_p2020_05 replica identity full;
alter table payment_p2020_06 replica identity full;
commit;
