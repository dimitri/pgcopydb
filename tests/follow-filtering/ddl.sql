---
--- follow-filtering/ddl.sql
---
--- Set REPLICA IDENTITY FULL on partitioned payment tables so that
--- UPDATE/DELETE changes can be captured without a primary key.

begin;
alter table payment_p2022_01 replica identity full;
alter table payment_p2022_02 replica identity full;
alter table payment_p2022_03 replica identity full;
alter table payment_p2022_04 replica identity full;
alter table payment_p2022_05 replica identity full;
alter table payment_p2022_06 replica identity full;
alter table payment_p2022_07 replica identity full;
commit;
