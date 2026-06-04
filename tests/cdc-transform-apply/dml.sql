--
-- cdc-transform-apply/dml.sql
--
-- DML applied after the snapshot is taken.  All three mutation types
-- (INSERT / UPDATE / DELETE) must appear in the CDC stream so that the
-- transform step exercises every code path.
--

begin;
insert into items (name) values ('cdc-new-1'), ('cdc-new-2');
update items set name = 'seed-updated-1' where name = 'seed-1';
delete from items where name = 'seed-2';
commit;
