--
-- tests/cdc-matview/target.sql
--
-- Creates the target schema: the same table and materialized view as the
-- source.  The matview is created over an initially empty src table and
-- is populated with REFRESH after pgcopydb copy table-data fills src.
--

begin;

create table src(id int primary key, val text);
create materialized view mv1 as select id, val from src;
create unique index mv1_id_idx on mv1(id);

commit;
