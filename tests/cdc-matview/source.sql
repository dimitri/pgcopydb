--
-- tests/cdc-matview/source.sql
--
-- Creates the source schema: a plain table plus a materialized view with a
-- unique index (required for REFRESH MATERIALIZED VIEW CONCURRENTLY) and some
-- initial rows.
--

begin;

create table src(id int primary key, val text);
insert into src values (1, 'alpha'), (2, 'beta'), (3, 'gamma');

create materialized view mv1 as select id, val from src;
create unique index mv1_id_idx on mv1(id);

commit;
