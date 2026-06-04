--
-- cdc-transform-apply/ddl.sql
--
-- Simple table with a plain primary key.  test_decoding emits UPDATE messages
-- without an old-key: section when only non-PK columns change, so the
-- transform step needs the source catalog to resolve attisprimary.
--

begin;
create table items (
    id   bigserial primary key,
    name text      not null
);
commit;

-- Seed rows present in the initial clone snapshot (ids 1-3).
insert into items (name)
select 'seed-' || i from generate_series(1, 3) as g(i);
