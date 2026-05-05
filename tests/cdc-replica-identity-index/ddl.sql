---
--- pgcopydb tests/cdc-replica-identity-index/ddl.sql
---
--- Exercises the case where a table uses REPLICA IDENTITY USING INDEX on a
--- non-primary-key unique index. This previously caused the test_decoding
--- parser to fail UPDATE messages with "WHERE clause columns not found",
--- because the parser only recognized primary-key columns as the identity.
---

begin;

create table event_matches
(
    id         bigserial      not null,
    created_at timestamp(6)   not null default now(),
    name       text           not null
);

-- Unique index (not a primary key) that is also the replica identity.
create unique index event_matches_ri on event_matches (id, created_at);
alter table event_matches replica identity using index event_matches_ri;

commit;

-- Seed rows that exist before the clone snapshot.
insert into event_matches (name)
select 'initial-' || i from generate_series(1, 5) as g(i);
