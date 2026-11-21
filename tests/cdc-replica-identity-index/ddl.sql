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

-- Regression fixture for a second flavour of the same bug: REPLICA IDENTITY
-- USING INDEX pointing at the index backing a PRIMARY KEY constraint. pg_dump
-- embeds this ALTER TABLE inside the CONSTRAINT TOC entry rather than a
-- stand-alone INDEX entry, but pgcopydb builds the index and the identity via
-- the same CREATE INDEX worker code path either way.
create table event_matches_pk
(
    id   bigserial not null primary key,
    name text      not null
);

alter table event_matches_pk replica identity using index event_matches_pk_pkey;

commit;

-- Seed rows that exist before the clone snapshot.
insert into event_matches (name)
select 'initial-' || i from generate_series(1, 5) as g(i);

insert into event_matches_pk (name)
select 'initial-' || i from generate_series(1, 5) as g(i);
