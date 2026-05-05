---
--- pgcopydb tests/cdc-replica-identity-index/dml.sql
---
--- DML exercised during CDC streaming. The UPDATEs are the interesting part:
--- they must replicate correctly even though the table has no PRIMARY KEY,
--- only a REPLICA IDENTITY USING INDEX (id, created_at).
---

begin;

-- New rows inserted during CDC.
insert into event_matches (name)
select 'cdc-' || i from generate_series(1, 5) as g(i);

-- UPDATEs where the replica identity columns are NOT changed. This is the
-- case the previously-broken parser mishandled: test_decoding emits the
-- new tuple without an explicit old-key section, and the parser has to
-- identify which columns make up the identity by reading the attribute
-- metadata.
update event_matches
   set name = 'initial-updated-' || id
 where name like 'initial-%';

update event_matches
   set name = 'cdc-updated-' || id
 where name like 'cdc-%';

-- DELETE also relies on the replica identity columns.
delete from event_matches where name = 'cdc-updated-5';

commit;
