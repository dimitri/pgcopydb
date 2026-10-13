---
--- pgcopydb test/cdc/messages.sql
---
--- Exercises the safety net around DDL-as-message decoding: decoding and
--- applying a pg_logical_emit_message() "ddl" message is always on (no CLI
--- flag), so this proves that traffic NOT meant for pgcopydb is safely
--- ignored rather than breaking the stream or getting misapplied.
---
---   1. A message of an unrecognized type, with our own "pgcopydb" prefix:
---      must be logged and skipped, not break the stream.
---   2. A "ddl" message with a different prefix (as if another tool were
---      sharing the same publication): must be filtered out at decode time,
---      never applied.
---
--- The actual "ddl" message that gets applied (see ddl-trigger.sql) is
--- produced by the real event trigger installed via
--- `pgcopydb stream event-trigger`, not by a hand-crafted call here.

begin;
select pg_logical_emit_message(
    true,
    'pgcopydb',
    '{"type":"frobnicate","body":"should be ignored, not an error"}'
);
commit;

begin;
select pg_logical_emit_message(
    true,
    'othertool',
    '{"type":"ddl","body":"alter table rental add column should_not_apply text"}'
);
commit;
