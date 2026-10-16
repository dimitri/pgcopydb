---
--- pgcopydb test/cdc/ddl-trigger.sql
---
--- A plain DDL statement. With the event trigger installed by
--- `pgcopydb stream event-trigger` (see inject.sh), this statement itself
--- fires the trigger, which emits a {"type":"ddl","body":"<this statement>"}
--- pg_logical_emit_message() envelope with the "pgcopydb" prefix. pgcopydb
--- follow decodes and applies it: the "note" column must show up on the
--- target's "rental" table with no manual DDL replay.

alter table rental add column note text;
