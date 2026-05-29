---
--- pgcopydb tests/cdc-partitioned-target/target.sql
---
--- Target: partitioned (relkind='p') table with the same qualified name as
--- the source flat table. Postgres rejects "TRUNCATE ONLY" against a
--- partitioned table; the apply path must rewrite the replicated
--- statement to plain "TRUNCATE".
---

create schema partitioned_target;

create table partitioned_target.events (
    id bigint not null,
    bucket smallint not null,
    payload text,
    primary key (id, bucket)
) partition by list (bucket);

create table partitioned_target.events_a
    partition of partitioned_target.events for values in (0);

create table partitioned_target.events_b
    partition of partitioned_target.events for values in (1);
