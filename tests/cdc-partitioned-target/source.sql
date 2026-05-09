---
--- pgcopydb tests/cdc-partitioned-target/source.sql
---
--- Source: flat (relkind='r') table. Pairs with target.sql to build the
--- asymmetric source-flat / target-partitioned case that the apply-side
--- TRUNCATE rewrite has to handle.
---

create schema partitioned_target;

create table partitioned_target.events (
    id bigint primary key,
    bucket smallint not null,
    payload text
);

-- Seed rows that exist before the snapshot, copied to the target by
-- pgcopydb copy table-data and then wiped by the replicated TRUNCATE.
insert into partitioned_target.events (id, bucket, payload)
     values (1, 0, 'a'),
            (2, 1, 'b'),
            (3, 0, 'c');
