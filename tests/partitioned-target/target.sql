--
-- Target schema: same name, but partitioned (relkind 'p').
--
-- pgcopydb must:
--   - emit TRUNCATE (not TRUNCATE ONLY), since Postgres rejects TRUNCATE
--     ONLY on partitioned tables.
--   - skip COPY FREEZE, since Postgres rejects FREEZE on partitioned tables.
--
create schema partitioned_target;

create table partitioned_target.events (
    id bigint not null,
    bucket smallint not null,
    payload text
) partition by list (bucket);

create table partitioned_target.events_a
    partition of partitioned_target.events for values in (0);

create table partitioned_target.events_b
    partition of partitioned_target.events for values in (1);
