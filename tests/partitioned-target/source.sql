--
-- Source schema: flat (non-partitioned) tables.
--
-- Pairs with target.sql to build the asymmetric-partitioning case:
-- source has relkind 'r', target has relkind 'p' for the same qualified
-- name. The runtime fallback in pgsql_truncate and pg_copy must detect
-- the target's relkind and adjust TRUNCATE / COPY FREEZE accordingly.
--
create schema partitioned_target;

create table partitioned_target.events (
    id bigint not null,
    bucket smallint not null,
    payload text
);

insert into partitioned_target.events (id, bucket, payload)
     values (1, 0, 'a'),
            (2, 1, 'b'),
            (3, 0, 'c'),
            (4, 1, 'd');
