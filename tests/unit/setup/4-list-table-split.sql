---
--- This file creates tables and populate the pgcopdyb.table_size cache with
--  fake sizes.
---

-- Create three tables with identical schema and data

create table table_1 (
    c_bigserial bigserial primary key,
    c_char char(10)
);

create table table_2 (
    c_bigserial bigserial primary key,
    c_char char(10)
);

create table table_3 (
    c_bigserial bigserial primary key,
    c_char char(10)
);

-- Insert 100 rows into table_1 and duplicate data in table_2 and table_3.

insert into table_1 (c_char)
select
    left (md5(random()::text),
        10)
from
    generate_series(1, 100) s (i);

insert into table_2
select
    *
from
    table_1;

insert into table_3
select
    *
from
    table_1;

-- Create pgcopydb.table_size cache and populate with fake table size data

drop schema if exists pgcopydb;

create schema pgcopydb;

create table pgcopydb.pgcopydb_table_size (
    oid oid unique,
    bytes bigint
);

-- Insert fake size of 100 KB for table_1 and 50 KB for table_2 into the cache.
-- No value inserted for table_3 to check if pgcopydb breaks.

with cache_table_size as ((
        select
            'table_1'::regclass::oid,
            102400)
    union (
        select
            'table_2'::regclass::oid,
            51200))
insert into pgcopydb.pgcopydb_table_size (oid, bytes)
select
    *
from
    cache_table_size;
