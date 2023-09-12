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

--
-- also create tables with names that needs double-quoting to see that our
-- partitioning queries can cope with that
--
CREATE SCHEMA IF NOT EXISTS "Sp1eCial .Char";

CREATE TABLE "Sp1eCial .Char"."source1testing"
 (
   "s0" int PRIMARY KEY,
   "s1" int NOT NULL
 );

insert into "Sp1eCial .Char"."source1testing"("s0", "s1")
select x, (x * 2) % 100000
  from generate_series(1, 10000) AS t(x);


CREATE TABLE "Sp1eCial .Char"."Tabl e.1testing"
 (
  "iD" int PRIMARY KEY,
  "regId" int,
  "status" int,
  "nA M.e" character varying(20) NOT NULL,

   CONSTRAINT "Tabl e_fk_1_testing"
  FOREIGN KEY ("iD")
   REFERENCES "Sp1eCial .Char"."source1testing"("s0")
);

insert into "Sp1eCial .Char"."Tabl e.1testing"("iD", "regId", "status", "nA M.e")
select
    "s0",
    "s0",
    random() * 100,
    'Name ' || "s0"
from
    "Sp1eCial .Char"."source1testing";

insert into pgcopydb.pgcopydb_table_size (oid, bytes)
     select tname::regclass,
            10 * 1024 * 1024
       from (values ('"Sp1eCial .Char"."source1testing"'),
                    ('"Sp1eCial .Char"."Tabl e.1testing"')) as t(tname);
