---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL to create postgres objects covering special cases
--  like maximum identifiers lengths, identifiers that requires double quotes,
--  etc.

--
-- create schemas and tables with names that needs double-quoting
--

begin;

create schema if not exists "Sp1eCial .Char";

create table "Sp1eCial .Char"."source1testing"(
    "s0" serial primary key,
    "s1" int not null
);

create schema if not exists "sp4ecial$char";

create table "sp4ecial$char"."source4testing"(
    "s0" serial primary key,
    "s1" int not null
);

commit;

--
-- create schema and table with length of NAMEDATALEN
--

begin;

create schema "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456";

create table "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456"."abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456"(
    "abcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456" serial primary key,
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789012345678901234567890123456" int not null
);

commit;
