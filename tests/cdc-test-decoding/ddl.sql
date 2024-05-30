---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;
alter table payment_p2022_01 replica identity full;
alter table payment_p2022_02 replica identity full;
alter table payment_p2022_03 replica identity full;
alter table payment_p2022_04 replica identity full;
alter table payment_p2022_05 replica identity full;
alter table payment_p2022_06 replica identity full;
alter table payment_p2022_07 replica identity full;
commit;

begin;

CREATE TABLE IF NOT EXISTS public."""dqname"""
(
    id bigserial
);

commit;


begin;

CREATE TABLE IF NOT EXISTS public.identifer_as_column
(
    time bigserial
);
alter table public.identifer_as_column replica identity full;

commit;

--
-- See https://github.com/dimitri/pgcopydb/issues/736
--
begin;

CREATE TABLE t_bit_types
(
    id serial primary key,
     a bit(3),
     b bit varying(5)
);

INSERT INTO t_bit_types (a,b) VALUES (B'101', B'00');

commit;
