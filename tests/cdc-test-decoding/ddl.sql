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

CREATE SCHEMA IF NOT EXISTS "Foo"".Bar";

CREATE TABLE IF NOT EXISTS "Foo"".Bar".":Identifer As ""Column"".$1:"
(
    time bigserial,
    "[column name]" text,
    primary key (time, "[column name]")
);

CREATE SCHEMA IF NOT EXISTS "Unicode""Test";

CREATE TYPE "[Status]" AS ENUM ('new', 'open', 'closed');

CREATE TABLE IF NOT EXISTS "Unicode""Test".U&"\0441\043B\043E\043D"
(
    id bigserial,
    U&"!0441!043B!043E!043D" UESCAPE '!' "[Status]",
    U&"!043A!043E!043B!043E!043D!043A!0430" UESCAPE '!' text,
    primary key (id, U&"!0441!043B!043E!043D" UESCAPE '!')
);

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

begin;
create table if not exists generated_column_test
(
    id bigint primary key,
    name text,
    greet_hello text generated always as ('Hello ' || name) stored,
    greet_hi  text generated always as ('Hi ' || name) stored,
    -- tests identifier escaping
    time text generated always as ('identifier 1' || 'now') stored,
    email text not null,
    -- tests identifier escaping
    "table" text generated always as ('identifier 2' || name) stored,
    """table""" text generated always as ('identifier 3' || name) stored,
    """hel""lo""" text generated always as ('identifier 4' || name) stored
);
commit;
