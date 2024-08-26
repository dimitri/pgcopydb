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

-- Create a custom type with the longest possible name which is PG_MAX_IDENTIFIER_LENGTH
create type type_with_a_long_name_that_is_exactly_pg_max_identifier_length_ AS ENUM ('that', 'means', '63', 'characters');

-- Create a function with the longest possible signature to test edge cases.
-- Particularly, we had a bug where the Archive TOC line corresponding to a
-- function with a long name was not parsed correctly.
create function func_with_a_long_name_that_is_exactly_pg_max_identifier_length_(
    arg_1 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_2 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_3 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_4 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_5 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_6 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_7 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_8 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_9 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_10 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_11 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_12 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_13 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_14 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_15 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_16 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_17 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_18 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_19 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_20 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_21 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_22 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_23 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_24 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_25 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_26 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_27 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_28 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_29 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_30 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_31 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_32 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_33 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_34 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_35 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_36 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_37 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_38 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_39 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_40 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_41 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_42 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_43 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_44 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_45 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_46 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_47 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_48 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_49 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_50 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_51 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_52 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_53 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_54 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_55 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_56 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_57 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_58 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_59 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_60 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_61 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_62 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_63 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_64 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_65 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_66 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_67 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_68 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_69 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_70 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_71 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_72 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_73 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_74 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_75 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_76 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_77 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_78 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_79 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_80 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_81 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_82 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_83 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_84 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_85 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_86 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_87 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_88 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_89 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_90 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_91 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_92 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_93 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_94 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_95 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_96 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_97 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_98 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_99 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_,
    arg_100 type_with_a_long_name_that_is_exactly_pg_max_identifier_length_
) returns integer as $$
begin
    return 1;
end;
$$ language plpgsql;


--
-- See https://github.com/dimitri/pgcopydb/issues/710
-- Tables with toast columns may output `unchanged-toast-datum` values
begin;
create sequence xpto_rand_seq start 79 increment 1499; -- portable "random"

-- test table from PG regression tests
create table xpto (
    id serial primary key,
    toasted_col1 text,
    rand1 float8 default nextval('xpto_rand_seq'),
    toasted_col2 text,
    rand2 float8 default nextval('xpto_rand_seq')
);

-- table with only toastable columns
create table xpto2 (
    toasted_col1 text,
    toasted_col2 text
);

commit;
