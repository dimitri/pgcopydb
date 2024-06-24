--
-- See https://github.com/dimitri/pgcopydb/issues/280
--
create schema foo;

-- create status dictionary table
create table foo.tbl_status (
    id bigserial not null primary key,
    name varchar(32) not null unique check (name != '')
);

insert into foo.tbl_status (id, name)
     values (1, 'draft'),
            (2, 'active'),
            (3, 'closed');

-- fix id sequence value the manual way
SELECT setval(pg_get_serial_sequence('foo.tbl_status', 'id'),
              (SELECT COALESCE(MAX(id) + 1, 1) FROM foo.tbl_status),
              false);

-- create first table
create table foo.tbl1 (
    id bigserial not null primary key,
    status_id bigint not null default 1 references foo.tbl_status(id),
    desc_text varchar(32)
);

create index if not exists tbl1_status_id_idx on foo.tbl1(status_id);

-- create second table
create table foo.tbl2 (
    id bigserial not null primary key,
    tbl1_id bigint not null references foo.tbl1(id),
    desc_text varchar(32)
);

create index if not exists tbl2_tbl1_id_idx on foo.tbl2(tbl1_id);

--
-- And another schema that we exclude wholesale.
--
create schema bar;


--
-- See https://github.com/dimitri/pgcopydb/issues/390
--
create schema app;
create schema copy;

create table app.foo(id bigserial, f1 text);
create table copy.foo(like app.foo including all);


--
-- See https://github.com/dimitri/pgcopydb/issues/413
--
create schema schema_name_20_chars;

create table schema_name_20_chars.very______long______table______name_______50_chars
 (
   id serial
 );

--
-- To test materialized view filtering
--
create materialized view foo.matview_1 as select 1 as id;
create index matview_1_idx on foo.matview_1(id);

create materialized view foo.matview_1_exclude_data as select 1;

create materialized view foo.matview_1_exclude_as_table as select 1 as id;

create materialized view foo.matview_2_depends_on_matview_1_exclude_as_table as select * from foo.matview_1_exclude_as_table;

--
-- TODO: We don't handle the case where a materialized view depends
-- on another materialized view that's refresh is filtered out.
-- In that case, we should exclude the materialized refresh of
-- the dependent materialized view as well.
--
-- create materialized view foo.matview_3_depends_on_matview_1_exclude_table as select * from foo.matview_1_exclude_data;

--
-- See: https://github.com/dimitri/pgcopydb/issues/817
--
create schema seq;

-- A sequence used as default
create sequence seq.default_table_id_seq;
create table seq.default_table (id integer primary key default nextval('seq.default_table_id_seq'));
select setval('seq.default_table_id_seq', 667);

-- A sequence used as identity
create table seq.identity_table (id integer primary key generated always as identity);
select setval('seq.identity_table_id_seq', 668);

-- A standalone sequence
create sequence seq.standalone_id_seq;
select setval('seq.standalone_id_seq', 669);

-- A standalone sequence smallint
create sequence seq.standalone_smallint_id_seq as smallint;
select setval('seq.standalone_smallint_id_seq', 670);

-- A standalone sequence with a minvalue that has not been set
create sequence seq.standalone_minvalue_id_seq minvalue 671;
