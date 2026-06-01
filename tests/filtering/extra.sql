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

create schema partitioned_tables;

create table partitioned_tables.sellers (
    id bigint,
    archive smallint not null
) partition by list (archive);

create table partitioned_tables.sellers_active partition of partitioned_tables.sellers default;
create table partitioned_tables.sellers_archive partition of partitioned_tables.sellers for values in ('1');

insert into partitioned_tables.sellers (id, archive) values (1, 0), (2, 1), (3, 0);

--
-- To test event trigger filtering
--
create or replace function foo.evt_trigger_func()
returns event_trigger language plpgsql as $$
begin
  raise notice 'event trigger fired';
end;
$$;

create event trigger evt_keep on ddl_command_end execute function foo.evt_trigger_func();
create event trigger evt_exclude on ddl_command_start execute function foo.evt_trigger_func();

--
-- To test cross-schema dependency filtering
-- Objects in non-excluded schemas that reference excluded schemas
-- should have their cross-schema dependencies filtered out
--
create schema excluded_test;

create table excluded_test.users (
    id bigserial primary key,
    email text
);

insert into excluded_test.users (id, email) values (1, 'test@example.com');

create or replace function excluded_test.get_uid()
returns bigint language sql stable as $$ select 1::bigint; $$;

-- Table in foo schema with FK referencing excluded schema
create table foo.tbl_with_cross_fk (
    id bigserial primary key,
    user_id bigint references excluded_test.users(id)
);

-- Table to be excluded by table-level filter (referenced by FK below)
create table foo.tbl_ref_target (
    id bigserial primary key,
    name text
);

-- Table with FK referencing the excluded table above
create table foo.tbl_with_fk_to_excluded_tbl (
    id bigserial primary key,
    ref_id bigint references foo.tbl_ref_target(id)
);

insert into foo.tbl_with_cross_fk (id, user_id) values (1, 1);

-- View in foo schema referencing excluded schema
create view foo.cross_schema_view as select * from excluded_test.users;

-- Table in foo schema with RLS policy referencing excluded schema function
create table foo.tbl_with_cross_rls (
    id bigserial primary key,
    owner_id bigint
);

insert into foo.tbl_with_cross_rls (id, owner_id) values (1, 1);

alter table foo.tbl_with_cross_rls enable row level security;

create policy cross_schema_policy on foo.tbl_with_cross_rls
    using (owner_id = excluded_test.get_uid());
