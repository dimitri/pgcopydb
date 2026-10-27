--
-- tests/toc-coverage/fixture.sql
--
-- Installs one (or a small number) of as many distinct pg_dump/pg_restore
-- Archive TOC object types as can reasonably be created with plain SQL, on
-- a single superuser connection, without external extensions that aren't
-- guaranteed present -- except fixture_transform_ext, a tiny do-nothing C
-- extension committed alongside this file, needed for a real TRANSFORM
-- (see its own README for why plain SQL/plpgsql can't make one). Used to
-- capture a real, saved `pg_restore --list` fixture exercising pgcopydb's
-- Archive TOC parser against real pg_dump output rather than hand-typed
-- sample lines.
--
-- Deliberately NOT exhaustive: ARCHIVE_TAG_ACCESS_METHOD, SHELL_TYPE and
-- DUMMY_TYPE need a C-level handler function or a genuine circular type
-- reference; a real ARCHIVE_TAG_SUBSCRIPTION needs a reachable publisher.
-- Noted inline where a tag type is skipped for that reason.

create schema toc_fixture;
set search_path = toc_fixture, public;

--
-- SCHEMA, TABLE, SEQUENCE (standalone + owned via serial), DEFAULT,
-- ATTRDEF, SEQUENCE_OWNED_BY, INDEX, CONSTRAINT (pkey), CHECK_CONSTRAINT,
-- COLUMN (for ACL/COMMENT), FK_CONSTRAINT, TRIGGER, RULE, POLICY,
-- ROW_SECURITY
--
create table parent (
    id    serial primary key,
    name  text not null default 'n/a',
    note  text check (note <> '')
);

comment on table parent is 'fixture parent table';
comment on column parent.name is 'fixture parent.name column';
grant select (name) on parent to public;

create sequence standalone_seq;

create table child (
    id         serial primary key,
    parent_id  int references parent(id),
    amount     numeric(10,2)
);

create index child_parent_id_idx on child (parent_id);

create function parent_trg_fn() returns trigger
language plpgsql as $$ begin return new; end $$;

create trigger parent_trg before insert on parent
    for each row execute function parent_trg_fn();

comment on trigger parent_trg on parent is 'fixture trigger comment';

create rule child_rule as on update to child do also nothing;
comment on rule child_rule on child is 'fixture rule comment';

alter table child enable row level security;
create policy child_pol on child using (true);
comment on policy child_pol on child is 'fixture policy comment';

--
-- VIEW, MATERIALIZED VIEW, REFRESH_MATERIALIZED_VIEW (dump side effect)
--
create view parent_view as select id, name from parent;
comment on view parent_view is 'fixture view comment';

create materialized view parent_matview as select id, name from parent;
comment on materialized view parent_matview is 'fixture matview comment';

--
-- DOMAIN
--
create domain positive_int as integer check (value > 0);
comment on domain positive_int is 'fixture domain comment';

--
-- FUNCTION, PROCEDURE, AGGREGATE
--
create function fixture_add(a int, b int) returns int
language sql as $$ select a + b $$;
comment on function fixture_add(int, int) is 'fixture function comment';

create procedure fixture_proc(a int)
language plpgsql as $$ begin perform a; end $$;
comment on procedure fixture_proc(int) is 'fixture procedure comment';

create aggregate fixture_concat(text) (
    sfunc = textcat,
    stype = text,
    initcond = ''
);
comment on aggregate fixture_concat(text) is 'fixture aggregate comment';

-- a zero-argument aggregate: format_function_arguments' own is_agg
-- special case prints "(*)" literally instead of calling
-- pg_get_function_arguments (see pg_dump.c) -- archive_toc_parser.c's
-- splitFunctionArgs needs to recognize that as zero real arguments, not
-- as one literal "*" argument
create aggregate fixture_count_fn (*) (
    sfunc = int4inc,
    stype = int,
    initcond = '0'
);
comment on aggregate fixture_count_fn (*) is 'fixture zero-arg aggregate comment';

-- a function with an array-typed argument, and more than one argument to
-- split on. Nested parens around a single argument (a type's own
-- typmod, e.g. numeric(10,2), or a default value's own parenthesized
-- expression) are deliberately NOT exercised here: pg_dump's COMMENT/ACL
-- dumping uses pg_get_function_identity_arguments(), which keeps argmode
-- and argname but drops default values outright, and typmod was never
-- part of a function's own argument types to begin with -- there is no
-- real pg_dump output where a paren ends up nested inside one of these
-- signatures. archive_toc_parser.c's splitFunctionArgs still handles
-- that case defensively (see its own synthetic-input test coverage),
-- it's just not reachable from an actual captured fixture.
create function fixture_scale(b int[], a int) returns int
language sql as $$ select a * coalesce(array_length(b, 1), 1) $$;
comment on function fixture_scale(int[], int) is 'fixture typed-args function comment';

-- a function name that needs fmtId() double-quoting: mixed case and an
-- embedded space
create function "Fixture Weird Fn"(a int) returns int
language sql as $$ select a $$;
comment on function "Fixture Weird Fn"(int) is 'fixture quoted function name comment';

--
-- OPERATOR, OPERATOR_CLASS, OPERATOR_FAMILY
--
create function fixture_eq_fn(a int, b int) returns boolean
language sql as $$ select a = b $$;

create operator === (
    leftarg = int,
    rightarg = int,
    procedure = fixture_eq_fn
);
comment on operator === (int, int) is 'fixture operator comment';

-- a prefix (left-unary) operator: pg_dump writes "NONE" for the leftarg
-- it doesn't have (postfix/right-unary operators were removed in PG 14,
-- so there is no equivalent "NONE" on the right to cover here)
create function fixture_neg_fn(a int) returns int
language sql as $$ select -a $$;

create operator @-@ (
    rightarg = int,
    procedure = fixture_neg_fn
);
comment on operator @-@ (NONE, int) is 'fixture prefix operator comment';

create operator family fixture_int_ops using btree;
comment on operator family fixture_int_ops using btree is 'fixture operator family comment';

create operator class fixture_int_ops_class for type int using btree family fixture_int_ops as
    operator 1 <,
    operator 2 <=,
    operator 3 =,
    operator 4 >=,
    operator 5 >,
    function 1 btint4cmp(int, int);
comment on operator class fixture_int_ops_class using btree is 'fixture operator class comment';

--
-- CAST
--
create function positive_int_to_int(positive_int) returns int
language sql as $$ select $1::int $$;
create cast (positive_int as int) with function positive_int_to_int(positive_int);
comment on cast (positive_int as int) is 'fixture cast comment';

--
-- COLLATION
--
create collation fixture_collation (provider = libc, locale = 'C');
comment on collation fixture_collation is 'fixture collation comment';

--
-- CONVERSION
--
create default conversion fixture_conv for 'UTF8' to 'LATIN1' from utf8_to_iso8859_1;
comment on conversion fixture_conv is 'fixture conversion comment';

--
-- STATISTICS (extended statistics object)
--
create statistics fixture_stats (dependencies) on id, parent_id from child;
comment on statistics fixture_stats is 'fixture statistics comment';

--
-- TEXT SEARCH: CONFIGURATION, DICTIONARY, TEMPLATE, PARSER (parser needs a
-- C handler, so we only comment on the built-in default parser's OID via
-- its config instead of creating a new one)
--
create text search dictionary fixture_tsdict (
    template = simple,
    stopwords = english
);

create text search configuration fixture_tsconfig (copy = english);
alter text search configuration fixture_tsconfig
    alter mapping for asciiword with fixture_tsdict;

comment on text search dictionary fixture_tsdict is 'fixture ts dictionary comment';
comment on text search configuration fixture_tsconfig is 'fixture ts configuration comment';
comment on text search template simple is 'fixture ts template comment';

--
-- FOREIGN DATA WRAPPER, SERVER, FOREIGN TABLE, USER MAPPING
--
create extension if not exists postgres_fdw;

create server fixture_server foreign data wrapper postgres_fdw
    options (host 'localhost', dbname 'postgres');
comment on server fixture_server is 'fixture server comment';

create user mapping for current_user server fixture_server
    options (user 'postgres');

create foreign table fixture_foreign_table (id int, name text)
    server fixture_server options (schema_name 'public', table_name 'parent');
comment on foreign table fixture_foreign_table is 'fixture foreign table comment';

comment on foreign data wrapper postgres_fdw is 'fixture fdw comment';

--
-- EXTENSION (a second, simple one; postgres_fdw above already covers one)
--
create extension if not exists pg_trgm;

--
-- EVENT TRIGGER
--
create function fixture_evt_fn() returns event_trigger
language plpgsql as $$ begin end $$;
create event trigger fixture_evt on ddl_command_end
    execute function fixture_evt_fn();
comment on event trigger fixture_evt is 'fixture event trigger comment';

--
-- PUBLICATION, PUBLICATION_TABLE, PUBLICATION_TABLES_IN_SCHEMA
--
create publication fixture_pub for table parent, child;
create publication fixture_pub_schema for tables in schema toc_fixture;
comment on publication fixture_pub is 'fixture publication comment';

--
-- SUBSCRIPTION (connect = false so it does not need a reachable publisher)
--
create subscription fixture_sub
    connection 'host=localhost dbname=postgres'
    publication fixture_pub
    with (connect = false, create_slot = false, enabled = false);
comment on subscription fixture_sub is 'fixture subscription comment';

--
-- TRANSFORM: https://www.postgresql.org/docs/current/sql-createtransform.html
-- -- tells a procedural language (PL/Python, PL/Perl, ...) how to convert
-- a given SQL type to and from that language's own native representation.
-- A real one needs a C-language transform function pair (the pseudo-type
-- `internal` their signature requires can't be used from plpgsql/SQL),
-- so this uses the do-nothing fixture_transform_ext extension committed
-- alongside this file instead of a stock PL extension (plpython3/plperl
-- aren't guaranteed installed, and their own *_plpython/*_plperl
-- transform contrib modules aren't packaged on every distro -- see that
-- extension's own README for why). The CREATE TRANSFORM statement itself
-- is run standalone, after the extension, so it's dumped as its own
-- object rather than hidden as an extension member.
--
create extension if not exists fixture_transform;

create transform for int4 language plpgsql (
    from sql with function fixture_transform_from_sql(internal),
    to sql with function fixture_transform_to_sql(internal)
);
comment on transform for int4 language plpgsql is 'fixture transform comment';

--
-- LARGE OBJECT / BLOB / BLOB_DATA
--
select lo_create(0);

--
-- GRANT / ACL and COMMENT on a few more object kinds, to exercise the
-- composite-tag path broadly (table, sequence, function, schema, domain)
--
grant select on standalone_seq to public;
grant execute on function fixture_add(int, int) to public;
grant usage on schema toc_fixture to public;
grant usage on domain positive_int to public;
comment on schema toc_fixture is 'fixture schema comment';
comment on sequence standalone_seq is 'fixture sequence comment';

--
-- DEFAULT ACL
--
alter default privileges in schema toc_fixture grant select on tables to public;

--
-- EXTENSION comment (for an extension's own COMMENT entry, distinct from
-- COMMENT ON EXTENSION filtering already covered elsewhere in the test
-- suite)
--
comment on extension pg_trgm is 'fixture extension comment';
