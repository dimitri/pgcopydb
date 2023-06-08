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
