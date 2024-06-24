---
--- See https://github.com/dimitri/pgcopydb/issues/777
---

-- A sequence used as default
create sequence default_table_id_seq;
create table default_table (id integer primary key default nextval('default_table_id_seq'));
select setval('default_table_id_seq', 667);

-- A sequence used as identity
create table identity_table (id integer primary key generated always as identity);
select setval('identity_table_id_seq', 668);

-- A standalone sequence
create sequence standalone_id_seq;
select setval('standalone_id_seq', 669);

-- A standalone sequence smallint
create sequence standalone_smallint_id_seq as smallint;
select setval('standalone_smallint_id_seq', 670);

-- A standalone sequence with a minvalue that has not been set
create sequence standalone_minvalue_id_seq minvalue 671;
