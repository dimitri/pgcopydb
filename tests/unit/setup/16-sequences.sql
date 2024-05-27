---
--- See https://github.com/dimitri/pgcopydb/issues/777
---

create sequence normal_table_id_seq;
create table normal_table (id integer primary key default nextval('normal_table_id_seq'));
create table identity_table (id integer primary key generated always as identity);
select setval('identity_table_id_seq', 667);
select setval('normal_table_id_seq', 667);
