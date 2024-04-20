---
--- pgcopydb test/extensions/ddl.sql
---
--- This file implements DDL changes in the test database.

begin;
-- Create new tables with foreign key relationships to test
-- the query for `schema_list_extensions()`
create table one (id int primary key, two_id int);
create table two (id int primary key, one_id int, three_id int);
create table three (id int primary key);
alter table one add constraint one_two_fk foreign key (two_id) references two(id);
alter table two add constraint two_one_fk foreign key (one_id) references one(id);
alter table two add constraint two_three_fk foreign key (three_id) references three(id);
commit;
