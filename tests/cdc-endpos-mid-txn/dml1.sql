---
--- pgcopydb test/cdc/dml.sql
---
--- This file implements DML changes in the pagila database.

-- first transaction
begin;

insert into category(category_id, name, last_update) values (1000, 'Fantasy', '2022-12-08 00:00:01');
insert into category(category_id, name, last_update) values (1001, 'History', '2022-12-09 00:00:01');
insert into category(category_id, name, last_update) values (1002, 'Adventure', '2022-12-10 00:00:01');
insert into category(category_id, name, last_update) values (1003, 'Musical', '2022-12-11 00:00:01');
insert into category(category_id, name, last_update) values (1004, 'Western', '2022-12-12 00:00:01');

commit;
