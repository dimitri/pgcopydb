-- This file contains DML scripts in the pagila database for
-- the follow mode tests with filtering enabled.

-- DML scripts to test filtering with exclude.ini

-- DML on public.actor should not be migrated because it is excluded
-- in exclude.ini
update public.actor set first_name = 'JOHN' where actor_id = 1;

-- DML on foo.tbl1 schema should be migrated because it is
-- not filtered out in exclude.ini
insert into foo.tbl1(desc_text) values ('bar');

-- DML on app.foo should not be migrated because it is
-- filtered out in exclude.ini
insert into app.foo(f1) values ('foo');

