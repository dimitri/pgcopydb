-- This file contains DML scripts in the pagila database for
-- the follow mode tests with filtering enabled.

-- DML scripts to test filtering with include.ini

-- DML on public.actor should be migrated because it is included
-- in include.ini
update public.actor set first_name = 'JENNIFER' where actor_id = 1;

-- DML on foo.tbl1 schema should not be migrated because it is
-- filtered out in include.ini
insert into foo.tbl1(desc_text) values ('foo');

-- DML on public.film should be migrated because it is
-- filtered in include.ini
delete from public.film_category where film_id = 1;