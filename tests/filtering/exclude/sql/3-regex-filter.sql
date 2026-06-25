-- Tables excluded by the regex pattern ~/^tbl/ in [exclude-table] must not
-- appear in the target database.
select tablename
  from pg_tables
 where schemaname = 'foo'
   and tablename like 'tbl%'
 order by 1;
