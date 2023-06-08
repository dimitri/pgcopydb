select nspname
  from pg_namespace
 where nspname !~ '^pg_' and nspname <> 'information_schema';
