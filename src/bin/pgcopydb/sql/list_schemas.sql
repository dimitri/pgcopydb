select n.oid, n.nspname,
       format('- %s %s',
                regexp_replace(n.nspname, '[\n\r]', ' '),
                regexp_replace(auth.rolname, '[\n\r]', ' '))
  from pg_namespace n
       join pg_roles auth ON auth.oid = n.nspowner
 where nspname <> 'information_schema' and nspname !~ '^pg_';
