select distinct on (n.oid) n.oid, n.nspname,
       format('- %s %s',
                regexp_replace(n.nspname, '[\n\r]', ' '),
                regexp_replace(auth.rolname, '[\n\r]', ' '))
  from pg_namespace n
       join pg_roles auth ON auth.oid = n.nspowner
       join pg_depend d
         on d.refclassid = 'pg_namespace'::regclass
        and d.refobjid = n.oid
        and d.classid = 'pg_extension'::regclass
 where nspname <> 'public' and nspname !~ '^pg_'
