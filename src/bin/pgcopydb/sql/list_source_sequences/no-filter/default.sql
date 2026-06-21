  select c.oid,
         format('%I', n.nspname) as nspname,
         format('%I', c.relname) as relname,
         format('%s %s %s',
                regexp_replace(n.nspname, '[\n\r]', ' '),
                regexp_replace(c.relname, '[\n\r]', ' '),
                regexp_replace(auth.rolname, '[\n\r]', ' ')),
         NULL as ownedby,
         NULL as attrelid,
         NULL as attroid

    from pg_catalog.pg_class c
         join pg_catalog.pg_namespace n on c.relnamespace = n.oid
         join pg_roles auth ON auth.oid = c.relowner

   where c.relkind = 'S' and c.relpersistence in ('p', 'u')
     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
     and n.nspname !~ 'pgcopydb'

-- avoid pg_class entries which belong to extensions
     and not exists
       (
         select 1
           from pg_depend d
          where d.classid = 'pg_class'::regclass
            and d.objid = c.oid
            and d.deptype = 'e'
       )

order by n.nspname, c.relname
