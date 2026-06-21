  select c.oid, pg_table_size(c.oid) as bytes,
         pg_size_pretty(pg_table_size(c.oid))
    from pg_catalog.pg_class c
         join pg_catalog.pg_namespace n on c.relnamespace = n.oid

-- include-only-table
         join pg_temp.filter_include_only_table inc
           on n.nspname = inc.nspname
          and c.relname = inc.relname

   where relkind = 'r' and c.relpersistence in ('p', 'u')
     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'

-- avoid pg_class entries which belong to extensions
     and not exists
       (
         select 1
           from pg_depend d
          where d.classid = 'pg_class'::regclass
            and d.objid = c.oid
            and d.deptype = 'e'
       )
