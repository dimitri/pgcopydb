  select c.oid, pg_table_size(c.oid) as bytes,
         pg_size_pretty(pg_table_size(c.oid))
    from pg_catalog.pg_class c
         join pg_catalog.pg_namespace n on c.relnamespace = n.oid

-- exclude-schema
         left join pg_temp.filter_exclude_schema fn
                on n.nspname = fn.nspname

-- exclude-table
         left join pg_temp.filter_exclude_table ft
                on n.nspname = ft.nspname
               and c.relname = ft.relname

-- exclude-table-data
         left join pg_temp.filter_exclude_table_data ftd
                on n.nspname = ftd.nspname
               and c.relname = ftd.relname

   where relkind = 'r' and c.relpersistence in ('p', 'u')
     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'

-- WHERE clause for exclusion filters
     and fn.nspname is null
     and ft.relname is null
     and ftd.relname is null

-- avoid pg_class entries which belong to extensions
     and not exists
       (
         select 1
           from pg_depend d
          where d.classid = 'pg_class'::regclass
            and d.objid = c.oid
            and d.deptype = 'e'
       )
