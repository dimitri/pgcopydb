  SELECT n.nspname, c.relname,
         refclassid, refobjid, classid, objid,
         deptype, type, identity
    FROM unconcat

         join pg_class c
           on unconcat.refclassid = 'pg_class'::regclass
          and unconcat.refobjid = c.oid

         join pg_catalog.pg_namespace n
           on c.relnamespace = n.oid

-- exclude-schema
         left join pg_temp.filter_exclude_schema fn
                on n.nspname = fn.nspname

-- exclude-table
         left join pg_temp.filter_exclude_table ft
                on n.nspname = ft.nspname
               and c.relname = ft.relname

         , pg_identify_object(classid, objid, objsubid)

   WHERE NOT (refclassid = classid AND refobjid = objid)
      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
      and n.nspname !~ 'pgcopydb'
      and type not in ('toast table column', 'default value')

-- WHERE clause for exclusion filters
     and (   fn.nspname is not null
          or ft.relname is not null )

-- remove duplicates due to multiple refobjsubid / objsubid
GROUP BY n.nspname, c.relname,
         refclassid, refobjid, classid, objid, deptype, type, identity;
