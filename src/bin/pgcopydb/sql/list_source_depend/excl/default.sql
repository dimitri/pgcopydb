  SELECT cn.nspname, c.relname,
         refclassid, refobjid, classid, objid,
         deptype, type, identity
    FROM unconcat

         join pg_class c
           on unconcat.refclassid = 'pg_class'::regclass
          and unconcat.refobjid = c.oid

         join pg_catalog.pg_namespace cn
           on c.relnamespace = cn.oid

-- exclude-schema
         left join pg_temp.filter_exclude_schema fn
                on cn.nspname = fn.nspname

-- exclude-table
         left join pg_temp.filter_exclude_table ft
                on cn.nspname = ft.nspname
               and c.relname = ft.relname

         , pg_identify_object(classid, objid, objsubid)

   WHERE NOT (refclassid = classid AND refobjid = objid)
      and cn.nspname !~ '^pg_' and cn.nspname <> 'information_schema'
      and cn.nspname !~ 'pgcopydb'
      and type not in ('toast table column', 'default value')

-- WHERE clause for exclusion filters
     and fn.nspname is null
     and ft.relname is null

-- remove duplicates due to multiple refobjsubid / objsubid
GROUP BY cn.nspname, c.relname,
         refclassid, refobjid, classid, objid, deptype, type, identity
