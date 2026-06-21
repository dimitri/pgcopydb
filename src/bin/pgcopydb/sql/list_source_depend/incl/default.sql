  SELECT n.nspname, c.relname,
         refclassid, refobjid, classid, objid,
         deptype, type, identity
    FROM unconcat

-- include-only-table
         join pg_class c
           on unconcat.refclassid = 'pg_class'::regclass
          and unconcat.refobjid = c.oid

         join pg_catalog.pg_namespace n on c.relnamespace = n.oid

         join pg_temp.filter_include_only_table inc
           on n.nspname = inc.nspname
          and c.relname = inc.relname

         , pg_identify_object(classid, objid, objsubid)

   WHERE NOT (refclassid = classid AND refobjid = objid)
      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
      and n.nspname !~ 'pgcopydb'
      and type not in ('toast table column', 'default value')

-- remove duplicates due to multiple refobjsubid / objsubid
GROUP BY n.nspname, c.relname,
         refclassid, refobjid, classid, objid, deptype, type, identity
