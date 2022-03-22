   select n.nspname, i.relname,
          rn.nspname, r.relname,
          indisprimary,
          indisunique,
          (select string_agg(attname, ',')
             from pg_attribute
            where attrelid = r.oid
              and array[attnum::integer] <@ indkey::integer[]
          ) as cols,
          pg_get_indexdef(indexrelid),
          c.conname,
          pg_get_constraintdef(c.oid)
     from pg_index x
          join pg_class i ON i.oid = x.indexrelid
          join pg_class r ON r.oid = x.indrelid
          join pg_namespace n ON n.oid = i.relnamespace
          join pg_namespace rn ON rn.oid = r.relnamespace
          left join pg_depend d
                 on d.classid = 'pg_class'::regclass
                and d.objid = i.oid
                and d.refclassid = 'pg_constraint'::regclass
                and d.deptype = 'i'
          left join pg_constraint c ON c.oid = d.refobjid
    where r.relkind = 'r' and r.relpersistence = 'p'
      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
      and rn.nspname = 'public' and r.relname = 'exclcon'
 order by n.nspname, r.relname;
