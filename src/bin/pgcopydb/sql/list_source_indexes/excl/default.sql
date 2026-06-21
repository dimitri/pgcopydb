   select i.oid,
          format('%I', n.nspname) as inspname,
          format('%I', i.relname) as irelname,
          r.oid,
          format('%I', rn.nspname) as rnspname,
          format('%I', r.relname) as rrelname,
          indisprimary,
          indisunique,
          (select string_agg(format('%I', attname), ',')
             from pg_attribute
            where attrelid = r.oid
              and array[attnum::integer] <@ indkey::integer[]
          ) as cols,
          pg_get_indexdef(indexrelid),
          c.oid,
          c.condeferrable,
          c.condeferred,
          case when conname is not null
               then format('%I', c.conname)
           end as conname,
          pg_get_constraintdef(c.oid),
          format('%s %s %s',
                 regexp_replace(n.nspname, '[\n\r]', ' '),
                 regexp_replace(i.relname, '[\n\r]', ' '),
                 regexp_replace(auth.rolname, '[\n\r]', ' '))
     from pg_index x
          join pg_class i ON i.oid = x.indexrelid
          join pg_class r ON r.oid = x.indrelid
          join pg_namespace n ON n.oid = i.relnamespace
          join pg_namespace rn ON rn.oid = r.relnamespace
          join pg_roles auth ON auth.oid = i.relowner
          left join pg_depend d
                 on d.classid = 'pg_class'::regclass
                and d.objid = i.oid
                and d.refclassid = 'pg_constraint'::regclass
                and d.deptype = 'i'
          left join pg_constraint c ON c.oid = d.refobjid

-- exclude-schema
         left join pg_temp.filter_exclude_schema fn
                on rn.nspname = fn.nspname

-- exclude-table
         left join pg_temp.filter_exclude_table ft
                on rn.nspname = ft.nspname
               and r.relname = ft.relname

-- exclude-table-data
         left join pg_temp.filter_exclude_table_data ftd
                on rn.nspname = ftd.nspname
               and r.relname = ftd.relname

-- exclude-index
         left join pg_temp.filter_exclude_index fei
                on n.nspname = fei.nspname
               and i.relname = fei.relname

    where r.relkind = 'r' and r.relpersistence in ('p', 'u')
      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
      and n.nspname !~ 'pgcopydb'

-- WHERE clause for exclusion filters
     and fn.nspname is null
     and ft.relname is null
     and ftd.relname is null
     and fei.relname is null

-- avoid pg_class entries which belong to extensions
     and not exists
       (
         select 1
           from pg_depend d
          where d.classid = 'pg_class'::regclass
            and d.objid = r.oid
            and d.deptype = 'e'
       )

 order by n.nspname, r.relname, i.relname
