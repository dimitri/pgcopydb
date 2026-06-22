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

-- include-only-table
    left join pg_temp.filter_include_only_table inc
           on rn.nspname = inc.nspname
          and r.relname = inc.relname

-- exclude-index
    left join pg_temp.filter_exclude_index fei
           on n.nspname = fei.nspname
          and i.relname = fei.relname

    where r.relkind = 'r' and r.relpersistence in ('p', 'u')
      and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
      and n.nspname !~ 'pgcopydb'

-- WHERE clause for exclusion filters
     and (  inc.relname is null
          or
            fei.relname is not null )

-- avoid pg_class entries which belong to extensions
     and not exists
       (
         select 1
           from pg_depend d
          where d.classid = 'pg_class'::regclass
            and d.objid = r.oid
            and d.deptype = 'e'
       )

 order by n.nspname, r.relname, i.relname;
