with
 seqs(seqoid, nspname, relname, restore_list_name) as
 (
    select s.oid as seqoid,
           format('%I', sn.nspname) as nspname,
           format('%I', s.relname) as relname,
           format('%s %s %s',
                  regexp_replace(sn.nspname, '[\n\r]', ' '),
                  regexp_replace(s.relname, '[\n\r]', ' '),
                  regexp_replace(auth.rolname, '[\n\r]', ' '))
      from pg_class s
           join pg_namespace sn on sn.oid = s.relnamespace
           join pg_roles auth ON auth.oid = s.relowner

     where s.relkind = 'S'
       and sn.nspname !~ 'pgcopydb'

-- avoid pg_class entries which belong to extensions

       and not exists
         (
           select 1
             from pg_depend d
            where d.classid = 'pg_class'::regclass
              and d.objid = s.oid
              and d.deptype = 'e'
         )
    )

    select s.seqoid, s.nspname, s.relname, s.restore_list_name,
           r1.oid as ownedby,
           r2.oid as attrelid,
           a.oid as attroid
      from seqs as s

       left join pg_depend d1 on d1.objid = s.seqoid
        and d1.classid = 'pg_class'::regclass
        and d1.refclassid = 'pg_class'::regclass
        and d1.deptype in ('i', 'a')

       left join pg_depend d2 on d2.refobjid = s.seqoid
        and d2.refclassid = 'pg_class'::regclass
        and d2.classid = 'pg_attrdef'::regclass
       left join pg_attrdef a on a.oid = d2.objid
       left join pg_attribute at
         on at.attrelid = a.adrelid
        and at.attnum = a.adnum

       left join pg_class r1 on r1.oid = d1.refobjid
       left join pg_namespace rn1 on rn1.oid = r1.relnamespace

       left join pg_class r2 on r2.oid = at.attrelid
       left join pg_namespace rn2 on rn2.oid = r2.relnamespace

-- exclude-schema
      left join pg_temp.filter_exclude_schema fn1
             on rn1.nspname = fn1.nspname

      left join pg_temp.filter_exclude_schema fn2
             on rn2.nspname = fn2.nspname

      left join pg_temp.filter_exclude_schema fn3
			 on s.nspname = fn3.nspname

-- exclude-table
      left join pg_temp.filter_exclude_table ft1
             on rn1.nspname = ft1.nspname
            and r1.relname = ft1.relname

      left join pg_temp.filter_exclude_table ft2
             on rn2.nspname = ft2.nspname
            and r2.relname = ft2.relname

-- exclude-table-data
      left join pg_temp.filter_exclude_table_data ftd1
             on rn1.nspname = ftd1.nspname
            and r1.relname = ftd1.relname

      left join pg_temp.filter_exclude_table_data ftd2
             on rn2.nspname = ftd2.nspname
            and r2.relname = ftd2.relname

-- WHERE clause for exclusion filters
     where case

-- Default sequences
           when (r2.oid is null and r1.oid is not null) or r1.oid = r2.oid
           then rn1.nspname is not null and fn1.nspname is null
            and r1.relname is not null and ft1.relname is null
            and r1.relname is not null and ftd1.relname is null

-- Identity sequences
           when r1.oid is null and r2.oid is not null
           then rn2.nspname is not null and fn2.nspname is null
            and r2.relname is not null and ft2.relname is null
            and r2.relname is not null and ftd2.relname is null

-- Standalone sequences - no table relationships here
           else s.nspname is not null and fn3.nspname is null
           end

   order by nspname, relname
