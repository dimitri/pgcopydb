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

-- include-only-table
       left join pg_temp.filter_include_only_table inc1
         on rn1.nspname = inc1.nspname
        and r1.relname = inc1.relname

       left join pg_temp.filter_include_only_table inc2
         on rn2.nspname = inc2.nspname
        and r2.relname = inc2.relname

      where (r1.relname is not null and inc1.relname is not null)
         or (r2.relname is not null and inc2.relname is not null)

   order by nspname, relname;
