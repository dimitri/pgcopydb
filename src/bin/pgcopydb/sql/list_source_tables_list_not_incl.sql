  select c.oid,
         format('%I', n.nspname) as nspname,
         format('%I', c.relname) as relname,
         c.relkind as relkind,
         pg_am.amname,
         c.relpages, c.reltuples::bigint,
         c.relpages * current_setting('block_size')::bigint as bytesestimate,
         pg_size_pretty(c.relpages * current_setting('block_size')::bigint),
         false as excludedata,
         format('%s %s %s',
                regexp_replace(n.nspname, '[\n\r]', ' '),
                regexp_replace(c.relname, '[\n\r]', ' '),
                regexp_replace(auth.rolname, '[\n\r]', ' ')),
         case when pkeys.attname is not null
              then format('%I', pkeys.attname)
               end as partkey

    from pg_catalog.pg_class c
         join pg_catalog.pg_namespace n on c.relnamespace = n.oid
         left join pg_catalog.pg_am on c.relam = pg_am.oid
         join pg_roles auth ON auth.oid = c.relowner

-- include-only-table
    left join pg_temp.filter_include_only_table inc
           on n.nspname = inc.nspname
          and c.relname = inc.relname

-- find a copy partition key candidate
         left join lateral (
             select indrelid, indexrelid, a.attname

               from pg_index x
               join pg_class i on i.oid = x.indexrelid
               join pg_attribute a
                 on a.attrelid = c.oid and attnum = indkey[0]

              where x.indrelid = c.oid
                and (indisprimary or indisunique)
                and array_length(indkey::integer[], 1) = 1
                and atttypid in ('smallint'::regtype,
                                 'int'::regtype,
                                 'bigint'::regtype)
           order by not indisprimary, not indisunique
              limit 1
         ) as pkeys on true

   where relkind in ('r', 'p', 'm') and c.relpersistence in ('p', 'u')
     and n.nspname !~ '^pg_' and n.nspname <> 'information_schema'
     and n.nspname !~ 'pgcopydb'

-- WHERE clause for exclusion filters
     and inc.nspname is null

-- avoid pg_class entries which belong to extensions
     and not exists
       (
         select 1
           from pg_depend d
          where d.classid = 'pg_class'::regclass
            and d.objid = c.oid
            and d.deptype = 'e'
       )

order by n.nspname, c.relname;
