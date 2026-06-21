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
               end as partkey,
         attrs.js as attributes

    from pg_catalog.pg_class c
         join pg_catalog.pg_namespace n on c.relnamespace = n.oid
         left join pg_catalog.pg_am on c.relam = pg_am.oid
         join pg_roles auth ON auth.oid = c.relowner
         join lateral (
               with atts as
               (
                  select attnum, atttypid::integer,
                         format('%I', attname) as attname,
                         i.indrelid is not null as attisprimary,
                         ri.indrelid is not null as attisreplident,
						  col.is_generated = 'ALWAYS' as attisgenerated,
                         coalesce(a.attidentity, '') as attidentity,
                         bincompat.hasbinaryio,
                         bincompat.typsendfunc
                    from pg_attribute a
                         left join pg_index i
                                on i.indrelid = a.attrelid
                               and a.attnum = ANY(i.indkey)
                               and i.indisprimary
                         left join pg_index ri
                                on ri.indrelid = a.attrelid
                               and a.attnum = ANY(ri.indkey)
                               and ri.indisreplident
					 	  left join information_schema.columns col
                    			on col.column_name = a.attname
					  			and col.table_name = c.relname
          			 			and col.table_schema = n.nspname

-- For each column, resolve binary I/O support through the full type
-- hierarchy: domains (follow typbasetype chain), arrays (unwrap one
-- element level via typelem), and composite types (one level of
-- attribute inspection).  Returns hasbinaryio (bool) and a
-- comma-separated list of send function names for C-side blocklist checking.
                         join lateral (
                             with recursive resolved(
                                 oid, typtype, typsend, typreceive,
                                 typbasetype, typelem, typrelid,
                                 typsendfunc, depth, followed_array
                             ) as (
                                 select t.oid, t.typtype,
                                        t.typsend, t.typreceive,
                                        t.typbasetype, t.typelem,
                                        t.typrelid,
                                        p.proname::text,
                                        0, false
                                   from pg_type t
                                   left join pg_proc p
                                          on p.oid = t.typsend
                                  where t.oid = a.atttypid
-- Single recursive arm covering BOTH domain-follow and array-element
-- unwrap. PostgreSQL's WITH RECURSIVE allows only one non-recursive
-- anchor; splitting into two UNION ALL arms causes "recursive reference
-- must not appear within its non-recursive term".
                                 union all
                                 select t.oid, t.typtype,
                                        t.typsend, t.typreceive,
                                        t.typbasetype, t.typelem,
                                        t.typrelid,
                                        p.proname::text,
                                        r.depth + 1,
                                        case when r.typtype = 'd'
                                             then r.followed_array
                                             else true end
                                   from pg_type t
                                   join resolved r
                                     on (
                                          t.oid = r.typbasetype
                                          and r.typtype = 'd'
                                         )
                                        or (
                                             t.oid = r.typelem
                                             and r.typelem <> 0
                                             and r.typtype <> 'd'
                                             and not r.followed_array
                                            )
                                   left join pg_proc p
                                          on p.oid = t.typsend
                                  where r.depth < 10
                             ),
                             composite_attrs as (
                                 select distinct cp.proname
                                   from resolved r
                                   join pg_attribute ca
                                     on ca.attrelid = r.typrelid
                                    and ca.attnum > 0
                                    and not ca.attisdropped
                                   join pg_type ct
                                     on ct.oid = ca.atttypid
                                   join pg_proc cp
                                     on cp.oid = ct.typsend
                                  where r.typtype = 'c'
                             ),
                             all_send_funcs(typsendfunc) as (
                                 select typsendfunc
                                   from resolved
                                  where typsendfunc is not null
                                 union
                                 select proname from composite_attrs
                             )
                             select
                                 bool_and(
                                     r.typsend <> 0
                                     and r.typreceive <> 0
                                 ) as hasbinaryio,
                                 (
                                     select string_agg(f.typsendfunc, ',')
                                       from all_send_funcs f
                                 ) as typsendfunc
                               from resolved r
                         ) bincompat on true
                   where a.attrelid = c.oid and not a.attisdropped
                     and a.attnum > 0
                order by attnum
               )
               select json_agg(row_to_json(atts)) as js
                from atts
              ) as attrs on true

-- exclude-schema
         left join pg_temp.filter_exclude_schema fn
                on n.nspname = fn.nspname

-- exclude-table
         left join pg_temp.filter_exclude_table ft
                on n.nspname = ft.nspname
               and c.relname = ft.relname

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
     and (   fn.nspname is not null
          or ft.relname is not null )

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
