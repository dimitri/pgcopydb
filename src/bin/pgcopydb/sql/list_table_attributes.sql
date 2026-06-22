select a.attrelid,
       a.attnum,
       a.atttypid::integer,
       format('%I', a.attname) as attname,
       i.indrelid is not null as attisprimary,
       ri.indrelid is not null as attisreplident,
       col.is_generated = 'ALWAYS' as attisgenerated,
       coalesce(a.attidentity, '') as attidentity,
       bincompat.hasbinaryio,
       bincompat.typsendfunc

  from pg_attribute a
       join pg_class c on c.oid = a.attrelid
       join pg_namespace n on n.oid = c.relnamespace
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

 where a.attrelid = any($1::oid[])
   and not a.attisdropped
   and a.attnum > 0

 order by a.attrelid, a.attnum
