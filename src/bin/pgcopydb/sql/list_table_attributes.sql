with recursive

-- Resolve binary I/O support for each distinct column type used across the
-- selected tables.  Running the recursive walk once per distinct type (rather
-- than once per column via LATERAL) keeps cost proportional to the number of
-- distinct types, not the number of columns.
resolved(
    root_typid,
    oid, typtype, typsend, typreceive,
    typbasetype, typelem, typrelid,
    typsendfunc, depth, followed_array
) as (
    select t.oid,
           t.oid, t.typtype,
           t.typsend, t.typreceive,
           t.typbasetype, t.typelem, t.typrelid,
           p.proname::text,
           0, false
      from (
          select distinct a.atttypid
            from pg_attribute a
           where a.attrelid = any($1::oid[])
             and not a.attisdropped
             and a.attnum > 0
      ) dt
      join pg_type t on t.oid = dt.atttypid
      left join pg_proc p on p.oid = t.typsend

-- Single recursive arm covering domain-follow and array-element unwrap.
    union all

    select r.root_typid,
           t.oid, t.typtype,
           t.typsend, t.typreceive,
           t.typbasetype, t.typelem, t.typrelid,
           p.proname::text,
           r.depth + 1,
           case when r.typtype = 'd'
                then r.followed_array
                else true end
      from resolved r
      join pg_type t
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
      left join pg_proc p on p.oid = t.typsend
     where r.depth < 10
),

composite_attrs(root_typid, typsendfunc) as (
    select distinct r.root_typid, cp.proname
      from resolved r
      join pg_attribute ca
        on ca.attrelid = r.typrelid
       and ca.attnum > 0
       and not ca.attisdropped
      join pg_type ct on ct.oid = ca.atttypid
      join pg_proc cp on cp.oid = ct.typsend
     where r.typtype = 'c'
),

all_send_funcs(root_typid, typsendfunc) as (
    select root_typid, typsendfunc
      from resolved
     where typsendfunc is not null
    union
    select root_typid, typsendfunc
      from composite_attrs
),

-- One row per distinct type: hasbinaryio and comma-separated send func names
-- for C-side blocklist checking.
type_bincompat(atttypid, hasbinaryio, typsendfunc) as (
    select r.root_typid,
           bool_and(r.typsend <> 0 and r.typreceive <> 0) as hasbinaryio,
           (
               select string_agg(f.typsendfunc, ',')
                 from all_send_funcs f
                where f.root_typid = r.root_typid
           ) as typsendfunc
      from resolved r
     group by r.root_typid
)

select a.attrelid,
       a.attnum,
       a.atttypid::integer,
       format('%I', a.attname) as attname,
       i.indrelid is not null as attisprimary,
       ri.indrelid is not null as attisreplident,
       col.is_generated = 'ALWAYS' as attisgenerated,
       coalesce(a.attidentity, '') as attidentity,
       tb.hasbinaryio,
       tb.typsendfunc

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
       join type_bincompat tb on tb.atttypid = a.atttypid

 where a.attrelid = any($1::oid[])
   and not a.attisdropped
   and a.attnum > 0

 order by a.attrelid, a.attnum
