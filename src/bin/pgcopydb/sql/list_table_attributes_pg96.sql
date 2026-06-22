select a.attrelid,
       a.attnum,
       a.atttypid::integer,
       format('%I', a.attname) as attname,
       i.indrelid is not null as attisprimary,
       ri.indrelid is not null as attisreplident,
       col.is_generated = 'ALWAYS' as attisgenerated,
       '' as attidentity

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

 where a.attrelid = any($1::oid[])
   and not a.attisdropped
   and a.attnum > 0

 order by a.attrelid, a.attnum
