  select n.nspname, c.relname

    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace

   where c.relkind in ('r', 'm', 'p')
     and c.relpersistence in ('p', 'u')
     and n.nspname not like 'pg_%'
     and n.nspname <> 'information_schema'
     and n.nspname <> 'pgcopydb'
     and ($1::text[] is null or n.nspname = any($1::text[]))
     and ($2::text[] is null or n.nspname <> all($2::text[]))

order by n.nspname, c.relname;
