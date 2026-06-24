  select n.nspname,
         quote_ident(n.nspname) as restorelistname

    from pg_catalog.pg_namespace n

   where n.nspname ~ $1
     and n.nspname not like 'pg_%'
     and n.nspname <> 'information_schema'
     and n.nspname <> 'pgcopydb'

order by n.nspname;
