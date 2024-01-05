select exists
       (
         select 1
           from pg_class c
                join pg_namespace n on n.oid = c.relnamespace
          where n.nspname = 'app' and c.relname = 'foo'
       );
