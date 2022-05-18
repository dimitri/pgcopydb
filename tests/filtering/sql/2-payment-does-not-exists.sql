select exists
       (
         select 1
           from pg_class c
                join pg_namespace n on n.oid = c.relnamespace
          where n.nspname = 'public' and c.relname = 'payment'
       );
