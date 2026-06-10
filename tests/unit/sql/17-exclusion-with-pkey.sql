  select c.conname, c.contype, pg_get_constraintdef(c.oid) as condef
    from pg_constraint c
         join pg_class r on r.oid = c.conrelid
         join pg_namespace n on n.oid = r.relnamespace
   where n.nspname = 'public' and r.relname = 'excl_with_pkey'
order by c.contype, c.conname;
