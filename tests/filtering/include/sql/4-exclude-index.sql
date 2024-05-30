select exists(select schemaname, tablename, indexname
  from pg_indexes
 where schemaname = 'public'
  and tablename = 'actor'
  and indexname = 'idx_actor_last_name'
);
