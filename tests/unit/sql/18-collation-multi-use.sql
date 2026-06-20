select viewname
  from pg_views
 where viewname like 'v_coll_test%'
   and schemaname = 'public'
order by viewname;
