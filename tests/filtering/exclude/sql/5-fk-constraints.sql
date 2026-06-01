-- FK referencing an excluded schema should NOT exist on target
select count(*) as fk_to_excluded_schema_count
  from pg_constraint c
  join pg_class cl on c.conrelid = cl.oid
  join pg_namespace n on cl.relnamespace = n.oid
 where c.contype = 'f'
   and n.nspname = 'foo'
   and cl.relname = 'tbl_with_cross_fk';

-- FK referencing an excluded table should NOT exist on target
select count(*) as fk_to_excluded_table_count
  from pg_constraint c
  join pg_class cl on c.conrelid = cl.oid
  join pg_namespace n on cl.relnamespace = n.oid
 where c.contype = 'f'
   and n.nspname = 'foo'
   and cl.relname = 'tbl_with_fk_to_excluded_tbl';
