-- With include-only-table, a partitioned parent none of whose partitions are
-- selected is not copied, so its identity sequence must not be either.
select count(*)
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
 where c.relkind = 'S'
   and n.nspname = 'schema_name_20_chars'
   and c.relname = 'partitioned_identity_id_seq';
