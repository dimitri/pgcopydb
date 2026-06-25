select format('select create_reference_table(%L)', table_name::text)
  from citus_tables
 where citus_table_type = 'reference'
 order by table_name::text;
