with ordered as (
  select
    table_name::text                                            as tname,
    distribution_column,
    colocation_id,
    shard_count,
    row_number() over (partition by colocation_id
                       order by table_name::text)              as rn,
    first_value(table_name::text) over (
        partition by colocation_id
        order by table_name::text)                             as anchor
  from citus_tables
  where citus_table_type = 'distributed'
)
 select
  case
    when rn = 1 then
      format(
        'select create_distributed_table(%L, %L, shard_count := %s, colocate_with := %L)',
        tname, distribution_column, shard_count, 'none')
    else
      format(
        'select create_distributed_table(%L, %L, colocate_with := %L)',
        tname, distribution_column, anchor)
  end
 from ordered
 order by colocation_id, rn;
