-- Identity sequences owned by partitioned parents must be copied with their
-- position when a table filter is active.
select last_value, is_called from partitioned_identity.range_always_id_seq;
select last_value, is_called from partitioned_identity.list_by_default_id_seq;
select last_value, is_called from partitioned_identity.nested_always_id_seq;
