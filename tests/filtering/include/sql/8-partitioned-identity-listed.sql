-- Selecting a partition of a partitioned table copies the identity sequence
-- of its parent, with its position.
select last_value, is_called
  from schema_name_20_chars.partitioned_identity_listed_id_seq;
