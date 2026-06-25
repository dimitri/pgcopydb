-- The [include-only-schema] regex ~/^seq$/ must pull seq.default_table and
-- seq.identity_table into the target.  Count should be 2.
select count(*)
  from pg_tables
 where schemaname = 'seq';
