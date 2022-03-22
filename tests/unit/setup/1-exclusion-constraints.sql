CREATE TABLE recreate (a integer, b integer);

insert into recreate values (1,2), (3,4);

ALTER TABLE recreate
  ADD CONSTRAINT constraint_fail_idx EXCLUDE USING btree (a WITH=);
