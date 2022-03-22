DROP TABLE IF EXISTS exclcon;

CREATE TABLE exclcon (a integer, b integer);

insert into exclcon values (1,2), (3,4);

ALTER TABLE exclcon
  ADD CONSTRAINT constraint_fail_idx EXCLUDE USING btree (a WITH=);
