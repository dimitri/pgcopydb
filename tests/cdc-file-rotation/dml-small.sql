--
-- pgcopydb test/cdc-file-rotation/dml-small.sql
--
-- 20 small independent transactions (each ~50 bytes of data).
-- With --max-replaydb-size 1kB this should trigger multiple rotations.

begin; insert into rotation_test(data) values ('small-txn-01'); commit;
begin; insert into rotation_test(data) values ('small-txn-02'); commit;
begin; insert into rotation_test(data) values ('small-txn-03'); commit;
begin; insert into rotation_test(data) values ('small-txn-04'); commit;
begin; insert into rotation_test(data) values ('small-txn-05'); commit;
begin; insert into rotation_test(data) values ('small-txn-06'); commit;
begin; insert into rotation_test(data) values ('small-txn-07'); commit;
begin; insert into rotation_test(data) values ('small-txn-08'); commit;
begin; insert into rotation_test(data) values ('small-txn-09'); commit;
begin; insert into rotation_test(data) values ('small-txn-10'); commit;
begin; insert into rotation_test(data) values ('small-txn-11'); commit;
begin; insert into rotation_test(data) values ('small-txn-12'); commit;
begin; insert into rotation_test(data) values ('small-txn-13'); commit;
begin; insert into rotation_test(data) values ('small-txn-14'); commit;
begin; insert into rotation_test(data) values ('small-txn-15'); commit;
begin; insert into rotation_test(data) values ('small-txn-16'); commit;
begin; insert into rotation_test(data) values ('small-txn-17'); commit;
begin; insert into rotation_test(data) values ('small-txn-18'); commit;
begin; insert into rotation_test(data) values ('small-txn-19'); commit;
begin; insert into rotation_test(data) values ('small-txn-20'); commit;
