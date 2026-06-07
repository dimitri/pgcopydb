--
-- pgcopydb test/cdc-file-rotation/dml-large.sql
--
-- A single transaction that inserts ~100kB of data.
-- With --max-replaydb-size 1kB this transaction exceeds the threshold
-- but must NOT be split across files (atomicity invariant).
--
-- Each row has ~1kB of payload (repeat('x', 1000)), and we insert 100 rows
-- in a single transaction, giving ~100kB total.

begin;
insert into rotation_test(data)
    select repeat('large-txn-row-' || n::text || '-', 60)
      from generate_series(1, 100) as n;
commit;
