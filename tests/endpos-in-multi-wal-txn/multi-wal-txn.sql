-- transaction that spans multiple WAL files

BEGIN;

INSERT INTO table_a(f1) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

INSERT INTO table_a(f1) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

INSERT INTO table_a(f1) VALUES (101), (102), (103), (104), (:a);

SELECT
    pg_switch_wal();

INSERT INTO table_a(f1) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

COMMIT;
