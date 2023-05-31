-- transaction that spans multiple WAL files

BEGIN;

INSERT INTO table_a(some_field) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

INSERT INTO table_a(some_field) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

INSERT INTO table_a(some_field) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

COMMIT;

