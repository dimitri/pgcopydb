FROM pagila

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh              copydb.sh
COPY ./ddl.sql                ddl.sql
COPY ./source_schema.sql      source_schema.sql
COPY ./fixture_test_decoding.jsonl  fixture_test_decoding.jsonl
COPY ./fixture_wal2json.jsonl       fixture_wal2json.jsonl
COPY ./expected_stmts_test_decoding.sql  expected_stmts_test_decoding.sql
COPY ./expected_stmts_wal2json.sql       expected_stmts_wal2json.sql
COPY ./expected_apply_test_decoding.sql  expected_apply_test_decoding.sql
COPY ./expected_apply_wal2json.sql       expected_apply_wal2json.sql

USER docker
CMD ["/usr/src/pgcopydb/copydb.sh"]
