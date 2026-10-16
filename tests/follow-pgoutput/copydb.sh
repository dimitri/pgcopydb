#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

# Verify that the source has a short idle_in_transaction_session_timeout so
# that this test exercises the fix for #960: GUC settings must be applied on
# the replication connection before CREATE_REPLICATION_SLOT is sent, otherwise
# a strict server-side timeout would abort slot creation.
timeout_ms=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
  -c "show idle_in_transaction_session_timeout")
echo "source idle_in_transaction_session_timeout: ${timeout_ms}"
test "${timeout_ms}" = "1s"

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# pgcopydb clone uses the environment variables (pgoutput plugin, auto-managed publication)
# DDL-as-message decoding/apply is always on (no CLI flag): the "pgcopydb
# stream event-trigger" subcommand (see inject.sh) is the only opt-in step.
pgcopydb clone --follow --plugin pgoutput --notice

#
# Verify that the data made it to the target (row count assertions).
#
for tbl in actor film category address; do
  src_count=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from ${tbl}")
  tgt_count=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from ${tbl}")
  echo "source ${tbl}: ${src_count}, target ${tbl}: ${tgt_count}"
  test "${src_count}" -eq "${tgt_count}"
done

#
# Verify the DDL message was actually applied to the target database: the
# "note" column must exist on the target's "rental" table (added by the
# "ddl" message emitted with our own "pgcopydb" prefix, see messages.sql).
#
note_col=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from information_schema.columns where table_name = 'rental' and column_name = 'note'")
echo "target rental.note column present (should be 1): ${note_col}"
test "${note_col}" -eq 1

#
# Verify that a message with a different prefix never gets applied: the
# "should_not_apply" column must NOT exist on the target.
#
other_col=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from information_schema.columns where table_name = 'rental' and column_name = 'should_not_apply'")
echo "target rental.should_not_apply column present (should be 0): ${other_col}"
test "${other_col}" -eq 0

# Query the SQLite CDC databases to verify the tables were populated.
outdb=$(find ${TMPDIR}/cdc/pgcopydb -name "*-output.db" -type f | head -1)
repdb=$(find ${TMPDIR}/cdc/pgcopydb -name "*-replay.db" -type f | head -1)

if [ -n "$outdb" ] && [ -f "$outdb" ]; then
  sqlite3 "$outdb" \
    "select id, action, xid, lsn, nspname, relname from output limit 10;"

  #
  # Validate that the pgoutput_col table exists and has rows.
  #
  col_rows=$(sqlite3 -init /dev/null -noheader -list "$outdb" \
    "select count(*) from pgoutput_col;")
  echo "pgoutput_col rows: ${col_rows}"
  test "${col_rows}" -gt 0

  #
  # Validate that DML rows have nspname/relname set and NULL message.
  #
  null_msg=$(sqlite3 -init /dev/null -noheader -list "$outdb" \
    "select count(*) from output where action in ('I','U','D','T') and message is not null;")
  echo "DML rows with non-NULL message (should be 0): ${null_msg}"
  test "${null_msg}" -eq 0

  nspname_set=$(sqlite3 -init /dev/null -noheader -list "$outdb" \
    "select count(*) from output where action in ('I','U','D') and nspname is not null;")
  echo "DML rows with nspname set: ${nspname_set}"
  test "${nspname_set}" -gt 0

  #
  # Validate that the "M" (MESSAGE) rows were decoded: our own-prefix "ddl"
  # message and the unknown-type message both reach the output table, and
  # so does the "endpos" marker message that setting --current emits (see
  # pgsql_emit_endpos_marker) -- the different-prefix message is filtered
  # out at decode time and must NOT appear here at all.
  #
  message_rows=$(sqlite3 -init /dev/null -noheader -list "$outdb" \
    "select count(*) from output where action = 'M';")
  echo "decoded MESSAGE rows (should be 3: our ddl + the unknown type + the endpos marker): ${message_rows}"
  test "${message_rows}" -eq 3

  other_prefix_rows=$(sqlite3 -init /dev/null -noheader -list "$outdb" \
    "select count(*) from output where action = 'M' and message like '%should_not_apply%';")
  echo "MESSAGE rows from a different prefix (should be 0): ${other_prefix_rows}"
  test "${other_prefix_rows}" -eq 0
else
  echo "CDC output database not found at ${TMPDIR}/cdc/pgcopydb/"
  exit 1
fi

if [ -n "$repdb" ] && [ -f "$repdb" ]; then
  sqlite3 "$repdb" "select hash, sql from stmt limit 5;"
  sqlite3 "$repdb" \
    "select id, action, xid, lsn, endlsn, stmt_hash, stmt_args from replay limit 10;"
else
  echo "CDC replay database not found at ${TMPDIR}/cdc/pgcopydb/"
  exit 1
fi

# cleanup (drops the auto-managed publication)
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2
pgcopydb stream cleanup

#
# Validate that the auto-managed publication was dropped by cleanup.
#
pub_after=$(psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
  -c "select count(*) from pg_publication where pubname = 'pgcopydb'" 2>/dev/null || echo 0)
echo "publication rows after cleanup (should be 0): ${pub_after}"
test "${pub_after}" -eq 0
