#! /bin/bash

set -x
# Without -e this script kept going after `pgcopydb clone --follow` had
# failed outright and still exited 0, so the test reported success while
# no CDC happened at all.  That is what hid the output_plugin_libraries
# breakage until #1057: every other test caught it immediately.
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# pgcopydb clone uses the environment variables
pgcopydb clone --follow --plugin wal2json --notice

# dml.sql inserts actor rows after logical decoding messages: all of them must
# have reached the target by the time follow is done
src_count=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from actor"`
tgt_count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from actor"`

echo "source actor count: ${src_count}, target actor count: ${tgt_count}"
test "${src_count}" -eq "${tgt_count}" || exit 1

#
# Verify the DDL message was actually applied to the target database: the
# "note" column must exist on the target's "rental" table (added by the
# "ddl" message emitted with our own "pgcopydb" prefix, see ddl-trigger.sql
# and inject.sh). wal2json decodes and applies this the same way pgoutput
# does.
#
note_col=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from information_schema.columns where table_name = 'rental' and column_name = 'note'")
echo "target rental.note column present (should be 1): ${note_col}"
test "${note_col}" -eq 1

#
# Verify that a message with a different prefix never gets applied: the
# "should_not_apply" column must NOT exist on the target (see messages.sql).
#
other_col=$(psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "select count(*) from information_schema.columns where table_name = 'rental' and column_name = 'should_not_apply'")
echo "target rental.should_not_apply column present (should be 0): ${other_col}"
test "${other_col}" -eq 0

# Query the SQLite CDC databases to verify the tables were populated.  In the
# 2-process model the `output` table lives in the *-output.db while `stmt` and
# `replay` live in the *-replay.db.
outdb=$(find ${TMPDIR}/cdc/pgcopydb -name "*-output.db" -type f | head -1)
repdb=$(find ${TMPDIR}/cdc/pgcopydb -name "*-replay.db" -type f | head -1)

if [ -n "$outdb" ] && [ -f "$outdb" ]; then
  sqlite3 "$outdb" \
    "select id, action, xid, lsn, substring(message, 1, 48) from output limit 10;"
else
  echo "CDC output database not found at ${TMPDIR}/cdc/pgcopydb/"
fi

if [ -n "$repdb" ] && [ -f "$repdb" ]; then
  sqlite3 "$repdb" "select hash, sql from stmt limit 5;"
  sqlite3 "$repdb" \
    "select id, action, xid, lsn, endlsn, stmt_hash, stmt_args from replay limit 10;"
else
  echo "CDC replay database not found at ${TMPDIR}/cdc/pgcopydb/"
fi

# cleanup
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2
pgcopydb stream cleanup
