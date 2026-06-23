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

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql
# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/special-ddl.sql
psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/special-dml.sql

# create the replication slot that captures all the changes
coproc ( pgcopydb snapshot --follow )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone --split-tables-larger-than 200kB

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject some SQL DML changes to the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/special-dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

#
# Receive CDC messages into the SQLite outputDB (output table).  In the
# 2-process model the receive step only fills the output table; the transform
# into stmt+replay happens later, inside the apply (catchup) process.
#
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

SHAREDIR=${XDG_DATA_HOME:-/var/lib/postgres/.local/share}/pgcopydb

ls -la ${SHAREDIR}/

#
# Validate that the SQLite output table contains the expected messages.
# Extract the JSON message field from every output row and compare against
# the golden file (ignoring volatile fields: lsn, xid, timestamp).
#
OUTPUTDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-output.db' -type f | head -1)

sqlite3 ${OUTPUTDB} "select count(*) as output_rows from output;"

sqlite3 -init /dev/null -json ${OUTPUTDB} \
  "select action, json(message) as message from output where action not in ('K','X','E') order by id" \
  > /tmp/result.jsonl

diff /usr/src/pgcopydb/output.jsonl /tmp/result.jsonl

#
# Run prefetch again — should be a no-op (idempotent).
#
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

#
# Allow the apply process and apply the CDC changes to the target.  The apply
# process performs the inline transform (output -> stmt+replay), creating the
# replayDB, then applies to the target.
#
pgcopydb stream sentinel set apply

pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# replayDB now exists: validate the replay table and the SQL templates
# written by the transform step.
#
REPLAYDB=$(find ${SHAREDIR} -maxdepth 1 -name '*-replay.db' -type f | head -1)

sqlite3 ${REPLAYDB} "select count(*) as replay_rows from replay;"

sqlite3 -init /dev/null -list -noheader ${REPLAYDB} \
  "select s.sql from stmt s join replay r on r.stmt_hash = s.hash where r.action not in ('B','C','R','K','X','E') group by s.hash order by min(r.id)" \
  > /tmp/stmt-actual.sql
diff /usr/src/pgcopydb/stmt.sql /tmp/stmt-actual.sql

# Applying the same endpos again should be a no-op (already reached).
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# Verify that the data made it to the target: compare row counts for a
# representative table that was modified by dml.sql.
#
src_count=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from actor"`
tgt_count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from actor"`

echo "source actor count: ${src_count}, target actor count: ${tgt_count}"
test "${src_count}" -eq "${tgt_count}"

# Verify that double precision values are replayed without loss of precision
# (issue #968): the previous %f format emitted only 6 decimal places.
psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} \
     -c "select id, val::text from float8_precision_test order by id" \
     > /tmp/src_float8.txt
psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} \
     -c "select id, val::text from float8_precision_test order by id" \
     > /tmp/tgt_float8.txt
diff /tmp/src_float8.txt /tmp/tgt_float8.txt

# cleanup
pgcopydb stream drop
