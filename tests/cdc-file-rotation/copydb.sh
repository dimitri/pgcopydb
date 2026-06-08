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

# Create the test schema on source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# create the replication slot that captures all the changes
coproc ( pgcopydb snapshot --follow )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone copies the (empty) schema to the target
pgcopydb clone

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

#
# Phase 1: inject 20 small transactions.  With --max-replaydb-size 1kB these
# should trigger several rotations.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml-small.sql

#
# Phase 2: inject one large (~100 kB) transaction.  Despite far exceeding the
# 1 kB threshold, the entire transaction must land in a single output.db file.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml-large.sql

# grab the current LSN as our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

#
# Receive: stream all changes into the SQLite outputDB files.
# --max-replaydb-size 1kB forces rotation after nearly every transaction.
#
pgcopydb stream prefetch --resume --endpos "${lsn}" --max-replaydb-size 1kB -vv

SHAREDIR=${XDG_DATA_HOME:-/var/lib/postgres/.local/share}/pgcopydb

ls -la ${SHAREDIR}/

#
# Verify that rotation happened: there should be at least 2 output.db files.
#
output_count=$(find ${SHAREDIR} -maxdepth 1 -name '*-output.db' -type f | wc -l | tr -d ' ')
echo "output.db file count: ${output_count}"
test "${output_count}" -ge 2

#
# Verify cdc_files tracking: all files except the last should have non-NULL
# endpos (done_time_epoch IS NOT NULL).
#
SOURCE_DB=${TMPDIR:-/tmp}/pgcopydb/schema/source.db

total_files=$(sqlite3 -init /dev/null -noheader -list ${SOURCE_DB} "select count(*) from cdc_files;")
closed_files=$(sqlite3 -init /dev/null -noheader -list ${SOURCE_DB} "select count(*) from cdc_files where done_time_epoch is not null;")
open_files=$(sqlite3 -init /dev/null -noheader -list ${SOURCE_DB} "select count(*) from cdc_files where done_time_epoch is null;")

echo "cdc_files: total=${total_files} closed=${closed_files} open=${open_files}"

# exactly one file should remain open (the current one)
test "${open_files}" -eq 1
# at least one file should be closed (rotation happened)
test "${closed_files}" -ge 1

#
# Verify large-transaction atomicity: find the output.db file that contains
# the large transaction rows.  That file must contain ALL 100 large rows —
# none must have been split across files.
#
large_row_count=0
for dbfile in $(find ${SHAREDIR} -maxdepth 1 -name '*-output.db' -type f | sort); do
    n=$(sqlite3 -init /dev/null -noheader -list ${dbfile} \
        "select count(*) from output where message like '%large-txn-row%';" 2>/dev/null || echo 0)
    echo "  ${dbfile}: ${n} large-txn rows"
    if [ "${n}" -gt 0 ]; then
        large_row_count=$((large_row_count + n))
        # All large rows must be in a single file (not split)
        test "${n}" -eq 100
    fi
done

echo "Total large-txn rows across all files: ${large_row_count}"
test "${large_row_count}" -eq 100

#
# Apply: transform and apply all CDC changes to the target.
#
pgcopydb stream sentinel set apply

pgcopydb stream catchup --resume --endpos "${lsn}" --max-replaydb-size 1kB -vv

#
# Verify that data was applied correctly: row counts must match.
#
src_count=`psql -AtqX -d ${PGCOPYDB_SOURCE_PGURI} -c "select count(*) from rotation_test"`
tgt_count=`psql -AtqX -d ${PGCOPYDB_TARGET_PGURI} -c "select count(*) from rotation_test"`

echo "source rotation_test count: ${src_count}, target: ${tgt_count}"
test "${src_count}" -eq "${tgt_count}"

# Total rows: 20 small + 100 large = 120
test "${src_count}" -eq 120

# cleanup
pgcopydb stream cleanup
