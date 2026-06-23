#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

verify() { python3 /usr/src/pgcopydb/verify.py "$1"; }

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
lsn=$(psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()')

#
# Receive: stream all changes into the SQLite outputDB files.
# --max-replaydb-size 1kB forces rotation after nearly every transaction.
#
pgcopydb stream prefetch --resume --endpos "${lsn}" --max-replaydb-size 1kB -vv

ls -la ${XDG_DATA_HOME:-/var/lib/postgres/.local/share}/pgcopydb/

verify output-rotation
verify cdc-files
verify large-txn-atomicity

#
# Apply: transform and apply all CDC changes to the target.
#
pgcopydb stream sentinel set apply

pgcopydb stream catchup --resume --endpos "${lsn}" --max-replaydb-size 1kB -vv

verify row-counts

#
# stream prune: remove already-applied CDC file pairs to reclaim disk space.
# Files whose endpos < sentinel.replay_lsn are safe to delete; the slot will
# never re-deliver those transactions.
#
ls -la ${XDG_DATA_HOME:-/var/lib/postgres/.local/share}/pgcopydb/

pgcopydb stream prune

ls -la ${XDG_DATA_HOME:-/var/lib/postgres/.local/share}/pgcopydb/

verify cleanup

# tear down the replication slot and origin
pgcopydb stream cleanup
