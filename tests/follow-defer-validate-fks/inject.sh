#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI

# ensure TMPDIR is writable
sudo mkdir -p ${TMPDIR}
sudo chown -R `whoami` ${TMPDIR}

pgcopydb ping

#
# Wait for the pgcopydb schema catalog (source.db) to exist, indicating
# that the test service has loaded the schema and started clone --follow.
#
dbfile=${TMPDIR}/pgcopydb/schema/source.db

until [ -s ${dbfile} ]
do
    sleep 1
done

echo "=== source.db exists, pgcopydb has started ==="

#
# Wait for the TIMING_SECTION_SNAPSHOT_DONE signal in the SQLite catalog.
# This is written by the clone subprocess after COPY completes. With
# --defer-indexes --follow, the clone subprocess exits after this signal,
# and the parent process releases the snapshot.
#
echo "=== Waiting for COPY phase to complete (snapshot-done signal) ==="

snapshot_done=false
for i in $(seq 120)
do
    # Query the SQLite catalog for the snapshot-done timing entry
    done_time=$(sqlite3 -batch -bail -noheader ${dbfile} \
        "SELECT COALESCE(done_time_epoch, 0) FROM timings
          WHERE label = 'Snapshot Done'" 2>/dev/null || echo "0")

    if [ "${done_time}" != "0" ] && [ "${done_time}" != "" ]; then
        echo "=== snapshot-done signal found, COPY phase is complete ==="
        snapshot_done=true
        break
    fi
    sleep 1
done

if [ "${snapshot_done}" != "true" ]; then
    echo "ERROR: Timed out waiting for snapshot-done signal after 120 seconds"
    echo "  The COPY phase did not complete or the signal was not written."
    exit 1
fi

# Give the parent process time to close the snapshot
sleep 5

#
# Verify the slot's xmin is now NULL (snapshot released after COPY).
# This is the key assertion for the early-release feature.
#
echo "=== Slot state after COPY phase ==="
psql -d ${PGCOPYDB_SOURCE_PGURI} -c \
    "SELECT slot_name, xmin, catalog_xmin, active, restart_lsn
       FROM pg_replication_slots
      WHERE slot_name = 'pgcopydb'"

slot_xmin=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
    "SELECT COALESCE(xmin::text, 'NULL')
       FROM pg_replication_slots
      WHERE slot_name = 'pgcopydb'")

echo "Slot xmin after COPY: ${slot_xmin}"

if [ "${slot_xmin}" != "NULL" ]; then
    echo "ERROR: Slot xmin is still set to ${slot_xmin}"
    echo "  Expected NULL after COPY phase completes."
    echo "  The snapshot was not released — this is the bug we are fixing."
    exit 1
fi

echo "=== VERIFIED: xmin is NULL — snapshot released after COPY ==="

#
# Now inject DML changes while the parent continues with STEP 10 (indexes).
# This exercises the CDC path after the snapshot was released.
#
echo "=== Injecting DML changes ==="

for i in $(seq 5)
do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 1

    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 1

    psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'
    sleep 1
done

# grab the current LSN, it's going to be our streaming end position
lsn=$(psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()')

pgcopydb stream sentinel set endpos --current --debug
pgcopydb stream sentinel get

endpos=$(pgcopydb stream sentinel get --endpos 2>/dev/null)

if [ "${endpos}" = "0/0" ]; then
    echo "expected ${lsn} endpos, found ${endpos}"
    exit 1
fi

#
# Wait for pgcopydb to catch up to the endpos.
#
flushlsn="0/0"

while [ "${flushlsn}" \< "${endpos}" ]
do
    flushlsn=$(pgcopydb stream sentinel get --flush-lsn 2>/dev/null)
    sleep 1
done

#
# Give the test service time to finish cleanup.
#
sleep 10
