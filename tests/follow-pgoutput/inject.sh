#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

pgcopydb ping

#
# Follow coordinator TCP endpoint (provided by docker-compose). We remote-control
# the sentinel over TCP, so this container does NOT share the SQLite catalog
# volume with the follow process.
#
HP="--host ${PGCOPYDB_HOST} --port ${PGCOPYDB_PORT}"

#
# Only start injecting DML traffic once the follow process is streaming: that is
# exactly when `pgcopydb clone --follow` has finished the initial copy and opened
# its TCP coordinator.  Poll the coordinator over TCP until it answers.
#
until pgcopydb stream sentinel get ${HP} >/dev/null 2>&1
do
    sleep 1
done

#
# Inject a batch of DML changes.
# Then switch WAL segment to demonstrate that the SQLite CDC pipeline is
# independent of PostgreSQL WAL segment boundaries (in the old file-based
# design, WAL switches were critical milestones; now they're invisible).
#
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

# Inject another batch on the new WAL segment to show pipeline continues
# across segment boundaries without any special handling.
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

# Set endpos to current flush LSN to signal follow where to stop (over TCP)
echo "Setting endpos to current WAL position..."
pgcopydb stream sentinel set endpos --current --debug ${HP} || { echo "Failed to set endpos"; exit 1; }
echo "Final sentinel state:"
pgcopydb stream sentinel get ${HP}

endpos=`pgcopydb stream sentinel get --endpos ${HP} 2>/dev/null`

if [ -z "${endpos}" ] || [ "${endpos}" = "0/0" ]
then
    echo "ERROR: endpos not set correctly (got: ${endpos})"
    exit 1
fi
echo "Successfully set endpos to ${endpos}"

#
# Because we're using docker-compose --abort-on-container-exit make sure
# that the other process in the pgcopydb service is done before exiting
# here. Wait for flush_lsn to reach endpos (indicates CDC pipeline caught up).
#
flushlsn="0/0"

while [ ${flushlsn} \< ${endpos} ]
do
    flushlsn=`pgcopydb stream sentinel get --flush-lsn ${HP} 2>/dev/null`
    sleep 1
done

#
# Give some time to the pgcopydb service to finish cleanup.
#
sleep 5
