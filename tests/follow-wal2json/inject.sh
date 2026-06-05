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
# Only start injecting DML traffic on the source database when the pagila
# schema and base data set has been deployed already. Our proxy to know that
# that's the case is the existence of the pgcopydb.sentinel table on the
# source database.
#
dbfile=$(find ${TMPDIR}/pgcopydb/schema -name "source.db" -type f 2>/dev/null | head -1)

until [ -n "${dbfile}" ] && [ -s "${dbfile}" ]
do
    dbfile=$(find ${TMPDIR}/pgcopydb/schema -name "source.db" -type f 2>/dev/null | head -1)
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

# Set endpos to current flush LSN to signal follow where to stop
echo "Setting endpos to current WAL position..."
pgcopydb stream sentinel set endpos --current --debug || { echo "Failed to set endpos"; exit 1; }
echo "Final sentinel state:"
pgcopydb stream sentinel get

endpos=`pgcopydb stream sentinel get --endpos 2>/dev/null`

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
    flushlsn=`pgcopydb stream sentinel get --flush-lsn 2>/dev/null`
    sleep 1
done

#
# Give some time to the pgcopydb service to finish cleanup.
#
sleep 5
