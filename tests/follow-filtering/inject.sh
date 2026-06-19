#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_HOST / PGCOPYDB_PORT  (follow coordinator TCP endpoint)

pgcopydb ping

HP="--host ${PGCOPYDB_HOST} --port ${PGCOPYDB_PORT}"

# Wait until clone --follow is streaming (coordinator is up)
until pgcopydb stream sentinel get ${HP} >/dev/null 2>&1
do
    sleep 1
done

# Inject DML changes (does NOT touch staff — it is excluded)
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'

# Signal follow to stop at current WAL position
echo "Setting endpos..."
pgcopydb stream sentinel set endpos --current --debug ${HP} || { echo "Failed to set endpos"; exit 1; }
echo "Final sentinel state:"
pgcopydb stream sentinel get ${HP}

endpos=$(pgcopydb stream sentinel get --endpos ${HP} 2>/dev/null)

if [ -z "${endpos}" ] || [ "${endpos}" = "0/0" ]; then
    echo "ERROR: endpos not set correctly (got: ${endpos})"
    exit 1
fi
echo "Successfully set endpos to ${endpos}"

# Wait until flush_lsn reaches endpos before exiting
flushlsn="0/0"
while [ "${flushlsn}" \< "${endpos}" ]
do
    flushlsn=$(pgcopydb stream sentinel get --flush-lsn ${HP} 2>/dev/null)
    sleep 1
done

sleep 5
