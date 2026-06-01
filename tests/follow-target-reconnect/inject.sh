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
dbfile=${TMPDIR}/pgcopydb/schema/source.db

deadline=$(($(date +%s) + 120))
until [ -s ${dbfile} ]
do
    if [ $(date +%s) -gt ${deadline} ]; then
        echo "TIMEOUT: pgcopydb did not initialize within 120s"
        exit 1
    fi
    sleep 1
done

#
# Inject 2 rounds of DML with WAL switches to generate CDC traffic before
# simulating the connection failure.
#
for i in `seq 2`
do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 1

    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 1

    psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'
    sleep 1
done

#
# Wait for CDC follow mode to be actively streaming to the target before
# simulating the failure. A non-zero flush_lsn means pgcopydb has begun
# applying changes to the target.
#
flushlsn="0/0"

deadline=$(($(date +%s) + 120))
until [ "${flushlsn}" != "0/0" ]
do
    if [ $(date +%s) -gt ${deadline} ]; then
        echo "TIMEOUT: pgcopydb CDC did not start streaming within 120s"
        exit 1
    fi
    flushlsn=$(pgcopydb stream sentinel get --flush-lsn 2>/dev/null || echo "0/0")
    sleep 2
done

#
# Simulate a connection failure by triggering the netblock sidecar, which
# shares the target's network namespace and inserts an iptables REJECT rule
# for port 5432. This sends an immediate TCP RST so pgcopydb detects the
# failure in milliseconds rather than waiting ~340 s for TCP retransmit.
#
# Signal the sidecar via a trigger file on the shared volume; it blocks for
# 15 s, removes the rule, then writes a done file.
#
# Clean up any stale signal files from previous runs before triggering.
rm -f /var/run/pgcopydb/block_target /var/run/pgcopydb/target_unblocked
touch /var/run/pgcopydb/block_target

deadline=$(($(date +%s) + 60))
until [ -f /var/run/pgcopydb/target_unblocked ]
do
    if [ $(date +%s) -gt ${deadline} ]; then
        echo "TIMEOUT: netblock sidecar did not complete within 60s"
        exit 1
    fi
    sleep 0.5
done

rm -f /var/run/pgcopydb/target_unblocked

# Target is accessible again since postgres never stopped.
deadline=$(($(date +%s) + 30))
until psql -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT 1" >/dev/null 2>&1
do
    if [ $(date +%s) -gt ${deadline} ]; then
        echo "TIMEOUT: target did not become reachable within 30s after unblock"
        exit 1
    fi
    sleep 1
done

#
# Inject 2 more rounds of DML after the reconnect to verify that changes
# applied after the outage are also captured and replayed correctly.
#
for i in `seq 2`
do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 1

    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 1

    psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_switch_wal()'
    sleep 1
done

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`

pgcopydb stream sentinel set endpos --current --debug
pgcopydb stream sentinel get

endpos=`pgcopydb stream sentinel get --endpos 2>/dev/null`

if [ ${endpos} = "0/0" ]
then
    echo "expected ${lsn} endpos, found ${endpos}"
    exit 1
fi

#
# Because we're using docker-compose --abort-on-container-exit make sure
# that the other process in the pgcopydb service is done before exiting
# here.
#
flushlsn="0/0"

deadline=$(($(date +%s) + 120))
while [ ${flushlsn} \< ${endpos} ]
do
    if [ $(date +%s) -gt ${deadline} ]; then
        echo "TIMEOUT: pgcopydb did not reach endpos ${endpos} within 120s (at ${flushlsn})"
        exit 1
    fi
    flushlsn=`pgcopydb stream sentinel get --flush-lsn 2>/dev/null`
    sleep 1
done

#
# Still give some time to the pgcopydb service to finish its processing,
# with the cleanup and all.
#
sleep 5
