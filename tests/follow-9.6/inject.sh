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
# Follow coordinator TCP endpoint (from docker-compose). We remote-control the
# sentinel over TCP, so this container does not share the SQLite catalog volume.
#
HP="--host ${PGCOPYDB_HOST} --port ${PGCOPYDB_PORT}"

#
# Only start injecting DML traffic once the follow process is streaming: that is
# when `pgcopydb clone --follow` has finished the initial copy and opened its TCP
# coordinator.  Poll it over TCP until it answers.
#
until pgcopydb stream sentinel get ${HP} >/dev/null 2>&1
do
    sleep 1
done

#
# Inject changes from our DML file in a loop, again and again.
#
# Every other round of DML changes, we also force the source server to
# switch to another WAL file, to test that our streaming solution can follow
# WAL file changes.
#
switchwal='select pg_switch_xlog()'

if [ "${PGVERSION}" == "10" ]
then
    switchwal='select pg_switch_wal()'
fi

for i in `seq 5`
do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 1

    psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 1

    psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${switchwal}"
    sleep 1
done

# grab the current LSN, it's going to be our streaming end position
if [ "${PGVERSION}" == "9.5" ]
then
    sql="select pg_current_xlog_location()"
elif [ "${PGVERSION}" == "9.6" ]
then
    sql="select pg_current_xlog_flush_location()"
else
    sql="select pg_current_wal_flush_lsn()"
fi

# print current lsn, see that it's the same when using --current
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`

pgcopydb stream sentinel set endpos --current ${HP}
pgcopydb stream sentinel get ${HP}

endpos=`pgcopydb stream sentinel get --endpos ${HP} 2>/dev/null`

if [ ${endpos} = "0/0" ]
then
    echo "expected ${lsn} endpos, found ${endpos}"
    exit 1
fi

#
# Becaure we're using docker compose --abort-on-container-exit make sure
# that the other process in the pgcopydb service is done before exiting
# here.
#
flushlsn="0/0"

while [ ${flushlsn} \< ${endpos} ]
do
    flushlsn=`pgcopydb stream sentinel get --flush-lsn ${HP} 2>/dev/null`
    sleep 1
done

#
# Still give some time to the pgcopydb service to finish its processing,
# with the cleanup and all.
#
sleep 10
