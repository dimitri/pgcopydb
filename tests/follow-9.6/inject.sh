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

until [ -s ${dbfile} ]
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

pgcopydb stream sentinel set endpos --current
pgcopydb stream sentinel get

endpos=`pgcopydb stream sentinel get --endpos 2>/dev/null`

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
    flushlsn=`pgcopydb stream sentinel get --flush-lsn 2>/dev/null`
    sleep 1
done

#
# Still give some time to the pgcopydb service to finish its processing,
# with the cleanup and all.
#
sleep 10
