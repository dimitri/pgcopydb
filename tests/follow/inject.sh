#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

#
# pgcopydb list extensions include a retry loop, so we use that as a proxy
# to depend on the source/target Postgres images to be ready
#
pgcopydb list extensions --source ${PGCOPYDB_SOURCE_PGURI}

#
# Only start injecting DML traffic on the source database when the pagila
# schema and base data set has been deployed already. Our proxy to know that
# that's the case is the existence of the pgcopydb.sentinel table on the
# source database.
#
sql="select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace where relname = 'sentinel' and nspname = 'pgcopydb' union all select 0 limit 1"
while [ `psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"` -ne 1 ]
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
for i in `seq 5`
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
pgcopydb stream sentinel set endpos --current
pgcopydb stream sentinel get

#
# Becaure we're using docker-compose --abort-on-container-exit make sure
# that the other process in the pgcopydb service is done before exiting
# here.
#
sql="select '${lsn}'::pg_lsn <= flush_lsn from pgcopydb.sentinel"

while [ `psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"` != 't' ]
do
    sleep 1
done

#
# Still give some time to the pgcopydb service to finish its processing,
# with the cleanup and all.
#
sleep 10
