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
# Only start injecting DML traffic on the source database when pgcopydb
# follow command has been started. We know that by querying the SQLite
# catalogs database, where the prefetch/transform/catchup sub-processes
# register themselves in the process table.
#
dbf=${TMPDIR}/pgcopydb/schema/source.db

while [ ! -s ${dbf} ]
do
    sleep 1
done

sql="select pid from process where ps_type = 'prefetch'"
pidf=/tmp/prefetch.pid

while [ ! -s ${pidf} ]
do
    # sometimes we have "Error: database is locked", ignore
    sqlite3 -batch -bail -noheader ${dbf} "${sql}" > ${pidf} || echo error
    sleep 1
done

sqlite3 -batch -bail -noheader ${dbf} "select * from s_table"

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
sql="select exists(select 1 from pg_replication_slots where slot_name = 'pgcopydb')"

while [ `psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"` != 't' ]
do
    sleep 1
done

#
# Still give some time to the pgcopydb service to finish its processing,
# with the cleanup and all.
#
sleep 10
