#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

#
# Only start injecting DML traffic on the source database ready. Our proxy
# to know that that's the case is the existence of the pgcopydb.sentinel
# table on the source database.
#
sql="select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace where relname = 'sentinel' and nspname = 'pgcopydb' union all select 0 limit 1"
while [ `psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"` -ne 1 ]
do
    sleep 1
done

# allow replaying changes now that pgcopydb follow command is running
pgcopydb stream sentinel set apply

# then insert another batch of 10 rows (21..30)
psql -v a=21 -v b=30 -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_flush_lsn()'`
pgcopydb stream sentinel set endpos --current
pgcopydb stream sentinel get

#
# Becaure we're using docker-compose --abort-on-container-exit make sure
# that the other process in the pgcopydb service is done before exiting
# here.
#
sql="select '${lsn}'::pg_lsn <= replay_lsn from pgcopydb.sentinel"

while [ `psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"` != 't' ]
do
    sleep 1
done

#
# Still give some time to the pgcopydb service to finish its processing,
# with the cleanup and all.
#
sleep 10
