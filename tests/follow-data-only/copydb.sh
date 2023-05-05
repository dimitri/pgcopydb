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

# take care of the schema manually
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql
psql -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/ddl.sql

# insert a first batch of 1000 rows
psql -v a=1 -v b=10 -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab a snapshot on the source database
coproc ( pgcopydb snapshot --notice )

sleep 1

# copy the data
pgcopydb copy table-data

# insert another batch of 1000 rows
psql -v a=11 -v b=20 -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# start following and applying changes from source to target
pgcopydb follow --plugin wal2json --notice

# the follow command ends when reaching endpos, set in inject.sh
pgcopydb stream cleanup

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# check how many rows we have on source and target
sql="select count(*), sum(some_field) from table_a"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out
