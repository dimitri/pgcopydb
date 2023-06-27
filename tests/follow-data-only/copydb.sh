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

# insert a first batch of 10 rows (1..10)
psql -v a=1 -v b=10 -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# take a snapshot with concurrent activity happening. As it would be hard to
# sync concurrent activity in the inject service, so use a job instead
bash ./run-background-traffic.sh &
BACKGROUND_TRAFFIC_PID=$!

# wait for few seconds to allow snapshot to happen in between the traffic
sleep 2

# grab a snapshot on the source database
coproc ( pgcopydb snapshot --follow --plugin wal2json --notice )

sleep 2

# stop the background traffic
kill -TERM ${BACKGROUND_TRAFFIC_PID}

# run a transaction that spans multiple wal files. Again, we are doing this here
# instead of inject service to ensure this data is captured during prefetch
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/multi-wal-txn.sql

# check the replication slot file contents
cat /var/lib/postgres/.local/share/pgcopydb/slot

# copy the data
pgcopydb copy table-data

# insert another batch of 10 rows (11..20)
psql -v a=11 -v b=20 -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# start following and applying changes from source to target
pgcopydb follow --verbose

# the follow command ends when reaching endpos, set in inject.sh
pgcopydb stream cleanup

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# check how many rows we have on source and target
sql="select count(*), sum(f1) from table_a"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out

sql="select f1, f2 from table_a where f2 is not null order by f1"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out
