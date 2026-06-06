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
coproc ( pgcopydb snapshot --follow --plugin test_decoding --notice )

sleep 2

# stop the background traffic
kill -TERM ${BACKGROUND_TRAFFIC_PID}

# run a transaction that spans multiple wal files. Again, we are doing this here
# instead of inject service to ensure this data is captured during prefetch
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/multi-wal-txn.sql

# In the SQLite design, slot info is stored in the catalog, not in a separate file.
# Verify the replication slot was created by querying the catalog.
sqlite3 ${TMPDIR}/pgcopydb/schema/source.db <<EOF
select slot_name, lsn, snapshot, plugin from replication_slot;
EOF

# check the sqlite setup contents too
sqlite3 ${TMPDIR}/pgcopydb/schema/source.db <<EOF
.mode line
select * from setup;
EOF

# copy the data
pgcopydb copy table-data

# insert another batch of 10 rows (11..20)
psql -v a=11 -v b=20 -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

 # start following and applying changes from source to target
pgcopydb follow --plugin test_decoding --notice

# the follow command ends when reaching endpos, set in inject.sh
kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# In the SQLite design, the stmt/replay tables live in the *-replay.db
# (the *-output.db only holds the output table).
db=$(find ${TMPDIR}/cdc/pgcopydb -name "*-replay.db" -type f | head -1)
if [ -z "${db}" ]; then
  echo "Error: No CDC replay database file found in ${TMPDIR}/cdc/pgcopydb/" >&2
  exit 1
fi
sqlite3 "${db}" <<EOF
select count(*) as null_text_sqls from replay
  where stmt_hash in (select hash from stmt where sql like '%null%');
EOF

# cleanup files and replication slot now
pgcopydb stream cleanup

# check how many rows we have on source and target
sql="select count(*), sum(f1) from table_a"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out

sql="select f1, length(f2) from table_a where f2 is not null order by f1"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out

sql="select id, text_col, json_col from null_texts order by id"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out

sql="select * from update_test"
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}" > /tmp/s.out
psql -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}" > /tmp/t.out

diff /tmp/s.out /tmp/t.out
